import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { setImmediate as nextMacrotask } from 'node:timers/promises';
import type { WorkflowClock } from '../interfaces/workflow-clock.interface.js';
import { toMs, type Duration } from '../utils/duration.util.js';
import { WorkflowNonDeterminismError } from '../errors/workflow-non-determinism.error.js';
import { WorkflowDefinitionError } from '../errors/workflow-definition.error.js';
import { StepTimeoutError } from '../errors/step-timeout.error.js';
import { WorkflowFailedError } from '../errors/workflow-failed.error.js';
import { StepFailedError } from '../errors/step-failed.error.js';
import { NonRetryableStepError } from '../errors/non-retryable-step.error.js';
import {
  isWorkflowInterrupt,
  WorkflowInterrupt,
  type InterruptReason,
} from '../errors/workflow-interrupt.error.js';
import { serializeError } from '../utils/serialize-error.util.js';
import { runInStepScope } from '../utils/step-scope.util.js';
import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';
import type {
  Journaled,
  WaitForSignalOptions,
  WorkflowCompensationContext,
  WorkflowContext,
  WorkflowStepContext,
  WorkflowStepOptions,
} from '../interfaces/workflow-context.interface.js';
import type {
  WorkflowInstance,
  WorkflowJournalEntry,
  WorkflowWait,
} from '../interfaces/workflow-instance.interface.js';
import type { WorkflowRetryOptions } from '../interfaces/workflow-retry-options.interface.js';
import { resolveRetry, retryDelay, type ResolvedRetry } from '../utils/retry.util.js';
import type { WorkflowEvents } from '../events/workflow-events.service.js';
import type { WorkflowEvent } from '../events/workflow-events.interface.js';
import { signalName, type WorkflowSignal } from '../signals/workflow.signal.js';
import type { WorkflowStore } from '../interfaces/workflow-store.interface.js';

/** An instance under a worker's lease. */
export interface ClaimedWorkflowInstance extends WorkflowInstance {
  leaseToken: string;
}

/** One entry per name, as the store expects: the last version of each, in the order the names first appear. */
export function uniqueEntries<T extends { name: string }>(entries: T[]): T[] {
  const latest = new Map<string, T>();
  for (const entry of entries) {
    latest.set(entry.name, entry);
  }
  return [...latest.values()];
}

/**
 * Round-trips through JSON so the first run sees exactly what every replay
 * will see (a `Date` becomes a string in both, `undefined` stays `undefined`).
 */
export function normalize<T>(value: T): T {
  if (value === undefined) {
    return value;
  }
  const json = JSON.stringify(value);
  return json === undefined ? (undefined as T) : JSON.parse(json);
}

type EventBody = WorkflowEvent extends infer E
  ? E extends WorkflowEvent
    ? Omit<E, 'id' | 'workflow' | 'version' | 'at'>
    : never
  : never;

type JournalKind = WorkflowJournalEntry['kind'];
type RetrySetting = number | false | WorkflowRetryOptions | undefined;

export interface ExecutionDeps {
  store: WorkflowStore;
  clock: WorkflowClock;
  events: WorkflowEvents;
  defaultRetry: ResolvedRetry;
  leaseMs: number;
}

/** A step's compensation, reserved when the step is called and armed when it completes. */
interface Compensation {
  step: string;
  completed: boolean;
  result?: unknown;
  fn: (result: any, ctx: WorkflowCompensationContext) => unknown;
  retry: RetrySetting;
}

export type RunOutcome = { ok: true; value: unknown } | { ok: false; error: unknown };

export type CompensationOutcome =
  | { state: 'done'; count: number }
  | { state: 'suspended'; wakeAt: number | null }
  | { state: 'failed'; error: SerializedWorkflowError }
  | { state: 'interrupted' };

/** Step or compensation options, validated before the attempt is recorded. */
interface AttemptPlan {
  policy: ResolvedRetry;
  timeoutMs?: number;
  heartbeatTimeoutMs?: number;
}

/**
 * One execution of a workflow instance: runs `run()` from the top against the
 * journal loaded at claim time.
 *
 * - A journaled step returns its stored result (or re-throws its stored
 *   failure) without running.
 * - The first step that is not journaled is the frontier. Before it runs, the
 *   execution lets the replay settle and checks that every journaled entry was
 *   reached. If one was not, the code changed under a running instance and the
 *   instance fails instead of re-running a renamed side effect.
 * - A pending sleep, wait or retry backoff records a suspension and throws
 *   `WorkflowInterrupt`. After that no new step starts and nothing new is
 *   journaled except other sleeps and waits of the same `Promise.all`; the
 *   engine parks the instance once in-flight steps settle.
 * - A cancel, shutdown, lost lease or store failure also throws
 *   `WorkflowInterrupt`, and decides the outcome whatever `run()` does with it:
 *   swallowing or wrapping it can't complete the instance.
 * - In replay-only mode (compensating or cancelling after a restart) nothing
 *   new runs: the replay only rebuilds the list of compensations.
 */
export class WorkflowExecution {
  readonly context: WorkflowContext;
  readonly journal: Map<string, WorkflowJournalEntry>;
  readonly abort = new AbortController();

  suspension: { wakeAt: number | null; waits: WorkflowWait[] } | null = null;
  fatal: Error | null = null;
  leaseLost = false;
  storeError: unknown = null;
  shuttingDown = false;
  detached = false;
  cancelRequested = false;
  /** Attempt counters to restore when a shutdown interrupts an attempt. */
  readonly rollbacks: WorkflowJournalEntry[] = [];

  private mode: 'run' | 'replay' | 'closed' | 'compensate';
  private userSettled = false;
  private buffer: WorkflowJournalEntry[] = [];
  private frontier?: Promise<void>;
  private readonly visited = new Set<string>();
  private readonly compensations: Compensation[] = [];
  private readonly inflight = new Set<Promise<void>>();
  private readonly consumed = new Set<number>();
  /** The last journal write issued; the next one waits for it (see `write()`). */
  private writes: Promise<void> = Promise.resolve();
  private readonly counters = { now: 0, random: 0, uuid: 0 };
  /** The first interrupt that stops this execution (not a suspension). */
  private stoppedBy: WorkflowInterrupt | null = null;
  /** The step whose function is running, in that function's async context. */
  private readonly insideStep = new AsyncLocalStorage<string>();

  constructor(
    readonly instance: ClaimedWorkflowInstance,
    journal: WorkflowJournalEntry[],
    /** Highest signal id visible to this execution's waits. */
    readonly signalCursor: number,
    private readonly deps: ExecutionDeps,
    replayOnly: boolean,
  ) {
    this.mode = replayOnly ? 'replay' : 'run';
    this.journal = new Map(journal.map((entry) => [entry.name, entry]));

    for (const entry of journal) {
      const signalId = (entry.result as { signalId?: unknown } | undefined)?.signalId;
      if (entry.kind === 'signal' && typeof signalId === 'number') {
        this.consumed.add(signalId);
      }
    }

    this.context = {
      workflowId: instance.id,
      workflowName: instance.workflow,
      version: instance.version,
      step: (name, fn, options) => this.track(this.step(name, fn, options)),
      sleep: (name, duration) => this.track(this.sleep(name, duration)),
      waitForSignal: (name, signal, options) => this.track(this.waitForSignal(name, signal, options)),
      now: () => this.helper('now', () => this.deps.clock.now()),
      random: () => this.helper('random', () => Math.random()),
      uuid: () => this.helper('uuid', () => randomUUID()),
      commit: (name) => this.commit(name),
      fail: (message) => {
        this.assertNotInStep('fail()', 'Throw a NonRetryableStepError from the step instead.');
        throw new WorkflowFailedError(message);
      },
    };
  }

  /** Runs the user's `run()` and waits for every step it started. */
  async run(fn: () => Promise<unknown>): Promise<RunOutcome> {
    let outcome: RunOutcome;
    try {
      outcome = { ok: true, value: await fn() };
    } catch (error) {
      outcome = { ok: false, error };
    }

    this.userSettled = true;
    while (this.inflight.size) {
      await Promise.all(this.inflight);
    }

    this.mode = 'closed';
    // A cancel, shutdown, lost lease or store failure reached run() as an
    // interrupt. Whether run() rethrew, wrapped or swallowed it, it decides the
    // outcome: a run that carried on anyway never completes the instance.
    return this.stoppedBy ? { ok: false, error: this.stoppedBy } : outcome;
  }

  /** Journal entries that this execution never reached (compensations excluded). */
  unvisited(): string[] {
    return [...this.journal.values()]
      .filter((entry) => entry.kind !== 'compensation' && !this.visited.has(entry.name))
      .map((entry) => entry.name);
  }

  /** Staged entries (sleeps, helpers) not yet written; the engine commits them. */
  drainBuffer(): WorkflowJournalEntry[] {
    const now = this.deps.clock.now();
    return this.buffer.splice(0).map((entry) => ({ ...entry, updatedAt: now }));
  }

  requestShutdown(): void {
    if (this.shuttingDown) {
      return;
    }
    this.shuttingDown = true;
    this.abort.abort(new WorkflowInterrupt('shutdown'));
  }

  /** After a shutdown timeout: never touch the store again. */
  detach(): void {
    this.detached = true;
    this.requestShutdown();
  }

  /** Extends the lease; called by the engine's heartbeat and by `WorkflowStepContext.heartbeat()`. */
  async renew(): Promise<void> {
    if (this.detached || this.leaseLost) {
      return;
    }

    let result: { cancelRequested: boolean } | null;
    try {
      result = await this.deps.store.renew(
        this.instance.id,
        this.instance.leaseToken,
        this.deps.clock.now() + this.deps.leaseMs,
      );
    } catch {
      return; // transient; the next heartbeat tries again before the lease runs out
    }

    if (!result) {
      this.loseLease();
    } else if (result.cancelRequested) {
      this.cancelRequested = true;
    }
  }

  /** Runs registered compensations in reverse order, each as a journaled, retried step. */
  async compensate(reason: SerializedWorkflowError): Promise<CompensationOutcome> {
    this.mode = 'compensate';
    this.suspension = null;

    let count = 0;
    for (const compensation of [...this.compensations].reverse()) {
      if (!compensation.completed) {
        continue;
      }

      const name = `${COMPENSATE}${compensation.step}`;
      const entry = this.journal.get(name);
      if (entry?.status === 'completed') {
        continue;
      }
      if (entry?.status === 'failed') {
        return { state: 'failed', error: entry.error! };
      }

      try {
        const attempt = await this.attempt(
          name,
          'compensation',
          // Whatever the undo returns (a provider's response, say) is discarded: nothing
          // reads it, so it can't fail the undo by being unserializable.
          async (ctx) => {
            await compensation.fn(compensation.result, { ...ctx, reason });
          },
          this.plan(name, { retry: compensation.retry }),
          entry,
        );
        count++;
        this.emit({ type: 'step-compensated', step: compensation.step, attempt: attempt.attempt });
      } catch (error) {
        if (error instanceof StepFailedError) {
          return { state: 'failed', error: { ...error.cause, message: `${error.message}` } };
        }

        // A compensation that calls ctx can never run: it needs a person.
        if (this.fatal) {
          return { state: 'failed', error: serializeError(this.fatal) };
        }

        if (isWorkflowInterrupt(error)) {
          return error.reason === 'suspend' ? { state: 'suspended', wakeAt: this.suspension!.wakeAt } : { state: 'interrupted' };
        }

        // Invalid options: retrying the same code would fail the same way.
        return { state: 'failed', error: serializeError(error) };
      }
    }

    return { state: 'done', count };
  }

  // ---------------------------------------------------------------------------
  // ctx operations

  private async step<T, P>(
    name: string,
    fn: (ctx: WorkflowStepContext<P>) => T | Promise<T>,
    options: WorkflowStepOptions<T> = {},
  ): Promise<Journaled<T>> {
    this.assertNotInStep(`step("${name}")`);
    this.visit(name, 'step');
    const entry = this.journal.get(name);
    if (entry?.status === 'completed') {
      this.reserveCompensation(name, options)?.arm(entry.result);
      return entry.result as Journaled<T>;
    }
    if (entry?.status === 'failed') {
      throw new StepFailedError(name, entry.attempts, entry.error!);
    }
    if (this.mode !== 'run') {
      throw this.interrupt('halt');
    }

    this.assertCanStart();
    const plan = this.plan(name, options);
    // Reserved in call order, so parallel steps compensate in the same order
    // on the first run and on a replay, whichever finished first.
    const compensation = this.reserveCompensation(name, options);

    await this.reachFrontier(name);
    this.assertCanStart();
    const { value } = await this.attempt<Journaled<T>>(name, 'step', fn as (ctx: WorkflowStepContext) => unknown, plan, entry);

    compensation?.arm(value);
    return value;
  }

  private async sleep(name: string, duration: Duration | { until: Date | number }): Promise<void> {
    this.assertNotInStep(`sleep("${name}")`);
    this.visit(name, 'sleep');
    const entry = this.journal.get(name);
    if (entry?.status === 'completed') {
      return;
    }
    if (this.mode !== 'run') {
      throw this.interrupt('halt');
    }

    this.assertAlive();
    const now = this.deps.clock.now();
    const wakeAt = entry?.wakeAt ?? wakeTime(name, duration, now);
    if (now >= wakeAt) {
      this.stage({ name, kind: 'sleep', status: 'completed', attempts: 0, wakeAt });
      return;
    }

    if (!entry) {
      this.stage({ name, kind: 'sleep', status: 'pending', attempts: 0, wakeAt });
    }
    throw this.suspendUntil(wakeAt);
  }

  private async waitForSignal<T>(
    name: string,
    signal: WorkflowSignal<T> | string,
    options: WaitForSignalOptions<T> = {},
  ): Promise<Journaled<T> | null> {
    this.assertNotInStep(`waitForSignal("${name}")`);
    this.visit(name, 'signal');
    const entry = this.journal.get(name);
    if (entry?.status === 'completed') {
      return (entry.result as { payload: Journaled<T> | null }).payload;
    }
    if (this.mode !== 'run') {
      throw this.interrupt('halt');
    }

    this.assertAlive();
    const wait: WorkflowWait = { signal: signalName(signal), key: options.key ?? null };
    const deadline = entry
      ? (entry.wakeAt ?? null)
      : options.timeout !== undefined
        ? this.deps.clock.now() + toMs(options.timeout)
        : null;
    if (!entry) {
      this.stage({ name, kind: 'signal', status: 'pending', attempts: 0, wakeAt: deadline, data: wait });
    }

    // Signals sent since the instance started, up to this execution's cursor.
    // A signal that arrived before the wait was reached still counts.
    const candidates = await this.deps.store.signals({
      name: wait.signal,
      key: wait.key,
      afterId: this.instance.signalCursor,
      upToId: this.signalCursor,
    });
    this.assertAlive();

    for (const candidate of candidates) {
      if (this.consumed.has(candidate.id)) {
        continue;
      }
      if (deadline !== null && candidate.createdAt > deadline) {
        continue;
      }
      if (options.match && !options.match(candidate.payload as Journaled<T>)) {
        continue;
      }

      this.consumed.add(candidate.id);
      await this.write([
        {
          name,
          kind: 'signal',
          status: 'completed',
          attempts: 0,
          wakeAt: deadline,
          data: wait,
          result: { signalId: candidate.id, payload: candidate.payload },
        },
      ]);
      this.emit({ type: 'signal-received', wait: name, signal: wait.signal, signalId: candidate.id });
      return candidate.payload as Journaled<T>;
    }

    if (deadline !== null && this.deps.clock.now() >= deadline) {
      await this.write([
        { name, kind: 'signal', status: 'completed', attempts: 0, wakeAt: deadline, data: wait, result: { signalId: null, payload: null } },
      ]);
      this.emit({ type: 'signal-timed-out', wait: name, signal: wait.signal });
      return null;
    }

    throw this.suspendUntil(deadline, wait);
  }

  private commit(name: string): void {
    this.assertNotInStep(`commit("${name}")`);
    this.visit(name, 'commit');
    const entry = this.journal.get(name);
    if (entry?.status !== 'completed') {
      // Not reached by the run being compensated: its compensations stand.
      if (this.mode !== 'run') {
        throw this.interrupt('halt');
      }

      // Not after a suspension either: code that swallowed the interrupt of a
      // wait must not pass the point of no return on the wait's behalf.
      this.assertCanStart();

      // Written with the next journal write, which always happens before the
      // next side effect, a suspension, or the switch to compensating.
      this.stage({ name, kind: 'commit', status: 'completed', attempts: 0 });
    }

    this.compensations.splice(0);
  }

  /**
   * Entries still pending when the instance ends without completing: a sleep
   * or wait that will never resolve, a retry that will never run.
   */
  abandoned(): WorkflowJournalEntry[] {
    return [...this.journal.values()]
      .filter((entry) => entry.status === 'pending' && entry.kind !== 'compensation')
      .map((entry) => ({ ...entry, status: 'cancelled', wakeAt: null }));
  }

  private helper<T>(kind: 'now' | 'random' | 'uuid', produce: () => T): T {
    this.assertNotInStep(`${kind}()`, 'Inside a step, read the real value directly: the step result is journaled.');
    const name = `$${kind}:${++this.counters[kind]}`;
    this.visit(name, kind);
    const entry = this.journal.get(name);
    if (entry?.status === 'completed') {
      return entry.result as T;
    }
    if (this.mode !== 'run') {
      throw this.interrupt('halt');
    }

    this.assertCanStart();
    const value = produce();
    // Written with the next journal write, which always happens before the
    // next side effect: a value can never influence an effect without being
    // persisted first.
    this.stage({ name, kind, status: 'completed', attempts: 0, result: value });
    return value;
  }

  // ---------------------------------------------------------------------------
  // attempts

  /**
   * Resolves a step's (or compensation's) retry and timeouts. Throws a
   * `TypeError` for invalid options before an attempt is recorded, instead of
   * spending the step's attempts on it.
   */
  private plan(name: string, options: { retry?: RetrySetting; timeout?: Duration; heartbeatTimeout?: Duration }): AttemptPlan {
    try {
      return {
        policy: resolveRetry(options.retry, this.deps.defaultRetry),
        timeoutMs: options.timeout === undefined ? undefined : toMs(options.timeout),
        heartbeatTimeoutMs: options.heartbeatTimeout === undefined ? undefined : toMs(options.heartbeatTimeout),
      };
    } catch (error) {
      throw new TypeError(`Invalid options for "${name}": ${(error as Error).message}`);
    }
  }

  private async attempt<T>(
    name: string,
    kind: 'step' | 'compensation',
    fn: (ctx: WorkflowStepContext) => unknown,
    plan: AttemptPlan,
    entry: WorkflowJournalEntry | undefined,
  ): Promise<{ value: T; attempt: number }> {
    const { policy } = plan;
    const previous = entry?.status === 'pending' ? entry.attempts : 0;
    if (entry?.status === 'pending' && previous > 0) {
      if (entry.wakeAt != null && this.deps.clock.now() < entry.wakeAt) {
        throw this.suspendUntil(entry.wakeAt);
      }
      if (entry.wakeAt == null && previous >= policy.attempts) {
        throw await this.giveUp(entry, previous, {
          name: 'StepInterruptedError',
          message:
            `Attempt ${previous} did not finish: the worker stopped, lost its lease, or could not write the ` +
            'result to the store (the worker logged it).',
        });
      }
    }

    const attempt = previous + 1;
    // Recorded before the side effect, so a step that crashes the process
    // still uses up its attempts instead of crash-looping forever.
    let current: WorkflowJournalEntry = {
      name,
      kind,
      status: 'pending',
      attempts: attempt,
      wakeAt: null,
      progress: entry?.progress,
      error: entry?.error,
    };
    await this.write([current]);

    const startedAt = performance.now();
    let value: T;
    try {
      const raw = await this.invoke(name, fn, plan, attempt, current, (next) => (current = next));
      try {
        value = normalize(raw) as T;
      } catch (error) {
        throw new NonRetryableStepError(`Result of "${name}" is not JSON-serializable: ${(error as Error).message}`);
      }
    } catch (error) {
      if (this.abort.signal.aborted) {
        // Shutdown, lost lease or store failure: not the step's fault, and
        // nothing about this attempt is recorded. A shutdown gives the attempt back.
        if (this.shuttingDown && !this.leaseLost && !this.storeError) {
          this.rollbacks.push({ ...current, attempts: attempt - 1 });
        }
        throw this.abortInterrupt();
      }

      // A definition error raised inside the step (a nested ctx call) fails
      // the instance; retrying the same code would hit it again.
      if (this.fatal) {
        throw this.fatal;
      }
      if (isWorkflowInterrupt(error)) {
        throw error;
      }

      let serialized = serializeError(error);
      let retryAt: number | null = null;
      if (!(error instanceof NonRetryableStepError) && attempt < policy.attempts) {
        try {
          if (policy.retryIf?.(error, attempt) ?? true) {
            retryAt = this.deps.clock.now() + retryDelay(policy, attempt, error);
          }
        } catch (callbackError) {
          // A throwing retryIf or backoff gives up, journaled like any failure,
          // so a replay sees the same StepFailedError instead of re-running the step.
          const thrown = serializeError(callbackError);
          serialized = {
            ...thrown,
            message: `The retry options of "${name}" threw ${thrown.name}: ${thrown.message} (handling ${serialized.name}: ${serialized.message})`,
          };
        }
      }

      if (retryAt === null) {
        throw await this.giveUp(current, attempt, serialized);
      }

      await this.write([{ ...current, error: serialized, wakeAt: retryAt }]);
      this.emit({ type: 'step-failed', step: name, attempt, error: serialized, retryAt });
      throw this.suspendUntil(retryAt);
    }

    await this.write([{ ...current, status: 'completed', result: value, error: undefined, progress: undefined }]);
    if (kind === 'step') {
      this.emit({ type: 'step-completed', step: name, attempt, durationMs: Math.round(performance.now() - startedAt) });
    }
    return { value, attempt };
  }

  private invoke(
    name: string,
    fn: (ctx: WorkflowStepContext) => unknown,
    { timeoutMs: overallMs, heartbeatTimeoutMs: idleMs }: AttemptPlan,
    attempt: number,
    entry: WorkflowJournalEntry,
    update: (entry: WorkflowJournalEntry) => void,
  ): Promise<unknown> {
    const controller = new AbortController();
    const forward = () => controller.abort(this.abort.signal.reason);
    if (this.abort.signal.aborted) {
      forward();
    } else {
      this.abort.signal.addEventListener('abort', forward, { once: true });
    }

    let expire!: (error: StepTimeoutError) => void;
    const watchdog = new Promise<never>((_, reject) => {
      expire = (error) => {
        controller.abort(error);
        reject(error);
      };
    });

    const overall =
      overallMs === undefined
        ? undefined
        : setTimeout(() => expire(new StepTimeoutError(`Step "${name}" timed out after ${overallMs}ms.`)), overallMs).unref();
    let idle: NodeJS.Timeout | undefined;
    const armIdle = () => {
      if (idleMs === undefined) {
        return;
      }
      clearTimeout(idle);
      idle = setTimeout(() => expire(new StepTimeoutError(`Step "${name}" sent no heartbeat for ${idleMs}ms.`)), idleMs).unref();
    };
    armIdle();

    let current = entry;
    let settled = false;
    const ctx: WorkflowStepContext = {
      idempotencyKey: `${this.instance.id}:${name}`,
      attempt,
      signal: controller.signal,
      progress: entry.progress,
      heartbeat: async (progress?: unknown) => {
        // After the attempt ended (a timeout, or a heartbeat left running),
        // nothing may touch its journal entry any more.
        if (settled) {
          controller.signal.throwIfAborted();
          return;
        }

        armIdle();
        if (progress === undefined) {
          await this.renew();
          if (this.leaseLost) {
            throw this.interrupt('lease-lost');
          }
          return;
        }

        current = { ...current, progress: normalize(progress) };
        update(current);
        await this.write([current]);
      },
    };

    const run = Promise.resolve().then(() => runInStepScope(ctx.idempotencyKey, () => this.insideStep.run(name, () => fn(ctx))));
    return Promise.race([run, watchdog]).finally(() => {
      settled = true;
      clearTimeout(overall);
      clearTimeout(idle);
      this.abort.signal.removeEventListener('abort', forward);
    });
  }

  private async giveUp(
    entry: WorkflowJournalEntry,
    attempts: number,
    error: SerializedWorkflowError,
  ): Promise<StepFailedError> {
    await this.write([{ ...entry, status: 'failed', attempts, error, wakeAt: null, progress: undefined }]);
    if (entry.kind === 'step') {
      this.emit({ type: 'step-failed', step: entry.name, attempt: attempts, error, retryAt: null });
    }
    return new StepFailedError(entry.name, attempts, error);
  }

  // ---------------------------------------------------------------------------
  // bookkeeping

  private track<T>(promise: Promise<T>): Promise<T> {
    const settled = promise.then(
      () => undefined,
      () => undefined,
    );
    this.inflight.add(settled);
    void settled.then(() => this.inflight.delete(settled));
    return promise;
  }

  /**
   * A step's function runs once; its result is all a replay sees. A `ctx` call
   * made inside it would be journaled on the first run and missing on every
   * replay, so it is a definition error, reported where it happens.
   */
  private assertNotInStep(call: string, advice = 'Call ctx methods from run() and pass the values the step needs into it.'): void {
    const step = this.insideStep.getStore();
    if (step === undefined) {
      return;
    }

    const where = step.startsWith(COMPENSATE) ? `the compensation of step "${step.slice(COMPENSATE.length)}"` : `step "${step}"`;
    throw this.setFatal(
      new WorkflowDefinitionError(
        `${this.describe()}: ${where} called ctx.${call}. A step's function runs once and replays return its ` +
          `result, so it can't use ctx. ${advice}`,
      ),
    );
  }

  private visit(name: string, kind: JournalKind): void {
    if (this.fatal) {
      throw this.fatal;
    }

    const helper = kind === 'now' || kind === 'random' || kind === 'uuid';
    if (typeof name !== 'string' || name.length === 0 || (!helper && name.startsWith('$'))) {
      throw this.setFatal(new WorkflowDefinitionError(`Invalid ${kind} name "${name}". Names cannot be empty or start with "$".`));
    }

    if (this.visited.has(name)) {
      throw this.setFatal(
        new WorkflowDefinitionError(
          `"${name}" is used twice in one run of workflow "${this.instance.workflow}@${this.instance.version}". ` +
            'Step, sleep and wait names must be unique per run; inside a loop, add the index (`remind-${i}`).',
        ),
      );
    }
    this.visited.add(name);

    const entry = this.journal.get(name);
    if (entry && entry.kind !== kind) {
      throw this.setFatal(
        new WorkflowNonDeterminismError(
          `${this.describe()} does not match its journal: "${name}" was recorded as a ${entry.kind}, but the code now calls it as a ${kind}. ${ADVICE}`,
        ),
      );
    }
  }

  /**
   * Before the first new step runs, let every replayable call happen (replayed
   * results resolve in microtasks, so one macrotask is enough), then require
   * that the whole journal was reached.
   */
  private reachFrontier(step: string): Promise<void> {
    this.frontier ??= (async () => {
      await nextMacrotask();
      const missing = this.unvisited();
      if (missing.length) {
        throw this.setFatal(
          new WorkflowNonDeterminismError(
            `${this.describe()} does not match its journal: ${missing.map((n) => `"${n}"`).join(', ')} ` +
              `${missing.length === 1 ? 'was' : 'were'} recorded by an earlier run but not reached before the new step "${step}". ${ADVICE}`,
          ),
        );
      }
    })();
    return this.frontier;
  }

  /** Reserves the step's compensation in call order; `arm()` it with the result once the step completed. */
  private reserveCompensation<T>(step: string, options: WorkflowStepOptions<T>): { arm(result: unknown): void } | undefined {
    if (!options.compensate) {
      return undefined;
    }

    const compensation: Compensation = {
      step,
      completed: false,
      fn: options.compensate as Compensation['fn'],
      retry: options.compensateRetry ?? options.retry,
    };
    this.compensations.push(compensation);

    return {
      arm: (result) => {
        compensation.result = result;
        compensation.completed = true;
      },
    };
  }

  private assertAlive(): void {
    if (this.fatal) {
      throw this.fatal;
    }
    if (this.leaseLost) {
      throw this.interrupt('lease-lost');
    }
    if (this.storeError) {
      throw this.interrupt('store-error');
    }
    if (this.shuttingDown) {
      throw this.interrupt('shutdown');
    }
    if (this.cancelRequested) {
      throw this.interrupt('cancel');
    }
    // Only new work stops: a replay to compensate never gets here.
    if (this.pastDeadline()) {
      throw this.interrupt('timeout');
    }
  }

  /** Whether the instance's run timeout has passed. */
  pastDeadline(): boolean {
    return this.instance.deadline !== null && this.deps.clock.now() >= this.instance.deadline;
  }

  /** No new step starts once the execution is suspending or `run()` has settled. */
  private assertCanStart(): void {
    this.assertAlive();
    if (this.suspension) {
      throw this.interrupt('suspend');
    }
    if (this.userSettled) {
      throw this.interrupt('halt');
    }
  }

  private suspendUntil(wakeAt: number | null, wait?: WorkflowWait): WorkflowInterrupt {
    this.suspension ??= { wakeAt: null, waits: [] };
    if (wakeAt !== null) {
      this.suspension.wakeAt = this.suspension.wakeAt === null ? wakeAt : Math.min(this.suspension.wakeAt, wakeAt);
    }
    if (wait) {
      this.suspension.waits.push(wait);
    }

    return this.interrupt('suspend');
  }

  private stage(entry: WorkflowJournalEntry): void {
    this.journal.set(entry.name, entry);
    this.buffer.push(entry);
  }

  /** Fenced write of staged entries plus `entries`. */
  private async write(entries: WorkflowJournalEntry[]): Promise<void> {
    if (this.detached) {
      throw this.interrupt('shutdown');
    }

    for (const entry of entries) {
      this.journal.set(entry.name, entry);
    }

    const now = this.deps.clock.now();
    const batch = [...this.buffer.splice(0), ...entries].map((entry) => ({ ...entry, updatedAt: now }));

    // Writes reach the store one at a time, in the order they were issued. On a store with
    // a connection pool, two writes in flight can commit in either order, and the later one
    // wins: a checkpoint the step didn't await, landing after the step's completion, would
    // turn the completed step back into a pending one.
    const previous = this.writes;
    let done!: () => void;
    this.writes = new Promise<void>((resolve) => (done = resolve));
    try {
      await previous;
      if (this.detached) {
        throw this.interrupt('shutdown');
      }

      let ok: boolean;
      try {
        ok = await this.deps.store.write(this.instance.id, this.instance.leaseToken, { now, entries: uniqueEntries(batch) });
      } catch (error) {
        this.storeError = error;
        this.abort.abort(new WorkflowInterrupt('store-error'));
        throw this.interrupt('store-error');
      }

      if (!ok) {
        this.loseLease();
        throw this.interrupt('lease-lost');
      }
    } finally {
      done();
    }
  }

  private loseLease(): void {
    if (this.leaseLost) {
      return;
    }
    this.leaseLost = true;
    this.abort.abort(new WorkflowInterrupt('lease-lost'));
  }

  private abortInterrupt(): WorkflowInterrupt {
    return this.interrupt(this.leaseLost ? 'lease-lost' : this.storeError ? 'store-error' : 'shutdown');
  }

  /** Every interrupt `ctx` throws goes through here; the first one that stops the run is kept. */
  private interrupt(reason: InterruptReason): WorkflowInterrupt {
    const interrupt = new WorkflowInterrupt(reason);
    if (reason !== 'suspend' && reason !== 'halt') {
      this.stoppedBy ??= interrupt;
    }
    return interrupt;
  }

  private setFatal(error: Error): Error {
    this.fatal ??= error;
    return this.fatal;
  }

  private describe(): string {
    return `Instance "${this.instance.id}" of workflow "${this.instance.workflow}@${this.instance.version}"`;
  }

  private emit(body: EventBody): void {
    this.deps.events.emit({
      id: this.instance.id,
      workflow: this.instance.workflow,
      version: this.instance.version,
      at: this.deps.clock.now(),
      ...body,
    } as WorkflowEvent);
  }
}

function wakeTime(name: string, when: Duration | { until: Date | number }, now: number): number {
  if (typeof when !== 'object') {
    return now + toMs(when);
  }

  const until = when.until instanceof Date ? when.until.getTime() : when.until;
  if (typeof until !== 'number' || !Number.isFinite(until)) {
    throw new TypeError(`Invalid deadline for sleep "${name}": ${String(when.until)}. Pass a valid Date or a timestamp in milliseconds.`);
  }
  return until;
}

const COMPENSATE = '$compensate:';

const ADVICE =
  'A deployed change renamed, removed or reordered steps. Ship such changes as a new version ' +
  '(@Workflow(name, { version })) and keep the old class registered until its instances finish.';
