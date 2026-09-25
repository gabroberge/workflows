import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
  type OnModuleDestroy,
} from '@nestjs/common';
import { systemClock } from '../utils/clock.util.js';
import type { WorkflowClock } from '../interfaces/workflow-clock.interface.js';
import { toMs } from '../utils/duration.util.js';
import { WorkflowNonDeterminismError } from '../errors/workflow-non-determinism.error.js';
import { isWorkflowInterrupt } from '../errors/workflow-interrupt.error.js';
import { serializeError } from '../utils/serialize-error.util.js';
import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';
import { DEFAULT_RETRY, resolveRetry } from '../utils/retry.util.js';
import {
  normalize,
  uniqueEntries,
  WorkflowExecution,
  type ClaimedWorkflowInstance,
  type ExecutionDeps,
  type RunOutcome,
} from './workflow-execution.service.js';
import { WorkflowEvents } from '../events/workflow-events.service.js';
import type { WorkflowEvent } from '../events/workflow-events.interface.js';
import { WorkflowStorage } from '../storage/workflow.storage.js';
import type {
  WorkflowRelease,
  WorkflowStore,
  WorkflowWrite,
} from '../interfaces/workflow-store.interface.js';
import { WorkflowRegistry } from './workflow-registry.service.js';
import { WORKFLOWS_MODULE_OPTIONS } from '../workflows.module-definition.js';
import type { WorkflowsModuleOptions } from '../interfaces/workflows-module-options.interface.js';

interface Running {
  exec?: WorkflowExecution;
  done?: Promise<void>;
  stopHeartbeat: () => void;
}

type EventBody = WorkflowEvent extends infer E
  ? E extends WorkflowEvent
    ? Omit<E, 'id' | 'workflow' | 'version' | 'at'>
    : never
  : never;

/**
 * Claims due instances under a lease, executes them and records the outcome.
 * Any number of processes can run a worker against the same store. Inject it
 * for `drain()`, which runs due instances on demand (tests, scripts).
 */
@Injectable()
export class WorkflowWorker implements OnApplicationBootstrap, OnModuleDestroy, OnApplicationShutdown {
  /** Shown as `leaseOwner` on the instances this worker runs. */
  readonly id: string;
  private readonly logger = new Logger('Workflows');
  private readonly clock: WorkflowClock;
  private readonly deps: ExecutionDeps;
  private readonly enabled: boolean;
  private readonly concurrency: number;
  private readonly pollInterval: number;
  private readonly heartbeatInterval: number;
  private readonly shutdownTimeout: number;
  private readonly running = new Set<Running>();
  /** Claims in flight: shutdown waits for them, so nothing they claim starts unobserved. */
  private readonly claiming = new Set<Promise<unknown>>();
  private stopped = false;
  private stop!: () => void;
  private readonly stoppedPromise = new Promise<void>((resolve) => (this.stop = resolve));
  private wake?: () => void;
  private kicked = false;
  private loop?: Promise<void>;

  constructor(
    private readonly storage: WorkflowStorage,
    @Inject(WORKFLOWS_MODULE_OPTIONS) options: WorkflowsModuleOptions,
    private readonly registry: WorkflowRegistry,
    private readonly events: WorkflowEvents,
  ) {
    const worker = options.worker === false ? { enabled: false } : (options.worker ?? {});
    const leaseMs = toMs(worker.leaseDuration ?? '30s');

    this.clock = options.clock ?? systemClock;
    this.enabled = worker.enabled ?? true;
    this.id = worker.id ?? `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
    this.concurrency = worker.concurrency ?? 10;
    this.pollInterval = toMs(worker.pollInterval ?? '1s');
    this.heartbeatInterval = toMs(worker.heartbeatInterval ?? Math.floor(leaseMs / 3));
    this.shutdownTimeout = toMs(worker.shutdownTimeout ?? '10s');

    if (!Number.isInteger(this.concurrency) || this.concurrency < 1) {
      throw new TypeError(`worker.concurrency (${this.concurrency}) must be a positive integer.`);
    }
    if (this.pollInterval <= 0 || this.heartbeatInterval <= 0) {
      throw new TypeError('worker.pollInterval and worker.heartbeatInterval must be positive, such as "1s".');
    }
    if (this.heartbeatInterval >= leaseMs) {
      throw new TypeError(`worker.heartbeatInterval (${this.heartbeatInterval}ms) must be shorter than worker.leaseDuration (${leaseMs}ms).`);
    }

    this.deps = {
      get store() {
        return storage.source;
      },
      clock: this.clock,
      events,
      leaseMs,
      defaultRetry: resolveRetry(options.retry, DEFAULT_RETRY),
    };
  }

  onApplicationBootstrap() {
    this.registry.keys(); // validate definitions at startup
    if (this.enabled) {
      this.loop = this.poll();
    }
  }

  /** Read at each use, never in the constructor: sources register while providers are created. */
  private get store(): WorkflowStore {
    return this.storage.source;
  }

  async onModuleDestroy() {
    await this.shutdown();
  }

  /** Stops the worker if `onModuleDestroy` didn't run (an application closed before it initialized). */
  async onApplicationShutdown() {
    await this.shutdown();
  }

  /** @internal Look for work now instead of at the next poll (after a local start or signal). */
  kick(): void {
    this.kicked = true;
    this.wake?.();
  }

  /**
   * @internal After an accepted `WorkflowClient.cancel()` in this process: an
   * execution of the instance running here stops at its next `ctx` call now,
   * instead of when its next heartbeat reads the flag.
   */
  noticeCancel(id: string): void {
    for (const running of this.running) {
      if (running.exec?.instance.id === id) {
        running.exec.cancelRequested = true;
      }
    }
    this.kick();
  }

  /**
   * Executes every due instance, repeatedly, until none is left. Returns the
   * number of executions. For tests (with a `ManualWorkflowClock` and `worker: false`),
   * scripts and cron-driven workers.
   */
  async drain(options: { maxRounds?: number } = {}): Promise<number> {
    let total = 0;
    for (let round = 0; round < (options.maxRounds ?? 1_000) && !this.stopped; round++) {
      const executions = await this.claim(this.concurrency);
      if (executions.length === 0) {
        break;
      }

      total += executions.length;
      await Promise.race([Promise.all(executions), this.stoppedPromise]);
    }

    return total;
  }

  /**
   * @internal Called on application shutdown. Stops claiming, asks running
   * executions to stop (their steps see `signal.aborted`), and waits up to
   * `shutdownTimeout`. Executions that stopped are handed back at once; the
   * rest keep their lease until it expires, because their step may still be running.
   */
  async shutdown(): Promise<void> {
    if (this.stopped) {
      return;
    }

    this.stopped = true;
    this.stop();
    this.wake?.();

    // Instances claimed by a claim that was in flight start now (and are told to
    // stop at once), so they are handed back before the store closes.
    await Promise.allSettled(this.claiming);
    for (const running of this.running) {
      running.exec?.requestShutdown();
    }

    let timer: NodeJS.Timeout | undefined;
    await Promise.race([
      Promise.allSettled([...this.running].map((running) => running.done)),
      new Promise((resolve) => (timer = setTimeout(resolve, this.shutdownTimeout))),
    ]);
    clearTimeout(timer);

    for (const running of this.running) {
      running.stopHeartbeat();
      running.exec?.detach();
    }

    await this.loop;
  }

  private async poll(): Promise<void> {
    while (!this.stopped) {
      const free = this.concurrency - this.running.size;
      if (free > 0) {
        try {
          const executions = await this.claim(free);
          for (const execution of executions) {
            void execution.finally(() => this.kick());
          }
        } catch (error) {
          this.logger.error('Claiming workflow instances failed.', error as Error);
        }
      }

      await this.idle();
    }
  }

  private idle(): Promise<void> {
    if (this.kicked || this.stopped) {
      this.kicked = false;
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer);
        this.wake = undefined;
        resolve();
      };
      const timer = setTimeout(done, this.pollInterval);
      this.wake = () => {
        this.kicked = false;
        done();
      };
    });
  }

  /** Claims due instances and starts executing them (before any caller sees them). */
  private async claim(limit: number): Promise<Promise<void>[]> {
    const workflows = this.registry.versions();
    if (workflows.length === 0 || limit <= 0) {
      return [];
    }

    const now = this.clock.now();
    const token = randomUUID();
    const claim = this.store.claim({ owner: this.id, token, now, leaseUntil: now + this.deps.leaseMs, limit, workflows });
    this.claiming.add(claim);

    try {
      const { instances, lastSignalId } = await claim;
      return instances.map((instance) => this.start({ ...instance, leaseToken: token }, lastSignalId));
    } finally {
      this.claiming.delete(claim);
    }
  }

  private start(instance: ClaimedWorkflowInstance, signalCursor: number): Promise<void> {
    const running: Running = { stopHeartbeat: () => undefined };
    this.running.add(running);
    const done = this.execute(instance, signalCursor, running).finally(() => this.running.delete(running));
    running.done = done;
    return done;
  }

  private async execute(instance: ClaimedWorkflowInstance, signalCursor: number, running: Running): Promise<void> {
    const definition = this.registry.get(instance.workflow, instance.version)!;
    const heartbeat = setInterval(() => void running.exec?.renew(), this.heartbeatInterval);
    heartbeat.unref();
    running.stopHeartbeat = () => clearInterval(heartbeat);

    try {
      const details = await this.store.get(instance.id, { journal: true });
      if (!details) {
        throw new Error(`The store has no instance "${instance.id}", which it just leased.`);
      }

      const replayOnly = instance.status === 'compensating' || instance.cancelRequested;
      const exec = new WorkflowExecution(instance, details.journal ?? [], signalCursor, this.deps, replayOnly);
      running.exec = exec;
      if (this.stopped) {
        exec.requestShutdown();
      }

      this.emit(instance, instance.runs === 1 ? { type: 'workflow-started' } : { type: 'workflow-resumed', run: instance.runs });
      const outcome = await exec.run(async () => definition.instance.run(exec.context, instance.input));
      await this.conclude(exec, instance, outcome);
    } catch (error) {
      this.logger.error(
        `Executing "${instance.id}" (${instance.workflow}@${instance.version}) failed; it is retried when its lease expires.`,
        error as Error,
      );
    } finally {
      clearInterval(heartbeat);
    }
  }

  private async conclude(exec: WorkflowExecution, instance: ClaimedWorkflowInstance, outcome: RunOutcome): Promise<void> {
    if (await this.interrupted(exec, instance, outcome.ok)) {
      return;
    }

    if (exec.fatal) {
      return this.finish(exec, instance, 'failed', { error: serializeError(exec.fatal) });
    }

    if (instance.status === 'compensating') {
      return this.compensate(exec, instance, instance.error ?? { name: 'Error', message: 'Unknown failure.' }, true);
    }

    if (instance.cancelRequested) {
      return this.compensate(exec, instance, cancelled(instance.cancelReason), false);
    }

    // A cancel noticed during the run (a heartbeat, or cancel() in this process) stopped it.
    // Checked before a suspension: a parked sleep, wait or retry backoff left next to it
    // will never resume, so the instance compensates now instead of parking first and being
    // woken again for that. The same goes for a run timeout.
    if (!outcome.ok && isWorkflowInterrupt(outcome.error) && outcome.error.reason === 'cancel') {
      const current = await this.store.get(instance.id);
      return this.compensate(exec, instance, cancelled(current?.cancelReason ?? null), false);
    }
    if (!outcome.ok && isWorkflowInterrupt(outcome.error) && outcome.error.reason === 'timeout') {
      return this.compensate(exec, instance, timedOut(instance), false);
    }

    if (exec.suspension) {
      const { waits } = exec.suspension;
      // Parked no later than the run timeout, so a sleep or wait past it can't outlive it.
      const wakeAt = instance.deadline === null ? exec.suspension.wakeAt : Math.min(exec.suspension.wakeAt ?? instance.deadline, instance.deadline);
      const ok = await this.write(instance, {
        entries: exec.drainBuffer(),
        status: 'suspended',
        release: this.release(exec, wakeAt, waits),
      });
      if (ok) {
        this.emit(instance, { type: 'workflow-suspended', wakeAt, waits });
      } else {
        this.leaseLostWarning(instance);
      }
      return;
    }

    if (outcome.ok) {
      const missing = exec.unvisited();
      if (missing.length) {
        const error = new WorkflowNonDeterminismError(
          `Instance "${instance.id}" of workflow "${instance.workflow}@${instance.version}" does not match its journal: ` +
            `${missing.map((n) => `"${n}"`).join(', ')} ${missing.length === 1 ? 'was' : 'were'} recorded by an earlier run but never reached. ` +
            'Ship step removals as a new workflow version.',
        );
        return this.finish(exec, instance, 'failed', { error: serializeError(error) });
      }

      let output: unknown;
      try {
        output = normalize(outcome.value);
      } catch (error) {
        return this.compensate(exec, instance, serializeError(error), false);
      }

      return this.finish(exec, instance, 'completed', { output });
    }

    return this.compensate(exec, instance, serializeError(outcome.error), false);
  }

  private async compensate(
    exec: WorkflowExecution,
    instance: ClaimedWorkflowInstance,
    reason: SerializedWorkflowError,
    alreadyCompensating: boolean,
  ): Promise<void> {
    if (!alreadyCompensating) {
      const ok = await this.write(instance, { entries: exec.drainBuffer(), status: 'compensating', error: reason });
      if (!ok) {
        return this.leaseLostWarning(instance);
      }
      this.emit(instance, { type: 'workflow-compensating', error: reason });
    }

    const result = await exec.compensate(reason);
    switch (result.state) {
      case 'done':
        return this.finish(exec, instance, reason.name === 'WorkflowCancelledError' ? 'cancelled' : 'failed', {
          error: reason,
        });
      case 'failed':
        return this.finish(exec, instance, 'compensation_failed', { error: { ...reason, compensation: result.error } });
      case 'suspended': {
        const ok = await this.write(instance, {
          entries: exec.drainBuffer(),
          status: 'compensating',
          release: this.release(exec, result.wakeAt, []),
        });
        if (ok) {
          this.emit(instance, { type: 'workflow-suspended', wakeAt: result.wakeAt, waits: [] });
        }
        return;
      }
      case 'interrupted':
        await this.interrupted(exec, instance, false);
    }
  }

  /**
   * Handles executions that must not record an outcome. Returns true if
   * handled. A run that finished cleanly during shutdown is still recorded;
   * one that was cut short is handed back, and whatever it was doing is
   * re-derived by the next execution.
   */
  private async interrupted(exec: WorkflowExecution, instance: ClaimedWorkflowInstance, finished: boolean): Promise<boolean> {
    if (exec.leaseLost) {
      this.leaseLostWarning(instance);
      return true;
    }

    if (exec.storeError) {
      this.logger.error(
        `Store failed while executing "${instance.id}"; it is retried when its lease expires.`,
        exec.storeError as Error,
      );
      return true;
    }

    if (exec.detached) {
      return true;
    }

    if (exec.shuttingDown && !finished) {
      // Handed back: due at once, for another worker (or this one, after a restart).
      await this.write(instance, {
        entries: [...exec.drainBuffer(), ...exec.rollbacks],
        release: this.release(exec, this.clock.now(), []),
      });
      return true;
    }

    return false;
  }

  private async finish(
    exec: WorkflowExecution,
    instance: ClaimedWorkflowInstance,
    status: 'completed' | 'failed' | 'cancelled' | 'compensation_failed',
    result: { output?: unknown; error?: SerializedWorkflowError },
  ): Promise<void> {
    const entries = exec.drainBuffer();
    if (status !== 'completed') {
      entries.push(...exec.abandoned().map((entry) => ({ ...entry, updatedAt: this.clock.now() })));
    }

    const ok = await this.write(instance, {
      entries,
      status,
      output: result.output,
      error: result.error ?? null,
      release: this.release(exec, null, []),
    });
    if (!ok) {
      return this.leaseLostWarning(instance);
    }

    if (status === 'completed') {
      this.emit(instance, { type: 'workflow-completed', output: result.output });
    } else if (status === 'cancelled') {
      this.emit(instance, { type: 'workflow-cancelled', error: result.error! });
    } else if (status === 'failed') {
      this.emit(instance, { type: 'workflow-failed', error: result.error! });
    } else {
      this.emit(instance, { type: 'workflow-compensation-failed', error: result.error! });
    }
  }

  /** A fenced write under the execution's lease, stamped with the clock. */
  private write(instance: ClaimedWorkflowInstance, write: Omit<WorkflowWrite, 'now'>): Promise<boolean> {
    return this.store.write(instance.id, instance.leaseToken, { ...write, entries: uniqueEntries(write.entries), now: this.clock.now() });
  }

  private release(exec: WorkflowExecution, wakeAt: number | null, waits: WorkflowRelease['waits']): WorkflowRelease {
    return { wakeAt, waits, signalCursor: exec.signalCursor };
  }

  private leaseLostWarning(instance: ClaimedWorkflowInstance): void {
    this.logger.warn(
      `Lost the lease on "${instance.id}" (${instance.workflow}); another worker took over. Nothing from this execution after that point was recorded.`,
    );
  }

  private emit(instance: ClaimedWorkflowInstance, body: EventBody): void {
    this.events.emit({
      id: instance.id,
      workflow: instance.workflow,
      version: instance.version,
      at: this.clock.now(),
      ...body,
    } as WorkflowEvent);
  }
}

function cancelled(reason: string | null): SerializedWorkflowError {
  return { name: 'WorkflowCancelledError', message: reason ?? 'Cancelled.' };
}

function timedOut(instance: ClaimedWorkflowInstance): SerializedWorkflowError {
  return {
    name: 'WorkflowTimeoutError',
    message:
      `Instance "${instance.id}" of workflow "${instance.workflow}@${instance.version}" did not finish within its timeout ` +
      `(its deadline was ${new Date(instance.deadline!).toISOString()}).`,
  };
}
