import { randomUUID } from 'node:crypto';
import { Inject, Injectable, type Type } from '@nestjs/common';
import { systemClock } from '../utils/clock.util.js';
import { runTimeoutMs, toMs } from '../utils/duration.util.js';
import type { WorkflowClock } from '../interfaces/workflow-clock.interface.js';
import { WorkflowIdConflictError } from '../errors/workflow-id-conflict.error.js';
import { WorkflowNotFoundError } from '../errors/workflow-not-found.error.js';
import type { WorkflowInstance, WorkflowJournalEntry, WorkflowStatus } from '../interfaces/workflow-instance.interface.js';
import type { WorkflowInput, WorkflowOutput } from '../interfaces/workflow-runner.interface.js';
import type { Journaled } from '../interfaces/workflow-context.interface.js';
import { WorkflowFailedError, type WorkflowFailureStatus } from '../errors/workflow-failed.error.js';
import { WorkflowResultTimeoutError } from '../errors/workflow-result-timeout.error.js';
import { normalize } from '../utils/normalize.util.js';
import { assertSameInstance, newInstance } from '../utils/new-instance.util.js';
import { signalName, type WorkflowSignal } from '../signals/workflow.signal.js';
import type { EncodedWorkflowStore } from '../storage/encoded-workflow.store.js';
import { ENGINE_STORE, WorkflowStorage } from '../storage/workflow.storage.js';
import { stepSignalId, stepStartId } from '../utils/step-scope.util.js';
import type { WorkflowInstanceDetails, WorkflowPurgeResult, WorkflowStore } from '../interfaces/workflow-store.interface.js';
import { WorkflowRegistry } from './workflow-registry.service.js';
import { WorkflowSchedules } from './workflow-schedules.service.js';
import { WorkflowWorker } from './workflow-worker.service.js';
import { WORKFLOWS_MODULE_OPTIONS } from '../workflows.module-definition.js';
import { CHILD_ENDED_SIGNAL } from '../workflows.constants.js';
import type { WorkflowsModuleOptions } from '../interfaces/workflows-module-options.interface.js';
import type {
  StartWorkflowOptions,
  SignalWorkflowOptions,
  WorkflowStartResult,
  WorkflowCancelResult,
  WorkflowListFilter,
  WorkflowDeleteOptions,
  WorkflowPurgeOptions,
  WorkflowResultOptions,
  WorkflowRetryInstanceOptions,
  WorkflowSignalSendResult,
} from '../interfaces/workflow-client.interface.js';
import { WorkflowStateError } from '../errors/workflow-state.error.js';
import { WorkflowEvents } from '../events/workflow-events.service.js';
import type { WorkflowEvent } from '../events/workflow-events.interface.js';
import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';

/** Starts, signals, inspects and cancels workflow instances. Works with or without a local worker. */
@Injectable()
export class WorkflowClient {
  private readonly clock: WorkflowClock;

  constructor(
    private readonly storage: WorkflowStorage,
    @Inject(WORKFLOWS_MODULE_OPTIONS) options: WorkflowsModuleOptions,
    private readonly registry: WorkflowRegistry,
    private readonly worker: WorkflowWorker,
    private readonly events: WorkflowEvents,
    /** The schedules that start instances: `upsert()`, `get()`, `list()`, `pause()`, `resume()`, `trigger()`... */
    readonly schedules: WorkflowSchedules,
  ) {
    this.clock = options.clock ?? systemClock;
  }

  /** Read at each call, never in the constructor: sources register while providers are created. */
  private get store(): EncodedWorkflowStore {
    return this.storage[ENGINE_STORE];
  }

  /**
   * Creates an instance, or returns the existing one with the same id and
   * input (`created: false`). Throws `WorkflowIdConflictError` for the same id
   * with a different workflow or input. With `transaction`, the instance is
   * created in your transaction and commits or rolls back with it. Inside a
   * workflow step, a start without an `id` gets one derived from the step, so a
   * retried step gets its instance back instead of starting another.
   */
  async start<W>(workflow: Type<W> | string, input: WorkflowInput<W>, options: StartWorkflowOptions = {}): Promise<WorkflowStartResult> {
    const resolved = this.registry.resolve(workflow as Type<unknown> | string, options.version);
    const derived = options.id === undefined ? stepStartId(resolved.name) : undefined;
    const id = options.id ?? derived ?? randomUUID();
    const data = newInstance(resolved, id, input, {
      caller: 'start()',
      now: this.clock.now(),
      timeout: options.timeout,
      concurrencyKey: options.concurrencyKey,
      rateLimitKey: options.rateLimitKey,
      priority: options.priority,
    });
    // Nothing is awaited before the store's call: on a driver whose transactions are
    // synchronous, its statements must run before the application's transaction callback returns.
    const { instance, created } = await (options.transaction === undefined
      ? this.store.create(data)
      : this.storeMethod('createInTransaction', 'start')(options.transaction, data));

    if (!created) {
      // A retried step may build its input anew (a timestamp in it, say): with an id derived
      // from the step, the first input wins.
      assertSameInstance(instance, data, { input: derived === undefined, parent: false });
    } else {
      this.worker.kick();
    }

    return { id: data.id, workflow: data.workflow, version: instance.version, created, status: instance.status };
  }

  /**
   * The instance, with the signals it waits for and, on request, its journal and its children
   * (every instance it started with `ctx.startChild()`, oldest first; `list({ parentId })` pages
   * through them). Its own parent is `parentId`. `null` for an unknown id.
   */
  getStatus(
    id: string,
    options: { journal: true; children?: boolean },
  ): Promise<(WorkflowInstanceDetails & { journal: WorkflowJournalEntry[] }) | null>;
  getStatus(id: string, options?: { journal?: boolean; children?: boolean }): Promise<WorkflowInstanceDetails | null>;
  async getStatus(id: string, options: { journal?: boolean; children?: boolean } = {}): Promise<WorkflowInstanceDetails | null> {
    const details = await this.store.get(id, { journal: options.journal === true });
    if (!details) {
      return null;
    }
    if (!options.journal) {
      delete details.journal;
    }

    if (options.children) {
      details.children = [];
      for (let offset = 0; ; offset += 500) {
        const page = await this.store.inner.list({ parentId: id, limit: 500, offset });
        details.children.push(...(await Promise.all(page.map((child) => this.store.readableInstance(child)))));
        if (page.length < 500) {
          break;
        }
      }
    }
    return details;
  }

  /**
   * Waits for the instance to end and resolves with its output, or rejects with a
   * `WorkflowFailedError` (its `status` is `failed`, `cancelled` or `compensation_failed`, its
   * `cause` the instance's error). Rejects with `WorkflowResultTimeoutError` past `timeout` (the
   * instance keeps running), and `WorkflowNotFoundError` for an unknown id or one deleted while
   * waiting. An instance run by this process's worker is seen the moment it ends; one run
   * elsewhere, at the next read of the store (every 25ms at first, backing off to every second).
   */
  async result<O = unknown>(id: string, options: WorkflowResultOptions = {}): Promise<Journaled<O>> {
    const timeoutMs = options.timeout === undefined ? Infinity : toMs(options.timeout);
    const deadline = performance.now() + timeoutMs;
    options.signal?.throwIfAborted();

    let ended = false;
    let closed = false;
    let wake: (() => void) | undefined;
    const nudge = () => {
      ended = true;
      wake?.();
    };
    const subscription = this.events.events$.subscribe({
      next: (event) => {
        if (event.id === id && ENDED_EVENTS.has(event.type)) {
          nudge();
        }
      },
      complete: () => {
        closed = true;
        nudge();
      },
    });
    options.signal?.addEventListener('abort', nudge);

    try {
      for (let delay = 25; ; delay = Math.min(delay * 2, 1_000)) {
        ended = false;
        const instance = await this.store.get(id);
        if (!instance) {
          throw new WorkflowNotFoundError(`No workflow instance with id "${id}".`);
        }
        if (instance.status === 'completed') {
          return instance.output as Journaled<O>;
        }
        if (FINISHED.includes(instance.status)) {
          throw failure(instance);
        }

        options.signal?.throwIfAborted();
        if (closed) {
          throw new Error(`The application shut down while waiting for the result of instance "${id}".`);
        }
        const remaining = deadline - performance.now();
        if (remaining <= 0) {
          throw new WorkflowResultTimeoutError(id, timeoutMs);
        }
        if (!ended) {
          await new Promise<void>((resolve) => {
            const timer = setTimeout(resolve, Math.min(delay, remaining));
            wake = () => {
              clearTimeout(timer);
              resolve();
            };
          });
          wake = undefined;
        }
      }
    } finally {
      subscription.unsubscribe();
      options.signal?.removeEventListener('abort', nudge);
    }
  }

  /**
   * `start()`, then `result()`: resolves with the output of the instance (the existing one, for
   * an id that already has one), or rejects as `result()` does. `wait` is `result()`'s options;
   * `options.timeout` stays the instance's run timeout. Not with `{ transaction }`: the instance
   * wouldn't exist before your transaction commits.
   */
  async startAndWait<W>(
    workflow: Type<W> | string,
    input: WorkflowInput<W>,
    options: StartWorkflowOptions = {},
    wait: WorkflowResultOptions = {},
  ): Promise<Journaled<WorkflowOutput<W>>> {
    if (options.transaction !== undefined) {
      throw new TypeError(
        "startAndWait() can't take { transaction }: the instance only exists once your transaction commits. Call start() in it, and result() after the commit.",
      );
    }

    const { id } = await this.start(workflow, input, options);
    return this.result<WorkflowOutput<W>>(id, wait);
  }

  /** Instances by status, workflow name, version, parent and schedule, oldest first. At most `limit` (default 100). */
  async list(filter: WorkflowListFilter = {}): Promise<WorkflowInstance[]> {
    const limit = filter.limit ?? 100;
    const offset = filter.offset ?? 0;
    if (!Number.isSafeInteger(limit) || limit < 0 || !Number.isSafeInteger(offset) || offset < 0) {
      throw new TypeError(`list(): limit (${limit}) and offset (${offset}) must be non-negative integers.`);
    }

    const status = filter.status === undefined ? undefined : Array.isArray(filter.status) ? filter.status : [filter.status];
    if (status?.length === 0 || limit === 0) {
      return [];
    }

    // Read as stored, then decoded one by one: one no codec can read any more is listed without its payloads.
    const instances = await this.store.inner.list({
      limit,
      offset,
      ...(status ? { status } : {}),
      ...(filter.workflow !== undefined ? { workflow: filter.workflow } : {}),
      ...(filter.version !== undefined ? { version: filter.version } : {}),
      ...(filter.parentId !== undefined ? { parentId: filter.parentId } : {}),
      ...(filter.scheduleId !== undefined ? { scheduleId: filter.scheduleId } : {}),
    });
    return Promise.all(instances.map((instance) => this.store.readableInstance(instance)));
  }

  /**
   * Requests cancellation. A parked instance wakes at once. A running one lets
   * its current step finish and stops at its next `ctx` call: at once when it
   * runs in this process, otherwise once its worker's heartbeat reads the flag
   * (within a third of `leaseDuration`). Then it runs its compensations and
   * ends as `cancelled`. Compensations of steps before a `ctx.commit()` never
   * run. An instance that completes first stays completed, and one that is
   * already compensating is not accepted. Throws `WorkflowNotFoundError` for an
   * unknown id.
   */
  cancel(id: string, reason?: string): Promise<WorkflowCancelResult> {
    return this.stop(id, reason, false);
  }

  /**
   * Stops an instance without running its compensations, for one that can't or mustn't finish
   * on its own: a parked one wakes at once, a running one stops at its next `ctx` call (its
   * current step finishes first), and a compensating one after its current compensation. It
   * ends as `cancelled` with a `WorkflowTerminatedError` whose message is `reason`. Accepted
   * after a `cancel()` too. Throws `WorkflowNotFoundError` for an unknown id.
   */
  terminate(id: string, reason?: string): Promise<WorkflowCancelResult> {
    return this.stop(id, reason, true);
  }

  private async stop(id: string, reason: string | undefined, terminate: boolean): Promise<WorkflowCancelResult> {
    const accepted = await this.store.requestCancel(id, { reason: reason ?? null, now: this.clock.now(), terminate });
    // Accepted is accepted: an instance whose payloads no codec can read any more is returned as stored.
    const details = await this.store.get(id).catch(() => this.store.inner.get(id));
    if (!details) {
      throw new WorkflowNotFoundError(`No workflow instance with id "${id}".`);
    }

    if (accepted) {
      this.worker.noticeCancel(id, terminate);
    }

    const { waits: _waits, journal: _journal, ...instance } = details;
    return { ...instance, accepted };
  }

  /**
   * Sends a signal to the instances waiting for it with the same `key`, and
   * wakes the suspended ones. Signals are durable: an instance that reaches
   * `ctx.waitForSignal()` later still gets a signal sent after it started, and
   * each signal is consumed at most once per instance. `woken` counts only the
   * instances parked on a matching wait right now. With `transaction`, the
   * signal and the wake-ups commit or roll back with your transaction. With
   * `id`, or from inside a workflow step, a repeated signal is stored once.
   */
  async signal<T>(
    signal: WorkflowSignal<T> | string,
    payload: NoInfer<T>,
    options: SignalWorkflowOptions = {},
  ): Promise<WorkflowSignalSendResult> {
    const name = signalName(signal);
    const key = options.key ?? null;
    if (options.id !== undefined && (typeof options.id !== 'string' || options.id.length === 0)) {
      throw new TypeError(`Invalid signal id ${JSON.stringify(options.id)}. Use a non-empty string, such as the id of the event that causes it.`);
    }

    const dedupeId = options.id ?? stepSignalId(name, key) ?? null;
    const data = { name, key, dedupeId, payload: normalize(payload), now: this.clock.now() };
    // Nothing is awaited before the store's call, as in start().
    const result = await (options.transaction === undefined
      ? this.store.signal(data)
      : this.storeMethod('signalInTransaction', 'signal')(options.transaction, data));

    if (!result.created && result.key !== key) {
      throw new WorkflowIdConflictError(
        `Signal id "${dedupeId}" of "${name}" was already used with ${describeKey(result.key)}, not ${describeKey(key)}.`,
      );
    }
    if (result.woken > 0) {
      this.worker.kick();
    }

    return { signalId: result.id, woken: result.woken, created: result.created };
  }

  /**
   * Retries an instance that needs a person, once its cause is fixed:
   *
   * - `failed`: runs again from its journal. Completed steps return their results, and each
   *   step that gave up gets its attempts back. Refused when its compensations ran: the
   *   completed steps were undone, and resuming would build on undone work.
   * - `compensation_failed`: runs the compensations that didn't complete again, each with its
   *   attempts back, and ends as `failed` or `cancelled` as it would have.
   *
   * Journaled under `$retry:<n>`, and emitted as `workflow-retried`. Throws
   * `WorkflowNotFoundError` for an unknown id, and `WorkflowStateError` for any other status or
   * when another change to the instance races it.
   */
  async retry(id: string, options: WorkflowRetryInstanceOptions = {}): Promise<WorkflowInstance> {
    const details = await this.store.get(id, { journal: true });
    if (!details) {
      throw new WorkflowNotFoundError(`No workflow instance with id "${id}".`);
    }

    const { status, journal = [] } = details;
    const now = this.clock.now();
    const reset = (entry: WorkflowJournalEntry): WorkflowJournalEntry => ({ ...entry, status: 'pending', attempts: 0, wakeAt: null, updatedAt: now });
    let entries: WorkflowJournalEntry[];
    let error: SerializedWorkflowError | null;
    if (status === 'failed') {
      const undone = journal.filter((entry) => entry.kind === 'compensation' && entry.status === 'completed').map((entry) => `"${entry.name}"`);
      if (undone.length > 0) {
        throw new WorkflowStateError(
          `Instance "${id}" failed and its compensations ran (${undone.join(', ')}): its completed steps were undone, so ` +
            'resuming it would build on undone work. Start a new instance instead.',
        );
      }
      entries = journal.filter((entry) => entry.kind === 'step' && entry.status === 'failed').map(reset);
      error = null;
    } else if (status === 'compensation_failed') {
      entries = journal.filter((entry) => entry.kind === 'compensation' && entry.status !== 'completed').map(reset);
      const { compensation: _compensation, ...reason } = details.error ?? { name: 'Error', message: 'Unknown failure.' };
      error = reason;
    } else {
      throw new WorkflowStateError(
        `Instance "${id}" is ${status}: only failed and compensation_failed instances can be retried.` +
          (FINISHED.includes(status) ? '' : ' cancel() stops one that is still running.'),
      );
    }

    let deadline: number | null | undefined;
    if (options.timeout !== undefined) {
      deadline = options.timeout === false ? null : now + runTimeoutMs(options.timeout, 'retry()');
    } else if (status === 'failed' && details.deadline !== null && details.deadline <= now) {
      throw new WorkflowStateError(
        `Instance "${id}" is past its run timeout, so it would time out again at once. Pass { timeout } with a new one, or false for none.`,
      );
    }

    const retries = journal.filter((entry) => entry.kind === 'retry').length;
    entries.push({ name: `$retry:${retries + 1}`, kind: 'retry', status: 'completed', attempts: 0, data: { from: status, error: details.error ?? null }, updatedAt: now });
    const accepted = await this.store.reopen(id, {
      expect: { status, runs: details.runs },
      status: status === 'failed' ? 'pending' : 'compensating',
      error,
      ...(deadline !== undefined ? { deadline } : {}),
      entries,
      now,
    });
    if (!accepted) {
      throw new WorkflowStateError(`Instance "${id}" changed while it was being retried. Read it again, and retry if it still needs it.`);
    }

    this.emit(details, { type: 'workflow-retried', from: status, error: details.error ?? null });
    this.worker.kick();
    const { waits: _waits, journal: _journal, ...instance } = (await this.store.get(id))!;
    return instance;
  }

  /**
   * Deletes an instance with its journal, and emits `workflow-deleted`. Only a finished one,
   * unless `force`: then an unfinished one too, without compensating (to remove an instance no
   * worker can run any more, such as one of a version you no longer deploy). A parent waiting
   * for a child deleted that way gets a `ChildWorkflowFailedError` (`cancelled`); a deleted
   * parent's children keep running. Throws
   * `WorkflowNotFoundError` for an unknown id, and `WorkflowStateError` for an unfinished one
   * without `force`.
   */
  async delete(id: string, options: WorkflowDeleteOptions = {}): Promise<void> {
    // Read as stored: deleting needs no payload, so an instance no codec can read any more can go too.
    const details = await this.store.inner.get(id);
    if (!details) {
      throw new WorkflowNotFoundError(`No workflow instance with id "${id}".`);
    }

    const deleted = await this.store.delete(id, options.force ? ALL : FINISHED);
    if (!deleted) {
      const current = await this.store.inner.get(id);
      if (!current) {
        throw new WorkflowNotFoundError(`No workflow instance with id "${id}".`);
      }
      throw new WorkflowStateError(
        `Instance "${id}" is ${current.status}: delete() removes finished instances. cancel() it first, or pass ` +
          '{ force: true } to delete it without running its compensations.',
      );
    }

    this.emit(details, { type: 'workflow-deleted', status: details.status });

    // A parent waiting for this child would wait forever: it ends for the parent as cancelled.
    if (details.parentId !== null && !FINISHED.includes(details.status)) {
      const ended = { status: 'cancelled', error: { name: 'WorkflowDeletedError', message: `Child instance "${id}" was deleted before it ended.` } };
      await this.store.signal({ name: CHILD_ENDED_SIGNAL, key: id, dedupeId: id, payload: ended, now: this.clock.now() });
      this.worker.kick();
    }
  }

  /**
   * Deletes finished instances older than `olderThan`, with their journals, the signals no
   * unfinished instance can take any more, and the rate-limit windows that ended before it, in
   * batches until none is left. Returns how many of each it deleted. Run it from a scheduled job;
   * concurrent runs are safe, only wasteful.
   */
  async purge(options: WorkflowPurgeOptions): Promise<WorkflowPurgeResult> {
    const olderThan = toMs(options.olderThan);
    const statuses = options.status === undefined ? DEFAULT_PURGE : Array.isArray(options.status) ? options.status : [options.status];
    const unfinished = statuses.filter((status) => !FINISHED.includes(status));
    if (statuses.length === 0 || unfinished.length > 0) {
      throw new TypeError(
        `purge(): status must list finished statuses (${FINISHED.join(', ')}), not ${unfinished.length ? unfinished.join(', ') : 'none'}. ` +
          'Cancel an unfinished instance first.',
      );
    }
    const limit = options.batchSize ?? 500;
    if (!Number.isSafeInteger(limit) || limit < 1) {
      throw new TypeError(`purge(): batchSize (${limit}) must be a positive integer.`);
    }

    const before = this.clock.now() - olderThan;
    const total = { instances: 0, signals: 0, rateLimits: 0 };
    for (;;) {
      const batch = await this.store.purge({ statuses, before, limit });
      total.instances += batch.instances;
      total.signals += batch.signals;
      total.rateLimits += batch.rateLimits;
      if (batch.instances < limit && batch.signals < limit && batch.rateLimits < limit) {
        return total;
      }
    }
  }

  private emit(instance: WorkflowInstance, body: { type: 'workflow-retried' | 'workflow-deleted' } & Record<string, unknown>): void {
    this.events.emit({ id: instance.id, workflow: instance.workflow, version: instance.version, at: this.clock.now(), ...body } as WorkflowEvent);
  }

  /** The store's method for joining the application's transaction, bound; throws if it has none. */
  private storeMethod<M extends 'createInTransaction' | 'signalInTransaction'>(method: M, caller: string): NonNullable<WorkflowStore[M]> {
    const store = this.store;
    const fn = store[method];
    if (typeof fn !== 'function') {
      throw new TypeError(
        `${caller}() with { transaction } needs a WorkflowStore on your database that implements ${method}(); ` +
          `${this.storage.source.constructor.name} has none. See https://docs.nestjs.com/reliability/workflows#the-store-contract.`,
      );
    }

    return fn.bind(store) as NonNullable<WorkflowStore[M]>;
  }
}

const FINISHED: WorkflowStatus[] = ['completed', 'failed', 'cancelled', 'compensation_failed'];
const ENDED_EVENTS = new Set<WorkflowEvent['type']>(['workflow-completed', 'workflow-failed', 'workflow-cancelled', 'workflow-compensation-failed', 'workflow-deleted']);

/** What `result()` rejects with for an instance that ended without completing. */
function failure(instance: WorkflowInstance): WorkflowFailedError {
  const status = instance.status as WorkflowFailureStatus;
  const cause = instance.error ?? { name: 'Error', message: 'Unknown failure.' };
  return new WorkflowFailedError(`Instance "${instance.id}" of workflow "${instance.workflow}" ${status.replace('_', ' ')}: ${cause.name}: ${cause.message}`, {
    instanceId: instance.id,
    status,
    cause,
  });
}
const DEFAULT_PURGE: WorkflowStatus[] = ['completed', 'failed', 'cancelled'];
const ALL: WorkflowStatus[] = ['pending', 'running', 'suspended', 'compensating', ...FINISHED];

function describeKey(key: string | null): string {
  return key === null ? 'no key' : `key "${key}"`;
}
