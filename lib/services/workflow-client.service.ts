import { randomUUID } from 'node:crypto';
import { Inject, Injectable, type Type } from '@nestjs/common';
import { systemClock } from '../utils/clock.util.js';
import type { WorkflowClock } from '../interfaces/workflow-clock.interface.js';
import { WorkflowIdConflictError } from '../errors/workflow-id-conflict.error.js';
import { WorkflowNotFoundError } from '../errors/workflow-not-found.error.js';
import type { WorkflowInstance, WorkflowJournalEntry } from '../interfaces/workflow-instance.interface.js';
import type { WorkflowInput } from '../interfaces/workflow-runner.interface.js';
import { normalize } from './workflow-execution.service.js';
import { signalName, type WorkflowSignal } from '../signals/workflow.signal.js';
import { WorkflowStorage } from '../storage/workflow.storage.js';
import type { WorkflowInstanceDetails, WorkflowStore } from '../interfaces/workflow-store.interface.js';
import { WorkflowRegistry } from './workflow-registry.service.js';
import { WorkflowWorker } from './workflow-worker.service.js';
import { WORKFLOWS_MODULE_OPTIONS } from '../workflows.module-definition.js';
import type { WorkflowsModuleOptions } from '../interfaces/workflows-module-options.interface.js';
import type {
  StartWorkflowOptions,
  SignalWorkflowOptions,
  WorkflowStartResult,
  WorkflowCancelResult,
  WorkflowListFilter,
} from '../interfaces/workflow-client.interface.js';

/** Starts, signals, inspects and cancels workflow instances. Works with or without a local worker. */
@Injectable()
export class WorkflowClient {
  private readonly clock: WorkflowClock;

  constructor(
    private readonly storage: WorkflowStorage,
    @Inject(WORKFLOWS_MODULE_OPTIONS) options: WorkflowsModuleOptions,
    private readonly registry: WorkflowRegistry,
    private readonly worker: WorkflowWorker,
  ) {
    this.clock = options.clock ?? systemClock;
  }

  /** Read at each call, never in the constructor: sources register while providers are created. */
  private get store(): WorkflowStore {
    return this.storage.source;
  }

  /**
   * Creates an instance, or returns the existing one with the same id and
   * input (`created: false`). Throws `WorkflowIdConflictError` for the same id
   * with a different workflow or input. With `transaction`, the instance is
   * created in your transaction and commits or rolls back with it.
   */
  async start<W>(workflow: Type<W> | string, input: WorkflowInput<W>, options: StartWorkflowOptions = {}): Promise<WorkflowStartResult> {
    const { name, version } = this.registry.resolve(workflow as Type<unknown> | string, options.version);
    const id = options.id ?? randomUUID();
    if (typeof id !== 'string' || id.length === 0) {
      throw new TypeError(`Invalid workflow instance id ${JSON.stringify(id)}. Use a non-empty string, such as \`order-\${orderId}\`.`);
    }

    const normalized = normalize(input);
    const data = { id, workflow: name, version, input: normalized, now: this.clock.now() };
    // Nothing is awaited before the store's call: on a driver whose transactions are
    // synchronous, its statements must run before the application's transaction callback returns.
    const { instance, created } = await (options.transaction === undefined
      ? this.store.create(data)
      : this.storeMethod('createInTransaction', 'start')(options.transaction, data));

    if (!created) {
      if (instance.workflow !== name) {
        throw new WorkflowIdConflictError(`Instance "${id}" already exists for workflow "${instance.workflow}", not "${name}".`);
      }
      // A store may read an `undefined` input back as `null`.
      if (canonical(instance.input ?? null) !== canonical(normalized ?? null)) {
        throw new WorkflowIdConflictError(`Instance "${id}" of "${name}" already exists with a different input.`);
      }
    } else {
      this.worker.kick();
    }

    return { id, workflow: name, version: instance.version, created, status: instance.status };
  }

  /** The instance, with the signals it waits for and, on request, its journal. `null` for an unknown id. */
  getStatus(
    id: string,
    options: { journal: true },
  ): Promise<(WorkflowInstanceDetails & { journal: WorkflowJournalEntry[] }) | null>;
  getStatus(id: string, options?: { journal?: boolean }): Promise<WorkflowInstanceDetails | null>;
  async getStatus(id: string, options: { journal?: boolean } = {}): Promise<WorkflowInstanceDetails | null> {
    const details = await this.store.get(id, { journal: options.journal === true });
    if (!details) {
      return null;
    }
    if (!options.journal) {
      delete details.journal;
    }
    return details;
  }

  /** Instances by status, workflow name and version, oldest first. At most `limit` (default 100). */
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

    return this.store.list({
      limit,
      offset,
      ...(status ? { status } : {}),
      ...(filter.workflow !== undefined ? { workflow: filter.workflow } : {}),
      ...(filter.version !== undefined ? { version: filter.version } : {}),
    });
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
  async cancel(id: string, reason?: string): Promise<WorkflowCancelResult> {
    const accepted = await this.store.requestCancel(id, reason ?? null, this.clock.now());
    const details = await this.store.get(id);
    if (!details) {
      throw new WorkflowNotFoundError(`No workflow instance with id "${id}".`);
    }

    if (accepted) {
      this.worker.noticeCancel(id);
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
   * signal and the wake-ups commit or roll back with your transaction.
   */
  async signal<T>(
    signal: WorkflowSignal<T> | string,
    payload: NoInfer<T>,
    options: SignalWorkflowOptions = {},
  ): Promise<{ signalId: number; woken: number }> {
    const data = { name: signalName(signal), key: options.key ?? null, payload: normalize(payload), now: this.clock.now() };
    const result = await (options.transaction === undefined
      ? this.store.signal(data)
      : this.storeMethod('signalInTransaction', 'signal')(options.transaction, data));

    if (result.woken > 0) {
      this.worker.kick();
    }

    return { signalId: result.id, woken: result.woken };
  }

  /** The store's method for joining the application's transaction, bound; throws if it has none. */
  private storeMethod<M extends 'createInTransaction' | 'signalInTransaction'>(method: M, caller: string): NonNullable<WorkflowStore[M]> {
    const store = this.store;
    const fn = store[method];
    if (typeof fn !== 'function') {
      throw new TypeError(
        `${caller}() with { transaction } needs a WorkflowStore on your database that implements ${method}(); ` +
          `${store.constructor.name} has none. See "Implementing a store" in the README.`,
      );
    }

    return fn.bind(store) as NonNullable<WorkflowStore[M]>;
  }
}

/** Internal: JSON with sorted object keys, to compare inputs and payloads. */
export function canonical(value: unknown): string {
  return JSON.stringify(value, (_key, v) =>
    v && typeof v === 'object' && !Array.isArray(v)
      ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : v,
  ) ?? 'undefined';
}
