import { Logger } from '@nestjs/common';
import type {
  WorkflowInstance,
  WorkflowJournalEntry,
  WorkflowWait,
} from '../interfaces/workflow-instance.interface.js';
import type {
  NewWorkflowInstance,
  NewWorkflowSignal,
  WorkflowClaim,
  WorkflowClaimRequest,
  WorkflowInstanceDetails,
  WorkflowListQuery,
  WorkflowSignalQuery,
  WorkflowSignalRecord,
  WorkflowSignalResult,
  WorkflowStore,
  WorkflowWrite,
} from '../interfaces/workflow-store.interface.js';

interface Row {
  instance: WorkflowInstance;
  leaseToken: string | null;
  /** By name, in first-write order (a `Map` keeps insertion order, and `set()` on a key keeps its place). */
  journal: Map<string, WorkflowJournalEntry>;
  waits: WorkflowWait[];
}

const RUNNABLE = new Set(['pending', 'running', 'suspended', 'compensating']);
const CANCELLABLE = new Set(['pending', 'running', 'suspended']);

/**
 * The default store, used when no source is registered: everything lives in this process's
 * memory, so it is lost on restart and not shared with other instances of the application.
 * Also the store for tests: `overrideProvider(YourStore).useValue(new InMemoryWorkflowStore())`
 * (a plain instance doesn't register, so the default applies), or register one instance with
 * `WorkflowStorage.registerSource(store, { replace: true })` in several applications of one
 * test to share its "database" between them, as a restart would.
 *
 * Every method does its work synchronously before its first `await`, so calls never
 * interleave: claims, fenced writes and signals are atomic without locks. Values are copied
 * on the way in and out, as a database would.
 *
 * It can't join your database's transactions: `createInTransaction()` and
 * `signalInTransaction()` (`start()` and `signal()` with `{ transaction }`) write at once, so
 * a rollback doesn't undo them, and the first call logs a warning (once per store).
 */
export class InMemoryWorkflowStore implements WorkflowStore {
  private static readonly logger = new Logger('WorkflowsModule');
  private readonly rows = new Map<string, Row>();
  private readonly signalLog: Array<WorkflowSignalRecord & { dedupeId: string | null }> = [];
  private warnedAboutTransactions = false;

  /** `create()`, at once: it can't join `transaction` (see the class). */
  async createInTransaction(transaction: unknown, i: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }> {
    this.cannotJoin('createInTransaction');
    return this.create(i);
  }

  /** `signal()`, at once: it can't join `transaction` (see the class). */
  async signalInTransaction(transaction: unknown, s: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    this.cannotJoin('signalInTransaction');
    return this.signal(s);
  }

  private cannotJoin(method: string) {
    if (this.warnedAboutTransactions) {
      return;
    }

    this.warnedAboutTransactions = true;
    InMemoryWorkflowStore.logger.warn(
      `InMemoryWorkflowStore.${method}() received your transaction handle, and can't join it: the write applies at ` +
        "once, so a rolled-back transaction won't undo it. Register a store for your database with " +
        'WorkflowStorage.registerSource(). (Logged once.)',
    );
  }

  async create(i: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }> {
    const existing = this.rows.get(i.id);
    if (existing) {
      return { instance: copy(existing.instance), created: false };
    }

    const instance: WorkflowInstance = {
      id: i.id,
      workflow: i.workflow,
      version: i.version,
      status: 'pending',
      input: copy(i.input),
      output: undefined,
      error: null,
      wakeAt: i.now,
      leaseOwner: null,
      leaseUntil: null,
      cancelRequested: false,
      cancelReason: null,
      deadline: i.deadline,
      signalCursor: this.lastSignalId(),
      runs: 0,
      createdAt: i.now,
      updatedAt: i.now,
    };

    this.rows.set(i.id, { instance, leaseToken: null, journal: new Map(), waits: [] });
    return { instance: copy(instance), created: true };
  }

  async get(id: string, options: { journal?: boolean } = {}): Promise<WorkflowInstanceDetails | null> {
    const row = this.rows.get(id);
    if (!row) {
      return null;
    }

    return {
      ...copy(row.instance),
      waits: copy(row.waits),
      ...(options.journal ? { journal: copy([...row.journal.values()]) } : {}),
    };
  }

  async list(query: WorkflowListQuery): Promise<WorkflowInstance[]> {
    return [...this.rows.values()]
      .map((row) => row.instance)
      .filter(
        (i) =>
          (query.status === undefined || query.status.includes(i.status)) &&
          (query.workflow === undefined || i.workflow === query.workflow) &&
          (query.version === undefined || i.version === query.version),
      )
      .sort((a, b) => a.createdAt - b.createdAt || compare(a.id, b.id))
      .slice(query.offset, query.offset + query.limit)
      .map(copy);
  }

  async requestCancel(id: string, reason: string | null, now: number): Promise<boolean> {
    const instance = this.rows.get(id)?.instance;
    if (!instance || !CANCELLABLE.has(instance.status) || instance.cancelRequested) {
      return false;
    }

    instance.cancelRequested = true;
    instance.cancelReason = reason;
    instance.updatedAt = now;
    if (instance.wakeAt === null || instance.wakeAt > now) {
      instance.wakeAt = now;
    }

    return true;
  }

  async signal(s: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    const earlier = s.dedupeId === null ? undefined : this.signalLog.find((r) => r.name === s.name && r.dedupeId === s.dedupeId);
    if (earlier) {
      return { id: earlier.id, woken: 0, created: false, key: earlier.key };
    }

    const id = this.lastSignalId() + 1;
    this.signalLog.push({ id, name: s.name, key: s.key, dedupeId: s.dedupeId, payload: copy(s.payload), createdAt: s.now });

    let woken = 0;
    for (const { instance, waits } of this.rows.values()) {
      if (instance.status !== 'suspended' || (instance.wakeAt !== null && instance.wakeAt <= s.now)) {
        continue;
      }
      if (!waits.some((wait) => wait.signal === s.name && wait.key === s.key)) {
        continue;
      }

      instance.wakeAt = s.now;
      instance.updatedAt = s.now;
      woken++;
    }

    return { id, woken, created: true, key: s.key };
  }

  async signals(query: WorkflowSignalQuery): Promise<WorkflowSignalRecord[]> {
    return this.signalLog
      .filter((s) => s.name === query.name && s.key === query.key && s.id > query.afterId && s.id <= query.upToId)
      .map(({ dedupeId: _dedupeId, ...record }) => copy(record));
  }

  async claim(request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    const runs = new Set(request.workflows.map((w) => `${w.version}:${w.name}`));
    const due = [...this.rows.values()]
      .filter(
        ({ instance: i }) =>
          RUNNABLE.has(i.status) &&
          i.wakeAt !== null &&
          i.wakeAt <= request.now &&
          (i.leaseUntil === null || i.leaseUntil < request.now) &&
          runs.has(`${i.version}:${i.workflow}`),
      )
      .sort((a, b) => a.instance.wakeAt! - b.instance.wakeAt! || a.instance.createdAt - b.instance.createdAt || compare(a.instance.id, b.instance.id))
      .slice(0, request.limit);

    for (const row of due) {
      const i = row.instance;
      row.leaseToken = request.token;
      i.leaseOwner = request.owner;
      i.leaseUntil = request.leaseUntil;
      i.runs++;
      i.updatedAt = request.now;
      if (i.status !== 'compensating') {
        i.status = 'running';
      }
    }

    return { instances: due.map((row) => copy(row.instance)), lastSignalId: this.lastSignalId() };
  }

  async renew(id: string, token: string, leaseUntil: number): Promise<{ cancelRequested: boolean } | null> {
    const row = this.leased(id, token);
    if (!row) {
      return null;
    }
    row.instance.leaseUntil = leaseUntil;
    return { cancelRequested: row.instance.cancelRequested };
  }

  async write(id: string, token: string, w: WorkflowWrite): Promise<boolean> {
    const row = this.leased(id, token);
    if (!row) {
      return false;
    }

    for (const entry of w.entries) {
      row.journal.set(entry.name, copy(entry));
    }

    const i = row.instance;
    if (w.status !== undefined) {
      i.status = w.status;
    }
    if (w.output !== undefined) {
      i.output = copy(w.output);
    }
    if (w.error !== undefined) {
      i.error = copy(w.error);
    }

    if (w.release) {
      const { waits, signalCursor } = w.release;
      row.waits = copy(waits);
      row.leaseToken = null;
      i.leaseUntil = null;

      const missed = this.signalLog.some((s) => s.id > signalCursor && waits.some((wait) => wait.signal === s.name && wait.key === s.key));
      i.wakeAt = missed || (i.cancelRequested && w.status === 'suspended') ? w.now : w.release.wakeAt;
    }

    if (w.status !== undefined || w.output !== undefined || w.error !== undefined || w.release) {
      i.updatedAt = w.now;
    }

    return true;
  }

  private leased(id: string, token: string): Row | undefined {
    const row = this.rows.get(id);
    return row && row.leaseToken !== null && row.leaseToken === token ? row : undefined;
  }

  private lastSignalId(): number {
    return this.signalLog.at(-1)?.id ?? 0;
  }
}

function copy<T>(value: T): T {
  return value === undefined ? value : structuredClone(value);
}

function compare(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}
