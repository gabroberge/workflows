import { Logger } from '@nestjs/common';
import type {
  WorkflowInstance,
  WorkflowJournalEntry,
  WorkflowStatus,
  WorkflowWait,
} from '../interfaces/workflow-instance.interface.js';
import type {
  NewWorkflowInstance,
  NewWorkflowSignal,
  WorkflowCancelRequest,
  WorkflowClaim,
  WorkflowClaimRequest,
  WorkflowInstanceDetails,
  WorkflowListQuery,
  WorkflowPurgeQuery,
  WorkflowPurgeResult,
  WorkflowReopen,
  WorkflowScheduleClaimRequest,
  WorkflowScheduleQuery,
  WorkflowScheduleRecord,
  WorkflowScheduleSave,
  WorkflowScheduleWrite,
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
const FINISHED = new Set(['completed', 'failed', 'cancelled', 'compensation_failed']);
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
  /** Rate-limit windows by `JSON.stringify([workflow])` or `JSON.stringify([workflow, key])`. */
  private readonly windows = new Map<string, { windowEnd: number; count: number }>();
  private readonly schedules = new Map<string, { record: WorkflowScheduleRecord; leaseToken: string | null }>();
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
      parentId: i.parentId ?? null,
      parentClose: i.parentClose ?? null,
      concurrencyKey: i.concurrencyKey ?? null,
      rateLimitKey: i.rateLimitKey ?? null,
      priority: i.priority ?? 0,
      scheduleId: i.scheduleId ?? null,
      scheduledAt: i.scheduledAt ?? null,
      status: 'pending',
      input: copy(i.input),
      output: undefined,
      error: null,
      wakeAt: i.now,
      leaseOwner: null,
      leaseUntil: null,
      cancelRequested: false,
      terminateRequested: false,
      cancelReason: null,
      deadline: i.deadline,
      customStatus: null,
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
          (query.version === undefined || i.version === query.version) &&
          (query.parentId === undefined || i.parentId === query.parentId) &&
          (query.scheduleId === undefined || i.scheduleId === query.scheduleId),
      )
      .sort((a, b) => a.createdAt - b.createdAt || compare(a.id, b.id))
      .slice(query.offset, query.offset + query.limit)
      .map(copy);
  }

  async requestCancel(id: string, { reason, now, terminate }: WorkflowCancelRequest): Promise<boolean> {
    const instance = this.rows.get(id)?.instance;
    const applies = terminate ? RUNNABLE.has(instance?.status ?? '') && !instance!.terminateRequested : CANCELLABLE.has(instance?.status ?? '') && !instance!.cancelRequested;
    if (!instance || !applies) {
      return false;
    }

    instance.cancelRequested = true;
    instance.terminateRequested ||= terminate;
    instance.cancelReason = reason;
    instance.updatedAt = now;
    if (instance.wakeAt === null || instance.wakeAt > now) {
      instance.wakeAt = now;
    }

    return true;
  }

  async reopen(id: string, r: WorkflowReopen): Promise<boolean> {
    const row = this.rows.get(id);
    if (!row || row.leaseToken !== null || row.instance.status !== r.expect.status || row.instance.runs !== r.expect.runs) {
      return false;
    }

    for (const entry of r.entries) {
      row.journal.set(entry.name, copy(entry));
    }

    const i = row.instance;
    i.status = r.status;
    i.error = copy(r.error);
    if (r.deadline !== undefined) {
      i.deadline = r.deadline;
    }
    i.wakeAt = r.now;
    i.updatedAt = r.now;
    return true;
  }

  async delete(id: string, statuses: WorkflowStatus[]): Promise<boolean> {
    const row = this.rows.get(id);
    if (!row || !statuses.includes(row.instance.status)) {
      return false;
    }

    this.rows.delete(id);
    return true;
  }

  async signal(s: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    return this.record(s);
  }

  private record(s: NewWorkflowSignal): WorkflowSignalResult {
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

  async purge(query: WorkflowPurgeQuery): Promise<WorkflowPurgeResult> {
    const finished = [...this.rows.values()]
      .map((row) => row.instance)
      .filter((i) => FINISHED.has(i.status) && query.statuses.includes(i.status) && i.updatedAt < query.before)
      .sort((a, b) => a.updatedAt - b.updatedAt || compare(a.id, b.id))
      .slice(0, query.limit);
    for (const instance of finished) {
      this.rows.delete(instance.id);
    }

    const newest = this.lastSignalId();
    const cursors = [...this.rows.values()].filter((row) => RUNNABLE.has(row.instance.status)).map((row) => row.instance.signalCursor);
    const floor = Math.min(newest, ...cursors);
    const prunable = new Set(
      this.signalLog
        .filter((s) => s.id <= floor && s.id < newest && s.createdAt < query.before)
        .slice(0, query.limit)
        .map((s) => s.id),
    );
    const kept = this.signalLog.filter((s) => !prunable.has(s.id));
    this.signalLog.splice(0, this.signalLog.length, ...kept);

    const ended = [...this.windows]
      .filter(([, window]) => window.windowEnd < query.before)
      .sort(([a, x], [b, y]) => x.windowEnd - y.windowEnd || compare(a, b))
      .slice(0, query.limit);
    for (const [key] of ended) {
      this.windows.delete(key);
    }

    return { instances: finished.length, signals: prunable.size, rateLimits: ended.length };
  }

  async saveSchedule(save: WorkflowScheduleSave): Promise<WorkflowScheduleRecord | null> {
    const existing = this.schedules.get(save.id);
    if (save.expectRevision === null ? existing !== undefined : existing?.record.revision !== save.expectRevision) {
      return null;
    }

    const { expectRevision: _expect, releaseLease, now, ...fields } = save;
    const record: WorkflowScheduleRecord = {
      ...copy(fields),
      revision: (existing?.record.revision ?? 0) + 1,
      leaseOwner: existing?.record.leaseOwner ?? null,
      leaseUntil: releaseLease ? null : (existing?.record.leaseUntil ?? null),
      createdAt: existing?.record.createdAt ?? now,
      updatedAt: now,
    };
    this.schedules.set(save.id, { record, leaseToken: releaseLease ? null : (existing?.leaseToken ?? null) });
    return copy(record);
  }

  async getSchedule(id: string): Promise<WorkflowScheduleRecord | null> {
    const schedule = this.schedules.get(id);
    return schedule ? copy(schedule.record) : null;
  }

  async listSchedules(query: WorkflowScheduleQuery): Promise<WorkflowScheduleRecord[]> {
    return [...this.schedules.values()]
      .map((schedule) => schedule.record)
      .filter((record) => (query.workflow === undefined || record.workflow === query.workflow) && (query.declared === undefined || record.declared === query.declared))
      .sort((a, b) => compare(a.id, b.id))
      .slice(query.offset, query.offset + query.limit)
      .map(copy);
  }

  async deleteSchedule(id: string, revision?: number): Promise<boolean> {
    const schedule = this.schedules.get(id);
    if (!schedule || (revision !== undefined && schedule.record.revision !== revision)) {
      return false;
    }
    return this.schedules.delete(id);
  }

  async claimSchedules(request: WorkflowScheduleClaimRequest): Promise<WorkflowScheduleRecord[]> {
    const workflows = new Set(request.workflows);
    const due = [...this.schedules.values()]
      .filter(
        ({ record }) =>
          !record.paused &&
          record.wakeAt !== null &&
          record.wakeAt <= request.now &&
          (record.leaseUntil === null || record.leaseUntil < request.now) &&
          workflows.has(record.workflow),
      )
      .sort((a, b) => a.record.wakeAt! - b.record.wakeAt! || compare(a.record.id, b.record.id))
      .slice(0, request.limit);

    for (const schedule of due) {
      schedule.leaseToken = request.token;
      schedule.record.leaseOwner = request.owner;
      schedule.record.leaseUntil = request.leaseUntil;
    }
    return due.map((schedule) => copy(schedule.record));
  }

  async writeSchedule(id: string, token: string, write: WorkflowScheduleWrite): Promise<boolean> {
    const schedule = this.schedules.get(id);
    if (!schedule || schedule.leaseToken === null || schedule.leaseToken !== token) {
      return false;
    }

    const { record } = schedule;
    record.state = copy(write.state);
    record.wakeAt = write.wakeAt;
    record.revision++;
    record.updatedAt = write.now;
    if (write.release) {
      schedule.leaseToken = null;
      record.leaseUntil = null;
    }
    return true;
  }

  async claim(request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    const runs = new Set(request.workflows.map((w) => `${w.version}:${w.name}`));
    const candidates = [...this.rows.values()]
      .filter(
        ({ instance: i }) =>
          RUNNABLE.has(i.status) &&
          i.wakeAt !== null &&
          i.wakeAt <= request.now &&
          (i.leaseUntil === null || i.leaseUntil < request.now) &&
          runs.has(`${i.version}:${i.workflow}`),
      )
      .sort(
        (a, b) =>
          a.instance.priority - b.instance.priority ||
          a.instance.wakeAt! - b.instance.wakeAt! ||
          a.instance.createdAt - b.instance.createdAt ||
          compare(a.instance.id, b.instance.id),
      );

    const due = this.pick(request, candidates);
    this.recordWindows(request, due);

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

  /**
   * The staged pick of `WorkflowStore.claim()` under limits: candidates with no room at all are passed over first;
   * then each concurrency key keeps its first candidates, as many as it has free slots; of those, each rate-limit
   * key as many as its window has room for; of those, each workflow as many as both its free slots and its window
   * allow; then the first `limit`.
   */
  private pick(request: WorkflowClaimRequest, candidates: Row[]): Row[] {
    const limits = new Map((request.limits ?? []).map((limit) => [limit.workflow, limit]));
    const rules = new Map((request.rateLimits ?? []).map((rule) => [rule.workflow, rule]));
    const held = new Map<string, number>();
    for (const { instance: i } of this.rows.values()) {
      if (limits.has(i.workflow) && i.leaseUntil !== null && i.leaseUntil >= request.now) {
        for (const key of [slot(i.workflow), ...(i.concurrencyKey === null ? [] : [slot(i.workflow, i.concurrencyKey)])]) {
          held.set(key, (held.get(key) ?? 0) + 1);
        }
      }
    }

    const heldBy = (key: string) => held.get(key) ?? 0;
    const usedBy = (key: string) => this.openWindow(key, request.now)?.count ?? 0;
    const free = (i: WorkflowInstance) => {
      const limit = limits.get(i.workflow);
      const rule = rules.get(i.workflow);
      return {
        total: limit?.limit != null ? limit.limit - heldBy(slot(i.workflow)) : Infinity,
        key: limit?.perKey != null && i.concurrencyKey !== null ? limit.perKey - heldBy(slot(i.workflow, i.concurrencyKey)) : Infinity,
        rate: rule?.limit ? rule.limit.max - usedBy(slot(i.workflow)) : Infinity,
        rateKey: rule?.perKey && i.rateLimitKey !== null ? rule.perKey.max - usedBy(slot(i.workflow, i.rateLimitKey)) : Infinity,
      };
    };

    const open = candidates.filter(({ instance: i }) => Object.values(free(i)).every((room) => room > 0));
    const byKey = firsts(open, (i) => (Number.isFinite(free(i).key) ? slot(i.workflow, i.concurrencyKey!) : null), (i) => free(i).key);
    const byRateKey = firsts(byKey, (i) => (Number.isFinite(free(i).rateKey) ? slot(i.workflow, i.rateLimitKey!) : null), (i) => free(i).rateKey);
    const byWorkflow = firsts(
      byRateKey,
      (i) => (limits.has(i.workflow) || rules.has(i.workflow) ? i.workflow : null),
      (i) => Math.min(free(i).total, free(i).rate),
    );
    return byWorkflow.slice(0, request.limit);
  }

  /** Records the claims of `claimed` in their rate-limit windows, starting a window where the last one ended. */
  private recordWindows(request: WorkflowClaimRequest, claimed: Row[]): void {
    const rules = new Map((request.rateLimits ?? []).map((rule) => [rule.workflow, rule]));
    for (const { instance: i } of claimed) {
      const rule = rules.get(i.workflow);
      const windows = [
        ...(rule?.limit ? [{ key: slot(i.workflow), duration: rule.limit.duration }] : []),
        ...(rule?.perKey && i.rateLimitKey !== null ? [{ key: slot(i.workflow, i.rateLimitKey), duration: rule.perKey.duration }] : []),
      ];
      for (const { key, duration } of windows) {
        const window = this.openWindow(key, request.now);
        if (window) {
          window.count++;
        } else {
          this.windows.set(key, { windowEnd: request.now + duration, count: 1 });
        }
      }
    }
  }

  /** The window under `key` if it is still open at `now`. */
  private openWindow(key: string, now: number): { windowEnd: number; count: number } | undefined {
    const window = this.windows.get(key);
    return window && window.windowEnd > now ? window : undefined;
  }

  async renew(id: string, token: string, leaseUntil: number): Promise<{ cancelRequested: boolean; terminateRequested: boolean } | null> {
    const row = this.leased(id, token);
    if (!row) {
      return null;
    }
    row.instance.leaseUntil = leaseUntil;
    return { cancelRequested: row.instance.cancelRequested, terminateRequested: row.instance.terminateRequested };
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
    if (w.customStatus !== undefined) {
      i.customStatus = copy(w.customStatus);
    }
    if (w.signal) {
      this.record(w.signal);
    }

    if (w.release) {
      const { waits, signalCursor } = w.release;
      row.waits = copy(waits);
      row.leaseToken = null;
      i.leaseUntil = null;

      const missed = this.signalLog.some((s) => s.id > signalCursor && waits.some((wait) => wait.signal === s.name && wait.key === s.key));
      i.wakeAt = missed || (i.cancelRequested && w.status === 'suspended') ? w.now : w.release.wakeAt;
    }

    if (w.status !== undefined || w.output !== undefined || w.error !== undefined || w.customStatus !== undefined || w.release) {
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

/** A workflow's slot or window key, or one of its keys'. */
function slot(workflow: string, key?: string): string {
  return JSON.stringify(key === undefined ? [workflow] : [workflow, key]);
}

/** Of `rows` (in order), each partition's first ones, as many as `room` says; rows in no partition (`null`) all stay. */
function firsts(rows: Row[], partition: (instance: WorkflowInstance) => string | null, room: (instance: WorkflowInstance) => number): Row[] {
  const seen = new Map<string, number>();
  return rows.filter(({ instance }) => {
    const key = partition(instance);
    if (key === null) {
      return true;
    }

    const n = (seen.get(key) ?? 0) + 1;
    seen.set(key, n);
    return n <= room(instance);
  });
}
