/**
 * Where schedules live, for the core's `Scheduler`: any store can implement it (SQL, a Redis store as scripts), and
 * `scheduleStoreContract()` from `@nestjs/workflows/testing` checks one, races included. `InMemoryScheduleStore` is
 * the in-memory one. A schedule's `spec`, `payload` and `state` are the scheduler's JSON: keep them as they are.
 *
 * Three methods need more than a plain read or write: `claimSchedules` (a lock that skips rows other claims hold),
 * `writeSchedule` (fenced by the lease token) and `saveSchedule` (conditional on a revision). Everything else is safe
 * to implement naively.
 *
 * ```ts
 * export class RedisScheduleStore implements ScheduleStore {
 *   async claimSchedules(request: ScheduleClaimRequest): Promise<ScheduleRecord[]> {
 *     // One script: due ids by wakeAt (ZRANGEBYSCORE), skipping leased ones, leased to request.token.
 *   }
 *   // ...
 * }
 * ```
 */
export interface ScheduleStore {
  /**
   * Stores a schedule, as one conditional write, and returns it as stored, or `null` when the condition fails. With
   * `expectRevision: null`, inserts it unless a schedule with the id exists (revision 1, `createdAt = now`); with a
   * number, replaces every field of the one whose `revision` is that number (revision + 1). Either way sets
   * `updatedAt = now`; with `releaseLease`, clears its lease, so the worker that holds it can't `writeSchedule()` any
   * more. Of two concurrent saves expecting the same revision, one lands.
   */
  saveSchedule(schedule: ScheduleSave): Promise<ScheduleRecord | null>;
  /** The schedule, or `null` for an unknown id. */
  getSchedule(id: string): Promise<ScheduleRecord | null>;
  /** Schedules matching the filter, ordered by `id`; a page of them. */
  listSchedules(query: ScheduleQuery): Promise<ScheduleRecord[]>;
  /** Deletes the schedule (if its `revision` is still `revision`, when given), and returns whether it did. */
  deleteSchedule(id: string, revision?: number): Promise<boolean>;
  /**
   * Leases up to `limit` due schedules to a worker, most overdue first (`wakeAt`, then `id`). Due: not paused,
   * `wakeAt <= now`, no lease or an expired one (`leaseUntil < now`), and `target` in `targets`. Each gets
   * `leaseToken = token`, `leaseOwner = owner` and `leaseUntil`; nothing else changes (not `revision`). Two
   * concurrent claims never return the same schedule: lock the candidates and skip those another claim holds.
   */
  claimSchedules(request: ScheduleClaimRequest): Promise<ScheduleRecord[]>;
  /**
   * The lease holder's write: sets `state` and `wakeAt`, `revision + 1` and `updatedAt = now`, and with `release`
   * clears the lease (`leaseToken`, `leaseUntil`), only while `token` is the schedule's lease token; otherwise writes
   * nothing and returns `false`. One conditional update.
   */
  writeSchedule(id: string, token: string, write: ScheduleWrite): Promise<boolean>;
}

/**
 * A schedule as stored: `ScheduleStore.getSchedule()`, `listSchedules()` and `claimSchedules()` return it.
 *
 * ```ts
 * const due = await store.claimSchedules({ owner, token, now, leaseUntil, limit: 100, targets: ['emails'] });
 * ```
 */
export interface ScheduleRecord {
  id: string;
  /** What it starts runs of, such as a workflow's or a queue's name (claims filter on it). */
  target: string;
  /** Declared in the code (kept in step with it), rather than saved at runtime. */
  declared: boolean;
  /** The scheduler's JSON: when it runs and how. Store it as it is. */
  spec: unknown;
  /** What the runs it starts get (JSON: a string with a codec), or `null`. */
  payload: unknown;
  paused: boolean;
  /** When a worker next has something to do for it (`claimSchedules()` looks for `wakeAt <= now`), or `null`. */
  wakeAt: number | null;
  /** The scheduler's JSON bookkeeping. Store it as it is. */
  state: unknown;
  /** Bumped by every `saveSchedule()` and `writeSchedule()`. */
  revision: number;
  leaseOwner: string | null;
  leaseUntil: number | null;
  createdAt: number;
  updatedAt: number;
}

/**
 * What `ScheduleStore.saveSchedule()` receives.
 *
 * ```ts
 * await store.saveSchedule({ ...fields, id: 'weekly-digest', expectRevision: current?.revision ?? null, releaseLease: false, now });
 * ```
 */
export interface ScheduleSave {
  id: string;
  target: string;
  declared: boolean;
  spec: unknown;
  payload: unknown;
  paused: boolean;
  wakeAt: number | null;
  state: unknown;
  /** `null`: insert it, if no schedule has the id. A number: replace the one whose `revision` is still this. */
  expectRevision: number | null;
  /** Clear the schedule's lease (a change to when it runs: the lease holder's work is outdated). */
  releaseLease: boolean;
  now: number;
}

/**
 * What `ScheduleStore.listSchedules()` receives.
 *
 * ```ts
 * await store.listSchedules({ target: 'emails', declared: true, limit: 100, offset: 0 });
 * ```
 */
export interface ScheduleQuery {
  target?: string;
  /** Only the declared ones (`true`), or only the others (`false`). */
  declared?: boolean;
  limit: number;
  offset: number;
}

/**
 * What `ScheduleStore.claimSchedules()` receives.
 *
 * ```ts
 * await store.claimSchedules({ owner: 'pod-1', token: randomUUID(), now, leaseUntil: now + 30_000, limit: 100, targets: ['emails'] });
 * ```
 */
export interface ScheduleClaimRequest {
  owner: string;
  token: string;
  now: number;
  leaseUntil: number;
  /** At least 1. */
  limit: number;
  /** The targets this worker runs (at least one): it starts only their schedules' runs. */
  targets: string[];
}

/**
 * What `ScheduleStore.writeSchedule()` receives.
 *
 * ```ts
 * await store.writeSchedule(id, token, { now, state, wakeAt: next, release: true });
 * ```
 */
export interface ScheduleWrite {
  now: number;
  state: unknown;
  wakeAt: number | null;
  /** Hand the schedule back: clear its lease. */
  release: boolean;
}
