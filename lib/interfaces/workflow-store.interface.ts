import type { SerializedWorkflowError } from './serialized-workflow-error.interface.js';
import type {
  WorkflowInstance,
  WorkflowJournalEntry,
  WorkflowStatus,
  WorkflowWait,
} from './workflow-instance.interface.js';

/**
 * Where workflow instances, their journals, their waits and the signals sent to them live.
 * The package ships `InMemoryWorkflowStore` (the default, for development and tests); in
 * production, write a provider on your database that implements this interface and registers
 * itself with `WorkflowStorage.registerSource(this)` in its constructor.
 *
 * "The store contract" (https://docs.nestjs.com/reliability/workflows#the-store-contract) sums up
 * every method; each one's JSDoc here says what it must do, what must be atomic, and the race
 * each rule prevents. `workflowStoreContract()` from `@nestjs/workflows/testing` checks an
 * implementation against it, races included.
 *
 * Four methods need more than a plain read or write: `claim` (a lock that skips rows other
 * claims hold), `write` and `renew` (fenced by the lease token), and `signal` together with
 * `write` when it registers waits (a lock that orders them, see `signal`). `reopen` and `purge`
 * re-check their conditions on the rows they change. Everything else is safe to implement
 * naively.
 */
export interface WorkflowStore {
  // ---------------------------------------------------------------- instances

  /**
   * Inserts a `pending` instance, due at `now`, unless one with the same id exists. Returns the
   * stored instance: the new one (`created: true`) or the existing one, unchanged
   * (`created: false`). Must be atomic per id: of two concurrent calls, one creates.
   */
  create(instance: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }>;
  /**
   * Optional: `create()` through the application's transaction, for
   * `start(workflow, input, { transaction })`. `transaction` is what the application's ORM
   * hands its transaction callback (a Drizzle `tx`, a TypeORM `EntityManager`...). Write only
   * through it, so the instance commits or rolls back with the application's rows, and never
   * catch a database error inside it (on PostgreSQL that aborts the transaction: use
   * insert-or-ignore). Without this method, `start()` with a transaction throws.
   */
  createInTransaction?(transaction: unknown, instance: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }>;
  /**
   * The instance with the waits its last suspension registered (`[]` when none), and its
   * journal when `options.journal` is set, or `null` for an unknown id.
   */
  get(id: string, options?: { journal?: boolean }): Promise<WorkflowInstanceDetails | null>;
  /** Instances matching every given filter, ordered by `createdAt`, then `id`; a page of them. */
  list(query: WorkflowListQuery): Promise<WorkflowInstance[]>;
  /**
   * Sets `cancelRequested` and `cancelReason` on a `pending`, `running` or `suspended`
   * instance whose `cancelRequested` is still false, makes it due now (`wakeAt` becomes `now`
   * unless it is already due) and sets `updatedAt`. Returns whether it changed anything:
   * `false` for an unknown id, a finished or compensating instance, or a repeated request.
   * One conditional update: two concurrent requests accept one.
   */
  requestCancel(id: string, reason: string | null, now: number): Promise<boolean>;
  /**
   * An operator's retry of a finished instance (`WorkflowClient.retry()`), as one conditional
   * write: if the instance holds no lease, its status is `expect.status` and its `runs` is
   * `expect.runs` (nothing changed it since the engine read it), upsert `entries` as `write()`
   * does, set `status`, `error` and, when given, `deadline`, make it due (`wakeAt = now`), set
   * `updatedAt = now`, and return `true`. Otherwise change nothing and return `false`. One
   * transaction that locks the instance row: of two concurrent retries, one is accepted.
   */
  reopen(id: string, reopen: WorkflowReopen): Promise<boolean>;
  /**
   * Deletes the instance, with its journal and waits, if its status is one of `statuses`, and
   * returns whether it did. One statement: a worker that holds its lease finds it gone at its
   * next write or renewal.
   */
  delete(id: string, statuses: WorkflowStatus[]): Promise<boolean>;

  // ---------------------------------------------------------------- signals

  /**
   * Records a signal under the next signal id and makes due now (`wakeAt = now` unless already
   * due, and `updatedAt`) every `suspended` instance with a wait for the same name and exactly
   * the same key. Returns the id and how many instances it woke.
   *
   * Signals must be serialized with each other and with `write()`s that register waits: take
   * an exclusive lock before choosing the id and hold it until commit (the `write()` side takes
   * it shared). That gives the two guarantees the engine relies on: signal ids become visible
   * in id order, and a signal and a suspension of the same instance never interleave.
   *
   * With a `dedupeId`, a signal stored earlier with the same name and `dedupeId` makes the call
   * a no-op: write and wake nothing, and return that signal's id and key with `created: false`.
   * Enforce it with a unique constraint on `(name, dedupeId)` and insert-or-ignore, never a read
   * followed by a write, and never by catching the duplicate-key error (in the application's
   * transaction, on PostgreSQL, that aborts it). Signals without a `dedupeId` never conflict.
   */
  signal(signal: NewWorkflowSignal): Promise<WorkflowSignalResult>;
  /**
   * Optional: `signal()` through the application's transaction, for
   * `signal(signal, payload, { transaction })`, under the same rules as `createInTransaction()`.
   * The lock is then held until the application's transaction ends. On PostgreSQL the
   * transaction must be READ COMMITTED, or the wake-up can miss waits committed after its
   * snapshot: refuse other isolation levels.
   */
  signalInTransaction?(transaction: unknown, signal: NewWorkflowSignal): Promise<WorkflowSignalResult>;
  /** Signals named `name` with exactly `key` and `afterId < id <= upToId`, by id. */
  signals(query: WorkflowSignalQuery): Promise<WorkflowSignalRecord[]>;

  // ---------------------------------------------------------------- retention

  /**
   * Deletes, oldest `updatedAt` first, up to `limit` instances whose status is one of
   * `statuses` (finished ones only) and whose `updatedAt` is below `before`, with their
   * journals and waits. Re-check the status and `updatedAt` on the rows it deletes (in the
   * `DELETE`'s own `WHERE`, not only in a subquery), so an instance that an operator reopened in
   * the meantime stays.
   *
   * Also deletes, lowest id first, up to `limit` signals no instance can take any more: an
   * instance only takes signals with an id above its `signalCursor`, and a new one starts at the
   * last signal id. So a signal can go when its id is at or below the lowest `signalCursor` of
   * the unfinished instances (`pending`, `running`, `suspended`, `compensating`), its
   * `createdAt` is below `before` (its `dedupeId` keeps deduplicating until then), and it isn't
   * the newest signal (which keeps the last signal id from going back). Returns how many
   * instances and signals it deleted. Each delete is one statement; nothing else is atomic.
   */
  purge(query: WorkflowPurgeQuery): Promise<WorkflowPurgeResult>;

  // ---------------------------------------------------------------- the worker

  /**
   * Leases up to `limit` due instances to a worker, most overdue first. Due: status `pending`,
   * `running`, `suspended` or `compensating`, `wakeAt <= now`, no lease or an expired one
   * (`leaseUntil < now`), and a `workflow`/`version` pair in `workflows`. Each claimed instance
   * gets `leaseToken = token`, `leaseOwner = owner`, `leaseUntil`, `runs + 1`,
   * `updatedAt = now` and status `running` (a `compensating` one keeps its status).
   *
   * Two concurrent claims must never return the same instance: lock the candidates and skip
   * those another claim holds (`FOR UPDATE SKIP LOCKED`). Also returns the last signal id,
   * read after the claim, as the execution's signal cursor.
   */
  claim(request: WorkflowClaimRequest): Promise<WorkflowClaim>;
  /**
   * Extends the lease to `leaseUntil` if `token` is still the instance's lease token, and
   * returns its `cancelRequested`, or `null` (changing nothing) if the lease is gone. One
   * conditional update.
   */
  renew(id: string, token: string, leaseUntil: number): Promise<{ cancelRequested: boolean } | null>;
  /**
   * Every write by the worker that holds the lease: journal entries, a status change, the
   * outcome, and handing the instance back. All or nothing, in one transaction, and only while
   * `token` is the instance's lease token: otherwise write nothing and return `false`.
   * See `WorkflowWrite` for what each field does.
   */
  write(id: string, token: string, write: WorkflowWrite): Promise<boolean>;
}

/** `WorkflowStore.get()`'s result, and `WorkflowClient.getStatus()`'s. */
export interface WorkflowInstanceDetails extends WorkflowInstance {
  /** Signals the instance waits for, if suspended in `waitForSignal()`. */
  waits: WorkflowWait[];
  /** With `{ journal: true }`: every step, sleep, wait and compensation, in first-write order. */
  journal?: WorkflowJournalEntry[];
}

export interface NewWorkflowInstance {
  id: string;
  workflow: string;
  version: number;
  input: unknown;
  /** The instance's `deadline`: when its run timeout passes, or `null`. Stored as is. */
  deadline: number | null;
  /**
   * `createdAt`, `updatedAt` and `wakeAt`. The new instance also gets `signalCursor` = the
   * last signal id (signals sent after it started can match its waits), `runs: 0`, no lease,
   * no cancel request and `customStatus: null`.
   */
  now: number;
}

/** What `WorkflowStore.list()` receives: validated and defaulted by the engine. */
export interface WorkflowListQuery {
  /** Any of these statuses (never empty). */
  status?: WorkflowStatus[];
  workflow?: string;
  version?: number;
  limit: number;
  offset: number;
}

export interface NewWorkflowSignal {
  name: string;
  /** Correlation key; `null` for a signal sent without one. Matches waits with exactly this key. */
  key: string | null;
  /**
   * The sender's id for this signal (`WorkflowClient.signal()`'s `id` option), unique per signal
   * name: a signal with the same name and `dedupeId` is stored once. `null`: never deduplicated.
   */
  dedupeId: string | null;
  payload: unknown;
  now: number;
}

/** What `WorkflowStore.signal()` and `signalInTransaction()` return. */
export interface WorkflowSignalResult {
  /** The stored signal's id: the new one, or with `created: false` the one stored earlier. */
  id: number;
  /** Instances made due; `0` with `created: false`. */
  woken: number;
  /** `false` when a signal with the same name and `dedupeId` was stored earlier. */
  created: boolean;
  /** The stored signal's key: the given one, or with `created: false` the earlier signal's. */
  key: string | null;
}

/** What `WorkflowStore.reopen()` receives. */
export interface WorkflowReopen {
  /** What the engine read: the write applies only while the instance is still like this. */
  expect: { status: WorkflowStatus; runs: number };
  /** `pending` to run again, `compensating` to retry its compensations. */
  status: 'pending' | 'compensating';
  error: SerializedWorkflowError | null;
  /** A new `deadline`; `undefined` leaves it as it is. */
  deadline?: number | null;
  /** Journal entries to upsert by name, as in `WorkflowWrite.entries`. */
  entries: WorkflowJournalEntry[];
  now: number;
}

/** What `WorkflowStore.purge()` receives. */
export interface WorkflowPurgeQuery {
  /** Finished statuses to delete (at least one). */
  statuses: WorkflowStatus[];
  /** Instances whose `updatedAt` (when they finished), and signals whose `createdAt`, is below this. */
  before: number;
  /** At least 1: the most instances, and the most signals, one call deletes. */
  limit: number;
}

/** How many instances and signals a purge deleted. */
export interface WorkflowPurgeResult {
  instances: number;
  signals: number;
}

export interface WorkflowSignalQuery {
  name: string;
  /** Exactly this key: `null` matches only signals sent without one. */
  key: string | null;
  afterId: number;
  upToId: number;
}

/** A signal as stored. */
export interface WorkflowSignalRecord {
  id: number;
  name: string;
  key: string | null;
  payload: unknown;
  createdAt: number;
}

export interface WorkflowClaimRequest {
  /** The worker's id, for `leaseOwner`. */
  owner: string;
  /** A new token for this claim; every write under the lease presents it. */
  token: string;
  now: number;
  leaseUntil: number;
  /** At least 1. */
  limit: number;
  /** The workflow versions this worker runs (at least one). Leave the others to other workers. */
  workflows: Array<{ name: string; version: number }>;
}

export interface WorkflowClaim {
  /** The claimed instances, as updated by the claim. */
  instances: WorkflowInstance[];
  /** The last signal id: the highest id `signal()` has returned, or 0. */
  lastSignalId: number;
}

/**
 * One write by the lease holder (`WorkflowStore.write()`). In one transaction:
 *
 * 1. If `release.waits` is not empty, take the signal lock in shared mode (see
 *    `WorkflowStore.signal()`), before anything else.
 * 2. Lock the instance row if its lease token is `token`; if not, write nothing, return `false`.
 * 3. Upsert `entries` by name.
 * 4. Set `status`, `output`, `error` and `customStatus` when present (`undefined` leaves them as
 *    they are).
 * 5. With `release`: replace the instance's waits with `release.waits`, clear `leaseToken` and
 *    `leaseUntil` (keep `leaseOwner`), and set `wakeAt`: `now` if a signal with an id above
 *    `release.signalCursor` matches one of the new waits (name and exact key), or if the
 *    instance has `cancelRequested` and the new status is `suspended`; otherwise
 *    `release.wakeAt`.
 * 6. Set `updatedAt = now` when anything besides the journal changed.
 */
export interface WorkflowWrite {
  now: number;
  /**
   * Journal entries, each with a name unique within the write. An entry replaces the stored
   * entry with the same name entirely; a new name goes after every name the instance already
   * has, in array order. `get(id, { journal: true })` returns them in that first-write order,
   * with `null` and `undefined` fields kept apart (store each entry as one JSON document).
   */
  entries: WorkflowJournalEntry[];
  status?: WorkflowStatus;
  output?: unknown;
  error?: SerializedWorkflowError | null;
  /** The instance's `customStatus` (`null` clears it). */
  customStatus?: unknown;
  /** Hand the instance back: parked (`suspended`), finished, or due again at once. */
  release?: WorkflowRelease;
}

export interface WorkflowRelease {
  /** When the instance is next due; `null`: only a signal or a cancel can wake it. */
  wakeAt: number | null;
  /** The waits to register, replacing the previous ones (`[]` clears them). */
  waits: WorkflowWait[];
  /** The execution's signal cursor (`WorkflowClaim.lastSignalId`): signals above it weren't seen. */
  signalCursor: number;
}
