/**
 * A tiny job queue on PostgreSQL, built only from `@nestjs/workflows/core` and `@nestjs/workflows/postgres`, as a
 * sketch of how a queue package composes them (tiny-queue.spec.ts runs it; nothing of it is published):
 *
 * - its SQL runs through a `SqlExecutor` (`fromPg()` and the rest), in the application's transaction for `add()`;
 * - a `LeasedWorker` claims jobs under leases, renews them, aborts a job's signal when its lease is lost, and drains;
 * - `resolveRetry()` and `nextRetry()` decide when a failed job runs again;
 * - a `Scheduler` over its own PostgreSQL `ScheduleStore` (which passes `scheduleStoreContract()`) adds a job per
 *   occurrence, with the occurrence's id;
 * - `PayloadCodecs` encrypts the jobs' data, results and errors, and the schedules' data, as workflows does;
 * - a `ResultWaiter` answers `result()`, at once for a job this process ran.
 */
import { randomUUID } from 'node:crypto';
import {
  LeasedWorker,
  nextRetry,
  parseSchedule,
  PayloadCodecs,
  resolveRetry,
  ResultWaiter,
  Scheduler,
  systemClock,
  type Clock,
  type LeasedRun,
  type PayloadCodec,
  type ResolvedRetry,
  type ResultOutcome,
  type ResultWaitOptions,
  type RetrySettings,
  type ScheduleClaimRequest,
  type ScheduleOptions,
  type ScheduleQuery,
  type ScheduleRecord,
  type ScheduleSave,
  type ScheduleStore,
  type ScheduleWrite,
  type SerializedError,
} from '../../lib/core/index.js';
import type { SqlExecutor, SqlTransaction } from '../../lib/postgres/index.js';

export interface TinyQueueOptions<D, R> {
  /** The queue's name: its jobs' `queue`, and its schedules' target. */
  name: string;
  executor: SqlExecutor;
  /** The schema of its tables. */
  schema: string;
  handler: (data: D, context: { signal: AbortSignal; attempt: number }) => Promise<R>;
  codecs?: PayloadCodec[];
  retry?: number | false | RetrySettings;
  clock?: Clock;
  concurrency?: number;
  leaseDuration?: `${number}ms` | `${number}s`;
}

/** A claimed job, as the worker runs it. */
interface Job {
  id: string;
  token: string;
  attempt: number;
  data: unknown;
}

type Row = Record<string, string | null>;

export class TinyQueue<D = unknown, R = unknown> {
  readonly worker: LeasedWorker<Job>;
  readonly scheduler: Scheduler;
  /** Its schedules' store, on its own table. */
  readonly scheduleStore: PostgresScheduleStore;
  private readonly codecs: PayloadCodecs;
  private readonly results: ResultWaiter<R>;
  private readonly retry: ResolvedRetry;
  private readonly clock: Clock;
  private readonly jobs: string;

  constructor(private readonly options: TinyQueueOptions<D, R>) {
    const { name, executor, schema } = options;
    this.clock = options.clock ?? systemClock;
    this.jobs = `"${schema}".jobs`;
    this.codecs = new PayloadCodecs(options.codecs ?? [], { name: "TinyQueue's codec" });
    this.retry = resolveRetry(options.retry);
    this.scheduleStore = new PostgresScheduleStore(executor, schema);
    this.results = new ResultWaiter<R>({ read: (id) => this.outcomeOf(id) });
    this.scheduler = new Scheduler({
      store: this.scheduleStore,
      clock: this.clock,
      codecs: this.codecs,
      payloadContext: (schedule) => ({ field: 'data', queue: name, schedule }),
      targets: () => [name],
      fire: async ({ schedule, id }) => this.add(schedule.payload as D, { id, scheduleId: schedule.id }),
      running: async (schedule) =>
        (await executor.query<Row>(`SELECT id FROM ${this.jobs} WHERE schedule_id = $1 AND state IN ('waiting', 'active')`, [schedule.id])).map((row) => row.id!),
      cancel: async (id) => (await executor.query(`UPDATE ${this.jobs} SET state = 'cancelled' WHERE id = $1 AND state = 'waiting' RETURNING id`, [id])).length === 1,
      labels: { target: (queue) => `queue "${queue}"`, targets: 'queues', payload: 'data' },
    });
    this.worker = new LeasedWorker<Job>({
      concurrency: options.concurrency ?? 5,
      pollInterval: '20ms',
      leaseDuration: options.leaseDuration ?? '30s',
      clock: this.clock,
      claim: (lease, limit) => this.claim(lease.token, lease.owner, lease.now, lease.until, limit),
      renew: async (job, until) =>
        (await executor.query(`UPDATE ${this.jobs} SET lease_until = $1 WHERE id = $2 AND lease_token = $3 RETURNING id`, [until, job.id, job.token])).length === 1,
      execute: (job, run) => this.execute(job, run),
      produce: async () => (await this.scheduler.produce(this.worker.owner, this.worker.leaseMs)).started,
    });
  }

  /** Creates its schema and tables. */
  async migrate(): Promise<void> {
    const { executor, schema } = this.options;
    await executor.query(`CREATE SCHEMA IF NOT EXISTS "${schema}"`);
    await executor.query(`CREATE TABLE IF NOT EXISTS ${this.jobs} (
  id text PRIMARY KEY, queue text NOT NULL, data jsonb, state text NOT NULL DEFAULT 'waiting', attempts integer NOT NULL DEFAULT 0,
  run_at bigint NOT NULL, lease_token text, lease_owner text, lease_until bigint, result jsonb, error jsonb, schedule_id text,
  created_at bigint NOT NULL, updated_at bigint NOT NULL)`);
    await executor.query(`CREATE TABLE IF NOT EXISTS "${schema}".schedules (
  id text PRIMARY KEY, target text NOT NULL, declared boolean NOT NULL, spec jsonb NOT NULL, payload jsonb, paused boolean NOT NULL,
  wake_at bigint, state jsonb NOT NULL, revision integer NOT NULL, lease_token text, lease_owner text, lease_until bigint,
  created_at bigint NOT NULL, updated_at bigint NOT NULL)`);
  }

  /** Adds a job (in `transaction`, the application's, when given): a job with the same id makes it a no-op. */
  async add(data: D, options: { id?: string; transaction?: unknown; scheduleId?: string } = {}): Promise<{ id: string; created: boolean }> {
    const id = options.id ?? randomUUID();
    const db: SqlTransaction = options.transaction === undefined ? this.options.executor : this.options.executor.wrapTransaction(options.transaction);
    const now = this.clock.now();
    const stored = await this.codecs.encode(data, { field: 'data', queue: this.options.name, job: id });
    const rows = await db.query(
      `INSERT INTO ${this.jobs} (id, queue, data, run_at, schedule_id, created_at, updated_at) VALUES ($1, $2, $3::text::jsonb, $4, $5, $4, $4)
ON CONFLICT (id) DO NOTHING RETURNING id`,
      [id, this.options.name, stored === undefined ? null : JSON.stringify(stored), now, options.scheduleId ?? null],
    );
    this.worker.kick();
    return { id, created: rows.length === 1 };
  }

  /** Saves a schedule that adds a job with `data` per occurrence. */
  upsertSchedule(id: string, schedule: ScheduleOptions, data: D): Promise<ScheduleRecord | null> {
    return this.scheduler.save(
      id,
      (current) => this.scheduler.changed(current, { target: this.options.name, declared: false, spec: parseSchedule(schedule, `Schedule "${id}"`), payload: data }),
      { replacing: true },
    );
  }

  /** The job's result: at once when this process ran it, else once the table says it ended. */
  result(id: string, options?: ResultWaitOptions): Promise<R> {
    return this.results.wait(id, options);
  }

  async stop(): Promise<void> {
    await this.worker.shutdown();
    await this.results.close();
  }

  private async claim(token: string, owner: string, now: number, until: number, limit: number): Promise<Job[]> {
    const rows = await this.options.executor.query<Row>(
      `UPDATE ${this.jobs} SET state = 'active', attempts = attempts + 1, lease_token = $1, lease_owner = $2, lease_until = $3, updated_at = $4
WHERE id IN (
  SELECT id FROM ${this.jobs}
  WHERE queue = $5 AND state IN ('waiting', 'active') AND run_at <= $4 AND (lease_until IS NULL OR lease_until < $4)
  ORDER BY run_at, id LIMIT $6 FOR UPDATE SKIP LOCKED)
RETURNING id, data::text AS data, attempts::text AS attempts`,
      [token, owner, until, now, this.options.name, limit],
    );
    return Promise.all(
      rows.map(async (row) => ({
        id: row.id!,
        token,
        attempt: Number(row.attempts),
        data: await this.codecs.decode(row.data === null ? null : JSON.parse(row.data), { field: 'data', queue: this.options.name, job: row.id! }),
      })),
    );
  }

  private async execute(job: Job, run: LeasedRun): Promise<void> {
    const context = (field: string) => ({ field, queue: this.options.name, job: job.id });
    let outcome: ResultOutcome<R>;
    let write: { state: string; result?: unknown; error?: SerializedError; runAt?: number };
    try {
      const value = await this.options.handler(job.data as D, { signal: run.signal, attempt: job.attempt });
      outcome = { value };
      write = { state: 'completed', result: await this.codecs.encode(value, context('result')) };
    } catch (error) {
      const next = nextRetry(this.retry, job.attempt, error);
      const failure = { name: (error as Error).name ?? 'Error', message: String((error as Error).message ?? error) };
      if (next.retry) {
        await this.finish(job, run, { state: 'waiting', runAt: this.clock.now() + next.delay, error: await this.codecs.encodeError(failure, context('error')) });
        return;
      }
      outcome = { error: new Error(`Job "${job.id}" failed: ${failure.message}`) };
      write = { state: 'failed', error: await this.codecs.encodeError(failure, context('error')) };
    }

    if (await this.finish(job, run, write)) {
      this.results.settle(job.id, outcome);
    }
  }

  /** The lease holder's write, fenced by its token: `false` (and the lease lost) once another worker took the job. */
  private async finish(job: Job, run: LeasedRun, write: { state: string; result?: unknown; error?: unknown; runAt?: number }): Promise<boolean> {
    const rows = await this.options.executor.query(
      `UPDATE ${this.jobs} SET state = $1, result = $2::text::jsonb, error = $3::text::jsonb, run_at = coalesce($4, run_at), lease_token = NULL, lease_until = NULL, updated_at = $5
WHERE id = $6 AND lease_token = $7 RETURNING id`,
      [write.state, write.result === undefined ? null : JSON.stringify(write.result), write.error === undefined ? null : JSON.stringify(write.error), write.runAt ?? null, this.clock.now(), job.id, job.token],
    );
    if (rows.length === 0) {
      run.loseLease();
      return false;
    }
    return true;
  }

  private async outcomeOf(id: string): Promise<ResultOutcome<R> | null> {
    const [row] = await this.options.executor.query<Row>(`SELECT state, result::text AS result, error::text AS error FROM ${this.jobs} WHERE id = $1`, [id]);
    const context = (field: string) => ({ field, queue: this.options.name, job: id });
    if (!row) {
      return { error: new Error(`No job "${id}".`) };
    }
    if (row.state === 'completed') {
      return { value: (await this.codecs.decode(row.result === null ? null : JSON.parse(row.result), context('result'))) as R };
    }
    if (row.state === 'failed') {
      const error = await this.codecs.decodeError(JSON.parse(row.error!) as SerializedError, context('error'));
      return { error: new Error(`Job "${id}" failed: ${error.message}`) };
    }
    return null;
  }
}

/**
 * The core's `ScheduleStore` on a PostgreSQL table of the queue's, through a `SqlExecutor`: what a queue's PostgreSQL
 * store implements, and what `scheduleStoreContract()` checks.
 */
export class PostgresScheduleStore implements ScheduleStore {
  private readonly table: string;

  constructor(
    private readonly executor: SqlExecutor,
    schema: string,
  ) {
    this.table = `"${schema}".schedules`;
  }

  async saveSchedule(save: ScheduleSave): Promise<ScheduleRecord | null> {
    const values = [save.id, save.target, save.declared, JSON.stringify(save.spec), json(save.payload), save.paused, save.wakeAt, JSON.stringify(save.state), save.now];
    if (save.expectRevision === null) {
      const [row] = await this.executor.query<Row>(
        `INSERT INTO ${this.table} (id, target, declared, spec, payload, paused, wake_at, state, revision, created_at, updated_at)
VALUES ($1, $2, $3, $4::text::jsonb, $5::text::jsonb, $6, $7, $8::text::jsonb, 1, $9, $9) ON CONFLICT (id) DO NOTHING RETURNING ${COLUMNS}`,
        values,
      );
      return row ? record(row) : null;
    }

    // One conditional update: of two saves that read the same revision, the second finds it changed.
    const [row] = await this.executor.query<Row>(
      `UPDATE ${this.table} SET target = $2, declared = $3, spec = $4::text::jsonb, payload = $5::text::jsonb, paused = $6, wake_at = $7, state = $8::text::jsonb,
  revision = revision + 1, updated_at = $9${save.releaseLease ? ', lease_token = NULL, lease_until = NULL' : ''}
WHERE id = $1 AND revision = $10 RETURNING ${COLUMNS}`,
      [...values, save.expectRevision],
    );
    return row ? record(row) : null;
  }

  async getSchedule(id: string): Promise<ScheduleRecord | null> {
    const [row] = await this.executor.query<Row>(`SELECT ${COLUMNS} FROM ${this.table} WHERE id = $1`, [id]);
    return row ? record(row) : null;
  }

  async listSchedules(query: ScheduleQuery): Promise<ScheduleRecord[]> {
    const rows = await this.executor.query<Row>(
      `SELECT ${COLUMNS} FROM ${this.table} s WHERE ($1::text IS NULL OR target = $1) AND ($2::boolean IS NULL OR declared = $2) ORDER BY s.id LIMIT $3 OFFSET $4`,
      [query.target ?? null, query.declared ?? null, query.limit, query.offset],
    );
    return rows.map(record);
  }

  async deleteSchedule(id: string, revision?: number): Promise<boolean> {
    const rows = await this.executor.query(`DELETE FROM ${this.table} WHERE id = $1 AND ($2::integer IS NULL OR revision = $2) RETURNING id`, [id, revision ?? null]);
    return rows.length === 1;
  }

  async claimSchedules(request: ScheduleClaimRequest): Promise<ScheduleRecord[]> {
    // Schedules another claim is locking right now are skipped instead of waited for.
    const rows = await this.executor.query<Row>(
      `WITH claimed AS (
  UPDATE ${this.table} SET lease_token = $1, lease_owner = $2, lease_until = $3
  WHERE id IN (
    SELECT id FROM ${this.table}
    WHERE NOT paused AND wake_at IS NOT NULL AND wake_at <= $4 AND (lease_until IS NULL OR lease_until < $4) AND target = ANY($5::text[])
    ORDER BY wake_at, id LIMIT $6 FOR UPDATE SKIP LOCKED)
  RETURNING *)
SELECT ${COLUMNS} FROM claimed ORDER BY claimed.wake_at, claimed.id`,
      [request.token, request.owner, request.leaseUntil, request.now, request.targets, request.limit],
    );
    return rows.map(record);
  }

  async writeSchedule(id: string, token: string, write: ScheduleWrite): Promise<boolean> {
    const rows = await this.executor.query(
      `UPDATE ${this.table} SET state = $1::text::jsonb, wake_at = $2, revision = revision + 1, updated_at = $3${write.release ? ', lease_token = NULL, lease_until = NULL' : ''}
WHERE id = $4 AND lease_token = $5 RETURNING id`,
      [JSON.stringify(write.state), write.wakeAt, write.now, id, token],
    );
    return rows.length === 1;
  }
}

const COLUMNS = ['id', 'target', 'declared', 'spec', 'payload', 'paused', 'wake_at', 'state', 'revision', 'lease_owner', 'lease_until', 'created_at', 'updated_at']
  .map((column) => `${column}::text AS ${column}`)
  .join(', ');

function record(row: Row): ScheduleRecord {
  return {
    id: row.id!,
    target: row.target!,
    declared: row.declared === 'true',
    spec: JSON.parse(row.spec!),
    payload: row.payload === null ? null : JSON.parse(row.payload),
    paused: row.paused === 'true',
    wakeAt: row.wake_at === null ? null : Number(row.wake_at),
    state: JSON.parse(row.state!),
    revision: Number(row.revision),
    leaseOwner: row.lease_owner,
    leaseUntil: row.lease_until === null ? null : Number(row.lease_until),
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
  };
}

/** A payload as a jsonb parameter: `null` stays SQL NULL. */
function json(value: unknown): string | null {
  return value === null || value === undefined ? null : JSON.stringify(value);
}
