import { Injectable } from '@nestjs/common';
import { InjectDrizzle } from '@nestjs/drizzle';
import {
  WorkflowStorage,
  type NewWorkflowInstance,
  type NewWorkflowSignal,
  type WorkflowCancelRequest,
  type WorkflowClaim,
  type WorkflowClaimRequest,
  type WorkflowInstance,
  type WorkflowInstanceDetails,
  type WorkflowJournalEntry,
  type WorkflowListQuery,
  type WorkflowPurgeQuery,
  type WorkflowPurgeResult,
  type WorkflowReopen,
  type WorkflowScheduleClaimRequest,
  type WorkflowScheduleQuery,
  type WorkflowScheduleRecord,
  type WorkflowScheduleSave,
  type WorkflowScheduleWrite,
  type WorkflowSignalQuery,
  type WorkflowSignalRecord,
  type WorkflowSignalResult,
  type WorkflowStatus,
  type WorkflowStore,
  type WorkflowWrite,
} from '../../../lib/index.js';
import { and, asc, eq, gt, inArray, isNotNull, isNull, lt, lte, max, or, sql, type Column, type SQL, type SQLWrapper } from 'drizzle-orm';
import { PgTransaction } from 'drizzle-orm/pg-core';
import type { Database, Transaction } from './drizzle.js';
import {
  workflowInstances as instances,
  workflowJournal as journal,
  workflowRateLimits as rateLimits,
  workflowSchedules as schedules,
  workflowSignals as signals,
  workflowWaits as waits,
} from './schema.js';

/**
 * Serializes signals with each other (exclusive) and with suspensions that register waits
 * (shared), until the transaction ends: signal ids become visible in id order, and a signal
 * can't slip between a suspension's check for missed signals and its commit.
 */
const signalLock = sql`hashtext('@nestjs/workflows:signals')`;

const RUNNABLE = ['pending', 'running', 'suspended', 'compensating'] as const;
const CANCELLABLE = ['pending', 'running', 'suspended'] as const;

/** Our transactions run READ COMMITTED: a statement that waited for a lock sees what its holder committed. */
const READ_COMMITTED = { isolationLevel: 'read committed' } as const;

/** Workflow instances, journals, waits and signals in the app's PostgreSQL database, through Drizzle. */
@Injectable()
export class DrizzleWorkflowStore implements WorkflowStore {
  constructor(
    @InjectDrizzle() private readonly db: Database,
    storage: WorkflowStorage,
  ) {
    storage.registerSource(this);
  }

  // ---------------------------------------------------------------- instances

  create(instance: NewWorkflowInstance) {
    return this.insertInstance(this.db, instance);
  }

  /** `start(..., { transaction: tx })`: the instance commits or rolls back with the app's rows. */
  async createInTransaction(tx: Transaction, instance: NewWorkflowInstance) {
    return this.insertInstance(appTransaction(tx), instance);
  }

  async get(id: string, options: { journal?: boolean } = {}): Promise<WorkflowInstanceDetails | null> {
    const [instance] = await this.db.select().from(instances).where(eq(instances.id, id));
    if (!instance) {
      return null;
    }
    const waiting = await this.db
      .select({ signal: waits.signal, key: waits.key })
      .from(waits)
      .where(eq(waits.instanceId, id))
      .orderBy(asc(waits.position));
    if (!options.journal) {
      return { ...toInstance(instance), waits: waiting };
    }
    const entries = await this.db.select({ entry: journal.entry }).from(journal).where(eq(journal.instanceId, id)).orderBy(asc(journal.seq));
    return { ...toInstance(instance), waits: waiting, journal: entries.map((row) => row.entry) };
  }

  async list(query: WorkflowListQuery): Promise<WorkflowInstance[]> {
    const rows = await this.db
      .select()
      .from(instances)
      .where(
        and(
          query.status ? inArray(instances.status, query.status) : undefined,
          query.workflow !== undefined ? eq(instances.workflow, query.workflow) : undefined,
          query.version !== undefined ? eq(instances.version, query.version) : undefined,
          query.parentId !== undefined ? eq(instances.parentId, query.parentId) : undefined,
          query.scheduleId !== undefined ? eq(instances.scheduleId, query.scheduleId) : undefined,
        ),
      )
      .orderBy(asc(instances.createdAt), asc(instances.id))
      .limit(query.limit)
      .offset(query.offset);
    return rows.map(toInstance);
  }

  async requestCancel(id: string, { reason, now, terminate }: WorkflowCancelRequest): Promise<boolean> {
    // A terminate also stops a compensating instance, and follows a cancel.
    const applies = terminate
      ? and(inArray(instances.status, RUNNABLE), eq(instances.terminateRequested, false))
      : and(inArray(instances.status, CANCELLABLE), eq(instances.cancelRequested, false));
    const accepted = await this.db
      .update(instances)
      .set({
        cancelRequested: true,
        ...(terminate ? { terminateRequested: true } : {}),
        cancelReason: reason,
        updatedAt: now,
        wakeAt: sql`least(coalesce(${instances.wakeAt}, ${now}), ${now})`,
      })
      .where(and(eq(instances.id, id), applies))
      .returning({ id: instances.id });
    return accepted.length === 1;
  }

  /** `WorkflowClient.retry()`: one conditional update, then the journal, in one transaction. */
  reopen(id: string, reopen: WorkflowReopen): Promise<boolean> {
    return this.db.transaction(async (tx) => {
      // The update locks the row; of two concurrent retries, the second finds it changed.
      const reopened = await tx
        .update(instances)
        .set({ status: reopen.status, error: reopen.error, deadline: reopen.deadline, wakeAt: reopen.now, updatedAt: reopen.now })
        .where(
          and(
            eq(instances.id, id),
            isNull(instances.leaseToken),
            eq(instances.status, reopen.expect.status),
            eq(instances.runs, reopen.expect.runs),
          ),
        )
        .returning({ id: instances.id });
      if (reopened.length === 0) {
        return false;
      }

      if (reopen.entries.length > 0) {
        await this.upsertEntries(tx, id, reopen.entries);
      }
      return true;
    }, READ_COMMITTED);
  }

  /** `WorkflowClient.delete()`: its journal and waits go with it (ON DELETE CASCADE). */
  async delete(id: string, statuses: WorkflowStatus[]): Promise<boolean> {
    const deleted = await this.db
      .delete(instances)
      .where(and(eq(instances.id, id), inArray(instances.status, statuses)))
      .returning({ id: instances.id });
    return deleted.length === 1;
  }

  // ---------------------------------------------------------------- signals

  signal(signal: NewWorkflowSignal) {
    return this.db.transaction((tx) => this.insertSignal(tx, signal), READ_COMMITTED);
  }

  /** `signal(..., { transaction: tx })`: the signal and its wake-ups commit with the app's rows. */
  async signalInTransaction(tx: Transaction, signal: NewWorkflowSignal) {
    const { rows } = await appTransaction(tx).execute<{ isolation: string }>(sql`SELECT current_setting('transaction_isolation') AS isolation`);
    if (rows[0]?.isolation !== 'read committed') {
      throw new TypeError(`signal() with { transaction } needs a READ COMMITTED transaction; this one is ${rows[0]?.isolation}.`);
    }
    return this.insertSignal(tx, signal);
  }

  async signals(query: WorkflowSignalQuery): Promise<WorkflowSignalRecord[]> {
    return this.db
      .select({ id: signals.id, name: signals.name, key: signals.key, payload: signals.payload, createdAt: signals.createdAt })
      .from(signals)
      .where(and(eq(signals.name, query.name), keyIs(signals.key, query.key), gt(signals.id, query.afterId), lte(signals.id, query.upToId)))
      .orderBy(asc(signals.id));
  }

  // ---------------------------------------------------------------- retention

  async purge(query: WorkflowPurgeQuery): Promise<WorkflowPurgeResult> {
    const finished = and(inArray(instances.status, query.statuses), lt(instances.updatedAt, query.before));
    const oldest = this.db.select({ id: instances.id }).from(instances).where(finished).orderBy(asc(instances.updatedAt), asc(instances.id)).limit(query.limit);
    // `finished` again on the deleted rows: an instance reopened since the subquery read it stays.
    // Its journal and waits go with it (ON DELETE CASCADE).
    const purged = await this.db.delete(instances).where(and(inArray(instances.id, oldest), finished)).returning({ id: instances.id });

    // Signals no instance can take: at or below every unfinished instance's cursor (new ones start
    // at the newest signal), old enough, and never the newest, so the last signal id never goes back.
    const newest = sql`(SELECT max(${signals.id}) FROM ${signals})`;
    const floor = sql`coalesce((SELECT min(${instances.signalCursor}) FROM ${instances} WHERE ${inArray(instances.status, RUNNABLE)}), ${newest})`;
    const prunable = this.db
      .select({ id: signals.id })
      .from(signals)
      .where(and(lt(signals.createdAt, query.before), lt(signals.id, newest), lte(signals.id, floor)))
      .orderBy(asc(signals.id))
      .limit(query.limit);
    const pruned = await this.db.delete(signals).where(inArray(signals.id, prunable)).returning({ id: signals.id });

    // Rate-limit windows that ended: again on the deleted rows, as a claim may have opened a new one meanwhile. Rows a
    // claim is locking are skipped: waiting for them, in another order than the claim's, could deadlock.
    const ended = lt(rateLimits.windowEnd, query.before);
    const oldestWindows = this.db
      .select({ workflow: rateLimits.workflow, key: rateLimits.key })
      .from(rateLimits)
      .where(ended)
      .orderBy(asc(rateLimits.windowEnd), asc(rateLimits.workflow), asc(rateLimits.key))
      .limit(query.limit)
      .for('update', { skipLocked: true });
    const windows = await this.db
      .delete(rateLimits)
      .where(and(sql`(${rateLimits.workflow}, ${rateLimits.key}) IN ${oldestWindows}`, ended))
      .returning({ workflow: rateLimits.workflow });

    return { instances: purged.length, signals: pruned.length, rateLimits: windows.length };
  }

  // ---------------------------------------------------------------- schedules

  async saveSchedule(save: WorkflowScheduleSave): Promise<WorkflowScheduleRecord | null> {
    const { id, workflow, declared, spec, input, paused, wakeAt, state, now } = save;
    const fields = { workflow, declared, spec, input, paused, wakeAt, state, updatedAt: now };
    if (save.expectRevision === null) {
      const [created] = await this.db.insert(schedules).values({ id, ...fields, revision: 1, createdAt: now }).onConflictDoNothing().returning();
      return created ? toSchedule(created) : null;
    }

    // One conditional update: of two saves that read the same revision, the second finds it changed.
    const [saved] = await this.db
      .update(schedules)
      .set({ ...fields, revision: sql`${schedules.revision} + 1`, ...(save.releaseLease ? { leaseToken: null, leaseUntil: null } : {}) })
      .where(and(eq(schedules.id, id), eq(schedules.revision, save.expectRevision)))
      .returning();
    return saved ? toSchedule(saved) : null;
  }

  async getSchedule(id: string): Promise<WorkflowScheduleRecord | null> {
    const [row] = await this.db.select().from(schedules).where(eq(schedules.id, id));
    return row ? toSchedule(row) : null;
  }

  async listSchedules(query: WorkflowScheduleQuery): Promise<WorkflowScheduleRecord[]> {
    const rows = await this.db
      .select()
      .from(schedules)
      .where(
        and(
          query.workflow !== undefined ? eq(schedules.workflow, query.workflow) : undefined,
          query.declared !== undefined ? eq(schedules.declared, query.declared) : undefined,
        ),
      )
      .orderBy(asc(schedules.id))
      .limit(query.limit)
      .offset(query.offset);
    return rows.map(toSchedule);
  }

  async deleteSchedule(id: string, revision?: number): Promise<boolean> {
    const deleted = await this.db
      .delete(schedules)
      .where(and(eq(schedules.id, id), revision !== undefined ? eq(schedules.revision, revision) : undefined))
      .returning({ id: schedules.id });
    return deleted.length === 1;
  }

  async claimSchedules(request: WorkflowScheduleClaimRequest): Promise<WorkflowScheduleRecord[]> {
    const { now } = request;
    // Locked; schedules another claim is locking right now are skipped instead of waited for.
    const due = this.db
      .select({ id: schedules.id })
      .from(schedules)
      .where(
        and(
          eq(schedules.paused, false),
          isNotNull(schedules.wakeAt),
          lte(schedules.wakeAt, now),
          or(isNull(schedules.leaseUntil), lt(schedules.leaseUntil, now)),
          inArray(schedules.workflow, request.workflows),
        ),
      )
      .orderBy(asc(schedules.wakeAt), asc(schedules.id))
      .limit(request.limit)
      .for('update', { skipLocked: true });
    const claimed = await this.db
      .update(schedules)
      .set({ leaseToken: request.token, leaseOwner: request.owner, leaseUntil: request.leaseUntil })
      .where(inArray(schedules.id, due))
      .returning();
    claimed.sort((a, b) => a.wakeAt! - b.wakeAt! || (a.id < b.id ? -1 : 1));
    return claimed.map(toSchedule);
  }

  async writeSchedule(id: string, token: string, write: WorkflowScheduleWrite): Promise<boolean> {
    const written = await this.db
      .update(schedules)
      .set({
        state: write.state,
        wakeAt: write.wakeAt,
        revision: sql`${schedules.revision} + 1`,
        updatedAt: write.now,
        ...(write.release ? { leaseToken: null, leaseUntil: null } : {}),
      })
      .where(and(eq(schedules.id, id), eq(schedules.leaseToken, token)))
      .returning({ id: schedules.id });
    return written.length === 1;
  }

  // ---------------------------------------------------------------- the worker

  async claim(request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    const { now } = request;
    // Due, unleased instances of the versions this worker runs.
    const isDue = and(
      isNotNull(instances.wakeAt),
      lte(instances.wakeAt, now),
      inArray(instances.status, RUNNABLE),
      or(isNull(instances.leaseUntil), lt(instances.leaseUntil, now)),
      or(...request.workflows.map((w) => and(eq(instances.workflow, w.name), eq(instances.version, w.version)))),
    );
    if (request.limits?.length || request.rateLimits?.length) {
      return this.db.transaction((tx) => this.claimWithin(tx, request, isDue!), READ_COMMITTED);
    }

    // Locked; rows another claim is locking right now are skipped instead of waited for.
    const due = this.db
      .select({ id: instances.id })
      .from(instances)
      .where(isDue)
      .orderBy(asc(instances.priority), asc(instances.wakeAt), asc(instances.createdAt), asc(instances.id))
      .limit(request.limit)
      .for('update', { skipLocked: true });
    return { instances: await this.lease(this.db, request, due), lastSignalId: await this.lastSignalId(this.db) };
  }

  /**
   * A claim under concurrency or rate limits. Claims of a workflow with a concurrency limit take its lock first
   * (in name order, so two claims never wait for each other's), so they count the slots live leases hold and
   * lease the instances that fit one after the other: two never both take the last slot. Rate-limit windows are
   * counted again under their rows' locks, once the instances are picked (see `takeRoom()`).
   */
  private async claimWithin(tx: Transaction, request: WorkflowClaimRequest, isDue: SQL): Promise<WorkflowClaim> {
    const limits = request.limits ?? [];
    const rates = request.rateLimits ?? [];
    for (const workflow of [...new Set(limits.map((limit) => limit.workflow))].sort()) {
      await tx.execute(sql`SELECT pg_advisory_xact_lock(hashtext(${`@nestjs/workflows:concurrency:${workflow}`}))`);
    }

    // Candidates with no room at all are passed over; then each concurrency key's first, as many as its free slots;
    // of those, each rate key's, as many as its window has room for; of those, each workflow's, as many as both its
    // free slots and its window allow. A full key is passed over, not waited behind. The windows read here are a
    // snapshot, re-counted under their locks in takeRoom().
    const { now } = request;
    const { rows } = await tx.execute<{ id: string }>(sql`
      WITH limits AS (
        SELECT * FROM jsonb_to_recordset(${JSON.stringify(limits.map((l) => ({ workflow: l.workflow, total: l.limit, per_key: l.perKey })))}::jsonb)
          AS l(workflow text, total int, per_key int)
      ),
      rates AS (
        SELECT * FROM jsonb_to_recordset(${JSON.stringify(rates.map((r) => ({ workflow: r.workflow, total: r.limit?.max ?? null, per_key: r.perKey?.max ?? null })))}::jsonb)
          AS r(workflow text, total int, per_key int)
      ),
      held AS (
        SELECT ${instances.workflow} AS workflow, ${instances.concurrencyKey} AS key, count(*)::int AS n
        FROM ${instances}
        WHERE ${instances.leaseUntil} >= ${now} AND ${instances.workflow} IN (SELECT workflow FROM limits)
        GROUP BY 1, 2
      ),
      used AS (
        SELECT ${rateLimits.workflow} AS workflow, ${rateLimits.key} AS key, ${rateLimits.count} AS n
        FROM ${rateLimits}
        WHERE ${rateLimits.windowEnd} > ${now} AND ${rateLimits.workflow} IN (SELECT workflow FROM rates)
      ),
      due AS (
        SELECT ${instances.id} AS id, ${instances.workflow} AS workflow, ${instances.concurrencyKey} AS key,
          ${instances.rateLimitKey} AS rate_key, ${instances.priority} AS priority, ${instances.wakeAt} AS wake_at,
          ${instances.createdAt} AS created_at,
          row_number() OVER (
            PARTITION BY ${instances.workflow}, ${instances.concurrencyKey}
            ORDER BY ${instances.priority}, ${instances.wakeAt}, ${instances.createdAt}, ${instances.id}
          ) AS key_rank
        FROM ${instances}
        LEFT JOIN limits l ON l.workflow = ${instances.workflow}
        LEFT JOIN rates r ON r.workflow = ${instances.workflow}
        WHERE ${isDue}
          AND (l.total IS NULL OR coalesce((SELECT sum(n) FROM held h WHERE h.workflow = l.workflow), 0) < l.total)
          AND (l.per_key IS NULL OR ${instances.concurrencyKey} IS NULL
            OR coalesce((SELECT n FROM held h WHERE h.workflow = l.workflow AND h.key = ${instances.concurrencyKey}), 0) < l.per_key)
          AND (r.total IS NULL OR coalesce((SELECT n FROM used u WHERE u.workflow = r.workflow AND u.key = ''), 0) < r.total)
          AND (r.per_key IS NULL OR ${instances.rateLimitKey} IS NULL
            OR coalesce((SELECT n FROM used u WHERE u.workflow = r.workflow AND u.key = ${instances.rateLimitKey}), 0) < r.per_key)
      ),
      fits_key AS (
        SELECT d.*, row_number() OVER (PARTITION BY d.workflow, d.rate_key ORDER BY d.priority, d.wake_at, d.created_at, d.id) AS rate_key_rank
        FROM due d
        LEFT JOIN limits l ON l.workflow = d.workflow
        WHERE l.per_key IS NULL OR d.key IS NULL
          OR d.key_rank <= l.per_key - coalesce((SELECT n FROM held h WHERE h.workflow = d.workflow AND h.key = d.key), 0)
      ),
      fits_rate_key AS (
        SELECT f.*, row_number() OVER (PARTITION BY f.workflow ORDER BY f.priority, f.wake_at, f.created_at, f.id) AS workflow_rank
        FROM fits_key f
        LEFT JOIN rates r ON r.workflow = f.workflow
        WHERE r.per_key IS NULL OR f.rate_key IS NULL
          OR f.rate_key_rank <= r.per_key - coalesce((SELECT n FROM used u WHERE u.workflow = f.workflow AND u.key = f.rate_key), 0)
      )
      SELECT f.id FROM fits_rate_key f
      LEFT JOIN limits l ON l.workflow = f.workflow
      LEFT JOIN rates r ON r.workflow = f.workflow
      WHERE (l.total IS NULL OR f.workflow_rank <= l.total - coalesce((SELECT sum(n)::int FROM held h WHERE h.workflow = f.workflow), 0))
        AND (r.total IS NULL OR f.workflow_rank <= r.total - coalesce((SELECT n FROM used u WHERE u.workflow = f.workflow AND u.key = ''), 0))
      ORDER BY f.priority, f.wake_at, f.created_at, f.id
      LIMIT ${request.limit}
    `);
    if (rows.length === 0) {
      return { instances: [], lastSignalId: await this.lastSignalId(tx) };
    }

    const picked = await tx
      .select({ id: instances.id, workflow: instances.workflow, rateLimitKey: instances.rateLimitKey })
      .from(instances)
      .where(and(inArray(instances.id, rows.map((row) => row.id)), isDue))
      .orderBy(asc(instances.priority), asc(instances.wakeAt), asc(instances.createdAt), asc(instances.id))
      .for('update', { skipLocked: true });
    const granted = await this.takeRoom(tx, request, picked);
    if (granted.length === 0) {
      return { instances: [], lastSignalId: await this.lastSignalId(tx) };
    }
    return { instances: await this.lease(tx, request, tx.select({ id: instances.id }).from(instances).where(inArray(instances.id, granted))), lastSignalId: await this.lastSignalId(tx) };
  }

  /**
   * Of `picked` (in claim order), the instances their rate-limit windows have room for, recorded in the windows.
   * Each window's row is inserted or locked first, in a fixed order: the count read under the lock is exact (a
   * concurrent claim of the same window waits for this one), and a purge can't delete the row in between.
   */
  private async takeRoom(tx: Transaction, request: WorkflowClaimRequest, picked: Array<{ id: string; workflow: string; rateLimitKey: string | null }>): Promise<string[]> {
    const rules = new Map((request.rateLimits ?? []).map((rule) => [rule.workflow, rule]));
    const windowsOf = ({ workflow, rateLimitKey }: (typeof picked)[number]) => {
      const rule = rules.get(workflow);
      return [
        ...(rule?.limit ? [{ workflow, key: '', ...rule.limit }] : []),
        ...(rule?.perKey && rateLimitKey !== null ? [{ workflow, key: rateLimitKey, ...rule.perKey }] : []),
      ];
    };
    const named = new Map(picked.flatMap(windowsOf).map((window) => [JSON.stringify([window.workflow, window.key]), window]));
    if (named.size === 0) {
      return picked.map((instance) => instance.id);
    }

    const { now } = request;
    const order = [...named.values()].sort((a, b) => (a.workflow < b.workflow ? -1 : a.workflow > b.workflow ? 1 : a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    const locked = await tx
      .insert(rateLimits)
      .values(order.map(({ workflow, key }) => ({ workflow, key, windowEnd: 0, count: 0 })))
      .onConflictDoUpdate({ target: [rateLimits.workflow, rateLimits.key], set: { count: sql`${rateLimits.count}` } })
      .returning();
    const open = new Map(locked.map((row) => [JSON.stringify([row.workflow, row.key]), row.windowEnd > now ? { windowEnd: row.windowEnd, count: row.count } : null]));

    const granted: string[] = [];
    const changed = new Map<string, { workflow: string; key: string; windowEnd: number; count: number }>();
    for (const instance of picked) {
      const windows = windowsOf(instance);
      if (!windows.every((window) => (open.get(JSON.stringify([window.workflow, window.key]))?.count ?? 0) < window.max)) {
        continue;
      }

      for (const window of windows) {
        const name = JSON.stringify([window.workflow, window.key]);
        const current = open.get(name) ?? { windowEnd: now + window.duration, count: 0 };
        current.count++;
        open.set(name, current);
        changed.set(name, { workflow: window.workflow, key: window.key, ...current });
      }
      granted.push(instance.id);
    }

    if (changed.size > 0) {
      await tx.execute(sql`
        UPDATE ${rateLimits} SET window_end = v.window_end, count = v.count
        FROM jsonb_to_recordset(${JSON.stringify([...changed.values()].map((w) => ({ workflow: w.workflow, key: w.key, window_end: w.windowEnd, count: w.count })))}::jsonb)
          AS v(workflow text, key text, window_end bigint, count int)
        WHERE ${rateLimits.workflow} = v.workflow AND ${rateLimits.key} = v.key
      `);
    }
    return granted;
  }

  /** Leases the instances `due` selects (and locks). */
  private async lease(db: Database | Transaction, request: WorkflowClaimRequest, due: SQLWrapper): Promise<WorkflowInstance[]> {
    const { now } = request;
    const claimed = await db
      .update(instances)
      .set({
        leaseToken: request.token,
        leaseOwner: request.owner,
        leaseUntil: request.leaseUntil,
        runs: sql`${instances.runs} + 1`,
        updatedAt: now,
        status: sql`CASE WHEN ${instances.status} = 'compensating' THEN ${instances.status} ELSE 'running' END`,
      })
      .where(inArray(instances.id, due))
      .returning();
    claimed.sort((a, b) => a.priority - b.priority || a.wakeAt! - b.wakeAt! || a.createdAt - b.createdAt || (a.id < b.id ? -1 : 1));
    return claimed.map(toInstance);
  }

  async renew(id: string, token: string, leaseUntil: number) {
    const [row] = await this.db
      .update(instances)
      .set({ leaseUntil })
      .where(and(eq(instances.id, id), eq(instances.leaseToken, token)))
      .returning({ cancelRequested: instances.cancelRequested, terminateRequested: instances.terminateRequested });
    return row ?? null;
  }

  write(id: string, token: string, write: WorkflowWrite): Promise<boolean> {
    return this.db.transaction(async (tx) => {
      const release = write.release;
      if (write.signal) {
        await tx.execute(sql`SELECT pg_advisory_xact_lock(${signalLock})`);
      } else if (release && release.waits.length > 0) {
        await tx.execute(sql`SELECT pg_advisory_xact_lock_shared(${signalLock})`);
      }
      // The fence: only the lease holder writes, and the row stays locked until commit.
      const [fenced] = await tx
        .select({ cancelRequested: instances.cancelRequested })
        .from(instances)
        .where(and(eq(instances.id, id), eq(instances.leaseToken, token)))
        .for('update');
      if (!fenced) {
        return false;
      }

      if (write.entries.length > 0) {
        await this.upsertEntries(tx, id, write.entries);
      }
      if (write.signal) {
        await this.insertSignal(tx, write.signal);
      }

      let handBack = {};
      if (release) {
        await tx.delete(waits).where(eq(waits.instanceId, id));
        let missed = false;
        if (release.waits.length > 0) {
          await tx.insert(waits).values(release.waits.map((wait, position) => ({ instanceId: id, position, signal: wait.signal, key: wait.key })));
          // A signal committed after the execution read its cursor: stay due instead of losing it.
          const [hit] = await tx
            .select({ id: signals.id })
            .from(signals)
            .where(and(gt(signals.id, release.signalCursor), or(...release.waits.map((wait) => and(eq(signals.name, wait.signal), keyIs(signals.key, wait.key))))))
            .limit(1);
          missed = hit !== undefined;
        }
        const wakeNow = missed || (fenced.cancelRequested && write.status === 'suspended');
        handBack = { leaseToken: null, leaseUntil: null, wakeAt: wakeNow ? write.now : release.wakeAt };
      }

      // Drizzle skips undefined values: fields the write leaves out keep their value.
      const changes = { status: write.status, output: write.output, error: write.error, customStatus: write.customStatus, ...handBack };
      if (Object.values(changes).some((value) => value !== undefined)) {
        await tx.update(instances).set({ ...changes, updatedAt: write.now }).where(eq(instances.id, id));
      }
      return true;
    }, READ_COMMITTED);
  }

  // ---------------------------------------------------------------- internals

  private async insertInstance(db: Database | Transaction, i: NewWorkflowInstance) {
    const [created] = await db
      .insert(instances)
      .values({
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
        input: i.input,
        deadline: i.deadline,
        wakeAt: i.now,
        // Signals sent from now on can match the new instance's waits.
        signalCursor: sql`(SELECT coalesce(max(${signals.id}), 0) FROM ${signals})`,
        createdAt: i.now,
        updatedAt: i.now,
      })
      .onConflictDoNothing()
      .returning();
    if (created) {
      return { instance: toInstance(created), created: true };
    }
    const [existing] = await db.select().from(instances).where(eq(instances.id, i.id));
    return { instance: toInstance(existing!), created: false };
  }

  private async insertSignal(tx: Transaction, s: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    await tx.execute(sql`SELECT pg_advisory_xact_lock(${signalLock})`);
    // A name and dedupe id stored before make this a no-op: the unique constraint decides,
    // with nothing to catch, because an error would abort the app's transaction.
    const [signal] = await tx
      .insert(signals)
      .values({ name: s.name, key: s.key, dedupeId: s.dedupeId, payload: s.payload, createdAt: s.now })
      .onConflictDoNothing({ target: [signals.name, signals.dedupeId] })
      .returning({ id: signals.id });
    if (!signal) {
      const [earlier] = await tx
        .select({ id: signals.id, key: signals.key })
        .from(signals)
        .where(and(eq(signals.name, s.name), eq(signals.dedupeId, s.dedupeId!)));
      return { id: earlier!.id, woken: 0, created: false, key: earlier!.key };
    }

    const waiting = tx
      .select({ id: waits.instanceId })
      .from(waits)
      .where(and(eq(waits.signal, s.name), keyIs(waits.key, s.key)));
    const woken = await tx
      .update(instances)
      .set({ wakeAt: s.now, updatedAt: s.now })
      .where(
        and(
          eq(instances.status, 'suspended'),
          or(isNull(instances.wakeAt), gt(instances.wakeAt, s.now)),
          inArray(instances.id, waiting),
        ),
      )
      .returning({ id: instances.id });
    return { id: signal.id, woken: woken.length, created: true, key: s.key };
  }

  /** Journal entries by name: a new name goes last (`seq`), a known one is replaced in place. */
  private async upsertEntries(tx: Transaction, id: string, entries: WorkflowJournalEntry[]) {
    await tx
      .insert(journal)
      .values(entries.map((entry) => ({ instanceId: id, name: entry.name, entry })))
      .onConflictDoUpdate({ target: [journal.instanceId, journal.name], set: { entry: sql`excluded.entry` } });
  }

  private async lastSignalId(db: Database | Transaction) {
    const [row] = await db.select({ id: max(signals.id) }).from(signals);
    return row?.id ?? 0;
  }
}

/** Keys match exactly: `null` only matches a signal sent without one. */
function keyIs(column: Column, key: string | null): SQL {
  return key === null ? isNull(column) : eq(column, key);
}

/** The app's `tx`, never the root database: a write outside the transaction would commit without it. */
function appTransaction(tx: Transaction): Transaction {
  if (!(tx instanceof PgTransaction)) {
    throw new TypeError('Pass the transaction your db.transaction() callback receives (its tx), not the database.');
  }
  return tx;
}

function toInstance(row: typeof instances.$inferSelect): WorkflowInstance {
  const { leaseToken: _token, ...instance } = row;
  return instance;
}

function toSchedule(row: typeof schedules.$inferSelect): WorkflowScheduleRecord {
  const { leaseToken: _token, ...schedule } = row;
  return schedule;
}
