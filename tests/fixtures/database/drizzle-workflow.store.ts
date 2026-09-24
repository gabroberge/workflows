import { Injectable } from '@nestjs/common';
import { InjectDrizzle } from '@nestjs/drizzle';
import {
  WorkflowStorage,
  type NewWorkflowInstance,
  type NewWorkflowSignal,
  type WorkflowClaim,
  type WorkflowClaimRequest,
  type WorkflowInstance,
  type WorkflowInstanceDetails,
  type WorkflowListQuery,
  type WorkflowSignalQuery,
  type WorkflowSignalRecord,
  type WorkflowStore,
  type WorkflowWrite,
} from '../../../lib/index.js';
import { and, asc, eq, gt, inArray, isNotNull, isNull, lt, lte, max, or, sql, type Column, type SQL } from 'drizzle-orm';
import { PgTransaction } from 'drizzle-orm/pg-core';
import type { Database, Transaction } from './drizzle.js';
import { workflowInstances as instances, workflowJournal as journal, workflowSignals as signals, workflowWaits as waits } from './schema.js';

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
    if (!instance) return null;
    const waiting = await this.db
      .select({ signal: waits.signal, key: waits.key })
      .from(waits)
      .where(eq(waits.instanceId, id))
      .orderBy(asc(waits.position));
    if (!options.journal) return { ...toInstance(instance), waits: waiting };
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
        ),
      )
      .orderBy(asc(instances.createdAt), asc(instances.id))
      .limit(query.limit)
      .offset(query.offset);
    return rows.map(toInstance);
  }

  async requestCancel(id: string, reason: string | null, now: number): Promise<boolean> {
    const accepted = await this.db
      .update(instances)
      .set({ cancelRequested: true, cancelReason: reason, updatedAt: now, wakeAt: sql`least(coalesce(${instances.wakeAt}, ${now}), ${now})` })
      .where(and(eq(instances.id, id), inArray(instances.status, CANCELLABLE), eq(instances.cancelRequested, false)))
      .returning({ id: instances.id });
    return accepted.length === 1;
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
      .select()
      .from(signals)
      .where(and(eq(signals.name, query.name), keyIs(signals.key, query.key), gt(signals.id, query.afterId), lte(signals.id, query.upToId)))
      .orderBy(asc(signals.id));
  }

  // ---------------------------------------------------------------- the worker

  async claim(request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    const { now } = request;
    // Due, unleased instances of the versions this worker runs, locked; rows another claim is
    // locking right now are skipped instead of waited for.
    const due = this.db
      .select({ id: instances.id })
      .from(instances)
      .where(
        and(
          isNotNull(instances.wakeAt),
          lte(instances.wakeAt, now),
          inArray(instances.status, RUNNABLE),
          or(isNull(instances.leaseUntil), lt(instances.leaseUntil, now)),
          or(...request.workflows.map((w) => and(eq(instances.workflow, w.name), eq(instances.version, w.version)))),
        ),
      )
      .orderBy(asc(instances.wakeAt), asc(instances.createdAt), asc(instances.id))
      .limit(request.limit)
      .for('update', { skipLocked: true });
    const claimed = await this.db
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
    claimed.sort((a, b) => a.wakeAt! - b.wakeAt! || a.createdAt - b.createdAt || (a.id < b.id ? -1 : 1));
    return { instances: claimed.map(toInstance), lastSignalId: await this.lastSignalId(this.db) };
  }

  async renew(id: string, token: string, leaseUntil: number) {
    const [row] = await this.db
      .update(instances)
      .set({ leaseUntil })
      .where(and(eq(instances.id, id), eq(instances.leaseToken, token)))
      .returning({ cancelRequested: instances.cancelRequested });
    return row ?? null;
  }

  write(id: string, token: string, write: WorkflowWrite): Promise<boolean> {
    return this.db.transaction(async (tx) => {
      const release = write.release;
      if (release && release.waits.length > 0) await tx.execute(sql`SELECT pg_advisory_xact_lock_shared(${signalLock})`);
      // The fence: only the lease holder writes, and the row stays locked until commit.
      const [fenced] = await tx
        .select({ cancelRequested: instances.cancelRequested })
        .from(instances)
        .where(and(eq(instances.id, id), eq(instances.leaseToken, token)))
        .for('update');
      if (!fenced) return false;

      if (write.entries.length > 0) {
        await tx
          .insert(journal)
          .values(write.entries.map((entry) => ({ instanceId: id, name: entry.name, entry })))
          .onConflictDoUpdate({ target: [journal.instanceId, journal.name], set: { entry: sql`excluded.entry` } });
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
      const changes = { status: write.status, output: write.output, error: write.error, ...handBack };
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
        status: 'pending',
        input: i.input,
        wakeAt: i.now,
        // Signals sent from now on can match the new instance's waits.
        signalCursor: sql`(SELECT coalesce(max(${signals.id}), 0) FROM ${signals})`,
        createdAt: i.now,
        updatedAt: i.now,
      })
      .onConflictDoNothing()
      .returning();
    if (created) return { instance: toInstance(created), created: true };
    const [existing] = await db.select().from(instances).where(eq(instances.id, i.id));
    return { instance: toInstance(existing!), created: false };
  }

  private async insertSignal(tx: Transaction, s: NewWorkflowSignal) {
    await tx.execute(sql`SELECT pg_advisory_xact_lock(${signalLock})`);
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
    const [signal] = await tx
      .insert(signals)
      .values({ name: s.name, key: s.key, payload: s.payload, createdAt: s.now })
      .returning({ id: signals.id });
    return { id: signal!.id, woken: woken.length };
  }

  private async lastSignalId(db: Database) {
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
