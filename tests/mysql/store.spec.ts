/**
 * What MySqlWorkflowStore does on MySQL beyond the contract's cases, through every executor:
 *
 * - no foreign keys: `delete()` and `purge()` take an instance's journal and waits with it in their own transaction, so
 *   no row is left behind and a recreated id starts with an empty journal;
 * - claims under a concurrency limit take the workflow's lock before counting: a claim that ran while another was
 *   leasing, and saw a new, more urgent instance, never leases past the limit (SKIP LOCKED alone lets it through: the
 *   two claims lease different rows);
 * - a claim reaches past the rows another claim is taking: its rounds of candidates widen by the rows it found held;
 * - the kit's lock table keeps one row per lock key the store takes, a bounded few, however much work goes through it,
 *   and the store creates the signal lock's row at startup, in a transaction of its own: had the application's
 *   transaction created it and rolled back, the signals waiting for it would deadlock.
 */
import { createHash } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { mysqlErrorCode } from '@nestjs/store-kit/mysql';
import mysql from 'mysql2/promise';
import { MySqlWorkflowStore, type SqlExecutor } from '../../lib/mysql/index.js';
import { clients, onMysql, testDatabase, truncate, type Client } from './support.js';

const { database, reason } = await testDatabase('mysql_store');
const FAR = 9_000_000_000_000;
const W = { name: 'order-fulfilment', version: 1 };
const release = { wakeAt: FAR, waits: [] as Array<{ signal: string; key: string | null }>, signalCursor: 0 };
const entry = (name: string) => ({ name, kind: 'step' as const, status: 'completed' as const, attempts: 1, updatedAt: 1 });

const admin = async (sql: string, params: unknown[] = []) => (await database!.admin.query<mysql.RowDataPacket[]>(sql, params))[0] as Array<Record<string, any>>;

describe.each(clients)('MySqlWorkflowStore through $name on MySQL', (factory) => {
  let client: Client | null = null;
  let store: MySqlWorkflowStore;
  const schema = `st_${clients.indexOf(factory)}`;
  beforeAll(async () => {
    if (database) {
      client = await factory.open(database.url);
      store = new MySqlWorkflowStore({ executor: client.executor, schema });
      await store.migrate();
    }
  });
  afterAll(() => client?.close());
  onMysql(reason);
  beforeEach(async () => {
    if (client) {
      await truncate(client.executor, schema);
    }
  });

  const create = (id: string, extra: Partial<Parameters<MySqlWorkflowStore['create']>[0]> = {}) =>
    store.create({ id, workflow: W.name, version: 1, input: null, deadline: null, now: 0, ...extra });
  const children = async (id: string) => ({
    journal: Number((await admin(`SELECT COUNT(*) AS n FROM ${schema}_journal WHERE instance_id = ?`, [id]))[0]!.n),
    waits: Number((await admin(`SELECT COUNT(*) AS n FROM ${schema}_waits WHERE instance_id = ?`, [id]))[0]!.n),
  });

  it('deletes an instance with its journal and waits, and purges them with it: a recreated id starts empty', async () => {
    for (const id of ['deleted', 'purged', 'kept']) {
      await create(id);
    }
    await store.claim({ owner: 'w', token: 't', now: 0, leaseUntil: FAR, limit: 10, workflows: [W] });
    for (const id of ['deleted', 'kept']) {
      await store.write(id, 't', { now: 1, entries: [entry('a'), entry('b')], status: 'suspended', release: { ...release, waits: [{ signal: 's', key: id }] } });
    }
    await store.write('purged', 't', { now: 1, entries: [entry('a')], status: 'completed', output: 1, error: null, release });
    expect(await children('deleted')).toEqual({ journal: 2, waits: 1 });

    expect(await store.delete('deleted', ['suspended'])).toBe(true);
    expect(await children('deleted')).toEqual({ journal: 0, waits: 0 });
    expect(await store.purge({ statuses: ['completed'], before: FAR, limit: 10 })).toMatchObject({ instances: 1 });
    expect(await children('purged')).toEqual({ journal: 0, waits: 0 });
    expect(await children('kept')).toEqual({ journal: 2, waits: 1 });

    await create('deleted');
    expect(await store.get('deleted', { journal: true })).toMatchObject({ status: 'pending', journal: [], waits: [] });
  });

  it("never leases past a workflow's limit when a claim runs while another is leasing, and sees a more urgent instance", async () => {
    await create('a', { priority: 5 });
    await create('b', { priority: 5 });
    const limits = [{ workflow: W.name, limit: 2, perKey: null }];
    const claim = (s: MySqlWorkflowStore, token: string) => s.claim({ owner: token, token, now: 10, leaseUntil: FAR, limit: 2, workflows: [W], limits });

    // The first claim stops after it picked its instances (a, b), before it leases them.
    const picked = deferred();
    const proceed = deferred();
    const paused = new MySqlWorkflowStore({ executor: pausing(client!.executor, (text) => text.startsWith('WITH limits AS'), picked.resolve, proceed.promise), schema, migrate: false });
    const first = claim(paused, 'first');
    await picked.promise;

    // Meanwhile a more urgent instance arrives, and another claim runs: it waits for the workflow's lock.
    await create('urgent', { priority: 0 });
    const second = claim(store, 'second');
    const settled = await Promise.race([second.then(() => 'second done'), sleep(1_000).then(() => 'second waiting')]);
    proceed.resolve();

    expect((await first).instances.map((instance) => instance.id)).toEqual(['a', 'b']);
    expect((await second).instances).toEqual([]);
    expect(settled).toBe('second waiting');
    expect((await store.list({ limit: 10, offset: 0 })).filter((instance) => instance.leaseUntil === FAR).map((instance) => instance.id)).toEqual(['a', 'b']);
  });

  it('claims past the rows another claim is taking: one of one, while ten are held', async () => {
    for (let i = 0; i < 20; i++) {
      await create(`i${String(i).padStart(2, '0')}`, { now: i });
    }
    const claim = (s: MySqlWorkflowStore, token: string, limit: number) => s.claim({ owner: token, token, now: 100, leaseUntil: FAR, limit, workflows: [W] });

    // The first claim stops once it has locked its ten, before it leases them.
    const locked = deferred();
    const proceed = deferred();
    const lockedTen = (text: string) => text.includes('FORCE INDEX (PRIMARY)') && text.includes('FOR UPDATE SKIP LOCKED');
    const paused = new MySqlWorkflowStore({ executor: pausing(client!.executor, lockedTen, locked.resolve, proceed.promise), schema, migrate: false });
    const first = claim(paused, 'first', 10);
    await locked.promise;
    try {
      expect((await claim(store, 'second', 1)).instances.map((instance) => instance.id)).toEqual(['i10']);
    } finally {
      proceed.resolve();
    }
    expect((await first).instances.map((instance) => instance.id)).toEqual(Array.from({ length: 10 }, (_, i) => `i0${i}`));
  });

  it("lets signals waiting for one whose transaction rolls back go on, the signal lock's row created at startup", async () => {
    const fresh = new MySqlWorkflowStore({ executor: client!.executor, schema: `${schema}_fresh` });
    await fresh.onModuleInit();
    const signal = (key: string) => ({ name: 's', key, dedupeId: null, payload: null, now: 1 });

    // The application's transaction takes the signal lock, and rolls back while others wait for it.
    const holding = deferred();
    const rollBack = deferred();
    const first = client!.transaction(async (tx) => {
      await fresh.signalInTransaction(tx, signal('first'));
      holding.resolve();
      await rollBack.promise;
      throw new Error('rolled back');
    });
    await holding.promise;
    const waiting = ['a', 'b', 'c', 'd'].map((key) => client!.transaction((tx) => fresh.signalInTransaction(tx, signal(key))));
    await sleep(300);
    rollBack.resolve();

    await expect(first).rejects.toThrow('rolled back');
    const outcomes = await Promise.allSettled(waiting);
    expect(outcomes.flatMap((outcome) => (outcome.status === 'rejected' ? [mysqlErrorCode(outcome.reason) ?? outcome.reason] : []))).toEqual([]);
    expect((await fresh.signals({ name: 's', key: 'a', afterId: 0, upToId: FAR })).length).toBe(1);
    expect((await fresh.signals({ name: 's', key: 'first', afterId: 0, upToId: FAR })).length).toBe(0);
  });

  it("starts while a signal in the application's transaction holds the signal lock, not waiting for it", async () => {
    const holding = deferred();
    const commit = deferred();
    const signalling = client!.transaction(async (tx) => {
      await store.signalInTransaction(tx, { name: 's', key: 'held', dedupeId: null, payload: null, now: 1 });
      holding.resolve();
      await commit.promise;
    });
    await holding.promise;
    try {
      // Another process starts on the same schema: its lock row is there, and only noted.
      const starting = new MySqlWorkflowStore({ executor: client!.executor, schema, migrate: false });
      const started = await Promise.race([starting.onModuleInit().then(() => 'started'), sleep(1_000).then(() => 'waiting')]);
      expect(started).toBe('started');
    } finally {
      commit.resolve();
      await signalling;
    }
  });

  it('keeps one lock row per lock key: the signals lock, and a concurrency lock per workflow with a limit', async () => {
    await admin(`DELETE FROM ${schema}_locks`);
    const workflows = ['w1', 'w2', 'w3'].map((name) => ({ name, version: 1 }));
    for (let i = 0; i < 45; i++) {
      await store.create({ id: `i${i}`, workflow: workflows[i % 3]!.name, version: 1, input: null, deadline: null, concurrencyKey: `c${i}`, rateLimitKey: `r${i}`, now: i });
    }
    for (let round = 0; round < 5; round++) {
      const { instances } = await store.claim({
        owner: 'w',
        token: `t${round}`,
        now: 1_000 + round,
        leaseUntil: FAR,
        limit: 9,
        workflows,
        limits: workflows.map((w) => ({ workflow: w.name, limit: 50, perKey: 1 })),
        rateLimits: workflows.map((w) => ({ workflow: w.name, limit: null, perKey: { max: 1, duration: 10 } })),
      });
      expect(instances).toHaveLength(9);
      for (const [j, instance] of instances.entries()) {
        const signal = j % 3 === 0 ? { name: 'child.ended', key: instance.id, dedupeId: instance.id, payload: null, now: 2_000 } : undefined;
        await store.write(instance.id, `t${round}`, { now: 2_000, entries: [], status: 'suspended', signal, release: { ...release, waits: [{ signal: 'go', key: instance.id }] } });
      }
    }
    for (let i = 0; i < 30; i++) {
      expect(await store.signal({ name: 'go', key: `i${i}`, dedupeId: `evt-${i}`, payload: i, now: 3_000 })).toMatchObject({ created: true, woken: 1 });
    }
    expect(await store.delete('i0', ['suspended', 'pending'])).toBe(true);
    await store.purge({ statuses: ['completed'], before: FAR, limit: 100 });

    const sha = (key: string) => createHash('sha256').update(key).digest('hex');
    const rows = (await admin(`SELECT id FROM ${schema}_locks ORDER BY id`)).map((row) => row.id);
    expect(rows).toEqual(['signals', 'concurrency:w1', 'concurrency:w2', 'concurrency:w3'].map(sha).sort());
  });
});

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((settle) => (resolve = settle));
  return { promise, resolve };
}

/**
 * `executor`, whose transactions stop after the first statement that `at` matches (`reached()`), until `proceed`
 * settles.
 */
function pausing(executor: SqlExecutor, at: (text: string) => boolean, reached: () => void, proceed: Promise<void>): SqlExecutor {
  let stopped = false;
  return {
    dialect: executor.dialect,
    query: (text, params) => executor.query(text, params),
    execute: (text, params) => executor.execute(text, params),
    wrapTransaction: (transaction) => executor.wrapTransaction(transaction),
    transaction: (work, options) =>
      executor.transaction(
        (tx) =>
          work({
            query: async <R extends object>(text: string, params?: readonly unknown[]) => {
              const rows = await tx.query<R>(text, params);
              if (!stopped && at(text)) {
                stopped = true;
                reached();
                await proceed;
              }
              return rows;
            },
            execute: (text, params) => tx.execute(text, params),
          }),
        options,
      ),
  };
}
