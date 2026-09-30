/**
 * MySqlWorkflowStore's keys on MySQL, whose default collation compares text case- and accent-insensitively: ids, names
 * and keys that differ only in case, accents or trailing spaces stay apart (binary `utf8mb4_0900_bin` columns); a key
 * longer than its column fails with a RangeError that names it, before anything is written, and one of exactly the
 * limit fits, counted in characters (an emoji is one).
 */
import { Test } from '@nestjs/testing';
import { Workflow, WorkflowClient, WorkflowsModule, WorkflowStorage, type WorkflowContext } from '../../lib/index.js';
import { MySqlWorkflowStore } from '../../lib/mysql/index.js';
import { mysql2Client, onMysql, testDatabase, truncate, type Client } from './support.js';

const { database, reason } = await testDatabase('mysql_keys');
const FAR = 9_000_000_000_000;

let client: Client | null = null;
let store: MySqlWorkflowStore;
beforeAll(async () => {
  if (database) {
    client = await mysql2Client.open(database.url);
    store = new MySqlWorkflowStore({ executor: client.executor, schema: 'keys' });
    await store.migrate();
  }
});
afterAll(() => client?.close());
beforeEach(async () => {
  if (client) {
    await truncate(client.executor, 'keys');
  }
});

const create = (id: string, extra: Partial<Parameters<MySqlWorkflowStore['create']>[0]> = {}) =>
  store.create({ id, workflow: 'w', version: 1, input: { id }, deadline: null, now: 0, ...extra });
const release = { wakeAt: FAR, waits: [] as Array<{ signal: string; key: string | null }>, signalCursor: 0 };

describe('keys that differ only in case, accents or trailing spaces', () => {
  onMysql(reason);

  it('are different instances, workflows, concurrency keys and schedules', async () => {
    const ids = ['order-1', 'Order-1', 'ORDER-1', 'ördér-1', 'order-1 '];
    for (const id of ids) {
      expect(await create(id), id).toMatchObject({ created: true, instance: { id, input: { id } } });
    }
    for (const id of ids) {
      expect(await store.get(id)).toMatchObject({ id, input: { id } });
    }
    expect((await store.list({ workflow: 'w', limit: 10, offset: 0 })).map((i) => i.id).sort()).toEqual([...ids].sort());
    await create('other', { workflow: 'W' });
    expect((await store.list({ workflow: 'W', limit: 10, offset: 0 })).map((i) => i.id)).toEqual(['other']);

    // A concurrency key per spelling: one slot each.
    await truncate(client!.executor, 'keys');
    for (const [i, key] of ['customer', 'Customer', 'cüstomer'].entries()) {
      await create(`k${i}`, { concurrencyKey: key, now: i });
    }
    const claimed = await store.claim({
      owner: 'w',
      token: 't',
      now: 10,
      leaseUntil: FAR,
      limit: 10,
      workflows: [{ name: 'w', version: 1 }],
      limits: [{ workflow: 'w', limit: null, perKey: 1 }],
    });
    expect(claimed.instances.map((i) => i.concurrencyKey)).toEqual(['customer', 'Customer', 'cüstomer']);

    for (const id of ['digest', 'Digest', 'dïgest']) {
      const save = { id, workflow: 'w', declared: false, spec: { every: id }, input: null, paused: false, wakeAt: null, state: {}, expectRevision: null, releaseLease: false, now: 1 };
      expect(await store.saveSchedule(save), id).toMatchObject({ id, revision: 1, spec: { every: id } });
    }
    expect((await store.listSchedules({ limit: 10, offset: 0 })).map((s) => s.id).sort()).toEqual(['Digest', 'digest', 'dïgest'].sort());
  });

  it('are different signals, waits, dedupe ids and journal entries', async () => {
    await create('a');
    await store.claim({ owner: 'w', token: 't', now: 0, leaseUntil: FAR, limit: 1, workflows: [{ name: 'w', version: 1 }] });
    await store.write('a', 't', {
      now: 1,
      entries: ['step', 'Step', 'stép'].map((name) => ({ name, kind: 'step' as const, status: 'completed' as const, attempts: 1, updatedAt: 1, result: name })),
      status: 'suspended',
      release: { ...release, waits: [{ signal: 'paid', key: 'k' }] },
    });
    expect((await store.get('a', { journal: true }))!.journal!.map((entry) => [entry.name, entry.result])).toEqual([
      ['step', 'step'],
      ['Step', 'Step'],
      ['stép', 'stép'],
    ]);

    // Only the exact name and key wake it.
    for (const [name, key] of [
      ['Paid', 'k'],
      ['päid', 'k'],
      ['paid', 'K'],
      ['paid', 'k '],
      ['paid ', 'k'],
    ]) {
      expect(await store.signal({ name, key, dedupeId: null, payload: null, now: 2 }), `${name}/${key}`).toMatchObject({ created: true, woken: 0 });
    }
    expect(await store.get('a')).toMatchObject({ status: 'suspended', wakeAt: FAR });
    expect(await store.signal({ name: 'paid', key: 'k', dedupeId: null, payload: null, now: 3 })).toMatchObject({ created: true, woken: 1 });

    // A dedupe id per spelling.
    const first = await store.signal({ name: 'captured', key: null, dedupeId: 'ch_1', payload: 1, now: 4 });
    expect(await store.signal({ name: 'captured', key: null, dedupeId: 'CH_1', payload: 2, now: 4 })).toMatchObject({ created: true });
    expect(await store.signal({ name: 'captured', key: null, dedupeId: 'ch_1 ', payload: 3, now: 4 })).toMatchObject({ created: true });
    expect(await store.signal({ name: 'captured', key: null, dedupeId: 'ch_1', payload: 4, now: 4 })).toMatchObject({ id: first.id, created: false });
    expect((await store.signals({ name: 'paid', key: 'K', afterId: 0, upToId: FAR })).map((s) => s.key)).toEqual(['K']);
  });
});

describe('a key longer than its column', () => {
  onMysql(reason);

  const tooLong = (what: string, limit: number, characters: number) =>
    new RegExp(`^MySqlWorkflowStore: ${what.replace(/[$()]/g, '\\$&')} holds at most ${limit} characters on MySQL, and ".{40}…" has ${characters}\\.`, 'u');

  it('fails with a RangeError that names it, and writes nothing', async () => {
    await expect(create('x'.repeat(256))).rejects.toThrow(RangeError);
    await expect(create('x'.repeat(256))).rejects.toThrow(tooLong('an instance id', 255, 256));
    await expect(create('a', { workflow: 'w'.repeat(256) })).rejects.toThrow(tooLong("a workflow's name", 255, 256));
    await expect(create('a', { concurrencyKey: 'k'.repeat(300) })).rejects.toThrow(tooLong('a concurrency key', 255, 300));
    await expect(create('a', { rateLimitKey: 'r'.repeat(256) })).rejects.toThrow(tooLong('a rate-limit key', 255, 256));
    await expect(create('a', { parentId: 'p'.repeat(256) })).rejects.toThrow(tooLong("an instance's parent id", 255, 256));
    expect(await store.list({ limit: 10, offset: 0 })).toEqual([]);

    await expect(store.signal({ name: 's'.repeat(256), key: null, dedupeId: null, payload: null, now: 1 })).rejects.toThrow(tooLong("a signal's name", 255, 256));
    await expect(store.signal({ name: 's', key: 'k'.repeat(256), dedupeId: null, payload: null, now: 1 })).rejects.toThrow(tooLong("a signal's key", 255, 256));
    await expect(store.signal({ name: 's', key: null, dedupeId: 'd'.repeat(256), payload: null, now: 1 })).rejects.toThrow(tooLong("a signal's id", 255, 256));
    expect((await store.claim({ owner: 'w', token: 't', now: 0, leaseUntil: 1, limit: 1, workflows: [{ name: 'w', version: 1 }] })).lastSignalId).toBe(0);

    const schedule = { workflow: 'w', declared: false, spec: {}, input: null, paused: false, wakeAt: null, state: {}, expectRevision: null, releaseLease: false, now: 1 };
    await expect(store.saveSchedule({ ...schedule, id: 'd'.repeat(231) })).rejects.toThrow(tooLong('a schedule id', 230, 231));
    expect(await store.saveSchedule({ ...schedule, id: 'd'.repeat(230) })).toMatchObject({ revision: 1 });

    // In a write: the journal entry's name and the waits' keys, checked before the transaction; the lease is kept.
    await create('a');
    await store.claim({ owner: 'w', token: 't', now: 0, leaseUntil: FAR, limit: 1, workflows: [{ name: 'w', version: 1 }] });
    const entry = (name: string) => ({ name, kind: 'step' as const, status: 'completed' as const, attempts: 1, updatedAt: 1 });
    await expect(store.write('a', 't', { now: 1, entries: [entry('ok'), entry('n'.repeat(513))] })).rejects.toThrow(tooLong("a journal entry's name (a step's, a wait's, or the engine's, such as $child: and a child's id)", 512, 513));
    await expect(store.write('a', 't', { now: 1, entries: [], status: 'suspended', release: { ...release, waits: [{ signal: 'go', key: 'k'.repeat(256) }] } })).rejects.toThrow(
      tooLong("a wait's signal key", 255, 256),
    );
    expect(await store.get('a', { journal: true })).toMatchObject({ status: 'running', leaseUntil: FAR, journal: [], waits: [] });
    expect(await store.write('a', 't', { now: 1, entries: [entry('n'.repeat(512))] })).toBe(true);
  });

  it('counts characters, not bytes: ids of four-byte characters fit up to the limit', async () => {
    const emoji = '🚀'.repeat(255);
    expect(await create(emoji)).toMatchObject({ created: true, instance: { id: emoji } });
    expect(await store.get(emoji)).toMatchObject({ id: emoji });
    await expect(create(`${emoji}🚀`)).rejects.toThrow(tooLong('an instance id', 255, 256));
  });

  it("reaches the application as start()'s error", async () => {
    @Workflow('short')
    class ShortWorkflow {
      async run(_ctx: WorkflowContext) {}
    }
    const moduleRef = await Test.createTestingModule({
      imports: [WorkflowsModule.forRoot({ worker: false })],
      providers: [
        ShortWorkflow,
        {
          provide: MySqlWorkflowStore,
          inject: [WorkflowStorage],
          useFactory: (storage: WorkflowStorage) => new MySqlWorkflowStore({ executor: client!.executor, schema: 'keys' }, storage),
        },
      ],
    }).compile();
    await moduleRef.init();
    try {
      await expect(moduleRef.get(WorkflowClient).start(ShortWorkflow, {}, { id: 'o'.repeat(256) })).rejects.toThrow(tooLong('an instance id', 255, 256));
    } finally {
      await moduleRef.close();
    }
  });
});
