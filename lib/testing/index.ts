import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import type { WorkflowJournalEntry } from '../interfaces/workflow-instance.interface.js';
import type { WorkflowClaim, WorkflowStore, WorkflowWrite } from '../interfaces/workflow-store.interface.js';

export interface WorkflowStoreContractOptions {
  /**
   * Also run the concurrency cases: many calls in flight at once, racing claims, signals,
   * suspensions and cancels. Set it for a store on a connection pool (they are the cases a
   * store without the right locks fails); a single connection serializes them, which they
   * must survive too.
   */
  concurrent?: boolean;
  /**
   * Opens an application transaction and runs `work` in it, such as
   * `(work) => db.transaction(work)`: the cases for `createInTransaction()` and
   * `signalInTransaction()` run when it is set and the store has them.
   */
  transaction?: <T>(work: (transaction: unknown) => Promise<T>) => Promise<T>;
  /** Called after each case with the store `createStore` returned (close it, drop its tables). */
  dispose?: (store: WorkflowStore) => unknown;
}

export interface WorkflowStoreContractCase {
  name: string;
  /** Throws on failure. */
  run(): Promise<void>;
}

/**
 * The `WorkflowStore` contract as test cases, for any test runner:
 *
 * ```ts
 * const cases = workflowStoreContract(async () => new DrizzleWorkflowStore(await freshDb(), new WorkflowStorage()), {
 *   concurrent: true,
 *   transaction: (work) => db.transaction(work),
 * });
 * for (const c of cases) it(c.name, c.run);
 * ```
 *
 * `createStore` is called once per case and must return a store on empty tables.
 */
export function workflowStoreContract(
  createStore: () => WorkflowStore | Promise<WorkflowStore>,
  options: WorkflowStoreContractOptions = {},
): WorkflowStoreContractCase[] {
  const cases: WorkflowStoreContractCase[] = [];
  const add = (name: string, run: (t: Harness) => Promise<void>) =>
    cases.push({
      name,
      run: async () => {
        const store = await createStore();
        try {
          await run(new Harness(store));
        } finally {
          await options.dispose?.(store);
        }
      },
    });

  const transaction = options.transaction;

  // ---------------------------------------------------------------- instances

  add('create() inserts a pending instance once, and returns the stored one for an existing id', async (t) => {
    const input = { title: "O'Reilly — Designing Data-Intensive Applications ü 🚀", qty: 2, nested: [null, { a: true }], empty: '' };
    const first = await t.store.create({ id: 'order-1', workflow: 'order-fulfilment', version: 3, input, now: 1_000 });
    expect(first, {
      created: true,
      instance: {
        id: 'order-1',
        workflow: 'order-fulfilment',
        version: 3,
        status: 'pending',
        input,
        error: null,
        wakeAt: 1_000,
        leaseOwner: null,
        leaseUntil: null,
        cancelRequested: false,
        cancelReason: null,
        signalCursor: 0,
        runs: 0,
        createdAt: 1_000,
        updatedAt: 1_000,
      },
    }, 'the new instance');
    absent(first.instance.output, 'output of a new instance', { orNull: true });

    const again = await t.store.create({ id: 'order-1', workflow: 'other', version: 1, input: 'different', now: 2_000 });
    expect(again, { created: false, instance: first.instance }, 'the existing instance, unchanged');
    expect(await t.store.get('order-1'), { ...first.instance, waits: [] }, 'get()');
    equal(await t.store.get('missing'), null, 'get() of an unknown id');

    equal((await t.store.create({ id: 'null-input', workflow: 'w', version: 1, input: null, now: 0 })).instance.input, null, 'a null input');
    absent((await t.store.create({ id: 'no-input', workflow: 'w', version: 1, input: undefined, now: 0 })).instance.input, 'an undefined input', { orNull: true });
    equal((await t.store.get('order-1', { journal: true }))?.journal, [], 'the journal of a new instance');
  });

  add('create() starts the signal cursor at the last signal sent before the instance', async (t) => {
    await t.store.signal({ name: 's', key: null, dedupeId: null, payload: 1, now: 0 });
    const { id } = await t.store.signal({ name: 's', key: null, dedupeId: null, payload: 2, now: 0 });
    equal((await t.create('a')).instance.signalCursor, id, 'signalCursor');
    equal((await t.claim(0)).lastSignalId, id, 'claim().lastSignalId');
  });

  add('list() filters by status, workflow and version, oldest first, a page at a time', async (t) => {
    await t.create('c', 3);
    await t.create('a', 1);
    await t.create('b', 2, 'order-fulfilment', 2);
    await t.create('d', 4, 'invoice-batch');
    await t.create('e', 3); // same createdAt as 'c': ordered by id
    await t.claim(1, { limit: 1 }); // 'a' is now running

    const ids = async (query: Partial<Parameters<WorkflowStore['list']>[0]>) =>
      (await t.store.list({ limit: 100, offset: 0, ...query })).map((i) => i.id);
    equal(await ids({}), ['a', 'b', 'c', 'e', 'd'], 'every instance');
    equal(await ids({ status: ['running'] }), ['a'], 'by status');
    equal(await ids({ status: ['pending', 'running'], limit: 2, offset: 1 }), ['b', 'c'], 'a page');
    equal(await ids({ workflow: 'order-fulfilment', version: 1 }), ['a', 'c', 'e'], 'by workflow and version');
    equal(await ids({ workflow: 'invoice-batch' }), ['d'], 'by workflow');
    equal(await ids({ status: ['completed'] }), [], 'nothing matches');
  });

  // ---------------------------------------------------------------- claims and leases

  add('claim() leases due, unleased instances of the given versions, most overdue first', async (t) => {
    await t.create('late', 50);
    await t.create('early', 10);
    await t.create('v2', 5, 'order-fulfilment', 2);
    await t.create('future', 5_000);

    const first = await t.claim(100, { owner: 'w1', token: 't1' });
    equal(first.instances.map((i) => i.id), ['early', 'late'], 'claimed, by wakeAt');
    expect(first.instances[0], { status: 'running', leaseOwner: 'w1', leaseUntil: 1_100, runs: 1, updatedAt: 100 }, 'a claimed instance');
    equal((await t.claim(1_100, { owner: 'w2' })).instances, [], 'leased until 1100 inclusive');

    const retaken = await t.claim(1_101, { owner: 'w2', token: 't2' });
    equal(retaken.instances.map((i) => [i.id, i.leaseOwner, i.runs]), [['early', 'w2', 2], ['late', 'w2', 2]], 'claimed again after the lease expired');
    equal(await t.store.write('early', 't1', t.journalWrite([entry('x')])), false, 'a write under the first token');
    equal(await t.store.write('early', 't2', t.journalWrite([entry('x')])), true, 'a write under the new token');

    const v2 = await t.claim(2_000, { owner: 'w3', limit: 1, workflows: [{ name: 'order-fulfilment', version: 2 }, { name: 'invoice-batch', version: 1 }] });
    equal(v2.instances.map((i) => i.id), ['v2'], 'only the versions the worker runs');
    equal((await t.claim(6_000, { owner: 'w4', limit: 1 })).instances.map((i) => i.id), ['early'], 'at most limit');
  });

  add('claim() keeps a compensating instance compensating, and never claims a finished one', async (t) => {
    await t.create('a');
    await t.create('b');
    const { instances } = await t.claim(1, { token: 't1' });
    equal(instances.length, 2, 'claimed');

    await t.store.write('a', 't1', { now: 2, entries: [], status: 'compensating', error: { name: 'StepFailedError', message: 'no' } });
    expect(await t.store.get('a'), { status: 'compensating', error: { name: 'StepFailedError', message: 'no' }, updatedAt: 2, leaseOwner: 'w1' }, 'compensating, lease kept');
    await t.store.write('a', 't1', { now: 3, entries: [], release: { wakeAt: 4, waits: [], signalCursor: 0 } });
    expect((await t.claim(4, { token: 't2' })).instances[0], { id: 'a', status: 'compensating', runs: 2 }, 'claimed again, still compensating');

    await t.store.write('b', 't1', { now: 3, entries: [], status: 'completed', output: 1, error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    equal((await t.claim(FAR, { token: 't3' })).instances.map((i) => i.id), ['a'], "'b' finished: never claimed again");
  });

  add('renew() extends the lease and reads the cancel flag while the token is current, and returns null after', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 't1' });

    equal(await t.store.renew('a', 't1', 5_000), { cancelRequested: false }, 'renewed');
    expect(await t.store.get('a'), { leaseUntil: 5_000 }, 'the new lease');
    equal(await t.store.requestCancel('a', 'stop', 2), true, 'cancel requested');
    equal(await t.store.renew('a', 't1', 6_000), { cancelRequested: true }, 'renew reads the flag');
    equal(await t.store.renew('a', 'other', 7_000), null, 'another token');
    equal(await t.store.renew('missing', 't1', 7_000), null, 'an unknown id');
    expect(await t.store.get('a'), { leaseUntil: 6_000 }, 'unchanged by a stale renew');
  });

  add('write() changes nothing under a stale token', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 'stale' });
    await t.claim(5_000, { owner: 'w2', token: 'current' }); // the first lease expired, w2 took over

    const writes: WorkflowWrite[] = [
      t.journalWrite([entry('x')]),
      { now: 1, entries: [], status: 'compensating', error: { name: 'Error', message: 'boom' } },
      { now: 1, entries: [entry('y')], status: 'suspended', release: { wakeAt: 9, waits: [{ signal: 's', key: null }], signalCursor: 0 } },
      { now: 1, entries: [], status: 'completed', output: 1, error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } },
      { now: 1, entries: [entry('z')], release: { wakeAt: 1, waits: [], signalCursor: 0 } },
    ];
    for (const [i, write] of writes.entries()) {
      equal(await t.store.write('a', 'stale', write), false, `stale write #${i + 1}`);
    }

    equal(await t.store.write('missing', 'current', t.journalWrite([entry('x')])), false, 'a write to an unknown id');
    expect(await t.store.get('a', { journal: true }), { status: 'running', leaseOwner: 'w2', leaseUntil: 6_000, error: null, waits: [], journal: [] }, 'nothing written');
  });

  add('write() keeps journal entries in first-write order, replaces them whole, and keeps null apart from undefined', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 't' });

    const progress = { cursor: 4, note: "it's 100% ü" };
    await t.store.write('a', 't', t.journalWrite([entry('one', { status: 'pending', attempts: 1, wakeAt: null, progress })]));
    await t.store.write('a', 't', t.journalWrite([
      entry('two', { kind: 'signal', status: 'pending', attempts: 0, wakeAt: 99, data: { signal: 's', key: null } }),
      entry('one', { status: 'failed', attempts: 2, error: { name: 'Error', message: 'x' }, wakeAt: 50 }),
      entry('three', { kind: 'now', result: 123 }),
    ]));
    await t.store.write('a', 't', t.journalWrite([
      entry('four', { result: null }),
      entry('two', { kind: 'signal', status: 'completed', attempts: 0, wakeAt: 99, result: { signalId: 7, payload: null }, data: { signal: 's', key: null } }),
    ]));

    const journal = (await t.store.get('a', { journal: true }))!.journal!;
    equal(journal.map((e) => e.name), ['one', 'two', 'three', 'four'], 'first-write order');
    expect(journal[0], { name: 'one', kind: 'step', status: 'failed', error: { name: 'Error', message: 'x' }, attempts: 2, wakeAt: 50, updatedAt: 1 }, "'one', replaced");
    absent(journal[0]!.progress, "'one'.progress, dropped by the replacement");
    expect(journal[1], { name: 'two', status: 'completed', result: { signalId: 7, payload: null }, data: { signal: 's', key: null } }, "'two'");
    equal(journal[3]!.result, null, 'a null result stays null');
    absent(journal[2]!.error, 'an undefined error stays undefined');
    equal((await t.store.get('a'))!.journal, undefined, 'no journal unless asked');
  });

  add('write() with release parks, finishes or hands back the instance', async (t) => {
    for (const id of ['parked', 'done', 'failed', 'back']) {
      await t.create(id);
    }
    await t.claim(1, { token: 't' });

    const waits = [{ signal: 'x', key: 'k' }, { signal: 'y', key: null }, { signal: 'a', key: 'z' }];
    equal(await t.store.write('parked', 't', { now: 2, entries: [entry('wait', { kind: 'signal', status: 'pending' })], status: 'suspended', release: { wakeAt: 500, waits, signalCursor: 0 } }), true, 'suspend');
    expect(await t.store.get('parked', { journal: true }), { status: 'suspended', wakeAt: 500, leaseUntil: null, leaseOwner: 'w1', updatedAt: 2, waits, journal: [{ name: 'wait' }] }, 'parked with its waits, in order');
    equal(await t.store.write('parked', 't', t.journalWrite([entry('late')])), false, 'the lease ended with the release');
    const [again] = (await t.claim(500, { token: 't2' })).instances;
    equal(again?.id, 'parked', 'due again at wakeAt');
    await t.store.write('parked', 't2', { now: 501, entries: [], status: 'suspended', release: { wakeAt: 900, waits: [{ signal: 'q', key: 'r' }], signalCursor: 0 } });
    equal((await t.store.get('parked'))!.waits, [{ signal: 'q', key: 'r' }], 'waits replaced');

    await t.store.write('done', 't', { now: 3, entries: [entry('last')], status: 'completed', output: { ok: [1, 'two'] }, error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    expect(await t.store.get('done'), { status: 'completed', output: { ok: [1, 'two'] }, error: null, wakeAt: null, leaseOwner: 'w1', leaseUntil: null, updatedAt: 3, waits: [] }, 'finished');
    await t.store.write('failed', 't', { now: 4, entries: [], status: 'failed', output: null, error: { name: 'WorkflowFailedError', message: 'no' }, release: { wakeAt: null, waits: [], signalCursor: 0 } });
    expect(await t.store.get('failed'), { status: 'failed', output: null, error: { name: 'WorkflowFailedError', message: 'no' } }, 'a null output stays null');

    await t.store.write('back', 't', { now: 5, entries: [entry('partial', { status: 'pending' })], release: { wakeAt: 5, waits: [], signalCursor: 0 } });
    expect(await t.store.get('back'), { status: 'running', wakeAt: 5, leaseUntil: null, error: null }, 'handed back: due, status unchanged');
    absent((await t.store.get('back'))!.output, 'output, never set', { orNull: true });
    equal((await t.claim(5, { owner: 'w2', token: 't3' })).instances.map((i) => i.id), ['back'], 'claimable at once');
  });

  // ---------------------------------------------------------------- signals

  add('signal() matches keys exactly (null, empty and other keys all differ) and wakes suspended instances only', async (t) => {
    for (const id of ['none', 'empty', 'k1', 'running']) {
      await t.create(id);
    }
    await t.claim(1, { token: 't' });
    const keys: Record<string, string | null> = { none: null, empty: '', k1: 'k1' };
    for (const id of Object.keys(keys)) {
      await t.store.write(id, 't', { now: 2, entries: [], status: 'suspended', release: { wakeAt: null, waits: [{ signal: 'shipment.delivered', key: keys[id]! }], signalCursor: 0 } });
    }

    const s1 = await t.store.signal({ name: 'shipment.delivered', key: 'k2', dedupeId: null, payload: 'x', now: 3 });
    equal(s1.woken, 0, 'another key');
    const s2 = await t.store.signal({ name: 'other', key: null, dedupeId: null, payload: 'x', now: 3 });
    equal(s2.woken, 0, 'another name');
    const s3 = await t.store.signal({ name: 'shipment.delivered', key: '', dedupeId: null, payload: { e: '' }, now: 4 });
    equal(s3.woken, 1, "the '' key");
    expect(await t.store.get('empty'), { wakeAt: 4, updatedAt: 4 }, "woken: the '' wait");
    expect(await t.store.get('none'), { wakeAt: null }, 'not woken: the wait without a key');

    const s4 = await t.store.signal({ name: 'shipment.delivered', key: null, dedupeId: null, payload: null, now: 5 });
    equal(s4.woken, 1, 'no key');
    const s5 = await t.store.signal({ name: 'shipment.delivered', key: 'k1', dedupeId: null, payload: [1], now: 6 });
    const s6 = await t.store.signal({ name: 'shipment.delivered', key: 'k1', dedupeId: null, payload: [2], now: 7 });
    equal([s5.woken, s6.woken], [1, 0], 'an instance already due is not woken again');

    const ids = [s1, s2, s3, s4, s5, s6].map((s) => s.id);
    equal([...ids].sort((a, b) => a - b), ids, 'ids increase');
    equal(new Set(ids).size, 6, 'ids are unique');

    const read = (key: string | null, afterId = 0, upToId = s6.id) => t.store.signals({ name: 'shipment.delivered', key, afterId, upToId });
    equal(await read(null), [{ id: s4.id, name: 'shipment.delivered', key: null, payload: null, createdAt: 5 }], 'signals() without a key');
    equal(await read(''), [{ id: s3.id, name: 'shipment.delivered', key: '', payload: { e: '' }, createdAt: 4 }], "signals() with ''");
    equal((await read('k1')).map((s) => s.id), [s5.id, s6.id], 'signals() by id');
    equal((await read('k1', s5.id)).map((s) => s.id), [s6.id], 'afterId is exclusive');
    equal((await read('k1', 0, s5.id)).map((s) => s.id), [s5.id], 'upToId is inclusive');
    equal((await t.claim(0)).lastSignalId, s6.id, 'the last signal id');
    expect(await t.store.get('running'), { status: 'running' }, 'a running instance is left to write()');
  });

  add('write() keeps an instance due when a signal for its new waits arrived after its cursor', async (t) => {
    await t.create('a');
    await t.create('b');
    const { lastSignalId: cursor } = await t.claim(1, { token: 't' });
    await t.store.signal({ name: 'go', key: 'a', dedupeId: null, payload: 1, now: 2 }); // while a and b were executing

    const suspend = (id: string) =>
      t.store.write(id, 't', { now: 3, entries: [], status: 'suspended', release: { wakeAt: FAR, waits: [{ signal: 'go', key: id }], signalCursor: cursor } });
    equal(await suspend('a'), true, 'suspend a');
    equal(await suspend('b'), true, 'suspend b');
    expect(await t.store.get('a'), { status: 'suspended', wakeAt: 3, leaseUntil: null }, 'a saw the signal: due now');
    expect(await t.store.get('b'), { status: 'suspended', wakeAt: FAR, waits: [{ signal: 'go', key: 'b' }] }, 'b: another key, parked');

    const [c] = (await t.claim(3, { token: 't2' })).instances;
    equal(c?.id, 'a', 'a is claimable');
    await t.store.write('a', 't2', { now: 4, entries: [], status: 'suspended', release: { wakeAt: FAR, waits: [{ signal: 'go', key: 'a' }], signalCursor: (await t.claim(0)).lastSignalId } });
    expect(await t.store.get('a'), { wakeAt: FAR }, 'a signal at or below the cursor was seen: parked');
  });

  add('signal() with a dedupeId stores the signal once per name, and a repeat writes and wakes nothing', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 't' });
    const waits = [{ signal: 'payment.captured', key: 'o1' }];
    await t.store.write('a', 't', { now: 1, entries: [], status: 'suspended', release: { wakeAt: FAR, waits, signalCursor: 0 } });

    const send = (name: string, key: string | null, dedupeId: string | null, payload: unknown, now: number) =>
      t.store.signal({ name, key, dedupeId, payload, now });
    const first = await send('payment.captured', 'o1', 'ch_1', { amount: 2499 }, 2);
    expect(first, { created: true, woken: 1, key: 'o1' }, 'the first signal');

    // Parked again on the same wait, having seen the first signal.
    await t.claim(2, { token: 't2' });
    await t.store.write('a', 't2', { now: 3, entries: [], status: 'suspended', release: { wakeAt: FAR, waits, signalCursor: first.id } });
    equal(await send('payment.captured', 'o1', 'ch_1', { amount: 1 }, 4), { id: first.id, woken: 0, created: false, key: 'o1' }, 'a repeat');
    expect(await t.store.get('a'), { status: 'suspended', wakeAt: FAR, updatedAt: 3 }, 'not woken by the repeat');
    equal(await send('payment.captured', 'o2', 'ch_1', 'x', 5), { id: first.id, woken: 0, created: false, key: 'o1' }, 'the same id with another key');

    const otherName = await send('refund.issued', 'o1', 'ch_1', 'y', 6);
    expect(otherName, { created: true, key: 'o1' }, 'the same id under another name');
    const plain = [await send('payment.captured', 'o1', null, 'p', 7), await send('payment.captured', 'o1', null, 'p', 8)];
    expect(plain, [{ created: true }, { created: true }], 'signals without a dedupeId');
    equal(new Set([first.id, otherName.id, ...plain.map((s) => s.id)]).size, 4, 'distinct ids');

    const read = (name: string, key: string) => t.store.signals({ name, key, afterId: 0, upToId: Number.MAX_SAFE_INTEGER });
    equal((await read('payment.captured', 'o1')).map((s) => s.payload), [{ amount: 2499 }, 'p', 'p'], 'the first payload, stored once');
    equal(await read('payment.captured', 'o2'), [], 'nothing under the other key');
    equal((await t.claim(0)).lastSignalId, plain[1]!.id, 'the last signal id');
  });

  // ---------------------------------------------------------------- cancel

  add('requestCancel() accepts once, for pending, running and suspended instances only', async (t) => {
    for (const id of ['pending', 'running', 'suspended', 'compensating', 'done']) {
      await t.create(id, 0);
    }
    await t.create('pending-later', 50);
    await t.claim(1, { token: 't' });

    const park = { now: 1, entries: [], release: { wakeAt: FAR, waits: [], signalCursor: 0 } };
    await t.store.write('pending', 't', { ...park, status: 'pending' });
    await t.store.write('suspended', 't', { ...park, status: 'suspended' });
    await t.store.write('compensating', 't', { now: 1, entries: [], status: 'compensating', error: { name: 'E', message: 'x' } });
    await t.store.write('done', 't', { now: 1, entries: [], status: 'completed', error: null, release: { wakeAt: null, waits: [], signalCursor: 0 } });

    equal(await t.store.requestCancel('suspended', 'Changed my mind.', 10), true, 'suspended');
    expect(await t.store.get('suspended'), { status: 'suspended', cancelRequested: true, cancelReason: 'Changed my mind.', wakeAt: 10, updatedAt: 10 }, 'due now');
    equal(await t.store.requestCancel('suspended', 'Again.', 11), false, 'a repeated request');
    expect(await t.store.get('suspended'), { cancelReason: 'Changed my mind.', wakeAt: 10 }, 'the first reason stays');

    equal(await t.store.requestCancel('pending', null, 12), true, 'pending');
    expect(await t.store.get('pending'), { cancelRequested: true, cancelReason: null, wakeAt: 12 }, 'pending, due now');
    equal(await t.store.requestCancel('pending-later', null, 12), true, 'pending, not yet due');
    expect(await t.store.get('pending-later'), { wakeAt: 12 }, 'due now');

    equal(await t.store.requestCancel('running', 'r', 13), true, 'running');
    expect(await t.store.get('running'), { cancelRequested: true, wakeAt: 0 }, 'an instance already due keeps its wakeAt');

    equal(await t.store.requestCancel('compensating', 'x', 14), false, 'compensating');
    equal(await t.store.requestCancel('done', 'x', 15), false, 'finished');
    equal(await t.store.requestCancel('missing', 'x', 16), false, 'unknown');
    expect(await t.store.get('done'), { status: 'completed', cancelRequested: false }, 'finished: unchanged');
  });

  add('write() keeps a suspending instance with a pending cancel due, but not a compensating one', async (t) => {
    await t.create('a');
    await t.claim(1, { token: 't' });
    await t.store.requestCancel('a', null, 2);
    await t.store.write('a', 't', { now: 20, entries: [], status: 'suspended', release: { wakeAt: FAR, waits: [], signalCursor: 0 } });
    expect(await t.store.get('a'), { status: 'suspended', wakeAt: 20 }, 'suspended with a cancel: due now');

    await t.claim(20, { token: 't2' });
    await t.store.write('a', 't2', { now: 21, entries: [], status: 'compensating', error: { name: 'WorkflowCancelledError', message: 'Cancelled.' }, release: { wakeAt: 500, waits: [], signalCursor: 0 } });
    expect(await t.store.get('a'), { status: 'compensating', wakeAt: 500 }, 'compensating: its own wakeAt');
  });

  // ---------------------------------------------------------------- the application's transaction

  if (transaction) {
    add("createInTransaction() and signalInTransaction() commit and roll back with the application's transaction", async (t) => {
      const { createInTransaction, signalInTransaction } = requireTransactionMethods(t.store);
      await rejects(
        transaction(async (tx) => {
          expect(await createInTransaction(tx, { id: 'rolled-back', workflow: 'order-fulfilment', version: 1, input: 1, now: 1 }), { created: true }, 'created in the transaction');
          await signalInTransaction(tx, { name: 'go', key: 'x', dedupeId: null, payload: 1, now: 1 });
          throw new Error('payment declined');
        }),
        'payment declined',
      );
      equal(await t.store.get('rolled-back'), null, 'rolled back: no instance');
      equal((await t.claim(0)).lastSignalId, 0, 'rolled back: no signal');

      await t.create('waiting');
      await t.claim(1, { token: 't' });
      await t.store.write('waiting', 't', { now: 1, entries: [], status: 'suspended', release: { wakeAt: null, waits: [{ signal: 'go', key: 'w' }], signalCursor: 0 } });
      const results = await transaction(async (tx) => [
        await createInTransaction(tx, { id: 'committed', workflow: 'order-fulfilment', version: 1, input: { a: 1 }, now: 2 }),
        await createInTransaction(tx, { id: 'committed', workflow: 'order-fulfilment', version: 1, input: { a: 2 }, now: 3 }),
        await signalInTransaction(tx, { name: 'go', key: 'w', dedupeId: null, payload: 'p', now: 4 }),
      ] as const);

      expect(results[0], { created: true, instance: { id: 'committed', status: 'pending', input: { a: 1 }, signalCursor: 0 } }, 'created');
      expect(results[1], { created: false, instance: { id: 'committed', input: { a: 1 } } }, 'the row written earlier in the transaction');
      equal(results[2].woken, 1, 'woken');
      expect(await t.store.get('committed'), { status: 'pending', wakeAt: 2 }, 'committed');
      expect(await t.store.get('waiting'), { wakeAt: 4 }, 'woken with the commit');
      expect(await t.store.signals({ name: 'go', key: 'w', afterId: 0, upToId: results[2].id }), [{ id: results[2].id, payload: 'p' }], 'the signal');
    });
  }

  if (transaction) {
    add('signalInTransaction() stores a dedupeId once per commit, and a rolled-back signal leaves its id free', async (t) => {
      const { signalInTransaction } = requireTransactionMethods(t.store);
      const data = (payload: unknown, now: number) => ({ name: 'payment.captured', key: 'o1', dedupeId: 'ch_1', payload, now });
      await rejects(
        transaction(async (tx) => {
          expect(await signalInTransaction(tx, data('rolled back', 1)), { created: true }, 'stored in the transaction');
          throw new Error('payment declined');
        }),
        'payment declined',
      );

      const [first, again] = await transaction(async (tx) => [await signalInTransaction(tx, data('committed', 2)), await signalInTransaction(tx, data('again', 3))] as const);
      expect(first, { created: true }, 'the id is free after the rollback');
      equal(again, { id: first.id, woken: 0, created: false, key: 'o1' }, 'a repeat in the same transaction');
      expect(await t.store.signal(data('outside', 4)), { id: first.id, created: false }, 'a repeat after the commit');
      equal(
        (await t.store.signals({ name: 'payment.captured', key: 'o1', afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).map((s) => s.payload),
        ['committed'],
        'one signal',
      );
    });
  }

  // ---------------------------------------------------------------- concurrency

  if (options.concurrent) {
    add('concurrent claims never return the same instance twice', async (t) => {
      for (let i = 0; i < 60; i++) {
        await t.create(`i${String(i).padStart(2, '0')}`, i);
      }

      const owners = new Map<string, string>();
      const claimer = async (owner: string) => {
        for (;;) {
          const { instances } = await t.claim(1_000, { owner, token: randomUUID(), limit: 7, leaseUntil: FAR });
          if (instances.length === 0) {
            return;
          }

          for (const instance of instances) {
            if (owners.has(instance.id)) {
              throw new Error(`${instance.id} was claimed by ${owners.get(instance.id)} and ${owner}`);
            }
            owners.set(instance.id, owner);
          }
          await jitter();
        }
      };

      await Promise.all(['c1', 'c2', 'c3', 'c4', 'c5'].map(claimer));
      equal(owners.size, 60, 'every instance claimed');
      const all = await t.store.list({ limit: 100, offset: 0 });
      equal(all.filter((i) => i.runs !== 1 || i.leaseOwner !== owners.get(i.id)).map((i) => i.id), [], 'each claimed once, by its owner');
    });

    add('a signal racing a suspension never loses the wake-up', async (t) => {
      const n = 60;
      for (let i = 0; i < n; i++) {
        await t.create(`r${i}`);
      }

      const { instances } = await t.claim(1, { limit: n, leaseUntil: FAR, token: 't' });
      equal(instances.length, n, 'claimed');

      await Promise.all(
        instances.map(async (instance, i) => {
          const { lastSignalId: cursor } = await t.claim(0); // what the execution saw when it started
          const suspend = async () => {
            await jitter();
            const ok = await t.store.write(instance.id, 't', {
              now: 5,
              entries: [entry('wait', { kind: 'signal', status: 'pending', attempts: 0, wakeAt: FAR })],
              status: 'suspended',
              release: { wakeAt: FAR, waits: [{ signal: 'go', key: instance.id }], signalCursor: cursor },
            });
            if (!ok) {
              throw new Error(`suspending ${instance.id} was refused`);
            }
          };

          const signal = async () => {
            await jitter();
            const data = { name: 'go', key: instance.id, dedupeId: null, payload: i, now: 6 };
            if (i % 2 === 0 || !transaction || !t.store.signalInTransaction) {
              return t.store.signal(data);
            }

            // In the application's transaction, which holds the lock a little longer.
            return transaction(async (tx) => {
              const result = await t.store.signalInTransaction!(tx, data);
              await jitter();
              return result;
            });
          };

          await Promise.all([suspend(), signal()]);
        }),
      );

      const parked = (await t.store.list({ limit: n, offset: 0 })).filter((i) => i.wakeAt === FAR).map((i) => i.id);
      equal(parked, [], 'every instance was woken by its signal (wakeAt 6) or saw it while suspending (5)');
    });

    add('signal ids become visible in id order: nothing below a cursor shows up later', async (t) => {
      const probes: Array<{ cursor: number; seen: number[] }> = [];
      let sending = true;
      const probe = async () => {
        while (sending) {
          const { lastSignalId: cursor } = await t.claim(0);
          const seen = (await t.store.signals({ name: 's', key: null, afterId: 0, upToId: cursor })).map((s) => s.id);
          probes.push({ cursor, seen });
          await jitter();
        }
      };

      const send = async (i: number) => {
        await jitter();
        if (i % 3 !== 0 || !transaction || !t.store.signalInTransaction) {
          return t.store.signal({ name: 's', key: null, dedupeId: null, payload: i, now: 1 });
        }
        return transaction(async (tx) => {
          const result = await t.store.signalInTransaction!(tx, { name: 's', key: null, dedupeId: null, payload: i, now: 1 });
          await jitter();
          return result;
        });
      };

      const probing = [probe(), probe()];
      const sent = await Promise.all(Array.from({ length: 60 }, (_, i) => send(i)));
      sending = false;
      await Promise.all(probing);

      equal(new Set(sent.map((s) => s.id)).size, 60, 'unique ids');
      const all = (await t.store.signals({ name: 's', key: null, afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).map((s) => s.id);
      for (const { cursor, seen } of probes) {
        equal(all.filter((id) => id <= cursor), seen, `the signals at or below cursor ${cursor}`);
      }
    });

    add('concurrent signals with one dedupeId store it once, and every call returns its id', async (t) => {
      const send = async (i: number) => {
        await jitter();
        const data = { name: 'payment.captured', key: 'o1', dedupeId: 'ch_1', payload: i, now: i };
        if (i % 2 === 0 || !transaction || !t.store.signalInTransaction) {
          return t.store.signal(data);
        }

        return transaction(async (tx) => {
          const result = await t.store.signalInTransaction!(tx, data);
          await jitter();
          return result;
        });
      };

      const results = await Promise.all(Array.from({ length: 12 }, (_, i) => send(i)));
      equal(results.filter((r) => r.created).length, 1, 'created once');
      equal(new Set(results.map((r) => r.id)).size, 1, 'one id for every call');
      const stored = await t.store.signals({ name: 'payment.captured', key: 'o1', afterId: 0, upToId: Number.MAX_SAFE_INTEGER });
      equal(stored.map((s) => s.id), [results[0]!.id], 'one signal stored');
    });

    add('a cancel racing a suspension leaves the instance due', async (t) => {
      const n = 40;
      for (let i = 0; i < n; i++) {
        await t.create(`c${i}`);
      }

      const { instances } = await t.claim(1, { limit: n, leaseUntil: FAR, token: 't' });
      await Promise.all(
        instances.map(async (instance) => {
          await Promise.all([
            jitter().then(() => t.store.requestCancel(instance.id, 'stop', 3)),
            jitter().then(() =>
              t.store.write(instance.id, 't', { now: 4, entries: [], status: 'suspended', release: { wakeAt: FAR, waits: [], signalCursor: 0 } }),
            ),
          ]);
        }),
      );

      const all = await t.store.list({ limit: n, offset: 0 });
      equal(all.filter((i) => !i.cancelRequested || i.wakeAt === FAR).map((i) => i.id), [], 'cancelled and due (wakeAt 3 or 4)');
    });

    add('concurrent create() and requestCancel() of one id: one wins', async (t) => {
      const created = await Promise.all(Array.from({ length: 8 }, (_, i) => jitter().then(() => t.store.create({ id: 'same', workflow: 'w', version: 1, input: i, now: i }))));
      equal(created.filter((r) => r.created).length, 1, 'created once');
      const winner = created.find((r) => r.created)!.instance;
      equal(created.filter((r) => !isDeepStrictEqual(r.instance.input, winner.input)).length, 0, 'every call returns the stored instance');

      const accepted = await Promise.all(Array.from({ length: 8 }, (_, i) => jitter().then(() => t.store.requestCancel('same', `r${i}`, 10))));
      equal(accepted.filter(Boolean).length, 1, 'one cancel accepted');
    });

    add("a stale lease holder's writes never land, however they interleave with the new holder's", async (t) => {
      const n = 20;
      for (let i = 0; i < n; i++) {
        await t.create(`f${i}`);
      }
      await t.claim(1, { limit: n, token: 'old', leaseUntil: 10 });
      await t.claim(11, { limit: n, token: 'new', owner: 'w2', leaseUntil: FAR });

      const results = await Promise.all(
        Array.from({ length: n }, (_, i) => `f${i}`).flatMap((id) => [
          jitter().then(() => t.store.write(id, 'old', { now: 12, entries: [entry('stale')], status: 'compensating', error: { name: 'E', message: 'stale' } })),
          jitter().then(() => t.store.write(id, 'new', t.journalWrite([entry('fresh')]))),
          jitter().then(() => t.store.write(id, 'old', { now: 12, entries: [], status: 'failed', release: { wakeAt: null, waits: [], signalCursor: 0 } })),
        ]),
      );

      equal(results.filter((ok, i) => ok !== (i % 3 === 1)).length, 0, 'only the new holder writes');
      for (let i = 0; i < n; i++) {
        expect(await t.store.get(`f${i}`, { journal: true }), { status: 'running', leaseUntil: FAR, error: null, journal: [{ name: 'fresh' }] }, `f${i}`);
      }
    });
  }

  return cases;
}

// ---------------------------------------------------------------- helpers

const FAR = 9_000_000_000_000; // a deadline nobody reaches in a test
const W = { name: 'order-fulfilment', version: 1 };

class Harness {
  constructor(readonly store: WorkflowStore) {}

  create(id: string, now = 0, workflow = 'order-fulfilment', version = 1) {
    return this.store.create({ id, workflow, version, input: { id }, now });
  }

  claim(
    now: number,
    o: { owner?: string; token?: string; limit?: number; leaseUntil?: number; workflows?: Array<{ name: string; version: number }> } = {},
  ): Promise<WorkflowClaim> {
    return this.store.claim({
      owner: o.owner ?? 'w1',
      token: o.token ?? randomUUID(),
      now,
      leaseUntil: o.leaseUntil ?? now + 1_000,
      limit: o.limit ?? 100,
      workflows: o.workflows ?? [W],
    });
  }

  journalWrite(entries: WorkflowJournalEntry[]): WorkflowWrite {
    return { now: 1, entries };
  }
}

function entry(name: string, extra: Partial<WorkflowJournalEntry> = {}): WorkflowJournalEntry {
  return { name, kind: 'step', status: 'completed', attempts: 1, updatedAt: 1, ...extra };
}

function requireTransactionMethods(store: WorkflowStore) {
  const { createInTransaction, signalInTransaction } = store;
  if (typeof createInTransaction !== 'function' || typeof signalInTransaction !== 'function') {
    throw new Error('options.transaction is set, but the store lacks createInTransaction() or signalInTransaction().');
  }
  return { createInTransaction: createInTransaction.bind(store), signalInTransaction: signalInTransaction.bind(store) };
}

const jitter = () => new Promise((resolve) => setTimeout(resolve, Math.random() * 4));

function show(value: unknown): string {
  return JSON.stringify(value, (_key, v) => (v === undefined ? '<undefined>' : v)) ?? String(value);
}

function equal(actual: unknown, expected: unknown, label: string): void {
  if (!isDeepStrictEqual(normalizeUndefined(actual), normalizeUndefined(expected))) {
    throw new Error(`${label}: expected ${show(expected)}, got ${show(actual)}`);
  }
}

/** Every key of `pattern` matches, recursively (arrays by length and element); other keys are ignored. */
function expect(actual: unknown, pattern: unknown, label: string): void {
  const mismatch = match(actual, pattern, '');
  if (mismatch !== null) {
    throw new Error(`${label}: ${mismatch}\n  expected ${show(pattern)}\n  got      ${show(actual)}`);
  }
}

function match(actual: unknown, pattern: unknown, path: string): string | null {
  if (Array.isArray(pattern)) {
    if (!Array.isArray(actual) || actual.length !== pattern.length) {
      return `at ${path || 'the root'}: expected an array of ${pattern.length}`;
    }

    for (let i = 0; i < pattern.length; i++) {
      const mismatch = match(actual[i], pattern[i], `${path}[${i}]`);
      if (mismatch) {
        return mismatch;
      }
    }
    return null;
  }

  if (pattern !== null && typeof pattern === 'object') {
    if (actual === null || typeof actual !== 'object') {
      return `at ${path || 'the root'}: expected an object`;
    }

    for (const [key, value] of Object.entries(pattern)) {
      const mismatch = match((actual as Record<string, unknown>)[key], value, path ? `${path}.${key}` : key);
      if (mismatch) {
        return mismatch;
      }
    }
    return null;
  }
  return Object.is(actual, pattern) ? null : `at ${path || 'the root'}: expected ${show(pattern)}, got ${show(actual)}`;
}

function absent(value: unknown, label: string, options: { orNull?: boolean } = {}): void {
  if (value !== undefined && !(options.orNull && value === null)) {
    throw new Error(`${label}: expected undefined, got ${show(value)}`);
  }
}

async function rejects(promise: Promise<unknown>, message: string): Promise<void> {
  try {
    await promise;
  } catch (error) {
    if (String((error as Error)?.message ?? error).includes(message)) {
      return;
    }
    throw error;
  }

  throw new Error(`expected a rejection with "${message}"`);
}

/** Drops `undefined` object fields, so `{ a: undefined }` equals `{}` (a store may omit them). */
function normalizeUndefined(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(normalizeUndefined);
  }
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).filter(([, v]) => v !== undefined).map(([k, v]) => [k, normalizeUndefined(v)]));
  }
  return value;
}
