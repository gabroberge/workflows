import { randomUUID } from 'node:crypto';
import type { ScheduleSave, ScheduleStore } from '../core/interfaces/schedule-store.interface.js';
import { equal, expect, jitter } from './assertions.js';

export interface ScheduleStoreContractOptions {
  /**
   * Also run the concurrency cases: claims and saves in flight at once. Set it for a store on a connection pool (they
   * are the cases a store without the right locks fails); a single connection serializes them, which they must
   * survive too.
   */
  concurrent?: boolean;
  /** Called after each case with the store `createStore` returned (close it, drop its tables). */
  dispose?: (store: ScheduleStore) => unknown;
}

export interface ScheduleStoreContractCase {
  name: string;
  /** Throws on failure. */
  run(): Promise<void>;
}

/**
 * The core's `ScheduleStore` contract as test cases, for any test runner: what a store of schedules must do for the
 * `Scheduler`, whatever it runs on (SQL, Redis scripts, memory), races included.
 *
 * ```ts
 * const cases = scheduleStoreContract(async () => new RedisScheduleStore(await freshRedis()), { concurrent: true });
 * for (const c of cases) it(c.name, c.run);
 * ```
 *
 * `createStore` is called once per case and must return a store without schedules.
 */
export function scheduleStoreContract(
  createStore: () => ScheduleStore | Promise<ScheduleStore>,
  options: ScheduleStoreContractOptions = {},
): ScheduleStoreContractCase[] {
  const cases: ScheduleStoreContractCase[] = [];
  const add = (name: string, run: (store: ScheduleStore) => Promise<void>) =>
    cases.push({
      name,
      run: async () => {
        const store = await createStore();
        try {
          await run(store);
        } finally {
          await options.dispose?.(store);
        }
      },
    });

  scheduleCases(add, 'base');
  if (options.concurrent) {
    scheduleCases(add, 'concurrent');
  }
  return cases;
}

/**
 * @internal The contract's cases, by `group`, added with `add`: `workflowStoreContract()` runs them on a workflow
 * store's schedules.
 */
export function scheduleCases(add: (name: string, run: (store: ScheduleStore) => Promise<void>) => void, group: 'base' | 'concurrent'): void {
  if (group === 'concurrent') {
    add('concurrent claimSchedules() never return the same schedule twice, and saves of one revision land once', async (store) => {
      for (let i = 0; i < 40; i++) {
        await store.saveSchedule(schedule(`s${String(i).padStart(2, '0')}`, { wakeAt: i }));
      }

      const owners = new Map<string, string>();
      const claimer = async (owner: string) => {
        for (;;) {
          const claimed = await store.claimSchedules({ owner, token: randomUUID(), now: 1_000, leaseUntil: FAR, limit: 3, targets: [TARGET] });
          if (claimed.length === 0) {
            return;
          }
          for (const record of claimed) {
            if (owners.has(record.id)) {
              throw new Error(`${record.id} was claimed by ${owners.get(record.id)} and ${owner}`);
            }
            owners.set(record.id, owner);
          }
          await jitter();
        }
      };
      await Promise.all(['c1', 'c2', 'c3', 'c4', 'c5'].map(claimer));
      equal(owners.size, 40, 'every schedule claimed');

      const saves = await Promise.all(
        Array.from({ length: 8 }, (_, i) => jitter().then(() => store.saveSchedule(schedule('s00', { paused: true, now: 2_000 + i, expectRevision: 1 })))),
      );
      equal(saves.filter((saved) => saved !== null).length, 1, 'one save at revision 1');
      const inserts = await Promise.all(Array.from({ length: 8 }, () => jitter().then(() => store.saveSchedule(schedule('new')))));
      equal(inserts.filter((saved) => saved !== null).length, 1, 'one insert');
    });
    return;
  }

  add('saveSchedule() creates a schedule once, and replaces one only at the revision it read', async (store) => {
    const payload = { tenant: "O'Reilly — ü 🚀", ids: [1, null], empty: '' };
    const created = await store.saveSchedule(schedule('s1', { payload, now: 10 }));
    expect(
      created,
      { id: 's1', target: TARGET, declared: false, spec: SPEC, payload, paused: false, wakeAt: 100, state: { next: 100, runs: 0 }, revision: 1, leaseOwner: null, leaseUntil: null, createdAt: 10, updatedAt: 10 },
      'the new schedule',
    );
    equal(await store.saveSchedule(schedule('s1', { now: 11 })), null, 'a second insert');
    equal(await store.getSchedule('s1'), created, 'getSchedule()');
    equal(await store.getSchedule('missing'), null, 'an unknown id');

    const replaced = await store.saveSchedule(schedule('s1', { paused: true, payload: null, wakeAt: null, declared: true, now: 20, expectRevision: 1 }));
    expect(replaced, { revision: 2, paused: true, payload: null, wakeAt: null, declared: true, createdAt: 10, updatedAt: 20 }, 'replaced at revision 1');
    equal(await store.saveSchedule(schedule('s1', { now: 21, expectRevision: 1 })), null, 'a save at a stale revision');
    equal(await store.saveSchedule(schedule('other', { now: 21, expectRevision: 1 })), null, 'a save of an unknown id');
    expect(await store.getSchedule('s1'), { revision: 2, paused: true, updatedAt: 20 }, 'unchanged by them');

    const encoded = await store.saveSchedule(schedule('s2', { payload: '$wf1:aes-256-gcm:k1.Zm9v' }));
    equal(encoded?.payload, '$wf1:aes-256-gcm:k1.Zm9v', 'a string payload, as it is');
  });

  add('listSchedules() filters by target and declared, and pages by id; deleteSchedule() deletes at a revision, or any', async (store) => {
    for (const [id, target, declared] of [['c', TARGET, true], ['a', TARGET, false], ['b', OTHER, true], ['d', TARGET, false]] as const) {
      await store.saveSchedule(schedule(id, { target, declared }));
    }
    const ids = async (query: Partial<Parameters<ScheduleStore['listSchedules']>[0]> = {}) => (await store.listSchedules({ limit: 100, offset: 0, ...query })).map((s) => s.id);
    equal(await ids(), ['a', 'b', 'c', 'd'], 'every schedule, by id');
    equal(await ids({ target: TARGET }), ['a', 'c', 'd'], 'by target');
    equal(await ids({ declared: true }), ['b', 'c'], 'the declared ones');
    equal(await ids({ target: TARGET, declared: false }), ['a', 'd'], 'by both');
    equal(await ids({ target: TARGET, limit: 1, offset: 1 }), ['c'], 'a page');

    equal(await store.deleteSchedule('a', 2), false, 'at another revision');
    equal(await store.deleteSchedule('a', 1), true, 'at its revision');
    equal(await store.deleteSchedule('c'), true, 'at any revision');
    equal(await store.deleteSchedule('c'), false, 'already gone');
    equal(await ids(), ['b', 'd'], 'the rest');
    expect(await store.saveSchedule(schedule('a')), { revision: 1 }, 'the id, free again');
  });

  add('claimSchedules() leases due, unpaused, unleased schedules of the given targets, most overdue first', async (store) => {
    await store.saveSchedule(schedule('late', { wakeAt: 50 }));
    await store.saveSchedule(schedule('early', { wakeAt: 10 }));
    await store.saveSchedule(schedule('paused', { wakeAt: 10, paused: true }));
    await store.saveSchedule(schedule('ended', { wakeAt: null }));
    await store.saveSchedule(schedule('future', { wakeAt: 5_000 }));
    await store.saveSchedule(schedule('others', { wakeAt: 20, target: OTHER }));
    const claim = (now: number, o: { owner?: string; token?: string; targets?: string[]; limit?: number } = {}) =>
      store.claimSchedules({ owner: o.owner ?? 'w1', token: o.token ?? randomUUID(), now, leaseUntil: now + 1_000, limit: o.limit ?? 100, targets: o.targets ?? [TARGET] });

    const first = await claim(100, { token: 't1' });
    equal(first.map((s) => s.id), ['early', 'late'], 'due and unpaused, by wakeAt');
    expect(first[0], { leaseOwner: 'w1', leaseUntil: 1_100, revision: 1, updatedAt: 0 }, 'leased, nothing else changed');
    equal(await claim(1_100), [], 'leased until 1100 inclusive');
    equal((await claim(1_101, { owner: 'w2' })).map((s) => [s.id, s.leaseOwner]), [['early', 'w2'], ['late', 'w2']], 'claimed again once the lease expired');
    equal((await claim(1_101, { targets: [OTHER, 'unknown'] })).map((s) => s.id), ['others'], 'only the given targets');
    equal((await claim(9_000, { limit: 1 })).map((s) => s.id), ['early'], 'at most limit');
  });

  add('writeSchedule() writes only under the lease token, and a save that releases the lease makes the token stale', async (store) => {
    await store.saveSchedule(schedule('s', { wakeAt: 10 }));
    const claim = (now: number, token: string) => store.claimSchedules({ owner: 'w1', token, now, leaseUntil: now + 1_000, limit: 10, targets: [TARGET] });
    await claim(10, 't1');

    const state = { next: 500, runs: 1, pending: [{ at: 10, cancel: [] }] };
    equal(await store.writeSchedule('s', 't1', { now: 11, state, wakeAt: 11, release: false }), true, 'the lease holder');
    expect(await store.getSchedule('s'), { state, wakeAt: 11, revision: 2, updatedAt: 11, leaseUntil: 1_010 }, 'written, still leased');
    equal(await store.writeSchedule('s', 'other', { now: 12, state: {}, wakeAt: 0, release: true }), false, 'another token');
    equal(await store.writeSchedule('missing', 't1', { now: 12, state: {}, wakeAt: 0, release: true }), false, 'an unknown id');

    // A save that keeps the lease (a pause) leaves the holder its write, which keeps the save's fields.
    const current = (await store.getSchedule('s'))!;
    await store.saveSchedule(schedule('s', { paused: true, wakeAt: current.wakeAt, state: current.state, now: 13, expectRevision: current.revision }));
    equal(await store.writeSchedule('s', 't1', { now: 14, state: { next: 500, runs: 1, pending: [] }, wakeAt: 500, release: true }), true, 'after a save that kept the lease');
    expect(await store.getSchedule('s'), { paused: true, wakeAt: 500, revision: 4, leaseUntil: null, state: { pending: [] } }, 'released, still paused');
    equal(await claim(600, 't2'), [], 'a paused schedule is not claimed');

    await store.saveSchedule(schedule('s', { paused: false, wakeAt: 600, now: 15, expectRevision: 4 }));
    equal((await claim(600, 't3')).map((s) => s.id), ['s'], 'resumed');
    await store.saveSchedule(schedule('s', { wakeAt: 700, now: 16, expectRevision: 5, releaseLease: true }));
    equal(await store.writeSchedule('s', 't3', { now: 17, state: {}, wakeAt: 0, release: true }), false, 'the token of a lease a save released');
    expect(await store.getSchedule('s'), { wakeAt: 700, leaseUntil: null, revision: 6 }, "the save's");
    equal((await claim(700, 't4')).map((s) => s.id), ['s'], 'claimable at once');
  });
}

const FAR = 9_000_000_000_000; // a deadline nobody reaches in a test
const TARGET = 'order-fulfilment';
const OTHER = 'invoice-batch';
/** A schedule's spec, as the scheduler stores it: JSON the store keeps as it is. */
const SPEC = { cron: '0 0 8 * * MON', tz: 'Europe/Warsaw', startAt: null, limit: 52, missed: 'once', overlap: 'buffer-one' };

/** `saveSchedule()`'s argument for a schedule of `TARGET`, due at 100, to insert unless `expectRevision` says otherwise. */
function schedule(id: string, o: Partial<ScheduleSave> = {}): ScheduleSave {
  return {
    id,
    target: TARGET,
    declared: false,
    spec: SPEC,
    payload: null,
    paused: false,
    wakeAt: 100,
    state: { next: 100, runs: 0, pending: [] },
    expectRevision: null,
    releaseLease: false,
    now: 0,
    ...o,
  };
}
