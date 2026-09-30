/**
 * `scheduleStoreContract()` (`@nestjs/workflows/testing`) on the core's `InMemoryScheduleStore`, and the suite itself: it
 * fails a store that ignores a schedule's lease, its fence or its revision, and it runs its concurrency cases only
 * when asked. tests/postgres/schedule-store.spec.ts runs it on PostgreSQL; the workflow stores' suites run the same
 * cases on their schedules.
 */
import { InMemoryScheduleStore, type ScheduleStore } from '../../lib/core/index.js';
import { scheduleStoreContract } from '../../lib/testing/index.js';

describe('InMemoryScheduleStore', () => {
  for (const c of scheduleStoreContract(() => new InMemoryScheduleStore(), { concurrent: true })) {
    it(c.name, c.run);
  }
});

describe('scheduleStoreContract()', () => {
  /** The lease token of a schedule, as the in-memory store keeps it. */
  const tokenOf = (store: InMemoryScheduleStore, id: string): string | null => (store as any).schedules.get(id)?.leaseToken ?? null;

  /** Claims schedules whatever their leases. */
  class NoLease extends InMemoryScheduleStore {
    override async claimSchedules(...[request]: Parameters<InMemoryScheduleStore['claimSchedules']>) {
      for (const schedule of await this.listSchedules({ limit: 1_000, offset: 0 })) {
        await this.writeSchedule(schedule.id, tokenOf(this, schedule.id) ?? '', { now: request.now, state: schedule.state, wakeAt: schedule.wakeAt, release: true });
      }
      return super.claimSchedules(request);
    }
  }

  /** Takes any token for a schedule's lease. */
  class NoFence extends InMemoryScheduleStore {
    override async writeSchedule(...[id, , write]: Parameters<InMemoryScheduleStore['writeSchedule']>) {
      const schedule = (this as any).schedules.get(id);
      return schedule ? super.writeSchedule(id, schedule.leaseToken ?? (schedule.leaseToken = 'any'), write) : false;
    }
  }

  /** Saves schedules whatever their revision. */
  class NoRevision extends InMemoryScheduleStore {
    override async saveSchedule(...[save]: Parameters<InMemoryScheduleStore['saveSchedule']>) {
      const current = await this.getSchedule(save.id);
      return super.saveSchedule({ ...save, expectRevision: current?.revision ?? null });
    }
  }

  const failures = async (create: () => ScheduleStore) => {
    const failed: string[] = [];
    for (const c of scheduleStoreContract(create, { concurrent: true })) {
      await c.run().catch(() => failed.push(c.name));
    }
    return failed;
  };

  it("fails a store that ignores a schedule's lease, its fence or its revision", async () => {
    expect(await failures(() => new NoLease())).toEqual([
      'claimSchedules() leases due, unpaused, unleased schedules of the given targets, most overdue first',
      'concurrent claimSchedules() never return the same schedule twice, and saves of one revision land once',
    ]);
    expect(await failures(() => new NoFence())).toEqual(['writeSchedule() writes only under the lease token, and a save that releases the lease makes the token stale']);
    expect(await failures(() => new NoRevision())).toEqual([
      'saveSchedule() creates a schedule once, and replaces one only at the revision it read',
      'concurrent claimSchedules() never return the same schedule twice, and saves of one revision land once',
    ]);
  });

  it('creates a store per case, disposes of it after, and adds the concurrency cases only when asked, under unique names', async () => {
    const created: ScheduleStore[] = [];
    const disposed: ScheduleStore[] = [];
    const base = scheduleStoreContract(
      () => {
        const store = new InMemoryScheduleStore();
        created.push(store);
        return store;
      },
      { dispose: (store) => void disposed.push(store) },
    );
    for (const c of base) {
      await c.run();
    }
    expect(created).toHaveLength(base.length);
    expect(disposed).toEqual(created);

    const concurrent = scheduleStoreContract(() => new InMemoryScheduleStore(), { concurrent: true }).map((c) => c.name);
    expect(new Set(concurrent).size).toBe(concurrent.length);
    expect(concurrent.slice(0, base.length)).toEqual(base.map((c) => c.name));
    expect(concurrent.filter((name) => !base.some((c) => c.name === name))).toEqual([
      'concurrent claimSchedules() never return the same schedule twice, and saves of one revision land once',
    ]);
  });
});
