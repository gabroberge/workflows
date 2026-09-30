/**
 * The WorkflowStore contract (lib/testing/index.ts, `@nestjs/workflows/testing`) on the store
 * this run uses (support.ts, `WORKFLOWS_TEST_STORE`). The in-memory store's calls never
 * interleave (it does its work before its first await), so the concurrency cases check that it
 * survives being raced. On PGlite and PostgreSQL it runs on the workflows tutorial's hand-written
 * DrizzleWorkflowStore, with its drizzle-kit migrations (copied into fixtures/): the proof that a
 * store of your own still works (tests/postgres/ runs it on PostgresWorkflowStore). On PGlite one
 * statement at a time, and on PostgreSQL on a pool, where the cases race real connections.
 */
import { DrizzleWorkflowStore } from './fixtures/database/drizzle-workflow.store.js';
import type { Database } from './fixtures/database/drizzle.js';
import { InMemoryWorkflowStore, WorkflowStorage, type WorkflowStore } from '../lib/index.js';
import { workflowStoreContract } from '../lib/testing/index.js';
import { connect, skipReason, storeKind, tempDb, type Connection } from './support.js';

if (storeKind === 'memory') {
  describe('InMemoryWorkflowStore', () => {
    for (const c of workflowStoreContract(() => new InMemoryWorkflowStore(), { concurrent: true })) {
      it(c.name, c.run);
    }
  });
} else {
  describe(`the hand-written DrizzleWorkflowStore on ${storeKind === 'pglite' ? 'PGlite' : 'PostgreSQL'}`, () => {
    // One connection (a pool on PostgreSQL) for every case, on emptied tables.
    let connection: Connection;
    const db = () => connection.db as Database;
    beforeAll(async () => {
      if (!skipReason) {
        connection = connect(await tempDb());
      }
    });
    afterAll(() => connection?.close());

    const cases = workflowStoreContract(
      async () => {
        await tempDb();
        return new DrizzleWorkflowStore(db(), new WorkflowStorage());
      },
      { concurrent: true, transaction: (work) => db().transaction(work) },
    );
    for (const c of cases) {
      it(c.name, c.run, 30_000);
    }
  });
}

// The suite itself, once: it needs no database.
if (storeKind === 'memory') {
  describe('the contract suite', () => {
    it("fails a store that loses a wake-up, one that skips the fence, one that ignores dedupe ids, one that drops the custom status, one that ignores terminates, ones that drop a write's signal or the parent filter, ones that ignore concurrency limits, rate limits or priorities, and ones that ignore a schedule's lease, fence or revision", async () => {
      /** Registers waits without looking for signals that arrived after the execution's cursor. */
      class NoMissedSignalCheck extends InMemoryWorkflowStore {
        override async write(...[id, token, write]: Parameters<InMemoryWorkflowStore['write']>) {
          return super.write(id, token, write.release ? { ...write, release: { ...write.release, signalCursor: Number.MAX_SAFE_INTEGER } } : write);
        }
      }

      /** Checks the lease's expiry instead of its token. */
      class ExpiryFence extends InMemoryWorkflowStore {
        override async write(...[id, , write]: Parameters<InMemoryWorkflowStore['write']>) {
          const instance = await this.get(id);
          return instance?.leaseUntil != null && instance.leaseUntil >= write.now ? super.write(id, (this as any).rows.get(id).leaseToken, write) : false;
        }
      }

      /** Drops the custom status. */
      class NoCustomStatus extends InMemoryWorkflowStore {
        override async write(...[id, token, write]: Parameters<InMemoryWorkflowStore['write']>) {
          return super.write(id, token, { ...write, customStatus: undefined });
        }
      }

      /** Takes a terminate for a cancel. */
      class NoTerminate extends InMemoryWorkflowStore {
        override async requestCancel(...[id, request]: Parameters<InMemoryWorkflowStore['requestCancel']>) {
          return super.requestCancel(id, { ...request, terminate: false });
        }
      }

      /** Drops the signal a write carries. */
      class NoWriteSignal extends InMemoryWorkflowStore {
        override async write(...[id, token, write]: Parameters<InMemoryWorkflowStore['write']>) {
          return super.write(id, token, { ...write, signal: undefined });
        }
      }

      /** Lists every instance, whatever parentId asks for. */
      class NoParentFilter extends InMemoryWorkflowStore {
        override async list(...[query]: Parameters<InMemoryWorkflowStore['list']>) {
          return super.list({ ...query, parentId: undefined });
        }
      }

      /** Claims past every concurrency limit. */
      class NoLimits extends InMemoryWorkflowStore {
        override async claim(...[request]: Parameters<InMemoryWorkflowStore['claim']>) {
          return super.claim({ ...request, limits: [] });
        }
      }

      /** Claims past every rate limit. */
      class NoRateLimits extends InMemoryWorkflowStore {
        override async claim(...[request]: Parameters<InMemoryWorkflowStore['claim']>) {
          return super.claim({ ...request, rateLimits: [] });
        }
      }

      /** Stores every instance without its priority. */
      class NoPriority extends InMemoryWorkflowStore {
        override async create(...[instance]: Parameters<InMemoryWorkflowStore['create']>) {
          return super.create({ ...instance, priority: 0 });
        }
      }

      /** Claims schedules whatever their leases. */
      class NoScheduleLease extends InMemoryWorkflowStore {
        override async claimSchedules(...[request]: Parameters<InMemoryWorkflowStore['claimSchedules']>) {
          const all = await this.listSchedules({ limit: 1_000, offset: 0 });
          for (const schedule of all) {
            await this.writeSchedule(schedule.id, (this as any).schedules.get(schedule.id).leaseToken ?? '', { now: request.now, state: schedule.state, wakeAt: schedule.wakeAt, release: true });
          }
          return super.claimSchedules(request);
        }
      }

      /** Takes any token for a schedule's lease. */
      class NoScheduleFence extends InMemoryWorkflowStore {
        override async writeSchedule(...[id, , write]: Parameters<InMemoryWorkflowStore['writeSchedule']>) {
          const schedule = (this as any).schedules.get(id);
          return schedule ? super.writeSchedule(id, schedule.leaseToken ?? (schedule.leaseToken = 'any'), write) : false;
        }
      }

      /** Saves schedules whatever their revision. */
      class NoScheduleRevision extends InMemoryWorkflowStore {
        override async saveSchedule(...[save]: Parameters<InMemoryWorkflowStore['saveSchedule']>) {
          const current = await this.getSchedule(save.id);
          return super.saveSchedule({ ...save, expectRevision: current?.revision ?? null });
        }
      }

      /** Stores every signal, dedupe id or not. */
      class NoDedupe extends InMemoryWorkflowStore {
        override async signal(...[signal]: Parameters<InMemoryWorkflowStore['signal']>) {
          return super.signal({ ...signal, dedupeId: null });
        }
      }

      const failures = async (store: () => WorkflowStore) => {
        const failed: string[] = [];
        for (const c of workflowStoreContract(store, { concurrent: true })) {
          await c.run().catch(() => failed.push(c.name));
        }
        return failed;
      };

      expect(await failures(() => new NoMissedSignalCheck())).toEqual(
        expect.arrayContaining(['write() keeps an instance due when a signal for its new waits arrived after its cursor']),
      );
      expect(await failures(() => new NoDedupe())).toEqual([
        'write() with a signal records it as signal() does, only when the write lands',
        'signal() with a dedupeId stores the signal once per name, and a repeat writes and wakes nothing',
        'concurrent signals with one dedupeId store it once, and every call returns its id',
      ]);
      expect(await failures(() => new NoCustomStatus())).toEqual(['write() sets the custom status under the lease, and leaves it as it is when not given']);
      expect(await failures(() => new NoTerminate())).toEqual([
        'renew() extends the lease and reads the cancel flags while the token is current, and returns null after',
        'requestCancel() with terminate accepts once, also after a cancel and for a compensating instance',
        'concurrent create(), requestCancel() and terminates of one id: one wins',
      ]);
      expect(await failures(() => new NoWriteSignal())).toEqual([
        'write() with a signal records it as signal() does, only when the write lands',
        "a child's ending write racing its parent's suspension never loses the wake-up",
      ]);
      expect(await failures(() => new NoParentFilter())).toEqual(['create() keeps the parent link, and list() finds the children by parentId']);
      expect(await failures(() => new NoLimits())).toEqual([
        "claim() under a workflow's limit leases no more than it allows, counting the leases still live",
        'claim() keeps at most perKey of a key leased, passes over a full key, and takes each key in order',
        'claim() takes the lowest priority first (none before any), then the most overdue, with or without limits',
        'claim() applies rate limits and concurrency limits together, in stages',
        'concurrent claims never lease more than a limit allows, and fill every free slot',
      ]);
      expect(await failures(() => new NoRateLimits())).toEqual([
        "claim() under a workflow's rate limit starts at most max per window, and opens the next window at the first claim after it ended",
        'claim() under per-key rate limits keeps each key to its window, passes over a full key, and counts keyless instances only toward the workflow',
        'claim() applies rate limits and concurrency limits together, in stages',
        'purge() deletes the rate-limit windows that ended before `before`, oldest first, and keeps the open ones',
        'concurrent claims never start more than a rate limit allows in a window, and fill its room',
        'purge() racing claims that reuse ended rate-limit windows neither deadlocks nor lets a window overflow',
      ]);
      expect(await failures(() => new NoPriority())).toEqual([
        'claim() takes the lowest priority first (none before any), then the most overdue, with or without limits',
      ]);
      expect(await failures(() => new NoScheduleLease())).toEqual([
        'claimSchedules() leases due, unpaused, unleased schedules of the given workflows, most overdue first',
        'concurrent claimSchedules() never return the same schedule twice, and saves of one revision land once',
      ]);
      expect(await failures(() => new NoScheduleFence())).toEqual([
        'writeSchedule() writes only under the lease token, and a save that releases the lease makes the token stale',
      ]);
      expect(await failures(() => new NoScheduleRevision())).toEqual([
        'saveSchedule() creates a schedule once, and replaces one only at the revision it read',
        'concurrent claimSchedules() never return the same schedule twice, and saves of one revision land once',
      ]);
      expect(await failures(() => new ExpiryFence())).toEqual(
        expect.arrayContaining(['write() changes nothing under a stale token', "a stale lease holder's writes never land, however they interleave with the new holder's"]),
      );
    });
  });
}
