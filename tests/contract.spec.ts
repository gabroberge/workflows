/**
 * The WorkflowStore contract (lib/testing/index.ts, `@nestjs/workflows/testing`) on the store
 * this run uses (support.ts, `WORKFLOWS_TEST_STORE`). The in-memory store's calls never
 * interleave (it does its work before its first await), so the concurrency cases check that it
 * survives being raced. The workflows tutorial's DrizzleWorkflowStore runs them with its
 * drizzle-kit migrations (copied into fixtures/): on PGlite one statement at a time, and on
 * PostgreSQL on a pool, where they race real connections.
 */
import { DrizzleWorkflowStore } from './fixtures/database/drizzle-workflow.store.js';
import type { Database } from './fixtures/database/drizzle.js';
import { InMemoryWorkflowStore, WorkflowStorage, type WorkflowStore } from '../lib/index.js';
import { workflowStoreContract } from '../lib/testing/index.js';
import { connect, skipReason, storeKind, storeLabel, tempDb, type Connection } from './support.js';

if (storeKind === 'memory') {
  describe('InMemoryWorkflowStore', () => {
    for (const c of workflowStoreContract(() => new InMemoryWorkflowStore(), { concurrent: true })) {
      it(c.name, c.run);
    }
  });
} else {
  describe(storeLabel, () => {
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
    it('fails a store that loses a wake-up, one that skips the fence, and one that ignores dedupe ids', async () => {
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
        'signal() with a dedupeId stores the signal once per name, and a repeat writes and wakes nothing',
        'concurrent signals with one dedupeId store it once, and every call returns its id',
      ]);
      expect(await failures(() => new ExpiryFence())).toEqual(
        expect.arrayContaining(['write() changes nothing under a stale token', "a stale lease holder's writes never land, however they interleave with the new holder's"]),
      );
    });
  });
}
