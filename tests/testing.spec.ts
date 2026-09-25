/**
 * The testing tools on their own: the contract suite's options (which cases it adds, `dispose`,
 * a `transaction` option on a store that can't join one), and `ManualWorkflowClock`. They need
 * no store of the run's kind, so they run once.
 */
import { InMemoryWorkflowStore, ManualWorkflowClock, type WorkflowStore } from '../lib/index.js';
import { workflowStoreContract } from '../lib/testing/index.js';
import { storeKind } from './support.js';

describe.runIf(storeKind === 'memory')('workflowStoreContract()', () => {
  it('adds the transaction and concurrency cases only when asked, under unique names', () => {
    const create = () => new InMemoryWorkflowStore();
    const base = workflowStoreContract(create).map((c) => c.name);
    const withTransaction = workflowStoreContract(create, { transaction: (work) => work({}) }).map((c) => c.name);
    const concurrent = workflowStoreContract(create, { concurrent: true }).map((c) => c.name);

    expect(new Set(base).size).toBe(base.length);
    expect(new Set(concurrent).size).toBe(concurrent.length);

    expect(withTransaction.filter((name) => !base.includes(name))).toEqual([
      "createInTransaction() and signalInTransaction() commit and roll back with the application's transaction",
    ]);
    const added = concurrent.filter((name) => !base.includes(name));
    expect(added).toEqual(expect.arrayContaining(['concurrent claims never return the same instance twice', 'concurrent create() and requestCancel() of one id: one wins']));
    expect(concurrent.slice(0, base.length)).toEqual(base);
  });

  it('creates a store per case and disposes of it after the case, passed or failed', async () => {
    const created: WorkflowStore[] = [];
    const disposed: WorkflowStore[] = [];

    /** Forgets every journal entry: the journal cases fail, the others pass. */
    class ForgetfulStore extends InMemoryWorkflowStore {
      override async get(...args: Parameters<InMemoryWorkflowStore['get']>) {
        const details = await super.get(...args);
        return details && args[1]?.journal ? { ...details, journal: [] } : details;
      }
    }

    const cases = workflowStoreContract(
      () => {
        const store = new ForgetfulStore();
        created.push(store);
        return store;
      },
      { dispose: (store) => void disposed.push(store) },
    );

    const outcomes: boolean[] = [];
    for (const c of cases) {
      outcomes.push(await c.run().then(() => true, () => false));
    }

    expect(outcomes).toContain(true);
    expect(outcomes).toContain(false);
    expect(created).toHaveLength(cases.length);
    expect(new Set(created).size).toBe(cases.length);
    expect(disposed).toEqual(created);
  });

  it('passes on the in-memory store without the optional cases, and says what a failing case expected', async () => {
    for (const c of workflowStoreContract(() => new InMemoryWorkflowStore())) {
      await c.run();
    }

    /** Hands back every claim with another owner than the caller's. */
    class WrongOwner extends InMemoryWorkflowStore {
      override async claim(...[request]: Parameters<InMemoryWorkflowStore['claim']>) {
        return super.claim({ ...request, owner: 'someone-else' });
      }
    }

    const failures: string[] = [];
    for (const c of workflowStoreContract(() => new WrongOwner())) {
      await c.run().catch((error: Error) => failures.push(error.message));
    }
    expect(failures.length).toBeGreaterThan(0);
    expect(failures.join('\n')).toContain('someone-else');
  });

  it("fails the transaction case, naming what's missing, for a store without the optional methods", async () => {
    const bare = () => {
      const store = new InMemoryWorkflowStore();
      return Object.assign(Object.create(store), { createInTransaction: undefined, signalInTransaction: undefined }) as WorkflowStore;
    };

    const [transactionCase] = workflowStoreContract(bare, { transaction: (work) => work({}) }).filter((c) => c.name.includes('InTransaction'));
    await expect(transactionCase.run()).rejects.toThrow(
      'options.transaction is set, but the store lacks createInTransaction() or signalInTransaction().',
    );
  });
});

describe.runIf(storeKind === 'memory')('ManualWorkflowClock', () => {
  it('starts at 2026-01-01 UTC, or at the time it is given', () => {
    expect(new ManualWorkflowClock().now()).toBe(Date.UTC(2026, 0, 1));
    expect(new ManualWorkflowClock(42).now()).toBe(42);
  });

  it('moves only when advanced or set', () => {
    const clock = new ManualWorkflowClock(0);
    expect(clock.now()).toBe(0);

    expect(clock.advance('1.5s')).toBe(1_500);
    expect(clock.advance(500)).toBe(2_000);
    expect(clock.advance('1d')).toBe(2_000 + 86_400_000);

    clock.set(new Date('2030-06-01T00:00:00Z'));
    expect(clock.now()).toBe(Date.UTC(2030, 5, 1));
    clock.set(7);
    expect(clock.now()).toBe(7);
  });

  it('refuses to move by an invalid duration, and stays put', () => {
    const clock = new ManualWorkflowClock(0);
    expect(() => clock.advance(-1)).toThrow('Invalid duration -1.');
    expect(() => clock.advance('tomorrow' as '1d')).toThrow('Invalid duration "tomorrow".');
    expect(clock.now()).toBe(0);
  });
});
