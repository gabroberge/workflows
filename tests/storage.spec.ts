/**
 * WorkflowStorage, the registry a store provider registers with (CONVENTIONS.md, rule 2):
 * shape validation, one source unless replaced on purpose, the lock at module init or first
 * read (an internal symbol), the log line, and the production guard.
 */
import { Injectable, Logger, Module, type OnApplicationBootstrap, type OnModuleInit } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import {
  InMemoryWorkflowStore,
  Workflow,
  WorkflowClient,
  WorkflowsModule,
  WorkflowStorage,
  WorkflowWorker,
  type WorkflowContext,
  type WorkflowStore,
} from '../lib/index.js';
import { LOCK_STORAGE } from '../lib/storage/workflow.storage.js';

@Workflow('ping')
class Ping {
  async run(ctx: WorkflowContext) {
    return ctx.step('pong', () => 'pong');
  }
}

/** A store provider, as an application writes one (on the in-memory store, for the test). */
@Injectable()
class AppStore extends InMemoryWorkflowStore {
  constructor(storage: WorkflowStorage) {
    super();
    storage.registerSource(this);
  }
}

@Injectable()
class OtherStore extends InMemoryWorkflowStore {
  constructor(storage: WorkflowStorage) {
    super();
    storage.registerSource(this);
  }
}

const boot = async (providers: any[], options: object = {}) => {
  const moduleRef = await Test.createTestingModule({
    imports: [WorkflowsModule.forRoot({ worker: false, ...options })],
    providers: [Ping, ...providers],
  }).compile();
  return moduleRef;
};

let logs: string[];
let warnings: string[];
beforeEach(() => {
  logs = [];
  warnings = [];
  vi.spyOn(Logger.prototype, 'log').mockImplementation(function (this: Logger, message: unknown) {
    logs.push(String(message));
  });
  vi.spyOn(Logger.prototype, 'warn').mockImplementation(function (this: Logger, message: unknown) {
    warnings.push(String(message));
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

describe('WorkflowStorage.registerSource()', () => {
  it('makes the registered provider the store, and logs it when the registry locks', async () => {
    const moduleRef = await boot([AppStore]);
    expect(logs).not.toContain('WorkflowStorage: AppStore');
    await moduleRef.init();
    expect(logs).toContain('WorkflowStorage: AppStore');

    const store = moduleRef.get(AppStore);
    expect(moduleRef.get(WorkflowStorage).source).toBe(store);

    await moduleRef.get(WorkflowClient).start(Ping, undefined, { id: 'p' });
    await moduleRef.get(WorkflowWorker).drain();
    expect(await store.get('p')).toMatchObject({ status: 'completed', output: 'pong' });
    await moduleRef.close();
  });

  it('falls back to the in-memory store, and says so', async () => {
    const moduleRef = await boot([]);
    await moduleRef.init();
    expect(logs).toContain('WorkflowStorage: InMemoryWorkflowStore (the default: state is lost on restart and not shared between instances)');
    expect(logs.filter((line) => line.startsWith('WorkflowStorage:'))).toHaveLength(1);
    expect(moduleRef.get(WorkflowStorage).source).toBeInstanceOf(InMemoryWorkflowStore);
    await moduleRef.close();
  });

  it('validates the shape at once, naming every missing method', () => {
    const storage = new WorkflowStorage();
    class HalfDone {
      create() {}
      get() {}
      list() {}
      signal() {}
    }

    expect(() => storage.registerSource(new HalfDone() as unknown as WorkflowStore)).toThrow(
      "WorkflowStorage.registerSource(): HalfDone doesn't implement WorkflowStore: requestCancel(), signals(), claim(), renew(), write() are missing.",
    );

    const withBadOptional = Object.assign(new InMemoryWorkflowStore(), { createInTransaction: 'yes' });
    expect(() => storage.registerSource(withBadOptional as unknown as WorkflowStore)).toThrow(
      "WorkflowStorage.registerSource(): InMemoryWorkflowStore doesn't implement WorkflowStore: createInTransaction is set but not a method.",
    );

    expect(() => storage.registerSource(undefined as unknown as WorkflowStore)).toThrow(
      'WorkflowStorage.registerSource(): expected an object implementing WorkflowStore, got undefined.',
    );
    expect(() => storage.registerSource(InMemoryWorkflowStore as unknown as WorkflowStore)).toThrow(
      'WorkflowStorage.registerSource(): expected an object implementing WorkflowStore, got the class InMemoryWorkflowStore (pass an instance).',
    );

    // Nothing was registered by the failed calls.
    storage.registerSource(new InMemoryWorkflowStore());
  });

  it('refuses a second source, naming both, unless it replaces the first on purpose', async () => {
    await expect(boot([AppStore, OtherStore])).rejects.toThrow(
      "WorkflowStorage.registerSource(): OtherStore can't register, AppStore already did. " +
        'Register one store, or pass { replace: true } to replace it on purpose (tests, wrappers).',
    );

    // A test swaps the app's store for a fake before init().
    const moduleRef = await boot([AppStore]);
    const fake = new InMemoryWorkflowStore();
    moduleRef.get(WorkflowStorage).registerSource(fake, { replace: true });
    await moduleRef.init();
    expect(moduleRef.get(WorkflowStorage).source).toBe(fake);
    expect(logs).toContain('WorkflowStorage: InMemoryWorkflowStore');
    await moduleRef.close();

    // The same instance twice is a second registration too.
    const storage = new WorkflowStorage();
    const store = new InMemoryWorkflowStore();
    storage.registerSource(store);
    expect(() => storage.registerSource(store)).toThrow(
      "WorkflowStorage.registerSource(): InMemoryWorkflowStore can't register, it already did (the same instance, twice).",
    );
  });

  it('locks when the module initializes: a later registration throws', async () => {
    @Injectable()
    class LateStore extends InMemoryWorkflowStore implements OnApplicationBootstrap {
      constructor(private readonly storage: WorkflowStorage) {
        super();
      }
      onApplicationBootstrap() {
        this.storage.registerSource(this); // too late: the worker may already be running
      }
    }

    const moduleRef = await boot([LateStore]);
    await expect(moduleRef.init()).rejects.toThrow(
      'WorkflowStorage.registerSource(): LateStore registered after WorkflowsModule initialized (or after its storage ' +
        'was first read), which already uses InMemoryWorkflowStore (the default: state is lost on restart and not ' +
        'shared between instances). Register from the constructor of a singleton provider: providers of lazy-loaded ' +
        'modules, request-scoped and transient providers, and lifecycle hooks run too late.',
    );
    await moduleRef.close().catch(() => undefined); // close() rethrows the failed init

    const started = await boot([AppStore]);
    await started.init();
    expect(() => started.get(WorkflowStorage).registerSource(new InMemoryWorkflowStore(), { replace: true })).toThrow(
      'registered after WorkflowsModule initialized (or after its storage was first read), which already uses AppStore.',
    );
    await started.close();
  });

  it("locks at the first read when that comes before the module's onModuleInit", async () => {
    const seen: string[] = [];
    let ref!: TestingModule;
    /** A provider of a module initialized before WorkflowsModule's hook reads the store. */
    @Injectable()
    class EarlyReader implements OnModuleInit {
      async onModuleInit() {
        seen.push(ref.get(WorkflowStorage).source.constructor.name);
        expect(() => ref.get(WorkflowStorage).registerSource(new InMemoryWorkflowStore(), { replace: true })).toThrow(
          'registered after WorkflowsModule initialized (or after its storage was first read)',
        );
      }
    }

    @Module({ providers: [EarlyReader] })
    class EarlyModule {}
    @Module({ imports: [EarlyModule] })
    class FeatureModule {}

    ref = await Test.createTestingModule({
      imports: [WorkflowsModule.forRoot({ worker: false }), FeatureModule],
      providers: [Ping, AppStore],
    }).compile();
    await ref.init();
    expect(seen).toEqual(['AppStore']);
    expect(logs.filter((line) => line.startsWith('WorkflowStorage:'))).toEqual(['WorkflowStorage: AppStore']);
    await ref.close();

    const storage = new WorkflowStorage();
    expect(storage.source).toBeInstanceOf(InMemoryWorkflowStore);
    expect(() => storage.registerSource(new InMemoryWorkflowStore())).toThrow('registered after WorkflowsModule initialized');
  });

  it('keeps the lock internal: no public lock(), and locking again changes nothing', async () => {
    const storage = new WorkflowStorage();
    expect('lock' in storage).toBe(false);

    const store = new InMemoryWorkflowStore();
    storage.registerSource(store);
    storage[LOCK_STORAGE]();
    storage[LOCK_STORAGE]();

    expect(storage.source).toBe(store);
    expect(logs.filter((line) => line.startsWith('WorkflowStorage:'))).toHaveLength(1);
  });

  it('works when WorkflowsModule is not global and the store lives in another module that imports it', async () => {
    @Module({ imports: [WorkflowsModule.forRoot({ isGlobal: false, worker: false })], providers: [AppStore] })
    class PersistenceModule {}

    const moduleRef = await Test.createTestingModule({ imports: [PersistenceModule] }).compile();
    await moduleRef.init();
    expect(logs).toContain('WorkflowStorage: AppStore');
    await moduleRef.close();
  });
});

describe('the in-memory default and transactions', () => {
  it("runs start() and signal() with { transaction } at once, and warns once that it can't join it", async () => {
    const moduleRef = await boot([]);
    await moduleRef.init();
    const client = moduleRef.get(WorkflowClient);

    const tx = { some: 'transaction of your ORM' };
    expect(await client.start(Ping, undefined, { id: 'p1', transaction: tx })).toMatchObject({ created: true });
    await client.signal('go', 1, { transaction: tx });
    await client.start(Ping, undefined, { id: 'p2', transaction: tx });

    expect(warnings).toEqual([
      "InMemoryWorkflowStore.createInTransaction() received your transaction handle, and can't join it: the write " +
        "applies at once, so a rolled-back transaction won't undo it. Register a store for your database with " +
        'WorkflowStorage.registerSource(). (Logged once.)',
    ]);
    expect(await client.getStatus('p2')).toMatchObject({ status: 'pending' });
    await moduleRef.close();

    // Once per store: another one warns again, here from signalInTransaction().
    await new InMemoryWorkflowStore().signalInTransaction(tx, { name: 'go', key: null, dedupeId: null, payload: 1, now: 1 });
    expect(warnings[1]).toMatch(/^InMemoryWorkflowStore\.signalInTransaction\(\) received your transaction handle/);
  });
});

describe('the production guard', () => {
  it('refuses the in-memory default when NODE_ENV is production', async () => {
    vi.stubEnv('NODE_ENV', 'production');
    const moduleRef = await boot([]);
    await expect(moduleRef.init()).rejects.toThrow(
      'WorkflowStorage: no WorkflowStore is registered, and NODE_ENV is "production": in memory, running workflows ' +
        'would be lost on restart and not shared between instances. Implement WorkflowStore in a provider that ' +
        'injects WorkflowStorage and calls `storage.registerSource(this)` in its constructor, or set ' +
        '`allowInMemoryStorage: true` in the WorkflowsModule options to run in memory anyway.',
    );
    await moduleRef.close().catch(() => undefined); // close() rethrows the failed init
  });

  it('boots in production with a registered store, or with allowInMemoryStorage', async () => {
    vi.stubEnv('NODE_ENV', 'production');
    const registered = await boot([AppStore]);
    await registered.init();
    await registered.close();

    const allowed = await boot([], { allowInMemoryStorage: true });
    await allowed.init();
    expect(allowed.get(WorkflowStorage).source).toBeInstanceOf(InMemoryWorkflowStore);
    await allowed.close();
  });

  it('reads allowInMemoryStorage from forRootAsync()', async () => {
    vi.stubEnv('NODE_ENV', 'production');
    const moduleRef = await Test.createTestingModule({
      imports: [WorkflowsModule.forRootAsync({ useFactory: async () => ({ worker: false as const, allowInMemoryStorage: true }) })],
    }).compile();
    await moduleRef.init();
    await moduleRef.close();
  });
});
