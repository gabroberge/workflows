import { fileURLToPath } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import { Inject, Injectable, Module, type DynamicModule, type OnApplicationShutdown, type Provider, type Type } from '@nestjs/common';
import { getDrizzleToken } from '@nestjs/drizzle';
import { Test, type TestingModule } from '@nestjs/testing';
import { sql } from 'drizzle-orm';
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres';
import { migrate as migratePg } from 'drizzle-orm/node-postgres/migrator';
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite';
import { migrate as migratePglite } from 'drizzle-orm/pglite/migrator';
import pg from 'pg';
import type { Database } from './fixtures/database/drizzle.js';
import { DrizzleWorkflowStore } from './fixtures/database/drizzle-workflow.store.js';
import * as schema from './fixtures/database/schema.js';
import { startPostgres } from './support/postgres.js';
import {
  InMemoryWorkflowStore,
  WorkflowClient,
  WorkflowEvents,
  WorkflowsModule,
  WorkflowStorage,
  WorkflowWorker,
  type WorkflowClock,
  type WorkflowEvent,
  type WorkflowRetryOptions,
  type WorkflowStore,
  type WorkflowWorkerOptions,
} from '../lib/index.js';

/**
 * The engine suites run once per store, chosen by `WORKFLOWS_TEST_STORE`
 * (vitest.config.ts runs one project per value, so a plain `npx vitest run` runs all three):
 *
 * - `memory` (the default): `InMemoryWorkflowStore`. A "restart" registers the same store
 *   object in the new application, as a database outlives a process.
 * - `pglite`: the workflows tutorial's `DrizzleWorkflowStore`, with its drizzle-kit
 *   migrations, on PGlite (PostgreSQL in-process, one connection). A restart opens a new
 *   Drizzle instance on the same PGlite.
 * - `postgres`: the same store on a PostgreSQL server: `SQL_TEST_PG_URL`, else a throwaway
 *   cluster from local binaries, else every test is skipped with the reason. Every
 *   application opens its own pool, so several on one database race on real connections.
 *
 * One database per test file, emptied by `tempDb()` for each test (one test database at a time).
 */
export type StoreKind = 'memory' | 'pglite' | 'postgres';
export const storeKind = (process.env.WORKFLOWS_TEST_STORE ?? 'memory') as StoreKind;
if (!['memory', 'pglite', 'postgres'].includes(storeKind)) {
  throw new Error(`WORKFLOWS_TEST_STORE must be memory, pglite or postgres, not "${storeKind}".`);
}
export const storeLabel = {
  memory: 'InMemoryWorkflowStore',
  pglite: 'DrizzleWorkflowStore on PGlite',
  postgres: 'DrizzleWorkflowStore on PostgreSQL',
}[storeKind];

const migrationsFolder = fileURLToPath(new URL('./fixtures/drizzle', import.meta.url));

/** A database the Drizzle store runs on: emptied per test, and connected to once per "process". */
interface SqlBackend {
  reset(): Promise<void>;
  connect(): { db: Database; close(): Promise<void> };
}

const truncate = async (db: Database) => {
  await db.execute(sql`TRUNCATE workflow_instances, workflow_journal, workflow_waits, workflow_signals RESTART IDENTITY`);
};

async function pgliteBackend(): Promise<SqlBackend> {
  const client = new PGlite();
  const db = drizzlePglite(client, { schema }) as unknown as Database;
  await migratePglite(db as never, { migrationsFolder });
  afterAll(() => client.close());

  return {
    reset: () => truncate(db),
    // PGlite is the server: a new "process" gets a new Drizzle instance on it, and closes nothing.
    connect: () => ({ db: drizzlePglite(client, { schema }) as unknown as Database, close: async () => undefined }),
  };
}

async function postgresBackend(): Promise<SqlBackend | string> {
  const { postgres, reason } = await startPostgres();
  if (!postgres) {
    return reason;
  }

  const url = await postgres.createDatabase('workflows_engine');
  const admin = new pg.Pool({ connectionString: url, max: 2 });
  const adminDb = drizzlePg(admin, { schema });
  await migratePg(adminDb, { migrationsFolder });

  afterAll(async () => {
    await admin.end();
    postgres.stop();
  });

  return {
    reset: () => truncate(adminDb),
    connect: () => {
      const pool = new pg.Pool({ connectionString: url, max: 10 });
      let ended: Promise<void> | undefined;
      return { db: drizzlePg(pool, { schema }), close: () => (ended ??= pool.end()) };
    },
  };
}

const backend = storeKind === 'pglite' ? await pgliteBackend() : storeKind === 'postgres' ? await postgresBackend() : null;
const sqlBackend = typeof backend === 'string' ? null : backend;

/** Why this store's tests are skipped (no PostgreSQL server), if they are. */
export const skipReason = typeof backend === 'string' ? `no PostgreSQL: ${backend}` : undefined;
if (skipReason) {
  beforeEach((context) => context.skip(skipReason));
}

const memory = new Map<string, InMemoryWorkflowStore>();
let databases = 0;

export interface TestDb {
  /** Names the database a "restart" reopens. */
  name: string;
  cleanup(): void;
}

/** A fresh database per test, so a "restart" can reopen the same one. */
export async function tempDb(): Promise<TestDb> {
  const name = `workflows-${++databases}`;
  await sqlBackend?.reset();
  return { name, cleanup: () => void memory.delete(name) };
}

export interface Connection {
  /** The Drizzle database, or (in memory) the database's one store object. */
  db: Database | InMemoryWorkflowStore;
  close(): Promise<void>;
}

/** A connection to the test database, as a newly started process opens one. */
export function connect(db: TestDb): Connection {
  if (sqlBackend) {
    return sqlBackend.connect();
  }

  let store = memory.get(db.name);
  if (!store) {
    memory.set(db.name, (store = new InMemoryWorkflowStore()));
  }
  return { db: store, close: async () => undefined };
}

/** A store on the test database outside any application, to look at what one left behind. */
export function openStore(db: TestDb): { store: WorkflowStore; close(): Promise<void> } {
  const connection = connect(db);
  const store = connection.db instanceof InMemoryWorkflowStore ? connection.db : new DrizzleWorkflowStore(connection.db, new WorkflowStorage());
  return { store, close: () => connection.close() };
}

/** The connection a `databaseModule()` provides. */
export const DATABASE = Symbol('DATABASE');

/** The app's store provider in the memory runs: over the test database's in-memory store. */
@Injectable()
class InMemoryAppStore implements WorkflowStore {
  private readonly db: InMemoryWorkflowStore;

  constructor(@Inject(DATABASE) connection: Connection, storage: WorkflowStorage) {
    this.db = connection.db as InMemoryWorkflowStore;
    storage.registerSource(this);
  }

  create(...args: Parameters<WorkflowStore['create']>) {
    return this.db.create(...args);
  }
  createInTransaction(...args: Parameters<InMemoryWorkflowStore['createInTransaction']>) {
    return this.db.createInTransaction(...args);
  }
  get(...args: Parameters<WorkflowStore['get']>) {
    return this.db.get(...args);
  }
  list(...args: Parameters<WorkflowStore['list']>) {
    return this.db.list(...args);
  }
  requestCancel(...args: Parameters<WorkflowStore['requestCancel']>) {
    return this.db.requestCancel(...args);
  }
  signal(...args: Parameters<WorkflowStore['signal']>) {
    return this.db.signal(...args);
  }
  signalInTransaction(...args: Parameters<InMemoryWorkflowStore['signalInTransaction']>) {
    return this.db.signalInTransaction(...args);
  }
  signals(...args: Parameters<WorkflowStore['signals']>) {
    return this.db.signals(...args);
  }
  claim(...args: Parameters<WorkflowStore['claim']>) {
    return this.db.claim(...args);
  }
  renew(...args: Parameters<WorkflowStore['renew']>) {
    return this.db.renew(...args);
  }
  write(...args: Parameters<WorkflowStore['write']>) {
    return this.db.write(...args);
  }
}

/**
 * The store provider as an app writes one: it injects its database (from `databaseModule()`)
 * and registers itself in its constructor. The tutorial's `DrizzleWorkflowStore`, or in the
 * memory runs a provider over the test database's in-memory store. A subclass that declares no
 * constructor (and no decorator) keeps the injection.
 */
export const AppWorkflowStore: Type<WorkflowStore> = sqlBackend ? DrizzleWorkflowStore : InMemoryAppStore;

/**
 * The app's database module on the test database, like a hand-written database module: it provides
 * the connection (`DATABASE`, and the Drizzle store's database under `getDrizzleToken()`) and closes it in its own
 * `onApplicationShutdown()`, which it records in `shutdowns`.
 */
export function databaseModule(db: TestDb, shutdowns: string[] = []): Type<unknown> {
  @Module({
    providers: [
      { provide: DATABASE, useFactory: () => connect(db) },
      { provide: getDrizzleToken(), inject: [DATABASE], useFactory: (connection: Connection) => connection.db },
    ],
    exports: [DATABASE, getDrizzleToken()],
  })
  class DatabaseModule implements OnApplicationShutdown {
    constructor(@Inject(DATABASE) private readonly connection: Connection) {}

    async onApplicationShutdown() {
      shutdowns.push('close');
      await this.connection.close();
    }
  }
  return DatabaseModule;
}

/**
 * The outside world (payment provider, warehouse, mail server). It survives
 * application restarts, so it is where "exactly once" is measured.
 */
export class World {
  readonly calls: Array<{ op: string; key: string; attempt?: number }> = [];

  record(op: string, key: string, attempt?: number) {
    this.calls.push({ op, key, attempt });
  }

  count(op: string) {
    return this.calls.filter((call) => call.op === op).length;
  }

  ops() {
    return this.calls.map((call) => call.op);
  }
}

export interface Node {
  moduleRef: TestingModule;
  client: WorkflowClient;
  worker: WorkflowWorker;
  /** The registered store: the database's in-memory store, or a DrizzleWorkflowStore on this node's connection. */
  store: WorkflowStore;
  events: WorkflowEvent[];
  /** Graceful shutdown (app.close()), then the node's connection is closed. */
  close(): Promise<void>;
}

/** Boots one application instance ("node") on the given test database. */
export async function boot(options: {
  db: TestDb;
  workflows: Type<unknown>[];
  providers?: Provider[];
  /** More modules next to `WorkflowsModule`, such as `CqrsModule.forRoot()`. */
  imports?: Array<Type<unknown> | DynamicModule>;
  clock?: WorkflowClock;
  worker?: WorkflowWorkerOptions;
  retry?: number | false | WorkflowRetryOptions;
}): Promise<Node> {
  const connection = connect(options.db);
  const drizzle = connection.db instanceof InMemoryWorkflowStore ? null : connection.db;
  const moduleRef = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        clock: options.clock,
        retry: options.retry,
        worker: { enabled: false, shutdownTimeout: 50, ...options.worker },
      }),
      ...(options.imports ?? []),
    ],
    providers: [
      ...options.workflows,
      ...(options.providers ?? []),
      // As an app registers it: a provider that injects the database and registers itself.
      ...(drizzle ? [DrizzleWorkflowStore, { provide: getDrizzleToken(), useValue: drizzle }] : []),
    ],
  }).compile();

  const store = drizzle ? moduleRef.get(DrizzleWorkflowStore) : (connection.db as InMemoryWorkflowStore);
  if (!drizzle) {
    moduleRef.get(WorkflowStorage).registerSource(store);
  }
  await moduleRef.init();

  const events: WorkflowEvent[] = [];
  moduleRef.get(WorkflowEvents).events$.subscribe((event) => events.push(event));

  return {
    moduleRef,
    client: moduleRef.get(WorkflowClient),
    worker: moduleRef.get(WorkflowWorker),
    store,
    events,
    close: async () => {
      await moduleRef.close();
      await connection.close();
    },
  };
}

export function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

/** A promise that never settles: a step that is "running" when the process dies. */
export const forever = () => new Promise<never>(() => undefined);

export async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
