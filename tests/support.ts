import { fileURLToPath } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import { Inject, Injectable, Module, type DynamicModule, type OnApplicationShutdown, type Provider, type Type } from '@nestjs/common';
import { getDrizzleToken } from '@nestjs/drizzle';
import { Test, type TestingModule } from '@nestjs/testing';
import { sql } from 'drizzle-orm';
import { bigint as mysqlBigint, mysqlTable, varchar } from 'drizzle-orm/mysql-core';
import { drizzle as drizzleMysql } from 'drizzle-orm/mysql2';
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres';
import { migrate as migratePg } from 'drizzle-orm/node-postgres/migrator';
import { bigint, pgSchema, text } from 'drizzle-orm/pg-core';
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite';
import { migrate as migratePglite } from 'drizzle-orm/pglite/migrator';
import mysql from 'mysql2/promise';
import pg from 'pg';
import type { Database } from './fixtures/database/drizzle.js';
import * as schema from './fixtures/database/schema.js';
import type { Database as MySqlDatabase } from './fixtures/mysql/drizzle.js';
import * as mysqlSchema from './fixtures/mysql/schema.js';
import { adminPool, startMysql } from './support/mysql.js';
import { endPool, startPostgres } from './support/postgres.js';
import { fromDrizzle as fromMysqlDrizzle, fromMysql2, MySqlWorkflowStore } from '../lib/mysql/index.js';
import { fromDrizzle, PostgresWorkflowStore } from '../lib/postgres/index.js';
import {
  InMemoryWorkflowStore,
  WorkflowClient,
  WorkflowEvents,
  WorkflowsModule,
  WorkflowStorage,
  WorkflowWorker,
  type WorkflowClock,
  type WorkflowEvent,
  type WorkflowJournalLimits,
  type WorkflowRetryOptions,
  type WorkflowStore,
  type WorkflowsModuleOptions,
  type WorkflowWorkerOptions,
} from '../lib/index.js';

/**
 * The engine suites run once per store, chosen by `WORKFLOWS_TEST_STORE`
 * (vitest.config.ts runs one project per value, so a plain `npx vitest run` runs all four):
 *
 * - `memory` (the default): `InMemoryWorkflowStore`. A "restart" registers the same store
 *   object in the new application, as a database outlives a process.
 * - `pglite`: `PostgresWorkflowStore` (`@nestjs/workflows/postgres`) through `fromDrizzle()`,
 *   as the tutorial's app registers it, on PGlite (PostgreSQL in-process, one connection). A
 *   restart opens a new Drizzle instance on the same PGlite. The database also has the
 *   tutorial's tables (its drizzle-kit migrations): the app's `orders`, and the hand-written
 *   `DrizzleWorkflowStore`'s, which contract.spec.ts checks.
 * - `postgres`: the same on a PostgreSQL server: `SQL_TEST_PG_URL`, else a throwaway
 *   cluster from local binaries, else every test is skipped with the reason. Every
 *   application opens its own pool, so several on one database race on real connections.
 * - `mysql`: `MySqlWorkflowStore` (`@nestjs/workflows/mysql`) through `fromDrizzle()` on
 *   mysql2, on the MySQL of `SQL_TEST_MYSQL_URL`, else every test is skipped with the reason.
 *   The database has the tutorial's `orders` table (its MySQL version, fixtures/mysql). Every
 *   application opens its own pool (small: the server may be shared).
 *
 * One database per test file, emptied by `tempDb()` for each test (one test database at a time).
 */
export type StoreKind = 'memory' | 'pglite' | 'postgres' | 'mysql';
export const storeKind = (process.env.WORKFLOWS_TEST_STORE ?? 'memory') as StoreKind;
if (!['memory', 'pglite', 'postgres', 'mysql'].includes(storeKind)) {
  throw new Error(`WORKFLOWS_TEST_STORE must be memory, pglite, postgres or mysql, not "${storeKind}".`);
}
export const storeLabel = {
  memory: 'InMemoryWorkflowStore',
  pglite: 'PostgresWorkflowStore on PGlite',
  postgres: 'PostgresWorkflowStore on PostgreSQL',
  mysql: 'MySqlWorkflowStore on MySQL',
}[storeKind];

/** The connections each application's pool opens at most on MySQL: the server may be shared. */
const MYSQL_POOL_SIZE = 3;

const migrationsFolder = fileURLToPath(new URL('./fixtures/drizzle', import.meta.url));

/** A database the Drizzle store runs on: emptied per test, and connected to once per "process". */
interface SqlBackend {
  /** The database's URL, on a server (not PGlite). */
  url?: string;
  reset(): Promise<void>;
  /** A connection as an application opens one: a Drizzle database on a pool of its own (the MySQL one on MySQL). */
  connect(): { db: Database; close(): Promise<void> };
}

const truncate = async (db: Database) => {
  await db.execute(
    sql`TRUNCATE nest_workflows.instances, nest_workflows.journal, nest_workflows.waits, nest_workflows.signals, nest_workflows.rate_limits, nest_workflows.schedules RESTART IDENTITY`,
  );
  await db.execute(sql`TRUNCATE workflow_instances, workflow_journal, workflow_waits, workflow_signals, workflow_rate_limits, workflow_schedules RESTART IDENTITY`);
};

/** The tutorial's migrations (its `orders`, and the hand-written store's tables), then PostgresWorkflowStore's own. */
const migrate = async (db: Database, drizzleKit: (db: Database) => Promise<void>) => {
  await drizzleKit(db);
  await new PostgresWorkflowStore({ executor: fromDrizzle(db) }).migrate();
};

async function pgliteBackend(): Promise<SqlBackend> {
  const client = new PGlite();
  const db = drizzlePglite(client, { schema }) as unknown as Database;
  await migrate(db, (db) => migratePglite(db as never, { migrationsFolder }));
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
  await migrate(adminDb, (db) => migratePg(db, { migrationsFolder }));

  afterAll(async () => {
    await endPool(admin);
    await postgres.stop();
  });

  return {
    url,
    reset: () => truncate(adminDb),
    connect: () => {
      const pool = new pg.Pool({ connectionString: url, max: 10 });
      let ended: Promise<void> | undefined;
      return { db: drizzlePg(pool, { schema }), close: () => (ended ??= endPool(pool)) };
    },
  };
}

/** The store's tables on MySQL, emptied for each test: TRUNCATE also starts the signals' ids at 1 again. */
const MYSQL_STORE_TABLES = ['instances', 'journal', 'waits', 'signals', 'schedules', 'rate_limits'].map((table) => `nest_workflows_${table}`);

async function mysqlBackend(): Promise<SqlBackend | string> {
  const { mysql: server, reason } = await startMysql();
  if (!server) {
    return reason;
  }

  const { url } = await server.createDatabase('workflows_engine');
  const admin = adminPool(url);
  // The tutorial's table (its own migration), then MySqlWorkflowStore's.
  await admin.query(mysqlSchema.ORDERS_DDL);
  await new MySqlWorkflowStore({ executor: fromMysql2(admin) }).migrate();

  afterAll(async () => {
    await admin.end();
    await server.stop();
  });

  return {
    url,
    reset: async () => {
      for (const table of MYSQL_STORE_TABLES) {
        await admin.query(`TRUNCATE TABLE \`${table}\``);
      }
    },
    connect: () => {
      const pool = mysql.createPool({ uri: url, connectionLimit: MYSQL_POOL_SIZE });
      let ended: Promise<void> | undefined;
      const db = drizzleMysql(pool, { schema: mysqlSchema, mode: 'default' });
      // Typed as the PostgreSQL database the specs are written against: the query builders they use are the same.
      return { db: db as unknown as Database, close: () => (ended ??= pool.end()) };
    },
  };
}

const backend =
  storeKind === 'pglite' ? await pgliteBackend() : storeKind === 'postgres' ? await postgresBackend() : storeKind === 'mysql' ? await mysqlBackend() : null;
const sqlBackend = typeof backend === 'string' ? null : backend;

/** Why this store's tests are skipped (no PostgreSQL or MySQL server), if they are. */
export const skipReason = typeof backend === 'string' ? `no ${storeKind === 'mysql' ? 'MySQL' : 'PostgreSQL'}: ${backend}` : undefined;
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
  const store =
    connection.db instanceof InMemoryWorkflowStore
      ? connection.db
      : storeKind === 'mysql'
        ? new MySqlWorkflowStore({ executor: fromMysqlDrizzle(connection.db as unknown as MySqlDatabase), migrate: false })
        : new PostgresWorkflowStore({ executor: fromDrizzle(connection.db), migrate: false });
  return { store, close: () => connection.close() };
}

/** The class of this run's store: tests that spy on its methods do it on its prototype. */
export const storeClass: Type<WorkflowStore> = storeKind === 'memory' ? InMemoryWorkflowStore : storeKind === 'mysql' ? MySqlWorkflowStore : PostgresWorkflowStore;

/**
 * The tutorial's `orders` table on this run's database (fixtures/database on PostgreSQL, fixtures/mysql on MySQL),
 * typed as PostgreSQL's: the specs write to it with the query builders both dialects share.
 */
export const orders = (storeKind === 'mysql' ? mysqlSchema.orders : schema.orders) as unknown as typeof schema.orders;

const pgSignals = pgSchema('nest_workflows').table('signals', {
  id: bigint('id', { mode: 'number' }).notNull(),
  name: text('name').notNull(),
  key: text('key'),
  dedupeId: text('dedupe_id'),
});
const mysqlSignals = mysqlTable('nest_workflows_signals', {
  id: mysqlBigint('id', { mode: 'number' }).notNull(),
  name: varchar('name', { length: 255 }).notNull(),
  key: varchar('key', { length: 255 }),
  dedupeId: varchar('dedupe_id', { length: 255 }),
});

/** The SQL store's signals, for tests that read what a run left in the database (typed as PostgreSQL's). */
export const storedSignals = (storeKind === 'mysql' ? mysqlSignals : pgSignals) as unknown as typeof pgSignals;

/** Every row of the SQL store's tables, each as JSON: what someone who can read the database sees. */
export async function storeRows(connection: Connection): Promise<string[]> {
  const rows: string[] = [];
  for (const name of ['instances', 'journal', 'waits', 'signals', 'schedules', 'rate_limits']) {
    if (storeKind === 'mysql') {
      const [result] = (await (connection.db as unknown as MySqlDatabase).execute(sql.raw(`SELECT * FROM nest_workflows_${name}`))) as unknown as [object[]];
      rows.push(...result.map((row) => JSON.stringify(row)));
    } else {
      const result = await (connection.db as Database).execute<{ row: string }>(sql.raw(`SELECT row_to_json(t)::text AS row FROM nest_workflows.${name} t`));
      rows.push(...result.rows.map((row) => row.row));
    }
  }
  return rows;
}

/**
 * A connection of its own to kill inside a transaction, as a process that dies mid-transaction does: a Drizzle
 * database on one connection, and `kill(tx)`, which ends that connection from inside the transaction.
 */
export async function killableConnection(): Promise<{ db: Database; kill(tx: unknown): Promise<unknown>; close(): Promise<void> }> {
  if (storeKind === 'mysql') {
    const connection = await mysql.createConnection({ uri: sqlBackend!.url! });
    connection.on('error', () => undefined); // the killed connection reports itself here too
    return {
      db: drizzleMysql(connection, { schema: mysqlSchema, mode: 'default' }) as unknown as Database,
      kill: (tx) => (tx as MySqlDatabase).execute(sql`KILL CONNECTION_ID()`),
      close: () => connection.end().catch(() => undefined),
    };
  }

  const client = new pg.Client({ connectionString: sqlBackend!.url! });
  client.on('error', () => undefined); // the terminated connection reports itself here too
  await client.connect();
  return {
    db: drizzlePg(client, { schema }),
    kill: (tx) => (tx as Database).execute(sql`SELECT pg_terminate_backend(pg_backend_pid())`),
    close: () => client.end().catch(() => undefined),
  };
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
  reopen(...args: Parameters<WorkflowStore['reopen']>) {
    return this.db.reopen(...args);
  }
  delete(...args: Parameters<WorkflowStore['delete']>) {
    return this.db.delete(...args);
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
  purge(...args: Parameters<WorkflowStore['purge']>) {
    return this.db.purge(...args);
  }
  saveSchedule(...args: Parameters<WorkflowStore['saveSchedule']>) {
    return this.db.saveSchedule(...args);
  }
  getSchedule(...args: Parameters<WorkflowStore['getSchedule']>) {
    return this.db.getSchedule(...args);
  }
  listSchedules(...args: Parameters<WorkflowStore['listSchedules']>) {
    return this.db.listSchedules(...args);
  }
  deleteSchedule(...args: Parameters<WorkflowStore['deleteSchedule']>) {
    return this.db.deleteSchedule(...args);
  }
  claimSchedules(...args: Parameters<WorkflowStore['claimSchedules']>) {
    return this.db.claimSchedules(...args);
  }
  writeSchedule(...args: Parameters<WorkflowStore['writeSchedule']>) {
    return this.db.writeSchedule(...args);
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

/** PostgresWorkflowStore as an app registers it: on the Drizzle database it injects. */
@Injectable()
class PostgresAppStore extends PostgresWorkflowStore {
  constructor(@Inject(getDrizzleToken()) db: Database, storage: WorkflowStorage) {
    super({ executor: fromDrizzle(db) }, storage);
  }
}

/** MySqlWorkflowStore as an app registers it: on the Drizzle MySQL database it injects. */
@Injectable()
class MySqlAppStore extends MySqlWorkflowStore {
  constructor(@Inject(getDrizzleToken()) db: MySqlDatabase, storage: WorkflowStorage) {
    super({ executor: fromMysqlDrizzle(db) }, storage);
  }
}

/** This run's SQL store, as an app registers it. */
const SqlAppStore: Type<WorkflowStore> = storeKind === 'mysql' ? MySqlAppStore : PostgresAppStore;

/**
 * The store provider as an app writes one: it injects its database (from `databaseModule()`)
 * and registers itself in its constructor. PostgresWorkflowStore or MySqlWorkflowStore on the
 * app's Drizzle database, or in the memory runs a provider over the test database's in-memory
 * store. A subclass that declares no constructor (and no decorator) keeps the injection.
 */
export const AppWorkflowStore: Type<WorkflowStore> = sqlBackend ? SqlAppStore : InMemoryAppStore;

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
  /** The registered store: the database's in-memory store, or the SQL store on this node's connection. */
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
  journal?: WorkflowJournalLimits;
  codec?: WorkflowsModuleOptions['codec'];
}): Promise<Node> {
  const connection = connect(options.db);
  try {
    return await bootOn(connection, options);
  } catch (error) {
    // A startup that fails (a definition error, say) still closes the process's connection: the store may have
    // opened one to check its schema, and a pool left open is terminated, with an error, when the database is dropped.
    await connection.close();
    throw error;
  }
}

async function bootOn(connection: Connection, options: Parameters<typeof boot>[0]): Promise<Node> {
  const drizzle = connection.db instanceof InMemoryWorkflowStore ? null : connection.db;
  const moduleRef = await Test.createTestingModule({
    imports: [
      WorkflowsModule.forRoot({
        clock: options.clock,
        retry: options.retry,
        journal: options.journal,
        codec: options.codec,
        worker: { enabled: false, shutdownTimeout: 50, ...options.worker },
      }),
      ...(options.imports ?? []),
    ],
    providers: [
      ...options.workflows,
      ...(options.providers ?? []),
      // As an app registers it: a provider that injects the database and registers itself.
      ...(drizzle ? [SqlAppStore, { provide: getDrizzleToken(), useValue: drizzle }] : []),
    ],
  }).compile();

  const store = drizzle ? moduleRef.get(SqlAppStore) : (connection.db as InMemoryWorkflowStore);
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

/**
 * Resolves once the node's heartbeats have read `flag` (a cancel or terminate requested elsewhere) twice: by then
 * the execution has taken in the first read. Spies on the node's store until the test restores its mocks.
 */
export function heartbeatRead(node: Node, flag: 'cancelRequested' | 'terminateRequested'): Promise<void> {
  const read = deferred();
  const renew = node.store.renew.bind(node.store);
  let reads = 0;
  vi.spyOn(node.store, 'renew').mockImplementation(async (...args) => {
    const flags = await renew(...args);
    if (flags?.[flag] && ++reads === 2) {
      read.resolve();
    }
    return flags;
  });
  return read.promise;
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
