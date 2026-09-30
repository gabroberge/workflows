/**
 * MySqlWorkflowStore's tests: the database clients an application may hand it (mysql2, Drizzle on mysql2, TypeORM,
 * Prisma with its MariaDB adapter, Kysely), each with the ORM's own way of running a transaction (REPEATABLE READ,
 * MySQL's default, unless a test says otherwise), and a database per test file on the MySQL of `SQL_TEST_MYSQL_URL`
 * (else those tests are skipped with the reason), through tests/support/mysql.ts, which names (`wft_`) and sweeps them.
 * Every pool has at most 3 connections, the admin pool 1: the server is shared, and four files run at once.
 */
import { PrismaMariaDb } from '@prisma/adapter-mariadb';
import { drizzle } from 'drizzle-orm/mysql2';
import { mysqlTable, varchar } from 'drizzle-orm/mysql-core';
import { Kysely, MysqlDialect } from 'kysely';
import mysqlCallbacks from 'mysql2';
import mysql from 'mysql2/promise';
import { Column, DataSource, Entity, PrimaryColumn } from 'typeorm';
import { fromDrizzle, fromKysely, fromMysql2, fromPrisma, fromTypeOrm, MySqlWorkflowStore, type SqlExecutor } from '../../lib/mysql/index.js';
import { workflowStoreContract } from '../../lib/testing/index.js';
import { PrismaClient } from '../fixtures/prisma-mysql/generated/client.js';
import { adminPool, startMysql } from '../support/mysql.js';

/** The application's table, written in the same transactions as the workflows it starts and signals. */
export const ORDERS_DDL = 'CREATE TABLE IF NOT EXISTS orders (id varchar(191) NOT NULL PRIMARY KEY, status varchar(32) NOT NULL)';

export type Isolation = 'read committed' | 'repeatable read';

/** The connections each client's pool may open. */
export const POOL_SIZE = 3;

/** A database client as an application holds one. */
export interface Client {
  name: string;
  executor: SqlExecutor;
  /** A transaction as the application runs one with this client: `work` gets the ORM's own transaction object. */
  transaction<T>(work: (tx: unknown) => Promise<T>, isolation?: Isolation): Promise<T>;
  /** What an application might pass by mistake instead of its transaction: the pool, the database, the client. */
  root: unknown;
  /** The application's write, through its transaction object. */
  insertOrder(tx: unknown, id: string): Promise<void>;
  close(): Promise<void>;
}

export interface ClientFactory {
  name: string;
  open(url: string): Promise<Client>;
}

const drizzleOrders = mysqlTable('orders', { id: varchar('id', { length: 191 }).primaryKey(), status: varchar('status', { length: 32 }).notNull() });

@Entity('orders')
class OrderEntity {
  @PrimaryColumn('varchar', { length: 191 })
  id!: string;

  @Column('varchar', { length: 32 })
  status!: string;
}

interface KyselyDatabase {
  orders: { id: string; status: string };
}

export const mysql2Client: ClientFactory = {
  name: 'fromMysql2 (mysql2 pool)',
  async open(url) {
    const pool = mysql.createPool({ uri: url, connectionLimit: POOL_SIZE });
    return {
      name: this.name,
      executor: fromMysql2(pool),
      root: pool,
      async transaction(work, isolation) {
        const connection = await pool.getConnection();
        try {
          if (isolation) {
            await connection.query(`SET TRANSACTION ISOLATION LEVEL ${isolation.toUpperCase()}`);
          }
          await connection.beginTransaction();
          const result = await work(connection);
          await connection.commit();
          return result;
        } catch (error) {
          await connection.rollback();
          throw error;
        } finally {
          connection.release();
        }
      },
      insertOrder: async (tx, id) => {
        await (tx as mysql.PoolConnection).query("INSERT INTO orders (id, status) VALUES (?, 'placed')", [id]);
      },
      close: () => pool.end(),
    };
  },
};

export const drizzleClient: ClientFactory = {
  name: 'fromDrizzle (mysql2)',
  async open(url) {
    const pool = mysql.createPool({ uri: url, connectionLimit: POOL_SIZE });
    const db = drizzle(pool);
    return {
      name: this.name,
      executor: fromDrizzle(db),
      root: db,
      transaction: (work, isolation) => db.transaction(work, isolation ? { isolationLevel: isolation } : undefined),
      insertOrder: async (tx, id) => {
        await (tx as typeof db).insert(drizzleOrders).values({ id, status: 'placed' });
      },
      close: () => pool.end(),
    };
  },
};

export const typeOrmClient: ClientFactory = {
  name: 'fromTypeOrm',
  async open(url) {
    const dataSource = await new DataSource({ type: 'mysql', url, entities: [OrderEntity], poolSize: POOL_SIZE }).initialize();
    return {
      name: this.name,
      executor: fromTypeOrm(dataSource),
      root: dataSource.manager,
      transaction: (work, isolation) => (isolation ? dataSource.transaction(isolation.toUpperCase() as 'REPEATABLE READ', work) : dataSource.transaction(work)),
      insertOrder: async (tx, id) => {
        await (tx as DataSource['manager']).insert(OrderEntity, { id, status: 'placed' });
      },
      close: () => dataSource.destroy(),
    };
  },
};

export const prismaClient: ClientFactory = {
  name: 'fromPrisma (@prisma/adapter-mariadb)',
  async open(url) {
    const prisma = new PrismaClient({ adapter: new PrismaMariaDb(mariadbConfig(url)) });
    return {
      name: this.name,
      executor: fromPrisma(prisma),
      root: prisma,
      // The application's own limits: Prisma's default maxWait (2 s) is short for a pool of 3 that 60 signals, which
      // the store serializes, share with 60 suspensions.
      transaction: (work, isolation) =>
        prisma.$transaction((tx) => work(tx), {
          maxWait: 10_000,
          timeout: 30_000,
          ...(isolation ? { isolationLevel: isolation === 'repeatable read' ? 'RepeatableRead' : 'ReadCommitted' } : {}),
        }),
      insertOrder: async (tx, id) => {
        await (tx as PrismaClient).order.create({ data: { id, status: 'placed' } });
      },
      close: () => prisma.$disconnect(),
    };
  },
};

export const kyselyClient: ClientFactory = {
  name: 'fromKysely',
  async open(url) {
    const db = new Kysely<KyselyDatabase>({ dialect: new MysqlDialect({ pool: mysqlCallbacks.createPool({ uri: url, connectionLimit: POOL_SIZE }) }) });
    return {
      name: this.name,
      executor: fromKysely(db),
      root: db,
      transaction: (work, isolation) => (isolation ? db.transaction().setIsolationLevel(isolation).execute(work) : db.transaction().execute(work)),
      insertOrder: async (tx, id) => {
        await (tx as typeof db).insertInto('orders').values({ id, status: 'placed' }).execute();
      },
      close: () => db.destroy(),
    };
  },
};

/** Every client on MySQL. */
export const clients = [mysql2Client, drizzleClient, typeOrmClient, prismaClient, kyselyClient];

/**
 * The MariaDB connector's settings for `url`. `allowPublicKeyRetrieval`: MySQL's `caching_sha2_password` over a
 * connection without TLS needs the server's RSA key until the server has cached the password (a fresh server, as in CI).
 */
export function mariadbConfig(url: string) {
  const parsed = new URL(url);
  return {
    host: parsed.hostname,
    port: Number(parsed.port || 3306),
    user: decodeURIComponent(parsed.username),
    password: decodeURIComponent(parsed.password),
    database: parsed.pathname.slice(1),
    connectionLimit: POOL_SIZE,
    allowPublicKeyRetrieval: true,
  };
}

export interface TestDatabase {
  /** The database's name. */
  name: string;
  url: string;
  /** A connection for looking at the database from outside the store (one: the server is shared). */
  admin: mysql.Pool;
}

/**
 * A database of this test file on MySQL (with `orders`), dropped after the file; `null`, with the reason, where
 * there's no MySQL. Tests that need it skip with the reason.
 */
export async function testDatabase(name: string): Promise<{ database: TestDatabase; reason?: undefined } | { database: null; reason: string }> {
  const { mysql: server, reason } = await startMysql();
  if (!server) {
    return { database: null, reason: `no MySQL: ${reason}` };
  }

  const { name: database, url } = await server.createDatabase(name);
  const admin = adminPool(url);
  await admin.query(ORDERS_DDL);
  afterAll(async () => {
    await admin.end();
    await server.stop();
  });
  return { database: { name: database, url, admin } };
}

/** In a describe of tests that run on MySQL: skips them, with the reason, where there's none. */
export function onMysql(reason: string | undefined): void {
  beforeEach((context) => {
    if (reason) {
      context.skip(reason);
    }
  });
}

/** An executor that records every statement it runs, and its parameters. */
export function recording(executor: SqlExecutor): { executor: SqlExecutor; statements: Array<{ text: string; params?: readonly unknown[] }> } {
  const statements: Array<{ text: string; params?: readonly unknown[] }> = [];
  const record = (tx: Pick<SqlExecutor, 'query' | 'execute'>) => ({
    query: <R extends object>(text: string, params?: readonly unknown[]) => {
      statements.push({ text, params });
      return tx.query<R>(text, params);
    },
    execute: (text: string, params?: readonly unknown[]) => {
      statements.push({ text, params });
      return tx.execute(text, params);
    },
  });
  return {
    statements,
    executor: {
      dialect: executor.dialect,
      query: (text, params) => record(executor).query(text, params),
      execute: (text, params) => record(executor).execute(text, params),
      transaction: (work, options) => executor.transaction((tx) => work(record(tx)), options),
      wrapTransaction: (transaction) => record(executor.wrapTransaction(transaction)),
    },
  };
}

/** The store's tables in `schema` (`<schema>_<table>`), quoted. */
export function storeTables(schema: string): string[] {
  return ['instances', 'journal', 'waits', 'signals', 'schedules', 'rate_limits'].map((table) => `\`${schema}_${table}\``);
}

/** Empties the store's tables in `schema`, as the contract wants a store on empty tables. */
export async function truncate(executor: SqlExecutor, schema: string): Promise<void> {
  for (const table of storeTables(schema)) {
    await executor.execute(`DELETE FROM ${table}`);
  }
}

/**
 * The `WorkflowStore` contract on MySqlWorkflowStore through `client`, with the concurrency cases and the ORM's own
 * transactions (REPEATABLE READ, MySQL's default), in the schema `schema` of the client's database.
 */
export function describeContract(label: string, client: () => Promise<Client | null>, schema: string, skipReason?: string): void {
  describe(label, () => {
    let opened: Client | null = null;
    beforeAll(async () => {
      opened = await client();
      if (opened) {
        await new MySqlWorkflowStore({ executor: opened.executor, schema }).migrate();
      }
    });
    afterAll(() => opened?.close());
    onMysql(skipReason);

    const cases = workflowStoreContract(
      async () => {
        await truncate(opened!.executor, schema);
        return new MySqlWorkflowStore({ executor: opened!.executor, schema, migrate: false });
      },
      { concurrent: true, transaction: (work) => opened!.transaction(work) },
    );
    for (const c of cases) {
      it(c.name, c.run);
    }
  });
}
