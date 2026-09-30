/**
 * The executors on their own: what each takes (and refuses) as the application's client and as its transaction
 * object, transactions that commit, roll back and take an isolation level, and each client's particulars: a single
 * node-postgres Client, Kysely's plugins, Prisma's transaction limits.
 */
import { PrismaPg } from '@prisma/adapter-pg';
import { CamelCasePlugin, Kysely, PostgresDialect } from 'kysely';
import pg from 'pg';
import { fromDrizzle, fromKysely, fromPg, fromPrisma, fromTypeOrm } from '../../lib/postgres/index.js';
import { PrismaClient } from '../fixtures/prisma/generated/client.js';
import { clients, openPglite, testDatabase, type Client } from './support.js';

const { database, reason } = await testDatabase('pgstore_executors');

const targets = [
  ...clients.map((factory, i) => ({ name: `${factory.name} on PostgreSQL`, open: () => factory.open(database!.url), table: `probes_${i}`, skip: reason })),
  { name: 'fromDrizzle (PGlite)', open: openPglite, table: 'probes', skip: undefined },
];

describe.each(targets)('$name', ({ open, table, skip }) => {
  let client: Client;

  beforeAll(async () => {
    if (!skip) {
      client = await open();
      await client.executor.query(`CREATE TABLE ${table} (id text PRIMARY KEY)`);
    }
  });

  afterAll(() => client?.close());

  beforeEach(async (context) => {
    if (skip) {
      context.skip(skip);
    }
    await client.executor.query(`DELETE FROM ${table}`);
  });

  const probes = async () => (await client.executor.query<{ id: string }>(`SELECT id FROM ${table} ORDER BY id`)).map((row) => row.id);

  it('runs a statement with its parameters in order, and answers every row of one that returns some', async () => {
    const rows = await client.executor.query<{ a: string; b: string | null; n: string }>('SELECT $2::text AS a, $1::text AS b, $3::text::bigint::text AS n', [
      null,
      "O'Reilly — ü 🚀 $1",
      '9007199254740991',
    ]);
    expect(rows).toEqual([{ a: "O'Reilly — ü 🚀 $1", b: null, n: '9007199254740991' }]);
    expect(await client.executor.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['plain'])).toEqual([]);
    expect(await client.executor.query(`DELETE FROM ${table} WHERE id = $1::text RETURNING id`, ['plain'])).toEqual([{ id: 'plain' }]);
  });

  it('commits a transaction whose work resolves, with its result, and rolls back one whose work throws', async () => {
    expect(
      await client.executor.transaction(async (tx) => {
        await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['kept']);
        return (await tx.query<{ id: string }>(`SELECT id FROM ${table} WHERE id = $1::text`, ['kept'])).length;
      }),
    ).toBe(1);

    await expect(
      client.executor.transaction(async (tx) => {
        await tx.query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['undone']);
        throw new Error('changed my mind');
      }),
    ).rejects.toThrow('changed my mind');
    // Drizzle wraps the driver's error ("Failed query: ..."), with it as the cause.
    await expect(client.executor.transaction((tx) => tx.query(`INSERT INTO ${table} (id) VALUES ($1::text), ($1::text)`, ['twice']))).rejects.toSatisfy(
      (error: Error) => /duplicate key/.test(`${error.message} ${(error.cause as Error | undefined)?.message}`),
    );
    expect(await probes()).toEqual(['kept']);
  });

  it('runs a transaction at the isolation level asked for', async () => {
    const level = (options?: { isolationLevel: 'read committed' | 'repeatable read' | 'serializable' }) =>
      client.executor.transaction(async (tx) => (await tx.query<{ level: string }>("SELECT current_setting('transaction_isolation') AS level"))[0]!.level, options);
    expect(await level({ isolationLevel: 'read committed' })).toBe('read committed');
    expect(await level({ isolationLevel: 'repeatable read' })).toBe('repeatable read');
    expect(await level({ isolationLevel: 'serializable' })).toBe('serializable');
    expect(await level()).toBe('read committed');
    await expect(client.executor.transaction(async () => 1, { isolationLevel: `snapshot; DROP TABLE ${table}` as 'serializable' })).rejects.toThrow(TypeError);
  });

  it("joins the application's transaction object, and refuses its database, pool or client and anything else", async () => {
    await expect(
      client.transaction(async (tx) => {
        await client.executor.wrapTransaction(tx).query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['joined']);
        throw new Error('rolled back');
      }),
    ).rejects.toThrow('rolled back');
    await client.transaction((tx) => client.executor.wrapTransaction(tx).query(`INSERT INTO ${table} (id) VALUES ($1::text)`, ['committed']));
    expect(await probes()).toEqual(['committed']);

    for (const junk of [client.root, client.executor, {}, null, undefined, 42]) {
      expect(() => client.executor.wrapTransaction(junk)).toThrow(TypeError);
    }
    expect(() => client.executor.wrapTransaction(client.root)).toThrow(/^Pass the .* not /);
    expect(() => client.executor.wrapTransaction({})).toThrow(/^Pass the .* got an object\.$/);
  });
});

describe('the clients each executor takes', () => {
  beforeEach((context) => {
    if (reason) {
      context.skip(reason);
    }
  });

  it('fromPg() takes a pool or a client, and joins a client in a transaction (not the pool, not one idle or failed)', async () => {
    expect(() => fromPg({} as pg.Pool)).toThrow('fromPg() takes a node-postgres Pool (or a connected Client), got an object.');
    const pool = new pg.Pool({ connectionString: database!.url, max: 2 });
    const client = await pool.connect();
    try {
      const executor = fromPg(pool);
      expect(() => executor.wrapTransaction(pool)).toThrow('not the pool: it runs each statement on any of its connections, outside your transaction.');
      expect(() => executor.wrapTransaction(client)).toThrow("The node-postgres client isn't in a transaction: send BEGIN on it first, or each statement commits on its own.");

      await client.query('BEGIN');
      expect(await executor.wrapTransaction(client).query('SELECT 1::text AS one')).toEqual([{ one: '1' }]);
      await client.query('SELECT 1 / 0').catch(() => undefined);
      expect(() => executor.wrapTransaction(client)).toThrow('The node-postgres client is in a failed transaction: roll it back.');
      await client.query('ROLLBACK');
    } finally {
      client.release();
      await pool.end();
    }
  });

  it('fromPg() on a single Client runs its statements and transactions one after another', async () => {
    const client = new pg.Client({ connectionString: database!.url });
    await client.connect();
    try {
      const executor = fromPg(client);
      // Two statements of a transaction see its transaction id: another transaction's in between would join it.
      const outside = Array.from({ length: 10 }, () => executor.query<{ id: string }>('SELECT txid_current()::text AS id'));
      const ids = await Promise.all(
        Array.from({ length: 10 }, () =>
          executor.transaction(async (tx) => {
            const [first] = await tx.query<{ id: string }>('SELECT txid_current()::text AS id');
            await new Promise((resolve) => setTimeout(resolve, Math.random() * 5));
            const [second] = await tx.query<{ id: string }>('SELECT txid_current()::text AS id');
            return [first!.id, second!.id];
          }, { isolationLevel: 'read committed' }).catch((error: Error) => [error.message, ''])),
      );
      expect(ids.every(([first, second]) => first === second)).toBe(true);
      expect(new Set([...ids.map(([first]) => first), ...(await Promise.all(outside)).map(([row]) => row!.id)]).size).toBe(20);
    } finally {
      await client.end();
    }
  });

  it('fromDrizzle() takes a PostgreSQL database, not its tx or another dialect', async () => {
    const drizzle = await clients[1]!.open(database!.url);
    try {
      await drizzle.transaction(async (tx) => {
        expect(() => fromDrizzle(tx as never)).toThrow('fromDrizzle() takes the database drizzle() returns, not a transaction');
      });
      class MySqlDatabase {
        execute() {}
      }
      Object.assign(MySqlDatabase, { [Symbol.for('drizzle:entityKind')]: 'MySqlDatabase' });
      expect(() => fromDrizzle(new MySqlDatabase() as never)).toThrow('fromDrizzle() takes a Drizzle PostgreSQL database (drizzle() of drizzle-orm/node-postgres, /pglite...), got a MySqlDatabase.');
      expect(() => fromDrizzle({} as never)).toThrow(TypeError);
    } finally {
      await drizzle.close();
    }
  });

  it("fromTypeOrm() takes a PostgreSQL DataSource or its manager, not a transaction's manager", async () => {
    const typeorm = await clients[2]!.open(database!.url);
    try {
      const manager = typeorm.root as { connection: unknown };
      expect(await fromTypeOrm(manager as never).query('SELECT 1::text AS one')).toEqual([{ one: '1' }]);
      await typeorm.transaction(async (tx) => {
        expect(() => fromTypeOrm(tx as never)).toThrow("fromTypeOrm() takes the DataSource (or its manager), not a transaction's manager");
      });
      const mysql = { '@instanceof': Symbol.for('DataSource'), createQueryRunner() {}, options: { type: 'mysql' } };
      expect(() => fromTypeOrm(mysql as never)).toThrow("fromTypeOrm() takes a DataSource of type 'postgres', not 'mysql'.");
      expect(() => fromTypeOrm({} as never)).toThrow('fromTypeOrm() takes a TypeORM DataSource (or its manager), got an object.');

      // A QueryRunner of the application's, after startTransaction().
      const runner = (manager.connection as { createQueryRunner(): any }).createQueryRunner();
      await runner.startTransaction();
      expect(await typeorm.executor.wrapTransaction(runner).query('SELECT 1::text AS one')).toEqual([{ one: '1' }]);
      await runner.rollbackTransaction();
      expect(() => typeorm.executor.wrapTransaction(runner)).toThrow('Pass the EntityManager your dataSource.transaction() callback receives');
      await runner.release();
    } finally {
      await typeorm.close();
    }
  });

  it('fromPrisma() takes the client, not a transaction client, and gives its own transactions the limits asked for', async () => {
    const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: database!.url, max: 2 }) });
    try {
      await prisma.$transaction(async (tx) => {
        expect(() => fromPrisma(tx as never)).toThrow('fromPrisma() takes the Prisma client, not a transaction client');
      });
      expect(() => fromPrisma({} as never)).toThrow('fromPrisma() takes a Prisma client, got an object.');

      const hurried = fromPrisma(prisma, { timeout: '500ms' });
      await expect(hurried.transaction((tx) => tx.query('SELECT pg_sleep(1)::text'))).rejects.toThrow(/timeout|expired|closed/i);
      expect(await fromPrisma(prisma).transaction((tx) => tx.query('SELECT pg_sleep(1)::text AS slept'))).toEqual([{ slept: '' }]);
    } finally {
      await prisma.$disconnect();
    }
  });

  it("fromKysely() takes the Kysely instance, not a transaction, and runs the store's statements without its plugins", async () => {
    const db = new Kysely<object>({ dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: database!.url, max: 2 }) }), plugins: [new CamelCasePlugin()] });
    try {
      const executor = fromKysely(db);
      expect(await executor.query('SELECT 1::text AS lease_until')).toEqual([{ lease_until: '1' }]);
      expect(await executor.transaction((tx) => tx.query('SELECT 1::text AS lease_until'))).toEqual([{ lease_until: '1' }]);
      await db.transaction().execute(async (trx) => {
        expect(await executor.wrapTransaction(trx).query('SELECT 1::text AS lease_until')).toEqual([{ lease_until: '1' }]);
        expect(() => fromKysely(trx as never)).toThrow('fromKysely() takes the Kysely instance, not a transaction');
      });
      expect(() => fromKysely({} as never)).toThrow('fromKysely() takes a Kysely instance, got an object.');
    } finally {
      await db.destroy();
    }
  });
});
