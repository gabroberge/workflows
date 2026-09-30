/**
 * PostgresWorkflowStore's migrations: a new database, a rerun, processes migrating at once, `migrationSql()` against
 * what `migrate()` applies, a schema behind the code (and ahead of it), a failing migration, a schema someone created
 * or filled, the production default, and a first call inside the application's transaction.
 */
import { readFileSync } from 'node:fs';
import { drizzle } from 'drizzle-orm/pglite';
import { PGlite } from '@electric-sql/pglite';
import pg from 'pg';
import { fromDrizzle, fromPg, PostgresWorkflowStore, WorkflowSchemaError, type SqlExecutor, type SqlTransaction } from '../../lib/postgres/index.js';
import { MIGRATIONS } from '../../lib/postgres/migrations/index.js';
import { applyMigrations, assertMigrated, migrationStatements, type StoreMigration } from '../../lib/postgres/utils/migrations.util.js';
import { testDatabase } from './support.js';

const { database, reason } = await testDatabase('pgstore_migrations');
const pools: pg.Pool[] = [];

/** In a describe of tests that run on PostgreSQL: skips them, with the reason, where there's none. */
const onPostgres = () =>
  beforeEach((context) => {
    if (reason) {
      context.skip(reason);
    }
  });

afterAll(async () => {
  await Promise.all(pools.map((pool) => pool.end()));
});

/** A pool of its own, as each process has. */
const pool = () => {
  const opened = new pg.Pool({ connectionString: database!.url, max: 2 });
  pools.push(opened);
  return opened;
};

const store = (schema: string, options: { migrate?: boolean; executor?: SqlExecutor } = {}) =>
  new PostgresWorkflowStore({ executor: options.executor ?? fromPg(pool()), schema, migrate: options.migrate });

/** An executor that records every statement it runs. */
function recording(executor: SqlExecutor): { executor: SqlExecutor; statements: string[] } {
  const statements: string[] = [];
  const record = (tx: SqlTransaction): SqlTransaction => ({
    query: (text, params) => {
      statements.push(text);
      return tx.query(text, params);
    },
  });
  return {
    statements,
    executor: {
      query: (text, params) => {
        statements.push(text);
        return executor.query(text, params);
      },
      transaction: (work, options) => executor.transaction((tx) => work(record(tx)), options),
      wrapTransaction: (transaction) => record(executor.wrapTransaction(transaction)),
    },
  };
}

/** The statements that change the schema: not the lock, and not the reads of what it has. */
const changes = (statements: string[]) => statements.filter((statement) => !statement.startsWith('SELECT'));

const rows = async (sql: string, params: unknown[] = []) => (await database!.admin.query(sql, params)).rows;

/** Everything about a schema's tables that migrations define, with the schema's name taken out. */
async function catalog(schema: string) {
  const anonymize = (value: unknown) => JSON.parse(JSON.stringify(value).replaceAll(schema, '<schema>'));
  return anonymize({
    columns: await rows(
      `SELECT table_name, column_name, ordinal_position, data_type, is_nullable, column_default, is_identity, identity_generation
       FROM information_schema.columns WHERE table_schema = $1 ORDER BY table_name, ordinal_position`,
      [schema],
    ),
    constraints: await rows(
      `SELECT conrelid::regclass::text AS table, conname, pg_get_constraintdef(oid) AS definition
       FROM pg_constraint WHERE connamespace = $1::regnamespace ORDER BY 1, 2`,
      [schema],
    ),
    indexes: await rows('SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = $1 ORDER BY tablename, indexname', [schema]),
    versions: await rows(`SELECT version, name FROM "${schema}".migrations ORDER BY version`),
  });
}

const tables = async (schema: string) =>
  (await rows('SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name', [schema])).map((row) => row.table_name);

const extra: StoreMigration = { version: 2, name: 'extra', up: (s) => [`CREATE TABLE ${s}.extra (id integer PRIMARY KEY)`] };

describe('migrate()', () => {
  onPostgres();

  it('creates the schema, its tables and the version record on a new database, and applies nothing the second time', async () => {
    const first = store('m_fresh');
    expect(await first.migrate()).toEqual([1]);
    expect(await tables('m_fresh')).toEqual(['instances', 'journal', 'migrations', 'rate_limits', 'schedules', 'signals', 'waits']);
    expect(await rows('SELECT version, name FROM m_fresh.migrations')).toEqual([{ version: 1, name: 'initial' }]);
    expect(PostgresWorkflowStore.schemaVersion).toBe(1);

    expect(await first.migrate()).toEqual([]);
    expect(await store('m_fresh').migrate()).toEqual([]);
    expect(await rows('SELECT count(*)::int AS n FROM m_fresh.migrations')).toEqual([{ n: 1 }]);
    await expect(store('m_fresh', { migrate: false }).onModuleInit()).resolves.toBeUndefined();
  });

  it('applies the migrations once when processes start together, each on its own connections', async () => {
    const stores = Array.from({ length: 8 }, () => store('m_together'));
    const applied = await Promise.all(stores.map((s) => s.migrate()));
    expect(applied.filter((versions) => versions.length > 0)).toEqual([[1]]);

    const starting = Array.from({ length: 8 }, () => store('m_starting'));
    await Promise.all(starting.map((s) => s.onModuleInit()));
    expect(await rows('SELECT version FROM m_starting.migrations')).toEqual([{ version: 1 }]);
    expect(await starting[3]!.create({ id: 'i-1', workflow: 'w', version: 1, input: null, deadline: null, now: 1 })).toMatchObject({ created: true });
  });

  it('runs the statements migrationSql() prints, and a database migrated with them is the same as one migrate() made', async () => {
    const recorder = recording(fromPg(pool()));
    await store('m_migrated', { executor: recorder.executor }).migrate();
    const script = PostgresWorkflowStore.migrationSql({ schema: 'm_migrated' });
    expect(script).toBe(
      `-- @nestjs/workflows: PostgresWorkflowStore's schema "m_migrated", from version 0 to 1.\n-- Run it in one transaction.\n\n` +
        `${changes(recorder.statements).map((statement) => `${statement};`).join('\n\n')}\n`,
    );
    expect(changes(recorder.statements)).toEqual(migrationStatements('m_migrated', MIGRATIONS));

    // As a team applies it with its own tool: one script, in one transaction.
    const client = await pool().connect();
    try {
      await client.query(`BEGIN; ${PostgresWorkflowStore.migrationSql({ schema: 'm_script' })} COMMIT;`);
    } finally {
      client.release();
    }
    expect(await catalog('m_script')).toEqual(await catalog('m_migrated'));
    await expect(store('m_script', { migrate: false }).onModuleInit()).resolves.toBeUndefined();
  });

  it("has every index and unique constraint of the tutorial's hand-written store (tests/fixtures/drizzle)", async () => {
    await store('m_indexes').migrate();
    const fixture = readFileSync(new URL('../fixtures/drizzle/0001_workflows.sql', import.meta.url), 'utf8');
    const names = [...fixture.matchAll(/(?:CREATE INDEX|CONSTRAINT) "workflow_(\w+?)"/g)]
      .map((match) => match[1]!)
      .filter((name) => !name.endsWith('_fk'))
      .map((name) => name.replace(/_(workflow_key|instance_id_position)_pk$/, '_pkey'));
    const indexes = (await rows('SELECT indexname FROM pg_indexes WHERE schemaname = $1', ['m_indexes'])).map((row) => row.indexname);
    expect(names).toHaveLength(15);
    expect(indexes).toEqual(expect.arrayContaining(names));

    // A deleted instance takes its journal and waits with it.
    const foreignKeys = await rows("SELECT conrelid::regclass::text AS table, pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE connamespace = 'm_indexes'::regnamespace AND contype = 'f' ORDER BY 1");
    expect(foreignKeys).toEqual([
      { table: 'm_indexes.journal', definition: 'FOREIGN KEY (instance_id) REFERENCES m_indexes.instances(id) ON DELETE CASCADE' },
      { table: 'm_indexes.waits', definition: 'FOREIGN KEY (instance_id) REFERENCES m_indexes.instances(id) ON DELETE CASCADE' },
    ]);
  });

  it("uses a schema someone created for the store as it is, without CREATE SCHEMA (which needs the database's CREATE privilege)", async () => {
    await rows('CREATE SCHEMA m_precreated');
    const recorder = recording(fromPg(pool()));
    expect(await store('m_precreated', { executor: recorder.executor }).migrate()).toEqual([1]);
    expect(changes(recorder.statements)).toEqual(migrationStatements('m_precreated', MIGRATIONS).filter((statement) => !statement.startsWith('CREATE SCHEMA')));
  });

  it('applies nothing of a migration that fails, and says which versions it was between', async () => {
    await store('m_failing').migrate();
    const broken: StoreMigration = { version: 2, name: 'broken', up: (s) => [`CREATE TABLE ${s}.extra (id integer)`, 'SELECT 1 / 0'] };

    const error = await applyMigrations(fromPg(pool()), 'm_failing', [...MIGRATIONS, broken]).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WorkflowSchemaError);
    expect(error).toMatchObject({ schema: 'm_failing', version: 1, requiredVersion: 2, cause: { message: 'division by zero' } });
    expect((error as Error).message).toBe(
      'PostgresWorkflowStore: migrating schema "m_failing" from version 1 to 2 failed, and nothing was applied: division by zero',
    );
    expect(await tables('m_failing')).not.toContain('extra');
    expect(await rows('SELECT max(version) AS version FROM m_failing.migrations')).toEqual([{ version: 1 }]);
  });

  it("fails on a schema that has other tables of the store's names, and creates nothing", async () => {
    await rows('CREATE SCHEMA m_taken');
    await rows('CREATE TABLE m_taken.instances (id serial PRIMARY KEY)');
    await expect(store('m_taken').migrate()).rejects.toThrow('PostgresWorkflowStore: migrating schema "m_taken" from version 0 to 1 failed, and nothing was applied: relation "instances" already exists');
    expect(await tables('m_taken')).toEqual(['instances']);
  });
});

describe('a schema behind the code', () => {
  onPostgres();

  it('fails the startup (and every call) with a WorkflowSchemaError that says how to migrate, changing nothing', async () => {
    const behind = store('m_behind', { migrate: false });
    const error = await behind.onModuleInit().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WorkflowSchemaError);
    expect(error).toMatchObject({ name: 'WorkflowSchemaError', schema: 'm_behind', version: 0, requiredVersion: 1 });
    expect((error as Error).message).toBe(
      'PostgresWorkflowStore: schema "m_behind" is at version 0, and this version of @nestjs/workflows needs version 1. Apply its migrations: ' +
        'set `migrate: true` to apply them at startup, run `npx nest-workflows migrate --url <database url> --schema m_behind`, ' +
        "or apply `PostgresWorkflowStore.migrationSql({ schema: 'm_behind', from: 0 })` with your migration tool.",
    );
    await expect(behind.get('any')).rejects.toThrow(WorkflowSchemaError);
    expect(await rows("SELECT nspname FROM pg_namespace WHERE nspname = 'm_behind'")).toEqual([]);

    // Checked again at the next call: migrated meanwhile, it serves.
    await store('m_behind').migrate();
    expect(await behind.get('any')).toBeNull();
  });

  it('is found at the version the code needs next, while a schema ahead of the code (migrated by a newer version) serves', async () => {
    const executor = fromPg(pool());
    await store('m_versions').migrate();
    const newer = [...MIGRATIONS, extra];

    await expect(assertMigrated(executor, 'm_versions', newer)).rejects.toMatchObject({ version: 1, requiredVersion: 2 });
    expect(migrationStatements('m_versions', newer, { from: 1 })).toEqual([
      'CREATE TABLE "m_versions".extra (id integer PRIMARY KEY)',
      `INSERT INTO "m_versions".migrations (version, name) VALUES (2, 'extra')`,
    ]);
    expect(await applyMigrations(executor, 'm_versions', newer)).toEqual([2]);

    // This version of the package (version 1) on a schema a newer one migrated to version 2, as in a rolling deploy.
    const older = store('m_versions', { migrate: false });
    await expect(older.onModuleInit()).resolves.toBeUndefined();
    expect(await older.create({ id: 'i-1', workflow: 'w', version: 1, input: null, deadline: null, now: 1 })).toMatchObject({ created: true });
    expect(await store('m_versions').migrate()).toEqual([]);
  });
});

describe('options', () => {
  onPostgres();

  it("don't migrate by default with NODE_ENV=production, and do otherwise", async () => {
    const previous = process.env.NODE_ENV;
    try {
      process.env.NODE_ENV = 'production';
      await expect(store('m_production').onModuleInit()).rejects.toThrow(WorkflowSchemaError);
      process.env.NODE_ENV = 'development';
      await expect(store('m_production').onModuleInit()).resolves.toBeUndefined();
      delete process.env.NODE_ENV;
      await expect(store('m_unset').onModuleInit()).resolves.toBeUndefined();
    } finally {
      process.env.NODE_ENV = previous;
    }
    expect(await tables('m_production')).toContain('instances');
  });

  it('take a schema name of letters, digits and underscores, quoted in every statement', async () => {
    for (const schema of ['bad-name', '1st', '', 'x'.repeat(64), 'a"b', 'a$1']) {
      expect(() => new PostgresWorkflowStore({ executor: fromPg(pool()), schema })).toThrow(TypeError);
      expect(() => PostgresWorkflowStore.migrationSql({ schema })).toThrow(`PostgresWorkflowStore: invalid schema ${JSON.stringify(schema)}.`);
    }
    expect(await store('Mixed_Case').migrate()).toEqual([1]);
    expect(await tables('Mixed_Case')).toContain('instances');
  });

  it('refuse an executor that is none, and a migrate that is no boolean', () => {
    expect(() => new PostgresWorkflowStore({ executor: {} as SqlExecutor })).toThrow('PostgresWorkflowStore: `executor` must be a SqlExecutor');
    expect(() => new PostgresWorkflowStore({ executor: fromPg(pool()), migrate: 'yes' as unknown as boolean })).toThrow(
      'PostgresWorkflowStore: `migrate` must be true or false, not "yes".',
    );
  });
});

describe('migrationSql()', () => {
  it('prints the default schema from a new database, a range of versions, and never a downgrade', () => {
    const script = PostgresWorkflowStore.migrationSql();
    expect(script).toMatch(/^-- @nestjs\/workflows: PostgresWorkflowStore's schema "nest_workflows", from version 0 to 1\.\n/);
    expect(script).toContain('CREATE SCHEMA IF NOT EXISTS "nest_workflows";');
    expect(script).toContain(`INSERT INTO "nest_workflows".migrations (version, name) VALUES (1, 'initial');`);
    expect(PostgresWorkflowStore.migrationSql({ from: 1 })).not.toContain('CREATE');

    expect(() => PostgresWorkflowStore.migrationSql({ from: 1, to: 0 })).toThrow(RangeError);
    expect(() => PostgresWorkflowStore.migrationSql({ from: 1, to: 0 })).toThrow("downgrades aren't supported");
    expect(() => PostgresWorkflowStore.migrationSql({ to: 2 })).toThrow('no migrations lead from version 0 to 2');
    expect(() => PostgresWorkflowStore.migrationSql({ from: -1 })).toThrow(RangeError);
  });
});

describe('a first call inside the application transaction, on PGlite', () => {
  it("checks the schema through that transaction instead of waiting for it, and can't migrate in it", async () => {
    const pglite = new PGlite();
    const db = drizzle(pglite);
    try {
      const instance = { id: 'i-1', workflow: 'w', version: 1, input: null, deadline: null, now: 1 };
      const unmigrated = new PostgresWorkflowStore({ executor: fromDrizzle(db) });
      await expect(db.transaction((tx) => unmigrated.createInTransaction(tx, instance))).rejects.toThrow(
        "is at version 0, and this version of @nestjs/workflows needs version 1. The store applies its migrations when the application starts (onModuleInit), or at its first call outside a transaction: it can't apply them in yours.",
      );

      await new PostgresWorkflowStore({ executor: fromDrizzle(db) }).migrate();
      const fresh = new PostgresWorkflowStore({ executor: fromDrizzle(db) });
      await db.transaction((tx) => fresh.createInTransaction(tx, instance));
      expect(await fresh.get('i-1')).toMatchObject({ status: 'pending' });
    } finally {
      await pglite.close();
    }
  });
});
