/**
 * The `nest-workflows` command on MySQL (lib/cli.ts: the kit's runStoreCli() on both stores' schemas): `sql --dialect
 * mysql` prints what MySqlWorkflowStore.migrationSql() does, `migrate` and `status` pick MySqlWorkflowStore by the
 * URL's `mysql://`, and a database it can't migrate says where it stopped, in workflows' words.
 */
import { runStoreCli, type StoreCliIo } from '@nestjs/store-kit';
import mysql from 'mysql2/promise';
import { MySqlWorkflowStore } from '../../lib/mysql/index.js';
import { mysqlWorkflowStoreSchema } from '../../lib/mysql/migrations/index.js';
import { PostgresWorkflowStore } from '../../lib/postgres/index.js';
import { workflowStoreSchema } from '../../lib/postgres/migrations/index.js';
import { onMysql, testDatabase } from './support.js';

const { database, reason } = await testDatabase('mysql_cli');

/** The command as the bin runs it: both stores' schemas. */
async function run(argv: string[], env: StoreCliIo['env'] = {}) {
  const output = { out: '', err: '' };
  const code = await runStoreCli([workflowStoreSchema, mysqlWorkflowStoreSchema], argv, { out: (text) => (output.out += text), err: (text) => (output.err += text), env });
  return { code, ...output };
}

describe('nest-workflows, for MySqlWorkflowStore', () => {
  it('sql --dialect mysql prints the MySQL migrations as migrationSql() does, and PostgreSQL stays the default', async () => {
    expect(await run(['sql', '--dialect', 'mysql'])).toEqual({ code: 0, out: MySqlWorkflowStore.migrationSql(), err: '' });
    expect(await run(['sql', '--dialect', 'mysql', '--schema', 'shop_workflows', '--from', '0', '--to', '1', '--statement-breakpoints'])).toEqual({
      code: 0,
      out: MySqlWorkflowStore.migrationSql({ schema: 'shop_workflows', from: 0, to: 1, statementBreakpoints: true }),
      err: '',
    });
    expect(await run(['sql'])).toEqual({ code: 0, out: PostgresWorkflowStore.migrationSql(), err: '' });
    expect((await run(['sql', '--dialect', 'mysql', '--schema', 'Shop'])).err).toContain('MySqlWorkflowStore: invalid schema "Shop".');
    expect((await run(['sql', '--dialect', 'oracle'])).err).toBe('--dialect takes postgres or mysql, not "oracle".\n');
  });

  it('lists both stores and both kinds of URL in its usage', async () => {
    const help = await run(['--help']);
    expect(help.out).toContain("PostgresWorkflowStore's schema (@nestjs/workflows/postgres):\nMySqlWorkflowStore's schema (@nestjs/workflows/mysql):");
    expect(help.out).toContain('--url <url>        The database (postgres://... or mysql://...). Default: $DATABASE_URL');
  });

  describe('on MySQL', () => {
    onMysql(reason);

    it('migrate applies the pending migrations once, and status exits with 1 until they are', async () => {
      const url = database!.url;
      expect(await run(['status', '--url', url, '--schema', 'cli_store'])).toEqual({
        code: 1,
        out: 'Schema "cli_store" is at version 0; this version of @nestjs/workflows needs version 1.\n',
        err: '',
      });
      expect(await run(['migrate', '--schema', 'cli_store'], { DATABASE_URL: url })).toEqual({
        code: 0,
        out: 'Migrated schema "cli_store" to version 1 (applied 1).\n',
        err: '',
      });
      expect(await run(['migrate', '--url', url, '--schema', 'cli_store'])).toEqual({ code: 0, out: 'Schema "cli_store" is up to date (version 1).\n', err: '' });
      expect(await run(['status', '--url', url, '--schema', 'cli_store'])).toMatchObject({ code: 0, out: 'Schema "cli_store" is at version 1; this version of @nestjs/workflows needs version 1.\n' });
      const [versions] = await database!.admin.query<mysql.RowDataPacket[]>('SELECT version FROM cli_store_migrations');
      expect(versions).toEqual([{ version: 1 }]);
    });

    it('reports a database it cannot migrate, where it stopped, and exits with 1', async () => {
      await database!.admin.query('CREATE TABLE cli_taken_instances (id int NOT NULL PRIMARY KEY)');
      expect(await run(['migrate', '--url', database!.url, '--schema', 'cli_taken'])).toEqual({
        code: 1,
        out: '',
        err:
          'MySqlWorkflowStore: migrating schema "cli_taken" from version 0 to 1 stopped at migration 1 (initial), statement 1 of 6: ' +
          "Table 'cli_taken_instances' already exists. The statements before it are applied, and migrating again resumes at it.\n",
      });
    });
  });
});
