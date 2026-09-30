/**
 * The executor types a user writes, from this package's entries: `SqlExecutor` from `@nestjs/workflows/postgres` is a
 * PostgreSQL executor, which PostgresWorkflowStore takes, and from `@nestjs/workflows/mysql` a MySQL one, which
 * MySqlWorkflowStore takes, so `const executor: SqlExecutor = fromDrizzle(db)` compiles for its dialect's store. The
 * other dialect's is a compile error (the `@ts-expect-error` lines, which the package's typecheck checks) and a
 * TypeError at run time. Nothing connects: the pools open their connections at their first statement.
 */
import { drizzle as drizzleMysql } from 'drizzle-orm/mysql2';
import { drizzle as drizzlePostgres } from 'drizzle-orm/node-postgres';
import { createPool } from 'mysql2/promise';
import pg from 'pg';
import { fromDrizzle as fromMysqlDrizzle, MySqlWorkflowStore, type SqlExecutor as MySqlSqlExecutor } from '../../lib/mysql/index.js';
import { fromDrizzle, PostgresWorkflowStore, type SqlExecutor } from '../../lib/postgres/index.js';

describe("the executor types of the package's entries", () => {
  it("takes an executor annotated with /postgres's SqlExecutor in PostgresWorkflowStore, and with /mysql's in MySqlWorkflowStore, and neither in the other", async () => {
    const pgPool = new pg.Pool({ connectionString: 'postgres://nobody@127.0.0.1:1/none' });
    const mysqlPool = createPool({ host: '127.0.0.1', port: 1, connectionLimit: 1 });
    try {
      const postgres: SqlExecutor = fromDrizzle(drizzlePostgres(pgPool));
      const mysql: MySqlSqlExecutor = fromMysqlDrizzle(drizzleMysql(mysqlPool));
      expect(new PostgresWorkflowStore({ executor: postgres })).toBeInstanceOf(PostgresWorkflowStore);
      expect(new MySqlWorkflowStore({ executor: mysql })).toBeInstanceOf(MySqlWorkflowStore);

      // @ts-expect-error A MySQL executor in PostgresWorkflowStore's options
      expect(() => new PostgresWorkflowStore({ executor: mysql })).toThrow('PostgresWorkflowStore runs on PostgreSQL, and `executor` is a MySQL executor');
      // @ts-expect-error A PostgreSQL executor in MySqlWorkflowStore's options
      expect(() => new MySqlWorkflowStore({ executor: postgres })).toThrow('MySqlWorkflowStore runs on MySQL, and `executor` is a PostgreSQL executor');
    } finally {
      await pgPool.end();
      await mysqlPool.end();
    }
  });
});
