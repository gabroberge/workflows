import { configDefaults, defineConfig } from 'vitest/config';

/** The PostgreSQL projects' setup: their Prisma client, and the sweep of stale `wft_` databases (SQL_TEST_PG_URL). */
const postgresSetup = ['tests/support/generate-prisma-client.ts', 'tests/support/global-setup.ts'];

/** The MySQL project's setup: the sweep of stale `wft_` databases on SQL_TEST_MYSQL_URL. */
const mysqlSetup = ['tests/support/mysql-global-setup.ts'];

/**
 * The MySQL project runs after the others, on at most this many files at a time: the server may be shared (the nest
 * repo's integration MySQL, max_connections 151), and each file keeps a few small pools open (about 20 connections at
 * most in all, measured).
 */
const mysqlWorkers = 4;

export default defineConfig({
  // Legacy decorators with emitted metadata, as in the nestjs/nest monorepo, so
  // parameter decorators and DI type lookup work in the specs. Class fields declared
  // without an initializer are types only, as under `tsc` with `useDefineForClassFields: false`.
  oxc: {
    decorator: {
      legacy: true,
      emitDecoratorMetadata: true,
    },
    assumptions: { setPublicClassFields: true },
    typescript: { removeClassFieldsWithoutInitializer: true },
  },
  test: {
    globals: true,
    setupFiles: ['reflect-metadata'],
    // The engine suites run once per store (tests/support.ts): in memory, and PostgresWorkflowStore
    // (@nestjs/workflows/postgres) on PGlite and on PostgreSQL (SQL_TEST_PG_URL, else a throwaway
    // cluster from local binaries, else skipped with the reason). There, contract.spec.ts checks the
    // tutorial's hand-written DrizzleWorkflowStore (tests/fixtures/). tests/postgres/ and tests/mysql/
    // are the SQL stores' own projects (MySQL: SQL_TEST_MYSQL_URL, else skipped with the reason): their
    // contract through every executor, transactions, migrations. `--project workflows:pglite` runs one
    // of them.
    projects: [
      {
        extends: true,
        test: {
          name: 'workflows:memory',
          include: ['tests/**/*.spec.ts'],
          exclude: [...configDefaults.exclude, 'tests/postgres/**', 'tests/mysql/**'],
          env: { WORKFLOWS_TEST_STORE: 'memory' },
          globalSetup: postgresSetup,
        },
      },
      ...(['pglite', 'postgres'] as const).map((store) => ({
        extends: true as const,
        test: {
          name: `workflows:${store}`,
          include: ['tests/**/*.spec.ts'],
          // The registry's own tests don't touch a store, nor do the core's (tests/core/: time zones, cron, RRULE and
          // the rest; tests/postgres/ runs its PostgreSQL parts), and the CQRS checks and routing run on the in-memory
          // store (cqrs.integration.spec.ts covers the SQL stores).
          exclude: [
            ...configDefaults.exclude,
            'tests/postgres/**',
            'tests/mysql/**',
            'tests/core/**',
            'tests/storage.spec.ts',
            'tests/cqrs.spec.ts',
          ],
          env: { WORKFLOWS_TEST_STORE: store },
          testTimeout: 20_000,
          hookTimeout: 30_000,
          globalSetup: postgresSetup,
        },
      })),
      {
        extends: true,
        test: {
          name: 'workflows:postgres-store',
          include: ['tests/postgres/**/*.spec.ts'],
          testTimeout: 30_000,
          hookTimeout: 30_000,
          globalSetup: postgresSetup,
        },
      },
      {
        extends: true,
        test: {
          name: 'workflows:mysql-store',
          include: ['tests/mysql/**/*.spec.ts'],
          testTimeout: 30_000,
          hookTimeout: 30_000,
          // Its own Prisma client too (tests/fixtures/prisma-mysql), so no two projects' setups write the same files.
          globalSetup: ['tests/support/generate-prisma-mysql-client.ts', ...mysqlSetup],
          // vitest 5 runs projects with another worker count in a group of their own.
          maxWorkers: mysqlWorkers,
          sequence: { groupOrder: 1 },
        },
      },
    ],
  },
});
