import { configDefaults, defineConfig } from 'vitest/config';

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
    globalSetup: ['tests/support/generate-prisma-client.ts', 'tests/support/global-setup.ts'],
    // The engine suites run once per store (tests/support.ts): in memory, and PostgresWorkflowStore
    // (@nestjs/workflows/postgres) on PGlite and on PostgreSQL (SQL_TEST_PG_URL, else a throwaway
    // cluster from local binaries, else skipped with the reason). There, contract.spec.ts checks the
    // tutorial's hand-written DrizzleWorkflowStore (tests/fixtures/). tests/postgres/ is
    // PostgresWorkflowStore's own project: its contract through every executor, transactions,
    // migrations. `--project workflows:pglite` runs one of them.
    projects: [
      {
        extends: true,
        test: {
          name: 'workflows:memory',
          include: ['tests/**/*.spec.ts'],
          exclude: [...configDefaults.exclude, 'tests/postgres/**'],
          env: { WORKFLOWS_TEST_STORE: 'memory' },
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
            'tests/core/**',
            'tests/storage.spec.ts',
            'tests/cqrs.spec.ts',
          ],
          env: { WORKFLOWS_TEST_STORE: store },
          testTimeout: 20_000,
          hookTimeout: 30_000,
        },
      })),
      {
        extends: true,
        test: { name: 'workflows:postgres-store', include: ['tests/postgres/**/*.spec.ts'], testTimeout: 30_000, hookTimeout: 30_000 },
      },
    ],
  },
});
