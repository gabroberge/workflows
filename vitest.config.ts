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
    globalSetup: ['tests/support/global-setup.ts'],
    // The engine suites and the store contract run once per store (tests/support.ts): in
    // memory, and the tutorial's DrizzleWorkflowStore (tests/fixtures/) on PGlite and on
    // PostgreSQL (SQL_TEST_PG_URL, else a throwaway cluster from local binaries, else skipped
    // with the reason). `--project workflows:pglite` runs one of them.
    projects: [
      {
        extends: true,
        test: { name: 'workflows:memory', include: ['tests/**/*.spec.ts'], env: { WORKFLOWS_TEST_STORE: 'memory' } },
      },
      ...(['pglite', 'postgres'] as const).map((store) => ({
        extends: true as const,
        test: {
          name: `workflows:${store}`,
          include: ['tests/**/*.spec.ts'],
          // The registry's own tests don't touch a store, and the CQRS checks and routing run
          // on the in-memory store (cqrs.integration.spec.ts covers the SQL stores).
          exclude: [...configDefaults.exclude, 'tests/storage.spec.ts', 'tests/cqrs.spec.ts'],
          env: { WORKFLOWS_TEST_STORE: store },
          testTimeout: 20_000,
          hookTimeout: 30_000,
        },
      })),
    ],
  },
});
