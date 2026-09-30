// The `@nestjs/workflows/postgres` entry: the first-party PostgreSQL store. Nothing here imports a driver or an ORM:
// the executors reach the client the application passes them.

// The store, and the error it fails startup with while its schema is behind
export { PostgresWorkflowStore } from './postgres-workflow.store.js';
export { WorkflowSchemaError } from './errors/index.js';
export type { PostgresWorkflowStoreOptions } from './interfaces/index.js';

// Executors: the store's SQL through the application's pool or ORM, and its transactions
export { fromDrizzle, fromKysely, fromPg, fromPrisma, fromTypeOrm, type PrismaExecutorOptions } from './executors/index.js';
export type { SqlExecutor, SqlIsolationLevel, SqlTransaction, SqlTransactionOptions } from './interfaces/index.js';
