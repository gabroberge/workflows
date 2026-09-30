import { StoreSchema } from '@nestjs/store-kit/mysql';
import { WorkflowSchemaError } from '../../sql/workflow-schema.error.js';
import { initialMigration } from './initial.migration.js';

/**
 * MySqlWorkflowStore's schema: every version of it, in order (a new one goes last, and none ever changes once
 * released), and what the store, its statics and `nest-workflows` do with them. Its tables live in the connection's
 * database, named `<schema>_<table>`, next to the kit's `<schema>_migrations` and `<schema>_locks`.
 */
export const mysqlWorkflowStoreSchema = new StoreSchema({
  packageName: '@nestjs/workflows',
  storeName: 'MySqlWorkflowStore',
  command: 'nest-workflows',
  defaultSchema: 'nest_workflows',
  migrations: [initialMigration],
  createError: (message, details) => new WorkflowSchemaError(message, details),
});
