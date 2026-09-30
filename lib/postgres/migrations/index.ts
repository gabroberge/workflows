import { StoreSchema } from '@nestjs/store-kit/postgres';
import { WorkflowSchemaError } from '../errors/workflow-schema.error.js';
import { initialMigration } from './initial.migration.js';

/**
 * PostgresWorkflowStore's schema: every version of it, in order (a new one goes last, and none ever changes once
 * released), and what the store, its statics and `nest-workflows` do with them.
 */
export const workflowStoreSchema = new StoreSchema({
  packageName: '@nestjs/workflows',
  storeName: 'PostgresWorkflowStore',
  command: 'nest-workflows',
  defaultSchema: 'nest_workflows',
  migrations: [initialMigration],
  createError: (message, details) => new WorkflowSchemaError(message, details),
});
