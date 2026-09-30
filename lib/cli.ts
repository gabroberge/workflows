#!/usr/bin/env node
// The `nest-workflows` command: the first-party stores' migrations from a shell or a CI step. `migrate` and `status`
// pick the store by the URL (postgres:// or postgresql://: PostgresWorkflowStore; mysql://: MySqlWorkflowStore), and
// `sql` prints PostgreSQL's unless `--dialect mysql` says otherwise.
import { runStoreCli } from '@nestjs/store-kit';
import { mysqlWorkflowStoreSchema } from './mysql/migrations/index.js';
import { workflowStoreSchema } from './postgres/migrations/index.js';

process.exitCode = await runStoreCli([workflowStoreSchema, mysqlWorkflowStoreSchema], process.argv.slice(2));
