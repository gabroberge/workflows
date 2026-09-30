#!/usr/bin/env node
// The `nest-workflows` command: PostgresWorkflowStore's migrations from a shell or a CI step.
import { runStoreCli } from '@nestjs/store-kit';
import { workflowStoreSchema } from './migrations/index.js';

process.exitCode = await runStoreCli([workflowStoreSchema], process.argv.slice(2));
