#!/usr/bin/env node
// The `nest-workflows` command: PostgresWorkflowStore's migrations from a shell or a CI step.
import { runCli } from './utils/cli.util.js';

process.exitCode = await runCli(process.argv.slice(2), {
  out: (text) => process.stdout.write(text),
  err: (text) => process.stderr.write(text),
  env: process.env,
});
