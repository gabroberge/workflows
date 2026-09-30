import { parseArgs } from 'node:util';
import { fromPg } from '../executors/pg.executor.js';
import { MIGRATIONS } from '../migrations/index.js';
import { applyMigrations, latestVersion, migrationScript, schemaVersion } from './migrations.util.js';

const USAGE = `Usage: nest-workflows <command> [options]

PostgresWorkflowStore's schema (@nestjs/workflows/postgres):

  migrate   Apply the migrations the schema hasn't had yet (one transaction, under an advisory lock)
  status    Print the schema's version and the one this version of @nestjs/workflows needs;
            exit with 1 while it is behind
  sql       Print the migrations' SQL, for your own migration tool (no database needed)

Options:
  --url <url>        The database (postgres://...). Default: $DATABASE_URL
  --schema <name>    The store's schema. Default: nest_workflows
  --from <version>   sql: the version to start from. Default: 0 (a new database)
  --to <version>     sql: the version to end at. Default: the latest
`;

/** Where the command line writes, and what it reads from the environment. */
export interface CliIo {
  out(text: string): void;
  err(text: string): void;
  env: Record<string, string | undefined>;
}

/** `nest-workflows <command>`: resolves to the process's exit code. */
export async function runCli(argv: string[], io: CliIo): Promise<number> {
  let parsed: ReturnType<typeof parse>;
  try {
    parsed = parse(argv);
  } catch (error) {
    io.err(`${(error as Error).message}\n\n${USAGE}`);
    return 1;
  }

  const { positionals, values } = parsed;
  const [command] = positionals;
  if (values.help || command === undefined || command === 'help') {
    (command === undefined && !values.help ? io.err : io.out)(USAGE);
    return command === undefined && !values.help ? 1 : 0;
  }

  const schema = values.schema ?? 'nest_workflows';
  try {
    if (command === 'sql') {
      io.out(migrationScript(schema, MIGRATIONS, { from: version(values.from, '--from'), to: version(values.to, '--to') }));
      return 0;
    }
    if (command !== 'migrate' && command !== 'status') {
      io.err(`Unknown command "${command}".\n\n${USAGE}`);
      return 1;
    }

    const url = values.url ?? io.env.DATABASE_URL;
    if (!url) {
      io.err(`nest-workflows ${command} needs the database: pass --url, or set DATABASE_URL.\n`);
      return 1;
    }
    return await withPool(url, io, async (pool) => {
      const executor = fromPg(pool);
      const latest = latestVersion(MIGRATIONS);
      if (command === 'status') {
        const current = await schemaVersion(executor, schema);
        io.out(`Schema "${schema}" is at version ${current}; this version of @nestjs/workflows needs version ${latest}.\n`);
        return current >= latest ? 0 : 1;
      }

      const applied = await applyMigrations(executor, schema, MIGRATIONS);
      io.out(
        applied.length > 0
          ? `Migrated schema "${schema}" to version ${applied.at(-1)} (applied ${applied.join(', ')}).\n`
          : `Schema "${schema}" is up to date (version ${await schemaVersion(executor, schema)}).\n`,
      );
      return 0;
    });
  } catch (error) {
    io.err(`${(error as Error)?.message ?? error}\n`);
    return 1;
  }
}

function parse(argv: string[]) {
  return parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      url: { type: 'string' },
      schema: { type: 'string' },
      from: { type: 'string' },
      to: { type: 'string' },
      help: { type: 'boolean', short: 'h' },
    },
  });
}

function version(value: string | undefined, option: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!/^\d+$/.test(value)) {
    throw new TypeError(`${option} takes a version number, not "${value}".`);
  }
  return Number(value);
}

/** A pool of one connection on `url`, ended afterwards; `pg` comes from the application's dependencies. */
async function withPool(url: string, io: CliIo, work: (pool: import('pg').Pool) => Promise<number>): Promise<number> {
  let Pool: typeof import('pg').Pool;
  try {
    ({ Pool } = (await import('pg')).default);
  } catch {
    io.err('nest-workflows needs the pg package to reach the database (npm i pg), or print the SQL with `nest-workflows sql` and apply it with your own tool.\n');
    return 1;
  }

  const pool = new Pool({ connectionString: url, max: 1 });
  try {
    return await work(pool);
  } finally {
    await pool.end();
  }
}
