import type { SqlExecutor, SqlTransaction } from '../interfaces/sql-executor.interface.js';
import { WorkflowSchemaError } from '../errors/workflow-schema.error.js';
import { quoteSchema } from './sql.util.js';

/**
 * One version of the store's schema. Migrations only ever add (tables, nullable or defaulted columns, indexes): in a
 * rolling deploy, the processes of the previous version keep running on the migrated schema. Downgrades aren't
 * supported: a schema never goes back to an earlier version.
 */
export interface StoreMigration {
  /** 1, 2, 3...: the schema's version once it's applied. */
  version: number;
  name: string;
  /** Its statements, for the schema (quoted). Each runs on its own, in the migration's transaction. */
  up(schema: string): string[];
}

/**
 * The SQL that brings `schema` from version `from` to `to`, statement by statement, bookkeeping included: from
 * version 0, the schema and the `migrations` table that records each version.
 */
export function migrationStatements(schema: string, migrations: readonly StoreMigration[], range: { from?: number; to?: number } = {}): string[] {
  const s = quoteSchema(schema);
  const latest = latestVersion(migrations);
  const from = range.from ?? 0;
  const to = range.to ?? latest;
  if (!Number.isInteger(from) || !Number.isInteger(to) || from < 0 || to > latest || from > to) {
    throw new RangeError(
      `PostgresWorkflowStore.migrationSql(): no migrations lead from version ${from} to ${to}. Versions go from 0 (none applied) to ${latest}, and never back: downgrades aren't supported.`,
    );
  }

  const statements = from === 0 && to > 0 ? [createSchemaStatement(s), `CREATE TABLE IF NOT EXISTS ${s}.migrations (
  version integer PRIMARY KEY,
  name text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
)`] : [];
  for (const migration of migrations.filter((m) => m.version > from && m.version <= to)) {
    statements.push(...migration.up(s), `INSERT INTO ${s}.migrations (version, name) VALUES (${migration.version}, '${migration.name}')`);
  }
  return statements;
}

/** `migrationStatements()` as one script: a statement per paragraph, each ending with a semicolon. */
export function migrationScript(schema: string, migrations: readonly StoreMigration[], range: { from?: number; to?: number } = {}): string {
  const statements = migrationStatements(schema, migrations, range);
  const from = range.from ?? 0;
  const to = range.to ?? latestVersion(migrations);
  const header = [
    `-- @nestjs/workflows: PostgresWorkflowStore's schema "${schema}", from version ${from} to ${to}.`,
    '-- Run it in one transaction.',
  ];
  return `${header.join('\n')}\n\n${statements.map((statement) => `${statement};`).join('\n\n')}\n`;
}

/**
 * Applies the migrations `schema` hasn't had yet, in one transaction that holds an advisory lock for the schema: of
 * processes that start together, one migrates, and the others wait for it and find nothing to do. Resolves to the
 * versions it applied.
 */
export async function applyMigrations(executor: SqlExecutor, schema: string, migrations: readonly StoreMigration[]): Promise<number[]> {
  const latest = latestVersion(migrations);
  return executor.transaction(
    async (tx) => {
      await tx.query('SELECT pg_advisory_xact_lock(hashtext($1::text))::text AS locked', [`@nestjs/workflows:migrate:${schema}`]);
      const current = await schemaVersion(tx, schema);
      if (current >= latest) {
        return [];
      }

      // A schema someone created for the store is used as it is: CREATE SCHEMA needs the CREATE privilege on the
      // database even with IF NOT EXISTS.
      const exists = await schemaExists(tx, schema);
      const skip = exists ? createSchemaStatement(quoteSchema(schema)) : undefined;
      for (const statement of migrationStatements(schema, migrations, { from: current })) {
        if (statement !== skip) {
          try {
            await tx.query(statement);
          } catch (error) {
            throw new WorkflowSchemaError(
              `PostgresWorkflowStore: migrating schema "${schema}" from version ${current} to ${latest} failed, and nothing was applied: ${(error as Error)?.message ?? error}`,
              { schema, version: current, requiredVersion: latest, cause: error },
            );
          }
        }
      }
      return migrations.filter((m) => m.version > current).map((m) => m.version);
    },
    { isolationLevel: 'read committed' },
  );
}

/** Throws a `WorkflowSchemaError` unless `schema` has every migration (a newer one is fine: its code migrated it). */
export async function assertMigrated(executor: SqlTransaction, schema: string, migrations: readonly StoreMigration[], hint?: string): Promise<void> {
  const current = await schemaVersion(executor, schema);
  const latest = latestVersion(migrations);
  if (current >= latest) {
    return;
  }

  throw new WorkflowSchemaError(
    `PostgresWorkflowStore: schema "${schema}" is at version ${current}, and this version of @nestjs/workflows needs version ${latest}. ` +
      (hint ??
        `Apply its migrations: set \`migrate: true\` to apply them at startup, run \`npx nest-workflows migrate --url <database url> --schema ${schema}\`, ` +
          `or apply \`PostgresWorkflowStore.migrationSql({ schema: '${schema}', from: ${current} })\` with your migration tool.`),
    { schema, version: current, requiredVersion: latest },
  );
}

/** The last version applied to `schema`: `0` when it has none (or doesn't exist). */
export async function schemaVersion(executor: SqlTransaction, schema: string): Promise<number> {
  const [table] = await executor.query<{ exists: string }>(
    `SELECT EXISTS (
  SELECT FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = $1::text AND c.relname = 'migrations' AND c.relkind = 'r'
)::text AS exists`,
    [schema],
  );
  if (table?.exists !== 'true') {
    return 0;
  }

  const [row] = await executor.query<{ version: string }>(`SELECT coalesce(max(version), 0)::text AS version FROM ${quoteSchema(schema)}.migrations`);
  return Number(row!.version);
}

export function latestVersion(migrations: readonly StoreMigration[]): number {
  return migrations.at(-1)?.version ?? 0;
}

function createSchemaStatement(s: string): string {
  return `CREATE SCHEMA IF NOT EXISTS ${s}`;
}

async function schemaExists(tx: SqlTransaction, schema: string): Promise<boolean> {
  const [row] = await tx.query<{ exists: string }>('SELECT EXISTS (SELECT FROM pg_catalog.pg_namespace WHERE nspname = $1::text)::text AS exists', [schema]);
  return row?.exists === 'true';
}
