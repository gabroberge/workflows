import { WorkflowError } from '../errors/workflow.error.js';

/**
 * A first-party SQL store's schema can't serve this version of the package (`PostgresWorkflowStore`,
 * `MySqlWorkflowStore`): it is behind the store's migrations (and `migrate` is off), or applying them failed (`cause`).
 * The store refuses every call until it's fixed.
 */
export class WorkflowSchemaError extends WorkflowError {
  override name = 'WorkflowSchemaError';
  /** The store's schema: a PostgreSQL schema, or on MySQL the prefix of its tables' names. */
  readonly schema: string;
  /** The schema's version: the last migration applied to it, `0` for none. */
  readonly version: number;
  /** The version this version of the package needs: its last migration. */
  readonly requiredVersion: number;

  constructor(message: string, details: { schema: string; version: number; requiredVersion: number; cause?: unknown }) {
    super(message, details.cause === undefined ? undefined : { cause: details.cause });
    this.schema = details.schema;
    this.version = details.version;
    this.requiredVersion = details.requiredVersion;
  }
}
