/** A PostgreSQL transaction isolation level. */
export type SqlIsolationLevel = 'read uncommitted' | 'read committed' | 'repeatable read' | 'serializable';

/** What `SqlExecutor.transaction()` takes. */
export interface SqlTransactionOptions {
  /** Default: the database's (`default_transaction_isolation`, `read committed` unless changed). */
  isolationLevel?: SqlIsolationLevel;
}

/**
 * Runs SQL on one transaction's connection: what `SqlExecutor.transaction()` hands its callback, and what
 * `SqlExecutor.wrapTransaction()` makes of the application's own transaction object.
 */
export interface SqlTransaction {
  /**
   * Runs one statement, with `$1`, `$2`... bound to `params` in that order, and resolves to its rows (`[]` for a
   * statement that returns none).
   */
  query<R extends object = Record<string, unknown>>(text: string, params?: readonly unknown[]): Promise<R[]>;
}

/**
 * How a store reaches PostgreSQL through the client the application already has. `fromPg()`, `fromDrizzle()`,
 * `fromTypeOrm()`, `fromPrisma()` and `fromKysely()` make one of a pool or an ORM; anything else can implement these
 * three methods.
 *
 * ```ts
 * new PostgresWorkflowStore({ executor: fromDrizzle(db) }, storage);
 * ```
 */
export interface SqlExecutor {
  /** Runs one statement outside any transaction (on a pool, on any of its connections), as `SqlTransaction.query()`. */
  query<R extends object = Record<string, unknown>>(text: string, params?: readonly unknown[]): Promise<R[]>;
  /**
   * Runs `work` in a transaction of its own, on one connection: commits when `work` resolves, rolls back and rethrows
   * when it rejects.
   */
  transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options?: SqlTransactionOptions): Promise<T>;
  /**
   * The application's transaction object (Drizzle's `tx`, a TypeORM `EntityManager`, a Prisma transaction client, a
   * Kysely `Transaction`, a node-postgres client after `BEGIN`), so statements run in it and commit or roll back with
   * the application's own writes. Throws a `TypeError` for anything else, such as the database or pool itself.
   */
  wrapTransaction(transaction: unknown): SqlTransaction;
}
