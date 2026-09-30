/**
 * A PostgreSQL transaction isolation level.
 *
 * ```ts
 * const isolationLevel: SqlIsolationLevel = 'read committed';
 * ```
 */
export type SqlIsolationLevel = 'read uncommitted' | 'read committed' | 'repeatable read' | 'serializable';

/**
 * What `SqlExecutor.transaction()` takes.
 *
 * ```ts
 * await executor.transaction((tx) => tx.query('SELECT pg_advisory_xact_lock(hashtext($1))', ['emails']), { isolationLevel: 'read committed' });
 * ```
 */
export interface SqlTransactionOptions {
  /** Default: the database's (`default_transaction_isolation`, `read committed` unless changed). */
  isolationLevel?: SqlIsolationLevel;
}

/**
 * Runs SQL on one transaction's connection: what `SqlExecutor.transaction()` hands its callback, and what
 * `SqlExecutor.wrapTransaction()` makes of the application's own transaction object.
 *
 * ```ts
 * const tx = executor.wrapTransaction(applicationTx);
 * await tx.query('INSERT INTO nest_queues.jobs (id, data) VALUES ($1, $2::jsonb)', [id, JSON.stringify(data)]);
 * ```
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
 * three methods. Nothing in it is about workflows: another package's PostgreSQL store (a queue's) takes the same
 * executors, and with them the same transaction objects.
 *
 * ```ts
 * new PostgresWorkflowStore({ executor: fromDrizzle(db) }, storage);
 *
 * // Or a store of your own
 * const [row] = await executor.query<{ id: string }>('SELECT id FROM nest_queues.jobs WHERE state = $1 LIMIT 1', ['waiting']);
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
