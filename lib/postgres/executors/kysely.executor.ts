import type { SqlExecutor, SqlIsolationLevel, SqlTransaction, SqlTransactionOptions } from '../interfaces/sql-executor.interface.js';
import { describeValue, hasMethod, isolationSql } from '../utils/executor.util.js';

/** The part of a Kysely instance (or of a `Transaction`) the executor uses. */
export interface KyselyLike {
  readonly isTransaction: boolean;
  executeQuery(query: any): Promise<{ rows: any[] }>;
  withoutPlugins(): KyselyLike;
  transaction(): { setIsolationLevel(level: SqlIsolationLevel): { execute<T>(work: (trx: any) => Promise<T>): Promise<T> }; execute<T>(work: (trx: any) => Promise<T>): Promise<T> };
}

/**
 * A `SqlExecutor` on a Kysely instance with a PostgreSQL dialect. The store's statements skip the instance's plugins
 * (a `CamelCasePlugin` would rename the columns it reads). The transaction object is the `trx` that
 * `db.transaction().execute()` hands its callback (or a controlled transaction).
 *
 * ```ts
 * new PostgresWorkflowStore({ executor: fromKysely(db) }, storage);
 *
 * await db.transaction().execute(async (trx) => {
 *   await trx.insertInto('orders').values(order).execute();
 *   await workflowClient.start(OrderFulfilmentWorkflow, order, { transaction: trx });
 * });
 * ```
 */
export function fromKysely(db: KyselyLike): SqlExecutor {
  if (!isKysely(db) || db.isTransaction) {
    throw new TypeError(
      isKysely(db)
        ? 'fromKysely() takes the Kysely instance, not a transaction: pass that to start() and signal() as { transaction: trx }.'
        : `fromKysely() takes a Kysely instance, got ${describeValue(db)}.`,
    );
  }
  return new KyselyExecutor(db);
}

class KyselyExecutor implements SqlExecutor {
  private readonly raw: KyselyLike;

  constructor(private readonly db: KyselyLike) {
    this.raw = db.withoutPlugins();
  }

  query<R extends object>(text: string, params: readonly unknown[] = []): Promise<R[]> {
    return run<R>(this.raw, text, params);
  }

  async transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options: SqlTransactionOptions = {}): Promise<T> {
    const builder = this.db.transaction();
    const inTransaction = (trx: KyselyLike) => work(kyselyTransaction(trx));
    if (!options.isolationLevel) {
      return builder.execute(inTransaction);
    }

    isolationSql(options.isolationLevel);
    return builder.setIsolationLevel(options.isolationLevel).execute(inTransaction);
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    if (!isKysely(transaction) || !transaction.isTransaction) {
      throw new TypeError(
        isKysely(transaction)
          ? 'Pass the trx your db.transaction().execute() callback receives, not the Kysely instance: it runs each statement outside your transaction.'
          : `Pass the trx your Kysely db.transaction().execute() callback receives, got ${describeValue(transaction)}.`,
      );
    }
    return kyselyTransaction(transaction);
  }
}

function kyselyTransaction(trx: KyselyLike): SqlTransaction {
  const raw = trx.withoutPlugins();
  return { query: <R extends object>(text: string, params: readonly unknown[] = []) => run<R>(raw, text, params) };
}

let kysely: Promise<typeof import('kysely')> | undefined;

async function run<R extends object>(db: KyselyLike, text: string, params: readonly unknown[]): Promise<R[]> {
  const { CompiledQuery } = await (kysely ??= import('kysely'));
  return (await db.executeQuery(CompiledQuery.raw(text, [...params]))).rows as R[];
}

function isKysely(value: unknown): value is KyselyLike {
  return hasMethod(value, 'executeQuery') && hasMethod(value, 'withoutPlugins') && typeof (value as { isTransaction?: unknown }).isTransaction === 'boolean';
}
