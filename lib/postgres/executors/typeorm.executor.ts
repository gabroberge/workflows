import type { SqlExecutor, SqlTransaction, SqlTransactionOptions } from '../interfaces/sql-executor.interface.js';
import { describeValue, hasMethod, isolationSql } from '../utils/executor.util.js';

/** The part of a TypeORM `QueryRunner` the executor uses. */
export interface TypeOrmQueryRunnerLike {
  query(query: string, parameters?: any[], useStructuredResult?: boolean): Promise<any>;
  readonly isTransactionActive: boolean;
  release(): Promise<void>;
}

/** The part of a TypeORM `DataSource` the executor uses. */
export interface TypeOrmDataSourceLike {
  createQueryRunner(): TypeOrmQueryRunnerLike;
  transaction<T>(work: (manager: any) => Promise<T>): Promise<T>;
  transaction<T>(isolationLevel: any, work: (manager: any) => Promise<T>): Promise<T>;
  readonly options: { readonly type: string };
}

/** The part of a TypeORM `EntityManager` the executor uses. */
export interface TypeOrmEntityManagerLike {
  readonly connection: TypeOrmDataSourceLike;
  readonly queryRunner?: TypeOrmQueryRunnerLike;
}

const DATA_SOURCE = Symbol.for('DataSource');
const ENTITY_MANAGER = Symbol.for('EntityManager');

/**
 * A `SqlExecutor` on a TypeORM `DataSource` for PostgreSQL (or its `manager`). The transaction object is the
 * `EntityManager` that `dataSource.transaction()` hands its callback, or a `QueryRunner` (or its `manager`) after
 * `startTransaction()`.
 *
 * ```ts
 * new PostgresWorkflowStore({ executor: fromTypeOrm(dataSource) }, storage);
 *
 * await dataSource.transaction(async (manager) => {
 *   await manager.save(OrderEntity, order);
 *   await workflowClient.start(OrderFulfilmentWorkflow, order, { transaction: manager });
 * });
 * ```
 */
export function fromTypeOrm(dataSource: TypeOrmDataSourceLike | TypeOrmEntityManagerLike): SqlExecutor {
  const source = isEntityManager(dataSource) ? dataSource.connection : dataSource;
  if (isEntityManager(dataSource) && dataSource.queryRunner?.isTransactionActive) {
    throw new TypeError("fromTypeOrm() takes the DataSource (or its manager), not a transaction's manager: pass that to start() and signal() as { transaction: manager }.");
  }
  if (!isDataSource(source)) {
    throw new TypeError(`fromTypeOrm() takes a TypeORM DataSource (or its manager), got ${describeValue(dataSource)}.`);
  }
  if (source.options.type !== 'postgres') {
    throw new TypeError(`fromTypeOrm() takes a DataSource of type 'postgres', not '${source.options.type}'.`);
  }
  return new TypeOrmExecutor(source);
}

class TypeOrmExecutor implements SqlExecutor {
  constructor(private readonly dataSource: TypeOrmDataSourceLike) {}

  async query<R extends object>(text: string, params: readonly unknown[] = []): Promise<R[]> {
    const runner = this.dataSource.createQueryRunner();
    try {
      return await run<R>(runner, text, params);
    } finally {
      await runner.release();
    }
  }

  async transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options: SqlTransactionOptions = {}): Promise<T> {
    const run = (manager: TypeOrmEntityManagerLike) => work(runnerTransaction(manager.queryRunner!));
    return options.isolationLevel ? this.dataSource.transaction(isolationSql(options.isolationLevel), run) : this.dataSource.transaction(run);
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    const runner = isEntityManager(transaction) ? transaction.queryRunner : isQueryRunner(transaction) ? transaction : undefined;
    if (!runner?.isTransactionActive) {
      throw new TypeError(
        isEntityManager(transaction) || isQueryRunner(transaction)
          ? 'Pass the EntityManager your dataSource.transaction() callback receives (or a QueryRunner after startTransaction()), not dataSource.manager: it runs each statement outside your transaction.'
          : `Pass the EntityManager your TypeORM dataSource.transaction() callback receives, got ${describeValue(transaction)}.`,
      );
    }
    return runnerTransaction(runner);
  }
}

function runnerTransaction(runner: TypeOrmQueryRunnerLike): SqlTransaction {
  return { query: <R extends object>(text: string, params: readonly unknown[] = []) => run<R>(runner, text, params) };
}

/** The structured result: a plain `query()` answers an UPDATE or DELETE with `[rows, count]`. */
async function run<R extends object>(runner: TypeOrmQueryRunnerLike, text: string, params: readonly unknown[]): Promise<R[]> {
  const result = await runner.query(text, [...params], true);
  return (result?.records ?? []) as R[];
}

function isDataSource(value: unknown): value is TypeOrmDataSourceLike {
  return (value as { '@instanceof'?: unknown } | null)?.['@instanceof'] === DATA_SOURCE && hasMethod(value, 'createQueryRunner');
}

function isEntityManager(value: unknown): value is TypeOrmEntityManagerLike {
  return (value as { '@instanceof'?: unknown } | null)?.['@instanceof'] === ENTITY_MANAGER;
}

function isQueryRunner(value: unknown): value is TypeOrmQueryRunnerLike & { manager: unknown } {
  return hasMethod(value, 'query') && hasMethod(value, 'startTransaction') && typeof (value as { isTransactionActive?: unknown }).isTransactionActive === 'boolean';
}
