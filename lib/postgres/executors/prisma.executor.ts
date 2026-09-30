import type { SqlExecutor, SqlTransaction, SqlTransactionOptions } from '../interfaces/sql-executor.interface.js';
import { parseDuration, type Duration } from '../../core/time/duration.js';
import { describeValue, hasMethod, isolationSql } from '../utils/executor.util.js';

/** The part of a Prisma client (or of the transaction client `$transaction()` hands its callback) the executor uses. */
export interface PrismaClientLike {
  $queryRawUnsafe(query: string, ...values: any[]): PromiseLike<unknown>;
}

/** The part of a Prisma client the executor uses. */
export interface PrismaRootClientLike extends PrismaClientLike {
  $connect(): Promise<void>;
  $transaction<T>(work: (tx: any) => Promise<T>, options?: { isolationLevel?: any; maxWait?: number; timeout?: number }): Promise<T>;
}

/** What `fromPrisma()` takes besides the client: limits of the interactive transactions the store runs itself. */
export interface PrismaExecutorOptions {
  /** How long a transaction of the store's may wait to start (for a connection). Default: `'10s'` (Prisma's: 2 s). */
  maxWait?: Duration;
  /**
   * How long a transaction of the store's may run before Prisma rolls it back: its statements may wait for locks the
   * application's transactions hold, such as a `signal()` in one. Default: `'1m'` (Prisma's: 5 s).
   */
  timeout?: Duration;
}

const PRISMA_ISOLATION: Record<string, string> = {
  'READ UNCOMMITTED': 'ReadUncommitted',
  'READ COMMITTED': 'ReadCommitted',
  'REPEATABLE READ': 'RepeatableRead',
  SERIALIZABLE: 'Serializable',
};

/**
 * A `SqlExecutor` on a Prisma client for PostgreSQL (`$queryRawUnsafe()` and interactive transactions, with a driver
 * adapter such as `@prisma/adapter-pg` or Prisma's own engine). The transaction object is the transaction client that
 * `prisma.$transaction(async (tx) => ...)` hands its callback. Prisma runs one statement per raw call, and ends an
 * interactive transaction that outlasts its `timeout`: yours keep your own settings, the store's take `options`.
 *
 * ```ts
 * new PostgresWorkflowStore({ executor: fromPrisma(prismaService) }, storage);
 *
 * await prismaService.$transaction(async (tx) => {
 *   await tx.order.create({ data: order });
 *   await workflowClient.start(OrderFulfilmentWorkflow, order, { transaction: tx });
 * });
 * ```
 */
export function fromPrisma(prisma: PrismaRootClientLike, options: PrismaExecutorOptions = {}): SqlExecutor {
  if (!hasMethod(prisma, '$queryRawUnsafe') || !hasMethod(prisma, '$transaction') || !hasMethod(prisma, '$connect')) {
    throw new TypeError(
      hasMethod(prisma, '$queryRawUnsafe')
        ? 'fromPrisma() takes the Prisma client, not a transaction client: pass that to start() and signal() as { transaction: tx }.'
        : `fromPrisma() takes a Prisma client, got ${describeValue(prisma)}.`,
    );
  }
  return new PrismaExecutor(prisma, { maxWait: parseDuration(options.maxWait ?? '10s'), timeout: parseDuration(options.timeout ?? '1m') });
}

class PrismaExecutor implements SqlExecutor {
  constructor(
    private readonly prisma: PrismaRootClientLike,
    private readonly limits: { maxWait: number; timeout: number },
  ) {}

  query<R extends object>(text: string, params: readonly unknown[] = []): Promise<R[]> {
    return run<R>(this.prisma, text, params);
  }

  async transaction<T>(work: (transaction: SqlTransaction) => Promise<T>, options: SqlTransactionOptions = {}): Promise<T> {
    return this.prisma.$transaction((tx: PrismaClientLike) => work(prismaTransaction(tx)), {
      ...this.limits,
      ...(options.isolationLevel ? { isolationLevel: PRISMA_ISOLATION[isolationSql(options.isolationLevel)] } : {}),
    });
  }

  wrapTransaction(transaction: unknown): SqlTransaction {
    // A transaction client is the client without $connect() and $disconnect(): Prisma leaves them out of it.
    if (!hasMethod(transaction, '$queryRawUnsafe') || hasMethod(transaction, '$connect')) {
      throw new TypeError(
        hasMethod(transaction, '$connect')
          ? 'Pass the tx your prisma.$transaction(async (tx) => ...) callback receives, not the client: it runs each statement outside your transaction.'
          : `Pass the tx your Prisma $transaction(async (tx) => ...) callback receives, got ${describeValue(transaction)}.`,
      );
    }
    return prismaTransaction(transaction as PrismaClientLike);
  }
}

function prismaTransaction(tx: PrismaClientLike): SqlTransaction {
  return { query: <R extends object>(text: string, params: readonly unknown[] = []) => run<R>(tx, text, params) };
}

async function run<R extends object>(prisma: PrismaClientLike, text: string, params: readonly unknown[]): Promise<R[]> {
  return ((await prisma.$queryRawUnsafe(text, ...params)) ?? []) as R[];
}
