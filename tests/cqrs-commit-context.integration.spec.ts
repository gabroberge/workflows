/**
 * An aggregate's `commit({ transaction })` and `@nestjs/workflows/cqrs` (@nestjs/cqrs 12.1 or
 * later, which passes a dispatcher context through `commit()` and returns what the event bus
 * returns): for aggregates merged with `mergeObjectContext()` and `mergeClassContext()`, and for
 * `@Publishable()` ones, the starts and signals commit and roll back with the command handler's
 * transaction on the SQL stores, a failed start rejects `commit()`, and the in-memory handlers
 * and sagas still receive the events. Skipped, with the reason, when the installed
 * @nestjs/cqrs's `commit()` takes no context (`commitTakesContext` tries it).
 */
import { Logger } from '@nestjs/common';
import { AggregateRoot, CqrsModule, EventBus, EventPublisher, Publishable, UnhandledExceptionBus, type UnhandledExceptionInfo } from '@nestjs/cqrs';
import { sql } from 'drizzle-orm';
import { WorkflowsCqrsModule } from '../lib/cqrs/index.js';
import { ManualWorkflowClock, WorkflowIdConflictError } from '../lib/index.js';
import {
  commitTakesContext,
  cqrsProviders,
  fulfilmentId,
  Ledger,
  OrderFulfilmentWorkflow,
  OrderPlacedEvent,
  PaymentCapturedEvent,
  type CommitWithContext,
} from './cqrs-app.js';
import type { Database } from './fixtures/database/drizzle.js';
import { orders } from './fixtures/database/schema.js';
import { boot, connect, storedSignals, storeKind, tempDb, waitFor, type Connection, type Node, type TestDb } from './support.js';

class Order extends AggregateRoot {
  constructor(readonly id: string) {
    super();
  }

  place(total: number) {
    this.apply(new OrderPlacedEvent(this.id, total));
  }

  pay(chargeId: string, amount: number) {
    this.apply(new PaymentCapturedEvent(this.id, chargeId, amount));
  }
}

/** `aggregate.commit(context)`, which type-checks against @nestjs/cqrs 12.0 and 11 too (CI runs tsc on them). */
const commit = (aggregate: CommitWithContext, context?: unknown) => aggregate.commit(context);

const COMMIT_WARNING = "starts or signals workflows, and an aggregate's commit() published it";

describe('an aggregate’s commit() with a dispatcher context', () => {
  if (!commitTakesContext) {
    beforeEach((context) =>
      context.skip("the installed @nestjs/cqrs doesn't pass a dispatcher context through an aggregate's commit() (12.1 or later does)"),
    );
  }

  let db: TestDb;
  let ledger: Ledger;
  let warnings: string[];
  let errors: string[];
  const nodes: Node[] = [];

  beforeEach(async () => {
    db = await tempDb();
    ledger = new Ledger();
    ledger.lookUpInstances = false;
    warnings = [];
    errors = [];
    vi.spyOn(Logger.prototype, 'warn').mockImplementation((message: unknown) => void warnings.push(String(message)));
    vi.spyOn(Logger.prototype, 'error').mockImplementation((message: unknown) => void errors.push(String(message)));
    if (storeKind !== 'memory') {
      const connection = connect(db);
      await (connection.db as Database).execute(sql`DELETE FROM orders`);
      await connection.close();
    }
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    for (const node of nodes.splice(0)) {
      await node.close();
    }
    db?.cleanup();
  });

  const start = async () => {
    const node = await boot({
      db,
      clock: new ManualWorkflowClock(),
      imports: [CqrsModule.forRoot(), WorkflowsCqrsModule],
      workflows: [OrderFulfilmentWorkflow],
      providers: [...cqrsProviders, { provide: Ledger, useValue: ledger }],
    });
    nodes.push(node);
    const unhandled: UnhandledExceptionInfo<unknown>[] = [];
    node.moduleRef.get(UnhandledExceptionBus).subscribe((info) => unhandled.push(info));
    return { ...node, eventBus: node.moduleRef.get(EventBus), publisher: node.moduleRef.get(EventPublisher), unhandled };
  };

  /** Boots an application and returns it with a way to create orders of one kind of aggregate. */
  const kinds: Array<[string, () => Promise<{ node: Awaited<ReturnType<typeof start>>; order: (id: string) => Order }>]> = [
    [
      'merged with mergeObjectContext()',
      async () => {
        const node = await start();
        return { node, order: (id) => node.publisher.mergeObjectContext(new Order(id)) };
      },
    ],
    [
      'merged with mergeClassContext()',
      async () => {
        const node = await start();
        const MergedOrder = node.publisher.mergeClassContext(Order);
        return { node, order: (id) => new MergedOrder(id) };
      },
    ],
    [
      '@Publishable()',
      async () => {
        // Declared per test: CqrsModule binds @Publishable() classes to the event bus of the next application that starts.
        @Publishable()
        class PublishableOrder extends Order {}
        const node = await start();
        return { node, order: (id) => new PublishableOrder(id) };
      },
    ],
  ];

  describe.runIf(storeKind !== 'memory')('with the command handler’s transaction', () => {
    let connection: Connection;
    let database: Database;

    beforeEach(() => {
      connection = connect(db);
      database = connection.db as Database;
    });

    afterEach(async () => {
      await connection?.close();
    });

    /** A command handler that saves the order and commits the aggregate in the same transaction. */
    const placeAndPay = (order: Order, fail?: string) =>
      database.transaction(async (tx) => {
        order.place(2499);
        order.pay(`ch_${order.id}`, 2499);
        await tx.insert(orders).values({ id: order.id, userId: 'u_42', items: [], total: 2499, status: 'placed' });
        await commit(order, { transaction: tx });
        if (fail) {
          throw new Error(fail);
        }
      });

    describe.each(kinds)('of an aggregate %s', (_name, make) => {
      it('commits the start and the signal with the order, without the commit() warning, and the handlers and sagas see the events', async () => {
        const { node, order } = await make();
        const aggregate = order('o-1');

        await placeAndPay(aggregate);
        expect(aggregate.getUncommittedEvents()).toEqual([]);
        expect(await database.select({ id: orders.id }).from(orders)).toEqual([{ id: 'o-1' }]);
        expect(await database.select({ key: storedSignals.key, dedupeId: storedSignals.dedupeId }).from(storedSignals)).toEqual([
          { key: 'o-1', dedupeId: 'ch_o-1' },
        ]);
        await waitFor(() => ledger.saga.length === 1);
        expect(ledger.handled).toEqual([{ event: 'OrderPlacedEvent', orderId: 'o-1' }]);
        expect(ledger.saga).toEqual(['o-1']);

        // The signal was stored after the instance started, so its wait takes it on the first run.
        expect(await node.worker.drain()).toBe(1);
        expect(await node.client.getStatus(fulfilmentId('o-1'))).toMatchObject({ status: 'completed', runs: 1, output: { chargeId: 'ch_o-1' } });
        expect(warnings.filter((warning) => warning.includes(COMMIT_WARNING))).toEqual([]);
      });

      it('rolls the start and the signal back with the order', async () => {
        const { node, order } = await make();

        await expect(placeAndPay(order('o-1'), 'the warehouse is closed')).rejects.toThrow('the warehouse is closed');
        expect(await database.select().from(orders)).toEqual([]);
        expect(await database.select().from(storedSignals)).toEqual([]);
        expect(await node.client.list()).toEqual([]);
        expect(await node.worker.drain()).toBe(0);
      });

      it('rejects commit() when the start fails, which rolls the transaction back, and reports nothing', async () => {
        const { node, order } = await make();
        await placeAndPay(order('o-1'));
        await database.execute(sql`DELETE FROM orders`);

        // The same order placed again with another total: the start conflicts, and the handler awaits it.
        const again = order('o-1');
        await expect(
          database.transaction(async (tx) => {
            again.place(9999);
            await tx.insert(orders).values({ id: 'o-1', userId: 'u_42', items: [], total: 9999, status: 'placed' });
            await commit(again, { transaction: tx });
          }),
        ).rejects.toThrow(WorkflowIdConflictError);
        expect(await database.select().from(orders)).toEqual([]);
        // Cleared when commit() handed them over, as before: a retry loads the order again.
        expect(again.getUncommittedEvents()).toEqual([]);
        await new Promise((resolve) => setTimeout(resolve, 20));
        expect(node.unhandled).toEqual([]);
        expect(errors).toEqual([]);
        // Delivered to the in-memory reactions once, by the first commit().
        expect(ledger.saga).toEqual(['o-1']);
      });
    });
  });

  describe('without a transaction', () => {
    it('resolves once a merged aggregate’s workflow started, and still warns about the transaction it lacks', async () => {
      const { node, order } = await kinds[0][1]();
      const aggregate = order('o-1');
      aggregate.place(2499);

      await commit(aggregate);
      expect(await node.client.getStatus(fulfilmentId('o-1'))).toMatchObject({ status: 'pending' });
      expect(ledger.handled).toEqual([{ event: 'OrderPlacedEvent', orderId: 'o-1' }]);
      expect(ledger.saga).toEqual(['o-1']);
      expect(warnings).toEqual([expect.stringContaining(`OrderPlacedEvent ${COMMIT_WARNING} without a transaction`)]);

      // Awaited, a failure rejects commit(); it is reported too, as an unawaited one would be.
      const again = order('o-1');
      again.place(9999);
      await expect(commit(again)).rejects.toThrow(WorkflowIdConflictError);
      expect(node.unhandled).toEqual([{ cause: expect.any(OrderPlacedEvent), exception: expect.any(WorkflowIdConflictError) }]);
      expect(errors).toEqual([expect.stringContaining("Publishing OrderPlacedEvent from an aggregate's commit() without a transaction failed")]);
    });

    it('resolves once a @Publishable() aggregate’s workflow started, and rejects when the start fails, without a warning', async () => {
      const { node, order } = await kinds[2][1]();
      const aggregate = order('o-1');
      aggregate.place(2499);

      await commit(aggregate);
      expect(await node.client.getStatus(fulfilmentId('o-1'))).toMatchObject({ status: 'pending' });
      expect(ledger.saga).toEqual(['o-1']);

      const again = order('o-1');
      again.place(9999);
      await expect(commit(again)).rejects.toThrow(WorkflowIdConflictError);
      // It passes no dispatcher context, so nothing tells it apart from a plain publish(): no warning, no report.
      expect(node.unhandled).toEqual([]);
      expect(warnings).toEqual([]);
      expect(errors).toEqual([]);
    });
  });
});
