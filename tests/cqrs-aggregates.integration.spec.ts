/**
 * An aggregate's events and `@nestjs/workflows/cqrs`, on every store: the durable path the docs
 * recommend with any @nestjs/cqrs version (`publishAll(aggregate.getUncommittedEvents(),
 * { transaction })`, then `uncommit()`), committing and rolling back with the command handler's
 * transaction on the SQL stores, and `commit()` without a dispatcher context, of an aggregate
 * merged with `EventPublisher` and of a `@Publishable()` aggregate: both start the workflow
 * outside the transaction, in the background unless something awaits `commit()` (12.1 or later).
 * cqrs-commit-context.integration.spec.ts covers `commit({ transaction })`.
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

const COMMIT_WARNING = "OrderPlacedEvent starts or signals workflows, and an aggregate's commit() published it";

/** What `commit()` returns: nothing before @nestjs/cqrs 12.1, and from 12.1 on what the event bus returns (a promise here). */
const committed = () => (commitTakesContext ? expect.any(Promise) : undefined);

describe('aggregates’ events', () => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let ledger: Ledger;
  let warnings: string[];
  const nodes: Node[] = [];

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    ledger = new Ledger();
    ledger.lookUpInstances = false;
    warnings = [];
    vi.spyOn(Logger.prototype, 'warn').mockImplementation((message: unknown) => void warnings.push(String(message)));
    vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined); // the failed commit() below, reported
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
    db.cleanup();
  });

  const start = async () => {
    const node = await boot({
      db,
      clock,
      imports: [CqrsModule.forRoot(), WorkflowsCqrsModule],
      workflows: [OrderFulfilmentWorkflow],
      providers: [...cqrsProviders, { provide: Ledger, useValue: ledger }],
    });
    nodes.push(node);
    const unhandled: UnhandledExceptionInfo<unknown>[] = [];
    node.moduleRef.get(UnhandledExceptionBus).subscribe((info) => unhandled.push(info));
    return { ...node, eventBus: node.moduleRef.get(EventBus), publisher: node.moduleRef.get(EventPublisher), unhandled };
  };

  describe.runIf(storeKind !== 'memory')('published with publishAll() in the command handler’s transaction', () => {
    let connection: Connection;
    let database: Database;

    beforeEach(() => {
      connection = connect(db);
      database = connection.db as Database;
    });

    afterEach(async () => {
      await connection.close();
    });

    /** A command handler that saves the order and publishes its aggregate's events, the durable way. */
    const placeAndPay = (node: Awaited<ReturnType<typeof start>>, order: Order, fail?: string) =>
      database.transaction(async (tx) => {
        order.place(2499);
        order.pay(`ch_${order.id}`, 2499);
        await tx.insert(orders).values({ id: order.id, userId: 'u_42', items: [], total: 2499, status: 'placed' });
        await node.eventBus.publishAll(order.getUncommittedEvents(), { transaction: tx });
        order.uncommit();
        if (fail) {
          throw new Error(fail);
        }
      });

    it('commits the start and the signal with the order, in the events’ order', async () => {
      const node = await start();
      const order = new Order('o-1');

      await placeAndPay(node, order);
      expect(order.getUncommittedEvents()).toEqual([]);
      expect(await database.select({ id: orders.id }).from(orders)).toEqual([{ id: 'o-1' }]);
      expect(await database.select({ key: storedSignals.key, dedupeId: storedSignals.dedupeId }).from(storedSignals)).toEqual([
        { key: 'o-1', dedupeId: 'ch_o-1' },
      ]);

      // The signal was stored after the instance started, so its wait takes it on the first run.
      expect(await node.worker.drain()).toBe(1);
      expect(await node.client.getStatus(fulfilmentId('o-1'))).toMatchObject({ status: 'completed', runs: 1, output: { chargeId: 'ch_o-1' } });
      expect(warnings).toEqual([]);
    });

    it('rolls the start and the signal back with the order', async () => {
      const node = await start();

      await expect(placeAndPay(node, new Order('o-1'), 'the warehouse is closed')).rejects.toThrow('the warehouse is closed');
      expect(await database.select().from(orders)).toEqual([]);
      expect(await database.select().from(storedSignals)).toEqual([]);
      expect(await node.client.list()).toEqual([]);
      expect(await node.worker.drain()).toBe(0);
    });

    it('starts from a merged aggregate’s commit() outside the transaction: a rollback leaves the instance behind', async () => {
      const node = await start();

      await expect(
        database.transaction(async (tx) => {
          const order = node.publisher.mergeObjectContext(new Order('o-1'));
          order.place(2499);
          await tx.insert(orders).values({ id: 'o-1', userId: 'u_42', items: [], total: 2499, status: 'placed' });
          order.commit();
          throw new Error('the warehouse is closed');
        }),
      ).rejects.toThrow('the warehouse is closed');

      // The lossy path the docs warn about: a fulfilment for an order that doesn't exist.
      await waitFor(async () => (await node.client.getStatus(fulfilmentId('o-1'))) !== null);
      expect(await database.select().from(orders)).toEqual([]);
      expect(warnings).toEqual([expect.stringContaining(COMMIT_WARNING)]);
    });
  });

  it('starts the workflow from a merged aggregate’s commit() in the background, warning once, and reports a failure', async () => {
    const node = await start();

    const order = node.publisher.mergeObjectContext(new Order('o-1'));
    order.place(2499);
    expect(order.commit()).toEqual(committed()); // not awaited
    await waitFor(async () => (await node.client.getStatus(fulfilmentId('o-1'))) !== null);
    // commit() empties the array it hands over as soon as publishAll() returns: the events still reach the
    // in-memory reactions, after the start.
    await waitFor(() => ledger.saga.length === 1);
    expect(ledger.handled).toEqual([{ event: 'OrderPlacedEvent', orderId: 'o-1' }]);
    expect(ledger.saga).toEqual(['o-1']);

    const again = node.publisher.mergeObjectContext(new Order('o-1'));
    again.place(9999); // another input for the same business key: the start fails, and nobody awaits it
    again.commit();
    await waitFor(() => node.unhandled.length === 1);
    expect(node.unhandled[0]).toMatchObject({ cause: expect.any(OrderPlacedEvent), exception: expect.any(WorkflowIdConflictError) });
    expect(warnings).toEqual([expect.stringContaining(COMMIT_WARNING)]);
  });

  it('starts the workflow from a @Publishable() aggregate’s commit() in the background, without a warning, and rejects unhandled', async () => {
    // Declared per test: CqrsModule binds @Publishable() classes to the event bus of the next application that starts.
    @Publishable()
    class PublishableOrder extends Order {}

    const node = await start();
    // Keep what commit()'s publishAll() returns, handled, so the test can look at it: commit() drops it before
    // @nestjs/cqrs 12.1, and returns it from 12.1 on. (In an application nothing handles it: an unhandled rejection.)
    const returned: unknown[] = [];
    const publishAll = node.eventBus.publishAll.bind(node.eventBus);
    vi.spyOn(node.eventBus, 'publishAll').mockImplementation((...args: Parameters<EventBus['publishAll']>) => {
      const result = publishAll(...args);
      returned.push(result);
      Promise.resolve(result).catch(() => undefined);
      return result;
    });

    const order = new PublishableOrder('o-1');
    order.place(2499);
    expect(order.commit()).toEqual(committed());
    await waitFor(async () => (await node.client.getStatus(fulfilmentId('o-1'))) !== null);
    await waitFor(() => ledger.saga.length === 1);
    expect(ledger.handled).toEqual([{ event: 'OrderPlacedEvent', orderId: 'o-1' }]);
    expect(ledger.saga).toEqual(['o-1']);

    const again = new PublishableOrder('o-1');
    again.place(9999);
    again.commit();
    await expect(returned[1]).rejects.toThrow(WorkflowIdConflictError);
    // It passes no dispatcher context, so nothing tells it apart from a plain publish(): no warning, no report.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(node.unhandled).toEqual([]);
    expect(warnings).toEqual([]);
  });
});
