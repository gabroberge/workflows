/**
 * `@nestjs/workflows/cqrs` through `CqrsModule` and `WorkflowsModule`, on every store: command
 * handlers publish events that start and signal a workflow, the workflow executes a command and
 * publishes an event from its steps, and handlers and sagas keep receiving the events. On the SQL
 * stores, the events are published in the handler's Drizzle transaction: the workflow's instance
 * and signals commit and roll back with the order, a repeated event finds the instance, and a
 * connection that dies mid-transaction leaves nothing behind.
 */
import { CommandBus, CqrsModule, EventBus } from '@nestjs/cqrs';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import pg from 'pg';
import { WorkflowsCqrsModule } from '../lib/cqrs/index.js';
import { ManualWorkflowClock, WorkflowIdConflictError } from '../lib/index.js';
import {
  CapturePaymentCommand,
  cqrsProviders,
  fulfilmentId,
  Ledger,
  OrderFulfilmentWorkflow,
  OrderPlacedEvent,
  PaymentCapturedEvent,
  PlaceOrderCommand,
} from './cqrs-app.js';
import type { Database } from './fixtures/database/drizzle.js';
import * as schema from './fixtures/database/schema.js';
import { orders, workflowSignals } from './fixtures/database/schema.js';
import { boot, connect, openStore, storeKind, tempDb, waitFor, type Connection, type Node, type TestDb } from './support.js';

describe('workflows started and signalled by CQRS events', () => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let ledger: Ledger;
  const nodes: Node[] = [];

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    ledger = new Ledger();
    if (storeKind !== 'memory') {
      const connection = connect(db);
      await (connection.db as Database).execute(sql`DELETE FROM orders`);
      await connection.close();
    }
  });

  afterEach(async () => {
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
    return { ...node, commandBus: node.moduleRef.get(CommandBus), eventBus: node.moduleRef.get(EventBus) };
  };

  const inMemory = storeKind === 'memory';

  it('starts the workflow before publish() resolves, then hands the event to handlers and sagas', async () => {
    const node = await start();
    ledger.lookUpInstances = inMemory; // in a transaction, the handler runs before the commit

    await node.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
    // Durable before publish() resolved: no waiting for a background reaction.
    expect(await node.client.getStatus(fulfilmentId('o-1'))).toMatchObject({
      workflow: 'order-fulfilment',
      version: 1,
      status: 'pending',
      input: { orderId: 'o-1', total: 2499 },
    });

    await waitFor(() => ledger.handled.length === 1);
    expect(ledger.saga).toEqual(['o-1']);
    expect(ledger.handled).toEqual([
      { event: 'OrderPlacedEvent', orderId: 'o-1', ...(inMemory ? { instance: true } : {}) },
    ]);
  });

  it('signals the waiting instance, which executes a command and publishes an event from its steps', async () => {
    const node = await start();
    ledger.lookUpInstances = false;

    await node.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
    await node.worker.drain();
    expect(await node.client.getStatus(fulfilmentId('o-1'))).toMatchObject({
      status: 'suspended',
      waits: [{ signal: 'payment.captured', key: 'o-1' }],
    });

    // Another order's payment doesn't wake it: the key is the order id.
    await node.commandBus.execute(new CapturePaymentCommand('o-2', 'ch_2', 1599));
    expect(await node.worker.drain()).toBe(0);

    await node.commandBus.execute(new CapturePaymentCommand('o-1', 'ch_1', 2499));
    await node.worker.drain();
    const done = await node.client.getStatus(fulfilmentId('o-1'), { journal: true });
    expect(done).toMatchObject({
      status: 'completed',
      output: { chargeId: 'ch_1', reservationId: 'order-o-1:reserve-stock' },
    });
    expect(done!.journal.map((entry) => `${entry.name}:${entry.status}`)).toEqual([
      'await-payment:completed',
      'reserve-stock:completed',
      'announce-ready:completed',
    ]);
    expect(ledger.reserveCalls).toBe(1);
    await waitFor(() => ledger.handled.some(({ event }) => event === 'OrderReadyEvent'));
  });

  it('starts one instance for an event published twice, and keeps the second publish working', async () => {
    const node = await start();
    ledger.lookUpInstances = false;

    await node.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
    await node.eventBus.publish(new OrderPlacedEvent('o-1', 2499));
    expect((await node.client.list()).map(({ id }) => id)).toEqual([fulfilmentId('o-1')]);
    await waitFor(() => ledger.handled.length === 2);
  });

  it('starts, then signals, the events of one publishAll() in order', async () => {
    const node = await start();
    ledger.lookUpInstances = false;

    await node.eventBus.publishAll([new OrderPlacedEvent('o-1', 2499), new PaymentCapturedEvent('o-1', 'ch_1', 2499)]);
    // The signal was sent after the instance started, so the wait takes it.
    await node.worker.drain();
    expect(await node.client.getStatus(fulfilmentId('o-1'))).toMatchObject({ status: 'completed' });
  });

  it('rejects publish() when a start fails, and hands the event to no handler', async () => {
    const node = await start();
    ledger.lookUpInstances = false;
    await node.eventBus.publish(new OrderPlacedEvent('o-1', 2499));
    await waitFor(() => ledger.handled.length === 1);

    // Same business key, different input: the instance can't be the one the event asks for.
    await expect(node.eventBus.publish(new OrderPlacedEvent('o-1', 9999))).rejects.toThrow(WorkflowIdConflictError);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(ledger.handled).toHaveLength(1);
    expect(ledger.saga).toEqual(['o-1']);
  });

  describe.runIf(storeKind !== 'memory')('in the command handler’s transaction', () => {
    let connection: Connection;
    let database: Database;

    beforeEach(async () => {
      connection = connect(db);
      database = connection.db as Database;
      ledger.lookUpInstances = false;
    });

    afterEach(async () => {
      await connection.close();
    });

    const orderIds = async () => (await database.select({ id: orders.id }).from(orders)).map(({ id }) => id).sort();
    const instanceIds = async () => {
      const { store, close } = openStore(db);
      try {
        return (await store.list({ limit: 100, offset: 0 })).map(({ id }) => id).sort();
      } finally {
        await close();
      }
    };
    const signalCount = async () => (await database.select({ id: workflowSignals.id }).from(workflowSignals)).length;

    it('commits the instance with the order, and the worker runs it after the commit', async () => {
      const node = await start();

      await node.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
      expect(await orderIds()).toEqual(['o-1']);
      expect(await instanceIds()).toEqual([fulfilmentId('o-1')]);
      expect(await node.worker.drain()).toBe(1);
      expect(await node.client.getStatus(fulfilmentId('o-1'))).toMatchObject({ status: 'suspended' });
    });

    it('rolls the instance back with the order, and a signal back with the payment', async () => {
      const node = await start();

      await expect(node.commandBus.execute(new PlaceOrderCommand('o-1', 2499, 'the warehouse is closed'))).rejects.toThrow(
        'the warehouse is closed',
      );
      expect(await orderIds()).toEqual([]);
      expect(await instanceIds()).toEqual([]);
      expect(await node.worker.drain()).toBe(0);

      await node.commandBus.execute(new PlaceOrderCommand('o-2', 1599));
      await node.worker.drain();
      await expect(node.commandBus.execute(new CapturePaymentCommand('o-2', 'ch_2', 1599, 'the ledger is locked'))).rejects.toThrow(
        'the ledger is locked',
      );
      expect(await signalCount()).toBe(0);
      expect(await node.worker.drain()).toBe(0);
      expect(await node.client.getStatus(fulfilmentId('o-2'))).toMatchObject({ status: 'suspended' });

      await node.commandBus.execute(new CapturePaymentCommand('o-2', 'ch_2', 1599));
      expect(await signalCount()).toBe(1);
      await node.worker.drain();
      expect(await node.client.getStatus(fulfilmentId('o-2'))).toMatchObject({ status: 'completed' });
    });

    it('finds the instance when the event comes again in another transaction, which still commits', async () => {
      const node = await start();

      await node.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
      await database.execute(sql`DELETE FROM orders`); // the order is placed again, say by a replayed command
      await node.commandBus.execute(new PlaceOrderCommand('o-1', 2499));

      expect(await orderIds()).toEqual(['o-1']);
      expect(await instanceIds()).toEqual([fulfilmentId('o-1')]);
    });

    it.runIf(storeKind === 'postgres')('starts one instance when concurrent transactions publish the same event', async () => {
      const node = await start();

      // Five handlers race on the pool; each inserts its own order row and publishes the same business key.
      const results = await Promise.allSettled(
        Array.from({ length: 5 }, (_, i) =>
          database.transaction(async (tx) => {
            await tx.insert(orders).values({ id: `o-${i}`, userId: 'u_42', items: [], total: 2499, status: 'placed' });
            await node.eventBus.publish(new OrderPlacedEvent('shared', 2499), { transaction: tx });
          }),
        ),
      );
      expect(results.map(({ status }) => status)).toEqual(Array(5).fill('fulfilled'));
      expect(await orderIds()).toEqual(['o-0', 'o-1', 'o-2', 'o-3', 'o-4']);
      expect(await instanceIds()).toEqual([fulfilmentId('shared')]);
    });

    it.runIf(storeKind === 'postgres')('leaves nothing behind when the connection dies inside the transaction', async () => {
      const node = await start();
      // A connection of its own, to kill: the process dies after publish() resolved, before the commit.
      const url = (database as unknown as { $client: pg.Pool }).$client.options.connectionString;
      const client = new pg.Client({ connectionString: url });
      client.on('error', () => undefined); // the terminated connection reports itself here too
      await client.connect();
      const dying = drizzle(client, { schema });

      const placed = dying.transaction(async (tx) => {
        await tx.insert(orders).values({ id: 'o-1', userId: 'u_42', items: [], total: 2499, status: 'placed' });
        await node.eventBus.publish(new OrderPlacedEvent('o-1', 2499), { transaction: tx });
        await tx.execute(sql`SELECT pg_terminate_backend(pg_backend_pid())`);
      });
      await expect(placed).rejects.toThrow();
      await client.end().catch(() => undefined);

      expect(await orderIds()).toEqual([]);
      expect(await instanceIds()).toEqual([]);
    });

    it('without the transaction, a crash between the commit and publish() leaves an order that nothing fulfils', async () => {
      const node = await start();

      // Why the dispatcher context matters: this handler publishes after its commit, and dies in between.
      await database.transaction(async (tx) => {
        await tx.insert(orders).values({ id: 'o-1', userId: 'u_42', items: [], total: 2499, status: 'placed' });
      });
      // (the process is gone here: publish() never runs)
      expect(await orderIds()).toEqual(['o-1']);
      expect(await instanceIds()).toEqual([]);

      // Recovery is the application's: publish again (start is idempotent), from a reconciliation job.
      await node.eventBus.publish(new OrderPlacedEvent('o-1', 2499));
      expect(await instanceIds()).toEqual([fulfilmentId('o-1')]);
    });
  });
});
