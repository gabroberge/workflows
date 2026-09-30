/**
 * `start()` and `signal()` inside the application's transaction, on the workflows tutorial's
 * application (its controllers, services and workflow, and `PostgresWorkflowStore` on its Drizzle
 * database) served on Express and Fastify, over PGlite and PostgreSQL; and on MySQL, the same
 * application on MySQL (fixtures/mysql: `MySqlWorkflowStore` registered by a factory provider on
 * its Drizzle mysql2 database, as the docs show it). The in-memory store can't roll anything back,
 * so these run on the SQL stores only (storage.spec.ts covers its warning).
 */
import { BadRequestException, Body, Controller, Module, Post } from '@nestjs/common';
import { APP_FILTER } from '@nestjs/core';
import { getDrizzleToken, InjectDrizzle } from '@nestjs/drizzle';
import { eq, inArray, sql } from 'drizzle-orm';
import { createApp, adapters, type AdapterName } from './support/adapters.js';
import { AppModule } from './fixtures/app.module.js';
import type { Database } from './fixtures/database/drizzle.js';
import { MySqlAppModule } from './fixtures/mysql/app.module.js';
import { fulfilmentId, OrderFulfilmentWorkflow } from './fixtures/orders/order-fulfilment.workflow.js';
import type { Order } from './fixtures/orders/order.js';
import { PaymentProviderClient } from './fixtures/payments/payment-provider.client.js';
import {
  ManualWorkflowClock,
  WORKFLOWS_MODULE_OPTIONS,
  WorkflowClient,
  WorkflowWorker,
  type WorkflowWorkerOptions,
} from '../lib/index.js';
import { WorkflowErrorFilter } from './http-app.js';
import { connect, openStore, orders, storeKind, tempDb, type Connection, type TestDb, waitFor } from './support.js';

const kibble = { productId: 'salmon-kibble-2kg', quantity: 1, price: 2499 };

/** A bulk import: every order and its fulfilment commit together, or none of them do. */
@Controller('imports')
class OrderImportsController {
  constructor(
    @InjectDrizzle() private readonly db: Database,
    private readonly workflowClient: WorkflowClient,
  ) {}

  @Post()
  import(@Body() body: { orders: Array<{ id: string; userId: string; quantity: number }> }) {
    return this.db.transaction(async (tx) => {
      const started = [];
      for (const line of body.orders) {
        if (line.quantity < 1) {
          throw new BadRequestException(`Order ${line.id} has no items.`);
        }

        const items = [{ ...kibble, quantity: line.quantity }];
        const order: Order = { id: line.id, userId: line.userId, items, total: kibble.price * line.quantity, status: 'placed' };
        const insert = tx.insert(orders).values(order);
        // Insert-or-ignore: MySQL's is an update to the key the row already has.
        await (storeKind === 'mysql' ? (insert as unknown as MySqlInsert).onDuplicateKeyUpdate({ set: { id: sql`id` } }) : insert.onConflictDoNothing());
        started.push(await this.workflowClient.start(OrderFulfilmentWorkflow, order, { id: fulfilmentId(order.id), transaction: tx }));
      }
      return started;
    });
  }
}

/** Drizzle's MySQL insert, which the spec's PostgreSQL types don't know. */
interface MySqlInsert {
  onDuplicateKeyUpdate(config: { set: Record<string, unknown> }): Promise<unknown>;
}

@Module({
  imports: [storeKind === 'mysql' ? MySqlAppModule : AppModule],
  controllers: [OrderImportsController],
  providers: [{ provide: APP_FILTER, useClass: WorkflowErrorFilter }],
})
class TutorialWithImports {}

describe.runIf(storeKind !== 'memory').each(adapters)('the tutorial app in its own transactions ($name)', ({ name: adapter }) => {
  let db: TestDb;
  let connection: Connection;
  let database: Database;
  let clock: ManualWorkflowClock;
  const closers: Array<() => Promise<void>> = [];

  beforeEach(async () => {
    db = await tempDb();
    connection = connect(db);
    database = connection.db as Database;
    await database.execute(sql`DELETE FROM orders`);
    clock = new ManualWorkflowClock();
  });

  afterEach(async () => {
    for (const close of closers.splice(0)) {
      await close();
    }
    await connection.close();
    db.cleanup();
  });

  const boot = async (worker: WorkflowWorkerOptions = {}) => {
    const podConnection = connect(db);
    const app = await createApp(adapter as AdapterName, TutorialWithImports, {
      override: (builder) =>
        builder
          .overrideProvider(getDrizzleToken())
          .useValue(podConnection.db)
          .overrideProvider(WORKFLOWS_MODULE_OPTIONS)
          .useValue({ clock, worker: { enabled: false, shutdownTimeout: 50, ...worker } }),
      setup: (app) => app.useLogger(false),
    });
    closers.push(async () => {
      await app.close();
      await podConnection.close();
    });

    const url = await app.getUrl();
    const http = async (method: 'GET' | 'POST', path: string, body?: unknown) => {
      const response = await fetch(`${url}${path}`, {
        method,
        headers: body === undefined ? undefined : { 'content-type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      return { status: response.status, body: (await response.json()) as any };
    };
    return { app, http, client: app.get(WorkflowClient), worker: app.get(WorkflowWorker), payments: app.get(PaymentProviderClient) };
  };

  const orderIds = async () => (await database.select({ id: orders.id }).from(orders)).map((row) => row.id).sort();
  const instanceIds = async () => {
    const { store, close } = openStore(db);
    try {
      return (await store.list({ limit: 100, offset: 0 })).map((instance) => instance.id).sort();
    } finally {
      await close();
    }
  };

  it('saves an order and starts its fulfilment in one transaction, and the worker runs it after the commit', async () => {
    const pod = await boot();

    const placed = await pod.http('POST', '/orders', { userId: 'ada', items: [kibble] });
    expect(placed.status).toBe(201);
    const id: string = placed.body.id;
    expect(placed.body).toMatchObject({
      status: 'placed',
      total: 2499,
      fulfilment: { id: fulfilmentId(id), workflow: 'order-fulfilment', version: 1, created: true, status: 'pending' },
    });
    expect(await orderIds()).toEqual([id]);

    await pod.worker.drain();
    expect(await pod.http('GET', `/orders/${id}/fulfilment`)).toEqual({
      status: 200,
      body: {
        status: 'suspended',
        waitingFor: ['shipment.delivered'],
        wakeAt: new Date(clock.now() + 3 * 86_400_000).toISOString(),
        steps: { 'charge-payment': 'completed', 'reserve-stock': 'completed', 'await-delivery': 'pending' },
        error: null,
      },
    });
    expect(pod.payments.charges).toMatchObject([{ reference: id, amount: 2499 }]);
  });

  it('rolls the orders and their fulfilments back together when the import fails halfway', async () => {
    const pod = await boot();

    const failed = await pod.http('POST', '/imports', {
      orders: [
        { id: 'imp-1', userId: 'ada', quantity: 1 },
        { id: 'imp-2', userId: 'bob', quantity: 0 },
      ],
    });
    expect(failed).toMatchObject({ status: 400, body: { message: 'Order imp-2 has no items.' } });
    expect(await orderIds()).toEqual([]);
    expect(await instanceIds()).toEqual([]);
    expect(await pod.worker.drain()).toBe(0);

    const imported = await pod.http('POST', '/imports', {
      orders: [
        { id: 'imp-1', userId: 'ada', quantity: 1 },
        { id: 'imp-2', userId: 'bob', quantity: 2 },
      ],
    });
    expect(imported.status).toBe(201);
    expect(await orderIds()).toEqual(['imp-1', 'imp-2']);
    expect(await instanceIds()).toEqual(['order-imp-1', 'order-imp-2']);
    expect(await pod.worker.drain()).toBe(2);
  });

  it('keeps the transaction alive for an id it already started in it, and rolls back on a conflicting input', async () => {
    const pod = await boot();

    const repeated = await pod.http('POST', '/imports', {
      orders: [
        { id: 'imp-1', userId: 'ada', quantity: 1 },
        { id: 'imp-1', userId: 'ada', quantity: 1 },
      ],
    });
    expect(repeated.status).toBe(201);
    expect(repeated.body.map((result: { created: boolean }) => result.created)).toEqual([true, false]);
    expect(await instanceIds()).toEqual(['order-imp-1']);

    const conflicting = await pod.http('POST', '/imports', {
      orders: [
        { id: 'imp-2', userId: 'bob', quantity: 1 },
        { id: 'imp-1', userId: 'ada', quantity: 3 },
      ],
    });
    expect(conflicting).toEqual({
      status: 409,
      body: {
        statusCode: 409,
        error: 'WorkflowIdConflictError',
        message: 'Instance "order-imp-1" of "order-fulfilment" already exists with a different input.',
      },
    });
    expect(await orderIds()).toEqual(['imp-1']);
    expect(await instanceIds()).toEqual(['order-imp-1']);
  });

  it('records a delivery webhook with the order status in one transaction, and nothing for an unknown order', async () => {
    const pod = await boot();
    const { body: order } = await pod.http('POST', '/orders', { userId: 'ada', items: [kibble] });
    await pod.worker.drain();

    const unknown = await pod.http('POST', '/webhooks/carrier', {
      type: 'shipment.delivered',
      reference: 'nope',
      trackingNumber: 'TRK-0',
      occurredAt: '2026-01-02T00:00:00Z',
    });
    expect(unknown.status).toBe(404);
    const { store, close } = openStore(db);
    try {
      expect(await store.signals({ name: 'shipment.delivered', key: 'nope', afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).toEqual([]);
    } finally {
      await close();
    }

    const delivered = await pod.http('POST', '/webhooks/carrier', {
      type: 'shipment.delivered',
      reference: order.id,
      trackingNumber: 'TRK-1',
      occurredAt: '2026-01-02T00:00:00Z',
    });
    expect(delivered).toEqual({ status: 200, body: { received: true } });
    const [row] = await database.select({ status: orders.status }).from(orders).where(eq(orders.id, order.id));
    expect(row).toEqual({ status: 'delivered' });

    await pod.worker.drain();
    clock.advance('7d');
    await pod.worker.drain();
    expect((await pod.http('GET', `/orders/${order.id}/fulfilment`)).body).toMatchObject({
      status: 'completed',
      steps: { delivered: 'completed', 'send-review-request': 'completed' },
    });
    expect(await pod.client.getStatus(fulfilmentId(order.id))).toMatchObject({
      output: { chargeId: pod.payments.charges[0]!.id, trackingNumber: 'TRK-1' },
    });
  });

  it('cancels through the support route: refunds, releases and marks the order cancelled', async () => {
    const pod = await boot();
    const { body: order } = await pod.http('POST', '/orders', { userId: 'ada', items: [kibble] });
    await pod.worker.drain();

    const cancelled = await pod.http('POST', `/orders/${order.id}/cancel`, { reason: 'Changed my mind.' });
    expect(cancelled).toEqual({ status: 202, body: { accepted: true } });
    expect((await pod.http('POST', `/orders/${order.id}/cancel`, { reason: 'Again.' })).body).toEqual({ accepted: false });

    await pod.worker.drain();
    expect((await pod.http('GET', `/orders/${order.id}/fulfilment`)).body).toMatchObject({ status: 'cancelled', error: 'Changed my mind.' });
    expect(pod.payments.refunds).toMatchObject([{ chargeId: pod.payments.charges[0]!.id }]);
    const rows = await database.select({ status: orders.status }).from(orders).where(inArray(orders.id, [order.id]));
    expect(rows).toEqual([{ status: 'cancelled' }]);
  });

  it('is picked up by a polling worker once the transaction commits', async () => {
    const pod = await boot({ enabled: true, pollInterval: '20ms' });

    const { body: order } = await pod.http('POST', '/orders', { userId: 'ada', items: [kibble] });
    await waitFor(async () => (await pod.client.getStatus(fulfilmentId(order.id)))?.status === 'suspended');
    expect(pod.payments.charges).toHaveLength(1);
  });
});
