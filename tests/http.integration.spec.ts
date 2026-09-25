/**
 * The README's API walkthrough as an application serves it, on Express and on Fastify: the
 * orders routes start, inspect and cancel a fulfilment, the carrier webhook signals it, and
 * the worker (drained, or polling) runs it on the test store.
 */
import { subscribe, unsubscribe } from 'node:diagnostics_channel';
import { adapters } from './support/adapters.js';
import { ManualWorkflowClock, type WorkflowEvent } from '../lib/index.js';
import { CarrierWebhookController, OrdersController, bootHttp, type HttpNode } from './http-app.js';
import { OrderFulfilment, orderProviders } from './order-fulfilment.js';
import { tempDb, type TestDb, waitFor, World } from './support.js';

const body = { amount: 4200, email: 'ada@example.com' };

describe.each(adapters)('order fulfilment over HTTP ($name)', ({ name: adapter }) => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let world: World;
  const nodes: HttpNode[] = [];

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    world = new World();
  });

  afterEach(async () => {
    for (const node of nodes.splice(0)) {
      await node.close();
    }
    db.cleanup();
  });

  const boot = async (worker: Parameters<typeof bootHttp>[1]['worker'] = {}) => {
    const node = await bootHttp(adapter, {
      db,
      clock,
      worker,
      workflows: [OrderFulfilment],
      providers: orderProviders(world),
      controllers: [OrdersController, CarrierWebhookController],
    });
    nodes.push(node);
    return node;
  };

  const types = (events: WorkflowEvent[]) => events.map((event) => event.type);

  it('starts one fulfilment per order however often the route is called, and answers 409 for another body', async () => {
    const node = await boot();

    const first = await node.http('POST', '/orders/o1/fulfil', body);
    const again = await node.http('POST', '/orders/o1/fulfil', body);
    const conflict = await node.http('POST', '/orders/o1/fulfil', { ...body, amount: 1 });

    expect(first).toEqual({
      status: 201,
      body: { id: 'order-o1', workflow: 'order-fulfilment', version: 1, created: true, status: 'pending' },
    });
    expect(again).toEqual({ status: 201, body: { ...first.body, created: false } });
    expect(conflict).toEqual({
      status: 409,
      body: {
        statusCode: 409,
        error: 'WorkflowIdConflictError',
        message: 'Instance "order-o1" of "order-fulfilment" already exists with a different input.',
      },
    });
    expect(await node.client.list()).toHaveLength(1);
  });

  it('runs an order through its routes: parked for the webhook, then the 7-day sleep, then the review request', async () => {
    const node = await boot();
    const completed: unknown[] = [];
    const onCompleted = (event: unknown) => completed.push(event);
    subscribe('nestjs:workflows:workflow-completed', onCompleted);

    try {
      await node.http('POST', '/orders/o1/fulfil', body);
      expect(await node.worker.drain()).toBe(1);
      expect(await node.http('GET', '/orders/o1/fulfilment')).toEqual({
        status: 200,
        body: { status: 'suspended', waits: [{ signal: 'shipment.delivered', key: 'o1' }], error: null },
      });

      const other = await node.http('POST', '/webhooks/carrier', { orderId: 'o2', trackingId: 'TRK-2' });
      const delivered = await node.http('POST', '/webhooks/carrier', { orderId: 'o1', trackingId: 'TRK-1' });
      expect(other).toEqual({ status: 200, body: { signalId: 1, woken: 0 } });
      expect(delivered).toEqual({ status: 200, body: { signalId: 2, woken: 1 } });

      await node.worker.drain();
      expect((await node.http('GET', '/orders/o1/fulfilment')).body).toEqual({ status: 'suspended', waits: [], error: null });
      expect(world.count('mail:review')).toBe(0);

      clock.advance('7d');
      await node.worker.drain();
      expect((await node.http('GET', '/orders/o1/fulfilment')).body).toEqual({ status: 'completed', waits: [], error: null });
    } finally {
      unsubscribe('nestjs:workflows:workflow-completed', onCompleted);
    }

    expect(world.calls).toEqual([
      { op: 'reserve', key: 'order-o1:reserve-stock', attempt: undefined },
      { op: 'charge', key: 'order-o1:charge', attempt: undefined },
      { op: 'mail:review', key: 'order-o1:review-request', attempt: undefined },
    ]);
    expect(await node.client.getStatus('order-o1')).toMatchObject({
      output: { chargeId: 'ch_o1', trackingId: 'TRK-1' },
      runs: 3,
    });
    expect(types(node.events)).toEqual([
      'workflow-started',
      'step-completed',
      'step-completed',
      'workflow-suspended',
      'workflow-resumed',
      'signal-received',
      'workflow-suspended',
      'workflow-resumed',
      'step-completed',
      'workflow-completed',
    ]);
    expect(completed).toEqual([
      expect.objectContaining({ type: 'workflow-completed', id: 'order-o1', workflow: 'order-fulfilment', version: 1, at: clock.now() }),
    ]);
  });

  it('counts a webhook that arrives before the workflow reaches its wait', async () => {
    const node = await boot();

    await node.http('POST', '/orders/o1/fulfil', body);
    expect((await node.http('POST', '/webhooks/carrier', { orderId: 'o1', trackingId: 'TRK-1' })).body).toEqual({
      signalId: 1,
      woken: 0,
    });
    await node.worker.drain();
    clock.advance('7d');
    await node.worker.drain();

    expect((await node.http('GET', '/orders/o1/fulfilment')).body.status).toBe('completed');
    expect(await node.client.getStatus('order-o1')).toMatchObject({ output: { trackingId: 'TRK-1' } });
  });

  it('refunds, then releases the stock, when the webhook does not come within 3 days, and ignores a late one', async () => {
    const node = await boot();

    await node.http('POST', '/orders/o1/fulfil', body);
    await node.worker.drain();
    clock.advance('3d');
    await node.worker.drain();

    expect((await node.http('GET', '/orders/o1/fulfilment')).body).toEqual({
      status: 'failed',
      waits: [],
      error: 'Order o1 was not delivered within 3 days.',
    });
    expect(world.ops()).toEqual(['reserve', 'charge', 'refund', 'release']);
    expect(types(node.events).slice(4)).toEqual([
      'workflow-resumed',
      'signal-timed-out',
      'workflow-compensating',
      'step-compensated',
      'step-compensated',
      'workflow-failed',
    ]);

    const late = await node.http('POST', '/webhooks/carrier', { orderId: 'o1', trackingId: 'TRK-1' });
    expect(late.body).toEqual({ signalId: 1, woken: 0 });
    expect(await node.worker.drain()).toBe(0);
    expect((await node.http('GET', '/orders/o1/fulfilment')).body.status).toBe('failed');
  });

  it('cancels a parked order through DELETE, compensating it once, and answers 404 for an unknown order', async () => {
    const node = await boot();

    await node.http('POST', '/orders/o1/fulfil', body);
    await node.worker.drain();
    const cancel = await node.http('DELETE', '/orders/o1/fulfilment');
    const repeated = await node.http('DELETE', '/orders/o1/fulfilment');
    expect(cancel).toEqual({ status: 200, body: { accepted: true, status: 'suspended' } });
    expect(repeated).toEqual({ status: 200, body: { accepted: false, status: 'suspended' } });

    expect(await node.worker.drain()).toBe(1); // a cancel wakes a parked instance at once
    expect((await node.http('GET', '/orders/o1/fulfilment')).body).toEqual({
      status: 'cancelled',
      waits: [],
      error: 'Cancelled by customer.',
    });
    expect(world.ops()).toEqual(['reserve', 'charge', 'refund', 'release']);
    expect(node.events.at(-1)).toMatchObject({ type: 'workflow-cancelled', error: { message: 'Cancelled by customer.' } });

    expect(await node.http('DELETE', '/orders/o1/fulfilment')).toEqual({ status: 200, body: { accepted: false, status: 'cancelled' } });
    expect(await node.http('GET', '/orders/o9/fulfilment')).toMatchObject({ status: 404 });
    expect(await node.http('DELETE', '/orders/o9/fulfilment')).toEqual({
      status: 404,
      body: { statusCode: 404, error: 'WorkflowNotFoundError', message: 'No workflow instance with id "order-o9".' },
    });
  });

  it('runs what the routes start on the polling worker, without anyone draining it', async () => {
    const node = await boot({ enabled: true, pollInterval: '20ms' });
    const status = async () => (await node.http('GET', '/orders/o1/fulfilment')).body.status;

    await node.http('POST', '/orders/o1/fulfil', body);
    await waitFor(async () => (await status()) === 'suspended');

    await node.http('POST', '/webhooks/carrier', { orderId: 'o1', trackingId: 'TRK-1' });
    await waitFor(async () => (await node.client.getStatus('order-o1'))?.runs === 2 && (await status()) === 'suspended');
    expect(world.count('mail:review')).toBe(0);

    clock.advance('7d');
    await waitFor(async () => (await status()) === 'completed');
    expect(world.ops()).toEqual(['reserve', 'charge', 'mail:review']);
  });
});
