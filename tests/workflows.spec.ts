import { AsyncLocalStorage } from 'node:async_hooks';
import { Inject } from '@nestjs/common';
import {
  ManualWorkflowClock,
  NonRetryableStepError,
  Workflow,
  WorkflowClient,
  WorkflowIdConflictError,
  type WorkflowContext,
} from '../lib/index.js';
import { OrderFulfilment, orderProviders, type Order } from './order-fulfilment.js';
import { boot, tempDb, type TestDb, waitFor, World, type Node } from './support.js';

const DAY = 86_400_000;
const order: Order = { orderId: 'o1', amount: 4200, email: 'ada@example.com' };

describe('order fulfilment saga', () => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let world: World;
  let node: Node;

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    world = new World();
    node = await boot({ db, clock, workflows: [OrderFulfilment], providers: orderProviders(world) });
  });

  afterEach(async () => {
    await node.close();
    db.cleanup();
  });

  it('runs top to bottom with injected services, a durable wait and a durable sleep', async () => {
    const handle = await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    expect(handle).toMatchObject({ id: 'order-o1', workflow: 'order-fulfilment', version: 1, created: true });

    await node.worker.drain();
    expect(world.ops()).toEqual(['reserve', 'charge']);
    expect(await node.client.getStatus('order-o1')).toMatchObject({
      status: 'suspended',
      wakeAt: clock.now() + 3 * DAY, // the wait's timeout
      waits: [{ signal: 'shipment.delivered', key: 'o1' }],
    });

    // Another order's delivery does not wake this instance.
    expect(await node.client.signal('shipment.delivered', { orderId: 'o2', trackingId: 'T2' }, { key: 'o2' })).toMatchObject({ woken: 0 });

    clock.advance('1d');
    expect(await node.client.signal('shipment.delivered', { orderId: 'o1', trackingId: 'T1' }, { key: 'o1' })).toMatchObject({ woken: 1 });
    await node.worker.drain();
    expect(await node.client.getStatus('order-o1')).toMatchObject({ status: 'suspended', wakeAt: clock.now() + 7 * DAY, waits: [] });

    clock.advance('7d');
    await node.worker.drain();

    const done = await node.client.getStatus('order-o1', { journal: true });
    expect(done).toMatchObject({ status: 'completed', output: { chargeId: 'ch_o1', trackingId: 'T1' }, runs: 3 });
    expect(done!.journal!.map((e) => `${e.kind}:${e.name}:${e.status}`)).toEqual([
      'step:reserve-stock:completed',
      'step:charge:completed',
      'signal:await-delivery:completed',
      'sleep:before-review-request:completed',
      'step:review-request:completed',
    ]);

    // Each effect happened once, with a stable idempotency key.
    expect(world.calls.map((c) => c.key)).toEqual([
      'order-o1:reserve-stock',
      'order-o1:charge',
      'order-o1:review-request',
    ]);
    expect(node.events.map((e) => e.type)).toEqual([
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
    expect(await node.client.list({ status: 'completed' })).toHaveLength(1);
  });

  it('times out the wait and compensates in reverse order (refund, then release)', async () => {
    await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    await node.worker.drain();

    clock.advance('3d');
    await node.worker.drain();

    const status = await node.client.getStatus('order-o1', { journal: true });
    expect(status).toMatchObject({
      status: 'failed',
      error: { name: 'WorkflowFailedError', message: 'Order o1 was not delivered within 3 days.' },
    });
    expect(world.ops()).toEqual(['reserve', 'charge', 'refund', 'release']);
    expect(world.calls.slice(2).map((c) => c.key)).toEqual([
      'order-o1:$compensate:charge',
      'order-o1:$compensate:reserve-stock',
    ]);
    expect(status!.journal!.filter((e) => e.kind === 'compensation').map((e) => e.name)).toEqual([
      '$compensate:charge',
      '$compensate:reserve-stock',
    ]);
    expect(node.events.slice(-5).map((e) => e.type)).toEqual([
      'signal-timed-out',
      'workflow-compensating',
      'step-compensated',
      'step-compensated',
      'workflow-failed',
    ]);
  });

  it('matches a delivery that arrived before the workflow reached the wait', async () => {
    await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    // The webhook beats the worker: nobody is waiting yet.
    expect(await node.client.signal('shipment.delivered', { orderId: 'o1', trackingId: 'T1' }, { key: 'o1' })).toMatchObject({ woken: 0 });

    await node.worker.drain();
    expect(await node.client.getStatus('order-o1')).toMatchObject({ status: 'suspended', wakeAt: clock.now() + 7 * DAY });
  });

  it('ignores deliveries recorded after the timeout even if the worker was late', async () => {
    await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    await node.worker.drain();

    clock.advance('4d'); // workers were down past the 3-day deadline
    await node.client.signal('shipment.delivered', { orderId: 'o1', trackingId: 'T1' }, { key: 'o1' });
    await node.worker.drain();

    expect(await node.client.getStatus('order-o1')).toMatchObject({ status: 'failed' });
    expect(world.ops()).toEqual(['reserve', 'charge', 'refund', 'release']);
  });

  it('cancel runs the compensations and ends as cancelled', async () => {
    await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    await node.worker.drain();

    const requested = await node.client.cancel('order-o1', 'Customer changed their mind.');
    expect(requested).toMatchObject({ status: 'suspended', cancelRequested: true, wakeAt: clock.now() });
    await node.worker.drain();

    expect(await node.client.getStatus('order-o1')).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowCancelledError', message: 'Customer changed their mind.' },
    });
    expect(world.ops()).toEqual(['reserve', 'charge', 'refund', 'release']);
    expect(node.events.at(-1)).toMatchObject({ type: 'workflow-cancelled' });

    // Late signals and repeated cancels are no-ops.
    expect(await node.client.signal('shipment.delivered', { orderId: 'o1', trackingId: 'T1' }, { key: 'o1' })).toMatchObject({ woken: 0 });
    expect(await node.client.cancel('order-o1')).toMatchObject({ status: 'cancelled' });
    expect(await node.worker.drain()).toBe(0);
  });

  it('cancel before the first execution compensates nothing', async () => {
    await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    await node.client.cancel('order-o1');
    await node.worker.drain();

    expect(await node.client.getStatus('order-o1')).toMatchObject({ status: 'cancelled' });
    expect(world.ops()).toEqual([]);
  });

  it('cancel does not touch a completed instance', async () => {
    await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    await node.client.signal('shipment.delivered', { orderId: 'o1', trackingId: 'T1' }, { key: 'o1' });
    await node.worker.drain();

    clock.advance('7d');
    await node.worker.drain();

    expect(await node.client.cancel('order-o1')).toMatchObject({ status: 'completed', cancelRequested: false });
  });

  it('starts idempotently by id', async () => {
    const first = await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    const again = await node.client.start(OrderFulfilment, { ...order }, { id: 'order-o1' });
    expect(first.created).toBe(true);
    expect(again).toMatchObject({ id: 'order-o1', created: false });

    // Key order does not matter; values do.
    const reordered = { email: order.email, amount: order.amount, orderId: order.orderId };
    expect((await node.client.start(OrderFulfilment, reordered, { id: 'order-o1' })).created).toBe(false);
    await expect(node.client.start(OrderFulfilment, { ...order, amount: 1 }, { id: 'order-o1' })).rejects.toBeInstanceOf(
      WorkflowIdConflictError,
    );

    await node.worker.drain();
    expect(await node.client.list()).toHaveLength(1);
    expect(world.count('charge')).toBe(1);
  });
});

describe('durable execution basics', () => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let world: World;
  const nodes: Node[] = [];

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

  const start = async (workflows: any[], providers: any[] = []) => {
    const node = await boot({
      db,
      clock,
      workflows,
      providers: [{ provide: World, useValue: world }, ...providers],
    });

    nodes.push(node);
    return node;
  };

  @Workflow('nap')
  class Nap {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      await ctx.step('before', (s) => this.world.record('before', s.idempotencyKey));
      await ctx.sleep('nap', '1d');
      await ctx.step('after', (s) => this.world.record('after', s.idempotencyKey));
      return 'rested';
    }
  }

  it('sleeps across a restart', async () => {
    const first = await start([Nap]);
    await first.client.start(Nap, undefined, { id: 'nap-1' });
    await first.worker.drain();
    expect(await first.client.getStatus('nap-1')).toMatchObject({ status: 'suspended', wakeAt: clock.now() + 86_400_000 });
    await first.close();
    nodes.splice(0);

    const second = await start([Nap]);
    clock.advance(86_400_000 - 1);
    expect(await second.worker.drain()).toBe(0);
    clock.advance(1);
    expect(await second.worker.drain()).toBe(1);

    expect(await second.client.getStatus('nap-1')).toMatchObject({ status: 'completed', output: 'rested' });
    expect(world.ops()).toEqual(['before', 'after']);
  });

  const seen: unknown[] = [];

  @Workflow('helpers')
  class Helpers {
    async run(ctx: WorkflowContext) {
      const values = { now: ctx.now(), random: ctx.random(), uuid: ctx.uuid() };
      seen.push(values); // runs on every execution: shows what a replay sees
      const branch = values.random < 2 ? 'always' : 'never'; // branching on a journaled value is safe
      await ctx.sleep('wait', '1h');
      return { values, branch, later: ctx.now() };
    }
  }

  it('journals now(), random() and uuid() so replays see the same values', async () => {
    const node = await start([Helpers]);
    const t0 = clock.now();
    await node.client.start(Helpers, undefined, { id: 'h-1' });
    await node.worker.drain();
    clock.advance('1h');
    await node.worker.drain();

    const status = await node.client.getStatus('h-1', { journal: true });
    const output = status!.output as any;
    expect(seen).toHaveLength(2);
    expect(seen[1]).toEqual(seen[0]);
    expect(output.values).toEqual(seen[0]);
    expect(output.values.now).toBe(t0);
    expect(output.later).toBe(t0 + 3_600_000);
    expect(status!.journal!.map((e) => e.name)).toEqual(['$now:1', '$random:1', '$uuid:1', 'wait', '$now:2']);
  });

  @Workflow('signal-race')
  class SignalRace {
    constructor(private readonly client: WorkflowClient) {}

    async run(ctx: WorkflowContext, input: { key: string }) {
      // The event is recorded while this execution runs, after it read its
      // event cursor: the classic lost wake-up.
      await ctx.step('trigger', () => this.client.signal('ping', { n: 1 }, { key: input.key }));
      return ctx.waitForSignal<{ n: number }>('pong', 'ping', { key: input.key });
    }
  }

  it('does not lose a signal recorded while the instance was executing', async () => {
    const node = await start([SignalRace]);
    await node.client.start(SignalRace, { key: 'k' }, { id: 'race-1' });
    await node.worker.drain(); // suspends, is kept runnable by suspend(), then resumes
    expect(await node.client.getStatus('race-1')).toMatchObject({ status: 'completed', output: { n: 1 }, runs: 2 });
  });

  @Workflow('approval')
  class Approval {
    async run(ctx: WorkflowContext) {
      const first = await ctx.waitForSignal<{ ok: boolean; by: string }>('first-approval', 'approval', {
        match: (e) => e.ok,
      });
      const second = await ctx.waitForSignal<{ ok: boolean; by: string }>('second-approval', 'approval', {
        match: (e) => e.ok && e.by !== first!.by,
      });

      return [first!.by, second!.by];
    }
  }

  it('filters with match() and never hands one event to two waits', async () => {
    const node = await start([Approval]);
    await node.client.start(Approval, undefined, { id: 'a-1' });
    await node.worker.drain();

    await node.client.signal('approval', { ok: false, by: 'eve' }); // wakes, does not match
    await node.worker.drain();
    expect(await node.client.getStatus('a-1')).toMatchObject({ status: 'suspended', waits: [{ signal: 'approval', key: null }] });

    await node.client.signal('approval', { ok: true, by: 'ada' });
    await node.client.signal('approval', { ok: true, by: 'ada' }); // same approver twice
    await node.worker.drain();
    expect(await node.client.getStatus('a-1')).toMatchObject({ status: 'suspended' });

    await node.client.signal('approval', { ok: true, by: 'bob' });
    await node.worker.drain();
    expect(await node.client.getStatus('a-1')).toMatchObject({ status: 'completed', output: ['ada', 'bob'] });
  });
});

describe('point of no return (ctx.commit)', () => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let world: World;
  const nodes: Node[] = [];
  const switches = { reviewFails: false, labelFlaky: false };

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    world = new World();
    switches.reviewFails = false;
    switches.labelFlaky = false;
  });

  afterEach(async () => {
    for (const node of nodes.splice(0)) {
      await node.close();
    }
    db.cleanup();
  });

  const start = async () => {
    const node = await boot({ db, clock, workflows: [Delivery], providers: [{ provide: World, useValue: world }] });
    nodes.push(node);
    return node;
  };

  /** Charge, wait for delivery, commit, then post-delivery work that may fail. */
  @Workflow('delivery')
  class Delivery {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      await ctx.step('charge', (s) => this.world.record('charge', s.idempotencyKey), {
        compensate: (_, s) => this.world.record('refund', s.idempotencyKey),
      });

      const delivered = await ctx.waitForSignal('await-delivery', 'delivered', { key: ctx.workflowId, timeout: '3d' });
      if (!delivered) {
        ctx.fail('Not delivered.');
      }
      ctx.commit('delivered');

      await ctx.step('print-label', (s) => this.world.record('label', s.idempotencyKey), {
        compensate: (_, s) => {
          this.world.record('void-label', s.idempotencyKey, s.attempt);
          if (switches.labelFlaky && s.attempt === 1) {
            throw new Error('label API down');
          }
        },
      });

      await ctx.sleep('before-review', '7d');
      await ctx.step('review', (s) => {
        this.world.record('review', s.idempotencyKey);
        if (switches.reviewFails) {
          throw new NonRetryableStepError('mail provider rejected the address');
        }
      });

      return 'done';
    }
  }

  const deliver = async (node: Node, id: string) => {
    await node.client.start(Delivery, undefined, { id });
    await node.worker.drain();
    await node.client.signal('delivered', {}, { key: id });
    await node.worker.drain();
  };

  it('a failure after the commit does not run the compensations registered before it', async () => {
    switches.reviewFails = true;
    const node = await start();
    await deliver(node, 'd-1');
    clock.advance('7d');
    await node.worker.drain();

    const status = await node.client.getStatus('d-1', { journal: true });
    expect(status).toMatchObject({ status: 'failed', error: { name: 'StepFailedError' } });
    // The label (after the commit) is voided; the charge (before it) is not refunded.
    expect(world.ops()).toEqual(['charge', 'label', 'review', 'void-label']);
    expect(status!.journal!.map((e) => `${e.kind}:${e.name}:${e.status}`)).toEqual([
      'step:charge:completed',
      'signal:await-delivery:completed',
      'commit:delivered:completed',
      'step:print-label:completed',
      'sleep:before-review:completed',
      'step:review:failed',
      'compensation:$compensate:print-label:completed',
    ]);
  });

  it('a cancel after the commit stops the workflow without refunding', async () => {
    const node = await start();
    await deliver(node, 'd-1');
    expect(await node.client.cancel('d-1', 'Too late.')).toMatchObject({ accepted: true });
    await node.worker.drain();

    expect(await node.client.getStatus('d-1')).toMatchObject({ status: 'cancelled', error: { message: 'Too late.' } });
    expect(world.ops()).toEqual(['charge', 'label', 'void-label']);
  });

  it('a cancel before the commit still compensates everything', async () => {
    const node = await start();
    await node.client.start(Delivery, undefined, { id: 'd-1' });
    await node.worker.drain();

    await node.client.cancel('d-1');
    await node.worker.drain();

    expect(await node.client.getStatus('d-1')).toMatchObject({ status: 'cancelled' });
    expect(world.ops()).toEqual(['charge', 'refund']);
  });

  it('honours the commit when a later execution replays the journal to compensate', async () => {
    switches.reviewFails = true;
    switches.labelFlaky = true;
    const node = await start();
    await deliver(node, 'd-1');
    clock.advance('7d');
    await node.worker.drain();

    // The label compensation failed once and is parked on its backoff.
    expect(await node.client.getStatus('d-1')).toMatchObject({ status: 'compensating' });
    await node.close();
    nodes.splice(0);

    // A new process rebuilds the compensations by replaying: the commit discards the refund again.
    const next = await start();
    clock.advance('1s');
    await next.worker.drain();

    expect(await next.client.getStatus('d-1')).toMatchObject({ status: 'failed' });
    expect(world.ops()).toEqual(['charge', 'label', 'review', 'void-label', 'void-label']);
  });

  it('rejects a commit name that is also used by a step', async () => {
    @Workflow('clash')
    class Clash {
      async run(ctx: WorkflowContext) {
        await ctx.step('x', () => 1);
        ctx.commit('x');
      }
    }

    const node = await boot({ db, clock, workflows: [Clash] });
    nodes.push(node);
    await node.client.start(Clash, undefined, { id: 'c-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('c-1')).toMatchObject({ status: 'failed', error: { name: 'WorkflowDefinitionError' } });
  });
});

describe('cancel bookkeeping and shutdown', () => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let world: World;
  let node: Node;

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    world = new World();
    node = await boot({ db, clock, workflows: [OrderFulfilment], providers: orderProviders(world) });
  });

  afterEach(async () => {
    await node.close();
    db.cleanup();
  });

  it('tells the first cancel apart from a repeated or late one, and keeps the first reason', async () => {
    await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    await node.worker.drain();

    expect(await node.client.cancel('order-o1', 'Customer changed their mind.')).toMatchObject({
      accepted: true,
      cancelRequested: true,
    });
    expect(await node.client.cancel('order-o1', 'Duplicate click.')).toMatchObject({
      accepted: false,
      cancelReason: 'Customer changed their mind.',
    });

    await node.worker.drain();
    expect(await node.client.cancel('order-o1', 'Way too late.')).toMatchObject({ accepted: false, status: 'cancelled' });
    expect(await node.client.getStatus('order-o1')).toMatchObject({ error: { message: 'Customer changed their mind.' } });
  });

  it('marks a wait that never resolved as cancelled when the instance is cancelled', async () => {
    await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    await node.worker.drain();
    await node.client.cancel('order-o1');
    await node.worker.drain();

    const status = await node.client.getStatus('order-o1', { journal: true });
    expect(status!.journal!.find((e) => e.name === 'await-delivery')).toMatchObject({ status: 'cancelled', wakeAt: null });
    expect(status!.journal!.filter((e) => e.status === 'pending')).toEqual([]);
  });

  it('never closes a registered store: its provider owns it', async () => {
    await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    await node.moduleRef.close();

    // The node's connection is still open: the store still reads and writes.
    expect(await node.store.get('order-o1')).toMatchObject({ status: 'pending' });
    expect(await node.store.signal({ name: 'x', key: null, payload: 1, now: 0 })).toMatchObject({ id: 1 });
  });
});

describe("the worker loop and the caller's async context", () => {
  // What a request keeps in AsyncLocalStorage: its user, its locale.
  const request = new AsyncLocalStorage<string>();
  const seen: Array<string | undefined> = [];

  @Workflow('context-probe')
  class ContextProbe {
    async run(ctx: WorkflowContext, input: { key: string }) {
      await ctx.step('before', () => void seen.push(request.getStore()));
      await ctx.waitForSignal('approved', 'approval', { key: input.key });
      await ctx.step('after', () => void seen.push(request.getStore()));
    }
  }

  it('runs steps outside the request that started or signalled the instance, and stays outside', async () => {
    const db = await tempDb();
    // A poll a minute away: only the wake-ups from start() and signal() run the steps.
    const node = await boot({ db, workflows: [ContextProbe], worker: { enabled: true, pollInterval: '1m' } });
    const status = (id: string) => node.client.getStatus(id).then((s) => s?.status);

    try {
      await request.run('alice', () => node.client.start(ContextProbe, { key: 'a' }, { id: 'probe-a' }));
      await waitFor(async () => (await status('probe-a')) === 'suspended');
      await request.run('bob', () => node.client.signal('approval', {}, { key: 'a' }));
      await waitFor(async () => (await status('probe-a')) === 'completed');

      // Started outside any request, after the loop was woken from inside two.
      await node.client.start(ContextProbe, { key: 'b' }, { id: 'probe-b' });
      await waitFor(async () => (await status('probe-b')) === 'suspended');

      expect(seen).toEqual([undefined, undefined, undefined]);
    } finally {
      await node.close();
      db.cleanup();
    }
  });
});
