/**
 * Child workflows: `ctx.startChild()`, `ctx.executeChild()` and a child's handle. The start is
 * journaled and idempotent, the child's end reaches the parent in the child's final write, and
 * `parentClose` decides what happens to children still running when the parent ends.
 */
import { Logger } from '@nestjs/common';
import {
  ChildWorkflowFailedError,
  isWorkflowInterrupt,
  ManualWorkflowClock,
  Workflow,
  WorkflowIdConflictError,
  WorkflowSignal,
  type WorkflowContext,
} from '../lib/index.js';
import { boot, tempDb, World, type Node, type TestDb } from './support.js';

const pickedUp = new WorkflowSignal<{ by: string }>('carrier.picked-up');
const go = new WorkflowSignal<string>('go');

@Workflow('shipping')
class Shipping {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { orderId: string; fail?: boolean }) {
    await ctx.step('book', () => this.world.record('book', input.orderId), { compensate: () => this.world.record('cancel-booking', input.orderId) });
    if (input.fail) {
      ctx.fail(`No carrier serves the address of ${input.orderId}.`);
    }

    const pickup = await ctx.waitForSignal('pickup', pickedUp, { key: input.orderId });
    return { label: `LBL-${input.orderId}`, by: pickup!.by };
  }
}

@Workflow('fulfilment')
class Fulfilment {
  async run(ctx: WorkflowContext, input: { orderId: string; fail?: boolean; catch?: boolean }) {
    try {
      const shipment = await ctx.executeChild(Shipping, { orderId: input.orderId, fail: input.fail });
      return { shipped: shipment.label, by: shipment.by };
    } catch (error) {
      if (!input.catch || isWorkflowInterrupt(error) || !(error instanceof ChildWorkflowFailedError)) {
        throw error;
      }
      return { shipped: null, why: `${error.status}: ${error.cause!.message}`, child: error.instanceId };
    }
  }
}

@Workflow('split-shipment')
class SplitShipment {
  async run(ctx: WorkflowContext, input: { orderId: string }) {
    const first = await ctx.startChild(Shipping, { orderId: `${input.orderId}-a` });
    const second = await ctx.startChild('shipping', { orderId: `${input.orderId}-b` });
    const [a, b] = await Promise.all([first.result(), second.result()]);
    return [a.label, (b as { label: string }).label, first.id, second.id];
  }
}

@Workflow('deadline-shipping')
class DeadlineShipping {
  async run(ctx: WorkflowContext, orderId: string) {
    const child = await ctx.startChild(Shipping, { orderId }, { id: `ship-${orderId}`, timeout: '7d' });
    const outcome = await ctx.waitForAny('shipped-or-late', { shipped: child, late: ctx.timer('2d') });
    // The same handle, awaited again: the outcome waitForAny() took.
    return outcome.key === 'shipped' ? { shipped: outcome.value.label, again: (await child.result()).label } : { late: true };
  }
}

@Workflow('close-policies')
class ClosePolicies {
  async run(ctx: WorkflowContext, orderId: string) {
    await ctx.startChild(Shipping, { orderId: `${orderId}-cancel` });
    await ctx.startChild(Shipping, { orderId: `${orderId}-terminate` }, { parentClose: 'terminate' });
    await ctx.startChild(Shipping, { orderId: `${orderId}-abandon` }, { parentClose: 'abandon' });
    const how = await ctx.waitForSignal('go', go, { key: orderId });
    if (how === 'fail') {
      ctx.fail('Out of stock.');
    }
    return 'done';
  }
}

@Workflow('explicit-child')
class ExplicitChild {
  async run(ctx: WorkflowContext, input: { id: string; orderId: string }) {
    return ctx.executeChild(Shipping, { orderId: input.orderId }, { id: input.id });
  }
}

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

async function start(workflows: any[] = [Shipping, Fulfilment, SplitShipment, DeadlineShipping, ClosePolicies, ExplicitChild]) {
  const node = await boot({ db, clock, workflows, providers: [{ provide: World, useValue: world }] });
  nodes.push(node);
  return node;
}

it('runs a child and hands its output to the waiting parent', async () => {
  const node = await start();
  await node.client.start(Fulfilment, { orderId: 'o-1' }, { id: 'order-1' });
  await node.worker.drain();

  const child = 'order-1/shipping#1';
  expect(await node.client.getStatus(child)).toMatchObject({ workflow: 'shipping', status: 'suspended', parentId: 'order-1', parentClose: 'cancel' });
  expect(await node.client.getStatus('order-1', { children: true })).toMatchObject({
    status: 'suspended',
    parentId: null,
    waits: [{ signal: '$child-ended', key: child }],
    children: [{ id: child, status: 'suspended' }],
  });
  expect((await node.client.list({ parentId: 'order-1' })).map((i) => i.id)).toEqual([child]);

  await node.client.signal(pickedUp, { by: 'DHL' }, { key: 'o-1' });
  await node.worker.drain();
  expect(await node.client.getStatus('order-1', { journal: true })).toMatchObject({
    status: 'completed',
    output: { shipped: 'LBL-o-1', by: 'DHL' },
    journal: [
      { name: `$child:${child}`, kind: 'child', status: 'completed', result: { id: child, workflow: 'shipping', version: 1 } },
      { name: `$result:${child}`, kind: 'signal', status: 'completed', result: { payload: { status: 'completed', output: { label: 'LBL-o-1', by: 'DHL' } } } },
    ],
  });
  expect(node.events.filter((event) => event.type === 'child-started')).toMatchObject([{ id: 'order-1', child, childWorkflow: 'shipping', childVersion: 1 }]);
  expect(node.events.filter((event) => event.type === 'signal-received')).toMatchObject([{ id: child, signal: 'carrier.picked-up' }]);
});

it("throws the child's failure into the parent as a ChildWorkflowFailedError", async () => {
  const node = await start();
  await node.client.start(Fulfilment, { orderId: 'o-2', fail: true, catch: true }, { id: 'order-2' });
  await node.client.start(Fulfilment, { orderId: 'o-3', fail: true }, { id: 'order-3' });
  await node.worker.drain();

  expect(await node.client.getStatus('order-2')).toMatchObject({
    status: 'completed',
    output: { shipped: null, why: 'failed: No carrier serves the address of o-2.', child: 'order-2/shipping#1' },
  });
  expect(await node.client.getStatus('order-3')).toMatchObject({
    status: 'failed',
    error: { name: 'ChildWorkflowFailedError', message: 'Child workflow "shipping" ("order-3/shipping#1") failed: WorkflowFailedError: No carrier serves the address of o-3.' },
  });
  expect(world.calls.filter((call) => call.op === 'cancel-booking').map((call) => call.key).sort()).toEqual(['o-2', 'o-3']);
});

it('reports a cancelled child as a ChildWorkflowFailedError, and one deleted by force as cancelled too', async () => {
  const node = await start();
  await node.client.start(Fulfilment, { orderId: 'o-4', catch: true }, { id: 'order-4' });
  await node.client.start(Fulfilment, { orderId: 'o-5', catch: true }, { id: 'order-5' });
  await node.worker.drain();

  await node.client.cancel('order-4/shipping#1', 'The carrier lost the parcel.');
  await node.client.delete('order-5/shipping#1', { force: true });
  await node.worker.drain();
  expect(await node.client.getStatus('order-4')).toMatchObject({ output: { shipped: null, why: 'cancelled: The carrier lost the parcel.' } });
  expect(await node.client.getStatus('order-5')).toMatchObject({ output: { shipped: null, why: 'cancelled: Child instance "order-5/shipping#1" was deleted before it ended.' } });
});

it('numbers default child ids per workflow, and starts each child once across replays', async () => {
  const node = await start();
  await node.client.start(SplitShipment, { orderId: 'o-6' }, { id: 'order-6' });
  await node.worker.drain();
  await node.client.signal(pickedUp, { by: 'UPS' }, { key: 'o-6-a' });
  await node.worker.drain();
  await node.client.signal(pickedUp, { by: 'UPS' }, { key: 'o-6-b' });
  await node.worker.drain();

  expect(await node.client.getStatus('order-6')).toMatchObject({
    status: 'completed',
    output: ['LBL-o-6-a', 'LBL-o-6-b', 'order-6/shipping#1', 'order-6/shipping#2'],
  });
  expect(await node.client.list({ workflow: 'shipping' })).toHaveLength(2);
  expect(node.events.filter((event) => event.type === 'child-started')).toHaveLength(2);
});

it('adopts the child an interrupted execution created, instead of starting another', async () => {
  const logged = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
  const node = await start();
  const store = node.store;
  const write = store.write.bind(store);
  let broken = true;
  store.write = async (id, token, w) => {
    if (broken && w.entries.some((entry) => entry.kind === 'child' && entry.status === 'completed')) {
      broken = false;
      throw new Error('The connection was reset.');
    }
    return write(id, token, w);
  };

  await node.client.start(Fulfilment, { orderId: 'o-7' }, { id: 'order-7' });
  await node.worker.drain();
  expect(await node.client.getStatus('order-7', { journal: true })).toMatchObject({ status: 'running', journal: [{ name: '$child:order-7/shipping#1', status: 'pending' }] });
  expect(logged).toHaveBeenCalledWith(expect.stringContaining('Store failed while executing "order-7"'), expect.objectContaining({ message: 'The connection was reset.' }));
  logged.mockRestore();

  clock.advance('31s'); // the lease expires
  await node.worker.drain();
  expect(await node.client.getStatus('order-7', { journal: true })).toMatchObject({ status: 'suspended', journal: [{ status: 'completed' }, { status: 'pending' }] });
  expect(await node.client.list({ parentId: 'order-7' })).toHaveLength(1);
});

it("races a child's end against a timer, and keeps the outcome for the handle's result()", async () => {
  const node = await start();
  await node.client.start(DeadlineShipping, 'o-8', { id: 'order-8' });
  await node.client.start(DeadlineShipping, 'o-9', { id: 'order-9' });
  await node.worker.drain();
  expect(await node.client.getStatus('ship-o-8')).toMatchObject({ deadline: clock.now() + 7 * 86_400_000 });

  await node.client.signal(pickedUp, { by: 'GLS' }, { key: 'o-8' });
  await node.worker.drain();
  clock.advance('2d');
  await node.worker.drain();

  expect(await node.client.getStatus('order-8')).toMatchObject({ status: 'completed', output: { shipped: 'LBL-o-8', again: 'LBL-o-8' } });
  expect(await node.client.getStatus('order-9')).toMatchObject({ status: 'completed', output: { late: true } });
  // Its parent completed: the late child is cancelled, and its compensation runs.
  expect(await node.client.getStatus('ship-o-9')).toMatchObject({
    status: 'cancelled',
    error: { name: 'WorkflowCancelledError', message: 'Cancelled: its parent "order-9" ended as completed.' },
  });
  expect(world.ops()).toEqual(['book', 'book', 'cancel-booking']);
});

describe('parentClose', () => {
  it("cancels, terminates or abandons the children still running when the parent ends", async () => {
    const node = await start();
    await node.client.start(ClosePolicies, 'o-10', { id: 'order-10' });
    await node.worker.drain();
    await node.client.signal(go, 'complete', { key: 'o-10' });
    await node.worker.drain();

    expect(await node.client.getStatus('order-10')).toMatchObject({ status: 'completed' });
    expect(await node.client.getStatus('order-10/shipping#1')).toMatchObject({ status: 'cancelled', error: { name: 'WorkflowCancelledError' } });
    expect(await node.client.getStatus('order-10/shipping#2')).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowTerminatedError', message: 'Terminated: its parent "order-10" ended as completed.' },
    });
    expect(await node.client.getStatus('order-10/shipping#3')).toMatchObject({ status: 'suspended', cancelRequested: false });
    expect(world.ops()).toEqual(['book', 'book', 'book', 'cancel-booking']);
  });

  it('closes them when the parent starts compensating, and when it is cancelled', async () => {
    const node = await start();
    await node.client.start(ClosePolicies, 'o-11', { id: 'order-11' });
    await node.client.start(ClosePolicies, 'o-12', { id: 'order-12' });
    await node.worker.drain();

    await node.client.signal(go, 'fail', { key: 'o-11' });
    await node.client.cancel('order-12', 'The customer cancelled the order.');
    await node.worker.drain();

    expect(await node.client.getStatus('order-11')).toMatchObject({ status: 'failed' });
    expect(await node.client.getStatus('order-11/shipping#1')).toMatchObject({
      status: 'cancelled',
      error: { message: 'Cancelled: its parent "order-11" is compensating (WorkflowFailedError: Out of stock.).' },
    });
    expect(await node.client.getStatus('order-12')).toMatchObject({ status: 'cancelled' });
    expect(await node.client.getStatus('order-12/shipping#2')).toMatchObject({ status: 'cancelled', error: { name: 'WorkflowTerminatedError' } });
    expect(await node.client.getStatus('order-12/shipping#3')).toMatchObject({ status: 'suspended' });
  });
});

it('fails the parent with a WorkflowIdConflictError for an id another instance holds, the same on every replay', async () => {
  const node = await start();
  await node.client.start(Shipping, { orderId: 'o-13' }, { id: 'taken' });
  await node.client.start(ExplicitChild, { id: 'taken', orderId: 'o-13' }, { id: 'order-13' });
  await node.worker.drain();

  expect(await node.client.getStatus('order-13', { journal: true })).toMatchObject({
    status: 'failed',
    error: { name: 'WorkflowIdConflictError', message: 'Instance "taken" of "shipping" already exists with no parent.' },
    journal: [{ name: '$child:taken', status: 'failed', error: { name: 'WorkflowIdConflictError' } }],
  });
  expect(WorkflowIdConflictError).toBeDefined();
});

it('fails an instance whose code now starts another workflow, or another input, under a journaled child id', async () => {
  const before = await start();
  await before.client.start(Fulfilment, { orderId: 'o-14' }, { id: 'order-14' });
  await before.worker.drain();
  await before.close();
  nodes.splice(0);

  @Workflow('fulfilment')
  class FulfilmentWithOtherInput {
    async run(ctx: WorkflowContext, input: { orderId: string }) {
      return ctx.executeChild(Shipping, { orderId: `${input.orderId}-changed` });
    }
  }

  const after = await start([Shipping, FulfilmentWithOtherInput]);
  await after.client.signal(pickedUp, { by: 'DPD' }, { key: 'o-14' });
  await after.worker.drain();
  expect(await after.client.getStatus('order-14')).toMatchObject({
    status: 'failed',
    error: {
      name: 'WorkflowNonDeterminismError',
      message: expect.stringContaining('child "order-14/shipping#1" was started as "shipping" with another input, but the code now starts "shipping".'),
    },
  });
});

it('refuses signal names starting with "$", which are the engine\'s', async () => {
  const node = await start();
  await expect(node.client.signal('$child-ended', {}, { key: 'x' })).rejects.toThrow(
    'Invalid signal name "$child-ended": names starting with "$" are reserved for the engine\'s own signals.',
  );
  expect(() => new WorkflowSignal('$mine')).toThrow('reserved');
});
