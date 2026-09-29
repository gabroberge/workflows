/**
 * Applications that die mid-way, and the new ones that start on the same database: a process dies in a step, in
 * the store call that creates a child or a schedule's occurrence, or after a waitForAny() was decided; its leases
 * expire, and the next process finishes the work once, with the journal's answers.
 */
import { Injectable, type Type } from '@nestjs/common';
import { ManualWorkflowClock, NonRetryableStepError, Workflow, WorkflowSignal, type WorkflowContext, type WorkflowParentClose } from '../lib/index.js';
import { boot, deferred, forever, tempDb, waitFor, World, type Node, type TestDb } from './support.js';

const T0 = Date.UTC(2026, 0, 1);
const iso = (at: number) => new Date(at).toISOString();
const pickedUp = new WorkflowSignal<{ carrier: string }>('parcel.picked-up');
const approved = new WorkflowSignal<{ by: string }>('refund.approved');
const go = new WorkflowSignal<'complete' | 'fail'>('batch.go');

/** Where processes die: the call at each of these points never returns, once, and the test closes the application. */
@Injectable()
class Crash {
  readonly at = new Set<string>();
  readonly reached: string[] = [];

  async point(name: string) {
    if (this.at.delete(name)) {
      this.reached.push(name);
      await forever();
    }
  }
}

@Workflow('packing')
class PackingWorkflow {
  constructor(
    private readonly world: World,
    private readonly crash: Crash,
  ) {}

  async run(ctx: WorkflowContext, input: { orderId: string }) {
    ctx.setStatus({ stage: 'picking' });
    await ctx.step('pick', ({ idempotencyKey, attempt }) => this.world.record('pick', idempotencyKey, attempt));
    ctx.setStatus({ stage: 'packing' });
    await ctx.step('pack', async ({ idempotencyKey, attempt }) => {
      this.world.record('pack', idempotencyKey, attempt);
      await this.crash.point('pack');
    });
    ctx.setStatus({ stage: 'packed' });
    return { parcel: `PCL-${input.orderId}` };
  }
}

@Workflow('order')
class OrderWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { orderId: string; parentClose?: WorkflowParentClose }) {
    const packed = await ctx.executeChild(PackingWorkflow, { orderId: input.orderId }, { parentClose: input.parentClose });
    await ctx.step('notify', ({ idempotencyKey }) => this.world.record('notify', idempotencyKey));
    return packed;
  }
}

@Workflow('shipment')
class ShipmentWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { orderId: string }) {
    await ctx.step('book', () => this.world.record('book', input.orderId), {
      compensate: () => this.world.record('cancel-booking', input.orderId),
    });
    const pickup = await ctx.waitForSignal('pickup', pickedUp, { key: input.orderId });
    return pickup!.carrier;
  }
}

@Workflow('batch')
class BatchWorkflow {
  constructor(private readonly crash: Crash) {}

  async run(ctx: WorkflowContext, batchId: string) {
    for (const parentClose of ['cancel', 'terminate', 'abandon'] as const) {
      await ctx.startChild(ShipmentWorkflow, { orderId: `${batchId}-${parentClose}` }, { id: `${batchId}-${parentClose}`, parentClose });
    }
    const how = await ctx.waitForSignal('go', go, { key: batchId });
    await ctx.step('close-batch', async ({ attempt }) => {
      await this.crash.point(`close-batch:${batchId}`);
      if (how === 'fail' && attempt > 1) {
        throw new NonRetryableStepError(`Batch ${batchId} doesn't add up.`);
      }
    });
    return 'closed';
  }
}

@Workflow('refund-review')
class RefundReviewWorkflow {
  constructor(
    private readonly world: World,
    private readonly crash: Crash,
  ) {}

  async run(ctx: WorkflowContext, input: { orderId: string }) {
    await ctx.sleep('cooling-off', '1h');
    const outcome = await ctx.waitForAny('decision', { approved: ctx.signalWait(approved, { key: input.orderId }), expired: ctx.timer('1d') });
    await ctx.step('act', async ({ attempt }) => {
      this.world.record(`act:${outcome.key}`, input.orderId, attempt);
      await this.crash.point('act');
    });

    // A signal that lost the race stays for a later wait.
    const late = await ctx.waitForSignal('late-approval', approved, { key: input.orderId, timeout: '1h' });
    return { outcome: outcome.key, late: late?.by ?? null };
  }
}

@Workflow('stock-count', { schedules: [{ id: 'stock-count', every: '1h' }] })
class StockCountWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    const at = iso(ctx.schedule!.at);
    await ctx.step('count', () => this.world.record('count', at));
  }
}

@Workflow('exclusive-export', { concurrency: { limit: 1 } })
class ExclusiveExportWorkflow {
  constructor(
    private readonly world: World,
    private readonly crash: Crash,
  ) {}

  async run(ctx: WorkflowContext) {
    const id = ctx.workflowId;
    await ctx.step('export', async ({ attempt }) => {
      this.world.record('export', id, attempt);
      await this.crash.point(`export:${id}`);
    });
  }
}

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
let crash: Crash;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock(T0);
  world = new World();
  crash = new Crash();
});

afterEach(async () => {
  vi.restoreAllMocks();
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

async function start(workflows: Type<unknown>[], id?: string) {
  const node = await boot({
    db,
    clock,
    workflows,
    worker: id ? { id } : {},
    providers: [
      { provide: World, useValue: world },
      { provide: Crash, useValue: crash },
    ],
  });
  nodes.push(node);
  return node;
}

/** The process dies: whatever it runs never returns, and the leases it holds stay until they expire. */
async function kill(node: Node) {
  nodes.splice(nodes.indexOf(node), 1);
  await node.close();
}

/** Makes the next `create()` of `id` never return, before or after the store created it: the process dies there. */
function dieInCreate(node: Node, id: string, when: 'before' | 'after') {
  const create = node.store.create.bind(node.store);
  const reached = deferred();
  const spy = vi.spyOn(node.store, 'create').mockImplementation(async (instance) => {
    if (instance.id !== id) {
      return create(instance);
    }

    if (when === 'after') {
      await create(instance);
    }
    reached.resolve();
    return forever();
  });
  return { reached: reached.promise, restore: () => spy.mockRestore() };
}

describe('children', () => {
  it('resume in the next process after theirs died mid-step, and report to their parent', async () => {
    const first = await start([OrderWorkflow, PackingWorkflow], 'first');
    crash.at.add('pack');
    await first.client.start(OrderWorkflow, { orderId: 'o-1' }, { id: 'order-1' });
    void first.worker.drain();
    await waitFor(() => crash.reached.length === 1);
    await kill(first);

    const second = await start([OrderWorkflow, PackingWorkflow], 'second');
    expect(await second.client.getStatus('order-1/packing#1')).toMatchObject({ status: 'running', leaseOwner: 'first', customStatus: { stage: 'packing' } });
    expect(await second.worker.drain()).toBe(0);
    clock.advance('31s');
    expect(await second.worker.drain()).toBe(2);

    expect(await second.client.getStatus('order-1', { journal: true, children: true })).toMatchObject({
      status: 'completed',
      output: { parcel: 'PCL-o-1' },
      journal: [
        { name: '$child:order-1/packing#1', status: 'completed' },
        { name: '$result:order-1/packing#1', status: 'completed' },
        { name: 'notify', status: 'completed' },
      ],
      children: [{ id: 'order-1/packing#1', status: 'completed', runs: 2, leaseOwner: 'second', customStatus: { stage: 'packed' } }],
    });
    expect(world.calls).toEqual([
      { op: 'pick', key: 'order-1/packing#1:pick', attempt: 1 },
      { op: 'pack', key: 'order-1/packing#1:pack', attempt: 1 },
      { op: 'pack', key: 'order-1/packing#1:pack', attempt: 2 },
      { op: 'notify', key: 'order-1:notify', attempt: undefined },
    ]);
  });

  it('of a parent that died after creating one are adopted, not started twice', async () => {
    const first = await start([OrderWorkflow, PackingWorkflow]);
    const dying = dieInCreate(first, 'order-2/packing#1', 'after');
    await first.client.start(OrderWorkflow, { orderId: 'o-2' }, { id: 'order-2' });
    void first.worker.drain();
    await dying.reached;
    await kill(first);
    dying.restore();

    // The start was journaled before the child existed; the child exists, the journal doesn't say so yet.
    const second = await start([OrderWorkflow, PackingWorkflow]);
    expect(await second.client.getStatus('order-2', { journal: true })).toMatchObject({ journal: [{ name: '$child:order-2/packing#1', status: 'pending' }] });
    expect(await second.client.getStatus('order-2/packing#1')).toMatchObject({ status: 'pending', parentId: 'order-2' });
    clock.advance('31s');
    await second.worker.drain();

    expect(await second.client.getStatus('order-2')).toMatchObject({ status: 'completed', output: { parcel: 'PCL-o-2' } });
    expect((await second.client.list({ parentId: 'order-2' })).map((child) => [child.id, child.status, child.runs])).toEqual([
      ['order-2/packing#1', 'completed', 1],
    ]);
    expect(second.events.filter((event) => event.type === 'child-started')).toMatchObject([{ id: 'order-2', child: 'order-2/packing#1' }]);
    expect(world.ops()).toEqual(['pick', 'pack', 'notify']);
  });

  it('of a parent that died after creating one are closed when the parent is cancelled before it runs again', async () => {
    const first = await start([OrderWorkflow, PackingWorkflow]);
    const dying = dieInCreate(first, 'order-3/packing#1', 'after');
    await first.client.start(OrderWorkflow, { orderId: 'o-3' }, { id: 'order-3' });
    void first.worker.drain();
    await dying.reached;
    await kill(first);
    dying.restore();

    // The parent's process takes over first; the child's runs after it, and finds it cancelled.
    const parents = await start([OrderWorkflow]);
    expect(await parents.client.cancel('order-3', 'The customer cancelled.')).toMatchObject({ accepted: true, status: 'running' });
    clock.advance('31s');
    expect(await parents.worker.drain()).toBe(1);
    expect(await parents.client.getStatus('order-3')).toMatchObject({ status: 'cancelled', error: { message: 'The customer cancelled.' } });
    expect(await parents.client.getStatus('order-3/packing#1')).toMatchObject({ status: 'pending', cancelRequested: true });

    const children = await start([PackingWorkflow]);
    expect(await children.worker.drain()).toBe(1);
    expect(await children.client.getStatus('order-3/packing#1')).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowCancelledError', message: 'Cancelled: its parent "order-3" is compensating (WorkflowCancelledError: The customer cancelled.).' },
    });
    expect(world.calls).toEqual([]);
  });
});

describe('parentClose', () => {
  it('applies when the parent ends in the process that took over from the one that died', async () => {
    const first = await start([BatchWorkflow, ShipmentWorkflow]);
    for (const batchId of ['batch-1', 'batch-2']) {
      await first.client.start(BatchWorkflow, batchId, { id: batchId });
    }
    await first.worker.drain();
    await first.client.signal(go, 'complete', { key: 'batch-1' });
    await first.client.signal(go, 'fail', { key: 'batch-2' });
    crash.at.add('close-batch:batch-1');
    crash.at.add('close-batch:batch-2');
    void first.worker.drain();
    await waitFor(() => crash.reached.length === 2);
    await kill(first);

    const second = await start([BatchWorkflow, ShipmentWorkflow]);
    clock.advance('31s');
    await second.worker.drain();

    expect(await second.client.getStatus('batch-1')).toMatchObject({ status: 'completed', output: 'closed' });
    expect(await second.client.getStatus('batch-2')).toMatchObject({ status: 'failed', error: { name: 'StepFailedError' } });
    const children = Object.fromEntries(
      (await second.client.list({ workflow: 'shipment' })).map((child) => [child.id, [child.status, child.error?.message ?? null, child.cancelRequested]]),
    );
    expect(children).toEqual({
      'batch-1-cancel': ['cancelled', 'Cancelled: its parent "batch-1" ended as completed.', true],
      'batch-1-terminate': ['cancelled', 'Terminated: its parent "batch-1" ended as completed.', true],
      'batch-1-abandon': ['suspended', null, false],
      'batch-2-cancel': [
        'cancelled',
        'Cancelled: its parent "batch-2" is compensating (StepFailedError: Step "close-batch" failed after 2 attempt(s): NonRetryableStepError: Batch batch-2 doesn\'t add up.).',
        true,
      ],
      'batch-2-terminate': ['cancelled', expect.stringContaining('Terminated: its parent "batch-2" is compensating'), true],
      'batch-2-abandon': ['suspended', null, false],
    });
    // The cancelled children undid their bookings; the terminated and the abandoned ones didn't.
    expect(world.calls.filter((call) => call.op === 'cancel-booking').map((call) => call.key).sort()).toEqual(['batch-1-cancel', 'batch-2-cancel']);

    await second.client.signal(pickedUp, { carrier: 'GLS' }, { key: 'batch-1-abandon' });
    await second.worker.drain();
    expect(await second.client.getStatus('batch-1-abandon')).toMatchObject({ status: 'completed', output: 'GLS' });
  });
});

describe('waitForAny()', () => {
  it('replays the winner it journaled in the process that took over, whatever arrived meanwhile', async () => {
    const first = await start([RefundReviewWorkflow]);
    await first.client.start(RefundReviewWorkflow, { orderId: 'o-4' }, { id: 'review-4' });
    await first.worker.drain();
    clock.advance('1h');
    await first.worker.drain();
    const deadline = T0 + 3_600_000 + 86_400_000;
    // The timer counts from when the wait was reached, an hour after the start, and is journaled with it.
    expect(await first.client.getStatus('review-4', { journal: true })).toMatchObject({
      status: 'suspended',
      wakeAt: deadline,
      journal: [{ name: 'cooling-off' }, { name: 'decision', status: 'pending', data: { timers: { expired: deadline } } }],
    });
    await kill(first);

    const second = await start([RefundReviewWorkflow]);
    clock.set(T0 + 86_400_000);
    expect(await second.worker.drain()).toBe(0);
    clock.set(deadline);
    crash.at.add('act');
    const deciding = second.worker.drain();
    await Promise.race([waitFor(() => crash.reached.length === 1), deciding]);
    expect(world.calls).toEqual([{ op: 'act:expired', key: 'o-4', attempt: 1 }]);
    await kill(second);

    // Sent at the deadline: had the wait not been decided, this approval would have won it.
    await (await start([])).client.signal(approved, { by: 'ops' }, { key: 'o-4' });
    const third = await start([RefundReviewWorkflow]);
    clock.advance('31s');
    await third.worker.drain();

    expect(await third.client.getStatus('review-4', { journal: true })).toMatchObject({
      status: 'completed',
      output: { outcome: 'expired', late: 'ops' },
      journal: [
        { name: 'cooling-off' },
        { name: 'decision', status: 'completed', result: { key: 'expired', signalId: null } },
        { name: 'act', status: 'completed', attempts: 2 },
        { name: 'late-approval', status: 'completed' },
      ],
    });
    expect(world.calls).toEqual([
      { op: 'act:expired', key: 'o-4', attempt: 1 },
      { op: 'act:expired', key: 'o-4', attempt: 2 },
    ]);
  });
});

describe('a schedule', () => {
  it.each(['before', 'after'] as const)(
    'neither loses nor repeats an occurrence whose producer died %s creating its instance',
    async (when) => {
      const first = await start([StockCountWorkflow]);
      const occurrence = `stock-count@${iso(T0 + 3_600_000)}`;
      const dying = dieInCreate(first, occurrence, when);
      clock.set(T0 + 3_600_000);
      void first.worker.drain();
      await dying.reached;
      await kill(first);
      dying.restore();

      // The next process starts long after the occurrence, and after the dead producer's lease.
      clock.advance('5m');
      const second = await start([StockCountWorkflow]);
      // The producer recorded the start it decided on before it made it.
      expect((await second.store.getSchedule('stock-count'))!.state).toMatchObject({ runs: 1, pending: [{ at: T0 + 3_600_000 }] });
      expect(await second.store.get(occurrence)).toEqual(when === 'after' ? expect.objectContaining({ status: 'pending' }) : null);
      await second.worker.drain();

      expect(world.calls.map((call) => call.key)).toEqual([iso(T0 + 3_600_000)]);
      expect((await second.client.list({ scheduleId: 'stock-count' })).map((instance) => [instance.id, instance.status, instance.scheduledAt])).toEqual([
        [occurrence, 'completed', T0 + 3_600_000],
      ]);
      expect(await second.client.schedules.get('stock-count')).toMatchObject({ runs: 1, nextAt: T0 + 7_200_000 });
      expect((await second.store.getSchedule('stock-count'))!.state).toMatchObject({ pending: [] });
      expect(second.events.filter((event) => event.type === 'schedule-skipped')).toEqual([]);
    },
  );
});

describe('a concurrency slot', () => {
  it('held by a process that died is freed when its lease expires, and the instance runs again first', async () => {
    const first = await start([ExclusiveExportWorkflow]);
    crash.at.add('export:e-1');
    await first.client.start(ExclusiveExportWorkflow, {}, { id: 'e-1' });
    void first.worker.drain();
    await waitFor(() => crash.reached.length === 1);
    await kill(first);

    const second = await start([ExclusiveExportWorkflow]);
    await second.client.start(ExclusiveExportWorkflow, {}, { id: 'e-2' });
    expect(await second.worker.drain()).toBe(0);
    clock.advance('30s');
    expect(await second.worker.drain()).toBe(0);
    clock.advance('1s');
    expect(await second.worker.drain()).toBe(2);

    expect(world.calls).toEqual([
      { op: 'export', key: 'e-1', attempt: 1 },
      { op: 'export', key: 'e-1', attempt: 2 },
      { op: 'export', key: 'e-2', attempt: 1 },
    ]);
  });
});
