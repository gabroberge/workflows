/**
 * The features together, across processes: priorities with concurrency keys and rate limits, children that
 * inherit a priority and queue behind a busy key, a schedule whose occurrences start children, a child's custom
 * status and waitForAny() read from an API process, terminate() and the three parentClose policies, purge() over
 * parents, children, schedules and rate-limit windows, `@StartOn()` with a concurrency key, and the HTTP routes of
 * an app that reads custom statuses and results.
 */
import { Body, Controller, Get, Injectable, NotFoundException, Param, Post, type Type } from '@nestjs/common';
import { CqrsModule, EventBus } from '@nestjs/cqrs';
import { StartOn, WorkflowsCqrsModule } from '../lib/cqrs/index.js';
import {
  ChildWorkflowFailedError,
  ManualWorkflowClock,
  NonRetryableStepError,
  Workflow,
  WorkflowClient,
  WorkflowResultTimeoutError,
  WorkflowSignal,
  type WorkflowContext,
  type WorkflowWorkerOptions,
} from '../lib/index.js';
import { bootHttp, type HttpNode } from './http-app.js';
import { boot, deferred, tempDb, waitFor, World, type Node, type TestDb } from './support.js';

const T0 = Date.UTC(2026, 0, 1);
const iso = (at: number) => new Date(at).toISOString();
const pickedUp = new WorkflowSignal<{ carrier: string }>('parcel.picked-up');
const supplierConfirmed = new WorkflowSignal<{ reference: string }>('supplier.confirmed');
const courierAccepted = new WorkflowSignal<{ courier: string }>('courier.accepted');
const renewed = new WorkflowSignal<null>('subscription.renewed');
const refundDecision = new WorkflowSignal<{ approved: boolean }>('refund.decision');

/** Holds the calls that pass one of `holding` until `open()`. */
@Injectable()
class Gate {
  readonly holding = new Set<string>();
  readonly reached: string[] = [];
  private readonly opened = deferred();

  async pass(key: string) {
    if (this.holding.has(key)) {
      this.reached.push(key);
      await this.opened.promise;
    }
  }

  open() {
    this.opened.resolve();
  }
}

@Workflow('restock', {
  concurrency: { limit: 1, key: (input: { supplier: string }) => input.supplier },
  rateLimit: { max: 3, duration: '1m' },
})
class RestockWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    const id = ctx.workflowId;
    await ctx.step('order', () => this.world.record('order', id));
  }
}

@Workflow('warehouse-pick', { concurrency: { limit: 1, key: (input: { warehouse: string }) => input.warehouse } })
class WarehousePickWorkflow {
  constructor(
    private readonly world: World,
    private readonly gate: Gate,
  ) {}

  async run(ctx: WorkflowContext, input: { warehouse: string; order: string }) {
    const id = ctx.workflowId;
    await ctx.step('pick', async () => {
      await this.gate.pass(id);
      this.world.record('pick', id);
    });
    return `${input.order}@${input.warehouse}`;
  }
}

@Workflow('order-batch')
class OrderBatchWorkflow {
  async run(ctx: WorkflowContext, input: { order: string; warehouse: string; otherWarehouse?: string }) {
    return ctx.executeChild(WarehousePickWorkflow, { warehouse: input.warehouse, order: input.order }, { concurrencyKey: input.otherWarehouse });
  }
}

@Workflow('supplier-order')
class SupplierOrderWorkflow {
  async run(ctx: WorkflowContext, input: { supplier: string }) {
    const confirmation = await ctx.waitForSignal('confirmed', supplierConfirmed, { key: input.supplier });
    return { supplier: input.supplier, reference: confirmation!.reference, schedule: ctx.schedule };
  }
}

@Workflow('nightly-restock', { schedules: [{ id: 'nightly-restock', cron: '0 2 * * *', priority: 4 }] })
class NightlyRestockWorkflow {
  async run(ctx: WorkflowContext) {
    const orders = [];
    for (const supplier of ['acme', 'globex']) {
      orders.push(await ctx.startChild(SupplierOrderWorkflow, { supplier }, { id: `${ctx.workflowId}/${supplier}` }));
    }
    return Promise.all(orders.map((order) => order.result()));
  }
}

@Workflow('courier-booking')
class CourierBookingWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { parcel: string }) {
    ctx.setStatus({ stage: 'waiting for a courier' });
    const outcome = await ctx.waitForAny('courier', { accepted: ctx.signalWait(courierAccepted, { key: input.parcel }), expired: ctx.timer('2h') });
    const courier = outcome.key === 'accepted' ? outcome.value.courier : null;
    ctx.setStatus({ stage: courier ? `booked with ${courier}` : 'no courier' });
    await ctx.step('confirm', () => this.world.record('confirm', `${input.parcel}:${courier}`));
    return courier;
  }
}

@Workflow('parcel-dispatch')
class ParcelDispatchWorkflow {
  async run(ctx: WorkflowContext, input: { parcels: string[] }) {
    const bookings = [];
    for (const parcel of input.parcels) {
      bookings.push(await ctx.startChild(CourierBookingWorkflow, { parcel }, { id: `${ctx.workflowId}/${parcel}` }));
    }
    return Promise.all(bookings.map((booking) => booking.result()));
  }
}

/** The label printer, shared by every process: broken until the test fixes it. */
@Injectable()
class Printer {
  broken = true;
}

@Workflow('label-request')
class LabelRequestWorkflow {
  constructor(private readonly printer: Printer) {}

  async run(ctx: WorkflowContext, input: { parcel: string }) {
    return ctx.step('print', () => {
      if (this.printer.broken) {
        throw new NonRetryableStepError(`No label for ${input.parcel}: the printer is broken.`);
      }
      return `LBL-${input.parcel}`;
    });
  }
}

@Workflow('shipment-with-deadline')
class ShipmentWithDeadlineWorkflow {
  async run(ctx: WorkflowContext, input: { parcel: string }) {
    const label = await ctx.startChild(LabelRequestWorkflow, { parcel: input.parcel }, { id: `${ctx.workflowId}/label` });
    try {
      const outcome = await ctx.waitForAny('label-or-late', { label, late: ctx.timer('1h') });
      return outcome.key === 'label' ? outcome.value : 'late';
    } catch (error) {
      if (!(error instanceof ChildWorkflowFailedError)) {
        throw error;
      }
      return `no label (${error.status}: ${error.cause!.message})`;
    }
  }
}

@Workflow('box-shipment')
class BoxShipmentWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { box: string }) {
    await ctx.step('book', () => this.world.record('book', input.box), { compensate: () => this.world.record('cancel-booking', input.box) });
    const pickup = await ctx.waitForSignal('pickup', pickedUp, { key: input.box });
    return pickup!.carrier;
  }
}

@Workflow('subscription-box')
class SubscriptionBoxWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { customer: string }) {
    await ctx.step('reserve-box', () => this.world.record('reserve-box', input.customer), {
      compensate: () => this.world.record('release-box', input.customer),
    });
    for (const parentClose of ['cancel', 'terminate', 'abandon'] as const) {
      const box = `${input.customer}-${parentClose}`;
      await ctx.startChild(BoxShipmentWorkflow, { box }, { id: box, parentClose });
    }
    await ctx.waitForSignal('renewal', renewed, { key: input.customer });
  }
}

@Workflow('report-part', { rateLimit: { max: 5, duration: '1m' } })
class ReportPartWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { part: string }) {
    await ctx.step('render', () => this.world.record('render', input.part));
    return `rendered ${input.part}`;
  }
}

@Workflow('daily-report', { schedules: [{ id: 'daily-report', cron: '0 3 * * *' }] })
class DailyReportWorkflow {
  async run(ctx: WorkflowContext) {
    return ctx.executeChild(ReportPartWorkflow, { part: iso(ctx.schedule!.at) });
  }
}

@Workflow('audit')
class AuditWorkflow {
  async run(ctx: WorkflowContext) {
    return ctx.executeChild(ReportPartWorkflow, { part: 'audit' });
  }
}

class RestockRequestedEvent {
  constructor(
    readonly sku: string,
    readonly supplier: string,
  ) {}
}

@Workflow('restock-order', { concurrency: { limit: 1, key: (input: { supplier: string }) => input.supplier } })
@StartOn(RestockRequestedEvent, { id: (event) => `restock-${event.sku}`, input: (event) => ({ sku: event.sku, supplier: event.supplier }) })
class RestockOrderWorkflow {
  constructor(
    private readonly world: World,
    private readonly gate: Gate,
  ) {}

  async run(ctx: WorkflowContext) {
    const id = ctx.workflowId;
    await ctx.step('order', async () => {
      await this.gate.pass(id);
      this.world.record('order', id);
    });
  }
}

class StockLowEvent {
  constructor(
    readonly sku: string,
    readonly supplier: string,
    readonly urgent: boolean,
  ) {}
}

@Workflow('purchase-order', {
  concurrency: { limit: 1, key: (input: { supplier: string }) => input.supplier },
  rateLimit: { max: 2, duration: '1m', key: (input: { supplier: string }) => input.supplier },
})
@StartOn(StockLowEvent, {
  id: (event) => `po-${event.sku}`,
  input: (event) => ({ sku: event.sku, supplier: event.supplier }),
  priority: (event) => (event.urgent ? 1 : 5),
  concurrencyKey: (event) => (event.supplier === 'acme' ? 'acme-warehouse' : undefined),
  rateLimitKey: 'purchasing',
})
class PurchaseOrderWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    const id = ctx.workflowId;
    await ctx.step('order', () => this.world.record('purchase', id));
  }
}

@Workflow('refund-process')
class RefundProcessWorkflow {
  async run(ctx: WorkflowContext, input: { orderId: string; amount: number }) {
    ctx.setStatus({ stage: 'awaiting approval' });
    const decision = await ctx.waitForSignal('decision', refundDecision, { key: input.orderId });
    if (!decision!.approved) {
      ctx.fail(`Refund of ${input.orderId} was declined.`);
    }

    ctx.setStatus({ stage: 'refunding' });
    const refunded = await ctx.step('refund', () => input.amount);
    ctx.setStatus({ stage: 'refunded' });
    return { refunded };
  }
}

/** An app's refund routes: start and wait a little, then report progress and the result on request. */
@Controller('refunds')
class RefundsController {
  constructor(private readonly workflowClient: WorkflowClient) {}

  @Post(':orderId')
  async refund(@Param('orderId') orderId: string, @Body() body: { amount: number }) {
    try {
      return await this.workflowClient.startAndWait(RefundProcessWorkflow, { orderId, amount: body.amount }, { id: `refund-${orderId}` }, { timeout: '200ms' });
    } catch (error) {
      if (error instanceof WorkflowResultTimeoutError) {
        return { status: 'processing' };
      }
      throw error;
    }
  }

  @Get(':orderId/progress')
  async progress(@Param('orderId') orderId: string) {
    const instance = await this.workflowClient.getStatus(`refund-${orderId}`);
    if (!instance) {
      throw new NotFoundException();
    }
    return { status: instance.status, progress: instance.customStatus };
  }

  @Get(':orderId/result')
  result(@Param('orderId') orderId: string) {
    return this.workflowClient.result(`refund-${orderId}`, { timeout: '10s' });
  }
}

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
let gate: Gate;
let printer: Printer;
const nodes: Array<Node | HttpNode> = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock(T0);
  world = new World();
  gate = new Gate();
  printer = new Printer();
});

afterEach(async () => {
  gate.open();
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

const providers = () => [
  { provide: World, useValue: world },
  { provide: Gate, useValue: gate },
  { provide: Printer, useValue: printer },
];

async function start(workflows: Type<unknown>[], options: { worker?: WorkflowWorkerOptions; cqrs?: boolean } = {}) {
  const node = await boot({
    db,
    clock,
    workflows,
    worker: options.worker,
    imports: options.cqrs ? [CqrsModule.forRoot(), WorkflowsCqrsModule] : [],
    providers: providers(),
  });
  nodes.push(node);
  return node;
}

async function stop(node: Node | HttpNode) {
  nodes.splice(nodes.indexOf(node), 1);
  await node.close();
}

const recorded = (op: string) => world.calls.filter((call) => call.op === op).map((call) => call.key);

describe('priorities with concurrency keys and rate limits', () => {
  it('claim by priority within both limits, passing over a busy key, however many processes claim', async () => {
    const pods = [await start([RestockWorkflow]), await start([RestockWorkflow])];
    const instances: Array<[string, string, number | undefined]> = [
      ['r-a', 'acme', undefined],
      ['r-b', 'acme', 1],
      ['r-c', 'globex', 2],
      ['r-d', 'initech', 1],
      ['r-e', 'globex', 3],
      ['r-f', 'globex', undefined],
    ];
    for (const [id, supplier, priority] of instances) {
      await pods[0]!.client.start(RestockWorkflow, { supplier }, { id, priority });
    }
    const drain = async () => {
      const before = world.calls.length;
      await Promise.all(pods.map((pod) => pod.worker.drain()));
      return recorded('order').slice(before);
    };

    // The window takes three: those without a priority first, one per supplier; acme's is taken, so r-b, although
    // before r-d, waits.
    expect((await drain()).sort()).toEqual(['r-a', 'r-d', 'r-f']);
    clock.advance('59s');
    expect(await drain()).toEqual([]);

    // The next window: globex runs one at a time, so r-e, the lowest priority, follows r-c.
    clock.advance('1s');
    const next = await drain();
    expect([...next].sort()).toEqual(['r-b', 'r-c', 'r-e']);
    expect(next.indexOf('r-c')).toBeLessThan(next.indexOf('r-e'));
    expect((await pods[1]!.client.list()).map((instance) => [instance.id, instance.status, instance.runs])).toEqual(
      instances.map(([id]) => [id, 'completed', 1]),
    );
  });
});

describe('children and concurrency keys', () => {
  it('queue a child behind its busy key in priority order, the parent in another process waiting, unless given its own key', async () => {
    const pickers = await start([WarehousePickWorkflow], { worker: { id: 'pickers' } });
    const batches = await start([OrderBatchWorkflow], { worker: { id: 'batches' } });
    gate.holding.add('pick-0');
    await batches.client.start(WarehousePickWorkflow, { warehouse: 'w1', order: 'o-0' }, { id: 'pick-0' });
    const running = pickers.worker.drain();
    await waitFor(() => gate.reached.length === 1);

    await batches.client.start(OrderBatchWorkflow, { order: 'o-1', warehouse: 'w1' }, { id: 'batch-1', priority: 5 });
    await batches.client.start(OrderBatchWorkflow, { order: 'o-2', warehouse: 'w1', otherWarehouse: 'w2' }, { id: 'batch-2' });
    await batches.client.start(WarehousePickWorkflow, { warehouse: 'w1', order: 'o-3' }, { id: 'pick-3', priority: 1 });
    await batches.client.start(WarehousePickWorkflow, { warehouse: 'w1', order: 'o-4' }, { id: 'pick-4' });
    expect(await batches.worker.drain()).toBe(2);
    expect(await batches.client.getStatus('batch-1/warehouse-pick#1')).toMatchObject({
      status: 'pending',
      parentId: 'batch-1',
      priority: 5,
      concurrencyKey: 'w1',
    });

    // w1 is busy: only the child with a key of its own runs.
    expect(await pickers.worker.drain()).toBe(1);
    expect(recorded('pick')).toEqual(['batch-2/warehouse-pick#1']);
    expect(await batches.client.getStatus('batch-2/warehouse-pick#1')).toMatchObject({ status: 'completed', concurrencyKey: 'w2' });

    gate.open();
    await running;
    await batches.worker.drain();
    expect(recorded('pick')).toEqual(['batch-2/warehouse-pick#1', 'pick-0', 'pick-4', 'pick-3', 'batch-1/warehouse-pick#1']);
    expect(await batches.client.getStatus('batch-1')).toMatchObject({ status: 'completed', output: 'o-1@w1' });
    expect(await batches.client.getStatus('batch-2')).toMatchObject({ status: 'completed', output: 'o-2@w1' });
  });
});

describe('a schedule whose occurrences start children', () => {
  it('gives the children its priority but not its occurrence, and counts only the parent for overlap', async () => {
    const node = await start([NightlyRestockWorkflow, SupplierOrderWorkflow]);
    const first = `nightly-restock@${iso(T0 + 2 * 3_600_000)}`;
    clock.set(T0 + 2 * 3_600_000);
    await node.worker.drain();

    expect(await node.client.getStatus(first, { children: true })).toMatchObject({
      status: 'suspended',
      priority: 4,
      scheduleId: 'nightly-restock',
      children: [
        { id: `${first}/acme`, priority: 4, parentId: first, scheduleId: null, status: 'suspended' },
        { id: `${first}/globex`, priority: 4, parentId: first, scheduleId: null, status: 'suspended' },
      ],
    });
    expect((await node.client.list({ scheduleId: 'nightly-restock' })).map((instance) => instance.id)).toEqual([first]);

    // The next night the first is still waiting for its suppliers: skipped (overlap: 'skip').
    clock.advance('1d');
    await node.worker.drain();
    expect(node.events.filter((event) => event.type === 'schedule-skipped')).toMatchObject([{ id: 'nightly-restock', reason: 'overlap' }]);

    await node.client.signal(supplierConfirmed, { reference: 'PO-1' }, { key: 'acme' });
    await node.client.signal(supplierConfirmed, { reference: 'PO-2' }, { key: 'globex' });
    await node.worker.drain();
    expect(await node.client.result(first)).toEqual([
      { supplier: 'acme', reference: 'PO-1', schedule: null },
      { supplier: 'globex', reference: 'PO-2', schedule: null },
    ]);

    clock.advance('1d');
    await node.worker.drain();
    expect((await node.client.list({ scheduleId: 'nightly-restock' })).map((instance) => instance.id)).toEqual([
      first,
      `nightly-restock@${iso(T0 + 2 * 86_400_000 + 2 * 3_600_000)}`,
    ]);
  });
});

describe("a child's custom status and waitForAny()", () => {
  it('are read from an API process while a courier process runs the children, and the parent gets both outcomes', async () => {
    const api = await start([]);
    const dispatchers = await start([ParcelDispatchWorkflow]);
    const couriers = await start([CourierBookingWorkflow]);
    await api.client.start(ParcelDispatchWorkflow, { parcels: ['p-1', 'p-2'] }, { id: 'dispatch-1' });
    await dispatchers.worker.drain();
    await couriers.worker.drain();

    const stages = async () => (await api.client.getStatus('dispatch-1', { children: true }))!.children!.map((child) => child.customStatus);
    expect(await stages()).toEqual([{ stage: 'waiting for a courier' }, { stage: 'waiting for a courier' }]);

    await api.client.signal(courierAccepted, { courier: 'Felix' }, { key: 'p-1' });
    await couriers.worker.drain();
    expect(await stages()).toEqual([{ stage: 'booked with Felix' }, { stage: 'waiting for a courier' }]);

    clock.advance('2h');
    await couriers.worker.drain();
    await dispatchers.worker.drain();
    expect(await stages()).toEqual([{ stage: 'booked with Felix' }, { stage: 'no courier' }]);
    expect(await api.client.result('dispatch-1')).toEqual(['Felix', null]);
    expect(recorded('confirm')).toEqual(['p-1:Felix', 'p-2:null']);
    const emitted = (child: string) =>
      couriers.events.filter((event) => event.type === 'custom-status' && event.id === child).map((event) => (event as { status: { stage: string } }).status.stage);
    expect(emitted('dispatch-1/p-1')).toEqual(['waiting for a courier', 'booked with Felix']);
    expect(emitted('dispatch-1/p-2')).toEqual(['waiting for a courier', 'no courier']);
    expect(api.events).toEqual([]);
  });
});

describe('a child that fails', () => {
  it('wins a waitForAny() as its ChildWorkflowFailedError, and a retry of it that completes notifies its parent no more', async () => {
    const parents = await start([ShipmentWithDeadlineWorkflow]);
    const labels = await start([LabelRequestWorkflow]);
    await parents.client.start(ShipmentWithDeadlineWorkflow, { parcel: 'p-7' }, { id: 'shipment-7' });
    await parents.worker.drain();
    await labels.worker.drain();
    await parents.worker.drain();

    const failed = 'no label (failed: Step "print" failed after 1 attempt(s): NonRetryableStepError: No label for p-7: the printer is broken.)';
    expect(await parents.client.getStatus('shipment-7', { journal: true })).toMatchObject({
      status: 'completed',
      output: failed,
      journal: [{ name: '$child:shipment-7/label' }, { name: 'label-or-late', status: 'completed', result: { key: 'label', payload: { status: 'failed' } } }],
    });

    printer.broken = false;
    await labels.client.retry('shipment-7/label');
    await labels.worker.drain();
    expect(await labels.client.getStatus('shipment-7/label')).toMatchObject({ status: 'completed', output: 'LBL-p-7' });
    // Its first end is the one its parent got: the retried end, deduplicated, stores no second signal.
    const ends = await labels.store.signals({ name: '$child-ended', key: 'shipment-7/label', afterId: 0, upToId: Number.MAX_SAFE_INTEGER });
    expect(ends.map((signal) => (signal.payload as { status: string }).status)).toEqual(['failed']);
    expect(await parents.worker.drain()).toBe(0);
    expect(await parents.client.getStatus('shipment-7')).toMatchObject({ output: failed });
  });
});

describe('terminate() of a parent', () => {
  it('undoes nothing of the parent, and cancels, terminates or leaves its children by their parentClose', async () => {
    const api = await start([]);
    const worker = await start([SubscriptionBoxWorkflow, BoxShipmentWorkflow]);
    await api.client.start(SubscriptionBoxWorkflow, { customer: 'c-1' }, { id: 'box-1' });
    await worker.worker.drain();

    expect(await api.client.terminate('box-1', 'The customer closed the account.')).toMatchObject({ accepted: true, status: 'suspended' });
    await worker.worker.drain();

    expect(await api.client.getStatus('box-1')).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowTerminatedError', message: 'The customer closed the account.' },
    });
    expect(await api.client.getStatus('c-1-cancel')).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowCancelledError', message: 'Cancelled: its parent "box-1" ended as cancelled.' },
    });
    expect(await api.client.getStatus('c-1-terminate')).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowTerminatedError', message: 'Terminated: its parent "box-1" ended as cancelled.' },
    });
    expect(await api.client.getStatus('c-1-abandon')).toMatchObject({ status: 'suspended', cancelRequested: false });
    expect(recorded('release-box')).toEqual([]);
    expect(recorded('cancel-booking')).toEqual(['c-1-cancel']);

    await api.client.signal(pickedUp, { carrier: 'DPD' }, { key: 'c-1-abandon' });
    await worker.worker.drain();
    expect(await api.client.result('c-1-abandon')).toBe('DPD');
  });
});

describe('purge()', () => {
  it('removes finished parents, children and ended windows, keeps schedules and the end of a child its parent still waits for', async () => {
    const app = await start([DailyReportWorkflow, ReportPartWorkflow]);
    const auditors = await start([AuditWorkflow]);
    clock.set(T0 + 3 * 3_600_000);
    await app.worker.drain();
    await app.client.start(AuditWorkflow, undefined, { id: 'audit-1' });
    await auditors.worker.drain();
    await app.worker.drain();
    // The audit's child ended, and its parent was woken, but no process runs audits until later.
    await stop(auditors);
    expect(await app.client.getStatus('audit-1')).toMatchObject({ status: 'suspended', wakeAt: T0 + 3 * 3_600_000 });

    clock.set(T0 + 40 * 86_400_000 + 2 * 3_600_000);
    const api = await start([]);
    expect(await api.client.purge({ olderThan: '30d' })).toEqual({ instances: 3, signals: 1, rateLimits: 1 });
    expect((await api.client.list()).map((instance) => [instance.id, instance.status])).toEqual([['audit-1', 'suspended']]);
    expect(await api.client.schedules.get('daily-report')).toMatchObject({ runs: 1 });

    const back = await start([AuditWorkflow]);
    await back.worker.drain();
    expect(await api.client.getStatus('audit-1', { children: true })).toMatchObject({ status: 'completed', output: 'rendered audit', children: [] });

    // The schedule and the rate limit carry on as before the purge.
    clock.set(T0 + 40 * 86_400_000 + 3 * 3_600_000);
    await app.worker.drain();
    const today = `daily-report@${iso(T0 + 40 * 86_400_000 + 3 * 3_600_000)}`;
    expect((await api.client.list({ scheduleId: 'daily-report' })).map((instance) => [instance.id, instance.status, instance.output])).toEqual([
      [today, 'completed', `rendered ${iso(T0 + 40 * 86_400_000 + 3 * 3_600_000)}`],
    ]);
    expect(recorded('render')).toEqual([iso(T0 + 3 * 3_600_000), 'audit', iso(T0 + 40 * 86_400_000 + 3 * 3_600_000)]);
  });
});

describe('@StartOn() and a concurrency key', () => {
  it('starts instances from events published in an API process, keyed from their input, one per key at a time', async () => {
    const api = await start([RestockOrderWorkflow], { cqrs: true });
    const a = await start([RestockOrderWorkflow], { cqrs: true, worker: { concurrency: 1 } });
    const b = await start([RestockOrderWorkflow], { cqrs: true });
    const eventBus = api.moduleRef.get(EventBus);
    for (const [sku, supplier] of [
      ['salmon-kibble-2kg', 'acme'],
      ['clumping-litter-10l', 'acme'],
      ['feather-wand', 'globex'],
    ] as const) {
      await eventBus.publish(new RestockRequestedEvent(sku, supplier));
      clock.advance('1s');
    }
    expect((await api.client.list()).map((instance) => [instance.id, instance.concurrencyKey])).toEqual([
      ['restock-salmon-kibble-2kg', 'acme'],
      ['restock-clumping-litter-10l', 'acme'],
      ['restock-feather-wand', 'globex'],
    ]);

    gate.holding.add('restock-salmon-kibble-2kg');
    const running = a.worker.drain({ maxRounds: 1 });
    await waitFor(() => gate.reached.length === 1);
    // acme is busy in the other process: the litter waits; globex's order runs.
    expect(await b.worker.drain()).toBe(1);
    expect(recorded('order')).toEqual(['restock-feather-wand']);

    gate.open();
    await running;
    expect(await b.worker.drain()).toBe(1);
    expect(recorded('order')).toEqual(['restock-feather-wand', 'restock-salmon-kibble-2kg', 'restock-clumping-litter-10l']);
  });
});

describe('@StartOn() with a priority and keys', () => {
  it('starts instances with the priority and keys it takes, fixed or from the event, in the process that publishes', async () => {
    const api = await start([PurchaseOrderWorkflow], { cqrs: true });
    const worker = await start([PurchaseOrderWorkflow], { cqrs: true, worker: { concurrency: 1 } });
    const eventBus = api.moduleRef.get(EventBus);
    await eventBus.publish(new StockLowEvent('salmon-kibble-2kg', 'acme', false));
    await eventBus.publish(new StockLowEvent('clumping-litter-10l', 'globex', true));
    await eventBus.publish(new StockLowEvent('feather-wand', 'initech', false));

    expect((await api.client.list()).map(({ id, priority, concurrencyKey, rateLimitKey }) => ({ id, priority, concurrencyKey, rateLimitKey }))).toEqual([
      { id: 'po-clumping-litter-10l', priority: 1, concurrencyKey: 'globex', rateLimitKey: 'purchasing' },
      { id: 'po-feather-wand', priority: 5, concurrencyKey: 'initech', rateLimitKey: 'purchasing' },
      { id: 'po-salmon-kibble-2kg', priority: 5, concurrencyKey: 'acme-warehouse', rateLimitKey: 'purchasing' },
    ]);

    // The urgent one first; all three share one window of two, the key every event gives.
    await worker.worker.drain();
    expect(recorded('purchase')).toEqual(['po-clumping-litter-10l', 'po-feather-wand']);
    clock.advance('1m');
    await worker.worker.drain();
    expect(recorded('purchase')).toEqual(['po-clumping-litter-10l', 'po-feather-wand', 'po-salmon-kibble-2kg']);
  });
});

describe('an app that reads custom statuses and results over HTTP', () => {
  it('answers while the refund is processed, then with its progress, its result, or its failure', async () => {
    const api = await bootHttp('express', { db, clock, workflows: [RefundProcessWorkflow], providers: providers(), controllers: [RefundsController] });
    nodes.push(api);
    const worker = await start([RefundProcessWorkflow]);

    expect(await api.http('POST', '/refunds/o-1', { amount: 2_499 })).toEqual({ status: 201, body: { status: 'processing' } });
    expect(await api.http('GET', '/refunds/o-1/progress')).toEqual({ status: 200, body: { status: 'pending', progress: null } });
    await worker.worker.drain();
    expect(await api.http('GET', '/refunds/o-1/progress')).toEqual({ status: 200, body: { status: 'suspended', progress: { stage: 'awaiting approval' } } });

    const result = api.http('GET', '/refunds/o-1/result');
    await api.client.signal(refundDecision, { approved: true }, { key: 'o-1' });
    await worker.worker.drain();
    expect(await result).toEqual({ status: 200, body: { refunded: 2_499 } });
    expect((await api.http('GET', '/refunds/o-1/progress')).body).toEqual({ status: 'completed', progress: { stage: 'refunded' } });
    // Started again with the same id, it answers with the result at once.
    expect(await api.http('POST', '/refunds/o-1', { amount: 2_499 })).toEqual({ status: 201, body: { refunded: 2_499 } });

    await api.http('POST', '/refunds/o-2', { amount: 799 });
    await api.client.signal(refundDecision, { approved: false }, { key: 'o-2' });
    await worker.worker.drain();
    expect(await api.http('GET', '/refunds/o-2/result')).toEqual({
      status: 500,
      body: {
        statusCode: 500,
        error: 'WorkflowFailedError',
        message: 'Instance "refund-o-2" of workflow "refund-process" failed: WorkflowFailedError: Refund of o-2 was declined.',
      },
    });
    expect(await api.http('GET', '/refunds/o-9/progress')).toMatchObject({ status: 404 });
  });
});
