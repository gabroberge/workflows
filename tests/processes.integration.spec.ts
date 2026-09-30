/**
 * Several application instances ("processes") on one database, each with its own connection pool on PostgreSQL: a
 * parent in one and its child claimed by another, results awaited, and instances terminated or cancelled, from an
 * API process that runs no workflow, concurrency and rate limits that hold across processes (a shared meter counts
 * what runs, the store what holds a lease or started), and schedules whose occurrences several workers race to
 * start.
 */
import { Injectable, Logger, type Type } from '@nestjs/common';
import {
  ChildWorkflowFailedError,
  ManualWorkflowClock,
  NonRetryableStepError,
  Workflow,
  WorkflowFailedError,
  WorkflowNotFoundError,
  WorkflowSignal,
  type WorkflowContext,
  type WorkflowStore,
  type WorkflowWorkerOptions,
} from '../lib/index.js';
import { boot, deferred, heartbeatRead, storeClass, tempDb, waitFor, World, type Node, type TestDb } from './support.js';

const pickedUp = new WorkflowSignal<{ carrier: string }>('parcel.picked-up');

/** Where a step stops until the test lets it go, shared by every process as the outside world is. */
@Injectable()
class Gate {
  holdAt: string | null = null;
  reached = deferred();
  release = deferred();

  async pass(point: string) {
    if (this.holdAt === point) {
      this.reached.resolve();
      await this.release.promise;
    }
  }
}

/**
 * Counts the steps running at once, per key, and keeps each one running until `release()`: what a limit lets run
 * together runs together, however the processes are scheduled.
 */
@Injectable()
class Meter {
  readonly entered: string[] = [];
  readonly peak = new Map<string, number>();
  private readonly running = new Map<string, number>();
  private gates: Array<() => void> = [];

  async hold(id: string, keys: string[]) {
    this.entered.push(id);
    for (const key of keys) {
      const n = this.now(key) + 1;
      this.running.set(key, n);
      this.peak.set(key, Math.max(this.peak.get(key) ?? 0, n));
    }

    await new Promise<void>((resolve) => this.gates.push(resolve));
    for (const key of keys) {
      this.running.set(key, this.now(key) - 1);
    }
  }

  now(key: string) {
    return this.running.get(key) ?? 0;
  }

  release() {
    for (const open of this.gates.splice(0)) {
      open();
    }
  }
}

@Workflow('shipping')
class ShippingWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { orderId: string }) {
    await ctx.step('book-carrier', ({ idempotencyKey }) => this.world.record('book', idempotencyKey), {
      compensate: (_booking, { idempotencyKey }) => this.world.record('cancel-booking', idempotencyKey),
    });

    const pickup = await ctx.waitForSignal('pickup', pickedUp, { key: input.orderId });
    return { label: `LBL-${input.orderId}`, carrier: pickup!.carrier };
  }
}

@Workflow('fulfilment')
class FulfilmentWorkflow {
  async run(ctx: WorkflowContext, input: { orderId: string }) {
    try {
      const shipment = await ctx.executeChild(ShippingWorkflow, { orderId: input.orderId });
      return { shipped: shipment.label, by: shipment.carrier };
    } catch (error) {
      if (!(error instanceof ChildWorkflowFailedError)) {
        throw error;
      }
      return { shipped: null, status: error.status, cause: error.cause!.name, reason: error.cause!.message };
    }
  }
}

@Workflow('import')
class ImportWorkflow {
  constructor(
    private readonly world: World,
    private readonly gate: Gate,
  ) {}

  async run(ctx: WorkflowContext, input: { file: string; failAt?: string; undoFails?: boolean }) {
    for (const step of ['download', 'transform', 'load']) {
      await ctx.step(
        step,
        async ({ idempotencyKey }) => {
          await this.gate.pass(step);
          if (input.failAt === step) {
            throw new NonRetryableStepError(`The ${step} of ${input.file} failed.`);
          }
          this.world.record(step, idempotencyKey);
        },
        {
          compensate: (_result, { idempotencyKey }) => {
            if (input.undoFails) {
              throw new Error(`Undoing the ${step} of ${input.file} failed.`);
            }
            this.world.record(`undo-${step}`, idempotencyKey);
          },
          compensateRetry: false,
        },
      );
    }
    return { file: input.file, loaded: true };
  }
}

@Workflow('render', { concurrency: [{ limit: 2 }, { limit: 1, key: (input: { tenant: string }) => input.tenant }] })
class RenderWorkflow {
  constructor(private readonly meter: Meter) {}

  async run(ctx: WorkflowContext, input: { tenant: string }) {
    const id = ctx.workflowId;
    await ctx.step('render', () => this.meter.hold(id, ['all', `tenant:${input.tenant}`]));
  }
}

@Workflow('sync', { rateLimit: [{ max: 3, duration: '1m' }, { max: 1, duration: '1m', key: (input: { account: string }) => input.account }] })
class SyncWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { account: string }) {
    // Every execution runs run() once: this counts the executions that started.
    this.world.record('execution', ctx.workflowId);
    await ctx.step('pull', () => this.world.record('pull', input.account));
    await ctx.sleep('settle', '10s');
    await ctx.step('push', () => this.world.record('push', input.account));
  }
}

@Workflow('hourly-digest', { schedules: [{ id: 'hourly-digest', cron: '0 * * * *', missed: 'all', overlap: 'allow' }] })
class HourlyDigestWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    const at = new Date(ctx.schedule!.at).toISOString();
    await ctx.step('send', () => this.world.record('digest', at));
  }
}

@Workflow('label-print', { concurrency: { limit: 1 } })
class LabelPrintWorkflow {
  constructor(
    private readonly world: World,
    private readonly gate: Gate,
  ) {}

  async run(ctx: WorkflowContext, input: { fail?: boolean }) {
    const id = ctx.workflowId;
    ctx.setStatus({ stage: 'printing' });
    await ctx.step(
      'print',
      async () => {
        await this.gate.pass(`print:${id}`);
        this.world.record('print', id);
      },
      {
        compensate: async () => {
          await this.gate.pass(`shred:${id}`);
          this.world.record('shred', id);
        },
      },
    );
    if (input.fail) {
      ctx.setStatus({ stage: 'jammed' });
      ctx.fail('The printer jammed.');
    }
  }
}

@Workflow('invoice-run')
class InvoiceRunV1 {
  constructor(private readonly meter: Meter) {}

  async run(ctx: WorkflowContext) {
    const id = ctx.workflowId;
    await ctx.step('bill', () => this.meter.hold(id, ['invoice-run']));
  }
}

@Workflow('invoice-run', { version: 2, concurrency: { limit: 1 } })
class InvoiceRunV2 {
  constructor(private readonly meter: Meter) {}

  async run(ctx: WorkflowContext) {
    const id = ctx.workflowId;
    await ctx.step('bill', () => this.meter.hold(id, ['invoice-run']));
  }
}

@Workflow('newsletter-send', { rateLimit: { max: 1, duration: '1m' } })
class NewsletterSendWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    const id = ctx.workflowId;
    this.world.record('execution', id);
    await ctx.step('send', () => this.world.record('send', id), { compensate: () => this.world.record('unsend', id) });
    await ctx.sleep('cool-down', '10s');
  }
}

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
let gate: Gate;
let meter: Meter;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  world = new World();
  gate = new Gate();
  meter = new Meter();
});

afterEach(async () => {
  vi.restoreAllMocks();
  gate.release.resolve();
  meter.release();
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

/** One process: its own application, connection pool and worker id, running `workflows` (none: an API process). */
async function start(id: string, workflows: Type<unknown>[], worker: WorkflowWorkerOptions = {}) {
  const node = await boot({
    db,
    clock,
    workflows,
    worker: { id, ...worker },
    providers: [
      { provide: World, useValue: world },
      { provide: Gate, useValue: gate },
      { provide: Meter, useValue: meter },
    ],
  });
  nodes.push(node);
  return node;
}

async function stop(node: Node) {
  nodes.splice(nodes.indexOf(node), 1);
  await node.close();
}

/** Every process's store is one of these: in memory they share one, on SQL each opens its own. */
const storePrototype = storeClass.prototype as WorkflowStore;

const types = (node: Node) => node.events.map((event) => event.type);

describe('a parent and its child in different processes', () => {
  it('runs the child where it is registered, and the parent gets its output in its own process', async () => {
    const api = await start('api', []);
    const parents = await start('parents', [FulfilmentWorkflow]);
    const children = await start('children', [ShippingWorkflow]);
    await api.client.start(FulfilmentWorkflow, { orderId: 'o-1' }, { id: 'order-1' });
    const result = api.client.result('order-1', { timeout: '10s' });

    expect(await parents.worker.drain()).toBe(1);
    expect(await parents.worker.drain()).toBe(0);
    expect(await children.worker.drain()).toBe(1);
    expect(await api.client.getStatus('order-1/shipping#1')).toMatchObject({ status: 'suspended', parentId: 'order-1', leaseOwner: 'children' });

    await api.client.signal(pickedUp, { carrier: 'DHL' }, { key: 'o-1' });
    expect(await children.worker.drain()).toBe(1);
    // The child's final write woke the parent, which only the parents' process runs.
    expect(await children.worker.drain()).toBe(0);
    expect(await parents.worker.drain()).toBe(1);

    await expect(result).resolves.toEqual({ shipped: 'LBL-o-1', by: 'DHL' });
    expect(await api.client.getStatus('order-1', { children: true })).toMatchObject({
      status: 'completed',
      runs: 2,
      leaseOwner: 'parents',
      children: [{ id: 'order-1/shipping#1', status: 'completed', runs: 2, leaseOwner: 'children' }],
    });
    expect(types(parents)).toEqual(['workflow-started', 'child-started', 'workflow-suspended', 'workflow-resumed', 'workflow-completed']);
    expect(types(children)).toEqual([
      'workflow-started',
      'step-completed',
      'workflow-suspended',
      'workflow-resumed',
      'signal-received',
      'workflow-completed',
    ]);
    expect(api.events).toEqual([]);
  });

  it('reports a child that an API process terminates or cancels to its parent as a ChildWorkflowFailedError', async () => {
    const api = await start('api', []);
    const parents = await start('parents', [FulfilmentWorkflow]);
    const children = await start('children', [ShippingWorkflow]);
    for (const orderId of ['o-2', 'o-3']) {
      await api.client.start(FulfilmentWorkflow, { orderId }, { id: `order-${orderId}` });
    }
    await parents.worker.drain();
    await children.worker.drain();

    expect(await api.client.terminate('order-o-2/shipping#1', 'The carrier went out of business.')).toMatchObject({
      accepted: true,
      status: 'suspended',
      terminateRequested: true,
    });
    expect(await api.client.cancel('order-o-3/shipping#1', 'The customer collects it.')).toMatchObject({ accepted: true, status: 'suspended' });
    expect(await children.worker.drain()).toBe(2);
    expect(await parents.worker.drain()).toBe(2);

    expect(await api.client.result('order-o-2')).toEqual({
      shipped: null,
      status: 'cancelled',
      cause: 'WorkflowTerminatedError',
      reason: 'The carrier went out of business.',
    });
    expect(await api.client.result('order-o-3')).toEqual({ shipped: null, status: 'cancelled', cause: 'WorkflowCancelledError', reason: 'The customer collects it.' });
    // Only the cancelled child undid its booking.
    expect(world.calls.filter((call) => call.op === 'cancel-booking').map((call) => call.key)).toEqual(['order-o-3/shipping#1:$compensate:book-carrier']);
  });
});

describe('an API process that runs no workflow', () => {
  it('terminates an execution running in another process, which stops at its next step and undoes nothing', async () => {
    const api = await start('api', []);
    const worker = await start('worker', [ImportWorkflow], { heartbeatInterval: '20ms' });
    const read = heartbeatRead(worker, 'terminateRequested');
    gate.holdAt = 'transform';
    await api.client.start(ImportWorkflow, { file: 'stock.csv' }, { id: 'import-1' });
    const running = worker.worker.drain();
    await gate.reached.promise;

    expect(await api.client.terminate('import-1', 'The supplier sent the wrong file.')).toMatchObject({ accepted: true, status: 'running' });
    await read;
    gate.release.resolve();
    await running;

    expect(await api.client.getStatus('import-1', { journal: true })).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowTerminatedError', message: 'The supplier sent the wrong file.' },
      journal: [{ name: 'download', status: 'completed' }, { name: 'transform', status: 'completed' }],
    });
    expect(world.ops()).toEqual(['download', 'transform']);
    expect(api.events).toEqual([]);
    expect(worker.events.at(-1)).toMatchObject({ type: 'workflow-cancelled', error: { name: 'WorkflowTerminatedError' } });
  });

  it('cancels an execution running in another process, which stops at its next step and compensates there', async () => {
    const api = await start('api', []);
    const worker = await start('worker', [ImportWorkflow], { heartbeatInterval: '20ms' });
    const read = heartbeatRead(worker, 'cancelRequested');
    gate.holdAt = 'transform';
    await api.client.start(ImportWorkflow, { file: 'stock.csv' }, { id: 'import-2' });
    const running = worker.worker.drain();
    await gate.reached.promise;

    expect(await api.client.cancel('import-2', 'Imported by hand instead.')).toMatchObject({ accepted: true, status: 'running' });
    await read;
    gate.release.resolve();
    await running;

    expect(await api.client.getStatus('import-2')).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowCancelledError', message: 'Imported by hand instead.' },
    });
    expect(world.ops()).toEqual(['download', 'transform', 'undo-transform', 'undo-download']);
    expect(await api.client.terminate('import-2')).toMatchObject({ accepted: false, status: 'cancelled' });
  });

  it('waits for results that another process produces: a failure, a deletion, and its own shutdown', async () => {
    // The deleted instance's execution finds its lease gone, and says so.
    vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
    const api = await start('api', []);
    const leaving = await start('leaving', []);
    const worker = await start('worker', [ImportWorkflow]);
    await api.client.start(ImportWorkflow, { file: 'bad.csv', failAt: 'load', undoFails: true }, { id: 'failing' });
    await api.client.start(ImportWorkflow, { file: 'late.csv' }, { id: 'doomed' });
    await api.client.start(ImportWorkflow, { file: 'slow.csv' }, { id: 'waiting' });
    const failed = api.client.result('failing').catch((error: unknown) => error);
    const deleted = api.client.result('doomed').catch((error: unknown) => error);
    const interrupted = leaving.client.result('waiting').catch((error: unknown) => error);

    gate.holdAt = 'download';
    const running = worker.worker.drain();
    await gate.reached.promise;
    await worker.client.delete('doomed', { force: true });
    expect(await deleted).toBeInstanceOf(WorkflowNotFoundError);

    await stop(leaving);
    expect(await interrupted).toMatchObject({ message: 'The application shut down while waiting for the result of instance "waiting".' });
    gate.release.resolve();
    await running;

    const failure = (await failed) as WorkflowFailedError;
    expect(failure).toBeInstanceOf(WorkflowFailedError);
    expect(failure).toMatchObject({
      instanceId: 'failing',
      status: 'compensation_failed',
      cause: {
        name: 'StepFailedError',
        message: expect.stringContaining('The load of bad.csv failed.'),
        compensation: { name: 'Error', message: expect.stringContaining('Undoing the transform of bad.csv failed.') },
      },
    });
    expect(await api.client.result('waiting')).toEqual({ file: 'slow.csv', loaded: true });
  });
  it('terminates an instance no process ran yet, which then runs nothing, and refuses a cancel after it', async () => {
    const api = await start('api', []);
    const worker = await start('worker', [ImportWorkflow]);
    await api.client.start(ImportWorkflow, { file: 'duplicate.csv' }, { id: 'import-3' });

    expect(await api.client.terminate('import-3', 'Uploaded by mistake.')).toMatchObject({ accepted: true, status: 'pending' });
    expect(await api.client.cancel('import-3', 'Too late.')).toMatchObject({ accepted: false, status: 'pending', cancelReason: 'Uploaded by mistake.' });
    expect(await worker.worker.drain()).toBe(1);

    expect(await api.client.getStatus('import-3', { journal: true })).toMatchObject({
      status: 'cancelled',
      runs: 1,
      error: { name: 'WorkflowTerminatedError', message: 'Uploaded by mistake.' },
      journal: [],
    });
    expect(world.calls).toEqual([]);
    expect(types(worker)).toEqual(['workflow-cancelled']);
  });
});

describe('limits across processes', () => {
  it('lets no more instances run at once than the limits allow, whichever process claims them', async () => {
    const [a, b, c] = [await start('pod-a', [RenderWorkflow]), await start('pod-b', [RenderWorkflow]), await start('pod-c', [RenderWorkflow])];
    const tenants = ['acme', 'acme', 'globex', 'globex', 'initech', 'initech'];
    for (const [i, tenant] of tenants.entries()) {
      await a.client.start(RenderWorkflow, { tenant }, { id: `r-${i + 1}` });
    }

    // The first wave is a race; each later one runs in a process the test picks. Each key runs one at a time and
    // the workflow two, so the waves are the same whoever wins.
    const waves = [
      { drain: [a, b, c], ids: ['r-1', 'r-3'] },
      { drain: [b], ids: ['r-2', 'r-4'] },
      { drain: [c], ids: ['r-5'] },
      { drain: [a], ids: ['r-6'] },
    ];
    const owners: string[][] = [];
    let entered = 0;
    for (const wave of waves) {
      const draining = wave.drain.map((node) => node.worker.drain({ maxRounds: 1 }));
      entered += wave.ids.length;
      await waitFor(() => meter.entered.length >= entered);

      // Every process tries again while the wave runs: the others' leases hold the slots.
      for (const node of [a, b, c]) {
        expect(await node.worker.drain({ maxRounds: 1 })).toBe(0);
      }
      expect(meter.entered.slice(-wave.ids.length).sort()).toEqual(wave.ids);
      expect(meter.now('all')).toBe(wave.ids.length);
      const leased = await liveLeases(a);
      expect(leased.map((instance) => instance.id)).toEqual(wave.ids);
      owners.push([...new Set(leased.map((instance) => instance.leaseOwner!))]);

      meter.release();
      await Promise.all(draining);
    }

    expect(meter.entered).toHaveLength(6);
    expect(meter.peak.get('all')).toBe(2);
    expect(tenants.map((tenant) => meter.peak.get(`tenant:${tenant}`))).toEqual([1, 1, 1, 1, 1, 1]);
    expect(owners[0]).toHaveLength(1);
    expect(owners.slice(1)).toEqual([['pod-b'], ['pod-c'], ['pod-a']]);
    expect((await a.client.list({ status: 'completed' })).map((instance) => instance.runs)).toEqual([1, 1, 1, 1, 1, 1]);
  });

  it('starts no more executions per window than the rate limits allow, however many processes claim', async () => {
    const pods = [await start('pod-a', [SyncWorkflow]), await start('pod-b', [SyncWorkflow]), await start('pod-c', [SyncWorkflow])];
    const accounts = ['x', 'x', 'y', 'z', 'w'];
    for (const [i, account] of accounts.entries()) {
      await pods[0]!.client.start(SyncWorkflow, { account }, { id: `s-${i + 1}` });
    }
    const t0 = clock.now();
    const executions: string[][] = [];
    const window = async (at: number) => {
      clock.set(t0 + at);
      const before = world.calls.length;
      await Promise.all(pods.map((pod) => pod.worker.drain()));
      executions.push(
        world.calls
          .slice(before)
          .filter((call) => call.op === 'execution')
          .map((call) => call.key)
          .sort(),
      );
    };

    // Each window takes 3 executions, one per account: first runs and resumptions alike, most overdue first.
    await window(0);
    await window(10_000); // the three that ran wake from their sleep, but the window is full
    await window(59_999);
    await window(60_000);
    await window(70_000);
    await window(120_000);
    await window(180_000);

    expect(executions).toEqual([['s-1', 's-3', 's-4'], [], [], ['s-2', 's-3', 's-5'], [], ['s-1', 's-4', 's-5'], ['s-2']]);
    const instances = await pods[0]!.client.list();
    expect(instances.map((instance) => [instance.id, instance.status, instance.runs])).toEqual(
      ['s-1', 's-2', 's-3', 's-4', 's-5'].map((id) => [id, 'completed', 2]),
    );
  });
  it('keeps a compensating instance in its slot, wherever it compensates, with the status it switched with', async () => {
    const api = await start('api', []);
    const [a, b] = [await start('pod-a', [LabelPrintWorkflow]), await start('pod-b', [LabelPrintWorkflow])];
    await api.client.start(LabelPrintWorkflow, { fail: true }, { id: 'l-1' });
    await api.client.start(LabelPrintWorkflow, {}, { id: 'l-2' });
    gate.holdAt = 'shred:l-1';
    const running = a.worker.drain();
    await gate.reached.promise;

    expect(await b.worker.drain()).toBe(0);
    expect(await a.worker.drain({ maxRounds: 1 })).toBe(0);
    expect((await liveLeases(api)).map((instance) => [instance.id, instance.status, instance.customStatus])).toEqual([['l-1', 'compensating', { stage: 'jammed' }]]);

    gate.release.resolve();
    await running;
    expect(world.calls.map((call) => `${call.op} ${call.key}`)).toEqual(['print l-1', 'shred l-1', 'print l-2']);
    expect(await api.client.getStatus('l-1')).toMatchObject({ status: 'failed', customStatus: { stage: 'jammed' } });
  });

  it('ends a cancelled instance that waits for a slot once it gets one, without running it', async () => {
    const api = await start('api', []);
    const [a, b] = [await start('pod-a', [LabelPrintWorkflow]), await start('pod-b', [LabelPrintWorkflow])];
    await api.client.start(LabelPrintWorkflow, {}, { id: 'l-3' });
    await api.client.start(LabelPrintWorkflow, {}, { id: 'l-4' });
    gate.holdAt = 'print:l-3';
    const running = a.worker.drain();
    await gate.reached.promise;

    expect(await api.client.cancel('l-4', 'Printed by hand.')).toMatchObject({ accepted: true, status: 'pending' });
    expect(await b.worker.drain()).toBe(0);
    expect(await api.client.getStatus('l-4')).toMatchObject({ status: 'pending', cancelRequested: true, runs: 0 });

    gate.release.resolve();
    await running;
    expect(await api.client.getStatus('l-4')).toMatchObject({ status: 'cancelled', runs: 1, error: { message: 'Printed by hand.' } });
    expect(world.calls.map((call) => `${call.op} ${call.key}`)).toEqual(['print l-3']);
  });

  it("applies the highest registered version's limit to every version, in each process that runs that code", async () => {
    const api = await start('api', []);
    const current = await start('current', [InvoiceRunV1, InvoiceRunV2]);
    const legacy = await start('legacy', [InvoiceRunV1]);
    await api.client.start(InvoiceRunV1, {}, { id: 'i-1', version: 1 });
    await api.client.start(InvoiceRunV2, {}, { id: 'i-2', version: 2 });
    await api.client.start(InvoiceRunV1, {}, { id: 'i-3', version: 1 });

    // Version 2's limit holds version 1 too, where the code that declares it runs.
    const first = current.worker.drain({ maxRounds: 1 });
    await waitFor(() => meter.entered.length === 1);
    expect(await current.worker.drain({ maxRounds: 1 })).toBe(0);
    // A process of the old code knows no limit: in a rolling deploy it runs version 1 beside it.
    const second = legacy.worker.drain({ maxRounds: 1 });
    await waitFor(() => meter.entered.length === 2);
    expect(await legacy.worker.drain({ maxRounds: 1 })).toBe(0);
    expect(meter.entered).toEqual(['i-1', 'i-3']);
    expect(meter.now('invoice-run')).toBe(2);

    meter.release();
    await Promise.all([first, second]);
    const third = current.worker.drain();
    await waitFor(() => meter.entered.length === 3);
    meter.release();
    await third;
    expect((await api.client.list()).map((instance) => [instance.id, instance.version, instance.status, instance.leaseOwner])).toEqual([
      ['i-1', 1, 'completed', 'current'],
      ['i-2', 2, 'completed', 'current'],
      ['i-3', 1, 'completed', 'legacy'],
    ]);
  });

  it('counts the executions that compensate toward a rate limit, as every execution', async () => {
    const pods = [await start('pod-a', [NewsletterSendWorkflow]), await start('pod-b', [NewsletterSendWorkflow])];
    const t0 = clock.now();
    const window = async (at: number) => {
      clock.set(t0 + at);
      const before = world.calls.length;
      await Promise.all(pods.map((pod) => pod.worker.drain()));
      return world.calls.slice(before).map((call) => `${call.op} ${call.key}`);
    };
    for (const id of ['n-1', 'n-2']) {
      await pods[0]!.client.start(NewsletterSendWorkflow, undefined, { id });
    }

    expect(await window(0)).toEqual(['execution n-1', 'send n-1']);
    clock.set(t0 + 5_000);
    await pods[1]!.client.cancel('n-1', 'Sent to the wrong list.');
    expect(await window(5_000)).toEqual([]);
    // The next window goes to n-2, which waited longer; n-1's compensation waits for the one after.
    expect(await window(60_000)).toEqual(['execution n-2', 'send n-2']);
    expect(await window(120_000)).toEqual(['execution n-1', 'unsend n-1']);
    expect(await window(180_000)).toEqual(['execution n-2']);
    expect((await pods[0]!.client.list()).map((instance) => [instance.id, instance.status, instance.runs])).toEqual([
      ['n-1', 'cancelled', 2],
      ['n-2', 'completed', 2],
    ]);
  });
});

describe('a schedule several processes produce', () => {
  it('saves a declared schedule once and starts each occurrence once, however many processes race for it', async () => {
    const pods = await Promise.all(['pod-a', 'pod-b', 'pod-c', 'pod-d'].map((id) => start(id, [HourlyDigestWorkflow])));
    expect(await pods[0]!.store.getSchedule('hourly-digest')).toMatchObject({ revision: 1, declared: true });
    const create = vi.spyOn(storePrototype, 'create');

    // Five occurrences are due at once (missed: 'all'), then one more.
    clock.advance('5h');
    clock.advance('30m');
    await Promise.all(pods.map((pod) => pod.worker.drain()));
    clock.advance('30m');
    await Promise.all(pods.map((pod) => pod.worker.drain()));

    const hours = [1, 2, 3, 4, 5, 6].map((hour) => new Date(Date.UTC(2026, 0, 1, hour)).toISOString());
    expect(world.calls.map((call) => call.key).sort()).toEqual(hours);
    expect((await pods[1]!.client.list({ scheduleId: 'hourly-digest' })).map((instance) => [instance.id, instance.status])).toEqual(
      hours.map((at) => [`hourly-digest@${at}`, 'completed']),
    );
    // One producer per occurrence: each instance was asked for once.
    const asked = create.mock.calls.map(([instance]) => instance.id).filter((id) => id.startsWith('hourly-digest@'));
    expect(asked.sort()).toEqual(hours.map((at) => `hourly-digest@${at}`));
    expect(await pods[2]!.client.schedules.get('hourly-digest')).toMatchObject({ runs: 6, nextAt: Date.UTC(2026, 0, 1, 7) });
  });
});

/** The instances whose lease is live: those an execution runs right now. */
async function liveLeases(node: Node) {
  const running = await node.client.list({ status: ['running', 'compensating'] });
  return running.filter((instance) => instance.leaseUntil !== null && instance.leaseUntil >= clock.now());
}
