/**
 * `WorkflowWorker` and composition: the concurrency limit and rounds of `drain()`, the lease
 * a running instance shows, the startup errors for worker settings, the polling loop woken by
 * local starts and signals, a cancel during a retry backoff, and a child workflow composed
 * from a step and a signal.
 */
import { Inject } from '@nestjs/common';
import {
  ManualWorkflowClock,
  Workflow,
  WorkflowClient,
  WorkflowSignal,
  type WorkflowContext,
} from '../lib/index.js';
import { boot, deferred, tempDb, type Node, type TestDb, waitFor, World } from './support.js';

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

const start = async (workflows: any[], options: Omit<Parameters<typeof boot>[0], 'db' | 'workflows'> = {}) => {
  const node = await boot({ db, clock, workflows, providers: [{ provide: World, useValue: world }], ...options });
  nodes.push(node);
  return node;
};

const busy = { active: 0, max: 0, started: 0 };

@Workflow('busy')
class Busy {
  async run(ctx: WorkflowContext) {
    await ctx.step('work', async () => {
      busy.active++;
      busy.started++;
      busy.max = Math.max(busy.max, busy.active);
      // The executions of a round meet (of five, two at a time, the last round has one): whether they overlap
      // doesn't depend on how fast the machine is.
      const round = Math.ceil(busy.started / 2);
      await waitFor(() => busy.started >= Math.min(round * 2, 5));
      busy.active--;
    });
  }
}

describe('drain()', () => {
  beforeEach(() => {
    busy.active = 0;
    busy.max = 0;
    busy.started = 0;
  });

  it('runs at most `concurrency` executions at once, until none is due', async () => {
    const node = await start([Busy], { worker: { concurrency: 2 } });
    for (let i = 0; i < 5; i++) {
      await node.client.start(Busy, undefined, { id: `b-${i}` });
    }

    expect(await node.worker.drain()).toBe(5);
    expect(busy.max).toBe(2);
    expect(await node.client.list({ status: 'completed' })).toHaveLength(5);
  });

  it('stops after maxRounds, leaving the rest due', async () => {
    const node = await start([Busy], { worker: { concurrency: 2 } });
    for (let i = 0; i < 5; i++) {
      await node.client.start(Busy, undefined, { id: `b-${i}` });
    }

    expect(await node.worker.drain({ maxRounds: 1 })).toBe(2);
    expect(await node.client.list({ status: 'pending' })).toHaveLength(3);
    expect(await node.worker.drain()).toBe(3);
  });

  it('runs nothing once the application has shut down', async () => {
    const node = await start([Busy]);
    await node.client.start(Busy, undefined, { id: 'late' });
    await node.worker.shutdown();

    expect(await node.worker.drain()).toBe(0);
    expect(await node.client.getStatus('late')).toMatchObject({ status: 'pending', runs: 0 });
  });
});

describe('the lease', () => {
  it("shows the worker's id and the lease end while the instance runs, and clears the lease when it ends", async () => {
    const gate = deferred();

    @Workflow('gated')
    class Gated {
      async run(ctx: WorkflowContext) {
        return ctx.step('wait-for-gate', () => gate.promise.then(() => 'through'));
      }
    }

    const node = await start([Gated], { worker: { id: 'worker-a', leaseDuration: '1m' } });
    await node.client.start(Gated, undefined, { id: 'g-1' });
    const draining = node.worker.drain();
    await waitFor(async () => (await node.client.getStatus('g-1'))?.status === 'running');

    expect(await node.client.getStatus('g-1')).toMatchObject({ leaseOwner: 'worker-a', leaseUntil: clock.now() + 60_000, runs: 1 });
    expect(node.worker.id).toBe('worker-a');

    gate.resolve();
    await draining;
    expect(await node.client.getStatus('g-1')).toMatchObject({ status: 'completed', output: 'through', leaseUntil: null });
  });

  it('defaults the worker id to host, pid and a random suffix', async () => {
    const a = await start([]);
    const b = await start([]);
    expect(a.worker.id).toMatch(new RegExp(`^.+:${process.pid}:[0-9a-f]{8}$`));
    expect(b.worker.id).not.toBe(a.worker.id);
  });
});

describe('worker settings', () => {
  it('refuses a heartbeat that would not come before the lease expires', async () => {
    await expect(start([], { worker: { leaseDuration: '30s', heartbeatInterval: '30s' } })).rejects.toThrow(
      'worker.heartbeatInterval (30000ms) must be shorter than worker.leaseDuration (30000ms).',
    );
  });

  it('refuses a duration it cannot parse', async () => {
    await expect(start([], { worker: { pollInterval: 'soon' as '1s' } })).rejects.toThrow(
      'Invalid duration "soon". Use milliseconds or a string such as "15m" or "3d".',
    );
    await expect(start([], { worker: { leaseDuration: -5 } })).rejects.toThrow('Invalid duration -5. Use a non-negative number of milliseconds.');
  });
});

describe('the polling loop', () => {
  it('picks up a local start and a local signal at once, without waiting for the next poll', async () => {
    @Workflow('handshake')
    class Handshake {
      async run(ctx: WorkflowContext) {
        await ctx.step('hello', () => 'hi');
        return ctx.waitForSignal<string>('reply', 'reply', { key: 'h-1' });
      }
    }

    // The system clock and an hour between polls: only the wake-ups can run it within the test.
    const node = await start([Handshake], { clock: undefined, worker: { enabled: true, pollInterval: '1h' } });
    await node.client.start(Handshake, undefined, { id: 'h-1' });
    await waitFor(async () => (await node.client.getStatus('h-1'))?.status === 'suspended');

    await node.client.signal('reply', 'hello back', { key: 'h-1' });
    await waitFor(async () => (await node.client.getStatus('h-1'))?.status === 'completed');
    expect(await node.client.getStatus('h-1')).toMatchObject({ output: 'hello back', runs: 2 });
  });
});

describe('cancel during a retry backoff', () => {
  it('wakes the instance at once, compensates, and marks the retry that will never run as cancelled', async () => {
    @Workflow('retrying')
    class Retrying {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('book', () => 'b-1', { compensate: () => this.world.record('unbook', '') });
        await ctx.step(
          'charge',
          (s) => {
            this.world.record('charge', s.idempotencyKey, s.attempt);
            throw new Error('503');
          },
          { retry: { attempts: 5, backoff: { delay: '1m' } } },
        );
      }
    }

    const node = await start([Retrying]);
    const t0 = clock.now();
    await node.client.start(Retrying, undefined, { id: 'r-1' });
    await node.worker.drain();
    expect(await node.client.getStatus('r-1')).toMatchObject({ status: 'suspended', wakeAt: t0 + 60_000 });

    expect(await node.client.cancel('r-1', 'Order withdrawn.')).toMatchObject({ accepted: true });
    expect(await node.worker.drain()).toBe(1);

    expect(await node.client.getStatus('r-1', { journal: true })).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowCancelledError', message: 'Order withdrawn.' },
      journal: [
        { name: 'book', status: 'completed' },
        { name: 'charge', status: 'cancelled', attempts: 1, wakeAt: null },
        { name: '$compensate:book', status: 'completed' },
      ],
    });
    expect(world.ops()).toEqual(['charge', 'unbook']);
  });
});

describe('child workflows', () => {
  const invoiced = new WorkflowSignal<{ total: number }>('invoice.done');

  @Workflow('invoice')
  class Invoice {
    constructor(private readonly workflowClient: WorkflowClient) {}

    async run(ctx: WorkflowContext, input: { parent: string; lines: number[] }) {
      const total = await ctx.step('sum', () => input.lines.reduce((sum, line) => sum + line, 0));
      await ctx.step('report', () => this.workflowClient.signal(invoiced, { total }, { key: input.parent }));
      return total;
    }
  }

  @Workflow('billing-run')
  class BillingRun {
    constructor(private readonly workflowClient: WorkflowClient) {}

    async run(ctx: WorkflowContext, input: { lines: number[] }) {
      const parent = ctx.workflowId;
      // The child's id derives from the parent's, so a re-run of this step finds the same child.
      const child = await ctx.step('start-invoice', () =>
        this.workflowClient.start(Invoice, { parent, lines: input.lines }, { id: `${parent}:invoice` }),
      );
      const result = await ctx.waitForSignal('invoice-done', invoiced, { key: parent, timeout: '1h' });
      return { child: child.id, total: result?.total ?? null };
    }
  }

  it('starts a child from a step and waits for the signal it sends when done', async () => {
    const node = await start([BillingRun, Invoice]);
    await node.client.start(BillingRun, { lines: [100, 250, 50] }, { id: 'run-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('run-1')).toMatchObject({ status: 'completed', output: { child: 'run-1:invoice', total: 400 }, runs: 2 });
    expect(await node.client.getStatus('run-1:invoice')).toMatchObject({ workflow: 'invoice', status: 'completed', output: 400 });

    // Starting the child again, as a retried step would, finds the same instance.
    expect(await node.client.start(Invoice, { parent: 'run-1', lines: [100, 250, 50] }, { id: 'run-1:invoice' })).toMatchObject({
      created: false,
      status: 'completed',
    });
  });
});
