import { setTimeout as sleep } from 'node:timers/promises';
import { Inject, Injectable } from '@nestjs/common';
import {
  ManualWorkflowClock,
  NonRetryableStepError,
  Workflow,
  type WorkflowContext,
} from '../lib/index.js';
import { boot, deferred, tempDb, type TestDb, World, type Node } from './support.js';

/**
 * `WorkflowInterrupt` is control flow: the engine throws it out of `ctx` calls
 * to unwind `run()`. User code is told to rethrow it, but the engine must not
 * depend on that: a swallowed or wrapped interrupt may not cause a side
 * effect, move the point of no return, or change how the instance ends.
 */
@Injectable()
class Control {
  started = deferred();
  release = deferred();
  readonly caught: unknown[] = [];
}

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
let control: Control;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  world = new World();
  control = new Control();
});

afterEach(async () => {
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

async function start(workflows: any[], options: Omit<Parameters<typeof boot>[0], 'db' | 'workflows'> = {}) {
  const node = await boot({
    db,
    clock,
    workflows,
    ...options,
    providers: [
      { provide: World, useValue: world },
      { provide: Control, useValue: control },
    ],
  });

  nodes.push(node);
  return node;
}

describe('swallowed interrupts', () => {
  @Workflow('swallow-suspend')
  class SwallowSuspend {
    constructor(
      @Inject(World) private readonly world: World,
      @Inject(Control) private readonly control: Control,
    ) {}

    async run(ctx: WorkflowContext) {
      try {
        await ctx.sleep('nap', '1h');
      } catch (error) {
        this.control.caught.push(error);
      }

      try {
        await ctx.step('charge', (s) => this.world.record('charge', s.idempotencyKey));
      } catch (error) {
        this.control.caught.push(error);
      }

      return 'done';
    }
  }

  it('starts no step and records no outcome until the instance really resumes', async () => {
    const node = await start([SwallowSuspend]);
    await node.client.start(SwallowSuspend, undefined, { id: 's-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('s-1')).toMatchObject({ status: 'suspended', wakeAt: clock.now() + 3_600_000 });
    expect(world.ops()).toEqual([]);
    expect(control.caught.map((e: any) => e.reason)).toEqual(['suspend', 'suspend']);

    clock.advance('1h');
    await node.worker.drain();
    expect(await node.client.getStatus('s-1')).toMatchObject({ status: 'completed', output: 'done' });
    expect(world.ops()).toEqual(['charge']);
  });

  @Workflow('swallow-then-commit')
  class SwallowThenCommit {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      await ctx.step('charge', (s) => this.world.record('charge', s.idempotencyKey), {
        compensate: (_, s) => this.world.record('refund', s.idempotencyKey),
      });

      let delivered: unknown = null;
      try {
        delivered = await ctx.waitForSignal('await-delivery', 'delivered', { key: ctx.workflowId, timeout: '3d' });
      } catch {
        // Wrong: swallows the suspension and carries on as if nothing happened.
      }

      ctx.commit('delivered');
      const at = ctx.now();
      await ctx.sleep('before-review', '7d');
      return { delivered, at };
    }
  }

  it('cannot pass the point of no return, so a later cancel still compensates', async () => {
    const node = await start([SwallowThenCommit]);
    await node.client.start(SwallowThenCommit, undefined, { id: 'c-1' });
    await node.worker.drain();

    const parked = await node.client.getStatus('c-1', { journal: true });
    expect(parked).toMatchObject({ status: 'suspended', waits: [{ signal: 'delivered', key: 'c-1' }] });
    // Nothing after the swallowed wait was journaled: no commit, no ctx.now(), no sleep.
    expect(parked!.journal.map((e) => e.name)).toEqual(['charge', 'await-delivery']);

    await node.client.cancel('c-1', 'Changed my mind.');
    await node.worker.drain();
    expect(await node.client.getStatus('c-1')).toMatchObject({ status: 'cancelled' });
    expect(world.ops()).toEqual(['charge', 'refund']);
  });

  @Workflow('provision')
  class Provision {
    constructor(
      @Inject(World) private readonly world: World,
      @Inject(Control) private readonly control: Control,
    ) {}

    async run(ctx: WorkflowContext, input: { mode: 'swallow' | 'wrap' }) {
      const undo = (name: string) => ({ compensate: () => this.world.record(`undo-${name}`, '') });

      await ctx.step('create-db', () => this.world.record('create-db', ''), undo('create-db'));
      await ctx.step(
        'create-vm',
        async () => {
          this.control.started.resolve();
          await this.control.release.promise;
          this.world.record('create-vm', '');
        },
        undo('create-vm'),
      );

      try {
        await ctx.step('create-dns', () => this.world.record('create-dns', ''), undo('create-dns'));
      } catch (error) {
        this.control.caught.push(error);
        if (input.mode === 'wrap') {
          throw new Error('DNS setup failed.', { cause: error });
        }
      }

      return 'provisioned';
    }
  }

  it.each(['swallow', 'wrap'] as const)('ends as cancelled when run() %ss the cancel interrupt', async (mode) => {
    const node = await start([Provision], { worker: { heartbeatInterval: 20 } });
    await node.client.start(Provision, { mode }, { id: 'p-1' });
    const running = node.worker.drain();
    await control.started.promise;

    await node.client.cancel('p-1', 'Plan downgraded.');
    await sleep(60); // a heartbeat or two
    control.release.resolve();
    await running;

    expect(control.caught.map((e: any) => e.reason)).toEqual(['cancel']);
    expect(await node.client.getStatus('p-1')).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowCancelledError', message: 'Plan downgraded.' },
    });
    expect(world.ops()).toEqual(['create-db', 'create-vm', 'undo-create-vm', 'undo-create-db']);
  });

  @Workflow('cancel-while-parking')
  class CancelWhileParking {
    constructor(
      @Inject(World) private readonly world: World,
      @Inject(Control) private readonly control: Control,
    ) {}

    async run(ctx: WorkflowContext) {
      await ctx.step('create-db', () => this.world.record('create-db', ''), { compensate: () => this.world.record('undo-create-db', '') });

      try {
        await Promise.all([
          ctx.step('create-vm', async () => {
            this.control.started.resolve();
            await this.control.release.promise;
            this.world.record('create-vm', '');
          }),
          ctx.step('create-dns', async () => {
            await this.control.release.promise;
            throw new Error('DNS provider is down.'); // parks the instance on its retry backoff
          }),
        ]);
      } catch (error) {
        this.control.caught.push(error);
      }

      await ctx.step('notify', () => this.world.record('notify', ''));
    }
  }

  it('compensates at once when a cancel stops a run that was also parking, instead of parking it first', async () => {
    const node = await start([CancelWhileParking]);
    await node.client.start(CancelWhileParking, undefined, { id: 'cp-1' });
    const running = node.worker.drain();
    await control.started.promise;
    expect((await node.client.cancel('cp-1', 'Plan downgraded.')).accepted).toBe(true);
    control.release.resolve();
    await running;

    // One execution: the swallowed retry suspension did not win over the cancel.
    expect(await node.client.getStatus('cp-1', { journal: true })).toMatchObject({
      status: 'cancelled',
      runs: 1,
      error: { name: 'WorkflowCancelledError', message: 'Plan downgraded.' },
      journal: [
        { name: 'create-db', status: 'completed' },
        { name: 'create-vm', status: 'completed' },
        { name: 'create-dns', status: 'cancelled' },
        { name: '$compensate:create-db', status: 'completed' },
      ],
    });
    expect(world.ops()).toEqual(['create-db', 'create-vm', 'undo-create-db']);
    expect(node.events.map((e) => e.type)).not.toContain('workflow-suspended');
  });

  @Workflow('swallow-shutdown')
  class SwallowShutdown {
    constructor(
      @Inject(World) private readonly world: World,
      @Inject(Control) private readonly control: Control,
    ) {}

    async run(ctx: WorkflowContext) {
      let vm: string | null = null;
      try {
        vm = await ctx.step('create-vm', async (s) => {
          this.world.record('create-vm', s.idempotencyKey, s.attempt);
          if (this.world.count('create-vm') === 1) {
            this.control.started.resolve();
            await new Promise((_, reject) => s.signal.addEventListener('abort', () => reject(s.signal.reason)));
          }
          return 'vm-1';
        });
      } catch (error) {
        this.control.caught.push(error);
      }

      return { vm };
    }
  }

  it('hands the instance back on shutdown instead of completing it without the step', async () => {
    const first = await start([SwallowShutdown]);
    await first.client.start(SwallowShutdown, undefined, { id: 'vm-1' });
    const running = first.worker.drain();
    await control.started.promise;
    await first.close(); // graceful: aborts the step's signal
    nodes.splice(nodes.indexOf(first), 1);
    await running;

    expect(control.caught.map((e: any) => e.reason)).toEqual(['shutdown']);

    const second = await start([SwallowShutdown]);
    const status = await second.client.getStatus('vm-1', { journal: true });
    expect(status).toMatchObject({ status: 'running', leaseOwner: expect.any(String), leaseUntil: null, wakeAt: clock.now() });
    expect(status!.journal).toMatchObject([{ name: 'create-vm', status: 'pending', attempts: 0 }]);

    await second.worker.drain();
    expect(await second.client.getStatus('vm-1')).toMatchObject({ status: 'completed', output: { vm: 'vm-1' } });
    expect(world.calls.map((c) => c.attempt)).toEqual([1, 1]); // the shutdown gave the attempt back
  });
});

describe('Promise.all over ctx calls', () => {
  @Workflow('fan-out')
  class FanOut {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext, input: { flaky: string | null; fatal: string | null }) {
      const call = (name: string) =>
        ctx.step(name, (s) => {
          this.world.record(name, s.idempotencyKey, s.attempt);
          if (input.fatal === name) {
            throw new NonRetryableStepError(`${name} rejected`);
          }
          if (input.flaky === name && s.attempt === 1) {
            throw new Error(`${name} timed out`);
          }
          return name.toUpperCase();
        }, { compensate: () => this.world.record(`undo-${name}`, '') });

      return Promise.all([call('a'), call('b'), call('c')]);
    }
  }

  it('keeps the steps that completed next to one parked on its retry, and never runs them again', async () => {
    const node = await start([FanOut]);
    await node.client.start(FanOut, { flaky: 'b', fatal: null }, { id: 'f-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('f-1', { journal: true })).toMatchObject({
      status: 'suspended',
      wakeAt: clock.now() + 1_000,
      journal: [
        { name: 'a', status: 'completed' },
        { name: 'b', status: 'pending', attempts: 1 },
        { name: 'c', status: 'completed' },
      ],
    });

    clock.advance('1s');
    await node.worker.drain();
    expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'completed', output: ['A', 'B', 'C'] });
    expect(world.calls.map((c) => `${c.op}#${c.attempt}`)).toEqual(['a#1', 'b#1', 'c#1', 'b#2']);
  });

  it('lets in-flight steps finish when one fails, and compensates them too', async () => {
    const node = await start([FanOut]);
    await node.client.start(FanOut, { flaky: null, fatal: 'a' }, { id: 'f-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'failed', error: { name: 'StepFailedError' } });
    // Compensations run in reverse call order: c, then b. "a" never completed.
    expect(world.ops()).toEqual(['a', 'b', 'c', 'undo-c', 'undo-b']);
  });

  @Workflow('step-and-sleep')
  class StepAndSleep {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      const [label] = await Promise.all([
        ctx.step('print-label', (s) => {
          this.world.record('label', s.idempotencyKey);
          return 'L-1';
        }),
        ctx.sleep('cool-off', '1h'),
      ]);

      return label;
    }
  }

  it('starts no step in an execution that is parking: a step next to a sleep runs once, after it', async () => {
    const node = await start([StepAndSleep]);
    await node.client.start(StepAndSleep, undefined, { id: 'ss-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('ss-1')).toMatchObject({ status: 'suspended' });
    expect(world.ops()).toEqual([]);

    clock.advance('1h');
    await node.worker.drain();
    expect(await node.client.getStatus('ss-1')).toMatchObject({ status: 'completed', output: 'L-1' });
    expect(world.ops()).toEqual(['label']);
  });

  @Workflow('parallel-bookings')
  class ParallelBookings {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext, input: { pause: boolean }) {
      const book = (name: string, ms: number) =>
        ctx.step(
          `book-${name}`,
          async () => {
            await sleep(ms); // the flight answers after the hotel
            this.world.record(`book-${name}`, '');
          },
          { compensate: () => this.world.record(`cancel-${name}`, '') },
        );

      await Promise.all([book('flight', 30), book('hotel', 0)]);
      if (input.pause) {
        await ctx.sleep('pause', '1h');
      }

      await ctx.step('charge', () => {
        throw new NonRetryableStepError('card declined');
      });
    }
  }

  it('compensates parallel steps in the same order with or without a restart in between', async () => {
    const node = await start([ParallelBookings]);
    await node.client.start(ParallelBookings, { pause: false }, { id: 'no-pause' });
    await node.worker.drain();
    const withoutRestart = world.ops().filter((op) => op.startsWith('cancel-'));

    world.calls.length = 0;
    await node.client.start(ParallelBookings, { pause: true }, { id: 'pause' });
    await node.worker.drain();
    clock.advance('1h');
    await node.worker.drain(); // a new execution: the bookings are replayed from the journal
    const afterReplay = world.ops().filter((op) => op.startsWith('cancel-'));

    expect(withoutRestart).toEqual(['cancel-hotel', 'cancel-flight']);
    expect(afterReplay).toEqual(withoutRestart);
  });
});
