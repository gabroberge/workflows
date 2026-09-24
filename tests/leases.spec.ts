import { setTimeout as sleep } from 'node:timers/promises';
import { Inject, Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import {
  ManualWorkflowClock,
  Workflow,
  WorkflowClient,
  WorkflowsModule,
  WorkflowStorage,
  WorkflowWorker,
  type WorkflowClaimRequest,
  type WorkflowContext,
  type WorkflowWrite,
} from '../lib/index.js';
import { AppWorkflowStore, boot, databaseModule, deferred, forever, openStore, tempDb, type TestDb, waitFor, World, type Node } from './support.js';

@Injectable()
class Control {
  release = deferred();
  started = deferred();
  crashAtChunk: number | null = null;
  stepMs = 0;
}

let db: TestDb;
let world: World;
let control: Control;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
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

@Workflow('slow')
class Slow {
  constructor(
    @Inject(World) private readonly world: World,
    @Inject(Control) private readonly control: Control,
  ) {}

  async run(ctx: WorkflowContext) {
    return ctx.step('slow-step', async (s) => {
      this.world.record('slow', s.idempotencyKey, s.attempt);
      this.control.started.resolve();
      if (this.control.stepMs) {
        await sleep(this.control.stepMs);
      } else if (s.attempt === 1) {
        await this.control.release.promise;
      }
      return `attempt-${s.attempt}`;
    });
  }
}

describe('leases', () => {
  it('two workers never run the same step while the lease is kept alive', async () => {
    // Real clock. The step takes 1s; the lease is 400ms, so only the
    // heartbeat (every 50ms) keeps the second worker away.
    control.stepMs = 1_000;
    const worker = { leaseDuration: 400, heartbeatInterval: 50 };
    const a = await start([Slow], { worker });
    const b = await start([Slow], { worker });
    await a.client.start(Slow, undefined, { id: 'slow-1' });

    const running = a.worker.drain();
    await control.started.promise;

    let stolen = 0;
    const until = Date.now() + 1_200;
    while (Date.now() < until) {
      stolen += await b.worker.drain();
      await sleep(20);
    }
    await running;

    expect(stolen).toBe(0);
    expect(world.count('slow')).toBe(1);
    expect(await a.client.getStatus('slow-1')).toMatchObject({ status: 'completed', output: 'attempt-1', runs: 1 });
  });

  it('fences off a worker that lost its lease: its late result is discarded', async () => {
    // Worker A stalls (think: a long GC pause or a frozen VM) past its lease.
    const clock = new ManualWorkflowClock();
    const a = await start([Slow], { clock });
    const b = await start([Slow], { clock });
    await a.client.start(Slow, undefined, { id: 'slow-1' });

    const running = a.worker.drain();
    await control.started.promise;
    clock.advance('31s');
    expect(await b.worker.drain()).toBe(1); // B takes over and finishes attempt 2
    control.release.resolve(); // A wakes up and tries to record attempt 1
    await running;

    // At-least-once: the side effect ran twice, with the same idempotency key.
    expect(world.calls).toEqual([
      { op: 'slow', key: 'slow-1:slow-step', attempt: 1 },
      { op: 'slow', key: 'slow-1:slow-step', attempt: 2 },
    ]);

    // Exactly one result was recorded, and A emitted nothing after losing the lease.
    expect(await b.client.getStatus('slow-1')).toMatchObject({ status: 'completed', output: 'attempt-2' });
    expect(a.events.map((e) => e.type)).toEqual(['workflow-started']);
  });

  it('runs many instances across two polling workers, each step exactly once', async () => {
    @Workflow('two-steps')
    class TwoSteps {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext, input: { n: number }) {
        await ctx.step('first', (s) => this.world.record('first', s.idempotencyKey));
        await ctx.step('second', (s) => this.world.record('second', s.idempotencyKey));
        return input.n;
      }
    }

    const worker = { enabled: true, pollInterval: 20, concurrency: 4 };
    const a = await start([TwoSteps], { worker: { ...worker, id: 'a' } });
    await start([TwoSteps], { worker: { ...worker, id: 'b' } });
    for (let n = 0; n < 20; n++) {
      await a.client.start(TwoSteps, { n }, { id: `two-${n}` });
    }

    await waitFor(async () => (await a.client.list({ status: 'completed' })).length === 20);
    expect(world.count('first')).toBe(20);
    expect(world.count('second')).toBe(20);
    expect(new Set(world.calls.map((c) => c.key)).size).toBe(40);
  });

  it('hands back what a claim in flight at shutdown claimed, before the database connection closes', async () => {
    const calls: string[] = [];
    const claimed = deferred();
    /** A store provider whose claim takes a while (a network round trip), with every call logged. */
    class SlowClaims extends AppWorkflowStore {
      override async claim(request: WorkflowClaimRequest) {
        calls.push('claim');
        await claimed.promise;
        return super.claim(request);
      }
      override async get(id: string, options?: { journal?: boolean }) {
        calls.push(options?.journal ? 'journal' : 'get');
        return super.get(id, options);
      }
      override async write(id: string, token: string, write: WorkflowWrite) {
        calls.push(write.release?.wakeAt === clock.now() && write.status === undefined ? 'release' : 'write');
        return super.write(id, token, write);
      }
    }

    const clock = new ManualWorkflowClock();
    const moduleRef = await Test.createTestingModule({
      // The database module closes its connection in its onApplicationShutdown() ('close').
      imports: [WorkflowsModule.forRoot({ clock, worker: false }), databaseModule(db, calls)],
      providers: [Slow, SlowClaims, { provide: World, useValue: world }, { provide: Control, useValue: control }],
    }).compile();
    await moduleRef.init();
    expect(moduleRef.get(WorkflowStorage).source).toBeInstanceOf(SlowClaims);
    await moduleRef.get(WorkflowClient).start(Slow, undefined, { id: 'slow-1' });

    const draining = moduleRef.get(WorkflowWorker).drain();
    await waitFor(() => calls.includes('claim'));
    const closing = moduleRef.close(); // SIGTERM while the claim is on the wire
    await sleep(20);
    claimed.resolve();
    await Promise.all([closing, draining]);

    expect(calls).toEqual(['claim', 'journal', 'release', 'close']);

    const { store, close } = openStore(db);
    expect(await store.get('slow-1')).toMatchObject({ status: 'running', leaseUntil: null, wakeAt: clock.now() });
    await close();
    expect(world.count('slow')).toBe(0);
  });
});

describe('cancel while running', () => {
  @Workflow('provision')
  class Provision {
    constructor(
      @Inject(World) private readonly world: World,
      @Inject(Control) private readonly control: Control,
    ) {}

    async run(ctx: WorkflowContext) {
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
      await ctx.step('create-dns', () => this.world.record('create-dns', ''), undo('create-dns'));
    }
  }

  it('notices a cancel at the next step boundary (via the heartbeat) and compensates', async () => {
    const node = await start([Provision], { clock: new ManualWorkflowClock(), worker: { heartbeatInterval: 20 } });
    await node.client.start(Provision, undefined, { id: 'p-1' });
    const running = node.worker.drain();
    await control.started.promise;

    await node.client.cancel('p-1', 'Plan downgraded.');
    await sleep(60); // a heartbeat or two
    control.release.resolve(); // the in-flight step finishes and is kept
    await running;

    expect(await node.client.getStatus('p-1')).toMatchObject({
      status: 'cancelled',
      error: { message: 'Plan downgraded.' },
      runs: 1,
    });
    expect(world.ops()).toEqual(['create-db', 'create-vm', 'undo-create-vm', 'undo-create-db']);
  });

  it('notices a cancel sent from this process at once, without waiting for a heartbeat', async () => {
    const node = await start([Provision], { clock: new ManualWorkflowClock() }); // heartbeats every 10s
    await node.client.start(Provision, undefined, { id: 'p-1' });
    const running = node.worker.drain();
    await control.started.promise;

    await node.client.cancel('p-1', 'Plan downgraded.');
    control.release.resolve();
    await running;

    expect(await node.client.getStatus('p-1')).toMatchObject({ status: 'cancelled', runs: 1 });
    expect(world.ops()).toEqual(['create-db', 'create-vm', 'undo-create-vm', 'undo-create-db']);
  });
});

describe('long-running steps', () => {
  @Workflow('import')
  class Import {
    constructor(
      @Inject(World) private readonly world: World,
      @Inject(Control) private readonly control: Control,
    ) {}

    async run(ctx: WorkflowContext, input: { chunks: number }) {
      return ctx.step('import-rows', async (s) => {
        // Resume from the last checkpoint instead of starting over.
        for (let chunk = (s.progress as number | undefined) ?? 0; chunk < input.chunks; chunk++) {
          if (this.control.crashAtChunk === chunk) {
            this.control.started.resolve();
            await forever();
          }
          this.world.record(`chunk-${chunk}`, s.idempotencyKey, s.attempt);
          await s.heartbeat(chunk + 1); // checkpoint + proof of life
        }
        return { imported: input.chunks };
      });
    }
  }

  it('checkpoints progress with heartbeat(progress) and resumes from it after a crash', async () => {
    const clock = new ManualWorkflowClock();
    control.crashAtChunk = 6;
    const first = await start([Import], { clock });
    await first.client.start(Import, { chunks: 10 }, { id: 'import-1' });
    void first.worker.drain();
    await control.started.promise;
    await first.close();
    nodes.splice(0);

    control.crashAtChunk = null;
    const second = await start([Import], { clock });
    const running = await second.client.getStatus('import-1', { journal: true });
    expect(running!.journal![0]).toMatchObject({ name: 'import-rows', status: 'pending', attempts: 1, progress: 6 });

    clock.advance('31s');
    await second.worker.drain();
    expect(await second.client.getStatus('import-1')).toMatchObject({ status: 'completed', output: { imported: 10 } });
    expect(world.ops()).toEqual(Array.from({ length: 10 }, (_, i) => `chunk-${i}`));
    expect(world.calls.map((c) => c.attempt)).toEqual([1, 1, 1, 1, 1, 1, 2, 2, 2, 2]);
  });

  @Workflow('stuck')
  class Stuck {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      return ctx.step(
        'poll-partner',
        async (s) => {
          this.world.record('poll', s.idempotencyKey, s.attempt);
          if (s.attempt === 1) {
            // Hangs without heartbeats; the watchdog aborts it.
            await new Promise((_, reject) => s.signal.addEventListener('abort', () => reject(s.signal.reason)));
          }
          return 'answered';
        },
        { heartbeatTimeout: 50, retry: { backoff: { delay: 0 } } },
      );
    }
  }

  const zombie: unknown[] = [];

  @Workflow('zombie')
  class Zombie {
    constructor(@Inject(World) private readonly world: World) {}

    async run(ctx: WorkflowContext) {
      return ctx.step(
        'render',
        async (s) => {
          this.world.record('render', s.idempotencyKey, s.attempt);
          if (s.attempt === 1) {
            await sleep(80); // ignores its signal and outlives its timeout
            await s.heartbeat(99).catch((error) => zombie.push(error));
          }
          return 'rendered';
        },
        { timeout: 30, retry: { backoff: { delay: '1m' } } },
      );
    }
  }

  it('ignores heartbeats from an attempt that already timed out', async () => {
    const node = await start([Zombie], { clock: new ManualWorkflowClock() });
    await node.client.start(Zombie, undefined, { id: 'z-1' });
    await node.worker.drain(); // attempt 1 times out; the retry is parked for a minute
    const write = vi.spyOn(node.store, 'write');
    await sleep(100); // the timed-out attempt wakes up and checkpoints

    expect(write).not.toHaveBeenCalled();
    expect(zombie).toMatchObject([{ name: 'StepTimeoutError' }]);
    const { journal } = (await node.client.getStatus('z-1', { journal: true }))!;
    expect(journal).toMatchObject([{ name: 'render', status: 'pending', attempts: 1, error: { name: 'StepTimeoutError' } }]);
    expect(journal[0]!.progress).toBeUndefined();
  });

  it('fails and retries an attempt that stops heartbeating while the process is alive', async () => {
    const node = await start([Stuck], { clock: new ManualWorkflowClock() });
    await node.client.start(Stuck, undefined, { id: 'stuck-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('stuck-1')).toMatchObject({ status: 'completed', output: 'answered' });
    expect(node.events.find((e) => e.type === 'step-failed')).toMatchObject({
      attempt: 1,
      error: { name: 'StepTimeoutError', message: 'Step "poll-partner" sent no heartbeat for 50ms.' },
    });
  });
});
