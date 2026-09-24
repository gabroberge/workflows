import { subscribe, unsubscribe } from 'node:diagnostics_channel';
import { Inject, Injectable, Module, type Type } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import {
  ManualWorkflowClock,
  NonRetryableStepError,
  InMemoryWorkflowStore,
  StepFailedError,
  StepTimeoutError,
  Workflow,
  WORKFLOWS_MODULE_OPTIONS,
  WorkflowClient,
  WorkflowDefinitionError,
  WorkflowError,
  WorkflowFailedError,
  WorkflowIdConflictError,
  WorkflowInterrupt,
  WorkflowNonDeterminismError,
  WorkflowNotFoundError,
  WorkflowSignal,
  WorkflowsModule,
  WorkflowStorage,
  WorkflowWorker,
  type WorkflowContext,
  type WorkflowEvent,
  type WorkflowStepContext,
  type Journaled,
  type WorkflowInstanceDetails,
  type WorkflowJournalEntry,
  type WorkflowRunner,
  type WorkflowsModuleAsyncOptions,
  type WorkflowsOptionsFactory,
} from '../lib/index.js';
import { OrderFulfilment, orderProviders, shipmentDelivered, type Order } from './order-fulfilment.js';
import { AppWorkflowStore, boot, DATABASE, databaseModule, openStore, tempDb, type TestDb, World, type Node } from './support.js';

const order: Order = { orderId: 'o1', amount: 4200, email: 'ada@example.com' };

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

describe('module registration', () => {
  /** The app's database module and the store provider it writes on it (support.ts), with the module's shutdown recorded. */
  let shutdowns: string[];
  let DatabaseModule: Type<unknown>;
  beforeEach(() => {
    shutdowns = [];
    DatabaseModule = databaseModule(db, shutdowns);
  });

  @Workflow('ping')
  class Ping {
    async run(ctx: WorkflowContext) {
      return ctx.step('pong', () => 'pong');
    }
  }

  const run = async (moduleRef: TestingModule) => {
    await moduleRef.get(WorkflowClient).start(Ping, undefined, { id: 'p-1' });
    await moduleRef.get(WorkflowWorker).drain();
    return moduleRef.get(WorkflowClient).getStatus('p-1');
  };

  /** The instance as the test database has it, read outside any application. */
  const stored = async (id: string) => {
    const { store, close } = openStore(db);
    try {
      return await store.get(id);
    } finally {
      await close();
    }
  };

  it('runs on the in-memory store with no options and no store', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [WorkflowsModule.forRoot()], providers: [Ping] })
      .overrideProvider(WORKFLOWS_MODULE_OPTIONS)
      .useValue({ clock, worker: false })
      .compile();
    await moduleRef.init();

    expect(await run(moduleRef)).toMatchObject({ status: 'completed', output: 'pong' });
    expect(moduleRef.get(WorkflowStorage).source).toBeInstanceOf(InMemoryWorkflowStore);
    await moduleRef.close();
  });

  it('uses the store a provider registers, and leaves its lifecycle to Nest', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [WorkflowsModule.forRoot({ clock, worker: false }), DatabaseModule],
      providers: [Ping, AppWorkflowStore],
    }).compile();
    await moduleRef.init();

    expect(await run(moduleRef)).toMatchObject({ status: 'completed', output: 'pong' });
    const store = moduleRef.get(AppWorkflowStore);
    expect(moduleRef.get(WorkflowStorage).source).toBe(store);
    expect(await stored('p-1')).toMatchObject({ status: 'completed', output: 'pong' }); // in the injected database

    await moduleRef.close();
    expect(shutdowns).toEqual(['close']); // the database module's own onApplicationShutdown()
    expect(moduleRef.get(WORKFLOWS_MODULE_OPTIONS)).toEqual({ clock, worker: false });
  });

  it('is replaced in tests with overrideProvider(...).useValue(new InMemoryWorkflowStore())', async () => {
    // As an app module declares it: the store provider next to the module.
    @Module({ imports: [WorkflowsModule.forRoot(), DatabaseModule], providers: [Ping, AppWorkflowStore] })
    class AppModule {}

    const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(AppWorkflowStore)
      .useValue(new InMemoryWorkflowStore())
      .overrideProvider(WORKFLOWS_MODULE_OPTIONS)
      .useValue({ clock, worker: false })
      .compile();
    await moduleRef.init();

    expect(await run(moduleRef)).toMatchObject({ status: 'completed' });
    // A plain instance doesn't register: the default in-memory store runs, and the database is never written.
    expect(moduleRef.get(WorkflowStorage).source).toBeInstanceOf(InMemoryWorkflowStore);
    expect(moduleRef.get(WorkflowStorage).source).not.toBe(moduleRef.get(AppWorkflowStore));
    expect(await stored('p-1')).toBeNull();
    await moduleRef.close();
  });

  it('takes its options from forRootAsync(), with a factory or a useClass', async () => {
    @Injectable()
    class WorkflowsConfig implements WorkflowsOptionsFactory {
      createWorkflowsOptions() {
        return { clock, worker: false as const };
      }
    }

    const factory = await Test.createTestingModule({
      imports: [WorkflowsModule.forRootAsync({ imports: [DatabaseModule], inject: [DATABASE], useFactory: () => ({ clock, worker: false as const }) }), DatabaseModule],
      providers: [Ping, AppWorkflowStore],
    }).compile();
    await factory.init();
    expect(await run(factory)).toMatchObject({ status: 'completed' });
    expect(factory.get(WorkflowStorage).source).toBeInstanceOf(AppWorkflowStore);
    await factory.close();

    const options: WorkflowsModuleAsyncOptions = { useClass: WorkflowsConfig };
    const useClass = await Test.createTestingModule({ imports: [WorkflowsModule.forRootAsync(options)], providers: [Ping] }).compile();
    await useClass.init();
    expect(await run(useClass)).toMatchObject({ status: 'completed' });
    await useClass.close();
  });

  it('rejects worker settings that would never run anything, or poll in a busy loop', async () => {
    const compile = (worker: object) => Test.createTestingModule({ imports: [WorkflowsModule.forRoot({ worker })] }).compile();
    await expect(compile({ concurrency: 0 })).rejects.toThrow('worker.concurrency (0) must be a positive integer.');
    await expect(compile({ pollInterval: 0 })).rejects.toThrow('worker.pollInterval and worker.heartbeatInterval must be positive');
    await expect(compile({ heartbeatInterval: 0 })).rejects.toThrow('worker.pollInterval and worker.heartbeatInterval must be positive');
  });
});

describe('WorkflowClient.list()', () => {
  it('validates the page and returns nothing for an empty status filter', async () => {
    const node = await start([]);
    await node.store.create({ id: 'a', workflow: 'w', version: 1, input: null, now: 0 });

    expect(await node.client.list({ status: [] })).toEqual([]);
    expect(await node.client.list({ status: 'pending' })).toMatchObject([{ id: 'a' }]);
    expect(await node.client.list({ limit: 0 })).toEqual([]);
    await expect(node.client.list({ limit: 1.5 })).rejects.toThrow('must be non-negative integers');
    await expect(node.client.list({ offset: -1 })).rejects.toThrow('must be non-negative integers');
  });
});

describe('retry options', () => {
  const log: Array<{ attempt: number; at: number }> = [];
  const failures = { until: Infinity, error: () => new Error('503') };

  const flaky = (retry: unknown) => {
    @Workflow(`flaky-${Math.random().toString(36).slice(2)}`)
    class Flaky {
      async run(ctx: WorkflowContext) {
        return ctx.step(
          'call',
          (s) => {
            log.push({ attempt: s.attempt, at: clock.now() });
            if (s.attempt < failures.until) {
              throw failures.error();
            }
            return 'ok';
          },
          { retry: retry as any },
        );
      }
    }

    return Flaky;
  };

  beforeEach(() => {
    log.length = 0;
    failures.until = Infinity;
    failures.error = () => new Error('503');
  });

  const runToEnd = async (node: Node, id: string) => {
    for (let i = 0; i < 20; i++) {
      await node.worker.drain();
      const status = await node.client.getStatus(id);
      if (!['pending', 'running', 'suspended'].includes(status!.status)) {
        return status!;
      }
      clock.set(status!.wakeAt!);
    }

    throw new Error('did not finish');
  };

  it('takes a number of attempts, or false for a single attempt', async () => {
    const TwoAttempts = flaky(2);
    const OneAttempt = flaky(false);
    const node = await start([TwoAttempts, OneAttempt]);
    await node.client.start(TwoAttempts, undefined, { id: 'two' });
    await node.client.start(OneAttempt, undefined, { id: 'one' });

    expect(await runToEnd(node, 'two')).toMatchObject({ status: 'failed', error: { message: expect.stringContaining('after 2 attempt(s)') } });
    expect(await runToEnd(node, 'one')).toMatchObject({ status: 'failed', error: { message: expect.stringContaining('after 1 attempt(s)') } });
  });

  it('merges step backoff over the module default field by field', async () => {
    const Constant = flaky({ attempts: 4, backoff: { factor: 1 } });
    const node = await start([Constant], { retry: { backoff: { delay: '10s', factor: 3 } } });
    const t0 = clock.now();
    await node.client.start(Constant, undefined, { id: 'c' });
    await runToEnd(node, 'c');

    // The module's 10s delay, the step's factor of 1.
    expect(log.map((entry) => entry.at - t0)).toEqual([0, 10_000, 20_000, 30_000]);
  });

  it('calls a backoff function and retryIf with the attempt that just failed', async () => {
    const seen: Array<[string, number]> = [];
    failures.until = 3;
    failures.error = () => new Error(`503 on attempt ${log.length}`);
    const Custom = flaky({
      attempts: 5,
      backoff: (attempt: number, error: Error) => {
        seen.push([error.message, attempt]);
        return `${attempt}m`;
      },
      retryIf: (_error: unknown, attempt: number) => attempt < 5,
    });

    const node = await start([Custom]);
    const t0 = clock.now();
    await node.client.start(Custom, undefined, { id: 'f' });

    expect(await runToEnd(node, 'f')).toMatchObject({ status: 'completed', output: 'ok' });
    expect(seen).toEqual([
      ['503 on attempt 1', 1],
      ['503 on attempt 2', 2],
    ]);
    expect(log.map((entry) => entry.at - t0)).toEqual([0, 60_000, 180_000]);
  });

  it('stops when retryIf says so, or at a NonRetryableStepError', async () => {
    const Picky = flaky({ attempts: 5, retryIf: (error: Error) => !error.message.startsWith('4') });
    failures.error = () => new Error('404 Not Found');
    const node = await start([Picky]);
    await node.client.start(Picky, undefined, { id: 'p' });
    expect(await runToEnd(node, 'p')).toMatchObject({ status: 'failed' });
    expect(log).toHaveLength(1);

    log.length = 0;
    failures.error = () => new NonRetryableStepError('card declined');
    const Declined = flaky(10);
    const other = await start([Declined]);
    await other.client.start(Declined, undefined, { id: 'd' });
    expect(await runToEnd(other, 'd')).toMatchObject({ status: 'failed' });
    expect(log).toHaveLength(1);
  });

  it('randomizes waits with full or equal jitter, within the capped delay', async () => {
    const random = vi.spyOn(Math, 'random');
    const Full = flaky({ attempts: 2, backoff: { delay: '10s', jitter: 'full' } });
    const Equal = flaky({ attempts: 2, backoff: { delay: '10s', jitter: 'equal' } });
    const node = await start([Full, Equal]);
    random.mockReturnValue(0.25);
    const t0 = clock.now();
    await node.client.start(Full, undefined, { id: 'full' });
    await node.client.start(Equal, undefined, { id: 'equal' });
    await node.worker.drain();
    random.mockRestore();

    expect(await node.client.getStatus('full')).toMatchObject({ wakeAt: t0 + 2_500 }); // 0.25 × 10s
    expect(await node.client.getStatus('equal')).toMatchObject({ wakeAt: t0 + 6_250 }); // 5s + 0.25 × 5s
  });

  it('gives up, journaled, when retryIf or a backoff function throws', async () => {
    const Buggy = flaky({
      attempts: 5,
      retryIf: () => {
        throw new TypeError('Cannot read properties of undefined');
      },
    });

    const node = await start([Buggy]);
    await node.client.start(Buggy, undefined, { id: 'b' });

    const status = await runToEnd(node, 'b');
    expect(status).toMatchObject({
      status: 'failed',
      error: {
        name: 'StepFailedError',
        message:
          'Step "call" failed after 1 attempt(s): TypeError: The retry options of "call" threw TypeError: ' +
          'Cannot read properties of undefined (handling Error: 503)',
      },
    });

    // Journaled as failed, so a workflow that catches StepFailedError takes the same branch on every replay.
    expect((await node.client.getStatus('b', { journal: true }))!.journal).toMatchObject([{ name: 'call', status: 'failed', attempts: 1 }]);
    expect(log).toHaveLength(1);
  });

  it('rejects invalid step options before spending an attempt on them', async () => {
    const calls: string[] = [];
    @Workflow('bad-options')
    class BadOptions {
      async run(ctx: WorkflowContext) {
        await ctx.step('call', () => calls.push('call'), { timeout: 'soon' as '1s' });
      }
    }

    const node = await start([BadOptions]);
    await node.client.start(BadOptions, undefined, { id: 'o' });
    await node.worker.drain();

    const status = await node.client.getStatus('o', { journal: true });
    expect(status).toMatchObject({
      status: 'failed',
      error: { name: 'TypeError', message: 'Invalid options for "call": Invalid duration "soon". Use milliseconds or a string such as "15m" or "3d".' },
      journal: [],
    });
    expect(calls).toEqual([]);
  });

  it('ends as compensation_failed, instead of looping, when a compensation has invalid options', async () => {
    @Workflow('bad-compensation')
    class BadCompensation {
      async run(ctx: WorkflowContext) {
        await ctx.step('book', () => 'b-1', { compensate: () => undefined, compensateRetry: { attempts: -1 } });
        await ctx.step('charge', () => {
          throw new NonRetryableStepError('card declined');
        });
      }
    }

    const node = await start([BadCompensation]);
    await node.client.start(BadCompensation, undefined, { id: 'bc' });
    await node.worker.drain();

    expect(await node.client.getStatus('bc')).toMatchObject({
      status: 'compensation_failed',
      leaseUntil: null,
      error: { name: 'StepFailedError', compensation: { name: 'TypeError', message: expect.stringContaining('Invalid retry attempts -1') } },
    });
  });

  it('rejects an invalid module default at startup', async () => {
    await expect(start([], { retry: { attempts: -1 } })).rejects.toThrow('Invalid retry attempts -1.');
    await expect(start([], { retry: { backoff: { jitter: 'some' as 'full' } } })).rejects.toThrow('Invalid backoff jitter "some"');
  });

  it('ignores what a compensation returns: an unserializable value is not a failed undo', async () => {
    @Workflow('refund-returns-bigint')
    class RefundReturnsBigint {
      async run(ctx: WorkflowContext) {
        // A refund helper that returns the provider's response object, with a BigInt in it.
        await ctx.step('charge', () => 'ch-1', { compensate: () => ({ refunded: 4200n }) as unknown });
        ctx.fail('not delivered');
      }
    }

    const node = await start([RefundReturnsBigint]);
    await node.client.start(RefundReturnsBigint, undefined, { id: 'rb' });
    await node.worker.drain();

    const status = await node.client.getStatus('rb', { journal: true });
    expect(status).toMatchObject({ status: 'failed', error: { name: 'WorkflowFailedError', message: 'not delivered' } });
    expect(status!.journal).toMatchObject([{ name: 'charge', status: 'completed' }, { name: '$compensate:charge', status: 'completed' }]);
    expect(status!.journal[1]!.result).toBeUndefined();
  });
});

describe('signals', () => {
  it('match keys exactly: a wait without a key never takes a keyed signal', async () => {
    @Workflow('keys')
    class Keys {
      async run(ctx: WorkflowContext) {
        const any = await ctx.waitForSignal('unkeyed', shipmentDelivered, { timeout: '1h' });
        const mine = await ctx.waitForSignal('keyed', shipmentDelivered, { key: 'o1', timeout: '1h' });
        return { any, mine };
      }
    }

    const node = await start([Keys]);
    await node.client.start(Keys, undefined, { id: 'k-1' });
    await node.worker.drain();

    // Another order's delivery, and this order's own: neither is for the unkeyed wait.
    expect(await node.client.signal(shipmentDelivered, { orderId: 'o2', trackingId: 'T2' }, { key: 'o2' })).toMatchObject({ woken: 0 });
    await node.client.signal(shipmentDelivered, { orderId: 'o1', trackingId: 'T1' }, { key: 'o1' });
    expect(await node.worker.drain()).toBe(0);

    // A signal sent without a key reaches the unkeyed wait; the keyed one then takes o1's.
    expect(await node.client.signal(shipmentDelivered, { orderId: '*', trackingId: 'T*' })).toMatchObject({ woken: 1 });
    await node.worker.drain();
    expect(await node.client.getStatus('k-1')).toMatchObject({
      status: 'completed',
      output: { any: { orderId: '*' }, mine: { orderId: 'o1', trackingId: 'T1' } },
    });
  });

  it('share one typed definition between the sender and the wait', async () => {
    const node = await start([OrderFulfilment], { providers: orderProviders(world) });
    await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
    await node.worker.drain();
    await node.client.signal(shipmentDelivered, { orderId: 'o1', trackingId: 'T1' }, { key: 'o1' });
    await node.worker.drain();

    expect(await node.client.getStatus('order-o1', { journal: true })).toMatchObject({
      status: 'suspended',
      journal: expect.arrayContaining([
        expect.objectContaining({ name: 'await-delivery', kind: 'signal', data: { signal: 'shipment.delivered', key: 'o1' } }),
      ]),
    });

    // @ts-expect-error: the payload must be a ShipmentDelivered
    await expect(node.client.signal(shipmentDelivered, { orderId: 'o1' }, { key: 'o1' })).resolves.toBeDefined();
  });

  it('rejects an empty signal name', () => {
    expect(() => new WorkflowSignal('')).toThrow('Invalid signal name ""');
  });
});

describe('input validation', () => {
  it('fails a sleep with an invalid deadline instead of parking the instance forever', async () => {
    @Workflow('bad-deadline')
    class BadDeadline {
      async run(ctx: WorkflowContext, input: { until: string }) {
        await ctx.sleep('until-launch', { until: new Date(input.until) });
      }
    }

    const node = await start([BadDeadline]);
    await node.client.start(BadDeadline, { until: 'next tuesday' }, { id: 'd' });
    await node.worker.drain();

    expect(await node.client.getStatus('d')).toMatchObject({
      status: 'failed',
      error: { name: 'TypeError', message: expect.stringContaining('Invalid deadline for sleep "until-launch": Invalid Date') },
    });
  });

  it('rejects an empty instance id', async () => {
    const node = await start([]);
    await expect(node.client.start('anything', {}, { id: '', version: 1 })).rejects.toThrow(
      'Invalid workflow instance id "". Use a non-empty string, such as `order-${orderId}`.',
    );
  });

  it('rejects a pinned version no worker could ever claim, instead of creating an instance that stays pending', async () => {
    const node = await start([OrderFulfilment], { providers: orderProviders(world) });
    for (const version of [0, 1.5, -1, Number.NaN]) {
      await expect(node.client.start(OrderFulfilment, order, { id: 'v', version })).rejects.toThrow(
        `Invalid version ${version} for workflow "order-fulfilment". Use a positive integer.`,
      );
    }

    expect(await node.client.list()).toEqual([]);
  });
});

describe('determinism guards', () => {
  it('fails the instance when a step calls ctx, instead of recording what a replay would miss', async () => {
    @Workflow('nested')
    class Nested {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('outer', async (s) => {
          this.world.record('outer', s.idempotencyKey, s.attempt);
          return ctx.step('inner', () => 'never');
        });
      }
    }

    const node = await start([Nested]);
    await node.client.start(Nested, undefined, { id: 'n-1' });
    await node.worker.drain();

    const status = await node.client.getStatus('n-1', { journal: true });
    expect(status).toMatchObject({ status: 'failed', error: { name: 'WorkflowDefinitionError' } });
    expect(status!.error!.message).toBe(
      'Instance "n-1" of workflow "nested@1": step "outer" called ctx.step("inner"). A step\'s function runs once and ' +
        "replays return its result, so it can't use ctx. Call ctx methods from run() and pass the values the step needs into it.",
    );
    expect(world.calls).toHaveLength(1); // not retried: the same code would fail again
    expect(status!.journal.map((entry) => entry.name)).toEqual(['outer']);
    expect(new WorkflowDefinitionError('x')).toBeInstanceOf(Error);
  });

  it('ends as compensation_failed when a compensation calls ctx, instead of retrying it forever', async () => {
    @Workflow('nested-undo')
    class NestedUndo {
      async run(ctx: WorkflowContext) {
        await ctx.step('charge', () => 'ch_1', { compensate: () => ctx.step('refund', () => 're_1') });
        ctx.fail('Out of stock.');
      }
    }

    const node = await start([NestedUndo]);
    await node.client.start(NestedUndo, undefined, { id: 'u-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('u-1')).toMatchObject({
      status: 'compensation_failed',
      error: {
        name: 'WorkflowFailedError',
        message: 'Out of stock.',
        compensation: { name: 'WorkflowDefinitionError', message: expect.stringContaining('the compensation of step "charge" called ctx.step("refund")') },
      },
    });
    expect(await node.worker.drain()).toBe(0);
  });

  it('points ctx.fail() inside a step to NonRetryableStepError', async () => {
    @Workflow('fail-inside')
    class FailInside {
      async run(ctx: WorkflowContext) {
        await ctx.step('charge', () => ctx.fail('declined'));
      }
    }

    const node = await start([FailInside]);
    await node.client.start(FailInside, undefined, { id: 'f-1' });
    await node.worker.drain();

    expect((await node.client.getStatus('f-1'))!.error!.message).toContain(
      'step "charge" called ctx.fail(). A step\'s function runs once and replays return its result, so it can\'t use ctx. Throw a NonRetryableStepError from the step instead.',
    );
  });
});

describe('observability', () => {
  it('publishes each lifecycle event on its own diagnostics channel', async () => {
    const received: Array<[string, WorkflowEvent]> = [];
    const listeners = ['workflow-started', 'step-completed', 'signal-received', 'workflow-completed'].map((type) => {
      const name = `nestjs:workflows:${type}`;
      const listener = (message: unknown) => received.push([name, message as WorkflowEvent]);
      subscribe(name, listener);
      return () => unsubscribe(name, listener);
    });

    try {
      const node = await start([OrderFulfilment], { providers: orderProviders(world) });
      await node.client.start(OrderFulfilment, order, { id: 'order-o1' });
      await node.client.signal(shipmentDelivered, { orderId: 'o1', trackingId: 'T1' }, { key: 'o1' });
      await node.worker.drain();
      clock.advance('7d');
      await node.worker.drain();
    } finally {
      for (const stop of listeners) {
        stop();
      }
    }

    expect(received.map(([name]) => name)).toEqual([
      'nestjs:workflows:workflow-started',
      'nestjs:workflows:step-completed',
      'nestjs:workflows:step-completed',
      'nestjs:workflows:signal-received',
      'nestjs:workflows:step-completed',
      'nestjs:workflows:workflow-completed',
    ]);
    expect(received[3][1]).toMatchObject({
      type: 'signal-received',
      id: 'order-o1',
      workflow: 'order-fulfilment',
      version: 1,
      wait: 'await-delivery',
      signal: 'shipment.delivered',
    });
    expect(received[5][1]).toMatchObject({ type: 'workflow-completed', output: { chargeId: 'ch_o1', trackingId: 'T1' } });
  });

  it('lists the unfinished instances of one version, to know when it can be removed', async () => {
    @Workflow('billing')
    class BillingV1 {
      async run(ctx: WorkflowContext) {
        await ctx.sleep('grace', '1d');
      }
    }

    @Workflow('billing', { version: 2 })
    class BillingV2 {
      async run(ctx: WorkflowContext) {
        await ctx.sleep('grace', '1d');
      }
    }

    const node = await start([BillingV1, BillingV2]);
    await node.client.start('billing', undefined, { id: 'old', version: 1 });
    await node.client.start('billing', undefined, { id: 'new' });
    await node.worker.drain();

    const unfinished = ['pending', 'running', 'suspended', 'compensating'] as const;
    expect(await node.client.list({ workflow: 'billing', version: 1, status: [...unfinished] })).toMatchObject([{ id: 'old' }]);

    clock.advance('1d');
    await node.worker.drain();
    expect(await node.client.list({ workflow: 'billing', version: 1, status: [...unfinished] })).toEqual([]);
  });
});

describe('errors', () => {
  it('share a WorkflowError base, except the control-flow interrupt and the error apps throw', () => {
    const caught = [
      new StepFailedError('charge', 1, { name: 'Error', message: '503' }),
      new StepTimeoutError('slow'),
      new WorkflowFailedError('not delivered'),
      new WorkflowDefinitionError('twice'),
      new WorkflowNonDeterminismError('renamed'),
      new WorkflowNotFoundError('unknown'),
      new WorkflowIdConflictError('taken'),
    ];

    for (const error of caught) {
      expect(error).toBeInstanceOf(WorkflowError);
    }
    expect(caught.map((error) => error.name)).toEqual([
      'StepFailedError',
      'StepTimeoutError',
      'WorkflowFailedError',
      'WorkflowDefinitionError',
      'WorkflowNonDeterminismError',
      'WorkflowNotFoundError',
      'WorkflowIdConflictError',
    ]);

    expect(new WorkflowInterrupt('suspend')).not.toBeInstanceOf(WorkflowError);
    expect(new NonRetryableStepError('declined')).not.toBeInstanceOf(WorkflowError);
  });

  it('marks the caller mistakes with a 4xx status, so other packages classify them without importing them', async () => {
    const node = await start([]);
    await expect(node.client.cancel('nope')).rejects.toMatchObject({ name: 'WorkflowNotFoundError', status: 404 });
    await expect(node.client.start('unregistered', {})).rejects.toMatchObject({ name: 'WorkflowNotFoundError', status: 404 });

    await node.client.start('billing', { month: 9 }, { id: 'b-1', version: 1 });
    await expect(node.client.start('billing', { month: 10 }, { id: 'b-1', version: 1 })).rejects.toMatchObject({
      name: 'WorkflowIdConflictError',
      status: 409,
    });

    expect(new StepFailedError('charge', 1, { name: 'Error', message: '503' })).not.toHaveProperty('status');
  });

  it('records a thrown value it cannot serialize as JSON, instead of leaving the instance running', async () => {
    const circular = (label: string) => {
      const self: Record<string, unknown> = { label };
      self.self = self;
      return self;
    };

    @Workflow('throws-values')
    class ThrowsValues {
      async run(ctx: WorkflowContext, input: { where: 'run' | 'step' }) {
        await ctx.step('a', () => 1, { compensate: () => world.record('undo-a', '') });
        if (input.where === 'run') {
          throw circular('from run');
        }
        await ctx.step('b', () => Promise.reject(circular('from step')), { retry: false });
      }
    }

    const node = await start([ThrowsValues]);
    await node.client.start(ThrowsValues, { where: 'run' }, { id: 'r' });
    await node.client.start(ThrowsValues, { where: 'step' }, { id: 's' });
    await node.worker.drain();

    // Both ended, compensated, with a message that shows what was thrown.
    expect(await node.client.getStatus('r')).toMatchObject({
      status: 'failed',
      leaseUntil: null,
      error: { name: 'Error', message: expect.stringContaining("label: 'from run'") },
    });
    expect(await node.client.getStatus('s', { journal: true })).toMatchObject({
      status: 'failed',
      leaseUntil: null,
      error: { name: 'StepFailedError', message: expect.stringContaining("label: 'from step'") },
      journal: [
        { name: 'a', status: 'completed' },
        { name: 'b', status: 'failed', error: { name: 'Error', message: expect.stringContaining("label: 'from step'") } },
        { name: '$compensate:a', status: 'completed' },
      ],
    });
    expect(world.ops()).toEqual(['undo-a', 'undo-a']);
  });
});

describe('types', () => {
  it('types Journaled<T> as the JSON round-trip produces it', () => {
    class Money {
      constructor(
        readonly cents: number,
        readonly currency: string,
      ) {}
      format() {
        return `${this.cents} ${this.currency}`;
      }
    }

    expectTypeOf<Journaled<Date>>().toEqualTypeOf<string>();
    expectTypeOf<Journaled<{ at: Date; tags: Set<string>; byId: Map<string, number> }>>().toEqualTypeOf<{
      at: string;
      tags: Record<string, never>;
      byId: Record<string, never>;
    }>();
    expectTypeOf<Journaled<Money>>().toEqualTypeOf<{ readonly cents: number; readonly currency: string }>();
    expectTypeOf<Journaled<undefined>>().toEqualTypeOf<undefined>();
    expectTypeOf<Journaled<bigint>>().toEqualTypeOf<never>();

    // JSON.stringify writes undefined, functions and symbols in arrays as null.
    expectTypeOf<Journaled<Array<string | undefined>>>().toEqualTypeOf<Array<string | null>>();
    expectTypeOf<Journaled<[Date, () => void]>>().toEqualTypeOf<[string, null]>();
    expectTypeOf(JSON.parse(JSON.stringify(['a', undefined, () => 1]))).toEqualTypeOf<any>();
    expect(JSON.parse(JSON.stringify(['a', undefined, () => 1]))).toEqual(['a', null, null]);
  });

  it('infers start() input from the workflow class', async () => {
    @Workflow('typed-input')
    class TypedInput implements WorkflowRunner<{ orderId: string }, number> {
      async run(_ctx: WorkflowContext, input: { orderId: string }) {
        return input.orderId.length;
      }
    }

    const node = await start([TypedInput]);
    await node.client.start(TypedInput, { orderId: 'o1' }, { id: 'ti-1' });

    // @ts-expect-error: the input must match run()'s second parameter
    await expect(node.client.start(TypedInput, { order: 'o1' }, { id: 'ti-2' })).resolves.toBeDefined();

    // A name alone can't be checked: any input goes.
    await node.client.start('typed-input', 42, { id: 'ti-3' });
    expectTypeOf<WorkflowRunner>().toEqualTypeOf<WorkflowRunner<unknown, unknown>>();

    const status = await node.client.getStatus('ti-1');
    expectTypeOf(status).toEqualTypeOf<WorkflowInstanceDetails | null>();
    expectTypeOf(status!.journal).toEqualTypeOf<WorkflowJournalEntry[] | undefined>();
  });

  it('types journaled values as their JSON form, and progress by annotation', async () => {
    interface Checkpoint {
      page: number;
      since: Date;
    }

    @Workflow('typed')
    class Typed {
      async run(ctx: WorkflowContext) {
        const charge = await ctx.step('charge', () => ({ id: 'ch_1', at: new Date(0), format: () => 'x' }), {
          compensate: (result) => expectTypeOf(result).toEqualTypeOf<{ id: string; at: string }>(),
        });
        expectTypeOf(charge).toEqualTypeOf<{ id: string; at: string }>();

        const pages = await ctx.step('pages', async ({ progress, heartbeat }: WorkflowStepContext<Checkpoint>) => {
          expectTypeOf(progress).toEqualTypeOf<{ page: number; since: string } | undefined>();
          await heartbeat({ page: 1, since: new Date(0) });
          return progress?.page ?? 0;
        });
        expectTypeOf(pages).toEqualTypeOf<number>();

        const delivered = await ctx.waitForSignal('await', shipmentDelivered, { timeout: 0 });
        expectTypeOf(delivered).toEqualTypeOf<{ orderId: string; trackingId: string } | null>();

        return charge.at;
      }
    }

    const node = await start([Typed]);
    await node.client.start(Typed, undefined, { id: 't-1' });
    await node.worker.drain();

    const status = await node.client.getStatus('t-1', { journal: true });
    expectTypeOf(status!.journal).toEqualTypeOf<import('../lib/index.js').WorkflowJournalEntry[]>();
    expect(status).toMatchObject({ status: 'completed', output: '1970-01-01T00:00:00.000Z' });
  });
});
