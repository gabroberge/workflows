/**
 * `WorkflowEvents.events$`: the events of each outcome in order, with the payloads an
 * operator alerts on, and the stream's end when the application shuts down.
 */
import { Inject } from '@nestjs/common';
import {
  ManualWorkflowClock,
  NonRetryableStepError,
  Workflow,
  WorkflowEvents,
  type WorkflowContext,
  type WorkflowEvent,
} from '../lib/index.js';
import { boot, tempDb, type Node, type TestDb, World } from './support.js';

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

const start = async (workflows: any[]) => {
  const node = await boot({ db, clock, workflows, providers: [{ provide: World, useValue: world }] });
  nodes.push(node);
  return node;
};

const types = (events: WorkflowEvent[]) => events.map((event) => event.type);

@Workflow('checkout')
class Checkout {
  constructor(@Inject(World) private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { undo: 'ok' | 'fails' }) {
    await ctx.step('reserve', () => 'r-1', {
      compensate: () => {
        this.world.record('release', '');
        if (input.undo === 'fails') {
          throw new NonRetryableStepError('warehouse offline');
        }
      },
    });
    await ctx.step(
      'charge',
      (s) => {
        throw s.attempt === 1 ? new Error('503') : new NonRetryableStepError('card declined');
      },
      { retry: { attempts: 3, backoff: { delay: '1s' } } },
    );
  }
}

describe('events$', () => {
  it('reports a retry, the resumed execution, the undo trail and the failure, in order', async () => {
    const node = await start([Checkout]);
    const t0 = clock.now();
    await node.client.start(Checkout, { undo: 'ok' }, { id: 'c-1' });
    await node.worker.drain();
    clock.advance('1s');
    await node.worker.drain();

    expect(types(node.events)).toEqual([
      'workflow-started',
      'step-completed',
      'step-failed',
      'workflow-suspended',
      'workflow-resumed',
      'step-failed',
      'workflow-compensating',
      'step-compensated',
      'workflow-failed',
    ]);

    const failure = { name: 'StepFailedError', message: 'Step "charge" failed after 2 attempt(s): NonRetryableStepError: card declined' };
    expect(node.events).toMatchObject([
      { type: 'workflow-started', at: t0 },
      { type: 'step-completed', step: 'reserve', attempt: 1, durationMs: expect.any(Number) },
      { type: 'step-failed', step: 'charge', attempt: 1, retryAt: t0 + 1_000, error: { name: 'Error', message: '503' } },
      { type: 'workflow-suspended', wakeAt: t0 + 1_000, waits: [] },
      { type: 'workflow-resumed', run: 2, at: t0 + 1_000 },
      { type: 'step-failed', step: 'charge', attempt: 2, retryAt: null, error: { name: 'NonRetryableStepError', message: 'card declined' } },
      { type: 'workflow-compensating', error: failure },
      { type: 'step-compensated', step: 'reserve', attempt: 1 },
      { type: 'workflow-failed', error: failure },
    ]);

    for (const event of node.events) {
      expect(event).toMatchObject({ id: 'c-1', workflow: 'checkout', version: 1 });
    }
  });

  it('reports a compensation that gives up, with both errors', async () => {
    const node = await start([Checkout]);
    await node.client.start(Checkout, { undo: 'fails' }, { id: 'c-2' });
    await node.worker.drain();
    clock.advance('1s');
    await node.worker.drain();

    expect(types(node.events).slice(-2)).toEqual(['workflow-compensating', 'workflow-compensation-failed']);
    expect(node.events.at(-1)).toMatchObject({
      type: 'workflow-compensation-failed',
      error: {
        name: 'StepFailedError',
        compensation: { name: 'NonRetryableStepError', message: 'Step "$compensate:reserve" failed after 1 attempt(s): NonRetryableStepError: warehouse offline' },
      },
    });
    expect(node.events.some((event) => event.type === 'step-compensated')).toBe(false);
  });

  it('reports a cancel with its reason, and the parked wait it ended', async () => {
    @Workflow('awaiting')
    class Awaiting {
      async run(ctx: WorkflowContext) {
        await ctx.waitForSignal('approval', 'approve', { key: 'a' });
      }
    }

    const node = await start([Awaiting]);
    await node.client.start(Awaiting, undefined, { id: 'a-1' });
    await node.worker.drain();
    expect(node.events.at(-1)).toMatchObject({ type: 'workflow-suspended', wakeAt: null, waits: [{ signal: 'approve', key: 'a' }] });

    await node.client.cancel('a-1', 'Customer changed their mind.');
    await node.worker.drain();

    expect(types(node.events)).toEqual(['workflow-started', 'workflow-suspended', 'workflow-resumed', 'workflow-compensating', 'workflow-cancelled']);
    expect(node.events.at(-1)).toMatchObject({
      type: 'workflow-cancelled',
      error: { name: 'WorkflowCancelledError', message: 'Customer changed their mind.' },
    });
  });

  it("carries only this application's events, and completes when it shuts down", async () => {
    @Workflow('quick')
    class Quick {
      async run() {
        return 'ok';
      }
    }

    const a = await start([Quick]);
    const b = await start([Quick]);
    let completed = false;
    a.moduleRef.get(WorkflowEvents).events$.subscribe({ complete: () => (completed = true) });

    await a.client.start(Quick, undefined, { id: 'q-1' });
    await b.worker.drain();

    expect(types(a.events)).toEqual([]);
    expect(types(b.events)).toEqual(['workflow-started', 'workflow-completed']);
    expect(b.events.at(-1)).toMatchObject({ id: 'q-1', output: 'ok' });

    await a.close();
    nodes.splice(nodes.indexOf(a), 1);
    expect(completed).toBe(true);
  });
});
