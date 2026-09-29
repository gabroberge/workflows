/**
 * `ctx.waitForAny()`: the first of several signals and timers, journaled as one entry. The
 * losers' signals stay for later waits, and a replay returns the same winner.
 */
import { Injectable } from '@nestjs/common';
import { ManualWorkflowClock, Workflow, WorkflowSignal, type WorkflowContext } from '../lib/index.js';
import { boot, tempDb, type Node, type TestDb } from './support.js';

const delivered = new WorkflowSignal<{ at: string }>('shipment.delivered');
const cancelled = new WorkflowSignal<{ reason: string }>('order.cancelled');
const priced = new WorkflowSignal<{ total: number }>('order.priced');

@Injectable()
class Outcomes {
  readonly seen: unknown[] = [];
  failFollowUp = 0;
}

@Workflow('delivery')
class Delivery {
  constructor(private readonly outcomes: Outcomes) {}

  async run(ctx: WorkflowContext, orderId: string) {
    const outcome = await ctx.waitForAny('delivered-or-cancelled', {
      delivered: ctx.signalWait(delivered, { key: orderId }),
      cancelled: ctx.signalWait(cancelled, { key: orderId }),
      timeout: ctx.timer('3d'),
    });
    if (outcome.key === 'delivered') {
      // Typed per key: a payload with `at`.
      this.outcomes.seen.push(`delivered at ${outcome.value.at}`);
    } else if (outcome.key === 'cancelled') {
      this.outcomes.seen.push(`cancelled: ${outcome.value.reason}`);
    } else {
      this.outcomes.seen.push(`timed out: ${String(outcome.value)}`);
    }

    // A cancel that lost the race is still there for a later wait.
    const late = await ctx.waitForSignal('late-cancel', cancelled, { key: orderId, timeout: '1h' });
    await ctx.step('follow-up', () => {
      if (this.outcomes.failFollowUp-- > 0) {
        throw new Error('The mail server hung up.');
      }
    }, { retry: { attempts: 2, backoff: { delay: '1s' } } });
    return { winner: outcome.key, late: late?.reason ?? null };
  }
}

@Workflow('pricing')
class Pricing {
  async run(ctx: WorkflowContext, orderId: string) {
    const [big, any] = await Promise.all([
      ctx.waitForAny('big', { big: ctx.signalWait(priced, { key: orderId, match: (p) => p.total >= 1_000 }), give_up: ctx.timer({ until: Date.UTC(2026, 0, 2) }) }),
      ctx.waitForSignal('any', priced, { key: orderId }),
    ]);
    return { big: big.key === 'big' ? big.value.total : null, any: any?.total };
  }
}

@Workflow('any-misuse')
class Misuse {
  async run(ctx: WorkflowContext, input: 'empty' | 'not-a-condition') {
    if (input === 'empty') {
      await ctx.waitForAny('nothing', {});
    } else {
      await ctx.waitForAny('bad', { soon: ctx.timer('1h'), later: '1d' as never });
    }
  }
}

let db: TestDb;
let clock: ManualWorkflowClock;
let outcomes: Outcomes;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  outcomes = new Outcomes();
});

afterEach(async () => {
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

async function start(workflows: any[] = [Delivery, Pricing, Misuse]) {
  const node = await boot({ db, clock, workflows, providers: [{ provide: Outcomes, useValue: outcomes }] });
  nodes.push(node);
  return node;
}

it('returns the first signal, leaves the loser for a later wait, and parks on every condition at once', async () => {
  const node = await start();
  await node.client.start(Delivery, 'o-1', { id: 'd-1' });
  await node.worker.drain();
  expect(await node.client.getStatus('d-1')).toMatchObject({
    status: 'suspended',
    wakeAt: clock.now() + 3 * 86_400_000,
    waits: [
      { signal: 'shipment.delivered', key: 'o-1' },
      { signal: 'order.cancelled', key: 'o-1' },
    ],
  });

  const { signalId } = await node.client.signal(delivered, { at: '2026-01-02' }, { key: 'o-1' });
  await node.client.signal(cancelled, { reason: 'Too late.' }, { key: 'o-1' });
  await node.worker.drain();

  expect(await node.client.getStatus('d-1', { journal: true })).toMatchObject({
    status: 'completed',
    output: { winner: 'delivered', late: 'Too late.' },
    journal: [
      {
        name: 'delivered-or-cancelled',
        kind: 'any',
        status: 'completed',
        result: { key: 'delivered', signalId, payload: { at: '2026-01-02' } },
        data: {
          waits: { delivered: { signal: 'shipment.delivered', key: 'o-1' }, cancelled: { signal: 'order.cancelled', key: 'o-1' } },
          timers: { timeout: Date.UTC(2026, 0, 4) },
        },
      },
      { name: 'late-cancel', result: { payload: { reason: 'Too late.' } } },
      { name: 'follow-up', status: 'completed' },
    ],
  });
  expect(outcomes.seen).toEqual(['delivered at 2026-01-02']);
  expect(node.events.filter((event) => event.type === 'signal-received')).toMatchObject([
    { wait: 'delivered-or-cancelled', signal: 'shipment.delivered', signalId },
    { wait: 'late-cancel', signal: 'order.cancelled' },
  ]);
});

it('takes a signal sent before the wait was reached, without parking', async () => {
  const node = await start();
  await node.client.start(Delivery, 'o-2', { id: 'd-2' });
  await node.client.signal(cancelled, { reason: 'Changed my mind.' }, { key: 'o-2' });
  await node.worker.drain({ maxRounds: 1 });
  expect(outcomes.seen).toEqual(['cancelled: Changed my mind.']);
  expect(node.events.filter((event) => event.type === 'workflow-suspended')).toMatchObject([{ waits: [{ signal: 'order.cancelled' }] }]);
});

it('lets the timer win at its deadline, from when the wait was first reached, and ignores signals sent after it', async () => {
  const node = await start();
  await node.client.start(Delivery, 'o-3', { id: 'd-3' });
  await node.worker.drain();
  clock.advance('1d');
  await node.worker.drain(); // nothing is due: the deadline stays three days after the first execution
  expect(outcomes.seen).toEqual([]);

  clock.advance('3d');
  await node.client.signal(delivered, { at: 'after the deadline' }, { key: 'o-3' });
  await node.worker.drain();
  clock.advance('1h');
  await node.worker.drain();
  expect(new Set(outcomes.seen)).toEqual(new Set(['timed out: null']));
  expect(await node.client.getStatus('d-3', { journal: true })).toMatchObject({
    status: 'completed',
    output: { winner: 'timeout', late: null },
    journal: [{ name: 'delivered-or-cancelled', result: { key: 'timeout', signalId: null, payload: null } }, {}, {}],
  });
});

it('returns the journaled winner on every replay', async () => {
  outcomes.failFollowUp = 1;
  const node = await start();
  await node.client.start(Delivery, 'o-4', { id: 'd-4' });
  await node.client.signal(cancelled, { reason: 'First.' }, { key: 'o-4' });
  await node.worker.drain({ maxRounds: 1 });
  clock.advance('1h');
  await node.worker.drain({ maxRounds: 1 });
  // The follow-up step failed once and parked for its retry; a delivery arrives meanwhile.
  await node.client.signal(delivered, { at: 'meanwhile' }, { key: 'o-4' });
  clock.advance('1s');
  await node.worker.drain();
  expect(outcomes.seen).toEqual(['cancelled: First.', 'cancelled: First.', 'cancelled: First.']);
  expect(await node.client.getStatus('d-4')).toMatchObject({ status: 'completed', output: { winner: 'cancelled', late: null } });

});

it('fails an instance whose code no longer has the condition that won', async () => {
  const before = await start();
  await before.client.start(Delivery, 'o-5', { id: 'd-5' });
  await before.client.signal(cancelled, { reason: 'x' }, { key: 'o-5' });
  await before.worker.drain({ maxRounds: 1 });
  await before.close();
  nodes.splice(0);

  @Workflow('delivery')
  class DeliveryWithoutCancel {
    async run(ctx: WorkflowContext, orderId: string) {
      await ctx.waitForAny('delivered-or-cancelled', { delivered: ctx.signalWait(delivered, { key: orderId }), timeout: ctx.timer('3d') });
      await ctx.waitForSignal('late-cancel', cancelled, { key: orderId, timeout: '1h' });
    }
  }

  const after = await start([DeliveryWithoutCancel]);
  clock.advance('1h');
  await after.worker.drain();
  expect(await after.client.getStatus('d-5')).toMatchObject({
    status: 'failed',
    error: {
      name: 'WorkflowNonDeterminismError',
      message: expect.stringContaining('waitForAny("delivered-or-cancelled") was won by its signal "cancelled", which the code no longer has.'),
    },
  });
});

it('keeps parallel waits from taking the same signal, and applies each condition its match', async () => {
  const node = await start();
  await node.client.start(Pricing, 'o-6', { id: 'p-6' });
  await node.client.signal(priced, { total: 500 }, { key: 'o-6' });
  await node.worker.drain();
  expect(await node.client.getStatus('p-6')).toMatchObject({ status: 'suspended', waits: [{ signal: 'order.priced', key: 'o-6' }] });

  await node.client.signal(priced, { total: 1_500 }, { key: 'o-6' });
  await node.worker.drain();
  expect(await node.client.getStatus('p-6')).toMatchObject({ status: 'completed', output: { big: 1_500, any: 500 } });
});

it('fails an instance that passes no condition, or something else as one', async () => {
  const node = await start();
  await node.client.start(Misuse, 'empty', { id: 'empty' });
  await node.client.start(Misuse, 'not-a-condition', { id: 'not-a-condition' });
  await node.worker.drain();
  expect(await node.client.getStatus('empty')).toMatchObject({
    status: 'failed',
    error: { name: 'TypeError', message: 'waitForAny("nothing") takes an object with at least one condition, such as { delivered: ctx.signalWait(shipmentDelivered) }.' },
  });
  expect(await node.client.getStatus('not-a-condition')).toMatchObject({
    status: 'failed',
    error: { name: 'TypeError', message: 'waitForAny("bad"): "later" is not a condition. Make each one with ctx.signalWait() or ctx.timer().' },
  });
});
