/**
 * Concurrency limits: `@Workflow(name, { concurrency })` caps the instances that run at once,
 * per workflow and per key, across workers. A parked instance holds no slot.
 */
import { setTimeout as sleep } from 'node:timers/promises';
import { Injectable } from '@nestjs/common';
import { ManualWorkflowClock, Workflow, type WorkflowContext } from '../lib/index.js';
import { boot, tempDb, type Node, type TestDb } from './support.js';

/**
 * Counts the steps running at once, overall and per key. A step of a key in `meet` waits (up to a
 * second) until that many are running, so a limit that lets them run together shows it however
 * slow the machine.
 */
@Injectable()
class Meter {
  running = new Map<string, number>();
  peak = new Map<string, number>();
  meet = new Map<string, number>();
  order: string[] = [];

  async enter(keys: string[], id: string) {
    this.order.push(id);
    for (const key of keys) {
      const n = (this.running.get(key) ?? 0) + 1;
      this.running.set(key, n);
      this.peak.set(key, Math.max(this.peak.get(key) ?? 0, n));
    }

    const deadline = Date.now() + 1_000;
    while (keys.some((key) => (this.running.get(key) ?? 0) < (this.meet.get(key) ?? 0)) && Date.now() < deadline) {
      await sleep(5);
    }
    await sleep(15);
    for (const key of keys) {
      this.running.set(key, this.running.get(key)! - 1);
    }
  }
}

interface Order {
  id: string;
  customerId: string | null;
}

@Workflow('fulfilment', { concurrency: { limit: 1, key: (order: Order) => order.customerId } })
class Fulfilment {
  constructor(private readonly meter: Meter) {}

  async run(ctx: WorkflowContext, order: Order) {
    await ctx.step('pack', () => this.meter.enter([`customer:${order.customerId}`], order.id));
    await ctx.sleep('wait-for-carrier', '1h');
    await ctx.step('ship', () => this.meter.enter([`customer:${order.customerId}`], `${order.id}:ship`));
  }
}

@Workflow('report', { concurrency: [{ limit: 2 }, { limit: 1, key: (input: { tenant: string }) => input.tenant }] })
class Report {
  constructor(private readonly meter: Meter) {}

  async run(ctx: WorkflowContext, input: { tenant: string; n: number }) {
    await ctx.step('render', () => this.meter.enter(['reports', `tenant:${input.tenant}`], `${input.tenant}-${input.n}`));
  }
}

@Workflow('unlimited')
class Unlimited {
  async run() {}
}

let db: TestDb;
let clock: ManualWorkflowClock;
let meter: Meter;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  meter = new Meter();
});

afterEach(async () => {
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

async function start() {
  const node = await boot({ db, clock, workflows: [Fulfilment, Report, Unlimited], providers: [{ provide: Meter, useValue: meter }] });
  nodes.push(node);
  return node;
}

it("runs one instance per key at a time, in order, and lets a parked one's slot go", async () => {
  const node = await start();
  meter.meet.set('customer:null', 2);
  for (const [id, customerId] of [['o-1', 'c-1'], ['o-2', 'c-1'], ['o-3', 'c-2'], ['o-4', null], ['o-5', null]] as const) {
    await node.client.start(Fulfilment, { id, customerId }, { id });
  }
  expect(await node.client.getStatus('o-1')).toMatchObject({ concurrencyKey: 'c-1' });
  expect(await node.client.getStatus('o-4')).toMatchObject({ concurrencyKey: null });

  await node.worker.drain();
  expect(meter.peak.get('customer:c-1')).toBe(1);
  expect(meter.peak.get('customer:null')).toBe(2);
  // o-2 waited for o-1, which let its slot go when it parked on its sleep.
  expect(meter.order.indexOf('o-2')).toBeGreaterThan(meter.order.indexOf('o-1'));
  expect((await node.client.list({ status: 'suspended' })).map((i) => i.id)).toEqual(['o-1', 'o-2', 'o-3', 'o-4', 'o-5']);

  clock.advance('1h');
  await node.worker.drain();
  expect(meter.peak.get('customer:c-1')).toBe(1);
  expect(await node.client.list({ status: 'completed' })).toHaveLength(5);
});

it('holds a workflow to its limit and each key to its own, across two workers', async () => {
  const [a, b] = [await start(), await start()];
  meter.meet.set('reports', 2);
  for (let n = 0; n < 8; n++) {
    await a.client.start(Report, { tenant: n % 3 === 0 ? 'acme' : `t${n}`, n }, { id: `r-${n}` });
  }

  await Promise.all([a.worker.drain(), b.worker.drain(), a.worker.drain(), b.worker.drain()]);
  expect(await a.client.list({ status: 'completed' })).toHaveLength(8);
  expect(meter.peak.get('reports')).toBe(2);
  expect(meter.peak.get('tenant:acme')).toBe(1);
});

it("takes start()'s concurrencyKey over the computed one, and refuses one a workflow can't count", async () => {
  const node = await start();
  await node.client.start(Fulfilment, { id: 'o-6', customerId: 'c-1' }, { id: 'o-6', concurrencyKey: 'warehouse-2' });
  expect(await node.client.getStatus('o-6')).toMatchObject({ concurrencyKey: 'warehouse-2' });

  await expect(node.client.start(Unlimited, {}, { concurrencyKey: 'x' })).rejects.toThrow(
    'start(): workflow "unlimited" has no concurrency limit per key, so concurrencyKey would count for nothing.',
  );
  await expect(node.client.start(Fulfilment, { id: 'o-7', customerId: 'c' }, { concurrencyKey: '' })).rejects.toThrow('start(): invalid concurrencyKey "".');
  await expect(node.client.start(Fulfilment, { id: 'o-8', customerId: 42 as never })).rejects.toThrow(
    'The concurrency key of workflow "fulfilment" returned 42. Return a non-empty string, or null for none.',
  );
});

it('validates the limits in the decorator', () => {
  expect(() => Workflow('a', { concurrency: { limit: 0 } })).toThrow('Invalid concurrency limit 0 for workflow "a". Use a positive integer.');
  expect(() => Workflow('b', { concurrency: [{ limit: 1 }, { limit: 2 }] })).toThrow('Workflow "b" has two concurrency limits without a key. Give it at most one of each.');
  expect(() => Workflow('c', { concurrency: [] })).toThrow('Workflow "c" has 0 concurrency limits. Give it one, or two: one without a key and one with.');
  expect(() => Workflow('d', { concurrency: { limit: 1, key: 'customerId' as never } })).toThrow('Invalid concurrency key for workflow "d".');
});
