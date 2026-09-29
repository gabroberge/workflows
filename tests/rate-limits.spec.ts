/**
 * Rate limits and priorities: `@Workflow(name, { rateLimit })` caps how many executions start per window, per
 * workflow and per key, across workers; `start(..., { priority })` orders the claims.
 */
import { ManualWorkflowClock, Workflow, type WorkflowContext } from '../lib/index.js';
import { boot, tempDb, World, type Node, type TestDb } from './support.js';

interface Sync {
  id: string;
  customerId?: string | null;
}

@Workflow('sync', { rateLimit: { max: 2, duration: '1m' } })
class SyncWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: Sync) {
    await ctx.step('pull', () => this.world.record('pull', input.id));
    await ctx.sleep('nap', '30s');
    await ctx.step('push', () => this.world.record('push', input.id));
  }
}

@Workflow('customer-sync', { rateLimit: [{ max: 3, duration: '1m' }, { max: 1, duration: '1m', key: (input: Sync) => input.customerId }] })
class CustomerSyncWorkflow {
  constructor(private readonly world: World) {}

  async run(_ctx: WorkflowContext, input: Sync) {
    this.world.record('customer-sync', input.id);
  }
}

@Workflow('report')
class ReportWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { id: string; children?: boolean }) {
    await ctx.step('render', () => this.world.record('render', input.id));
    if (input.children) {
      await ctx.startChild(ReportWorkflow, { id: `${input.id}/inherited` }, { id: `${input.id}/inherited`, parentClose: 'abandon' });
      await ctx.startChild(ReportWorkflow, { id: `${input.id}/urgent` }, { id: `${input.id}/urgent`, parentClose: 'abandon', priority: 1 });
    }
  }
}

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

async function start(worker: { concurrency?: number } = {}) {
  const node = await boot({ db, clock, worker, workflows: [SyncWorkflow, CustomerSyncWorkflow, ReportWorkflow], providers: [{ provide: World, useValue: world }] });
  nodes.push(node);
  return node;
}

const calls = (op: string) => world.calls.filter((call) => call.op === op).map((call) => call.key);

it('starts at most max executions per window, resumptions included, the window opening with the first after it', async () => {
  const node = await start();
  for (const id of ['s-1', 's-2', 's-3']) {
    await node.client.start(SyncWorkflow, { id }, { id });
  }

  await node.worker.drain();
  expect(calls('pull')).toEqual(['s-1', 's-2']);
  expect(await node.client.getStatus('s-3')).toMatchObject({ status: 'pending', runs: 0 });

  // s-1 and s-2 wake from their naps, but the window (t0 to t0+1m) is full.
  clock.advance('30s');
  expect(await node.worker.drain()).toBe(0);

  // A new window at t0+1m: s-3 has waited longest, then s-1; s-2 waits for the next window.
  clock.advance('30s');
  expect(await node.worker.drain()).toBe(2);
  expect(calls('pull')).toEqual(['s-1', 's-2', 's-3']);
  expect(calls('push')).toEqual(['s-1']);

  clock.advance('1m');
  await node.worker.drain();
  expect(calls('push').sort()).toEqual(['s-1', 's-2', 's-3']);
  expect(await node.client.list({ status: 'completed' })).toHaveLength(3);

  // Its window, long over, goes with a purge.
  clock.advance('2d');
  expect(await node.client.purge({ olderThan: '1d' })).toEqual({ instances: 3, signals: 0, rateLimits: 1 });
});

it("keeps each key to its window and the workflow to its own, and passes over a busy key", async () => {
  const [a, b] = [await start(), await start()];
  const instances: Array<[string, string | null]> = [
    ['c1-a', 'c-1'],
    ['c1-b', 'c-1'],
    ['c2-a', 'c-2'],
    ['none', null],
    ['c3-a', 'c-3'],
    ['c1-c', 'c-1'],
  ];
  for (const [id, customerId] of instances) {
    await a.client.start(CustomerSyncWorkflow, { id, customerId }, { id });
  }
  expect(await a.client.getStatus('c1-a')).toMatchObject({ rateLimitKey: 'c-1' });
  expect(await a.client.getStatus('none')).toMatchObject({ rateLimitKey: null });

  // Two workers, one window of 3 for the workflow, one per customer. They start at the same time, so they go by
  // id: c-1's others are passed over for c-2 and c-3, and the workflow's window leaves the keyless one for later.
  await Promise.all([a.worker.drain(), b.worker.drain()]);
  expect(calls('customer-sync').sort()).toEqual(['c1-a', 'c2-a', 'c3-a']);

  clock.advance('1m');
  await Promise.all([a.worker.drain(), b.worker.drain()]);
  expect(calls('customer-sync').slice(3).sort()).toEqual(['c1-b', 'none']);

  clock.advance('1m');
  await a.worker.drain();
  expect(calls('customer-sync').slice(5)).toEqual(['c1-c']);
});

it('claims the instances without a priority first, then the lowest, and children inherit it', async () => {
  const node = await start({ concurrency: 1 });
  await node.client.start(ReportWorkflow, { id: 'p-5' }, { id: 'p-5', priority: 5 });
  await node.client.start(ReportWorkflow, { id: 'none' }, { id: 'none' });
  await node.client.start(ReportWorkflow, { id: 'p-2', children: true }, { id: 'p-2', priority: 2 });
  await node.client.start(ReportWorkflow, { id: 'p-3' }, { id: 'p-3', priority: 3 });
  expect(await node.client.getStatus('p-5')).toMatchObject({ priority: 5 });
  expect(await node.client.getStatus('none')).toMatchObject({ priority: 0 });

  await node.worker.drain();
  // p-2's children start after it: the one it gave priority 1 goes first, the one that inherited 2 next.
  expect(calls('render')).toEqual(['none', 'p-2', 'p-2/urgent', 'p-2/inherited', 'p-3', 'p-5']);
  expect(await node.client.getStatus('p-2/inherited')).toMatchObject({ priority: 2 });
  expect(await node.client.getStatus('p-2/urgent')).toMatchObject({ priority: 1 });
});

it("takes start()'s rateLimitKey over the computed one, and refuses keys and priorities that can't count", async () => {
  const node = await start();
  await node.client.start(CustomerSyncWorkflow, { id: 'x', customerId: 'c-1' }, { id: 'x', rateLimitKey: 'reseller-7' });
  expect(await node.client.getStatus('x')).toMatchObject({ rateLimitKey: 'reseller-7' });

  await expect(node.client.start(SyncWorkflow, { id: 'y' }, { rateLimitKey: 'k' })).rejects.toThrow(
    'start(): workflow "sync" has no rate limit per key, so rateLimitKey would count for nothing. Declare one with @Workflow(name, { rateLimit: { max, duration, key } }).',
  );
  await expect(node.client.start(CustomerSyncWorkflow, { id: 'z', customerId: 7 as never })).rejects.toThrow(
    'The rate limit key of workflow "customer-sync" returned 7. Return a non-empty string, or null for none.',
  );
  for (const priority of [-1, 1.5, 2_097_152]) {
    await expect(node.client.start(ReportWorkflow, { id: 'p' }, { priority })).rejects.toThrow(
      `start(): invalid priority ${priority}. Use an integer from 1 (first) to 2097151.`,
    );
  }
});

it('validates the rate limits in the decorator', () => {
  expect(() => Workflow('a', { rateLimit: { max: 0, duration: '1m' } })).toThrow('Invalid rate limit max 0 for workflow "a". Use a positive integer.');
  expect(() => Workflow('b', { rateLimit: { max: 1, duration: '0s' } })).toThrow('Invalid rate limit duration "0s" for workflow "b". Use a positive duration, such as "1m".');
  expect(() => Workflow('c', { rateLimit: [{ max: 1, duration: '1s' }, { max: 2, duration: '1s' }] })).toThrow(
    'Workflow "c" has two rate limits without a key. Give it at most one of each.',
  );
  expect(() => Workflow('d', { rateLimit: [] })).toThrow('Workflow "d" has 0 rate limits. Give it one, or two: one without a key and one with.');
  expect(() => Workflow('e', { rateLimit: { max: 1, duration: '1m', key: 'customerId' as never } })).toThrow('Invalid rate limit key for workflow "e".');
});
