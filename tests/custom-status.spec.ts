/**
 * `ctx.setStatus()`: a custom status the outside world reads with `getStatus()`, written with
 * the instance's next write and re-derived, not journaled, by every replay.
 */
import { Injectable } from '@nestjs/common';
import { ManualWorkflowClock, Workflow, WorkflowSignal, type WorkflowContext } from '../lib/index.js';
import { boot, deferred, tempDb, type Node, type TestDb } from './support.js';

const delivered = new WorkflowSignal<{ at: string }>('shipment.delivered');

@Injectable()
class Warehouse {
  readonly seen: unknown[] = [];
  packing = deferred();
  release = deferred();
}

@Workflow('fulfilment')
class Fulfilment {
  constructor(private readonly warehouse: Warehouse) {}

  async run(ctx: WorkflowContext, input: { orderId: string; items: number }) {
    ctx.setStatus({ stage: 'packing', items: input.items });
    await ctx.step('pack', async () => {
      this.warehouse.packing.resolve();
      await this.warehouse.release.promise;
    });

    ctx.setStatus({ stage: 'shipped', items: input.items });
    const delivery = await ctx.waitForSignal('delivery', delivered, { key: input.orderId });
    ctx.setStatus({ stage: 'delivered', at: delivery!.at });
    ctx.setStatus({ stage: 'delivered', at: delivery!.at });
    return 'done';
  }
}

@Workflow('status-misuse')
class Misuse {
  async run(ctx: WorkflowContext, input: 'huge' | 'bigint' | 'in-step' | 'clear') {
    if (input === 'huge') {
      ctx.setStatus({ log: 'x'.repeat(16_384) });
    } else if (input === 'bigint') {
      ctx.setStatus({ total: 10n });
    } else if (input === 'in-step') {
      await ctx.step('bad', () => ctx.setStatus('inside'));
    } else {
      ctx.setStatus('set');
      await ctx.sleep('nap', '1h');
      ctx.setStatus(undefined);
    }
  }
}

let db: TestDb;
let clock: ManualWorkflowClock;
let warehouse: Warehouse;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  warehouse = new Warehouse();
});

afterEach(async () => {
  warehouse.release.resolve();
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

async function start() {
  const node = await boot({ db, clock, workflows: [Fulfilment, Misuse], providers: [{ provide: Warehouse, useValue: warehouse }] });
  nodes.push(node);
  return node;
}

const statuses = (node: Node) => node.events.filter((event) => event.type === 'custom-status').map((event) => (event as { status: unknown }).status);

it('is written before the next step runs, with the suspension, and at the end, and emitted when it changes', async () => {
  const node = await start();
  await node.client.start(Fulfilment, { orderId: 'o-1', items: 3 }, { id: 'f-1' });
  expect(await node.client.getStatus('f-1')).toMatchObject({ customStatus: null });

  const draining = node.worker.drain();
  await warehouse.packing.promise;
  // The step's attempt record carried it, before the step started.
  expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'running', customStatus: { stage: 'packing', items: 3 } });
  warehouse.release.resolve();
  await draining;
  expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'suspended', customStatus: { stage: 'shipped', items: 3 } });
  expect((await node.client.list())[0]).toMatchObject({ customStatus: { stage: 'shipped', items: 3 } });

  await node.client.signal(delivered, { at: '2026-01-03' }, { key: 'o-1' });
  await node.worker.drain();
  expect(await node.client.getStatus('f-1', { journal: true })).toMatchObject({ status: 'completed', customStatus: { stage: 'delivered', at: '2026-01-03' } });
  // A replay sets 'packing' and 'shipped' again, which changes nothing that was written; the
  // repeated 'delivered' is no change either.
  expect(statuses(node)).toEqual([{ stage: 'packing', items: 3 }, { stage: 'shipped', items: 3 }, { stage: 'delivered', at: '2026-01-03' }]);
  expect((await node.client.getStatus('f-1', { journal: true }))!.journal.map((entry) => entry.name)).toEqual(['pack', 'delivery']);
});

it('is re-derived by the replay after a restart, without being journaled', async () => {
  const first = await start();
  await first.client.start(Fulfilment, { orderId: 'o-2', items: 1 }, { id: 'f-2' });
  warehouse.release.resolve();
  await first.worker.drain();
  await first.close();
  nodes.splice(0);

  const second = await start();
  await second.client.signal(delivered, { at: '2026-01-04' }, { key: 'o-2' });
  await second.worker.drain();
  expect(statuses(second)).toEqual([{ stage: 'delivered', at: '2026-01-04' }]);
  expect(await second.client.getStatus('f-2')).toMatchObject({ customStatus: { stage: 'delivered', at: '2026-01-04' } });
});

it('refuses a status over 16 KiB, one JSON cannot hold, and one set inside a step; undefined clears it', async () => {
  const node = await start();
  for (const input of ['huge', 'bigint', 'in-step', 'clear'] as const) {
    await node.client.start(Misuse, input, { id: input });
  }
  await node.worker.drain();
  clock.advance('1h');
  await node.worker.drain();

  expect(await node.client.getStatus('huge')).toMatchObject({
    status: 'failed',
    customStatus: null,
    error: { name: 'TypeError', message: expect.stringContaining('ctx.setStatus() got 16394 bytes of JSON, over the 16384-byte limit.') },
  });
  expect(await node.client.getStatus('bigint')).toMatchObject({
    status: 'failed',
    error: { name: 'TypeError', message: expect.stringContaining('ctx.setStatus() takes a JSON-serializable value') },
  });
  expect(await node.client.getStatus('in-step')).toMatchObject({
    status: 'failed',
    error: { name: 'WorkflowDefinitionError', message: expect.stringContaining('step "bad" called ctx.setStatus()') },
  });
  expect(await node.client.getStatus('clear')).toMatchObject({ status: 'completed', customStatus: null });
  expect(statuses(node)).toEqual(['set', null]);
});
