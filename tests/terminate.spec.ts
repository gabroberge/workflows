/**
 * `WorkflowClient.terminate()`: stop an instance without running its compensations, whether it
 * is parked, running here or in another process, or already compensating.
 */
import { setTimeout as sleep } from 'node:timers/promises';
import { Injectable } from '@nestjs/common';
import { ManualWorkflowClock, Workflow, WorkflowSignal, type WorkflowContext } from '../lib/index.js';
import { boot, deferred, tempDb, World, type Node, type TestDb } from './support.js';

const confirmed = new WorkflowSignal<boolean>('carrier.confirmed');

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

@Workflow('shipment')
class Shipment {
  constructor(
    private readonly world: World,
    private readonly gate: Gate,
  ) {}

  async run(ctx: WorkflowContext, input: { fail?: boolean }) {
    const undo = (step: string) => ({ compensate: async () => {
      await this.gate.pass(`undo-${step}`);
      this.world.record(`undo-${step}`, '');
    } });

    await ctx.step('reserve', () => this.world.record('reserve', ''), undo('reserve'));
    await ctx.step('label', async () => {
      await this.gate.pass('label');
      this.world.record('label', '');
    }, undo('label'));
    if (input.fail) {
      ctx.fail('The carrier refused the parcel.');
    }

    await ctx.waitForSignal('confirmation', confirmed, { timeout: '1d' });
    await ctx.step('dispatch', () => this.world.record('dispatch', ''), undo('dispatch'));
    return 'shipped';
  }
}

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
let gate: Gate;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  world = new World();
  gate = new Gate();
});

afterEach(async () => {
  gate.release.resolve();
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

async function start(worker: { heartbeatInterval?: number } = {}) {
  const node = await boot({
    db,
    clock,
    workflows: [Shipment],
    worker,
    providers: [
      { provide: World, useValue: world },
      { provide: Gate, useValue: gate },
    ],
  });
  nodes.push(node);
  return node;
}

it('ends a parked instance as cancelled without compensating it', async () => {
  const node = await start();
  await node.client.start(Shipment, {}, { id: 's-1' });
  await node.worker.drain();

  const result = await node.client.terminate('s-1', 'The carrier went out of business.');
  expect(result).toMatchObject({ accepted: true, cancelRequested: true, terminateRequested: true });
  await node.worker.drain();

  expect(await node.client.getStatus('s-1', { journal: true })).toMatchObject({
    status: 'cancelled',
    error: { name: 'WorkflowTerminatedError', message: 'The carrier went out of business.' },
    journal: [{ name: 'reserve' }, { name: 'label' }, { name: 'confirmation', status: 'cancelled' }],
  });
  expect(world.ops()).toEqual(['reserve', 'label']);
  expect(node.events.map((event) => event.type).slice(-1)).toEqual(['workflow-cancelled']);
  expect(node.events.filter((event) => event.type === 'workflow-resumed')).toEqual([]);
  expect(await node.client.terminate('s-1')).toMatchObject({ accepted: false, status: 'cancelled' });
});

it('lets the running step of an instance in this process finish, then stops it', async () => {
  const node = await start();
  gate.holdAt = 'label';
  await node.client.start(Shipment, {}, { id: 's-2' });
  const running = node.worker.drain();
  await gate.reached.promise;

  await node.client.terminate('s-2');
  gate.release.resolve();
  await running;

  expect(await node.client.getStatus('s-2')).toMatchObject({ status: 'cancelled', error: { name: 'WorkflowTerminatedError', message: 'Terminated.' } });
  expect(world.ops()).toEqual(['reserve', 'label']);
});

it('stops an instance running in another process once its heartbeat reads the request', async () => {
  const worker = await start({ heartbeatInterval: 20 });
  const api = await start();
  gate.holdAt = 'label';
  await api.client.start(Shipment, {}, { id: 's-3' });
  const running = worker.worker.drain();
  await gate.reached.promise;

  await api.client.terminate('s-3', 'Stuck.');
  await sleep(60);
  gate.release.resolve();
  await running;

  expect(await api.client.getStatus('s-3')).toMatchObject({ status: 'cancelled', error: { name: 'WorkflowTerminatedError', message: 'Stuck.' } });
  expect(world.ops()).toEqual(['reserve', 'label']);
});

it('stops a compensating instance after the compensation that is running', async () => {
  const node = await start({ heartbeatInterval: 20 });
  gate.holdAt = 'undo-label';
  await node.client.start(Shipment, { fail: true }, { id: 's-4' });
  const running = node.worker.drain();
  await gate.reached.promise;

  expect(await node.client.terminate('s-4', 'Leave the reservation.')).toMatchObject({ accepted: true, status: 'compensating' });
  gate.release.resolve();
  await running;

  expect(world.ops()).toEqual(['reserve', 'label', 'undo-label']);
  expect(await node.client.getStatus('s-4')).toMatchObject({ status: 'cancelled', error: { name: 'WorkflowTerminatedError', message: 'Leave the reservation.' } });
});

it('takes over from a cancel that no worker has run yet', async () => {
  const node = await start();
  await node.client.start(Shipment, {}, { id: 's-5' });
  await node.worker.drain();

  expect(await node.client.cancel('s-5', 'Changed my mind.')).toMatchObject({ accepted: true });
  expect(await node.client.terminate('s-5', 'Right now.')).toMatchObject({ accepted: true });
  await node.worker.drain();
  expect(await node.client.getStatus('s-5')).toMatchObject({ status: 'cancelled', error: { name: 'WorkflowTerminatedError', message: 'Right now.' } });
  expect(world.ops()).toEqual(['reserve', 'label']);
});
