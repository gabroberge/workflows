/**
 * What an operator does with an instance that needs a person: `retry()` a failed one once its
 * cause is fixed (a step that gave up, a deploy that broke replay, a run timeout), `retry()` the
 * compensations of a `compensation_failed` one, and `delete()` one; each journaled or emitted,
 * refused for statuses where it makes no sense, and accepted once when two race.
 */
import { Inject } from '@nestjs/common';
import {
  ManualWorkflowClock,
  NonRetryableStepError,
  Workflow,
  WorkflowNotFoundError,
  WorkflowStateError,
  type WorkflowContext,
} from '../lib/index.js';
import { boot, tempDb, type Node, type TestDb, World } from './support.js';

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
const nodes: Node[] = [];

/** What the outside world does right now: an operator fixes it before retrying. */
const outage = { mail: false, warehouse: false };

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  world = new World();
  outage.mail = false;
  outage.warehouse = false;
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

const restart = async (node: Node, workflows: any[]) => {
  await node.close();
  nodes.splice(nodes.indexOf(node), 1);
  return start(workflows);
};

const types = (node: Node) => node.events.map((e) => e.type);

@Workflow('fulfil')
class Fulfil {
  constructor(@Inject(World) private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { cancelAfterReserve?: boolean }) {
    await ctx.step('charge', ({ idempotencyKey }) => this.world.record('charge', idempotencyKey), {
      compensate: () => this.world.record('refund', ''),
    });
    await ctx.step('reserve', () => this.world.record('reserve', ''), {
      retry: 1,
      compensate: () => {
        if (outage.warehouse) {
          throw new NonRetryableStepError('warehouse offline');
        }
        this.world.record('release', '');
      },
    });
    if (input.cancelAfterReserve) {
      await ctx.sleep('hold', '1h');
    }
    ctx.commit('shipped');
    await ctx.step(
      'review-request',
      () => {
        if (outage.mail) {
          throw new Error('mail provider down');
        }
        this.world.record('mail', '');
      },
      { retry: { attempts: 2, backoff: { delay: '1s' } } },
    );
    return 'done';
  }
}

describe('retry() of a failed instance', () => {
  it('runs it again from its journal: completed steps return their results, the step that gave up gets its attempts back', async () => {
    outage.mail = true;
    const node = await start([Fulfil]);
    await node.client.start(Fulfil, {}, { id: 'f-1' });
    await node.worker.drain();
    clock.advance('1s');
    await node.worker.drain();
    const failed = await node.client.getStatus('f-1');
    expect(failed).toMatchObject({ status: 'failed', error: { name: 'StepFailedError' } });

    outage.mail = false;
    const retried = await node.client.retry('f-1');
    expect(retried).toMatchObject({ id: 'f-1', status: 'pending', error: null, wakeAt: clock.now() });
    expect(retried).not.toHaveProperty('journal');
    await node.worker.drain();

    expect(await node.client.getStatus('f-1', { journal: true })).toMatchObject({
      status: 'completed',
      output: 'done',
      journal: [
        { name: 'charge', status: 'completed', attempts: 1 },
        { name: 'reserve', status: 'completed' },
        { name: 'shipped', status: 'completed' },
        { name: 'review-request', status: 'completed', attempts: 1 },
        { name: '$retry:1', kind: 'retry', status: 'completed', data: { from: 'failed', error: failed!.error } },
      ],
    });
    expect(world.ops()).toEqual(['charge', 'reserve', 'mail']);
    expect(node.events.find((e) => e.type === 'workflow-retried')).toEqual({
      type: 'workflow-retried',
      id: 'f-1',
      workflow: 'fulfil',
      version: 1,
      at: clock.now(),
      from: 'failed',
      error: failed!.error,
    });
    expect(types(node).slice(-3)).toEqual(['workflow-resumed', 'step-completed', 'workflow-completed']);
  });

  it('resumes an instance that a deploy broke, once the fixed code is deployed', async () => {
    @Workflow('fulfil')
    class Renamed {
      async run(ctx: WorkflowContext) {
        await ctx.step('charge-card', () => 'charged twice');
        return 'wrong';
      }
    }

    outage.mail = true;
    let node = await start([Fulfil]);
    await node.client.start(Fulfil, {}, { id: 'f-1' });
    await node.worker.drain(); // parked for the review request's retry

    node = await restart(node, [Renamed]);
    clock.advance('1s');
    await node.worker.drain();
    expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'failed', error: { name: 'WorkflowNonDeterminismError' } });

    outage.mail = false;
    node = await restart(node, [Fulfil]);
    await node.client.retry('f-1');
    await node.worker.drain();
    expect(await node.client.getStatus('f-1')).toMatchObject({ status: 'completed', output: 'done' });
    expect(world.ops()).toEqual(['charge', 'reserve', 'mail']);
  });

  it('restarts a sleep or wait the instance abandoned, and needs a new run timeout once the old one passed', async () => {
    @Workflow('approval', { timeout: '1h' })
    class Approval {
      async run(ctx: WorkflowContext) {
        await ctx.step('ask', () => 'asked');
        return ctx.waitForSignal('answer', 'approval.answer', { timeout: '1d' });
      }
    }

    const node = await start([Approval]);
    await node.client.start(Approval, undefined, { id: 'a-1' });
    await node.worker.drain();
    clock.advance('1h');
    await node.worker.drain();
    expect(await node.client.getStatus('a-1', { journal: true })).toMatchObject({
      status: 'failed',
      error: { name: 'WorkflowTimeoutError' },
      journal: [{ name: 'ask' }, { name: 'answer', status: 'cancelled' }],
    });

    await expect(node.client.retry('a-1')).rejects.toThrow(
      new WorkflowStateError('Instance "a-1" is past its run timeout, so it would time out again at once. Pass { timeout } with a new one, or false for none.'),
    );
    expect(await node.client.retry('a-1', { timeout: '2d' })).toMatchObject({ status: 'pending', deadline: clock.now() + 2 * 86_400_000 });
    await node.worker.drain();
    expect(await node.client.getStatus('a-1', { journal: true })).toMatchObject({
      status: 'suspended',
      wakeAt: clock.now() + 86_400_000,
      journal: [{ name: 'ask', status: 'completed' }, { name: 'answer', status: 'pending', wakeAt: clock.now() + 86_400_000 }, { name: '$retry:1' }],
    });

    await node.client.signal('approval.answer', 'yes');
    await node.worker.drain();
    expect(await node.client.getStatus('a-1')).toMatchObject({ status: 'completed', output: 'yes' });
  });

  it('refuses an instance whose compensations ran: its completed steps were undone', async () => {
    @Workflow('declined')
    class Declined {
      constructor(@Inject(World) private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        await ctx.step('reserve', () => 'r-1', { compensate: () => this.world.record('release', '') });
        await ctx.step('charge', () => {
          throw new NonRetryableStepError('card declined');
        });
      }
    }

    const node = await start([Declined]);
    await node.client.start(Declined, undefined, { id: 'd-1' });
    await node.worker.drain();

    await expect(node.client.retry('d-1')).rejects.toThrow(
      new WorkflowStateError(
        'Instance "d-1" failed and its compensations ran ("$compensate:reserve"): its completed steps were undone, so ' +
          'resuming it would build on undone work. Start a new instance instead.',
      ),
    );
    expect(await node.client.getStatus('d-1')).toMatchObject({ status: 'failed' });
  });
});

describe('retry() of a compensation_failed instance', () => {
  it('runs the compensations that did not complete again, and ends as it would have: failed, or cancelled', async () => {
    outage.warehouse = true;
    const node = await start([Fulfil]);
    await node.client.start(Fulfil, { cancelAfterReserve: true }, { id: 'f-1' });
    await node.worker.drain();
    await node.client.cancel('f-1', 'Customer changed their mind.');
    await node.worker.drain();

    const stuck = await node.client.getStatus('f-1', { journal: true });
    expect(stuck).toMatchObject({
      status: 'compensation_failed',
      error: { name: 'WorkflowCancelledError', message: 'Customer changed their mind.', compensation: { message: expect.stringContaining('warehouse offline') } },
      journal: [{ name: 'charge' }, { name: 'reserve' }, { name: 'hold', status: 'cancelled' }, { name: '$compensate:reserve', status: 'failed', attempts: 1 }],
    });
    expect(world.ops()).toEqual(['charge', 'reserve']);

    outage.warehouse = false;
    expect(await node.client.retry('f-1')).toMatchObject({
      status: 'compensating',
      error: { name: 'WorkflowCancelledError', message: 'Customer changed their mind.' },
    });
    await node.worker.drain();

    const done = await node.client.getStatus('f-1', { journal: true });
    expect(done).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowCancelledError', message: 'Customer changed their mind.' },
      journal: [
        { name: 'charge' },
        { name: 'reserve' },
        { name: 'hold', status: 'cancelled' },
        { name: '$compensate:reserve', status: 'completed', attempts: 1 },
        { name: '$retry:1', data: { from: 'compensation_failed' } },
        { name: '$compensate:charge', status: 'completed' },
      ],
    });
    expect(done!.error).not.toHaveProperty('compensation');
    expect(world.ops()).toEqual(['charge', 'reserve', 'release', 'refund']);
    expect(types(node).slice(-5)).toEqual(['workflow-retried', 'workflow-resumed', 'step-compensated', 'step-compensated', 'workflow-cancelled']);
  });
});

describe('retry() refusals and races', () => {
  it('refuses an unknown id, and every status but failed and compensation_failed', async () => {
    outage.mail = true;
    const node = await start([Fulfil]);
    await expect(node.client.retry('missing')).rejects.toThrow(new WorkflowNotFoundError('No workflow instance with id "missing".'));

    await node.client.start(Fulfil, {}, { id: 'f-1' });
    await expect(node.client.retry('f-1')).rejects.toThrow(
      new WorkflowStateError('Instance "f-1" is pending: only failed and compensation_failed instances can be retried. cancel() stops one that is still running.'),
    );
    await node.worker.drain();
    await expect(node.client.retry('f-1')).rejects.toThrow('Instance "f-1" is suspended: only failed');

    outage.mail = false;
    clock.advance('1s');
    await node.worker.drain();
    await expect(node.client.retry('f-1')).rejects.toThrow(
      new WorkflowStateError('Instance "f-1" is completed: only failed and compensation_failed instances can be retried.'),
    );
    await expect(node.client.retry('f-1')).rejects.toMatchObject({ status: 409 });
  });

  it('accepts one of two concurrent retries', async () => {
    outage.mail = true;
    const node = await start([Fulfil]);
    await node.client.start(Fulfil, {}, { id: 'f-1' });
    await node.worker.drain();
    clock.advance('1s');
    await node.worker.drain();

    const results = await Promise.allSettled([node.client.retry('f-1'), node.client.retry('f-1')]);
    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
    const [rejected] = results.filter((r) => r.status === 'rejected') as PromiseRejectedResult[];
    expect(rejected!.reason).toBeInstanceOf(WorkflowStateError);
    expect((await node.client.getStatus('f-1', { journal: true }))!.journal.filter((e) => e.kind === 'retry')).toHaveLength(1);
  });
});

describe('delete()', () => {
  it('deletes a finished instance with its journal, and an unfinished one only with force', async () => {
    const node = await start([Fulfil]);
    await node.client.start(Fulfil, {}, { id: 'done' });
    await node.client.start(Fulfil, { cancelAfterReserve: true }, { id: 'holding' });
    await node.worker.drain();

    await node.client.delete('done');
    expect(await node.client.getStatus('done')).toBeNull();

    await expect(node.client.delete('holding')).rejects.toThrow(
      new WorkflowStateError(
        'Instance "holding" is suspended: delete() removes finished instances. cancel() it first, or pass { force: true } to delete it without running its compensations.',
      ),
    );
    await node.client.delete('holding', { force: true });
    expect(await node.client.list()).toEqual([]);
    clock.advance('1h');
    expect(await node.worker.drain()).toBe(0);
    expect([world.count('charge'), world.count('reserve'), world.count('mail'), world.count('refund')]).toEqual([2, 2, 1, 0]);

    await expect(node.client.delete('done')).rejects.toThrow(new WorkflowNotFoundError('No workflow instance with id "done".'));
    expect(node.events.filter((e) => e.type === 'workflow-deleted')).toEqual([
      expect.objectContaining({ id: 'done', status: 'completed' }),
      expect.objectContaining({ id: 'holding', status: 'suspended' }),
    ]);
  });
});
