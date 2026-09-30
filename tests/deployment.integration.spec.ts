/**
 * Several application instances ("pods") on one database, each serving HTTP and some running
 * the polling worker: a pod that dies mid-step, a pod shut down by a deploy, two workers racing
 * for the same instances, and API pods that start and signal while worker pods execute.
 */
import { setTimeout as sleep } from 'node:timers/promises';
import { Body, Controller, Get, HttpCode, Inject, Injectable, NotFoundException, Param, Post } from '@nestjs/common';
import { adapters } from './support/adapters.js';
import { ManualWorkflowClock, Workflow, WorkflowClient, WorkflowSignal, type WorkflowContext, type WorkflowWorkerOptions } from '../lib/index.js';
import { bootHttp, type HttpNode } from './http-app.js';
import { deferred, forever, tempDb, type TestDb, waitFor, World } from './support.js';

const approved = new WorkflowSignal<{ batchId: string; by: string }>('batch.approved');

/** Where a step hangs, shared by every pod as the outside world is. */
@Injectable()
class Gate {
  hangAt: string | null = null;
  /** A hanging step that honours its abort signal (a graceful shutdown) instead of hanging on. */
  abortable = false;
  reached = deferred();

  async maybeHang(step: string, signal: AbortSignal) {
    if (this.hangAt !== step) {
      return;
    }

    this.hangAt = null;
    this.reached.resolve();
    if (!this.abortable) {
      return forever();
    }
    await new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }));
  }
}

@Workflow('batch-import')
class BatchImport {
  constructor(
    @Inject(World) private readonly world: World,
    @Inject(Gate) private readonly gate: Gate,
  ) {}

  async run(ctx: WorkflowContext, input: { rows: number }) {
    const done: string[] = [];
    for (const name of ['download', 'validate', 'transform', 'load']) {
      done.push(
        await ctx.step(name, async ({ idempotencyKey, attempt, signal }) => {
          this.world.record(name, idempotencyKey, attempt);
          await this.gate.maybeHang(name, signal);
          return `${name}:${input.rows}`;
        }),
      );
    }

    const approval = await ctx.waitForSignal('approval', approved, { key: ctx.workflowId });
    await ctx.step('publish', ({ idempotencyKey }) => this.world.record('publish', idempotencyKey));
    return { done, approvedBy: approval!.by };
  }
}

@Controller()
class BatchesController {
  constructor(private readonly workflowClient: WorkflowClient) {}

  @Post('batches/:id')
  start(@Param('id') id: string, @Body() body: { rows: number }) {
    return this.workflowClient.start(BatchImport, body, { id });
  }

  @Get('batches/:id')
  async status(@Param('id') id: string) {
    const instance = await this.workflowClient.getStatus(id, { journal: true });
    if (!instance) {
      throw new NotFoundException();
    }

    const steps = Object.fromEntries(instance.journal.map((entry) => [entry.name, entry.status]));
    return { status: instance.status, runs: instance.runs, leaseOwner: instance.leaseOwner, steps };
  }

  @Post('webhooks/approvals')
  @HttpCode(200)
  approve(@Body() body: { batchId: string; by: string }) {
    return this.workflowClient.signal(approved, body, { key: body.batchId });
  }
}

describe.each(adapters)('pods on one database ($name)', ({ name: adapter }) => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let world: World;
  let gate: Gate;
  const pods: HttpNode[] = [];

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    world = new World();
    gate = new Gate();
  });

  afterEach(async () => {
    for (const pod of pods.splice(0)) {
      await pod.close();
    }
    db.cleanup();
  });

  const boot = async (options: { worker?: WorkflowWorkerOptions; workflows?: boolean } = {}) => {
    const pod = await bootHttp(adapter, {
      db,
      clock,
      worker: options.worker,
      workflows: options.workflows === false ? [] : [BatchImport],
      providers: [
        { provide: World, useValue: world },
        { provide: Gate, useValue: gate },
      ],
      controllers: [BatchesController],
    });
    pods.push(pod);
    return pod;
  };

  /** A worker pod: polls every 20ms and shows `id` as the lease owner. */
  const workerPod = (id: string) => boot({ worker: { enabled: true, id, pollInterval: '20ms' } });

  const stop = async (pod: HttpNode) => {
    pods.splice(pods.indexOf(pod), 1);
    await pod.close();
  };

  const statusOf = async (pod: HttpNode, id: string) => (await pod.http('GET', `/batches/${id}`)).body;

  it('resumes on a second pod after the first died mid-step, once its lease expired, without re-running finished steps', async () => {
    gate.hangAt = 'transform';
    const first = await workerPod('pod-a');
    await first.http('POST', '/batches/b1', { rows: 3 });
    await gate.reached.promise;
    await stop(first); // the step never returns: shutdown gives up on it, and the lease stays

    const second = await workerPod('pod-b');
    expect(await statusOf(second, 'b1')).toEqual({
      status: 'running',
      runs: 1,
      leaseOwner: 'pod-a',
      steps: { download: 'completed', validate: 'completed', transform: 'pending' },
    });
    await sleep(100); // five polls: the dead pod's lease is still valid
    expect(world.count('transform')).toBe(1);

    clock.advance('31s');
    await waitFor(async () => (await statusOf(second, 'b1')).status === 'suspended');
    expect((await second.http('POST', '/webhooks/approvals', { batchId: 'b1', by: 'ops' })).body).toEqual({ signalId: 1, woken: 1, created: true });
    await waitFor(async () => (await statusOf(second, 'b1')).status === 'completed');

    expect(await statusOf(second, 'b1')).toMatchObject({ runs: 3, leaseOwner: 'pod-b' });
    expect(world.calls).toEqual([
      { op: 'download', key: 'b1:download', attempt: 1 },
      { op: 'validate', key: 'b1:validate', attempt: 1 },
      { op: 'transform', key: 'b1:transform', attempt: 1 },
      { op: 'transform', key: 'b1:transform', attempt: 2 },
      { op: 'load', key: 'b1:load', attempt: 1 },
      { op: 'publish', key: 'b1:publish', attempt: undefined },
    ]);
    expect(await second.client.getStatus('b1')).toMatchObject({
      output: { done: ['download:3', 'validate:3', 'transform:3', 'load:3'], approvedBy: 'ops' },
    });
    expect(second.events[0]).toMatchObject({ type: 'workflow-resumed', id: 'b1', run: 2 });
  });

  it('hands a running instance back when a deploy shuts the pod down, so the next pod runs it at once', async () => {
    gate.hangAt = 'validate';
    gate.abortable = true;
    const first = await workerPod('pod-a');
    await first.http('POST', '/batches/b1', { rows: 1 });
    await gate.reached.promise;
    await stop(first);

    const second = await workerPod('pod-b');
    await waitFor(async () => (await statusOf(second, 'b1')).status === 'suspended'); // the clock never moved

    // The aborted attempt was given back: the retry is attempt 1 again.
    expect(world.calls.filter((call) => call.op === 'validate').map((call) => call.attempt)).toEqual([1, 1]);
    expect(world.count('download')).toBe(1);
  });

  it('runs each step once when two worker pods race for the same instances', async () => {
    const racers = [await workerPod('pod-a'), await workerPod('pod-b')];
    const ids = Array.from({ length: 10 }, (_, i) => `b${i}`);

    for (const [i, id] of ids.entries()) {
      await racers[i % 2]!.http('POST', `/batches/${id}`, { rows: i });
    }
    await waitFor(async () => (await racers[0]!.client.list({ status: 'suspended' })).length === ids.length, 10_000);
    for (const [i, id] of ids.entries()) {
      await racers[(i + 1) % 2]!.http('POST', '/webhooks/approvals', { batchId: id, by: 'ops' });
    }
    await waitFor(async () => (await racers[1]!.client.list({ status: 'completed' })).length === ids.length, 10_000);

    for (const op of ['download', 'validate', 'transform', 'load', 'publish']) {
      const keys = world.calls.filter((call) => call.op === op).map((call) => call.key);
      expect(keys.sort()).toEqual(ids.map((id) => `${id}:${op}`).sort());
    }
    // One execution per due moment (the start, the approval): no instance was claimed twice.
    const instances = await racers[0]!.client.list({ limit: 20 });
    expect(instances.map((instance) => instance.runs)).toEqual(ids.map(() => 2));
  });

  it('lets API pods start and signal while worker pods execute', async () => {
    const api = await boot({ worker: { enabled: false }, workflows: false }); // registers no workflow
    const worker = await workerPod('worker-1');

    const started = await api.http('POST', '/batches/b1', { rows: 2 });
    expect(started.body).toEqual({ id: 'b1', workflow: 'batch-import', version: 1, created: true, status: 'pending' });
    expect(await api.worker.drain()).toBe(0); // it has nothing it could claim

    await waitFor(async () => (await statusOf(api, 'b1')).status === 'suspended');
    expect((await api.http('POST', '/webhooks/approvals', { batchId: 'b1', by: 'api' })).body).toEqual({ signalId: 1, woken: 1, created: true });
    await waitFor(async () => (await statusOf(api, 'b1')).status === 'completed'); // the worker pod's next poll

    expect(await statusOf(api, 'b1')).toMatchObject({ runs: 2, leaseOwner: 'worker-1' });
    expect(api.events).toEqual([]);
    // The worker emits its events once its writes commit: the API pod can read the status first.
    await waitFor(() => worker.events.some((event) => event.type === 'workflow-completed'));
  });
});
