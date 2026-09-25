/**
 * A long step on a worker pod while other pods serve HTTP: its progress checkpoints survive
 * the pod dying mid-step, and a cancel sent to another pod reaches it through the heartbeat.
 */
import { Controller, Delete, Get, Inject, Injectable, NotFoundException, Param, Post } from '@nestjs/common';
import { adapters } from './support/adapters.js';
import {
  ManualWorkflowClock,
  Workflow,
  WorkflowClient,
  type WorkflowContext,
  type WorkflowStepContext,
  type WorkflowStore,
  type WorkflowWorkerOptions,
} from '../lib/index.js';
import { bootHttp, type HttpNode } from './http-app.js';
import { AppWorkflowStore, deferred, forever, tempDb, type TestDb, waitFor, World } from './support.js';

/** The outside world's switches for the render step, shared by every pod. */
@Injectable()
class Printer {
  /** The pod dies once this page is checkpointed. */
  crashAtPage: number | null = null;
  /** Page 1 waits for this before finishing. */
  hold: ReturnType<typeof deferred<void>> | null = null;
  started = deferred();
  crashed = deferred();

  async afterPage(page: number) {
    this.started.resolve();
    if (page === 1 && this.hold) {
      await this.hold.promise;
    }
    if (page === this.crashAtPage) {
      this.crashAtPage = null;
      this.crashed.resolve();
      await forever();
    }
  }
}

@Workflow('report')
class Report {
  constructor(
    @Inject(World) private readonly world: World,
    @Inject(Printer) private readonly printer: Printer,
  ) {}

  async run(ctx: WorkflowContext, input: { pages: number }) {
    await ctx.step('prepare', ({ idempotencyKey }) => this.world.record('prepare', idempotencyKey), {
      compensate: (_result, { idempotencyKey }) => this.world.record('discard', idempotencyKey),
    });

    const pages = await ctx.step('render', async ({ progress, heartbeat, attempt }: WorkflowStepContext<{ page: number }>) => {
      for (let page = (progress?.page ?? 0) + 1; page <= input.pages; page++) {
        this.world.record(`page-${page}`, ctx.workflowId, attempt);
        await heartbeat({ page });
        await this.printer.afterPage(page);
      }
      return input.pages;
    });

    await ctx.step('publish', ({ idempotencyKey }) => this.world.record('publish', idempotencyKey));
    return pages;
  }
}

@Controller('reports')
class ReportsController {
  constructor(private readonly workflowClient: WorkflowClient) {}

  @Post(':id')
  start(@Param('id') id: string) {
    return this.workflowClient.start(Report, { pages: 5 }, { id });
  }

  @Get(':id')
  async status(@Param('id') id: string) {
    const instance = await this.workflowClient.getStatus(id, { journal: true });
    if (!instance) {
      throw new NotFoundException();
    }

    const render = instance.journal.find((entry) => entry.name === 'render');
    return { status: instance.status, progress: render?.progress ?? null };
  }

  @Delete(':id')
  async cancel(@Param('id') id: string) {
    const { accepted } = await this.workflowClient.cancel(id, 'No longer needed.');
    return { accepted };
  }
}

describe.each(adapters)('long-running steps across pods ($name)', ({ name: adapter }) => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let world: World;
  let printer: Printer;
  const pods: HttpNode[] = [];

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    world = new World();
    printer = new Printer();
  });

  afterEach(async () => {
    for (const pod of pods.splice(0)) {
      await pod.close();
    }
    db.cleanup();
  });

  const boot = async (worker: WorkflowWorkerOptions) => {
    const pod = await bootHttp(adapter, {
      db,
      clock,
      worker,
      workflows: [Report],
      providers: [
        { provide: World, useValue: world },
        { provide: Printer, useValue: printer },
      ],
      controllers: [ReportsController],
    });
    pods.push(pod);
    return pod;
  };

  const pages = () => world.calls.filter((call) => call.op.startsWith('page-')).map((call) => `${call.op}#${call.attempt}`);

  it('resumes the step from its last checkpoint on another pod after the first died', async () => {
    printer.crashAtPage = 3;
    const first = await boot({ enabled: true, pollInterval: '20ms' });
    await first.http('POST', '/reports/r1');
    await printer.crashed.promise;
    pods.splice(pods.indexOf(first), 1);
    await first.close();

    const second = await boot({ enabled: true, pollInterval: '20ms' });
    expect((await second.http('GET', '/reports/r1')).body).toEqual({ status: 'running', progress: { page: 3 } });

    clock.advance('31s');
    await waitFor(async () => (await second.http('GET', '/reports/r1')).body.status === 'completed');
    expect(pages()).toEqual(['page-1#1', 'page-2#1', 'page-3#1', 'page-4#2', 'page-5#2']);
    expect(world.count('prepare')).toBe(1);
    expect(await second.client.getStatus('r1')).toMatchObject({ output: 5 });
  });

  it('stops at the next step after a cancel sent to another pod, once the heartbeat has read it', async () => {
    printer.hold = deferred();
    const worker = await boot({ enabled: true, pollInterval: '20ms', leaseDuration: '30s', heartbeatInterval: '20ms' });
    const api = await boot({ enabled: false });

    const store = worker.app.get<WorkflowStore>(AppWorkflowStore);
    const renew = store.renew.bind(store);
    let noticed = false;
    vi.spyOn(store, 'renew').mockImplementation(async (...args) => {
      const result = await renew(...args);
      noticed ||= result?.cancelRequested === true;
      return result;
    });

    await api.http('POST', '/reports/r1');
    await printer.started.promise;
    expect((await api.http('DELETE', '/reports/r1')).body).toEqual({ accepted: true });
    await waitFor(() => noticed);
    printer.hold.resolve(); // the step in flight finishes and is journaled

    await waitFor(async () => (await api.http('GET', '/reports/r1')).body.status === 'cancelled');
    expect(world.count('publish')).toBe(0);
    expect(world.ops()).toEqual(['prepare', 'page-1', 'page-2', 'page-3', 'page-4', 'page-5', 'discard']);
    expect(worker.events.map((event) => event.type).slice(-3)).toEqual(['workflow-compensating', 'step-compensated', 'workflow-cancelled']);
  });
});
