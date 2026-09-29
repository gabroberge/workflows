/**
 * Scheduled jobs next to workflows, guarded by `@OnOneInstance()` from `@nestjs/locks`
 * (https://docs.nestjs.com/reliability/locks) on every pod of the app: one that starts a
 * workflow per day, and the retention job of the workflows page's production checklist. The
 * jobs are triggered the way the locks page allows besides `@Cron()`: a plain call from an
 * admin route. The per-day instance id makes a second run of the same day a no-op anyway.
 */
import { Controller, Inject, Injectable, Param, Post } from '@nestjs/common';
import { adapters } from './support/adapters.js';
import { InMemoryLockStore, LocksModule, LocksStorage, ManualLockClock, OnOneInstance } from '@nestjs/locks';
import { ManualWorkflowClock, Workflow, WorkflowClient, type WorkflowContext, type WorkflowStartResult } from '../lib/index.js';
import { bootHttp, type HttpNode } from './http-app.js';
import { tempDb, type TestDb, World } from './support.js';

@Workflow('invoice-run')
class InvoiceRun {
  constructor(@Inject(World) private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { day: string }) {
    await ctx.step('export', ({ idempotencyKey }) => this.world.record('export', idempotencyKey));
    return input.day;
  }
}

@Injectable()
class NightlyInvoicesJob {
  constructor(private readonly workflowClient: WorkflowClient) {}

  // With @nestjs/schedule: @Cron('0 2 * * *') here too.
  @OnOneInstance({ key: 'invoices:nightly' })
  async run(day: string): Promise<WorkflowStartResult> {
    return this.workflowClient.start(InvoiceRun, { day }, { id: `invoices-${day}` });
  }
}

/** Retention, as the docs' production checklist runs it: nightly, on one pod. */
@Injectable()
class WorkflowRetentionJob {
  constructor(private readonly workflowClient: WorkflowClient) {}

  // With @nestjs/schedule: @Cron('30 3 * * *') here too.
  @OnOneInstance({ key: 'workflows:purge' })
  async purge() {
    return this.workflowClient.purge({ olderThan: '30d' });
  }
}

@Controller('admin/jobs')
class JobsController {
  constructor(
    private readonly nightlyInvoicesJob: NightlyInvoicesJob,
    private readonly workflowRetentionJob: WorkflowRetentionJob,
  ) {}

  @Post('workflow-retention')
  async retention() {
    const purged = await this.workflowRetentionJob.purge();
    return purged === undefined ? { ran: false } : { ran: true, ...purged };
  }

  @Post('nightly-invoices/:day')
  async trigger(@Param('day') day: string) {
    const started = await this.nightlyInvoicesJob.run(day);
    return started === undefined ? { ran: false } : { ran: true, created: started.created, id: started.id };
  }
}

describe.each(adapters)('a scheduled job that starts workflows on one pod ($name)', ({ name: adapter }) => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let lockClock: ManualLockClock;
  let lockStore: InMemoryLockStore;
  let world: World;
  const pods: HttpNode[] = [];

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    lockClock = new ManualLockClock();
    lockStore = new InMemoryLockStore({ clock: lockClock });
    world = new World();
  });

  afterEach(async () => {
    for (const pod of pods.splice(0)) {
      await pod.close();
    }
    db.cleanup();
  });

  const boot = async () => {
    const pod = await bootHttp(adapter, {
      db,
      clock,
      imports: [LocksModule.forRoot({ clock: lockClock })],
      workflows: [InvoiceRun],
      providers: [{ provide: World, useValue: world }, NightlyInvoicesJob, WorkflowRetentionJob],
      controllers: [JobsController],
      setup: (app) => app.get(LocksStorage).registerSource(lockStore, { replace: true }),
    });
    pods.push(pod);
    return pod;
  };

  it('starts the day’s run on the pod that owns the job, skips it on the others, and starts it once', async () => {
    const [a, b] = [await boot(), await boot()];

    expect((await a.http('POST', '/admin/jobs/nightly-invoices/2026-01-01')).body).toEqual({ ran: true, created: true, id: 'invoices-2026-01-01' });
    expect((await b.http('POST', '/admin/jobs/nightly-invoices/2026-01-01')).body).toEqual({ ran: false });
    expect((await a.http('POST', '/admin/jobs/nightly-invoices/2026-01-01')).body).toEqual({ ran: true, created: false, id: 'invoices-2026-01-01' });

    await a.worker.drain();
    await b.worker.drain();
    expect(await a.client.list()).toMatchObject([{ id: 'invoices-2026-01-01', status: 'completed', output: '2026-01-01' }]);
    expect(world.count('export')).toBe(1);
  });

  it('moves the job to another pod when its owner shuts down, and the next day runs there', async () => {
    const [a, b] = [await boot(), await boot()];
    await a.http('POST', '/admin/jobs/nightly-invoices/2026-01-01');

    await lockClock.advance('2s'); // a lease taken less than a second ago is kept through shutdown
    pods.splice(pods.indexOf(a), 1);
    await a.close();

    expect((await b.http('POST', '/admin/jobs/nightly-invoices/2026-01-02')).body).toEqual({ ran: true, created: true, id: 'invoices-2026-01-02' });
    await b.worker.drain();
    expect((await b.client.list({ status: 'completed' })).map((instance) => instance.id)).toEqual(['invoices-2026-01-01', 'invoices-2026-01-02']);
  });

  it('purges finished runs older than 30 days on the pod that owns the retention job', async () => {
    const [a, b] = [await boot(), await boot()];
    await a.http('POST', '/admin/jobs/nightly-invoices/2026-01-01');
    await a.worker.drain();
    clock.advance('31d');
    await a.http('POST', '/admin/jobs/nightly-invoices/2026-02-01');
    await a.worker.drain();

    expect((await a.http('POST', '/admin/jobs/workflow-retention')).body).toEqual({ ran: true, instances: 1, signals: 0, rateLimits: 0 });
    expect((await b.http('POST', '/admin/jobs/workflow-retention')).body).toEqual({ ran: false });
    expect((await a.client.list()).map((instance) => instance.id)).toEqual(['invoices-2026-02-01']);
  });
});
