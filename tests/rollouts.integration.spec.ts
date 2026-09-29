/**
 * Declared schedules in a rolling deploy: processes of the old and of the new code side by side on one database,
 * started in either order. A process leaves a schedule the code of a running process declares, whatever its own
 * code says, and a newer version's declaration as it is; a declaration no running code confirms any more goes, or
 * is taken over, within minutes, whether or not the process runs its workflow. Every running process drains at least
 * once a minute of the clock, as a live worker polls.
 */
import { type Type } from '@nestjs/common';
import { ManualWorkflowClock, Workflow, type WorkflowContext, type WorkflowScheduleSkippedEvent } from '../lib/index.js';
import { boot, tempDb, World, type Node, type TestDb } from './support.js';

const T0 = Date.UTC(2026, 0, 1);
const minutes = (n: number) => T0 + n * 60_000;
const iso = (at: number) => new Date(at).toISOString();

/** What every version of the workflow does: records the occurrence that started it. */
async function report(world: World, ctx: WorkflowContext) {
  const at = iso(ctx.schedule!.at);
  await ctx.step('report', () => world.record('report', at));
}

@Workflow('stock-report')
class StockReportV1 {
  constructor(private readonly world: World) {}

  run(ctx: WorkflowContext) {
    return report(this.world, ctx);
  }
}

@Workflow('stock-report', { schedules: [{ id: 'stock-report', cron: '0 * * * *' }] })
class StockReportV1Hourly {
  constructor(private readonly world: World) {}

  run(ctx: WorkflowContext) {
    return report(this.world, ctx);
  }
}

@Workflow('stock-report', { version: 2, schedules: [{ id: 'stock-report', cron: '30 * * * *' }] })
class StockReportV2HalfPast {
  constructor(private readonly world: World) {}

  run(ctx: WorkflowContext) {
    return report(this.world, ctx);
  }
}

@Workflow('stock-report', { version: 2 })
class StockReportV2 {
  constructor(private readonly world: World) {}

  run(ctx: WorkflowContext) {
    return report(this.world, ctx);
  }
}

/** A workflow of code without the stock report at all: the new code, or another service on the database. */
@Workflow('stock-alert')
class StockAlert {
  async run() {}
}

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
const pods: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock(T0);
  world = new World();
});

afterEach(async () => {
  for (const pod of pods.splice(0)) {
    await pod.close();
  }
  db.cleanup();
});

async function start(workflows: Type<unknown>[]) {
  const pod = await boot({ db, clock, workflows, providers: [{ provide: World, useValue: world }] });
  pods.push(pod);
  return pod;
}

async function stop(pod: Node) {
  pods.splice(pods.indexOf(pod), 1);
  await pod.close();
}

/** Moves the clock to `at`, every running pod draining a minute before and at it, as live workers do. */
async function runUntil(at: number) {
  for (const time of [at - 60_000, at]) {
    clock.set(time);
    for (const pod of pods) {
      await pod.worker.drain();
    }
  }
}

const reports = () => world.calls.map((call) => call.key);
const schedule = () => pods[0]!.client.schedules.get('stock-report');

describe('a declared schedule in a rolling deploy', () => {
  it.each([
    ['of the same version', [StockReportV1Hourly], minutes(60)],
    ['of a new version', [StockReportV1, StockReportV2HalfPast], minutes(30)],
  ])('keeps starting when a pod of the old code restarts after one of the new code %s', async (_, newCode, first) => {
    const fresh = await start(newCode);
    expect(await schedule()).toMatchObject({ declared: true, nextAt: first });
    await start([StockReportV1]);
    expect(await schedule()).toMatchObject({ nextAt: first });

    await runUntil(first);
    // The old code restarts again, mid-way: the new code's schedule stays.
    await stop(pods[1]!);
    await start([StockReportV1]);
    await runUntil(first + 3_600_000);

    expect(reports()).toEqual([iso(first), iso(first + 3_600_000)]);
    expect(await fresh.client.schedules.get('stock-report')).toMatchObject({ runs: 2, nextAt: first + 7_200_000 });
  });

  it('keeps starting when a pod of the new code starts after one of the old code, whose reconciling leaves it', async () => {
    await start([StockReportV1]);
    await start([StockReportV1Hourly]);
    await runUntil(minutes(60));
    await runUntil(minutes(120));

    expect(reports()).toEqual([iso(minutes(60)), iso(minutes(120))]);
  });

  it("leaves a newer version's declaration as it is when a pod of the old code restarts, and takes it back after a rollback", async () => {
    await start([StockReportV1Hourly]);
    const fresh = await start([StockReportV1, StockReportV2HalfPast]);
    expect(await schedule()).toMatchObject({ cron: '30 * * * *', nextAt: minutes(30) });

    await stop(pods[0]!);
    await start([StockReportV1Hourly]);
    expect(await schedule()).toMatchObject({ cron: '30 * * * *', nextAt: minutes(30) });
    await runUntil(minutes(30));

    // Rolled back: no pod runs version 2 any more, and five minutes later the old declaration is back.
    await stop(fresh);
    await runUntil(minutes(34));
    expect(await schedule()).toMatchObject({ cron: '30 * * * *' });
    await runUntil(minutes(36));
    expect(await schedule()).toMatchObject({ cron: '0 * * * *', nextAt: minutes(60) });
    await runUntil(minutes(60));

    expect(reports()).toEqual([iso(minutes(30)), iso(minutes(60))]);
  });

  it('removes a declaration a new version dropped once no pod of the old code has run for five minutes, not before', async () => {
    const old = await start([StockReportV1Hourly]);
    const fresh = await start([StockReportV1, StockReportV2]);
    expect(await schedule()).toMatchObject({ declared: true, cron: '0 * * * *' });

    // The old code still runs: it declares the schedule, and starts its occurrences.
    await runUntil(minutes(60));
    expect(reports()).toEqual([iso(minutes(60))]);
    await stop(old);

    await runUntil(minutes(64));
    expect(await fresh.client.schedules.get('stock-report')).toMatchObject({ declared: true });
    await runUntil(minutes(66));
    expect(await fresh.client.schedules.get('stock-report')).toBeNull();
    await runUntil(minutes(120));

    expect(reports()).toEqual([iso(minutes(60))]);
    expect(fresh.events.filter((event): event is WorkflowScheduleSkippedEvent => event.type === 'schedule-skipped')).toEqual([]);
  });

  it('removes the declaration of a workflow the new code no longer has once no pod of the old code has run for five minutes', async () => {
    const old = await start([StockReportV1Hourly]);
    const fresh = await start([StockAlert]);

    // The old code still runs: it declares the schedule, and starts its occurrences.
    await runUntil(minutes(60));
    expect(reports()).toEqual([iso(minutes(60))]);
    await stop(old);

    await runUntil(minutes(64));
    expect(await fresh.client.schedules.get('stock-report')).toMatchObject({ declared: true });
    await runUntil(minutes(66));
    expect(await fresh.client.schedules.get('stock-report')).toBeNull();
    await runUntil(minutes(120));

    expect(reports()).toEqual([iso(minutes(60))]);
    expect(await fresh.client.list({ scheduleId: 'stock-report' })).toMatchObject([{ id: `stock-report@${iso(minutes(60))}`, status: 'completed' }]);
  });

  it('keeps a schedule while the pods that declare it are gone for less than five minutes, and saves it again as the next one starts', async () => {
    let pool = await start([StockReportV1Hourly]);
    await start([StockAlert]);
    await runUntil(minutes(60));

    // Scaled to zero for four minutes: the schedule keeps its progress.
    await stop(pool);
    await runUntil(minutes(64));
    pool = await start([StockReportV1Hourly]);
    expect(await schedule()).toMatchObject({ runs: 1, nextAt: minutes(120) });
    await runUntil(minutes(120));

    // For five: another pod removes it, and the pool's next pod saves it again, from its next occurrence.
    await stop(pool);
    await runUntil(minutes(126));
    expect(await schedule()).toBeNull();
    await start([StockReportV1Hourly]);
    expect(await schedule()).toMatchObject({ declared: true, runs: 0, nextAt: minutes(180) });
    await runUntil(minutes(180));

    expect(reports()).toEqual([iso(minutes(60)), iso(minutes(120)), iso(minutes(180))]);
  });
});
