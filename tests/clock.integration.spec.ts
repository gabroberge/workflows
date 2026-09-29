/**
 * The manual clock through the whole engine, across processes and restarts: schedules over the days the clocks
 * change in a real time zone, occurrences missed during a downtime, each overlap mode while another process runs
 * the previous occurrence, bounds, and pausing, resuming, triggering and previewing through `WorkflowSchedules`
 * over HTTP; rate-limit windows that open with the first execution after the last one ended; waitForAny() timers
 * against signals sent before the wait was reached; and result() timing out while the instance runs on.
 */
import { Controller, Get, HttpCode, Injectable, Param, Post, Query, type Type } from '@nestjs/common';
import {
  ManualWorkflowClock,
  Workflow,
  WorkflowResultTimeoutError,
  WorkflowSchedules,
  WorkflowSignal,
  type WorkflowContext,
  type WorkflowScheduleSkippedEvent,
  type WorkflowWorkerOptions,
} from '../lib/index.js';
import { bootHttp, type HttpNode } from './http-app.js';
import { boot, deferred, heartbeatRead, tempDb, waitFor, World, type Node, type TestDb } from './support.js';

const T0 = Date.UTC(2026, 0, 1);
const hours = (n: number) => T0 + n * 3_600_000;
const minutes = (n: number) => T0 + n * 60_000;
const iso = (at: number) => new Date(at).toISOString();
const replied = new WorkflowSignal<{ text: string }>('ticket.replied');
const exported = new WorkflowSignal<null>('export.done');

/** Holds the calls that pass one of `holding` until `open()`. */
@Injectable()
class Gate {
  readonly holding = new Set<string>();
  readonly reached: string[] = [];
  private readonly opened = deferred();

  async pass(key: string) {
    if (this.holding.has(key)) {
      this.reached.push(key);
      await this.opened.promise;
    }
  }

  open() {
    this.opened.resolve();
  }
}

@Workflow('warsaw-nightly', { schedules: [{ id: 'nightly', cron: '0 30 2 * * *', tz: 'Europe/Warsaw' }] })
class WarsawNightlyWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    const { id, at } = ctx.schedule!;
    await ctx.step('report', () => this.world.record(id, iso(at)));
  }
}

@Workflow('warsaw-hourly', { schedules: [{ id: 'hourly', cron: '0 * * * *', tz: 'Europe/Warsaw', missed: 'all', overlap: 'allow' }] })
class WarsawHourlyWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    const { id, at } = ctx.schedule!;
    await ctx.step('report', () => this.world.record(id, iso(at)));
  }
}

@Workflow('reminders', {
  schedules: [
    { id: 'skip-missed', every: '1h', missed: 'skip' },
    { id: 'once-missed', every: '1h', missed: 'once' },
    { id: 'all-missed', every: '1h', missed: 'all', overlap: 'allow' },
  ],
})
class RemindersWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    const { id, at } = ctx.schedule!;
    await ctx.step('remind', () => this.world.record(id, iso(at)));
  }
}

@Workflow('long-job')
class LongJobWorkflow {
  constructor(
    private readonly world: World,
    private readonly gate: Gate,
  ) {}

  async run(ctx: WorkflowContext) {
    const id = ctx.workflowId;
    const at = iso(ctx.schedule!.at);
    await ctx.step(
      'work',
      async () => {
        await this.gate.pass(at);
        this.world.record('work', id);
      },
      { compensate: () => this.world.record('undo-work', id) },
    );
    await ctx.step('finish', () => this.world.record('finish', id));
  }
}

@Workflow('stocktake', {
  schedules: [
    { id: 'stocktake', every: '1h', startAt: hours(2), endAt: hours(6), limit: 3 },
    { id: 'shelf-check', cron: '0 * * * *', startAt: hours(2.5), endAt: hours(4) },
  ],
})
class StocktakeWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    const { id, at } = ctx.schedule!;
    await ctx.step('count', () => this.world.record(id, iso(at)));
  }
}

@Workflow('nightly-export', { schedules: [{ id: 'nightly-export', every: '1h', input: { target: 'warehouse' } }] })
class NightlyExportWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { target: string }) {
    const at = iso(ctx.schedule!.at);
    await ctx.step('export', () => this.world.record('export', `${at} ${input.target}`));
    await ctx.waitForSignal('done', exported, { key: ctx.workflowId });
  }
}

@Workflow('ping', { rateLimit: { max: 2, duration: '1m' } })
class PingWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    this.world.record('ping', ctx.workflowId);
  }
}

@Workflow('support-ticket')
class SupportTicketWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { ticket: string }) {
    await ctx.step('acknowledge', () => this.world.record('acknowledge', input.ticket));
    await ctx.sleep('triage', '2h');
    const outcome = await ctx.waitForAny('reply-or-close', { reply: ctx.signalWait(replied, { key: input.ticket }), close: ctx.timer('1h') });
    return outcome.key === 'reply' ? `replied: ${outcome.value.text}` : 'closed';
  }
}

@Workflow('quote')
class QuoteWorkflow {
  constructor(private readonly gate: Gate) {}

  async run(ctx: WorkflowContext, input: { items: number }) {
    const id = ctx.workflowId;
    return ctx.step('price', async () => {
      await this.gate.pass(id);
      return input.items * 1_299;
    });
  }
}

@Controller('schedules')
class SchedulesController {
  constructor(private readonly schedules: WorkflowSchedules) {}

  @Get(':id')
  get(@Param('id') id: string) {
    return this.schedules.get(id);
  }

  @Get(':id/preview')
  preview(@Param('id') id: string, @Query('count') count: string) {
    return this.schedules.preview(id, { count: Number(count) });
  }

  @Post(':id/pause')
  @HttpCode(200)
  pause(@Param('id') id: string) {
    return this.schedules.pause(id);
  }

  @Post(':id/resume')
  @HttpCode(200)
  resume(@Param('id') id: string) {
    return this.schedules.resume(id);
  }

  @Post(':id/trigger')
  trigger(@Param('id') id: string) {
    return this.schedules.trigger(id);
  }
}

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
let gate: Gate;
const nodes: Array<Node | HttpNode> = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock(T0);
  world = new World();
  gate = new Gate();
});

afterEach(async () => {
  vi.restoreAllMocks();
  gate.open();
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

const providers = () => [
  { provide: World, useValue: world },
  { provide: Gate, useValue: gate },
];

async function start(workflows: Type<unknown>[], worker: WorkflowWorkerOptions = {}) {
  const node = await boot({ db, clock, workflows, worker, providers: providers() });
  nodes.push(node);
  return node;
}

async function stop(node: Node | HttpNode) {
  nodes.splice(nodes.indexOf(node), 1);
  await node.close();
}

/** The occurrences a schedule's instances recorded, as ISO times, in order. */
const ran = (schedule: string) =>
  world.calls
    .filter((call) => call.op === schedule)
    .map((call) => call.key)
    .sort();

const skipped = (node: Node) =>
  node.events
    .filter((event): event is WorkflowScheduleSkippedEvent => event.type === 'schedule-skipped')
    .map(({ id, reason, from, to }) => ({ id, reason, from, to }));

/**
 * Moves the clock to each occurrence of the nightly schedule until `until`, then to `until`, draining at each: the
 * nightly one runs on time, the hourly one catches up the hours in between (`missed: 'all'`).
 */
async function nightsUntil(node: Node, until: number) {
  for (;;) {
    const next = (await node.client.schedules.get('nightly'))!.nextAt!;
    if (next > until) {
      break;
    }

    clock.set(next);
    await node.worker.drain();
  }

  clock.set(until);
  await node.worker.drain();
}

describe('schedules in a time zone with daylight saving time', () => {
  it('run a time the clocks skip that much later, once, on the day they go forward (Europe/Warsaw, 29 March 2026)', async () => {
    clock.set(Date.UTC(2026, 2, 28));
    const node = await start([WarsawNightlyWorkflow, WarsawHourlyWorkflow]);
    await nightsUntil(node, Date.UTC(2026, 2, 30, 6));

    // 02:30 CET, then 03:30 CEST (02:30 doesn't exist that day), then 02:30 CEST.
    expect(ran('nightly')).toEqual([Date.UTC(2026, 2, 28, 1, 30), Date.UTC(2026, 2, 29, 1, 30), Date.UTC(2026, 2, 30, 0, 30)].map(iso));
    const hourly = ran('hourly');
    expect(new Set(hourly).size).toBe(hourly.length);
    // Local 01:00 CET, then 02:00, which doesn't exist and runs at 03:00 CEST, once, then 04:00 CEST.
    expect(hourly.filter((at) => at >= iso(Date.UTC(2026, 2, 29, 0)) && at <= iso(Date.UTC(2026, 2, 29, 2)))).toEqual(
      [Date.UTC(2026, 2, 29, 0), Date.UTC(2026, 2, 29, 1), Date.UTC(2026, 2, 29, 2)].map(iso),
    );
    // The local day is 23 hours long, and runs 23 times.
    expect(hourly.filter((at) => at >= iso(Date.UTC(2026, 2, 28, 23)) && at < iso(Date.UTC(2026, 2, 29, 22)))).toHaveLength(23);
    expect(skipped(node)).toEqual([]);
  });

  it('run a time that happens twice once, the first time, on the day they go back (Europe/Warsaw, 25 October 2026)', async () => {
    clock.set(Date.UTC(2026, 9, 24));
    const node = await start([WarsawNightlyWorkflow, WarsawHourlyWorkflow]);
    await nightsUntil(node, Date.UTC(2026, 9, 26, 6));

    // 02:30 CEST, then the first 02:30 of the 25th (CEST), then 02:30 CET.
    expect(ran('nightly')).toEqual([Date.UTC(2026, 9, 24, 0, 30), Date.UTC(2026, 9, 25, 0, 30), Date.UTC(2026, 9, 26, 1, 30)].map(iso));
    const hourly = ran('hourly');
    expect(new Set(hourly).size).toBe(hourly.length);
    // Local 02:00 CEST, then 02:00 CET an hour later, which doesn't run again, then 03:00 CET.
    expect(hourly.filter((at) => at >= iso(Date.UTC(2026, 9, 24, 23)) && at <= iso(Date.UTC(2026, 9, 25, 3)))).toEqual(
      [Date.UTC(2026, 9, 24, 23), Date.UTC(2026, 9, 25, 0), Date.UTC(2026, 9, 25, 2), Date.UTC(2026, 9, 25, 3)].map(iso),
    );
    // The local day is 25 hours long, and runs 24 times.
    expect(hourly.filter((at) => at >= iso(Date.UTC(2026, 9, 24, 22)) && at < iso(Date.UTC(2026, 9, 25, 23)))).toHaveLength(24);
    expect(skipped(node)).toEqual([]);
  });
});

describe('occurrences missed during a downtime', () => {
  it('are skipped, started once or all started by the next process, with the time they were due', async () => {
    const first = await start([RemindersWorkflow]);
    clock.set(hours(1));
    await first.worker.drain();
    await stop(first);

    // Down from 1:00 to 5:30.
    clock.set(hours(5.5));
    const second = await start([RemindersWorkflow]);
    await second.worker.drain();
    expect(ran('skip-missed')).toEqual([iso(hours(1))]);
    expect(ran('once-missed')).toEqual([iso(hours(1)), iso(hours(5))]);
    expect(ran('all-missed')).toEqual([1, 2, 3, 4, 5].map((n) => iso(hours(n))));
    expect(skipped(second)).toEqual([
      { id: 'once-missed', reason: 'missed', from: hours(2), to: hours(4) },
      { id: 'skip-missed', reason: 'missed', from: hours(2), to: hours(5) },
    ]);

    // Found within a minute of its time, an occurrence is on time, 'skip' or not.
    clock.set(hours(6) + 59_000);
    await second.worker.drain();
    expect(ran('skip-missed')).toEqual([iso(hours(1)), iso(hours(6))]);
    expect((await second.client.schedules.list()).map((schedule) => [schedule.id, schedule.runs, schedule.nextAt])).toEqual([
      ['all-missed', 6, hours(7)],
      ['once-missed', 3, hours(7)],
      ['skip-missed', 2, hours(7)],
    ]);
  });
});

describe('overlapping occurrences', () => {
  it('skip, allow, buffer or cancel an instance that another process still runs', async () => {
    const api = await start([]);
    // Its leases outlive the clock's jumps, as a live worker's heartbeats keep them.
    const runner = await start([LongJobWorkflow], { id: 'runner', leaseDuration: '3h', heartbeatInterval: '20ms' });
    const producer = await start([LongJobWorkflow], { id: 'producer' });
    for (const overlap of ['skip', 'allow', 'buffer-one', 'cancel-previous'] as const) {
      await api.client.schedules.upsert(overlap, { workflow: LongJobWorkflow, every: '1h', overlap });
    }
    const cancelRead = heartbeatRead(runner, 'cancelRequested');

    gate.holding.add(iso(hours(1)));
    clock.set(hours(1));
    const running = runner.worker.drain();
    await waitFor(() => gate.reached.length === 4);

    clock.set(hours(2));
    await producer.worker.drain();
    const status = async (schedule: string, hour: number) => (await api.client.getStatus(`${schedule}@${iso(hours(hour))}`))?.status ?? null;
    expect([await status('skip', 1), await status('skip', 2)]).toEqual(['running', null]);
    expect([await status('allow', 1), await status('allow', 2)]).toEqual(['running', 'completed']);
    expect([await status('buffer-one', 1), await status('buffer-one', 2)]).toEqual(['running', null]);
    expect(await api.client.schedules.get('buffer-one')).toMatchObject({ bufferedAt: hours(2), runs: 2 });
    // The new occurrence starts at once; the previous one is asked to stop, in the process that runs it.
    expect([await status('cancel-previous', 1), await status('cancel-previous', 2)]).toEqual(['running', 'completed']);
    expect(await api.client.getStatus(`cancel-previous@${iso(hours(1))}`)).toMatchObject({ cancelRequested: true });
    expect(skipped(producer)).toEqual([{ id: 'skip', reason: 'overlap', from: hours(2), to: hours(2) }]);

    await cancelRead;
    gate.open();
    await running;

    // The runner finished its instances, stopped the cancelled one, which compensated, and started the buffered one.
    expect(await api.client.getStatus(`cancel-previous@${iso(hours(1))}`)).toMatchObject({
      status: 'cancelled',
      error: { message: `Cancelled: schedule "cancel-previous" started its occurrence of ${iso(hours(2))} (overlap: 'cancel-previous').` },
    });
    expect(world.calls.filter((call) => call.key === `cancel-previous@${iso(hours(1))}`).map((call) => call.op)).toEqual(['work', 'undo-work']);
    expect([await status('buffer-one', 1), await status('buffer-one', 2)]).toEqual(['completed', 'completed']);
    expect(await api.client.schedules.get('buffer-one')).toMatchObject({ bufferedAt: null, runs: 2 });
    expect([await status('skip', 1), await status('allow', 1)]).toEqual(['completed', 'completed']);

    clock.set(hours(3));
    await producer.worker.drain();
    expect((await api.client.list({ scheduleId: 'skip' })).map((instance) => instance.id)).toEqual([`skip@${iso(hours(1))}`, `skip@${iso(hours(3))}`]);
  });
});

describe('bounds', () => {
  it('start no occurrence before startAt, after endAt or past limit, across racing processes and a restart', async () => {
    const pods = [await start([StocktakeWorkflow]), await start([StocktakeWorkflow])];
    for (const at of [1, 2, 3]) {
      clock.set(hours(at));
      await Promise.all(pods.map((pod) => pod.worker.drain()));
    }
    await stop(pods.pop()!);
    pods.push(await start([StocktakeWorkflow]));
    for (const at of [4, 5, 6, 7]) {
      clock.set(hours(at));
      await Promise.all(pods.map((pod) => pod.worker.drain()));
    }

    expect(ran('stocktake')).toEqual([2, 3, 4].map((n) => iso(hours(n))));
    expect(ran('shelf-check')).toEqual([3, 4].map((n) => iso(hours(n))));
    expect(await pods[0]!.client.schedules.get('stocktake')).toMatchObject({ runs: 3, nextAt: null });
    expect(await pods[1]!.client.schedules.get('shelf-check')).toMatchObject({ runs: 2, nextAt: null });
    expect(await pods[1]!.client.schedules.preview('stocktake')).toEqual([]);
  });
});

describe('WorkflowSchedules, injected in a controller', () => {
  it('previews, pauses at once, triggers while paused, resumes past what it missed, and counts a trigger for overlap', async () => {
    const api = await bootHttp('express', { db, clock, workflows: [NightlyExportWorkflow], providers: providers(), controllers: [SchedulesController] });
    nodes.push(api);
    let worker = await start([NightlyExportWorkflow]);

    expect((await api.http('GET', '/schedules/nightly-export')).body).toMatchObject({ declared: true, paused: false, nextAt: hours(1) });
    expect((await api.http('GET', '/schedules/nightly-export/preview?count=3')).body).toEqual([hours(1), hours(2), hours(3)]);
    expect(await worker.client.schedules.preview('nightly-export', { count: 3 })).toEqual([hours(1), hours(2), hours(3)]);

    clock.set(minutes(30));
    expect(await api.http('POST', '/schedules/nightly-export/pause')).toMatchObject({ status: 200, body: { paused: true, nextAt: null } });
    // Paused, it still previews what it would run.
    expect((await api.http('GET', '/schedules/nightly-export/preview?count=2')).body).toEqual([hours(1), hours(2)]);
    clock.set(hours(1));
    expect(await worker.worker.drain()).toBe(0);

    clock.set(minutes(70));
    expect(await api.http('POST', '/schedules/nightly-export/trigger')).toEqual({
      status: 201,
      body: { id: `nightly-export@${iso(minutes(70))}`, workflow: 'nightly-export', version: 1, created: true, status: 'pending' },
    });
    await worker.worker.drain();
    expect(world.calls.map((call) => call.key)).toEqual([`${iso(minutes(70))} warehouse`]);
    expect((await api.http('GET', '/schedules/nightly-export')).body).toMatchObject({ paused: true, runs: 0 });

    // A deploy keeps the pause.
    await stop(worker);
    worker = await start([NightlyExportWorkflow]);
    expect((await api.http('GET', '/schedules/nightly-export')).body).toMatchObject({ paused: true });

    clock.set(minutes(160));
    expect(await api.http('POST', '/schedules/nightly-export/resume')).toMatchObject({ status: 200, body: { paused: false, nextAt: hours(3) } });

    // The triggered instance still runs: 3:00 is skipped (overlap: 'skip'), 4:00 starts once it has ended.
    clock.set(hours(3));
    await worker.worker.drain();
    expect(skipped(worker)).toEqual([{ id: 'nightly-export', reason: 'overlap', from: hours(3), to: hours(3) }]);
    await worker.client.signal(exported, null, { key: `nightly-export@${iso(minutes(70))}` });
    await worker.worker.drain();
    clock.set(hours(4));
    await worker.worker.drain();

    expect((await worker.client.list({ scheduleId: 'nightly-export' })).map((instance) => [instance.id, instance.status])).toEqual([
      [`nightly-export@${iso(minutes(70))}`, 'completed'],
      [`nightly-export@${iso(hours(4))}`, 'suspended'],
    ]);
    expect((await api.http('GET', '/schedules/nightly-export')).body).toMatchObject({ runs: 1, nextAt: hours(5) });
    expect(await api.http('POST', '/schedules/missing/pause')).toEqual({
      status: 404,
      body: { statusCode: 404, error: 'WorkflowNotFoundError', message: 'No workflow schedule with id "missing".' },
    });
  });
});

describe('rate-limit windows', () => {
  it('open with the first execution after the last one ended, not on a fixed grid, in every process', async () => {
    const pods = [await start([PingWorkflow]), await start([PingWorkflow])];
    const window = async (at: number, starts: string[] = []) => {
      clock.set(at);
      for (const id of starts) {
        await pods[0]!.client.start(PingWorkflow, undefined, { id });
      }
      const before = world.calls.length;
      await Promise.all(pods.map((pod) => pod.worker.drain()));
      return world.calls
        .slice(before)
        .map((call) => call.key)
        .sort();
    };

    expect(await window(minutes(10.5), ['p-1', 'p-2', 'p-3'])).toEqual(['p-1', 'p-2']);
    expect(await window(minutes(11))).toEqual([]);
    expect(await window(minutes(11.5) - 1)).toEqual([]);
    expect(await window(minutes(11.5))).toEqual(['p-3']);
    expect(await window(minutes(12), ['p-4', 'p-5'])).toEqual(['p-4']);
    expect(await window(minutes(12.5) - 1)).toEqual([]);
    expect(await window(minutes(12.5))).toEqual(['p-5']);
  });
});

describe('waitForAny() timers', () => {
  it('count from when the wait is reached; a signal sent before that wins, one sent before the start or after the deadline does not', async () => {
    const api = await start([]);
    const first = await start([SupportTicketWorkflow]);
    await api.client.signal(replied, { text: 'too early' }, { key: 't-3' });
    for (const ticket of ['t-1', 't-2', 't-3', 't-4']) {
      await api.client.start(SupportTicketWorkflow, { ticket }, { id: ticket });
    }
    await first.worker.drain();

    clock.set(minutes(30));
    await api.client.signal(replied, { text: 'before the triage ended' }, { key: 't-1' });
    clock.set(hours(2));
    await first.worker.drain();
    expect(await api.client.getStatus('t-1')).toMatchObject({ status: 'completed', output: 'replied: before the triage ended' });
    for (const ticket of ['t-2', 't-3', 't-4']) {
      expect(await api.client.getStatus(ticket, { journal: true })).toMatchObject({
        status: 'suspended',
        wakeAt: hours(3),
        journal: [{ name: 'acknowledge' }, { name: 'triage' }, { name: 'reply-or-close', status: 'pending', data: { timers: { close: hours(3) } } }],
      });
    }
    // Took its early reply without parking for it.
    expect(first.events.filter((event) => event.id === 't-1').map((event) => event.type)).toEqual([
      'workflow-started',
      'step-completed',
      'workflow-suspended',
      'workflow-resumed',
      'signal-received',
      'workflow-completed',
    ]);

    // Nothing runs the tickets from 2:00 until after their deadline, when a reply to t-4 arrives.
    await stop(first);
    clock.set(hours(3) + 600_000);
    await api.client.signal(replied, { text: 'after the deadline' }, { key: 't-4' });
    clock.set(hours(3) + 900_000);
    const second = await start([SupportTicketWorkflow]);
    await second.worker.drain();

    for (const ticket of ['t-2', 't-3', 't-4']) {
      expect(await api.client.getStatus(ticket, { journal: true })).toMatchObject({
        status: 'completed',
        output: 'closed',
        journal: [{}, {}, { name: 'reply-or-close', result: { key: 'close', signalId: null, payload: null } }],
      });
    }
  });
});

describe('result() with a timeout', () => {
  it('gives up in the API process while another process runs the instance on, and resolves once it ends', async () => {
    const api = await start([]);
    const worker = await start([QuoteWorkflow]);
    gate.holding.add('quote-1');
    await api.client.start(QuoteWorkflow, { items: 3 }, { id: 'quote-1' });
    const running = worker.worker.drain();
    await waitFor(() => gate.reached.length === 1);

    const error = await api.client.result('quote-1', { timeout: '100ms' }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WorkflowResultTimeoutError);
    expect(error).toMatchObject({ instanceId: 'quote-1', timeoutMs: 100 });
    expect(await api.client.getStatus('quote-1')).toMatchObject({ status: 'running', leaseOwner: worker.worker.id });

    const result = api.client.result('quote-1', { timeout: '10s' });
    gate.open();
    await running;
    await expect(result).resolves.toBe(3_897);
  });
});
