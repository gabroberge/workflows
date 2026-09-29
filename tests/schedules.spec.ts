/**
 * Schedules: `@Workflow(name, { schedules })` and `WorkflowSchedules` start an instance per occurrence of a cron
 * expression, an interval or an RRULE, exactly once however many workers produce them, with the missed and
 * overlap policies, pauses, triggers and previews. The cron and RRULE arithmetic has its own specs.
 */
import { Logger } from '@nestjs/common';
import {
  ManualWorkflowClock,
  Workflow,
  WorkflowNotFoundError,
  WorkflowSignal,
  WorkflowStateError,
  type WorkflowContext,
  type WorkflowScheduleSkippedEvent,
} from '../lib/index.js';
import { boot, tempDb, World, type Node, type TestDb } from './support.js';

const T0 = Date.UTC(2026, 0, 1); // a Thursday, midnight UTC
const hours = (n: number) => T0 + n * 3_600_000;
const iso = (at: number) => new Date(at).toISOString();
const release = new WorkflowSignal<null>('release');

@Workflow('digest', {
  schedules: [
    {
      id: 'weekly-digest',
      cron: '0 0 8 * * MON',
      tz: 'Europe/Warsaw',
      input: ({ id, at }: { id: string; at: number }) => ({ schedule: id, week: iso(at).slice(0, 10) }),
    },
  ],
})
class DigestWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { schedule: string; week: string }) {
    const schedule = ctx.schedule;
    await ctx.step('send', () => this.world.record('digest', `${input.week} ${schedule?.id} ${schedule && iso(schedule.at)}`));
  }
}

@Workflow('tick')
class TickWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { tenant?: string } | undefined) {
    this.world.record('tick', `${ctx.schedule?.id} ${ctx.schedule && iso(ctx.schedule.at)} ${input?.tenant ?? '-'}`);
  }
}

/** Runs until it gets `release` with its own id as the key. */
@Workflow('long')
class LongWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext) {
    const id = ctx.workflowId;
    await ctx.step('begin', () => this.world.record('begin', id));
    await ctx.waitForSignal('release', release, { key: id });
    await ctx.step('end', () => this.world.record('end', id));
  }
}

// The same workflow as four deployments: first, the input and policies changed, the timing changed, the schedule gone.
@Workflow('newsletter', { schedules: [{ id: 'newsletter', cron: '0 9 * * *', input: { edition: 'daily' } }] })
class Newsletter {
  async run() {}
}

@Workflow('newsletter', { schedules: [{ id: 'newsletter', cron: '0 9 * * *', input: { edition: 'weekday' }, overlap: 'allow' }] })
class NewsletterPolicies {
  async run() {}
}

@Workflow('newsletter', { schedules: [{ id: 'newsletter', cron: '0 17 * * *', input: { edition: 'evening' }, overlap: 'allow' }] })
class NewsletterRetimed {
  async run() {}
}

@Workflow('newsletter')
class NewsletterDropped {
  async run() {}
}

@Workflow('stock-report', {
  schedules: [
    {
      id: 'stock-report',
      every: '1h',
      input: ({ at }: { at: number }) => {
        if (at === Date.UTC(2026, 0, 1, 1)) {
          throw new Error('The warehouse API is down.');
        }
        return { hour: new Date(at).getUTCHours() };
      },
    },
  ],
})
class StockReport {
  constructor(private readonly world: World) {}

  async run(_ctx: WorkflowContext, input: { hour: number }) {
    this.world.record('stock', String(input.hour));
  }
}

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock(T0);
  world = new World();
});

afterEach(async () => {
  vi.restoreAllMocks();
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

async function start(workflows: any[] = [DigestWorkflow, TickWorkflow, LongWorkflow]) {
  const node = await boot({ db, clock, workflows, providers: [{ provide: World, useValue: world }] });
  nodes.push(node);
  return node;
}

async function restart(workflows: any[]) {
  await nodes.pop()!.close();
  return start(workflows);
}

const skippedEvents = (node: Node) =>
  node.events.filter((event): event is WorkflowScheduleSkippedEvent => event.type === 'schedule-skipped').map(({ id, reason, from, to }) => ({ id, reason, from, to }));

describe('a declared schedule', () => {
  it('is saved at startup, and starts one instance per occurrence however many workers produce them', async () => {
    const [a, b] = [await start(), await start()];
    const monday = Date.UTC(2026, 0, 5, 7); // 08:00 in Warsaw (UTC+1)
    expect(await a.client.schedules.get('weekly-digest')).toMatchObject({
      workflow: 'digest',
      declared: true,
      cron: '0 0 8 * * MON',
      tz: 'Europe/Warsaw',
      missed: 'skip',
      overlap: 'skip',
      paused: false,
      nextAt: monday,
      runs: 0,
    });

    clock.set(monday);
    await Promise.all([a.worker.drain(), b.worker.drain(), a.worker.drain(), b.worker.drain()]);
    expect(world.calls.map((call) => call.key)).toEqual([`2026-01-05 weekly-digest ${iso(monday)}`]);
    expect(await a.client.list({ scheduleId: 'weekly-digest' })).toMatchObject([
      { id: `weekly-digest@${iso(monday)}`, workflow: 'digest', status: 'completed', scheduleId: 'weekly-digest', scheduledAt: monday },
    ]);
    expect(await b.client.schedules.get('weekly-digest')).toMatchObject({ runs: 1, nextAt: monday + 7 * 86_400_000 });
  });

  it('keeps its progress across restarts, follows the code when it changes, stays paused, and goes when the code drops it', async () => {
    let node = await start([Newsletter]);
    expect(await node.client.schedules.get('newsletter')).toMatchObject({ declared: true, nextAt: hours(9), input: { edition: 'daily' } });
    clock.set(hours(9));
    await node.worker.drain();
    const ran = await node.client.schedules.get('newsletter');
    expect(ran).toMatchObject({ runs: 1, nextAt: hours(33) });

    node = await restart([Newsletter]);
    expect(await node.client.schedules.get('newsletter')).toEqual(ran);
    await node.client.schedules.pause('newsletter');

    // Its input and overlap change, its timing doesn't: it keeps its next occurrence and its pause.
    node = await restart([NewsletterPolicies]);
    expect(await node.client.schedules.get('newsletter')).toMatchObject({ paused: true, nextAt: null, runs: 1, overlap: 'allow', input: { edition: 'weekday' } });
    expect(await node.client.schedules.resume('newsletter')).toMatchObject({ paused: false, nextAt: hours(33) });

    // Its timing changes: it starts over from now, its runs kept.
    node = await restart([NewsletterRetimed]);
    expect(await node.client.schedules.get('newsletter')).toMatchObject({ cron: '0 17 * * *', nextAt: hours(17), runs: 1, input: { edition: 'evening' } });

    node = await restart([NewsletterDropped]);
    expect(await node.client.schedules.get('newsletter')).toBeNull();
    expect(await node.client.list({ scheduleId: 'newsletter' })).toMatchObject([{ id: `newsletter@${iso(hours(9))}`, status: 'completed' }]);
  });

  it("can't be changed or removed at runtime, but pauses, resumes and triggers", async () => {
    const node = await start();
    await expect(node.client.schedules.upsert('weekly-digest', { workflow: TickWorkflow, every: '1h' })).rejects.toThrow(WorkflowStateError);
    await expect(node.client.schedules.remove('weekly-digest')).rejects.toThrow(
      'Schedule "weekly-digest" is declared by workflow "digest" (@Workflow(name, { schedules })): change or remove it there.',
    );

    clock.set(hours(30));
    const triggered = await node.client.schedules.trigger('weekly-digest');
    expect(triggered).toEqual({ id: `weekly-digest@${iso(hours(30))}`, workflow: 'digest', version: 1, created: true, status: 'pending' });
    await node.worker.drain();
    expect(world.calls.map((call) => call.key)).toEqual([`2026-01-02 weekly-digest ${iso(hours(30))}`]);
    expect(await node.client.schedules.get('weekly-digest')).toMatchObject({ runs: 0 });
  });
});

describe('a declared schedule whose input throws', () => {
  it('skips an occurrence whose input function throws, logged, and starts the next', async () => {
    const error = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    const node = await start([StockReport]);
    for (const at of [1, 2]) {
      clock.set(hours(at));
      await node.worker.drain();
    }

    expect(world.calls.map((call) => call.key)).toEqual(['2']);
    expect(error).toHaveBeenCalledWith(`Schedule "stock-report" couldn't start its occurrence of ${iso(hours(1))}: The warehouse API is down.`);
    expect(await node.client.schedules.get('stock-report')).toMatchObject({ runs: 1, nextAt: hours(3) });
  });
});

describe('WorkflowSchedules', () => {
  it('upserts, reads, lists, previews, pauses, resumes, triggers and removes a schedule', async () => {
    const node = await start();
    const saved = await node.client.schedules.upsert('tenant-7', { workflow: TickWorkflow, every: '1h', input: { tenant: 't-7' } });
    expect(saved).toEqual({
      id: 'tenant-7',
      workflow: 'tick',
      version: null,
      declared: false,
      every: 3_600_000,
      tz: 'UTC',
      startAt: null,
      endAt: null,
      limit: null,
      missed: 'skip',
      overlap: 'skip',
      priority: 0,
      input: { tenant: 't-7' },
      paused: false,
      nextAt: hours(1),
      runs: 0,
      bufferedAt: null,
      createdAt: T0,
      updatedAt: T0,
    });
    expect((await node.client.schedules.list()).map((schedule) => schedule.id)).toEqual(['tenant-7', 'weekly-digest']);
    expect((await node.client.schedules.list({ workflow: 'tick' })).map((schedule) => schedule.id)).toEqual(['tenant-7']);
    expect(await node.client.schedules.preview('tenant-7', { count: 3 })).toEqual([hours(1), hours(2), hours(3)]);
    expect(await node.client.schedules.preview({ cron: '0 9 * * MON-FRI', tz: 'America/New_York' }, { from: T0, count: 2 })).toEqual([
      Date.UTC(2026, 0, 1, 14),
      Date.UTC(2026, 0, 2, 14),
    ]);

    clock.set(hours(1));
    await node.worker.drain();
    expect(world.calls.map((call) => call.key)).toEqual([`tenant-7 ${iso(hours(1))} t-7`]);

    await node.client.schedules.pause('tenant-7');
    clock.set(hours(3));
    await node.worker.drain();
    expect(await node.client.schedules.get('tenant-7')).toMatchObject({ paused: true, nextAt: null, runs: 1 });

    // Resumed: the occurrences due while it was paused (2:00, 3:00) are passed over.
    expect(await node.client.schedules.resume('tenant-7')).toMatchObject({ paused: false, nextAt: hours(4) });
    expect(await node.client.schedules.trigger('tenant-7')).toMatchObject({ id: `tenant-7@${iso(hours(3))}`, created: true });
    expect(await node.client.schedules.trigger('tenant-7')).toMatchObject({ id: `tenant-7@${iso(hours(3))}`, created: false });
    await node.worker.drain();

    // A new timing starts over from now; the runs carry over, triggers not counted.
    clock.set(hours(3) + 600_000);
    expect(await node.client.schedules.upsert('tenant-7', { workflow: 'tick', every: '30m', input: { tenant: 't-7' } })).toMatchObject({
      every: 1_800_000,
      nextAt: hours(3.5),
      runs: 1,
    });
    clock.set(hours(3.5));
    await node.worker.drain();
    expect(world.calls.map((call) => call.key).slice(1)).toEqual([`tenant-7 ${iso(hours(3))} t-7`, `tenant-7 ${iso(hours(3.5))} t-7`]);

    expect(await node.client.schedules.remove('tenant-7')).toBe(true);
    expect(await node.client.schedules.remove('tenant-7')).toBe(false);
    expect(await node.client.schedules.get('tenant-7')).toBeNull();
    expect(await node.client.list({ scheduleId: 'tenant-7' })).toHaveLength(3);
  });

  it('refuses invalid schedules with the reason', async () => {
    const node = await start();
    const upsert = (options: object, id = 'bad') => node.client.schedules.upsert(id, { workflow: TickWorkflow, ...options } as never);

    await expect(upsert({ cron: '0 8 * * *', every: '1h' })).rejects.toThrow('Schedule "bad": give exactly one of cron, every and rrule, not cron and every.');
    await expect(upsert({})).rejects.toThrow('Schedule "bad": give exactly one of cron, every and rrule.');
    await expect(upsert({ cron: '0 25 * * *' })).rejects.toThrow(TypeError);
    await expect(upsert({ rrule: 'FREQ=DAILY;COUNT=3' })).rejects.toThrow('Schedule "bad": an rrule with COUNT counts from startAt: give one (or use limit).');
    await expect(upsert({ rrule: 'FREQ=MINUTELY;COUNT=10001', startAt: T0 })).rejects.toThrow("Schedule \"bad\": an rrule's COUNT (10001) can be at most 10000. For more, use limit.");
    await expect(upsert({ every: '1h', tz: 'Europe/Warsaw' })).rejects.toThrow("Schedule \"bad\": every repeats a fixed interval from startAt");
    await expect(upsert({ every: '500ms' })).rejects.toThrow('Schedule "bad": every ("500ms") must be at least a second.');
    await expect(upsert({ cron: '0 8 * * *', tz: 'Mars/Olympus' })).rejects.toThrow(TypeError);
    await expect(upsert({ every: '1h', startAt: hours(2), endAt: hours(1) })).rejects.toThrow('must be after startAt');
    await expect(upsert({ every: '1h', missed: 'sometimes' })).rejects.toThrow("Schedule \"bad\": invalid missed \"sometimes\". Use 'skip', 'once' or 'all'.");
    await expect(upsert({ every: '1h', overlap: 'queue' })).rejects.toThrow("Use 'skip', 'allow', 'cancel-previous' or 'buffer-one'.");
    await expect(upsert({ every: '1h', limit: 0 })).rejects.toThrow('Schedule "bad": invalid limit 0. Use a positive integer.');
    await expect(upsert({ every: '1h', input: () => ({}) })).rejects.toThrow("upsert() stores its input, so it can't be a function.");
    await expect(upsert({ every: '1h' }, 'no spaces allowed')).rejects.toThrow('Invalid schedule id "no spaces allowed".');
    await expect(node.client.schedules.upsert('x', { workflow: 'unknown', every: '1h' })).rejects.toThrow(WorkflowNotFoundError);
    await expect(node.client.schedules.pause('missing')).rejects.toThrow(WorkflowNotFoundError);
    await expect(node.client.schedules.trigger('missing')).rejects.toThrow('No workflow schedule with id "missing".');
    expect(await node.client.schedules.list({ workflow: 'tick' })).toEqual([]);

    expect(() => Workflow('a', { schedules: [{ id: 'x', every: '1h' }, { id: 'x', every: '2h' }] })).toThrow('Workflow "a" declares schedule "x" twice.');
    expect(() => Workflow('b', { schedules: [{ id: 'y@z', every: '1h' }] })).toThrow('Invalid schedule id "y@z" of workflow "b".');
    expect(() => Workflow('c', { schedules: [{ id: 'z', cron: '61 * * * *' }] })).toThrow(TypeError);
  });

  it('previews across a change of the clocks', async () => {
    const node = await start();
    // Europe/Warsaw skips 02:00-03:00 on 29 March 2026: 02:30 runs at 03:30 CEST that day.
    expect(await node.client.schedules.preview({ cron: '0 30 2 * * *', tz: 'Europe/Warsaw' }, { from: Date.UTC(2026, 2, 28, 12), count: 3 })).toEqual([
      Date.UTC(2026, 2, 29, 1, 30),
      Date.UTC(2026, 2, 30, 0, 30),
      Date.UTC(2026, 2, 31, 0, 30),
    ]);
    expect(await node.client.schedules.preview({ every: '1d', limit: 2 }, { from: T0 })).toEqual([hours(24), hours(48)]);
  });
});

describe('missed occurrences', () => {
  it("'skip' starts only an occurrence on time, 'once' the latest, 'all' every one", async () => {
    const node = await start();
    for (const missed of ['skip', 'once', 'all'] as const) {
      await node.client.schedules.upsert(missed, { workflow: TickWorkflow, every: '1h', missed, overlap: missed === 'all' ? 'allow' : 'skip' });
    }
    await expect(node.client.schedules.upsert('x', { workflow: TickWorkflow, every: '1h', missed: 'all' })).rejects.toThrow(
      "Schedule \"x\": missed: 'all' starts every missed occurrence at once, so it needs overlap: 'allow'.",
    );

    // Nothing ran from 1:00 to 5:30.
    clock.set(hours(5.5));
    await node.worker.drain();
    const ran = (id: string) =>
      world.calls
        .filter((call) => call.key.startsWith(`${id} `))
        .map((call) => call.key.split(' ')[1])
        .sort();
    expect(ran('skip')).toEqual([]);
    expect(ran('once')).toEqual([iso(hours(5))]);
    expect(ran('all')).toEqual([1, 2, 3, 4, 5].map((n) => iso(hours(n))));
    expect(skippedEvents(node)).toEqual([
      { id: 'once', reason: 'missed', from: hours(1), to: hours(4) },
      { id: 'skip', reason: 'missed', from: hours(1), to: hours(5) },
    ]);

    clock.set(hours(6));
    await node.worker.drain();
    expect(ran('skip')).toEqual([iso(hours(6))]);
    expect(await node.client.schedules.get('skip')).toMatchObject({ runs: 1, nextAt: hours(7) });
  });

  it("'all' catches up the latest 100, and passes over the older ones", async () => {
    const node = await start();
    await node.client.schedules.upsert('burst', { workflow: TickWorkflow, every: '1s', missed: 'all', overlap: 'allow' });
    clock.set(T0 + 600_000);
    await node.worker.drain();

    const started = await node.client.list({ scheduleId: 'burst', limit: 1_000 });
    expect(started).toHaveLength(100);
    expect(started.map((instance) => instance.scheduledAt).sort((a, b) => a! - b!)[0]).toBe(T0 + 501_000);
    expect(skippedEvents(node)).toEqual([{ id: 'burst', reason: 'missed', from: T0 + 1_000, to: T0 + 500_000 }]);
  });
});

describe('overlapping occurrences', () => {
  it("'skip', 'allow', 'buffer-one' and 'cancel-previous' while an instance still runs", async () => {
    const node = await start();
    for (const overlap of ['skip', 'allow', 'buffer-one', 'cancel-previous'] as const) {
      await node.client.schedules.upsert(overlap, { workflow: LongWorkflow, every: '1h', overlap });
    }
    const status = async (id: string, at: number) => (await node.client.getStatus(`${id}@${iso(hours(at))}`))?.status ?? 'none';

    for (const at of [1, 2, 3]) {
      clock.set(hours(at));
      await node.worker.drain();
    }
    expect([await status('skip', 1), await status('skip', 2), await status('skip', 3)]).toEqual(['suspended', 'none', 'none']);
    expect([await status('allow', 1), await status('allow', 2), await status('allow', 3)]).toEqual(['suspended', 'suspended', 'suspended']);
    expect([await status('buffer-one', 1), await status('buffer-one', 2), await status('buffer-one', 3)]).toEqual(['suspended', 'none', 'none']);
    expect(await node.client.schedules.get('buffer-one')).toMatchObject({ bufferedAt: hours(2), runs: 2 });
    expect([await status('cancel-previous', 1), await status('cancel-previous', 2), await status('cancel-previous', 3)]).toEqual([
      'cancelled',
      'cancelled',
      'suspended',
    ]);
    expect(await node.client.getStatus(`cancel-previous@${iso(hours(1))}`)).toMatchObject({
      error: { name: 'WorkflowCancelledError', message: `Cancelled: schedule "cancel-previous" started its occurrence of ${iso(hours(2))} (overlap: 'cancel-previous').` },
    });
    expect(skippedEvents(node).filter((event) => event.reason === 'overlap')).toEqual([
      { id: 'skip', reason: 'overlap', from: hours(2), to: hours(2) },
      { id: 'buffer-one', reason: 'overlap', from: hours(3), to: hours(3) },
      { id: 'skip', reason: 'overlap', from: hours(3), to: hours(3) },
    ]);

    // The buffered occurrence starts once the running instance ends: in the same drain.
    await node.client.signal(release, null, { key: `buffer-one@${iso(hours(1))}` });
    await node.worker.drain();
    expect([await status('buffer-one', 1), await status('buffer-one', 2)]).toEqual(['completed', 'suspended']);
    expect(await node.client.schedules.get('buffer-one')).toMatchObject({ bufferedAt: null, runs: 2 });
  });
});

describe('bounds and limits', () => {
  it('ends after limit occurrences, and keeps to startAt and endAt', async () => {
    const node = await start();
    await node.client.schedules.upsert('limited', { workflow: TickWorkflow, every: '1h', limit: 2 });
    await node.client.schedules.upsert('windowed', { workflow: TickWorkflow, cron: '0 * * * *', startAt: hours(2.5), endAt: hours(4) });
    expect(await node.client.schedules.get('windowed')).toMatchObject({ nextAt: hours(3), startAt: hours(2.5), endAt: hours(4) });

    for (let at = 1; at <= 6; at++) {
      clock.set(hours(at));
      await node.worker.drain();
    }
    expect((await node.client.list({ scheduleId: 'limited' })).map((instance) => instance.scheduledAt)).toEqual([hours(1), hours(2)]);
    expect((await node.client.list({ scheduleId: 'windowed' })).map((instance) => instance.scheduledAt)).toEqual([hours(3), hours(4)]);
    expect(await node.client.schedules.get('limited')).toMatchObject({ runs: 2, nextAt: null });
    expect(await node.client.schedules.get('windowed')).toMatchObject({ runs: 2, nextAt: null });
    expect(await node.client.schedules.preview('limited')).toEqual([]);
  });
});

describe('crashes and takeovers', () => {
  it('finishes the start a worker decided on and never made, once, after its lease', async () => {
    const [a, b] = [await start(), await start()];
    const error = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => {});
    await a.client.schedules.upsert('crashy', { workflow: TickWorkflow, every: '1h' });
    const create = vi.spyOn(a.store, 'create').mockRejectedValueOnce(new Error('Connection terminated unexpectedly'));

    clock.set(hours(1));
    await a.worker.drain();
    expect(create).toHaveBeenCalledTimes(1);
    expect(error).toHaveBeenCalledWith(expect.stringContaining('Starting the occurrences of schedule "crashy" failed'), expect.any(Error));
    expect(await a.client.list({ scheduleId: 'crashy' })).toEqual([]);
    expect(await b.worker.drain()).toBe(0); // its lease holds

    clock.advance('31s');
    await Promise.all([a.worker.drain(), b.worker.drain()]);
    expect((await a.client.list({ scheduleId: 'crashy' })).map((instance) => [instance.id, instance.status])).toEqual([[`crashy@${iso(hours(1))}`, 'completed']]);
    expect(await a.client.schedules.get('crashy')).toMatchObject({ runs: 1, nextAt: hours(2) });
  });

  it("leaves a retimed schedule's start in flight to the next worker, which makes it once", async () => {
    const node = await start();
    await node.client.schedules.upsert('retimed', { workflow: TickWorkflow, every: '1h' });
    const create = node.store.create.bind(node.store);
    vi.spyOn(node.store, 'create').mockImplementationOnce(async (instance) => {
      // Retimed while the worker makes the occurrence: its lease goes, so its last write doesn't land.
      await node.client.schedules.upsert('retimed', { workflow: TickWorkflow, every: '2h' });
      return create(instance);
    });

    clock.set(hours(1));
    await node.worker.drain();
    expect((await node.client.list({ scheduleId: 'retimed' })).map((instance) => [instance.id, instance.status])).toEqual([[`retimed@${iso(hours(1))}`, 'completed']]);
    expect(await node.client.schedules.get('retimed')).toMatchObject({ every: 7_200_000, runs: 1, nextAt: hours(2) });
  });
});
