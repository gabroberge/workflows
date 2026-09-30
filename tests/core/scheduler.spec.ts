/**
 * `@nestjs/workflows/core`'s `Scheduler` on its own, over the in-memory `ScheduleStore`, with runs of a package that
 * isn't workflows (jobs in a map): one run per occurrence however many schedulers produce them, decisions recorded
 * before the runs start, the missed and overlap policies, declared schedules through a rolling deploy, codecs, and
 * pauses. schedules.spec.ts, rollouts.integration.spec.ts and clock.integration.spec.ts cover it through workflows.
 */
import { randomBytes } from 'node:crypto';
import {
  AesGcmPayloadCodec,
  InMemoryScheduleStore,
  isEncodedPayload,
  ManualClock,
  occurrenceId,
  parseSchedule,
  PayloadCodecs,
  Scheduler,
  type DeclaredSchedule,
  type ScheduleOptions,
  type ScheduleOccurrence,
  type ScheduleSkip,
  type ScheduleStore,
  type SchedulerOptions,
} from '../../lib/core/index.js';

const T0 = Date.UTC(2026, 0, 1);
const hours = (n: number) => T0 + n * 3_600_000;
const LEASE = 30_000;

/** A package's runs: jobs by id, which a schedule adds with the id it is given. */
class Jobs {
  readonly byId = new Map<string, { schedule: string; at: number; data: unknown; done: boolean }>();
  readonly cancels: Array<{ id: string; reason: string }> = [];
  adds = 0;

  add(id: string, schedule: string, at: number, data: unknown): { created: boolean } {
    this.adds++;
    if (this.byId.has(id)) {
      return { created: false };
    }
    this.byId.set(id, { schedule, at, data, done: false });
    return { created: true };
  }

  finish(id: string): void {
    this.byId.get(id)!.done = true;
  }

  unfinished(schedule: string): string[] {
    return [...this.byId].filter(([, job]) => job.schedule === schedule && !job.done).map(([id]) => id);
  }

  ids(schedule: string): string[] {
    return [...this.byId].filter(([, job]) => job.schedule === schedule).map(([id]) => id);
  }
}

const silent = () => ({ log: vi.fn(), warn: vi.fn(), error: vi.fn() });

function schedulerOf(store: ScheduleStore, jobs: Jobs, clock: ManualClock, options: Partial<SchedulerOptions> = {}) {
  const skips: ScheduleSkip[] = [];
  const logger = silent();
  const scheduler = new Scheduler({
    store,
    clock,
    logger,
    targets: () => ['emails'],
    fire: async ({ schedule, declared, id, at }) => {
      const data = typeof declared?.payload === 'function' ? (declared.payload as (occurrence: ScheduleOccurrence) => unknown)({ id: schedule.id, at }) : schedule.payload;
      return jobs.add(id, schedule.id, at, data);
    },
    running: async (schedule) => jobs.unfinished(schedule.id),
    cancel: async (id, reason) => {
      jobs.cancels.push({ id, reason });
      jobs.finish(id);
      return true;
    },
    skipped: (_schedule, skip) => void skips.push(skip),
    ...options,
  });
  return { scheduler, skips, logger };
}

/** Saves a runtime schedule, as a package's `upsert()` does. */
function upsert(scheduler: Scheduler, id: string, options: ScheduleOptions, payload: unknown = null) {
  return scheduler.save(id, (current) => scheduler.changed(current, { target: 'emails', declared: false, spec: parseSchedule(options), payload }), { replacing: true });
}

let clock: ManualClock;
let store: InMemoryScheduleStore;
let jobs: Jobs;

beforeEach(() => {
  clock = new ManualClock(T0);
  store = new InMemoryScheduleStore();
  jobs = new Jobs();
});

describe('Scheduler', () => {
  it('starts one run per occurrence, with its payload, however many schedulers produce them at once', async () => {
    const pods = [1, 2, 3].map(() => schedulerOf(store, jobs, clock).scheduler);
    await upsert(pods[0]!, 'digest', { every: '1h' }, { template: 'weekly' });

    clock.set(hours(1));
    const productions = await Promise.all(pods.map((pod, i) => pod.produce(`pod-${i}`, LEASE)));
    expect(productions.reduce((sum, production) => sum + production.started, 0)).toBe(1);
    expect(jobs.ids('digest')).toEqual([occurrenceId('digest', hours(1))]);
    expect(jobs.byId.get(occurrenceId('digest', hours(1)))).toMatchObject({ at: hours(1), data: { template: 'weekly' } });
    expect(pods[0]!.progress((await store.getSchedule('digest'))!)).toEqual({ nextAt: hours(2), runs: 1, bufferedAt: null });
  });

  it('records its decisions before it starts the runs, so a producer that dies in between leaves them to the next, which starts them once', async () => {
    const writes = { count: 0 };
    const failing: ScheduleStore = Object.assign(Object.create(store) as ScheduleStore, {
      writeSchedule: (...args: Parameters<ScheduleStore['writeSchedule']>) => {
        if (++writes.count === 2) {
          return Promise.reject(new Error('Connection terminated unexpectedly'));
        }
        return store.writeSchedule(...args);
      },
    });
    const dying = schedulerOf(failing, jobs, clock);
    const next = schedulerOf(store, jobs, clock).scheduler;
    await upsert(next, 'digest', { every: '1h' });

    clock.set(hours(1));
    await dying.scheduler.produce('pod-1', LEASE);
    expect(dying.logger.error).toHaveBeenCalledWith(expect.stringContaining('Starting the occurrences of schedule "digest" failed'), expect.any(Error));
    expect((await store.getSchedule('digest'))!.state).toMatchObject({ pending: [{ at: hours(1) }] });
    expect(await next.produce('pod-2', LEASE)).toEqual({ started: 0, cancelled: [] });

    clock.advance(LEASE + 1);
    await next.produce('pod-2', LEASE);
    expect(jobs.ids('digest')).toEqual([occurrenceId('digest', hours(1))]);
    expect(jobs.adds).toBe(2);
    expect(next.progress((await store.getSchedule('digest'))!)).toMatchObject({ runs: 1, nextAt: hours(2) });
    expect((await store.getSchedule('digest'))!.state).toMatchObject({ pending: [] });
  });

  it('passes over missed occurrences, or starts the latest, or all of them, as missed says, and reports what it passed over', async () => {
    const { scheduler, skips } = schedulerOf(store, jobs, clock);
    await upsert(scheduler, 'skip', { every: '1h' });
    await upsert(scheduler, 'once', { every: '1h', missed: 'once' });
    await upsert(scheduler, 'all', { every: '1h', missed: 'all', overlap: 'allow' });

    clock.set(hours(3.5));
    await scheduler.produce('pod', LEASE);
    expect(jobs.ids('skip')).toEqual([]);
    expect(jobs.ids('once')).toEqual([occurrenceId('once', hours(3))]);
    expect(jobs.ids('all')).toEqual([1, 2, 3].map((n) => occurrenceId('all', hours(n))));
    expect(skips).toEqual(
      expect.arrayContaining([
        { reason: 'missed', from: hours(1), to: hours(3) },
        { reason: 'missed', from: hours(1), to: hours(2) },
      ]),
    );
  });

  it('keeps overlapping runs to the overlap policy: skip, allow, buffer one, or cancel the previous', async () => {
    const { scheduler, skips } = schedulerOf(store, jobs, clock);
    for (const overlap of ['skip', 'allow', 'buffer-one', 'cancel-previous'] as const) {
      await upsert(scheduler, overlap, { every: '1h', overlap });
    }
    clock.set(hours(1));
    await scheduler.produce('pod', LEASE);
    clock.set(hours(2));
    await scheduler.produce('pod', LEASE);

    expect(jobs.ids('skip')).toEqual([occurrenceId('skip', hours(1))]);
    expect(skips).toContainEqual({ reason: 'overlap', from: hours(2), to: hours(2) });
    expect(jobs.ids('allow')).toHaveLength(2);
    expect(jobs.ids('buffer-one')).toHaveLength(1);
    expect(scheduler.progress((await store.getSchedule('buffer-one'))!)).toMatchObject({ bufferedAt: hours(2), runs: 2 });
    expect(jobs.cancels).toEqual([
      { id: occurrenceId('cancel-previous', hours(1)), reason: `Cancelled: schedule "cancel-previous" started its occurrence of ${new Date(hours(2)).toISOString()} (overlap: 'cancel-previous').` },
    ]);
    expect(jobs.ids('cancel-previous')).toHaveLength(2);

    // The buffered occurrence starts once the running one ended, with the time it was due.
    jobs.finish(occurrenceId('buffer-one', hours(1)));
    clock.set(hours(2.25));
    await scheduler.produce('pod', LEASE);
    expect(jobs.ids('buffer-one')).toEqual([occurrenceId('buffer-one', hours(1)), occurrenceId('buffer-one', hours(2))]);
  });

  it('retries a start the store refuses, backing off, then gives up on it without counting it', async () => {
    const { scheduler, logger } = schedulerOf(store, jobs, clock, {
      fire: async () => {
        throw new Error('value too long for type character varying(255)');
      },
    });
    await upsert(scheduler, 'refused', { every: '1h', overlap: 'allow' });

    clock.set(hours(1));
    for (let i = 0; i < 20; i++) {
      await scheduler.produce('pod', LEASE);
      clock.advance('30s');
    }
    expect(logger.error).toHaveBeenCalledTimes(10);
    expect(logger.error).toHaveBeenLastCalledWith(`Schedule "refused" couldn't start its occurrence of ${new Date(hours(1)).toISOString()} (attempt 10 of 10); it gives up on it.`, expect.any(Error));
    expect(scheduler.progress((await store.getSchedule('refused'))!)).toMatchObject({ runs: 0, nextAt: hours(2) });
  });

  it('pauses at once, starts nothing while paused, and resumes from the first occurrence after now', async () => {
    const { scheduler } = schedulerOf(store, jobs, clock);
    await upsert(scheduler, 'digest', { every: '1h', missed: 'once' });
    expect(await scheduler.pause('digest')).toMatchObject({ paused: true });
    expect(await scheduler.pause('missing')).toBeNull();

    clock.set(hours(3.5));
    await scheduler.produce('pod', LEASE);
    expect(jobs.ids('digest')).toEqual([]);

    const resumed = (await scheduler.resume('digest'))!;
    expect(scheduler.progress(resumed)).toMatchObject({ nextAt: hours(4), runs: 0 });
    expect(await scheduler.resume('missing')).toBeNull();
  });

  it("rejects with its conflict error when a save keeps losing to other writers", async () => {
    const losing: ScheduleStore = Object.assign(Object.create(store) as ScheduleStore, { saveSchedule: async () => null });
    const { scheduler } = schedulerOf(losing, jobs, clock, { conflictError: (message) => new RangeError(message) });
    await expect(upsert(scheduler, 'digest', { every: '1h' })).rejects.toThrow(new RangeError('Schedule "digest" kept changing while it was being saved. Try again.'));
  });
});

describe('Scheduler with declared schedules', () => {
  const digest = (version: number, options: ScheduleOptions): DeclaredSchedule => ({
    id: 'digest',
    target: 'emails',
    version,
    spec: parseSchedule(options),
    payload: ({ at }: ScheduleOccurrence) => ({ week: new Date(at).toISOString().slice(0, 10) }),
  });
  const pod = (declared: DeclaredSchedule[]) => schedulerOf(store, jobs, clock, { declared: () => new Map(declared.map((schedule) => [schedule.id, schedule])) });

  it('saves them at startup, starts their runs with the payload function, and confirms them every minute', async () => {
    const { scheduler } = pod([digest(1, { every: '1h' })]);
    await scheduler.synced();
    const saved = (await store.getSchedule('digest'))!;
    expect(saved).toMatchObject({ declared: true, payload: null, spec: { every: 3_600_000, declaredBy: 1, confirmedAt: T0 } });

    clock.set(hours(1));
    await scheduler.produce('pod', LEASE);
    expect(jobs.byId.get(occurrenceId('digest', hours(1)))!.data).toEqual({ week: '2026-01-01' });
    expect((await store.getSchedule('digest'))!.spec).toMatchObject({ confirmedAt: hours(1) });
  });

  it("follows the code in a rolling deploy: a newer version's declaration stays while its code runs, and one no code confirms goes after five minutes", async () => {
    const log = vi.fn();
    const v1 = pod([digest(1, { every: '1h' })]).scheduler;
    await v1.synced();
    const v2 = pod([digest(2, { every: '2h' })]).scheduler;
    await v2.synced();
    expect((await store.getSchedule('digest'))!.spec).toMatchObject({ every: 7_200_000, declaredBy: 2 });

    // A pod of the old code restarts mid-rollout: it leaves the new code's declaration.
    await pod([digest(1, { every: '1h' })]).scheduler.synced();
    expect((await store.getSchedule('digest'))!.spec).toMatchObject({ every: 7_200_000, declaredBy: 2 });

    // The new code drops the schedule; the old code's pods are gone.
    const v3 = schedulerOf(store, jobs, clock, { declared: () => new Map(), logger: { log, warn: vi.fn(), error: vi.fn() } }).scheduler;
    clock.advance('4m');
    await v3.produce('pod-3', LEASE);
    expect(await store.getSchedule('digest')).not.toBeNull();
    clock.advance('2m');
    await v3.produce('pod-3', LEASE);
    expect(await store.getSchedule('digest')).toBeNull();
    expect(log).toHaveBeenCalledWith('Deleted schedule "digest" of target "emails": no process whose code declares it confirmed it for five minutes.');
  });

  it('leaves a declared schedule its code does not declare to a producer whose code does', async () => {
    const declaring = pod([digest(1, { every: '1h' })]).scheduler;
    await declaring.synced();
    clock.set(hours(1) - 30_000);
    await declaring.produce('pod-1', LEASE);
    const { scheduler, logger } = pod([]);

    clock.set(hours(1));
    await scheduler.produce('pod-2', LEASE);
    expect(jobs.ids('digest')).toEqual([]);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('Schedule "digest" is declared by code this worker doesn\'t run'));
    expect((await store.getSchedule('digest'))!.leaseUntil).toBeNull();
  });
});

describe('Scheduler with codecs', () => {
  const key = randomBytes(32);
  const codecs = (keys: Record<string, Buffer>) => new PayloadCodecs([new AesGcmPayloadCodec({ keys, current: Object.keys(keys)[0]! })]);

  it("stores payloads encoded, reads them decoded, and hands back a schedule whose payload it can't read", async () => {
    const writer = schedulerOf(store, jobs, clock, { codecs: codecs({ k1: key }) }).scheduler;
    await upsert(writer, 'digest', { every: '1h' }, { to: 'ada@example.com' });
    const raw = (await store.getSchedule('digest'))!;
    expect(isEncodedPayload(raw.payload)).toBe(true);
    expect(JSON.stringify(raw)).not.toContain('ada@example.com');
    expect((await writer.read('digest'))!.payload).toEqual({ to: 'ada@example.com' });

    const { scheduler: reader, logger } = schedulerOf(store, jobs, clock, { codecs: codecs({ k2: randomBytes(32) }) });
    clock.set(hours(1));
    expect(await reader.produce('pod', LEASE)).toEqual({ started: 0, cancelled: [] });
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('Schedule "digest" can\'t be read, so it isn\'t run; it is claimed again later.'));
    expect(await store.getSchedule('digest')).toMatchObject({ leaseUntil: null, wakeAt: hours(1) + LEASE });
    expect((await reader.readable(raw)).payload).toBeNull();
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('Schedule "digest" is shown without its payload'));

    clock.advance(LEASE);
    await writer.produce('pod', LEASE);
    expect(jobs.byId.get(occurrenceId('digest', hours(1)))!.data).toEqual({ to: 'ada@example.com' });
  });
});
