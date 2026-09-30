import type { LoggerService } from '@nestjs/common';
import type { PayloadCodecs } from '../codecs/payload-codecs.js';
import type { Clock } from './clock.interface.js';
import type { PayloadContext } from './payload-codec.interface.js';
import type { ScheduleOccurrence, ScheduleSpec } from './schedule.interface.js';
import type { ScheduleRecord, ScheduleStore } from './schedule-store.interface.js';

/**
 * A schedule the code declares (a decorator's, a queue definition's), which the `Scheduler` keeps in step with the
 * store at startup and every minute, also through a rolling deploy.
 *
 * ```ts
 * const declared: DeclaredSchedule = {
 *   id: 'weekly-digest',
 *   target: 'emails',
 *   version: 1,
 *   spec: parseSchedule({ cron: '0 0 8 * * MON', tz: 'Europe/Warsaw' }),
 *   payload: ({ at }: ScheduleOccurrence) => ({ week: isoWeek(new Date(at)) }),
 * };
 * ```
 */
export interface DeclaredSchedule<S extends ScheduleSpec = ScheduleSpec> {
  /** Unique across the schedules of the store. */
  id: string;
  /** What it starts runs of. */
  target: string;
  /**
   * The version of the code that declares it, such as the workflow's highest registered version (a package without
   * versions passes 1): a process of older code leaves a newer version's declaration as it is while a process of that
   * code confirms it.
   */
  version: number;
  spec: S;
  /** What each run gets: a JSON value, stored, or a function of the occurrence, which `fire` calls (never stored). */
  payload: unknown | ((occurrence: ScheduleOccurrence) => unknown);
}

/**
 * An occurrence the `Scheduler` asks its `fire` to start: the schedule as stored (its payload decoded), its spec, the
 * code's declaration of it (for a declared schedule whose payload is a function), and the run's id, which is
 * `occurrenceId(schedule.id, at)`: starting a second run with the same id must do nothing.
 *
 * ```ts
 * fire: async ({ schedule, declared, id, at }) => {
 *   const data = typeof declared?.payload === 'function' ? declared.payload({ id: schedule.id, at }) : schedule.payload;
 *   const { created } = await jobs.add(schedule.target, data, { id });
 *   return { created };
 * },
 * ```
 */
export interface DueOccurrence<S extends ScheduleSpec = ScheduleSpec> {
  schedule: ScheduleRecord;
  spec: S;
  declared?: DeclaredSchedule<S>;
  /** The run's id: `occurrenceId(schedule.id, at)`. */
  id: string;
  /** When the occurrence was due. */
  at: number;
  /** The production's time. */
  now: number;
}

/**
 * Occurrences a production passed over: missed while no worker was up (`'missed'`, see `missed`), or due while a run
 * the schedule started was unfinished (`'overlap'`).
 *
 * ```ts
 * skipped: (schedule, { reason, from, to }) => events.emit({ type: 'schedule-skipped', id: schedule.id, reason, from, to }),
 * ```
 */
export interface ScheduleSkip {
  reason: 'missed' | 'overlap';
  /** The first occurrence it passed over (epoch milliseconds). */
  from: number;
  /** The last one. */
  to: number;
}

/**
 * What one `Scheduler.produce()` did: how many runs it started, and the runs it cancelled (`overlap:
 * 'cancel-previous'`), which a worker running them in this process should stop at once.
 *
 * ```ts
 * const { started, cancelled } = await scheduler.produce(worker.owner, worker.leaseMs);
 * ```
 */
export interface ScheduleProduction {
  started: number;
  cancelled: string[];
}

/**
 * What the `Scheduler` takes: its store, and the package's side of schedules (what a run is, how to start, list and
 * cancel one), which it plugs in as functions.
 *
 * ```ts
 * const scheduler = new Scheduler({
 *   store: new InMemoryScheduleStore(),
 *   targets: () => ['emails'],
 *   declared: () => new Map([[digest.id, digest]]),
 *   fire: ({ schedule, id }) => jobs.add(schedule.target, schedule.payload, { id }),
 *   running: (schedule) => jobs.unfinished({ scheduleId: schedule.id }),
 *   cancel: (id, reason) => jobs.cancel(id, reason),
 * });
 * ```
 */
export interface SchedulerOptions<S extends ScheduleSpec = ScheduleSpec> {
  /** Where the schedules live, their payloads as the store keeps them (the scheduler encodes them with `codecs`). */
  store: ScheduleStore;
  /** The targets this process runs: it produces only their schedules. None: it neither produces nor reconciles. */
  targets(): string[];
  /** The schedules this process's code declares, by id. Default: none. */
  declared?(): ReadonlyMap<string, DeclaredSchedule<S>>;
  /**
   * Starts the run of an occurrence with the id it is given, or finds the one that has it: `{ created }`. `null` skips
   * the occurrence for good (its payload couldn't be computed: log why); throwing (the store refused it) retries it,
   * backing off from a second to a minute, ten times in all.
   */
  fire(occurrence: DueOccurrence<S>): Promise<{ created: boolean } | null>;
  /** The ids of the unfinished runs the schedule started, for its `overlap`. */
  running(schedule: ScheduleRecord): Promise<string[]>;
  /** Cancels a run (`overlap: 'cancel-previous'`): whether it was accepted. */
  cancel(run: string, reason: string, now: number): Promise<boolean>;
  /** Called after a production that passed over occurrences. */
  skipped?(schedule: ScheduleRecord, skip: ScheduleSkip, now: number): void;
  /** Where times come from. Default `systemClock`. */
  clock?: Clock;
  /** Encodes the schedules' payloads (`PayloadCodecs` of the package's codecs). Default: stored as they are. */
  codecs?: PayloadCodecs;
  /** The context a schedule's payload is encoded with. Default `{ field: 'payload', schedule: id }`. */
  payloadContext?(schedule: string): PayloadContext;
  /** The error a save that keeps losing races rejects with. Default: an `Error`. */
  conflictError?(message: string): Error;
  /** How its logs name things, such as `{ target: (name) => \`workflow "${name}"\`, targets: 'workflows' }`. */
  labels?: SchedulerLabels;
  /** Where it logs what it did (a deletion, a takeover) and what failed. Default: a Nest `Logger`. */
  logger?: Pick<LoggerService, 'log' | 'warn' | 'error'>;
}

/**
 * How the `Scheduler`'s logs name a package's things.
 *
 * ```ts
 * const labels: SchedulerLabels = { target: (name) => `queue "${name}"`, targets: 'queues', payload: 'data', upsert: 'schedules.upsert()' };
 * ```
 */
export interface SchedulerLabels {
  /** A target, such as `workflow "digest"`. Default `target "digest"`. */
  target?(name: string): string;
  /** The targets, as what declares schedules, such as `'workflows'`. Default `'targets'`. */
  targets?: string;
  /** A schedule's payload, such as `'input'`. Default `'payload'`. */
  payload?: string;
  /** The call that saves a schedule at runtime, such as `'WorkflowSchedules.upsert()'`. Default `'upsert()'`. */
  upsert?: string;
}
