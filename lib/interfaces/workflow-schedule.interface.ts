import type { Type } from '@nestjs/common';
import type { Duration } from '../core/time/duration.js';
import type { WorkflowInput } from './workflow-runner.interface.js';

/**
 * When a schedule's occurrences are: exactly one of `cron`, `every` or `rrule`, with its time zone and
 * bounds. Times are wall-clock times in `tz`: on the day clocks go forward, a time that doesn't exist runs
 * that much later (02:30 runs at 03:30); on the day they go back, a time that happens twice runs once, the
 * first time. For a fixed interval whatever the clocks do, use `every` (or `tz: 'UTC'`).
 */
export interface WorkflowScheduleTiming {
  /**
   * A cron expression: 5 fields (minute, hour, day of month, month, day of week) or 6 (seconds first), such as
   * `'0 8 * * MON'` or `'0 0 8 * * MON'`. Ranges, steps, lists, names (`JAN`, `MON-FRI`), `L` (the last day of
   * the month; `5L` the last Friday), `#` (`1#2`: the second Monday) and `@daily`-style nicknames. When both
   * the day of month and the day of week are restricted, a day matching either runs, as in cron.
   */
  cron?: string;
  /** A fixed interval of at least a second, counted from `startAt` (default: the Unix epoch), whatever `tz` says. */
  every?: Duration;
  /**
   * An RFC 5545 recurrence rule: FREQ, INTERVAL, COUNT, UNTIL, WKST, BYMONTH, BYMONTHDAY, BYDAY (with ordinals:
   * `-1FR`), BYHOUR, BYMINUTE and BYSECOND, such as `'FREQ=MONTHLY;BYDAY=-1FR;BYHOUR=17'`. `startAt` is its
   * DTSTART (default: midnight, 1 January 1970, in `tz`), whose fields fill the parts the rule leaves out (so
   * give BYHOUR and BYMINUTE), and COUNT counts from it (so COUNT needs a `startAt`).
   */
  rrule?: string;
  /** The IANA time zone of `cron` and `rrule`, such as `'Europe/Warsaw'`. Default `'UTC'`. */
  tz?: string;
  /** No occurrence before it. With `every`, the first occurrence; with `rrule`, its DTSTART. */
  startAt?: Date | number;
  /** No occurrence after it. */
  endAt?: Date | number;
}

/**
 * What a schedule does with occurrences no worker was up to start (a deploy, an outage): `'skip'` them (only an
 * occurrence found within a minute of its time starts), start the latest of them `'once'`, or start `'all'` of
 * them at once (the latest 100 at most; it needs `overlap: 'allow'`).
 */
export type WorkflowScheduleMissed = 'skip' | 'once' | 'all';

/**
 * What an occurrence does while an instance the schedule started is still running: `'skip'` it, `'allow'` it
 * (both run), `'cancel-previous'` (the running one is cancelled, and compensates, while the new one starts), or
 * `'buffer-one'` (it starts once the running one ends; one occurrence waits at most, later ones are skipped).
 */
export type WorkflowScheduleOverlap = 'skip' | 'allow' | 'cancel-previous' | 'buffer-one';

/** What `@Workflow(name, { schedules })` and `WorkflowSchedules.upsert()` share. */
export interface WorkflowScheduleOptions extends WorkflowScheduleTiming {
  /** The most occurrences it starts, after which it ends. Default: none. */
  limit?: number;
  /** Default `'skip'`. */
  missed?: WorkflowScheduleMissed;
  /** Default `'skip'`. */
  overlap?: WorkflowScheduleOverlap;
  /** The priority of the instances it starts, as `WorkflowClient.start()`'s. Default: none. */
  priority?: number;
}

/** An occurrence of a schedule: what a declared schedule's `input` function receives, and `ctx.schedule`. */
export interface WorkflowScheduleOccurrence {
  /** The schedule's id. */
  id: string;
  /** When the occurrence was due (epoch milliseconds), also when it starts later (`missed`, `'buffer-one'`) or was triggered (then: when). */
  at: number;
}

/** One entry of `@Workflow(name, { schedules })`. */
export interface WorkflowScheduleDeclaration<I = any> extends WorkflowScheduleOptions {
  /** Unique across the application's schedules: letters, digits, `.`, `:`, `_` and `-`. */
  id: string;
  /**
   * The input of the instances it starts: a JSON value, or a function of the occurrence, called by the worker
   * that starts it (`input: ({ at }) => ({ week: isoWeek(at) })`). Default: none.
   */
  input?: ((occurrence: WorkflowScheduleOccurrence) => I) | ScheduleInputValue<I>;
}

/**
 * A declared schedule's input as a value. Without a type for it, any JSON value: not `I` itself, since `any` or
 * `unknown` in the union would swallow the function beside it, and with it the occurrence's type for its parameter.
 */
type ScheduleInputValue<I> = unknown extends I ? string | number | boolean | object | null : I;

/** What `WorkflowSchedules.upsert()` takes. */
export interface UpsertWorkflowScheduleOptions<W = unknown> extends WorkflowScheduleOptions {
  /** The workflow it starts: the class, or its name. */
  workflow: Type<W> | string;
  /** Pin a version. Default: the highest the worker that starts an occurrence registers. */
  version?: number;
  /** The input of the instances it starts: a JSON value, stored with the schedule. Default: none. */
  input?: WorkflowInput<W>;
}

/** What `WorkflowSchedules.preview()` takes, with the spec of a schedule that isn't stored. */
export type WorkflowSchedulePreviewSpec = WorkflowScheduleTiming & Pick<WorkflowScheduleOptions, 'limit'>;

/** A schedule, as `WorkflowSchedules.get()` and `list()` return it. */
export interface WorkflowSchedule {
  id: string;
  /** The workflow it starts. */
  workflow: string;
  /** The version it starts, or `null`: the highest the worker that starts an occurrence registers. */
  version: number | null;
  /** Declared with `@Workflow(name, { schedules })`: kept in step with the code at startup. */
  declared: boolean;
  cron?: string;
  /** In milliseconds. */
  every?: number;
  rrule?: string;
  tz: string;
  startAt: number | null;
  endAt: number | null;
  limit: number | null;
  missed: WorkflowScheduleMissed;
  overlap: WorkflowScheduleOverlap;
  /** `0`: none. */
  priority: number;
  /** The stored input; `undefined` for a declared schedule whose input is a function. */
  input?: unknown;
  paused: boolean;
  /** When its next occurrence is due (epoch milliseconds), or `null`: paused, or ended (`endAt`, `limit`, COUNT, UNTIL). */
  nextAt: number | null;
  /** Occurrences it started (or buffered) so far: what `limit` counts. */
  runs: number;
  /** An occurrence waiting for the running instance to end (`overlap: 'buffer-one'`), or `null`. */
  bufferedAt: number | null;
  createdAt: number;
  updatedAt: number;
}

/** What `WorkflowSchedules.list()` takes. */
export interface WorkflowScheduleListFilter {
  /** Only the schedules of this workflow. */
  workflow?: string;
  /** Default 100. */
  limit?: number;
  offset?: number;
}

/** What `WorkflowSchedules.preview()` takes. */
export interface WorkflowSchedulePreviewOptions {
  /** Occurrences after this time. Default: now. */
  from?: Date | number;
  /** How many. Default 10, at most 1,000. */
  count?: number;
}
