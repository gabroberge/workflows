import type { Duration } from '../time/duration.js';

/**
 * When a schedule's occurrences are: exactly one of `cron`, `every` or `rrule`, with its time zone and bounds. Times
 * are wall-clock times in `tz`: on the day clocks go forward, a time that doesn't exist runs that much later (02:30
 * runs at 03:30); on the day they go back, a time that happens twice runs once, the first time. For a fixed interval
 * whatever the clocks do, use `every` (or `tz: 'UTC'`).
 *
 * ```ts
 * const timing: ScheduleTiming = { cron: '0 0 8 * * MON', tz: 'Europe/Warsaw' };
 * ```
 */
export interface ScheduleTiming {
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
 *
 * ```ts
 * const missed: ScheduleMissed = 'once';
 * ```
 */
export type ScheduleMissed = 'skip' | 'once' | 'all';

/**
 * What an occurrence does while a run the schedule started is still unfinished: `'skip'` it, `'allow'` it (both run),
 * `'cancel-previous'` (the running one is cancelled while the new one starts), or `'buffer-one'` (it starts once the
 * running one ends; one occurrence waits at most, later ones are skipped).
 *
 * ```ts
 * const overlap: ScheduleOverlap = 'buffer-one';
 * ```
 */
export type ScheduleOverlap = 'skip' | 'allow' | 'cancel-previous' | 'buffer-one';

/**
 * A schedule's timing and policies, as a package takes them (it adds its own, such as workflows' `priority`):
 * `parseSchedule()` checks them.
 *
 * ```ts
 * const schedule: ScheduleOptions = { rrule: 'FREQ=WEEKLY;BYDAY=MO;BYHOUR=8', tz: 'Europe/Warsaw', missed: 'once', limit: 52 };
 * ```
 */
export interface ScheduleOptions extends ScheduleTiming {
  /** The most occurrences it starts, after which it ends. Default: none. */
  limit?: number;
  /** Default `'skip'`. */
  missed?: ScheduleMissed;
  /** Default `'skip'`. */
  overlap?: ScheduleOverlap;
}

/**
 * A schedule's options, checked, as `parseSchedule()` returns them and a `ScheduleStore` keeps them (`spec`, JSON):
 * one of `cron`, `every` (milliseconds) or `rrule`, times in epoch milliseconds, defaults applied. A package adds
 * fields of its own, which the `Scheduler` keeps and compares with the rest.
 *
 * ```ts
 * const spec: ScheduleSpec = parseSchedule({ every: '15m' });
 * // { every: 900_000, tz: 'UTC', startAt: null, endAt: null, limit: null, missed: 'skip', overlap: 'skip' }
 * ```
 */
export interface ScheduleSpec {
  cron?: string;
  every?: number;
  rrule?: string;
  tz: string;
  startAt: number | null;
  endAt: number | null;
  limit: number | null;
  missed: ScheduleMissed;
  overlap: ScheduleOverlap;
}

/**
 * An occurrence of a schedule: what a declared schedule's payload function receives. The run it starts has the id
 * `occurrenceId(id, at)`.
 *
 * ```ts
 * const data = ({ at }: ScheduleOccurrence) => ({ week: isoWeek(new Date(at)) });
 * ```
 */
export interface ScheduleOccurrence {
  /** The schedule's id. */
  id: string;
  /** When the occurrence was due (epoch milliseconds), also when it starts later (`missed`, `'buffer-one'`) or was triggered (then: when). */
  at: number;
}
