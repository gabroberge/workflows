import { fromWallTime, recentClockChange, toWallTime } from './time-zone.util.js';

const SECOND = 1_000;
const MINUTE = 60_000;
const HOUR = 3_600_000;
const DAY = 86_400_000;
const IDLE_PERIODS = 10_000;

const FREQUENCIES = ['SECONDLY', 'MINUTELY', 'HOURLY', 'DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY'] as const;
const PARTS = ['FREQ', 'INTERVAL', 'COUNT', 'UNTIL', 'WKST', 'BYMONTH', 'BYMONTHDAY', 'BYDAY', 'BYHOUR', 'BYMINUTE', 'BYSECOND'];
const WEEKDAYS = ['SU', 'MO', 'TU', 'WE', 'TH', 'FR', 'SA'];
const ALL_MONTHS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
const UNITS: Partial<Record<RRuleFrequency, number>> = { DAILY: DAY, HOURLY: HOUR, MINUTELY: MINUTE, SECONDLY: SECOND };

/** FREQ: the length of the periods a rule repeats in. */
export type RRuleFrequency = (typeof FREQUENCIES)[number];

/** A BYDAY entry: `weekday` 0-6 (Sunday is 0); `nth` the n-th such day, negative from the end (`-1FR`), 0 for all. */
export interface RRuleWeekday {
  readonly weekday: number;
  readonly nth: number;
}

/** UNTIL: `YYYYMMDD` (`date`, through the end of that day), `YYYYMMDDTHHMMSS` (`local`) or `YYYYMMDDTHHMMSSZ` (`utc`). */
export interface RRuleUntil {
  /** A wall time in the schedule's zone (midnight of the day for `date`), or an instant for `utc`. */
  readonly time: number;
  readonly form: 'date' | 'local' | 'utc';
}

/** A parsed RRULE; a missing BYxxx part is null. */
export interface RRule {
  /** The rule as given. */
  readonly source: string;
  readonly freq: RRuleFrequency;
  readonly interval: number;
  readonly count: number | null;
  readonly until: RRuleUntil | null;
  /** WKST, 0-6 (Sunday is 0; Monday by default). */
  readonly weekStart: number;
  readonly byMonth: readonly number[] | null;
  readonly byMonthDay: readonly number[] | null;
  readonly byDay: readonly RRuleWeekday[] | null;
  readonly byHour: readonly number[] | null;
  readonly byMinute: readonly number[] | null;
  readonly bySecond: readonly number[] | null;
}

/** The rule with DTSTART's defaults filled in: the sets a day and a time of day must be in (null: any). */
interface Plan {
  readonly freq: RRuleFrequency;
  readonly interval: number;
  readonly weekStart: number;
  readonly months: readonly number[] | null;
  readonly monthDays: readonly number[] | null;
  readonly weekdays: readonly RRuleWeekday[] | null;
  /** Numbered weekdays count within the year (YEARLY without BYMONTH), not the month. */
  readonly withinYear: boolean;
  readonly hours: readonly number[] | null;
  readonly minutes: readonly number[] | null;
  readonly seconds: readonly number[] | null;
}

type Fail = (reason: string) => never;

/**
 * Parses an RFC 5545 (§3.3.10) recurrence rule such as "FREQ=WEEKLY;BYDAY=MO;BYHOUR=8", with an optional "RRULE:"
 * prefix, in any case. Parts: FREQ (required), INTERVAL, COUNT or UNTIL, WKST, BYMONTH, BYMONTHDAY (not with
 * WEEKLY), BYDAY (numbered, like `1MO` or `-1FR`, only with MONTHLY or YEARLY), BYHOUR, BYMINUTE and BYSECOND.
 * Throws a TypeError for anything else, for a repeated part, and for a rule that never matches (BYMONTH=2;BYMONTHDAY=30).
 */
export function parseRRule(rule: string): RRule {
  if (typeof rule !== 'string') {
    throw new TypeError(`Invalid RRULE ${JSON.stringify(rule)}. Use a string, such as "FREQ=WEEKLY;BYDAY=MO;BYHOUR=8".`);
  }

  const parts = new Map<string, string>();
  for (const part of rule.trim().replace(/^RRULE:/i, '').split(';')) {
    if (part.trim() === '') {
      continue;
    }
    const match = /^([A-Za-z][A-Za-z0-9-]*)=(.*)$/.exec(part.trim());
    if (!match) {
      throw new TypeError(`Invalid RRULE part "${part}" in "${rule}": use NAME=VALUE parts separated by ";", such as FREQ=DAILY;BYHOUR=8.`);
    }
    const name = match[1].toUpperCase();
    if (!PARTS.includes(name)) {
      throw new TypeError(
        `Unsupported RRULE part "${name}" in "${rule}": use FREQ, INTERVAL, COUNT, UNTIL, WKST, BYMONTH, BYMONTHDAY, BYDAY, BYHOUR, BYMINUTE or BYSECOND.`,
      );
    }
    if (parts.has(name)) {
      throw new TypeError(`Invalid RRULE "${rule}": ${name} is given twice; give each part once.`);
    }
    parts.set(name, match[2].trim().toUpperCase());
  }

  const invalid = (reason: string) => new TypeError(`Invalid RRULE "${rule}": ${reason}.`);
  const failIn = (name: string): Fail => (reason) => {
    throw new TypeError(`Invalid RRULE part "${name}=${parts.get(name)}" in "${rule}": ${reason}.`);
  };
  const freq = FREQUENCIES.find((frequency) => frequency === parts.get('FREQ'));
  if (!parts.has('FREQ')) {
    throw invalid('FREQ is required, such as FREQ=DAILY');
  }
  if (freq === undefined) {
    throw new TypeError(
      `Invalid RRULE part "FREQ=${parts.get('FREQ')}" in "${rule}": use SECONDLY, MINUTELY, HOURLY, DAILY, WEEKLY, MONTHLY or YEARLY.`,
    );
  }
  if (parts.has('COUNT') && parts.has('UNTIL')) {
    throw invalid('use COUNT or UNTIL, not both');
  }

  const parsed: RRule = {
    source: rule,
    freq,
    interval: parts.has('INTERVAL') ? positive(parts.get('INTERVAL')!, failIn('INTERVAL')) : 1,
    count: parts.has('COUNT') ? positive(parts.get('COUNT')!, failIn('COUNT')) : null,
    until: parts.has('UNTIL') ? parseUntil(parts.get('UNTIL')!, failIn('UNTIL')) : null,
    weekStart: parts.has('WKST') ? weekday(parts.get('WKST')!, failIn('WKST')) : 1,
    byMonth: numbers(parts.get('BYMONTH'), 1, 12, false, failIn('BYMONTH')),
    byMonthDay: numbers(parts.get('BYMONTHDAY'), 1, 31, true, failIn('BYMONTHDAY')),
    byDay: weekdays(parts.get('BYDAY'), failIn('BYDAY')),
    byHour: numbers(parts.get('BYHOUR'), 0, 23, false, failIn('BYHOUR')),
    byMinute: numbers(parts.get('BYMINUTE'), 0, 59, false, failIn('BYMINUTE')),
    bySecond: numbers(parts.get('BYSECOND'), 0, 59, false, failIn('BYSECOND')),
  };

  if (parsed.byMonthDay !== null && parsed.freq === 'WEEKLY') {
    throw invalid("BYMONTHDAY doesn't go with FREQ=WEEKLY (RFC 5545); use BYDAY");
  }
  if (parsed.byDay?.some(({ nth }) => nth !== 0) && parsed.freq !== 'MONTHLY' && parsed.freq !== 'YEARLY') {
    throw invalid('a numbered BYDAY (such as 1MO or -1FR) needs FREQ=MONTHLY or FREQ=YEARLY');
  }
  // Probed without INTERVAL, whose phase depends on the start: this catches BYxxx parts that can never meet.
  const probe: RRule = { ...parsed, interval: 1, count: null, until: null };
  if (rruleOccurrences(probe, null, 'UTC', Number.NEGATIVE_INFINITY).next().done) {
    throw invalid('it never matches, as no date fits all of its BYxxx parts');
  }
  return parsed;
}

/**
 * Occurrences strictly after `after`, ascending, strictly increasing. `start` is DTSTART as an instant (the
 * schedule's startAt) or null: then DTSTART is midnight, 1 January 1970, wall time in `tz` (a Thursday). DTSTART
 * gives the defaults (a missing BYMONTH/BYMONTHDAY/BYDAY/BYHOUR/BYMINUTE/BYSECOND takes DTSTART's value, as RFC 5545
 * says, so "FREQ=WEEKLY;BYDAY=MO;BYHOUR=8" is Mondays 08:00:00 without a start) and the INTERVAL phase; with a
 * `start`, nothing before it occurs. COUNT counts occurrences from DTSTART (and requires `start`); UNTIL is inclusive.
 *
 * Unlike RFC 5545, which counts DTSTART as the first occurrence whether or not it fits the rule, a DTSTART that
 * doesn't fit isn't returned (nor counted), as in the common libraries (python-dateutil, rrule.js). Occurrences are
 * wall times in `tz` mapped as `fromWallTime` does: one skipped by clocks going forward runs that much later (unless
 * that instant already occurs), one repeated by clocks going back runs once, the first time. HOURLY, MINUTELY and
 * SECONDLY periods count wall time, so FREQ=HOURLY runs once in the hour clocks repeat.
 */
export function rruleOccurrences(rule: RRule, start: number | null, tz: string, after: number): Generator<number> {
  if (rule.count !== null && start === null) {
    throw new TypeError(`The RRULE "${rule.source}" has a COUNT, which counts from the schedule's start: give it one.`);
  }
  return occurrences(rule, start, tz, after);
}

/** Whether the rule uses COUNT (the caller requires a start for it). */
export function rruleCounts(rule: RRule): boolean {
  return rule.count !== null;
}

function* occurrences(rule: RRule, start: number | null, tz: string, after: number): Generator<number> {
  const dtstart = start === null ? Date.UTC(1970, 0, 1) : Math.floor(toWallTime(start, tz) / SECOND) * SECOND;
  const first = start ?? fromWallTime(dtstart, tz);
  const until = rule.until === null ? null : untilInstant(rule.until, tz);
  // Right after clocks went forward a skipped wall time can still land after `after`; right after they went back, a
  // wall time past UNTIL's own can still be before it. Both widen the wall times scanned by the clocks' jump.
  const lastWall = until === null ? Infinity : toWallTime(until, tz) + Math.max(0, -recentClockChange(until, tz));
  const from = rule.count !== null || after < first ? dtstart : toWallTime(after, tz) - Math.max(0, recentClockChange(after, tz));

  let counted = 0;
  for (const instant of ascending(wallTimes(plan(rule, dtstart), dtstart, from), tz, lastWall)) {
    if (instant < first) {
      continue;
    }
    if (until !== null && instant > until) {
      return;
    }
    counted += 1;
    if (instant > after) {
      yield instant;
    }
    if (counted === rule.count) {
      return;
    }
  }
}

/**
 * The wall times the rule generates, ascending, from the period that holds `from` (on the INTERVAL grid from
 * DTSTART's period). Expansion follows RFC 5545's table: a BYxxx part coarser than FREQ limits, a finer one expands.
 * Stops after 10,000 periods in a row without one.
 */
function* wallTimes(plan: Plan, dtstart: number, from: number): Generator<number> {
  const origin = periodOf(plan, dtstart);
  const onGrid = (period: number) => origin + Math.ceil((period - origin) / plan.interval) * plan.interval;
  let period = origin + Math.floor((periodOf(plan, from) - origin) / plan.interval) * plan.interval;

  for (let idle = 0; idle < IDLE_PERIODS; ) {
    const { walls, next } = expand(plan, period);
    idle = walls.length === 0 ? idle + 1 : 0;
    yield* walls;
    period = onGrid(Math.max(next, period + 1));
  }
}

/**
 * The instants of `walls`, ascending and each once. A wall time in a gap lands that much later, possibly after the
 * instants of the wall times that follow it, so each instant waits until the walls pass where it landed.
 */
function* ascending(walls: Iterable<number>, tz: string, lastWall: number): Generator<number> {
  const pending: { instant: number; landed: number }[] = [];
  let last = Number.NEGATIVE_INFINITY;
  for (const wall of walls) {
    while (pending.length > 0 && pending[0].landed < wall) {
      last = pending.shift()!.instant;
      yield last;
    }
    if (wall > lastWall) {
      break;
    }

    const instant = fromWallTime(wall, tz);
    let at = pending.length;
    while (at > 0 && pending[at - 1].instant > instant) {
      at -= 1;
    }
    if (instant > last && pending[at - 1]?.instant !== instant) {
      pending.splice(at, 0, { instant, landed: toWallTime(instant, tz) });
    }
  }

  for (const { instant } of pending) {
    yield instant;
  }
}

/** The wall times of `period`, and the period to look at next (a later one when a limit rules out a whole day or month). */
function expand(plan: Plan, period: number): { walls: number[]; next: number } {
  switch (plan.freq) {
    case 'YEARLY':
      return { walls: (plan.months ?? ALL_MONTHS).flatMap((month) => monthWalls(plan, period, month)), next: period + 1 };
    case 'MONTHLY':
      return { walls: monthWalls(plan, Math.floor(period / 12), (period % 12) + 1), next: period + 1 };
    case 'WEEKLY':
      return { walls: weekWalls(plan, period), next: period + 1 };
    default:
      return unitWalls(plan, period, UNITS[plan.freq]!);
  }
}

function monthWalls(plan: Plan, year: number, month: number): number[] {
  const walls: number[] = [];
  const length = daysIn(year, month);
  for (let day = 1; day <= length; day++) {
    walls.push(...dayWalls(plan, year, month, day));
  }
  return walls;
}

function weekWalls(plan: Plan, week: number): number[] {
  const walls: number[] = [];
  const firstDay = 7 * week - 4 + plan.weekStart;
  for (let day = firstDay; day < firstDay + 7; day++) {
    const date = new Date(day * DAY);
    walls.push(...dayWalls(plan, date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate()));
  }
  return walls;
}

function dayWalls(plan: Plan, year: number, month: number, day: number): number[] {
  if (!dayMatches(plan, year, month, day)) {
    return [];
  }
  return times(plan.hours!, plan.minutes!, plan.seconds!).map((time) => Date.UTC(year, month - 1, day) + time);
}

/** DAILY, HOURLY, MINUTELY or SECONDLY: `period` counts `unit`s of wall time since 1970, and fixes the fields down to it. */
function unitWalls(plan: Plan, period: number, unit: number): { walls: number[]; next: number } {
  const date = new Date(period * unit);
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth() + 1;
  const day = date.getUTCDate();
  const hour = date.getUTCHours();
  const minute = date.getUTCMinutes();
  const second = date.getUTCSeconds();
  const skipTo = (wall: number) => ({ walls: [], next: wall / unit });

  if (plan.months !== null && !plan.months.includes(month)) {
    const nextMonth = following(plan.months, month + 1);
    return skipTo(nextMonth === undefined ? Date.UTC(year + 1, plan.months[0] - 1) : Date.UTC(year, nextMonth - 1));
  }
  if (!dayMatches(plan, year, month, day)) {
    return skipTo(Date.UTC(year, month - 1, day + 1));
  }
  if (unit < DAY && plan.hours !== null && !plan.hours.includes(hour)) {
    const nextHour = following(plan.hours, hour + 1);
    return skipTo(Date.UTC(year, month - 1, nextHour === undefined ? day + 1 : day, nextHour ?? 0));
  }
  if (unit < HOUR && plan.minutes !== null && !plan.minutes.includes(minute)) {
    const nextMinute = following(plan.minutes, minute + 1);
    return skipTo(Date.UTC(year, month - 1, day, nextMinute === undefined ? hour + 1 : hour, nextMinute ?? 0));
  }
  if (unit < MINUTE && plan.seconds !== null && !plan.seconds.includes(second)) {
    const nextSecond = following(plan.seconds, second + 1);
    return skipTo(Date.UTC(year, month - 1, day, hour, nextSecond === undefined ? minute + 1 : minute, nextSecond ?? 0));
  }

  const walls = times(unit < DAY ? [hour] : plan.hours!, unit < HOUR ? [minute] : plan.minutes!, unit < MINUTE ? [second] : plan.seconds!);
  return { walls: walls.map((time) => Date.UTC(year, month - 1, day) + time), next: period + 1 };
}

function dayMatches(plan: Plan, year: number, month: number, day: number): boolean {
  if (plan.months !== null && !plan.months.includes(month)) {
    return false;
  }
  // A negative day counts from the month's end; a day the month doesn't have (February 30) never matches.
  const length = daysIn(year, month);
  if (plan.monthDays !== null && !plan.monthDays.some((monthDay) => (monthDay > 0 ? monthDay : length + 1 + monthDay) === day)) {
    return false;
  }
  if (plan.weekdays === null) {
    return true;
  }

  const date = Date.UTC(year, month - 1, day);
  const weekday = weekdayOf(date);
  const position = plan.withinYear ? (date - Date.UTC(year, 0, 1)) / DAY + 1 : day;
  const span = plan.withinYear ? (Date.UTC(year + 1, 0, 1) - Date.UTC(year, 0, 1)) / DAY : length;
  return plan.weekdays.some(
    ({ weekday: wanted, nth }) =>
      wanted === weekday && (nth === 0 || (nth > 0 ? Math.ceil(position / 7) === nth : Math.ceil((span - position + 1) / 7) === -nth)),
  );
}

/**
 * RFC 5545's defaults: a part the rule leaves out, but needs to pin an occurrence down, takes DTSTART's value: the
 * month and day for YEARLY, the day for MONTHLY and the weekday for WEEKLY (unless BYMONTHDAY or BYDAY is given),
 * and the hour, minute and second whenever FREQ is coarser than them.
 */
function plan(rule: RRule, dtstart: number): Plan {
  const date = new Date(dtstart);
  const noDays = rule.byMonthDay === null && rule.byDay === null;
  const coarser = (unit: RRuleFrequency) => FREQUENCIES.indexOf(rule.freq) > FREQUENCIES.indexOf(unit);
  return {
    freq: rule.freq,
    interval: rule.interval,
    weekStart: rule.weekStart,
    months: rule.byMonth ?? (noDays && rule.freq === 'YEARLY' ? [date.getUTCMonth() + 1] : null),
    monthDays: rule.byMonthDay ?? (noDays && (rule.freq === 'YEARLY' || rule.freq === 'MONTHLY') ? [date.getUTCDate()] : null),
    weekdays: rule.byDay ?? (noDays && rule.freq === 'WEEKLY' ? [{ weekday: date.getUTCDay(), nth: 0 }] : null),
    withinYear: rule.freq === 'YEARLY' && rule.byMonth === null,
    hours: rule.byHour ?? (coarser('HOURLY') ? [date.getUTCHours()] : null),
    minutes: rule.byMinute ?? (coarser('MINUTELY') ? [date.getUTCMinutes()] : null),
    seconds: rule.bySecond ?? (coarser('SECONDLY') ? [date.getUTCSeconds()] : null),
  };
}

function periodOf(plan: Plan, wall: number): number {
  const date = new Date(wall);
  switch (plan.freq) {
    case 'YEARLY':
      return date.getUTCFullYear();
    case 'MONTHLY':
      return date.getUTCFullYear() * 12 + date.getUTCMonth();
    case 'WEEKLY':
      // 1 January 1970, day 0, is a Thursday (4): weeks start on the WKST day.
      return Math.floor((Math.floor(wall / DAY) + 4 - plan.weekStart) / 7);
    default:
      return Math.floor(wall / UNITS[plan.freq]!);
  }
}

/** Times of day (ms), ascending. */
function times(hours: readonly number[], minutes: readonly number[], seconds: readonly number[]): number[] {
  return hours.flatMap((hour) => minutes.flatMap((minute) => seconds.map((second) => hour * HOUR + minute * MINUTE + second * SECOND)));
}

function untilInstant(until: RRuleUntil, tz: string): number {
  switch (until.form) {
    case 'utc':
      return until.time;
    case 'local':
      return fromWallTime(until.time, tz);
    default:
      return fromWallTime(until.time + DAY, tz) - 1;
  }
}

function parseUntil(value: string, fail: Fail): RRuleUntil {
  const match = /^(\d{4})(\d{2})(\d{2})(?:T(\d{2})(\d{2})(\d{2})(Z?))?$/.exec(value);
  const [year, month, day, hour, minute, second] = (match?.slice(1, 7) ?? []).map((field) => Number(field ?? 0));
  if (!match || month < 1 || month > 12 || day < 1 || day > daysIn(year, month) || hour > 23 || minute > 59 || second > 59) {
    fail('UNTIL is a date, YYYYMMDD, or a date and time, YYYYMMDDTHHMMSS in the schedule\'s time zone or YYYYMMDDTHHMMSSZ in UTC');
  }

  return {
    time: Date.UTC(year, month - 1, day, hour, minute, second),
    form: match[4] === undefined ? 'date' : match[7] === 'Z' ? 'utc' : 'local',
  };
}

function positive(value: string, fail: Fail): number {
  if (!/^\d+$/.test(value) || !(Number(value) >= 1) || !Number.isSafeInteger(Number(value))) {
    fail('use a positive whole number');
  }
  return Number(value);
}

function weekday(value: string, fail: Fail): number {
  if (!WEEKDAYS.includes(value)) {
    fail('use SU, MO, TU, WE, TH, FR or SA');
  }
  return WEEKDAYS.indexOf(value);
}

/** A list of whole numbers from `min` to `max`, ascending; `signed`: also -`max` to -`min`. */
function numbers(value: string | undefined, min: number, max: number, signed: boolean, fail: Fail): number[] | null {
  if (value === undefined) {
    return null;
  }

  const items = value.split(',');
  const inRange = (item: string) => (signed ? /^[+-]?\d+$/ : /^\d+$/).test(item) && Math.abs(Number(item)) >= min && Math.abs(Number(item)) <= max;
  if (!items.every(inRange)) {
    fail(signed ? `use ${min} to ${max}, or -${max} to -${min} to count from the end` : `use ${min} to ${max}`);
  }
  return [...new Set(items.map(Number))].sort((a, b) => a - b);
}

function weekdays(value: string | undefined, fail: Fail): RRuleWeekday[] | null {
  if (value === undefined) {
    return null;
  }

  return value.split(',').map((item) => {
    const match = /^([+-]?\d{1,2})?(SU|MO|TU|WE|TH|FR|SA)$/.exec(item);
    const nth = Number(match?.[1] ?? 0);
    if (!match || (match[1] !== undefined && (nth === 0 || Math.abs(nth) > 53))) {
      fail('use SU, MO, TU, WE, TH, FR or SA, numbered 1 to 53 or -1 to -53 for the n-th of the month or year (1MO, -1FR)');
    }
    return { weekday: WEEKDAYS.indexOf(match[2]), nth };
  });
}

function following(values: readonly number[], from: number): number | undefined {
  for (const value of values) {
    if (value >= from) {
      return value;
    }
  }
  return undefined;
}

function daysIn(year: number, month: number): number {
  return (Date.UTC(year, month, 1) - Date.UTC(year, month - 1, 1)) / DAY;
}

/** 0-6, Sunday is 0, of the day starting at wall time `date` (1 January 1970 was a Thursday). */
function weekdayOf(date: number): number {
  return (((Math.floor(date / DAY) + 4) % 7) + 7) % 7;
}
