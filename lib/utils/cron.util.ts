import { fromWallTime, recentClockChange, toWallTime } from './time-zone.util.js';

const SECOND = 1_000;
const DAY = 86_400_000;
const HORIZON_YEARS = 50;
// Any fixed wall time does: a cron that matches at all does so within 50 years of it (February 29 within 8 years,
// the fifth Monday of February within 40, as 2100 isn't a leap year).
const REFERENCE = Date.UTC(2000, 0, 1);

const MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const LONGEST_MONTHS = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

const NICKNAMES: Record<string, string> = {
  '@yearly': '0 0 1 1 *',
  '@annually': '0 0 1 1 *',
  '@monthly': '0 0 1 * *',
  '@weekly': '0 0 * * 0',
  '@daily': '0 0 * * *',
  '@midnight': '0 0 * * *',
  '@hourly': '0 * * * *',
};

interface Field {
  readonly min: number;
  readonly max: number;
  /** Names of `min`, `min + 1`, ... */
  readonly names?: readonly string[];
  /** What the field accepts, for errors. */
  readonly accepts: string;
}

const SECONDS: Field = { min: 0, max: 59, accepts: 'seconds are 0-59' };
const MINUTES: Field = { min: 0, max: 59, accepts: 'minutes are 0-59' };
const HOURS: Field = { min: 0, max: 23, accepts: 'hours are 0-23' };
const DAYS_OF_MONTH: Field = { min: 1, max: 31, accepts: 'days of the month are 1-31 or L (the last day)' };
const MONTHS: Field = {
  min: 1,
  max: 12,
  names: ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'],
  accepts: 'months are 1-12 or JAN-DEC',
};
const DAYS_OF_WEEK: Field = {
  min: 0,
  max: 7,
  names: ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'],
  accepts: 'days of the week are 0-7 (0 and 7 are Sunday) or SUN-SAT, nL (5L is the last Friday) or n#k (1#1 is the first Monday)',
};

type Fail = (reason: string) => never;

/** `n#k` in the day-of-week field: the `nth` (1-5) `weekday` (0-6, Sunday is 0) of the month. */
export interface CronNthWeekday {
  readonly weekday: number;
  readonly nth: number;
}

/** A parsed cron expression: the values each field allows, ascending. */
export interface CronExpression {
  /** The expression as given. */
  readonly source: string;
  readonly seconds: readonly number[];
  readonly minutes: readonly number[];
  readonly hours: readonly number[];
  /** 1-31; `L` is `lastDayOfMonth`. */
  readonly daysOfMonth: readonly number[];
  readonly lastDayOfMonth: boolean;
  /** 1-12. */
  readonly months: readonly number[];
  /** 0-6, Sunday is 0 (7 is read as 0). */
  readonly daysOfWeek: readonly number[];
  /** `nL`: the last such weekday of the month. */
  readonly lastWeekdays: readonly number[];
  readonly nthWeekdays: readonly CronNthWeekday[];
  /** Whether the day-of-month text starts with something other than `*` or `?` (Vixie cron's DOM_STAR, negated). */
  readonly dayOfMonthRestricted: boolean;
  /** Whether the day-of-week text starts with something other than `*` or `?` (Vixie cron's DOW_STAR, negated). */
  readonly dayOfWeekRestricted: boolean;
}

/**
 * Parses 5 fields (minute hour day-of-month month day-of-week) or 6 (second first), whitespace-separated, or a
 * nickname (`@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`). A field is a list
 * (`a,b-c,*\/n`) of `*` or `?` (any), values, ranges `a-b` and steps `*\/n`, `a-b/n` and `a/n` (a to the field's
 * maximum). Months take JAN-DEC and days of the week SUN-SAT (any case, in ranges too); days of the week are 0-7,
 * where 0 and 7 are Sunday. The day of the month also takes `L` (its last day), the day of the week `nL` (the last
 * weekday n of the month, `5L` the last Friday) and `n#k` (the k-th weekday n, k 1-5, `1#1` the first Monday).
 *
 * Days match as in Vixie cron and cronie: when both day fields are restricted, a day that matches either one runs;
 * otherwise a day must match both. A field whose text starts with `*` or `?` is not restricted, even with a step
 * (Vixie's DOM_STAR and DOW_STAR), so `0 0 *\/2 * 1` runs on odd days of the month that are Mondays, while
 * `0 0 1,15 * 1` runs on the 1st, the 15th and every Monday.
 *
 * Throws a TypeError for a malformed expression and for one that never matches (February 30).
 */
export function parseCron(expression: string): CronExpression {
  if (typeof expression !== 'string') {
    throw new TypeError(`Invalid cron expression ${JSON.stringify(expression)}. Use a string, such as "0 9 * * MON-FRI".`);
  }

  const trimmed = expression.trim();
  const text = trimmed.startsWith('@') ? NICKNAMES[trimmed.toLowerCase()] : trimmed;
  if (text === undefined) {
    throw new TypeError(
      `Invalid cron expression "${expression}": use @yearly, @annually, @monthly, @weekly, @daily, @midnight or @hourly, or 5 or 6 fields.`,
    );
  }
  const fields = text === '' ? [] : text.split(/\s+/);
  if (fields.length !== 5 && fields.length !== 6) {
    throw new TypeError(
      `Invalid cron expression "${expression}": use 5 fields (minute hour day-of-month month day-of-week) or 6 (second first), not ${fields.length}.`,
    );
  }

  const [second, minute, hour, dayOfMonth, month, dayOfWeek] = fields.length === 6 ? fields : ['0', ...fields];
  const failIn = (field: string): Fail => (reason) => {
    throw new TypeError(`Invalid cron field "${field}" in "${expression}": ${reason}.`);
  };
  const cron: CronExpression = {
    source: expression,
    seconds: parseList(second, SECONDS, failIn(second)),
    minutes: parseList(minute, MINUTES, failIn(minute)),
    hours: parseList(hour, HOURS, failIn(hour)),
    ...parseDaysOfMonth(dayOfMonth, failIn(dayOfMonth)),
    months: parseList(month, MONTHS, failIn(month)),
    ...parseDaysOfWeek(dayOfWeek, failIn(dayOfWeek)),
    dayOfMonthRestricted: !/^[*?]/.test(dayOfMonth),
    dayOfWeekRestricted: !/^[*?]/.test(dayOfWeek),
  };

  if (nextMatch(cron, REFERENCE, addYears(REFERENCE, HORIZON_YEARS)) === null) {
    throw new TypeError(`Invalid cron expression "${expression}": it never matches${neverReason(cron)}.`);
  }
  return cron;
}

/**
 * The first occurrence strictly after instant `after`, in `tz`; null if none within 50 years. An occurrence is a
 * wall time in `tz` that matches every field, to the second, mapped to an instant as `fromWallTime` does: on the day
 * clocks go forward a skipped 02:30 runs at 03:30 (once, even when 03:30 matches too); on the day they go back a
 * repeated 02:30 runs once, the first time (so an every-minute cron skips the repeated hour's second pass).
 */
export function nextCron(cron: CronExpression, after: number, tz: string): number | null {
  // Right after clocks went forward, a skipped wall time that lands later is still ahead, though its wall time is
  // before `after`'s: search from as far back as they jumped.
  const from = Math.floor((toWallTime(after, tz) - Math.max(0, recentClockChange(after, tz))) / SECOND) * SECOND + SECOND;
  let until = addYears(from, HORIZON_YEARS);
  let next: number | null = null;
  for (let wall = nextMatch(cron, from, until); wall !== null; wall = nextMatch(cron, wall + SECOND, until)) {
    const instant = fromWallTime(wall, tz);
    if (instant > after && (next === null || instant < next)) {
      next = instant;
      // A wall time in a gap lands the gap's length later, so a match between it and where it landed comes first
      // (with a 30-minute gap, 02:10 lands at 02:40, after 02:35).
      until = toWallTime(instant, tz) - SECOND;
    }
  }
  return next;
}

/** Occurrences strictly after `after`, ascending, strictly increasing (no duplicate instants), in `tz`. */
export function* cronOccurrences(cron: CronExpression, after: number, tz: string): Generator<number> {
  for (let next = nextCron(cron, after, tz); next !== null; next = nextCron(cron, next, tz)) {
    yield next;
  }
}

/** The first wall time at or after `from` (a whole second) that every field allows, or null past `until`. */
function nextMatch(cron: CronExpression, from: number, until: number): number | null {
  let wall = from;
  // A field that doesn't allow its value moves to its next allowed one, resetting the fields below; when none is
  // left, it moves the field above instead.
  while (wall <= until) {
    const date = new Date(wall);
    const year = date.getUTCFullYear();
    const month = date.getUTCMonth() + 1;
    const day = date.getUTCDate();
    const hour = date.getUTCHours();
    const minute = date.getUTCMinutes();
    const second = date.getUTCSeconds();

    const nextMonth = following(cron.months, month);
    if (nextMonth !== month) {
      wall = nextMonth === undefined ? Date.UTC(year + 1, cron.months[0] - 1, 1) : Date.UTC(year, nextMonth - 1, 1);
      continue;
    }
    if (!dayMatches(cron, year, month, day, date.getUTCDay())) {
      wall = Date.UTC(year, month - 1, day + 1);
      continue;
    }
    const nextHour = following(cron.hours, hour);
    if (nextHour !== hour) {
      wall = nextHour === undefined ? Date.UTC(year, month - 1, day + 1) : Date.UTC(year, month - 1, day, nextHour);
      continue;
    }
    const nextMinute = following(cron.minutes, minute);
    if (nextMinute !== minute) {
      wall = nextMinute === undefined ? Date.UTC(year, month - 1, day, hour + 1) : Date.UTC(year, month - 1, day, hour, nextMinute);
      continue;
    }
    const nextSecond = following(cron.seconds, second);
    if (nextSecond !== second) {
      wall =
        nextSecond === undefined ? Date.UTC(year, month - 1, day, hour, minute + 1) : Date.UTC(year, month - 1, day, hour, minute, nextSecond);
      continue;
    }
    return wall;
  }
  return null;
}

function dayMatches(cron: CronExpression, year: number, month: number, day: number, weekday: number): boolean {
  const length = daysIn(year, month);
  const byMonthDay = cron.daysOfMonth.includes(day) || (cron.lastDayOfMonth && day === length);
  const byWeekday =
    cron.daysOfWeek.includes(weekday) ||
    (cron.lastWeekdays.includes(weekday) && day + 7 > length) ||
    cron.nthWeekdays.some((nth) => nth.weekday === weekday && Math.ceil(day / 7) === nth.nth);

  return cron.dayOfMonthRestricted && cron.dayOfWeekRestricted ? byMonthDay || byWeekday : byMonthDay && byWeekday;
}

function parseDaysOfMonth(text: string, fail: Fail): Pick<CronExpression, 'daysOfMonth' | 'lastDayOfMonth'> {
  const items = text.split(',');
  const lastDayOfMonth = items.some((item) => item.toUpperCase() === 'L');
  const rest = items.filter((item) => item.toUpperCase() !== 'L');
  return { daysOfMonth: rest.length > 0 ? parseList(rest.join(','), DAYS_OF_MONTH, fail) : [], lastDayOfMonth };
}

function parseDaysOfWeek(text: string, fail: Fail): Pick<CronExpression, 'daysOfWeek' | 'lastWeekdays' | 'nthWeekdays'> {
  const lastWeekdays = new Set<number>();
  const nthWeekdays: CronNthWeekday[] = [];
  const rest: string[] = [];
  for (const item of text.split(',')) {
    const last = /^(\w+)L$/i.exec(item);
    const nth = /^(\w+)#(.*)$/.exec(item);
    if (last) {
      lastWeekdays.add(parseValue(last[1], DAYS_OF_WEEK, fail) % 7);
    } else if (nth) {
      if (!/^[1-5]$/.test(nth[2])) {
        fail(`k in n#k is 1-5 (1#1 is the first Monday of the month)`);
      }
      nthWeekdays.push({ weekday: parseValue(nth[1], DAYS_OF_WEEK, fail) % 7, nth: Number(nth[2]) });
    } else {
      rest.push(item);
    }
  }

  const daysOfWeek = rest.length > 0 ? parseList(rest.join(','), DAYS_OF_WEEK, fail).map((day) => day % 7) : [];
  return { daysOfWeek: [...new Set(daysOfWeek)].sort((a, b) => a - b), lastWeekdays: [...lastWeekdays].sort((a, b) => a - b), nthWeekdays };
}

/** The values a list of `*`, `?`, values, ranges and steps allows, ascending. */
function parseList(text: string, field: Field, fail: Fail): number[] {
  const values = new Set<number>();
  for (const item of text.split(',')) {
    if (item === '') {
      fail('a list item is empty');
    }
    const match = /^(?:([*?])|([^-/]+)(?:-([^-/]+))?)(?:\/(.*))?$/.exec(item);
    if (!match) {
      fail(field.accepts);
    }

    const [, any, low, high, step] = match;
    if (step !== undefined && !/^\d+$/.test(step)) {
      fail('a step is a whole number, such as */15');
    }
    const by = step === undefined ? 1 : Number(step);
    if (by === 0) {
      fail('a step of 0 never moves; use a positive one, such as */15');
    }
    const from = any ? field.min : parseValue(low, field, fail);
    const to = any ? field.max : high !== undefined ? parseValue(high, field, fail) : step !== undefined ? field.max : from;
    if (from > to) {
      fail(`the range ${low}-${high} is reversed; put the lower bound first`);
    }

    for (let value = from; value <= to; value += by) {
      values.add(value);
    }
  }
  return [...values].sort((a, b) => a - b);
}

function parseValue(text: string, field: Field, fail: Fail): number {
  const named = field.names?.indexOf(text.toUpperCase()) ?? -1;
  if (named >= 0) {
    return field.min + named;
  }
  if (!/^\d+$/.test(text) || Number(text) < field.min || Number(text) > field.max) {
    fail(field.accepts);
  }
  return Number(text);
}

/** Why a cron never matches, when it's because every day of the month it allows is past the end of its months. */
function neverReason(cron: CronExpression): string {
  const [first] = cron.daysOfMonth;
  if (cron.lastDayOfMonth || first === undefined || cron.months.some((month) => LONGEST_MONTHS[month - 1] >= first)) {
    return '';
  }

  const months = cron.months.map((month) => MONTH_NAMES[month - 1]);
  return ` (${join(months, 'and')} ${months.length === 1 ? 'has' : 'have'} no ${join(cron.daysOfMonth.map(ordinal), 'or')})`;
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

function addYears(wall: number, years: number): number {
  const date = new Date(wall);
  date.setUTCFullYear(date.getUTCFullYear() + years);
  return date.getTime();
}

function ordinal(n: number): string {
  const suffix = n % 100 >= 11 && n % 100 <= 13 ? 'th' : (['th', 'st', 'nd', 'rd'][n % 10] ?? 'th');
  return `${n}${suffix}`;
}

function join(items: string[], conjunction: string): string {
  return items.length === 1 ? items[0] : `${items.slice(0, -1).join(', ')} ${conjunction} ${items[items.length - 1]}`;
}
