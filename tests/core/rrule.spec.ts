/**
 * RFC 5545 recurrence rules (§3.3.10): parsing, the examples of §3.8.5.3 that fit the supported subset (their
 * DTSTART is in America/New_York, which went back to EST on 26 October 1997 and forward to EDT on 5 April 1998), the
 * defaults a rule takes from DTSTART, and daylight saving changes in Europe/Warsaw (02:00-03:00 skipped on 29 March
 * and repeated on 25 October 2026).
 */
import { parseRRule, rruleCounts, rruleOccurrences } from '../../lib/core/scheduling/rrule.util.js';

const NEW_YORK = 'America/New_York';
const WARSAW = 'Europe/Warsaw';
const LORD_HOWE = 'Australia/Lord_Howe';
const EDT = '-04:00';
const EST = '-05:00';

const at = (iso: string) => Date.parse(iso);
const iso = (instant: number) => new Date(instant).toISOString();
/** ISO strings with any offset, as UTC ones. */
const utc = (...list: string[]) => list.map((item) => iso(at(item)));
const range = (from: number, to: number, step = 1) => Array.from({ length: Math.floor((to - from) / step) + 1 }, (_, i) => from + i * step);

/** `list` days of `month` (YYYY-MM) at `time`, with `offset`, as UTC ISO strings. */
function days(month: string, list: number[], offset: string, time = '09:00'): string[] {
  return list.map((day) => iso(at(`${month}-${String(day).padStart(2, '0')}T${time}:00${offset}`)));
}

/** `list` times (HH:MM) on `date` (YYYY-MM-DD), with `offset`, as UTC ISO strings. */
function times(date: string, list: string[], offset: string): string[] {
  return list.map((time) => iso(at(`${date}T${time}:00${offset}`)));
}

/** Up to `limit` occurrences, as UTC ISO strings, checking that they strictly increase. */
function take(occurrences: Iterable<number>, limit: number): string[] {
  const instants: number[] = [];
  for (const instant of occurrences) {
    expect(instant).toBeGreaterThan(instants[instants.length - 1] ?? Number.NEGATIVE_INFINITY);
    instants.push(instant);
    if (instants.length === limit) {
      break;
    }
  }
  return instants.map(iso);
}

/** The occurrences from DTSTART on, as the RFC lists them (a finite rule's all of them). */
function fromStart(rule: string, dtstart: string, limit = 1_000, tz = NEW_YORK): string[] {
  const start = at(dtstart);
  return take(rruleOccurrences(parseRRule(rule), start, tz, start - 1), limit);
}

/** The occurrences strictly after `after`. */
function after(rule: string, options: { start?: string; after: string; limit: number; tz: string }): string[] {
  const start = options.start === undefined ? null : at(options.start);
  return take(rruleOccurrences(parseRRule(rule), start, options.tz, at(options.after)), options.limit);
}

function thrown(fn: () => unknown): Error {
  try {
    fn();
  } catch (error) {
    return error as Error;
  }
  throw new Error('Expected a throw.');
}

describe('RFC 5545 examples, with DTSTART in America/New_York', () => {
  it('daily for 10 occurrences', () => {
    expect(fromStart('FREQ=DAILY;COUNT=10', '1997-09-02T09:00:00-04:00')).toEqual(days('1997-09', range(2, 11), EDT));
  });

  it('daily until December 24, 1997, across the change to EST', () => {
    expect(fromStart('FREQ=DAILY;UNTIL=19971224T000000Z', '1997-09-02T09:00:00-04:00')).toEqual([
      ...days('1997-09', range(2, 30), EDT),
      ...days('1997-10', range(1, 25), EDT),
      ...days('1997-10', range(26, 31), EST),
      ...days('1997-11', range(1, 30), EST),
      ...days('1997-12', range(1, 23), EST),
    ]);
  });

  it('every other day, forever', () => {
    expect(fromStart('FREQ=DAILY;INTERVAL=2', '1997-09-02T09:00:00-04:00', 47)).toEqual([
      ...days('1997-09', range(2, 30, 2), EDT),
      ...days('1997-10', range(2, 24, 2), EDT),
      ...days('1997-10', [26, 28, 30], EST),
      ...days('1997-11', range(1, 29, 2), EST),
      ...days('1997-12', [1, 3], EST),
    ]);
  });

  it('every 10 days, 5 occurrences', () => {
    expect(fromStart('FREQ=DAILY;INTERVAL=10;COUNT=5', '1997-09-02T09:00:00-04:00')).toEqual([
      ...days('1997-09', [2, 12, 22], EDT),
      ...days('1997-10', [2, 12], EDT),
    ]);
  });

  it('every day in January, for 3 years, yearly with BYMONTH and BYDAY or daily with BYMONTH', () => {
    const januaries = [1998, 1999, 2000].flatMap((year) => days(`${year}-01`, range(1, 31), EST));
    expect(fromStart('FREQ=YEARLY;UNTIL=20000131T140000Z;BYMONTH=1;BYDAY=SU,MO,TU,WE,TH,FR,SA', '1998-01-01T09:00:00-05:00')).toEqual(januaries);
    expect(fromStart('FREQ=DAILY;UNTIL=20000131T140000Z;BYMONTH=1', '1998-01-01T09:00:00-05:00')).toEqual(januaries);
  });

  it('weekly for 10 occurrences', () => {
    expect(fromStart('FREQ=WEEKLY;COUNT=10', '1997-09-02T09:00:00-04:00')).toEqual([
      ...days('1997-09', [2, 9, 16, 23, 30], EDT),
      ...days('1997-10', [7, 14, 21], EDT),
      ...days('1997-10', [28], EST),
      ...days('1997-11', [4], EST),
    ]);
  });

  it('weekly until December 24, 1997', () => {
    expect(fromStart('FREQ=WEEKLY;UNTIL=19971224T000000Z', '1997-09-02T09:00:00-04:00')).toEqual([
      ...days('1997-09', [2, 9, 16, 23, 30], EDT),
      ...days('1997-10', [7, 14, 21], EDT),
      ...days('1997-10', [28], EST),
      ...days('1997-11', [4, 11, 18, 25], EST),
      ...days('1997-12', [2, 9, 16, 23], EST),
    ]);
  });

  it('every other week, forever', () => {
    expect(fromStart('FREQ=WEEKLY;INTERVAL=2;WKST=SU', '1997-09-02T09:00:00-04:00', 13)).toEqual([
      ...days('1997-09', [2, 16, 30], EDT),
      ...days('1997-10', [14], EDT),
      ...days('1997-10', [28], EST),
      ...days('1997-11', [11, 25], EST),
      ...days('1997-12', [9, 23], EST),
      ...days('1998-01', [6, 20], EST),
      ...days('1998-02', [3, 17], EST),
    ]);
  });

  it('weekly on Tuesday and Thursday for five weeks, until a date or for a count', () => {
    const expected = [...days('1997-09', [2, 4, 9, 11, 16, 18, 23, 25, 30], EDT), ...days('1997-10', [2], EDT)];
    expect(fromStart('FREQ=WEEKLY;UNTIL=19971007T000000Z;WKST=SU;BYDAY=TU,TH', '1997-09-02T09:00:00-04:00')).toEqual(expected);
    expect(fromStart('FREQ=WEEKLY;COUNT=10;WKST=SU;BYDAY=TU,TH', '1997-09-02T09:00:00-04:00')).toEqual(expected);
  });

  it('every other week on Monday, Wednesday and Friday until December 24, 1997, starting on Monday, September 1', () => {
    expect(fromStart('FREQ=WEEKLY;INTERVAL=2;UNTIL=19971224T000000Z;WKST=SU;BYDAY=MO,WE,FR', '1997-09-01T09:00:00-04:00')).toEqual([
      ...days('1997-09', [1, 3, 5, 15, 17, 19, 29], EDT),
      ...days('1997-10', [1, 3, 13, 15, 17], EDT),
      ...days('1997-10', [27, 29, 31], EST),
      ...days('1997-11', [10, 12, 14, 24, 26, 28], EST),
      ...days('1997-12', [8, 10, 12, 22], EST),
    ]);
  });

  it('every other week on Tuesday and Thursday, for 8 occurrences', () => {
    expect(fromStart('FREQ=WEEKLY;INTERVAL=2;COUNT=8;WKST=SU;BYDAY=TU,TH', '1997-09-02T09:00:00-04:00')).toEqual([
      ...days('1997-09', [2, 4, 16, 18, 30], EDT),
      ...days('1997-10', [2, 14, 16], EDT),
    ]);
  });

  it('WKST decides which days a week holds: Tuesdays and Sundays every other week from Tuesday, August 5', () => {
    expect(fromStart('FREQ=WEEKLY;INTERVAL=2;COUNT=4;BYDAY=TU,SU;WKST=MO', '1997-08-05T09:00:00-04:00')).toEqual(days('1997-08', [5, 10, 19, 24], EDT));
    expect(fromStart('FREQ=WEEKLY;INTERVAL=2;COUNT=4;BYDAY=TU,SU;WKST=SU', '1997-08-05T09:00:00-04:00')).toEqual(days('1997-08', [5, 17, 19, 31], EDT));
  });

  it('monthly on the first Friday, for 10 occurrences and until December 24, 1997', () => {
    expect(fromStart('FREQ=MONTHLY;COUNT=10;BYDAY=1FR', '1997-09-05T09:00:00-04:00')).toEqual([
      ...days('1997-09', [5], EDT),
      ...days('1997-10', [3], EDT),
      ...days('1997-11', [7], EST),
      ...days('1997-12', [5], EST),
      ...days('1998-01', [2], EST),
      ...days('1998-02', [6], EST),
      ...days('1998-03', [6], EST),
      ...days('1998-04', [3], EST),
      ...days('1998-05', [1], EDT),
      ...days('1998-06', [5], EDT),
    ]);
    expect(fromStart('FREQ=MONTHLY;UNTIL=19971224T000000Z;BYDAY=1FR', '1997-09-05T09:00:00-04:00')).toEqual([
      ...days('1997-09', [5], EDT),
      ...days('1997-10', [3], EDT),
      ...days('1997-11', [7], EST),
      ...days('1997-12', [5], EST),
    ]);
  });

  it('every other month on the first and last Sunday of the month for 10 occurrences', () => {
    expect(fromStart('FREQ=MONTHLY;INTERVAL=2;COUNT=10;BYDAY=1SU,-1SU', '1997-09-07T09:00:00-04:00')).toEqual([
      ...days('1997-09', [7, 28], EDT),
      ...days('1997-11', [2, 30], EST),
      ...days('1998-01', [4, 25], EST),
      ...days('1998-03', [1, 29], EST),
      ...days('1998-05', [3, 31], EDT),
    ]);
  });

  it('monthly on the second-to-last Monday of the month for 6 months', () => {
    expect(fromStart('FREQ=MONTHLY;COUNT=6;BYDAY=-2MO', '1997-09-22T09:00:00-04:00')).toEqual([
      ...days('1997-09', [22], EDT),
      ...days('1997-10', [20], EDT),
      ...days('1997-11', [17], EST),
      ...days('1997-12', [22], EST),
      ...days('1998-01', [19], EST),
      ...days('1998-02', [16], EST),
    ]);
  });

  it('monthly on the third-to-the-last day of the month, forever', () => {
    expect(fromStart('FREQ=MONTHLY;BYMONTHDAY=-3', '1997-09-28T09:00:00-04:00', 6)).toEqual([
      ...days('1997-09', [28], EDT),
      ...days('1997-10', [29], EST),
      ...days('1997-11', [28], EST),
      ...days('1997-12', [29], EST),
      ...days('1998-01', [29], EST),
      ...days('1998-02', [26], EST),
    ]);
  });

  it('monthly on the 2nd and 15th of the month for 10 occurrences', () => {
    expect(fromStart('FREQ=MONTHLY;COUNT=10;BYMONTHDAY=2,15', '1997-09-02T09:00:00-04:00')).toEqual([
      ...days('1997-09', [2, 15], EDT),
      ...days('1997-10', [2, 15], EDT),
      ...days('1997-11', [2, 15], EST),
      ...days('1997-12', [2, 15], EST),
      ...days('1998-01', [2, 15], EST),
    ]);
  });

  it('monthly on the first and last day of the month for 10 occurrences', () => {
    expect(fromStart('FREQ=MONTHLY;COUNT=10;BYMONTHDAY=1,-1', '1997-09-30T09:00:00-04:00')).toEqual([
      ...days('1997-09', [30], EDT),
      ...days('1997-10', [1], EDT),
      ...days('1997-10', [31], EST),
      ...days('1997-11', [1, 30], EST),
      ...days('1997-12', [1, 31], EST),
      ...days('1998-01', [1, 31], EST),
      ...days('1998-02', [1], EST),
    ]);
  });

  it('every 18 months on the 10th thru 15th of the month for 10 occurrences', () => {
    expect(fromStart('FREQ=MONTHLY;INTERVAL=18;COUNT=10;BYMONTHDAY=10,11,12,13,14,15', '1997-09-10T09:00:00-04:00')).toEqual([
      ...days('1997-09', range(10, 15), EDT),
      ...days('1999-03', range(10, 13), EST),
    ]);
  });

  it('every Tuesday, every other month', () => {
    expect(fromStart('FREQ=MONTHLY;INTERVAL=2;BYDAY=TU', '1997-09-02T09:00:00-04:00', 18)).toEqual([
      ...days('1997-09', [2, 9, 16, 23, 30], EDT),
      ...days('1997-11', [4, 11, 18, 25], EST),
      ...days('1998-01', [6, 13, 20, 27], EST),
      ...days('1998-03', [3, 10, 17, 24, 31], EST),
    ]);
  });

  it('yearly in June and July for 10 occurrences, on the day of DTSTART', () => {
    expect(fromStart('FREQ=YEARLY;COUNT=10;BYMONTH=6,7', '1997-06-10T09:00:00-04:00')).toEqual(
      [1997, 1998, 1999, 2000, 2001].flatMap((year) => [...days(`${year}-06`, [10], EDT), ...days(`${year}-07`, [10], EDT)]),
    );
  });

  it('every other year on January, February, and March for 10 occurrences', () => {
    expect(fromStart('FREQ=YEARLY;INTERVAL=2;COUNT=10;BYMONTH=1,2,3', '1997-03-10T09:00:00-05:00')).toEqual([
      ...days('1997-03', [10], EST),
      ...[1999, 2001, 2003].flatMap((year) => [1, 2, 3].flatMap((month) => days(`${year}-0${month}`, [10], EST))),
    ]);
  });

  it('every 20th Monday of the year, forever', () => {
    expect(fromStart('FREQ=YEARLY;BYDAY=20MO', '1997-05-19T09:00:00-04:00', 3)).toEqual([
      ...days('1997-05', [19], EDT),
      ...days('1998-05', [18], EDT),
      ...days('1999-05', [17], EDT),
    ]);
  });

  it('every Thursday in March, forever', () => {
    expect(fromStart('FREQ=YEARLY;BYMONTH=3;BYDAY=TH', '1997-03-13T09:00:00-05:00', 11)).toEqual([
      ...days('1997-03', [13, 20, 27], EST),
      ...days('1998-03', [5, 12, 19, 26], EST),
      ...days('1999-03', [4, 11, 18, 25], EST),
    ]);
  });

  it('every Thursday, but only during June, July, and August, forever', () => {
    expect(fromStart('FREQ=YEARLY;BYDAY=TH;BYMONTH=6,7,8', '1997-06-05T09:00:00-04:00', 39)).toEqual([
      ...days('1997-06', [5, 12, 19, 26], EDT),
      ...days('1997-07', [3, 10, 17, 24, 31], EDT),
      ...days('1997-08', [7, 14, 21, 28], EDT),
      ...days('1998-06', [4, 11, 18, 25], EDT),
      ...days('1998-07', [2, 9, 16, 23, 30], EDT),
      ...days('1998-08', [6, 13, 20, 27], EDT),
      ...days('1999-06', [3, 10, 17, 24], EDT),
      ...days('1999-07', [1, 8, 15, 22, 29], EDT),
      ...days('1999-08', [5, 12, 19, 26], EDT),
    ]);
  });

  it('every Friday the 13th, forever, without DTSTART (a Tuesday: the RFC excludes it with EXDATE, here it never fits)', () => {
    expect(fromStart('FREQ=MONTHLY;BYDAY=FR;BYMONTHDAY=13', '1997-09-02T09:00:00-04:00', 5)).toEqual([
      ...days('1998-02', [13], EST),
      ...days('1998-03', [13], EST),
      ...days('1998-11', [13], EST),
      ...days('1999-08', [13], EDT),
      ...days('2000-10', [13], EDT),
    ]);
  });

  it('the first Saturday that follows the first Sunday of the month, forever', () => {
    expect(fromStart('FREQ=MONTHLY;BYDAY=SA;BYMONTHDAY=7,8,9,10,11,12,13', '1997-09-13T09:00:00-04:00', 10)).toEqual([
      ...days('1997-09', [13], EDT),
      ...days('1997-10', [11], EDT),
      ...days('1997-11', [8], EST),
      ...days('1997-12', [13], EST),
      ...days('1998-01', [10], EST),
      ...days('1998-02', [7], EST),
      ...days('1998-03', [7], EST),
      ...days('1998-04', [11], EDT),
      ...days('1998-05', [9], EDT),
      ...days('1998-06', [13], EDT),
    ]);
  });

  it('every 4 years, the first Tuesday after a Monday in November, forever (U.S. Presidential Election day)', () => {
    expect(fromStart('FREQ=YEARLY;INTERVAL=4;BYMONTH=11;BYDAY=TU;BYMONTHDAY=2,3,4,5,6,7,8', '1996-11-05T09:00:00-05:00', 3)).toEqual([
      ...days('1996-11', [5], EST),
      ...days('2000-11', [7], EST),
      ...days('2004-11', [2], EST),
    ]);
  });

  it('every 15 minutes for 6 occurrences', () => {
    expect(fromStart('FREQ=MINUTELY;INTERVAL=15;COUNT=6', '1997-09-02T09:00:00-04:00')).toEqual(
      times('1997-09-02', ['09:00', '09:15', '09:30', '09:45', '10:00', '10:15'], EDT),
    );
  });

  it('every hour and a half for 4 occurrences', () => {
    expect(fromStart('FREQ=MINUTELY;INTERVAL=90;COUNT=4', '1997-09-02T09:00:00-04:00')).toEqual(times('1997-09-02', ['09:00', '10:30', '12:00', '13:30'], EDT));
  });

  it('every 20 minutes from 9:00 AM to 4:40 PM every day, daily with BYHOUR and BYMINUTE or minutely with BYHOUR', () => {
    const day = range(9, 16).flatMap((hour) => ['00', '20', '40'].map((minute) => `${String(hour).padStart(2, '0')}:${minute}`));
    const expected = [...times('1997-09-02', day, EDT), ...times('1997-09-03', day, EDT)];
    expect(fromStart('FREQ=DAILY;BYHOUR=9,10,11,12,13,14,15,16;BYMINUTE=0,20,40', '1997-09-02T09:00:00-04:00', 48)).toEqual(expected);
    expect(fromStart('FREQ=MINUTELY;INTERVAL=20;BYHOUR=9,10,11,12,13,14,15,16', '1997-09-02T09:00:00-04:00', 48)).toEqual(expected);
  });

  it('skips an invalid date (February 30)', () => {
    expect(fromStart('FREQ=MONTHLY;BYMONTHDAY=15,30;COUNT=5', '2007-01-15T09:00:00-05:00')).toEqual([
      ...days('2007-01', [15, 30], EST),
      ...days('2007-02', [15], EST),
      ...days('2007-03', [15, 30], EDT),
    ]);
  });
});

describe('rruleOccurrences()', () => {
  describe('without a start, from a DTSTART of midnight, 1 January 1970, in the zone', () => {
    it('takes the parts a rule leaves out from it: "FREQ=WEEKLY;BYDAY=MO;BYHOUR=8" is Mondays at 08:00:00', () => {
      expect(after('FREQ=WEEKLY;BYDAY=MO;BYHOUR=8', { after: '2026-09-29T10:00:00Z', limit: 4, tz: WARSAW })).toEqual(
        utc('2026-10-05T08:00:00+02:00', '2026-10-12T08:00:00+02:00', '2026-10-19T08:00:00+02:00', '2026-10-26T08:00:00+01:00'),
      );
    });

    it('runs FREQ alone at midnight: daily, on Thursdays, on the 1st, on 1 January, and every hour on the hour', () => {
      const next = (rule: string) => after(rule, { after: '2026-09-29T12:00:00+02:00', limit: 2, tz: WARSAW });
      expect(next('FREQ=DAILY')).toEqual(utc('2026-09-30T00:00:00+02:00', '2026-10-01T00:00:00+02:00'));
      expect(next('FREQ=WEEKLY')).toEqual(utc('2026-10-01T00:00:00+02:00', '2026-10-08T00:00:00+02:00'));
      expect(next('FREQ=MONTHLY')).toEqual(utc('2026-10-01T00:00:00+02:00', '2026-11-01T00:00:00+01:00'));
      expect(next('FREQ=YEARLY')).toEqual(utc('2027-01-01T00:00:00+01:00', '2028-01-01T00:00:00+01:00'));
      expect(next('FREQ=HOURLY')).toEqual(utc('2026-09-29T13:00:00+02:00', '2026-09-29T14:00:00+02:00'));
      expect(next('FREQ=MINUTELY;BYSECOND=15,45')).toEqual(utc('2026-09-29T12:00:15+02:00', '2026-09-29T12:00:45+02:00'));
      expect(next('FREQ=SECONDLY;BYMINUTE=0;BYSECOND=0,30')).toEqual(utc('2026-09-29T12:00:30+02:00', '2026-09-29T13:00:00+02:00'));
    });

    it('counts SECONDLY periods in wall seconds', () => {
      expect(fromStart('FREQ=SECONDLY;INTERVAL=20;COUNT=4', '1997-09-02T09:00:00-04:00')).toEqual(
        utc('1997-09-02T09:00:00-04:00', '1997-09-02T09:00:20-04:00', '1997-09-02T09:00:40-04:00', '1997-09-02T09:01:00-04:00'),
      );
      expect(fromStart('FREQ=SECONDLY;INTERVAL=7;BYSECOND=0;BYMINUTE=0', '1997-09-02T09:00:00-04:00', 3)).toEqual(
        utc('1997-09-02T09:00:00-04:00', '1997-09-02T16:00:00-04:00', '1997-09-02T23:00:00-04:00'),
      );
    });

    it('keeps the INTERVAL phase from it, counting wall time', () => {
      const next = (rule: string, tz: string, limit: number) => after(rule, { after: '2026-09-29T00:00:00Z', limit, tz });
      // 29 September 2026 is day 20,725 since 1 January 1970; its Monday-to-Sunday week is week 2,962.
      expect(next('FREQ=DAILY;INTERVAL=2', 'UTC', 3)).toEqual(utc('2026-09-30T00:00:00Z', '2026-10-02T00:00:00Z', '2026-10-04T00:00:00Z'));
      expect(next('FREQ=WEEKLY;INTERVAL=2;BYDAY=MO', 'UTC', 3)).toEqual(utc('2026-10-05T00:00:00Z', '2026-10-19T00:00:00Z', '2026-11-02T00:00:00Z'));
      // September 2026 is month 680.
      expect(next('FREQ=MONTHLY;INTERVAL=5', 'UTC', 2)).toEqual(utc('2027-02-01T00:00:00Z', '2027-07-01T00:00:00Z'));
      expect(next('FREQ=HOURLY;INTERVAL=5', 'UTC', 5)).toEqual(
        utc('2026-09-29T05:00:00Z', '2026-09-29T10:00:00Z', '2026-09-29T15:00:00Z', '2026-09-29T20:00:00Z', '2026-09-30T01:00:00Z'),
      );
      expect(next('FREQ=HOURLY;INTERVAL=5', WARSAW, 3)).toEqual(utc('2026-09-29T05:00:00+02:00', '2026-09-29T10:00:00+02:00', '2026-09-29T15:00:00+02:00'));
    });
  });

  describe('with a start', () => {
    it('takes the defaults and the INTERVAL phase from it, and nothing occurs before it', () => {
      const every3Days = { start: '2026-09-01T10:00:00+02:00', limit: 3, tz: WARSAW };
      expect(after('FREQ=DAILY;INTERVAL=3', { ...every3Days, after: '2026-08-01T00:00:00Z' })).toEqual(
        utc('2026-09-01T10:00:00+02:00', '2026-09-04T10:00:00+02:00', '2026-09-07T10:00:00+02:00'),
      );
      expect(after('FREQ=DAILY;INTERVAL=3', { ...every3Days, after: '2026-09-29T12:00:00+02:00' })).toEqual(
        utc('2026-10-01T10:00:00+02:00', '2026-10-04T10:00:00+02:00', '2026-10-07T10:00:00+02:00'),
      );
      // Starting on a Wednesday: that week's Monday is before the start.
      expect(after('FREQ=WEEKLY;BYDAY=MO,WE,FR', { start: '2026-09-02T09:00:00+02:00', after: '2026-08-01T00:00:00Z', limit: 3, tz: WARSAW })).toEqual(
        utc('2026-09-02T09:00:00+02:00', '2026-09-04T09:00:00+02:00', '2026-09-07T09:00:00+02:00'),
      );
    });

    it('counts COUNT from the start, whatever `after` is', () => {
      expect(after('FREQ=DAILY;COUNT=10', { start: '1997-09-02T09:00:00-04:00', after: '1997-09-06T09:00:00-04:00', limit: 100, tz: NEW_YORK })).toEqual(
        days('1997-09', range(7, 11), EDT),
      );
      expect(after('FREQ=MONTHLY;COUNT=10;BYDAY=1FR', { start: '1997-09-05T09:00:00-04:00', after: '1998-01-15T00:00:00Z', limit: 100, tz: NEW_YORK })).toEqual([
        ...days('1998-02', [6], EST),
        ...days('1998-03', [6], EST),
        ...days('1998-04', [3], EST),
        ...days('1998-05', [1], EDT),
        ...days('1998-06', [5], EDT),
      ]);
      expect(after('FREQ=DAILY;COUNT=10', { start: '1997-09-02T09:00:00-04:00', after: '1997-09-11T09:00:00-04:00', limit: 100, tz: NEW_YORK })).toEqual([]);
    });

    it('starts from the period that holds `after` without COUNT, on the INTERVAL grid', () => {
      expect(after('FREQ=DAILY;INTERVAL=10', { start: '1997-09-02T09:00:00-04:00', after: '1997-09-15T00:00:00-04:00', limit: 3, tz: NEW_YORK })).toEqual([
        ...days('1997-09', [22], EDT),
        ...days('1997-10', [2, 12], EDT),
      ]);
      expect(after('FREQ=WEEKLY;INTERVAL=2;WKST=SU', { start: '1997-09-02T09:00:00-04:00', after: '1997-12-01T00:00:00-05:00', limit: 3, tz: NEW_YORK })).toEqual([
        ...days('1997-12', [9, 23], EST),
        ...days('1998-01', [6], EST),
      ]);
    });

    it('throws a TypeError for COUNT without one', () => {
      expect(() => rruleOccurrences(parseRRule('FREQ=DAILY;COUNT=3'), null, 'UTC', 0)).toThrow(
        new TypeError(`The RRULE "FREQ=DAILY;COUNT=3" has a COUNT, which counts from the schedule's start: give it one.`),
      );
    });
  });

  describe('UNTIL', () => {
    it.each([
      ['19970905', 5],
      ['19970905T090000', 5],
      ['19970905T085959', 4],
      ['19970905T130000Z', 5],
      ['19970905T125959Z', 4],
    ])('UNTIL=%s is inclusive, then the series ends', (until, last) => {
      expect(fromStart(`FREQ=DAILY;UNTIL=${until}`, '1997-09-02T09:00:00-04:00')).toEqual(days('1997-09', range(2, last), EDT));
    });

    it('runs a date through the end of that day in the zone', () => {
      expect(fromStart('FREQ=HOURLY;UNTIL=19970902', '1997-09-02T20:00:00-04:00')).toEqual(times('1997-09-02', ['20:00', '21:00', '22:00', '23:00'], EDT));
    });

    it('reaches an instant in the second pass of a repeated hour, past wall times that come first', () => {
      // Until 02:15 CET, the second 02:15: the first 02:30 and 02:45 (CEST) are still before it.
      expect(fromStart('FREQ=MINUTELY;INTERVAL=15;UNTIL=20261025T011500Z', '2026-10-25T01:30:00+02:00', 1_000, WARSAW)).toEqual(
        utc(
          '2026-10-25T01:30:00+02:00',
          '2026-10-25T01:45:00+02:00',
          '2026-10-25T02:00:00+02:00',
          '2026-10-25T02:15:00+02:00',
          '2026-10-25T02:30:00+02:00',
          '2026-10-25T02:45:00+02:00',
        ),
      );
    });
  });

  describe('daylight saving time', () => {
    it('runs a daily 02:30 that clocks skip at 03:30 CEST, and a repeated one once, the first time', () => {
      const daily = (from: string) => after('FREQ=DAILY;BYHOUR=2;BYMINUTE=30', { after: from, limit: 3, tz: WARSAW });
      expect(daily('2026-03-27T12:00:00Z')).toEqual(utc('2026-03-28T02:30:00+01:00', '2026-03-29T03:30:00+02:00', '2026-03-30T02:30:00+02:00'));
      expect(daily('2026-10-23T12:00:00Z')).toEqual(utc('2026-10-24T02:30:00+02:00', '2026-10-25T02:30:00+02:00', '2026-10-26T02:30:00+01:00'));
      // Just past the change, the skipped 02:30 is still ahead, though its hour is before the one `after` is in.
      expect(daily('2026-03-29T03:15:00+02:00')).toEqual(utc('2026-03-29T03:30:00+02:00', '2026-03-30T02:30:00+02:00', '2026-03-31T02:30:00+02:00'));
      expect(after('FREQ=HOURLY;BYHOUR=2;BYMINUTE=30', { after: '2026-03-29T03:15:00+02:00', limit: 2, tz: WARSAW })).toEqual(
        utc('2026-03-29T03:30:00+02:00', '2026-03-30T02:30:00+02:00'),
      );
    });

    it('runs two wall times that land on one instant once', () => {
      expect(after('FREQ=DAILY;BYHOUR=2,3;BYMINUTE=30', { after: '2026-03-28T12:00:00Z', limit: 3, tz: WARSAW })).toEqual(
        utc('2026-03-29T03:30:00+02:00', '2026-03-30T02:30:00+02:00', '2026-03-30T03:30:00+02:00'),
      );
    });

    it('runs FREQ=HOURLY in every hour that exists, and once in the hour clocks repeat (periods count wall time)', () => {
      expect(after('FREQ=HOURLY', { after: '2026-03-28T22:30:00Z', limit: 4, tz: WARSAW })).toEqual(
        utc('2026-03-29T00:00:00+01:00', '2026-03-29T01:00:00+01:00', '2026-03-29T03:00:00+02:00', '2026-03-29T04:00:00+02:00'),
      );
      expect(after('FREQ=HOURLY', { after: '2026-10-24T21:30:00Z', limit: 5, tz: WARSAW })).toEqual(
        utc('2026-10-25T00:00:00+02:00', '2026-10-25T01:00:00+02:00', '2026-10-25T02:00:00+02:00', '2026-10-25T03:00:00+01:00', '2026-10-25T04:00:00+01:00'),
      );
    });

    it("orders a skipped wall time that lands later after the ones it passes (Lord Howe's 30-minute change)", () => {
      const expected = utc('2026-10-04T02:35:00+11:00', '2026-10-04T02:40:00+11:00', '2026-10-05T02:10:00+11:00');
      expect(after('FREQ=DAILY;BYHOUR=2;BYMINUTE=10,35', { after: '2026-10-03T12:00:00Z', limit: 3, tz: LORD_HOWE })).toEqual(expected);
      expect(after('FREQ=MINUTELY;BYHOUR=2;BYMINUTE=10,35', { after: '2026-10-03T12:00:00Z', limit: 3, tz: LORD_HOWE })).toEqual(expected);
    });
  });

  it('ends after 10,000 periods without an occurrence, when the start rules every one out', () => {
    // From January 1970 every other month is odd; from 30 January the default day of the month is the 30th.
    expect([...rruleOccurrences(parseRRule('FREQ=MONTHLY;INTERVAL=2;BYMONTH=2'), null, 'UTC', 0)]).toEqual([]);
    expect([...rruleOccurrences(parseRRule('FREQ=YEARLY;BYMONTH=2'), at('2026-01-30T00:00:00Z'), 'UTC', 0)]).toEqual([]);
    expect(after('FREQ=MONTHLY;INTERVAL=2;BYMONTH=2', { start: '2026-02-01T00:00:00Z', after: '2026-01-01T00:00:00Z', limit: 2, tz: 'UTC' })).toEqual(
      utc('2026-02-01T00:00:00Z', '2027-02-01T00:00:00Z'),
    );
  });
});

describe('parseRRule()', () => {
  it('parses the parts, with or without "RRULE:", in any case', () => {
    const source = 'rrule:freq=monthly;interval=2;count=10;wkst=su;bymonth=3,1;byday=1su,-1SU;byhour=9,8;byminute=0;bysecond=30';
    expect(parseRRule(source)).toEqual({
      source,
      freq: 'MONTHLY',
      interval: 2,
      count: 10,
      until: null,
      weekStart: 0,
      byMonth: [1, 3],
      byMonthDay: null,
      byDay: [
        { weekday: 0, nth: 1 },
        { weekday: 0, nth: -1 },
      ],
      byHour: [8, 9],
      byMinute: [0],
      bySecond: [30],
    });
    expect(parseRRule('FREQ=DAILY;')).toMatchObject({ freq: 'DAILY', interval: 1, count: null, until: null, weekStart: 1, byMonthDay: null });
    expect(parseRRule('FREQ=MONTHLY;BYMONTHDAY=+1,-1').byMonthDay).toEqual([-1, 1]);
  });

  it('reads the three forms of UNTIL', () => {
    expect(parseRRule('FREQ=DAILY;UNTIL=19971224').until).toEqual({ time: at('1997-12-24T00:00:00Z'), form: 'date' });
    expect(parseRRule('FREQ=DAILY;UNTIL=19971224T093000').until).toEqual({ time: at('1997-12-24T09:30:00Z'), form: 'local' });
    expect(parseRRule('FREQ=DAILY;UNTIL=19971224T093000Z').until).toEqual({ time: at('1997-12-24T09:30:00Z'), form: 'utc' });
  });

  it('tells whether a rule uses COUNT', () => {
    expect(rruleCounts(parseRRule('FREQ=DAILY;COUNT=3'))).toBe(true);
    expect(rruleCounts(parseRRule('FREQ=DAILY;UNTIL=20260101'))).toBe(false);
  });

  const unsupported = (name: string, rule: string) =>
    `Unsupported RRULE part "${name}" in "${rule}": use FREQ, INTERVAL, COUNT, UNTIL, WKST, BYMONTH, BYMONTHDAY, BYDAY, BYHOUR, BYMINUTE or BYSECOND.`;
  const until = "UNTIL is a date, YYYYMMDD, or a date and time, YYYYMMDDTHHMMSS in the schedule's time zone or YYYYMMDDTHHMMSSZ in UTC";
  const byDay = 'use SU, MO, TU, WE, TH, FR or SA, numbered 1 to 53 or -1 to -53 for the n-th of the month or year (1MO, -1FR)';
  it.each([
    ['FREQ=MONTHLY;BYDAY=MO;BYSETPOS=-1', unsupported('BYSETPOS', 'FREQ=MONTHLY;BYDAY=MO;BYSETPOS=-1')],
    ['FREQ=YEARLY;BYYEARDAY=100', unsupported('BYYEARDAY', 'FREQ=YEARLY;BYYEARDAY=100')],
    ['FREQ=YEARLY;BYWEEKNO=20', unsupported('BYWEEKNO', 'FREQ=YEARLY;BYWEEKNO=20')],
    ['RSCALE=GREGORIAN;FREQ=YEARLY;SKIP=FORWARD', unsupported('RSCALE', 'RSCALE=GREGORIAN;FREQ=YEARLY;SKIP=FORWARD')],
    ['FREQ=DAILY;X-NAME=1', unsupported('X-NAME', 'FREQ=DAILY;X-NAME=1')],
    ['FREQ=DAILY;BYHOUR', 'Invalid RRULE part "BYHOUR" in "FREQ=DAILY;BYHOUR": use NAME=VALUE parts separated by ";", such as FREQ=DAILY;BYHOUR=8.'],
    ['BYDAY=MO', 'Invalid RRULE "BYDAY=MO": FREQ is required, such as FREQ=DAILY.'],
    ['', 'Invalid RRULE "": FREQ is required, such as FREQ=DAILY.'],
    ['FREQ=FORTNIGHTLY', 'Invalid RRULE part "FREQ=FORTNIGHTLY" in "FREQ=FORTNIGHTLY": use SECONDLY, MINUTELY, HOURLY, DAILY, WEEKLY, MONTHLY or YEARLY.'],
    ['FREQ=DAILY;FREQ=WEEKLY', 'Invalid RRULE "FREQ=DAILY;FREQ=WEEKLY": FREQ is given twice; give each part once.'],
    ['FREQ=DAILY;COUNT=2;UNTIL=20260101', 'Invalid RRULE "FREQ=DAILY;COUNT=2;UNTIL=20260101": use COUNT or UNTIL, not both.'],
    ['FREQ=DAILY;INTERVAL=0', 'Invalid RRULE part "INTERVAL=0" in "FREQ=DAILY;INTERVAL=0": use a positive whole number.'],
    ['FREQ=DAILY;COUNT=-1', 'Invalid RRULE part "COUNT=-1" in "FREQ=DAILY;COUNT=-1": use a positive whole number.'],
    ['FREQ=DAILY;UNTIL=20260230', `Invalid RRULE part "UNTIL=20260230" in "FREQ=DAILY;UNTIL=20260230": ${until}.`],
    ['FREQ=DAILY;UNTIL=2026-01-01', `Invalid RRULE part "UNTIL=2026-01-01" in "FREQ=DAILY;UNTIL=2026-01-01": ${until}.`],
    ['FREQ=DAILY;UNTIL=20260101T240000Z', `Invalid RRULE part "UNTIL=20260101T240000Z" in "FREQ=DAILY;UNTIL=20260101T240000Z": ${until}.`],
    ['FREQ=WEEKLY;WKST=XX', 'Invalid RRULE part "WKST=XX" in "FREQ=WEEKLY;WKST=XX": use SU, MO, TU, WE, TH, FR or SA.'],
    ['FREQ=YEARLY;BYMONTH=13', 'Invalid RRULE part "BYMONTH=13" in "FREQ=YEARLY;BYMONTH=13": use 1 to 12.'],
    ['FREQ=MONTHLY;BYMONTHDAY=0', 'Invalid RRULE part "BYMONTHDAY=0" in "FREQ=MONTHLY;BYMONTHDAY=0": use 1 to 31, or -31 to -1 to count from the end.'],
    ['FREQ=MONTHLY;BYMONTHDAY=-32', 'Invalid RRULE part "BYMONTHDAY=-32" in "FREQ=MONTHLY;BYMONTHDAY=-32": use 1 to 31, or -31 to -1 to count from the end.'],
    ['FREQ=DAILY;BYHOUR=24', 'Invalid RRULE part "BYHOUR=24" in "FREQ=DAILY;BYHOUR=24": use 0 to 23.'],
    ['FREQ=DAILY;BYMINUTE=60', 'Invalid RRULE part "BYMINUTE=60" in "FREQ=DAILY;BYMINUTE=60": use 0 to 59.'],
    ['FREQ=DAILY;BYSECOND=60', 'Invalid RRULE part "BYSECOND=60" in "FREQ=DAILY;BYSECOND=60": use 0 to 59.'],
    ['FREQ=DAILY;BYHOUR=8,', 'Invalid RRULE part "BYHOUR=8," in "FREQ=DAILY;BYHOUR=8,": use 0 to 23.'],
    ['FREQ=MONTHLY;BYDAY=MO,XX', `Invalid RRULE part "BYDAY=MO,XX" in "FREQ=MONTHLY;BYDAY=MO,XX": ${byDay}.`],
    ['FREQ=MONTHLY;BYDAY=0MO', `Invalid RRULE part "BYDAY=0MO" in "FREQ=MONTHLY;BYDAY=0MO": ${byDay}.`],
    ['FREQ=YEARLY;BYDAY=54MO', `Invalid RRULE part "BYDAY=54MO" in "FREQ=YEARLY;BYDAY=54MO": ${byDay}.`],
    ['FREQ=WEEKLY;BYMONTHDAY=1', 'Invalid RRULE "FREQ=WEEKLY;BYMONTHDAY=1": BYMONTHDAY doesn\'t go with FREQ=WEEKLY (RFC 5545); use BYDAY.'],
    ['FREQ=WEEKLY;BYDAY=1MO', 'Invalid RRULE "FREQ=WEEKLY;BYDAY=1MO": a numbered BYDAY (such as 1MO or -1FR) needs FREQ=MONTHLY or FREQ=YEARLY.'],
    ['FREQ=DAILY;BYDAY=-1FR', 'Invalid RRULE "FREQ=DAILY;BYDAY=-1FR": a numbered BYDAY (such as 1MO or -1FR) needs FREQ=MONTHLY or FREQ=YEARLY.'],
    ['FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=30', 'Invalid RRULE "FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=30": it never matches, as no date fits all of its BYxxx parts.'],
    [
      'FREQ=DAILY;BYMONTH=4,6,9,11;BYMONTHDAY=31',
      'Invalid RRULE "FREQ=DAILY;BYMONTH=4,6,9,11;BYMONTHDAY=31": it never matches, as no date fits all of its BYxxx parts.',
    ],
    ['FREQ=MONTHLY;BYDAY=6MO', 'Invalid RRULE "FREQ=MONTHLY;BYDAY=6MO": it never matches, as no date fits all of its BYxxx parts.'],
    [
      'FREQ=SECONDLY;BYMONTH=2;BYMONTHDAY=30;BYSECOND=5',
      'Invalid RRULE "FREQ=SECONDLY;BYMONTH=2;BYMONTHDAY=30;BYSECOND=5": it never matches, as no date fits all of its BYxxx parts.',
    ],
  ])('throws a TypeError for %j', (rule, message) => {
    const error = thrown(() => parseRRule(rule));
    expect(error).toBeInstanceOf(TypeError);
    expect(error.message).toBe(message);
  });

  it('throws a TypeError for a value that is not a string', () => {
    expect(() => parseRRule(42 as unknown as string)).toThrow(new TypeError('Invalid RRULE 42. Use a string, such as "FREQ=WEEKLY;BYDAY=MO;BYHOUR=8".'));
  });

  it('accepts rules that only match now and then, such as February 29 or a fifth Monday of February', () => {
    expect(() => parseRRule('FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=29')).not.toThrow();
    expect(() => parseRRule('FREQ=YEARLY;BYMONTH=2;BYDAY=5MO')).not.toThrow();
    expect(() => parseRRule('FREQ=YEARLY;BYDAY=53MO')).not.toThrow();
    expect(() => parseRRule('FREQ=SECONDLY;BYMONTH=2;BYMONTHDAY=29;BYDAY=MO;BYHOUR=23;BYMINUTE=59;BYSECOND=59')).not.toThrow();
  });
});
