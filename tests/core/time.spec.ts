/**
 * `@nestjs/workflows/core`'s time: `parseDuration()`, `systemClock` and `ManualClock`, which the main entry exports as
 * `ManualWorkflowClock` (its own tests are in testing.spec.ts).
 */
import { ManualClock, parseDuration, systemClock, type Clock } from '../../lib/core/index.js';
import { ManualWorkflowClock, type WorkflowClock } from '../../lib/index.js';

describe('parseDuration()', () => {
  it('reads milliseconds and every unit, rounding a fraction to a whole millisecond', () => {
    expect(parseDuration(0)).toBe(0);
    expect(parseDuration(250)).toBe(250);
    expect(parseDuration(1.5)).toBe(1.5);
    expect(parseDuration('250ms')).toBe(250);
    expect(parseDuration('30s')).toBe(30_000);
    expect(parseDuration('15m')).toBe(900_000);
    expect(parseDuration('6h')).toBe(21_600_000);
    expect(parseDuration('3d')).toBe(259_200_000);
    expect(parseDuration('1w')).toBe(604_800_000);
    expect(parseDuration('1.5s')).toBe(1_500);
    expect(parseDuration('0.0004s')).toBe(0);
  });

  it('throws a TypeError for a negative or non-finite number, and for text it can not read', () => {
    expect(() => parseDuration(-1)).toThrow(new TypeError('Invalid duration -1. Use a non-negative number of milliseconds.'));
    expect(() => parseDuration(Infinity)).toThrow(TypeError);
    expect(() => parseDuration(Number.NaN)).toThrow(TypeError);
    for (const text of ['', '5', '5 m', '-5m', '5M', '5min', 'm5', '1e3ms']) {
      expect(() => parseDuration(text as '5m')).toThrow(`Invalid duration "${text}". Use milliseconds or a string such as "15m" or "3d".`);
    }
  });
});

describe('clocks', () => {
  it("reads the system clock as Date.now()", () => {
    const before = Date.now();
    const now = systemClock.now();
    expect(now).toBeGreaterThanOrEqual(before);
    expect(now).toBeLessThanOrEqual(Date.now());
  });

  it('is the same class as ManualWorkflowClock, and a Clock is a WorkflowClock', () => {
    expect(ManualWorkflowClock).toBe(ManualClock);
    const clock: Clock = new ManualClock(5);
    const workflowClock: WorkflowClock = clock;
    expect(workflowClock.now()).toBe(5);
    expect(new ManualWorkflowClock(1)).toBeInstanceOf(ManualClock);
  });

  it('moves a ManualClock only when told to', () => {
    const clock = new ManualClock(Date.UTC(2026, 0, 5, 8));
    expect(clock.advance('1h')).toBe(Date.UTC(2026, 0, 5, 9));
    expect(clock.now()).toBe(Date.UTC(2026, 0, 5, 9));
    clock.set(new Date('2026-01-06T00:00:00Z'));
    expect(clock.now()).toBe(Date.UTC(2026, 0, 6));
  });
});
