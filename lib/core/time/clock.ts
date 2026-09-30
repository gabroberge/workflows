import type { Clock } from '../interfaces/clock.interface.js';
import { parseDuration, type Duration } from './duration.js';

/**
 * The clock of the real world: `Date.now()`.
 *
 * ```ts
 * const clock = options.clock ?? systemClock;
 * ```
 */
export const systemClock: Clock = { now: () => Date.now() };

/**
 * A clock that only moves when told to, for tests: it starts at 1 January 2026, 00:00 UTC, or at the time it is given.
 * Also exported by `@nestjs/workflows` as `ManualWorkflowClock`.
 *
 * ```ts
 * const clock = new ManualClock(Date.UTC(2026, 0, 5, 8));
 * clock.advance('1h'); // 09:00
 * clock.set(new Date('2026-01-06T00:00:00Z'));
 * ```
 */
export class ManualClock implements Clock {
  constructor(private current: number = Date.UTC(2026, 0, 1)) {}

  now(): number {
    return this.current;
  }

  /** Moves the clock forward by `duration`, and returns the new time. */
  advance(duration: Duration): number {
    this.current += parseDuration(duration);
    return this.current;
  }

  set(time: Date | number): void {
    this.current = time instanceof Date ? time.getTime() : time;
  }
}
