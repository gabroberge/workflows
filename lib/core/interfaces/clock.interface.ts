/**
 * Where every timestamp comes from (deadlines, retry times, lease expiry, schedule occurrences), so tests can move
 * time without fake timers: `systemClock`, or a `ManualClock`.
 *
 * ```ts
 * class Deadline {
 *   constructor(private readonly clock: Clock = systemClock) {}
 *
 *   passed(at: number): boolean {
 *     return this.clock.now() >= at;
 *   }
 * }
 * ```
 */
export interface Clock {
  /** Epoch milliseconds. */
  now(): number;
}
