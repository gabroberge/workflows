/**
 * Every timestamp the engine reads (sleep deadlines, signal timeouts, retry
 * times, lease expiry, `ctx.now()`) comes from this clock, so tests can move
 * time without fake timers.
 */
export interface WorkflowClock {
  now(): number;
}
