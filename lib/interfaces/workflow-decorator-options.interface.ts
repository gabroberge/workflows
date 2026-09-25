import type { Duration } from '../utils/duration.util.js';

export interface WorkflowMetadata {
  name: string;
  version: number;
  /** In milliseconds. */
  timeout?: number;
}

export interface WorkflowDecoratorOptions {
  /**
   * Bump when a change would not replay against journals written by the
   * previous code (renamed, removed, reordered or inserted steps). Keep the old
   * class registered until its instances finish. Default 1.
   */
  version?: number;
  /**
   * How long an instance may run, from its start to its end, sleeps and waits included. Once it
   * passes, the instance stops at its next `ctx` call (a step that is running finishes first), runs
   * its compensations and ends as `failed` with a `WorkflowTimeoutError`. The deadline is stored
   * with the instance, so it holds across restarts, and a parked instance wakes for it. `start()`'s
   * `timeout` option overrides it. Default: none.
   */
  timeout?: Duration;
}
