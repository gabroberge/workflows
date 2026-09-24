import type { Duration } from '../utils/duration.util.js';

/**
 * How a step (or compensation) retries. `retry: 5` means `{ attempts: 5 }`;
 * `retry: false` means a single attempt. Workflows default to 3 attempts, a
 * 1s first delay doubling up to 5m, no jitter. Every retry parks the instance
 * durably: the wait survives restarts.
 */
export interface WorkflowRetryOptions {
  /** Total attempts, including the first. Default 3. */
  attempts?: number;
  /**
   * Wait between attempts. An object is merged field by field over the
   * default; a function receives the attempt that just failed (1-based) and
   * its error, and returns the wait.
   */
  backoff?:
    | {
        /** Wait before the first retry. Default `'1s'`. */
        delay?: Duration;
        /** Growth per retry; 1 = constant. Default 2. */
        factor?: number;
        /** Cap for a single wait. Default `'5m'`. */
        maxDelay?: Duration;
        /** Randomize each wait: `'full'` (0 to the wait), `'equal'` (half to all of it) or `'none'`. Default `'none'`. */
        jitter?: 'full' | 'equal' | 'none';
      }
    | ((attempt: number, error: unknown) => Duration);
  /**
   * Return false to stop retrying. Gets the error and the attempt that just
   * failed (1-based). A `NonRetryableStepError` never retries.
   */
  retryIf?: (error: unknown, attempt: number) => boolean;
}
