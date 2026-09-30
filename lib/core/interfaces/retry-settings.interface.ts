import type { Duration } from '../time/duration.js';

/**
 * The wait between attempts, in the family's retry vocabulary: `delay`, growing by `factor` per retry up to
 * `maxDelay`, optionally randomized (`jitter`). Named so it doesn't clash with `@nestjs/resilience`'s
 * `BackoffOptions`, which owns the unprefixed name.
 *
 * ```ts
 * const backoff: BackoffSettings = { delay: '1s', factor: 2, maxDelay: '5m', jitter: 'full' };
 * ```
 */
export interface BackoffSettings {
  /** Wait before the first retry. Default `'1s'`. */
  delay?: Duration;
  /** Growth per retry; 1 = constant. Default 2. */
  factor?: number;
  /** Cap for a single wait. Default `'5m'`. */
  maxDelay?: Duration;
  /** Randomize each wait: `'full'` (0 to the wait), `'equal'` (half to all of it) or `'none'`. Default `'none'`. */
  jitter?: 'full' | 'equal' | 'none';
}

/**
 * How an operation retries, in the family's retry vocabulary: a package takes `retry?: number | false |
 * RetrySettings` (`5` means `{ attempts: 5 }`, `false` a single attempt) and exports it under its own name
 * (`WorkflowRetryOptions`); `resolveRetry()` checks it. `attempt` arguments are 1-based: the attempt that just failed.
 *
 * ```ts
 * const retry: RetrySettings = {
 *   attempts: 5,
 *   backoff: { delay: '2s', jitter: 'equal' },
 *   retryIf: (error) => !(error instanceof ValidationError),
 * };
 * ```
 */
export interface RetrySettings {
  /** Total attempts, including the first. Default 3. */
  attempts?: number;
  /**
   * Wait between attempts. An object is merged field by field over the default; a function receives the attempt
   * that just failed (1-based) and its error, and returns the wait.
   */
  backoff?: BackoffSettings | ((attempt: number, error: unknown) => Duration);
  /** Return false to stop retrying. Gets the error and the attempt that just failed (1-based). */
  retryIf?: (error: unknown, attempt: number) => boolean;
}

/**
 * `BackoffSettings`, checked, with every field set: times in milliseconds.
 *
 * ```ts
 * const backoff: ResolvedBackoff = { delay: 1_000, factor: 2, maxDelay: 300_000, jitter: 'none' };
 * backoffDelay(backoff, 3); // 4_000
 * ```
 */
export interface ResolvedBackoff {
  delay: number;
  factor: number;
  maxDelay: number;
  jitter: 'full' | 'equal' | 'none';
}

/**
 * A retry policy as `resolveRetry()` returns it: checked, with every field set. A backoff function's `Duration` is
 * converted to milliseconds.
 *
 * ```ts
 * const policy: ResolvedRetry = resolveRetry({ attempts: 5 });
 * ```
 */
export interface ResolvedRetry {
  attempts: number;
  backoff: ResolvedBackoff | ((attempt: number, error: unknown) => number);
  retryIf?: (error: unknown, attempt: number) => boolean;
}
