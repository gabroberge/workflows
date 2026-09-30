import type { BackoffSettings, ResolvedBackoff, ResolvedRetry, RetrySettings } from '../interfaces/retry-settings.interface.js';
import { parseDuration } from '../time/duration.js';

/** The family's default: 3 attempts, 1s doubling up to 5m, no jitter. */
const DEFAULT_RETRY: ResolvedRetry = {
  attempts: 3,
  backoff: { delay: 1_000, factor: 2, maxDelay: 300_000, jitter: 'none' },
};

const DEFAULT_BACKOFF = DEFAULT_RETRY.backoff as ResolvedBackoff;

/**
 * What `nextRetry()` decided after a failed attempt: retry after `delay` milliseconds, or give up because the
 * attempts are spent (`'exhausted'`), `retryIf` returned false (`'refused'`), or `retryIf` or a backoff function threw
 * (`'threw'`, with what it threw: record it as the failure, since retrying on would hide the bug).
 *
 * ```ts
 * const next = nextRetry(policy, attempt, error);
 * if (next.retry) {
 *   await job.retryAt(clock.now() + next.delay);
 * } else {
 *   await job.fail(next.reason === 'threw' ? next.error : error);
 * }
 * ```
 */
export type RetryDecision =
  | { retry: true; delay: number }
  | { retry: false; reason: 'exhausted' | 'refused' }
  | { retry: false; reason: 'threw'; error: unknown };

/**
 * Checks `retry` (a count, `false` or `RetrySettings`) and resolves it over `base`, field by field: `undefined` is
 * `base`, `false` one attempt, a number the attempts. `base` defaults to the family's default: 3 attempts, 1s doubling
 * up to 5m, no jitter. Throws a `TypeError` for invalid settings, so they fail where they are given rather than at the
 * first retry.
 *
 * ```ts
 * const defaults = resolveRetry(moduleOptions.retry);
 * const policy = resolveRetry(jobOptions.retry, defaults);
 * ```
 */
export function resolveRetry(retry: number | false | RetrySettings | undefined, base: ResolvedRetry = DEFAULT_RETRY): ResolvedRetry {
  if (retry === undefined) {
    return base;
  }
  if (retry === false) {
    return { ...base, attempts: 1 };
  }
  if (typeof retry === 'number') {
    return { ...base, attempts: attempts(retry) };
  }

  const { backoff } = retry;
  return {
    attempts: retry.attempts === undefined ? base.attempts : attempts(retry.attempts),
    backoff:
      typeof backoff === 'function'
        ? (attempt, error) => parseDuration(backoff(attempt, error))
        : backoff
          ? resolveBackoff(backoff, typeof base.backoff === 'function' ? DEFAULT_BACKOFF : base.backoff)
          : base.backoff,
    retryIf: retry.retryIf ?? base.retryIf,
  };
}

/**
 * The wait (ms) after the failed `attempt` (1-based): `delay * factor^(attempt - 1)`, capped at `maxDelay`, then
 * randomized by `jitter` (`'full'`: from 0 to the wait, `'equal'`: from half of it to all of it). `random` returns a
 * number in `[0, 1)`, as `Math.random()` (the default) does.
 *
 * ```ts
 * backoffDelay({ delay: 1_000, factor: 2, maxDelay: 60_000, jitter: 'none' }, 4); // 8_000
 * ```
 */
export function backoffDelay(backoff: ResolvedBackoff, attempt: number, random: () => number = Math.random): number {
  const wait = Math.min(backoff.maxDelay, backoff.delay * backoff.factor ** (attempt - 1));
  switch (backoff.jitter) {
    case 'full':
      return Math.floor(random() * wait);
    case 'equal':
      return Math.floor(wait / 2 + random() * (wait / 2));
    default:
      return wait;
  }
}

/**
 * Whether, and when, to retry after `attempt` (1-based) failed with `error`: not once the policy's attempts are
 * spent, nor when `retryIf` returns false; otherwise after the backoff's wait. A `retryIf` or backoff function that
 * throws gives up, with what it threw. Errors that must never be retried (a `NonRetryableStepError`) are the caller's
 * to check first.
 *
 * ```ts
 * const next = nextRetry(resolveRetry({ attempts: 3 }), 1, new Error('ECONNRESET'));
 * // { retry: true, delay: 1_000 }
 * ```
 */
export function nextRetry(policy: ResolvedRetry, attempt: number, error: unknown): RetryDecision {
  if (attempt >= policy.attempts) {
    return { retry: false, reason: 'exhausted' };
  }

  try {
    if (!(policy.retryIf?.(error, attempt) ?? true)) {
      return { retry: false, reason: 'refused' };
    }
    const { backoff } = policy;
    return { retry: true, delay: typeof backoff === 'function' ? backoff(attempt, error) : backoffDelay(backoff, attempt) };
  } catch (thrown) {
    return { retry: false, reason: 'threw', error: thrown };
  }
}

function resolveBackoff(backoff: BackoffSettings, base: ResolvedBackoff): ResolvedBackoff {
  const factor = backoff.factor ?? base.factor;
  if (!(factor > 0)) {
    throw new TypeError(`Invalid backoff factor ${factor}. Use a positive number (1 = constant).`);
  }

  const jitter = backoff.jitter ?? base.jitter;
  if (!['full', 'equal', 'none'].includes(jitter)) {
    throw new TypeError(`Invalid backoff jitter "${jitter}". Use "full", "equal" or "none".`);
  }

  return {
    delay: backoff.delay === undefined ? base.delay : parseDuration(backoff.delay),
    factor,
    maxDelay: backoff.maxDelay === undefined ? base.maxDelay : parseDuration(backoff.maxDelay),
    jitter,
  };
}

function attempts(value: number): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new TypeError(`Invalid retry attempts ${value}. Use a whole number of attempts, including the first.`);
  }
  return Math.max(1, value);
}
