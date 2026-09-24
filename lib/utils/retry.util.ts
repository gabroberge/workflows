import { toMs } from './duration.util.js';
import type { WorkflowRetryOptions } from '../interfaces/workflow-retry-options.interface.js';

type Backoff = Exclude<NonNullable<WorkflowRetryOptions['backoff']>, (...args: any[]) => unknown>;

interface ResolvedBackoff {
  delay: number;
  factor: number;
  maxDelay: number;
  jitter: 'full' | 'equal' | 'none';
}

export interface ResolvedRetry {
  attempts: number;
  backoff: ResolvedBackoff | ((attempt: number, error: unknown) => number);
  retryIf?: (error: unknown, attempt: number) => boolean;
}

export const DEFAULT_RETRY: ResolvedRetry = {
  attempts: 3,
  backoff: { delay: 1_000, factor: 2, maxDelay: 300_000, jitter: 'none' },
};

const DEFAULT_BACKOFF = DEFAULT_RETRY.backoff as ResolvedBackoff;

/** Resolves `retry` (a count, `false` or options) over `base`, field by field. */
export function resolveRetry(retry: number | false | WorkflowRetryOptions | undefined, base: ResolvedRetry): ResolvedRetry {
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
        ? (attempt, error) => toMs(backoff(attempt, error))
        : backoff
          ? resolveBackoff(backoff, typeof base.backoff === 'function' ? DEFAULT_BACKOFF : base.backoff)
          : base.backoff,
    retryIf: retry.retryIf ?? base.retryIf,
  };
}

/** Wait (ms) after the failed `attempt` (1-based). */
export function retryDelay(policy: ResolvedRetry, attempt: number, error: unknown): number {
  const { backoff } = policy;
  if (typeof backoff === 'function') {
    return backoff(attempt, error);
  }

  const wait = Math.min(backoff.maxDelay, backoff.delay * backoff.factor ** (attempt - 1));
  switch (backoff.jitter) {
    case 'full':
      return Math.floor(Math.random() * wait);
    case 'equal':
      return Math.floor(wait / 2 + Math.random() * (wait / 2));
    default:
      return wait;
  }
}

function resolveBackoff(backoff: Backoff, base: ResolvedBackoff): ResolvedBackoff {
  const factor = backoff.factor ?? base.factor;
  if (!(factor > 0)) {
    throw new TypeError(`Invalid backoff factor ${factor}. Use a positive number (1 = constant).`);
  }

  const jitter = backoff.jitter ?? base.jitter;
  if (!['full', 'equal', 'none'].includes(jitter)) {
    throw new TypeError(`Invalid backoff jitter "${jitter}". Use "full", "equal" or "none".`);
  }

  return {
    delay: backoff.delay === undefined ? base.delay : toMs(backoff.delay),
    factor,
    maxDelay: backoff.maxDelay === undefined ? base.maxDelay : toMs(backoff.maxDelay),
    jitter,
  };
}

function attempts(value: number): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new TypeError(`Invalid retry attempts ${value}. Use a whole number of attempts, including the first.`);
  }
  return Math.max(1, value);
}
