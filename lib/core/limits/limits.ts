import type { ConcurrencyLimit, RateLimit, RateWindowState, ResolvedConcurrency, ResolvedRateLimit } from '../interfaces/limits.interface.js';
import { parseDuration } from '../time/duration.js';

/**
 * The highest priority (the last to run): BullMQ's, which the family shares. Priorities are integers from 1 (first)
 * to `MAX_PRIORITY`; lower runs first, and `0` (none given) runs before every other.
 *
 * ```ts
 * const priority = options.priority ?? 0; // 0..MAX_PRIORITY
 * ```
 */
export const MAX_PRIORITY = 2_097_151;

/**
 * Throws a `TypeError` that names `owner` unless `priority` is a priority: an integer from 0 (none) to
 * `MAX_PRIORITY`.
 *
 * ```ts
 * assertPriority(options.priority, 'add()'); // add(): invalid priority 1.5. Use an integer from 1 (first) to 2097151.
 * ```
 */
export function assertPriority(priority: unknown, owner: string): asserts priority is number {
  if (!isPriority(priority)) {
    throw new TypeError(`${owner}: invalid priority ${JSON.stringify(priority)}. Use an integer from 1 (first) to ${MAX_PRIORITY}.`);
  }
}

/** Whether `value` is an integer from 0 (none) to `MAX_PRIORITY`. */
export function isPriority(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 && value <= MAX_PRIORITY;
}

/**
 * Checks concurrency limits (one, or one without a key and one with) and resolves them. Throws a `TypeError` that
 * names `owner`, such as `queue "emails"`, for anything else.
 *
 * ```ts
 * resolveConcurrency('queue "emails"', [{ limit: 20 }, { limit: 1, key: (email) => email.to }]);
 * // { limit: 20, perKey: 1, key: (email) => email.to }
 * ```
 */
export function resolveConcurrency<I = any>(owner: string, concurrency: ConcurrencyLimit<I> | ConcurrencyLimit<I>[]): ResolvedConcurrency<I> {
  const limits = Array.isArray(concurrency) ? concurrency : [concurrency];
  if (limits.length === 0 || limits.length > 2) {
    throw new TypeError(`${capitalized(owner)} has ${limits.length} concurrency limits. Give it one, or two: one without a key and one with.`);
  }

  const resolved: ResolvedConcurrency<I> = { limit: null, perKey: null };
  for (const { limit, key } of limits) {
    if (!Number.isSafeInteger(limit) || limit < 1) {
      throw new TypeError(`Invalid concurrency limit ${JSON.stringify(limit)} for ${owner}. Use a positive integer.`);
    }
    if (key !== undefined && typeof key !== 'function') {
      throw new TypeError(`Invalid concurrency key for ${owner}. Use a function of the input, such as (order) => order.customerId.`);
    }

    const slot = key === undefined ? 'limit' : 'perKey';
    if (resolved[slot] !== null) {
      throw new TypeError(`${capitalized(owner)} has two concurrency limits ${key === undefined ? 'without' : 'with'} a key. Give it at most one of each.`);
    }
    resolved[slot] = limit;
    if (key) {
      resolved.key = key;
    }
  }
  return resolved;
}

/**
 * Checks rate limits (one, or one without a key and one with) and resolves them, durations in milliseconds. Throws a
 * `TypeError` that names `owner` for anything else.
 *
 * ```ts
 * resolveRateLimit('queue "emails"', { max: 100, duration: '1s' });
 * // { limit: { max: 100, duration: 1_000 }, perKey: null }
 * ```
 */
export function resolveRateLimit<I = any>(owner: string, rateLimit: RateLimit<I> | RateLimit<I>[]): ResolvedRateLimit<I> {
  const limits = Array.isArray(rateLimit) ? rateLimit : [rateLimit];
  if (limits.length === 0 || limits.length > 2) {
    throw new TypeError(`${capitalized(owner)} has ${limits.length} rate limits. Give it one, or two: one without a key and one with.`);
  }

  const resolved: ResolvedRateLimit<I> = { limit: null, perKey: null };
  for (const { max, duration, key } of limits) {
    if (!Number.isSafeInteger(max) || max < 1) {
      throw new TypeError(`Invalid rate limit max ${JSON.stringify(max)} for ${owner}. Use a positive integer.`);
    }
    let ms: number;
    try {
      ms = parseDuration(duration);
    } catch {
      ms = 0;
    }
    if (!(ms > 0)) {
      throw new TypeError(`Invalid rate limit duration ${JSON.stringify(duration)} for ${owner}. Use a positive duration, such as "1m".`);
    }
    if (key !== undefined && typeof key !== 'function') {
      throw new TypeError(`Invalid rate limit key for ${owner}. Use a function of the input, such as (order) => order.customerId.`);
    }

    const slot = key === undefined ? 'limit' : 'perKey';
    if (resolved[slot] !== null) {
      throw new TypeError(`${capitalized(owner)} has two rate limits ${key === undefined ? 'without' : 'with'} a key. Give it at most one of each.`);
    }
    resolved[slot] = { max, duration: ms };
    if (key) {
      resolved.key = key;
    }
  }
  return resolved;
}

/**
 * How many more starts a window of `max` takes at `now`: all of `max` when it ended (`windowEnd <= now`) or never
 * opened.
 *
 * ```ts
 * rateWindowRoom({ windowEnd: 60_000, count: 7 }, 10, 30_000); // 3
 * rateWindowRoom({ windowEnd: 60_000, count: 7 }, 10, 60_000); // 10: it ended
 * ```
 */
export function rateWindowRoom(window: RateWindowState | null | undefined, max: number, now: number): number {
  return window && window.windowEnd > now ? max - window.count : max;
}

/**
 * The window after `count` more starts at `now`: the open one, counted up, or, when it ended or never opened, a new
 * one from `now` for `duration` milliseconds. Take only what `rateWindowRoom()` said fits, under the store's lock.
 *
 * ```ts
 * takeRateWindow(undefined, 60_000, 1_000); // { windowEnd: 61_000, count: 1 }
 * takeRateWindow({ windowEnd: 61_000, count: 1 }, 60_000, 2_000, 2); // { windowEnd: 61_000, count: 3 }
 * ```
 */
export function takeRateWindow(window: RateWindowState | null | undefined, duration: number, now: number, count = 1): RateWindowState {
  return window && window.windowEnd > now ? { windowEnd: window.windowEnd, count: window.count + count } : { windowEnd: now + duration, count };
}

function capitalized(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}
