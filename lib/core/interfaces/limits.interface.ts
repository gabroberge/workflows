import type { Duration } from '../time/duration.js';

/**
 * A concurrency limit, in the family's words: at most `limit` run at once across every worker, of the whole (a
 * workflow, a queue), or with `key`, per key. A package takes one, or an array of one without a key and one with,
 * and checks them with `resolveConcurrency()`. A run holds its slot while its lease is live, not while it waits.
 *
 * ```ts
 * const concurrency: ConcurrencyLimit<Order>[] = [{ limit: 20 }, { limit: 1, key: (order) => order.customerId }];
 * ```
 */
export interface ConcurrencyLimit<I = any> {
  /** A positive integer: how many may run at once, of the whole, or with `key`, per key. */
  limit: number;
  /**
   * Makes `limit` a limit per key: the key of a run, computed from its (JSON) input when it is created, such as
   * `(order) => order.customerId`. `null` or `undefined` leaves it out of every key.
   */
  key?: (input: I) => string | null | undefined;
}

/**
 * A rate limit, in the family's words: at most `max` starts per window of `duration`, of the whole, or with `key`,
 * per key. A window opens with the first start after the previous one ended and lasts `duration` (BullMQ's limiter),
 * so up to `2 * max` can start around a window's end. `resolveRateLimit()` checks one, or an array of one without a
 * key and one with.
 *
 * ```ts
 * const rateLimit: RateLimit<Order>[] = [{ max: 100, duration: '1m' }, { max: 5, duration: '1m', key: (order) => order.customerId }];
 * ```
 */
export interface RateLimit<I = any> {
  /** A positive integer: how many may start per `duration`, of the whole, or with `key`, per key. */
  max: number;
  /** The window, such as `'1m'`. */
  duration: Duration;
  /** Makes the limit a limit per key, as `ConcurrencyLimit.key` does. */
  key?: (input: I) => string | null | undefined;
}

/**
 * A rate limit's window, resolved: at most `max` starts per `duration` milliseconds.
 *
 * ```ts
 * const window: RateWindow = { max: 10, duration: 60_000 };
 * ```
 */
export interface RateWindow {
  max: number;
  duration: number;
}

/**
 * Concurrency limits as `resolveConcurrency()` returns them: the whole's limit and the per-key one (`null`: none),
 * and the function that computes a run's key.
 *
 * ```ts
 * const { limit, perKey, key } = resolveConcurrency('queue "emails"', definition.concurrency);
 * ```
 */
export interface ResolvedConcurrency<I = any> {
  /** At most this many of the whole run at once, or `null`. */
  limit: number | null;
  /** At most this many per key, or `null`. */
  perKey: number | null;
  /** The key of a run, from its input. */
  key?: (input: I) => string | null | undefined;
}

/**
 * Rate limits as `resolveRateLimit()` returns them: the whole's window and the per-key one (`null`: none), and the
 * function that computes a run's key.
 *
 * ```ts
 * const { limit, perKey, key } = resolveRateLimit('queue "emails"', definition.rateLimit);
 * ```
 */
export interface ResolvedRateLimit<I = any> {
  /** The window of the whole, or `null`. */
  limit: RateWindow | null;
  /** Each key's window, or `null`. */
  perKey: RateWindow | null;
  /** The key of a run, from its input. */
  key?: (input: I) => string | null | undefined;
}

/**
 * A rate-limit window as a store keeps it: when it ends (epoch milliseconds), and how many starts it holds. A window
 * that ended is the same as none.
 *
 * ```ts
 * const window: RateWindowState = { windowEnd: now + 60_000, count: 1 };
 * ```
 */
export interface RateWindowState {
  windowEnd: number;
  count: number;
}
