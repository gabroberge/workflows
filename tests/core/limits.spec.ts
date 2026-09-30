/**
 * `@nestjs/workflows/core`'s limits vocabulary: concurrency and rate limits checked and resolved (the messages name
 * their owner, so a queue's read like a workflow's), priorities, and rate-limit window arithmetic. concurrency.spec.ts
 * and rate-limits.spec.ts cover the limits through `@Workflow()` and every store.
 */
import {
  assertPriority,
  MAX_PRIORITY,
  rateWindowRoom,
  resolveConcurrency,
  resolveRateLimit,
  takeRateWindow,
  type ConcurrencyLimit,
  type RateLimit,
  type RateWindowState,
} from '../../lib/core/index.js';
import type { WorkflowConcurrency, WorkflowRateLimit } from '../../lib/index.js';

interface Email {
  to: string;
}

describe('resolveConcurrency()', () => {
  it('resolves one limit, or one without a key and one with', () => {
    const key = (email: Email) => email.to;
    expect(resolveConcurrency('queue "emails"', { limit: 20 })).toEqual({ limit: 20, perKey: null });
    expect(resolveConcurrency('queue "emails"', { limit: 1, key })).toEqual({ limit: null, perKey: 1, key });
    expect(resolveConcurrency<Email>('queue "emails"', [{ limit: 1, key }, { limit: 20 }])).toEqual({ limit: 20, perKey: 1, key });
  });

  it('refuses anything else, naming its owner', () => {
    expect(() => resolveConcurrency('queue "emails"', [])).toThrow(new TypeError('Queue "emails" has 0 concurrency limits. Give it one, or two: one without a key and one with.'));
    expect(() => resolveConcurrency('queue "emails"', { limit: 0 })).toThrow(new TypeError('Invalid concurrency limit 0 for queue "emails". Use a positive integer.'));
    expect(() => resolveConcurrency('queue "emails"', { limit: 1.5 })).toThrow('Invalid concurrency limit 1.5 for queue "emails".');
    expect(() => resolveConcurrency('queue "emails"', { limit: 1, key: 'to' as never })).toThrow('Invalid concurrency key for queue "emails".');
    expect(() => resolveConcurrency('queue "emails"', [{ limit: 1 }, { limit: 2 }])).toThrow(
      new TypeError('Queue "emails" has two concurrency limits without a key. Give it at most one of each.'),
    );
  });
});

describe('resolveRateLimit()', () => {
  it('resolves windows in milliseconds, one of the whole and one per key', () => {
    const key = (email: Email) => email.to;
    expect(resolveRateLimit('queue "emails"', { max: 100, duration: '1s' })).toEqual({ limit: { max: 100, duration: 1_000 }, perKey: null });
    expect(resolveRateLimit<Email>('queue "emails"', [{ max: 100, duration: '1s' }, { max: 5, duration: 60_000, key }])).toEqual({
      limit: { max: 100, duration: 1_000 },
      perKey: { max: 5, duration: 60_000 },
      key,
    });
  });

  it('refuses anything else, naming its owner', () => {
    expect(() => resolveRateLimit('queue "emails"', [])).toThrow('Queue "emails" has 0 rate limits.');
    expect(() => resolveRateLimit('queue "emails"', { max: 0, duration: '1m' })).toThrow(new TypeError('Invalid rate limit max 0 for queue "emails". Use a positive integer.'));
    expect(() => resolveRateLimit('queue "emails"', { max: 1, duration: '0s' })).toThrow(
      new TypeError('Invalid rate limit duration "0s" for queue "emails". Use a positive duration, such as "1m".'),
    );
    expect(() => resolveRateLimit('queue "emails"', { max: 1, duration: 'soon' as '1m' })).toThrow('Invalid rate limit duration "soon"');
    expect(() => resolveRateLimit('queue "emails"', [{ max: 1, duration: '1m', key: (e: Email) => e.to }, { max: 2, duration: '1m', key: (e: Email) => e.to }])).toThrow(
      'Queue "emails" has two rate limits with a key.',
    );
  });

  it("takes workflows' limit types, which are the same shapes", () => {
    const concurrency: ConcurrencyLimit = { limit: 2 } satisfies WorkflowConcurrency;
    const rateLimit: RateLimit = { max: 2, duration: '1m' } satisfies WorkflowRateLimit;
    expect(resolveConcurrency('workflow "sync"', concurrency).limit).toBe(2);
    expect(resolveRateLimit('workflow "sync"', rateLimit).limit).toEqual({ max: 2, duration: 60_000 });
  });
});

describe('priorities', () => {
  it('accept an integer from 0 (none) to 2,097,151, and nothing else', () => {
    expect(MAX_PRIORITY).toBe(2_097_151);
    for (const priority of [0, 1, MAX_PRIORITY]) {
      expect(() => assertPriority(priority, 'add()')).not.toThrow();
    }
    for (const priority of [-1, 1.5, MAX_PRIORITY + 1, '1', null, undefined, Number.NaN]) {
      expect(() => assertPriority(priority, 'add()')).toThrow(TypeError);
    }
    expect(() => assertPriority(2.5, 'add()')).toThrow('add(): invalid priority 2.5. Use an integer from 1 (first) to 2097151.');
  });
});

describe('rate-limit windows', () => {
  it('open with the first start after the last window ended, and hold at most max until they end', () => {
    let window: RateWindowState | undefined;
    const max = 3;
    const starts: number[] = [];
    for (const now of [1_000, 1_500, 2_000, 2_500, 60_999, 61_000, 61_000, 200_000]) {
      if (rateWindowRoom(window, max, now) > 0) {
        window = takeRateWindow(window, 60_000, now);
        starts.push(now);
      }
    }
    // The first window runs from 1s to 61s: three starts, then none until it ended; the next opens at 61s.
    expect(starts).toEqual([1_000, 1_500, 2_000, 61_000, 61_000, 200_000]);
    expect(window).toEqual({ windowEnd: 260_000, count: 1 });
  });

  it('count several starts at once, and read an ended window as empty', () => {
    expect(takeRateWindow(null, 60_000, 0, 5)).toEqual({ windowEnd: 60_000, count: 5 });
    expect(takeRateWindow({ windowEnd: 60_000, count: 5 }, 60_000, 30_000, 2)).toEqual({ windowEnd: 60_000, count: 7 });
    expect(rateWindowRoom({ windowEnd: 60_000, count: 7 }, 10, 59_999)).toBe(3);
    expect(rateWindowRoom({ windowEnd: 60_000, count: 7 }, 10, 60_000)).toBe(10);
    expect(rateWindowRoom(undefined, 10, 0)).toBe(10);
  });
});
