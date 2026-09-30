/**
 * `@nestjs/workflows/core`'s retries: `resolveRetry()` checks the family's retry settings, `backoffDelay()` computes a
 * wait (fixed, exponential, capped, with jitter), and `nextRetry()` decides after a failure, `retryIf` included. The
 * workflow engine's steps use them (execution.spec.ts covers retries through `ctx.step()`).
 */
import { backoffDelay, nextRetry, resolveRetry, type ResolvedRetry } from '../../lib/core/index.js';

const FAMILY_DEFAULT: ResolvedRetry = { attempts: 3, backoff: { delay: 1_000, factor: 2, maxDelay: 300_000, jitter: 'none' } };

describe('resolveRetry()', () => {
  it("resolves over the family's default: 3 attempts, 1s doubling up to 5m, no jitter", () => {
    expect(resolveRetry(undefined)).toEqual(FAMILY_DEFAULT);
    expect(resolveRetry(false)).toEqual({ ...FAMILY_DEFAULT, attempts: 1 });
    expect(resolveRetry(5)).toEqual({ ...FAMILY_DEFAULT, attempts: 5 });
    expect(resolveRetry(0)).toEqual({ ...FAMILY_DEFAULT, attempts: 1 });
  });

  it('merges settings over a base field by field, and backoff fields over its backoff', () => {
    const base = resolveRetry({ attempts: 4, backoff: { delay: '2s', jitter: 'equal' } });
    expect(base).toEqual({ attempts: 4, backoff: { delay: 2_000, factor: 2, maxDelay: 300_000, jitter: 'equal' } });

    const retryIf = () => true;
    expect(resolveRetry({ backoff: { factor: 1, maxDelay: '1m' }, retryIf }, base)).toEqual({
      attempts: 4,
      backoff: { delay: 2_000, factor: 1, maxDelay: 60_000, jitter: 'equal' },
      retryIf,
    });
    expect(resolveRetry({ attempts: 2 }, { ...base, retryIf })).toMatchObject({ attempts: 2, retryIf });
  });

  it("converts a backoff function's durations to milliseconds, and merges an object over the default after one", () => {
    const fn = resolveRetry({ backoff: (attempt) => `${attempt}m` });
    expect(typeof fn.backoff).toBe('function');
    expect((fn.backoff as (attempt: number, error: unknown) => number)(3, null)).toBe(180_000);
    expect(resolveRetry({ backoff: { delay: 50 } }, fn).backoff).toEqual({ delay: 50, factor: 2, maxDelay: 300_000, jitter: 'none' });
  });

  it('throws a TypeError for attempts, a factor or a jitter it can not use', () => {
    expect(() => resolveRetry(1.5)).toThrow(new TypeError('Invalid retry attempts 1.5. Use a whole number of attempts, including the first.'));
    expect(() => resolveRetry({ attempts: -1 })).toThrow('Invalid retry attempts -1.');
    expect(() => resolveRetry({ backoff: { factor: 0 } })).toThrow(new TypeError('Invalid backoff factor 0. Use a positive number (1 = constant).'));
    expect(() => resolveRetry({ backoff: { jitter: 'some' as 'full' } })).toThrow(new TypeError('Invalid backoff jitter "some". Use "full", "equal" or "none".'));
    expect(() => resolveRetry({ backoff: { delay: 'soon' as '1s' } })).toThrow('Invalid duration "soon".');
  });
});

describe('backoffDelay()', () => {
  it('grows by the factor per attempt up to the cap; a factor of 1 keeps it fixed', () => {
    const exponential = { delay: 1_000, factor: 2, maxDelay: 5_000, jitter: 'none' as const };
    expect([1, 2, 3, 4, 5].map((attempt) => backoffDelay(exponential, attempt))).toEqual([1_000, 2_000, 4_000, 5_000, 5_000]);

    const fixed = { delay: 750, factor: 1, maxDelay: 300_000, jitter: 'none' as const };
    expect([1, 2, 10].map((attempt) => backoffDelay(fixed, attempt))).toEqual([750, 750, 750]);
  });

  it("randomizes a wait with 'full' jitter from 0 to all of it, and with 'equal' from half of it to all of it", () => {
    const wait = { delay: 1_000, factor: 2, maxDelay: 300_000 };
    expect(backoffDelay({ ...wait, jitter: 'full' }, 2, () => 0)).toBe(0);
    expect(backoffDelay({ ...wait, jitter: 'full' }, 2, () => 0.5)).toBe(1_000);
    expect(backoffDelay({ ...wait, jitter: 'full' }, 2, () => 0.9999)).toBe(1_999);
    expect(backoffDelay({ ...wait, jitter: 'equal' }, 2, () => 0)).toBe(1_000);
    expect(backoffDelay({ ...wait, jitter: 'equal' }, 2, () => 0.9999)).toBe(1_999);

    for (let i = 0; i < 100; i++) {
      const delay = backoffDelay({ ...wait, jitter: 'equal' }, 3);
      expect(delay).toBeGreaterThanOrEqual(2_000);
      expect(delay).toBeLessThan(4_000);
    }
  });
});

describe('nextRetry()', () => {
  it('retries after the backoff until the attempts are spent', () => {
    const policy = resolveRetry({ attempts: 3, backoff: { delay: '1s' } });
    expect(nextRetry(policy, 1, new Error('ECONNRESET'))).toEqual({ retry: true, delay: 1_000 });
    expect(nextRetry(policy, 2, new Error('ECONNRESET'))).toEqual({ retry: true, delay: 2_000 });
    expect(nextRetry(policy, 3, new Error('ECONNRESET'))).toEqual({ retry: false, reason: 'exhausted' });
    expect(nextRetry(resolveRetry(false), 1, new Error('ECONNRESET'))).toEqual({ retry: false, reason: 'exhausted' });
  });

  it('asks retryIf with the error and the attempt, and uses a backoff function', () => {
    const asked: unknown[] = [];
    const policy = resolveRetry({
      attempts: 5,
      backoff: (attempt, error) => ((error as Error).message === 'slow down' ? '1m' : attempt * 10),
      retryIf: (error, attempt) => {
        asked.push([(error as Error).message, attempt]);
        return (error as Error).message !== 'card declined';
      },
    });

    expect(nextRetry(policy, 2, new Error('slow down'))).toEqual({ retry: true, delay: 60_000 });
    expect(nextRetry(policy, 3, new Error('timeout'))).toEqual({ retry: true, delay: 30 });
    expect(nextRetry(policy, 1, new Error('card declined'))).toEqual({ retry: false, reason: 'refused' });
    expect(asked).toEqual([['slow down', 2], ['timeout', 3], ['card declined', 1]]);
  });

  it('gives up, with what it threw, when retryIf or the backoff function throws; not once the attempts are spent', () => {
    const bug = new TypeError("Cannot read properties of undefined (reading 'status')");
    const throwing = () => {
      throw bug;
    };
    const retryIf = nextRetry(resolveRetry({ retryIf: throwing }), 1, new Error('ECONNRESET'));
    expect(retryIf).toEqual({ retry: false, reason: 'threw', error: bug });

    const backoff = nextRetry(resolveRetry({ backoff: () => 'soon' as '1s' }), 1, new Error('ECONNRESET'));
    expect(backoff).toMatchObject({ retry: false, reason: 'threw', error: expect.objectContaining({ message: expect.stringContaining('Invalid duration "soon"') }) });

    let called = false;
    expect(nextRetry(resolveRetry({ attempts: 1, retryIf: () => (called = true) }), 1, new Error('x'))).toEqual({ retry: false, reason: 'exhausted' });
    expect(called).toBe(false);
  });
});
