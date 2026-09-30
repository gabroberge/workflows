import { isDeepStrictEqual } from 'node:util';

/** A random pause of up to 4 ms, to shuffle the calls a race starts together. */
export const jitter = () => new Promise((resolve) => setTimeout(resolve, Math.random() * 4));

export function show(value: unknown): string {
  return JSON.stringify(value, (_key, v) => (v === undefined ? '<undefined>' : v)) ?? String(value);
}

export function equal(actual: unknown, expected: unknown, label: string): void {
  if (!isDeepStrictEqual(normalizeUndefined(actual), normalizeUndefined(expected))) {
    throw new Error(`${label}: expected ${show(expected)}, got ${show(actual)}`);
  }
}

/** Every key of `pattern` matches, recursively (arrays by length and element); other keys are ignored. */
export function expect(actual: unknown, pattern: unknown, label: string): void {
  const mismatch = match(actual, pattern, '');
  if (mismatch !== null) {
    throw new Error(`${label}: ${mismatch}\n  expected ${show(pattern)}\n  got      ${show(actual)}`);
  }
}

function match(actual: unknown, pattern: unknown, path: string): string | null {
  if (Array.isArray(pattern)) {
    if (!Array.isArray(actual) || actual.length !== pattern.length) {
      return `at ${path || 'the root'}: expected an array of ${pattern.length}`;
    }

    for (let i = 0; i < pattern.length; i++) {
      const mismatch = match(actual[i], pattern[i], `${path}[${i}]`);
      if (mismatch) {
        return mismatch;
      }
    }
    return null;
  }

  if (pattern !== null && typeof pattern === 'object') {
    if (actual === null || typeof actual !== 'object') {
      return `at ${path || 'the root'}: expected an object`;
    }

    for (const [key, value] of Object.entries(pattern)) {
      const mismatch = match((actual as Record<string, unknown>)[key], value, path ? `${path}.${key}` : key);
      if (mismatch) {
        return mismatch;
      }
    }
    return null;
  }
  return Object.is(actual, pattern) ? null : `at ${path || 'the root'}: expected ${show(pattern)}, got ${show(actual)}`;
}

export function absent(value: unknown, label: string, options: { orNull?: boolean } = {}): void {
  if (value !== undefined && !(options.orNull && value === null)) {
    throw new Error(`${label}: expected undefined, got ${show(value)}`);
  }
}

export async function rejects(promise: Promise<unknown>, message: string): Promise<void> {
  try {
    await promise;
  } catch (error) {
    if (String((error as Error)?.message ?? error).includes(message)) {
      return;
    }
    throw error;
  }

  throw new Error(`expected a rejection with "${message}"`);
}

/** Drops `undefined` object fields, so `{ a: undefined }` equals `{}` (a store may omit them). */
function normalizeUndefined(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(normalizeUndefined);
  }
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).filter(([, v]) => v !== undefined).map(([k, v]) => [k, normalizeUndefined(v)]));
  }
  return value;
}
