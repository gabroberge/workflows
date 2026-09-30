import { parseDuration, type Duration } from '../core/time/duration.js';

/** A workflow's run timeout in milliseconds: a zero one would time out before the first step. */
export function runTimeoutMs(timeout: Duration, owner: string): number {
  let ms: number;
  try {
    ms = parseDuration(timeout);
  } catch {
    ms = 0;
  }

  if (ms <= 0) {
    throw new TypeError(`Invalid timeout ${JSON.stringify(timeout)} for ${owner}. Use a positive duration, such as "30d".`);
  }
  return ms;
}
