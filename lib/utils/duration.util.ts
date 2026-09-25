/** Milliseconds, or a string such as `"250ms"`, `"30s"`, `"15m"`, `"6h"`, `"3d"`, `"1w"`. */
export type Duration = number | `${number}${'ms' | 's' | 'm' | 'h' | 'd' | 'w'}`;

const UNITS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
  w: 604_800_000,
};

export function toMs(duration: Duration): number {
  if (typeof duration === 'number') {
    if (!Number.isFinite(duration) || duration < 0) {
      throw new TypeError(`Invalid duration ${duration}. Use a non-negative number of milliseconds.`);
    }
    return duration;
  }

  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h|d|w)$/.exec(duration);
  if (!match) {
    throw new TypeError(`Invalid duration "${duration}". Use milliseconds or a string such as "15m" or "3d".`);
  }

  return Math.round(Number(match[1]) * UNITS[match[2]]);
}

/** A workflow's run timeout in milliseconds: a zero one would time out before the first step. */
export function runTimeoutMs(timeout: Duration, owner: string): number {
  let ms: number;
  try {
    ms = toMs(timeout);
  } catch {
    ms = 0;
  }

  if (ms <= 0) {
    throw new TypeError(`Invalid timeout ${JSON.stringify(timeout)} for ${owner}. Use a positive duration, such as "30d".`);
  }
  return ms;
}
