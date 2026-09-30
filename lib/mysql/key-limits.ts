/**
 * The longest keys MySqlWorkflowStore keeps, in characters: its key columns' lengths (the migrations use them, and the
 * store checks every key it writes against them, so they can't drift apart). Every id, name and key is an indexed
 * `utf8mb4_0900_bin` column on MySQL, and InnoDB caps an index key at 3,072 bytes, 768 four-byte characters, which a
 * composite index's columns share: a journal entry's `(instance id, name)` takes 255 + 512.
 */
export const MYSQL_KEY_LIMITS = {
  /** An instance's id (`start({ id })`), and so a parent's; a child's id and a schedule run's id are instance ids too. */
  instanceId: 255,
  /** A schedule's id: its runs' ids (`<id>@<ISO time>`) add 25 characters and must fit an instance id. */
  scheduleId: 230,
  /** A workflow's name. */
  workflow: 255,
  /** An instance's concurrency key. */
  concurrencyKey: 255,
  /** An instance's rate-limit key. */
  rateLimitKey: 255,
  /** A signal's name, and a wait's. */
  signal: 255,
  /** A signal's key, and a wait's. */
  signalKey: 255,
  /** A signal's id (`signal(..., { id })`), which deduplicates it. */
  dedupeId: 255,
  /** A journal entry's name: a step's, a wait's, or the engine's (`$child:<child id>`, `$compensate:<step>`...). */
  journalName: 512,
  /** A lease's token (the engine's: a UUID). */
  leaseToken: 255,
} as const;

/** Throws a `RangeError` that names `what` when `value` is longer than `max` characters (code points, as MySQL counts). */
export function checkKey(what: string, value: string | null | undefined, max: number): void {
  // A string's UTF-16 length is at least its code points: most keys pass on the first test.
  if (typeof value !== 'string' || value.length <= max) {
    return;
  }

  const characters = [...value].length;
  if (characters > max) {
    const preview = characters > 40 ? `${[...value].slice(0, 40).join('')}…` : value;
    throw new RangeError(
      `MySqlWorkflowStore: ${what} holds at most ${max} characters on MySQL, and ${JSON.stringify(preview)} has ${characters}. ` +
        "The store's ids, names and keys are indexed columns of bounded length there.",
    );
  }
}
