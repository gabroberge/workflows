import type { WorkflowJournalLimits } from '../interfaces/workflows-module-options.interface.js';

export const DEFAULT_JOURNAL_LIMITS: Required<WorkflowJournalLimits> = {
  warnEntries: 1_000,
  warnBytes: 1_000_000,
  maxEntries: 10_000,
  maxBytes: 10_000_000,
};

/** The module's `journal` option over the defaults; throws for a limit that isn't a positive number. */
export function resolveJournalLimits(limits: WorkflowJournalLimits = {}): Required<WorkflowJournalLimits> {
  const resolved = { ...DEFAULT_JOURNAL_LIMITS };
  for (const key of Object.keys(DEFAULT_JOURNAL_LIMITS) as Array<keyof WorkflowJournalLimits>) {
    const value = limits[key];
    if (value === undefined) {
      continue;
    }
    if (typeof value !== 'number' || Number.isNaN(value) || value <= 0) {
      throw new TypeError(`journal.${key} (${String(value)}) must be a positive number, or Infinity to turn the check off.`);
    }
    resolved[key] = value;
  }

  return resolved;
}

/** An entry's size as the journal stores it: UTF-8 JSON. */
export function entryBytes(entry: unknown): number {
  return Buffer.byteLength(JSON.stringify(entry) ?? '', 'utf8');
}
