import type { WorkflowClaimRequest, WorkflowRateLimitRule } from '../interfaces/workflow-store.interface.js';

/** An instance a claim under rate limits picked and locked, in claim order. */
export interface PickedInstance {
  id: string;
  workflow: string;
  rateLimitKey: string | null;
}

/** A rate-limit window's row: its workflow's own (`key` ''), or a rate-limit key's. */
export interface RateWindowRow {
  workflow: string;
  key: string;
  windowEnd: number;
  count: number;
}

/**
 * The rate-limit windows of one claim (`WorkflowStore.claim()` under `rateLimits`), as the SQL stores take room in
 * them: lock the rows of `windows` (inserted where missing, so there is always a row to lock), in that order, which is
 * every claim's order; read them; then write back what `grant()` changed. The counts read under the locks are exact:
 * a concurrent claim of the same window waits for this one.
 */
export class RateWindowClaim {
  /** The windows the picked instances take room in, each once, by workflow then key. */
  readonly windows: ReadonlyArray<{ workflow: string; key: string }>;
  private readonly rules: Map<string, WorkflowRateLimitRule>;

  constructor(
    private readonly request: WorkflowClaimRequest,
    private readonly picked: readonly PickedInstance[],
  ) {
    this.rules = new Map((request.rateLimits ?? []).map((rule) => [rule.workflow, rule]));
    const named = new Map(picked.flatMap((instance) => this.windowsOf(instance)).map((window) => [nameOf(window), window]));
    this.windows = [...named.values()]
      .map(({ workflow, key }) => ({ workflow, key }))
      .sort((a, b) => (a.workflow < b.workflow ? -1 : a.workflow > b.workflow ? 1 : a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  }

  /**
   * Of the picked instances (in claim order), those their windows have room for, and the windows whose rows change:
   * a window that ended counts as none, and the first claim after it opens the next one, `duration` long. `locked`:
   * the windows' rows as read under their locks.
   */
  grant(locked: readonly RateWindowRow[]): { granted: string[]; changed: RateWindowRow[] } {
    const { now } = this.request;
    const open = new Map(locked.map((row) => [nameOf(row), row.windowEnd > now ? { windowEnd: row.windowEnd, count: row.count } : null]));
    const granted: string[] = [];
    const changed = new Map<string, RateWindowRow>();
    for (const instance of this.picked) {
      const windows = this.windowsOf(instance);
      if (!windows.every((window) => (open.get(nameOf(window))?.count ?? 0) < window.max)) {
        continue;
      }

      for (const window of windows) {
        const name = nameOf(window);
        const current = open.get(name) ?? { windowEnd: now + window.duration, count: 0 };
        current.count++;
        open.set(name, current);
        changed.set(name, { workflow: window.workflow, key: window.key, ...current });
      }
      granted.push(instance.id);
    }
    return { granted, changed: [...changed.values()] };
  }

  private windowsOf({ workflow, rateLimitKey }: PickedInstance): Array<{ workflow: string; key: string; max: number; duration: number }> {
    const rule = this.rules.get(workflow);
    return [
      ...(rule?.limit ? [{ workflow, key: '', ...rule.limit }] : []),
      ...(rule?.perKey && rateLimitKey !== null ? [{ workflow, key: rateLimitKey, ...rule.perKey }] : []),
    ];
  }
}

function nameOf(window: { workflow: string; key: string }): string {
  return JSON.stringify([window.workflow, window.key]);
}
