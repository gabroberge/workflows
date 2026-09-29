import type { SerializedWorkflowError } from './serialized-workflow-error.interface.js';

export type WorkflowStatus =
  | 'pending'
  | 'running'
  | 'suspended'
  | 'compensating'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'compensation_failed';

/** What happens to a child instance that is still running when its parent ends (`ctx.startChild()`'s `parentClose`). */
export type WorkflowParentClose = 'cancel' | 'terminate' | 'abandon';

export interface WorkflowInstance {
  id: string;
  workflow: string;
  version: number;
  /** The instance that started this one with `ctx.startChild()`, or `null`. */
  parentId: string | null;
  /** For a child: what happens to it if it is still running when its parent ends. */
  parentClose: WorkflowParentClose | null;
  /** The key its workflow's per-key concurrency limit counts it under, or `null`. */
  concurrencyKey: string | null;
  /** The key its workflow's per-key rate limit counts it under, or `null`. */
  rateLimitKey: string | null;
  /** Lower is claimed first; `0` (none given) before every other. */
  priority: number;
  status: WorkflowStatus;
  input: unknown;
  output?: unknown;
  error?: SerializedWorkflowError | null;
  /** When the instance is next runnable. `null` = only a signal or cancel can wake it. */
  wakeAt: number | null;
  leaseOwner: string | null;
  leaseUntil: number | null;
  cancelRequested: boolean;
  /** Set with `cancelRequested` by `WorkflowClient.terminate()`: stop without compensating. */
  terminateRequested: boolean;
  cancelReason: string | null;
  /**
   * When the run timeout passes (`@Workflow(name, { timeout })` or `start()`'s `timeout`), or
   * `null` for none.
   */
  deadline: number | null;
  /**
   * What the workflow last set with `ctx.setStatus()` (JSON), or `null`. Written with the lease
   * holder's writes, so it lags `setStatus()` until the instance's next journal write or
   * suspension.
   */
  customStatus: unknown;
  /** Signals with an id above this one can match the instance's waits. */
  signalCursor: number;
  /** Number of executions (claims) so far. */
  runs: number;
  createdAt: number;
  updatedAt: number;
}

export interface WorkflowJournalEntry {
  name: string;
  /** `retry`: an operator's `WorkflowClient.retry()`, recorded under `$retry:<n>` with what it retried in `data`. */
  kind: 'step' | 'sleep' | 'signal' | 'any' | 'child' | 'commit' | 'now' | 'random' | 'uuid' | 'compensation' | 'retry';
  /**
   * `cancelled`: still pending (a sleep, a wait, a retry backoff) when the
   * instance ended as `cancelled`, `failed` or `compensation_failed`.
   */
  status: 'pending' | 'completed' | 'failed' | 'cancelled';
  /**
   * JSON-safe step result, signal `{ signalId, payload }`, `waitForAny()` winner
   * `{ key, signalId, payload }` (`signalId: null` for a timer), started child
   * `{ id, workflow, version }`, or helper value.
   */
  result?: unknown;
  error?: SerializedWorkflowError;
  /** Attempts started (steps and compensations). */
  attempts: number;
  /** Sleep deadline, wait timeout, or retry time. `null` while an attempt runs. */
  wakeAt?: number | null;
  /** Last `heartbeat(progress)` checkpoint. */
  progress?: unknown;
  /** Wait spec (`{ signal, key }`), or a `waitForAny()`'s conditions (`{ waits, timers }`). */
  data?: unknown;
  updatedAt?: number;
}

/** A signal an instance is waiting for. */
export interface WorkflowWait {
  signal: string;
  key: string | null;
}
