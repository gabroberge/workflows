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

export interface WorkflowInstance {
  id: string;
  workflow: string;
  version: number;
  status: WorkflowStatus;
  input: unknown;
  output?: unknown;
  error?: SerializedWorkflowError | null;
  /** When the instance is next runnable. `null` = only a signal or cancel can wake it. */
  wakeAt: number | null;
  leaseOwner: string | null;
  leaseUntil: number | null;
  cancelRequested: boolean;
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
  kind: 'step' | 'sleep' | 'signal' | 'any' | 'commit' | 'now' | 'random' | 'uuid' | 'compensation' | 'retry';
  /**
   * `cancelled`: still pending (a sleep, a wait, a retry backoff) when the
   * instance ended as `cancelled`, `failed` or `compensation_failed`.
   */
  status: 'pending' | 'completed' | 'failed' | 'cancelled';
  /**
   * JSON-safe step result, signal `{ signalId, payload }`, `waitForAny()` winner
   * `{ key, signalId, payload }` (`signalId: null` for a timer), or helper value.
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
