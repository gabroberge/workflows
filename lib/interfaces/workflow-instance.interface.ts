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
  /** Signals with an id above this one can match the instance's waits. */
  signalCursor: number;
  /** Number of executions (claims) so far. */
  runs: number;
  createdAt: number;
  updatedAt: number;
}

export interface WorkflowJournalEntry {
  name: string;
  kind: 'step' | 'sleep' | 'signal' | 'commit' | 'now' | 'random' | 'uuid' | 'compensation';
  /**
   * `cancelled`: still pending (a sleep, a wait, a retry backoff) when the
   * instance ended as `cancelled`, `failed` or `compensation_failed`.
   */
  status: 'pending' | 'completed' | 'failed' | 'cancelled';
  /** JSON-safe step result, signal `{ signalId, payload }`, or helper value. */
  result?: unknown;
  error?: SerializedWorkflowError;
  /** Attempts started (steps and compensations). */
  attempts: number;
  /** Sleep deadline, wait timeout, or retry time. `null` while an attempt runs. */
  wakeAt?: number | null;
  /** Last `heartbeat(progress)` checkpoint. */
  progress?: unknown;
  /** Wait spec (`{ signal, key }`). */
  data?: unknown;
  updatedAt?: number;
}

/** A signal an instance is waiting for. */
export interface WorkflowWait {
  signal: string;
  key: string | null;
}
