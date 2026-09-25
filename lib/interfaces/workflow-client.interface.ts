import type { WorkflowInstance, WorkflowStatus } from './workflow-instance.interface.js';

export interface StartWorkflowOptions {
  /**
   * Instance id; default a random UUID. Starting twice with the same id (same
   * workflow and input) returns the existing instance instead of a second one,
   * so derive it from the business key: `order-${orderId}`.
   */
  id?: string;
  /**
   * Pin a version (a positive integer). Default: the highest version of the
   * workflow's name that this application registers, also when you pass a class.
   * Starting by name a workflow this process does not register requires it.
   */
  version?: number;
  /**
   * Your ORM's transaction: the `tx` (or `EntityManager`, `Transaction`...) your transaction
   * callback receives. The instance is created in it, so it exists if and only if your
   * transaction commits, together with the rows you wrote. The worker picks it up after the
   * commit, at its next poll. Needs a store on your database that implements
   * `createInTransaction()`.
   */
  transaction?: unknown;
}

export interface SignalWorkflowOptions {
  /** Correlation key: only waits with the same key take the signal. */
  key?: string;
  /**
   * Deduplication id, unique per signal name, such as the id of the message or event that
   * causes the signal. A signal with the same name and id stored earlier makes this call a
   * no-op that returns that signal's `signalId` with `created: false`: a sender that runs
   * again (a redelivered message, a retried job) stores its signal once, and the first payload
   * wins. The same name and id with a different key throws `WorkflowIdConflictError`.
   * Inside a workflow step, a signal sent without an id gets one derived from the step's
   * `idempotencyKey`, so a retried step doesn't send it again.
   */
  id?: string;
  /**
   * Your ORM's transaction, as for `start()`: the signal, and the wake-up of the instances
   * waiting for it, commit with your writes. Signals queue behind it until it ends, so keep
   * it short. On PostgreSQL it must be READ COMMITTED (the default).
   */
  transaction?: unknown;
}

/** What `signal()` returns. */
export interface WorkflowSignalSendResult {
  /** The stored signal's id: the new one, or with `created: false` the one stored earlier with the same `id`. */
  signalId: number;
  /** Instances parked on a matching wait that this call woke. */
  woken: number;
  /** `false` if a signal with the same name and `id` was stored earlier: nothing was written. */
  created: boolean;
}

/** What `start()` returns: safe to send to a client (no input, no lease details). */
export interface WorkflowStartResult {
  id: string;
  workflow: string;
  version: number;
  /** False if an instance with this id already existed. */
  created: boolean;
  status: WorkflowStatus;
}

export interface WorkflowCancelResult extends WorkflowInstance {
  /**
   * True if this call requested the cancellation. False if the instance was
   * already compensating or had finished, or a cancel was already requested
   * (the first reason is kept).
   */
  accepted: boolean;
}

export interface WorkflowListFilter {
  status?: WorkflowStatus | WorkflowStatus[];
  /** Workflow name. */
  workflow?: string;
  /** With `workflow`: only instances on this version (to see whether an old version has drained). */
  version?: number;
  /** Default 100. */
  limit?: number;
  offset?: number;
}
