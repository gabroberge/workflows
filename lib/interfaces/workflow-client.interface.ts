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
   * Your ORM's transaction, as for `start()`: the signal, and the wake-up of the instances
   * waiting for it, commit with your writes. Signals queue behind it until it ends, so keep
   * it short. On PostgreSQL it must be READ COMMITTED (the default).
   */
  transaction?: unknown;
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
