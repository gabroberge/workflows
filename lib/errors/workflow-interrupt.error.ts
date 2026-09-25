export type InterruptReason =
  /** A sleep, wait or retry backoff is pending: the run is parked until later. */
  | 'suspend'
  /** A replay for compensation reached code that has not run yet. */
  | 'halt'
  /** `WorkflowClient.cancel()` was accepted while the run was executing. */
  | 'cancel'
  /** The instance's run timeout passed. */
  | 'timeout'
  /** The instance's journal reached the module's `journal.maxEntries` or `journal.maxBytes`. */
  | 'journal-limit'
  /** Another worker took over the instance; nothing this run does is recorded. */
  | 'lease-lost'
  /** The application is shutting down; the instance is handed back. */
  | 'shutdown'
  /** The store failed; the instance is retried once the lease expires. */
  | 'store-error';

/**
 * Thrown by `ctx` operations to unwind the workflow function. It is control
 * flow, not a failure: code that catches errors inside `run()` must rethrow it
 * (`if (isWorkflowInterrupt(e)) throw e`). The engine does not depend on seeing
 * it, though. Once an execution is parking, no step starts and nothing passes a
 * `commit()`; a cancel, shutdown, lost lease or store failure decides the
 * outcome however `run()` ends. A swallowed interrupt only wastes the code that
 * runs after it.
 */
export class WorkflowInterrupt extends Error {
  override name = 'WorkflowInterrupt';
  constructor(readonly reason: InterruptReason) {
    super(`Workflow execution interrupted (${reason}). Rethrow this error if you catch it.`);
  }
}

export function isWorkflowInterrupt(error: unknown): error is WorkflowInterrupt {
  return error instanceof WorkflowInterrupt;
}
