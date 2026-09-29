import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';
import { WorkflowFailedError, type WorkflowFailureStatus } from './workflow-failed.error.js';

/**
 * A child workflow (`ctx.startChild()`) ended without completing: what its handle's `result()`
 * and `ctx.executeChild()` throw in the parent. `status` says how it ended, `cause` is its error.
 * Built from the journal on every replay, like `StepFailedError`, so a parent that catches it
 * and branches sees the same error each time.
 */
export class ChildWorkflowFailedError extends WorkflowFailedError {
  override name = 'ChildWorkflowFailedError';

  constructor(
    readonly workflow: string,
    instanceId: string,
    status: WorkflowFailureStatus,
    cause: SerializedWorkflowError,
  ) {
    super(`Child workflow "${workflow}" ("${instanceId}") ${status.replace('_', ' ')}: ${cause.name}: ${cause.message}`, { instanceId, status, cause });
  }
}
