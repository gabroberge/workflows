import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';
import { WorkflowError } from './workflow.error.js';

/** How an instance ended when it didn't complete. */
export type WorkflowFailureStatus = 'failed' | 'cancelled' | 'compensation_failed';

/**
 * Thrown by `ctx.fail()`: fails the instance and runs its compensations. Also what
 * `WorkflowClient.result()` and `startAndWait()` reject with when the instance ended without
 * completing: then `instanceId`, `status` and `cause` (the instance's error) are set.
 */
export class WorkflowFailedError extends WorkflowError {
  override name = 'WorkflowFailedError';
  readonly instanceId?: string;
  readonly status?: WorkflowFailureStatus;
  override readonly cause?: SerializedWorkflowError;

  constructor(message: string, details?: { instanceId: string; status: WorkflowFailureStatus; cause: SerializedWorkflowError }) {
    super(message);
    if (details) {
      this.instanceId = details.instanceId;
      this.status = details.status;
      this.cause = details.cause;
    }
  }
}
