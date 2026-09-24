import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';
import { WorkflowError } from './workflow.error.js';

/**
 * A step gave up (retries exhausted, or a `NonRetryableStepError`). Built from the
 * journal on both the first run and every replay, so a workflow that catches it
 * and branches sees exactly the same error each time. The original class is not
 * preserved; `cause` carries its name and message.
 */
export class StepFailedError extends WorkflowError {
  override name = 'StepFailedError';
  constructor(
    readonly step: string,
    readonly attempts: number,
    override readonly cause: SerializedWorkflowError,
  ) {
    super(`Step "${step}" failed after ${attempts} attempt(s): ${cause.name}: ${cause.message}`);
  }
}
