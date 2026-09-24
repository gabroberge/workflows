import { WorkflowError } from './workflow.error.js';

/** A step attempt ran past its `timeout` or `heartbeatTimeout`. Retryable. */
export class StepTimeoutError extends WorkflowError {
  override name = 'StepTimeoutError';
}
