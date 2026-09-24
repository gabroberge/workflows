import { WorkflowError } from './workflow.error.js';

/** `start()` with an existing id but a different workflow or input. `status` 409. */
export class WorkflowIdConflictError extends WorkflowError {
  override name = 'WorkflowIdConflictError';
  readonly status = 409;
}
