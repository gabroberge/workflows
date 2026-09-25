import { WorkflowError } from './workflow.error.js';

/**
 * An operator action that the instance's status rules out: `retry()` of an instance that isn't
 * `failed` or `compensation_failed` (or whose compensations already undid its steps),
 * `delete()` of an unfinished one without `force`, or either racing another change to the same
 * instance. `status` 409.
 */
export class WorkflowStateError extends WorkflowError {
  override name = 'WorkflowStateError';
  readonly status = 409;
}
