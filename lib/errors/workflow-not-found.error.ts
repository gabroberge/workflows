import { WorkflowError } from './workflow.error.js';

/**
 * `cancel()` of an unknown instance id, or `start()` of a workflow this
 * application cannot resolve. `status` 404: the caller asked for something
 * that doesn't exist, so retrying won't help.
 */
export class WorkflowNotFoundError extends WorkflowError {
  override name = 'WorkflowNotFoundError';
  readonly status = 404;
}
