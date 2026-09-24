import { WorkflowError } from './workflow.error.js';

/**
 * The workflow code cannot run against this instance's journal: a name is used
 * twice, a `ctx` method is called inside a step, or a deployed change renamed,
 * removed or reordered steps. The instance fails without compensation, because
 * the compensations are defined by the same code that no longer matches.
 */
export class WorkflowDefinitionError extends WorkflowError {
  override name = 'WorkflowDefinitionError';
}
