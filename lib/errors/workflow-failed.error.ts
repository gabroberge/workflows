import { WorkflowError } from './workflow.error.js';

/** Thrown by `ctx.fail()`. Fails the instance and runs its compensations. */
export class WorkflowFailedError extends WorkflowError {
  override name = 'WorkflowFailedError';
}
