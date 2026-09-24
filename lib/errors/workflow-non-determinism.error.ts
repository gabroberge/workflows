import { WorkflowDefinitionError } from './workflow-definition.error.js';

export class WorkflowNonDeterminismError extends WorkflowDefinitionError {
  override name = 'WorkflowNonDeterminismError';
}
