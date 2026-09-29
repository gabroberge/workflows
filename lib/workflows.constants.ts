export const WORKFLOW_METADATA = 'workflows:workflow';
/**
 * The signal a child instance sends its parent when it ends, keyed by the child's id. Signal names
 * starting with "$" are the engine's: `WorkflowSignal` and `WorkflowClient.signal()` refuse them.
 */
export const CHILD_ENDED_SIGNAL = '$child-ended';
/** `@StartOn()` and `@SignalOn()` (`@nestjs/workflows/cqrs`) on a workflow class. */
export const WORKFLOW_EVENT_ROUTES_METADATA = 'workflows:event-routes';
