/**
 * Base class of the errors the package throws for an app to catch or inspect.
 * Not of `WorkflowInterrupt` (control flow, never to be kept) or
 * `NonRetryableStepError` (thrown by the app).
 */
export abstract class WorkflowError extends Error {}
