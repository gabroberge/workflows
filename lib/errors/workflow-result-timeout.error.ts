import { WorkflowError } from './workflow.error.js';

/**
 * `WorkflowClient.result()` (or `startAndWait()`) waited for its `timeout` and the instance
 * hadn't ended. The instance keeps running: wait again, or read it with `getStatus()`.
 */
export class WorkflowResultTimeoutError extends WorkflowError {
  override name = 'WorkflowResultTimeoutError';

  constructor(
    readonly instanceId: string,
    readonly timeoutMs: number,
  ) {
    super(`Instance "${instanceId}" didn't end within ${timeoutMs}ms. It keeps running; wait for it again or read it with getStatus().`);
  }
}
