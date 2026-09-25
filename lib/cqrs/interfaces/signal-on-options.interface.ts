import type { WorkflowSignal } from '../../signals/workflow.signal.js';

/**
 * How `@SignalOn()` turns an event into a `WorkflowClient.signal()` call. `payload` may be
 * left out when the event itself has the signal's payload type.
 */
export type SignalOnOptions<E, T> = {
  /** The signal the workflow waits for with `ctx.waitForSignal()`. */
  signal: WorkflowSignal<T> | string;
  /** The correlation key, such as the order id. Without one, only waits without a key match. */
  key?: (event: E) => string;
} & ([E] extends [T] ? { payload?: (event: E) => T } : { payload: (event: E) => T });
