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
  /**
   * The signal's deduplication id (`WorkflowClient.signal()`'s `id`), such as the id of the
   * delivery the event reports: an event published again sends nothing new. Without one, the
   * signal is sent each time the event is published, except from inside a workflow step,
   * where the id derives from the step's `idempotencyKey`.
   */
  id?: (event: E) => string;
} & ([E] extends [T] ? { payload?: (event: E) => T } : { payload: (event: E) => T });
