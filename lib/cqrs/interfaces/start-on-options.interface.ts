/** How `@StartOn()` turns an event into a `WorkflowClient.start()` call. */
export interface StartOnOptions<E, I> {
  /**
   * The instance id, from the event's business key (`order-${event.order.id}`). An event
   * published twice then finds the instance the first one started, instead of starting another.
   */
  id: (event: E) => string;
  /** The workflow's input. Default: the event itself, as JSON (its own enumerable fields). */
  input?: (event: E) => I;
  /**
   * The instance's priority, as `WorkflowClient.start()`'s: lower is claimed first, an integer from 1 to
   * 2,097,151. A number, or a function of the event that returns one, or `undefined` for none. Default: none.
   */
  priority?: number | ((event: E) => number | undefined);
  /**
   * The key the workflow's per-key concurrency limit counts the instance under, instead of the one its
   * `concurrency.key` computes from the input, as `start()`'s `concurrencyKey`: a string, or a function of the event
   * that returns one, or `undefined` for the computed one. Needs a concurrency limit with a `key`.
   */
  concurrencyKey?: string | ((event: E) => string | undefined);
  /** The same for the workflow's per-key rate limit, as `start()`'s `rateLimitKey`. Needs a rate limit with a `key`. */
  rateLimitKey?: string | ((event: E) => string | undefined);
}
