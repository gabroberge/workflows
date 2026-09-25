/** How `@StartOn()` turns an event into a `WorkflowClient.start()` call. */
export interface StartOnOptions<E, I> {
  /**
   * The instance id, from the event's business key (`order-${event.order.id}`). An event
   * published twice then finds the instance the first one started, instead of starting another.
   */
  id: (event: E) => string;
  /** The workflow's input. Default: the event itself, as JSON (its own enumerable fields). */
  input?: (event: E) => I;
}
