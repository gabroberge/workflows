import type { Type } from '@nestjs/common';
import { signalName } from '../../signals/workflow.signal.js';
import type { SignalOnOptions } from '../interfaces/signal-on-options.interface.js';
import { addEventRoute, assertEventClass, assertMapper } from '../utils/event-routes.util.js';

/**
 * Sends `signal` whenever the application publishes `event` on the CQRS `EventBus`, through
 * `WorkflowClient.signal()`: in the publisher's transaction when it passes `{ transaction }` as
 * the dispatcher context, and before `publish()` resolves. Declare it on the workflow that
 * waits for the signal. Needs `WorkflowsCqrsModule`.
 *
 * A signal reaches every instance waiting for it with the same key, whatever its workflow, so
 * when several workflows (or versions) map one event to the same signal and key, it is sent once.
 * With `id`, an event published again (a redelivery, a retried handler) stores no second signal.
 *
 * @example
 * @Workflow('order-fulfilment')
 * @SignalOn(OrderDeliveredEvent, { signal: shipmentDelivered, key: (event) => event.orderId, id: (event) => event.deliveryId })
 * export class OrderFulfilmentWorkflow implements WorkflowRunner<Order, FulfilmentResult> {}
 */
export function SignalOn<E extends object, T>(event: Type<E>, options: SignalOnOptions<E, T>): ClassDecorator {
  assertEventClass('SignalOn', event);
  const signal = signalName(options?.signal);
  assertMapper('SignalOn', event, 'key', options.key);
  assertMapper('SignalOn', event, 'id', options.id);
  assertMapper('SignalOn', event, 'payload', options.payload);

  return (target) => {
    addEventRoute(target, {
      kind: 'signal',
      event,
      signal,
      key: options.key as ((event: object) => string) | undefined,
      id: options.id as ((event: object) => string) | undefined,
      payload: options.payload as ((event: object) => unknown) | undefined,
    });
  };
}
