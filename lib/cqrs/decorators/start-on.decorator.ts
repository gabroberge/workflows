import type { Type } from '@nestjs/common';
import type { WorkflowRunner } from '../../interfaces/workflow-runner.interface.js';
import type { StartOnOptions } from '../interfaces/start-on-options.interface.js';
import { addEventRoute, assertEventClass, assertMapper } from '../utils/event-routes.util.js';

/**
 * Starts the decorated `@Workflow()` whenever the application publishes `event` on the CQRS
 * `EventBus`, through `WorkflowClient.start()`: in the publisher's transaction when it passes
 * `{ transaction }` as the dispatcher context, and before `publish()` resolves. `id` makes a
 * repeated event find the instance the first one started. Needs `WorkflowsCqrsModule`.
 *
 * When several versions of a workflow are registered, the highest version's `@StartOn()`
 * decorators decide what starts it.
 *
 * @example
 * @Workflow('order-fulfilment')
 * @StartOn(OrderPlacedEvent, { id: (event) => `order-${event.order.id}`, input: (event) => event.order })
 * export class OrderFulfilmentWorkflow implements WorkflowRunner<Order, FulfilmentResult> {}
 */
export function StartOn<E extends object, I = E>(
  event: Type<E>,
  options: StartOnOptions<E, I>,
): <W extends Type<WorkflowRunner<I, unknown>>>(target: W) => void {
  assertEventClass('StartOn', event);
  assertMapper('StartOn', event, 'id', options?.id, true);
  assertMapper('StartOn', event, 'input', options.input);

  return (target) => {
    addEventRoute(target, {
      kind: 'start',
      event,
      id: options.id as (event: object) => string,
      input: options.input as ((event: object) => unknown) | undefined,
    });
  };
}
