import type { Type } from '@nestjs/common';
import type { WorkflowRunner } from '../../interfaces/workflow-runner.interface.js';
import { MAX_PRIORITY } from '../../utils/new-instance.util.js';
import type { StartOnOptions } from '../interfaces/start-on-options.interface.js';
import type { WorkflowStartRoute } from '../interfaces/workflow-event-route.interface.js';
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
  assertPriority(event, options.priority);
  assertKey(event, 'concurrencyKey', options.concurrencyKey);
  assertKey(event, 'rateLimitKey', options.rateLimitKey);

  return (target) => {
    addEventRoute(target, {
      kind: 'start',
      event,
      id: options.id as (event: object) => string,
      input: options.input as ((event: object) => unknown) | undefined,
      priority: options.priority as WorkflowStartRoute['priority'],
      concurrencyKey: options.concurrencyKey as WorkflowStartRoute['concurrencyKey'],
      rateLimitKey: options.rateLimitKey as WorkflowStartRoute['rateLimitKey'],
    });
  };
}

function assertPriority(event: Type<object>, priority: unknown): void {
  if (priority === undefined || typeof priority === 'function') {
    return;
  }

  if (typeof priority !== 'number' || !Number.isSafeInteger(priority) || priority < 0 || priority > MAX_PRIORITY) {
    throw new TypeError(
      `@StartOn(${event.name}): invalid priority ${JSON.stringify(priority)}. Use an integer from 1 (first) to ${MAX_PRIORITY}, or a function of the event.`,
    );
  }
}

function assertKey(event: Type<object>, option: 'concurrencyKey' | 'rateLimitKey', key: unknown): void {
  if (key === undefined || typeof key === 'function' || (typeof key === 'string' && key.length > 0)) {
    return;
  }

  throw new TypeError(
    `@StartOn(${event.name}): \`${option}\` must be a non-empty string, or a function of the event, such as (event) => event.warehouseId.`,
  );
}
