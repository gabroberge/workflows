import type { Type } from '@nestjs/common';
import { WORKFLOW_EVENT_ROUTES_METADATA } from '../../workflows.constants.js';
import type { WorkflowEventRoute } from '../interfaces/workflow-event-route.interface.js';

/** The class's own routes, in declaration order (decorators apply bottom-up). */
export function eventRoutesOf(target: Function): WorkflowEventRoute[] {
  return (Reflect.getOwnMetadata(WORKFLOW_EVENT_ROUTES_METADATA, target) as WorkflowEventRoute[] | undefined) ?? [];
}

export function addEventRoute(target: Function, route: WorkflowEventRoute): void {
  Reflect.defineMetadata(WORKFLOW_EVENT_ROUTES_METADATA, [route, ...eventRoutesOf(target)], target);
}

export function assertEventClass(decorator: string, event: unknown): asserts event is Type<object> {
  if (typeof event !== 'function') {
    throw new TypeError(`@${decorator}() takes the event class first, got ${event === null ? 'null' : typeof event}.`);
  }
}

export function assertMapper(decorator: string, event: Type<object>, option: string, value: unknown, required = false): void {
  if (value === undefined && !required) {
    return;
  }

  if (typeof value !== 'function') {
    throw new TypeError(
      `@${decorator}(${event.name}): \`${option}\` must be a function of the event, such as (event) => event.orderId` +
        `${required ? '' : ', or left out'}.`,
    );
  }
}
