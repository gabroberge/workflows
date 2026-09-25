import { Module } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { WorkflowEventPublisher } from './workflow-event.publisher.js';
import { WorkflowEventsExplorer } from './workflow-events.explorer.js';

/**
 * Makes CQRS events start and signal workflows. Import it next to `CqrsModule.forRoot()` and
 * `WorkflowsModule.forRoot()` (both global): it wraps the `EventBus` publisher, so an event a
 * workflow maps with `@StartOn()` or `@SignalOn()` starts or signals it before `publish()`
 * resolves, in the transaction passed as the dispatcher context
 * (`eventBus.publish(event, { transaction: tx })`), and then reaches the event handlers and
 * sagas as before.
 */
@Module({
  imports: [DiscoveryModule],
  providers: [WorkflowEventsExplorer, WorkflowEventPublisher],
})
export class WorkflowsCqrsModule {}
