import { Injectable, type OnApplicationShutdown } from '@nestjs/common';
import { Subject, type Observable } from 'rxjs';
import type { WorkflowEvent } from './workflow-events.interface.js';
import { channelFor } from './workflows.channels.js';

/**
 * This application's lifecycle events, for logs, metrics and alerts. Every event
 * is also published on its `node:diagnostics_channel` channel
 * (`nestjs:workflows:<type>`), process-wide, where tracing tools (NestJS Observe,
 * OpenTelemetry) subscribe without Nest. An instance maps onto one trace: the
 * instance is the root span, each execution a child span, each step attempt a
 * span inside it (https://docs.nestjs.com/reliability/workflows#events).
 */
@Injectable()
export class WorkflowEvents implements OnApplicationShutdown {
  private readonly subject = new Subject<WorkflowEvent>();
  readonly events$: Observable<WorkflowEvent> = this.subject.asObservable();

  emit(event: WorkflowEvent): void {
    const target = channelFor(event.type);
    if (target.hasSubscribers) {
      target.publish(event);
    }
    this.subject.next(event);
  }

  // After onModuleDestroy, where the worker drains and may still emit.
  onApplicationShutdown() {
    this.subject.complete();
  }
}
