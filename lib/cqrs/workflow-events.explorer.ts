import { Injectable, type Type } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';
import { WorkflowRegistry } from '../services/workflow-registry.service.js';
import { WORKFLOW_METADATA } from '../workflows.constants.js';
import type {
  WorkflowEventRoute,
  WorkflowSignalRoute,
  WorkflowStartRoute,
} from './interfaces/workflow-event-route.interface.js';
import { eventRoutesOf } from './utils/event-routes.util.js';

/** What publishing one event class does. */
export interface WorkflowEventTargets {
  /** At most one per workflow name: the highest registered version's `@StartOn()`. */
  starts: Array<{ workflow: Type<unknown>; name: string; route: WorkflowStartRoute }>;
  /** From every registered version: an older version's instances still wait for their signals. */
  signals: Array<{ workflow: Type<unknown>; name: string; route: WorkflowSignalRoute }>;
}

/** Reads `@StartOn()` and `@SignalOn()` from the registered workflows. */
@Injectable()
export class WorkflowEventsExplorer {
  constructor(
    private readonly discovery: DiscoveryService,
    private readonly registry: WorkflowRegistry,
  ) {}

  explore(): Map<Function, WorkflowEventTargets> {
    this.rejectRoutesOutsideWorkflows();

    const table = new Map<Function, WorkflowEventTargets>();
    const targetsOf = (event: Function) => {
      let targets = table.get(event);
      if (!targets) {
        table.set(event, (targets = { starts: [], signals: [] }));
      }
      return targets;
    };

    for (const { name, version } of this.registry.versions()) {
      const definition = this.registry.get(name, version)!;
      const routes = eventRoutesOf(definition.type);
      assertNoDuplicates(definition.type, routes);

      // Like start(Class): an event starts the highest registered version, never an older one
      // that is only kept registered to drain, and never twice.
      const latest = this.registry.latest(name)!.version === version;
      for (const route of routes) {
        if (route.kind === 'signal') {
          targetsOf(route.event).signals.push({ workflow: definition.type, name, route });
        } else if (latest) {
          targetsOf(route.event).starts.push({ workflow: definition.type, name, route });
        }
      }
    }

    return table;
  }

  private rejectRoutesOutsideWorkflows(): void {
    for (const wrapper of this.discovery.getProviders()) {
      const type = (!wrapper.metatype || wrapper.inject ? wrapper.instance?.constructor : wrapper.metatype) as
        | Type<unknown>
        | undefined;
      if (type && eventRoutesOf(type).length > 0 && !Reflect.getMetadata(WORKFLOW_METADATA, type)) {
        throw new Error(
          `${type.name} has @StartOn() or @SignalOn() but no @Workflow(). Events start and signal workflows: ` +
            'put them on the @Workflow() class.',
        );
      }
    }
  }
}

function assertNoDuplicates(workflow: Type<unknown>, routes: WorkflowEventRoute[]): void {
  // Per event class (not its name): two classes may share a name.
  const seen = new Map<Function, Set<string>>();
  for (const route of routes) {
    const kinds = seen.get(route.event) ?? new Set<string>();
    const kind = route.kind === 'start' ? 'start' : `signal:${route.signal}`;
    if (kinds.has(kind)) {
      throw new Error(
        route.kind === 'start'
          ? `${workflow.name} has @StartOn(${route.event.name}) twice. An event starts one instance of a workflow: keep one.`
          : `${workflow.name} has @SignalOn(${route.event.name}) for the signal "${route.signal}" twice: keep one.`,
      );
    }
    seen.set(route.event, kinds.add(kind));
  }
}
