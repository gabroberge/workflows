import { Injectable, type Type } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';
import { WorkflowNotFoundError } from '../errors/workflow-not-found.error.js';
import type { WorkflowRunner } from '../interfaces/workflow-runner.interface.js';
import type { WorkflowMetadata } from '../interfaces/workflow-decorator-options.interface.js';
import type { WorkflowConcurrencyLimit } from '../interfaces/workflow-store.interface.js';
import { WORKFLOW_EVENT_ROUTES_METADATA, WORKFLOW_METADATA } from '../workflows.constants.js';

export interface WorkflowDefinition extends WorkflowMetadata {
  key: string;
  instance: WorkflowRunner;
  type: Type<unknown>;
}

export const workflowKey = (name: string, version: number) => `${name}@${version}`;

/**
 * Internal: `WorkflowsCqrsModule` calls it from its publisher's constructor, before any
 * definition loads. Without it, a workflow mapped from CQRS events fails the startup check.
 */
export const ROUTE_EVENTS = Symbol('WorkflowRegistry.routeEvents');

/** Finds every `@Workflow()` provider, wherever it is registered. */
@Injectable()
export class WorkflowRegistry {
  private definitions?: Map<string, WorkflowDefinition>;
  private eventsRouted = false;

  constructor(private readonly discovery: DiscoveryService) {}

  /** `name@version` keys this process can run. */
  keys(): string[] {
    return [...this.load().keys()];
  }

  /** The workflow versions this process can run, for claims. */
  versions(): Array<{ name: string; version: number }> {
    return [...this.load().values()].map(({ name, version }) => ({ name, version }));
  }

  /** The concurrency limits of the workflows this process runs, for claims: each name's highest registered version's. */
  limits(): WorkflowConcurrencyLimit[] {
    const names = new Set([...this.load().values()].map((def) => def.name));
    return [...names].flatMap((name) => {
      const concurrency = this.latest(name)!.concurrency;
      return concurrency ? [{ workflow: name, limit: concurrency.limit, perKey: concurrency.perKey }] : [];
    });
  }

  [ROUTE_EVENTS](): void {
    this.eventsRouted = true;
  }

  get(name: string, version: number): WorkflowDefinition | undefined {
    return this.load().get(workflowKey(name, version));
  }

  /** Highest registered version of `name`. */
  latest(name: string): WorkflowDefinition | undefined {
    let latest: WorkflowDefinition | undefined;
    for (const def of this.load().values()) {
      if (def.name === name && (!latest || def.version > latest.version)) {
        latest = def;
      }
    }
    return latest;
  }

  /**
   * Name and version for a `start()` call. A class only identifies the
   * workflow by name: like a name, it starts the highest version registered in
   * this application, so a controller that passes the version 1 class starts
   * version 2 once it is deployed. `version` pins one explicitly.
   */
  resolve(workflow: Type<unknown> | string, version?: number): WorkflowMetadata {
    let name: string;
    let fallback: WorkflowMetadata | undefined;
    if (typeof workflow === 'string') {
      name = workflow;
    } else {
      const meta = Reflect.getMetadata(WORKFLOW_METADATA, workflow) as WorkflowMetadata | undefined;
      if (!meta) {
        throw new WorkflowNotFoundError(`${workflow.name} is not decorated with @Workflow().`);
      }
      name = meta.name;
      // A class this process does not register (an API pod starting work for
      // a worker pod) still knows its own version and timeout.
      fallback = meta;
    }

    if (version !== undefined) {
      if (!Number.isInteger(version) || version < 1) {
        throw new TypeError(`Invalid version ${version} for workflow "${name}". Use a positive integer.`);
      }
      const known = this.get(name, version) ?? (fallback?.version === version ? fallback : undefined);
      return { name, version, timeout: known?.timeout, concurrency: this.latest(name)?.concurrency ?? fallback?.concurrency ?? known?.concurrency };
    }

    const def = this.latest(name);
    if (def) {
      return { name: def.name, version: def.version, timeout: def.timeout, concurrency: def.concurrency };
    }

    if (fallback !== undefined) {
      return { name, version: fallback.version, timeout: fallback.timeout, concurrency: fallback.concurrency };
    }
    throw new WorkflowNotFoundError(
      `Workflow "${name}" is not registered in this application. Register it, or pass { version } to start it from a process that does not run it.`,
    );
  }

  private load(): Map<string, WorkflowDefinition> {
    if (this.definitions) {
      return this.definitions;
    }

    const definitions = new Map<string, WorkflowDefinition>();
    for (const wrapper of this.discovery.getProviders()) {
      const type = (!wrapper.metatype || wrapper.inject ? wrapper.instance?.constructor : wrapper.metatype) as
        | Type<unknown>
        | undefined;
      const meta = type && (Reflect.getMetadata(WORKFLOW_METADATA, type) as WorkflowMetadata | undefined);
      if (!meta) {
        continue;
      }

      if (!wrapper.isDependencyTreeStatic() || wrapper.isTransient) {
        throw new Error(`Workflow ${type.name} must be a singleton; request-scoped and transient workflows are not supported.`);
      }

      const key = workflowKey(meta.name, meta.version);
      const existing = definitions.get(key);
      if (existing && existing.type !== type) {
        throw new Error(`Workflow "${key}" is defined twice (${existing.type.name} and ${type.name}). Bump the version of one of them.`);
      }

      const instance = wrapper.instance as WorkflowRunner;
      if (typeof instance?.run !== 'function') {
        throw new Error(`Workflow ${type.name} must have a run(ctx, input) method.`);
      }

      // The main entry never imports @nestjs/cqrs: the decorators leave metadata, and the
      // subpath's module marks the registry. Mapped events with no publisher would start nothing.
      if (!this.eventsRouted && Reflect.getOwnMetadata(WORKFLOW_EVENT_ROUTES_METADATA, type)) {
        throw new Error(
          `Workflow ${type.name} is started or signalled by CQRS events (@StartOn(), @SignalOn()), but ` +
            'WorkflowsCqrsModule is not imported, so publishing those events would start and signal nothing. ' +
            "Import WorkflowsCqrsModule from '@nestjs/workflows/cqrs' next to CqrsModule.forRoot().",
        );
      }
      definitions.set(key, { ...meta, key, instance, type });
    }

    return (this.definitions = definitions);
  }
}
