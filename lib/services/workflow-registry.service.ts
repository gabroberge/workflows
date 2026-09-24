import { Injectable, type Type } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';
import { WorkflowNotFoundError } from '../errors/workflow-not-found.error.js';
import type { WorkflowRunner } from '../interfaces/workflow-runner.interface.js';
import type { WorkflowMetadata } from '../interfaces/workflow-decorator-options.interface.js';
import { WORKFLOW_METADATA } from '../workflows.constants.js';

export interface WorkflowDefinition extends WorkflowMetadata {
  key: string;
  instance: WorkflowRunner;
  type: Type<unknown>;
}

export const workflowKey = (name: string, version: number) => `${name}@${version}`;

/** Finds every `@Workflow()` provider, wherever it is registered. */
@Injectable()
export class WorkflowRegistry {
  private definitions?: Map<string, WorkflowDefinition>;

  constructor(private readonly discovery: DiscoveryService) {}

  /** `name@version` keys this process can run. */
  keys(): string[] {
    return [...this.load().keys()];
  }

  /** The workflow versions this process can run, for claims. */
  versions(): Array<{ name: string; version: number }> {
    return [...this.load().values()].map(({ name, version }) => ({ name, version }));
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
    let fallback: number | undefined;
    if (typeof workflow === 'string') {
      name = workflow;
    } else {
      const meta = Reflect.getMetadata(WORKFLOW_METADATA, workflow) as WorkflowMetadata | undefined;
      if (!meta) {
        throw new WorkflowNotFoundError(`${workflow.name} is not decorated with @Workflow().`);
      }
      name = meta.name;
      // A class this process does not register (an API pod starting work for
      // a worker pod) still knows its own version.
      fallback = meta.version;
    }

    if (version !== undefined) {
      if (!Number.isInteger(version) || version < 1) {
        throw new TypeError(`Invalid version ${version} for workflow "${name}". Use a positive integer.`);
      }
      return { name, version };
    }

    const def = this.latest(name);
    if (def) {
      return { name: def.name, version: def.version };
    }

    if (fallback !== undefined) {
      return { name, version: fallback };
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
      definitions.set(key, { ...meta, key, instance, type });
    }

    return (this.definitions = definitions);
  }
}
