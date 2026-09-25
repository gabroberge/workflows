import { applyDecorators, Injectable, SetMetadata } from '@nestjs/common';
import { runTimeoutMs } from '../utils/duration.util.js';
import { WORKFLOW_METADATA } from '../workflows.constants.js';
import type {
  WorkflowMetadata,
  WorkflowDecoratorOptions,
} from '../interfaces/workflow-decorator-options.interface.js';

/**
 * Marks an injectable class with a `run(ctx, input)` method as a durable
 * workflow. Register it as a provider in any module; constructor injection
 * works as usual. Workflows must be singletons (default scope).
 */
export function Workflow(name: string, options: WorkflowDecoratorOptions = {}): ClassDecorator {
  if (!/^[\w.:-]+$/.test(name)) {
    throw new TypeError(`Invalid workflow name "${name}". Use letters, digits, ".", ":", "_" or "-".`);
  }

  const version = options.version ?? 1;
  if (!Number.isInteger(version) || version < 1) {
    throw new TypeError(`Invalid version ${version} for workflow "${name}". Use a positive integer.`);
  }

  const timeout = options.timeout === undefined ? undefined : runTimeoutMs(options.timeout, `workflow "${name}"`);
  return applyDecorators(Injectable(), SetMetadata(WORKFLOW_METADATA, { name, version, timeout } satisfies WorkflowMetadata));
}
