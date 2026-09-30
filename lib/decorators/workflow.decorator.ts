import { applyDecorators, Injectable, SetMetadata } from '@nestjs/common';
import { resolveConcurrency, resolveRateLimit } from '../core/limits/limits.js';
import { runTimeoutMs } from '../utils/run-timeout.util.js';
import { normalize } from '../utils/normalize.util.js';
import { SCHEDULE_ID, scheduleSpec } from '../utils/schedule-spec.util.js';
import { WORKFLOW_METADATA } from '../workflows.constants.js';
import type { WorkflowDeclaredSchedule, WorkflowMetadata, WorkflowDecoratorOptions } from '../interfaces/workflow-decorator-options.interface.js';
import type { WorkflowScheduleDeclaration } from '../interfaces/workflow-schedule.interface.js';

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
  const concurrency = options.concurrency === undefined ? null : resolveConcurrency(`workflow "${name}"`, options.concurrency);
  const rateLimit = options.rateLimit === undefined ? null : resolveRateLimit(`workflow "${name}"`, options.rateLimit);
  const schedules = options.schedules === undefined ? [] : schedulesOf(name, options.schedules);
  return applyDecorators(Injectable(), SetMetadata(WORKFLOW_METADATA, { name, version, timeout, concurrency, rateLimit, schedules } satisfies WorkflowMetadata));
}

function schedulesOf(name: string, schedules: WorkflowScheduleDeclaration[]): WorkflowDeclaredSchedule[] {
  if (!Array.isArray(schedules)) {
    throw new TypeError(`Workflow "${name}": schedules must be an array, such as [{ id: 'weekly-digest', cron: '0 8 * * MON' }].`);
  }

  const ids = new Set<string>();
  return schedules.map((declaration) => {
    const id = declaration?.id;
    if (typeof id !== 'string' || !SCHEDULE_ID.test(id)) {
      throw new TypeError(`Invalid schedule id ${JSON.stringify(id)} of workflow "${name}". Use letters, digits, ".", ":", "_" or "-".`);
    }
    if (ids.has(id)) {
      throw new TypeError(`Workflow "${name}" declares schedule "${id}" twice.`);
    }
    ids.add(id);

    const owner = `Schedule "${id}" of workflow "${name}"`;
    const inputFn = typeof declaration.input === 'function';
    let input: unknown = declaration.input;
    if (!inputFn) {
      try {
        input = normalize(declaration.input);
      } catch (error) {
        throw new TypeError(`${owner}: its input is not JSON-serializable: ${(error as Error).message}`);
      }
    }
    return { id, spec: scheduleSpec(owner, declaration, { version: null, inputFn }), input };
  });
}
