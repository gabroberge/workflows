import { WorkflowIdConflictError } from '../errors/workflow-id-conflict.error.js';
import type { WorkflowMetadata } from '../interfaces/workflow-decorator-options.interface.js';
import type { WorkflowInstance, WorkflowParentClose } from '../interfaces/workflow-instance.interface.js';
import type { NewWorkflowInstance } from '../interfaces/workflow-store.interface.js';
import { canonical } from './canonical.util.js';
import { runTimeoutMs, type Duration } from './duration.util.js';
import { normalize } from './normalize.util.js';

/** What `WorkflowClient.start()` and `ctx.startChild()` ask the store to create. */
export function newInstance(
  workflow: WorkflowMetadata,
  id: unknown,
  input: unknown,
  options: { caller: string; now: number; timeout?: Duration; parentId?: string; parentClose?: WorkflowParentClose },
): NewWorkflowInstance {
  if (typeof id !== 'string' || id.length === 0) {
    throw new TypeError(`Invalid workflow instance id ${JSON.stringify(id)}. Use a non-empty string, such as \`order-\${orderId}\`.`);
  }

  const timeoutMs = options.timeout === undefined ? workflow.timeout : runTimeoutMs(options.timeout, options.caller);
  return {
    id,
    workflow: workflow.name,
    version: workflow.version,
    input: normalize(input),
    deadline: timeoutMs === undefined ? null : options.now + timeoutMs,
    parentId: options.parentId ?? null,
    parentClose: options.parentClose ?? null,
    now: options.now,
  };
}

/**
 * Throws `WorkflowIdConflictError` when the instance `create()` found under the id isn't the one
 * asked for: another workflow, another input (when `input`), or another parent (when `parent`).
 */
export function assertSameInstance(existing: WorkflowInstance, wanted: NewWorkflowInstance, compare: { input: boolean; parent: boolean }): void {
  if (existing.workflow !== wanted.workflow) {
    throw new WorkflowIdConflictError(`Instance "${wanted.id}" already exists for workflow "${existing.workflow}", not "${wanted.workflow}".`);
  }
  if (compare.parent && existing.parentId !== (wanted.parentId ?? null)) {
    const parent = existing.parentId === null ? 'no parent' : `parent "${existing.parentId}"`;
    throw new WorkflowIdConflictError(`Instance "${wanted.id}" of "${wanted.workflow}" already exists with ${parent}.`);
  }
  // A store may read an `undefined` input back as `null`.
  if (compare.input && canonical(existing.input ?? null) !== canonical(wanted.input ?? null)) {
    throw new WorkflowIdConflictError(`Instance "${wanted.id}" of "${wanted.workflow}" already exists with a different input.`);
  }
}
