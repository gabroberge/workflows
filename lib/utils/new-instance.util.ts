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
  options: { caller: string; now: number; timeout?: Duration; concurrencyKey?: string; parentId?: string; parentClose?: WorkflowParentClose },
): NewWorkflowInstance {
  if (typeof id !== 'string' || id.length === 0) {
    throw new TypeError(`Invalid workflow instance id ${JSON.stringify(id)}. Use a non-empty string, such as \`order-\${orderId}\`.`);
  }

  const json = normalize(input);
  const timeoutMs = options.timeout === undefined ? workflow.timeout : runTimeoutMs(options.timeout, options.caller);
  return {
    id,
    workflow: workflow.name,
    version: workflow.version,
    input: json,
    deadline: timeoutMs === undefined ? null : options.now + timeoutMs,
    parentId: options.parentId ?? null,
    parentClose: options.parentClose ?? null,
    concurrencyKey: concurrencyKey(workflow, json, options),
    now: options.now,
  };
}

/** The instance's key for its workflow's per-key concurrency limit: the start's, else the one `concurrency.key` computes from the input. */
function concurrencyKey(workflow: WorkflowMetadata, input: unknown, options: { caller: string; concurrencyKey?: string }): string | null {
  const { concurrency } = workflow;
  if (options.concurrencyKey !== undefined) {
    if (typeof options.concurrencyKey !== 'string' || options.concurrencyKey.length === 0) {
      throw new TypeError(`${options.caller}: invalid concurrencyKey ${JSON.stringify(options.concurrencyKey)}. Use a non-empty string.`);
    }
    // Unknown here (started by name from a process that doesn't register it): the key stands.
    if (concurrency !== undefined && concurrency?.perKey == null) {
      throw new TypeError(
        `${options.caller}: workflow "${workflow.name}" has no concurrency limit per key, so concurrencyKey would count for nothing. ` +
          'Declare one with @Workflow(name, { concurrency: { limit, key } }).',
      );
    }
    return options.concurrencyKey;
  }
  if (!concurrency?.key) {
    return null;
  }

  let key: unknown;
  try {
    key = concurrency.key(input);
  } catch (error) {
    throw new TypeError(`The concurrency key of workflow "${workflow.name}" threw: ${(error as Error)?.message ?? String(error)}`);
  }
  if (key === null || key === undefined) {
    return null;
  }
  if (typeof key !== 'string' || key.length === 0) {
    throw new TypeError(`The concurrency key of workflow "${workflow.name}" returned ${JSON.stringify(key)}. Return a non-empty string, or null for none.`);
  }
  return key;
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
