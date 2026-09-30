import { WorkflowIdConflictError } from '../errors/workflow-id-conflict.error.js';
import type { WorkflowMetadata } from '../interfaces/workflow-decorator-options.interface.js';
import type { WorkflowInstance, WorkflowParentClose } from '../interfaces/workflow-instance.interface.js';
import type { NewWorkflowInstance } from '../interfaces/workflow-store.interface.js';
import { assertPriority } from '../core/limits/limits.js';
import { canonical } from './canonical.util.js';
import type { Duration } from '../core/time/duration.js';
import { runTimeoutMs } from './run-timeout.util.js';
import { normalize } from './normalize.util.js';

export interface NewInstanceOptions {
  caller: string;
  now: number;
  timeout?: Duration;
  concurrencyKey?: string;
  rateLimitKey?: string;
  priority?: number;
  parentId?: string;
  parentClose?: WorkflowParentClose;
  scheduleId?: string;
  scheduledAt?: number;
}

/** What `WorkflowClient.start()` and `ctx.startChild()` ask the store to create. */
export function newInstance(workflow: WorkflowMetadata, id: unknown, input: unknown, options: NewInstanceOptions): NewWorkflowInstance {
  if (typeof id !== 'string' || id.length === 0) {
    throw new TypeError(`Invalid workflow instance id ${JSON.stringify(id)}. Use a non-empty string, such as \`order-\${orderId}\`.`);
  }
  if (options.priority !== undefined) {
    assertPriority(options.priority, options.caller);
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
    concurrencyKey: keyOf(workflow, json, options.caller, 'concurrency', options.concurrencyKey),
    rateLimitKey: keyOf(workflow, json, options.caller, 'rateLimit', options.rateLimitKey),
    priority: options.priority ?? 0,
    scheduleId: options.scheduleId ?? null,
    scheduledAt: options.scheduledAt ?? null,
    now: options.now,
  };
}

const LIMITS = {
  concurrency: { option: 'concurrencyKey', limit: 'concurrency limit', key: 'concurrency key', declare: '{ concurrency: { limit, key } }' },
  rateLimit: { option: 'rateLimitKey', limit: 'rate limit', key: 'rate limit key', declare: '{ rateLimit: { max, duration, key } }' },
} as const;

/**
 * The instance's key for its workflow's per-key concurrency or rate limit: the start's, else the one the limit's
 * `key` computes from the input.
 */
function keyOf(workflow: WorkflowMetadata, input: unknown, caller: string, kind: keyof typeof LIMITS, given: string | undefined): string | null {
  const limit = workflow[kind];
  const { option, limit: what, key: named, declare } = LIMITS[kind];
  if (given !== undefined) {
    if (typeof given !== 'string' || given.length === 0) {
      throw new TypeError(`${caller}: invalid ${option} ${JSON.stringify(given)}. Use a non-empty string.`);
    }
    // Unknown here (started by name from a process that doesn't register it): the key stands.
    if (limit !== undefined && limit?.perKey == null) {
      throw new TypeError(
        `${caller}: workflow "${workflow.name}" has no ${what} per key, so ${option} would count for nothing. Declare one with @Workflow(name, ${declare}).`,
      );
    }
    return given;
  }
  if (!limit?.key) {
    return null;
  }

  let key: unknown;
  try {
    key = limit.key(input);
  } catch (error) {
    throw new TypeError(`The ${named} of workflow "${workflow.name}" threw: ${(error as Error)?.message ?? String(error)}`);
  }
  if (key === null || key === undefined) {
    return null;
  }
  if (typeof key !== 'string' || key.length === 0) {
    throw new TypeError(`The ${named} of workflow "${workflow.name}" returned ${JSON.stringify(key)}. Return a non-empty string, or null for none.`);
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
