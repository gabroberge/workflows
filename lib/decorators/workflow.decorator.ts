import { applyDecorators, Injectable, SetMetadata } from '@nestjs/common';
import { runTimeoutMs, toMs } from '../utils/duration.util.js';
import { WORKFLOW_METADATA } from '../workflows.constants.js';
import type {
  WorkflowConcurrency,
  WorkflowConcurrencyMetadata,
  WorkflowMetadata,
  WorkflowDecoratorOptions,
  WorkflowRateLimit,
  WorkflowRateLimitMetadata,
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
  const concurrency = options.concurrency === undefined ? null : concurrencyOf(name, options.concurrency);
  const rateLimit = options.rateLimit === undefined ? null : rateLimitOf(name, options.rateLimit);
  return applyDecorators(Injectable(), SetMetadata(WORKFLOW_METADATA, { name, version, timeout, concurrency, rateLimit } satisfies WorkflowMetadata));
}

function concurrencyOf(name: string, concurrency: WorkflowConcurrency | WorkflowConcurrency[]): WorkflowConcurrencyMetadata {
  const limits = Array.isArray(concurrency) ? concurrency : [concurrency];
  if (limits.length === 0 || limits.length > 2) {
    throw new TypeError(`Workflow "${name}" has ${limits.length} concurrency limits. Give it one, or two: one without a key and one with.`);
  }

  const resolved: WorkflowConcurrencyMetadata = { limit: null, perKey: null };
  for (const { limit, key } of limits) {
    if (!Number.isSafeInteger(limit) || limit < 1) {
      throw new TypeError(`Invalid concurrency limit ${JSON.stringify(limit)} for workflow "${name}". Use a positive integer.`);
    }
    if (key !== undefined && typeof key !== 'function') {
      throw new TypeError(`Invalid concurrency key for workflow "${name}". Use a function of the input, such as (order) => order.customerId.`);
    }

    const slot = key === undefined ? 'limit' : 'perKey';
    if (resolved[slot] !== null) {
      throw new TypeError(`Workflow "${name}" has two concurrency limits ${key === undefined ? 'without' : 'with'} a key. Give it at most one of each.`);
    }
    resolved[slot] = limit;
    if (key) {
      resolved.key = key;
    }
  }
  return resolved;
}

function rateLimitOf(name: string, rateLimit: WorkflowRateLimit | WorkflowRateLimit[]): WorkflowRateLimitMetadata {
  const limits = Array.isArray(rateLimit) ? rateLimit : [rateLimit];
  if (limits.length === 0 || limits.length > 2) {
    throw new TypeError(`Workflow "${name}" has ${limits.length} rate limits. Give it one, or two: one without a key and one with.`);
  }

  const resolved: WorkflowRateLimitMetadata = { limit: null, perKey: null };
  for (const { max, duration, key } of limits) {
    if (!Number.isSafeInteger(max) || max < 1) {
      throw new TypeError(`Invalid rate limit max ${JSON.stringify(max)} for workflow "${name}". Use a positive integer.`);
    }
    let ms: number;
    try {
      ms = toMs(duration);
    } catch {
      ms = 0;
    }
    if (!(ms > 0)) {
      throw new TypeError(`Invalid rate limit duration ${JSON.stringify(duration)} for workflow "${name}". Use a positive duration, such as "1m".`);
    }
    if (key !== undefined && typeof key !== 'function') {
      throw new TypeError(`Invalid rate limit key for workflow "${name}". Use a function of the input, such as (order) => order.customerId.`);
    }

    const slot = key === undefined ? 'limit' : 'perKey';
    if (resolved[slot] !== null) {
      throw new TypeError(`Workflow "${name}" has two rate limits ${key === undefined ? 'without' : 'with'} a key. Give it at most one of each.`);
    }
    resolved[slot] = { max, duration: ms };
    if (key) {
      resolved.key = key;
    }
  }
  return resolved;
}
