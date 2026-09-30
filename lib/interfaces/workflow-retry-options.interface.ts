import type { RetrySettings } from '../core/interfaces/retry-settings.interface.js';

/**
 * How a step (or compensation) retries. `retry: 5` means `{ attempts: 5 }`;
 * `retry: false` means a single attempt. Workflows default to 3 attempts, a
 * 1s first delay doubling up to 5m, no jitter. Every retry parks the instance
 * durably: the wait survives restarts. A `NonRetryableStepError` never retries.
 */
export interface WorkflowRetryOptions extends RetrySettings {}
