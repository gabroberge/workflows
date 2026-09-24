import type { ConfigurableModuleAsyncOptions } from '@nestjs/common';
import type { WorkflowClock } from './workflow-clock.interface.js';
import type { Duration } from '../utils/duration.util.js';
import type { WorkflowRetryOptions } from './workflow-retry-options.interface.js';

export interface WorkflowWorkerOptions {
  /**
   * Run the background polling loop. Default true. With false the process can
   * still start, signal and inspect workflows, and `WorkflowWorker.drain()`
   * runs due instances on demand (tests, scripts, a separate worker process).
   */
  enabled?: boolean;
  /** Shown as `leaseOwner`. Default `hostname:pid:random`. */
  id?: string;
  /** Instances executed at once by this process. Default 10. */
  concurrency?: number;
  /** How often to look for due instances. Default 1s; local starts and signals wake the loop at once. */
  pollInterval?: Duration;
  /** How long a claim is valid without a heartbeat. Default 30s. */
  leaseDuration?: Duration;
  /** How often a running instance extends its lease. Default a third of `leaseDuration`. */
  heartbeatInterval?: Duration;
  /** How long shutdown waits for running executions before abandoning them. Default 10s. */
  shutdownTimeout?: Duration;
}

/** What `forRoot()` takes, and what `forRootAsync()`'s factory returns. */
export interface WorkflowsModuleOptions {
  /** Default: the system clock. Pass a `ManualWorkflowClock` in tests. */
  clock?: WorkflowClock;
  /** `false` is shorthand for `{ enabled: false }`. */
  worker?: WorkflowWorkerOptions | false;
  /** Default step retry: 3 attempts, 1s doubling up to 5m, no jitter. */
  retry?: number | false | WorkflowRetryOptions;
  /**
   * With `NODE_ENV=production`, startup fails when no `WorkflowStore` is registered, because
   * the in-memory default loses every running instance on restart. Set this to run on it
   * anyway (a demo, a single-process tool whose workflows may be lost).
   */
  allowInMemoryStorage?: boolean;
}

/** Implemented by a `forRootAsync({ useClass })` class: Nest calls `createWorkflowsOptions()`. */
export interface WorkflowsOptionsFactory {
  createWorkflowsOptions(): WorkflowsModuleOptions | Promise<WorkflowsModuleOptions>;
}

/** What `forRootAsync()` takes: `imports`, `inject` and `useFactory`, `useClass` or `useExisting`, and `isGlobal`. */
export type WorkflowsModuleAsyncOptions = ConfigurableModuleAsyncOptions<WorkflowsModuleOptions, 'createWorkflowsOptions'> & {
  /** Default true. */
  isGlobal?: boolean;
};
