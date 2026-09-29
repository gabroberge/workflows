import type { Duration } from '../utils/duration.util.js';
import type { SerializedWorkflowError } from './serialized-workflow-error.interface.js';
import type { WorkflowSignal } from '../signals/workflow.signal.js';
import type { WorkflowRetryOptions } from './workflow-retry-options.interface.js';

/**
 * What a journaled value looks like after its JSON round-trip: step results,
 * signal payloads and checkpoints come back this way on the first run and on
 * every replay (a `Date` is a string, methods and `undefined` fields are gone,
 * `undefined` in an array is `null`, a `Map` or `Set` is `{}`).
 */
export type Journaled<T> = unknown extends T
  ? T
  : T extends { toJSON(...args: any[]): infer R }
    ? Journaled<R>
    : T extends string | number | boolean | null | undefined | void
      ? T
      : T extends bigint | symbol | ((...args: any[]) => unknown)
        ? never
        : T extends ReadonlyMap<unknown, unknown> | ReadonlySet<unknown>
          ? Record<string, never>
          : T extends readonly unknown[]
            ? { [K in keyof T]: JournaledElement<T[K]> }
            : { [K in keyof T as T[K] extends (...args: any[]) => unknown ? never : K]: Journaled<T[K]> };

/** In arrays, `JSON.stringify` writes `undefined`, functions and symbols as `null`. */
type JournaledElement<T> = T extends undefined | void | symbol | ((...args: any[]) => unknown) ? null : Journaled<T>;

/** Handed to every step (and compensation) attempt. */
export interface WorkflowStepContext<P = unknown> {
  /**
   * `${workflowId}:${stepName}`. Identical for every attempt and every worker
   * that runs this step, so pass it to the API you call (`Idempotency-Key`,
   * a unique column) to make the side effect exactly-once.
   */
  readonly idempotencyKey: string;
  /** 1 for the first attempt. Interrupted attempts (crash, lost lease) count. */
  readonly attempt: number;
  /** Aborted on shutdown, on a lost lease, and on `timeout`/`heartbeatTimeout`. */
  readonly signal: AbortSignal;
  /**
   * The last value passed to `heartbeat(progress)` by an earlier attempt of
   * this step, or `undefined`. Type it by annotating the step function's
   * parameter: `async ({ progress }: WorkflowStepContext<Checkpoint>) => ...`.
   */
  readonly progress: Journaled<P> | undefined;
  /**
   * Proves the step is alive (resets `heartbeatTimeout`). With an argument,
   * the value is also checkpointed to the journal and handed to the next
   * attempt as `progress` if this one dies.
   */
  heartbeat(progress?: P): Promise<void>;
}

export interface WorkflowCompensationContext extends WorkflowStepContext {
  /** Why the workflow is being compensated. */
  readonly reason: SerializedWorkflowError;
}

export interface WorkflowStepOptions<T> {
  /** Default: the module's `retry` (3 attempts, 1s doubling up to 5m). */
  retry?: number | false | WorkflowRetryOptions;
  /** Fail the attempt (retryably) after this long. */
  timeout?: Duration;
  /** Fail the attempt (retryably) if `heartbeat()` is not called this often. */
  heartbeatTimeout?: Duration;
  /**
   * Undo this step, once it completed, if the workflow later fails or is
   * cancelled. Compensations run in reverse order of the `ctx.step()` calls
   * (parallel steps included), each one journaled and retried like a step. Its
   * return value is discarded. A later `ctx.commit()` discards it.
   */
  compensate?: (result: Journaled<T>, ctx: WorkflowCompensationContext) => unknown;
  /** How `compensate` retries. Default: the step's `retry`, else the module's. */
  compensateRetry?: number | false | WorkflowRetryOptions;
}

export interface WaitForSignalOptions<T> {
  /**
   * Correlation key: only signals sent with the same key match (see
   * `WorkflowClient.signal()`). Without one, the wait only matches signals sent
   * without a key.
   */
  key?: string;
  /** Extra filter over the payload. Must be a pure function of it. */
  match?: (payload: Journaled<T>) => boolean;
  /** Resolve with `null` if no matching signal was sent before this deadline. */
  timeout?: Duration;
}

/**
 * The API a workflow's `run()` receives. `run()` executes again from the top every time an instance
 * resumes: https://docs.nestjs.com/reliability/workflows#rules-for-workflow-code.
 */
export interface WorkflowContext {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly version: number;

  /**
   * Runs `fn` once and journals its result; replays return the journaled
   * result without running it. `name` is unique per run and part of the
   * contract with running instances. Don't call `ctx` methods inside `fn`.
   */
  step<T, P = unknown>(
    name: string,
    fn: (ctx: WorkflowStepContext<P>) => T | Promise<T>,
    options?: WorkflowStepOptions<T>,
  ): Promise<Journaled<T>>;
  /** Durable timer. The instance is parked and resumed by any worker after the deadline. */
  sleep(name: string, duration: Duration | { until: Date | number }): Promise<void>;
  /**
   * Durable wait for a signal sent with `WorkflowClient.signal()`, including
   * one sent after the instance started but before it reached this wait.
   * Resolves with the payload, or `null` once `timeout` passes (compare with
   * `=== null` when the payload itself can be falsy).
   */
  waitForSignal<T>(
    name: string,
    signal: WorkflowSignal<T> | string,
    options?: WaitForSignalOptions<T>,
  ): Promise<Journaled<T> | null>;
  /**
   * Journaled `Date.now()`: the same value on every replay. Helpers are
   * numbered in call order, so call them from sequential code, not from
   * parallel branches that reach them in a different order on a replay.
   */
  now(): number;
  /** Journaled `Math.random()`. */
  random(): number;
  /** Journaled `crypto.randomUUID()`. */
  uuid(): string;
  /**
   * Point of no return. Compensations of the steps called before it are
   * discarded: a later failure or cancel runs only the compensations of steps
   * called after it. Journaled under `name`, so it replays like a step and
   * shows in `getStatus(id, { journal: true })`.
   */
  commit(name: string): void;
  /**
   * Sets the instance's custom status, a JSON value of up to 16 KiB that the outside world
   * reads as `WorkflowClient.getStatus(id).customStatus` (a progress report, the stage an
   * order is in). It is written with the instance's next journal write, suspension or end,
   * and emitted as a `custom-status` event then. Every execution replays `run()`, and with it
   * the `setStatus()` calls, so the status needs no journal entry of its own and is never
   * read back by the workflow. `undefined` clears it (`null`).
   */
  setStatus(status: unknown): void;
  /** Fail the instance on purpose. Compensations run (only those after a `commit()`). */
  fail(message: string): never;
}
