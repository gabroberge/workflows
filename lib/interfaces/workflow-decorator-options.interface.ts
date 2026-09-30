import type { ConcurrencyLimit, RateLimit, RateWindow, ResolvedConcurrency, ResolvedRateLimit } from '../core/interfaces/limits.interface.js';
import type { Duration } from '../core/time/duration.js';
import type { ScheduleSpec } from '../utils/schedule-spec.util.js';
import type { WorkflowScheduleDeclaration } from './workflow-schedule.interface.js';

export interface WorkflowMetadata {
  name: string;
  version: number;
  /** In milliseconds. */
  timeout?: number;
  /** `null`: no limit. `undefined`: not known here (a workflow started by name that this process doesn't register). */
  concurrency?: WorkflowConcurrencyMetadata | null;
  /** `null`: no limit. `undefined`: not known here, as for `concurrency`. */
  rateLimit?: WorkflowRateLimitMetadata | null;
  /** `@Workflow(name, { schedules })`, validated. */
  schedules?: WorkflowDeclaredSchedule[];
}

/** One of `@Workflow(name, { schedules })`, validated: its spec (as the store keeps it) and its input. */
export interface WorkflowDeclaredSchedule {
  id: string;
  spec: ScheduleSpec;
  /** A JSON value (normalized), or a function of the occurrence. */
  input: unknown;
}

/** `@Workflow(name, { concurrency })`, validated. */
export type WorkflowConcurrencyMetadata = ResolvedConcurrency;

/** `@Workflow(name, { rateLimit })`, validated. */
export type WorkflowRateLimitMetadata = ResolvedRateLimit;

/** At most `max` executions start per window of `duration` milliseconds. */
export type WorkflowRateWindow = RateWindow;

/** One limit of `@Workflow(name, { concurrency })`. */
export interface WorkflowConcurrency extends ConcurrencyLimit {
  /** A positive integer: how many instances may run at once, of the workflow, or with `key`, per key. */
  limit: number;
  /**
   * Makes `limit` a limit per key: the key of an instance, computed from its (JSON) input when
   * it starts, such as `(order) => order.customerId`. `null` or `undefined` leaves the instance
   * out of every key. `start()`'s `concurrencyKey` option overrides it.
   */
  key?: (input: any) => string | null | undefined;
}

/** One limit of `@Workflow(name, { rateLimit })`. */
export interface WorkflowRateLimit extends RateLimit {
  /** A positive integer: how many executions may start per `duration`, of the workflow, or with `key`, per key. */
  max: number;
  /** The window, such as `'1m'`. */
  duration: Duration;
  /**
   * Makes the limit a limit per key: the key of an instance, computed from its (JSON) input when it starts, such
   * as `(order) => order.customerId`. `null` or `undefined` leaves the instance out of every key. `start()`'s
   * `rateLimitKey` option overrides it.
   */
  key?: (input: any) => string | null | undefined;
}

export interface WorkflowDecoratorOptions {
  /**
   * Bump when a change would not replay against journals written by the
   * previous code (renamed, removed, reordered or inserted steps). Keep the old
   * class registered until its instances finish. Default 1.
   */
  version?: number;
  /**
   * How long an instance may run, from its start to its end, sleeps and waits included. Once it
   * passes, the instance stops at its next `ctx` call (a step that is running finishes first), runs
   * its compensations and ends as `failed` with a `WorkflowTimeoutError`. The deadline is stored
   * with the instance, so it holds across restarts, and a parked instance wakes for it. `start()`'s
   * `timeout` option overrides it. Default: none.
   */
  timeout?: Duration;
  /**
   * How many instances may run at once, across every worker: `{ limit }` for the workflow,
   * `{ limit, key }` per key (at most one instance per customer: `{ limit: 1, key: (order) =>
   * order.customerId }`), or one of each in an array. An instance holds a slot while an
   * execution runs it (compensations included), not while it sleeps or waits for a signal: a
   * limit protects what the steps call, and an instance parked for days doesn't hold up the
   * others. Instances past a limit stay due and queue, most overdue first; one busy key never
   * holds back the others. Applies to every version of the workflow, with the highest
   * registered version's limits. Default: none.
   */
  concurrency?: WorkflowConcurrency | WorkflowConcurrency[];
  /**
   * How many executions may start per window, across every worker: `{ max, duration }` for the workflow,
   * `{ max, duration, key }` per key (at most 5 a minute per customer: `{ max: 5, duration: '1m', key: (order) =>
   * order.customerId }`), or one of each in an array. Every execution counts: an instance's first run and each
   * resumption (after a sleep, a signal, a retry's backoff, a cancel), because each can call what the limit
   * protects. A window starts with the first execution after the previous window ended. Instances past a limit stay
   * due and wait, in priority order; one busy key never holds back the others. Applies to every version of the
   * workflow, with the highest registered version's limits. Default: none.
   */
  rateLimit?: WorkflowRateLimit | WorkflowRateLimit[];
  /**
   * Starts instances on a schedule: a `cron` expression, an interval (`every`) or an RFC 5545 `rrule`, in a
   * time zone (`tz`), with an `id` unique across the application. Kept in step with the code, also in a rolling
   * deploy: a process saves and confirms the schedules its highest registered version of the workflow declares, at
   * startup and every minute, and removes one its code doesn't declare, of any workflow, once no process whose code
   * declares it has confirmed it for five minutes (a process whose code declares it saves it again as it starts);
   * it leaves a newer version's declaration as it is while a process of that code runs. Each occurrence starts one
   * instance, whose id is `<schedule id>@<ISO time of the occurrence>`, however many workers there are;
   * `WorkflowSchedules` pauses, resumes, triggers and lists them. Default: none.
   */
  schedules?: WorkflowScheduleDeclaration[];
}
