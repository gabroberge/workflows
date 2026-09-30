import type { ScheduleSpec } from '../core/interfaces/schedule.interface.js';
import { assertPriority } from '../core/limits/limits.js';
import { parseSchedule } from '../core/scheduling/schedule-spec.js';
import type { WorkflowScheduleOptions } from '../interfaces/workflow-schedule.interface.js';

/** A workflow schedule's spec as the engine stores it (`WorkflowScheduleRecord.spec`): the core's, and what it starts. */
export interface WorkflowScheduleSpec extends ScheduleSpec {
  /** The priority of the instances it starts: `0` for none. */
  priority: number;
  /** The pinned version, or `null`: the highest the starting worker registers. */
  version: number | null;
  /** The input is a function of the occurrence (a declared schedule): only a worker that declares it can compute it. */
  inputFn: boolean;
}

/** Checks a schedule's options and returns its spec; throws a `TypeError` that names `owner` for anything invalid. */
export function scheduleSpec(owner: string, options: WorkflowScheduleOptions, extra: { version: number | null; inputFn: boolean }): WorkflowScheduleSpec {
  const spec = parseSchedule(options, owner);
  const priority = options.priority ?? 0;
  assertPriority(priority, owner);
  return { ...spec, priority, ...extra };
}
