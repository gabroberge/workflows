import { Inject, Injectable, type Type } from '@nestjs/common';
import type { ScheduleRecord } from '../core/interfaces/schedule-store.interface.js';
import type { Scheduler } from '../core/scheduling/scheduler.js';
import { assertScheduleId, nextOccurrences, parseSchedule } from '../core/scheduling/schedule-spec.js';
import { systemClock } from '../core/time/clock.js';
import { WorkflowNotFoundError } from '../errors/workflow-not-found.error.js';
import { WorkflowStateError } from '../errors/workflow-state.error.js';
import type { WorkflowClock } from '../interfaces/workflow-clock.interface.js';
import type { WorkflowStartResult } from '../interfaces/workflow-client.interface.js';
import type {
  UpsertWorkflowScheduleOptions,
  WorkflowSchedule,
  WorkflowScheduleListFilter,
  WorkflowSchedulePreviewOptions,
  WorkflowSchedulePreviewSpec,
} from '../interfaces/workflow-schedule.interface.js';
import type { WorkflowsModuleOptions } from '../interfaces/workflows-module-options.interface.js';
import { normalize } from '../utils/normalize.util.js';
import { scheduleSpec, type WorkflowScheduleSpec } from '../utils/schedule-spec.util.js';
import { WORKFLOWS_MODULE_OPTIONS } from '../workflows.module-definition.js';
import { WorkflowRegistry } from './workflow-registry.service.js';
import { WorkflowScheduler } from './workflow-scheduler.service.js';
import { WorkflowWorker } from './workflow-worker.service.js';

/**
 * Schedules that start workflow instances: a cron expression, an interval or an RFC 5545 rule, in a time zone.
 * Declare them with `@Workflow(name, { schedules })`, or save them at runtime with `upsert()`; every worker that
 * runs the workflow starts their occurrences, each exactly once. Also `WorkflowClient.schedules`.
 */
@Injectable()
export class WorkflowSchedules {
  private readonly clock: WorkflowClock;

  constructor(
    @Inject(WORKFLOWS_MODULE_OPTIONS) options: WorkflowsModuleOptions,
    private readonly registry: WorkflowRegistry,
    private readonly scheduler: WorkflowScheduler,
    private readonly worker: WorkflowWorker,
  ) {
    this.clock = options.clock ?? systemClock;
  }

  /** The core's engine of schedules, over the registered store's. */
  private get engine(): Scheduler<WorkflowScheduleSpec> {
    return this.scheduler.engine;
  }

  /**
   * Creates or replaces the schedule with this id, and returns it. A schedule whose timing (`cron`, `every`,
   * `rrule`, `tz`, `startAt`, `endAt`) changes starts over from now, with its first occurrence after now; one
   * whose timing doesn't keeps its next occurrence, whatever else changes. Its runs, and whether it is paused,
   * carry over. Throws `WorkflowStateError` for a schedule a workflow declares (change it in the code), and a
   * `TypeError` for invalid options.
   */
  async upsert<W>(id: string, schedule: UpsertWorkflowScheduleOptions<W>): Promise<WorkflowSchedule> {
    assertScheduleId(id);
    const owner = `Schedule "${id}"`;
    if (schedule === null || typeof schedule !== 'object') {
      throw new TypeError(`${owner}: expected options with workflow and cron, every or rrule.`);
    }
    const workflow = this.registry.resolve(schedule.workflow as Type<unknown> | string, schedule.version);
    const spec = scheduleSpec(owner, schedule, { version: schedule.version ?? null, inputFn: false });
    if (typeof schedule.input === 'function') {
      throw new TypeError(
        `${owner}: upsert() stores its input, so it can't be a function. Pass a value; a declared schedule (@Workflow(name, { schedules })) can compute it.`,
      );
    }
    let input: unknown;
    try {
      input = normalize(schedule.input) ?? null;
    } catch (error) {
      throw new TypeError(`${owner}: its input is not JSON-serializable: ${(error as Error).message}`);
    }

    const saved = await this.engine.save(
      id,
      (current) => {
        if (current?.declared) {
          throw declaredError(current);
        }
        return this.engine.changed(current, { target: workflow.name, declared: false, spec, payload: input });
      },
      { replacing: true },
    );
    this.worker.kick();
    return this.scheduler.view(saved!);
  }

  /** The schedule, or `null` for an unknown id. */
  async get(id: string): Promise<WorkflowSchedule | null> {
    const record = await this.engine.read(id);
    return record ? this.scheduler.view(record) : null;
  }

  /** Schedules by id, all or a workflow's; at most `limit` (default 100). */
  async list(filter: WorkflowScheduleListFilter = {}): Promise<WorkflowSchedule[]> {
    const limit = filter.limit ?? 100;
    const offset = filter.offset ?? 0;
    if (!Number.isSafeInteger(limit) || limit < 0 || !Number.isSafeInteger(offset) || offset < 0) {
      throw new TypeError(`schedules.list(): limit (${limit}) and offset (${offset}) must be non-negative integers.`);
    }
    if (limit === 0) {
      return [];
    }

    // Read as stored, then decoded one by one: one whose input no codec can read any more doesn't hide the others.
    const records = await this.engine.store.listSchedules({ limit, offset, ...(filter.workflow !== undefined ? { target: filter.workflow } : {}) });
    return Promise.all(records.map((record) => this.scheduler.readableView(record)));
  }

  /**
   * Deletes the schedule, and returns whether it existed. The instances it started keep running. Throws
   * `WorkflowStateError` for a schedule a workflow declares: remove it from the code, or `pause()` it.
   */
  async remove(id: string): Promise<boolean> {
    const record = await this.engine.store.getSchedule(id);
    if (!record) {
      return false;
    }
    if (record.declared) {
      throw declaredError(record);
    }
    return this.engine.store.deleteSchedule(id);
  }

  /**
   * Stops starting occurrences until `resume()`, at once: a worker that is starting the schedule's occurrences at
   * that moment starts only those it had already decided on. The instances it started keep running. A declared
   * schedule stays paused across deploys. Throws `WorkflowNotFoundError` for an unknown id.
   */
  async pause(id: string): Promise<WorkflowSchedule> {
    const saved = await this.engine.pause(id);
    if (!saved) {
      throw notFound(id);
    }
    return this.scheduler.readableView(saved);
  }

  /**
   * Starts occurrences again, from the first one after now: those due while it was paused are passed over,
   * whatever `missed` says. Throws `WorkflowNotFoundError` for an unknown id.
   */
  async resume(id: string): Promise<WorkflowSchedule> {
    const saved = await this.engine.resume(id);
    if (!saved) {
      throw notFound(id);
    }
    this.worker.kick();
    return this.scheduler.readableView(saved);
  }

  /**
   * Starts an instance of the schedule now, whatever its `overlap`, even while it is paused, and outside its
   * `limit`; its id is `<schedule id>@<ISO time of now>`, and it counts as the schedule's for the overlap of the
   * occurrences after it. Throws `WorkflowNotFoundError` for an unknown id.
   */
  async trigger(id: string): Promise<WorkflowStartResult> {
    const record = await this.engine.read(id);
    if (!record) {
      throw notFound(id);
    }

    const spec = record.spec as WorkflowScheduleSpec;
    const declared = record.declared ? this.scheduler.declaration(id) : undefined;
    if (spec.inputFn && !declared) {
      throw new TypeError(
        `Schedule "${id}" computes its input with a function, which this process doesn't declare: trigger it from one that registers workflow "${record.target}".`,
      );
    }
    const workflow = this.registry.resolve(record.target, spec.version ?? undefined);
    const now = this.clock.now();
    const { instance, created } = (await this.scheduler.startOccurrence(record, spec, declared, workflow, now, now, true))!;
    if (created) {
      this.worker.kick();
    }
    return { id: instance.id, workflow: instance.workflow, version: instance.version, created, status: instance.status };
  }

  /**
   * The times (epoch milliseconds) of the next occurrences after `from` (default: now), at most `count` (default
   * 10), as a schedule saved at `from` would start them: of a stored schedule (by id, within what its `limit` has
   * left, paused or not), or of a spec, which isn't saved. Throws `WorkflowNotFoundError` for an unknown id.
   */
  async preview(schedule: string | WorkflowSchedulePreviewSpec, options: WorkflowSchedulePreviewOptions = {}): Promise<number[]> {
    const count = options.count ?? 10;
    if (!Number.isSafeInteger(count) || count < 1 || count > 1_000) {
      throw new TypeError(`schedules.preview(): count (${count}) must be an integer from 1 to 1000.`);
    }
    const from = options.from === undefined ? this.clock.now() : options.from instanceof Date ? options.from.getTime() : options.from;
    if (typeof from !== 'number' || !Number.isFinite(from)) {
      throw new TypeError(`schedules.preview(): invalid from ${String(options.from)}. Pass a valid Date or a timestamp in milliseconds.`);
    }

    if (typeof schedule !== 'string') {
      const { cron, every, rrule, tz, startAt, endAt, limit } = schedule;
      return nextOccurrences(parseSchedule({ cron, every, rrule, tz, startAt, endAt, limit }, 'schedules.preview()'), { from, count }, 'schedules.preview()');
    }

    const record = await this.engine.store.getSchedule(schedule);
    if (!record) {
      throw notFound(schedule);
    }
    return nextOccurrences(record.spec as WorkflowScheduleSpec, { from, count, runs: this.engine.progress(record).runs }, 'schedules.preview()');
  }
}

function notFound(id: string): WorkflowNotFoundError {
  return new WorkflowNotFoundError(`No workflow schedule with id "${id}".`);
}

function declaredError(record: ScheduleRecord): WorkflowStateError {
  return new WorkflowStateError(
    `Schedule "${record.id}" is declared by workflow "${record.target}" (@Workflow(name, { schedules })): change or remove it there. ` +
      'pause(), resume() and trigger() work on it.',
  );
}
