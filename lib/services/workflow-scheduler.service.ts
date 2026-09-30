import { Inject, Injectable, Logger, type OnApplicationBootstrap } from '@nestjs/common';
import type { DeclaredSchedule, DueOccurrence, ScheduleProduction, ScheduleSkip } from '../core/interfaces/scheduler-options.interface.js';
import type { ScheduleRecord } from '../core/interfaces/schedule-store.interface.js';
import { occurrenceId } from '../core/scheduling/schedule-spec.js';
import { Scheduler } from '../core/scheduling/scheduler.js';
import { systemClock } from '../core/time/clock.js';
import { WorkflowStateError } from '../errors/workflow-state.error.js';
import { WorkflowEvents } from '../events/workflow-events.service.js';
import type { WorkflowMetadata } from '../interfaces/workflow-decorator-options.interface.js';
import type { WorkflowClock } from '../interfaces/workflow-clock.interface.js';
import type { WorkflowInstance, WorkflowStatus } from '../interfaces/workflow-instance.interface.js';
import type { WorkflowSchedule } from '../interfaces/workflow-schedule.interface.js';
import type { WorkflowsModuleOptions } from '../interfaces/workflows-module-options.interface.js';
import type { EncodedWorkflowStore } from '../storage/encoded-workflow.store.js';
import { ENGINE_STORE, PAYLOAD_CODECS, WorkflowStorage } from '../storage/workflow.storage.js';
import { newInstance } from '../utils/new-instance.util.js';
import type { WorkflowScheduleSpec } from '../utils/schedule-spec.util.js';
import { workflowScheduleStore } from '../utils/schedule-store.util.js';
import { WORKFLOWS_MODULE_OPTIONS } from '../workflows.module-definition.js';
import { WorkflowRegistry } from './workflow-registry.service.js';

/** The unfinished instances a schedule's overlap looks at, at most. */
const RUNNING_PAGE = 100;
const UNFINISHED: WorkflowStatus[] = ['pending', 'running', 'suspended', 'compensating'];

/**
 * @internal Workflows' schedules on the core's `Scheduler`: this process's workflows are its targets, their declared
 * schedules its declarations, and an occurrence starts an instance. The `Scheduler` keeps the declared ones in step
 * with the code and starts the occurrences that are due, for the worker; `WorkflowSchedules` is the public side.
 */
@Injectable()
export class WorkflowScheduler implements OnApplicationBootstrap {
  private readonly logger = new Logger('Workflows');
  private readonly clock: WorkflowClock;
  /** The schedules' engine, over the registered store's schedules. */
  readonly engine: Scheduler<WorkflowScheduleSpec>;
  private declarations?: Map<string, DeclaredSchedule<WorkflowScheduleSpec>>;

  constructor(
    private readonly storage: WorkflowStorage,
    @Inject(WORKFLOWS_MODULE_OPTIONS) options: WorkflowsModuleOptions,
    private readonly registry: WorkflowRegistry,
    private readonly events: WorkflowEvents,
  ) {
    this.clock = options.clock ?? systemClock;
    this.engine = new Scheduler<WorkflowScheduleSpec>({
      // Read at each call, never in the constructor: sources register while providers are created.
      store: workflowScheduleStore(() => storage.source),
      clock: this.clock,
      codecs: storage[PAYLOAD_CODECS],
      payloadContext: (schedule) => ({ field: 'input', schedule }),
      targets: () => registry.names(),
      declared: () => this.declared(),
      fire: (occurrence) => this.fire(occurrence),
      running: (schedule) => this.running(schedule.id),
      cancel: (id, reason, now) => this.store.requestCancel(id, { reason, now, terminate: false }),
      skipped: (schedule, skip, now) => this.skipped(schedule, skip, now),
      conflictError: (message) => new WorkflowStateError(message),
      labels: { target: (name) => `workflow "${name}"`, targets: 'workflows', payload: 'input', upsert: 'WorkflowSchedules.upsert()' },
      logger: this.logger,
    });
  }

  /** Read at each call, never in the constructor: sources register while providers are created. */
  private get store(): EncodedWorkflowStore {
    return this.storage[ENGINE_STORE];
  }

  async onApplicationBootstrap(): Promise<void> {
    // A definition error (an id two workflows declare) fails the startup; the store's errors don't.
    this.registry.schedules();
    try {
      await this.engine.synced();
    } catch (error) {
      this.logger.error('Saving the schedules the workflows declare failed; the worker tries again before it starts any.', error as Error);
    }
  }

  /** @internal Called by the worker: starts the due occurrences of the schedules of the workflows it runs. */
  produce(owner: string, leaseMs: number): Promise<ScheduleProduction> {
    return this.engine.produce(owner, leaseMs);
  }

  /** The declaration of a schedule the workflows this process runs declare, or `undefined`. */
  declaration(id: string): DeclaredSchedule<WorkflowScheduleSpec> | undefined {
    return this.declared().get(id);
  }

  /**
   * Starts the instance of an occurrence (or of a trigger, `at` being now): the one `create()` stored, and whether it
   * created it. An input or a key that can't be computed skips the occurrence (`null`), logged: retrying would fail
   * the same way. With `rethrow`, that throws instead.
   */
  async startOccurrence(
    record: ScheduleRecord,
    spec: WorkflowScheduleSpec,
    declared: DeclaredSchedule<WorkflowScheduleSpec> | undefined,
    workflow: WorkflowMetadata,
    at: number,
    now: number,
    rethrow = false,
  ): Promise<{ instance: WorkflowInstance; created: boolean } | null> {
    const id = occurrenceId(record.id, at);
    let data;
    try {
      // A declared schedule's input is its code's (the stored one is for reading), an upserted one's is stored.
      const source = declared ? declared.payload : record.payload;
      const input = typeof source === 'function' ? (source as (occurrence: { id: string; at: number }) => unknown)({ id: record.id, at }) : source;
      data = newInstance(workflow, id, input ?? undefined, {
        caller: `Schedule "${record.id}"`,
        now,
        priority: spec.priority === 0 ? undefined : spec.priority,
        scheduleId: record.id,
        scheduledAt: at,
      });
    } catch (error) {
      if (rethrow) {
        throw error;
      }
      this.logger.error(`Schedule "${record.id}" couldn't start its occurrence of ${new Date(at).toISOString()}: ${(error as Error)?.message ?? String(error)}`);
      return null;
    }

    const stored = await this.store.create(data);
    if (!stored.created && (stored.instance.workflow !== data.workflow || stored.instance.scheduleId !== record.id)) {
      this.logger.warn(`Schedule "${record.id}" didn't start its occurrence of ${new Date(at).toISOString()}: instance "${id}" exists and isn't the schedule's.`);
    }
    return stored;
  }

  /** How `WorkflowSchedules` shows a stored schedule. */
  view(record: ScheduleRecord): WorkflowSchedule {
    const spec = record.spec as WorkflowScheduleSpec;
    const { nextAt, runs, bufferedAt } = this.engine.progress(record);
    return {
      id: record.id,
      workflow: record.target,
      version: spec.version,
      declared: record.declared,
      ...(spec.cron !== undefined ? { cron: spec.cron } : {}),
      ...(spec.every !== undefined ? { every: spec.every } : {}),
      ...(spec.rrule !== undefined ? { rrule: spec.rrule } : {}),
      tz: spec.tz,
      startAt: spec.startAt,
      endAt: spec.endAt,
      limit: spec.limit,
      missed: spec.missed,
      overlap: spec.overlap,
      priority: spec.priority,
      ...(spec.inputFn || record.payload === null ? {} : { input: record.payload }),
      paused: record.paused,
      nextAt,
      runs,
      bufferedAt,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
    };
  }

  /** How `WorkflowSchedules` shows a schedule as stored: without its input, if no codec can read it any more. */
  async readableView(record: ScheduleRecord): Promise<WorkflowSchedule> {
    return this.view(await this.engine.readable(record));
  }

  /** The schedules the workflows this process runs declare (each name's highest registered version's), for the `Scheduler`. */
  private declared(): Map<string, DeclaredSchedule<WorkflowScheduleSpec>> {
    this.declarations ??= new Map(
      [...this.registry.schedules()].map(([id, schedule]) => [
        id,
        { id, target: schedule.workflow, version: this.registry.latest(schedule.workflow)!.version, spec: schedule.spec, payload: schedule.input },
      ]),
    );
    return this.declarations;
  }

  private async fire({ schedule, spec, declared, at, now }: DueOccurrence<WorkflowScheduleSpec>): Promise<{ created: boolean } | null> {
    const workflow = this.registry.resolve(schedule.target, spec.version ?? undefined);
    const started = await this.startOccurrence(schedule, spec, declared, workflow, at, now);
    return started && { created: started.created };
  }

  /** The unfinished instances the schedule started. */
  private async running(schedule: string): Promise<string[]> {
    // Read as stored: only the ids count, and an instance no codec can read any more still runs.
    const instances = await this.store.inner.list({ scheduleId: schedule, status: UNFINISHED, limit: RUNNING_PAGE, offset: 0 });
    return instances.map((instance) => instance.id);
  }

  private skipped(schedule: ScheduleRecord, skip: ScheduleSkip, now: number): void {
    const workflow = this.registry.resolve(schedule.target, (schedule.spec as WorkflowScheduleSpec).version ?? undefined);
    this.events.emit({ type: 'schedule-skipped', id: schedule.id, workflow: workflow.name, version: workflow.version, at: now, ...skip });
  }
}
