import { randomUUID } from 'node:crypto';
import { Inject, Injectable, Logger, type OnApplicationBootstrap } from '@nestjs/common';
import { WorkflowStateError } from '../errors/workflow-state.error.js';
import { WorkflowEvents } from '../events/workflow-events.service.js';
import type { WorkflowDeclaredSchedule, WorkflowMetadata } from '../interfaces/workflow-decorator-options.interface.js';
import type { WorkflowClock } from '../interfaces/workflow-clock.interface.js';
import type { WorkflowInstance, WorkflowStatus } from '../interfaces/workflow-instance.interface.js';
import type { WorkflowSchedule } from '../interfaces/workflow-schedule.interface.js';
import type { WorkflowScheduleRecord, WorkflowScheduleSave } from '../interfaces/workflow-store.interface.js';
import type { WorkflowsModuleOptions } from '../interfaces/workflows-module-options.interface.js';
import type { EncodedWorkflowStore } from '../storage/encoded-workflow.store.js';
import { ENGINE_STORE, WorkflowStorage } from '../storage/workflow.storage.js';
import { canonical } from '../utils/canonical.util.js';
import { systemClock } from '../utils/clock.util.js';
import { newInstance } from '../utils/new-instance.util.js';
import { latestOccurrences, nextOccurrence, sameTiming, type ScheduleSpec } from '../utils/schedule-spec.util.js';
import { WORKFLOWS_MODULE_OPTIONS } from '../workflows.module-definition.js';
import { WorkflowRegistry } from './workflow-registry.service.js';

/** A schedule's bookkeeping (`WorkflowScheduleRecord.state`). */
export interface ScheduleState {
  /** The next occurrence not handled yet, or `null`: none left. */
  next: number | null;
  /** Occurrences started or buffered so far: what `limit` counts. */
  runs: number;
  /** `overlap: 'buffer-one'`: the occurrence waiting for the running instance to end. */
  buffered: number | null;
  /**
   * Starts decided but perhaps not made yet, with the instances to cancel first (`'cancel-previous'`). Written
   * before the instances are created and cleared after, so a worker that dies in between (or loses the lease)
   * leaves them to the next one, which makes them again: an occurrence's instance id makes that a no-op.
   */
  pending: Array<{ at: number; cancel: string[] }>;
}

/** The fields `saveSchedule()` takes, without the condition. */
type ScheduleFields = Omit<WorkflowScheduleSave, 'id' | 'expectRevision' | 'now'>;

/** What one production did, for the worker: the instances it started, and the ones it cancelled. */
export interface ScheduleProduction {
  started: number;
  cancelled: string[];
}

/** `missed: 'skip'` still starts an occurrence found this soon after its time. */
const ON_TIME_MS = 60_000;
/** `missed: 'all'` starts at most this many of the occurrences it missed, the latest. */
export const MAX_CATCH_UP = 100;
/** Schedules one production leases at most; the rest wait for the next. */
const BATCH = 100;
const UNFINISHED: WorkflowStatus[] = ['pending', 'running', 'suspended', 'compensating'];

/** The stored input of a declared schedule that no codec can read any more: never equal to the code's. */
const UNREADABLE = Symbol('unreadable');

/** The id of the instance an occurrence starts: the same wherever and however often it is started. */
export function occurrenceId(schedule: string, at: number): string {
  return `${schedule}@${new Date(at).toISOString()}`;
}

/**
 * @internal The engine of schedules: keeps the declared ones in step with the code, and starts the occurrences
 * that are due, for the worker. `WorkflowSchedules` is the public side.
 */
@Injectable()
export class WorkflowScheduler implements OnApplicationBootstrap {
  private readonly logger = new Logger('Workflows');
  private readonly clock: WorkflowClock;
  private syncing?: Promise<void>;
  private readonly warned = new Set<string>();

  constructor(
    private readonly storage: WorkflowStorage,
    @Inject(WORKFLOWS_MODULE_OPTIONS) options: WorkflowsModuleOptions,
    private readonly registry: WorkflowRegistry,
    private readonly events: WorkflowEvents,
  ) {
    this.clock = options.clock ?? systemClock;
  }

  /** Read at each call, never in the constructor: sources register while providers are created. */
  private get store(): EncodedWorkflowStore {
    return this.storage[ENGINE_STORE];
  }

  async onApplicationBootstrap(): Promise<void> {
    // A definition error (an id two workflows declare) fails the startup; the store's errors don't.
    this.registry.schedules();
    try {
      await this.synced();
    } catch (error) {
      this.logger.error('Saving the schedules the workflows declare failed; the worker tries again before it starts any.', error as Error);
    }
  }

  /** The declared schedules, saved once per process (again after a failure). */
  synced(): Promise<void> {
    this.syncing ??= this.sync().catch((error: unknown) => {
      this.syncing = undefined;
      throw error;
    });
    return this.syncing;
  }

  /**
   * Saves the schedules the code declares (a changed one is updated, an unchanged one left as it is, its progress
   * kept) and removes the ones the workflows this process runs no longer declare.
   */
  private async sync(): Promise<void> {
    if (this.registry.names().length === 0) {
      return;
    }

    const declared = this.registry.schedules();
    for (const [id, schedule] of declared) {
      await this.modify(
        id,
        (current) => {
          if (current && !current.declared && !this.warned.has(id)) {
            this.warned.add(id);
            this.logger.warn(`Schedule "${id}" was saved with WorkflowSchedules.upsert(); workflow "${schedule.workflow}" declares it now, and takes it over.`);
          }
          return this.declaredChange(current, schedule);
        },
        (id) => this.readDeclared(id),
      );
    }

    // Read as stored: removing a schedule needs nothing a codec encoded.
    const names = new Set(this.registry.names());
    for (const record of await this.allDeclared()) {
      if (names.has(record.workflow) && !declared.has(record.id)) {
        await this.remove(record);
      }
    }
  }

  private declaredChange(current: WorkflowScheduleRecord | null, schedule: WorkflowDeclaredSchedule & { workflow: string }): ScheduleFields | null {
    const input = schedule.spec.inputFn ? null : (schedule.input ?? null);
    // With a codec, the input is saved again at each startup: encoded with the current key, a rotated one can go.
    const same =
      current?.declared &&
      current.workflow === schedule.workflow &&
      canonical(current.spec) === canonical(schedule.spec) &&
      canonical(current.input ?? null) === canonical(input) &&
      !(this.store.encodes && input !== null);
    return same ? null : this.fields(current, { workflow: schedule.workflow, declared: true, spec: schedule.spec, input });
  }

  /** A declared schedule, with an input no codec can read any more (a dropped key) as unreadable: the code's replaces it. */
  private async readDeclared(id: string): Promise<WorkflowScheduleRecord | null> {
    try {
      return await this.store.getSchedule(id);
    } catch {
      const record = await this.store.inner.getSchedule(id);
      return record && { ...record, input: UNREADABLE };
    }
  }

  /**
   * Deletes a declared schedule the code no longer declares, again if a worker wrote it meanwhile (a worker of the
   * code that still declares it, in a rolling deploy).
   */
  private async remove(record: WorkflowScheduleRecord): Promise<void> {
    let current: WorkflowScheduleRecord | null = record;
    for (let attempt = 0; attempt < 10 && current?.declared; attempt++) {
      if (await this.store.inner.deleteSchedule(current.id, current.revision)) {
        return;
      }
      current = await this.store.inner.getSchedule(current.id);
    }
  }

  /**
   * A schedule's fields after a change of what it is: a new one starts with its first occurrence after now; one
   * whose timing is unchanged keeps its progress; one whose timing changed starts over from now (keeping its runs
   * and the starts in flight) and takes the lease from a worker that computed with the old timing.
   */
  fields(current: WorkflowScheduleRecord | null, next: { workflow: string; declared: boolean; spec: ScheduleSpec; input: unknown }): ScheduleFields {
    const now = this.clock.now();
    if (!current) {
      const state: ScheduleState = { next: nextOccurrence(next.spec, now), runs: 0, buffered: null, pending: [] };
      return { ...next, paused: false, state, wakeAt: wakeAt(state, now), releaseLease: false };
    }

    const previous = current.state as ScheduleState;
    const retimed = !sameTiming(current.spec as ScheduleSpec, next.spec);
    // A retimed schedule drops its buffered occurrence, which then never starts: it no longer counts.
    const runs = retimed && previous.buffered !== null ? previous.runs - 1 : previous.runs;
    const ended = limitReached(next.spec, runs);
    const state: ScheduleState = {
      ...previous,
      runs,
      next: ended ? null : retimed ? nextOccurrence(next.spec, now) : (previous.next ?? nextOccurrence(next.spec, now)),
      buffered: retimed ? null : previous.buffered,
    };
    return { ...next, paused: current.paused, state, wakeAt: wakeAt(state, now), releaseLease: retimed };
  }

  /**
   * Applies `change` to the stored schedule until it lands: a write conditional on the revision it read. `change`
   * returns the new fields, or `null` to leave it. Returns the schedule as stored (`null`: none).
   */
  async modify(
    id: string,
    change: (current: WorkflowScheduleRecord | null) => ScheduleFields | null,
    read: (id: string) => Promise<WorkflowScheduleRecord | null> = (id) => this.store.getSchedule(id),
  ): Promise<WorkflowScheduleRecord | null> {
    for (let attempt = 0; attempt < 10; attempt++) {
      const current = await read(id);
      const fields = change(current);
      if (!fields) {
        return current;
      }

      const saved = await this.store.saveSchedule({ ...fields, id, expectRevision: current?.revision ?? null, now: this.clock.now() });
      if (saved) {
        return saved;
      }
    }
    throw new WorkflowStateError(`Schedule "${id}" kept changing while it was being saved. Try again.`);
  }

  /** Every declared schedule as stored, a page at a time. */
  private async allDeclared(): Promise<WorkflowScheduleRecord[]> {
    const records: WorkflowScheduleRecord[] = [];
    for (let offset = 0; ; offset += BATCH) {
      const page = await this.store.inner.listSchedules({ declared: true, limit: BATCH, offset });
      records.push(...page);
      if (page.length < BATCH) {
        return records;
      }
    }
  }

  // ---------------------------------------------------------------- producing occurrences

  /**
   * @internal Called by the worker: leases the due schedules of the workflows it runs and starts their
   * occurrences. The lease makes one worker the producer of a schedule's occurrences at a time; an occurrence's
   * instance id makes a second start of it (by a worker that took over) a no-op.
   */
  async produce(owner: string, leaseMs: number): Promise<ScheduleProduction> {
    const workflows = this.registry.names();
    if (workflows.length === 0) {
      return { started: 0, cancelled: [] };
    }
    await this.synced();

    const now = this.clock.now();
    const token = randomUUID();
    const claimed = await this.store.claimSchedules({ owner, token, now, leaseUntil: now + leaseMs, limit: BATCH, workflows });
    const production: ScheduleProduction = { started: 0, cancelled: [] };
    for (const record of claimed) {
      try {
        const one = await this.produceOne(record, token, now);
        production.started += one.started;
        production.cancelled.push(...one.cancelled);
      } catch (error) {
        this.logger.error(`Starting the occurrences of schedule "${record.id}" failed; it is retried when its lease expires.`, error as Error);
      }
    }
    return production;
  }

  private async produceOne(record: WorkflowScheduleRecord, token: string, now: number): Promise<ScheduleProduction> {
    const spec = record.spec as ScheduleSpec;
    const state = record.state as ScheduleState;
    const declared = record.declared ? this.registry.schedules().get(record.id) : undefined;
    if (record.declared && !declared) {
      // A declared schedule belongs to the code that declares it: in a rolling deploy, a worker of code that doesn't
      // (yet, or any more) leaves it to one that does, and the last to start removes it.
      await this.store.writeSchedule(record.id, token, { now, state, wakeAt: record.wakeAt, release: true });
      if (!this.warned.has(`undeclared:${record.id}`)) {
        this.warned.add(`undeclared:${record.id}`);
        this.logger.warn(`Schedule "${record.id}" is declared by code this worker doesn't run; it leaves it to a worker whose code declares it.`);
      }
      return { started: 0, cancelled: [] };
    }

    const workflow = this.registry.resolve(record.workflow, spec.version ?? undefined);
    const st: ScheduleState = { ...state, pending: [...state.pending] };
    const skipped: Array<{ reason: 'missed' | 'overlap'; from: number; to: number }> = [];
    const running = spec.overlap === 'allow' ? [] : await this.running(record.id);
    const busy = () => running.length > 0 || st.pending.length > 0;

    if (st.buffered !== null && !busy()) {
      st.pending.push({ at: st.buffered, cancel: [] });
      st.buffered = null;
    }

    if (st.next !== null && st.next <= now) {
      const due = dueOccurrences(spec, st.next, now);
      if (due.skipped) {
        skipped.push({ reason: 'missed', ...due.skipped });
      }
      let starts = due.start;
      if (spec.overlap === 'cancel-previous' && starts.length > 1) {
        // Each would cancel the one before it at once: only the latest runs.
        skipped.push({ reason: 'overlap', from: starts[0]!, to: starts[starts.length - 2]! });
        starts = starts.slice(-1);
      }

      for (const at of starts) {
        if (limitReached(spec, st.runs)) {
          break;
        }

        if (spec.overlap === 'allow' || !busy()) {
          st.pending.push({ at, cancel: [] });
        } else if (spec.overlap === 'cancel-previous') {
          st.pending.push({ at, cancel: [...running, ...st.pending.map((start) => occurrenceId(record.id, start.at))] });
        } else if (spec.overlap === 'buffer-one' && st.buffered === null) {
          st.buffered = at;
        } else {
          skipped.push({ reason: 'overlap', from: at, to: at });
          continue;
        }
        st.runs++;
      }
      st.next = limitReached(spec, st.runs) ? null : nextOccurrence(spec, now);
    }

    // The decisions first: whatever happens to this worker from here on, the next producer finishes them.
    if (canonical(st) !== canonical(state) && !(await this.store.writeSchedule(record.id, token, { now, state: st, wakeAt: now, release: false }))) {
      return { started: 0, cancelled: [] };
    }

    const production: ScheduleProduction = { started: 0, cancelled: [] };
    let skippedStarts = 0;
    for (const start of st.pending) {
      for (const id of start.cancel) {
        const reason = `Cancelled: schedule "${record.id}" started its occurrence of ${new Date(start.at).toISOString()} (overlap: 'cancel-previous').`;
        if (await this.store.requestCancel(id, { reason, now, terminate: false })) {
          production.cancelled.push(id);
        }
      }

      const started = await this.startOccurrence(record, spec, declared, workflow, start.at, now);
      if (!started) {
        skippedStarts++;
      } else if (started.created) {
        production.started++;
      }
    }

    // An occurrence that couldn't start (its input threw) doesn't count toward `limit`.
    const runs = st.runs - skippedStarts;
    const done: ScheduleState = { ...st, runs, next: st.next === null && !limitReached(spec, runs) ? nextOccurrence(spec, now) : st.next, pending: [] };
    await this.store.writeSchedule(record.id, token, { now, state: done, wakeAt: wakeAt(done, now), release: true });
    for (const range of skipped) {
      this.events.emit({ type: 'schedule-skipped', id: record.id, workflow: workflow.name, version: workflow.version, at: now, ...range });
    }
    return production;
  }

  /** The unfinished instances the schedule started. */
  private async running(schedule: string): Promise<string[]> {
    const instances = await this.store.list({ scheduleId: schedule, status: UNFINISHED, limit: BATCH, offset: 0 });
    return instances.map((instance) => instance.id);
  }

  /**
   * Starts the instance of an occurrence (or of a trigger, `at` being now): the one `create()` stored, and whether it
   * created it. An input or a key that can't be computed skips the occurrence (`null`), logged: retrying would fail
   * the same way. With `rethrow`, that throws instead.
   */
  async startOccurrence(
    record: WorkflowScheduleRecord,
    spec: ScheduleSpec,
    declared: WorkflowDeclaredSchedule | undefined,
    workflow: WorkflowMetadata,
    at: number,
    now: number,
    rethrow = false,
  ): Promise<{ instance: WorkflowInstance; created: boolean } | null> {
    const id = occurrenceId(record.id, at);
    let data;
    try {
      // A declared schedule's input is its code's (the stored one is for reading), an upserted one's is stored.
      const source = declared ? declared.input : record.input;
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
  view(record: WorkflowScheduleRecord): WorkflowSchedule {
    const spec = record.spec as ScheduleSpec;
    const state = record.state as ScheduleState;
    return {
      id: record.id,
      workflow: record.workflow,
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
      ...(spec.inputFn || record.input === null ? {} : { input: record.input }),
      paused: record.paused,
      nextAt: record.paused ? null : state.next,
      runs: state.runs,
      bufferedAt: state.buffered,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
    };
  }
}

/** When a worker next has something to do for the schedule: now for starts in flight or a buffered one (checked at every poll), else its next occurrence. */
export function wakeAt(state: ScheduleState, now: number): number | null {
  return state.pending.length > 0 || state.buffered !== null ? now : state.next;
}

export function limitReached(spec: ScheduleSpec, runs: number): boolean {
  return spec.limit !== null && runs >= spec.limit;
}

/**
 * The due occurrences in `[from, now]` to start, by `spec.missed`, and the ones it passes over (as a range). Only
 * the latest can be on time; `'skip'` starts it if it is, `'once'` either way, and `'all'` the latest 100.
 */
export function dueOccurrences(spec: ScheduleSpec, from: number, now: number): { start: number[]; skipped: { from: number; to: number } | null } {
  const keep = spec.missed === 'all' ? MAX_CATCH_UP : 1;
  const latest = latestOccurrences(spec, from, now, keep + 1);
  const due = latest.slice(-keep);
  const start = spec.missed === 'skip' && due.length > 0 && now - due[due.length - 1]! > ON_TIME_MS ? [] : due;

  if (start.length === 0) {
    return { start, skipped: latest.length > 0 ? { from, to: latest[latest.length - 1]! } : null };
  }
  const passed = latest.length > keep ? latest[0]! : null;
  return { start, skipped: passed === null ? null : { from, to: passed } };
}
