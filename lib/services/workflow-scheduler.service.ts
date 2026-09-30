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
import { systemClock } from '../core/time/clock.js';
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
  pending: Array<{ at: number; cancel: string[]; attempts?: number }>;
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
/** How many times a production tries to start an occurrence the store refuses (about 5 minutes, backing off). */
const MAX_START_ATTEMPTS = 10;
const UNFINISHED: WorkflowStatus[] = ['pending', 'running', 'suspended', 'compensating'];

/** The stored input of a declared schedule that no codec can read any more: never equal to the code's. */
const UNREADABLE = Symbol('unreadable');

/** How often a process reconciles its code's declared schedules with the store, after doing so at its startup. */
const RECONCILE_MS = 60_000;
/** A process whose code declares a schedule confirms it when it reconciles, if its last confirmation is this old. */
const CONFIRM_MS = 60_000;
/** A declared schedule that no process confirmed for this long has no running code that declares it. */
const STALE_MS = 5 * 60_000;

/** A declared schedule's spec as stored: its declaration's, and the code that saved or last confirmed it, and when. */
interface DeclaredSpec extends ScheduleSpec {
  /** The version of its workflow whose code saved or last confirmed it: the highest the process registers. */
  declaredBy?: number;
  /** When a process whose code declares it saved or last confirmed it. */
  confirmedAt?: number;
}

/** What the code declares: the stored spec without its bookkeeping. */
function declaration({ declaredBy: _declaredBy, confirmedAt: _confirmedAt, ...spec }: DeclaredSpec): ScheduleSpec {
  return spec;
}

/** Whether a process whose code declares the schedule confirmed it lately: the code of a running process declares it. */
function confirmed(record: WorkflowScheduleRecord, now: number): boolean {
  const { confirmedAt } = record.spec as DeclaredSpec;
  return confirmedAt !== undefined && now - confirmedAt < STALE_MS;
}

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
  /** When `produce()` next reconciles the declared schedules with the code (the module's clock). */
  private reconcileAt = 0;
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

  /** The declared schedules, reconciled with the code once per process at startup (again after a failure). */
  synced(): Promise<void> {
    this.syncing ??= this.reconcile(true).then(
      () => {
        this.reconcileAt = this.clock.now() + RECONCILE_MS;
      },
      (error: unknown) => {
        this.syncing = undefined;
        throw error;
      },
    );
    return this.syncing;
  }

  /**
   * Reconciles the declared schedules with this process's code: at startup, then about once a minute (from the
   * productions). Safe in a rolling deploy, where processes of other code run beside it:
   *
   * - A schedule its code declares is saved when it is missing, and confirmed (`confirmedAt`) when its last
   *   confirmation is a minute old. One declared differently is replaced at startup, unless a newer version of the
   *   workflow declared it and a process of that code still confirms it; later, only once no process confirms it
   *   (the code that declared it is gone), or when an older version declared it.
   * - A declared schedule its code doesn't declare is left while a process confirms it, since the code of a running
   *   process declares it, and deleted once none has for five minutes, whether or not this process registers its
   *   workflow: otherwise the schedules of a workflow that no code has any more would stay forever. The processes
   *   whose code declares one save it again as they start, so after its pods were scaled to zero for longer, it
   *   starts over: from its next occurrence, unpaused, its runs counted from zero.
   */
  private async reconcile(startup: boolean): Promise<void> {
    if (this.registry.names().length === 0) {
      return;
    }

    const now = this.clock.now();
    const declared = this.registry.schedules();
    for (const [id, schedule] of declared) {
      const version = this.registry.latest(schedule.workflow)!.version;
      await this.modify(
        id,
        (current) => {
          if (current && !current.declared && !this.warned.has(id)) {
            this.warned.add(id);
            this.logger.warn(`Schedule "${id}" was saved with WorkflowSchedules.upsert(); workflow "${schedule.workflow}" declares it now, and takes it over.`);
          }
          return this.declaredChange(current, schedule, { version, now, startup });
        },
        { read: (id) => this.readReplacing(id) },
      );
    }

    // Read as stored: removing a schedule needs nothing a codec encoded.
    for (const record of await this.allDeclared()) {
      if (!declared.has(record.id)) {
        await this.remove(record, now);
      }
    }
  }

  private declaredChange(
    current: WorkflowScheduleRecord | null,
    schedule: WorkflowDeclaredSchedule & { workflow: string },
    at: { version: number; now: number; startup: boolean },
  ): ScheduleFields | null {
    const input = schedule.spec.inputFn ? null : (schedule.input ?? null);
    const spec: DeclaredSpec = { ...schedule.spec, declaredBy: at.version, confirmedAt: at.now };
    if (!current?.declared) {
      return this.fields(current, { workflow: schedule.workflow, declared: true, spec, input });
    }

    const stored = current.spec as DeclaredSpec;
    const sameWorkflow = current.workflow === schedule.workflow;
    const sameInput = canonical(current.input ?? null) === canonical(input);
    if (sameWorkflow && canonical(declaration(stored)) === canonical(schedule.spec) && (sameInput || current.input === UNREADABLE)) {
      // With a codec, the input is saved again at each startup: encoded with the current key, a rotated one can go.
      const rewrite = !sameInput || (at.startup && this.store.encodes && input !== null);
      const confirm = stored.confirmedAt === undefined || at.now - stored.confirmedAt >= CONFIRM_MS;
      if (!rewrite && !confirm) {
        return null;
      }

      const { workflow, paused, wakeAt, state } = current;
      const by = Math.max(stored.declaredBy ?? 0, at.version);
      return { workflow, declared: true, spec: { ...stored, declaredBy: by, confirmedAt: at.now }, input, paused, wakeAt, state, releaseLease: false };
    }

    // Declared otherwise by other code. A newer version's declaration stays while a process of it confirms it; the
    // code that starts takes the others over, and a running process one that no process confirms any more (the code
    // that declared it is gone) or an older version's.
    const by = stored.declaredBy ?? 0;
    const takeOver = !confirmed(current, at.now) || (sameWorkflow ? (at.startup ? by <= at.version : by < at.version) : at.startup);
    return takeOver ? this.fields(current, { workflow: schedule.workflow, declared: true, spec, input }) : null;
  }

  /**
   * A schedule to save with a new input (the code's, or `upsert()`'s): one with an input no codec can read any more
   * (a dropped key) is read as it is stored, its input marked unreadable, so the new input can replace it.
   */
  async readReplacing(id: string): Promise<WorkflowScheduleRecord | null> {
    try {
      return await this.store.getSchedule(id);
    } catch {
      const record = await this.store.inner.getSchedule(id);
      return record && { ...record, input: UNREADABLE };
    }
  }

  /** How `WorkflowSchedules` shows a schedule as stored: without its input, if no codec can read it any more. */
  async readableView(record: WorkflowScheduleRecord): Promise<WorkflowSchedule> {
    try {
      return this.view(await this.store.decodeSchedule(record));
    } catch (error) {
      if (!this.warned.has(`unreadable:${record.id}`)) {
        this.warned.add(`unreadable:${record.id}`);
        this.logger.warn(`Schedule "${record.id}" is shown without its input, which can't be read: ${(error as Error).message}`);
      }
      return this.view({ ...record, input: null });
    }
  }

  /**
   * Deletes a declared schedule this process's code doesn't declare, once no process whose code declares it has
   * confirmed it for five minutes; again if it was written meanwhile, unless that write confirmed it.
   */
  private async remove(record: WorkflowScheduleRecord, now: number): Promise<void> {
    let current: WorkflowScheduleRecord | null = record;
    for (let attempt = 0; attempt < 10 && current?.declared && !confirmed(current, now); attempt++) {
      if (await this.store.inner.deleteSchedule(current.id, current.revision)) {
        this.logger.log(`Deleted schedule "${current.id}" of workflow "${current.workflow}": no process whose code declares it confirmed it for five minutes.`);
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
   * returns the new fields, or `null` to leave it. Returns the schedule as stored (`null`: none). With `asStored`,
   * the schedule is read and saved as the store holds it, its input untouched (a pause needs no codec).
   */
  async modify(
    id: string,
    change: (current: WorkflowScheduleRecord | null) => ScheduleFields | null,
    options: { read?: (id: string) => Promise<WorkflowScheduleRecord | null>; asStored?: boolean } = {},
  ): Promise<WorkflowScheduleRecord | null> {
    const store = options.asStored ? this.store.inner : this.store;
    const read = options.read ?? ((id: string) => store.getSchedule(id));
    for (let attempt = 0; attempt < 10; attempt++) {
      const current = await read(id);
      const fields = change(current);
      if (!fields) {
        return current;
      }

      const saved = await store.saveSchedule({ ...fields, id, expectRevision: current?.revision ?? null, now: this.clock.now() });
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
    if (now >= this.reconcileAt) {
      this.reconcileAt = now + RECONCILE_MS;
      try {
        await this.reconcile(false);
      } catch (error) {
        this.logger.error('Reconciling the schedules the workflows declare failed; it is tried again in a minute.', error as Error);
      }
    }

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
      // (yet, or any more) leaves it to one that does, and it goes once no process confirms it (see reconcile()).
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
    const retries: ScheduleState['pending'] = [];
    let skippedStarts = 0;
    for (const start of st.pending) {
      const occurrence = new Date(start.at).toISOString();
      try {
        for (const id of start.cancel) {
          const reason = `Cancelled: schedule "${record.id}" started its occurrence of ${occurrence} (overlap: 'cancel-previous').`;
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
      } catch (error) {
        // The store refused the start: tried again (after a backoff) by the next production, a few times, so a
        // start that can never be stored doesn't hold the schedule up forever.
        const attempts = (start.attempts ?? 0) + 1;
        const message = `Schedule "${record.id}" couldn't start its occurrence of ${occurrence} (attempt ${attempts} of ${MAX_START_ATTEMPTS})`;
        this.logger.error(attempts < MAX_START_ATTEMPTS ? `${message}; it tries again.` : `${message}; it gives up on it.`, error as Error);
        if (attempts < MAX_START_ATTEMPTS) {
          retries.push({ ...start, attempts });
        } else {
          skippedStarts++;
        }
      }
    }

    // An occurrence that couldn't start (its input threw, or the store kept refusing it) doesn't count toward `limit`.
    const runs = st.runs - skippedStarts;
    const done: ScheduleState = { ...st, runs, next: st.next === null && !limitReached(spec, runs) ? nextOccurrence(spec, now) : st.next, pending: retries };
    const backoff = Math.min(1_000 * 2 ** (Math.max(0, ...retries.map((start) => start.attempts ?? 0)) - 1), 60_000);
    const due = retries.length > 0 ? Math.min(now + backoff, wakeAt({ ...done, pending: [] }, now) ?? Infinity) : wakeAt(done, now);
    await this.store.writeSchedule(record.id, token, { now, state: done, wakeAt: due, release: true });
    for (const range of skipped) {
      this.events.emit({ type: 'schedule-skipped', id: record.id, workflow: workflow.name, version: workflow.version, at: now, ...range });
    }
    return production;
  }

  /** The unfinished instances the schedule started. */
  private async running(schedule: string): Promise<string[]> {
    // Read as stored: only the ids count, and an instance no codec can read any more still runs.
    const instances = await this.store.inner.list({ scheduleId: schedule, status: UNFINISHED, limit: BATCH, offset: 0 });
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
