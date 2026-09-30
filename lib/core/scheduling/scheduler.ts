import { randomUUID } from 'node:crypto';
import { Logger, type LoggerService } from '@nestjs/common';
import { PayloadCodecs } from '../codecs/payload-codecs.js';
import type { Clock } from '../interfaces/clock.interface.js';
import type { ScheduleSpec } from '../interfaces/schedule.interface.js';
import type { ScheduleRecord, ScheduleSave, ScheduleStore } from '../interfaces/schedule-store.interface.js';
import type { DeclaredSchedule, ScheduleProduction, ScheduleSkip, SchedulerLabels, SchedulerOptions } from '../interfaces/scheduler-options.interface.js';
import { systemClock } from '../time/clock.js';
import { canonical } from '../utils/canonical.util.js';
import { EncodedScheduleStore } from './encoded-schedule.store.js';
import { latestOccurrences, nextOccurrence, occurrenceId, sameTiming } from './schedule-spec.js';

/** A schedule's bookkeeping (`ScheduleRecord.state`). */
interface ScheduleState {
  /** The next occurrence not handled yet, or `null`: none left. */
  next: number | null;
  /** Occurrences started or buffered so far: what `limit` counts. */
  runs: number;
  /** `overlap: 'buffer-one'`: the occurrence waiting for the running run to end. */
  buffered: number | null;
  /**
   * Starts decided but perhaps not made yet, with the runs to cancel first (`'cancel-previous'`). Written before the
   * runs are started and cleared after, so a worker that dies in between (or loses the lease) leaves them to the next
   * one, which makes them again: an occurrence's run id makes that a no-op.
   */
  pending: Array<{ at: number; cancel: string[]; attempts?: number }>;
}

/** A declared schedule's spec as stored: its declaration's, and the code that saved or last confirmed it, and when. */
interface DeclaredSpec extends ScheduleSpec {
  /** The version of the code that saved or last confirmed it. */
  declaredBy?: number;
  /** When a process whose code declares it saved or last confirmed it. */
  confirmedAt?: number;
}

/**
 * A schedule's fields, as `Scheduler.save()`'s change returns them: what `ScheduleStore.saveSchedule()` takes, without
 * the condition.
 */
export type ScheduleFields = Omit<ScheduleSave, 'id' | 'expectRevision' | 'now'>;

/** `missed: 'skip'` still starts an occurrence found this soon after its time. */
const ON_TIME_MS = 60_000;
/** `missed: 'all'` starts at most this many of the occurrences it missed, the latest. */
const MAX_CATCH_UP = 100;
/** Schedules one production leases at most; the rest wait for the next. */
const BATCH = 100;
/** How many times a production tries to start an occurrence the store refuses (about 5 minutes, backing off). */
const MAX_START_ATTEMPTS = 10;
/** How often a process reconciles its code's declared schedules with the store, after doing so at its startup. */
const RECONCILE_MS = 60_000;
/** A process whose code declares a schedule confirms it when it reconciles, if its last confirmation is this old. */
const CONFIRM_MS = 60_000;
/** A declared schedule that no process confirmed for this long has no running code that declares it. */
const STALE_MS = 5 * 60_000;

/** The stored payload of a schedule that no codec can read any more: never equal to the code's. */
const UNREADABLE = Symbol('unreadable');

/**
 * The engine of schedules, over a `ScheduleStore`: it starts each occurrence's run exactly once however many workers
 * produce them, and keeps the schedules the code declares in step with it, also through rolling deploys. A package
 * plugs in its runs (`fire`, `running`, `cancel`) and calls `produce()` from its worker's loop.
 *
 * - **One run per occurrence.** A production leases the due schedules (`claimSchedules`), records its decisions in
 *   each (the starts it will make, the runs to cancel) before making them, then clears them and hands the schedule
 *   back. A worker that dies or loses the lease in between leaves them to the next, which makes them again: a run's
 *   id, `occurrenceId(schedule, at)`, makes the second start a no-op. `runs`, what `limit` counts, is exact.
 * - **missed** and **overlap** as `ScheduleOptions` say; a start the store refuses is retried, backing off from a
 *   second to a minute, ten times in all, while the schedule's other starts go ahead.
 * - **Declared schedules follow the code.** At startup (`synced()`) and every minute (from `produce()`), a process
 *   saves the schedules its code declares, confirms them, and takes over one declared otherwise, unless a newer version
 *   of the code declared it and a process of that code still confirms it. It deletes a declared schedule its code
 *   doesn't declare once no process confirmed it for five minutes: the code that declares it is gone.
 *
 * ```ts
 * const scheduler = new Scheduler({ store, targets, declared, fire, running, cancel, clock });
 * await scheduler.synced(); // at startup
 * const worker = new LeasedWorker({ ..., produce: async () => (await scheduler.produce(owner, leaseMs)).started });
 * ```
 */
export class Scheduler<S extends ScheduleSpec = ScheduleSpec> {
  /** The store, as it holds the schedules: payloads undecoded. */
  readonly store: ScheduleStore;
  private readonly encoded: EncodedScheduleStore;
  private readonly clock: Clock;
  private readonly logger: Pick<LoggerService, 'log' | 'warn' | 'error'>;
  private readonly labels: Required<SchedulerLabels>;
  private syncing?: Promise<void>;
  /** When `produce()` next reconciles the declared schedules with the code (the clock's time). */
  private reconcileAt = 0;
  private readonly warned = new Set<string>();

  constructor(private readonly options: SchedulerOptions<S>) {
    this.store = options.store;
    this.clock = options.clock ?? systemClock;
    this.logger = options.logger ?? new Logger('Scheduler');
    this.labels = {
      target: options.labels?.target ?? ((name) => `target "${name}"`),
      targets: options.labels?.targets ?? 'targets',
      payload: options.labels?.payload ?? 'payload',
      upsert: options.labels?.upsert ?? 'upsert()',
    };
    this.encoded = new EncodedScheduleStore(
      options.store,
      options.codecs ?? new PayloadCodecs([]),
      options.payloadContext ?? ((schedule) => ({ field: 'payload', schedule })),
      this.logger,
    );
  }

  /** The declared schedules, reconciled with the code once per process (again after a failure). Call it at startup. */
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
   * Leases the due schedules of this process's targets and starts their occurrences' runs; every minute it reconciles
   * the declared schedules first. Call it from the worker's loop (`LeasedWorker`'s `produce`), with its owner and
   * lease duration.
   */
  async produce(owner: string, leaseMs: number): Promise<ScheduleProduction> {
    const targets = this.options.targets();
    if (targets.length === 0) {
      return { started: 0, cancelled: [] };
    }
    await this.synced();

    const now = this.clock.now();
    if (now >= this.reconcileAt) {
      this.reconcileAt = now + RECONCILE_MS;
      try {
        await this.reconcile(false);
      } catch (error) {
        this.logger.error(`Reconciling the schedules the ${this.labels.targets} declare failed; it is tried again in a minute.`, error as Error);
      }
    }

    const token = randomUUID();
    const claimed = await this.encoded.claimSchedules({ owner, token, now, leaseUntil: now + leaseMs, limit: BATCH, targets });
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

  /**
   * Applies `change` to the stored schedule until it lands: a write conditional on the revision it read. `change`
   * returns the new fields, or `null` to leave it. Resolves to the schedule as stored (`null`: none). With `replacing`,
   * a schedule whose payload no codec can read any more is read as it is stored, so the new payload replaces it; with
   * `asStored`, it is read and saved as the store holds it, its payload untouched (a pause needs no codec).
   */
  async save(
    id: string,
    change: (current: ScheduleRecord | null) => ScheduleFields | null,
    options: { replacing?: boolean; asStored?: boolean } = {},
  ): Promise<ScheduleRecord | null> {
    const store = options.asStored ? this.store : this.encoded;
    const read = options.replacing ? (id: string) => this.readReplacing(id) : (id: string) => store.getSchedule(id);
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

    const message = `Schedule "${id}" kept changing while it was being saved. Try again.`;
    throw this.options.conflictError?.(message) ?? new Error(message);
  }

  /**
   * A schedule's fields after a change of what it is: a new one starts with its first occurrence after now; one whose
   * timing is unchanged keeps its progress; one whose timing changed starts over from now (keeping its runs and the
   * starts in flight, dropping an occurrence buffered with the old timing) and takes the lease from a worker that
   * computed with the old timing. Its pause carries over.
   */
  changed(current: ScheduleRecord | null, next: { target: string; declared: boolean; spec: S; payload: unknown }): ScheduleFields {
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
   * Pauses the schedule at once: it takes the lease, so a worker deciding its occurrences at that moment starts only
   * what it had already decided, and nothing else starts until `resume()`. Resolves to it as stored, or `null` for an
   * unknown id.
   */
  pause(id: string): Promise<ScheduleRecord | null> {
    return this.save(id, (current) => (!current || current.paused ? null : { ...fieldsOf(current), paused: true, releaseLease: true }), { asStored: true });
  }

  /**
   * Resumes the schedule from its first occurrence after now: those due while it was paused are passed over, whatever
   * `missed` says. Resolves to it as stored, or `null` for an unknown id.
   */
  resume(id: string): Promise<ScheduleRecord | null> {
    return this.save(
      id,
      (current) => {
        if (!current?.paused) {
          return null;
        }

        const now = this.clock.now();
        const spec = current.spec as ScheduleSpec;
        const previous = current.state as ScheduleState;
        const state: ScheduleState = { ...previous, next: limitReached(spec, previous.runs) ? null : nextOccurrence(spec, now) };
        return { ...fieldsOf(current), paused: false, state, wakeAt: wakeAt(state, now), releaseLease: true };
      },
      { asStored: true },
    );
  }

  /** The schedule with its payload decoded, or `null` for an unknown id. Throws when no codec can read its payload. */
  read(id: string): Promise<ScheduleRecord | null> {
    return this.encoded.getSchedule(id);
  }

  /** A schedule as stored, with its payload decoded; `null` (logged once) when no codec can read it any more. */
  async readable(record: ScheduleRecord): Promise<ScheduleRecord> {
    try {
      return await this.encoded.decode(record);
    } catch (error) {
      if (!this.warned.has(`unreadable:${record.id}`)) {
        this.warned.add(`unreadable:${record.id}`);
        this.logger.warn(`Schedule "${record.id}" is shown without its ${this.labels.payload}, which can't be read: ${(error as Error).message}`);
      }
      return { ...record, payload: null };
    }
  }

  /**
   * Where a stored schedule is: when its next occurrence is due (`null` while paused, or once it ended), how many it
   * started (what `limit` counts), and the occurrence buffered for `overlap: 'buffer-one'`.
   */
  progress(record: ScheduleRecord): { nextAt: number | null; runs: number; bufferedAt: number | null } {
    const state = record.state as ScheduleState;
    return { nextAt: record.paused ? null : state.next, runs: state.runs, bufferedAt: state.buffered };
  }

  // ---------------------------------------------------------------- reconciling the declared schedules

  /**
   * Reconciles the declared schedules with this process's code: at startup, then about once a minute (from the
   * productions). Safe in a rolling deploy, where processes of other code run beside it:
   *
   * - A schedule its code declares is saved when it is missing, and confirmed (`confirmedAt`) when its last
   *   confirmation is a minute old. One declared differently is replaced at startup, unless a newer version of the
   *   code declared it and a process of that code still confirms it; later, only once no process confirms it (the code
   *   that declared it is gone), or when an older version declared it.
   * - A declared schedule its code doesn't declare is left while a process confirms it, since the code of a running
   *   process declares it, and deleted once none has for five minutes, whatever its target: otherwise the schedules of
   *   a target that no code has any more would stay forever. The processes whose code declares one save it again as
   *   they start, so after its processes were gone for longer, it starts over: from its next occurrence, unpaused, its
   *   runs counted from zero.
   */
  private async reconcile(startup: boolean): Promise<void> {
    if (this.options.targets().length === 0) {
      return;
    }

    const now = this.clock.now();
    const declared = this.declared();
    for (const [id, schedule] of declared) {
      await this.save(
        id,
        (current) => {
          if (current && !current.declared && !this.warned.has(id)) {
            this.warned.add(id);
            this.logger.warn(`Schedule "${id}" was saved with ${this.labels.upsert}; ${this.labels.target(schedule.target)} declares it now, and takes it over.`);
          }
          return this.declaredChange(current, schedule, { now, startup });
        },
        { replacing: true },
      );
    }

    // Read as stored: removing a schedule needs nothing a codec encoded.
    for (const record of await this.allDeclared()) {
      if (!declared.has(record.id)) {
        await this.remove(record, now);
      }
    }
  }

  private declaredChange(current: ScheduleRecord | null, schedule: DeclaredSchedule<S>, at: { now: number; startup: boolean }): ScheduleFields | null {
    const payload = typeof schedule.payload === 'function' ? null : (schedule.payload ?? null);
    const spec: DeclaredSpec = { ...schedule.spec, declaredBy: schedule.version, confirmedAt: at.now };
    if (!current?.declared) {
      return this.changed(current, { target: schedule.target, declared: true, spec: spec as S, payload });
    }

    const stored = current.spec as DeclaredSpec;
    const sameTarget = current.target === schedule.target;
    const samePayload = canonical(current.payload ?? null) === canonical(payload);
    if (sameTarget && canonical(declaration(stored)) === canonical(schedule.spec) && (samePayload || current.payload === UNREADABLE)) {
      // With a codec, the payload is saved again at each startup: encoded with the current key, a rotated one can go.
      const rewrite = !samePayload || (at.startup && this.encoded.encodes && payload !== null);
      const confirm = stored.confirmedAt === undefined || at.now - stored.confirmedAt >= CONFIRM_MS;
      if (!rewrite && !confirm) {
        return null;
      }

      const { target, paused, wakeAt, state } = current;
      const by = Math.max(stored.declaredBy ?? 0, schedule.version);
      return { target, declared: true, spec: { ...stored, declaredBy: by, confirmedAt: at.now }, payload, paused, wakeAt, state, releaseLease: false };
    }

    // Declared otherwise by other code. A newer version's declaration stays while a process of it confirms it; the
    // code that starts takes the others over, and a running process one that no process confirms any more (the code
    // that declared it is gone) or an older version's.
    const by = stored.declaredBy ?? 0;
    const takeOver = !confirmed(current, at.now) || (sameTarget ? (at.startup ? by <= schedule.version : by < schedule.version) : at.startup);
    return takeOver ? this.changed(current, { target: schedule.target, declared: true, spec: spec as S, payload }) : null;
  }

  /**
   * A schedule to save with a new payload (the code's, or an upsert's): one with a payload no codec can read any more
   * (a dropped key) is read as it is stored, its payload marked unreadable, so the new payload can replace it.
   */
  private async readReplacing(id: string): Promise<ScheduleRecord | null> {
    try {
      return await this.encoded.getSchedule(id);
    } catch {
      const record = await this.store.getSchedule(id);
      return record && { ...record, payload: UNREADABLE };
    }
  }

  /**
   * Deletes a declared schedule this process's code doesn't declare, once no process whose code declares it has
   * confirmed it for five minutes; again if it was written meanwhile, unless that write confirmed it.
   */
  private async remove(record: ScheduleRecord, now: number): Promise<void> {
    let current: ScheduleRecord | null = record;
    for (let attempt = 0; attempt < 10 && current?.declared && !confirmed(current, now); attempt++) {
      if (await this.store.deleteSchedule(current.id, current.revision)) {
        this.logger.log(
          `Deleted schedule "${current.id}" of ${this.labels.target(current.target)}: no process whose code declares it confirmed it for five minutes.`,
        );
        return;
      }
      current = await this.store.getSchedule(current.id);
    }
  }

  /** Every declared schedule as stored, a page at a time. */
  private async allDeclared(): Promise<ScheduleRecord[]> {
    const records: ScheduleRecord[] = [];
    for (let offset = 0; ; offset += BATCH) {
      const page = await this.store.listSchedules({ declared: true, limit: BATCH, offset });
      records.push(...page);
      if (page.length < BATCH) {
        return records;
      }
    }
  }

  private declared(): ReadonlyMap<string, DeclaredSchedule<S>> {
    return this.options.declared?.() ?? new Map();
  }

  // ---------------------------------------------------------------- producing occurrences

  private async produceOne(record: ScheduleRecord, token: string, now: number): Promise<ScheduleProduction> {
    const spec = record.spec as S;
    const state = record.state as ScheduleState;
    const declared = record.declared ? this.declared().get(record.id) : undefined;
    if (record.declared && !declared) {
      // A declared schedule belongs to the code that declares it: in a rolling deploy, a worker of code that doesn't
      // (yet, or any more) leaves it to one that does, and it goes once no process confirms it (see reconcile()).
      await this.encoded.writeSchedule(record.id, token, { now, state, wakeAt: record.wakeAt, release: true });
      if (!this.warned.has(`undeclared:${record.id}`)) {
        this.warned.add(`undeclared:${record.id}`);
        this.logger.warn(`Schedule "${record.id}" is declared by code this worker doesn't run; it leaves it to a worker whose code declares it.`);
      }
      return { started: 0, cancelled: [] };
    }

    const st: ScheduleState = { ...state, pending: [...state.pending] };
    const skipped: ScheduleSkip[] = [];
    const running = spec.overlap === 'allow' ? [] : await this.options.running(record);
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
    if (canonical(st) !== canonical(state) && !(await this.encoded.writeSchedule(record.id, token, { now, state: st, wakeAt: now, release: false }))) {
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
          if (await this.options.cancel(id, reason, now)) {
            production.cancelled.push(id);
          }
        }

        const started = await this.options.fire({ schedule: record, spec, declared, id: occurrenceId(record.id, start.at), at: start.at, now });
        if (!started) {
          skippedStarts++;
        } else if (started.created) {
          production.started++;
        }
      } catch (error) {
        // The store refused the start: tried again (after a backoff) by the next production, a few times, so a start
        // that can never be stored doesn't hold the schedule up forever.
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

    // An occurrence that couldn't start (its payload threw, or the store kept refusing it) doesn't count toward `limit`.
    const runs = st.runs - skippedStarts;
    const done: ScheduleState = { ...st, runs, next: st.next === null && !limitReached(spec, runs) ? nextOccurrence(spec, now) : st.next, pending: retries };
    const backoff = Math.min(1_000 * 2 ** (Math.max(0, ...retries.map((start) => start.attempts ?? 0)) - 1), 60_000);
    const due = retries.length > 0 ? Math.min(now + backoff, wakeAt({ ...done, pending: [] }, now) ?? Infinity) : wakeAt(done, now);
    await this.encoded.writeSchedule(record.id, token, { now, state: done, wakeAt: due, release: true });
    for (const skip of skipped) {
      this.options.skipped?.(record, skip, now);
    }
    return production;
  }
}

/** What the code declares: the stored spec without its bookkeeping. */
function declaration({ declaredBy: _declaredBy, confirmedAt: _confirmedAt, ...spec }: DeclaredSpec): ScheduleSpec {
  return spec;
}

/** Whether a process whose code declares the schedule confirmed it lately: the code of a running process declares it. */
function confirmed(record: ScheduleRecord, now: number): boolean {
  const { confirmedAt } = record.spec as DeclaredSpec;
  return confirmedAt !== undefined && now - confirmedAt < STALE_MS;
}

function fieldsOf(record: ScheduleRecord) {
  const { target, declared, spec, payload, paused, wakeAt, state } = record;
  return { target, declared, spec, payload, paused, wakeAt, state };
}

/** When a worker next has something to do for the schedule: now for starts in flight or a buffered one (checked at every production), else its next occurrence. */
function wakeAt(state: ScheduleState, now: number): number | null {
  return state.pending.length > 0 || state.buffered !== null ? now : state.next;
}

function limitReached(spec: ScheduleSpec, runs: number): boolean {
  return spec.limit !== null && runs >= spec.limit;
}

/**
 * The due occurrences in `[from, now]` to start, by `spec.missed`, and the ones it passes over (as a range). Only the
 * latest can be on time; `'skip'` starts it if it is, `'once'` either way, and `'all'` the latest 100.
 */
function dueOccurrences(spec: ScheduleSpec, from: number, now: number): { start: number[]; skipped: { from: number; to: number } | null } {
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
