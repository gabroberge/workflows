import type {
  ScheduleClaimRequest,
  ScheduleQuery,
  ScheduleRecord,
  ScheduleSave,
  ScheduleStore,
  ScheduleWrite,
} from '../interfaces/schedule-store.interface.js';

/**
 * A `ScheduleStore` in this process's memory: for tests, and for a package's in-memory store (the in-memory workflow
 * store keeps its schedules in one). Every method does its work before its first `await`, so calls never interleave,
 * which makes claims, fenced writes and conditional saves atomic without locks. Values are copied on the way in and
 * out, as a database would.
 *
 * ```ts
 * const scheduler = new Scheduler({ store: new InMemoryScheduleStore(), ... });
 * ```
 */
export class InMemoryScheduleStore implements ScheduleStore {
  private readonly schedules = new Map<string, { record: ScheduleRecord; leaseToken: string | null }>();

  async saveSchedule(save: ScheduleSave): Promise<ScheduleRecord | null> {
    const existing = this.schedules.get(save.id);
    if (save.expectRevision === null ? existing !== undefined : existing?.record.revision !== save.expectRevision) {
      return null;
    }

    const { expectRevision: _expect, releaseLease, now, ...fields } = save;
    const record: ScheduleRecord = {
      ...copy(fields),
      revision: (existing?.record.revision ?? 0) + 1,
      leaseOwner: existing?.record.leaseOwner ?? null,
      leaseUntil: releaseLease ? null : (existing?.record.leaseUntil ?? null),
      createdAt: existing?.record.createdAt ?? now,
      updatedAt: now,
    };
    this.schedules.set(save.id, { record, leaseToken: releaseLease ? null : (existing?.leaseToken ?? null) });
    return copy(record);
  }

  async getSchedule(id: string): Promise<ScheduleRecord | null> {
    const schedule = this.schedules.get(id);
    return schedule ? copy(schedule.record) : null;
  }

  async listSchedules(query: ScheduleQuery): Promise<ScheduleRecord[]> {
    return [...this.schedules.values()]
      .map((schedule) => schedule.record)
      .filter((record) => (query.target === undefined || record.target === query.target) && (query.declared === undefined || record.declared === query.declared))
      .sort((a, b) => compare(a.id, b.id))
      .slice(query.offset, query.offset + query.limit)
      .map(copy);
  }

  async deleteSchedule(id: string, revision?: number): Promise<boolean> {
    const schedule = this.schedules.get(id);
    if (!schedule || (revision !== undefined && schedule.record.revision !== revision)) {
      return false;
    }
    return this.schedules.delete(id);
  }

  async claimSchedules(request: ScheduleClaimRequest): Promise<ScheduleRecord[]> {
    const targets = new Set(request.targets);
    const due = [...this.schedules.values()]
      .filter(
        ({ record }) =>
          !record.paused &&
          record.wakeAt !== null &&
          record.wakeAt <= request.now &&
          (record.leaseUntil === null || record.leaseUntil < request.now) &&
          targets.has(record.target),
      )
      .sort((a, b) => a.record.wakeAt! - b.record.wakeAt! || compare(a.record.id, b.record.id))
      .slice(0, request.limit);

    for (const schedule of due) {
      schedule.leaseToken = request.token;
      schedule.record.leaseOwner = request.owner;
      schedule.record.leaseUntil = request.leaseUntil;
    }
    return due.map((schedule) => copy(schedule.record));
  }

  async writeSchedule(id: string, token: string, write: ScheduleWrite): Promise<boolean> {
    const schedule = this.schedules.get(id);
    if (!schedule || schedule.leaseToken === null || schedule.leaseToken !== token) {
      return false;
    }

    const { record } = schedule;
    record.state = copy(write.state);
    record.wakeAt = write.wakeAt;
    record.revision++;
    record.updatedAt = write.now;
    if (write.release) {
      schedule.leaseToken = null;
      record.leaseUntil = null;
    }
    return true;
  }
}

function copy<T>(value: T): T {
  return value === undefined ? value : structuredClone(value);
}

function compare(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}
