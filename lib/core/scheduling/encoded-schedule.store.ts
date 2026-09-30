import type { LoggerService } from '@nestjs/common';
import { isEncodedPayload, type PayloadCodecs } from '../codecs/payload-codecs.js';
import type { PayloadContext } from '../interfaces/payload-codec.interface.js';
import type {
  ScheduleClaimRequest,
  ScheduleQuery,
  ScheduleRecord,
  ScheduleSave,
  ScheduleStore,
  ScheduleWrite,
} from '../interfaces/schedule-store.interface.js';

/**
 * @internal A `ScheduleStore` as the `Scheduler` sees it: payloads encoded on the way in and decoded on the way out,
 * so the store sees only what the codec returns. A claimed schedule no codec can read any more is left out of the
 * claim (logged) and handed back, due when its lease would have ended, and read once the codec is back.
 */
export class EncodedScheduleStore implements ScheduleStore {
  constructor(
    readonly inner: ScheduleStore,
    private readonly codecs: PayloadCodecs,
    private readonly context: (schedule: string) => PayloadContext,
    private readonly logger: Pick<LoggerService, 'error'>,
  ) {}

  /** Whether a codec encodes what is written. */
  get encodes(): boolean {
    return this.codecs.encodes;
  }

  async saveSchedule(save: ScheduleSave): Promise<ScheduleRecord | null> {
    const payload = await this.codecs.encode(save.payload, this.context(save.id));
    const saved = await this.inner.saveSchedule({ ...save, payload });
    return saved && this.decode(saved);
  }

  async getSchedule(id: string): Promise<ScheduleRecord | null> {
    const record = await this.inner.getSchedule(id);
    return record && this.decode(record);
  }

  async listSchedules(query: ScheduleQuery): Promise<ScheduleRecord[]> {
    return Promise.all((await this.inner.listSchedules(query)).map((record) => this.decode(record)));
  }

  deleteSchedule(id: string, revision?: number): Promise<boolean> {
    return this.inner.deleteSchedule(id, revision);
  }

  async claimSchedules(request: ScheduleClaimRequest): Promise<ScheduleRecord[]> {
    const decoded: ScheduleRecord[] = [];
    for (const record of await this.inner.claimSchedules(request)) {
      try {
        decoded.push(await this.decode(record));
      } catch (error) {
        this.logger.error(`Schedule "${record.id}" can't be read, so it isn't run; it is claimed again later. ${(error as Error).message}`);
        // Handed back untouched, due when its lease would have ended; at worst its lease expires, as if its worker died.
        await this.inner.writeSchedule(record.id, request.token, { now: request.now, state: record.state, wakeAt: request.leaseUntil, release: true }).catch(() => undefined);
      }
    }
    return decoded;
  }

  writeSchedule(id: string, token: string, write: ScheduleWrite): Promise<boolean> {
    return this.inner.writeSchedule(id, token, write);
  }

  /** A schedule as stored, with its payload decoded. Throws if no listed codec can decode it. */
  async decode(record: ScheduleRecord): Promise<ScheduleRecord> {
    return isEncodedPayload(record.payload) ? { ...record, payload: await this.codecs.decode(record.payload, this.context(record.id)) } : record;
  }
}
