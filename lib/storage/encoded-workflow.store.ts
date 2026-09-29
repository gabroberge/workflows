import { Logger } from '@nestjs/common';
import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';
import type { WorkflowPayloadCodec, WorkflowPayloadContext } from '../interfaces/workflow-payload-codec.interface.js';
import type { WorkflowInstance, WorkflowJournalEntry, WorkflowStatus } from '../interfaces/workflow-instance.interface.js';
import type {
  NewWorkflowInstance,
  NewWorkflowSignal,
  WorkflowCancelRequest,
  WorkflowClaim,
  WorkflowClaimRequest,
  WorkflowInstanceDetails,
  WorkflowListQuery,
  WorkflowPurgeQuery,
  WorkflowPurgeResult,
  WorkflowReopen,
  WorkflowScheduleClaimRequest,
  WorkflowScheduleQuery,
  WorkflowScheduleRecord,
  WorkflowScheduleSave,
  WorkflowScheduleWrite,
  WorkflowSignalQuery,
  WorkflowSignalRecord,
  WorkflowSignalResult,
  WorkflowStore,
  WorkflowWrite,
} from '../interfaces/workflow-store.interface.js';

type Maybe<T> = T | Promise<T>;

/**
 * An encoded payload as stored: `$wf1:<codec id>:<what the codec returned>`. The prefix tells it from a payload
 * stored before a codec was set (JSON: a string of those would need to start with it), and the codec's id picks
 * the codec that decodes it.
 */
const ENVELOPE = '$wf1:';

/**
 * @internal The module's codecs: the first encodes, and each payload is decoded by the one whose id it carries.
 * `null` and `undefined` stay as they are. Without codecs, nothing is encoded, and an encoded payload can't be read
 * (rather than being taken for a plain string).
 */
export class PayloadCodecs {
  private readonly writer: WorkflowPayloadCodec | undefined;
  private readonly byId: Map<string, WorkflowPayloadCodec>;

  constructor(codecs: WorkflowPayloadCodec[]) {
    const ids = new Set<string>();
    for (const codec of codecs) {
      if (codec === null || typeof codec !== 'object' || typeof codec.encode !== 'function' || typeof codec.decode !== 'function') {
        throw new TypeError(`WorkflowsModule's codec: expected a WorkflowPayloadCodec (an object with id, encode() and decode()), got ${String(codec)}.`);
      }
      if (typeof codec.id !== 'string' || !/^[\w.-]+$/.test(codec.id)) {
        throw new TypeError(`WorkflowsModule's codec: ${codec.constructor.name} has an invalid id ${JSON.stringify(codec.id)}. Use letters, digits, ".", "_" and "-".`);
      }
      if (ids.has(codec.id)) {
        throw new TypeError(`WorkflowsModule's codec: two codecs have the id "${codec.id}". Give each its own.`);
      }
      ids.add(codec.id);
    }

    this.writer = codecs[0];
    this.byId = new Map(codecs.map((codec) => [codec.id, codec]));
  }

  /** Whether a codec encodes: without one, writes pass as they are. */
  get encoding(): boolean {
    return this.writer !== undefined;
  }

  encode(value: unknown, context: WorkflowPayloadContext): Maybe<unknown> {
    const writer = this.writer;
    if (value === undefined || value === null || !writer) {
      return value;
    }
    return then(writer.encode(value, context), (data) => `${ENVELOPE}${writer.id}:${data}`);
  }

  decode(stored: unknown, context: WorkflowPayloadContext): Maybe<unknown> {
    if (!encoded(stored)) {
      return stored;
    }

    const end = stored.indexOf(':', ENVELOPE.length);
    const id = stored.slice(ENVELOPE.length, end);
    const codec = this.byId.get(id);
    if (end < 0 || !codec) {
      throw new Error(
        `A payload of ${where(context)} was encoded by the codec "${id}", which WorkflowsModule's codec option doesn't list. ` +
          'Keep a codec listed (codec: [current, previous]) while payloads it encoded may still be read.',
      );
    }
    return codec.decode(stored.slice(end + 1), context);
  }

  /** An error with its message and stack (and its compensation's) encoded, its name as it is. */
  encodeError<E extends SerializedWorkflowError | null | undefined>(error: E, context: WorkflowPayloadContext): Maybe<E> {
    if (error === null || error === undefined) {
      return error;
    }

    const secret = { message: error.message, ...(error.stack === undefined ? {} : { stack: error.stack }) };
    return then(this.encode(secret, context), (message) =>
      then(this.encodeError(error.compensation, context), (compensation) => ({ name: error.name, message, ...(compensation ? { compensation } : {}) }) as E),
    );
  }

  async decodeError<E extends SerializedWorkflowError | null | undefined>(error: E, context: WorkflowPayloadContext): Promise<E> {
    if (!encoded(error?.message)) {
      return error;
    }

    const { message, stack } = (await this.decode(error.message, context)) as { message: string; stack?: string };
    const compensation = await this.decodeError(error.compensation, context);
    return { name: error.name, message, ...(stack === undefined ? {} : { stack }), ...(compensation ? { compensation } : {}) } as E;
  }
}

/**
 * @internal The store as the engine sees it with a codec: payloads encoded on the way in, decoded on the way out, so
 * the store sees only what the codec returns, and the engine only plain values. Ids, names, keys, statuses and
 * times pass as they are: the store matches on them.
 */
export class EncodedWorkflowStore implements WorkflowStore {
  private static readonly logger = new Logger('Workflows');
  readonly createInTransaction?: WorkflowStore['createInTransaction'];
  readonly signalInTransaction?: WorkflowStore['signalInTransaction'];

  constructor(
    private readonly inner: WorkflowStore,
    private readonly codecs: PayloadCodecs,
  ) {
    // Only when the store has them: the client tells a store that can't join transactions by their absence.
    // Nothing is awaited before the store's call when the codec is synchronous (see WorkflowClient.start()).
    if (typeof inner.createInTransaction === 'function') {
      this.createInTransaction = (transaction, instance) =>
        this.instanceOf(then(this.encodeNew(instance), (encoded) => inner.createInTransaction!(transaction, encoded)));
    }
    if (typeof inner.signalInTransaction === 'function') {
      this.signalInTransaction = (transaction, signal) => toPromise(then(this.encodeSignal(signal), (encoded) => inner.signalInTransaction!(transaction, encoded)));
    }
  }

  create(instance: NewWorkflowInstance) {
    return this.instanceOf(then(this.encodeNew(instance), (encoded) => this.inner.create(encoded)));
  }

  async get(id: string, options?: { journal?: boolean }): Promise<WorkflowInstanceDetails | null> {
    const details = await this.inner.get(id, options);
    if (!details) {
      return null;
    }

    const decoded = await this.decodeInstance(details);
    if (details.journal?.some(entryEncoded)) {
      decoded.journal = await Promise.all(details.journal.map((entry) => this.decodeEntry(entry, id)));
    }
    return decoded;
  }

  async list(query: WorkflowListQuery): Promise<WorkflowInstance[]> {
    return Promise.all((await this.inner.list(query)).map((instance) => this.decodeInstance(instance)));
  }

  async requestCancel(id: string, request: WorkflowCancelRequest): Promise<boolean> {
    if (!this.codecs.encoding) {
      return this.inner.requestCancel(id, request);
    }
    const reason = (await this.codecs.encode(request.reason, { field: 'cancelReason', instanceId: id })) as string | null;
    return this.inner.requestCancel(id, { ...request, reason });
  }

  async reopen(id: string, reopen: WorkflowReopen): Promise<boolean> {
    if (!this.codecs.encoding) {
      return this.inner.reopen(id, reopen);
    }
    const [error, entries] = await Promise.all([this.codecs.encodeError(reopen.error, { field: 'error', instanceId: id }), this.encodeEntries(reopen.entries, id)]);
    return this.inner.reopen(id, { ...reopen, error, entries });
  }

  delete(id: string, statuses: WorkflowStatus[]): Promise<boolean> {
    return this.inner.delete(id, statuses);
  }

  signal(signal: NewWorkflowSignal): Promise<WorkflowSignalResult> {
    return toPromise(then(this.encodeSignal(signal), (encoded) => this.inner.signal(encoded)));
  }

  async signals(query: WorkflowSignalQuery): Promise<WorkflowSignalRecord[]> {
    const records = await this.inner.signals(query);
    return Promise.all(
      records.map(async (record) => (encoded(record.payload) ? { ...record, payload: await this.codecs.decode(record.payload, { field: 'payload', signal: record.name }) } : record)),
    );
  }

  purge(query: WorkflowPurgeQuery): Promise<WorkflowPurgeResult> {
    return this.inner.purge(query);
  }

  async saveSchedule(save: WorkflowScheduleSave): Promise<WorkflowScheduleRecord | null> {
    const input = await this.codecs.encode(save.input, { field: 'input', schedule: save.id });
    const saved = await this.inner.saveSchedule({ ...save, input });
    return saved && this.decodeSchedule(saved);
  }

  async getSchedule(id: string): Promise<WorkflowScheduleRecord | null> {
    const record = await this.inner.getSchedule(id);
    return record && this.decodeSchedule(record);
  }

  async listSchedules(query: WorkflowScheduleQuery): Promise<WorkflowScheduleRecord[]> {
    return Promise.all((await this.inner.listSchedules(query)).map((record) => this.decodeSchedule(record)));
  }

  deleteSchedule(id: string, revision?: number): Promise<boolean> {
    return this.inner.deleteSchedule(id, revision);
  }

  async claimSchedules(request: WorkflowScheduleClaimRequest): Promise<WorkflowScheduleRecord[]> {
    return this.readable(await this.inner.claimSchedules(request), (record) => this.decodeSchedule(record), 'Schedule');
  }

  writeSchedule(id: string, token: string, write: WorkflowScheduleWrite): Promise<boolean> {
    return this.inner.writeSchedule(id, token, write);
  }

  async claim(request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    const claim = await this.inner.claim(request);
    return { ...claim, instances: await this.readable(claim.instances, (instance) => this.decodeInstance(instance), 'Instance') };
  }

  renew(id: string, token: string, leaseUntil: number) {
    return this.inner.renew(id, token, leaseUntil);
  }

  async write(id: string, token: string, write: WorkflowWrite): Promise<boolean> {
    if (!this.codecs.encoding) {
      return this.inner.write(id, token, write);
    }
    const context = (field: WorkflowPayloadContext['field']): WorkflowPayloadContext => ({ field, instanceId: id });
    const [entries, output, error, customStatus, signal] = await Promise.all([
      this.encodeEntries(write.entries, id),
      this.codecs.encode(write.output, context('output')),
      this.codecs.encodeError(write.error, context('error')),
      this.codecs.encode(write.customStatus, context('customStatus')),
      write.signal ? this.encodeSignal(write.signal) : undefined,
    ]);
    return this.inner.write(id, token, {
      ...write,
      entries,
      ...(write.output === undefined ? {} : { output }),
      ...(write.error === undefined ? {} : { error }),
      ...(write.customStatus === undefined ? {} : { customStatus }),
      ...(signal ? { signal } : {}),
    });
  }

  // ---------------------------------------------------------------- payloads

  private encodeNew(instance: NewWorkflowInstance): Maybe<NewWorkflowInstance> {
    return then(this.codecs.encode(instance.input, { field: 'input', instanceId: instance.id }), (input) => ({ ...instance, input }));
  }

  private encodeSignal(signal: NewWorkflowSignal): Maybe<NewWorkflowSignal> {
    return then(this.codecs.encode(signal.payload, { field: 'payload', signal: signal.name }), (payload) => ({ ...signal, payload }));
  }

  private encodeEntries(entries: WorkflowJournalEntry[], instanceId: string): Promise<WorkflowJournalEntry[]> {
    return Promise.all(
      entries.map(async (entry) => {
        const context = (field: WorkflowPayloadContext['field']): WorkflowPayloadContext => ({ field, instanceId, entry: entry.name });
        const [result, progress, data, error] = await Promise.all([
          this.codecs.encode(entry.result, context('result')),
          this.codecs.encode(entry.progress, context('progress')),
          this.codecs.encode(entry.data, context('data')),
          this.codecs.encodeError(entry.error, context('error')),
        ]);
        return { ...entry, result, progress, data, error };
      }),
    );
  }

  private async decodeEntry(entry: WorkflowJournalEntry, instanceId: string): Promise<WorkflowJournalEntry> {
    if (!entryEncoded(entry)) {
      return entry;
    }

    const context = (field: WorkflowPayloadContext['field']): WorkflowPayloadContext => ({ field, instanceId, entry: entry.name });
    const [result, progress, data, error] = await Promise.all([
      this.codecs.decode(entry.result, context('result')),
      this.codecs.decode(entry.progress, context('progress')),
      this.codecs.decode(entry.data, context('data')),
      this.codecs.decodeError(entry.error, context('error')),
    ]);
    // Only the fields the entry has: a store keeps `null` and `undefined` apart, and so does the engine.
    return {
      ...entry,
      ...('result' in entry ? { result } : {}),
      ...('progress' in entry ? { progress } : {}),
      ...('data' in entry ? { data } : {}),
      ...('error' in entry ? { error } : {}),
    };
  }

  private async decodeInstance<T extends WorkflowInstance>(instance: T): Promise<T> {
    if (![instance.input, instance.output, instance.error?.message, instance.customStatus, instance.cancelReason].some(encoded)) {
      return instance;
    }

    const context = (field: WorkflowPayloadContext['field']): WorkflowPayloadContext => ({ field, instanceId: instance.id });
    const [input, output, error, customStatus, cancelReason] = await Promise.all([
      this.codecs.decode(instance.input, context('input')),
      this.codecs.decode(instance.output, context('output')),
      this.codecs.decodeError(instance.error, context('error')),
      this.codecs.decode(instance.customStatus, context('customStatus')),
      this.codecs.decode(instance.cancelReason, context('cancelReason')),
    ]);
    return { ...instance, input, ...('output' in instance ? { output } : {}), error, customStatus, cancelReason: cancelReason as string | null };
  }

  private async decodeSchedule(record: WorkflowScheduleRecord): Promise<WorkflowScheduleRecord> {
    return encoded(record.input) ? { ...record, input: await this.codecs.decode(record.input, { field: 'input', schedule: record.id }) } : record;
  }

  private async instanceOf(stored: Maybe<{ instance: WorkflowInstance; created: boolean }>): Promise<{ instance: WorkflowInstance; created: boolean }> {
    const { instance, created } = await stored;
    return { instance: await this.decodeInstance(instance), created };
  }

  /**
   * What a claim leased, decoded; one that can't be (a codec that is gone) is left out, logged, and stays leased
   * until its lease expires: claimed again then, and read once the codec is back, while the others run.
   */
  private async readable<T extends { id: string }>(claimed: T[], decode: (item: T) => Promise<T>, kind: string): Promise<T[]> {
    const decoded: T[] = [];
    for (const item of claimed) {
      try {
        decoded.push(await decode(item));
      } catch (error) {
        EncodedWorkflowStore.logger.error(`${kind} "${item.id}" can't be read, so it isn't run; it is claimed again when its lease expires. ${(error as Error).message}`);
      }
    }
    return decoded;
  }
}

/** Whether a stored value is an encoded payload (the rest was stored as it is). */
function encoded(value: unknown): value is string {
  return typeof value === 'string' && value.startsWith(ENVELOPE);
}

function entryEncoded(entry: WorkflowJournalEntry): boolean {
  return encoded(entry.result) || encoded(entry.progress) || encoded(entry.data) || encoded(entry.error?.message);
}

function then<T, R>(value: Maybe<T>, next: (value: T) => Maybe<R>): Maybe<R> {
  return value instanceof Promise ? value.then(next) : next(value);
}

function toPromise<T>(value: Maybe<T>): Promise<T> {
  return value instanceof Promise ? value : Promise.resolve(value);
}

function where(context: WorkflowPayloadContext): string {
  const holder =
    context.entry !== undefined
      ? `entry "${context.entry}" of instance "${context.instanceId}"`
      : context.instanceId !== undefined
        ? `instance "${context.instanceId}"`
        : context.signal !== undefined
          ? `signal "${context.signal}"`
          : `schedule "${context.schedule}"`;
  return `${holder} (${context.field})`;
}
