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
 * stored as it is (without a codec), and the codec's id picks the codec that decodes it. A payload stored as it is
 * can't start with the prefix: a string that does (your data, or a signal someone sent) is stored as a `plain`
 * envelope of its JSON instead, so no payload is ever mistaken for another's envelope.
 */
const ENVELOPE = '$wf1:';

/** The engine's own envelope for a string that starts like one, when no codec encodes it: its JSON. */
const PLAIN: WorkflowPayloadCodec = {
  id: 'plain',
  encode: (value) => JSON.stringify(value),
  decode: (data) => JSON.parse(data),
};

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
      if (typeof codec.id !== 'string' || !/^[\w.-]+$/.test(codec.id) || codec.id === PLAIN.id) {
        throw new TypeError(
          `WorkflowsModule's codec: ${codec.constructor.name} has an invalid id ${JSON.stringify(codec.id)}. Use letters, digits, ".", "_" and "-" ` +
            `("${PLAIN.id}" is the engine's).`,
        );
      }
      if (ids.has(codec.id)) {
        throw new TypeError(`WorkflowsModule's codec: two codecs have the id "${codec.id}". Give each its own.`);
      }
      ids.add(codec.id);
    }

    this.writer = codecs[0];
    this.byId = new Map([...codecs, PLAIN].map((codec) => [codec.id, codec]));
  }

  /** Whether a codec encodes what is written (else payloads are stored as they are). */
  get encodes(): boolean {
    return this.writer !== undefined;
  }

  encode(value: unknown, context: WorkflowPayloadContext): Maybe<unknown> {
    const writer = this.writer ?? (encoded(value) ? PLAIN : undefined);
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

  /**
   * An error with its message and stack (and its compensation's) encoded, its name as it is. Without a codec, as it
   * is, unless its message starts like an envelope.
   */
  encodeError<E extends SerializedWorkflowError | null | undefined>(error: E, context: WorkflowPayloadContext): Maybe<E> {
    if (error === null || error === undefined || (!this.writer && !encoded(error.message) && !encoded(error.compensation?.message))) {
      return error;
    }

    const secret = { message: error.message, ...(error.stack === undefined ? {} : { stack: error.stack }) };
    const writer = this.writer ?? PLAIN;
    return then(then(writer.encode(secret, context), (data) => `${ENVELOPE}${writer.id}:${data}`), (message) =>
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
  private readonly unreadable = new Set<string>();
  readonly createInTransaction?: WorkflowStore['createInTransaction'];
  readonly signalInTransaction?: WorkflowStore['signalInTransaction'];

  constructor(
    /** The store itself: what it holds, undecoded. */
    readonly inner: WorkflowStore,
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

  /** Whether a codec encodes what is written. */
  get encodes(): boolean {
    return this.codecs.encodes;
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
    const reason = (await this.codecs.encode(request.reason, { field: 'cancelReason', instanceId: id })) as string | null;
    return this.inner.requestCancel(id, { ...request, reason });
  }

  async reopen(id: string, reopen: WorkflowReopen): Promise<boolean> {
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
    return this.readable(await this.inner.claimSchedules(request), (record) => this.decodeSchedule(record), 'Schedule', async (record) => {
      // Handed back untouched, due when its lease would have ended.
      await this.inner.writeSchedule(record.id, request.token, { now: request.now, state: record.state, wakeAt: request.leaseUntil, release: true });
    });
  }

  writeSchedule(id: string, token: string, write: WorkflowScheduleWrite): Promise<boolean> {
    return this.inner.writeSchedule(id, token, write);
  }

  async claim(request: WorkflowClaimRequest): Promise<WorkflowClaim> {
    const claim = await this.inner.claim(request);
    const instances = await this.readable(claim.instances, (instance) => this.decodeInstance(instance), 'Instance', (instance) =>
      this.handBack(instance.id, request.token, { now: request.now, leaseUntil: request.leaseUntil, signalCursor: claim.lastSignalId }),
    );
    return { ...claim, instances };
  }

  /**
   * Hands a leased instance back untouched, with its waits, due when its lease would have ended: its concurrency slot
   * is free at once, and the instances due before then go first, instead of this one taking the slot at every claim.
   */
  async handBack(id: string, token: string, lease: { now: number; leaseUntil: number; signalCursor: number }): Promise<void> {
    const waits = (await this.inner.get(id))?.waits ?? [];
    await this.inner.write(id, token, {
      now: lease.now,
      entries: [],
      release: { wakeAt: lease.leaseUntil, waits, signalCursor: lease.signalCursor },
    });
  }

  renew(id: string, token: string, leaseUntil: number) {
    return this.inner.renew(id, token, leaseUntil);
  }

  async write(id: string, token: string, write: WorkflowWrite): Promise<boolean> {
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

  /**
   * An instance as stored, decoded; one no listed codec can decode any more, without its payloads (its error's name
   * kept), logged once: a listing shows it instead of failing whole.
   */
  async readableInstance<T extends WorkflowInstance>(instance: T): Promise<T> {
    try {
      return await this.decodeInstance(instance);
    } catch (error) {
      if (!this.unreadable.has(instance.id)) {
        this.unreadable.add(instance.id);
        EncodedWorkflowStore.logger.warn(`Instance "${instance.id}" is listed without its payloads, which can't be read: ${(error as Error).message}`);
      }
      const { name } = instance.error ?? {};
      return { ...instance, input: undefined, output: undefined, customStatus: null, cancelReason: null, error: name === undefined ? null : { name, message: '(unreadable)' } };
    }
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

  /** A schedule as stored, with its input decoded. Throws if no listed codec can decode it. */
  async decodeSchedule(record: WorkflowScheduleRecord): Promise<WorkflowScheduleRecord> {
    return encoded(record.input) ? { ...record, input: await this.codecs.decode(record.input, { field: 'input', schedule: record.id }) } : record;
  }

  private async instanceOf(stored: Maybe<{ instance: WorkflowInstance; created: boolean }>): Promise<{ instance: WorkflowInstance; created: boolean }> {
    const { instance, created } = await stored;
    return { instance: await this.decodeInstance(instance), created };
  }

  /**
   * What a claim leased, decoded. One that can't be (a codec that is gone) is left out, logged, and handed back to
   * be claimed again later, and read once the codec is back, while the others run.
   */
  private async readable<T extends { id: string }>(claimed: T[], decode: (item: T) => Promise<T>, kind: string, handBack: (item: T) => Promise<unknown>): Promise<T[]> {
    const decoded: T[] = [];
    for (const item of claimed) {
      try {
        decoded.push(await decode(item));
      } catch (error) {
        EncodedWorkflowStore.logger.error(`${kind} "${item.id}" can't be read, so it isn't run; it is claimed again later. ${(error as Error).message}`);
        // At worst its lease expires, as if its worker had died.
        await handBack(item).catch(() => undefined);
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
  return thenable(value) ? Promise.resolve(value).then(next) : next(value as T);
}

function toPromise<T>(value: Maybe<T>): Promise<T> {
  return Promise.resolve(value);
}

/** A promise, from this realm or not (a codec's library may bring its own). */
function thenable<T>(value: Maybe<T>): value is Promise<T> {
  return typeof (value as { then?: unknown } | null)?.then === 'function';
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
