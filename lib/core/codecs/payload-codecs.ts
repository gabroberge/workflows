import type { PayloadCodec, PayloadContext, SerializedError } from '../interfaces/payload-codec.interface.js';
import { andThen, type Maybe } from '../utils/maybe.util.js';
import { describePlace } from '../utils/payload-place.util.js';

/**
 * An encoded payload as stored: `$wf1:<codec id>:<what the codec returned>`. The prefix tells it from a payload
 * stored as it is (without a codec), and the codec's id picks the codec that decodes it. A payload stored as it is
 * can't start with the prefix: a string that does (the application's data) is stored as a `plain` envelope of its
 * JSON instead, so no payload is ever mistaken for another's envelope.
 */
const ENVELOPE = '$wf1:';

/** The envelope's own codec, for a string that starts like one when no codec encodes it: its JSON. */
const PLAIN: PayloadCodec = {
  id: 'plain',
  encode: (value) => JSON.stringify(value),
  decode: (data) => JSON.parse(data),
};

/**
 * What `PayloadCodecs` names in its errors.
 *
 * ```ts
 * new PayloadCodecs(codecs, { name: "QueuesModule's codec", type: 'QueuePayloadCodec' });
 * ```
 */
export interface PayloadCodecsOptions {
  /** The option the codecs come from, such as `"WorkflowsModule's codec"`. Default `"the module's codec"`. */
  name?: string;
  /** What a codec implements, such as `'WorkflowPayloadCodec'`. Default `'PayloadCodec'`. */
  type?: string;
}

/**
 * A package's payload codecs and the family's envelope, which stores what they encode as
 * `$wf1:<codec id>:<encoded>`, so every package encodes alike and a codec reads what another package wrote. The
 * first codec encodes; each payload is decoded by the codec whose id it carries, so a codec that was replaced keeps
 * decoding what it wrote (list it after the new one). `null` and `undefined` stay as they are. Payloads stored before
 * any codec was set are read as they are, and without codecs nothing is encoded, but a string that looks like an
 * envelope is wrapped as one (`plain`) rather than stored as it is, and an encoded payload no listed codec knows fails
 * to decode instead of being read as a string.
 *
 * ```ts
 * const codecs = new PayloadCodecs([new AesGcmPayloadCodec({ keys, current: '2026-09' })], { name: "QueuesModule's codec" });
 * const stored = await codecs.encode(job.data, { field: 'data', queue: 'emails', job: job.id });
 * const data = await codecs.decode(row.data, { field: 'data', queue: 'emails', job: row.id });
 * ```
 */
export class PayloadCodecs<C extends PayloadContext = PayloadContext> {
  private readonly writer: PayloadCodec<C> | undefined;
  private readonly byId: Map<string, PayloadCodec<C>>;
  private readonly name: string;

  constructor(codecs: PayloadCodec<C>[], options: PayloadCodecsOptions = {}) {
    this.name = options.name ?? "the module's codec";
    const ids = new Set<string>();
    for (const codec of codecs) {
      if (codec === null || typeof codec !== 'object' || typeof codec.encode !== 'function' || typeof codec.decode !== 'function') {
        throw new TypeError(`${this.name}: expected a ${options.type ?? 'PayloadCodec'} (an object with id, encode() and decode()), got ${String(codec)}.`);
      }
      if (typeof codec.id !== 'string' || !/^[\w.-]+$/.test(codec.id) || codec.id === PLAIN.id) {
        throw new TypeError(
          `${this.name}: ${codec.constructor.name} has an invalid id ${JSON.stringify(codec.id)}. Use letters, digits, ".", "_" and "-" ` +
            `("${PLAIN.id}" is the envelope's).`,
        );
      }
      if (ids.has(codec.id)) {
        throw new TypeError(`${this.name}: two codecs have the id "${codec.id}". Give each its own.`);
      }
      ids.add(codec.id);
    }

    this.writer = codecs[0];
    this.byId = new Map([...codecs, PLAIN as PayloadCodec<C>].map((codec) => [codec.id, codec]));
  }

  /** Whether a codec encodes what is written (else payloads are stored as they are). */
  get encodes(): boolean {
    return this.writer !== undefined;
  }

  /** `value` in an envelope, or as it is (no codec, `null`, `undefined`). Synchronous when the codec is. */
  encode(value: unknown, context: C): Maybe<unknown> {
    const writer = this.writer ?? (isEncodedPayload(value) ? (PLAIN as PayloadCodec<C>) : undefined);
    if (value === undefined || value === null || !writer) {
      return value;
    }
    return andThen(writer.encode(value, context), (data) => `${ENVELOPE}${writer.id}:${data}`);
  }

  /** The value in an envelope, or `stored` as it is when it isn't one. Throws for a codec that isn't listed. */
  decode(stored: unknown, context: C): Maybe<unknown> {
    if (!isEncodedPayload(stored)) {
      return stored;
    }

    const end = stored.indexOf(':', ENVELOPE.length);
    const id = stored.slice(ENVELOPE.length, end);
    const codec = this.byId.get(id);
    if (end < 0 || !codec) {
      throw new Error(
        `A payload of ${describePlace(context)} was encoded by the codec "${id}", which ${this.name} option doesn't list. ` +
          'Keep a codec listed (codec: [current, previous]) while payloads it encoded may still be read.',
      );
    }
    return codec.decode(stored.slice(end + 1), context);
  }

  /**
   * An error with its `message` and `stack` encoded together into `message`, and its `name` as it is (the error's
   * class, which the store may filter on, not the application's data); a nested error (a field holding another
   * `SerializedError`) likewise, and nothing else. Without a codec, as it is, unless a message starts like an envelope.
   */
  encodeError<E extends SerializedError | null | undefined>(error: E, context: C): Maybe<E> {
    if (error === null || error === undefined || (!this.writer && !messages(error).some(isEncodedPayload))) {
      return error;
    }

    const secret = { message: error.message, ...(error.stack === undefined ? {} : { stack: error.stack }) };
    const writer = this.writer ?? (PLAIN as PayloadCodec<C>);
    const nested = nestedErrors(error);
    return andThen(andThen(writer.encode(secret, context), (data) => `${ENVELOPE}${writer.id}:${data}`), (message) =>
      andThen(all(nested.map(([, inner]) => this.encodeError(inner, context))), (encoded) => ({
        name: error.name,
        message,
        ...Object.fromEntries(nested.map(([field], i) => [field, encoded[i]])),
      }) as E),
    );
  }

  /** What `encodeError()` encoded, decoded; an error stored as it is comes back as it is. */
  async decodeError<E extends SerializedError | null | undefined>(error: E, context: C): Promise<E> {
    if (!isEncodedPayload(error?.message)) {
      return error;
    }

    const { message, stack } = (await this.decode(error.message, context)) as { message: string; stack?: string };
    const nested = await Promise.all(nestedErrors(error).map(async ([field, inner]) => [field, await this.decodeError(inner, context)] as const));
    return { name: error.name, message, ...(stack === undefined ? {} : { stack }), ...Object.fromEntries(nested) } as E;
  }
}

/**
 * Whether a stored value is an encoded payload, in the family's envelope, rather than one stored as it is: a string
 * starting with `$wf1:`. For stores that skip decoding what needs none.
 *
 * ```ts
 * const payload = isEncodedPayload(row.data) ? await codecs.decode(row.data, context) : row.data;
 * ```
 */
export function isEncodedPayload(value: unknown): value is string {
  return typeof value === 'string' && value.startsWith(ENVELOPE);
}

/** The fields of `error` that hold nested errors (workflows' `compensation`), in their order. */
function nestedErrors(error: SerializedError): Array<[string, SerializedError]> {
  return Object.entries(error).filter(
    (entry): entry is [string, SerializedError] =>
      entry[0] !== 'name' && entry[0] !== 'message' && entry[0] !== 'stack' && isSerializedError(entry[1]),
  );
}

/** The messages of `error` and of its nested errors. */
function messages(error: SerializedError): string[] {
  return [error.message, ...nestedErrors(error).flatMap(([, inner]) => messages(inner))];
}

function isSerializedError(value: unknown): value is SerializedError {
  return typeof value === 'object' && value !== null && typeof (value as SerializedError).name === 'string' && typeof (value as SerializedError).message === 'string';
}

/** Every value, synchronously when none is a promise. */
function all<T>(values: Array<Maybe<T>>): Maybe<T[]> {
  return values.reduce<Maybe<T[]>>((done, value) => andThen(done, (list) => andThen(value, (item) => [...list, item])), [] as T[]);
}
