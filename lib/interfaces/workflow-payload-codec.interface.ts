/**
 * Where a payload is stored, which the codec receives with it, the same to encode and to decode: a codec that
 * authenticates it (`AesGcmPayloadCodec` does) refuses a payload moved to another instance, entry or field.
 */
export interface WorkflowPayloadContext {
  /**
   * The field that holds it: of an instance (`input`, `output`, `error`, `customStatus`, `cancelReason`), of one of
   * its journal entries (`result`, `progress`, `error`, `data`), of a signal (`payload`), or of a schedule (`input`).
   */
  field: 'input' | 'output' | 'error' | 'customStatus' | 'cancelReason' | 'result' | 'progress' | 'data' | 'payload';
  /** The instance, for an instance's or a journal entry's payload. */
  instanceId?: string;
  /** The journal entry's name, for an entry's payload. */
  entry?: string;
  /** The signal's name, for a signal's payload. */
  signal?: string;
  /** The schedule's id, for a schedule's input. */
  schedule?: string;
}

/**
 * Encodes what workflows store for you before the store sees it, and decodes it on the way back: to encrypt it, or
 * compress it. The module applies it to inputs, outputs, step results, checkpoints, journal entries' data, signal
 * payloads, custom statuses, cancel reasons, schedule inputs, and errors' messages and stacks. Ids, names, keys,
 * statuses and times stay as they are: the store matches on them. `WorkflowsModule.forRoot({ codec })` takes one
 * (an instance, or a class Nest creates with its dependencies), or several: the first encodes, and each payload is
 * decoded by the codec whose `id` it was stored with, so a codec you replace keeps decoding what it encoded.
 * Payloads stored before any codec was set are read as they are.
 */
export interface WorkflowPayloadCodec {
  /**
   * Stored with every payload the codec encodes, to pick the codec that decodes it: letters, digits, `.`, `_` and
   * `-`, such as `'aes-256-gcm'`. Keep it when you change the codec's options; change it with a codec that can't
   * read what this one wrote.
   */
  readonly id: string;
  /**
   * `value` as a string (it is JSON-safe: what `JSON.parse(JSON.stringify(...))` gives). Synchronous where it can
   * be: an asynchronous codec delays `start()` and `signal()` with `{ transaction }` past their first await, which a
   * driver whose transactions run synchronously can't take.
   */
  encode(value: unknown, context: WorkflowPayloadContext): string | Promise<string>;
  /** The value `encode()` got (or a promise of it), from what it returned. Throw if `data` isn't what it encoded for `context`. */
  decode(data: string, context: WorkflowPayloadContext): unknown;
}
