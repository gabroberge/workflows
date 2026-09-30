/**
 * Where a payload is stored, which a codec receives with it, the same to encode and to decode: `field` names what
 * holds it, and the other keys say where that is (ids). A codec that authenticates the payload (`AesGcmPayloadCodec`
 * does) binds it to its context, and refuses it once it was moved to another place.
 *
 * ```ts
 * const context: PayloadContext = { field: 'data', queue: 'emails', job: 'order-confirmation-42' };
 * ```
 */
export interface PayloadContext {
  /** The field that holds the payload, such as `'input'` or `'data'`. */
  readonly field: string;
  /** Where that field is: the ids of what holds it, such as `instanceId` and `entry`, or `queue` and `job`. */
  readonly [key: string]: string | undefined;
}

/**
 * Encodes what a package stores for the application before its store sees it, to encrypt or compress it, and decodes
 * it on the way back. `PayloadCodecs` wraps what it returns in the family's envelope, with its `id`, so a codec that
 * was replaced keeps decoding what it encoded. `@nestjs/workflows` takes one as `WorkflowPayloadCodec`.
 *
 * ```ts
 * class Base64Codec implements PayloadCodec {
 *   readonly id = 'base64';
 *
 *   encode(value: unknown): string {
 *     return Buffer.from(JSON.stringify(value)).toString('base64');
 *   }
 *
 *   decode(data: string): unknown {
 *     return JSON.parse(Buffer.from(data, 'base64').toString());
 *   }
 * }
 * ```
 */
export interface PayloadCodec<C extends PayloadContext = PayloadContext> {
  /**
   * Stored with every payload the codec encodes, to pick the codec that decodes it: letters, digits, `.`, `_` and
   * `-`, such as `'aes-256-gcm'` (`plain` is the envelope's own). Keep it when you change the codec's options; change
   * it with a codec that can't read what this one wrote.
   */
  readonly id: string;
  /**
   * `value` as a string (it is JSON-safe: what `JSON.parse(JSON.stringify(...))` gives). Synchronous where it can be:
   * an asynchronous codec delays a write in the application's transaction past its first await, which a driver whose
   * transactions run synchronously can't take.
   */
  encode(value: unknown, context: C): string | Promise<string>;
  /** The value `encode()` got (or a promise of it), from what it returned. Throw if `data` isn't what it encoded for `context`. */
  decode(data: string, context: C): unknown;
}

/**
 * An error as the family stores it: JSON-safe, its class's `name` kept apart from what may hold the application's
 * data (`message`, `stack`), which `PayloadCodecs.encodeError()` encodes. A nested error (workflows' `compensation`)
 * is another `SerializedError` in a field of its own.
 *
 * ```ts
 * const failure: SerializedError = { name: 'CardDeclinedError', message: 'Card 4242 was declined.' };
 * ```
 */
export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
}
