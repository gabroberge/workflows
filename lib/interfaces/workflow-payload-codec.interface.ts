import type { PayloadCodec, PayloadContext } from '../core/interfaces/payload-codec.interface.js';

/**
 * Where a payload is stored, which the codec receives with it, the same to encode and to decode: a codec that
 * authenticates it (`AesGcmPayloadCodec` does) refuses a payload moved to another instance, entry or field.
 */
export interface WorkflowPayloadContext extends PayloadContext {
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
 * Payloads stored before any codec was set are read as they are. A `PayloadCodec` of `@nestjs/workflows/core` whose
 * methods take any context (`AesGcmPayloadCodec`) is one.
 */
export interface WorkflowPayloadCodec extends PayloadCodec<WorkflowPayloadContext> {}
