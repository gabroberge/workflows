import type { SerializedError } from '../core/interfaces/payload-codec.interface.js';

/** JSON-safe form of an error, as stored in the journal and on the instance. */
export interface SerializedWorkflowError extends SerializedError {
  /** For `compensation_failed`: the error of the compensation that gave up. */
  compensation?: SerializedWorkflowError;
}
