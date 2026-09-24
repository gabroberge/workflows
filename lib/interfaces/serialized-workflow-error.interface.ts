/** JSON-safe form of an error, as stored in the journal and on the instance. */
export interface SerializedWorkflowError {
  name: string;
  message: string;
  stack?: string;
  /** For `compensation_failed`: the error of the compensation that gave up. */
  compensation?: SerializedWorkflowError;
}
