/** Throw from a step to fail it without retrying (a declined card, a 4xx). */
export class NonRetryableStepError extends Error {
  override name = 'NonRetryableStepError';
}
