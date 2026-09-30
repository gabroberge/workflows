import type { PayloadContext } from '../interfaces/payload-codec.interface.js';

/** The keys of workflows' payload contexts, in the order their authenticated data lists them. */
export const WORKFLOW_PLACES = ['instanceId', 'entry', 'signal', 'schedule'] as const;

/**
 * Where a payload is, for errors: `entry "charge" of instance "order-1" (result)` for workflows' contexts, and the
 * context's keys otherwise (`job "42", queue "emails" (data)`).
 */
export function describePlace(context: PayloadContext): string {
  let where: string;
  if (context.entry !== undefined) {
    where = `entry "${context.entry}" of instance "${context.instanceId}"`;
  } else if (context.instanceId !== undefined) {
    where = `instance "${context.instanceId}"`;
  } else if (context.signal !== undefined) {
    where = `signal "${context.signal}"`;
  } else if (context.schedule !== undefined) {
    where = `schedule "${context.schedule}"`;
  } else {
    const keys = otherPlaces(context);
    where = keys.length > 0 ? keys.map((key) => `${key} "${context[key]}"`).join(', ') : 'an unnamed place';
  }
  return `${where} (${context.field})`;
}

/** The context's keys other than `field` and workflows' own, that hold a value, by name. */
export function otherPlaces(context: PayloadContext): string[] {
  return Object.keys(context)
    .filter((key) => key !== 'field' && !(WORKFLOW_PLACES as readonly string[]).includes(key) && context[key] !== undefined)
    .sort();
}
