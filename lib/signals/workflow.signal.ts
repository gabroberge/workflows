/**
 * A named signal with a payload type, shared by the code that sends it
 * (`WorkflowClient.signal()`) and the workflows that wait for it
 * (`ctx.waitForSignal()`). One constant for both ends means the name can't be
 * misspelled on one side, and the payload is type-checked on both.
 *
 * @example
 * export const shipmentDelivered = new WorkflowSignal<ShipCoEvent>('shipment.delivered');
 */
export class WorkflowSignal<T = unknown> {
  /** Type-only: the payload type. Never set at runtime. */
  declare readonly payload?: T;

  constructor(readonly name: string) {
    assertSignalName(name);
  }
}

export function assertSignalName(name: unknown): asserts name is string {
  if (typeof name !== 'string' || name.length === 0) {
    throw new TypeError(`Invalid signal name ${JSON.stringify(name)}. Use a non-empty string such as "shipment.delivered".`);
  }
}

export function signalName(signal: WorkflowSignal<unknown> | string): string {
  const name = typeof signal === 'string' ? signal : signal?.name;
  assertSignalName(name);
  return name;
}
