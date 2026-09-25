import { AsyncLocalStorage } from 'node:async_hooks';

/** The step attempt whose function is running, in that function's async context. */
interface StepScope {
  idempotencyKey: string;
  /** Signals sent so far in this attempt, per name and key. */
  signals: Map<string, number>;
}

const scope = new AsyncLocalStorage<StepScope>();

/** Runs a step attempt's function, so the signals it sends can derive their ids from the step. */
export function runInStepScope<T>(idempotencyKey: string, fn: () => T): T {
  return scope.run({ idempotencyKey, signals: new Map() }, fn);
}

/**
 * The dedupe id of a signal sent without one from inside a step, directly or through a mapped
 * event: the step's `idempotencyKey`, the key, and how many signals with this name and key the
 * attempt sent before it. A retried attempt sends the same ids in the same order, so the store
 * keeps each signal once. `undefined` outside a step.
 */
export function stepSignalId(name: string, key: string | null): string | undefined {
  const step = scope.getStore();
  if (!step) {
    return undefined;
  }

  const counter = JSON.stringify([name, key]);
  const n = (step.signals.get(counter) ?? 0) + 1;
  step.signals.set(counter, n);
  // JSON keeps the parts apart whatever they contain.
  return JSON.stringify([step.idempotencyKey, key, n]);
}
