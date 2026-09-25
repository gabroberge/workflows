import { AsyncLocalStorage } from 'node:async_hooks';

/** The step attempt whose function is running, in that function's async context. */
interface StepScope {
  idempotencyKey: string;
  /** Signals sent so far in this attempt, per name and key. */
  signals: Map<string, number>;
  /** Workflows started so far in this attempt, per workflow name. */
  starts: Map<string, number>;
}

const scope = new AsyncLocalStorage<StepScope>();

/** Runs a step attempt's function, so the signals it sends and the workflows it starts can derive their ids from the step. */
export function runInStepScope<T>(idempotencyKey: string, fn: () => T): T {
  return scope.run({ idempotencyKey, signals: new Map(), starts: new Map() }, fn);
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

/**
 * The id of a workflow started without one from inside a step: the step's `idempotencyKey`, the
 * workflow's name, and how many instances of that workflow the attempt started before it. A
 * retried attempt starts the same ids in the same order, so it gets its first instances back
 * instead of new ones. `undefined` outside a step.
 */
export function stepStartId(workflow: string): string | undefined {
  const step = scope.getStore();
  if (!step) {
    return undefined;
  }

  const n = (step.starts.get(workflow) ?? 0) + 1;
  step.starts.set(workflow, n);
  return JSON.stringify([step.idempotencyKey, workflow, n]);
}
