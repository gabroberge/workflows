/** A value, or a promise of it: what a synchronous codec keeps synchronous. */
export type Maybe<T> = T | Promise<T>;

/**
 * `next(value)`, at once when `value` isn't a promise: nothing is awaited before a synchronous codec's result. (Not
 * named `then`: a module that exports a `then` is a thenable, which a dynamic import awaits.)
 */
export function andThen<T, R>(value: Maybe<T>, next: (value: T) => Maybe<R>): Maybe<R> {
  return thenable(value) ? Promise.resolve(value).then(next) : next(value as T);
}

export function toPromise<T>(value: Maybe<T>): Promise<T> {
  return Promise.resolve(value);
}

/** A promise, from this realm or not (a codec's library may bring its own). */
function thenable<T>(value: Maybe<T>): value is Promise<T> {
  return typeof (value as { then?: unknown } | null)?.then === 'function';
}
