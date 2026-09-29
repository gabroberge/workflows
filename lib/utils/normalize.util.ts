/**
 * Round-trips through JSON so the first run sees exactly what every replay
 * will see (a `Date` becomes a string in both, `undefined` stays `undefined`).
 */
export function normalize<T>(value: T): T {
  if (value === undefined) {
    return value;
  }
  const json = JSON.stringify(value);
  return json === undefined ? (undefined as T) : JSON.parse(json);
}
