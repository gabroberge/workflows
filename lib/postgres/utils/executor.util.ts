/** How an error message names a value someone passed: its class, or what it is instead of an object. */
export function describeValue(value: unknown): string {
  if (value === null || value === undefined) {
    return String(value);
  }
  if (typeof value === 'function') {
    return `the function ${value.name || '(anonymous)'}`;
  }
  if (typeof value !== 'object') {
    return `a ${typeof value}`;
  }

  const name = (value as object).constructor?.name;
  return name && name !== 'Object' ? `a ${name}` : 'an object';
}

/** `value[method]` is a function. */
export function hasMethod(value: unknown, method: string): boolean {
  return typeof value === 'object' && value !== null && typeof (value as Record<string, unknown>)[method] === 'function';
}

/** The SQL of an isolation level, checked: it goes into a statement's text. */
export function isolationSql(level: string): string {
  if (!['read uncommitted', 'read committed', 'repeatable read', 'serializable'].includes(level)) {
    throw new TypeError(`Unknown isolation level ${JSON.stringify(level)}: use 'read committed', 'repeatable read', 'serializable' or 'read uncommitted'.`);
  }
  return level.toUpperCase();
}
