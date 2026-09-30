/**
 * Every parameter goes to the driver as a string (or `null`) and is cast in the statement, and every column comes back
 * cast to text: node-postgres, PGlite, Prisma and the rest parse and serialize values in their own ways (a `bigint` is
 * a string to one and a `BigInt` to another, a JSON string param is JSON to one and text to another), and text is
 * the one type they all pass through as it is.
 */
export class SqlParams {
  readonly values: Array<string | null> = [];

  text(value: string | null): string {
    return this.add(value, 'text');
  }

  int(value: number | null): string {
    return this.add(value === null ? null : String(value), 'integer');
  }

  bigint(value: number | null): string {
    return this.add(value === null ? null : String(value), 'bigint');
  }

  bool(value: boolean): string {
    return this.add(String(value), 'boolean');
  }

  /** JSON, stored as `jsonb`; `null` and `undefined` as SQL `NULL`. */
  json(value: unknown): string {
    return this.add(value === null || value === undefined ? null : JSON.stringify(value), 'jsonb');
  }

  /** `column = value`, or `column IS NULL` for `null`: keys match exactly, and `NULL = NULL` isn't true. */
  equals(column: string, value: string | null): string {
    return value === null ? `${column} IS NULL` : `${column} = ${this.text(value)}`;
  }

  /** `column IN (...)` of the given values; `FALSE` for none. */
  in(column: string, values: readonly string[]): string {
    return values.length === 0 ? 'FALSE' : `${column} IN (${values.map((value) => this.text(value)).join(', ')})`;
  }

  private add(value: string | null, type: string): string {
    this.values.push(value);
    return type === 'text' ? `$${this.values.length}::text` : `$${this.values.length}::text::${type}`;
  }
}

/** A schema name the store accepts: an unquoted-style identifier, so no quoting or `$` can surprise anyone. */
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

/** `"schema"`, after checking the name. */
export function quoteSchema(schema: unknown): string {
  if (typeof schema !== 'string' || !IDENTIFIER.test(schema)) {
    throw new TypeError(
      `PostgresWorkflowStore: invalid schema ${JSON.stringify(schema)}. Use letters, digits and underscores, not starting with a digit, at most 63 characters.`,
    );
  }
  return `"${schema}"`;
}

/** A text column as the store reads it. */
export function toText(value: unknown): string | null {
  return value === null || value === undefined ? null : String(value);
}

/** An integer column read as text (`::text`). */
export function toInt(value: unknown): number | null {
  return value === null || value === undefined ? null : Number(value);
}

/** A boolean column read as text (`::text`). */
export function toBool(value: unknown): boolean {
  return value === true || value === 'true';
}

/** A `jsonb` column read as text (`::text`); SQL `NULL` is `null`. */
export function toJson(value: unknown): unknown {
  return value === null || value === undefined ? null : JSON.parse(String(value));
}
