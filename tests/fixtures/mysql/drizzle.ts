// The types of the Drizzle MySQL database that DrizzleModule registers (app.module.ts).
import type { MySql2Database } from 'drizzle-orm/mysql2';
import type * as schema from './schema.js';

export type Database = MySql2Database<typeof schema>;
/** The `tx` that `db.transaction()` passes its callback. */
export type Transaction = Parameters<Parameters<Database['transaction']>[0]>[0];
