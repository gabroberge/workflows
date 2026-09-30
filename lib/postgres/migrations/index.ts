import { initialMigration } from './initial.migration.js';

/** Every version of the store's schema, in order. A new one goes last, and none ever changes once released. */
export const MIGRATIONS = [initialMigration];
