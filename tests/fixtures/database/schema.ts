import type { SerializedWorkflowError, WorkflowJournalEntry, WorkflowStatus } from '../../../lib/index.js';
import { sql } from 'drizzle-orm';
import { bigint, boolean, index, integer, jsonb, pgTable, primaryKey, text, unique } from 'drizzle-orm/pg-core';
import type { OrderItem, OrderStatus } from '../orders/order.js';

export const orders = pgTable('orders', {
  id: text('id').primaryKey(),
  userId: text('user_id').notNull(),
  items: jsonb('items').$type<OrderItem[]>().notNull(),
  /** In cents. */
  total: integer('total').notNull(),
  status: text('status').$type<OrderStatus>().notNull(),
});

// The workflow store's tables (DrizzleWorkflowStore). Times are epoch milliseconds from the
// engine's clock; inputs, outputs, errors, payloads and journal entries are JSON.

export const workflowInstances = pgTable(
  'workflow_instances',
  {
    id: text('id').primaryKey(),
    workflow: text('workflow').notNull(),
    version: integer('version').notNull(),
    status: text('status').$type<WorkflowStatus>().notNull(),
    input: jsonb('input'),
    output: jsonb('output'),
    error: jsonb('error').$type<SerializedWorkflowError>(),
    wakeAt: bigint('wake_at', { mode: 'number' }),
    leaseToken: text('lease_token'),
    leaseOwner: text('lease_owner'),
    leaseUntil: bigint('lease_until', { mode: 'number' }),
    cancelRequested: boolean('cancel_requested').notNull().default(false),
    cancelReason: text('cancel_reason'),
    signalCursor: bigint('signal_cursor', { mode: 'number' }).notNull(),
    runs: integer('runs').notNull().default(0),
    createdAt: bigint('created_at', { mode: 'number' }).notNull(),
    updatedAt: bigint('updated_at', { mode: 'number' }).notNull(),
  },
  (t) => [
    // What claims look for: due instances.
    index('workflow_instances_due').on(t.wakeAt).where(sql`${t.wakeAt} IS NOT NULL`),
    index('workflow_instances_created').on(t.createdAt, t.id),
  ],
);

/** One row per step, sleep, wait or compensation; `seq` keeps them in first-write order. */
export const workflowJournal = pgTable(
  'workflow_journal',
  {
    seq: bigint('seq', { mode: 'number' }).primaryKey().generatedAlwaysAsIdentity(),
    instanceId: text('instance_id')
      .notNull()
      .references(() => workflowInstances.id, { onDelete: 'cascade' }),
    name: text('name').notNull(),
    entry: jsonb('entry').$type<WorkflowJournalEntry>().notNull(),
  },
  (t) => [unique('workflow_journal_name').on(t.instanceId, t.name)],
);

/** The signals a suspended instance waits for. */
export const workflowWaits = pgTable(
  'workflow_waits',
  {
    instanceId: text('instance_id')
      .notNull()
      .references(() => workflowInstances.id, { onDelete: 'cascade' }),
    position: integer('position').notNull(),
    signal: text('signal').notNull(),
    key: text('key'),
  },
  (t) => [primaryKey({ columns: [t.instanceId, t.position] }), index('workflow_waits_signal').on(t.signal, t.key)],
);

export const workflowSignals = pgTable(
  'workflow_signals',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().generatedAlwaysAsIdentity(),
    name: text('name').notNull(),
    key: text('key'),
    payload: jsonb('payload'),
    createdAt: bigint('created_at', { mode: 'number' }).notNull(),
  },
  (t) => [index('workflow_signals_lookup').on(t.name, t.key, t.id)],
);
