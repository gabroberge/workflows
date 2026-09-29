import type { SerializedWorkflowError, WorkflowJournalEntry, WorkflowParentClose, WorkflowStatus } from '../../../lib/index.js';
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
    /** The instance that started this one with ctx.startChild(). */
    parentId: text('parent_id'),
    parentClose: text('parent_close').$type<WorkflowParentClose>(),
    /** The key its workflow's per-key concurrency limit counts it under. */
    concurrencyKey: text('concurrency_key'),
    /** The key its workflow's per-key rate limit counts it under. */
    rateLimitKey: text('rate_limit_key'),
    /** Lower is claimed first; 0 (none given) before every other. */
    priority: integer('priority').notNull().default(0),
    status: text('status').$type<WorkflowStatus>().notNull(),
    input: jsonb('input'),
    output: jsonb('output'),
    error: jsonb('error').$type<SerializedWorkflowError>(),
    wakeAt: bigint('wake_at', { mode: 'number' }),
    leaseToken: text('lease_token'),
    leaseOwner: text('lease_owner'),
    leaseUntil: bigint('lease_until', { mode: 'number' }),
    cancelRequested: boolean('cancel_requested').notNull().default(false),
    terminateRequested: boolean('terminate_requested').notNull().default(false),
    cancelReason: text('cancel_reason'),
    deadline: bigint('deadline', { mode: 'number' }),
    customStatus: jsonb('custom_status'),
    signalCursor: bigint('signal_cursor', { mode: 'number' }).notNull(),
    runs: integer('runs').notNull().default(0),
    createdAt: bigint('created_at', { mode: 'number' }).notNull(),
    updatedAt: bigint('updated_at', { mode: 'number' }).notNull(),
  },
  (t) => [
    // What claims look for: due instances.
    index('workflow_instances_due').on(t.wakeAt).where(sql`${t.wakeAt} IS NOT NULL`),
    index('workflow_instances_created').on(t.createdAt, t.id),
    // What purges and list() by status look for.
    index('workflow_instances_status').on(t.status, t.updatedAt),
    // What a claim counts for a concurrency limit: live leases, by workflow and key.
    index('workflow_instances_leased').on(t.workflow, t.concurrencyKey).where(sql`${t.leaseUntil} IS NOT NULL`),
    // What list({ parentId }) and closing a parent's children look for.
    index('workflow_instances_parent').on(t.parentId, t.createdAt).where(sql`${t.parentId} IS NOT NULL`),
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

/**
 * The rate-limit windows claims count: the workflow's own (`key` '', which no instance key is) and one per key.
 * A window whose `window_end` passed is over; the next claim starts a new one.
 */
export const workflowRateLimits = pgTable(
  'workflow_rate_limits',
  {
    workflow: text('workflow').notNull(),
    key: text('key').notNull(),
    windowEnd: bigint('window_end', { mode: 'number' }).notNull(),
    count: integer('count').notNull(),
  },
  // What purges look for: windows that ended.
  (t) => [primaryKey({ columns: [t.workflow, t.key] }), index('workflow_rate_limits_end').on(t.windowEnd)],
);

export const workflowSignals = pgTable(
  'workflow_signals',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().generatedAlwaysAsIdentity(),
    name: text('name').notNull(),
    key: text('key'),
    /** The sender's id for the signal: stored once per name. */
    dedupeId: text('dedupe_id'),
    payload: jsonb('payload'),
    createdAt: bigint('created_at', { mode: 'number' }).notNull(),
  },
  (t) => [index('workflow_signals_lookup').on(t.name, t.key, t.id), unique('workflow_signals_dedupe').on(t.name, t.dedupeId)],
);
