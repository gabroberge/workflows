import type { StoreMigration } from '../utils/migrations.util.js';

/**
 * The store's tables. Times are epoch milliseconds from the engine's clock (`bigint`); inputs, outputs, errors,
 * payloads, journal entries and schedule specs are the engine's JSON (`jsonb`).
 */
export const initialMigration: StoreMigration = {
  version: 1,
  name: 'initial',
  up: (s) => [
    `CREATE TABLE ${s}.instances (
  id text PRIMARY KEY,
  workflow text NOT NULL,
  version integer NOT NULL,
  parent_id text,
  parent_close text,
  concurrency_key text,
  rate_limit_key text,
  priority integer NOT NULL DEFAULT 0,
  schedule_id text,
  scheduled_at bigint,
  status text NOT NULL,
  input jsonb,
  output jsonb,
  error jsonb,
  wake_at bigint,
  lease_token text,
  lease_owner text,
  lease_until bigint,
  cancel_requested boolean NOT NULL DEFAULT false,
  terminate_requested boolean NOT NULL DEFAULT false,
  cancel_reason text,
  deadline bigint,
  custom_status jsonb,
  signal_cursor bigint NOT NULL,
  runs integer NOT NULL DEFAULT 0,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL
)`,
    // One row per step, sleep, wait or compensation; `seq` keeps them in first-write order.
    `CREATE TABLE ${s}.journal (
  seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  instance_id text NOT NULL REFERENCES ${s}.instances (id) ON DELETE CASCADE,
  name text NOT NULL,
  entry jsonb NOT NULL,
  CONSTRAINT journal_name UNIQUE (instance_id, name)
)`,
    // The signals a suspended instance waits for.
    `CREATE TABLE ${s}.waits (
  instance_id text NOT NULL REFERENCES ${s}.instances (id) ON DELETE CASCADE,
  position integer NOT NULL,
  signal text NOT NULL,
  key text,
  PRIMARY KEY (instance_id, position)
)`,
    `CREATE TABLE ${s}.signals (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name text NOT NULL,
  key text,
  dedupe_id text,
  payload jsonb,
  created_at bigint NOT NULL,
  CONSTRAINT signals_dedupe UNIQUE (name, dedupe_id)
)`,
    `CREATE TABLE ${s}.schedules (
  id text PRIMARY KEY,
  workflow text NOT NULL,
  declared boolean NOT NULL,
  spec jsonb NOT NULL,
  input jsonb,
  paused boolean NOT NULL,
  wake_at bigint,
  state jsonb NOT NULL,
  revision integer NOT NULL,
  lease_token text,
  lease_owner text,
  lease_until bigint,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL
)`,
    // The rate-limit windows claims count: the workflow's own (`key` '', which no instance key is) and one per key.
    `CREATE TABLE ${s}.rate_limits (
  workflow text NOT NULL,
  key text NOT NULL,
  window_end bigint NOT NULL,
  count integer NOT NULL,
  PRIMARY KEY (workflow, key)
)`,
    // What claims look for: due instances.
    `CREATE INDEX instances_due ON ${s}.instances (wake_at) WHERE wake_at IS NOT NULL`,
    `CREATE INDEX instances_created ON ${s}.instances (created_at, id)`,
    // What purges and list() by status look for.
    `CREATE INDEX instances_status ON ${s}.instances (status, updated_at)`,
    // What a claim counts for a concurrency limit: live leases, by workflow and key.
    `CREATE INDEX instances_leased ON ${s}.instances (workflow, concurrency_key) WHERE lease_until IS NOT NULL`,
    // What list({ parentId }) and closing a parent's children look for.
    `CREATE INDEX instances_parent ON ${s}.instances (parent_id, created_at) WHERE parent_id IS NOT NULL`,
    // What list({ scheduleId }) and a schedule's overlap check look for.
    `CREATE INDEX instances_schedule ON ${s}.instances (schedule_id, created_at) WHERE schedule_id IS NOT NULL`,
    `CREATE INDEX waits_signal ON ${s}.waits (signal, key)`,
    `CREATE INDEX signals_lookup ON ${s}.signals (name, key, id)`,
    // What claims look for: due schedules.
    `CREATE INDEX schedules_due ON ${s}.schedules (wake_at) WHERE wake_at IS NOT NULL`,
    `CREATE INDEX schedules_workflow ON ${s}.schedules (workflow, id)`,
    // What purges look for: windows that ended.
    `CREATE INDEX rate_limits_end ON ${s}.rate_limits (window_end)`,
  ],
};
