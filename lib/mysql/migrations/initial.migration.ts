import { keyColumn, type StoreMigration } from '@nestjs/store-kit/mysql';
import { MYSQL_KEY_LIMITS as L } from '../key-limits.js';

/**
 * The store's tables on MySQL, in the connection's database (`<schema>_instances`...), each created with its indexes in
 * one statement. Times are epoch milliseconds from the engine's clock (`bigint`); inputs, outputs, errors, payloads,
 * journal entries and schedule specs are the engine's JSON (`json`). Every id, name and key is a `utf8mb4_0900_bin`
 * column (`keyColumn()`), so keys that differ in case or accents stay apart, of a bounded length (`MYSQL_KEY_LIMITS`).
 * No foreign keys: a store's delete removes an instance's journal and waits in its own transaction.
 */
export const initialMigration: StoreMigration = {
  version: 1,
  name: 'initial',
  up: (t) => [
    `CREATE TABLE ${t('instances')} (
  id ${keyColumn(L.instanceId)} NOT NULL PRIMARY KEY,
  workflow ${keyColumn(L.workflow)} NOT NULL,
  version int NOT NULL,
  parent_id ${keyColumn(L.instanceId)},
  parent_close ${keyColumn(16)},
  concurrency_key ${keyColumn(L.concurrencyKey)},
  rate_limit_key ${keyColumn(L.rateLimitKey)},
  priority int NOT NULL DEFAULT 0,
  schedule_id ${keyColumn(L.scheduleId)},
  scheduled_at bigint,
  status ${keyColumn(32)} NOT NULL,
  input json,
  output json,
  error json,
  wake_at bigint,
  lease_token ${keyColumn(L.leaseToken)},
  lease_owner text,
  lease_until bigint,
  cancel_requested boolean NOT NULL DEFAULT false,
  terminate_requested boolean NOT NULL DEFAULT false,
  cancel_reason text,
  deadline bigint,
  custom_status json,
  signal_cursor bigint NOT NULL,
  runs int NOT NULL DEFAULT 0,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  KEY instances_due (wake_at),
  KEY instances_created (created_at, id),
  KEY instances_status (status, updated_at),
  KEY instances_leased (workflow, concurrency_key, lease_until),
  KEY instances_parent (parent_id, created_at),
  KEY instances_schedule (schedule_id, created_at)
)`,
    // One row per step, sleep, wait or compensation; seq keeps them in first-write order.
    `CREATE TABLE ${t('journal')} (
  seq bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  instance_id ${keyColumn(L.instanceId)} NOT NULL,
  name ${keyColumn(L.journalName)} NOT NULL,
  entry json NOT NULL,
  UNIQUE KEY journal_name (instance_id, name)
)`,
    // The signals a suspended instance waits for.
    `CREATE TABLE ${t('waits')} (
  instance_id ${keyColumn(L.instanceId)} NOT NULL,
  position int NOT NULL,
  \`signal\` ${keyColumn(L.signal)} NOT NULL,
  \`key\` ${keyColumn(L.signalKey)},
  PRIMARY KEY (instance_id, position),
  KEY waits_signal (\`signal\`, \`key\`)
)`,
    `CREATE TABLE ${t('signals')} (
  id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name ${keyColumn(L.signal)} NOT NULL,
  \`key\` ${keyColumn(L.signalKey)},
  dedupe_id ${keyColumn(L.dedupeId)},
  payload json,
  created_at bigint NOT NULL,
  UNIQUE KEY signals_dedupe (name, dedupe_id),
  KEY signals_lookup (name, \`key\`, id)
)`,
    `CREATE TABLE ${t('schedules')} (
  id ${keyColumn(L.scheduleId)} NOT NULL PRIMARY KEY,
  workflow ${keyColumn(L.workflow)} NOT NULL,
  declared boolean NOT NULL,
  spec json NOT NULL,
  input json,
  paused boolean NOT NULL,
  wake_at bigint,
  state json NOT NULL,
  revision int NOT NULL,
  lease_token ${keyColumn(L.leaseToken)},
  lease_owner text,
  lease_until bigint,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  KEY schedules_due (wake_at),
  KEY schedules_workflow (workflow, id)
)`,
    // The rate-limit windows claims count: the workflow's own (key '', which no instance key is) and one per key.
    `CREATE TABLE ${t('rate_limits')} (
  workflow ${keyColumn(L.workflow)} NOT NULL,
  \`key\` ${keyColumn(L.rateLimitKey)} NOT NULL,
  window_end bigint NOT NULL,
  \`count\` int NOT NULL,
  PRIMARY KEY (workflow, \`key\`),
  KEY rate_limits_end (window_end)
)`,
  ],
};
