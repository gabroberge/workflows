/**
 * The core's `ScheduleStore` contract (`scheduleStoreContract()` from `@nestjs/workflows/testing`) on PostgreSQL: the
 * schedules of PostgresWorkflowStore as the `Scheduler` reaches them, through fromPg() on a pool of real connections
 * (races included) and through fromDrizzle() on PGlite. The workflow store's own suites run the same cases.
 */
import { PostgresWorkflowStore } from '../../lib/postgres/index.js';
import { scheduleStoreContract } from '../../lib/testing/index.js';
import { workflowScheduleStore } from '../../lib/utils/schedule-store.util.js';
import { openPglite, pgClient, testDatabase, truncate, type Client } from './support.js';

const { database, reason } = await testDatabase('pgstore_schedule_store');

for (const [label, open, skip] of [
  [`PostgresWorkflowStore's schedules through ${pgClient.name} on PostgreSQL`, async () => (database ? pgClient.open(database.url) : null), reason],
  ["PostgresWorkflowStore's schedules through fromDrizzle (PGlite)", openPglite, undefined],
] as const) {
  describe(label, () => {
    let client: Client | null = null;
    beforeAll(async () => {
      client = await open();
      if (client) {
        await new PostgresWorkflowStore({ executor: client.executor, schema: 'nest_workflows' }).migrate();
      }
    });
    afterAll(() => client?.close());
    if (skip) {
      beforeEach((context) => context.skip(skip));
    }

    const cases = scheduleStoreContract(
      async () => {
        await truncate(client!.executor, 'nest_workflows');
        const store = new PostgresWorkflowStore({ executor: client!.executor, migrate: false });
        return workflowScheduleStore(() => store);
      },
      { concurrent: true },
    );
    for (const c of cases) {
      it(c.name, c.run);
    }
  });
}
