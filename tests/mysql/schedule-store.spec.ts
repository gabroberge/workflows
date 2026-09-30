/**
 * The core's `ScheduleStore` contract (`scheduleStoreContract()` from `@nestjs/workflows/testing`) on MySQL: the
 * schedules of MySqlWorkflowStore as the `Scheduler` reaches them, through every MySQL executor on a pool of real
 * connections (races included). The workflow store's own suites run the same cases.
 */
import { MySqlWorkflowStore } from '../../lib/mysql/index.js';
import { scheduleStoreContract } from '../../lib/testing/index.js';
import { workflowScheduleStore } from '../../lib/utils/schedule-store.util.js';
import { clients, onMysql, testDatabase, truncate, type Client } from './support.js';

const { database, reason } = await testDatabase('mysql_schedule_store');

describe.each(clients)("MySqlWorkflowStore's schedules through $name on MySQL", (factory) => {
  let client: Client | null = null;
  beforeAll(async () => {
    if (database) {
      client = await factory.open(database.url);
      await new MySqlWorkflowStore({ executor: client.executor, schema: 'schedules' }).migrate();
    }
  });
  afterAll(() => client?.close());
  onMysql(reason);

  const cases = scheduleStoreContract(
    async () => {
      await truncate(client!.executor, 'schedules');
      const store = new MySqlWorkflowStore({ executor: client!.executor, schema: 'schedules', migrate: false });
      return workflowScheduleStore(() => store);
    },
    { concurrent: true },
  );
  for (const c of cases) {
    it(c.name, c.run);
  }
});
