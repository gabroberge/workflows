/**
 * The WorkflowStore contract (`@nestjs/workflows/testing`) on MySqlWorkflowStore through drizzleClient's executor on
 * MySQL, races included: a pool of real connections, and the application's own transactions (Drizzle's `tx`,
 * REPEATABLE READ) for the in-transaction cases.
 */
import { describeContract, drizzleClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('contract_drizzle');

describeContract(`MySqlWorkflowStore through ${drizzleClient.name} on MySQL`, async () => (database ? drizzleClient.open(database.url) : null), 'nest_workflows', reason);
