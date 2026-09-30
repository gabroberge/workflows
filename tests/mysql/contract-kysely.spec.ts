/**
 * The WorkflowStore contract (`@nestjs/workflows/testing`) on MySqlWorkflowStore through kyselyClient's executor on
 * MySQL, races included: a pool of real connections, and the application's own transactions (Kysely's `trx`,
 * REPEATABLE READ) for the in-transaction cases.
 */
import { describeContract, kyselyClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('contract_kysely');

describeContract(`MySqlWorkflowStore through ${kyselyClient.name} on MySQL`, async () => (database ? kyselyClient.open(database.url) : null), 'nest_workflows', reason);
