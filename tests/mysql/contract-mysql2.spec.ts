/**
 * The WorkflowStore contract (`@nestjs/workflows/testing`) on MySqlWorkflowStore through mysql2Client's executor on
 * MySQL, races included: a pool of real connections, and the application's own transactions (a mysql2 connection after `beginTransaction()`,
 * REPEATABLE READ) for the in-transaction cases.
 */
import { describeContract, mysql2Client, testDatabase } from './support.js';

const { database, reason } = await testDatabase('contract_mysql2');

describeContract(`MySqlWorkflowStore through ${mysql2Client.name} on MySQL`, async () => (database ? mysql2Client.open(database.url) : null), 'nest_workflows', reason);
