/**
 * The WorkflowStore contract (`@nestjs/workflows/testing`) on MySqlWorkflowStore through typeOrmClient's executor on
 * MySQL, races included: a pool of real connections, and the application's own transactions (TypeORM's `EntityManager`,
 * REPEATABLE READ) for the in-transaction cases.
 */
import { describeContract, typeOrmClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('contract_typeorm');

describeContract(`MySqlWorkflowStore through ${typeOrmClient.name} on MySQL`, async () => (database ? typeOrmClient.open(database.url) : null), 'nest_workflows', reason);
