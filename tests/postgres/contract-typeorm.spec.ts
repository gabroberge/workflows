/**
 * The WorkflowStore contract (`@nestjs/workflows/testing`) on PostgresWorkflowStore through typeOrmClient's executor on
 * PostgreSQL, races included: a pool of real connections, and the ORM's own transactions for the in-transaction cases.
 */
import { describeContract, typeOrmClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('pgstore_contract_typeorm');

describeContract(`PostgresWorkflowStore through ${typeOrmClient.name} on PostgreSQL`, async () => (database ? typeOrmClient.open(database.url) : null), 'nest_workflows', reason);
