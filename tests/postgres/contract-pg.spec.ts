/**
 * The WorkflowStore contract (`@nestjs/workflows/testing`) on PostgresWorkflowStore through pgClient's executor on
 * PostgreSQL, races included: a pool of real connections, and the ORM's own transactions for the in-transaction cases.
 */
import { describeContract, pgClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('pgstore_contract_pg');

describeContract(`PostgresWorkflowStore through ${pgClient.name} on PostgreSQL`, async () => (database ? pgClient.open(database.url) : null), 'nest_workflows', reason);
