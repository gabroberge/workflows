/**
 * The WorkflowStore contract (`@nestjs/workflows/testing`) on PostgresWorkflowStore through kyselyClient's executor on
 * PostgreSQL, races included: a pool of real connections, and the ORM's own transactions for the in-transaction cases.
 */
import { describeContract, kyselyClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('pgstore_contract_kysely');

describeContract(`PostgresWorkflowStore through ${kyselyClient.name} on PostgreSQL`, async () => (database ? kyselyClient.open(database.url) : null), 'nest_workflows', reason);
