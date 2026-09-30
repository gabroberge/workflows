/**
 * The WorkflowStore contract (`@nestjs/workflows/testing`) on PostgresWorkflowStore through prismaClient's executor on
 * PostgreSQL, races included: a pool of real connections, and the ORM's own transactions for the in-transaction cases.
 */
import { describeContract, prismaClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('pgstore_contract_prisma');

describeContract(`PostgresWorkflowStore through ${prismaClient.name} on PostgreSQL`, async () => (database ? prismaClient.open(database.url) : null), 'nest_workflows', reason);
