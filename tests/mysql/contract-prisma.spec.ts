/**
 * The WorkflowStore contract (`@nestjs/workflows/testing`) on MySqlWorkflowStore through prismaClient's executor on
 * MySQL, races included: a pool of real connections, and the application's own transactions (Prisma's transaction client,
 * REPEATABLE READ) for the in-transaction cases.
 */
import { describeContract, prismaClient, testDatabase } from './support.js';

const { database, reason } = await testDatabase('contract_prisma');

describeContract(`MySqlWorkflowStore through ${prismaClient.name} on MySQL`, async () => (database ? prismaClient.open(database.url) : null), 'nest_workflows', reason);
