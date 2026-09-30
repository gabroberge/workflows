/**
 * The WorkflowStore contract (`@nestjs/workflows/testing`) on PostgresWorkflowStore through fromDrizzle(): on PostgreSQL
 * (a pool of real connections, races included), and on PGlite (one connection, which serializes every race).
 */
import { describeContract, drizzleClient, openPglite, testDatabase } from './support.js';

const { database, reason } = await testDatabase('pgstore_contract_drizzle');

describeContract(`PostgresWorkflowStore through ${drizzleClient.name} on PostgreSQL`, async () => (database ? drizzleClient.open(database.url) : null), 'nest_workflows', reason);

describeContract('PostgresWorkflowStore through fromDrizzle (PGlite)', openPglite, 'nest_workflows');
