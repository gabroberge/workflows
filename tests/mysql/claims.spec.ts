/**
 * Claims on MySQL, through every executor (Prisma's adapter prepares its statements on the server, the others bind on
 * the client): claims that run at once each lease their share, and together every due instance and schedule, in one
 * round each. None locks more rows than it leases: InnoDB's SKIP LOCKED under ORDER BY ... LIMIT locks every row its
 * sort reads, and the claims beside it then find nothing (the contract's claim race only checks that claims that go on
 * until nothing is left never take a row twice).
 */
import { MySqlWorkflowStore } from '../../lib/mysql/index.js';
import { clients, onMysql, testDatabase, truncate, type Client } from './support.js';

const { database, reason } = await testDatabase('mysql_claims');
const FAR = 9_000_000_000_000;
const W = { name: 'order-fulfilment', version: 1 };

describe.each(clients)('claims through $name on MySQL', (factory) => {
  let client: Client | null = null;
  let store: MySqlWorkflowStore;
  beforeAll(async () => {
    if (database) {
      client = await factory.open(database.url);
      store = new MySqlWorkflowStore({ executor: client.executor, schema: 'claims' });
      await store.migrate();
    }
  });
  afterAll(() => client?.close());
  onMysql(reason);
  beforeEach(async () => {
    if (client) {
      await truncate(client.executor, 'claims');
    }
  });

  it('lease every due instance once when six claims of ten run at once over sixty, each claim its ten', async () => {
    for (let i = 0; i < 60; i++) {
      await store.create({ id: `i${String(i).padStart(2, '0')}`, workflow: W.name, version: 1, input: null, deadline: null, now: i });
    }

    const claims = await Promise.all(
      Array.from({ length: 6 }, (_, c) => store.claim({ owner: `w${c}`, token: `t${c}`, now: 1_000, leaseUntil: FAR, limit: 10, workflows: [W] })),
    );
    expect(claims.map((claim) => claim.instances.length)).toEqual([10, 10, 10, 10, 10, 10]);
    const ids = claims.flatMap((claim) => claim.instances.map((instance) => instance.id));
    expect(new Set(ids).size).toBe(60);
    // Each by the claim that returned it.
    const leased = await store.list({ limit: 100, offset: 0 });
    const owners = new Map(claims.flatMap((claim, c) => claim.instances.map((instance) => [instance.id, `w${c}`] as const)));
    expect(leased.filter((instance) => instance.leaseOwner !== owners.get(instance.id) || instance.runs !== 1).map((instance) => instance.id)).toEqual([]);
  });

  it('lease every due schedule once when five claims of six run at once over thirty, each claim its six', async () => {
    for (let i = 0; i < 30; i++) {
      const id = `s${String(i).padStart(2, '0')}`;
      await store.saveSchedule({ id, workflow: W.name, declared: false, spec: {}, input: null, paused: false, wakeAt: i, state: {}, expectRevision: null, releaseLease: false, now: 0 });
    }

    const claims = await Promise.all(
      Array.from({ length: 5 }, (_, c) => store.claimSchedules({ owner: `w${c}`, token: `t${c}`, now: 1_000, leaseUntil: FAR, limit: 6, workflows: [W.name] })),
    );
    expect(claims.map((claimed) => claimed.length)).toEqual([6, 6, 6, 6, 6]);
    expect(new Set(claims.flat().map((schedule) => schedule.id)).size).toBe(30);
  });
});
