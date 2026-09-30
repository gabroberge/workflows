/**
 * `start()` and `signal()` with `{ transaction }` through PostgresWorkflowStore, with each client's own transaction
 * object (node-postgres's client after BEGIN, Drizzle's `tx`, TypeORM's `EntityManager`, Prisma's transaction client,
 * Kysely's `trx`): the instance and the signal commit with the application's row, or roll back with it; anything else
 * passed as the transaction is refused with a TypeError that says what to pass; so is a signal in a transaction that
 * isn't READ COMMITTED.
 */
import { Test, type TestingModule } from '@nestjs/testing';
import { Workflow, WorkflowClient, WorkflowsModule, WorkflowStorage, WorkflowWorker, type WorkflowContext } from '../../lib/index.js';
import { PostgresWorkflowStore } from '../../lib/postgres/index.js';
import { clients, openPglite, testDatabase, type Client } from './support.js';

@Workflow('fulfilment')
class FulfilmentWorkflow {
  async run(ctx: WorkflowContext, input: { orderId: string }) {
    const payment = await ctx.waitForSignal('paid', 'payment.captured', { key: input.orderId });
    return { orderId: input.orderId, payment };
  }
}

const { database, reason } = await testDatabase('pgstore_transactions');

const targets = [
  ...clients.map((factory, i) => ({ name: `${factory.name} on PostgreSQL`, schema: `tx_${i}`, open: () => factory.open(database!.url), postgres: true, skip: reason })),
  { name: 'fromDrizzle (PGlite)', schema: 'tx_pglite', open: openPglite, postgres: false, skip: undefined },
];

describe.each(targets)('$name', ({ schema, open, postgres, skip }) => {
  let client: Client;
  let moduleRef: TestingModule;
  let workflows: WorkflowClient;
  let worker: WorkflowWorker;
  let orderId: (name: string) => string;

  beforeAll(async () => {
    if (skip) {
      return;
    }

    client = await open();
    moduleRef = await Test.createTestingModule({
      imports: [WorkflowsModule.forRoot({ worker: { enabled: false, shutdownTimeout: 50 } })],
      providers: [
        FulfilmentWorkflow,
        {
          provide: PostgresWorkflowStore,
          inject: [WorkflowStorage],
          useFactory: (storage: WorkflowStorage) => new PostgresWorkflowStore({ executor: client.executor, schema }, storage),
        },
      ],
    }).compile();
    moduleRef.useLogger(false);
    await moduleRef.init();
    workflows = moduleRef.get(WorkflowClient);
    worker = moduleRef.get(WorkflowWorker);
    orderId = (name) => `${schema}-${name}`;
  });

  afterAll(async () => {
    await moduleRef?.close();
    await client?.close();
  });

  beforeEach((context) => {
    if (skip) {
      context.skip(skip);
    }
  });

  const orders = async (id: string) => (await client.executor.query<{ id: string }>('SELECT id FROM orders WHERE id = $1::text', [id])).length;

  it("start() in the application's transaction commits with its row, and the worker runs the instance after the commit", async () => {
    const id = orderId('committed');
    const started = await client.transaction(async (tx) => {
      await client.insertOrder(tx, id);
      const result = await workflows.start(FulfilmentWorkflow, { orderId: id }, { id: `f-${id}`, transaction: tx });
      if (postgres) {
        // Another connection doesn't see it before the commit: it's in the application's transaction.
        expect(await database!.admin.query(`SELECT id FROM "${schema}".instances WHERE id = $1`, [`f-${id}`])).toMatchObject({ rowCount: 0 });
      }
      return result;
    });

    expect(started).toMatchObject({ id: `f-${id}`, created: true, status: 'pending' });
    expect(await orders(id)).toBe(1);
    expect(await worker.drain()).toBe(1);
    expect(await workflows.getStatus(`f-${id}`)).toMatchObject({ status: 'suspended', waits: [{ signal: 'payment.captured', key: id }] });
  });

  it("a rolled-back start() leaves neither the application's row nor the instance", async () => {
    const id = orderId('rolled-back');
    await expect(
      client.transaction(async (tx) => {
        await client.insertOrder(tx, id);
        await workflows.start(FulfilmentWorkflow, { orderId: id }, { id: `f-${id}`, transaction: tx });
        throw new Error('payment declined');
      }),
    ).rejects.toThrow('payment declined');

    expect(await orders(id)).toBe(0);
    expect(await workflows.getStatus(`f-${id}`)).toBeNull();
    expect(await worker.drain()).toBe(0);
  });

  it("signal() in the application's transaction wakes the instance once it commits, and a rolled-back one sends nothing", async () => {
    const id = orderId('signalled');
    await workflows.start(FulfilmentWorkflow, { orderId: id }, { id: `f-${id}` });
    await worker.drain();
    const parked = await workflows.getStatus(`f-${id}`);
    expect(parked).toMatchObject({ status: 'suspended' });

    await expect(
      client.transaction(async (tx) => {
        expect(await workflows.signal('payment.captured', { amount: 2499 }, { key: id, transaction: tx })).toMatchObject({ woken: 1, created: true });
        throw new Error('refund instead');
      }),
    ).rejects.toThrow('refund instead');
    expect(await workflows.getStatus(`f-${id}`)).toMatchObject({ status: 'suspended', wakeAt: parked!.wakeAt });
    expect(await worker.drain()).toBe(0);

    const sent = await client.transaction(async (tx) => {
      await client.insertOrder(tx, id);
      return workflows.signal('payment.captured', { amount: 2499 }, { key: id, transaction: tx, id: `charge-${id}` });
    });
    expect(sent).toMatchObject({ woken: 1, created: true });
    expect(await worker.drain()).toBe(1);
    expect(await workflows.getStatus(`f-${id}`)).toMatchObject({ status: 'completed', output: { orderId: id, payment: { amount: 2499 } } });
    expect(await workflows.signal('payment.captured', { amount: 1 }, { key: id, id: `charge-${id}` })).toMatchObject({ signalId: sent.signalId, created: false });
  });

  it('refuses the database, pool or client itself, and anything else, as the transaction', async () => {
    const id = orderId('refused');
    for (const junk of [client.root, {}, 'tx']) {
      await expect(workflows.start(FulfilmentWorkflow, { orderId: id }, { id: `f-${id}`, transaction: junk })).rejects.toThrow(TypeError);
      await expect(workflows.signal('payment.captured', {}, { key: id, transaction: junk })).rejects.toThrow(TypeError);
    }
    await expect(workflows.start(FulfilmentWorkflow, { orderId: id }, { id: `f-${id}`, transaction: client.root })).rejects.toThrow(/^Pass the .*(not|got)/);
    expect(await workflows.getStatus(`f-${id}`)).toBeNull();
  });

  it("refuses to signal in a transaction that isn't READ COMMITTED, before it takes the signal lock", async () => {
    await expect(
      client.transaction((tx) => workflows.signal('payment.captured', {}, { key: orderId('isolated'), transaction: tx }), 'repeatable read'),
    ).rejects.toThrow("signal() with { transaction } needs a READ COMMITTED transaction (PostgreSQL's default); this one is repeatable read");

    // The lock wasn't taken: signals go on at once.
    expect(await workflows.signal('payment.captured', {}, { key: orderId('isolated') })).toMatchObject({ created: true });
  });
});
