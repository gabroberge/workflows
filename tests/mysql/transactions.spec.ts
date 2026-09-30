/**
 * `start()` and `signal()` with `{ transaction }` through MySqlWorkflowStore, registered by a factory provider, with
 * each client's own transaction object (mysql2's connection after `beginTransaction()`, Drizzle's `tx`, TypeORM's
 * `EntityManager`, Prisma's transaction client, Kysely's `trx`), in REPEATABLE READ transactions, MySQL's default:
 *
 * - the instance and the signal commit with the application's row, or roll back with it;
 * - the store needs no READ COMMITTED: a signal in a transaction whose snapshot predates a suspension still wakes it
 *   (the wake-up reads the latest committed waits with a locking read), and a start or a repeated signal that meets
 *   rows committed after the snapshot returns them (locking reads after the duplicate key);
 * - a deadlock in the application's transaction reaches the application, which runs its transaction again: the store
 *   retries only its own transactions;
 * - anything else passed as the transaction is refused with a TypeError that says what to pass.
 */
import { mysqlErrorCode } from '@nestjs/store-kit/mysql';
import { Test, type TestingModule } from '@nestjs/testing';
import mysql from 'mysql2/promise';
import { Workflow, WorkflowClient, WorkflowIdConflictError, WorkflowsModule, WorkflowStorage, WorkflowWorker, type WorkflowContext } from '../../lib/index.js';
import { MySqlWorkflowStore } from '../../lib/mysql/index.js';
import { deferred } from '../support.js';
import { clients, testDatabase, type Client } from './support.js';

@Workflow('fulfilment')
class FulfilmentWorkflow {
  async run(ctx: WorkflowContext, input: { orderId: string }) {
    const payment = await ctx.waitForSignal('paid', 'payment.captured', { key: input.orderId });
    return { orderId: input.orderId, payment };
  }
}

const { database, reason } = await testDatabase('mysql_transactions');

const targets = clients.map((factory, i) => ({ name: `${factory.name} on MySQL`, schema: `tx_${i}`, open: () => factory.open(database!.url) }));

describe.each(targets)('$name', ({ schema, open }) => {
  let client: Client;
  let moduleRef: TestingModule;
  let workflows: WorkflowClient;
  let worker: WorkflowWorker;
  let orderId: (name: string) => string;

  beforeAll(async () => {
    if (reason) {
      return;
    }

    client = await open();
    moduleRef = await Test.createTestingModule({
      imports: [WorkflowsModule.forRoot({ worker: { enabled: false, shutdownTimeout: 50 } })],
      providers: [
        FulfilmentWorkflow,
        {
          provide: MySqlWorkflowStore,
          inject: [WorkflowStorage],
          useFactory: (storage: WorkflowStorage) => new MySqlWorkflowStore({ executor: client.executor, schema }, storage),
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
    if (reason) {
      context.skip(reason);
    }
  });

  const orders = async (id: string) => (await client.executor.query<{ id: string }>('SELECT id FROM orders WHERE id = ?', [id])).length;
  const admin = async (sql: string, params: unknown[]) => (await database!.admin.query<mysql.RowDataPacket[]>(sql, params))[0];
  /** A read in the application's transaction, which starts its REPEATABLE READ snapshot. */
  const read = (tx: unknown, sql: string, params: unknown[] = []) => client.executor.wrapTransaction(tx).query<{ n: string }>(sql, params);

  it("start() in the application's transaction commits with its row, and the worker runs the instance after the commit", async () => {
    const id = orderId('committed');
    const started = await client.transaction(async (tx) => {
      await client.insertOrder(tx, id);
      const result = await workflows.start(FulfilmentWorkflow, { orderId: id }, { id: `f-${id}`, transaction: tx });
      // Another connection doesn't see it before the commit: it's in the application's transaction.
      expect(await admin(`SELECT id FROM ${schema}_instances WHERE id = ?`, [`f-${id}`])).toEqual([]);
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

  it('wakes, in a REPEATABLE READ transaction, an instance that suspended after the transaction took its snapshot', async () => {
    const id = orderId('snapshot');
    await workflows.start(FulfilmentWorkflow, { orderId: id }, { id: `f-${id}` });

    const sent = await client.transaction(async (tx) => {
      expect(await read(tx, `SELECT CAST(COUNT(*) AS CHAR) AS n FROM ${schema}_waits WHERE instance_id = ?`, [`f-${id}`])).toEqual([{ n: '0' }]);
      // Meanwhile, the instance suspends on the store's own connections, and commits its wait.
      expect(await worker.drain()).toBe(1);
      // The transaction's snapshot doesn't see the wait; the signal's wake-up (a locking read) does.
      expect(await read(tx, `SELECT CAST(COUNT(*) AS CHAR) AS n FROM ${schema}_waits WHERE instance_id = ?`, [`f-${id}`])).toEqual([{ n: '0' }]);
      await client.insertOrder(tx, id);
      return workflows.signal('payment.captured', { amount: 1500 }, { key: id, transaction: tx });
    }, 'repeatable read');

    expect(sent).toMatchObject({ woken: 1, created: true });
    expect(await worker.drain()).toBe(1);
    expect(await workflows.getStatus(`f-${id}`)).toMatchObject({ status: 'completed', output: { payment: { amount: 1500 } } });
  });

  it('returns, in a REPEATABLE READ transaction, the instance and the signal that others committed after its snapshot', async () => {
    const id = orderId('met');
    const [started, repeated] = await client.transaction(async (tx) => {
      await read(tx, 'SELECT CAST(COUNT(*) AS CHAR) AS n FROM orders');
      // Committed elsewhere after the snapshot: the transaction's own reads don't see them, its inserts meet them.
      const elsewhere = await workflows.start(FulfilmentWorkflow, { orderId: id }, { id: `f-${id}` });
      const first = await workflows.signal('payment.captured', { amount: 1 }, { key: id, id: `charge-${id}` });
      const again = await workflows.start(FulfilmentWorkflow, { orderId: id }, { id: `f-${id}`, transaction: tx });
      await expect(workflows.start(FulfilmentWorkflow, { orderId: 'other' }, { id: `f-${id}`, transaction: tx })).rejects.toThrow(WorkflowIdConflictError);
      const duplicate = await workflows.signal('payment.captured', { amount: 2 }, { key: id, id: `charge-${id}`, transaction: tx });
      expect(duplicate).toMatchObject({ signalId: first.signalId, created: false });
      return [elsewhere, again];
    }, 'repeatable read');

    expect(started).toMatchObject({ created: true });
    expect(repeated).toMatchObject({ id: `f-${id}`, created: false });
  });

  it("lets a deadlock in the application's transaction through, for the application to run its transaction again", async () => {
    const id = orderId('deadlock');
    const firstHoldsRow = deferred();
    const secondHoldsSignals = deferred();
    // The first transaction holds the order's row, then signals; the second signals, then writes the order's row.
    const first = client.transaction(async (tx) => {
      await client.insertOrder(tx, id);
      firstHoldsRow.resolve();
      await secondHoldsSignals.promise;
      return workflows.signal('order.placed', {}, { key: `${id}-1`, transaction: tx });
    });
    const second = client.transaction(async (tx) => {
      await firstHoldsRow.promise;
      await workflows.signal('order.placed', {}, { key: `${id}-2`, transaction: tx });
      secondHoldsSignals.resolve();
      await client.insertOrder(tx, id);
    });
    const outcomes = await Promise.allSettled([first, second]);

    const failed = outcomes.flatMap((outcome) => (outcome.status === 'rejected' ? [outcome.reason] : []));
    expect(failed).toHaveLength(1);
    expect(mysqlErrorCode(failed[0])).toBe(1213);
    // The victim's signal went with its transaction, and running it again succeeds.
    const victim = outcomes[0]!.status === 'rejected' ? 1 : 2;
    const signalled = async (key: string) =>
      (await admin(`SELECT CAST(COUNT(*) AS CHAR) AS n FROM ${schema}_signals WHERE name = 'order.placed' AND \`key\` = ?`, [key]))[0]!.n;
    expect(await signalled(`${id}-${victim}`)).toBe('0');
    expect(await signalled(`${id}-${3 - victim}`)).toBe('1');
    await client.transaction((tx) => workflows.signal('order.placed', {}, { key: `${id}-${victim}`, transaction: tx }));
    expect(await signalled(`${id}-${victim}`)).toBe('1');
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
});
