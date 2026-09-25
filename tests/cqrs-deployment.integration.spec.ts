/**
 * `@nestjs/workflows/cqrs` across processes, on every store: an API process that only publishes
 * (`worker: false`) next to a worker process, one that doesn't register the workflow, and
 * applications that stop, or die mid-step, between an event and the end of the workflow it
 * started. On the SQL stores the events are published in the command handler's transaction.
 */
import { Logger } from '@nestjs/common';
import { CommandBus, CqrsModule, EventBus } from '@nestjs/cqrs';
import { sql } from 'drizzle-orm';
import { WorkflowsCqrsModule } from '../lib/cqrs/index.js';
import { ManualWorkflowClock, type WorkflowWorkerOptions } from '../lib/index.js';
import {
  CapturePaymentCommand,
  cqrsProviders,
  fulfilmentId,
  Ledger,
  OrderFulfilmentWorkflow,
  PlaceOrderCommand,
} from './cqrs-app.js';
import type { Database } from './fixtures/database/drizzle.js';
import { boot, connect, storeKind, tempDb, waitFor, type Node, type TestDb } from './support.js';

describe('CQRS events across processes and restarts', () => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let ledger: Ledger;
  const nodes: Node[] = [];

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    // One outside world for every process of a test: what the handlers and the command did.
    ledger = new Ledger();
    ledger.lookUpInstances = false;
    if (storeKind !== 'memory') {
      const connection = connect(db);
      await (connection.db as Database).execute(sql`DELETE FROM orders`);
      await connection.close();
    }
  });

  afterEach(async () => {
    for (const node of nodes.splice(0)) {
      await node.close();
    }
    db.cleanup();
  });

  /** One application ("process") on the test database, as the CQRS example app. */
  const start = async (options: { workflows?: boolean; worker?: WorkflowWorkerOptions } = {}) => {
    const node = await boot({
      db,
      clock,
      worker: options.worker,
      imports: [CqrsModule.forRoot(), WorkflowsCqrsModule],
      workflows: options.workflows === false ? [] : [OrderFulfilmentWorkflow],
      providers: [...cqrsProviders, { provide: Ledger, useValue: ledger }],
    });
    nodes.push(node);
    return { ...node, commandBus: node.moduleRef.get(CommandBus), eventBus: node.moduleRef.get(EventBus) };
  };

  /** Simulates a crash: the process stops mid-step, and its lease is left to expire. */
  const crash = async (node: Node) => {
    nodes.splice(nodes.indexOf(node), 1);
    await node.close();
  };

  it('starts, from an API process that only publishes, an instance a worker process runs, and wakes it from there', async () => {
    const api = await start({ worker: { enabled: false } });
    const worker = await start({ worker: { enabled: true, pollInterval: 25 } });

    await api.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
    await waitFor(async () => (await api.client.getStatus(fulfilmentId('o-1')))?.status === 'suspended');
    await api.commandBus.execute(new CapturePaymentCommand('o-1', 'ch_1', 2499));
    await waitFor(async () => (await api.client.getStatus(fulfilmentId('o-1')))?.status === 'completed');

    expect(await api.client.getStatus(fulfilmentId('o-1'))).toMatchObject({ runs: 2, output: { chargeId: 'ch_1' } });
    // The API process executed nothing; the worker ran every step once.
    expect(api.events).toEqual([]);
    expect(worker.events.filter(({ type }) => type === 'step-completed').map((event) => (event as { step: string }).step)).toEqual([
      'reserve-stock',
      'announce-ready',
    ]);
    expect(ledger.reserveCalls).toBe(1);
    // Each process's handlers see the events published in it: the order in the API, the step's in the worker.
    await waitFor(() => ledger.handled.length === 2);
    expect(ledger.handled.map(({ event }) => event)).toEqual(['OrderPlacedEvent', 'OrderReadyEvent']);
    expect(ledger.saga).toEqual(['o-1']);
  });

  it('starts nothing from a process that doesn’t register the workflow, and says so at startup', async () => {
    const warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    try {
      const api = await start({ workflows: false });
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('no registered workflow maps an event with @StartOn() or @SignalOn()'));

      await api.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
      expect(await api.client.getStatus(fulfilmentId('o-1'))).toBeNull();
      // The event itself still reaches the handlers, as without WorkflowsCqrsModule.
      await waitFor(() => ledger.handled.length === 1);
    } finally {
      warn.mockRestore();
    }
  });

  it('runs an instance published before a restart, and resumes its signalled wait in the next process', async () => {
    const first = await start();
    await first.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
    await first.close(); // a deploy, before any worker picked the instance up
    nodes.splice(nodes.indexOf(first), 1);

    const second = await start();
    expect(await second.worker.drain()).toBe(1);
    expect(await second.client.getStatus(fulfilmentId('o-1'))).toMatchObject({ status: 'suspended', waits: [{ signal: 'payment.captured', key: 'o-1' }] });
    await crash(second);

    const third = await start();
    await third.commandBus.execute(new CapturePaymentCommand('o-1', 'ch_1', 2499));
    await third.worker.drain();
    const done = await third.client.getStatus(fulfilmentId('o-1'), { journal: true });
    expect(done).toMatchObject({ status: 'completed', runs: 2, output: { chargeId: 'ch_1', reservationId: 'order-o-1:reserve-stock' } });
    expect(done!.journal.map((entry) => `${entry.name}:${entry.status}`)).toEqual([
      'await-payment:completed',
      'reserve-stock:completed',
      'announce-ready:completed',
    ]);
  });

  it('re-runs the step the process died in with the same key, and never a completed one or the signalled wait', async () => {
    const first = await start();
    await first.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
    await first.worker.drain();
    await first.commandBus.execute(new CapturePaymentCommand('o-1', 'ch_1', 2499));
    ledger.pauseAt = 'announce-ready';
    void first.worker.drain();
    await ledger.paused.promise;
    await crash(first);

    const second = await start();
    expect(await second.worker.drain()).toBe(0); // the dead process's lease is still valid
    clock.advance('31s');
    expect(await second.worker.drain()).toBe(1);

    const done = await second.client.getStatus(fulfilmentId('o-1'), { journal: true });
    expect(done).toMatchObject({ status: 'completed', runs: 3, output: { chargeId: 'ch_1' } });
    expect(done!.journal.map((entry) => `${entry.name}:${entry.attempts}`)).toEqual(['await-payment:0', 'reserve-stock:1', 'announce-ready:2']);
    expect(ledger.reserveCalls).toBe(1); // completed before the crash: not run again
    await waitFor(() => ledger.handled.some(({ event }) => event === 'OrderReadyEvent'));
    // Published once: the first attempt died before it published.
    expect(ledger.handled.filter(({ event }) => event === 'OrderReadyEvent')).toHaveLength(1);
  });

  it('re-executes a command the process died in with the step’s idempotency key, so its effect happens once', async () => {
    const first = await start();
    await first.commandBus.execute(new PlaceOrderCommand('o-1', 2499));
    await first.worker.drain();
    await first.commandBus.execute(new CapturePaymentCommand('o-1', 'ch_1', 2499));
    ledger.pauseAt = 'reserve-stock'; // after the reservation was made
    void first.worker.drain();
    await ledger.paused.promise;
    await crash(first);

    const second = await start();
    clock.advance('31s');
    await second.worker.drain();

    expect(await second.client.getStatus(fulfilmentId('o-1'))).toMatchObject({ status: 'completed', output: { reservationId: 'order-o-1:reserve-stock' } });
    expect(ledger.reserveCalls).toBe(2);
    expect([...ledger.reservations]).toEqual([['order-o-1:reserve-stock', 'o-1']]);
  });

  it.runIf(storeKind !== 'memory')('leaves nothing for the next process when the handler’s transaction rolled back', async () => {
    const first = await start();
    await expect(first.commandBus.execute(new PlaceOrderCommand('o-1', 2499, 'the warehouse is closed'))).rejects.toThrow('the warehouse is closed');
    await first.close();
    nodes.splice(nodes.indexOf(first), 1);

    const second = await start();
    expect(await second.worker.drain()).toBe(0);
    expect(await second.client.list()).toEqual([]);
  });
});
