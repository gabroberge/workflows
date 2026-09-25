/**
 * Workflows next to the transactional outbox (`@nestjs/outbox`), as the README pairs them: a
 * checkout that saves the order, starts its workflow and adds its message in one transaction,
 * and an outbox handler that turns a message into a signal, with the handler's inbox absorbing
 * the redelivery. On SQL stores both stores are the tutorials' Drizzle recipes on the same
 * database; in memory, both in-memory defaults.
 */
import { readFileSync } from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';
import { BadRequestException, Body, Controller, HttpCode, Inject, Injectable, Param, Post } from '@nestjs/common';
import { getDrizzleToken } from '@nestjs/drizzle';
import { sql } from 'drizzle-orm';
import type { Database } from './fixtures/database/drizzle.js';
import { orders } from './fixtures/database/schema.js';
import { DrizzleOutboxStore } from './fixtures/outbox/drizzle-outbox.store.js';
import { adapters } from './support/adapters.js';
import { InMemoryMailTransport, MailModule, Mailer } from '@nestjs/mail';
import { OnOutboxMessage, Outbox, OutboxModule, OutboxRelay, type OutboxHandlerContext } from '@nestjs/outbox';
import { ManualWorkflowClock, Workflow, WorkflowClient, WorkflowSignal, type WorkflowContext } from '../lib/index.js';
import { bootHttp, type HttpNode } from './http-app.js';
import { connect, openStore, storeKind, tempDb, type TestDb, World } from './support.js';

interface Payment {
  orderId: string;
  amount: number;
}

const paymentSettled = new WorkflowSignal<Payment>('payment.settled');
const sqlStore = storeKind !== 'memory';

/** The outbox tutorial's tables, next to the workflow tables on the test database. */
async function createOutboxTables(database: Database) {
  const [{ exists }] = (await database.execute(sql`SELECT to_regclass('outbox_messages') IS NOT NULL AS exists`)).rows as Array<{
    exists: boolean;
  }>;
  if (!exists) {
    const migration = readFileSync(new URL('./fixtures/outbox/0001_outbox.sql', import.meta.url), 'utf8');
    for (const statement of migration.split('--> statement-breakpoint')) {
      await database.execute(sql.raw(statement));
    }
  }
  await database.execute(sql`TRUNCATE outbox_messages, outbox_dead_letters, outbox_inbox, orders`);
}

@Workflow('checkout')
class Checkout {
  constructor(
    @Inject(World) private readonly world: World,
    private readonly mailer: Mailer,
  ) {}

  async run(ctx: WorkflowContext, input: { orderId: string; email: string }) {
    await ctx.step('reserve', ({ idempotencyKey }) => this.world.record('reserve', idempotencyKey));
    const payment = await ctx.waitForSignal('payment', paymentSettled, { key: input.orderId, timeout: '1d' });
    if (!payment) {
      ctx.fail(`Order ${input.orderId} was never paid.`);
    }

    await ctx.step('receipt', ({ idempotencyKey, signal }) =>
      this.mailer.send({
        to: input.email,
        subject: `Receipt for ${input.orderId}`,
        text: `Paid ${payment.amount}.`,
        idempotencyKey,
        signal,
        retry: false,
      }),
    );
    return payment;
  }
}

@Injectable()
class CheckoutHandlers {
  constructor(
    @Inject(World) private readonly world: World,
    private readonly mailer: Mailer,
    private readonly workflowClient: WorkflowClient,
  ) {}

  @OnOutboxMessage('order.placed', { consumer: 'order-confirmation-mail' })
  async confirm(order: { orderId: string; email: string }, { message, signal }: OutboxHandlerContext) {
    await this.mailer.send({
      to: order.email,
      subject: `Order ${order.orderId} received`,
      text: 'Thank you.',
      idempotencyKey: message.id,
      signal,
      retry: false,
    });
  }

  @OnOutboxMessage('payment.settled', { consumer: 'checkout-workflow' })
  async settle(payment: Payment) {
    this.world.record('signal', payment.orderId);
    await this.workflowClient.signal(paymentSettled, payment, { key: payment.orderId });
  }

  /** A sibling consumer of the same message that fails its first delivery. */
  @OnOutboxMessage('payment.settled', { consumer: 'ledger' })
  ledger(payment: Payment, { attempt }: OutboxHandlerContext) {
    this.world.record('ledger', payment.orderId, attempt);
    if (attempt === 1) {
      throw new Error('Ledger unavailable.');
    }
  }
}

@Controller()
class CheckoutController {
  constructor(
    @Inject(getDrizzleToken()) private readonly db: Database,
    private readonly workflowClient: WorkflowClient,
    private readonly outbox: Outbox,
  ) {}

  /** The application's transaction: a Drizzle one on SQL, a stand-in handle in memory. */
  private transaction<T>(work: (tx: unknown) => Promise<T>): Promise<T> {
    return sqlStore ? this.db.transaction(work) : work({});
  }

  @Post('checkout/:orderId')
  checkout(@Param('orderId') orderId: string, @Body() body: { email: string; declined?: boolean }) {
    return this.transaction(async (tx) => {
      if (sqlStore) {
        await (tx as Database).insert(orders).values({ id: orderId, userId: body.email, items: [], total: 1200, status: 'placed' });
      }
      const started = await this.workflowClient.start(Checkout, { orderId, email: body.email }, { id: `checkout-${orderId}`, transaction: tx });
      await this.outbox.add(tx, { topic: 'order.placed', key: orderId, payload: { orderId, email: body.email } });
      if (body.declined) {
        throw new BadRequestException('Card declined.');
      }
      return started;
    });
  }

  @Post('payments/:orderId/settled')
  @HttpCode(202)
  settled(@Param('orderId') orderId: string, @Body() body: { amount: number }) {
    return this.transaction(async (tx) => {
      const message = await this.outbox.add(tx, { topic: 'payment.settled', key: orderId, payload: { orderId, amount: body.amount } });
      return { messageId: message.id };
    });
  }
}

describe.each(adapters)('workflows with the outbox ($name)', ({ name: adapter }) => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let world: World;
  let mailbox: InMemoryMailTransport;
  let pod: HttpNode;

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    world = new World();
    mailbox = new InMemoryMailTransport();
    if (sqlStore) {
      const connection = connect(db);
      await createOutboxTables(connection.db as Database);
      await connection.close();
    }

    pod = await bootHttp(adapter, {
      db,
      clock,
      imports: [
        OutboxModule.forRoot({ relay: { enabled: false }, retry: { attempts: 3, backoff: { delay: 1, jitter: 'none' } } }),
        MailModule.forRoot({ transport: mailbox, from: 'Orders <orders@example.com>' }),
      ],
      workflows: [Checkout],
      providers: [{ provide: World, useValue: world }, CheckoutHandlers, ...(sqlStore ? [DrizzleOutboxStore] : [])],
      controllers: [CheckoutController],
    });
  });

  afterEach(async () => {
    await pod.close();
    db.cleanup();
  });

  const relay = () => pod.app.get(OutboxRelay);

  it('turns a settled payment into one signal, however often the outbox redelivers it', async () => {
    await pod.http('POST', '/checkout/o1', { email: 'ada@example.com' });
    expect(await relay().runOnce()).toMatchObject({ claimed: 1, published: 1 });
    await pod.worker.drain();
    expect(await pod.client.getStatus('checkout-o1')).toMatchObject({ status: 'suspended' });

    expect((await pod.http('POST', '/payments/o1/settled', { amount: 1200 })).status).toBe(202);
    expect(await relay().runOnce()).toMatchObject({ claimed: 1, retried: 1 }); // the ledger failed
    await sleep(10);
    expect(await relay().runOnce()).toMatchObject({ claimed: 1, published: 1 });

    // The inbox skipped the handler that had already signalled.
    expect(world.calls.filter((call) => call.op !== 'reserve')).toEqual([
      { op: 'signal', key: 'o1', attempt: undefined },
      { op: 'ledger', key: 'o1', attempt: 1 },
      { op: 'ledger', key: 'o1', attempt: 2 },
    ]);
    const { store, close } = openStore(db);
    try {
      expect(await store.signals({ name: 'payment.settled', key: 'o1', afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).toHaveLength(1);
    } finally {
      await close();
    }

    await pod.worker.drain();
    expect(await pod.client.getStatus('checkout-o1')).toMatchObject({ status: 'completed', output: { orderId: 'o1', amount: 1200 } });
    expect(mailbox.mails.map((mail) => mail.subject)).toEqual(['Order o1 received', 'Receipt for o1']);
  });

  it.runIf(sqlStore)('commits the order, its workflow and its message together, or none of them', async () => {
    const declined = await pod.http('POST', '/checkout/o1', { email: 'ada@example.com', declined: true });
    expect(declined).toMatchObject({ status: 400, body: { message: 'Card declined.' } });

    const connection = connect(db);
    const database = connection.db as Database;
    try {
      expect(await database.select().from(orders)).toEqual([]);
      expect(await pod.client.getStatus('checkout-o1')).toBeNull();
      expect(await relay().runOnce()).toMatchObject({ claimed: 0 });
      expect(await pod.worker.drain()).toBe(0);
      expect(mailbox.mails).toEqual([]);

      const placed = await pod.http('POST', '/checkout/o1', { email: 'ada@example.com' });
      expect(placed).toMatchObject({ status: 201, body: { id: 'checkout-o1', created: true } });
      expect(await database.select({ id: orders.id }).from(orders)).toEqual([{ id: 'o1' }]);
      expect(await relay().runOnce()).toMatchObject({ claimed: 1, published: 1 });
      expect(await pod.worker.drain()).toBe(1);
      expect(mailbox.assertSent({ to: 'ada@example.com', subject: 'Order o1 received' })).toBeDefined();
      expect(world.ops()).toEqual(['reserve']);
    } finally {
      await connection.close();
    }
  });
});
