/**
 * The CQRS application the `@nestjs/workflows/cqrs` specs run: command handlers that write an
 * order and publish events (in the order's transaction when the app has a database), a workflow
 * those events start and signal, which executes a command and publishes an event from its
 * steps, and the in-memory reactions (an events handler, a saga) that must keep working.
 */
import { Inject, Injectable, Optional } from '@nestjs/common';
import {
  AggregateRoot,
  Command,
  CommandBus,
  CommandHandler,
  EventBus,
  EventPublisher,
  EventsHandler,
  ofType,
  Saga,
  type ICommandHandler,
  type IEvent,
  type IEventHandler,
} from '@nestjs/cqrs';
import { getDrizzleToken } from '@nestjs/drizzle';
import { eq } from 'drizzle-orm';
import { map, type Observable } from 'rxjs';
import { SignalOn, StartOn } from '../lib/cqrs/index.js';
import { Workflow, WorkflowClient, WorkflowSignal, type WorkflowContext } from '../lib/index.js';
import type { Database } from './fixtures/database/drizzle.js';
import { deferred, forever, orders } from './support.js';

export class OrderPlacedEvent {
  constructor(
    readonly orderId: string,
    readonly total: number,
  ) {}
}

export class PaymentCapturedEvent {
  constructor(
    readonly orderId: string,
    readonly chargeId: string,
    readonly amount: number,
  ) {}
}

export class OrderReadyEvent {
  constructor(readonly orderId: string) {}
}

export const paymentCaptured = new WorkflowSignal<{ chargeId: string; amount: number }>('payment.captured');

export const fulfilmentId = (orderId: string) => `order-${orderId}`;

/** An aggregate's `commit()`, typed so that it takes a dispatcher context with every supported @nestjs/cqrs. */
export type CommitWithContext = { commit(dispatcherContext?: unknown): unknown };

/**
 * Whether the installed @nestjs/cqrs passes a dispatcher context through an aggregate's
 * `commit(context)` to the event bus, and returns what the bus returns (12.1 and later): tried on
 * an aggregate merged with a stub bus that returns the context it receives, since the version
 * number alone says nothing about a local build.
 */
export const commitTakesContext = (() => {
  const context = { transaction: 'probe' };
  const bus = {
    publish: (_event: IEvent, received?: unknown) => received,
    publishAll: (_events: IEvent[], received?: unknown) => received,
  } as unknown as EventBus;
  const aggregate: CommitWithContext = new EventPublisher(bus).mergeObjectContext(new (class extends AggregateRoot {})());
  return aggregate.commit(context) === context;
})();

/** What the handlers, the saga and the command's side effect did. Shared by every "process" of a test. */
export class Ledger {
  /** Reservations by id (the step's idempotency key): a repeated command finds its own. */
  readonly reservations = new Map<string, string>();
  reserveCalls = 0;
  /** What `OrderEventsHandler` saw, and whether the workflow instance existed by then. */
  readonly handled: Array<{ event: string; orderId: string; instance?: boolean }> = [];
  readonly saga: string[] = [];
  /** Look the instance up from the events handler (not while the publisher's transaction is open). */
  lookUpInstances = true;
  /** Where the process "dies": `pause(at)` there never returns, as a step the process died in. */
  pauseAt: string | null = null;
  readonly paused = deferred();

  async pause(at: string) {
    if (this.pauseAt !== at) {
      return;
    }

    this.pauseAt = null;
    this.paused.resolve();
    await forever();
  }
}

export class ReserveStockCommand extends Command<{ reservationId: string }> {
  constructor(
    readonly orderId: string,
    readonly idempotencyKey: string,
  ) {
    super();
  }
}

@CommandHandler(ReserveStockCommand)
export class ReserveStockHandler implements ICommandHandler<ReserveStockCommand> {
  constructor(@Inject(Ledger) private readonly ledger: Ledger) {}

  async execute({ orderId, idempotencyKey }: ReserveStockCommand) {
    this.ledger.reserveCalls++;
    if (!this.ledger.reservations.has(idempotencyKey)) {
      this.ledger.reservations.set(idempotencyKey, orderId);
    }
    await this.ledger.pause('reserve-stock');
    return { reservationId: idempotencyKey };
  }
}

@Workflow('order-fulfilment')
@StartOn(OrderPlacedEvent, {
  id: (event) => fulfilmentId(event.orderId),
  input: (event) => ({ orderId: event.orderId, total: event.total }),
})
@SignalOn(PaymentCapturedEvent, {
  signal: paymentCaptured,
  key: (event) => event.orderId,
  // A redelivered payment event stores no second signal.
  id: (event) => event.chargeId,
  payload: (event) => ({ chargeId: event.chargeId, amount: event.amount }),
})
export class OrderFulfilmentWorkflow {
  constructor(
    private readonly commandBus: CommandBus,
    private readonly eventBus: EventBus,
    @Optional() @Inject(Ledger) private readonly ledger?: Ledger,
  ) {}

  async run(ctx: WorkflowContext, order: { orderId: string; total: number }) {
    const payment = await ctx.waitForSignal('await-payment', paymentCaptured, { key: order.orderId, timeout: '1h' });
    if (!payment) {
      ctx.fail(`Order ${order.orderId} was not paid within an hour.`);
    }

    const { reservationId } = await ctx.step('reserve-stock', ({ idempotencyKey }) =>
      this.commandBus.execute(new ReserveStockCommand(order.orderId, idempotencyKey)),
    );
    await ctx.step('announce-ready', async () => {
      await this.ledger?.pause('announce-ready');
      await this.eventBus.publish(new OrderReadyEvent(order.orderId));
    });
    return { chargeId: payment.chargeId, reservationId };
  }
}

@EventsHandler(OrderPlacedEvent, OrderReadyEvent)
export class OrderEventsHandler implements IEventHandler<OrderPlacedEvent | OrderReadyEvent> {
  constructor(
    @Inject(Ledger) private readonly ledger: Ledger,
    private readonly workflowClient: WorkflowClient,
  ) {}

  async handle(event: OrderPlacedEvent | OrderReadyEvent) {
    const seen: Ledger['handled'][number] = { event: event.constructor.name, orderId: event.orderId };
    this.ledger.handled.push(seen);
    if (this.ledger.lookUpInstances) {
      seen.instance = (await this.workflowClient.getStatus(fulfilmentId(event.orderId))) !== null;
    }
  }
}

@Injectable()
export class OrderSagas {
  constructor(@Inject(Ledger) private readonly ledger: Ledger) {}

  @Saga()
  placed = (events$: Observable<IEvent>): Observable<null> =>
    events$.pipe(
      ofType(OrderPlacedEvent),
      map((event) => {
        this.ledger.saga.push(event.orderId);
        return null;
      }),
    );
}

export class PlaceOrderCommand extends Command<void> {
  constructor(
    readonly orderId: string,
    readonly total: number,
    /** Throw this after publishing, inside the transaction: it rolls back. */
    readonly failAfterPublish?: string,
  ) {
    super();
  }
}

/** Saves the order and publishes `OrderPlacedEvent` in one transaction (on a database). */
@CommandHandler(PlaceOrderCommand)
export class PlaceOrderHandler implements ICommandHandler<PlaceOrderCommand> {
  constructor(
    private readonly eventBus: EventBus,
    @Optional() @Inject(getDrizzleToken()) private readonly db?: Database,
  ) {}

  async execute({ orderId, total, failAfterPublish }: PlaceOrderCommand) {
    const event = new OrderPlacedEvent(orderId, total);
    if (!this.db) {
      await this.eventBus.publish(event);
      return;
    }

    await this.db.transaction(async (tx) => {
      await tx.insert(orders).values({ id: orderId, userId: 'u_42', items: [], total, status: 'placed' });
      await this.eventBus.publish(event, { transaction: tx });
      if (failAfterPublish) {
        throw new Error(failAfterPublish);
      }
    });
  }
}

export class CapturePaymentCommand extends Command<void> {
  constructor(
    readonly orderId: string,
    readonly chargeId: string,
    readonly amount: number,
    readonly failAfterPublish?: string,
  ) {
    super();
  }
}

/** Records the payment on the order and publishes `PaymentCapturedEvent`, in one transaction. */
@CommandHandler(CapturePaymentCommand)
export class CapturePaymentHandler implements ICommandHandler<CapturePaymentCommand> {
  constructor(
    private readonly eventBus: EventBus,
    @Optional() @Inject(getDrizzleToken()) private readonly db?: Database,
  ) {}

  async execute({ orderId, chargeId, amount, failAfterPublish }: CapturePaymentCommand) {
    const event = new PaymentCapturedEvent(orderId, chargeId, amount);
    if (!this.db) {
      await this.eventBus.publish(event);
      return;
    }

    await this.db.transaction(async (tx) => {
      await tx.update(orders).set({ total: amount }).where(eq(orders.id, orderId));
      await this.eventBus.publish(event, { transaction: tx });
      if (failAfterPublish) {
        throw new Error(failAfterPublish);
      }
    });
  }
}

/** Everything but the workflow, which each spec registers (sometimes with a second version). */
export const cqrsProviders = [
  ReserveStockHandler,
  OrderEventsHandler,
  OrderSagas,
  PlaceOrderHandler,
  CapturePaymentHandler,
];
