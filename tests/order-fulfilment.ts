import { Injectable } from '@nestjs/common';
import { Workflow, WorkflowSignal, type WorkflowContext, type WorkflowRunner } from '../lib/index.js';
import { World } from './support.js';

export interface Order {
  orderId: string;
  amount: number;
  email: string;
}

export interface ShipmentDelivered {
  orderId: string;
  trackingId: string;
}

export const shipmentDelivered = new WorkflowSignal<ShipmentDelivered>('shipment.delivered');

@Injectable()
export class Payments {
  constructor(private readonly world: World) {}

  charge(orderId: string, amount: number, idempotencyKey: string) {
    this.world.record('charge', idempotencyKey);
    return { chargeId: `ch_${orderId}`, amount };
  }

  refund(chargeId: string, idempotencyKey: string) {
    this.world.record('refund', idempotencyKey);
    return { refundId: `re_${chargeId}` };
  }
}

@Injectable()
export class Inventory {
  constructor(private readonly world: World) {}

  reserve(orderId: string, idempotencyKey: string) {
    this.world.record('reserve', idempotencyKey);
    return { reservationId: `res_${orderId}` };
  }

  release(reservationId: string, idempotencyKey: string) {
    this.world.record('release', idempotencyKey);
  }
}

@Injectable()
export class Mailer {
  constructor(private readonly world: World) {}

  send(to: string, template: string, idempotencyKey: string) {
    this.world.record(`mail:${template}`, idempotencyKey);
  }
}

/**
 * Reserve stock, charge, wait up to 3 days for the carrier's delivery webhook.
 * No delivery: refund and release (compensations). Delivered: send a review
 * request 7 days later.
 */
@Workflow('order-fulfilment')
export class OrderFulfilment implements WorkflowRunner<Order, { chargeId: string; trackingId: string }> {
  constructor(
    private readonly inventory: Inventory,
    private readonly payments: Payments,
    private readonly mailer: Mailer,
  ) {}

  async run(ctx: WorkflowContext, order: Order) {
    await ctx.step('reserve-stock', ({ idempotencyKey }) => this.inventory.reserve(order.orderId, idempotencyKey), {
      compensate: (reservation, { idempotencyKey }) => this.inventory.release(reservation.reservationId, idempotencyKey),
    });

    const charge = await ctx.step(
      'charge',
      ({ idempotencyKey }) => this.payments.charge(order.orderId, order.amount, idempotencyKey),
      { compensate: (charge, { idempotencyKey }) => this.payments.refund(charge.chargeId, idempotencyKey) },
    );

    const delivered = await ctx.waitForSignal('await-delivery', shipmentDelivered, {
      key: order.orderId,
      timeout: '3d',
    });
    if (!delivered) {
      ctx.fail(`Order ${order.orderId} was not delivered within 3 days.`);
    }

    await ctx.sleep('before-review-request', '7d');
    await ctx.step('review-request', ({ idempotencyKey }) => this.mailer.send(order.email, 'review', idempotencyKey));

    return { chargeId: charge.chargeId, trackingId: delivered.trackingId };
  }
}

export const orderProviders = (world: World) => [{ provide: World, useValue: world }, Inventory, Payments, Mailer];
