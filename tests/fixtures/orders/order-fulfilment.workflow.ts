import { Workflow, type WorkflowContext, type WorkflowRunner } from '../../../lib/index.js';
import { InventoryService } from '../inventory/inventory.service.js';
import { MailService } from '../mail/mail.service.js';
import { PaymentsService } from '../payments/payments.service.js';
import { shipmentDelivered } from '../shipping/carrier-event.js';
import type { FulfilmentResult, Order } from './order.js';

/** One fulfilment per order: the order id is the workflow instance id. */
export const fulfilmentId = (orderId: string) => `order-${orderId}`;

@Workflow('order-fulfilment')
export class OrderFulfilmentWorkflow implements WorkflowRunner<Order, FulfilmentResult> {
  constructor(
    private readonly paymentsService: PaymentsService,
    private readonly inventoryService: InventoryService,
    private readonly mailService: MailService,
  ) {}

  async run(ctx: WorkflowContext, order: Order): Promise<FulfilmentResult> {
    const charge = await ctx.step(
      'charge-payment',
      ({ idempotencyKey }) => this.paymentsService.charge(order, idempotencyKey),
      {
        retry: { attempts: 5, backoff: { delay: '2s' } },
        compensate: (charge, { idempotencyKey }) => this.paymentsService.refund(charge.chargeId, idempotencyKey),
      },
    );

    await ctx.step(
      'reserve-stock',
      ({ idempotencyKey }) => this.inventoryService.reserve(order.items, idempotencyKey),
      { compensate: ({ reservationId }) => this.inventoryService.release(reservationId) },
    );

    const delivery = await ctx.waitForSignal('await-delivery', shipmentDelivered, {
      key: order.id,
      timeout: '3d',
    });
    if (!delivery) {
      ctx.fail(`Order ${order.id} was not delivered within 3 days.`);
    }
    // Point of no return: the parcel arrived, so nothing from here on refunds
    // the charge or releases the stock, not a failure and not a cancel.
    ctx.commit('delivered');

    await ctx.sleep('before-review-request', '7d');
    await ctx.step('send-review-request', ({ idempotencyKey }) =>
      this.mailService.sendReviewRequest(order, idempotencyKey),
    );

    return { chargeId: charge.chargeId, trackingNumber: delivery.trackingNumber };
  }
}
