import { Injectable } from '@nestjs/common';
import { NonRetryableStepError } from '../../../lib/index.js';
import type { Order } from '../orders/order.js';
import { PaymentProviderClient } from './payment-provider.client.js';

export interface Charge {
  chargeId: string;
  amount: number;
}

@Injectable()
export class PaymentsService {
  constructor(private readonly paymentProviderClient: PaymentProviderClient) {}

  async charge(order: Order, idempotencyKey: string): Promise<Charge> {
    // The payment provider answers a repeated Idempotency-Key with the first response,
    // so a retried step never charges the card twice.
    const charge = await this.paymentProviderClient.createCharge(
      { amount: order.total, currency: 'USD', reference: order.id },
      { idempotencyKey },
    );
    if (charge.status === 'declined') {
      // Retrying won't help. Fail the step now.
      throw new NonRetryableStepError(`The payment provider declined the card for order ${order.id}.`);
    }
    return { chargeId: charge.id, amount: charge.amount };
  }

  async refund(chargeId: string, idempotencyKey: string): Promise<void> {
    await this.paymentProviderClient.createRefund({ chargeId }, { idempotencyKey });
  }
}
