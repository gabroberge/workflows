import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';

export interface PaymentProviderCharge {
  id: string;
  amount: number;
  currency: string;
  reference: string;
  status: 'succeeded' | 'declined';
}

export interface PaymentProviderRefund {
  id: string;
  chargeId: string;
}

/**
 * Stand-in for the payment provider's SDK. Like the real API, it answers a repeated
 * `Idempotency-Key` with the original response instead of charging again.
 */
@Injectable()
export class PaymentProviderClient {
  /** Charges the provider created (declined ones included), one per idempotency key. */
  readonly charges: PaymentProviderCharge[] = [];
  readonly refunds: PaymentProviderRefund[] = [];
  /** Every request received, replays included. */
  requests = 0;
  /** Order references whose card the provider declines. */
  readonly declinedReferences = new Set<string>();
  private readonly responses = new Map<string, unknown>();

  async createCharge(
    params: { amount: number; currency: string; reference: string },
    options: { idempotencyKey: string },
  ): Promise<PaymentProviderCharge> {
    return this.idempotent(options.idempotencyKey, () => {
      const status = this.declinedReferences.has(params.reference) ? 'declined' : 'succeeded';
      const charge: PaymentProviderCharge = { id: `ch_${randomUUID().slice(0, 8)}`, ...params, status };
      this.charges.push(charge);
      return charge;
    });
  }

  async createRefund(params: { chargeId: string }, options: { idempotencyKey: string }): Promise<PaymentProviderRefund> {
    return this.idempotent(options.idempotencyKey, () => {
      const refund: PaymentProviderRefund = { id: `re_${randomUUID().slice(0, 8)}`, chargeId: params.chargeId };
      this.refunds.push(refund);
      return refund;
    });
  }

  private idempotent<T>(key: string, create: () => T): T {
    this.requests++;
    if (!this.responses.has(key)) this.responses.set(key, create());
    return this.responses.get(key) as T;
  }
}
