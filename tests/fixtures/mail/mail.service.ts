import { Injectable, Logger } from '@nestjs/common';
import type { Order } from '../orders/order.js';

export interface SentMail {
  to: string;
  template: 'review-request' | 'order-confirmation';
  orderId: string;
}

/**
 * Stand-in for your mail provider. The step's idempotency key is the message
 * id, and the provider drops a message id it has already sent.
 */
@Injectable()
export class MailService {
  private readonly logger = new Logger(MailService.name);
  readonly sent = new Map<string, SentMail>();

  async sendReviewRequest(order: Order, messageId: string): Promise<void> {
    this.send(messageId, { to: order.userId, template: 'review-request', orderId: order.id });
  }

  async sendOrderConfirmation(order: Order, messageId: string): Promise<void> {
    this.send(messageId, { to: order.userId, template: 'order-confirmation', orderId: order.id });
  }

  private send(messageId: string, mail: SentMail) {
    if (this.sent.has(messageId)) return;
    this.sent.set(messageId, mail);
    this.logger.log(`Sent ${mail.template} for order ${mail.orderId} to user ${mail.to}.`);
  }
}
