import { Injectable } from '@nestjs/common';
import { NonRetryableStepError } from '../../../lib/index.js';
import type { OrderItem } from '../orders/order.js';

interface Reservation {
  items: OrderItem[];
  released: boolean;
}

/**
 * Stand-in for the stock table in your database. `reserve()` uses the step's
 * idempotency key as the reservation's primary key, so a retried step finds
 * the reservation it already made instead of reserving the items twice.
 */
@Injectable()
export class InventoryService {
  /** Items in stock, by product id. */
  readonly stock = new Map<string, number>([
    ['salmon-kibble-2kg', 25],
    ['clumping-litter-10l', 12],
    ['sisal-scratching-post', 4],
  ]);
  readonly reservations = new Map<string, Reservation>();

  async reserve(items: OrderItem[], idempotencyKey: string): Promise<{ reservationId: string }> {
    if (!this.reservations.has(idempotencyKey)) {
      const missing = items.find((item) => (this.stock.get(item.productId) ?? 0) < item.quantity);
      if (missing) throw new NonRetryableStepError(`Product ${missing.productId} is out of stock.`);
      for (const item of items) this.stock.set(item.productId, this.stock.get(item.productId)! - item.quantity);
      this.reservations.set(idempotencyKey, { items, released: false });
    }
    return { reservationId: idempotencyKey };
  }

  /** Idempotent: releasing twice returns the items once. */
  async release(reservationId: string): Promise<void> {
    const reservation = this.reservations.get(reservationId);
    if (!reservation || reservation.released) return;
    for (const item of reservation.items) {
      this.stock.set(item.productId, (this.stock.get(item.productId) ?? 0) + item.quantity);
    }
    reservation.released = true;
  }
}
