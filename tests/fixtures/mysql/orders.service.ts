import { randomUUID } from 'node:crypto';
import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectDrizzle } from '@nestjs/drizzle';
import { WorkflowClient } from '../../../lib/index.js';
import { eq } from 'drizzle-orm';
import type { Database } from './drizzle.js';
import { orders } from './schema.js';
import { shipmentDelivered, type CarrierEvent } from '../shipping/carrier-event.js';
import { fulfilmentId, OrderFulfilmentWorkflow } from '../orders/order-fulfilment.workflow.js';
import type { Order, PlaceOrderDto } from '../orders/order.js';

/** The tutorial's OrdersService on MySQL: the same transactions, without RETURNING. */
@Injectable()
export class MySqlOrdersService {
  constructor(
    @InjectDrizzle() private readonly db: Database,
    private readonly workflowClient: WorkflowClient,
  ) {}

  async place(dto: PlaceOrderDto) {
    const total = dto.items.reduce((sum, item) => sum + item.price * item.quantity, 0);
    const order: Order = { id: randomUUID().slice(0, 8), userId: dto.userId, items: dto.items, total, status: 'placed' };

    return this.db.transaction(async (tx) => {
      await tx.insert(orders).values(order);
      // Same transaction: the fulfilment exists if and only if the order does.
      const fulfilment = await this.workflowClient.start(OrderFulfilmentWorkflow, order, {
        id: fulfilmentId(order.id),
        transaction: tx,
      });
      return { ...order, fulfilment };
    });
  }

  async findOne(id: string): Promise<Order> {
    const [order] = await this.db.select().from(orders).where(eq(orders.id, id));
    if (!order) {
      throw new NotFoundException(`Order ${id} not found.`);
    }
    return order;
  }

  async markDelivered(event: CarrierEvent): Promise<void> {
    await this.db.transaction(async (tx) => {
      // The rows it matched (mysql2 counts matched rows by default, changed or not).
      const [{ affectedRows }] = await tx.update(orders).set({ status: 'delivered' }).where(eq(orders.id, event.reference));
      if (affectedRows === 0) {
        throw new NotFoundException(`Order ${event.reference} not found.`);
      }
      // The order's new status and the signal commit together.
      await this.workflowClient.signal(shipmentDelivered, event, { key: event.reference, transaction: tx });
    });
  }

  async markCancelled(id: string): Promise<void> {
    await this.db.update(orders).set({ status: 'cancelled' }).where(eq(orders.id, id));
  }
}
