import { Injectable } from '@nestjs/common';
import { InjectDrizzle } from '@nestjs/drizzle';
import { asc } from 'drizzle-orm';
import type { InvoicePage } from '../invoices/invoices.service.js';
import type { Database } from './drizzle.js';
import { orders } from './schema.js';

/** The tutorial's InvoicesService on MySQL: the same page of orders, from the MySQL table. */
@Injectable()
export class MySqlInvoicesService {
  pageSize = 100;
  readonly invoices = new Map<string, { month: string; orderId: string; total: number }>();

  constructor(@InjectDrizzle() private readonly db: Database) {}

  async renderPage(month: string, cursor: number, signal: AbortSignal): Promise<InvoicePage> {
    signal.throwIfAborted();
    // One row more than a page tells whether another page follows.
    const rows = await this.db.select().from(orders).orderBy(asc(orders.id)).limit(this.pageSize + 1).offset(cursor);
    const page = rows.slice(0, this.pageSize);
    for (const order of page) {
      this.invoices.set(`${month}:${order.id}`, { month, orderId: order.id, total: order.total });
    }
    return { rendered: page.length, nextCursor: rows.length > this.pageSize ? cursor + page.length : null };
  }
}
