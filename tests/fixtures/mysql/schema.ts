// The workflows tutorial's application table on MySQL. MySqlWorkflowStore brings its own tables (its migrations).
import { int, json, mysqlTable, varchar } from 'drizzle-orm/mysql-core';
import type { OrderItem, OrderStatus } from '../orders/order.js';

export const orders = mysqlTable('orders', {
  id: varchar('id', { length: 64 }).primaryKey(),
  userId: varchar('user_id', { length: 255 }).notNull(),
  items: json('items').$type<OrderItem[]>().notNull(),
  /** In cents. */
  total: int('total').notNull(),
  status: varchar('status', { length: 32 }).$type<OrderStatus>().notNull(),
});

/** `orders`, as the application's own migration creates it. */
export const ORDERS_DDL = `CREATE TABLE IF NOT EXISTS orders (
  id varchar(64) NOT NULL PRIMARY KEY,
  user_id varchar(255) NOT NULL,
  items json NOT NULL,
  total int NOT NULL,
  status varchar(32) NOT NULL
)`;
