export interface OrderItem {
  isbn: string;
  quantity: number;
  /** Unit price in cents. */
  price: number;
}

export type OrderStatus = 'placed' | 'delivered' | 'cancelled';

export interface Order {
  id: string;
  userId: string;
  items: OrderItem[];
  /** In cents. */
  total: number;
  status: OrderStatus;
}
