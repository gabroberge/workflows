export interface OrderItem {
  productId: string;
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

export class PlaceOrderDto {
  userId!: string;
  items!: OrderItem[];
}

export interface FulfilmentResult {
  chargeId: string;
  trackingNumber: string;
}
