import { WorkflowSignal } from '../../../lib/index.js';

/** The body the carrier POSTs to our webhook. */
export interface CarrierEvent {
  type: 'shipment.in_transit' | 'shipment.delivered';
  /** The store's order id. */
  reference: string;
  trackingNumber: string;
  occurredAt: string;
}

/** Sent by the webhook, awaited by the fulfilment workflow. The order id is its key. */
export const shipmentDelivered = new WorkflowSignal<CarrierEvent>('shipment.delivered');
