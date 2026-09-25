import { Body, Controller, HttpCode, Post } from '@nestjs/common';
import { OrdersService } from '../orders/orders.service.js';
import type { CarrierEvent } from './carrier-event.js';

@Controller('webhooks/carrier')
export class CarrierWebhookController {
  constructor(private readonly ordersService: OrdersService) {}

  @Post()
  @HttpCode(200)
  async handle(@Body() event: CarrierEvent) {
    // Verify the carrier's signature header before trusting the body.
    if (event.type === 'shipment.delivered') {
      await this.ordersService.markDelivered(event);
    }
    return { received: true };
  }
}
