import { Module } from '@nestjs/common';
import { DrizzleModule } from '@nestjs/drizzle';
import { WorkflowsModule } from '../../lib/index.js';
import { drizzle } from 'drizzle-orm/node-postgres';
import { DrizzleWorkflowStore } from './database/drizzle-workflow.store.js';
import * as schema from './database/schema.js';
import { InventoryService } from './inventory/inventory.service.js';
import { InvoiceBatchWorkflow } from './invoices/invoice-batch.workflow.js';
import { InvoicesService } from './invoices/invoices.service.js';
import { MailService } from './mail/mail.service.js';
import { OrderFulfilmentWorkflow } from './orders/order-fulfilment.workflow.js';
import { OrdersController } from './orders/orders.controller.js';
import { OrdersService } from './orders/orders.service.js';
import { PaymentProviderClient } from './payments/payment-provider.client.js';
import { PaymentsService } from './payments/payments.service.js';
import { CarrierWebhookController } from './shipping/carrier-webhook.controller.js';

@Module({
  imports: [
    DrizzleModule.forRootAsync({
      // Drizzle on PostgreSQL: a pg pool on DATABASE_URL, closed after the worker handed its instances back
      useFactory: () => ({ drizzle, connection: process.env.DATABASE_URL!, schema }),
    }),
    WorkflowsModule.forRoot({
      worker: {
        concurrency: 10, // instances this process executes at once
        leaseDuration: '30s', // how long a crashed process keeps its instances
        shutdownTimeout: '10s', // how long a deploy waits for running steps
      },
    }),
  ],
  controllers: [OrdersController, CarrierWebhookController],
  providers: [
    DrizzleWorkflowStore, // instances and journals in your database
    OrdersService,
    PaymentProviderClient,
    PaymentsService,
    InventoryService,
    MailService,
    InvoicesService,
    OrderFulfilmentWorkflow,
    InvoiceBatchWorkflow,
  ],
})
export class AppModule {}
