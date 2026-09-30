import { Module } from '@nestjs/common';
import { DrizzleModule, getDrizzleToken } from '@nestjs/drizzle';
import { WorkflowsModule, WorkflowStorage } from '../../../lib/index.js';
import { fromDrizzle, MySqlWorkflowStore } from '../../../lib/mysql/index.js';
import { drizzle } from 'drizzle-orm/mysql2';
import type { Database } from './drizzle.js';
import * as schema from './schema.js';
import { InventoryService } from '../inventory/inventory.service.js';
import { InvoiceBatchWorkflow } from '../invoices/invoice-batch.workflow.js';
import { InvoicesService } from '../invoices/invoices.service.js';
import { MailService } from '../mail/mail.service.js';
import { OrderFulfilmentWorkflow } from '../orders/order-fulfilment.workflow.js';
import { OrdersController } from '../orders/orders.controller.js';
import { OrdersService } from '../orders/orders.service.js';
import { PaymentProviderClient } from '../payments/payment-provider.client.js';
import { PaymentsService } from '../payments/payments.service.js';
import { CarrierWebhookController } from '../shipping/carrier-webhook.controller.js';
import { MySqlInvoicesService } from './invoices.service.js';
import { MySqlOrdersService } from './orders.service.js';

/** The workflows tutorial's application on MySQL: its app module, with MySqlWorkflowStore as the docs register it. */
@Module({
  imports: [
    DrizzleModule.forRootAsync({
      // Drizzle on MySQL: a mysql2 pool on DATABASE_URL, closed after the worker handed its instances back
      useFactory: () => ({ drizzle, connection: { uri: process.env.DATABASE_URL!, connectionLimit: 10 }, schema, mode: 'default' as const }),
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
    {
      // Instances and journals in your database, in tables of their own (nest_workflows_*)
      provide: MySqlWorkflowStore,
      inject: [getDrizzleToken(), WorkflowStorage],
      useFactory: (db: Database, storage: WorkflowStorage) => new MySqlWorkflowStore({ executor: fromDrizzle(db) }, storage),
    },
    { provide: OrdersService, useClass: MySqlOrdersService },
    PaymentProviderClient,
    PaymentsService,
    InventoryService,
    MailService,
    { provide: InvoicesService, useClass: MySqlInvoicesService },
    OrderFulfilmentWorkflow,
    InvoiceBatchWorkflow,
  ],
})
export class MySqlAppModule {}
