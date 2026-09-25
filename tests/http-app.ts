import {
  Body,
  Catch,
  Controller,
  Delete,
  Get,
  HttpCode,
  Module,
  NotFoundException,
  Param,
  Post,
  type ArgumentsHost,
  type DynamicModule,
  type ExceptionFilter,
  type INestApplication,
  type Provider,
  type Type,
} from '@nestjs/common';
import { APP_FILTER, HttpAdapterHost } from '@nestjs/core';
import { createApp, type AdapterName } from './support/adapters.js';
import {
  WorkflowClient,
  WorkflowError,
  WorkflowEvents,
  WorkflowsModule,
  WorkflowWorker,
  type WorkflowClock,
  type WorkflowEvent,
  type WorkflowWorkerOptions,
} from '../lib/index.js';
import { OrderFulfilment, shipmentDelivered, type ShipmentDelivered } from './order-fulfilment.js';
import { AppWorkflowStore, databaseModule, type TestDb } from './support.js';

/**
 * The app's one place that turns the package's errors into responses: the ones that carry a
 * 4xx `status` (`WorkflowIdConflictError` 409, `WorkflowNotFoundError` 404) keep it.
 */
@Catch(WorkflowError)
export class WorkflowErrorFilter implements ExceptionFilter {
  constructor(private readonly adapterHost: HttpAdapterHost) {}

  catch(error: WorkflowError, host: ArgumentsHost) {
    const { status: code } = error as WorkflowError & { status?: unknown };
    const status = typeof code === 'number' ? code : 500;
    const body = { statusCode: status, error: error.name, message: error.message };
    this.adapterHost.httpAdapter.reply(host.switchToHttp().getResponse(), body, status);
  }
}

export class FulfilDto {
  amount!: number;
  email!: string;
}

/** The README's orders controller: start idempotently by order id, a status view, cancel. */
@Controller()
export class OrdersController {
  constructor(private readonly workflowClient: WorkflowClient) {}

  @Post('orders/:id/fulfil')
  fulfil(@Param('id') orderId: string, @Body() body: FulfilDto) {
    return this.workflowClient.start(OrderFulfilment, { orderId, ...body }, { id: `order-${orderId}` });
  }

  @Get('orders/:id/fulfilment')
  async status(@Param('id') orderId: string) {
    const instance = await this.workflowClient.getStatus(`order-${orderId}`);
    if (!instance) {
      throw new NotFoundException();
    }

    return { status: instance.status, waits: instance.waits, error: instance.error?.message ?? null };
  }

  @Delete('orders/:id/fulfilment')
  async cancel(@Param('id') orderId: string) {
    const { accepted, status } = await this.workflowClient.cancel(`order-${orderId}`, 'Cancelled by customer.');
    return { accepted, status };
  }
}

/** The README's carrier webhook: a durable signal keyed by the order. */
@Controller('webhooks/carrier')
export class CarrierWebhookController {
  constructor(private readonly workflowClient: WorkflowClient) {}

  @Post()
  @HttpCode(200)
  delivered(@Body() event: ShipmentDelivered) {
    return this.workflowClient.signal(shipmentDelivered, event, { key: event.orderId });
  }
}

export interface HttpNode {
  app: INestApplication;
  url: string;
  client: WorkflowClient;
  worker: WorkflowWorker;
  /** This application's `events$`, from boot until it closes. */
  events: WorkflowEvent[];
  /** `fetch` with JSON in and out. */
  http(method: 'GET' | 'POST' | 'DELETE', path: string, body?: unknown): Promise<{ status: number; body: any }>;
  close(): Promise<void>;
}

export interface HttpNodeOptions {
  db: TestDb;
  clock?: WorkflowClock;
  worker?: WorkflowWorkerOptions;
  workflows?: Type<unknown>[];
  providers?: Provider[];
  controllers?: Type<unknown>[];
  imports?: Array<Type<unknown> | DynamicModule>;
  /** Runs before `init()`: register shared sources (a lock store) here. */
  setup?: (app: INestApplication) => void;
}

/**
 * One application instance ("pod") on the test database, served on `adapter`: the app's
 * database module, the store provider that registers itself on it, and the workflow module
 * with the worker off unless `worker.enabled` says otherwise.
 */
export async function bootHttp(adapter: AdapterName, options: HttpNodeOptions): Promise<HttpNode> {
  @Module({
    imports: [
      WorkflowsModule.forRoot({ clock: options.clock, worker: { enabled: false, shutdownTimeout: 50, ...options.worker } }),
      databaseModule(options.db),
      ...(options.imports ?? []),
    ],
    controllers: options.controllers ?? [],
    providers: [
      AppWorkflowStore,
      ...(options.workflows ?? []),
      ...(options.providers ?? []),
      { provide: APP_FILTER, useClass: WorkflowErrorFilter },
    ],
  })
  class AppModule {}

  const app = await createApp(adapter, AppModule, {
    setup: (app) => {
      app.useLogger(false);
      options.setup?.(app);
    },
  });

  const events: WorkflowEvent[] = [];
  app.get(WorkflowEvents).events$.subscribe((event) => events.push(event));
  const url = (await app.getUrl()).replace('[::1]', '127.0.0.1');

  return {
    app,
    url,
    client: app.get(WorkflowClient),
    worker: app.get(WorkflowWorker),
    events,
    http: async (method, path, body) => {
      const response = await fetch(`${url}${path}`, {
        method,
        headers: body === undefined ? undefined : { 'content-type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      const text = await response.text();
      return { status: response.status, body: text ? JSON.parse(text) : undefined };
    },
    close: () => app.close(),
  };
}
