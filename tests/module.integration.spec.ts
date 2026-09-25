/**
 * The module as an application wires it: options from `forRootAsync({ useClass })` (worker,
 * retry default, clock), the store provider on the app's database, and the workflow with its
 * routes in a feature module that doesn't import `WorkflowsModule` (it is global).
 */
import { Body, Controller, Get, Injectable, Module, NotFoundException, Param, Post } from '@nestjs/common';
import { adapters, createApp } from './support/adapters.js';
import {
  ManualWorkflowClock,
  Workflow,
  WorkflowClient,
  WorkflowsModule,
  type WorkflowContext,
  type WorkflowsModuleOptions,
  type WorkflowsOptionsFactory,
} from '../lib/index.js';
import { AppWorkflowStore, databaseModule, tempDb, type TestDb, waitFor } from './support.js';

const clock = new ManualWorkflowClock();

@Injectable()
class WorkflowsConfig implements WorkflowsOptionsFactory {
  createWorkflowsOptions(): WorkflowsModuleOptions {
    return {
      clock,
      worker: { id: 'configured-worker', pollInterval: '20ms', shutdownTimeout: 50 },
      retry: { attempts: 2, backoff: { delay: '10s' } },
    };
  }
}

@Injectable()
class Warehouse {
  readonly attempts: number[] = [];
}

@Workflow('restock')
class Restock {
  constructor(private readonly warehouse: Warehouse) {}

  async run(ctx: WorkflowContext, input: { sku: string }) {
    return ctx.step('order-stock', ({ attempt }) => {
      this.warehouse.attempts.push(attempt);
      throw new Error(`Supplier has no ${input.sku}.`);
    });
  }
}

@Controller('restocks')
class RestocksController {
  constructor(private readonly workflowClient: WorkflowClient) {}

  @Post()
  start(@Body() body: { sku: string }) {
    return this.workflowClient.start(Restock, body, { id: `restock-${body.sku}` });
  }

  @Get(':sku')
  async status(@Param('sku') sku: string) {
    const instance = await this.workflowClient.getStatus(`restock-${sku}`);
    if (!instance) {
      throw new NotFoundException();
    }

    return { status: instance.status, leaseOwner: instance.leaseOwner, error: instance.error?.message ?? null };
  }
}

@Module({ controllers: [RestocksController], providers: [Restock, Warehouse] })
class InventoryModule {}

describe.each(adapters)('an application wiring the module ($name)', ({ name: adapter }) => {
  let db: TestDb;

  beforeEach(async () => {
    db = await tempDb();
  });

  afterEach(() => {
    db.cleanup();
  });

  it('runs a feature module’s workflow on the worker and retry default from forRootAsync({ useClass })', async () => {
    @Module({
      imports: [WorkflowsModule.forRootAsync({ useClass: WorkflowsConfig }), databaseModule(db), InventoryModule],
      providers: [AppWorkflowStore],
    })
    class AppModule {}

    const app = await createApp(adapter, AppModule, { setup: (app) => app.useLogger(false) });
    try {
      const url = await app.getUrl();
      const status = async () => (await (await fetch(`${url}/restocks/sku-1`)).json()) as { status: string; leaseOwner: string; error: string | null };

      const started = await fetch(`${url}/restocks`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ sku: 'sku-1' }),
      });
      expect(started.status).toBe(201);
      await waitFor(async () => (await status()).status === 'suspended'); // the first attempt failed: parked for 10s
      expect(app.get(Warehouse).attempts).toEqual([1]);

      clock.advance('10s');
      await waitFor(async () => (await status()).status === 'failed');
      expect(await status()).toEqual({
        status: 'failed',
        leaseOwner: 'configured-worker',
        error: expect.stringContaining('Supplier has no sku-1.'),
      });
      expect(app.get(Warehouse).attempts).toEqual([1, 2]);
    } finally {
      await app.close();
    }
  });
});
