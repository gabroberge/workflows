/**
 * A graceful shutdown of an application that serves HTTP, on Express and on Fastify. Nest closes the HTTP server
 * after `beforeApplicationShutdown`, and the server waits for the requests in flight: a route waiting for a result
 * must stop waiting before, or it holds the shutdown up to its wait's timeout. An instance that ends while the worker
 * drains still answers its route with its result.
 */
import { Body, Controller, Injectable, Param, Post, ServiceUnavailableException } from '@nestjs/common';
import { Workflow, WorkflowClient, WorkflowResultTimeoutError, WorkflowSignal, type WorkflowContext } from '../lib/index.js';
import { bootHttp, type HttpNode } from './http-app.js';
import { adapters } from './support/adapters.js';
import { deferred, tempDb, waitFor, type TestDb } from './support.js';

const approved = new WorkflowSignal<null>('quote.approved');

/** Prices take until the application shuts down, as a long computation that finishes as its worker drains. */
@Injectable()
class Pricing {
  readonly started = deferred();

  async price(items: number, signal: AbortSignal): Promise<number> {
    this.started.resolve();
    await new Promise((resolve) => signal.addEventListener('abort', resolve, { once: true }));
    return items * 1_299;
  }
}

@Workflow('quote')
class QuoteWorkflow {
  constructor(private readonly pricing: Pricing) {}

  async run(ctx: WorkflowContext, input: { items: number; approval?: boolean }) {
    if (input.approval) {
      await ctx.waitForSignal('approval', approved, { key: ctx.workflowId });
    }
    return ctx.step('price', ({ signal }) => this.pricing.price(input.items, signal));
  }
}

/** Waits up to 30 seconds for a quote, as the docs page's invoices route waits for a batch. */
@Controller('quotes')
class QuotesController {
  constructor(private readonly workflowClient: WorkflowClient) {}

  @Post(':id')
  async quote(@Param('id') id: string, @Body() body: { items: number; approval?: boolean }) {
    try {
      return { price: await this.workflowClient.startAndWait(QuoteWorkflow, body, { id: `quote-${id}` }, { timeout: '30s' }) };
    } catch (error) {
      if (error instanceof WorkflowResultTimeoutError) {
        return { pending: true };
      }
      // The application shuts down: the client asks again, and another instance answers.
      throw new ServiceUnavailableException((error as Error).message);
    }
  }
}

describe.each(adapters)('a graceful shutdown over HTTP ($name)', ({ name: adapter }) => {
  let db: TestDb;
  let node: HttpNode | undefined;

  beforeEach(async () => {
    db = await tempDb();
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await node?.close();
    node = undefined;
    db.cleanup();
  });

  it('answers the routes waiting for results before the server closes, which then closes at once', async () => {
    node = await bootHttp(adapter, {
      db,
      workflows: [QuoteWorkflow],
      providers: [Pricing],
      controllers: [QuotesController],
      worker: { enabled: true, pollInterval: '20ms', shutdownTimeout: '10s' },
    });
    const { app, client } = node;
    const result = vi.spyOn(client, 'result');

    const priced = node.http('POST', '/quotes/q-1', { items: 3 });
    const approval = node.http('POST', '/quotes/q-2', { items: 500, approval: true });
    await app.get(Pricing).started.promise;
    await waitFor(async () => result.mock.calls.length === 2 && (await client.getStatus('quote-q-2'))?.status === 'suspended');

    node = undefined;
    const closing = performance.now();
    await app.close();
    const closedIn = performance.now() - closing;

    // Its last step finished as the worker drained: the instance completed, and the route has its result.
    expect(await priced).toEqual({ status: 201, body: { price: 3_897 } });
    expect(await approval).toEqual({
      status: 503,
      body: { statusCode: 503, error: 'Service Unavailable', message: 'The application shut down while waiting for the result of instance "quote-q-2".' },
    });
    expect(closedIn).toBeLessThan(10_000);
  }, 60_000);
});
