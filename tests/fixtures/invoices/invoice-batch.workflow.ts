import { Workflow, type WorkflowContext, type WorkflowRunner, type WorkflowStepContext } from '../../../lib/index.js';
import { InvoicesService } from './invoices.service.js';

interface Checkpoint {
  cursor: number | null;
  rendered: number;
}

@Workflow('invoice-batch')
export class InvoiceBatchWorkflow implements WorkflowRunner<{ month: string }, { rendered: number }> {
  constructor(private readonly invoicesService: InvoicesService) {}

  async run(ctx: WorkflowContext, { month }: { month: string }) {
    return ctx.step(
      'render-invoices',
      async ({ progress, heartbeat, signal }: WorkflowStepContext<Checkpoint>) => {
        // After a crash, the next attempt starts from the last checkpoint.
        let { cursor, rendered } = progress ?? { cursor: 0, rendered: 0 };
        while (cursor !== null) {
          const page = await this.invoicesService.renderPage(month, cursor, signal);
          cursor = page.nextCursor;
          rendered += page.rendered;
          await heartbeat({ cursor, rendered });
        }
        return { rendered };
      },
      { heartbeatTimeout: '2m' },
    );
  }
}
