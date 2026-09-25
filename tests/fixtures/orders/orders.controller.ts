import { Body, ConflictException, Controller, Get, HttpCode, NotFoundException, Param, Post } from '@nestjs/common';
import { WorkflowClient } from '../../../lib/index.js';
import { fulfilmentId } from './order-fulfilment.workflow.js';
import { PlaceOrderDto } from './order.js';
import { OrdersService } from './orders.service.js';

@Controller('orders')
export class OrdersController {
  constructor(
    private readonly ordersService: OrdersService,
    private readonly workflowClient: WorkflowClient,
  ) {}

  @Post()
  place(@Body() dto: PlaceOrderDto) {
    return this.ordersService.place(dto);
  }

  @Get(':id/fulfilment')
  async fulfilment(@Param('id') id: string) {
    const instance = await this.workflowClient.getStatus(fulfilmentId(id), { journal: true });
    if (!instance) throw new NotFoundException(`Order ${id} not found.`);
    return {
      status: instance.status,
      waitingFor: instance.waits.map((wait) => wait.signal),
      wakeAt: instance.wakeAt === null ? null : new Date(instance.wakeAt).toISOString(),
      steps: Object.fromEntries(instance.journal.map((entry) => [entry.name, entry.status])),
      error: instance.error?.message ?? null,
    };
  }

  // Customer support only: protect it with your staff guard.
  @Post(':id/cancel')
  @HttpCode(202)
  async cancel(@Param('id') id: string, @Body('reason') reason: string) {
    const order = await this.ordersService.findOne(id);
    if (order.status === 'delivered') {
      throw new ConflictException(`Order ${id} was delivered. Start a return instead.`);
    }
    // accepted is false if the fulfilment already ended or was already cancelled.
    const { accepted } = await this.workflowClient.cancel(fulfilmentId(id), reason);
    if (accepted) await this.ordersService.markCancelled(id);
    return { accepted };
  }
}
