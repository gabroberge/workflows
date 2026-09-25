// The `@nestjs/workflows/cqrs` entry: the only files that import `@nestjs/cqrs`.

// Module: wraps the EventBus publisher, so mapped events start and signal workflows
export { WorkflowsCqrsModule } from './workflows-cqrs.module.js';

// Mapping events to workflows, and the dispatcher context that carries your transaction
export { SignalOn, StartOn } from './decorators/index.js';
export type { SignalOnOptions, StartOnOptions, WorkflowDispatcherContext } from './interfaces/index.js';
