import { Module, type DynamicModule, type OnModuleInit } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { WorkflowEvents } from './events/workflow-events.service.js';
import { LOCK_STORAGE, WorkflowStorage } from './storage/workflow.storage.js';
import { WorkflowClient } from './services/workflow-client.service.js';
import { WorkflowRegistry } from './services/workflow-registry.service.js';
import { WorkflowWorker } from './services/workflow-worker.service.js';
import { ConfigurableModuleClass } from './workflows.module-definition.js';
import type { WorkflowsModuleOptions } from './interfaces/workflows-module-options.interface.js';

/**
 * `WorkflowsModule.forRoot({ clock?, worker?, retry?, journal?, allowInMemoryStorage? })`, or
 * `forRootAsync({ imports, inject, useFactory | useClass | useExisting })`. Global by default.
 * `@Workflow()` classes are regular providers of any module. The store is not an option: a
 * provider registers it with `WorkflowStorage.registerSource(this)`, and without one the
 * module keeps its state in memory.
 */
@Module({
  imports: [DiscoveryModule],
  providers: [WorkflowStorage, WorkflowRegistry, WorkflowEvents, WorkflowWorker, WorkflowClient],
  // WorkflowRegistry is not public API (nor in the barrel): WorkflowsCqrsModule reads it.
  exports: [WorkflowStorage, WorkflowClient, WorkflowWorker, WorkflowEvents, WorkflowRegistry],
})
export class WorkflowsModule extends ConfigurableModuleClass implements OnModuleInit {
  static forRoot(options: WorkflowsModuleOptions & { isGlobal?: boolean } = {}): DynamicModule {
    return super.forRoot(options);
  }

  constructor(private readonly storage: WorkflowStorage) {
    super();
  }

  /**
   * Every provider constructor has run (so every source has registered), and the worker
   * starts later, in `onApplicationBootstrap`: the registry locks here (if a read in another
   * module's `onModuleInit` hasn't locked it already).
   */
  onModuleInit(): void {
    this.storage[LOCK_STORAGE]();
  }
}
