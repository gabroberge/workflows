import { Module, type DynamicModule, type OnModuleInit } from '@nestjs/common';
import { DiscoveryModule, ModuleRef } from '@nestjs/core';
import { WorkflowEvents } from './events/workflow-events.service.js';
import { LOCK_STORAGE, WorkflowStorage } from './storage/workflow.storage.js';
import { WorkflowClient } from './services/workflow-client.service.js';
import { WorkflowRegistry } from './services/workflow-registry.service.js';
import { WorkflowScheduler } from './services/workflow-scheduler.service.js';
import { WorkflowSchedules } from './services/workflow-schedules.service.js';
import { WorkflowWorker } from './services/workflow-worker.service.js';
import { ConfigurableModuleClass, WORKFLOWS_MODULE_OPTIONS } from './workflows.module-definition.js';
import { WORKFLOW_PAYLOAD_CODECS } from './workflows.constants.js';
import type { WorkflowPayloadCodec } from './interfaces/workflow-payload-codec.interface.js';
import type { WorkflowsModuleOptions } from './interfaces/workflows-module-options.interface.js';

/**
 * `WorkflowsModule.forRoot({ clock?, worker?, retry?, journal?, allowInMemoryStorage?, codec? })`, or
 * `forRootAsync({ imports, inject, useFactory | useClass | useExisting })`. Global by default.
 * `@Workflow()` classes are regular providers of any module. The store is not an option: a
 * provider registers it with `WorkflowStorage.registerSource(this)`, and without one the
 * module keeps its state in memory.
 */
@Module({
  imports: [DiscoveryModule],
  providers: [
    { provide: WORKFLOW_PAYLOAD_CODECS, inject: [WORKFLOWS_MODULE_OPTIONS, ModuleRef], useFactory: payloadCodecs },
    WorkflowStorage,
    WorkflowRegistry,
    WorkflowEvents,
    WorkflowScheduler,
    WorkflowWorker,
    WorkflowSchedules,
    WorkflowClient,
  ],
  // WorkflowRegistry is not public API (nor in the barrel): WorkflowsCqrsModule reads it.
  exports: [WorkflowStorage, WorkflowClient, WorkflowSchedules, WorkflowWorker, WorkflowEvents, WorkflowRegistry],
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

/** `codec`'s codecs, with the classes created by Nest (their dependencies from this module's scope). */
function payloadCodecs(options: WorkflowsModuleOptions, moduleRef: ModuleRef): Promise<WorkflowPayloadCodec[]> {
  const codecs = options.codec === undefined ? [] : Array.isArray(options.codec) ? options.codec : [options.codec];
  return Promise.all(codecs.map((codec) => (typeof codec === 'function' ? moduleRef.create(codec) : codec)));
}
