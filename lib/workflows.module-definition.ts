import { ConfigurableModuleBuilder } from '@nestjs/common';
import type { WorkflowsModuleOptions } from './interfaces/workflows-module-options.interface.js';

export const { ConfigurableModuleClass, MODULE_OPTIONS_TOKEN: WORKFLOWS_MODULE_OPTIONS } =
  new ConfigurableModuleBuilder<WorkflowsModuleOptions>({ moduleName: 'Workflows' })
    .setClassMethodName('forRoot')
    .setFactoryMethodName('createWorkflowsOptions')
    .setExtras<{ isGlobal?: boolean }>({ isGlobal: true }, (definition, { isGlobal }) => ({ ...definition, global: isGlobal }))
    .build();
