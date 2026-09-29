import type { WorkflowContext } from './workflow-context.interface.js';

/** Implemented by `@Workflow()` classes: `WorkflowRunner<Input, Output>`. */
export interface WorkflowRunner<I = unknown, O = unknown> {
  run(ctx: WorkflowContext, input: I): Promise<O>;
}

export type WorkflowInput<W> = W extends { run(ctx: any, input: infer I): any } ? I : unknown;

export type WorkflowOutput<W> = W extends { run(ctx: any, input: any): infer R } ? Awaited<R> : unknown;
