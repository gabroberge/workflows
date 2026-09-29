import type { Type } from '@nestjs/common';

/** What `@StartOn()` and `@SignalOn()` record on a workflow class. */
export type WorkflowEventRoute = WorkflowStartRoute | WorkflowSignalRoute;

export interface WorkflowStartRoute {
  kind: 'start';
  event: Type<object>;
  id: (event: object) => string;
  input?: (event: object) => unknown;
  priority?: number | ((event: object) => number | undefined);
  concurrencyKey?: string | ((event: object) => string | undefined);
  rateLimitKey?: string | ((event: object) => string | undefined);
}

export interface WorkflowSignalRoute {
  kind: 'signal';
  event: Type<object>;
  signal: string;
  key?: (event: object) => string;
  id?: (event: object) => string;
  payload?: (event: object) => unknown;
}
