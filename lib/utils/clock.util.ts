import { toMs, type Duration } from './duration.util.js';
import type { WorkflowClock } from '../interfaces/workflow-clock.interface.js';

export const systemClock: WorkflowClock = { now: () => Date.now() };

/** A clock that only moves when told to. For tests. */
export class ManualWorkflowClock implements WorkflowClock {
  constructor(private current: number = Date.UTC(2026, 0, 1)) {}

  now(): number {
    return this.current;
  }

  advance(duration: Duration): number {
    this.current += toMs(duration);
    return this.current;
  }

  set(time: Date | number): void {
    this.current = time instanceof Date ? time.getTime() : time;
  }
}
