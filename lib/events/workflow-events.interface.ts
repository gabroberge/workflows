import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';
import type { WorkflowStatus, WorkflowWait } from '../interfaces/workflow-instance.interface.js';

/** Fields every lifecycle event carries. */
export interface WorkflowEventBase {
  /** Instance id. */
  id: string;
  workflow: string;
  version: number;
  /** Engine clock time (ms). */
  at: number;
}

/** Channel `nestjs:workflows:workflow-started`: the first execution of an instance began. */
export interface WorkflowStartedEvent extends WorkflowEventBase {
  type: 'workflow-started';
}

/** Channel `nestjs:workflows:workflow-resumed`: a later execution began (after a wait, a sleep, a retry or a crash). */
export interface WorkflowResumedEvent extends WorkflowEventBase {
  type: 'workflow-resumed';
  /** The execution's number, 2 or more. */
  run: number;
}

/** Channel `nestjs:workflows:workflow-suspended`: an execution parked the instance. */
export interface WorkflowSuspendedEvent extends WorkflowEventBase {
  type: 'workflow-suspended';
  wakeAt: number | null;
  waits: WorkflowWait[];
}

/** Channel `nestjs:workflows:workflow-compensating`: the instance failed or was cancelled, and starts undoing. */
export interface WorkflowCompensatingEvent extends WorkflowEventBase {
  type: 'workflow-compensating';
  error: SerializedWorkflowError;
}

/** Channel `nestjs:workflows:workflow-completed`. */
export interface WorkflowCompletedEvent extends WorkflowEventBase {
  type: 'workflow-completed';
  output: unknown;
}

/** Channel `nestjs:workflows:workflow-failed`: failed, after its compensations ran (none for a definition error). */
export interface WorkflowFailedEvent extends WorkflowEventBase {
  type: 'workflow-failed';
  error: SerializedWorkflowError;
}

/** Channel `nestjs:workflows:workflow-cancelled`: cancelled, after its compensations ran. `error.message` is the reason. */
export interface WorkflowCancelledEvent extends WorkflowEventBase {
  type: 'workflow-cancelled';
  error: SerializedWorkflowError;
}

/** Channel `nestjs:workflows:workflow-compensation-failed`: an undo gave up halfway. Needs a person. */
export interface WorkflowCompensationFailedEvent extends WorkflowEventBase {
  type: 'workflow-compensation-failed';
  /** The original failure, with the compensation's own error as `compensation`. */
  error: SerializedWorkflowError;
}

/** Channel `nestjs:workflows:step-completed`. */
export interface WorkflowStepCompletedEvent extends WorkflowEventBase {
  type: 'step-completed';
  step: string;
  attempt: number;
  durationMs: number;
}

/** Channel `nestjs:workflows:step-failed`: an attempt failed. `retryAt` is `null` when the step gave up. */
export interface WorkflowStepFailedEvent extends WorkflowEventBase {
  type: 'step-failed';
  step: string;
  /** The attempt that failed (1-based). */
  attempt: number;
  error: SerializedWorkflowError;
  retryAt: number | null;
}

/** Channel `nestjs:workflows:step-compensated`: a step's compensation completed. */
export interface WorkflowStepCompensatedEvent extends WorkflowEventBase {
  type: 'step-compensated';
  step: string;
  attempt: number;
}

/** Channel `nestjs:workflows:signal-received`: a wait took a signal. */
export interface WorkflowSignalReceivedEvent extends WorkflowEventBase {
  type: 'signal-received';
  /** The wait's name. */
  wait: string;
  signal: string;
  signalId: number;
}

/** Channel `nestjs:workflows:signal-timed-out`: a wait's deadline passed without a matching signal. */
export interface WorkflowSignalTimedOutEvent extends WorkflowEventBase {
  type: 'signal-timed-out';
  wait: string;
  signal: string;
}

/**
 * Channel `nestjs:workflows:journal-large`: the instance's journal grew past the module's
 * `journal.warnEntries` or `journal.warnBytes`. Emitted once, when it crosses the line.
 */
export interface WorkflowJournalLargeEvent extends WorkflowEventBase {
  type: 'journal-large';
  entries: number;
  bytes: number;
}

/**
 * Channel `nestjs:workflows:workflow-retried`: an operator's `WorkflowClient.retry()` reopened a
 * `failed` instance (it runs again from its journal) or a `compensation_failed` one (its
 * compensations run again).
 */
export interface WorkflowRetriedEvent extends WorkflowEventBase {
  type: 'workflow-retried';
  from: 'failed' | 'compensation_failed';
  /** The error the instance had. */
  error: SerializedWorkflowError | null;
}

/** Channel `nestjs:workflows:workflow-deleted`: `WorkflowClient.delete()` removed an instance. */
export interface WorkflowDeletedEvent extends WorkflowEventBase {
  type: 'workflow-deleted';
  /** The status it had. */
  status: WorkflowStatus;
}

export type WorkflowEvent =
  | WorkflowStartedEvent
  | WorkflowResumedEvent
  | WorkflowSuspendedEvent
  | WorkflowCompensatingEvent
  | WorkflowCompletedEvent
  | WorkflowFailedEvent
  | WorkflowCancelledEvent
  | WorkflowCompensationFailedEvent
  | WorkflowStepCompletedEvent
  | WorkflowStepFailedEvent
  | WorkflowStepCompensatedEvent
  | WorkflowSignalReceivedEvent
  | WorkflowSignalTimedOutEvent
  | WorkflowJournalLargeEvent
  | WorkflowRetriedEvent
  | WorkflowDeletedEvent;
