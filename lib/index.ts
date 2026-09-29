// Module
export { WorkflowsModule } from './workflows.module.js';
export { WORKFLOWS_MODULE_OPTIONS } from './workflows.module-definition.js';
export type {
  WorkflowsModuleAsyncOptions,
  WorkflowsModuleOptions,
  WorkflowJournalLimits,
  WorkflowsOptionsFactory,
  WorkflowWorkerOptions,
} from './interfaces/index.js';

// Writing workflows
export * from './decorators/index.js';
export { WorkflowSignal } from './signals/index.js';
export type {
  ChildWorkflowHandle,
  Journaled,
  SignalWaitOptions,
  StartChildWorkflowOptions,
  WaitForSignalOptions,
  WorkflowAnyResult,
  WorkflowCompensationContext,
  WorkflowConcurrency,
  WorkflowCondition,
  WorkflowContext,
  WorkflowDecoratorOptions,
  WorkflowRateLimit,
  WorkflowRetryOptions,
  WorkflowRunner,
  WorkflowStepContext,
  WorkflowStepOptions,
} from './interfaces/index.js';
export type { Duration } from './utils/index.js';

// Starting, signalling, inspecting and cancelling instances, and the schedules that start them
export { WorkflowClient, WorkflowSchedules } from './services/index.js';
export type {
  SignalWorkflowOptions,
  StartWorkflowOptions,
  WorkflowCancelResult,
  WorkflowDeleteOptions,
  WorkflowInstance,
  WorkflowJournalEntry,
  WorkflowListFilter,
  WorkflowParentClose,
  WorkflowPurgeOptions,
  WorkflowResultOptions,
  WorkflowRetryInstanceOptions,
  UpsertWorkflowScheduleOptions,
  WorkflowSchedule,
  WorkflowScheduleDeclaration,
  WorkflowScheduleListFilter,
  WorkflowScheduleMissed,
  WorkflowScheduleOccurrence,
  WorkflowScheduleOptions,
  WorkflowScheduleOverlap,
  WorkflowSchedulePreviewOptions,
  WorkflowSchedulePreviewSpec,
  WorkflowScheduleTiming,
  WorkflowSignalSendResult,
  WorkflowStartResult,
  WorkflowStatus,
  WorkflowWait,
} from './interfaces/index.js';

// Errors
export {
  ChildWorkflowFailedError,
  isWorkflowInterrupt,
  NonRetryableStepError,
  StepFailedError,
  StepTimeoutError,
  WorkflowDefinitionError,
  WorkflowError,
  WorkflowFailedError,
  WorkflowIdConflictError,
  WorkflowInterrupt,
  WorkflowNonDeterminismError,
  WorkflowNotFoundError,
  WorkflowResultTimeoutError,
  WorkflowStateError,
  type WorkflowFailureStatus,
} from './errors/index.js';
export type { SerializedWorkflowError } from './interfaces/index.js';

// Observability: each event is also published on its `nestjs:workflows:<type>` diagnostics channel
export {
  WorkflowEvents,
  type WorkflowCancelledEvent,
  type WorkflowChildStartedEvent,
  type WorkflowCompensatingEvent,
  type WorkflowCompensationFailedEvent,
  type WorkflowCompletedEvent,
  type WorkflowCustomStatusEvent,
  type WorkflowDeletedEvent,
  type WorkflowEvent,
  type WorkflowFailedEvent,
  type WorkflowJournalLargeEvent,
  type WorkflowResumedEvent,
  type WorkflowRetriedEvent,
  type WorkflowScheduleSkippedEvent,
  type WorkflowSignalReceivedEvent,
  type WorkflowSignalTimedOutEvent,
  type WorkflowStartedEvent,
  type WorkflowStepCompensatedEvent,
  type WorkflowStepCompletedEvent,
  type WorkflowStepFailedEvent,
  type WorkflowSuspendedEvent,
} from './events/index.js';

// Storage: the interface your store implements, the registry it registers with in its
// constructor, and the in-memory default (for development and tests, and the test double).
// The contract's test suite is `@nestjs/workflows/testing` (lib/testing/index.ts).
export { WorkflowStorage } from './storage/index.js';
export type {
  NewWorkflowInstance,
  NewWorkflowSignal,
  WorkflowCancelRequest,
  WorkflowClaim,
  WorkflowClaimRequest,
  WorkflowConcurrencyLimit,
  WorkflowInstanceDetails,
  WorkflowListQuery,
  WorkflowPurgeQuery,
  WorkflowPurgeResult,
  WorkflowRateLimitRule,
  WorkflowRateWindow,
  WorkflowRelease,
  WorkflowReopen,
  WorkflowScheduleClaimRequest,
  WorkflowScheduleQuery,
  WorkflowScheduleRecord,
  WorkflowScheduleSave,
  WorkflowScheduleWrite,
  WorkflowSignalQuery,
  WorkflowSignalRecord,
  WorkflowSignalResult,
  WorkflowStorageRegisterOptions,
  WorkflowStore,
  WorkflowWrite,
} from './interfaces/index.js';
export * from './stores/index.js';

// Testing: a clock that only moves when told to, and `WorkflowWorker.drain()` to run due
// instances now (with the contract suite in `@nestjs/workflows/testing`)
export { ManualWorkflowClock } from './utils/index.js';
export type { WorkflowClock } from './interfaces/index.js';
export { WorkflowWorker } from './services/index.js';
