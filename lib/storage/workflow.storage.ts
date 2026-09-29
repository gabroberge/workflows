import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { InMemoryWorkflowStore } from '../stores/in-memory-workflow.store.js';
import type { WorkflowStore } from '../interfaces/workflow-store.interface.js';
import { WORKFLOWS_MODULE_OPTIONS } from '../workflows.module-definition.js';
import type { WorkflowsModuleOptions } from '../interfaces/workflows-module-options.interface.js';
import type { WorkflowStorageRegisterOptions } from '../interfaces/workflow-storage.interface.js';

/** Internal: locks the registry. `WorkflowsModule.onModuleInit()` and the first read call it. */
export const LOCK_STORAGE = Symbol('WorkflowStorage.lock');

const REQUIRED = [
  'create',
  'get',
  'list',
  'requestCancel',
  'reopen',
  'delete',
  'signal',
  'signals',
  'purge',
  'saveSchedule',
  'getSchedule',
  'listSchedules',
  'deleteSchedule',
  'claimSchedules',
  'writeSchedule',
  'claim',
  'renew',
  'write',
] as const;
const OPTIONAL = ['createInTransaction', 'signalInTransaction'] as const;
const DEFAULT = 'InMemoryWorkflowStore (the default: state is lost on restart and not shared between instances)';

/**
 * Where the application tells the package which `WorkflowStore` to use. A provider that
 * implements `WorkflowStore` injects it and registers itself in its constructor:
 *
 * ```ts
 * @Injectable()
 * export class DrizzleWorkflowStore implements WorkflowStore {
 *   constructor(@InjectDrizzle() private readonly db: Database, storage: WorkflowStorage) {
 *     storage.registerSource(this);
 *   }
 *   // ...
 * }
 * ```
 *
 * With no source registered, the package uses an `InMemoryWorkflowStore`. The registry locks
 * in `WorkflowsModule`'s `onModuleInit`, when every provider constructor has run and before
 * the worker starts, or at the first read of the store if that is earlier (another module's
 * `onModuleInit`). In production, it refuses the in-memory default unless
 * `allowInMemoryStorage` is set.
 */
@Injectable()
export class WorkflowStorage {
  private readonly logger = new Logger('WorkflowsModule');
  private registered?: WorkflowStore;
  private active?: WorkflowStore;

  constructor(@Optional() @Inject(WORKFLOWS_MODULE_OPTIONS) private readonly options?: WorkflowsModuleOptions) {}

  /**
   * Makes `source` the store. Call it from the constructor of a singleton provider. Throws
   * when `source` lacks a method of `WorkflowStore`, when another source is registered
   * (unless `replace`: tests, wrappers), and once the registry has locked.
   */
  registerSource(source: WorkflowStore, options: WorkflowStorageRegisterOptions = {}): void {
    validate(source);

    if (this.active) {
      throw new Error(
        `WorkflowStorage.registerSource(): ${nameOf(source)} registered after WorkflowsModule initialized (or after ` +
          `its storage was first read), which already uses ${this.registered ? nameOf(this.active) : DEFAULT}. ` +
          'Register from the constructor of a singleton provider: providers of lazy-loaded modules, request-scoped ' +
          'and transient providers, and lifecycle hooks run too late.',
      );
    }

    if (this.registered && !options.replace) {
      throw new Error(
        `WorkflowStorage.registerSource(): ${nameOf(source)} can't register, ` +
          `${this.registered === source ? 'it already did (the same instance, twice)' : `${nameOf(this.registered)} already did`}. ` +
          'Register one store, or pass { replace: true } to replace it on purpose (tests, wrappers).',
      );
    }

    this.registered = source;
  }

  /** The store in use: the registered one, or the in-memory default. Reading it locks the registry. */
  get source(): WorkflowStore {
    if (!this.active) {
      this[LOCK_STORAGE]();
    }
    return this.active!;
  }

  /** Fixes the source, logs it, and enforces the production guard (which leaves the registry open). */
  [LOCK_STORAGE](): void {
    if (this.active) {
      return;
    }

    if (!this.registered && process.env.NODE_ENV === 'production' && !this.options?.allowInMemoryStorage) {
      throw new Error(
        'WorkflowStorage: no WorkflowStore is registered, and NODE_ENV is "production": in memory, running workflows ' +
          'would be lost on restart and not shared between instances. Implement WorkflowStore in a provider that ' +
          'injects WorkflowStorage and calls `storage.registerSource(this)` in its constructor, or set ' +
          '`allowInMemoryStorage: true` in the WorkflowsModule options to run in memory anyway.',
      );
    }

    this.active = this.registered ?? new InMemoryWorkflowStore();
    this.logger.log(`WorkflowStorage: ${this.registered ? nameOf(this.registered) : DEFAULT}`);
  }
}

/** Throws unless `source` has every method of `WorkflowStore` (the optional ones may be absent). */
function validate(source: WorkflowStore): void {
  if (source === null || typeof source !== 'object') {
    throw new TypeError(`WorkflowStorage.registerSource(): expected an object implementing WorkflowStore, got ${nameOf(source)}.`);
  }

  const candidate = source as unknown as Record<string, unknown>;
  const missing = REQUIRED.filter((method) => typeof candidate[method] !== 'function');
  const invalid = OPTIONAL.filter((method) => candidate[method] !== undefined && typeof candidate[method] !== 'function');
  if (missing.length > 0 || invalid.length > 0) {
    const problems = [
      ...(missing.length ? [`${missing.map((m) => `${m}()`).join(', ')} ${missing.length === 1 ? 'is' : 'are'} missing`] : []),
      ...(invalid.length ? [`${invalid.join(', ')} ${invalid.length === 1 ? 'is' : 'are'} set but not a method`] : []),
    ];
    throw new TypeError(`WorkflowStorage.registerSource(): ${nameOf(source)} doesn't implement WorkflowStore: ${problems.join('; ')}.`);
  }
}

/** How a message names a value: its class, or what it is instead of an instance. */
function nameOf(value: unknown): string {
  if (typeof value === 'function') {
    return `the class ${value.name || '(anonymous)'} (pass an instance)`;
  }
  if (value === null || typeof value !== 'object') {
    return String(value);
  }

  const name = (value as object).constructor?.name;
  return name && name !== 'Object' ? name : 'an object';
}
