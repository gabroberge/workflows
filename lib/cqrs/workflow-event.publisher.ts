import { Injectable, Logger, type OnApplicationBootstrap, type OnModuleInit } from '@nestjs/common';
import { EventBus, UnhandledExceptionBus, type AsyncContext, type IEvent, type IEventPublisher } from '@nestjs/cqrs';
import { canonical, WorkflowClient } from '../services/workflow-client.service.js';
import { normalize } from '../services/workflow-execution.service.js';
import { ROUTE_EVENTS, WorkflowRegistry } from '../services/workflow-registry.service.js';
import type { WorkflowDispatcherContext } from './interfaces/workflow-dispatcher-context.interface.js';
import { WorkflowEventsExplorer, type WorkflowEventTargets } from './workflow-events.explorer.js';

/** One `start()` or `signal()` an event asks for. */
interface WorkflowWrite {
  event: object;
  run(transaction: unknown): Promise<unknown>;
}

/**
 * The CQRS `EventBus`'s publisher while `WorkflowsCqrsModule` is imported. It wraps the
 * publisher that was set before it (the in-memory one that feeds `@EventsHandler()`s and
 * sagas, or the one from `CqrsModule.forRoot({ eventPublisher })`): for an event that a
 * workflow maps with `@StartOn()` or `@SignalOn()`, it first starts and signals the workflows,
 * through the transaction in the dispatcher context when there is one, then hands the event to
 * the wrapped publisher. Other events go straight through, synchronously, as before.
 */
@Injectable()
export class WorkflowEventPublisher implements IEventPublisher, OnModuleInit, OnApplicationBootstrap {
  private readonly logger = new Logger('WorkflowsCqrsModule');
  private readonly inner: IEventPublisher;
  private routes?: Map<Function, WorkflowEventTargets>;

  constructor(
    private readonly eventBus: EventBus,
    private readonly unhandledExceptionBus: UnhandledExceptionBus,
    private readonly workflowClient: WorkflowClient,
    private readonly explorer: WorkflowEventsExplorer,
    registry: WorkflowRegistry,
  ) {
    // In the constructor, so no lifecycle hook can publish an event before it's installed.
    this.inner = eventBus.publisher;
    eventBus.publisher = this;
    registry[ROUTE_EVENTS]();
  }

  onModuleInit(): void {
    const routes = this.table();
    if (routes.size === 0) {
      return;
    }

    const described = [...routes].map(([event, { starts, signals }]) => {
      const actions = [
        ...starts.map(({ name }) => `starts ${name}`),
        ...new Set(signals.map(({ route }) => `signals ${route.signal}`)),
      ];
      return `${event.name} ${actions.join(', ')}`;
    });
    this.logger.log(`Events routed to workflows: ${described.join('; ')}`);
  }

  onApplicationBootstrap(): void {
    if (this.eventBus.publisher !== this) {
      throw new Error(
        `WorkflowsCqrsModule: EventBus.publisher was replaced (by ${this.eventBus.publisher?.constructor?.name ?? 'nothing'}) ` +
          'after WorkflowsCqrsModule installed its publisher, so events would no longer start or signal workflows. ' +
          'Set your publisher with CqrsModule.forRoot({ eventPublisher }) instead: WorkflowsCqrsModule wraps it.',
      );
    }
  }

  publish<TEvent extends IEvent>(event: TEvent, dispatcherContext?: unknown, asyncContext?: AsyncContext): unknown {
    if (!this.isRouted(event)) {
      return this.inner.publish(event, dispatcherContext, asyncContext);
    }

    return this.deliver([event], dispatcherContext, () => this.inner.publish(event, dispatcherContext, asyncContext));
  }

  publishAll<TEvent extends IEvent>(events: TEvent[], dispatcherContext?: unknown, asyncContext?: AsyncContext): unknown {
    const all = events ?? [];
    const forward = () =>
      this.inner.publishAll
        ? this.inner.publishAll(all, dispatcherContext, asyncContext)
        : all.map((event) => this.inner.publish(event, dispatcherContext, asyncContext));
    if (!all.some((event) => this.isRouted(event))) {
      return forward();
    }

    return this.deliver(all, dispatcherContext, async () => {
      const result = forward();
      return Array.isArray(result) ? Promise.all(result) : result;
    });
  }

  /**
   * Starts and signals the workflows the events map to, then forwards the events. Nothing is
   * forwarded when a write fails, so a retried publish reaches the handlers once.
   */
  private deliver(events: object[], context: unknown, forward: () => unknown): Promise<unknown> {
    const failure: { event?: object } = {};
    const delivered = this.write(events, transactionOf(context), failure).then(forward);

    // An aggregate's commit() drops the promise that publish() returns: report its failures
    // the way cqrs reports a failing event handler, rather than as an unhandled rejection.
    if (isAggregate(context)) {
      delivered.catch((exception: unknown) => this.report(failure.event ?? events[0], exception));
    }

    return delivered;
  }

  private async write(events: object[], transaction: unknown, failure: { event?: object }): Promise<void> {
    const writes = this.plan(events);

    if (transaction === undefined) {
      for (const write of writes) {
        failure.event = write.event;
        await write.run(undefined);
      }
      failure.event = undefined;
      return;
    }

    // Issued back to back, not awaited in between: a driver whose transactions are synchronous
    // must see every statement before the application's callback returns, and on the
    // transaction's one connection they still run in this order.
    const settled = await Promise.allSettled(writes.map((write) => write.run(transaction)));
    const index = settled.findIndex((result) => result.status === 'rejected');
    if (index !== -1) {
      failure.event = writes[index].event;
      throw (settled[index] as PromiseRejectedResult).reason;
    }
  }

  /**
   * Every start and signal, computed before any is written: a mapping function that throws
   * leaves nothing half done. Per event, starts come first, then signals, each signal once.
   */
  private plan(events: object[]): WorkflowWrite[] {
    const writes: WorkflowWrite[] = [];

    for (const event of events) {
      const targets = this.targetsOf(event);
      if (!targets) {
        continue;
      }

      for (const { workflow, route } of targets.starts) {
        const id = route.id(event);
        const input = route.input ? route.input(event) : event;
        writes.push({ event, run: (transaction) => this.workflowClient.start(workflow, input, { id, transaction }) });
      }

      // A signal reaches every instance waiting with its name and key, whatever the workflow:
      // two declarations that agree are one signal, and two that disagree are a mistake.
      const sent = new Map<string, { payload: string; id: string | undefined; name: string }>();
      for (const { name, route } of targets.signals) {
        const key = route.key ? route.key(event) : undefined;
        assertString(event, name, 'key', key, 'Waits match keys exactly, so convert it (String(event.orderId)) on both sides.');
        const id = route.id ? route.id(event) : undefined;
        assertString(event, name, 'id', id, 'Derive it from the event, such as event.deliveryId.');

        const payload = route.payload ? route.payload(event) : event;
        const identity = JSON.stringify([route.signal, key ?? null]);
        const json = canonical(normalize(payload) ?? null);
        const earlier = sent.get(identity);
        if (earlier) {
          if (earlier.payload !== json || earlier.id !== id) {
            throw new Error(
              `${event.constructor.name} maps to two different ${earlier.payload !== json ? 'payloads' : 'ids'} for the signal "${route.signal}"` +
                `${key === undefined ? '' : ` (key "${key}")`}: @SignalOn() on ${earlier.name} and on ${name}. ` +
                'A signal reaches every instance waiting for it: make the declarations agree, or keep one.',
            );
          }
          continue;
        }

        sent.set(identity, { payload: json, id, name });
        writes.push({ event, run: (transaction) => this.workflowClient.signal(route.signal, payload, { key, id, transaction }) });
      }
    }

    return writes;
  }

  private report(event: object, exception: unknown): void {
    this.logger.error(
      `Publishing ${event.constructor.name} from an aggregate's commit() failed, and commit() doesn't wait for it. ` +
        'Publish the aggregate\'s events with eventBus.publishAll(aggregate.getUncommittedEvents(), { transaction }) to handle it.',
      exception instanceof Error ? exception.stack : String(exception),
    );
    this.unhandledExceptionBus.publish({ cause: event, exception });
  }

  private isRouted(event: unknown): boolean {
    return this.targetsOf(event) !== undefined;
  }

  private targetsOf(event: unknown): WorkflowEventTargets | undefined {
    if (event === null || typeof event !== 'object') {
      return undefined;
    }
    return this.table().get(event.constructor);
  }

  /** Built at `onModuleInit`, or at the first publish if another module's hook publishes earlier. */
  private table(): Map<Function, WorkflowEventTargets> {
    return (this.routes ??= this.explorer.explore());
  }
}

/** A key may be empty (it matches waits for ''), an id may not (WorkflowClient.signal() refuses it). */
function assertString(event: object, workflow: string, option: 'key' | 'id', value: unknown, advice: string): void {
  if (value !== undefined && (typeof value !== 'string' || (option === 'id' && value.length === 0))) {
    throw new TypeError(
      `@SignalOn(${event.constructor.name}) on ${workflow}: \`${option}\` returned ${value === '' ? 'an empty string' : `${typeof value}, not a string`}. ${advice}`,
    );
  }
}

/** The dispatcher context's `transaction`. An aggregate (what `mergeObjectContext()` passes) never has one. */
function transactionOf(context: unknown): unknown {
  if (context === null || typeof context !== 'object' || isAggregate(context)) {
    return undefined;
  }
  return (context as Partial<WorkflowDispatcherContext>).transaction;
}

function isAggregate(context: unknown): boolean {
  return typeof (context as { getUncommittedEvents?: unknown } | null | undefined)?.getUncommittedEvents === 'function';
}
