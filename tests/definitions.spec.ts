/**
 * `@Workflow()` and how the engine finds workflow classes: the decorator's validation, the
 * startup errors for definitions it can't run, discovery wherever a workflow is provided,
 * and what `run()` sees about its own instance.
 */
import { Inject, Injectable, Module, Scope } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import {
  ManualWorkflowClock,
  Workflow,
  WorkflowClient,
  WorkflowNotFoundError,
  WorkflowsModule,
  WorkflowWorker,
  type WorkflowContext,
} from '../lib/index.js';
import { boot, storeKind, tempDb, type Node, type TestDb, World } from './support.js';

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  world = new World();
});

afterEach(async () => {
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

const start = async (workflows: any[], providers: any[] = []) => {
  const node = await boot({ db, clock, workflows, providers: [{ provide: World, useValue: world }, ...providers] });
  nodes.push(node);
  return node;
};

describe('@Workflow()', () => {
  it('rejects a name the engine could not key instances by', () => {
    for (const name of ['', 'order fulfilment', 'orders/fulfil', 'orders@2']) {
      expect(() => Workflow(name)).toThrow(`Invalid workflow name "${name}". Use letters, digits, ".", ":", "_" or "-".`);
    }

    expect(() => Workflow('orders.fulfil:v2_eu-west')).not.toThrow();
  });

  it('rejects a version that is not a positive integer, and defaults to 1', async () => {
    for (const version of [0, -1, 1.5, Number.NaN]) {
      expect(() => Workflow('versioned', { version })).toThrow(`Invalid version ${version} for workflow "versioned". Use a positive integer.`);
    }

    @Workflow('unversioned')
    class Unversioned {
      async run() {
        return 'ok';
      }
    }

    const node = await start([Unversioned]);
    expect(await node.client.start(Unversioned, undefined, { id: 'u' })).toMatchObject({ workflow: 'unversioned', version: 1, created: true });
  });

  it('hands run() the instance id, workflow name and version', async () => {
    @Workflow('introspect', { version: 3 })
    class Introspect {
      async run(ctx: WorkflowContext) {
        return { id: ctx.workflowId, name: ctx.workflowName, version: ctx.version };
      }
    }

    const node = await start([Introspect]);
    await node.client.start(Introspect, undefined, { id: 'self-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('self-1')).toMatchObject({
      status: 'completed',
      output: { id: 'self-1', name: 'introspect', version: 3 },
    });
  });
});

describe('startup errors', () => {
  it('refuses two classes that claim the same name and version', async () => {
    @Workflow('dup')
    class DupA {
      async run() {}
    }

    @Workflow('dup')
    class DupB {
      async run() {}
    }

    await expect(boot({ db, clock, workflows: [DupA, DupB] })).rejects.toThrow(
      'Workflow "dup@1" is defined twice (DupA and DupB). Bump the version of one of them.',
    );
  });

  it('refuses a workflow class without a run() method', async () => {
    @Workflow('no-run')
    class NoRun {
      execute() {}
    }

    await expect(boot({ db, clock, workflows: [NoRun] })).rejects.toThrow('Workflow NoRun must have a run(ctx, input) method.');
  });

  it('refuses transient and request-scoped workflows: every instance shares one singleton', async () => {
    @Workflow('transient')
    class TransientFlow {
      async run() {}
    }

    await expect(
      boot({ db, clock, workflows: [], providers: [{ provide: TransientFlow, useClass: TransientFlow, scope: Scope.TRANSIENT }] }),
    ).rejects.toThrow('Workflow TransientFlow must be a singleton; request-scoped and transient workflows are not supported.');

    // Request scope is inherited from a dependency.
    @Injectable({ scope: Scope.REQUEST })
    class RequestState {}

    @Workflow('request-bound')
    class RequestBound {
      constructor(@Inject(RequestState) readonly state: RequestState) {}
      async run() {}
    }

    await expect(boot({ db, clock, workflows: [RequestBound, RequestState] })).rejects.toThrow(
      'Workflow RequestBound must be a singleton; request-scoped and transient workflows are not supported.',
    );
  });
});

describe('discovery', () => {
  it('finds a workflow provided by a factory, by the class of the instance it returns', async () => {
    @Workflow('from-factory')
    class FromFactory {
      constructor(private readonly world: World) {}

      async run(ctx: WorkflowContext) {
        return ctx.step('call', (s) => {
          this.world.record('call', s.idempotencyKey);
          return 'made by a factory';
        });
      }
    }

    const node = await start([], [{ provide: 'FROM_FACTORY', inject: [World], useFactory: (w: World) => new FromFactory(w) }]);
    await node.client.start(FromFactory, undefined, { id: 'ff-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('ff-1')).toMatchObject({ status: 'completed', output: 'made by a factory' });
    expect(world.calls).toEqual([{ op: 'call', key: 'ff-1:call', attempt: undefined }]);
  });

  it('refuses to start a class that is not a workflow, naming it', async () => {
    class PlainService {
      async run() {}
    }

    const node = await start([]);
    const attempt = node.client.start(PlainService, undefined, { id: 'plain' });
    await expect(attempt).rejects.toBeInstanceOf(WorkflowNotFoundError);
    await expect(attempt).rejects.toThrow('PlainService is not decorated with @Workflow().');
    expect(await node.client.getStatus('plain')).toBeNull();
  });

  it('refuses to start a name this process does not register unless the version is pinned', async () => {
    const node = await start([]);
    await expect(node.client.start('invoice-batch', {})).rejects.toThrow(
      'Workflow "invoice-batch" is not registered in this application. Register it, or pass { version } to start it from a process that does not run it.',
    );

    // Pinned, it is created for a worker elsewhere, and this process never claims it.
    expect(await node.client.start('invoice-batch', {}, { id: 'ib-1', version: 4 })).toMatchObject({ version: 4, status: 'pending' });
    expect(await node.worker.drain()).toBe(0);
  });
});

// No store involved: once.
describe.runIf(storeKind === 'memory')('discovery across modules', () => {
  it('finds workflows in any module, including one that does not import WorkflowsModule', async () => {
    @Workflow('in-feature')
    class InFeature {
      async run(ctx: WorkflowContext) {
        return ctx.step('call', () => 'feature');
      }
    }

    @Module({ providers: [InFeature] })
    class FeatureModule {}

    const moduleRef = await Test.createTestingModule({
      imports: [WorkflowsModule.forRoot({ clock, worker: false }), FeatureModule],
    }).compile();
    await moduleRef.init();

    try {
      await moduleRef.get(WorkflowClient).start(InFeature, undefined, { id: 'feat-1' });
      expect(await moduleRef.get(WorkflowWorker).drain()).toBe(1);
      expect(await moduleRef.get(WorkflowClient).getStatus('feat-1')).toMatchObject({ status: 'completed', output: 'feature' });
    } finally {
      await moduleRef.close();
    }
  });
});
