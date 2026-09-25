/**
 * `WorkflowClient` at its edges: idempotent starts compared by value, what start() and
 * signal() do to their payloads, the views getStatus() and cancel() return, and the errors
 * for calls the store or the arguments can't serve.
 */
import { Test } from '@nestjs/testing';
import {
  InMemoryWorkflowStore,
  ManualWorkflowClock,
  Workflow,
  WorkflowClient,
  WorkflowIdConflictError,
  WorkflowsModule,
  WorkflowStorage,
  type WorkflowContext,
  type WorkflowStore,
} from '../lib/index.js';
import { boot, storeKind, tempDb, type Node, type TestDb } from './support.js';

let db: TestDb;
let clock: ManualWorkflowClock;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
});

afterEach(async () => {
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

const start = async (workflows: any[]) => {
  const node = await boot({ db, clock, workflows });
  nodes.push(node);
  return node;
};

@Workflow('echo')
class Echo {
  async run(_ctx: WorkflowContext, input: unknown) {
    return { input, keys: input && typeof input === 'object' ? Object.keys(input) : null };
  }
}

@Workflow('listen')
class Listen {
  async run(ctx: WorkflowContext) {
    const received: unknown[] = [];
    for (const key of ['zero', 'false', 'empty', 'date']) {
      received.push(await ctx.waitForSignal(`wait-${key}`, 'value', { key }));
    }
    return received;
  }
}

describe('start()', () => {
  it('treats the same input with its keys in another order as the same start', async () => {
    const node = await start([Echo]);
    const first = await node.client.start(Echo, { a: 1, b: { c: 1, d: [1, 2] } }, { id: 'e-1' });
    const again = await node.client.start(Echo, { b: { d: [1, 2], c: 1 }, a: 1 }, { id: 'e-1' });

    expect(first).toMatchObject({ created: true });
    expect(again).toEqual({ id: 'e-1', workflow: 'echo', version: 1, created: false, status: 'pending' });

    // Array order is part of the value.
    await expect(node.client.start(Echo, { a: 1, b: { c: 1, d: [2, 1] } }, { id: 'e-1' })).rejects.toThrow(
      'Instance "e-1" of "echo" already exists with a different input.',
    );
  });

  it('refuses an id taken by another workflow, whatever the input', async () => {
    @Workflow('other')
    class Other {
      async run() {}
    }

    const node = await start([Echo, Other]);
    await node.client.start(Echo, { a: 1 }, { id: 'shared' });

    const attempt = node.client.start(Other, { a: 1 }, { id: 'shared' });
    await expect(attempt).rejects.toBeInstanceOf(WorkflowIdConflictError);
    await expect(attempt).rejects.toThrow('Instance "shared" already exists for workflow "echo", not "other".');
  });

  it('returns the current status and version of an existing instance, also once it finished', async () => {
    const node = await start([Echo]);
    await node.client.start(Echo, undefined, { id: 'done' });
    await node.worker.drain();

    // An undefined input matches itself, although a store may read it back as null.
    expect(await node.client.start(Echo, undefined, { id: 'done' })).toEqual({
      id: 'done',
      workflow: 'echo',
      version: 1,
      created: false,
      status: 'completed',
    });
  });

  it('hands run() the input as JSON: dates as strings, undefined fields gone', async () => {
    const node = await start([Echo]);
    await node.client.start(Echo, { at: new Date(0), note: undefined, tags: ['x', undefined] }, { id: 'json' });
    await node.worker.drain();

    expect(await node.client.getStatus('json')).toMatchObject({
      status: 'completed',
      input: { at: '1970-01-01T00:00:00.000Z', tags: ['x', null] },
      output: { input: { at: '1970-01-01T00:00:00.000Z', tags: ['x', null] }, keys: ['at', 'tags'] },
    });
  });

  it('refuses an input that is not JSON before creating anything', async () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;

    const node = await start([Echo]);
    await expect(node.client.start(Echo, { amount: 10n }, { id: 'big' })).rejects.toThrow(TypeError);
    await expect(node.client.start(Echo, circular, { id: 'loop' })).rejects.toThrow(TypeError);
    expect(await node.client.list()).toEqual([]);
  });

  it('generates a fresh id for each start without one', async () => {
    const node = await start([Echo]);
    const a = await node.client.start(Echo, { same: true });
    const b = await node.client.start(Echo, { same: true });

    expect(a.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(b.id).not.toBe(a.id);
    expect([a.created, b.created]).toEqual([true, true]);
    expect(await node.client.list()).toHaveLength(2);
  });
});

describe('getStatus() and cancel()', () => {
  it('returns null for an unknown id, and the journal only on request', async () => {
    const node = await start([Echo]);
    await node.client.start(Echo, { a: 1 }, { id: 'e-1' });
    await node.worker.drain();

    expect(await node.client.getStatus('missing')).toBeNull();
    expect(await node.client.getStatus('missing', { journal: true })).toBeNull();

    const plain = await node.client.getStatus('e-1');
    expect(plain).not.toHaveProperty('journal');
    expect(plain).toMatchObject({ id: 'e-1', workflow: 'echo', version: 1, status: 'completed', waits: [] });
    expect(await node.client.getStatus('e-1', { journal: true })).toMatchObject({ journal: [] });
  });

  it('returns the instance without its waits or journal, and names a cancel without reason "Cancelled."', async () => {
    const node = await start([Listen]);
    await node.client.start(Listen, undefined, { id: 'l-1' });
    await node.worker.drain();

    const result = await node.client.cancel('l-1');
    expect(result).toMatchObject({ id: 'l-1', accepted: true, cancelRequested: true, cancelReason: null, status: 'suspended' });
    expect(result).not.toHaveProperty('waits');
    expect(result).not.toHaveProperty('journal');

    await node.worker.drain();
    expect(await node.client.getStatus('l-1')).toMatchObject({
      status: 'cancelled',
      error: { name: 'WorkflowCancelledError', message: 'Cancelled.' },
    });
  });
});

describe('signal()', () => {
  it('delivers falsy payloads as themselves, and payloads as JSON', async () => {
    const node = await start([Listen]);
    await node.client.start(Listen, undefined, { id: 'l-1' });
    await node.worker.drain();

    await node.client.signal('value', 0, { key: 'zero' });
    await node.client.signal('value', false, { key: 'false' });
    await node.client.signal('value', '', { key: 'empty' });
    await node.client.signal('value', { at: new Date(0), dropped: undefined }, { key: 'date' });
    await node.worker.drain();

    expect(await node.client.getStatus('l-1')).toMatchObject({
      status: 'completed',
      output: [0, false, '', { at: '1970-01-01T00:00:00.000Z' }],
    });
    expect((await node.client.getStatus('l-1'))!.output).toEqual([0, false, '', { at: '1970-01-01T00:00:00.000Z' }]);
  });

  it('returns increasing signal ids, and counts only the instances it wakes', async () => {
    const node = await start([Listen]);
    await node.client.start(Listen, undefined, { id: 'l-1' });
    await node.client.start(Listen, undefined, { id: 'l-2' });
    await node.worker.drain();

    const first = await node.client.signal('value', 1, { key: 'zero' });
    expect(first.woken).toBe(2);

    // Both are due already: a second matching signal wakes nobody new.
    const second = await node.client.signal('value', 2, { key: 'zero' });
    expect(second.signalId).toBeGreaterThan(first.signalId);
    expect(second.woken).toBe(0);
  });

  it('rejects an empty signal name before writing anything', async () => {
    const node = await start([]);
    await expect(node.client.signal('', { any: 1 })).rejects.toThrow('Invalid signal name "". Use a non-empty string such as "shipment.delivered".');
  });
});

// No store involved: once.
describe.runIf(storeKind === 'memory')('transactions on a store without the optional methods', () => {
  /** A store that implements only the required methods. */
  class BareStore implements WorkflowStore {
    private readonly inner = new InMemoryWorkflowStore();
    create = this.inner.create.bind(this.inner);
    get = this.inner.get.bind(this.inner);
    list = this.inner.list.bind(this.inner);
    requestCancel = this.inner.requestCancel.bind(this.inner);
    signal = this.inner.signal.bind(this.inner);
    signals = this.inner.signals.bind(this.inner);
    claim = this.inner.claim.bind(this.inner);
    renew = this.inner.renew.bind(this.inner);
    write = this.inner.write.bind(this.inner);
  }

  it('refuses start() and signal() with { transaction }, and writes nothing', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [WorkflowsModule.forRoot({ clock, worker: false })], providers: [Echo] }).compile();
    const store = new BareStore();
    moduleRef.get(WorkflowStorage).registerSource(store);
    await moduleRef.init();
    const client = moduleRef.get(WorkflowClient);

    try {
      await expect(client.start(Echo, {}, { id: 'tx', transaction: {} })).rejects.toThrow(
        'start() with { transaction } needs a WorkflowStore on your database that implements createInTransaction(); ' +
          'BareStore has none. See "Implementing a store" in the README.',
      );
      await expect(client.signal('value', 1, { transaction: {} })).rejects.toThrow(
        'signal() with { transaction } needs a WorkflowStore on your database that implements signalInTransaction(); BareStore has none.',
      );

      expect(await store.get('tx')).toBeNull();
      expect(await store.signals({ name: 'value', key: null, afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).toEqual([]);

      // Without a transaction, the same store serves both.
      expect(await client.start(Echo, {}, { id: 'plain' })).toMatchObject({ created: true });
    } finally {
      await moduleRef.close();
    }
  });
});
