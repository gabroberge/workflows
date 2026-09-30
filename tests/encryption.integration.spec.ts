/**
 * `AesGcmPayloadCodec` end to end, read from the database itself (every row of every table, or the in-memory
 * store's own state): nothing of what workflows store for the application is readable there, keys rotate under
 * instances in flight, a key rotation that is rolled back leaves the instances it wrote operable as documented,
 * payloads stored before the codec keep replaying, and strings that look like the envelope stay strings.
 */
import { randomBytes } from 'node:crypto';
import { inspect } from 'node:util';
import { Injectable, Logger, type Type } from '@nestjs/common';
import { sql } from 'drizzle-orm';
import {
  AesGcmPayloadCodec,
  InMemoryWorkflowStore,
  ManualWorkflowClock,
  NonRetryableStepError,
  Workflow,
  WorkflowSignal,
  type WorkflowContext,
  type WorkflowPayloadCodec,
} from '../lib/index.js';
import type { Database } from './fixtures/database/drizzle.js';
import { orders } from './fixtures/database/schema.js';
import { boot, connect, storeKind, tempDb, World, type Node, type TestDb } from './support.js';

/** What the application stores, one secret per place it is stored: a leak names the place. */
const S = {
  input: 'secret-input-4242',
  status: 'secret-status-7f1a',
  progress: 'secret-progress-81c2',
  retryError: 'secret-retry-error-19de',
  step: 'secret-step-result-5ab0',
  signal: 'secret-signal-payload-66e1',
  any: 'secret-any-payload-0c9d',
  childInput: 'secret-child-input-3b77',
  childOutput: 'secret-child-output-d2f4',
  output: 'secret-output-9e03',
  childFailure: 'secret-child-failure-47aa',
  failure: 'secret-failure-2c19',
  compensation: 'secret-compensation-e5b8',
  cancel: 'secret-cancel-reason-1d6f',
  terminate: 'secret-terminate-reason-a3c0',
  operator: 'secret-operator-retry-8b54',
  unread: 'secret-unread-signal-f09e',
  declared: 'secret-declared-schedule-6d21',
  upserted: 'secret-upserted-schedule-c4e7',
};

const K1 = randomBytes(32);
const K2 = randomBytes(32);
const aes = (keys: Record<string, Buffer>, current: string) => new AesGcmPayloadCodec({ keys, current });

const approved = new WorkflowSignal<{ by: string; note: string }>('refund.approved');
const shipped = new WorkflowSignal<{ note: string }>('parcel.shipped');
const enRoute = new WorkflowSignal<{ eta: string }>('courier.en-route');
const arrived = new WorkflowSignal<{ by: string }>('courier.arrived');
const message = new WorkflowSignal<string>('chat.message');

/** Switches the outside world's answers, shared by every process. */
@Injectable()
class Ledger {
  locked = true;
}

@Workflow('label')
class LabelWorkflow {
  async run(ctx: WorkflowContext, input: { address: string; fail?: boolean }) {
    if (input.fail) {
      ctx.fail(`No carrier delivers to ${S.childFailure}.`);
    }
    return { label: S.childOutput };
  }
}

@Workflow('refund')
class RefundWorkflow {
  async run(ctx: WorkflowContext, input: { orderId: string; card: string }) {
    ctx.setStatus({ stage: 'charging', note: S.status });
    const charge = await ctx.step(
      'charge',
      async ({ attempt, heartbeat }) => {
        await heartbeat({ checkpoint: S.progress });
        if (attempt === 1) {
          throw new Error(`Declined, try again: ${S.retryError}`);
        }
        // Over a KiB of JSON: deflated, then encrypted.
        return { receipt: S.step, lines: Array.from({ length: 60 }, (_, i) => `${S.step}-${i}`) };
      },
      { retry: { attempts: 2, backoff: { delay: '1s' } } },
    );
    const approval = await ctx.waitForSignal('approval', approved, { key: input.orderId });
    const dispatch = await ctx.waitForAny('dispatch', { shipped: ctx.signalWait(shipped, { key: input.orderId }), late: ctx.timer('1d') });
    const label = await ctx.executeChild(LabelWorkflow, { address: S.childInput });
    const reference = `${ctx.uuid()}@${ctx.now()}`;
    return {
      receipt: charge.receipt,
      approvedBy: approval!.by,
      dispatch: dispatch.key === 'shipped' ? dispatch.value.note : null,
      label: label.label,
      note: S.output,
      reference,
    };
  }
}

@Workflow('doomed-refund')
class DoomedRefundWorkflow {
  constructor(private readonly ledger: Ledger) {}

  async run(ctx: WorkflowContext, mode: 'child' | 'compensation' | 'wait' | 'operator') {
    if (mode === 'child') {
      await ctx.executeChild(LabelWorkflow, { address: 'nowhere', fail: true });
    } else if (mode === 'compensation') {
      await ctx.step('reserve', () => 'reserved', {
        compensate: () => {
          throw new Error(`Undoing the reservation failed: ${S.compensation}`);
        },
        compensateRetry: false,
      });
      ctx.fail(`Refund refused: ${S.failure}`);
    } else if (mode === 'wait') {
      await ctx.waitForSignal('never', approved, { key: ctx.workflowId });
    } else {
      await ctx.step('ledger', () => {
        if (this.ledger.locked) {
          throw new NonRetryableStepError(`The ledger is locked: ${S.operator}`);
        }
      });
    }
  }
}

@Workflow('nightly-refunds', { schedules: [{ id: 'nightly-refunds', every: '1d', input: { note: S.declared } }] })
class NightlyRefundsWorkflow {
  async run(_ctx: WorkflowContext, input: { note: string }) {
    return input.note.length;
  }
}

@Workflow('return')
class ReturnWorkflow {
  async run(ctx: WorkflowContext, input: { orderId: string }) {
    const pickup = await ctx.startChild(PickupWorkflow, { orderId: input.orderId }, { id: `${ctx.workflowId}/pickup` });
    return pickup.result();
  }
}

@Workflow('pickup')
class PickupWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { orderId: string }) {
    ctx.setStatus({ stage: 'booked' });
    await ctx.step('book', () => ({ booking: S.step }), { compensate: () => this.world.record('cancel-pickup', input.orderId) });
    const route = await ctx.waitForSignal('en-route', enRoute, { key: input.orderId });
    ctx.setStatus({ stage: 'en route', eta: route!.eta });
    const courier = await ctx.waitForSignal('arrived', arrived, { key: input.orderId });
    return courier!.by;
  }
}

@Workflow('ledger-sync', { concurrency: { limit: 1 } })
class LedgerSyncWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { account: string }) {
    await ctx.step('pull', () => ({ balance: S.step }));
    await ctx.sleep('settle', '1m');
    await ctx.step('push', () => this.world.record('push', input.account));
  }
}

@Workflow('bulky-export')
class BulkyExportWorkflow {
  async run(ctx: WorkflowContext) {
    // 1,000 characters that don't deflate: a kilobyte of JSON each, about 1.4 once encrypted.
    await ctx.step('part-0', () => randomBytes(750).toString('base64'));
    await ctx.step('part-1', () => randomBytes(750).toString('base64'));
    await ctx.sleep('breather', '1s');
    await ctx.step('part-2', () => randomBytes(750).toString('base64'));
  }
}

@Workflow('lookalike')
class LookalikeWorkflow {
  async run(ctx: WorkflowContext, input: string) {
    ctx.setStatus('$wf1:x:status');
    const stepped = await ctx.step('echo', () => '$wf1:rot13:step');
    const got = await ctx.waitForSignal('message', message, { key: ctx.workflowId });
    if (got === '$wf1:fail') {
      ctx.fail('$wf1:z:error');
    }
    return [input, stepped, got];
  }
}

let db: TestDb;
let clock: ManualWorkflowClock;
let world: World;
let ledger: Ledger;
const nodes: Node[] = [];

beforeEach(async () => {
  db = await tempDb();
  clock = new ManualWorkflowClock();
  world = new World();
  ledger = new Ledger();
});

afterEach(async () => {
  vi.restoreAllMocks();
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

async function start(codec: WorkflowPayloadCodec | WorkflowPayloadCodec[] | undefined, workflows: Type<unknown>[]) {
  const node = await boot({
    db,
    clock,
    codec,
    workflows,
    providers: [
      { provide: World, useValue: world },
      { provide: Ledger, useValue: ledger },
    ],
  });
  nodes.push(node);
  return node;
}

async function stop(node: Node) {
  nodes.splice(nodes.indexOf(node), 1);
  await node.close();
}

/** Everything the database holds, as someone who can read it sees it: every row of every table, or the in-memory store. */
async function dump(): Promise<string> {
  const connection = connect(db);
  try {
    if (connection.db instanceof InMemoryWorkflowStore) {
      return inspect(connection.db, { depth: null, maxArrayLength: null, maxStringLength: null, breakLength: Infinity });
    }

    const rows: string[] = [];
    for (const table of ['instances', 'journal', 'waits', 'signals', 'schedules', 'rate_limits'].map((name) => `nest_workflows.${name}`)) {
      const result = await connection.db.execute<{ row: string }>(sql.raw(`SELECT row_to_json(t)::text AS row FROM ${table} t`));
      rows.push(...result.rows.map((row) => row.row));
    }
    return rows.join('\n');
  } finally {
    await connection.close();
  }
}

const envelope = (key: string) => new RegExp(`^\\$wf1:aes-256-gcm:${key}\\.`);

describe('the database, with AesGcmPayloadCodec', () => {
  it('holds none of what workflows store for the application, only what it matches on', async () => {
    const node = await start(aes({ k1: K1 }, 'k1'), [RefundWorkflow, LabelWorkflow, DoomedRefundWorkflow, NightlyRefundsWorkflow]);
    await node.client.start(RefundWorkflow, { orderId: 'o-1', card: S.input }, { id: 'refund-1' });
    for (const mode of ['child', 'compensation', 'wait', 'operator'] as const) {
      await node.client.start(DoomedRefundWorkflow, mode, { id: `doomed-${mode}` });
    }
    await node.client.start(DoomedRefundWorkflow, 'wait', { id: 'doomed-terminated' });
    await node.worker.drain();
    clock.advance('1s');
    await node.worker.drain();

    await node.client.signal(approved, { by: 'agent-7', note: S.signal }, { key: 'o-1' });
    await node.client.signal(shipped, { note: S.any }, { key: 'o-1' });
    await node.client.signal(approved, { by: 'nobody', note: S.unread }, { key: 'nobody-waits' });
    await node.client.cancel('doomed-wait', `The customer asked: ${S.cancel}`);
    await node.client.terminate('doomed-terminated', `Stopped by support: ${S.terminate}`);
    await node.worker.drain();
    ledger.locked = false;
    await node.client.retry('doomed-operator');
    await node.client.schedules.upsert('tenant-refunds', { workflow: NightlyRefundsWorkflow, every: '1d', input: { note: S.upserted } });
    clock.advance('1d');
    await node.worker.drain();

    // Everything ran, and reads back as it was.
    expect(await node.client.result('refund-1')).toMatchObject({ receipt: S.step, approvedBy: 'agent-7', dispatch: S.any, label: S.childOutput, note: S.output });
    expect(await node.client.getStatus('refund-1')).toMatchObject({ input: { card: S.input }, customStatus: { note: S.status } });
    expect(await node.client.getStatus('doomed-child')).toMatchObject({ status: 'failed', error: { message: expect.stringContaining(S.childFailure) } });
    expect(await node.client.getStatus('doomed-compensation')).toMatchObject({
      status: 'compensation_failed',
      error: { message: expect.stringContaining(S.failure), compensation: { message: expect.stringContaining(S.compensation) } },
    });
    expect(await node.client.getStatus('doomed-wait')).toMatchObject({ status: 'cancelled', cancelReason: expect.stringContaining(S.cancel) });
    expect(await node.client.getStatus('doomed-terminated')).toMatchObject({ status: 'cancelled', error: { message: expect.stringContaining(S.terminate) } });
    expect(await node.client.getStatus('doomed-operator', { journal: true })).toMatchObject({
      status: 'completed',
      journal: [{ name: 'ledger', status: 'completed' }, { name: '$retry:1', data: { error: { message: expect.stringContaining(S.operator) } } }],
    });
    expect((await node.client.list({ workflow: 'nightly-refunds' })).map((instance) => instance.input)).toEqual([{ note: S.declared }, { note: S.upserted }]);
    expect(node.events.filter((event) => event.type === 'custom-status')).toContainEqual(expect.objectContaining({ status: { stage: 'charging', note: S.status } }));

    const stored = await dump();
    for (const [where, secret] of Object.entries(S)) {
      expect(stored.includes(secret), `the database holds the ${where} as it is`).toBe(false);
    }
    // What the store matches, filters and orders on stays readable, error names included.
    for (const plain of ['refund-1', 'doomed-compensation', 'refund.approved', 'nobody-waits', 'nightly-refunds', 'ChildWorkflowFailedError', 'WorkflowTerminatedError']) {
      expect(stored).toContain(plain);
    }
    expect(stored).toContain('$wf1:aes-256-gcm:k1.');
  });
});

describe('a key rotation', () => {
  it('decrypts with the old key what it wrote, writes with the new one, and needs the old one no more once it is purged', async () => {
    const workflows = [RefundWorkflow, LabelWorkflow];
    const first = await start(aes({ k1: K1 }, 'k1'), workflows);
    await first.client.start(RefundWorkflow, { orderId: 'o-2', card: S.input }, { id: 'refund-2' });
    await first.worker.drain();
    clock.advance('1s');
    await first.worker.drain();
    await stop(first);

    const second = await start(aes({ k1: K1, k2: K2 }, 'k2'), workflows);
    await second.client.signal(approved, { by: 'agent-7', note: S.signal }, { key: 'o-2' });
    await second.client.signal(shipped, { note: S.any }, { key: 'o-2' });
    await second.worker.drain();
    expect(await second.client.result('refund-2')).toMatchObject({ receipt: S.step, approvedBy: 'agent-7', note: S.output });

    // Below the codec: what the database holds, each payload under the key that was current when it was written.
    const stored = (await second.store.get('refund-2', { journal: true }))!;
    const entry = (name: string) => stored.journal!.find((candidate) => candidate.name === name)!;
    expect(stored.input).toMatch(envelope('k1'));
    expect(entry('charge').result).toMatch(envelope('k1'));
    expect(stored.output).toMatch(envelope('k2'));
    expect(entry('approval').result).toMatch(envelope('k2'));
    expect(entry('dispatch').result).toMatch(envelope('k2'));
    expect(stored.customStatus).toMatch(envelope('k1'));
    await stop(second);

    // k1 retired: what it encrypted can't be read, until it is purged; then nothing needs it.
    clock.advance('31d');
    const third = await start(aes({ k2: K2 }, 'k2'), workflows);
    await expect(third.client.getStatus('refund-2')).rejects.toThrow('was encrypted with key "k1", which isn\'t in keys');
    expect(await third.client.purge({ olderThan: '30d' })).toMatchObject({ instances: 2 });
    const after = await dump();
    expect(after).not.toContain('aes-256-gcm:k1.');
    expect(after).toContain('aes-256-gcm:k2.');
  });
});

describe('a key rotation rolled back', () => {
  it("leaves a child whose payloads it can't read operable: listed, closed by its parent, terminated, and run once the key is back", async () => {
    vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
    const error = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    const workflows = [ReturnWorkflow, PickupWorkflow];
    const old = await start(aes({ k1: K1 }, 'k1'), workflows);
    await old.client.start(ReturnWorkflow, { orderId: 'o-5' }, { id: 'return-5' });
    await old.worker.drain();
    await stop(old);

    // A canary with a new key moves the child on; the parent doesn't run there.
    const canary = await start(aes({ k1: K1, k2: K2 }, 'k2'), workflows);
    await canary.client.signal(enRoute, { eta: '14:00' }, { key: 'o-5' });
    expect(await canary.worker.drain()).toBe(1);
    await stop(canary);

    const rolledBack = await start(aes({ k1: K1 }, 'k1'), workflows);
    await expect(rolledBack.client.getStatus('return-5/pickup')).rejects.toThrow('was encrypted with key "k2", which isn\'t in keys');
    await expect(rolledBack.client.result('return-5/pickup')).rejects.toThrow('was encrypted with key "k2"');
    expect(await rolledBack.client.getStatus('return-5', { children: true })).toMatchObject({
      status: 'suspended',
      input: { orderId: 'o-5' },
      children: [{ id: 'return-5/pickup', status: 'suspended', input: undefined, customStatus: null }],
    });
    expect(await rolledBack.client.list({ workflow: 'pickup' })).toMatchObject([{ id: 'return-5/pickup', input: undefined, customStatus: null }]);

    // Woken, it is handed back at once, with its waits, instead of holding a lease it can't use.
    await rolledBack.client.signal(arrived, { by: 'courier-3' }, { key: 'o-5' });
    expect(await rolledBack.worker.drain()).toBe(0);
    expect(await rolledBack.store.get('return-5/pickup')).toMatchObject({
      leaseUntil: null,
      wakeAt: clock.now() + 30_000,
      waits: [{ signal: 'courier.arrived', key: 'o-5' }],
    });
    expect(error).toHaveBeenCalledWith(expect.stringContaining('Instance "return-5/pickup" can\'t be read, so it isn\'t run'));

    // Its parent can be read: cancelled, it closes the child, which needs no payload for that.
    await rolledBack.client.cancel('return-5', 'The customer kept the scratching post.');
    await rolledBack.worker.drain();
    expect(await rolledBack.client.getStatus('return-5')).toMatchObject({ status: 'cancelled' });
    const terminated = await rolledBack.client.terminate('return-5/pickup', 'The courier went home.');
    expect(terminated).toMatchObject({ accepted: true, cancelRequested: true, terminateRequested: true });
    expect(terminated.input).toMatch(envelope('k1'));
    await stop(rolledBack);

    // Rolled forward again: the child ends as its last request says, without its compensation.
    const fixed = await start(aes({ k1: K1, k2: K2 }, 'k2'), workflows);
    clock.advance('31s');
    await fixed.worker.drain();
    expect(await fixed.client.getStatus('return-5/pickup')).toMatchObject({
      status: 'cancelled',
      customStatus: { stage: 'en route', eta: '14:00' },
      error: { name: 'WorkflowTerminatedError', message: 'The courier went home.' },
    });
    expect(world.calls).toEqual([]);
  });

  it("doesn't let an instance whose journal it can't read hold a concurrency slot, or go first at every claim", async () => {
    const error = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    const old = await start(aes({ k1: K1 }, 'k1'), [LedgerSyncWorkflow]);
    await old.client.start(LedgerSyncWorkflow, { account: 'a' }, { id: 'sync-a' });
    await stop(old);

    // The canary runs it up to its sleep: its row stays under k1, its journal is under k2.
    const canary = await start(aes({ k1: K1, k2: K2 }, 'k2'), [LedgerSyncWorkflow]);
    await canary.worker.drain();
    await stop(canary);

    const rolledBack = await start(aes({ k1: K1 }, 'k1'), [LedgerSyncWorkflow]);
    clock.advance('1m');
    await rolledBack.client.start(LedgerSyncWorkflow, { account: 'b' }, { id: 'sync-b' });
    for (let i = 0; i < 4; i++) {
      await rolledBack.worker.drain();
      clock.advance('31s');
    }

    expect(await rolledBack.client.getStatus('sync-b')).toMatchObject({ status: 'completed' });
    expect(world.calls.map((call) => call.key)).toEqual(['b']);
    // Handed back each time, it holds no lease between its claims.
    expect(await rolledBack.store.get('sync-a')).toMatchObject({ status: 'running', leaseUntil: null });
    expect(error).toHaveBeenCalledWith(expect.stringContaining('"sync-a"'), expect.anything());
    await stop(rolledBack);

    const fixed = await start(aes({ k1: K1, k2: K2 }, 'k2'), [LedgerSyncWorkflow]);
    await fixed.worker.drain();
    expect(await fixed.client.getStatus('sync-a')).toMatchObject({ status: 'completed' });
    expect(world.calls.map((call) => call.key)).toEqual(['b', 'a']);
  });
});

describe('journal limits, with a codec', () => {
  it('measure the journal the workflow wrote, not its encrypted form', async () => {
    const node = await boot({ db, clock, codec: aes({ k1: K1 }, 'k1'), journal: { maxBytes: 2_700 }, workflows: [BulkyExportWorkflow] });
    nodes.push(node);
    await node.client.start(BulkyExportWorkflow, undefined, { id: 'export-1' });
    await node.worker.drain();
    clock.advance('1s');
    await node.worker.drain();

    expect(await node.client.getStatus('export-1')).toMatchObject({ status: 'completed' });
    // Encrypted, the entries before the last step were past the limit already.
    const stored = (await node.store.get('export-1', { journal: true }))!.journal!;
    const bytes = (entries: unknown[]) => entries.reduce<number>((sum, entry) => sum + Buffer.byteLength(JSON.stringify(entry)), 0);
    expect(bytes(stored.slice(0, 3))).toBeGreaterThan(2_700);
    expect(bytes((await node.client.getStatus('export-1', { journal: true }))!.journal.slice(0, 3))).toBeLessThan(2_700);
  });
});

describe.runIf(storeKind !== 'memory')("the application's transaction, with a codec", () => {
  it('commits an encoded start and signal with the rows written beside them, or rolls them back with them', async () => {
    const node = await start(aes({ k1: K1 }, 'k1'), [RefundWorkflow, LabelWorkflow]);
    const connection = connect(db);
    const database = connection.db as Database;
    try {
      await database.execute(sql`DELETE FROM orders`);
      await database.transaction(async (tx) => {
        await tx.insert(orders).values({ id: 'o-6', userId: 'u_42', items: [], total: 2_499, status: 'cancelled' });
        await node.client.start(RefundWorkflow, { orderId: 'o-6', card: S.input }, { id: 'refund-6', transaction: tx });
        await node.client.signal(approved, { by: 'agent-7', note: S.signal }, { key: 'o-6', transaction: tx });
      });
      await expect(
        database.transaction(async (tx) => {
          await tx.insert(orders).values({ id: 'o-7', userId: 'u_42', items: [], total: 799, status: 'cancelled' });
          await node.client.start(RefundWorkflow, { orderId: 'o-7', card: S.input }, { id: 'refund-7', transaction: tx });
          throw new Error('The payment provider is down.');
        }),
      ).rejects.toThrow('The payment provider is down.');

      expect((await database.select({ id: orders.id }).from(orders)).map((order) => order.id)).toEqual(['o-6']);
      expect(await node.client.getStatus('refund-7')).toBeNull();
      expect((await node.store.get('refund-6'))!.input).toMatch(envelope('k1'));
      expect(await node.store.signals({ name: approved.name, key: 'o-6', afterId: 0, upToId: Number.MAX_SAFE_INTEGER })).toMatchObject([
        { payload: expect.stringMatching(envelope('k1')) },
      ]);

      await node.worker.drain();
      clock.advance('1s');
      await node.worker.drain();
      expect(await node.client.getStatus('refund-6', { journal: true })).toMatchObject({
        status: 'suspended',
        journal: [{ name: 'charge', status: 'completed' }, { name: 'approval', result: { payload: { by: 'agent-7', note: S.signal } } }, { name: 'dispatch' }],
      });
    } finally {
      await connection.close();
    }
  });
});

describe('payloads stored before the codec', () => {
  it('replay in a process that encodes, next to what it writes from then on', async () => {
    const workflows = [RefundWorkflow, LabelWorkflow];
    const plain = await start(undefined, workflows);
    await plain.client.start(RefundWorkflow, { orderId: 'o-3', card: S.input }, { id: 'refund-3' });
    await plain.worker.drain();
    clock.advance('1s');
    await plain.worker.drain();
    await plain.client.signal(approved, { by: 'agent-9', note: S.signal }, { key: 'o-3' });
    await stop(plain);

    const encoding = await start(aes({ k1: K1 }, 'k1'), workflows);
    await encoding.client.signal(shipped, { note: S.any }, { key: 'o-3' });
    await encoding.worker.drain();
    expect(await encoding.client.result('refund-3')).toMatchObject({ receipt: S.step, approvedBy: 'agent-9', dispatch: S.any, note: S.output });

    const stored = (await encoding.store.get('refund-3', { journal: true }))!;
    const entry = (name: string) => stored.journal!.find((candidate) => candidate.name === name)!;
    expect(stored.input).toEqual({ orderId: 'o-3', card: S.input });
    expect(entry('charge').result).toMatchObject({ receipt: S.step });
    // The approval was sent before the codec, and taken after it: its entry is written by the encoding process.
    expect(entry('approval').result).toMatch(envelope('k1'));
    expect(entry('dispatch').result).toMatch(envelope('k1'));
    expect(stored.output).toMatch(envelope('k1'));
    const legacy = await dump();
    expect(legacy).toContain(S.input);
    expect(legacy).not.toContain(S.output);
  });
});

describe('strings that start like the envelope', () => {
  it('stay the strings they are in every place, stored without a codec and read with one', async () => {
    const plain = await start(undefined, [LookalikeWorkflow]);
    await plain.client.start(LookalikeWorkflow, '$wf1:plain:"input"', { id: 'lookalike-1' });
    await plain.client.start(LookalikeWorkflow, '$wf1:y:input', { id: 'lookalike-2' });
    await plain.client.start(LookalikeWorkflow, '$wf1:q:input', { id: 'lookalike-3' });
    await plain.worker.drain();
    await plain.client.signal(message, '$wf1:aes-256-gcm:k1.forged', { key: 'lookalike-1' });
    await plain.client.signal(message, '$wf1:fail', { key: 'lookalike-2' });
    await plain.client.cancel('lookalike-3', '$wf1:rot13:reason');
    await plain.client.schedules.upsert('lookalikes', { workflow: LookalikeWorkflow, every: '1h', input: '$wf1:s:schedule' });

    // Stored as the engine's own envelope of their JSON, not as a codec's payload.
    expect((await plain.store.get('lookalike-1'))!.input).toBe(`$wf1:plain:${JSON.stringify('$wf1:plain:"input"')}`);
    expect((await plain.store.getSchedule('lookalikes'))!.input).toBe(`$wf1:plain:${JSON.stringify('$wf1:s:schedule')}`);
    await stop(plain);

    const encoding = await start(aes({ k1: K1 }, 'k1'), [LookalikeWorkflow]);
    await encoding.worker.drain();
    clock.advance('1h');
    await encoding.worker.drain();

    expect(await encoding.client.getStatus('lookalike-1')).toMatchObject({
      status: 'completed',
      customStatus: '$wf1:x:status',
      output: ['$wf1:plain:"input"', '$wf1:rot13:step', '$wf1:aes-256-gcm:k1.forged'],
    });
    expect(await encoding.client.getStatus('lookalike-2')).toMatchObject({ status: 'failed', error: { message: '$wf1:z:error' } });
    expect(await encoding.client.getStatus('lookalike-3')).toMatchObject({ status: 'cancelled', cancelReason: '$wf1:rot13:reason', error: { message: '$wf1:rot13:reason' } });
    expect(await encoding.client.schedules.get('lookalikes')).toMatchObject({ input: '$wf1:s:schedule' });
    expect(await encoding.client.list({ scheduleId: 'lookalikes' })).toMatchObject([{ input: '$wf1:s:schedule', customStatus: '$wf1:x:status' }]);
  });
});
