/**
 * Payload codecs: `WorkflowsModule.forRoot({ codec })` encodes what workflows store for the user (inputs, results,
 * checkpoints, signals, statuses, outputs, errors' messages, schedule inputs) before the store sees it.
 * `AesGcmPayloadCodec` encrypts it. Instances stored before a codec was set, or by a codec that was replaced, keep
 * running.
 */
import { randomBytes } from 'node:crypto';
import { Global, Inject, Injectable, Logger, Module } from '@nestjs/common';
import {
  AesGcmPayloadCodec,
  ChildWorkflowFailedError,
  ManualWorkflowClock,
  Workflow,
  WorkflowSignal,
  type WorkflowContext,
  type WorkflowPayloadCodec,
  type WorkflowPayloadContext,
  type WorkflowStore,
} from '../lib/index.js';
import { boot, storeKind, tempDb, World, type Node, type TestDb } from './support.js';

const KEY_1 = randomBytes(32);
const KEY_2 = randomBytes(32).toString('base64');
const SECRET = 'card-4242-4242';
const context: WorkflowPayloadContext = { field: 'result', instanceId: 'order-1', entry: 'charge' };

describe.runIf(storeKind === 'memory')('AesGcmPayloadCodec', () => {
  it('round-trips JSON values, with a fresh IV each time, under the current key', () => {
    const codec = new AesGcmPayloadCodec({ keys: { k1: KEY_1 }, current: 'k1' });
    for (const value of [{ card: SECRET, items: [1, null, { a: 'ü 🚀' }] }, 'text', 0, false, [], { nested: { deep: [true] } }]) {
      const encoded = codec.encode(value, context);
      expect(encoded).toMatch(/^k1\.[\w-]+$/);
      expect(encoded).not.toContain(SECRET);
      expect(codec.decode(encoded, context)).toEqual(value);
    }
    expect(codec.encode('same', context)).not.toBe(codec.encode('same', context));
    expect(codec.id).toBe('aes-256-gcm');
  });

  it('decrypts with any known key and encrypts with the current one, and names a key it lacks', () => {
    const before = new AesGcmPayloadCodec({ keys: { k1: KEY_1 }, current: 'k1' });
    const rotated = new AesGcmPayloadCodec({ keys: { k1: KEY_1, k2: KEY_2 }, current: 'k2' });
    const old = before.encode({ card: SECRET }, context);

    expect(rotated.decode(old, context)).toEqual({ card: SECRET });
    expect(rotated.encode('new', context)).toMatch(/^k2\./);
    expect(() => new AesGcmPayloadCodec({ keys: { k2: KEY_2 }, current: 'k2' }).decode(old, context)).toThrow(
      'AesGcmPayloadCodec: a payload of entry "charge" of instance "order-1" (result) was encrypted with key "k1", which isn\'t in keys.',
    );
  });

  it('refuses a payload that was changed or moved to another place', () => {
    const codec = new AesGcmPayloadCodec({ keys: { k1: KEY_1 }, current: 'k1' });
    const encoded = codec.encode({ amount: 2499 }, context);
    const bytes = Buffer.from(encoded.slice(3), 'base64url');
    bytes[20] ^= 1;

    const failure = 'failed authentication: it was changed, or encrypted for another place.';
    expect(() => codec.decode(`k1.${bytes.toString('base64url')}`, context)).toThrow(failure);
    expect(() => codec.decode(encoded, { ...context, instanceId: 'order-2' })).toThrow(failure);
    expect(() => codec.decode(encoded, { ...context, entry: 'refund' })).toThrow(failure);
    expect(() => codec.decode(encoded, { ...context, field: 'progress' })).toThrow(failure);
  });

  it('deflates large payloads before encrypting them', () => {
    const codec = new AesGcmPayloadCodec({ keys: { k1: KEY_1 }, current: 'k1' });
    const plain = new AesGcmPayloadCodec({ keys: { k1: KEY_1 }, current: 'k1', compress: false });
    const large = { lines: Array.from({ length: 200 }, (_, i) => ({ sku: 'salmon-kibble-2kg', qty: i % 3 })) };

    const deflated = codec.encode(large, context);
    expect(deflated.length).toBeLessThan(plain.encode(large, context).length / 5);
    expect(codec.decode(deflated, context)).toEqual(large);
    expect(plain.decode(deflated, context)).toEqual(large);
  });

  it('validates its options', () => {
    expect(() => new AesGcmPayloadCodec({ keys: { k1: randomBytes(16) }, current: 'k1' })).toThrow('AesGcmPayloadCodec: key "k1" is 16 bytes; AES-256 takes 32');
    expect(() => new AesGcmPayloadCodec({ keys: { k1: KEY_1 }, current: 'k2' })).toThrow('current ("k2") must be one of the keys\' ids (k1).');
    expect(() => new AesGcmPayloadCodec({ keys: { 'k.1': KEY_1 }, current: 'k.1' })).toThrow('invalid key id "k.1"');
    expect(() => new AesGcmPayloadCodec({ keys: { k1: KEY_1 }, current: 'k1', compress: -1 })).toThrow('compress (-1) must be a number of bytes, or false.');
  });
});

const approved = new WorkflowSignal<{ by: string; note: string }>('refund.approved');

@Workflow('refund-review')
class ReviewWorkflow {
  async run(ctx: WorkflowContext, input: { orderId: string }) {
    const approval = await ctx.waitForSignal('approval', approved, { key: input.orderId });
    return { approvedBy: approval!.by };
  }
}

@Workflow('refund')
class RefundWorkflow {
  constructor(private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { orderId: string; card: string; fail?: boolean }) {
    ctx.setStatus({ stage: 'charging', card: input.card });
    const charge = await ctx.step(
      'charge',
      async ({ heartbeat }) => {
        await heartbeat({ checkpoint: `${input.card}-half` });
        this.world.record('charge', input.card);
        return { chargeId: `ch_${input.card}` };
      },
      { compensate: () => this.world.record('undo', input.card) },
    );
    const review = await ctx.executeChild(ReviewWorkflow, { orderId: input.orderId });
    if (input.fail) {
      ctx.fail(`Refund to ${input.card} declined.`);
    }
    return { ...charge, ...review, card: input.card };
  }
}

@Workflow('parent-of-failing')
class ParentOfFailing {
  async run(ctx: WorkflowContext) {
    try {
      await ctx.executeChild(RefundWorkflow, { orderId: 'o-9', card: SECRET, fail: true });
    } catch (error) {
      if (error instanceof ChildWorkflowFailedError) {
        return { failed: error.cause!.message };
      }
      throw error;
    }
  }
}

/** Every value it encodes, as `rot13(JSON)`: readable in a test, and not the plaintext. */
class Rot13Codec implements WorkflowPayloadCodec {
  readonly id = 'rot13';

  encode(value: unknown): string {
    return rot13(JSON.stringify(value));
  }

  decode(data: string): unknown {
    return JSON.parse(rot13(data));
  }
}

const PREFIX = Symbol('PREFIX');

/** A codec with a dependency, which Nest injects when the module is given the class. */
@Injectable()
class PrefixedCodec implements WorkflowPayloadCodec {
  readonly id = 'prefixed';
  constructor(@Inject(PREFIX) private readonly prefix: string) {}

  encode(value: unknown): string {
    return `${this.prefix}${Buffer.from(JSON.stringify(value)).toString('base64')}`;
  }

  decode(data: string): unknown {
    return JSON.parse(Buffer.from(data.slice(this.prefix.length), 'base64').toString());
  }
}

@Global()
@Module({ providers: [{ provide: PREFIX, useValue: 'b64:' }], exports: [PREFIX] })
class PrefixModule {}

function rot13(text: string): string {
  return text.replace(/[a-z]/gi, (c) => String.fromCharCode(((c.toLowerCase().charCodeAt(0) - 97 + 13) % 26) + (c === c.toLowerCase() ? 97 : 65)));
}

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
  vi.restoreAllMocks();
  for (const node of nodes.splice(0)) {
    await node.close();
  }
  db.cleanup();
});

const aes = (keys: Record<string, string | Buffer> = { k1: KEY_1 }, current = 'k1') => new AesGcmPayloadCodec({ keys, current });

async function start(codec?: unknown, workflows: any[] = [RefundWorkflow, ReviewWorkflow, ParentOfFailing]) {
  const node = await boot({ db, clock, workflows, providers: [{ provide: World, useValue: world }], codec: codec as never });
  nodes.push(node);
  return node;
}

/** Everything the store holds for an instance and the signals, as JSON: what a person with the database sees. */
async function raw(store: WorkflowStore, id: string): Promise<string> {
  const details = await store.get(id, { journal: true });
  const signals = await store.signals({ name: approved.name, key: 'o-1', afterId: 0, upToId: Number.MAX_SAFE_INTEGER });
  const ended = await store.signals({ name: '$child-ended', key: `${id}/refund-review#1`, afterId: 0, upToId: Number.MAX_SAFE_INTEGER });
  return JSON.stringify([details, signals, ended]);
}

describe('a codec', () => {
  it('encodes every payload the store keeps, and the engine reads them back', async () => {
    const node = await start(aes());
    await node.client.start(RefundWorkflow, { orderId: 'o-1', card: SECRET }, { id: 'refund-1' });
    await node.worker.drain();
    await node.client.signal(approved, { by: 'agent-7', note: `refund ${SECRET}` }, { key: 'o-1' });
    await node.worker.drain();

    const status = await node.client.getStatus('refund-1', { journal: true });
    expect(status).toMatchObject({ status: 'completed', input: { card: SECRET }, output: { chargeId: `ch_${SECRET}`, approvedBy: 'agent-7' } });
    expect(status!.customStatus).toEqual({ stage: 'charging', card: SECRET });
    expect(status!.journal.find((entry) => entry.name === 'charge')).toMatchObject({ result: { chargeId: `ch_${SECRET}` } });
    expect(await node.client.result('refund-1')).toMatchObject({ card: SECRET });

    const stored = await raw(node.store, 'refund-1');
    expect(stored).not.toContain('card-4242');
    expect(stored).toContain('$wf1:aes-256-gcm:k1.');
    // What the store matches on stays readable.
    expect(stored).toContain('"workflow":"refund"');
    expect(stored).toContain('"name":"charge"');
    expect(stored).toContain('"key":"o-1"');
  });

  it('encodes errors but their names, and a cancel reason', async () => {
    const node = await start(aes());
    await node.client.start(ParentOfFailing, undefined, { id: 'parent' });
    await node.worker.drain();
    await node.client.signal(approved, { by: 'agent-7', note: 'ok' }, { key: 'o-9' });
    await node.worker.drain();

    expect(await node.client.result('parent')).toEqual({ failed: `Refund to ${SECRET} declined.` });
    const child = (await node.client.getStatus('parent', { children: true }))!.children![0]!;
    expect(child).toMatchObject({ status: 'failed', error: { name: 'WorkflowFailedError', message: `Refund to ${SECRET} declined.` } });
    const stored = (await node.store.get(child.id))!;
    expect(stored.error).toEqual({ name: 'WorkflowFailedError', message: expect.stringMatching(/^\$wf1:aes-256-gcm:k1\./) });

    await node.client.start(RefundWorkflow, { orderId: 'o-3', card: 'x' }, { id: 'cancelled' });
    await node.client.cancel('cancelled', `Customer ${SECRET} asked`);
    await node.worker.drain();
    expect(await node.client.getStatus('cancelled')).toMatchObject({ status: 'cancelled', error: { message: `Customer ${SECRET} asked` } });
    expect(JSON.stringify(await node.store.get('cancelled', { journal: true }))).not.toContain('card-4242');
  });

  it("encodes a schedule's input, and the input of the instances it starts", async () => {
    const node = await start(aes());
    await node.client.schedules.upsert('nightly', { workflow: RefundWorkflow, every: '1d', input: { orderId: 'o-5', card: SECRET } });
    expect(await node.client.schedules.get('nightly')).toMatchObject({ input: { card: SECRET } });
    expect(JSON.stringify(await node.store.getSchedule('nightly'))).not.toContain('card-4242');

    clock.advance('1d');
    await node.worker.drain();
    const [started] = await node.client.list({ scheduleId: 'nightly' });
    expect(started).toMatchObject({ status: 'suspended', input: { orderId: 'o-5', card: SECRET } });
    expect(JSON.stringify(await node.store.get(started!.id))).not.toContain('card-4242');
  });

  it('keeps running what was stored before it, and what an earlier codec encoded while it is listed', async () => {
    // No codec: plaintext.
    let node = await start();
    await node.client.start(RefundWorkflow, { orderId: 'o-1', card: SECRET }, { id: 'legacy' });
    await node.worker.drain();
    expect(JSON.stringify(await node.store.get('legacy', { journal: true }))).toContain('card-4242');
    await nodes.pop()!.close();

    // A codec: the legacy instance resumes (its signal is encoded, its old entries plaintext), a new one encodes.
    node = await start(aes());
    await node.client.start(RefundWorkflow, { orderId: 'o-2', card: SECRET }, { id: 'first-key' });
    await node.worker.drain();
    await node.client.signal(approved, { by: 'agent-7', note: 'ok' }, { key: 'o-1' });
    await node.worker.drain();
    expect(await node.client.result('legacy')).toMatchObject({ card: SECRET, approvedBy: 'agent-7' });
    await nodes.pop()!.close();

    // Another codec first, the AES one still listed: both instances' payloads read.
    node = await start([Rot13Codec, aes()]);
    await node.client.signal(approved, { by: 'agent-8', note: 'ok' }, { key: 'o-2' });
    await node.worker.drain();
    expect(await node.client.result('first-key')).toMatchObject({ card: SECRET, approvedBy: 'agent-8' });
    const stored = JSON.stringify(await node.store.get('first-key', { journal: true }));
    expect(stored).toContain('$wf1:aes-256-gcm:');
    expect(stored).toContain('$wf1:rot13:');
    await nodes.pop()!.close();

    // The AES codec gone: its payloads can't be read, and say why.
    node = await start([Rot13Codec]);
    await expect(node.client.getStatus('first-key')).rejects.toThrow(
      'A payload of instance "first-key" (input) was encoded by the codec "aes-256-gcm", which WorkflowsModule\'s codec option doesn\'t list.',
    );
  });

  it("leaves an instance it can't read to its lease, and runs the others", async () => {
    let node = await start(aes());
    await node.client.start(RefundWorkflow, { orderId: 'o-1', card: SECRET }, { id: 'unreadable' });
    await nodes.pop()!.close();

    node = await start();
    const error = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    await node.client.start(RefundWorkflow, { orderId: 'o-2', card: 'plain' }, { id: 'readable' });
    await node.worker.drain();
    expect(await node.store.get('unreadable')).toMatchObject({ status: 'running', runs: 1 });
    expect(await node.store.get('readable')).toMatchObject({ status: 'suspended' });
    expect(error).toHaveBeenCalledWith(expect.stringContaining('Instance "unreadable" can\'t be read, so it isn\'t run; it is claimed again when its lease expires.'));
  });

  it('is created with its dependencies when given as a class, and validated at startup', async () => {
    const node = await boot({ db, clock, workflows: [RefundWorkflow, ReviewWorkflow], providers: [{ provide: World, useValue: world }], imports: [PrefixModule], codec: PrefixedCodec });
    nodes.push(node);
    await node.client.start(RefundWorkflow, { orderId: 'o-1', card: SECRET }, { id: 'prefixed' });
    expect((await node.store.get('prefixed'))!.input).toBe(`$wf1:prefixed:b64:${Buffer.from(JSON.stringify({ orderId: 'o-1', card: SECRET })).toString('base64')}`);
    expect(await node.client.getStatus('prefixed')).toMatchObject({ input: { card: SECRET } });

    await expect(boot({ db, clock, workflows: [], codec: [aes(), aes()] as never })).rejects.toThrow('two codecs have the id "aes-256-gcm"');
    await expect(boot({ db, clock, workflows: [], codec: { id: 'bad id', encode: String, decode: String } as never })).rejects.toThrow('has an invalid id "bad id"');
  });
});
