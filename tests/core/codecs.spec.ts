/**
 * `@nestjs/workflows/core`'s payloads: `PayloadCodecs` (the family's envelope: codec ids, payloads stored before a codec,
 * look-alike strings, errors) and `AesGcmPayloadCodec` under contexts that aren't workflows'. codec.spec.ts covers
 * both through the workflow engine.
 */
import { createDecipheriv, hkdfSync, randomBytes } from 'node:crypto';
import { AesGcmPayloadCodec, isEncodedPayload, PayloadCodecs, type PayloadCodec, type PayloadContext } from '../../lib/core/index.js';
import type { WorkflowPayloadCodec, WorkflowPayloadContext } from '../../lib/index.js';

const job: PayloadContext = { field: 'data', queue: 'emails', job: 'order-confirmation-42' };

/** Encodes as reversed JSON, and records what it was given. */
class ReversingCodec implements PayloadCodec {
  readonly contexts: PayloadContext[] = [];

  constructor(readonly id = 'reversed') {}

  encode(value: unknown, context: PayloadContext): string {
    this.contexts.push(context);
    return [...JSON.stringify(value)].reverse().join('');
  }

  decode(data: string): unknown {
    return JSON.parse([...data].reverse().join(''));
  }
}

describe('PayloadCodecs', () => {
  it('stores what the first codec encodes in the envelope, with its id, and decodes it with the codec it names', async () => {
    const codecs = new PayloadCodecs([new ReversingCodec()]);
    const stored = codecs.encode({ card: '4242' }, job);
    expect(stored).toBe('$wf1:reversed:}"2424":"drac"{');
    expect(isEncodedPayload(stored)).toBe(true);
    expect(await codecs.decode(stored, job)).toEqual({ card: '4242' });
    expect(codecs.encodes).toBe(true);
  });

  it('keeps null and undefined as they are, and reads a payload stored before any codec as it is', async () => {
    const codecs = new PayloadCodecs([new ReversingCodec()]);
    expect(codecs.encode(null, job)).toBeNull();
    expect(codecs.encode(undefined, job)).toBeUndefined();
    expect(await codecs.decode({ card: '4242' }, job)).toEqual({ card: '4242' });
    expect(await codecs.decode('a plain string', job)).toBe('a plain string');
  });

  it('encodes with the first codec and decodes with each listed one, so a replaced codec keeps reading what it wrote', async () => {
    const old = new ReversingCodec('old');
    const stored = new PayloadCodecs([old]).encode('written before', job);

    const rotated = new PayloadCodecs([new AesGcmPayloadCodec({ keys: { k1: randomBytes(32) }, current: 'k1' }), old]);
    expect(await rotated.decode(stored, job)).toBe('written before');
    expect(rotated.encode('written after', job)).toMatch(/^\$wf1:aes-256-gcm:k1\./);

    await expect(async () => new PayloadCodecs([new ReversingCodec('new')], { name: "QueuesModule's codec" }).decode(stored, job)).rejects.toThrow(
      'A payload of job "order-confirmation-42", queue "emails" (data) was encoded by the codec "old", which QueuesModule\'s codec option doesn\'t list.',
    );
  });

  it("without codecs, stores values as they are, wraps a string that looks like an envelope, and can't read an encoded one", async () => {
    const none = new PayloadCodecs([]);
    expect(none.encodes).toBe(false);
    expect(none.encode({ a: 1 }, job)).toEqual({ a: 1 });
    expect(none.encode('$wf1 is how it starts', job)).toBe('$wf1 is how it starts');

    const lookAlike = none.encode('$wf1:aes-256-gcm:k1.abc', job);
    expect(lookAlike).toBe('$wf1:plain:"$wf1:aes-256-gcm:k1.abc"');
    expect(await none.decode(lookAlike, job)).toBe('$wf1:aes-256-gcm:k1.abc');
    expect(() => none.decode('$wf1:aes-256-gcm:k1.abc', job)).toThrow('was encoded by the codec "aes-256-gcm", which the module\'s codec option doesn\'t list.');
  });

  it('stays synchronous with a synchronous codec, and returns a promise with an asynchronous one', async () => {
    const asynchronous: PayloadCodec = { id: 'kms', encode: async (value) => JSON.stringify(value), decode: async (data) => JSON.parse(data) };
    expect(typeof new PayloadCodecs([new ReversingCodec()]).encode(1, job)).toBe('string');

    const codecs = new PayloadCodecs([asynchronous]);
    const stored = codecs.encode({ a: 1 }, job);
    expect(stored).toBeInstanceOf(Promise);
    expect(await stored).toBe('$wf1:kms:{"a":1}');
    expect(await codecs.decode(await stored, job)).toEqual({ a: 1 });
  });

  it("encodes an error's message and stack together, and nested errors likewise, keeping names and dropping other fields", async () => {
    const codecs = new PayloadCodecs([new ReversingCodec()]);
    const error = {
      name: 'CompensationFailed',
      message: 'refund of card 4242 failed',
      stack: 'at refund()',
      compensation: { name: 'HttpError', message: 'card 4242: 502' },
      cause: { name: 'SocketError', message: 'reset', stack: 'at socket' },
      detail: 'card 4242',
    };

    const stored = (await codecs.encodeError(error, job))!;
    expect(stored.name).toBe('CompensationFailed');
    expect(isEncodedPayload(stored.message)).toBe(true);
    expect(JSON.stringify(stored)).not.toContain('4242');
    expect(Object.keys(stored)).toEqual(['name', 'message', 'compensation', 'cause']);
    expect(stored.compensation).toMatchObject({ name: 'HttpError' });

    const { detail: _detail, ...kept } = error;
    expect(await codecs.decodeError(stored, job)).toEqual(kept);
    expect(await codecs.encodeError(null, job)).toBeNull();
  });

  it('leaves errors as they are without a codec, unless a message looks like an envelope', async () => {
    const none = new PayloadCodecs([]);
    const plain = { name: 'Error', message: 'declined', stack: 'at charge()' };
    expect(none.encodeError(plain, job)).toBe(plain);
    expect(await none.decodeError(plain, job)).toBe(plain);

    const lookAlike = { name: 'Error', message: 'x', compensation: { name: 'Error', message: '$wf1:plain:"y"' } };
    const stored = (await none.encodeError(lookAlike, job))!;
    expect(stored.message).toBe('$wf1:plain:{"message":"x"}');
    expect(await none.decodeError(stored, job)).toEqual(lookAlike);
  });

  it('refuses codecs it can not use, naming the option they come from', () => {
    const options = { name: "QueuesModule's codec", type: 'QueuePayloadCodec' };
    expect(() => new PayloadCodecs([{} as PayloadCodec], options)).toThrow(
      new TypeError("QueuesModule's codec: expected a QueuePayloadCodec (an object with id, encode() and decode()), got [object Object]."),
    );
    expect(() => new PayloadCodecs([new ReversingCodec('has space')], options)).toThrow(`QueuesModule's codec: ReversingCodec has an invalid id "has space".`);
    expect(() => new PayloadCodecs([new ReversingCodec('plain')], options)).toThrow('("plain" is the envelope\'s)');
    expect(() => new PayloadCodecs([new ReversingCodec(), new ReversingCodec()], options)).toThrow(
      new TypeError(`QueuesModule's codec: two codecs have the id "reversed". Give each its own.`),
    );
  });
});

describe('AesGcmPayloadCodec under any context', () => {
  const key = randomBytes(32);
  const codec = new AesGcmPayloadCodec({ keys: { k1: key }, current: 'k1' });

  it("binds a payload to every key of its context, and refuses it elsewhere, naming the context's keys", () => {
    const encoded = codec.encode({ to: 'ada@example.com' }, job);
    expect(codec.decode(encoded, job)).toEqual({ to: 'ada@example.com' });

    const failure = 'failed authentication: it was changed, or encrypted for another place.';
    expect(() => codec.decode(encoded, { ...job, job: 'order-confirmation-43' })).toThrow(`a payload of job "order-confirmation-43", queue "emails" (data) ${failure}`);
    expect(() => codec.decode(encoded, { ...job, queue: 'sms' })).toThrow(failure);
    expect(() => codec.decode(encoded, { ...job, attempt: '2' })).toThrow(failure);
    expect(() => codec.decode(encoded, { field: 'data', job: 'order-confirmation-42' })).toThrow(failure);
    expect(() => codec.decode(encoded, { field: 'result', queue: 'emails', job: 'order-confirmation-42' })).toThrow(failure);
    expect(codec.decode(encoded, { job: 'order-confirmation-42', queue: 'emails', field: 'data', unused: undefined })).toEqual({ to: 'ada@example.com' });
  });

  it("authenticates workflows' contexts exactly as before core existed, so what it encrypted then still decrypts", () => {
    const places: WorkflowPayloadContext[] = [
      { field: 'result', instanceId: 'order-1', entry: 'charge' },
      { field: 'input', instanceId: 'order-1' },
      { field: 'payload', signal: 'payment.captured' },
      { field: 'input', schedule: 'weekly-digest' },
    ];
    for (const context of places) {
      const encoded = codec.encode({ amount: 2499 }, context);
      // The additional authenticated data this codec used when workflows had it to itself.
      const aad = JSON.stringify(['k1', context.field, context.instanceId ?? null, context.entry ?? null, context.signal ?? null, context.schedule ?? null]);
      const bytes = Buffer.from(encoded.slice('k1.'.length), 'base64url');
      const derived = Buffer.from(hkdfSync('sha256', key, bytes.subarray(0, 16), 'nestjs-workflows payload', 32));
      const decipher = createDecipheriv('aes-256-gcm', derived, bytes.subarray(16, 28));
      decipher.setAAD(Buffer.from(aad, 'utf8'));
      decipher.setAuthTag(bytes.subarray(bytes.length - 16));
      const plaintext = Buffer.concat([decipher.update(bytes.subarray(28, bytes.length - 16)), decipher.final()]);
      expect(JSON.parse(plaintext.subarray(1).toString('utf8'))).toEqual({ amount: 2499 });
    }
  });

  it('is a WorkflowPayloadCodec', () => {
    const workflows: WorkflowPayloadCodec = codec;
    expect(workflows.id).toBe('aes-256-gcm');
  });
});
