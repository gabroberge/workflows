import { createCipheriv, createDecipheriv, hkdfSync, randomBytes } from 'node:crypto';
import { deflateRawSync, inflateRawSync } from 'node:zlib';
import type { WorkflowPayloadCodec, WorkflowPayloadContext } from '../interfaces/workflow-payload-codec.interface.js';

/** What `AesGcmPayloadCodec` takes. */
export interface AesGcmPayloadCodecOptions {
  /**
   * The keys by id (letters, digits, `_` and `-`): 32 random bytes each (AES-256), as a `Buffer` or `Uint8Array`,
   * or as base64 (`openssl rand -base64 32`). Keep a key here while payloads it encrypted may still be read.
   */
  keys: Record<string, string | Uint8Array>;
  /** The id of the key that encrypts. The others only decrypt: to rotate, add a key and make it `current`. */
  current: string;
  /** Deflate a payload of at least this many bytes (as JSON) before encrypting it; `false` never. Default 1024. */
  compress?: number | false;
}

const SALT_BYTES = 16;
const IV_BYTES = 12;
const TAG_BYTES = 16;
const HEADER_BYTES = SALT_BYTES + IV_BYTES;
/** The first byte of every plaintext: its format. Bit 0: deflated. */
const DEFLATED = 1;

/**
 * Encrypts payloads with AES-256-GCM (`node:crypto`), under a key derived for each payload from the configured one
 * (HKDF-SHA256 with a random salt) and a random IV, so no key nears the 2^32 encryptions random IVs allow, and with
 * the payload's context as additional authenticated data, so a payload that was changed, or moved to another
 * instance, journal entry or field, fails to decrypt instead of being read. The key's id is stored with each
 * payload, so rotating keys needs no migration: payloads are decrypted with the key that encrypted them, and
 * written with the current one. Payloads of 1 KiB or more are deflated first. Its `id` is `'aes-256-gcm'`.
 *
 * ```ts
 * WorkflowsModule.forRoot({
 *   codec: new AesGcmPayloadCodec({ keys: { '2026-09': process.env.WORKFLOWS_KEY! }, current: '2026-09' }),
 * })
 * ```
 */
export class AesGcmPayloadCodec implements WorkflowPayloadCodec {
  readonly id = 'aes-256-gcm';
  private readonly keys = new Map<string, Buffer>();
  private readonly current: string;
  private readonly threshold: number;

  constructor(options: AesGcmPayloadCodecOptions) {
    if (options === null || typeof options !== 'object' || options.keys === null || typeof options.keys !== 'object') {
      throw new TypeError('AesGcmPayloadCodec: pass { keys: { [keyId]: key }, current: keyId }.');
    }

    for (const [id, key] of Object.entries(options.keys)) {
      if (!/^[\w-]+$/.test(id)) {
        throw new TypeError(`AesGcmPayloadCodec: invalid key id ${JSON.stringify(id)}. Use letters, digits, "_" and "-".`);
      }
      this.keys.set(id, keyBytes(id, key));
    }
    if (!this.keys.has(options.current)) {
      throw new TypeError(`AesGcmPayloadCodec: current (${JSON.stringify(options.current)}) must be one of the keys' ids (${[...this.keys.keys()].join(', ') || 'none'}).`);
    }
    this.current = options.current;

    const compress = options.compress ?? 1_024;
    if (compress !== false && (!Number.isSafeInteger(compress) || compress < 0)) {
      throw new TypeError(`AesGcmPayloadCodec: compress (${JSON.stringify(compress)}) must be a number of bytes, or false.`);
    }
    this.threshold = compress === false ? Infinity : compress;
  }

  encode(value: unknown, context: WorkflowPayloadContext): string {
    const json = Buffer.from(JSON.stringify(value), 'utf8');
    const deflated = json.length >= this.threshold;
    const plaintext = Buffer.concat([Buffer.of(deflated ? DEFLATED : 0), deflated ? deflateRawSync(json) : json]);

    const header = randomBytes(HEADER_BYTES);
    const cipher = createCipheriv('aes-256-gcm', derived(this.keys.get(this.current)!, header), header.subarray(SALT_BYTES));
    cipher.setAAD(associated(this.current, context));
    const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
    return `${this.current}.${Buffer.concat([header, ciphertext, cipher.getAuthTag()]).toString('base64url')}`;
  }

  decode(data: string, context: WorkflowPayloadContext): unknown {
    const dot = data.indexOf('.');
    const id = data.slice(0, dot);
    const key = this.keys.get(id);
    if (dot < 0 || !key) {
      throw new Error(
        `AesGcmPayloadCodec: a payload of ${describe(context)} was encrypted with key ${JSON.stringify(id)}, which isn't in keys. ` +
          'Keep a key in keys while payloads it encrypted may still be read.',
      );
    }

    const bytes = Buffer.from(data.slice(dot + 1), 'base64url');
    let plaintext: Buffer;
    try {
      const decipher = createDecipheriv('aes-256-gcm', derived(key, bytes), bytes.subarray(SALT_BYTES, HEADER_BYTES));
      decipher.setAAD(associated(id, context));
      decipher.setAuthTag(bytes.subarray(bytes.length - TAG_BYTES));
      plaintext = Buffer.concat([decipher.update(bytes.subarray(HEADER_BYTES, bytes.length - TAG_BYTES)), decipher.final()]);
    } catch {
      throw new Error(`AesGcmPayloadCodec: a payload of ${describe(context)} failed authentication: it was changed, or encrypted for another place.`);
    }

    const body = plaintext.subarray(1);
    return JSON.parse(((plaintext[0]! & DEFLATED) === DEFLATED ? inflateRawSync(body) : body).toString('utf8'));
  }
}

function keyBytes(id: string, key: unknown): Buffer {
  const advice = 'Give it 32 random bytes, as a Buffer, a Uint8Array, or base64 (`openssl rand -base64 32`).';
  let bytes: Buffer;
  if (key instanceof Uint8Array) {
    bytes = Buffer.from(key);
  } else if (typeof key === 'string') {
    // Buffer.from() skips what isn't base64, so a key with a typo would decode to other bytes instead of failing.
    const text = key.trim();
    if (text.length === 0) {
      throw new TypeError(`AesGcmPayloadCodec: key "${id}" is an empty string. ${advice}`);
    }
    if (!/^[A-Za-z0-9+/_-]+={0,2}$/.test(text)) {
      throw new TypeError(`AesGcmPayloadCodec: key "${id}" isn't base64. ${advice}`);
    }
    bytes = Buffer.from(text, 'base64');
  } else {
    const what = key === undefined ? "undefined (an environment variable that isn't set?)" : key === null ? 'null' : typeof key === 'object' ? 'an object' : `a ${typeof key}`;
    throw new TypeError(`AesGcmPayloadCodec: key "${id}" is ${what}. ${advice}`);
  }

  if (bytes.length !== 32) {
    throw new TypeError(
      `AesGcmPayloadCodec: key "${id}" is ${bytes.length} bytes; AES-256 takes 32 (as a Buffer, or base64: \`openssl rand -base64 32\`).`,
    );
  }
  return bytes;
}

/** The payload's own key: HKDF-SHA256 of the configured key, with the salt that starts the payload's bytes. */
function derived(key: Buffer, bytes: Buffer): Buffer {
  return Buffer.from(hkdfSync('sha256', key, bytes.subarray(0, SALT_BYTES), 'nestjs-workflows payload', 32));
}

/** The additional authenticated data: the key's id and where the payload is stored. */
function associated(key: string, context: WorkflowPayloadContext): Buffer {
  return Buffer.from(JSON.stringify([key, context.field, context.instanceId ?? null, context.entry ?? null, context.signal ?? null, context.schedule ?? null]), 'utf8');
}

function describe(context: WorkflowPayloadContext): string {
  const where = context.entry !== undefined
    ? `entry "${context.entry}" of instance "${context.instanceId}"`
    : context.instanceId !== undefined
      ? `instance "${context.instanceId}"`
      : context.signal !== undefined
        ? `signal "${context.signal}"`
        : `schedule "${context.schedule}"`;
  return `${where} (${context.field})`;
}
