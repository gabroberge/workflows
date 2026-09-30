/**
 * The tiny queue (tiny-queue.ts), built only from `@nestjs/workflows/core` and `@nestjs/workflows/postgres`, on
 * PostgreSQL: the pieces compose into a queue. A job added in the application's transaction is claimed under a lease
 * and run, a schedule adds one per occurrence, a codec encrypts what the tables hold, a waiter gets the result, a
 * failed job is retried with the core's backoff, and a lost lease aborts the job's signal. Its schedule store passes
 * the core's contract.
 */
import { randomBytes } from 'node:crypto';
import { AesGcmPayloadCodec, isEncodedPayload, ManualClock, occurrenceId } from '../../lib/core/index.js';
import { scheduleStoreContract } from '../../lib/testing/index.js';
import { pgClient, testDatabase, type Client } from './support.js';
import { PostgresScheduleStore, TinyQueue, type TinyQueueOptions } from './tiny-queue.js';

const { database, reason } = await testDatabase('tiny_queue');

const T0 = Date.UTC(2026, 0, 1);
const hours = (n: number) => T0 + n * 3_600_000;
const codec = new AesGcmPayloadCodec({ keys: { k1: randomBytes(32) }, current: 'k1' });

interface Email {
  to: string;
}

let client: Client | null = null;
const queues: TinyQueue<any, any>[] = [];
let schemas = 0;

beforeAll(async () => {
  client = database ? await pgClient.open(database.url) : null;
});
afterAll(() => client?.close());
afterEach(async () => {
  for (const queue of queues.splice(0)) {
    await queue.stop();
  }
});
if (reason) {
  beforeEach((context) => context.skip(reason));
}

/** A queue of its own tables (a schema per test), as a process of the application runs one. */
async function queueOf<D, R>(options: Partial<TinyQueueOptions<D, R>> & Pick<TinyQueueOptions<D, R>, 'handler'>, schema = `tq_${++schemas}`) {
  const queue = new TinyQueue<D, R>({ name: 'emails', executor: client!.executor, schema, codecs: [codec], ...options });
  await queue.migrate();
  queues.push(queue);
  return { queue, schema };
}

describe('a tiny queue built from the core and postgres exports', () => {
  it("adds a job in the application's transaction, runs it under a lease, keeps it encrypted, and hands its result to a waiter", async () => {
    const seen: Array<{ owner: string | null; leased: boolean; signal: boolean }> = [];
    const { queue, schema } = await queueOf<Email, { messageId: string }>({
      handler: async (email, { signal }) => {
        const [row] = await client!.executor.query<{ lease_owner: string | null; lease_until: string | null }>(
          `SELECT lease_owner, lease_until::text AS lease_until FROM "${schema}".jobs WHERE data IS NOT NULL LIMIT 1`,
        );
        seen.push({ owner: row!.lease_owner, leased: row!.lease_until !== null, signal: signal.aborted });
        return { messageId: `sent-to-${email.to.length}` };
      },
    });

    // Rolled back with the application's write: no job. Committed: one.
    await expect(
      client!.transaction(async (tx) => {
        await client!.insertOrder(tx, 'order-1');
        await queue.add({ to: 'ada@example.com' }, { id: 'rolled-back', transaction: tx });
        throw new Error('payment declined');
      }),
    ).rejects.toThrow('payment declined');
    const job = await client!.transaction(async (tx) => {
      await client!.insertOrder(tx, 'order-2');
      return queue.add({ to: 'ada@example.com' }, { id: 'confirm-order-2', transaction: tx });
    });
    expect(job).toEqual({ id: 'confirm-order-2', created: true });
    expect(await queue.add({ to: 'ada@example.com' }, { id: 'confirm-order-2' })).toEqual({ id: 'confirm-order-2', created: false });

    queue.worker.start();
    await expect(queue.result('confirm-order-2', { timeout: '10s' })).resolves.toEqual({ messageId: 'sent-to-15' });
    expect(seen).toEqual([{ owner: queue.worker.owner, leased: true, signal: false }]);

    const rows = await client!.executor.query<{ id: string; state: string; data: string; result: string; lease_until: string | null }>(
      `SELECT id, state, data::text AS data, result::text AS result, lease_until::text AS lease_until FROM "${schema}".jobs ORDER BY id`,
    );
    expect(rows.map((row) => [row.id, row.state, row.lease_until])).toEqual([['confirm-order-2', 'completed', null]]);
    expect(isEncodedPayload(JSON.parse(rows[0]!.data))).toBe(true);
    expect(isEncodedPayload(JSON.parse(rows[0]!.result))).toBe(true);
    expect(JSON.stringify(rows)).not.toContain('ada@example.com');
    expect(JSON.stringify(rows)).not.toContain('sent-to');
  });

  it('adds a job per occurrence of a schedule, once however many processes produce them, with its encrypted data', async () => {
    const clock = new ManualClock(T0);
    const handled: string[] = [];
    const handler = async (email: Email) => {
      handled.push(email.to);
      return { messageId: 'digest' };
    };
    const { queue: first, schema } = await queueOf<Email, { messageId: string }>({ clock, handler });
    const { queue: second } = await queueOf<Email, { messageId: string }>({ clock, handler }, schema);
    await first.upsertSchedule('weekly-digest', { every: '1h' }, { to: 'team@example.com' });

    clock.set(hours(1));
    const drained = await Promise.all([first.worker.drain(), second.worker.drain()]);
    expect(drained[0] + drained[1]).toBe(1);
    expect(handled).toEqual(['team@example.com']);
    await expect(second.result(occurrenceId('weekly-digest', hours(1)), { timeout: '5s' })).resolves.toEqual({ messageId: 'digest' });

    const [schedule] = await client!.executor.query<{ payload: string }>(`SELECT payload::text AS payload FROM "${schema}".schedules`);
    expect(isEncodedPayload(JSON.parse(schedule!.payload))).toBe(true);
    expect(first.scheduler.progress((await first.scheduleStore.getSchedule('weekly-digest'))!)).toMatchObject({ runs: 1, nextAt: hours(2) });
  });

  it("retries a failed job with the core's backoff, then fails it, and the waiter rejects", async () => {
    const clock = new ManualClock(T0);
    const attempts: number[] = [];
    const { queue } = await queueOf<Email, never>({
      clock,
      retry: { attempts: 2, backoff: { delay: '1m' } },
      handler: async (_email, { attempt }) => {
        attempts.push(attempt);
        throw new Error('SMTP 421: try again later');
      },
    });
    await queue.add({ to: 'ada@example.com' }, { id: 'flaky' });

    await queue.worker.drain();
    expect(attempts).toEqual([1]);
    clock.advance('59s');
    expect(await queue.worker.drain()).toBe(0);
    clock.advance('1s');
    await queue.worker.drain();
    expect(attempts).toEqual([1, 2]);
    await expect(queue.result('flaky', { timeout: '5s' })).rejects.toThrow('Job "flaky" failed: SMTP 421: try again later');
  });

  it("renews a running job's lease, and aborts its signal once a renewal finds the lease taken", async () => {
    let aborted = false;
    const { queue, schema } = await queueOf<Email, string>({
      leaseDuration: '300ms',
      handler: async (_email, { signal }) => {
        await new Promise((resolve) => setTimeout(resolve, 250));
        // Another worker takes the job over (its lease looked expired to it).
        await client!.executor.query(`UPDATE "${schema}".jobs SET lease_token = 'someone-else'`);
        await new Promise((resolve) => signal.addEventListener('abort', resolve, { once: true }));
        aborted = signal.aborted;
        return 'too late';
      },
    });
    await queue.add({ to: 'ada@example.com' }, { id: 'slow' });

    await queue.worker.drain();
    expect(aborted).toBe(true);
    const [row] = await client!.executor.query<{ state: string; lease_token: string }>(`SELECT state, lease_token FROM "${schema}".jobs`);
    expect(row).toEqual({ state: 'active', lease_token: 'someone-else' });
  });
});

describe("the tiny queue's schedule store", () => {
  let schema: string;
  beforeAll(async () => {
    if (client) {
      schema = (await queueOf({ handler: async () => undefined })).schema;
    }
  });

  const cases = scheduleStoreContract(
    async () => {
      await client!.executor.query(`TRUNCATE "${schema}".schedules`);
      return new PostgresScheduleStore(client!.executor, schema);
    },
    { concurrent: true },
  );
  for (const c of cases) {
    it(c.name, c.run);
  }
});
