/**
 * A workflow step that sends mail with `@nestjs/mail`, as its docs page
 * (https://docs.nestjs.com/application/mail) says a caller that retries does: the step's idempotency key as the mail's, one attempt per call
 * (`retry: false`), and the step retrying only what `MailError#permanent` says can succeed.
 */
import { Body, Controller, Inject, Injectable, Post } from '@nestjs/common';
import { adapters } from './support/adapters.js';
import { InMemoryMailTransport, MailError, MailModule, Mailer } from '@nestjs/mail';
import { ManualWorkflowClock, Workflow, WorkflowClient, type WorkflowContext, type WorkflowStepFailedEvent } from '../lib/index.js';
import { bootHttp, type HttpNode } from './http-app.js';
import { deferred, forever, tempDb, type TestDb, World } from './support.js';

/** A process that dies right after the mail went out, before the step's result is journaled. */
@Injectable()
class Crash {
  armed = false;
  reached = deferred();

  async afterSend() {
    if (!this.armed) {
      return;
    }

    this.armed = false;
    this.reached.resolve();
    await forever();
  }
}

@Workflow('welcome')
class Welcome {
  constructor(
    @Inject(World) private readonly world: World,
    @Inject(Crash) private readonly crash: Crash,
    private readonly mailer: Mailer,
  ) {}

  async run(ctx: WorkflowContext, input: { email: string }) {
    await ctx.step('create-account', ({ idempotencyKey }) => this.world.record('create-account', idempotencyKey), {
      compensate: (_account, { idempotencyKey }) => this.world.record('delete-account', idempotencyKey),
    });

    return ctx.step(
      'welcome-mail',
      async ({ idempotencyKey, signal }) => {
        const sent = await this.mailer.send({ to: input.email, subject: 'Welcome to the cat store', text: 'Hi!', idempotencyKey, signal, retry: false });
        await this.crash.afterSend();
        return { messageId: sent.messageId };
      },
      { retry: { attempts: 3, backoff: { delay: '1m' }, retryIf: (error) => !(error instanceof MailError && error.permanent) } },
    );
  }
}

@Controller('signups')
class SignupsController {
  constructor(private readonly workflowClient: WorkflowClient) {}

  @Post()
  signUp(@Body() body: { email: string }) {
    return this.workflowClient.start(Welcome, body, { id: `welcome-${body.email}` });
  }
}

describe.each(adapters)('a workflow that sends mail ($name)', ({ name: adapter }) => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let world: World;
  let crash: Crash;
  let mailbox: InMemoryMailTransport;
  const pods: HttpNode[] = [];

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    world = new World();
    crash = new Crash();
    mailbox = new InMemoryMailTransport(); // the mail provider, shared by every pod
  });

  afterEach(async () => {
    for (const pod of pods.splice(0)) {
      await pod.close();
    }
    db.cleanup();
  });

  const boot = async () => {
    const pod = await bootHttp(adapter, {
      db,
      clock,
      imports: [MailModule.forRoot({ transport: mailbox, from: 'The cat store <hello@example.com>' })],
      workflows: [Welcome],
      providers: [
        { provide: World, useValue: world },
        { provide: Crash, useValue: crash },
      ],
      controllers: [SignupsController],
    });
    pods.push(pod);
    return pod;
  };

  const stepFailures = (pod: HttpNode) => pod.events.filter((event): event is WorkflowStepFailedEvent => event.type === 'step-failed');

  it('retries a transient mail failure with the durable backoff, and sends one mail', async () => {
    const pod = await boot();
    mailbox.failNext(new MailError('421 Try again later', { permanent: false, code: 421 }));

    await pod.http('POST', '/signups', { email: 'ada@example.com' });
    await pod.worker.drain();
    expect(await pod.client.getStatus('welcome-ada@example.com')).toMatchObject({ status: 'suspended', wakeAt: clock.now() + 60_000 });
    expect(stepFailures(pod)).toMatchObject([{ step: 'welcome-mail', attempt: 1, retryAt: clock.now() + 60_000 }]);
    expect(mailbox.mails).toEqual([]);

    clock.advance('1m');
    await pod.worker.drain();
    const mail = mailbox.assertSent({ to: 'ada@example.com', subject: 'Welcome to the cat store' });
    expect(await pod.client.getStatus('welcome-ada@example.com')).toMatchObject({
      status: 'completed',
      output: { messageId: mail.messageId },
    });
  });

  it('gives up on a permanent mail failure at once and undoes the account', async () => {
    const pod = await boot();
    mailbox.failNext(new MailError('550 No such user', { permanent: true, code: 550 }));

    await pod.http('POST', '/signups', { email: 'nobody@example.com' });
    await pod.worker.drain();

    expect(await pod.client.getStatus('welcome-nobody@example.com')).toMatchObject({
      status: 'failed',
      error: { name: 'StepFailedError', message: expect.stringContaining('550 No such user') },
    });
    expect(stepFailures(pod)).toMatchObject([{ step: 'welcome-mail', attempt: 1, retryAt: null }]);
    expect(world.ops()).toEqual(['create-account', 'delete-account']);
    expect(mailbox.mails).toEqual([]);
  });

  it('re-sends with the same Message-ID after a crash between the send and the journal', async () => {
    crash.armed = true;
    const first = await boot();
    await first.http('POST', '/signups', { email: 'ada@example.com' });
    void first.worker.drain();
    await crash.reached.promise;
    pods.splice(pods.indexOf(first), 1);
    await first.close();

    const second = await boot();
    clock.advance('31s');
    await second.worker.drain();

    // At least once: the provider sees the duplicate under one Message-ID and can drop it.
    expect(mailbox.mails).toHaveLength(2);
    const [original, resent] = mailbox.mails;
    expect(resent!.messageId).toBe(original!.messageId);
    expect(await second.client.getStatus('welcome-ada@example.com')).toMatchObject({
      status: 'completed',
      output: { messageId: original!.messageId },
    });
    expect(world.count('create-account')).toBe(1);
  });
});
