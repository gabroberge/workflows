/**
 * Deploying new workflow code while instances are in flight, as the README's "Versioning"
 * section plans a rolling deploy: pods with the old code, pods with both versions, pods with
 * only the new one, and a change shipped without a new version.
 */
import { Body, Controller, Get, HttpCode, Inject, NotFoundException, Param, Post, Query, type Type } from '@nestjs/common';
import { adapters } from './support/adapters.js';
import { ManualWorkflowClock, Workflow, WorkflowClient, WorkflowSignal, type WorkflowContext, type WorkflowStatus } from '../lib/index.js';
import { bootHttp, type HttpNode } from './http-app.js';
import { tempDb, type TestDb, World } from './support.js';

const emailVerified = new WorkflowSignal<{ userId: string }>('email.verified');
const unfinished: WorkflowStatus[] = ['pending', 'running', 'suspended', 'compensating'];

@Workflow('onboarding')
class OnboardingV1 {
  constructor(@Inject(World) private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { userId: string }) {
    await ctx.step('create-account', ({ idempotencyKey }) => this.world.record('create-account', idempotencyKey), {
      compensate: (_result, { idempotencyKey }) => this.world.record('delete-account', idempotencyKey),
    });
    await ctx.waitForSignal('verify-email', emailVerified, { key: input.userId });
    await ctx.step('welcome', ({ idempotencyKey }) => this.world.record('welcome:v1', idempotencyKey));
    return 'v1';
  }
}

/** Version 2 provisions a trial before the verification, and sends another welcome. */
@Workflow('onboarding', { version: 2 })
class OnboardingV2 {
  constructor(@Inject(World) private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { userId: string }) {
    await ctx.step('create-account', ({ idempotencyKey }) => this.world.record('create-account', idempotencyKey));
    await ctx.step('provision-trial', ({ idempotencyKey }) => this.world.record('provision-trial', idempotencyKey));
    await ctx.waitForSignal('verify-email', emailVerified, { key: input.userId });
    await ctx.step('welcome', ({ idempotencyKey }) => this.world.record('welcome:v2', idempotencyKey));
    return 'v2';
  }
}

/** Version 1 with its first step renamed, deployed without a version bump. */
@Workflow('onboarding')
class OnboardingV1Renamed {
  constructor(@Inject(World) private readonly world: World) {}

  async run(ctx: WorkflowContext, input: { userId: string }) {
    await ctx.step('create-user', ({ idempotencyKey }) => this.world.record('create-user', idempotencyKey), {
      compensate: (_result, { idempotencyKey }) => this.world.record('delete-user', idempotencyKey),
    });
    await ctx.waitForSignal('verify-email', emailVerified, { key: input.userId });
    await ctx.step('welcome', ({ idempotencyKey }) => this.world.record('welcome:v1', idempotencyKey));
    return 'v1';
  }
}

@Controller()
class SignupsController {
  constructor(private readonly workflowClient: WorkflowClient) {}

  @Post('signups/:userId')
  start(@Param('userId') userId: string, @Query('version') version?: string) {
    return this.workflowClient.start(OnboardingV1, { userId }, { id: `onboarding-${userId}`, version: version ? Number(version) : undefined });
  }

  @Get('signups/:userId')
  async status(@Param('userId') userId: string) {
    const instance = await this.workflowClient.getStatus(`onboarding-${userId}`);
    if (!instance) {
      throw new NotFoundException();
    }

    return { version: instance.version, status: instance.status, output: instance.output ?? null, error: instance.error ?? null };
  }

  @Post('webhooks/email-verified')
  @HttpCode(200)
  verified(@Body() body: { userId: string }) {
    return this.workflowClient.signal(emailVerified, body, { key: body.userId });
  }
}

describe.each(adapters)('a deploy with instances in flight ($name)', ({ name: adapter }) => {
  let db: TestDb;
  let clock: ManualWorkflowClock;
  let world: World;
  const pods: HttpNode[] = [];

  beforeEach(async () => {
    db = await tempDb();
    clock = new ManualWorkflowClock();
    world = new World();
  });

  afterEach(async () => {
    for (const pod of pods.splice(0)) {
      await pod.close();
    }
    db.cleanup();
  });

  const deploy = async (workflows: Type<unknown>[]) => {
    const pod = await bootHttp(adapter, {
      db,
      clock,
      workflows,
      providers: [{ provide: World, useValue: world }],
      controllers: [SignupsController],
    });
    pods.push(pod);
    return pod;
  };

  const retire = async (pod: HttpNode) => {
    pods.splice(pods.indexOf(pod), 1);
    await pod.close();
  };

  const oldInstancesLeft = (pod: HttpNode) =>
    pod.client.list({ workflow: 'onboarding', version: 1, status: unfinished }).then((instances) => instances.map((instance) => instance.id));

  it('keeps running instances on version 1 and starts new ones on version 2 until version 1 has drained', async () => {
    const old = await deploy([OnboardingV1]);
    await old.http('POST', '/signups/u1');
    await old.http('POST', '/signups/u2');
    await old.worker.drain();
    await retire(old);

    const next = await deploy([OnboardingV1, OnboardingV2]);
    expect((await next.http('POST', '/signups/u3')).body).toMatchObject({ id: 'onboarding-u3', version: 2, created: true });
    expect((await next.http('POST', '/signups/u4?version=1')).body).toMatchObject({ version: 1, created: true }); // pinned
    expect((await next.http('POST', '/signups/u1')).body).toMatchObject({ version: 1, created: false, status: 'suspended' });

    await next.http('POST', '/webhooks/email-verified', { userId: 'u1' });
    await next.http('POST', '/webhooks/email-verified', { userId: 'u3' });
    await next.worker.drain();

    expect((await next.http('GET', '/signups/u1')).body).toEqual({ version: 1, status: 'completed', output: 'v1', error: null });
    expect((await next.http('GET', '/signups/u3')).body).toEqual({ version: 2, status: 'completed', output: 'v2', error: null });
    expect(world.calls.filter((call) => call.key.startsWith('onboarding-u1:')).map((call) => call.op)).toEqual(['create-account', 'welcome:v1']);
    expect(world.calls.filter((call) => call.key.startsWith('onboarding-u3:')).map((call) => call.op)).toEqual([
      'create-account',
      'provision-trial',
      'welcome:v2',
    ]);

    // Filtered in the query: what still needs a pod with version 1's code.
    expect(await oldInstancesLeft(next)).toEqual(['onboarding-u2', 'onboarding-u4']);
    await next.http('POST', '/webhooks/email-verified', { userId: 'u2' });
    await next.http('POST', '/webhooks/email-verified', { userId: 'u4' });
    await next.worker.drain();
    expect(await oldInstancesLeft(next)).toEqual([]);
  });

  it('leaves version 1 instances to the pods that still have its code', async () => {
    const old = await deploy([OnboardingV1]);
    await old.http('POST', '/signups/u1');
    await old.worker.drain();

    const next = await deploy([OnboardingV2]);
    await next.http('POST', '/webhooks/email-verified', { userId: 'u1' });
    expect(await next.worker.drain()).toBe(0); // due, but not a version it can run
    expect((await next.http('GET', '/signups/u1')).body).toMatchObject({ version: 1, status: 'suspended' });

    expect(await old.worker.drain()).toBe(1);
    expect((await next.http('GET', '/signups/u1')).body).toMatchObject({ version: 1, status: 'completed', output: 'v1' });
    expect(world.count('welcome:v2')).toBe(0);
  });

  it('fails an instance whose code changed without a new version, before running the renamed step, and undoes nothing', async () => {
    const old = await deploy([OnboardingV1]);
    await old.http('POST', '/signups/u1');
    await old.worker.drain();
    await retire(old);

    const next = await deploy([OnboardingV1Renamed]);
    await next.http('POST', '/webhooks/email-verified', { userId: 'u1' });
    await next.worker.drain();

    const { body } = await next.http('GET', '/signups/u1');
    expect(body).toMatchObject({ version: 1, status: 'failed', error: { name: 'WorkflowNonDeterminismError' } });
    expect(body.error.message).toContain('"create-account"');
    expect(world.ops()).toEqual(['create-account']); // no create-user, no delete-account, no welcome
    expect(next.events.map((event) => event.type)).toEqual(['workflow-resumed', 'workflow-failed']);
  });
});
