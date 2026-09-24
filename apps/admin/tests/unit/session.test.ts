import assert from 'node:assert/strict';
import test from 'node:test';
import { SessionModel } from '../../src/lib/session.ts';
import { ManagementFailure } from '@bigbrotr/api-client/management';
import type { ManagementClient, SessionObservation } from '@bigbrotr/api-client/management';

const futureExpiry = String(BigInt(Date.now() + 3_600_000) * 1000n);
const observed: SessionObservation = {
  format: 'bigbrotr-session/1', principal_id: 'a'.repeat(32), credential_revision: '1',
  grant_revision: '2', config_revision: '4', policy_revision: '3', expires_at_us: futureExpiry,
  idle_expires_at_us: futureExpiry, stored_actions: ['data.read'], authority: 'observation-only'
};
const client = (inspect: ManagementClient['inspect']): Pick<ManagementClient, 'inspect' | 'login' | 'logout'> => ({
  inspect, login: async () => observed, logout: async () => {}
});

test('logout clears private data before completion and a late inspect cannot restore it', async () => {
  let finish!: (value: SessionObservation) => void;
  let signal: AbortSignal | undefined;
  let invalidations = 0;
  const model = new SessionModel(client(async input => { signal = input; return new Promise(resolve => { finish = resolve; }); }), () => {}, () => { invalidations++; });
  try {
    const request = model.inspect();
    await model.logout();
    assert.equal(signal?.aborted, true);
    assert.equal(model.view.kind, 'signed-out');
    finish(observed); // Deliberately ignores AbortSignal to exercise generation fencing.
    await request;
    assert.equal(model.view.session, undefined);
    assert.equal(model.view.kind, 'signed-out');
    assert.equal(invalidations, 2);
  } finally { model.dispose(); }
});

test('another tab invalidates locally without echoing secrets or accepting queued login', async () => {
  let finish!: (value: SessionObservation) => void;
  let broadcasts = 0;
  const api = client(async () => observed);
  api.login = async () => new Promise(resolve => { finish = resolve; });
  const model = new SessionModel(api, () => {}, () => { broadcasts++; });
  try {
    const pending = model.login({ username: 'owner', password: 'sample password' });
    model.invalidate('changed');
    finish(observed);
    await pending;
    assert.equal(model.view.kind, 'signed-out');
    assert.equal(model.view.session, undefined);
    assert.equal(model.view.notice, 'changed');
    assert.equal(broadcasts, 2);
  } finally { model.dispose(); }
});

test('unknown logout stays explicitly unconfirmed and never restores the prior session', async () => {
  const api = client(async () => observed);
  api.logout = async () => { throw new ManagementFailure('unavailable'); };
  const model = new SessionModel(api, () => {}, () => {});
  try {
    await model.inspect();
    assert.equal(model.view.kind, 'signed-in');
    await model.logout();
    assert.equal(model.view.kind, 'signed-out');
    assert.equal(model.view.session, undefined);
    assert.equal(model.view.failedOperation, 'logout');
    assert.equal(model.view.failure?.kind, 'unavailable');
  } finally { model.dispose(); }
});

test('deadline clears an idle observation without polling or renewing it', async () => {
  let calls = 0;
  const api = client(async () => { calls++; return { ...observed, idle_expires_at_us: String(BigInt(Date.now() + 40) * 1000n) }; });
  let expired!: () => void;
  const done = new Promise<void>(resolve => { expired = resolve; });
  const model = new SessionModel(api, view => { if (view.notice === 'expired') expired(); }, () => {});
  try {
    await model.inspect();
    assert.equal(model.view.kind, 'signed-in');
    await Promise.race([done, new Promise((_, reject) => setTimeout(() => reject(new Error('display retained expired session')), 1000))]);
    assert.equal(model.view.session, undefined);
    assert.equal(calls, 1);
  } finally { model.dispose(); }
});

test('teardown cancels live work and cannot publish a late result', async () => {
  let finish!: (value: SessionObservation) => void;
  let signal: AbortSignal | undefined;
  let publications = 0;
  const model = new SessionModel(client(async input => { signal = input; return new Promise(resolve => { finish = resolve; }); }), () => { publications++; }, () => {});
  const request = model.inspect();
  model.dispose();
  assert.equal(signal?.aborted, true);
  const before = publications;
  finish(observed);
  await request;
  assert.equal(publications, before);
  assert.equal(model.view.session, undefined);
});

test('an uncertain mutation invalidates another tab that refreshed while it was in flight', async () => {
  for (const operation of ['login', 'logout'] as const) {
    let rejectMutation!: (error: unknown) => void;
    let finishInspect!: (value: SessionObservation) => void;
    let inspectSignal: AbortSignal | undefined;
    const apiA = client(async () => observed);
    apiA[operation] = async () => new Promise<never>((_, reject) => { rejectMutation = reject; });
    const b = new SessionModel(client(async signal => {
      inspectSignal = signal;
      return new Promise(resolve => { finishInspect = resolve; });
    }), () => {}, () => {});
    const a = new SessionModel(apiA, () => {}, () => b.invalidate('changed'));
    try {
      const mutation = operation === 'login' ? a.login({ username: 'owner', password: 'sample password' }) : a.logout();
      const previousIdentity = b.inspect();
      // Headers may have changed shared cookies before the incomplete body or
      // lost response fails. The observation in B was admitted under old cookies.
      rejectMutation(new ManagementFailure('unavailable'));
      await mutation;
      assert.equal(inspectSignal?.aborted, true);
      finishInspect(observed);
      await previousIdentity;
      assert.equal(b.view.session, undefined);
      assert.equal(b.view.notice, 'changed');
    } finally { a.dispose(); b.dispose(); }
  }
});

test('teardown of an uncertain mutation invalidates other tabs before channel closure', async () => {
  let rejectMutation!: (error: unknown) => void;
  let finishInspect!: (value: SessionObservation) => void;
  let inspectSignal: AbortSignal | undefined;
  const apiA = client(async () => observed);
  apiA.login = async () => new Promise<never>((_, reject) => { rejectMutation = reject; });
  const b = new SessionModel(client(async signal => {
    inspectSignal = signal;
    return new Promise(resolve => { finishInspect = resolve; });
  }), () => {}, () => {});
  const a = new SessionModel(apiA, () => {}, () => b.invalidate('changed'));
  const mutation = a.login({ username: 'owner', password: 'sample password' });
  const previousIdentity = b.inspect();
  a.dispose();
  try {
    assert.equal(inspectSignal?.aborted, true);
    rejectMutation(new ManagementFailure('unavailable'));
    finishInspect(observed);
    await Promise.all([mutation, previousIdentity]);
    assert.equal(b.view.session, undefined);
  } finally { b.dispose(); }
});

test('hiding an idle tab clears only that tab and cannot cancel another tab sign-in', async () => {
  let finish!: (value: SessionObservation) => void;
  let loginSignal: AbortSignal | undefined;
  const apiB = client(async () => observed);
  apiB.login = async (_, signal) => { loginSignal = signal; return new Promise(resolve => { finish = resolve; }); };
  const b = new SessionModel(apiB, () => {}, () => {});
  const a = new SessionModel(client(async () => observed), () => {}, () => b.invalidate('changed'));
  try {
    await a.inspect();
    const login = b.login({ username: 'owner', password: 'sample password' });
    a.suspend();
    assert.equal(a.view.session, undefined);
    assert.equal(a.view.notice, 'recheck');
    assert.equal(loginSignal?.aborted, false);
    finish(observed);
    await login;
    assert.equal(b.view.kind, 'signed-in');
  } finally { a.dispose(); b.dispose(); }
});
