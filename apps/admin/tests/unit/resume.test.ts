import assert from 'node:assert/strict';
import test from 'node:test';
import { ManagementFailure } from '@bigbrotr/api-client/management';
import type { JobObservation, ManagementClient, ResumeExportAcceptance } from '@bigbrotr/api-client/management';
import { ResumeModel } from '../../src/lib/resume.ts';
import { ResumeStorageFailure, type ResumeRecord, type ResumeStore } from '../../src/lib/resume-store.ts';

const context = { principal_id: '01'.repeat(16), config_revision: '9007199254740993', policy_revision: '3' };
const job: JobObservation = {
  format: 'bigbrotr-job-status/1', job_id: '02'.repeat(16), kind: 'export', state: 'paused',
  job_revision: '7', checkpoint_revision: '0', created_at_us: '1800000000000000',
  completed_at_us: null, observed_at_us: '1800000000000999', reason_code: 'resource_limit', authority: 'observation-only'
};
const saved: ResumeRecord = {
  format: 'bigbrotr-browser-resume/1', principal_id: context.principal_id, job_id: job.job_id, key: '03'.repeat(16),
  command: { expected_job_revision: '7', expected_config_revision: context.config_revision, expected_policy_revision: '3' }, outcome: 'pending'
};
const receipt = (key: string): ResumeExportAcceptance => ({ format: 'bigbrotr-export-resume/1', command_key: key,
  job_id: job.job_id, job_revision: '8', accepted_at_us: '1800000000001000', created: true, execution: 'unobserved', authority: 'acceptance-only' });
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(done => { resolve = done; }); return { promise, resolve }; }
const tick = () => new Promise<void>(resolve => setImmediate(resolve));
// Delays/failures are injected at the persistence and HTTP boundaries. Actual
// IDB transaction serialization/durability belongs to the browser scenarios.
function setup(options: { store?: Partial<ResumeStore>; send?: ManagementClient['resumeExport']; context?: typeof context } = {}) {
  let current: ResumeRecord | undefined;
  const sent: { id: string; key: string; body: unknown; signal?: AbortSignal }[] = [];
  let denied = 0;
  const store: ResumeStore = {
    load: async () => current,
    reserve: async value => { current = structuredClone(value); return structuredClone(value); },
    settle: async (value, outcome) => { current = { ...value, outcome }; return true; },
    forget: async () => { current = undefined; return true; }, ...options.store
  };
  const model = new ResumeModel(options.context ?? context, { resumeExport: async (id, key, body, signal) => {
    sent.push({ id, key, body: structuredClone(body), signal });
    return options.send ? options.send(id, key, body, signal) : receipt(key);
  } }, store, () => {}, () => { denied++; });
  return { model, sent, current: () => current, denied: () => denied };
}

test('HTTP waits for persisted identity; repeated clicks cannot dispatch twice', async () => {
  const commit = deferred<ResumeRecord>();
  let reserved: ResumeRecord | undefined;
  const run = setup({ store: { reserve: async value => { reserved = value; return commit.promise; } } });
  await run.model.restore();
  const sending = run.model.start(job);
  await tick();
  assert.ok(reserved);
  assert.match(reserved.key, /^[0-9a-f]{32}$/);
  assert.notEqual(reserved.key, '0'.repeat(32));
  assert.deepEqual(reserved.command, saved.command);
  assert.equal(run.sent.length, 0);
  await run.model.start(job);
  commit.resolve(reserved);
  await sending;
  assert.equal(run.sent.length, 1);
  assert.equal(run.sent[0]?.key, reserved.key);
  assert.equal(run.current()?.outcome, 'accepted');
  assert.equal(run.model.view.receipt?.authority, 'acceptance-only');
  await run.model.start(job);
  assert.equal(run.sent.length, 1, 'requires another explicit job observation');
  run.model.dispose();
});

test('reload never dispatches, and explicit retry retains old revisions under fresh context', async () => {
  const run = setup({ store: { load: async () => structuredClone(saved) }, context: { ...context, config_revision: '99', policy_revision: '100' } });
  await run.model.restore();
  assert.deepEqual(run.model.view.record, saved);
  assert.equal(run.sent.length, 0);
  await run.model.retry();
  assert.deepEqual(run.sent.map(({ id, key, body }) => ({ id, key, body })), [{ id: saved.job_id, key: saved.key, body: saved.command }]);
  run.model.dispose();
});

test('a competing pending request is shown without silently sending the other job', async () => {
  const prior = { ...saved, job_id: '04'.repeat(16) };
  const run = setup({ store: { reserve: async () => structuredClone(prior) } });
  await run.model.restore();
  await run.model.start(job);
  assert.equal(run.sent.length, 0);
  assert.deepEqual(run.model.view.record, prior);
  assert.equal(run.model.view.reviewSaved, true);
  run.model.dispose();
});

test('failed reservation never sends and disposal during local commit fences late results', async () => {
  const failure = setup({ store: { reserve: async () => { throw new ResumeStorageFailure('full'); } } });
  await failure.model.restore(); await failure.model.start(job);
  assert.equal(failure.sent.length, 0);
  assert.ok(failure.model.view.failure instanceof ResumeStorageFailure);
  failure.model.dispose();
  const commit = deferred<ResumeRecord>();
  let signal: AbortSignal | undefined;
  const run = setup({ store: { reserve: async (_, received) => { signal = received; return commit.promise; } } });
  await run.model.restore(); const sending = run.model.start(job); await tick();
  run.model.dispose(); assert.equal(signal?.aborted, true);
  commit.resolve(saved); await sending;
  assert.equal(run.sent.length, 0); assert.equal(run.model.view.record, undefined);
});

test('late HTTP acceptance after logout cannot settle or restore private state', async () => {
  const response = deferred<ResumeExportAcceptance>();
  const run = setup({ send: async () => response.promise });
  await run.model.restore(); const sending = run.model.start(job); await tick();
  assert.equal(run.sent.length, 1);
  run.model.dispose(); assert.equal(run.sent[0]?.signal?.aborted, true);
  response.resolve(receipt(run.sent[0]!.key)); await sending;
  assert.equal(run.current()?.outcome, 'pending');
  assert.equal(run.model.view.record, undefined); assert.equal(run.model.view.receipt, undefined);
});

test('only definitive409 settles refusal; auth/capacity/network uncertainty keeps original intent', async () => {
  const failures = [new ManagementFailure('unavailable'), new ManagementFailure('cancelled'), new ManagementFailure('incompatible'),
    new ManagementFailure('api', { status: 401, code: 'authentication_denied', message: 'Authentication required or denied.', retry_class: 'authenticate', correlation_id: null }),
    new ManagementFailure('api', { status: 403, code: 'permission_denied', message: 'This action is not permitted.', retry_class: 'check_state', correlation_id: null }),
    new ManagementFailure('api', { status: 409, code: 'command_conflict', message: 'Command key or expected revision conflicts with current state.', retry_class: 'check_state', correlation_id: null }),
    new ManagementFailure('api', { status: 429, code: 'capacity_or_rate_limited', message: 'Request capacity or attempt limit reached.', retry_class: 'retry_later', correlation_id: null }),
    new ManagementFailure('api', { status: 503, code: 'authority_unavailable', message: 'Current authority or dependency unavailable.', retry_class: 'check_state', correlation_id: null })];
  for (const failure of failures) {
    const run = setup({ send: async () => { throw failure; } });
    await run.model.restore(); await run.model.start(job);
    assert.equal(run.current()?.outcome, failure.api?.status === 409 ? 'refused' : 'pending');
    assert.equal(run.model.view.failure, failure);
    assert.equal(run.denied(), failure.api?.status === 401 ? 1 : 0);
    run.model.dispose();
  }
});

test('failed local settlement preserves acceptance and original recoverable request', async () => {
  const run = setup({ store: { settle: async () => { throw new ResumeStorageFailure('unavailable'); } } });
  await run.model.restore(); await run.model.start(job);
  assert.equal(run.model.view.receipt?.command_key, run.current()?.key);
  assert.equal(run.current()?.outcome, 'pending');
  assert.equal(run.model.view.record?.key, run.current()?.key);
  assert.ok(run.model.view.failure instanceof ResumeStorageFailure);
  run.model.dispose();
});

test('forget refuses pending metadata and cannot erase a replacement from another tab', async () => {
  let forgets = 0;
  let loaded = saved;
  const run = setup({ store: {
    load: async () => structuredClone(loaded),
    forget: async (principal, key) => { forgets++; assert.equal(principal, saved.principal_id); assert.equal(key, saved.key); return false; }
  } });
  await run.model.restore(); await run.model.forget(); assert.equal(forgets, 0);
  loaded = { ...saved, outcome: 'accepted' }; await run.model.restore();
  loaded = { ...saved, key: '05'.repeat(16) }; await run.model.forget();
  assert.equal(forgets, 1); assert.deepEqual(run.model.view.record, loaded);
  assert.equal(run.sent.length, 0); run.model.dispose();
});
