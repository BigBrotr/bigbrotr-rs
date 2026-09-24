import assert from 'node:assert/strict';
import test from 'node:test';
import { JobModel } from '../../src/lib/jobs.ts';
import { ManagementFailure } from '@bigbrotr/api-client/management';
import type { JobObservation } from '@bigbrotr/api-client/management';

const observed: JobObservation = {
  format: 'bigbrotr-job-status/1', job_id: '09'.repeat(16), kind: 'export', state: 'paused',
  job_revision: '4', checkpoint_revision: '0', created_at_us: '1800000000000000',
  completed_at_us: null, observed_at_us: '1800000000000999', reason_code: 'resource_limit',
  authority: 'observation-only'
};

test('superseded and disposed requests cannot restore private job observations', async () => {
  const pending: { signal?: AbortSignal; resolve: (value: JobObservation) => void }[] = [];
  const model = new JobModel({ jobStatus: async (_, signal) => new Promise(resolve => pending.push({ signal, resolve })) }, () => {}, () => {});
  const first = model.inspect(observed.job_id);
  const second = model.inspect('08'.repeat(16));
  assert.equal(pending[0]?.signal?.aborted, true);
  pending[0]!.resolve(observed);
  await first;
  assert.equal(model.view.observation, undefined);
  assert.equal(model.view.kind, 'checking');
  model.dispose();
  assert.equal(pending[1]?.signal?.aborted, true);
  pending[1]!.resolve({ ...observed, job_id: '08'.repeat(16) });
  await second;
  assert.equal(model.view.observation, undefined);
});

test('refusal clears old data and a denied session invalidates its parent view', async () => {
  let denied = 0;
  let calls = 0;
  const model = new JobModel({ jobStatus: async () => {
    calls++;
    if (calls === 1) return observed;
    throw new ManagementFailure('api', { status: 401, code: 'authentication_denied', message: 'Authentication required or denied.', retry_class: 'authenticate', correlation_id: null });
  } }, () => {}, () => { denied++; });
  await model.inspect(observed.job_id);
  assert.deepEqual(model.view.observation, observed);
  await model.inspect(observed.job_id);
  assert.equal(model.view.observation, undefined);
  assert.equal(denied, 1);
  assert.equal(calls, 2);
  model.dispose();
});
