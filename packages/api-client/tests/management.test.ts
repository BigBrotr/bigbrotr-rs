import assert from 'node:assert/strict';
import test from 'node:test';
import { createManagementClient, ManagementFailure, sessionDeadlineMs } from '../src/management.ts';

const observed = {
  format: 'bigbrotr-session/1', principal_id: '0123456789abcdef0123456789abcdef',
  credential_revision: '18446744073709551615', grant_revision: '9007199254740993', policy_revision: '1',
  expires_at_us: '1800000000000999', idle_expires_at_us: '1799999990000001',
  stored_actions: ['audit.read', 'data.read'], authority: 'observation-only'
};
const json = (value: unknown, status = 200, headers = {}) => new Response(JSON.stringify(value), {
  status, headers: { 'Content-Type': 'application/json', ...headers }
});
const failure = (kind: string) => (error: unknown) => error instanceof ManagementFailure && error.kind === kind;

test('fixed relative routes, exact credentials, cookies and no automatic retry', async () => {
  const requests: { input: unknown; init?: RequestInit }[] = [];
  const client = createManagementClient({
    cookie: () => `unrelated=1; BigBrotrLocalCsrf=${'a'.repeat(64)}`,
    fetch: async (input, init) => {
      requests.push({ input, init });
      return init?.method === 'DELETE' ? new Response(null, { status: 204 }) : json(observed, init?.method === 'POST' ? 201 : 200);
    }
  });
  const result = await client.login({ username: 'owner', password: '  exact password  ' });
  assert.equal(result.grant_revision, '9007199254740993');
  assert.equal(sessionDeadlineMs(result), 1799999990000);
  await client.inspect();
  await client.logout();
  assert.deepEqual(requests.map(v => v.input), ['/management/v1/sessions', '/management/v1/me', '/management/v1/sessions/current']);
  assert.equal(requests[0]?.init?.body, '{"username":"owner","password":"  exact password  "}');
  for (const { init } of requests) {
    assert.equal(init?.credentials, 'same-origin');
    assert.equal(init?.redirect, 'error');
    assert.equal(init?.cache, 'no-store');
    assert.equal(init?.mode, 'same-origin');
  }
  assert.equal(new Headers(requests[2]?.init?.headers).get('x-csrf-token'), 'a'.repeat(64));
});

test('absent, malformed and duplicate CSRF never dispatch a logout', async () => {
  for (const cookie of ['', 'BigBrotrLocalCsrf=wrong', `BigBrotrLocalCsrf=${'b'.repeat(64)}; BigBrotrLocalCsrf=${'b'.repeat(64)}`]) {
    let requests = 0;
    const client = createManagementClient({ cookie: () => cookie, fetch: async () => { requests++; return new Response(null, { status: 204 }); } });
    await assert.rejects(client.logout(), failure('csrf_missing'));
    assert.equal(requests, 0);
  }
});

test('closed response schema and exact integer ranges refuse incompatible observations', async () => {
  for (const patch of [
    { format: 'bigbrotr-session/2' }, { session: 'must-not-be-a-body-field' },
    { grant_revision: 9007199254740992 }, { grant_revision: '18446744073709551616' },
    { policy_revision: '01' }, { expires_at_us: '9223372036854775808' },
    { idle_expires_at_us: '1800000000001000' }, { principal_id: '0'.repeat(32) },
    { stored_actions: ['audit.read', 'audit.read'] }, { stored_actions: ['signing.admin'] },
    { authority: 'command-authority' }
  ]) {
    const client = createManagementClient({ fetch: async () => json({ ...observed, ...patch }) });
    await assert.rejects(client.inspect(), failure('incompatible'));
  }
  const { policy_revision: omitted, ...missing } = observed;
  assert.ok(omitted);
  await assert.rejects(createManagementClient({ fetch: async () => json(missing) }).inspect(), failure('incompatible'));
});

test('stream byte limit applies without Content-Length and with a lying small header', async () => {
  for (const length of [undefined, '2']) {
    let cancelled = false;
    let chunks = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) { chunks++; controller.enqueue(new Uint8Array(4096).fill(32)); },
      cancel() { cancelled = true; }
    });
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (length) headers['Content-Length'] = length;
    const client = createManagementClient({ fetch: async () => new Response(body, { headers }) });
    await assert.rejects(client.inspect(), failure('response_limit'));
    assert.equal(cancelled, true);
    assert.ok(chunks <= 6);
  }
});

test('invalid UTF-8, wrong media type and redirects are not accepted as observations', async () => {
  for (const response of [
    new Response(new Uint8Array([0xff]), { headers: { 'Content-Type': 'application/json' } }),
    new Response(JSON.stringify(observed), { headers: { 'Content-Type': 'text/html' } }),
    new Response(null, { status: 302, headers: { location: 'https://elsewhere.invalid/' } })
  ]) {
    await assert.rejects(createManagementClient({ fetch: async () => response }).inspect(), failure('incompatible'));
  }
});

test('typed throttling preserves fixed retry advice and cannot disclose arbitrary error text', async () => {
  const envelope = { error: { status: 429, code: 'capacity_or_rate_limited', message: 'Request capacity or attempt limit reached.', retry_class: 'retry_later', correlation_id: 'a'.repeat(32) } };
  let requests = 0;
  const client = createManagementClient({ fetch: async () => { requests++; return json(envelope, 429, { 'Retry-After': '60' }); } });
  await assert.rejects(client.inspect(), (error: unknown) => {
    assert.ok(error instanceof ManagementFailure);
    assert.equal(error.kind, 'api');
    assert.equal(error.api?.status, 429);
    assert.equal(error.retryAfterSeconds, 60);
    return true;
  });
  assert.equal(requests, 1);
  for (const changed of [
    { ...envelope.error, message: 'secret canary' },
    { ...envelope.error, status: 401 },
    { ...envelope.error, code: 'authentication_denied' }
  ]) await assert.rejects(createManagementClient({ fetch: async () => json({ error: changed }, 429, { 'Retry-After': '60' }) }).inspect(), failure('incompatible'));
});

test('cancellation reaches the transport and suppresses an already queued response', async () => {
  const controller = new AbortController();
  let signal: AbortSignal | undefined;
  let complete!: (response: Response) => void;
  const client = createManagementClient({ fetch: async (_, init) => {
    signal = init?.signal ?? undefined;
    return new Promise(resolve => { complete = resolve; });
  } });
  const request = client.inspect(controller.signal);
  controller.abort();
  assert.equal(signal?.aborted, true);
  complete(json(observed));
  await assert.rejects(request, failure('cancelled'));
});

test('network errors on mutations remain unknown, with no retry or secret in the exception', async () => {
  let requests = 0;
  const client = createManagementClient({ fetch: async () => { requests++; throw new Error('secret credential canary'); } });
  await assert.rejects(client.login({ username: 'owner', password: 'actual password' }), (error: unknown) => {
    assert.ok(error instanceof ManagementFailure);
    assert.equal(error.kind, 'unavailable');
    assert.equal(JSON.stringify(error).includes('canary'), false);
    assert.equal(error.message.includes('canary'), false);
    assert.equal(error.cause, undefined);
    return true;
  });
  assert.equal(requests, 1);
});

test('password limits use Unicode scalars and UTF-8 bytes without normalization', async () => {
  let requests = 0;
  const client = createManagementClient({ fetch: async () => { requests++; return json(observed, 201); } });
  for (const password of ['x'.repeat(11), 'é'.repeat(513), 'password\nwith control', '\ud800'.repeat(12)]) {
    await assert.rejects(client.login({ username: 'owner', password }), failure('invalid_input'));
  }
  await client.login({ username: 'owner', password: '😀'.repeat(12) });
  await client.login({ username: 'owner', password: ' '.repeat(12) });
  assert.equal(requests, 2);
});

test('a stalled response is cancelled at the finite client deadline', async () => {
  let cancelled = false;
  const body = new ReadableStream<Uint8Array>({ cancel() { cancelled = true; } });
  const client = createManagementClient({ fetch: async () => new Response(body, { headers: { 'Content-Type': 'application/json' } }) });
  const started = performance.now();
  await assert.rejects(client.inspect(), failure('unavailable'));
  assert.equal(cancelled, true);
  assert.ok(performance.now() - started < 10_000);
});

const job = {
  format: 'bigbrotr-job-status/1', job_id: '09'.repeat(16), kind: 'export', state: 'paused',
  job_revision: '9007199254740993', checkpoint_revision: '0', created_at_us: '1800000000000000',
  completed_at_us: null, observed_at_us: '1800000000000999', reason_code: 'resource_limit',
  authority: 'observation-only'
};

test('job status uses the canonical fixed route and CSRF without refreshing the session', async () => {
  const requests: string[] = [];
  const api = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${'a'.repeat(64)}`, fetch: async (path, init) => {
    requests.push(String(path));
    assert.equal(init?.method, 'GET');
    assert.equal(init?.body, undefined);
    assert.equal(new Headers(init?.headers).get('x-csrf-token'), 'a'.repeat(64));
    assert.equal(init?.credentials, 'same-origin');
    return json(job);
  } });
  assert.deepEqual(await api.jobStatus(job.job_id), job);
  for (const id of ['', '0'.repeat(32), 'AB'.repeat(16), '../me', `${job.job_id}?extra=1`]) {
    await assert.rejects(api.jobStatus(id), failure('invalid_input'));
  }
  assert.deepEqual(requests, [`/management/v1/jobs/${job.job_id}`]);
});

test('job observations reject wrong identities, private fields, invalid states and unsafe integers', async () => {
  for (const patch of [
    { job_id: '01'.repeat(16) }, { query: { private: true } }, { state: 'completed' },
    { reason_code: 'sensitive_arbitrary_database_text' }, { authority: 'execute' },
    { job_revision: '18446744073709551616' }, { checkpoint_revision: '-1' },
    { created_at_us: '9223372036854775808' }, { completed_at_us: '1800000000000001' },
    { state: 'succeeded', reason_code: null }, { reason_code: null }
  ]) {
    const api = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${'a'.repeat(64)}`, fetch: async () => json({ ...job, ...patch }) });
    await assert.rejects(api.jobStatus(job.job_id), failure('incompatible'));
  }
});

test('missing or invisible job is a typed non-retrying failure', async () => {
  let calls = 0;
  const api = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${'a'.repeat(64)}`, fetch: async () => {
    calls++;
    return json({ error: { status: 404, code: 'job_unavailable', message: 'Job unavailable to this account.', retry_class: 'check_state', correlation_id: null } }, 404);
  } });
  await assert.rejects(api.jobStatus(job.job_id), (error: unknown) => error instanceof ManagementFailure && error.api?.status === 404);
  assert.equal(calls, 1);
});
