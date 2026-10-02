import assert from 'node:assert/strict';
import test from 'node:test';
import { createManagementClient, ManagementFailure } from '../src/management.ts';

const csrf = 'a'.repeat(64);
const token = 'opaque_continuation-01';
const row = {
  format: 'bigbrotr-job-status/1', job_id: '09'.repeat(16), kind: 'export', state: 'paused',
  job_revision: '18446744073709551615', checkpoint_revision: '9007199254740993',
  created_at_us: '9007199254740993', completed_at_us: null,
  observed_at_us: '9007199254740999', reason_code: 'resource_limit', authority: 'observation-only'
};
const page = {
  format: 'bigbrotr-job-list/1', items: [row], next_cursor: token,
  effective_page_size: 2, observed_at_us: row.observed_at_us, authority: 'observation-only'
};
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), {
  status, headers: { 'Content-Type': 'application/json' }
});
const failure = (kind: string) => (error: unknown) => error instanceof ManagementFailure && error.kind === kind;

test('list uses fixed origin, explicit CSRF and exact integer filters; empty continuation does not trigger another request', async () => {
  const requests: { path: string; init?: RequestInit }[] = [];
  const api = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${csrf}`, fetch: async (path, init) => {
    requests.push({ path: String(path), init });
    return json({ ...page, items: [] });
  } });
  const result = await api.listJobs({ state: 'paused', created_from_us: '9007199254740993', created_before_us: '9223372036854775807', page_size: 2, cursor: token });
  assert.deepEqual(result, { ...page, items: [] });
  assert.equal(requests.length, 1);
  assert.equal(requests[0]?.path, `/management/v1/jobs?state=paused&created_from_us=9007199254740993&created_before_us=9223372036854775807&page_size=2&cursor=${token}`);
  const init = requests[0]?.init;
  assert.equal(init?.method, 'GET'); assert.equal(init?.body, undefined);
  assert.equal(init?.credentials, 'same-origin'); assert.equal(init?.mode, 'same-origin');
  assert.equal(init?.redirect, 'error'); assert.equal(init?.cache, 'no-store');
  assert.equal(new Headers(init?.headers).get('x-csrf-token'), csrf);
});

test('list rejects malformed or open-ended caller shapes before dispatch', async () => {
  let calls = 0;
  const api = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${csrf}`, fetch: async () => { calls++; return json(page); } });
  for (const query of [null, { extra: true }, { kind: 'import' }, { state: '' }, { state: 'done' },
    { page_size: 0 }, { page_size: 1.5 }, { page_size: 65536 }, { page_size: '2' },
    { cursor: '' }, { cursor: 'bad=padding' }, { cursor: 'a'.repeat(4097) },
    { created_from_us: '01' }, { created_before_us: '-0' }, { created_from_us: '9223372036854775808' },
    { created_from_us: '-9223372036854775809' }, { created_from_us: '2', created_before_us: '2' }]) {
    await assert.rejects(api.listJobs(query as never), failure('invalid_input'));
  }
  assert.equal(calls, 0);
});

test('list checks row visibility filters, descending order, exact times, limits and closed metadata', async () => {
  for (const patch of [
    { authority: 'execute' }, { private_scope: 'canary' }, { effective_page_size: 3 },
    { next_cursor: '' }, { next_cursor: 'a'.repeat(4097) }, { observed_at_us: '9223372036854775808' },
    { items: [row, row] }, { items: [row, { ...row, job_id: 'ff'.repeat(16) }] },
    { items: [{ ...row, observed_at_us: '9007199254740998' }] },
    { items: [{ ...row, state: 'running' }] },
    { items: [{ ...row, created_at_us: '9007199254740992' }] },
    { items: [{ ...row, created_at_us: '9007199254740994' }] },
    { items: [{ ...row, reason_code: null }] },
    { items: [{ ...row, kind: 'acquisition' }] },
    { items: [{ ...row, created_at_us: '9223372036854775808' }] }
  ]) {
    const api = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${csrf}`, fetch: async () => json({ ...page, ...patch }) });
    await assert.rejects(api.listJobs({ page_size: 2, state: 'paused', created_from_us: row.created_at_us, created_before_us: '9007199254740994' }), failure('incompatible'));
  }
  const valid = { ...page, items: [row, { ...row, job_id: '08'.repeat(16) }], next_cursor: null };
  const api = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${csrf}`, fetch: async () => json(valid) });
  assert.deepEqual(await api.listJobs({ page_size: 2 }), valid);
});

test('list permits a bounded full200row response larger than the old detail limit', async () => {
  const full = { ...page, effective_page_size: 200, next_cursor: null,
    items: Array.from({ length: 200 }, (_, index) => ({ ...row, job_id: (200 - index).toString(16).padStart(32, '0') })) };
  assert.ok(new TextEncoder().encode(JSON.stringify(full)).length > 16 * 1024);
  const api = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${csrf}`, fetch: async () => json(full) });
  assert.deepEqual(await api.listJobs({ page_size: 65535 }), full);
  let cancelled = false;
  let chunks = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) { chunks++; controller.enqueue(new Uint8Array(16384).fill(32)); },
    cancel() { cancelled = true; }
  });
  const excessive = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${csrf}`, fetch: async () => new Response(body, {
    headers: { 'Content-Type': 'application/json', 'Content-Length': '2' }
  }) });
  await assert.rejects(excessive.listJobs(), failure('response_limit'));
  assert.equal(cancelled, true); assert.ok(chunks <= 10);
});

test('cursor refusals remain typed and never restart a listing automatically', async () => {
  for (const [status, code, message, retry_class] of [
    [400, 'invalid_cursor', 'Continuation token refused.', 'correct_request'],
    [409, 'cursor_incompatible', 'Continuation no longer matches this request or authority.', 'check_state'],
    [410, 'cursor_expired', 'Continuation expired. Start a new listing.', 'check_state']
  ] as const) {
    let calls = 0;
    const api = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${csrf}`, fetch: async () => {
      calls++; return json({ error: { status, code, message, retry_class, correlation_id: null } }, status);
    } });
    await assert.rejects(api.listJobs({ cursor: token }), (error: unknown) => error instanceof ManagementFailure && error.api?.code === code);
    assert.equal(calls, 1);
  }
});

test('in-flight caller edits cannot change response bindings or the dispatched query', async () => {
  let complete!: (response: Response) => void;
  let path: string | undefined;
  const api = createManagementClient({ cookie: () => `BigBrotrLocalCsrf=${csrf}`, fetch: async input => {
    path = String(input); return new Promise(resolve => { complete = resolve; });
  } });
  const query = { state: 'paused' as const, page_size: 2, created_from_us: row.created_at_us };
  const result = api.listJobs(query);
  query.page_size = 1; query.created_from_us = '9223372036854775807';
  complete(json(page));
  assert.deepEqual(await result, page);
  assert.equal(path, '/management/v1/jobs?state=paused&created_from_us=9007199254740993&page_size=2');
});
