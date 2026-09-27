import assert from 'node:assert/strict';
import test from 'node:test';
import * as jobs from '../../src/lib/jobs.ts';
import { ManagementFailure } from '@bigbrotr/api-client/management';
import type { JobPage, JobListQuery } from '@bigbrotr/api-client/management';

const page: JobPage = { format: 'bigbrotr-job-list/1', items: [], next_cursor: 'opaque-next', effective_page_size: 2, observed_at_us: '9007199254740993', authority: 'observation-only' };

test('pages are explicit, filters are snapshotted and an empty page can advance', async () => {
  const queries: JobListQuery[] = [];
  const model = new jobs.JobPageModel({ listJobs: async query => { queries.push(query!); return queries.length === 2 ? { ...page, next_cursor: null } : page; } }, () => {}, () => {});
  const filters: JobListQuery = { state: 'paused', page_size: 2 };
  await model.start(filters);
  filters.state = 'failed';
  assert.equal(queries.length, 1);
  assert.deepEqual(model.view.page, page);
  await model.next();
  assert.deepEqual(queries[1], { state: 'paused', page_size: 2, cursor: 'opaque-next' });
  assert.equal(model.view.page?.next_cursor, null);
  await model.next();
  assert.equal(queries.length, 2);
  await model.start({ page_size: 2 });
  assert.deepEqual(queries[2], { page_size: 2 });
  model.dispose();
});

test('superseded pages, edited filters and disposal cancel work and release old views', async () => {
  const pending: { signal?: AbortSignal; resolve: (page: JobPage) => void }[] = [];
  const model = new jobs.JobPageModel({ listJobs: async (_, signal) => new Promise(resolve => pending.push({ signal, resolve })) }, () => {}, () => {});
  const first = model.start({ page_size: 2 });
  const second = model.start({ page_size: 3 });
  assert.equal(pending[0]?.signal?.aborted, true);
  pending[0]!.resolve(page); await first;
  assert.equal(model.view.kind, 'checking'); assert.equal(model.view.page, undefined);
  model.clear();
  assert.equal(pending[1]?.signal?.aborted, true);
  pending[1]!.resolve(page); await second;
  assert.equal(model.view.kind, 'empty');
  const third = model.start({ page_size: 2 });
  model.dispose();
  assert.equal(pending[2]?.signal?.aborted, true);
  pending[2]!.resolve(page); await third;
  await model.start(); await model.next();
  assert.equal(pending.length, 3); assert.equal(model.view.page, undefined);
});

test('an in-flight next page cannot dispatch duplicates and failures do not retain a cursor', async () => {
  let calls = 0;
  let reject!: (reason: unknown) => void;
  const model = new jobs.JobPageModel({ listJobs: async () => {
    calls++; if (calls === 1) return page;
    return new Promise((_, no) => { reject = no; });
  } }, () => {}, () => {});
  await model.start();
  const next = model.next();
  assert.equal(model.view.page, undefined);
  await model.next(); assert.equal(calls, 2);
  reject(new ManagementFailure('api', { status: 410, code: 'cursor_expired', message: 'Continuation expired. Start a new listing.', retry_class: 'check_state', correlation_id: null }));
  await next;
  assert.equal(model.view.kind, 'failed'); assert.equal(model.view.page, undefined);
  assert.match(jobs.jobPageFailure(model.view.failure!), /start a new listing/i);
  await model.next(); assert.equal(calls, 2);
  model.dispose();
});

test('session denial clears the page and invalidates its parent without automatic retry', async () => {
  let denied = 0;
  let calls = 0;
  const model = new jobs.JobPageModel({ listJobs: async () => {
    calls++;
    if (calls === 1) return page;
    throw new ManagementFailure('api', { status: 401, code: 'authentication_denied', message: 'Authentication required or denied.', retry_class: 'authenticate', correlation_id: null });
  } }, () => {}, () => { denied++; });
  await model.start(); await model.next();
  assert.equal(model.view.page, undefined); assert.equal(calls, 2); assert.equal(denied, 1);
  model.dispose();
});

test('UTC date filters are exact, empty is omitted and invalid calendar dates are rejected', () => {
  assert.equal(jobs.utcDayStart(''), undefined);
  assert.equal(jobs.utcDayStart('1970-01-01'), '0');
  assert.equal(jobs.utcDayStart('1969-12-31'), '-86400000000');
  assert.equal(jobs.utcDayStart('2000-02-29'), '951782400000000');
  for (const value of ['2001-02-29', '2000-02-30', '2026-13-01', '26-01-01', '2026-1-1', '2026-01-01T00:00', '2026-01-01\n']) {
    assert.throws(() => jobs.utcDayStart(value), error => error instanceof ManagementFailure && error.kind === 'invalid_input');
  }
});

test('start always discards a previous cursor even when a wider typed object supplies it', async () => {
  let actual: JobListQuery | undefined;
  const model = new jobs.JobPageModel({ listJobs: async query => { actual = query; return page; } }, () => {}, () => {});
  const prior: JobListQuery = { page_size: 2, cursor: 'old-continuation' };
  await model.start(prior);
  assert.deepEqual(actual, { page_size: 2 });
  model.dispose();
});
