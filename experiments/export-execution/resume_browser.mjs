// Actual production bundle and Rust/PostgreSQL. Only the first resume reply is
// faulted: Playwright forwards the real request, checks its real202, then aborts
// the browser route. Later retries use the browser's ordinary HTTP transport.
import assert from 'node:assert/strict';
import { readFile, writeFile, rename } from 'node:fs/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import { chromium, firefox, webkit, expect } from '@playwright/test';
import { inspectStorage, readResumeStorage } from './storage_oracle.mjs';
import { traceSkipLink } from './skip_trace.mjs';
const home = '/case', origin = 'http://127.0.0.1:18080';
const input = JSON.parse(await readFile(home + '/input.json', 'utf8'));
const [engine = 'chromium', mount = 'root'] = process.argv.slice(2);
assert.ok(['chromium', 'firefox', 'webkit'].includes(engine));
assert.ok(['root', 'control'].includes(mount));
const url = origin + (mount === 'root' ? '/' : '/control/');
const report = { format: 'bigbrotr-resume-browser/1', status: 'INCOMPLETE', engine, mount, cases: [], requests: [], responses: [] };
let browser, context, page, stage = 'launch', sequence = 0, journeyComplete = false;
const errors = [];
const headerReads = [];
async function save() {
  report.stage = stage;
  await writeFile(home + '/browser-report.tmp', JSON.stringify(report, null, 2) + '\n', { mode: 0o600 });
  await rename(home + '/browser-report.tmp', home + '/browser-report.json');
}
async function command(name, fields = {}) {
  const n = ++sequence;
  await writeFile(`${home}/command-${n}.tmp`, JSON.stringify({ name, ...fields }), { flag: 'wx', mode: 0o600 });
  await rename(`${home}/command-${n}.tmp`, `${home}/command-${n}.json`);
  const deadline = performance.now() + 12000;
  for (;;) {
    try { const value = JSON.parse(await readFile(`${home}/response-${n}.json`, 'utf8')); assert.equal(value.state, 'verified'); return value; }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    assert.ok(performance.now() < deadline, 'host command deadline'); await sleep(10);
  }
}
async function step(name, body) {
  stage = name; await save(); const started = performance.now(); await body();
  report.cases.push({ name, status: 'PASS', milliseconds: performance.now() - started });
  await save();
}
function observe(p) {
  p.on('pageerror', () => errors.push('pageerror'));
  p.on('request', request => {
    const url = new URL(request.url());
    if (url.origin !== origin) errors.push('unexpected-origin');
    if (!url.pathname.startsWith('/management/')) return;
    if (report.requests.length >= 128) { report.overflow = true; return; }
    const item = { path: url.pathname, method: request.method() };
    if (url.pathname.endsWith('/resume')) {
      item.key = request.headers()['idempotency-key']; item.body = request.postDataJSON();
    }
    report.requests.push(item);
  });
  p.on('response', response => {
    const path = new URL(response.url()).pathname;
    if (!path.startsWith('/management/')) return;
    if (report.responses.length >= 128) { report.overflow = true; return; }
    const item = { path, status: response.status() }; report.responses.push(item);
    // Browser Response.headers omits security/cookie headers in Playwright.
    // Keep only booleans; complete header values never enter the evidence.
    headerReads.push(response.allHeaders().then(headers => {
      item.noStore = headers['cache-control'] === 'no-store';
      item.resumeSetCookie = path.endsWith('/resume') && headers['set-cookie'] !== undefined;
    }).catch(() => { errors.push('header-read-failed'); }));
  });
}
async function login(who = 'alice') {
  await page.getByLabel('Username', { exact: true }).fill(who);
  await page.getByLabel('Password', { exact: true }).fill('Independent owner password 2026');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Your session', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Load saved request', exact: true })).toBeEnabled();
  await command(who + '-login');
}
async function logout(who = 'alice') {
  await page.getByRole('button', { name: 'Sign out', exact: true }).click();
  await expect(page.locator('#session-notice')).toContainText('signed out');
  await command(who + '-logout');
}
async function stored(p = page) {
  const result = await readResumeStorage(p); assert.equal(result.status, 'verified'); return result.records;
}
const resumes = () => report.requests.filter(r => r.path.endsWith('/resume'));
let original, accepted;
async function retry() {
  const response = page.waitForResponse(r => new URL(r.url()).pathname.endsWith('/resume'));
  await page.getByRole('button', { name: 'Retry saved request', exact: true }).click();
  const received = await response; assert.equal(received.status(), 200);
  assert.deepEqual(await received.json(), { ...accepted, created: false });
  await expect(page.locator('.resume-sheet .notice')).toContainText('Resume accepted');
  await expect(page.getByRole('button', { name: 'Forget resolved request', exact: true })).toBeEnabled();
}
try {
  // Leave time for failure evidence and teardown inside the host's 150 s bound.
  // The selected Playwright default launch timeout is longer than that bound.
  report.launchTimeoutMs = 30000; await save();
  browser = await ({ chromium, firefox, webkit })[engine].launch({ headless: true, timeout: report.launchTimeoutMs });
  report.version = browser.version(); stage = 'context'; await save();
  context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  stage = 'page'; await save();
  page = await context.newPage(); observe(page);
  await step('installed-bundle-session-and-paused-job', async () => {
    const response = await page.goto(url); assert.equal(response.status(), 200);
    assert.match(response.headers()['content-security-policy'], /frame-ancestors 'none'/);
    await login(); assert.deepEqual(await stored(), []);
    await page.getByLabel('Job ID', { exact: true }).fill(input.job);
    const received = page.waitForResponse(r => r.url() === `${origin}/management/v1/jobs/${input.job}`);
    await page.getByRole('button', { name: 'Check job', exact: true }).click();
    const value = await (await received).json();
    assert.equal(value.job_id, input.job); assert.equal(value.job_revision, '7'); assert.equal(value.state, 'paused');
    await expect(page.getByRole('button', { name: 'Resume this export', exact: true })).toBeEnabled();
    await command('before-loss');
  });
  await step('persist-original-request-before-real-acceptance-withheld-reply', async () => {
    let complete, reject;
    const fault = new Promise((resolve, fail) => { complete = resolve; reject = fail; });
    const path = `${origin}/management/v1/jobs/${input.job}/resume`;
    await page.route(path, async route => {
      try {
        const request = route.request(); const records = await stored(); assert.equal(records.length, 1);
        original = records[0];
        assert.equal(original.principal_id, input.actor); assert.equal(original.job_id, input.job); assert.equal(original.outcome, 'pending');
        assert.equal(original.key, request.headers()['idempotency-key']);
        assert.deepEqual(original.command, { expected_job_revision: '7', expected_config_revision: '1', expected_policy_revision: '2' });
        assert.deepEqual(request.postDataJSON(), original.command);
        const response = await route.fetch({ timeout: 5000, maxRedirects: 0, maxRetries: 0 }); assert.equal(response.status(), 202);
        assert.equal(response.headers()['cache-control'], 'no-store');
        assert.equal(response.headersArray().some(header => header.name.toLowerCase() === 'set-cookie'), false);
        accepted = await response.json();
        assert.deepEqual(accepted, (await command('accepted', { key: original.key, body: original.command })).receipt);
        report.withheld = { status: response.status(), receipt: accepted, pendingBeforeForward: original, noStore: true, setCookie: false };
        await route.abort('failed'); complete();
      } catch (error) { await route.abort('failed').catch(() => {}); reject(error); }
    }, { times: 1 });
    await page.getByRole('button', { name: 'Resume this export', exact: true }).click(); await fault;
    await expect(page.locator('.resume-sheet .failure')).toContainText('could not be confirmed');
    assert.deepEqual(await stored(), [original]);
    await expect(page.getByRole('button', { name: 'Resume this export', exact: true })).toBeDisabled();
    assert.equal(resumes().length, 1);
  });
  await step('reload-restores-without-dispatch-and-explicit-retry-resolves-original', async () => {
    await page.reload();
    await expect(page.getByRole('button', { name: 'Retry saved request', exact: true })).toBeEnabled();
    assert.deepEqual(await stored(), [original]); assert.equal(resumes().length, 1);
    await expect(page.locator('.job-observation')).toHaveCount(0);
    await command('before-retry'); await retry(); await command('after-retry');
    assert.deepEqual(await stored(), [{ ...original, outcome: 'accepted' }]);
    const response = page.waitForResponse(r => r.url() === `${origin}/management/v1/jobs/${input.job}`);
    await page.getByRole('button', { name: 'Check saved job', exact: true }).click();
    assert.equal((await (await response).json()).state, 'queued'); await expect(page.locator('.job-state')).toHaveText('Queued');
    await page.setViewportSize({ width: 375, height: 812 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    // The first pointer trial must precede every screenshot in this document.
    report.skipTrace = await traceSkipLink(page, home, async activate => {
      const checked = page.waitForResponse(r => r.url() === `${origin}/management/v1/jobs/${input.job}`, { timeout: 5000 });
      const [response] = await Promise.all([checked, activate()]);
      assert.equal((await response.json()).state, 'queued');
      await expect(page.getByRole('button', { name: 'Check job', exact: true })).toBeEnabled();
    });
    await page.screenshot({ path: home + '/resume-mobile.png', fullPage: true });
    assert.equal(report.skipTrace.status, 'PASS');
    await page.setViewportSize({ width: 1280, height: 900 });
  });
  await step('same-cookie-retry-after-control-restart-without-current-input-authority', async () => {
    await command('restart'); await retry(); await command('after-restart-retry');
    assert.deepEqual(resumes().map(r => ({ key: r.key, body: r.body })), Array(3).fill({ key: original.key, body: original.command }));
  });
  await step('logout-clears-both-tabs-and-other-account-does-not-display-saved-request', async () => {
    const other = await context.newPage(); observe(other); await other.goto(url);
    await expect(other.getByRole('button', { name: 'Retry saved request', exact: true })).toBeEnabled();
    await logout();
    for (const p of [page, other]) await expect(p.locator('.job-sheet')).toHaveCount(0);
    assert.deepEqual(await stored(), [{ ...original, outcome: 'accepted' }]); await other.close();
    await login('bob'); await expect(page.locator('.resume-sheet')).toContainText('No resume request');
    await expect(page.locator('.resume-sheet')).not.toContainText(original.key);
    assert.deepEqual(await stored(), [{ ...original, outcome: 'accepted' }]);
    await logout('bob');
    await expect(page.locator('.job-sheet')).toHaveCount(0);
  });
  await step('same-account-recovery-and-real-revocation', async () => {
    await login(); await expect(page.getByRole('button', { name: 'Retry saved request', exact: true })).toBeEnabled();
    assert.equal(resumes().length, 3); await command('deactivate');
    const denied = page.waitForResponse(r => new URL(r.url()).pathname.endsWith('/resume'));
    await page.getByRole('button', { name: 'Retry saved request', exact: true }).click(); assert.equal((await denied).status(), 401);
    await expect(page.locator('.job-sheet')).toHaveCount(0);
    report.retainedAfterRevocation = await stored(); assert.deepEqual(report.retainedAfterRevocation, [{ ...original, outcome: 'accepted' }]);
  });
  await step('forget-resolved-browser-metadata-leaves-durable-job-unchanged', async () => {
    await command('reactivate'); await login();
    await command('before-forget');
    await page.getByRole('button', { name: 'Forget resolved request', exact: true }).click();
    await expect(page.locator('.resume-sheet')).toContainText('No resume request'); assert.deepEqual(await stored(), []);
    await command('forgotten');
    await logout();
    await expect(page.locator('.job-sheet')).toHaveCount(0);
    report.webStorage = inspectStorage(await page.evaluate(() => ({ local: Object.entries(localStorage), session: Object.entries(sessionStorage) })));
    assert.equal(report.webStorage.status, 'verified'); report.finalSavedRequests = await stored();
  });
  await Promise.all(headerReads);
  assert.equal(resumes().length, 4); assert.deepEqual(errors, []); assert.equal(report.overflow, undefined);
  assert.ok(report.responses.every(r => r.noStore && !r.resumeSetCookie)); journeyComplete = true;
} catch (error) {
  report.status = 'FAIL'; report.failure = { stage, kind: error?.constructor?.name ?? 'UnknownError' };
  if (stage === 'launch') {
    // Before any page or credential exists, the launch error contains only
    // executable/process diagnostics. Later HTTP/UI errors remain redacted.
    const detail = String(error?.message ?? '');
    report.failure.detail = detail.slice(0, 4096);
    report.failure.detailTruncated = detail.length > 4096;
  }
  const line = String(error.stack).match(/resume_browser\.mjs:(\d+):(\d+)/); if (line) report.failure.location = { line: +line[1], column: +line[2] };
  await save();
  if (page) try { await page.screenshot({ path: home + '/failure.png', fullPage: true, timeout: 2000 }); } catch { report.failure.screenshot = 'unavailable'; }
} finally {
  // A rejected launch may have started a child without returning a browser
  // handle. Only the host's owned-container audit establishes native exit.
  report.browserHandleAcquired = Boolean(browser);
  report.teardown = 'pending'; await save();
  for (const resource of [context, browser]) if (resource) try { await resource.close(); } catch { report.status = 'FAIL'; report.teardown = 'failed'; }
  if (report.teardown === 'pending') report.teardown = 'available-handles-closed';
  if (journeyComplete && report.teardown === 'available-handles-closed') report.status = 'PASS';
  await save();
  process.exitCode = report.status === 'PASS' ? 0 : 1;
}
