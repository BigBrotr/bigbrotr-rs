// Actual production assets and Rust HTTP only. The native fixture temporarily
// omits one bootstrap module; no request interception, API mock or CSP bypass.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { writeFile, readFile, rename, readdir, readlink } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { chromium, firefox, webkit, expect } from '@playwright/test';
import { WireStatuses } from './wire-status.mjs';

const [engine, base, home, principal] = process.argv.slice(2);
assert.ok(['chromium', 'firefox', 'webkit'].includes(engine));
assert.ok(['/', '/control/'].includes(base));
assert.match(principal, /^[0-9a-f]{32}$/);
const origin = 'http://127.0.0.1:8081';
const password = 'Independent owner password 2026'; // Synthetic, matching actual CLI enrollment.
const report = { format: 'bigbrotr-private-panel-browser/1', status: 'RUNNING', browser: engine, base, cases: [] };
let browser;
let context;
let stage = 'launch';
let foreign;
let commandNumber = 0;
let primaryPage;
const observations = [];
const observationWork = [];
function observe(page) {
  const append = (request, event, status) => {
    const path = new URL(request.url()).pathname;
    if (!path.startsWith('/management/')) return;
    if (observations.length >= 128) { report.observationLimit = true; return; }
    const entry = { path, method: request.method(), event, status };
    observations.push(entry);
    observationWork.push(request.allHeaders().then(headers => {
      entry.origin = headers.origin === origin ? 'expected'
        : headers.origin === 'null' ? 'null' : headers.origin === undefined ? 'missing' : 'foreign';
      entry.contentType = ['application/json', 'text/plain'].includes(headers['content-type']) ? headers['content-type'] : 'other-or-missing';
    }).catch(() => { entry.origin = 'unobserved'; }));
  };
  page.on('request', request => append(request, 'request'));
  page.on('response', response => append(response.request(), 'response', response.status()));
  page.on('requestfailed', request => append(request, 'failed'));
}
async function step(name, body) {
  stage = name;
  const start = performance.now();
  await body();
  report.cases.push({ name, status: 'PASS', milliseconds: Math.round(performance.now() - start) });
}
async function command(value) {
  const number = ++commandNumber;
  const temporary = join(home, `browser-command-${number}.tmp`);
  await writeFile(temporary, JSON.stringify({ command: value }), { flag: 'wx', mode: 0o600 });
  await rename(temporary, join(home, `browser-command-${number}.json`));
  const deadline = performance.now() + 12_000;
  for (;;) {
    try {
      const response = JSON.parse(await readFile(join(home, `browser-response-${number}.json`), 'utf8'));
      assert.equal(response.state, 'verified');
      return response;
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
    assert.ok(performance.now() < deadline, 'restart response deadline');
    await sleep(20);
  }
}
try {
  browser = await ({ chromium, firefox, webkit }[engine]).launch({ headless: true });
  report.version = browser.version();
  report.executable = ({ chromium, firefox, webkit }[engine]).executablePath();
  context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();
  primaryPage = page;
  observe(page);
  // The default headless Chromium launch may use headless_shell rather than
  // executablePath(). Bind the actual native files in this private PID namespace.
  report.launchedExecutables = [];
  for (const pid of await readdir('/proc')) {
    if (!/^\d+$/.test(pid)) continue;
    let path;
    try { path = await readlink(`/proc/${pid}/exe`); }
    catch (error) { if (['ENOENT', 'EACCES'].includes(error.code)) continue; throw error; }
    if (!path.startsWith('/ms-playwright/') || report.launchedExecutables.some(entry => entry.path === path)) continue;
    const hash = createHash('sha256');
    for await (const chunk of createReadStream(path)) hash.update(chunk);
    report.launchedExecutables.push({ path, sha256: hash.digest('hex') });
  }
  assert.ok(report.launchedExecutables.length > 0, 'no actual browser executable observed');
  const errors = [];
  const external = [];
  page.on('pageerror', () => errors.push('pageerror')); // Never persist arbitrary exception text.
  page.on('request', request => { if (new URL(request.url()).origin !== origin) external.push('external-request'); });
  let logins = 0;
  page.on('request', request => { if (new URL(request.url()).pathname === '/management/v1/sessions' && request.method() === 'POST') logins++; });
  const username = page.getByLabel('Username', { exact: true });
  const secret = page.getByLabel('Password', { exact: true });
  const signIn = page.getByRole('button', { name: 'Sign in', exact: true });
  async function login(value) {
    await username.fill('alice');
    await secret.fill(value);
    await signIn.click();
  }
  await step('initial-load-and-keyboard', async () => {
    const response = await page.goto(origin + base);
    assert.equal(response.status(), 200);
    assert.match(response.headers()['content-security-policy'], /frame-ancestors 'none'/);
    assert.equal(response.headers()['referrer-policy'], 'same-origin');
    await expect(page.locator('meta[name="referrer"]')).toHaveAttribute('content', 'same-origin');
    await expect(signIn).toBeEnabled();
    await expect(page.locator('#startup-recovery')).toBeHidden();
    await page.keyboard.press('Tab');
    await expect(page.getByRole('link', { name: 'Skip to session' })).toBeFocused();
    await username.focus();
    await page.keyboard.press('Tab');
    await expect(secret).toBeFocused();
    await page.setViewportSize({ width: 375, height: 812 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: join(home, 'signed-out-mobile.png'), fullPage: true });
    await page.setViewportSize({ width: 1280, height: 800 });
  });
  await step('invalid-and-wrong-password', async () => {
    await login('x'.repeat(11));
    await expect(page.locator('#session-notice')).toContainText('at least 12');
    assert.equal(logins, 0);
    await login('Wrong independent password 2026');
    await expect(page.locator('#session-notice')).toContainText('Sign-in was refused');
    await expect(secret).toHaveValue('');
    assert.equal(logins, 1);
  });
  await step('login-cookie-boundaries', async () => {
    await login(password);
    await expect(page.getByRole('heading', { name: 'Your session', exact: true })).toBeVisible();
    await expect(page.locator('.session-facts')).toContainText(principal);
    await expect(page.locator('.permissions li')).toHaveText([
      'audit.read', 'budgets.change', 'data.read', 'policy.change',
      'principals.manage', 'roles.control', 'status.read'
    ]);
    const cookies = await context.cookies(origin);
    const session = cookies.find(cookie => cookie.name === 'BigBrotrLocalSession');
    const csrf = cookies.find(cookie => cookie.name === 'BigBrotrLocalCsrf');
    assert.ok(session && csrf);
    assert.equal(session.httpOnly, true);
    assert.equal(csrf.httpOnly, false);
    for (const cookie of [session, csrf]) {
      assert.equal(cookie.sameSite, 'Strict');
      assert.equal(cookie.secure, false); // Explicit trusted-host HTTP profile.
      assert.equal(cookie.path, '/');
      assert.equal(cookie.domain, '127.0.0.1');
    }
    const visible = await page.evaluate(() => document.cookie);
    assert.equal(visible.includes('BigBrotrLocalSession='), false);
    assert.equal(visible.includes('BigBrotrLocalCsrf='), true);
  });
  await step('explicit-csrf-and-foreign-origin', async () => {
    await command('observe-boundary');
    assert.equal(await page.evaluate(async () => (await fetch('/management/v1/sessions/current', { method: 'DELETE' })).status), 403);
    let hostCookies = false;
    foreign = createServer((request, response) => {
      hostCookies = /BigBrotrLocalSession=/.test(request.headers.cookie ?? '') && /BigBrotrLocalCsrf=/.test(request.headers.cookie ?? '');
      response.writeHead(200, { 'Content-Type': 'text/html', 'Cache-Control': 'no-store', 'Referrer-Policy': 'strict-origin-when-cross-origin' });
      response.end('<!doctype html><title>Isolated second-port control</title><form method="POST" enctype="text/plain" action="http://127.0.0.1:8081/management/v1/sessions"><input name="probe" value="boundary"><button type="submit">Attempt command</button></form>');
    });
    await new Promise(resolve => foreign.listen(0, '127.0.0.1', resolve));
    const other = await context.newPage();
    observe(other);
    let wire;
    if (engine === 'chromium') {
      // CORS errors can omit CDP responseReceived (and Playwright Response).
      // Observe only the selected wire status, never raw headers or body.
      const cdp = await context.newCDPSession(other);
      await cdp.send('Network.enable');
      wire = new WireStatuses();
      report.foreignWireStatuses = wire.statuses;
      cdp.on('Network.requestWillBeSent', event => {
        wire.request(event.requestId, event.request.url === origin + '/management/v1/sessions' && event.request.method === 'POST');
      });
      cdp.on('Network.responseReceivedExtraInfo', event => {
        wire.response(event.requestId, event.statusCode);
      });
    }
    await other.goto(`http://127.0.0.1:${foreign.address().port}/`);
    assert.equal(hostCookies, true, 'host cookies are not port-isolated');
    await other.evaluate(async url => { await fetch(url, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'text/plain' }, body: '{}' }).catch(() => {}); }, origin + '/management/v1/sessions');
    if (engine === 'chromium') await expect.poll(() => report.foreignWireStatuses).toEqual([403]);
    // A top-level form navigation exposes the actual refused HTTP response in
    // every engine. A rejected CORS fetch alone cannot establish its wire status.
    const denied = other.waitForResponse(response => response.url() === origin + '/management/v1/sessions' && response.request().isNavigationRequest(), { timeout: 5000 });
    await other.getByRole('button', { name: 'Attempt command', exact: true }).click();
    const refused = await denied;
    assert.equal(refused.status(), 403);
    assert.equal((await refused.request().allHeaders()).origin, `http://127.0.0.1:${foreign.address().port}`);
    if (wire) {
      report.foreignWireOverflow = wire.overflow;
      assert.equal(wire.overflow, false);
    }
    await other.close();
    await new Promise(resolve => foreign.close(resolve));
    foreign = undefined;
    await command('verify-boundary');
  });
  await step('same-cookie-process-restart', async () => {
    await command('restart');
    await page.reload();
    await expect(page.getByRole('heading', { name: 'Your session', exact: true })).toBeVisible();
    await expect(page.locator('.session-facts')).toContainText(principal);
    assert.equal(logins, 2);
  });
  await step('cross-tab-logout-and-forget', async () => {
    const other = await context.newPage();
    await other.goto(origin + base);
    await expect(other.getByRole('heading', { name: 'Your session', exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Sign out', exact: true }).click();
    await expect(page.locator('#session-notice')).toContainText('You have signed out');
    await expect(other.locator('#session-notice')).toContainText('another tab');
    await expect(other.locator('.session-facts')).toHaveCount(0);
    await expect(other.locator('.permissions')).toHaveCount(0);
    await expect(page.locator('.session-facts')).toHaveCount(0);
    await expect(page.locator('.permissions')).toHaveCount(0);
    assert.equal((await context.cookies(origin)).some(cookie => cookie.name.startsWith('BigBrotrLocal')), false);
    await other.close();
  });
  await step('new-login-reload-and-logout', async () => {
    await login(password);
    await expect(page.getByRole('heading', { name: 'Your session', exact: true })).toBeVisible();
    await page.reload();
    await expect(page.locator('.session-facts')).toContainText(principal);
    await page.getByRole('button', { name: 'Sign out', exact: true }).click();
    await expect(page.locator('#session-notice')).toContainText('You have signed out');
    assert.equal(logins, 3);
    await page.screenshot({ path: join(home, 'signed-out-desktop.png'), fullPage: true });
    assert.deepEqual(errors, []);
    assert.deepEqual(external, []);
  });
  await step('missing-bootstrap-recovery', async () => {
    const fault = await command('remove-bootstrap');
    assert.match(fault.module, /^_app\/immutable\/entry\/start\.[A-Za-z0-9_-]+\.js$/);
    const bootstrapUrl = origin + base + fault.module;
    const missing = await context.newPage();
    primaryPage = missing;
    let actualMissing = 0;
    let restoring = false;
    const diagnostic = report.missingBootstrap = { interception: false, noStore: true, script404s: 0, early200s: 0, restored200s: 0, events: [], navigations: 0 };
    const record = (request, event, status) => {
      if (diagnostic.events.length >= 128) { diagnostic.overflow = true; return; }
      const path = new URL(request.url()).pathname;
      const kind = path.includes('/entry/start.') ? 'start' : path === '/__absent-panel-start.js' ? 'absent' : request.isNavigationRequest() ? 'document' : 'other';
      diagnostic.events.push({ event, kind, type: request.resourceType(), status });
    };
    missing.on('request', request => record(request, 'request'));
    missing.on('requestfailed', request => record(request, 'failed'));
    missing.on('response', response => {
      record(response.request(), 'response', response.status());
      if (response.url() === bootstrapUrl) {
        if (response.status() === 404) actualMissing++;
        if (response.status() === 200) {
          if (restoring) diagnostic.restored200s++;
          else diagnostic.early200s++;
        }
        diagnostic.noStore &&= response.headers()['cache-control'] === 'no-store';
      }
      diagnostic.script404s = actualMissing;
    });
    missing.on('framenavigated', frame => { if (frame === missing.mainFrame()) diagnostic.navigations++; });
    await missing.goto(origin + base);
    await expect(missing.locator('#startup-recovery')).toBeVisible();
    await expect(missing.getByRole('link', { name: 'Reload the panel' })).toBeVisible();
    // Firefox may separately request preload and import. Their multiplicity
    // is not an invariant: require a real 404 for the exact bootstrap module.
    await expect.poll(() => actualMissing).toBeGreaterThan(0);
    await missing.screenshot({ path: join(home, 'missing-bootstrap.png'), fullPage: true });
    assert.equal((await command('restore-bootstrap')).module, fault.module);
    assert.equal(diagnostic.early200s, 0);
    restoring = true;
    diagnostic.beforeClick = diagnostic.navigations;
    await missing.getByRole('link', { name: 'Reload the panel' }).click();
    try {
      await expect(missing.getByRole('button', { name: 'Sign in', exact: true })).toBeEnabled();
    } catch (error) {
      diagnostic.afterClick = diagnostic.navigations;
      try { await missing.screenshot({ path: join(home, 'failed-reload-link.png'), fullPage: true, timeout: 3000 }); }
      catch { diagnostic.failedLinkScreenshot = 'unavailable'; }
      // Discriminate link navigation from a retained module failure. This
      // additional observation must never turn the original failed journey green.
      try {
        await missing.reload();
        await expect(missing.getByRole('button', { name: 'Sign in', exact: true })).toBeEnabled();
        diagnostic.explicitReload = 'recovered';
      } catch { diagnostic.explicitReload = 'failed'; }
      throw error;
    }
    diagnostic.afterClick = diagnostic.navigations;
    await expect.poll(() => diagnostic.restored200s).toBeGreaterThan(0);
    await expect(missing.locator('#startup-recovery')).toBeHidden();
    assert.equal(diagnostic.noStore, true);
    assert.equal(diagnostic.overflow, undefined);
    await missing.close();
  });
  assert.equal(report.observationLimit, undefined, 'browser observation limit');
  report.status = 'PASS';
} catch (error) {
  report.status = 'FAIL';
  // Call logs can contain filled credentials or headers. Keep only the stage
  // and exception class; SQL, wire data and arbitrary error text stay private.
  report.failure = { stage, kind: error?.constructor?.name ?? 'UnknownError' };
  if (Number.isFinite(error?.matcherResult?.actual)) report.failure.actualNumber = error.matcherResult.actual;
  const location = String(error?.stack).match(/browser\.mjs:(\d+):(\d+)/);
  if (location) report.failure.location = { line: Number(location[1]), column: Number(location[2]) };
  if (primaryPage) {
    try {
      const notice = await primaryPage.locator('#session-notice').textContent({ timeout: 1000 });
      report.failure.notice = ['at least 12', 'Sign-in was refused', 'browser request was refused', 'could not be confirmed'].find(text => notice?.includes(text)) ?? 'other';
    } catch { report.failure.inspection = 'unavailable'; }
    try { await primaryPage.screenshot({ path: join(home, 'failure.png'), fullPage: true, timeout: 3000 }); }
    catch { report.failure.screenshot = 'unavailable'; }
  }
} finally {
  report.teardown = [];
  for (const [name, resource] of [['context', context], ['browser', browser]]) {
    if (!resource) continue;
    try {
      await resource.close();
      report.teardown.push({ name, status: 'PASS' });
    } catch (error) {
      report.status = 'FAIL';
      report.teardown.push({ name, status: 'FAIL', kind: error?.constructor?.name ?? 'UnknownError' });
    }
  }
  if (foreign) await new Promise(resolve => foreign.close(resolve));
  await Promise.allSettled(observationWork);
  report.http = observations;
  await writeFile(join(home, 'browser-report.json'), JSON.stringify(report, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  process.exitCode = report.status === 'PASS' ? 0 : 1;
}
