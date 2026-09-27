// Actual typed client, then the production panel against installed Rust/PG.
// No mocked/intercepted responses; continuation and session bytes stay in RAM.
import assert from 'node:assert/strict';
import { readFile, writeFile, rename, readdir, readlink } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { createHash } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { chromium, firefox, webkit, expect } from '@playwright/test';
import { inspectStorage, readResumeStorage } from './storage_oracle.mjs';
import { createManagementClient, ManagementFailure } from '../../packages/api-client/src/management.ts';

const [engine, mount] = process.argv.slice(2);
assert(['chromium','firefox','webkit'].includes(engine) && ['root','control'].includes(mount));
assert.equal(process.version, 'v26.4.0');
const origin = 'http://127.0.0.1:18080', url = origin+(mount === 'root' ? '/' : '/control/');
const { expected } = JSON.parse(await readFile('/case/input.json', 'utf8'));
assert.equal(expected.length, 6);
const report = { status: 'INCOMPLETE', engine, mount, node: process.version, cases: [], nodeHttp: [], http: [], requests: [], pages: [] };
const cookies = new Map(), errors = [], pending = new Set(), ids = new WeakMap();
let browser, context, page, stage = 'typed-client', commandIndex = 0, journeyComplete = false;
async function save() {
  report.stage = stage;
  await writeFile('/case/browser-report.tmp',JSON.stringify(report,null,2)+'\n',{mode:0o600});
  await rename('/case/browser-report.tmp','/case/browser-report.json');
}
const jar = () => [...cookies].map(([key,value]) => `${key}=${value}`).join('; ');
const api = createManagementClient({ cookie: jar, fetch: async (path, init) => {
  const parsed = new URL(path, origin);
  assert.equal(parsed.origin, origin);
  assert(['/management/v1/sessions','/management/v1/sessions/current','/management/v1/jobs'].includes(parsed.pathname));
  assert(report.nodeHttp.length < 24);
  const headers = new Headers(init.headers); headers.set('Origin', origin);
  if (cookies.size) headers.set('Cookie', jar());
  const entry = { path: parsed.pathname, method: init.method, status: null };
  report.nodeHttp.push(entry);
  const response = await fetch(origin+path, { ...init, headers });
  entry.status = response.status;
  assert.equal(response.headers.get('cache-control'), 'no-store');
  for (const header of response.headers.getSetCookie()) {
    const pair = header.split(';',1)[0], position = pair.indexOf('=');
    const key = pair.slice(0,position), value = pair.slice(position+1);
    assert(['BigBrotrLocalSession','BigBrotrLocalCsrf'].includes(key));
    assert.match(value,/^(?:[0-9a-f]{64})?$/);
    if (value) cookies.set(key,value); else cookies.delete(key);
  }
  return response;
} });
async function command(name) {
  const index = ++commandIndex, temporary = `/case/command-${index}.tmp`;
  await writeFile(temporary, JSON.stringify({ name }), { flag: 'wx', mode: 0o600 });
  await rename(temporary, `/case/command-${index}.json`);
  const deadline = performance.now()+12000;
  for (;;) {
    try { const value = JSON.parse(await readFile(`/case/response-${index}.json`,'utf8')); assert.equal(value.state,'verified'); return; }
    catch(error) { if (error.code !== 'ENOENT') throw error; }
    assert(performance.now() < deadline, 'host command deadline'); await sleep(10);
  }
}
async function step(name, body) {
  stage = name; await save(); const started = performance.now(); await body();
  report.cases.push({ name, status:'PASS', milliseconds: Math.round(performance.now()-started) });
  await save();
}
function checkPage(name, value, rows, more) {
  assert.equal(value.format, 'bigbrotr-job-list/1'); assert.equal(value.authority,'observation-only');
  assert.equal(value.effective_page_size, 2); assert.equal(value.next_cursor !== null, more);
  assert.deepEqual(value.items.map(({ observed_at_us, ...row }) => {
    assert.equal(observed_at_us,value.observed_at_us); return row;
  }), rows);
  report.pages.push({ name, observed_at_us: value.observed_at_us, items: value.items, has_next: more });
}
function observe(p) {
  p.on('pageerror', () => errors.push('pageerror'));
  p.on('request', request => {
    const parsed = new URL(request.url()); if (parsed.origin !== origin) errors.push('external-request');
    if (!parsed.pathname.startsWith('/management/')) return;
    if (report.requests.length >= 128) { report.overflow = true; return; }
    const id = report.requests.length; ids.set(request,id);
    report.requests.push({ path:parsed.pathname, method:request.method(), continuation:parsed.searchParams.has('cursor') });
    if (parsed.pathname === '/management/v1/jobs') pending.add(id);
  });
  for (const kind of ['requestfinished','requestfailed']) p.on(kind, request => {
    const id = ids.get(request); if (id !== undefined) { report.requests[id].terminal = kind; pending.delete(id); }
  });
  p.on('response', response => {
    const path = new URL(response.url()).pathname; if (!path.startsWith('/management/')) return;
    if (report.http.length >= 128) { report.overflow = true; return; }
    report.http.push({ path, status:response.status(), noStore:response.headers()['cache-control'] === 'no-store' });
  });
}
async function login(who = 'alice') {
  await page.getByLabel('Username',{exact:true}).fill(who);
  await page.getByLabel('Password',{exact:true}).fill('Independent owner password 2026');
  await page.getByRole('button',{name:'Sign in',exact:true}).click();
  await expect(page.getByRole('heading',{name:'Your session',exact:true})).toBeVisible();
}
async function logout() {
  await page.getByRole('button',{name:'Sign out',exact:true}).click();
  await expect(page.locator('#session-notice')).toContainText('signed out');
  await expect(page.locator('.job-sheet')).toHaveCount(0);
}
const pageRequests = () => report.requests.filter(r => r.path === '/management/v1/jobs').length;
async function browse(name, rows, more, next = false) {
  const prior = pageRequests();
  const received = page.waitForResponse(r => new URL(r.url()).pathname === '/management/v1/jobs');
  await page.getByRole('button',{name:next ? 'Next page' : 'List my exports',exact:true}).click();
  const response = await received; assert.equal(response.status(),200);
  checkPage(name, await response.json(), rows, more);
  const visible = page.locator('.job-list .job-open code');
  await expect(visible).toHaveText(rows.map(row => row.job_id));
  if (more) await expect(page.getByRole('button',{name:'Next page',exact:true})).toBeEnabled();
  else await expect(page.getByRole('button',{name:'Next page',exact:true})).toBeDisabled();
  await expect.poll(() => pending.size).toBe(0);
  await sleep(150); assert.equal(pageRequests(), prior+1);
}
try {
  await step('actual-typed-client-pages-and-refusals',async () => {
    await api.login({ username:'alice', password:'Independent owner password 2026' });
    await command('node-before');
    const first = await api.listJobs({ page_size:2 }); checkPage('node-first',first,expected.slice(0,2),true);
    const second = await api.listJobs({ page_size:2,cursor:first.next_cursor }); checkPage('node-second',second,expected.slice(2,4),true);
    checkPage('node-last',await api.listJobs({ page_size:2,cursor:second.next_cursor }),expected.slice(4),false);
    const empty = await api.listJobs({ page_size:2,state:'succeeded' }); checkPage('node-empty',empty,[],true);
    const again = await api.listJobs({ page_size:2,state:'succeeded',cursor:empty.next_cursor }); checkPage('node-empty-next',again,[],true);
    checkPage('node-match',await api.listJobs({ page_size:2,state:'succeeded',cursor:again.next_cursor }),expected.slice(5),false);
    for (const [query,code] of [[{page_size:2,cursor:'abc'},'invalid_cursor'],[{page_size:2,state:'accepted',cursor:first.next_cursor},'cursor_incompatible']]) {
      const calls = report.nodeHttp.length;
      await assert.rejects(api.listJobs(query), error => error instanceof ManagementFailure && error.api?.code === code);
      assert.equal(report.nodeHttp.length,calls+1);
    }
    await command('node-after'); await api.logout(); assert.equal(cookies.size,0);
  });
  stage = 'launch'; report.launchTimeoutMs = 30000; await save();
  browser = await ({chromium,firefox,webkit}[engine]).launch({headless:true,timeout:report.launchTimeoutMs});
  report.version = browser.version(); report.executables = [];
  for (const pid of await readdir('/proc')) {
    if (!/^\d+$/.test(pid)) continue;
    let path; try { path = await readlink(`/proc/${pid}/exe`); } catch(error) { if (['ENOENT','EACCES'].includes(error.code)) continue; throw error; }
    if (!path.startsWith('/ms-playwright/') || report.executables.some(item => item.path === path)) continue;
    const hash = createHash('sha256'); for await (const block of createReadStream(path)) hash.update(block);
    report.executables.push({ path,sha256:hash.digest('hex') });
  }
  assert(report.executables.length > 0);
  context = await browser.newContext({viewport:{width:1280,height:900}}); page = await context.newPage(); observe(page);
  await step('production-page-explicit-discovery-and-detail',async () => {
    const response = await page.goto(url); assert.equal(response.status(),200);
    assert.match(response.headers()['content-security-policy'],/frame-ancestors 'none'/);
    await login(); await expect(page.getByRole('heading',{name:'Your exports',exact:true})).toBeVisible();
    assert.equal(pageRequests(),0); await command('browser-before');
    await page.getByLabel('Jobs per page',{exact:true}).fill('2');
    await browse('browser-first',expected.slice(0,2),true);
    const detail = page.waitForResponse(r => r.url() === origin+'/management/v1/jobs/'+expected[0].job_id);
    await page.getByRole('button',{name:'Inspect export '+expected[0].job_id,exact:true}).click();
    const value = await (await detail).json(); const { observed_at_us,...row } = value;
    assert.deepEqual(row,expected[0]); assert(BigInt(observed_at_us) > 0n);
    await expect(page.locator('.job-observation')).toContainText(expected[0].job_id);
    await expect(page.getByRole('heading',{name:'Inspect your job',exact:true})).toBeFocused();
    await browse('browser-second',expected.slice(2,4),true,true);
    await browse('browser-last',expected.slice(4),false,true);
  });
  await step('empty-pages-date-filters-and-no-fetch-all',async () => {
    const calls = pageRequests();
    await page.getByLabel('Job state',{exact:true}).selectOption('succeeded');
    await expect(page.locator('.job-rows')).toHaveCount(0); assert.equal(pageRequests(),calls);
    await browse('browser-empty',[],true); await browse('browser-empty-next',[],true,true);
    await browse('browser-match',expected.slice(5),false,true);
    await page.getByLabel('Job state',{exact:true}).selectOption('');
    await page.getByLabel('Created on or after (UTC)',{exact:true}).fill('2000-01-01');
    await page.getByLabel('Created before (UTC)',{exact:true}).fill('2000-01-02');
    await browse('browser-date-empty',[],false);
    await page.getByLabel('Created before (UTC)',{exact:true}).fill('');
    assert(expected.every(row => BigInt(row.created_at_us) < 4070908800000000n));
    await page.getByLabel('Created on or after (UTC)',{exact:true}).fill('2099-01-01');
    await browse('browser-lower-bound-empty',[],false);
    await page.getByLabel('Created on or after (UTC)',{exact:true}).fill('');
    await browse('browser-fresh',expected.slice(0,2),true); await command('browser-after');
    await page.setViewportSize({width:375,height:812});
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),true);
    await page.locator('.job-list').screenshot({path:'/case/job-list-mobile.png'});
    await page.setViewportSize({width:1280,height:900});
  });
  await step('continuation-survives-control-restart-and-reload-forgets-cursor',async () => {
    await command('restart'); await browse('browser-after-restart',expected.slice(2,4),true,true);
    const count = pageRequests(); await page.reload();
    await expect(page.getByRole('heading',{name:'Your exports',exact:true})).toBeVisible();
    await expect(page.locator('.job-rows')).toHaveCount(0); assert.equal(pageRequests(),count);
    await page.getByLabel('Jobs per page',{exact:true}).fill('2');
    await browse('browser-reopened',expected.slice(0,2),true);
    await logout();
  });
  await step('other-account-sees-no-prior-page',async () => {
    await login('bob'); await expect(page.locator('.job-rows')).toHaveCount(0);
    await page.getByLabel('Jobs per page',{exact:true}).fill('2');
    await browse('browser-bob',[],false); await logout();
  });
  await step('logout-cancels-actual-blocked-page',async () => {
    await login(); await page.getByLabel('Jobs per page',{exact:true}).fill('2');
    await browse('browser-before-hold',expected.slice(0,2),true);
    await command('hold-account'); const requestStart = report.requests.length;
    await page.getByRole('button',{name:'Next page',exact:true}).click();
    await expect(page.locator('.job-list .progress')).toContainText('Loading');
    await command('verify-blocked'); await page.getByRole('button',{name:'Sign out',exact:true}).click();
    await expect(page.locator('.job-sheet')).toHaveCount(0); await command('release-account');
    await expect(page.locator('#session-notice')).toContainText('signed out');
    await expect.poll(() => pending.size).toBe(0);
    const held = report.requests.slice(requestStart).filter(entry => entry.path === '/management/v1/jobs');
    assert.equal(held.length,1); assert.equal(held[0].continuation,true);
    assert.equal(held[0].terminal,'requestfailed');
    report.cancelledContinuation = { requestIndex:requestStart, terminal:held[0].terminal };
    await sleep(200);
    await expect(page.locator('.job-sheet')).toHaveCount(0);
  });
  await step('actual-account-revocation-clears-page',async () => {
    await login(); await page.getByLabel('Jobs per page',{exact:true}).fill('2');
    await browse('browser-before-revoke',expected.slice(0,2),true); await command('deactivate');
    const denied = page.waitForResponse(r => new URL(r.url()).pathname === '/management/v1/jobs');
    await page.getByRole('button',{name:'Next page',exact:true}).click(); assert.equal((await denied).status(),401);
    await expect(page.locator('.job-sheet')).toHaveCount(0); await expect(page.locator('.session-facts')).toHaveCount(0);
    report.storage = inspectStorage(await page.evaluate(() => ({local:Object.entries(localStorage),session:Object.entries(sessionStorage)})));
    assert.equal(report.storage.status,'verified');
    report.indexedDB = await readResumeStorage(page);
    assert.deepEqual(report.indexedDB,{status:'verified',records:[]});
  });
  await expect.poll(() => pending.size).toBe(0); assert.deepEqual(errors,[]);
  assert.equal(report.overflow,undefined); assert(report.http.every(item => item.noStore)); journeyComplete = true;
} catch(error) {
  report.status = 'FAIL'; report.failure = { stage,kind:error?.constructor?.name ?? 'UnknownError' };
  const location = String(error?.stack).match(/job_pages_browser\.mjs:(\d+):(\d+)/);
  if (location) report.failure.location = { line:+location[1],column:+location[2] };
  await save();
  if (page) try { await page.screenshot({path:'/case/failure.png',fullPage:true,timeout:2000}); } catch { report.failure.screenshot = 'unavailable'; }
} finally {
  cookies.clear();
  report.browserHandleAcquired = Boolean(browser); report.teardown = 'pending'; await save();
  for (const resource of [context,browser]) if (resource) try { await resource.close(); } catch { report.status = 'FAIL'; report.teardown = 'failed'; }
  if (report.teardown === 'pending') report.teardown = 'available-handles-closed';
  if (journeyComplete && report.teardown === 'available-handles-closed') report.status = 'PASS';
  await save();
  process.exitCode = report.status === 'PASS' ? 0 : 1;
}
