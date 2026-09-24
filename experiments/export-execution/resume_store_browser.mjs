// Native IndexedDB against the actual TS module. This is not installed HTTP/UI evidence.
import assert from 'node:assert/strict';
import { mkdir, writeFile, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { resolve } from 'node:path';
import { createRequire } from 'node:module';
import { chromium, firefox, webkit } from '@playwright/test';
const root = resolve(import.meta.dirname, '../..');
const require = createRequire(resolve(root, 'apps/admin/package.json'));
const { createServer } = await import(require.resolve('vite'));
const [output, engine = 'chromium'] = process.argv.slice(2);
assert.ok(output && ['chromium', 'firefox', 'webkit'].includes(engine));
await mkdir(output, { recursive: true });
const report = { format: 'bigbrotr-browser-resume-store/1', status: 'INCOMPLETE', engine,
  platform: process.platform, arch: process.arch, node: process.version, cases: [], inputs: [] };
for (const path of ['apps/admin/src/lib/resume-store.ts', 'packages/api-client/src/management.ts',
  'packages/api-client/src/generated/validators.js', 'pnpm-lock.yaml', 'experiments/export-execution/resume_store_browser.mjs']) {
  const bytes = await readFile(resolve(root, path));
  report.inputs.push({ path, bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') });
}
let server, browser, context, stage = 'startup';
async function step(name, body) {
  stage = name; const started = performance.now();
  let timer;
  try {
    const evidence = await Promise.race([body(), new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('probe deadline')), 20_000); })]);
    report.cases.push({ name, status: 'PASS', milliseconds: performance.now() - started, evidence });
  } finally { clearTimeout(timer); }
}
try {
  server = await createServer({ root: resolve(root, 'apps/admin'), configFile: false, appType: 'custom',
    server: { host: '127.0.0.1', port: 0 },
    plugins: [{ name: 'isolated-store-probe', configureServer(server) {
      server.middlewares.use((req, res, next) => {
        if (req.url !== '/probe') { next(); return; }
        res.setHeader('Content-Type', 'text/html'); res.end('<!doctype html><title>Native store probe</title>');
      });
    } }]
  });
  await server.listen();
  const url = `http://127.0.0.1:${server.httpServer.address().port}/probe`;
  browser = await ({ chromium, firefox, webkit }[engine]).launch({ headless: true });
  report.browserVersion = browser.version();
  context = await browser.newContext();
  const page = await context.newPage(); await page.goto(url);
  await page.evaluate(async () => {
    const module = await import('/src/lib/resume-store.ts');
    const DATABASE = 'bigbrotr-private-commands', STORE = 'export-resume';
    const check = (condition, why) => { if (!condition) throw new Error(why); };
    const open = () => new Promise((resolve, reject) => {
      const request = indexedDB.open(DATABASE, 1);
      request.onupgradeneeded = () => request.result.createObjectStore(STORE, { keyPath: 'principal_id' });
      request.onsuccess = () => resolve(request.result); request.onerror = () => reject(request.error);
    });
    const record = (number = 1) => ({ format: 'bigbrotr-browser-resume/1', principal_id: number.toString(16).padStart(32, '0'),
      job_id: '02'.repeat(16), key: '03'.repeat(16), outcome: 'pending',
      command: { expected_job_revision: '7', expected_config_revision: '9007199254740993', expected_policy_revision: '2' } });
    // A known refusal can reject the caller before the native abort event. An
    // independent queued transaction observes that native work has really ended.
    const drain = async () => {
      const db = await open();
      try { await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE); tx.objectStore(STORE).count();
        tx.oncomplete = resolve; tx.onabort = () => reject(tx.error);
      }); } finally { db.close(); }
    };
    globalThis.probe = { module, DATABASE, STORE, check, open, record, drain };
  });
  await step('blocked-open-does-not-accumulate-native-requests', () => page.evaluate(async () => {
    const { module, DATABASE, check, open, record } = globalThis.probe;
    const held = await open();
    const deletion = indexedDB.deleteDatabase(DATABASE);
    const deleted = new Promise((resolve, reject) => { deletion.onsuccess = resolve; deletion.onerror = () => reject(deletion.error); });
    await new Promise(resolve => { deletion.onblocked = resolve; });
    let opens = 0;
    const factory = { open: (...args) => { opens++; return indexedDB.open(...args); } };
    const store = new module.BrowserResumeStore(factory);
    const kinds = [], times = [];
    try {
      for (let attempt = 0; attempt < 3; attempt++) {
        const start = performance.now();
        try { await new module.BrowserResumeStore(factory).load(record().principal_id); kinds.push('unexpected-success'); }
        catch (error) { kinds.push(error.kind); }
        times.push(performance.now() - start);
      }
    } finally { held.close(); await deleted; }
    // Allow the original late open to close. Reopening from a separate factory
    // is the independent observation that no transaction wrote a record.
    const observed = await open();
    const rows = await new Promise((resolve, reject) => {
      const tx = observed.transaction('export-resume'), request = tx.objectStore('export-resume').getAll();
      tx.oncomplete = () => resolve(request.result); tx.onabort = () => reject(tx.error);
    }); observed.close();
    globalThis.probe.blockedEvidence = { opens, kinds, times, rows };
    check(opens === 1, `native opens accumulated: ${opens}`);
    check(kinds.every(kind => kind === 'unavailable'), 'all blocked attempts must refuse');
    check(times.every(time => time < 4000), 'local wait exceeded allowance');
    check(rows.length === 0, 'blocked reads must not write');
    const recovered = await store.load(record().principal_id);
    check(recovered === undefined, 'late open gate must release for recovery');
    return { opensBeforeRecovery: opens - 1, kinds, times, recovered: true };
  }));
  await step('request-success-followed-by-transaction-abort-is-not-persistence', () => page.evaluate(async () => {
    const { module, record, check, drain } = globalThis.probe;
    const store = new module.BrowserResumeStore();
    const original = IDBObjectStore.prototype.put;
    let succeeded = 0, kind;
    IDBObjectStore.prototype.put = function (...args) {
      const request = original.apply(this, args), tx = this.transaction;
      request.addEventListener('success', () => { succeeded++; tx.abort(); }, { once: true });
      return request;
    };
    try { await store.reserve(record()); }
    catch (error) { kind = error.kind; }
    finally { IDBObjectStore.prototype.put = original; }
    check(succeeded === 1 && kind === 'unavailable', 'request success must not resolve an aborted transaction');
    await drain();
    const actual = await store.load(record().principal_id);
    check(actual === undefined, 'aborted reservation persisted');
    return { requestSuccessEvents: succeeded, rejected: kind, stored: false };
  }));
  await step('two-tabs-reserve-one-original-identity-and-reload', async () => {
    const other = await context.newPage(); await other.goto(url);
    const first = await page.evaluate(() => globalThis.probe.record());
    const second = { ...first, key: '04'.repeat(16), job_id: '05'.repeat(16) };
    const reserve = value => import('/src/lib/resume-store.ts').then(({ BrowserResumeStore }) => new BrowserResumeStore().reserve(value));
    const results = await Promise.all([page.evaluate(reserve, first), other.evaluate(reserve, second)]);
    assert.deepEqual(results[0], results[1]); assert.ok([first.key, second.key].includes(results[0].key));
    await other.reload();
    const loaded = await other.evaluate(async principal => {
      const { BrowserResumeStore } = await import('/src/lib/resume-store.ts');
      return new BrowserResumeStore().load(principal);
    }, first.principal_id);
    assert.deepEqual(loaded, results[0]); await other.close(); return { original: loaded };
  });
  await step('identity-cas-and-pending-forget-refusal', () => page.evaluate(async () => {
    const { module, record, check, drain } = globalThis.probe; const store = new module.BrowserResumeStore();
    const original = await store.load(record().principal_id);
    let refused;
    try { await store.forget(original.principal_id, original.key); } catch (error) { refused = error.kind; }
    check(refused === 'pending', 'pending request forgotten');
    await drain();
    check(!await store.settle({ ...original, key: '09'.repeat(16) }, 'accepted'), 'wrong key settled');
    check(!await store.settle({ ...original, command: { ...original.command, expected_job_revision: '8' } }, 'accepted'), 'wrong body settled');
    check(await store.settle(original, 'accepted'), 'original did not settle');
    const same = await store.reserve({ ...original, key: '0a'.repeat(16) });
    check(same.key === original.key && same.outcome === 'accepted', 'accepted same body acquired another key');
    const fresh = await store.reserve({ ...original, key: '0b'.repeat(16), command: { ...original.command, expected_job_revision: '9' } });
    check(fresh.key === '0b'.repeat(16) && fresh.outcome === 'pending', 'new paused revision could not replace resolved request');
    check(!await store.forget(original.principal_id, original.key), 'old key erased replacement');
    check(await store.settle(fresh, 'refused'), 'refusal not saved');
    check(await store.forget(fresh.principal_id, fresh.key), 'resolved record not forgotten');
    check(await store.load(fresh.principal_id) === undefined, 'record survived deletion');
    return { pendingRefusal: refused, staleOperationsChangedNothing: true, resolvedForget: true };
  }));
  await step('capacity-and-account-local-corruption', () => page.evaluate(async () => {
    const { module, record, check, open, STORE, drain } = globalThis.probe; const store = new module.BrowserResumeStore();
    for (let n = 1; n <= 32; n++) await store.reserve(record(n));
    let capacity;
    try { await store.reserve(record(33)); } catch (error) { capacity = error.kind; }
    check(capacity === 'full', '33rd account accepted');
    await drain();
    check((await store.load(record(2).principal_id)).key === record(2).key, 'full store hides original request');
    const db = await open();
    await new Promise((resolve, reject) => {
      const tx = db.transaction(STORE, 'readwrite');
      tx.objectStore(STORE).put({ ...record(), credential: 'invalid-fixture-marker' });
      tx.oncomplete = resolve; tx.onabort = () => reject(tx.error);
    }); db.close();
    const refused = [];
    for (const action of [() => store.load(record().principal_id), () => store.reserve(record())]) {
      try { await action(); refused.push('unexpected-success'); } catch (error) { refused.push(error.kind); }
      await drain();
    }
    check(refused.every(kind => kind === 'invalid'), 'malformed current account accepted');
    check((await store.load(record(2).principal_id)).principal_id === record(2).principal_id, 'another account was exposed or made unreadable');
    return { capacity, admittedAccounts: 32, malformedCurrentAccount: refused, otherAccountPreserved: true };
  }));
  report.status = 'PASS';
} catch (error) {
  report.status = 'FAIL'; report.failure = { stage, message: String(error.message), stack: String(error.stack) };
  if (context) {
    try { report.blockedEvidence = await context.pages()[0].evaluate(() => globalThis.probe?.blockedEvidence); } catch { /* Browser itself failed. */ }
  }
} finally {
  for (const resource of [context, browser, server]) if (resource) try { await resource.close(); }
  catch { report.status = 'FAIL'; report.teardown = 'failed'; }
  await writeFile(resolve(output, 'report.json'), JSON.stringify(report, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  console.log(JSON.stringify({ status: report.status, stage, cases: report.cases.length, failure: report.failure?.message }));
  process.exitCode = report.status === 'PASS' ? 0 : 1;
}
