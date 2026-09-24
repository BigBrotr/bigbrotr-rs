// Production bundle, real Rust HTTP and PostgreSQL. No request interception.
import assert from 'node:assert/strict';
import { readFile, writeFile, rename, readdir, readlink } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { chromium, firefox, webkit, expect } from '@playwright/test';
const [engine, mount] = process.argv.slice(2);
assert.ok(['chromium','firefox','webkit'].includes(engine));
assert.ok(['root','control'].includes(mount));
const home='/case', origin='http://127.0.0.1:18080', url=origin+(mount==='root'?'/':'/control/');
const input=JSON.parse(await readFile(join(home,'input.json'),'utf8'));
const report={format:'bigbrotr-export-status-browser/1',status:'INCOMPLETE',engine,mount,cases:[],http:[],requests:[]};
let browser, context, page, stage='launch', commandIndex=0;
const errors=[], pendingJobs=new Set();
const requestIds=new WeakMap();
async function command(name) {
  const n=++commandIndex, tmp=join(home,`command-${n}.tmp`);
  await writeFile(tmp,JSON.stringify({name}),{flag:'wx',mode:0o600});
  await rename(tmp,join(home,`command-${n}.json`));
  const deadline=performance.now()+12000;
  for (;;) {
    try { const value=JSON.parse(await readFile(join(home,`response-${n}.json`),'utf8'));assert.equal(value.state,'verified');return value; }
    catch(error){if(error.code!=='ENOENT')throw error;}
    assert.ok(performance.now()<deadline,'host command deadline');await sleep(10);
  }
}
async function step(name, body){stage=name;const start=performance.now();await body();report.cases.push({name,status:'PASS',milliseconds:Math.round(performance.now()-start)});}
function observe(p) {
  p.on('pageerror',()=>errors.push('pageerror'));
  p.on('request',r=>{
    const parsed=new URL(r.url());if(parsed.origin!==origin)errors.push('external-request');
    if(!parsed.pathname.startsWith('/management/'))return;
    if(report.requests.length>=128){report.overflow=true;return;}
    const id=report.requests.length;requestIds.set(r,id);report.requests.push({path:parsed.pathname,method:r.method()});
    if(parsed.pathname.startsWith('/management/v1/jobs/'))pendingJobs.add(id);
  });
  p.on('requestfailed',r=>{const id=requestIds.get(r);if(id!==undefined){report.requests[id].terminal='failed';pendingJobs.delete(id);}});
  p.on('requestfinished',r=>{const id=requestIds.get(r);if(id!==undefined){report.requests[id].terminal='finished';pendingJobs.delete(id);}});
  p.on('response',r=>{
    const path=new URL(r.url()).pathname;
    if(!path.startsWith('/management/'))return;
    if(report.http.length>=128){report.overflow=true;return;}
    report.http.push({path,method:r.request().method(),status:r.status(),noStore:r.headers()['cache-control']==='no-store'});
  });
}
async function login(who='alice') {
  await page.getByLabel('Username',{exact:true}).fill(who);
  await page.getByLabel('Password',{exact:true}).fill('Independent owner password 2026');
  await page.getByRole('button',{name:'Sign in',exact:true}).click();
  await expect(page.getByRole('heading',{name:'Your session',exact:true})).toBeVisible();
}
async function lookup(p, expected) {
  await p.getByLabel('Job ID',{exact:true}).fill(expected.job_id);
  const received=p.waitForResponse(r=>r.url()===origin+'/management/v1/jobs/'+expected.job_id);
  await p.getByRole('button',{name:'Check job',exact:true}).click();
  const response=await received;assert.equal(response.status(),200);
  const value=await response.json();const {observed_at_us,...rest}=value;
  assert.deepEqual(rest,expected);assert.ok(BigInt(observed_at_us)>BigInt(expected.created_at_us));
  await expect(p.locator('.job-state')).toHaveText(expected.state==='succeeded'?'Succeeded':'Accepted');
  await expect(p.locator('.job-observation')).toContainText(expected.job_id);
  await expect(p.locator('.job-observation')).toContainText(expected.job_revision+' / '+expected.checkpoint_revision);
  assert.equal(response.headers()['set-cookie'],undefined);
}
async function unavailable(p,id) {
  await p.getByLabel('Job ID',{exact:true}).fill(id);
  const received=p.waitForResponse(r=>r.url()===origin+'/management/v1/jobs/'+id);
  await p.getByRole('button',{name:'Check job',exact:true}).click();assert.equal((await received).status(),404);
  await expect(p.locator('.job-sheet .failure')).toContainText('unavailable to your account');
  await expect(p.locator('.job-observation')).toHaveCount(0);
}
try {
  browser=await({chromium,firefox,webkit}[engine]).launch({headless:true});
  report.version=browser.version();report.launchedExecutables=[];
  for(const pid of await readdir('/proc')) {
    if(!/^\d+$/.test(pid))continue;
    let path;try{path=await readlink(`/proc/${pid}/exe`);}catch(e){if(['ENOENT','EACCES'].includes(e.code))continue;throw e;}
    if(!path.startsWith('/ms-playwright/')||report.launchedExecutables.some(x=>x.path===path))continue;
    const hash=createHash('sha256');for await(const block of createReadStream(path))hash.update(block);
    report.launchedExecutables.push({path,sha256:hash.digest('hex')});
  }
  assert.ok(report.launchedExecutables.length>0);
  context=await browser.newContext({viewport:{width:1280,height:900}});page=await context.newPage();observe(page);
  await step('production-assets-and-native-form',async()=>{
    const response=await page.goto(url);assert.equal(response.status(),200);assert.match(response.headers()['content-security-policy'],/frame-ancestors 'none'/);
    await login();
    const id=page.getByLabel('Job ID',{exact:true});await id.focus();await page.keyboard.press('Tab');
    await expect(page.getByRole('button',{name:'Check job',exact:true})).toBeFocused();
    await id.fill('bad');await page.getByRole('button',{name:'Check job',exact:true}).click();
    assert.equal(await id.evaluate(input=>input.checkValidity()),false);
    assert.equal(report.requests.filter(r=>r.path.startsWith('/management/v1/jobs/')).length,0);
  });
  await step('own-states-zero-checkpoint-no-idle-touch',async()=>{
    await command('observe-before');await lookup(page,input.completed);await lookup(page,input.accepted);
    const before=report.http.length;await sleep(600);assert.equal(report.http.length,before);await command('observe-after');
    await page.setViewportSize({width:375,height:812});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    await page.screenshot({path:join(home,'accepted-mobile.png'),fullPage:true});await page.setViewportSize({width:1280,height:900});
  });
  await step('missing-clears-prior-observation',async()=>{await unavailable(page,'ff'.repeat(16));await lookup(page,input.accepted);});
  await step('same-cookie-real-process-restart',async()=>{
    await command('restart');await page.reload();await expect(page.getByLabel('Job ID',{exact:true})).toHaveValue('');
    await expect(page.locator('.job-observation')).toHaveCount(0);await lookup(page,input.accepted);
  });
  await step('cross-tab-logout-forgets-job',async()=>{
    const other=await context.newPage();observe(other);await other.goto(url);await lookup(other,input.accepted);
    await page.getByRole('button',{name:'Sign out',exact:true}).click();
    await expect(page.locator('#session-notice')).toContainText('signed out');await expect(other.locator('#session-notice')).toContainText('another tab');
    for(const p of [page,other]){await expect(p.locator('.job-sheet')).toHaveCount(0);await expect(p.locator('.session-facts')).toHaveCount(0);}
    await other.close();
  });
  await step('blocked-sql-lookup-discarded-on-logout',async()=>{
    await login();await lookup(page,input.accepted);await command('hold-account');
    await page.getByRole('button',{name:'Check job',exact:true}).click();await expect(page.locator('.job-sheet .progress')).toContainText('Checking');
    await command('verify-blocked');await page.getByRole('button',{name:'Sign out',exact:true}).click();
    await expect(page.locator('.job-sheet')).toHaveCount(0);await command('release-account');
    await expect(page.locator('#session-notice')).toContainText('signed out');
    await expect.poll(()=>pendingJobs.size).toBe(0);await sleep(300);
    await expect(page.locator('.job-sheet')).toHaveCount(0);
  });
  await step('other-account-has-no-job-observation',async()=>{
    await login('bob');await unavailable(page,input.accepted.job_id);await unavailable(page,'ff'.repeat(16));
    await page.getByRole('button',{name:'Sign out',exact:true}).click();await expect(page.locator('#session-notice')).toContainText('signed out');
  });
  await step('real-session-revocation-forgets-job',async()=>{
    await login();await lookup(page,input.accepted);await command('deactivate');
    const denied=page.waitForResponse(r=>r.url()===origin+'/management/v1/jobs/'+input.accepted.job_id);
    await page.getByRole('button',{name:'Check job',exact:true}).click();assert.equal((await denied).status(),401);
    await expect(page.locator('.job-sheet')).toHaveCount(0);await expect(page.locator('.session-facts')).toHaveCount(0);
    assert.deepEqual(await page.evaluate(()=>({local:localStorage.length,session:sessionStorage.length})),{local:0,session:0});
  });
  assert.equal(report.requests.some(r=>r.path==='/management/v1/jobs/bad'),false);
  await expect.poll(()=>pendingJobs.size).toBe(0);
  assert.deepEqual(errors,[]);assert.equal(report.overflow,undefined);assert.ok(report.http.every(r=>r.noStore));report.status='PASS';
} catch(error) {
  report.status='FAIL';report.failure={stage,kind:error?.constructor?.name??'UnknownError'};
  const location=String(error?.stack).match(/job_browser\.mjs:(\d+):(\d+)/);if(location)report.failure.location={line:+location[1],column:+location[2]};
  if(page)try{await page.screenshot({path:join(home,'failure.png'),fullPage:true,timeout:2000});}catch{report.failure.screenshot='unavailable';}
} finally {
  for(const resource of [context,browser])if(resource)try{await resource.close();}catch{report.status='FAIL';report.teardown='failed';}
  await writeFile(join(home,'browser-report.json'),JSON.stringify(report,null,2)+'\n',{flag:'wx',mode:0o600});
  process.exitCode=report.status==='PASS'?0:1;
}
