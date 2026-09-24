import assert from 'node:assert/strict';
import test from 'node:test';
import {inspectStorage, inspectResumeStorage} from './storage_oracle.mjs';
const scroll={'1790258151295':{x:0,y:534.5}};
const input={local:[],session:[['sveltekit:scroll',JSON.stringify(scroll)],['sveltekit:snapshot','{}']]};
test('accept only exact framework scroll and empty snapshots',()=>{
  assert.deepEqual(inspectStorage(input),{status:'verified',scroll,snapshot:{}});
});

const resume = { format: 'bigbrotr-browser-resume/1', principal_id: '01'.repeat(16), job_id: '02'.repeat(16), key: '03'.repeat(16),
  command: { expected_job_revision: '7', expected_config_revision: '9007199254740993', expected_policy_revision: '2' }, outcome: 'pending' };
const database = records => [{ name: 'bigbrotr-private-commands', version: 1,
  stores: [{ name: 'export-resume', keyPath: 'principal_id', autoIncrement: false, indexes: [], records }] }];
test('independent IDB oracle admits only the exact bounded recovery metadata', () => {
  assert.deepEqual(inspectResumeStorage(database([resume])), { status: 'verified', records: [resume] });
  assert.deepEqual(inspectResumeStorage(database([])), { status: 'verified', records: [] });
  for (const patch of [{ password: 'fixture-private-marker' }, { session_id: '04'.repeat(16) }, { outcome: ['pending'] },
    { principal_id: '0'.repeat(32) }, { key: resume.key + '\n' }, { receipt: { accepted_at_us: '1' } },
    { command: { ...resume.command, expected_job_revision: '18446744073709551616' } },
    { command: { ...resume.command, destination: '/private' } }]) {
    const value = inspectResumeStorage(database([{ ...resume, ...patch }]));
    assert.deepEqual(value, { status: 'refused' });
  }
  for (const value of [[], [...database([]), { name: 'unexpected', version: 1 }],
    [{ ...database([])[0], version: 2 }], database([resume, resume]),
    database(Array.from({ length: 33 }, (_, i) => ({ ...resume, principal_id: (i + 1).toString(16).padStart(32, '0') }))),
    [{ ...database([])[0], stores: [{ ...database([])[0].stores[0], indexes: ['unexpected'] }] }]]) {
    assert.deepEqual(inspectResumeStorage(value), { status: 'refused' });
  }
});
test('refuse private state even under framework keys',()=>{
  const replacements=[
    ['sveltekit:scroll',JSON.stringify({'1790258151295':{x:0,y:0,job_id:'ab'.repeat(16)}})],
    ['sveltekit:snapshot',JSON.stringify({'1790258151295':[{job_id:'ab'.repeat(16)}]})],
    ['sveltekit:snapshot',JSON.stringify({session:{principal_id:'ab'.repeat(16)}})],
    ['sveltekit:scroll',JSON.stringify({['ab'.repeat(16)]:{x:0,y:0}})],
    ['sveltekit:scroll','{"1":{"x":"secret","x":0,"y":0}}'],
    ['sveltekit:scroll','{"1":{"x":"secret","y":0},"1":{"x":0,"y":0}}'],
    ['sveltekit:scroll','null'],['sveltekit:snapshot','[]'],['sveltekit:snapshot','malformed'],
    ['sveltekit:scroll',JSON.stringify({'1':{x:'ab'.repeat(32),y:0}})],
    ['sveltekit:scroll',' '.repeat(4097)],
  ];
  for(const [key,value] of replacements) {
    assert.equal(inspectStorage({...input,session:input.session.map(entry=>entry[0]===key?[key,value]:entry)}).status,'refused');
  }
  for(const extra of [['job','ab'.repeat(16)],['session','secret'],['sveltekit:scroll','{}']]) {
    assert.equal(inspectStorage({...input,session:[...input.session,extra]}).status,'refused');
    assert.equal(inspectStorage({...input,local:[extra]}).status,'refused');
  }
});
