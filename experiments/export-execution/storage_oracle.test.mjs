import assert from 'node:assert/strict';
import test from 'node:test';
import {inspectStorage} from './storage_oracle.mjs';
const scroll={'1790258151295':{x:0,y:534.5}};
const input={local:[],session:[['sveltekit:scroll',JSON.stringify(scroll)],['sveltekit:snapshot','{}']]};
test('accept only exact framework scroll and empty snapshots',()=>{
  assert.deepEqual(inspectStorage(input),{status:'verified',scroll,snapshot:{}});
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
