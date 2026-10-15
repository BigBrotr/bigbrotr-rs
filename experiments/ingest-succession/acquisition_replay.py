#!/usr/bin/env python3
"""Independent genuine zero-request claim, source closure and abandonment replay.

Only generated identities, transaction IDs and bounded clock coordinates are
parameters. Complete SQL/native/input states are reconstructed at every boundary.
This reader executes neither product code nor an engine.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import re
import sys
import tomllib

sys.dont_write_bytecode = True
import history_replay as history
import live_replay as live
import recovery_replay as recovery
import source_replay as source
import transition_replay as transition

encoded, sha, binary, be = history.encoded, history.sha, history.binary, history.be
only, same, loads, exact = history.only, history.same, history.loads, live.exact
CASES = ('empty-claims3', 'empty-source3', 'empty-abandon3', 'empty-abandon-retry3')
ACTOR = '\\x' + '10' * 16
SCOPE = '\\xa39723d7c49af2c99498fb2f59c03b16'


def specification(case):
    index = CASES.index(case['name'])
    assert case['origin'] == case['clone'] == 'empty'
    assert case['operation'] == ('acquisition-claims', 'source', 'abandon', 'abandon')[index]
    assert case['predecessor'] == (CASES[index - 1] if index else None)
    assert case['metadata_bytes'] == 262144 and case.get('proxy_port') is None
    assert re.fullmatch('[a-z0-9_]{1,63}', case['database'])
    assert case['config'] == '/lab/' + case['database'] + '.toml'
    return index


def science(pg, native, inputs):
    """Check the literal empty2 anchor and new activated-zero3 closure domain."""
    legacy = only(pg['initial_ingest_successions'])
    intent = only(pg['ingest_successions'], target_epoch=3)
    ds, resource, slot = (binary(legacy[k]) for k in ('dataset_id', 'owner_resource_id', 'producer_slot_id'))
    values = transition.native_map(native)
    manifest = binary(intent['target_manifest'])
    old = binary(legacy['target_manifest'])
    assert len(manifest) == 266
    domains = sorted((r for r in pg['allocation_domains'] if r['owner_epoch'] == 3), key=lambda r:r['dictionary_kind'])
    assert [r['dictionary_kind'] for r in domains] == list(range(1,10))
    wanted_manifest = (bytes.fromhex('01010003000100010001') + ds + be(2) + resource + be(0,4) + be(3)
        + binary(intent['target_incarnation']) + old[78:94] + be(1) + b'\x01\x09'
        + b''.join(be(r['dictionary_kind'],2) + binary(r['allocation_domain']) for r in domains))
    assert manifest == wanted_manifest and values['default','0101'] == manifest.hex()
    record = (b'\x02\x0a\x02' + slot + be(2) + be(0) + be(0) + bytes.fromhex(history.CLOSURES[2])
        + bytes.fromhex(sha(binary(legacy['handoff']))) + be(266,2) + old + be(266,2) + manifest)
    assert binary(intent['handoff']) == record
    new_keys = [('default',(b'\x01\x0a'+ds+resource+be(2)).hex()),
                ('default',(b'\x01\x02'+ds+be(0,4)+be(3)).hex()),
                ('receipts',(b'\x01\x31'+slot+be(3)).hex())]
    assert values[new_keys[0]] == record.hex() and values[new_keys[1]] == (b'\x01\x02'+be(0)).hex()
    closing = values[new_keys[2]] == (b'\x02\x31'+bytes(24)+b'\x01').hex()
    assert closing or values[new_keys[2]] == (b'\x01\x31'+bytes(24)).hex()
    assert len(values) == 55 and native['sequence'] == 67 + closing
    # Removing exactly the three declared new keys and restoring the singleton
    # yields the complete pinned historical native state, not a filtered census.
    historical = dict(values)
    for key in new_keys: del historical[key]
    historical['default','0101'] = old.hex()
    original = dict(event='dump-state',rows=transition.native_rows(historical),sequence=63)
    assert sha(encoded(original)) == history.PINS['empty']['native']
    assert len(inputs) == 4 and sha(encoded(inputs)) == history.PINS['empty']['inputs']
    known = history.science(pg, original, inputs, 'empty')
    assert len(pg['producer_requests']) == len(pg['producer_outcomes']) == 4
    digest = sha(b'bigbrotr/ingest-successor-source-closure/1\0'+bytes.fromhex(sha(record))
        + bytes.fromhex(sha(manifest))+slot+be(3)+be(0)+be(0))
    head = only(pg['source_streams'],owner_epoch=3)
    if head['state'] == 'closed':
        assert closing and head['last_complete_sequence'] == 0 and head['closure_digest'] == '\\x'+digest
    else:
        assert head['state'] == 'open' and head['closed_at_us'] is None
        assert head['last_complete_sequence'] is None and head['closure_digest'] is None
    assert only(pg['producer_epochs'],producer_epoch=3)['reserved_through_request'] == 0
    summary = dict(epoch=3,terminal_kind='activated',resolved_requests=0,last_complete_sequence=0,digest=digest,event_request_mask=0)
    return dict(head=3,slot=slot,manifest=manifest,counts=[4,0,0],summaries=[*known['summaries'],summary],issued=0,cut=0,closing=closing)


class Model(live.Model):
    def audit(self, observed, action, subject, xid, at=None):
        row = only(observed['management_audit'],action=action,subject_id=subject)
        ident = self.identity(row['audit_id'][2:])
        created = self.at(row['created_at_us']) if at is None else at
        self.pg['management_audit'].append(dict(dataset_id=self.ds['dataset_id'],actor_id=ACTOR,
            audit_id=ident,action=action,outcome='accepted',subject_id=subject,created_at_us=created,
            config_revision=1,policy_revision=1,reason=None,requester_database_role=None,requester_os_uid=None,xmin=xid))
        return ident

    def login_begin(self, observed):
        account = only(observed['login_buckets'],kind='account',bucket=97)
        at,xid = self.at(account['window_started_at_us']),self.fresh(account['xmin'])
        assert at < self.control['lease_expires_at_us']
        for kind,n in [('account',97),('peer',13)]:
            row = only(self.pg['login_buckets'],kind=kind,bucket=n)
            assert at-row['window_started_at_us'] >= 60_000_000
            row.update(window_started_at_us=at,attempts=1,xmin=xid)
        same(self.pg,observed)

    def login_finish(self, observed):
        account = only(self.pg['local_accounts'],principal_id=ACTOR)
        assert account['active'] and (account['credential_revision'],account['authentication_revision'],account['grant_revision']) == (1,1,4)
        session = only(observed['browser_sessions'])
        at,xid = self.at(session['issued_at_us']),self.fresh(session['xmin'])
        assert at < self.control['lease_expires_at_us']
        sid = self.identity(session['session_id'][2:])
        for row in self.pg['browser_sessions']:
            assert row['principal_id'] == ACTOR and (row['revoked_at_us'] is not None or row['expires_at_us'] <= at or row['idle_expires_at_us'] <= at)
        for key in ('token_digest','csrf_digest'):
            assert len(binary(session[key])) == 32 and any(binary(session[key]))
            assert session[key] not in {r[key] for r in self.pg['browser_sessions']}
        assert session['token_digest'] != session['csrf_digest']
        self.pg['browser_sessions'] = [dict(dataset_id=self.ds['dataset_id'],session_id=sid,principal_id=ACTOR,
            credential_revision=1,authentication_revision=1,token_digest=session['token_digest'],csrf_digest=session['csrf_digest'],
            issued_at_us=at,expires_at_us=at+28_800_000_000,idle_window_us=1_800_000_000,last_seen_at_us=at,
            idle_expires_at_us=at+1_800_000_000,revoked_at_us=None,xmin=xid)]
        self.audit(observed,'browser.session.issue',sid,xid)
        same(self.pg,observed)

    def accept(self, observed, key):
        command = only(observed['management_commands'],actor_id=ACTOR,request_key=key)
        at,xid = self.at(command['accepted_at_us']),self.fresh(command['xmin'])
        assert at < self.control['lease_expires_at_us']
        digest = '\\x'+sha(b'bigbrotr/acquisition-job/1\0'+binary(self.ds['dataset_id'])+binary(ACTOR)
            +binary(key)+binary(SCOPE)+be(30_000_000)+be(1)*2)
        audit = self.audit(observed,'job.acquisition.accept',key,xid,at)
        self.pg['management_commands'].append(dict(dataset_id=self.ds['dataset_id'],actor_id=ACTOR,request_key=key,
            request_digest=digest,action='job.acquisition.accept',accepted_at_us=at,expected_config_revision=1,
            policy_revision=1,audit_id=audit,authority_mode=None,document_digest=None,xmin=xid))
        job = only(observed['jobs'],request_key=key)
        ident = self.identity(job['job_id'][2:])
        self.pg['jobs'].append(dict(kind='acquisition',xmin=xid,state='accepted',job_id=ident,dataset_id=self.ds['dataset_id'],
            owner_role='control',owner_epoch=None,reason_code=None,request_key=key,job_revision=1,principal_id=ACTOR,
            created_at_us=at,input_complete=True,request_digest=digest,completed_at_us=None,config_revision=1,policy_revision=1,
            owner_incarnation=None,owner_resource_id=None,checkpoint_revision=0,export_job_protocol=None,
            result_manifest_digest=None,acquisition_job_protocol=1,input_topology_generation=None))
        self.pg['acquisition_jobs'].append(dict(xmin=xid,job_id=ident,actor_id=ACTOR,scope_id=SCOPE,dataset_id=self.ds['dataset_id'],
            session_id=only(self.pg['browser_sessions'])['session_id'],duration_us=30_000_000,request_key=key,
            deadline_at_us=at+30_000_000,actor_grant_revision=4,actor_credential_revision=1,actor_authentication_revision=1))
        ordinal = max(r['admission_ordinal'] for r in self.pg['job_admissions'])+1
        self.pg['job_admissions'].append(dict(job_id=ident,admission_ordinal=ordinal,xmin=xid))
        same(self.pg,observed)
        return only(self.pg['jobs'],job_id=ident)

    def claim_job(self, observed, key):
        job = only(self.pg['jobs'],request_key=key)
        assert job['state'] == 'accepted' and job['job_revision'] == 1
        actual = only(observed['jobs'],job_id=job['job_id'])
        job.update(state='queued',job_revision=2,owner_resource_id=self.control['resource_id'],owner_epoch=self.control['epoch'],
            owner_incarnation=self.control['process_incarnation'],xmin=self.fresh(actual['xmin']))
        same(self.pg,observed)
        return job

    def close(self, observed, known):
        recovery.Model.close(self,observed,known)

    def publish(self, observed, known):
        head = only(self.pg['source_streams'],owner_epoch=3)
        assert head['state']=='open'
        row = only(observed['source_streams'],owner_epoch=3)
        at = self.at(row['closed_at_us'],head['opened_at_us'])
        assert at < self.control['lease_expires_at_us']
        head.update(state='closed',closed_at_us=at,last_complete_sequence=0,closure_digest='\\x'+known['summaries'][-1]['digest'],xmin=self.fresh(row['xmin']))
        same(self.pg,observed)

    def abandon(self, observed, known, key, retry):
        job = only(self.pg['jobs'],request_key=key)
        activator = only(self.pg['ingest_activations'],target_epoch=3)
        head = only(self.pg['source_streams'],owner_epoch=3)
        assert head['state']=='closed' and head['last_complete_sequence']==0
        assert head['closure_digest']=='\\x'+known['summaries'][-1]['digest']
        if retry:
            assert job['state']=='cancelled' and job['job_revision']==4
            same(self.pg,observed)
            return
        assert job['state']=='queued' and job['job_revision']==2 and job['checkpoint_revision']==0
        assert (job['owner_resource_id'],job['owner_epoch'],job['owner_incarnation']) == tuple(activator[k] for k in ('activated_by_resource','activated_by_epoch','activated_by_incarnation'))
        row = only(observed['acquisition_gaps'],job_id=job['job_id'])
        at,xid=self.at(row['abandoned_at_us'],head['closed_at_us']),self.fresh(row['xmin'])
        assert at < self.control['lease_expires_at_us']
        self.pg['acquisition_gaps'].append(dict(xmin=xid,job_id=job['job_id'],reason='ingest_successor_abandoned',shard_id=0,
            dataset_id=self.ds['dataset_id'],owner_role='control',issuer_epoch=self.control['epoch'],producer_epoch=3,
            abandoned_at_us=at,issuer_resource=self.control['resource_id'],producer_slot_id=self.slot,
            issuer_incarnation=self.control['process_incarnation'],source_owner_epoch=3,previous_owner_epoch=job['owner_epoch'],
            previous_job_revision=2,source_closure_digest=head['closure_digest'],last_complete_sequence=0,
            previous_owner_resource=job['owner_resource_id'],previous_owner_incarnation=job['owner_incarnation']))
        job.update(state='cancelled',job_revision=4,reason_code='ingest_successor_abandoned',completed_at_us=at,
            owner_resource_id=self.control['resource_id'],owner_epoch=self.control['epoch'],owner_incarnation=self.control['process_incarnation'],xmin=xid)
        same(self.pg,observed)


class Transcript(source.Transcript):
    def live(self, name):
        event=self.events[self.position];self.position+=1
        assert event['event']=='lineage-live-'+name
        return event
    def acq(self, name):
        event=self.events[self.position];self.position+=1
        assert event['event']=='lineage-acquisition-'+name
        return event


def finish(t, m, backend, stage, complete):
    t.snapshot(stage)
    exact(t.take('native'),dict(event='lineage-history-native',state='closed'))
    t.snapshot('before-control-end')
    exact(t.take('driver'),dict(event='lineage-history-driver',driver='control',failed=False,joined=True))
    exact(t.take('backend'),dict(backend,present=False))
    exact(t.take('control'),dict(event='lineage-history-control',phase='released-and-joined',
        resource=m.control['resource_id'][2:],epoch=m.control['epoch']))
    t.snapshot('after-control-end',m.release)
    t.snapshot('final')
    exact(t.take('driver'),dict(event='lineage-history-driver',driver='observer',failed=False,joined=True))
    actual=t.events[t.position];t.position+=1
    exact(actual,dict(native='closed',control='released-and-joined',observer='joined',success=True,**complete))
    assert t.position==len(t.events),'unclassified trailing telemetry'


def checked(m,t,case,scope):
    return dict(status='PASS',case=case['name'],database=case['database'],snapshots=t.snapshots,tables=75,
        native_rows=len(m.native['rows']),sequence=m.native['sequence'],inputs=len(m.inputs),
        control_epoch=m.control['epoch'],refreshes=m.refreshes,scope=scope)


def verify_seed(events,before,native,inputs,case,server_window):
    origin,operation,target='empty','acquisition-claims',3
    history.baseline(before,native,inputs,origin,server_window)
    history.science(before,native,inputs,origin)
    m = Model(before, native, inputs, server_window)
    known = transition.lineage(before, native, origin)
    assert known['head'] == target - 1 and known['state'] == 'Terminal'
    t = Transcript(events, m)
    candidate = t.take('candidate', True)
    assert set(candidate) == {'event', 'role', 'incarnation', 'namespaces'}
    exact([r['kind'] for r in candidate['namespaces']], list(range(1, 10)))
    assert all(set(r) == {'kind', 'id'} for r in candidate['namespaces'])
    ids = [candidate['role'], candidate['incarnation']] + [r['id'] for r in candidate['namespaces']]
    assert len(set(ids)) == 11
    assert all(re.fullmatch('[0-9a-f]{32}', v) and int(v, 16) and v not in m.identities for v in ids)
    exact(t.acq('resources'),dict(event='lineage-acquisition-resources',purpose='claims',data_bytes=65*1024**2,native_bytes=0,max_jobs=2,admission=False))
    resource = t.take('resources')
    exact(resource, dict(event='lineage-history-resources', metadata_bytes=262144,
        metadata_scratch_bytes=131072, metadata_entry_bytes=512, native_bytes=128 * 1024**2,
        transport_bytes=4 * 1024**2, observation_bytes=32 * 1024**2, operation_timeout_ms=10000,
        frame_bytes=16 * 1024**2, **{k: resource[k] for k in ('input_bytes', 'outcome_bytes', 'effect_bytes')}))
    for field in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(resource[field]) is int and 0 < resource[field] <= 256 * 1024**2
    exact(t.take('resources', True), dict(event='lineage-transition-resources', operation=operation,
        mutation_data_bytes=8192, mutation_native_bytes=8192, lease_ttl_us='20000000', outstanding_request_limit=32))
    planned = t.take('control')
    assert set(planned) == {'event', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['phase'] == 'claim-planned' and planned['lease_ttl_us'] == '20000000'
    assert not ({planned['role'], planned['incarnation']} & set(ids))
    t.snapshot('before-claim')
    claimed, policy, backend = t.take('control'), t.take('policy'), t.take('backend')
    t.snapshot('after-claim', lambda pg: m.claim(pg, planned))
    exact(claimed, dict(event='lineage-history-control', phase='claimed', resource=m.control['resource_id'][2:],
        epoch=m.control['epoch'], role=planned['role'], incarnation=planned['incarnation']))
    exact(backend, dict(event='lineage-history-backend', pid=m.control['control_backend_pid'],
        backend_start=m.control['control_backend_start'], database=case['database'],
        control_epoch=m.control['epoch'], present=True))
    t.policy(policy, 'acquired', None, None)
    t.snapshot('before-read')
    t.history(known)
    event = t.take('plan', True)
    created, deadline = event['created_at_us'], event['lease_expires_at_us']
    assert re.fullmatch('[1-9][0-9]*', created) and re.fullmatch('[1-9][0-9]*', deadline)
    plan = m.plan(candidate['role'], candidate['incarnation'], [r['id'] for r in candidate['namespaces']],
                  target, int(created), int(deadline))
    record = transition.record_value(plan['record'], target)
    exact(event, dict(event='lineage-transition-plan', operation='prepare', record=record,
        role=candidate['role'], created_at_us=created, lease_expires_at_us=deadline,
        outstanding_request_limit=32, policy=dict(config_revision=m.ds['config_revision'],
            policy_revision=m.ds['policy_revision'],
            config_digest=only(m.pg['config_revisions'], revision=m.ds['config_revision'])['document_digest'][2:],
            policy_digest=only(m.pg['policy_revisions'], revision=m.ds['policy_revision'])['document_digest'][2:])))
    t.snapshot('planned')
    t.snapshot('transition-returned', lambda pg: m.prepare(pg, plan))
    exact(t.take('observed', True), dict(event='lineage-transition-observed', operation='prepare',
        prepared=True, record=record, activation=False))
    exact(t.live('activation-start'), dict(event='lineage-live-activation-start', target_epoch=target,
        control_epoch=m.control['epoch']))
    activation = t.live('activation-result')
    t.snapshot('activated-zero', lambda pg: m.activate(pg, plan))
    exact(activation, dict(event='lineage-live-activation-result', writer=True, target_epoch=target,
        activated_at_us=str(only(m.pg['ingest_activations'], target_epoch=target)['activated_at_us']),
        manifest=plan['manifest'].hex(), role=candidate['role'], producer_slot=m.slot[2:], error=None))
    t.policy(t.take('policy'), 'live-refreshed', target, None)
    t.snapshot('live-control-refreshed', m.refresh)
    exact(t.live('renewal'), dict(event='lineage-live-renewal', commit_started=True, error=None))
    t.snapshot('live-renew-returned', lambda pg: m.renew_ingest(pg, target))
    t.snapshot('before-acquisition-claims')
    exact(t.acq('seed-plan'),dict(event='lineage-acquisition-seed-plan',target_epoch=3,scope=SCOPE[2:],
        claimed_key='a6'*16,unclaimed_key='a7'*16,duration_us=30000000,config_revision=1,policy_revision=1,
        username='alice',peer='127.0.0.1',session_maximum_us='28800000000',session_idle_us='1800000000',executions=0))
    for phase,mutation in [('begin',m.login_begin),('verify',None),('finish',m.login_finish)]:
        t.snapshot('acquisition-login-'+phase+'-returned',mutation)
        exact(t.acq('login'),dict(event='lineage-acquisition-login',phase=phase,error=None))
    t.snapshot('acquisition-refresh-returned',m.refresh)
    exact(t.acq('refresh'),dict(event='lineage-acquisition-refresh',error=None,policy=dict(config_revision=1,policy_revision=1,
        config_digest=only(m.pg['config_revisions'],revision=1)['document_digest'][2:],
        policy_digest=only(m.pg['policy_revisions'],revision=1)['document_digest'][2:])))
    for purpose,key in [('claimed','a6'*16),('unclaimed','a7'*16)]:
        t.snapshot('acquisition-'+purpose+'-accept-returned',lambda pg:m.accept(pg,'\\x'+key))
        job=only(m.pg['jobs'],request_key='\\x'+key)
        exact(t.acq('acceptance'),dict(event='lineage-acquisition-acceptance',purpose=purpose,request_key=key,
            receipt=dict(command_key=key,job_id=job['job_id'][2:],accepted_at_us=job['created_at_us'],created=True),error=None,commit_started=True))
        if purpose=='claimed':
            t.snapshot('acquisition-claim-returned',lambda pg:m.claim_job(pg,'\\x'+key))
            exact(t.acq('claim'),dict(event='lineage-acquisition-claim',error=None,commit_started=True,
                job=dict(id=job['job_id'][2:],scope=SCOPE[2:],state='queued',job_revision=2,checkpoint_revision=0,
                    accepted_at_us=job['created_at_us'],deadline_at_us=job['created_at_us']+30000000,
                    owner=dict(resource=m.control['resource_id'][2:],epoch=m.control['epoch'],incarnation=m.control['process_incarnation'][2:]))))
    t.snapshot('acquisition-claims-returned')
    preserved=t.acq('seed-state')
    raw=preserved['inputs_before']
    assert [v['name'] for v in raw]==sorted(inputs) and all(set(v)=={'name','bytes'} for v in raw)
    for value in raw:
        data=bytes.fromhex(value['bytes'])
        assert data.hex()==value['bytes'] and dict(bytes=len(data),sha256=sha(data))==inputs[value['name']]
    exact(preserved,dict(event='lineage-acquisition-seed-state',target_epoch=3,native_rows=55,native_sequence=67,
        inputs_before=raw,inputs_after=raw,unchanged=True,executions=0,error=None))
    observation=dict(target_epoch=3,claimed_job=only(m.pg['jobs'],request_key='\\x'+'a6'*16)['job_id'][2:],
        unclaimed_job=only(m.pg['jobs'],request_key='\\x'+'a7'*16)['job_id'][2:],requests=0,source_cut=0,executions=0,
        writer_delivered=True,session_revoked=False,workflow=False,source_closure=False,retirement=False)
    exact(t.acq('result'),dict(event='lineage-acquisition-result',operation=operation,stage='complete',epoch=None,sequence=None,error=None,observation=observation))
    finish(t,m,backend,'after-acquisition',dict(event='lineage-acquisition-complete',operation=operation,
        writer=False,activation=True,admission=False,source_closure=False,workflow=False,retirement=False))
    assert len(m.native['rows'])==55 and m.native['sequence']==67 and m.inputs==inputs
    science(m.pg,m.native,m.inputs)
    return checked(m,t,case,'Genuine activated3 zero-request claim plus accepted-unclaimed intent; no acquisition execution or terminal authority')


def verify_terminal(events,before,native,inputs,case,server_window,index):
    head,issued=3,0
    abandon=index>=2
    retry=abandon
    known=science(before,native,inputs)
    assert only(before['source_streams'],owner_epoch=3)['state']==('closed' if abandon else 'open')
    assert only(before['producer_epochs'],producer_epoch=3)['state']==('closing' if abandon else 'open')
    assert known['closing']==abandon
    assert len([r for r in before['acquisition_gaps'] if r['producer_epoch']==3])==(1 if index==3 else 0)
    m = Model(before, native, inputs, server_window)
    t = Transcript(events, m)
    if abandon:
        exact(t.acq('resources'),dict(event='lineage-acquisition-resources',purpose='abandon',data_bytes=128*1024,native_bytes=0,max_jobs=64,admission=False))
    resources = t.take('resources')
    assert set(resources) == {'event', 'metadata_bytes', 'metadata_scratch_bytes', 'metadata_entry_bytes',
        'input_bytes', 'outcome_bytes', 'effect_bytes', 'native_bytes', 'transport_bytes',
        'observation_bytes', 'operation_timeout_ms', 'frame_bytes'}
    for field in ('input_bytes', 'outcome_bytes', 'effect_bytes'):
        assert type(resources[field]) is int and 0 < resources[field] <= 256 * 1024**2
    exact(resources, dict(event='lineage-history-resources', metadata_bytes=262144,
        metadata_scratch_bytes=131072, metadata_entry_bytes=512, input_bytes=resources['input_bytes'],
        outcome_bytes=resources['outcome_bytes'], effect_bytes=resources['outcome_bytes'], native_bytes=128 * 1024**2,
        transport_bytes=4 * 1024**2, observation_bytes=32 * 1024**2, operation_timeout_ms=10000, frame_bytes=16 * 1024**2))
    exact(t.take('resources', True), dict(event='lineage-transition-resources', operation=case['operation'],
        mutation_data_bytes=4096, mutation_native_bytes=4096, lease_ttl_us='20000000', outstanding_request_limit=32))
    planned = t.take('control')
    assert set(planned) == {'event', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['phase'] == 'claim-planned' and planned['lease_ttl_us'] == '20000000'
    t.snapshot('before-claim')
    claimed, policy, backend = t.take('control'), t.take('policy'), t.take('backend')
    t.snapshot('after-claim', lambda pg: m.claim(pg, planned))
    exact(claimed, dict(event='lineage-history-control', phase='claimed', resource=m.control['resource_id'][2:],
        epoch=m.control['epoch'], role=planned['role'], incarnation=planned['incarnation']))
    exact(backend, dict(event='lineage-history-backend', pid=m.control['control_backend_pid'],
        backend_start=m.control['control_backend_start'], database=case['database'], control_epoch=m.control['epoch'], present=True))
    t.policy(policy, 'acquired', None, None)
    t.snapshot('before-source')
    t.snapshot('source-inspect-returned')
    exact(t.source('loaded'), dict(event='lineage-source-loaded', epoch=head, manifest=known['manifest'].hex(),
        producer_slot=known['slot'].hex(), issued_through=issued, reserved_through=issued, staged_input=None,
        barrier_confirmed=False, sql_producer_state='closing' if abandon else 'open', sql_source_state='closed' if retry else 'open'))
    t.snapshot('before-source-producer-close')
    t.snapshot('source-producer-close-returned', lambda pg: m.close(pg, known))
    exact(t.source('producer-closed'), dict(event='lineage-source-producer-closed', epoch=head, confirmed=True,
        issued_through=issued, reserved_through=issued, staged_input=None, error=None))
    for epoch, count in enumerate(known['counts'], 1):
        for n in range(1, count + 1):
            t.refresh_source(epoch, n)
            t.snapshot(f'source-request-returned-{epoch}-{n}', lambda pg: same(m.pg,pg))
            exact(t.source('request'), dict(event='lineage-source-request', producer_slot=known['slot'].hex(),
                epoch=epoch, sequence=n, copy_mode='existing-terminal' if epoch < head else 'existing-source-closed' if retry else 'current-positive',
                reservation=only(m.pg['producer_requests'], producer_epoch=epoch, request_sequence=n),
                outcome=only(m.pg['producer_outcomes'], producer_epoch=epoch, request_sequence=n)))
        t.refresh_source(epoch, None)
        t.snapshot(f'source-epoch-returned-{epoch}')
        exact(t.source('epoch'), dict(event='lineage-source-epoch', summary=known['summaries'][epoch - 1]))
    t.refresh_source(None, None)
    t.snapshot('source-finish-returned')
    no_authority = dict(source_closure=False, workflow=False, retirement=False, admission=False, activation=False)
    exact(t.source('proof'), dict(event='lineage-source-proof', verified_through=head,
        last_epoch=known['summaries'][-1], matches_source=True, **no_authority))
    observation=dict(target_epoch=3,issued_through=0,reserved_through=0,staged_input=None,
        verified_through=3,last_epoch=known['summaries'][-1],positive_recovery=True,source_closure=not abandon)
    if abandon:
        t.snapshot('before-acquisition-abandon')
        t.snapshot('acquisition-abandon-returned',lambda pg:m.abandon(pg,known,'\\x'+'a6'*16,index==3))
        exact(t.acq('abandoned'),dict(event='lineage-acquisition-abandoned',epoch=3,jobs=1,error=None))
        observation.update(workflow=True,acquisition_jobs=1)
        exact(t.acq('result'),dict(event='lineage-acquisition-result',operation='abandon',stage='complete',
            epoch=None,sequence=None,error=None,observation=observation))
        finish(t,m,backend,'after-acquisition',dict(event='lineage-acquisition-complete',operation='abandon',
            writer=False,activation=False,admission=False,source_closure=False,workflow=True,retirement=False))
    else:
        t.snapshot('before-source-publish')
        t.snapshot('source-publish-returned',lambda pg:m.publish(pg,known))
        exact(t.source('published'),dict(event='lineage-source-published',epoch=3,confirmed=True,
            source=only(m.pg['source_streams'],owner_epoch=3),error=None))
        exact(t.source('result'),dict(event='lineage-source-result',operation='source',stage='complete',
            epoch=None,sequence=None,error=None,observation=observation))
        finish(t,m,backend,'after-source',dict(event='lineage-source-complete',operation='source',
            source_closure=True,workflow=False,retirement=False,admission=False,activation=False))
    assert len(m.native['rows'])==55 and m.native['sequence']==68 and m.inputs==inputs
    assert t.snapshots==28 and m.refreshes==8
    science(m.pg,m.native,m.inputs)
    return checked(m,t,case,'Fresh source closure or whole-intent abandonment/retry on genuine activated-zero claim; no retirement/execution/installed authority')


def verify(events,before,native,inputs,case,server_window):
    index=specification(case)
    assert all(isinstance(e,dict) for e in events)
    if index==0:return verify_seed(events,before,native,inputs,case,server_window)
    return verify_terminal(events,before,native,inputs,case,server_window,index)


def negative_controls(events,before,native,inputs,case,server_window):
    verify(events,before,native,inputs,case,server_window)
    index=specification(case);rejected=[]
    def event(v,name,**fields):return only(v,event=name,**fields)
    def reject(name,mutate):
        altered=copy.deepcopy(events);mutate(altered)
        try:verify(altered,before,native,inputs,case,server_window)
        except (AssertionError,ValueError,KeyError,TypeError,OverflowError):rejected.append(name)
        else:raise AssertionError('accepted altered evidence: '+name)
    def state(values,change,stage='final'):
        e=event(values,'lineage-history-state',stage=stage);s=e['state']
        pg={k:loads(raw) for k,raw in s['control_tables']};change(pg)
        s['control_tables']=[[k,encoded(rows).decode()] for k,rows in sorted(pg.items())]
        s['control_sha256']=sha(encoded(s['control_tables']))
    reject('historical-row-xmin-rehashed',lambda v:state(v,lambda pg:only(pg['producer_epochs'],producer_epoch=1).update(xmin='3999999999')))
    reject('native-sequence-rewritten',lambda v:event(v,'lineage-history-state',stage='final')['state'].update(sequence=999))
    reject('sidecar-digest-rewritten',lambda v:event(v,'lineage-history-state',stage='final')['state']['originals'][0][1].update(sha256='00'*32))
    reject('Control-driver-not-joined',lambda v:event(v,'lineage-history-driver',driver='control').update(joined=False))
    reject('observer-driver-failed',lambda v:event(v,'lineage-history-driver',driver='observer').update(failed=True))
    reject('backend-not-absent',lambda v:event(v,'lineage-history-backend',present=False).update(present=True))
    complete='lineage-source-complete' if index==1 else 'lineage-acquisition-complete'
    reject('retirement-authority-invented',lambda v:event(v,complete).update(retirement=True))
    reject('admission-authority-invented',lambda v:event(v,complete).update(admission=True))
    reject('unclaimed-job-cancelled',lambda v:state(v,lambda pg:only(pg['jobs'],request_key='\\x'+'a7'*16).update(state='cancelled')))
    reject('session-revoked',lambda v:state(v,lambda pg:only(pg['browser_sessions']).update(revoked_at_us=server_window['before_us'])))
    reject('legacy-gap-rewritten',lambda v:state(v,lambda pg:only(pg['acquisition_gaps'],producer_epoch=1).update(xmin='3999999999')))
    reject('claim-attributed-to-Ingest',lambda v:state(v,lambda pg:only(pg['jobs'],request_key='\\x'+'a6'*16).update(owner_resource_id=only(pg['initial_ingest_successions'])['owner_resource_id'])))
    if index==0:
        reject('wrong-scope',lambda v:event(v,'lineage-acquisition-seed-plan').update(scope='11'*16))
        reject('unclaimed-intent-omitted',lambda v:v.remove(event(v,'lineage-acquisition-acceptance',purpose='unclaimed')))
        reject('fabricated-claim',lambda v:event(v,'lineage-acquisition-claim')['job']['owner'].update(epoch=1))
        reject('uncommitted-acceptance',lambda v:event(v,'lineage-acquisition-acceptance',purpose='claimed').update(commit_started=False))
        reject('seed-execution-invented',lambda v:event(v,'lineage-acquisition-seed-state').update(executions=1))
        reject('seed-source-closure-invented',lambda v:event(v,complete).update(source_closure=True))
        reject('login-not-completed',lambda v:event(v,'lineage-acquisition-login',phase='finish').update(error='CommitUnknown'))
    else:
        reject('source-proof-confers-workflow',lambda v:event(v,'lineage-source-proof').update(workflow=True))
        reject('source-request-omitted',lambda v:v.remove(next(e for e in v if e['event']=='lineage-source-request')))
        reject('source-cut-invented',lambda v:state(v,lambda pg:only(pg['source_streams'],owner_epoch=3).update(last_complete_sequence=1)))
        reject('source-digest-invented',lambda v:state(v,lambda pg:only(pg['source_streams'],owner_epoch=3).update(closure_digest='\\x'+'00'*32)))
        if index==1:
            reject('source-not-published',lambda v:event(v,'lineage-source-published').update(confirmed=False))
            reject('source-published-before-proof',lambda v:state(v,lambda pg:only(pg['source_streams'],owner_epoch=3).update(state='closed'),stage='source-finish-returned'))
        else:
            reject('zero-request-claim-skipped',lambda v:event(v,'lineage-acquisition-abandoned').update(jobs=0))
            reject('unknown-abandonment',lambda v:event(v,'lineage-acquisition-abandoned').update(error='CommitUnknown'))
            reject('wrong-previous-activator',lambda v:state(v,lambda pg:only(pg['acquisition_gaps'],producer_epoch=3).update(previous_owner_epoch=1)))
            reject('gap-source-relabelled',lambda v:state(v,lambda pg:only(pg['acquisition_gaps'],producer_epoch=3).update(source_owner_epoch=2)))
            reject('claim-revision-wrong',lambda v:state(v,lambda pg:only(pg['jobs'],request_key='\\x'+'a6'*16).update(job_revision=3)))
            reject('source-xmin-rewritten',lambda v:state(v,lambda pg:only(pg['source_streams'],owner_epoch=3).update(xmin='3999999999')))
            reject('gap-issuer-rewritten',lambda v:state(v,lambda pg:only(pg['acquisition_gaps'],producer_epoch=3).update(issuer_epoch=1)))
            reject('gap-clock-rewritten',lambda v:state(v,lambda pg:only(pg['acquisition_gaps'],producer_epoch=3).update(abandoned_at_us=server_window['before_us']-1)))
    return dict(status='PASS',count=len(rejected),rejected=rejected)


def replay(root):
    """Bind actual stdout, original pins and every successive external baseline."""
    root = Path(root).resolve()
    folder, bound = root / 'acquisition', {}

    def raw(path):
        path = Path(path)
        value = path.read_bytes()
        bound[str(path)] = sha(value)
        return value

    def read(path):
        return loads(raw(path))

    report = read(folder / 'result.json')
    assert report['status'] == 'PASS_SUCCESSOR_ACQUISITION_FIRST_WAVE'
    assert [c['name'] for c in report['cases']] == list(CASES)
    assert [c['name'] for c in report['clones']] == ['empty']
    assert len({c['database'] for c in report['clones']}) == 1
    assert report['databases'] == [report['clones'][0]['database']]
    assert set(report['origins']) == set(history.PINS)
    for field in ('cleanup_errors', 'missing_owned', 'unrelated_changed', 'unexpected_containers'):
        assert report[field] == []
    origins = {}
    for origin, pin in history.PINS.items():
        declaration = report['origins'][origin]
        assert declaration['name'] == origin and declaration['pins'] == pin
        for field in ('run', 'database', 'snapshot', 'snapshot_sha256', 'result_sha256', 'status'):
            assert declaration[field] == pin[field]
        basis = root.parent / pin['run'] / 'bindings'
        result = read(basis / 'result.json')
        assert bound[str(basis / 'result.json')] == pin['result_sha256'] and result['status'] == pin['status']
        rows = read(basis / pin['snapshot'])
        assert bound[str(basis / pin['snapshot'])] == pin['snapshot_sha256']
        native = read(basis / pin['native_file'])
        assert bound[str(basis / pin['native_file'])] == pin['native_sha256']
        original_inputs = read(basis / pin['inputs_file'])
        assert bound[str(basis / pin['inputs_file'])] == pin['inputs_sha256']
        inputs = original_inputs.get('inputs', original_inputs)
        same(rows, declaration['rows'])
        assert declaration['native'] == native and declaration['inputs'] == inputs
        assert Path(declaration['store']) == basis / pin['store']
        assert declaration['previous_client'] == result['client']
        assert all(result[k] == [] for k in ('cleanup_errors', 'missing_owned', 'unrelated_changed'))
        same(rows, read(folder / (origin + '-original.json')))
        input_root = basis / pin['store'] / 'producer-inputs-v1'
        assert sorted(p.name for p in input_root.iterdir()) == sorted(inputs)
        for name, identity in inputs.items():
            content = raw(input_root / name)
            assert dict(bytes=len(content), sha256=sha(content)) == identity
            if 'contents' in original_inputs:
                assert content.hex() == original_inputs['contents'][name]
        preserved = dict(rows_unchanged=True, catalog_unchanged=True, native_unchanged=True,
            inputs_unchanged=True, physical_unchanged=True)
        assert declaration['preserved'] == preserved == read(folder / (origin + '-preserved.json'))
        assert all(v is True for v in declaration['preserved'].values())
        origins[origin] = rows, native, inputs

    chains, windows, clones = {}, {}, {}
    for clone in report['clones']:
        name, origin = clone['name'], clone['origin']
        assert name == origin == 'empty'
        assert clone['status'] == 'PASS' and clone['directory'] == name
        assert clone['config'] == '/lab/' + clone['database'] + '.toml'
        config_path = folder / Path(clone['config']).name
        config = tomllib.loads(raw(config_path).decode())
        ds = only(origins[origin][0]['datasets'])
        exact({k: config['postgres']['control'][k] for k in ('database','endpoint','user','password_ref')},
            dict(database=clone['database'],endpoint='127.0.0.1:5432',user='bigbrotr_control',password_ref='file:/run/secrets/application'))
        assert config['storage']['ingest_root'] == '/lab/' + name + '/store'
        assert config['topology']['dataset_id'] == ds['dataset_id'][2:]
        assert config['topology']['instance_id'] == ds['instance_id'][2:]
        assert config['runtime']['lease_ttl'] == '20s'
        validation = read(folder / (name + '-config-validation.json'))
        assert validation['returncode'] == 0 and validation['stderr'] == ''
        migrations = clone['migrations']
        assert [r['action'] for r in migrations] == ['plan', 'apply', 'apply']
        steps = [dict(name=f'{v:04d}_{history.MIGRATIONS[v][0]}.sql', sha256=history.MIGRATIONS[v][1], version=str(v))
                 for v in range(history.PINS[origin]['floor'] + 1, 39)]
        for index, answer in enumerate(migrations):
            assert set(answer) == {'action', 'exit_code', 'stdout', 'stderr'}
            assert answer['exit_code'] == 0 and answer['stderr'] == ''
            assert loads(answer['stdout']) == dict(format='bigbrotr-control-migration/1',
                scope='postgres-control', target_version='38',
                source_version=str(history.PINS[origin]['floor']) if index < 2 else '38',
                current_version=str(history.PINS[origin]['floor']) if index == 0 else '38',
                status=('planned', 'applied', 'current')[index], applied_count=str(len(steps)) if index == 1 else '0',
                steps=steps if index < 2 else [], dataset_initialization='not-performed',
                release_upgrade_qualification='not-performed')
        grants = clone['grants']
        assert len(grants) == 4 and all(set(g) == {'statement', 'stdout'} for g in grants)
        assert sha(grants[0]['statement'].encode()) == '36a16062c994adfe1ba7ace4c5e56d2ed942277b0e227ddd048e320939f0e588'
        assert [g['statement'] for g in grants[1:]] == [
            'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;',
            'GRANT UPDATE ON control.producer_epochs TO bigbrotr_control;',
            'GRANT INSERT ON control.producer_outcomes TO bigbrotr_control;']
        assert all(not g['stdout'].strip() for g in grants)
        assert isinstance(clone['catalog_before'], str) and clone['catalog_before']
        assert clone['catalog_after'] == clone['final_catalog'] == clone['catalog_before']
        assert clone['final_backends'] == []
        same(origins[origin][0], read(folder / (name + '-copy-before.json')))
        migrated = read(folder / (name + '-migrated.json'))
        chains[name] = migrated, origins[origin][1], origins[origin][2]
        clones[name] = clone

    checked = []
    for case in report['cases']:
        specification(case)
        origin = 'empty'
        name, clone_name = case['name'], case['clone']
        assert clone_name == origin
        clone = clones[clone_name]
        assert case['database'] == clone['database'] and case['config'] == clone['config']
        assert case['status'] == 'PASS' and case['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
        assert type(case['returncode']) is int and case['returncode'] == 0
        assert case['catalog_after'] == clone['catalog_before']
        case_read = lambda suffix: read(folder / (name + '-' + suffix + '.json'))
        before, native, inputs = case_read('before'), case_read('before-native'), case_read('inputs')
        previous = chains[clone_name]
        same(previous[0], before)
        assert native == previous[1] and inputs == previous[2], 'process boundary state changed'
        assert case_read('capture-errors') == []
        response = case_read('response')
        assert type(response['returncode']) is int and response['returncode'] == 0 and response['stderr'] == ''
        assert not any(k in response for k in ('error', 'orchestration_error', 'cleanup_error', 'clock_error', 'response_error'))
        assert type(response['seconds']) in (int, float) and math.isfinite(response['seconds'])
        assert 0 <= response['seconds'] <= 120
        assert case['response_bytes'] == len(response['stdout'].encode())
        events = [loads(line) for line in response['stdout'].splitlines()]
        window = case_read('clock')
        assert window == case['server_window']
        assert window['before_us'] >= windows.get(clone_name, 0), 'process windows overlap'
        result = verify(events, before, native, inputs, case, window)
        final = only(events, event='lineage-history-state', stage='final')
        after, after_native, after_inputs = case_read('after'), case_read('after-native'), case_read('after-inputs')
        same({n: loads(v) for n, v in final['state']['control_tables']}, after)
        assert after_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
        assert after_inputs == dict(final['state']['originals']) == inputs and case_read('backends') == []
        for input_name, identity in after_inputs.items():
            content = raw(folder / clone_name / 'store' / 'producer-inputs-v1' / input_name)
            assert dict(bytes=len(content), sha256=sha(content)) == identity
        assert case_read('custody') == dict(status='OBSERVED_CLOSED_AND_JOINED', database=case['database'],
            backend_absent=True, errors=[])
        result['negative_controls'] = negative_controls(events, before, native, inputs, case, window)
        checked.append(result)
        chains[clone_name], windows[clone_name] = (after, after_native, after_inputs), window['after_us']
    for name, clone in clones.items():
        assert clone['final_case'] == CASES[-1]
        same(chains[name][0], read(folder / (name + '-final-sql.json')))
        spool = folder / name / 'store' / 'producer-inputs-v1'
        assert sorted(p.name for p in spool.iterdir()) == sorted(chains[name][2]), 'unclassified sidecar'
    return dict(status='PASS_SUCCESSOR_ACQUISITION_FIRST_WAVE_REPLAY', source=report['source'], cases=checked,
        snapshots=sum(c['snapshots'] for c in checked), controls=sum(c['negative_controls']['count'] for c in checked),
        bound_files=bound, original_preservation='Pinned bytes and reported SQL/catalog/physical preservation; no fresh engine query',
        scope='Four finite processes, genuine activated-zero claim and unclaimed intent, source closure, bounded whole-intent abandonment and exact retry; no acquisition execution, retirement, installed, failure injection or RSS qualification')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = replay(args.root)
    output = json.dumps(result, indent=2) + '\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(output)
    else:
        print(output, end='')
