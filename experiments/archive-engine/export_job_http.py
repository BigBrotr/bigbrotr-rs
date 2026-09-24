#!/usr/bin/env python3
"""Actual private job client. Host-owned SQL verifies durable effects separately."""
from http.cookies import SimpleCookie
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import private_grant_http as http
from signed_publication_oracle import EVENT

SCOPE = dict(dataset_id='01'*16, dictionary_kind=4, allocation_domain='04'*16, ordinal='1')
DESTINATION = http.ROOT/'destination'


def command():
    return dict(kind='export', format='signed-events-jsonl',
        query=dict(version=1, resource='event_history', mode='snapshot', scope=SCOPE,
                   filters=[dict(ids=[EVENT['id']])]),
        allocation=dict(catalog_id='5a'*16, frontier_revision='1'),
        destination=dict(id='e1'*16, binding_sha256=hashlib.sha256((DESTINATION/'destination.json').read_bytes()).hexdigest() if DESTINATION.exists() else '00'*32),
        expected_config_revision='1', expected_policy_revision='2')


def login():
    status, _, headers = http.request('POST','/management/v1/sessions',dict(username='alice',password=http.PASSWORD))
    assert status==201
    cookies=SimpleCookie()
    for name,value in headers:
        if name.lower()=='set-cookie':cookies.load(value)
    auth={'Cookie':'BigBrotrLocalSession='+cookies['BigBrotrLocalSession'].value,
          'X-CSRF-Token':cookies['BigBrotrLocalCsrf'].value}
    descriptor=os.open(http.ROOT/'session.private',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(descriptor,'w') as stream:json.dump(auth,stream)
    return auth


def unselected():
    server=http.Server('no-destination-812')
    try:
        server.ready();auth=login()
        assert http.request('POST','/management/v1/jobs',command(),{**auth,'Idempotency-Key':'d0'*16})[0]==503
        assert http.request('GET','/management/v1/me',auth=auth)[0]==200
        server.stop()
    finally:server.cleanup()
    print(json.dumps(dict(status='PASS',case='unselected-destination-keeps812-session-runtime-healthy')))


def destination():
    DESTINATION.mkdir(mode=0o700)
    with http.CONFIG.open('a') as stream:
        stream.write(f'\n[export.destination]\nid="{"e1"*16}"\nroot="{DESTINATION}"\n')
    http.cli('config','validate')
    observed=http.cli('export','destination-initialize')
    assert observed['binding_sha256']==command()['destination']['binding_sha256']


def exercise(mode='ordinary'):
    assert mode in ('ordinary','checker','commits')
    auth=json.loads((http.ROOT/'session.private').read_text())
    principal=(http.ROOT/'principal').read_text()
    body=command();key='d1'*16
    server=http.Server('export-job')
    cases=[]
    def case(name):
        cases.append(dict(case=name,status='PASS'))
        (http.ROOT/'export-job-http-cases.json').write_text(json.dumps(cases,indent=2)+'\n')
    def job(value=body, request_key=key, credentials=None):
        return http.request('POST','/management/v1/jobs',value,
            {**(auth if credentials is None else credentials),'Idempotency-Key':request_key})
    def barrier(name):
        (http.ROOT/(name+'.waiting')).write_text('waiting\n')
        end=time.monotonic()+30
        while not (http.ROOT/(name+'.ready')).exists():
            if time.monotonic()>=end:raise TimeoutError(name)
            time.sleep(.02)
    def grant(action, enabled, revision, request_key):
        value=dict(principal_id=principal,action=action,enabled=enabled,scope=SCOPE,
            expected_config_revision='1',expected_policy_revision='2',expected_grant_revision=str(revision),
            allocation=body['allocation'] if enabled else None)
        headers={**auth,'Idempotency-Key':request_key}
        status,proof,_=http.request('POST','/management/v1/sessions/current/reauthenticate',dict(password=http.PASSWORD,command=value),headers)
        assert status==201,(status,proof)
        status,receipt,_=http.request('PUT',f'/management/v1/principals/{principal}/grants',dict(command=value,proof_id=proof['proof_id']),headers)
        assert status==200 and receipt['created'] and receipt['account_grant_revision']==str(revision+1)
    bundle,unavailable=Path('/out/archive-worker'),Path('/out/archive-worker-unavailable')
    marker=DESTINATION/'destination.json';moved=DESTINATION/'held-marker'
    try:
        server.ready()
        assert job(credentials={})[0]==401
        assert job(credentials={**auth,'X-CSRF-Token':'00'*32})[0]==401
        assert job()[0]==403
        for revision,action in enumerate(['history.read','export','private-export'],1):
            grant(action,True,revision,format(0xc0+revision,'02x')*16)
            if revision<3:assert job()[0]==403
        case('new-export-needs-live-session-csrf-and-each-independent-scope-action')
        if mode=='commits':
            server.stop()
            print(json.dumps(dict(status='PASS',cases=len(cases))))
            return
        if mode=='checker':
            server.stop()
            from export_destination_blocked import exercise as blocked
            blocked(auth,body)
            case('actual-blocked-destination-renews-control-and-signal-fails-stop-within-bound')
            print(json.dumps(dict(status='PASS',cases=len(cases))))
            return
        assert job({**body,'expected_policy_revision':'1'})[0]==409
        bad={**body,'destination':{**body['destination'],'binding_sha256':'00'*32}}
        assert job(bad)[0]==409
        case('revision-and-destination-digest-conflict-without-acceptance')
        marker.rename(moved)
        assert job()[0]==503
        assert http.request('GET','/management/v1/me',auth=auth)[0]==200
        moved.rename(marker)
        case('changed-destination-refuses-under-supervised-checker-and-keeps-control-healthy')
        bundle.rename(unavailable)
        assert job()[0]==503
        assert http.request('GET','/management/v1/me',auth=auth)[0]==200
        unavailable.rename(bundle)
        assert not list((http.ROOT/'scratch').iterdir())
        barrier('before-accept')
        case('missing-native-bundle-leaves-no-job-or-retained-reader-pin')
        status,receipt,headers=job()
        assert status==202 and receipt['created'],(status,receipt)
        assert set(receipt)=={'format','command_key','job_id','accepted_at_us','created','execution','authority'}
        assert receipt['format']=='bigbrotr-job-acceptance/1' and receipt['command_key']==key
        assert receipt['authority']=='acceptance-only' and receipt['execution']=='unobserved'
        assert len(receipt['job_id'])==32 and int(receipt['job_id'],16)>0
        assert isinstance(receipt['accepted_at_us'],str) and int(receipt['accepted_at_us'])>0
        assert not any(k.lower()=='set-cookie' for k,_ in headers)
        (http.ROOT/'export-accepted.json').write_text(json.dumps(dict(command=body,key=key,receipt=receipt),indent=2)+'\n')
        barrier('accepted')
        case('real-native-allocation-accepts-one-job-and-export-pin-atomically')
        historical={**receipt,'created':False}
        assert job()[:2]==(200,historical)
        assert job({**body,'query':{**body['query'],'page_size':99}})[0]==409
        barrier('retried')
        case('same-key-retry-preserves-durable-rows-and-changed-query-conflicts')
        server.stop()
        config=http.CONFIG.read_text()
        assert '[archive.reader]\nenabled=true' in config
        http.CONFIG.write_text(config.split('\n[export.destination]\n')[0].replace('[archive.reader]\nenabled=true','[archive.reader]\nenabled=false'))
        server=http.Server('export-job-restart-no-reader');server.ready()
        assert job()[:2]==(200,historical)
        assert job(request_key='d2'*16)[0]==503
        barrier('restarted')
        case('restart-with-destination-and-reader-removed-resolves-original-receipt')
        grant('export',False,4,'c4'*16)
        assert job()[:2]==(200,historical)
        assert job(request_key='d2'*16)[0]==403
        assert http.request('GET','/management/v1/me',auth=auth)[0]==200
        server.stop()
        assert not list((http.ROOT/'scratch').iterdir())
        case('revocation-does-not-reapply-or-release-original-export-obligation')
    finally:
        server.cleanup()
        if unavailable.exists():unavailable.rename(bundle)
        if moved.exists():moved.rename(marker)
    print(json.dumps(dict(status='PASS',cases=len(cases))))


if __name__=='__main__':
    {'unselected':unselected,'destination':destination,'exercise':exercise,'checker':lambda:exercise('checker'),'commits':lambda:exercise('commits')}[sys.argv[1]]()
