"""Independent complete-row expectations for relational retirement guard attempts.

A successful raw shape does not supply native authority. Deliberate missing-copy
and missing-gap baselines are proven single deletions, not normalized away.
"""
import copy
import hashlib
import math
import re

import activated_retirement_oracle as retirement
from activated_acquisition_guards_oracle import parsed_events, absence_checked
from initial_succession_oracle import only, same
from installed_empty_oracle import fresh_xmin, window
from installed_live_oracle import instant

MODE='activated-retirement-guards'
CASES=('ready-rollback','complete-rollback','producer-only','domain-only','role-only','owner-only',
    'wrong-producer-digest','wrong-common-time','wrong-domain-digest','immediate-before',
    'role-created-after-immediate','role-time-after-immediate','role-delete','role-truncate',
    'role-truncate-cascade','legacy-grants','missing-copy','missing-gap')
POSITIVE={'ready-rollback','complete-rollback','legacy-grants'}
PARTIAL=('activated retirement requires exactly ready or complete terminal metadata','control.require_activated_initial_retirement(bytea)')
ROLE=('activated target role permits only exact retirement','control.guard_activated_retirement_role()')
MISSING=('activated retirement requires exact copies and supported workflow obligations','control.lock_activated_initial_retirement(bytea)')
ERRORS={
    'producer-only':PARTIAL,'domain-only':PARTIAL,'role-only':PARTIAL,'wrong-common-time':PARTIAL,
    'wrong-domain-digest':PARTIAL,'immediate-before':PARTIAL,
    'owner-only':('ingest owner release requires closed domains, streams and producer epochs','guard_owner_ingest_dependents()'),
    'wrong-producer-digest':('activated retirement requires exact atomic transition staging','control.require_activated_initial_retirement_transition(bytea,bytea,bigint)'),
    'role-created-after-immediate':ROLE,'role-time-after-immediate':ROLE,
    'role-delete':('activated target role history cannot be deleted','control.guard_activated_retirement_role()'),
    'role-truncate-cascade':('activated target role history cannot be truncated','control.guard_activated_retirement_role()'),
    'missing-copy':MISSING,'missing-gap':MISSING,
}
# Exact reviewed scripts include all 72 read-only table captures and the fixed
# retained identities. Runtime clocks/backend identities remain actual SQL.
SQL_SHA256={
    'ready-rollback': '33fe5a23c10209dd7e11a81da4cdf2fb5f3d89d043a596022b6044d641e4f3ae',
    'complete-rollback': '6f310d702856eb797069bc19a3b1c2689ee2fbdc8efad0220ea764792ab5575e',
    'producer-only': 'a6aab9fb0e55ca17be3edd72e04b1516ab5c37b96c7abef5884a769845439b4f',
    'domain-only': '2b0dd4163c36ca7fbd55cc370cdff81a44e43e81a393a5020959806b818a6f8f',
    'role-only': '4fb505aaebfe56c859ad85030a9beb4ffa36f0bcf2adbbf98b47a2848639c6db',
    'owner-only': '7cf766bbb4dd568c200647146d68e4dff69447b996aeba76edd5dacb194e34d0',
    'wrong-producer-digest': '5c56b89f77b626f4328879404c80bd203d5c40ae1c5b30a3f38d73f794c47839',
    'wrong-common-time': '3a1cb2d85ac6ca3f4805b4eaceff1dc8bc9e4a43901d87d52c5a39e2df5aa809',
    'wrong-domain-digest': '6faba93dc07b52dea96d2e09ab0a53724fa3ee352261f4c21443fa7a8b15b5c9',
    'immediate-before': '9b9fd0d0b15d4656033547adcffd49d04595f6dc7d4a6674110b178c30cad8d4',
    'role-created-after-immediate': '07404b5f15f7dce3811227dda8f390cde54d0f82760e2060d64451e549edae96',
    'role-time-after-immediate': '33321d2551ff266d1d8151d8e8accd826b8f59508353b2dfcfb962ed2a504461',
    'role-delete': '90e82a9e636af9c8f88ea7c8bee7500d52cf69691b103ffc8080081aa35e3a56',
    'role-truncate': 'e0b6c2304c9f5e9f5b0c249f0ba3142c37f3763f118bf65afb915f5aa44e5a8e',
    'role-truncate-cascade': 'c5a0c5d293b6dd75c4e5fd7797916b1946c9ddd0e688f401dc8b4a51398727a4',
    'legacy-grants': '2d6584bf7161efb9fccd61aa2bd69122a8111c55c7537814a96583b025dc23fb',
    'missing-copy': '2610989e36e0199a6263de5b27a7504af7e9c2cb45166675d9c0078d1f9f7782',
    'missing-gap': 'bab5f32ecce6f91c8ebd83f3b2043e0da98650945b89104ddb3d564c6fe1b8d1',
}
GRANT='GRANT DELETE ON control.role_instances TO bigbrotr_control;\nGRANT TRUNCATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control;\n'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def identity(mode,kind):
    return sha(('bigbrotr/empty-sql-fixture/1\0'+MODE+'-'+mode+'\0'+kind).encode())[:32]


def response_ok(value):
    assert value==dict(returncode=0,stdout='',stderr='',outcome='PROCESS_EXITED')


def grants_checked(clone):
    value=clone['grants']
    assert value['statement']==GRANT
    response_ok(value['response'])
    assert value['before']==clone['setup']['catalog_after']
    before=value['privileges_before']
    assert set(before)==set(clone['baseline']) and all(type(x) is bool for x in before.values())
    assert value['privileges_after']=={name:True for name in before}
    expected=copy.deepcopy(value['before'])
    for row in expected['retirement']['privileges'].values():
        row['truncate']=True
    expected['retirement']['privileges']['role_instances']['delete']=True
    expected['acquisition']['privileges']['gap_truncate']=True
    assert value['after']==expected
    return expected


def baseline_checked(seed,clone,clock):
    assert seed['name']=='off'
    known=retirement.seed_checked(seed)
    expected=retirement.migrated(seed,clone['setup'],clock)
    same(clone['baseline'],expected)
    assert clone['native']==seed['native'] and len(expected)==72
    assert clone['native']['sequence']==85
    assert clone['files'] and all(isinstance(n,str) and not n.startswith('/') and '..' not in n.split('/') for n in clone['files'])
    assert all(r['state']=='released' for r in expected['owner_epochs'] if r['role']=='control')
    return known,expected,grants_checked(clone)


def missing_baseline(operator,mode,baseline):
    expected=copy.deepcopy(baseline)
    if not mode.startswith('missing-'):
        assert operator is None
        return expected
    assert set(operator)=={'statement','before','response','after'}
    if mode=='missing-copy':
        table,predicate='producer_outcomes','producer_epoch=2 AND request_sequence=2'
        removed=only(expected[table],producer_epoch=2,request_sequence=2)
    else:
        table,predicate='acquisition_gaps','producer_epoch=2'
        removed=only(expected[table],producer_epoch=2)
    sql="BEGIN;\nSET LOCAL statement_timeout='10s';\nSET LOCAL lock_timeout='5s';\nSET LOCAL session_replication_role=replica;\nDELETE FROM control."+table+' WHERE '+predicate+" RETURNING (to_jsonb("+table+")||jsonb_build_object('xmin',xmin::text))::text;\nCOMMIT;\n"
    assert operator['statement']==sql
    same(operator['before'],baseline)
    answer=operator['response']
    assert answer['returncode']==0 and answer['stderr']=='' and answer['outcome']=='PROCESS_EXITED'
    assert parsed_events(answer['stdout'])==[removed]
    expected[table].remove(removed)
    same(operator['after'],expected)
    return expected


def legacy_catalog(case,catalog):
    if case['mode']!='legacy-grants':
        assert case['legacy'] is None
        return catalog
    value=case['legacy']
    assert set(value)=={'statement','before','response','after'}
    assert value['statement']=='\n'.join(s.replace('GRANT EXECUTE','REVOKE EXECUTE').replace(' TO ',' FROM ') for s in retirement.GRANTS)+'\n'
    assert value['before']==catalog
    response_ok(value['response'])
    expected=copy.deepcopy(catalog)
    for row in expected['retirement']['functions']:
        if row['name'] in retirement.CATALOG_FUNCTIONS[1:4]:
            row['execute']=False
    assert value['after']==expected
    return expected


def apply_target(rows,mode,at,xmin):
    owner,role,producer,domains=retirement.target_rows(rows)
    source=only(rows['source_streams'],owner_epoch=2)
    if mode=='producer-only':
        producer.update(state='retired',retired_at_us=at,closure_digest=source['closure_digest'],xmin=xmin)
    elif mode=='domain-only':
        only(domains,dictionary_kind=1).update(state='closed',closed_at_us=at,closure_digest=source['closure_digest'],xmin=xmin)
    elif mode=='role-only':
        role.update(retired_at_us=at,xmin=xmin)
    else:
        producer.update(state='retired',retired_at_us=at,closure_digest=source['closure_digest'],xmin=xmin)
        owner.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest=source['closure_digest'],xmin=xmin)
        role.update(retired_at_us=at+(mode=='wrong-common-time'),xmin=xmin)
        digest=source['closure_digest']
        if mode=='wrong-domain-digest':
            raw=bytearray.fromhex(digest[2:]);raw[0]^=1;digest='\\x'+raw.hex()
        for row in domains:
            row.update(state='closed',closed_at_us=at,closure_digest=digest,xmin=xmin)


def verify_case(case,*,seed,clone):
    mode=case['mode'];assert mode in CASES
    clock=case['server_window'];window(clock)
    known,baseline,catalog=baseline_checked(seed,clone,clock)
    baseline=missing_baseline(case['operator'],mode,baseline)
    catalog=legacy_catalog(case,catalog)
    assert case['capture_errors']==[]
    assert sha(case['statement'].encode())==case['statement_sha256']==SQL_SHA256[mode]
    same(case['before'],baseline);same(case['after'],baseline)
    assert case['catalog_before']==case['catalog_after']==catalog
    assert case['privileges_before']==case['privileges_after']==clone['grants']['privileges_after']
    assert case['files_before']==case['files_after']==clone['files']
    assert case['inputs_before']==case['inputs_after']==seed['inputs']
    assert case['contents_before']==case['contents_after']==seed['contents']
    database=case['database'];assert database==clone['database'] and re.fullmatch('[a-z][a-z0-9_]{0,62}',database)
    command=case['command'];assert len(command)==19 and command[4]
    assert command==['exec','-i','--env','PGCONNECT_TIMEOUT=5',command[4],'psql','-X','-A','-t','-q','--no-password',
        '-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','bigbrotr_control','-d',database]
    response=case['response'];assert response['outcome']=='PROCESS_EXITED' and type(response['returncode']) is int
    assert type(response['seconds']) in (int,float) and math.isfinite(response['seconds']) and 0<=response['seconds']<=35
    events=parsed_events(response['stdout']);assert events==case['events']
    backend=events[0];at=backend['fixture_time']
    previous=max((r for r in baseline['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    assert type(at) is int and clock['before_us']<=at<=clock['after_us'] and at>=known['source']['closed_at_us']
    assert type(backend['pid']) is int and backend['pid']>0 and clock['before_us']<=instant(backend['backend_start'])<=at
    assert backend==dict(event='backend',pid=backend['pid'],backend_start=backend['backend_start'],database=database,
        user='bigbrotr_control',role='bigbrotr_control',superuser=False,isolation='read committed',control_epoch=previous['epoch']+1,
        incarnation=identity(mode,'incarnation'),role_instance=identity(mode,'role'),replication_role='origin',fixture_time=at)
    states=[e for e in events if e['event']=='state']
    phases=['baseline']
    if mode in ('producer-only','domain-only','role-only'):phases+=['partial']
    if mode in ('complete-rollback','wrong-common-time','wrong-domain-digest') or mode.startswith('role-') and mode!='role-only':
        phases+=['terminal']
        if mode not in ('wrong-common-time','wrong-domain-digest'):phases+=['checked']
    if mode=='legacy-grants':phases+=['legacy']
    assert [e['stage'] for e in states]==phases
    xid=states[0]['transaction_id'];assert isinstance(xid,str) and xid.isdecimal() and 0<int(xid)<1<<64
    xmin=str(int(xid)%(1<<32));fresh_xmin(xmin,baseline)
    owner=dict(previous,epoch=previous['epoch']+1,role_instance_id='\\x'+backend['role_instance'],process_incarnation='\\x'+backend['incarnation'],
        acquired_at_us=at,lease_expires_at_us=at+20_000_000,state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,
        archive_frontier_protocol=None,control_fence_version=1,control_backend_pid=backend['pid'],control_backend_start=backend['backend_start'],xmin=xmin)
    role=dict(role_instance_id=owner['role_instance_id'],dataset_id=owner['dataset_id'],role='control',created_at_us=at,retired_at_us=None,xmin=xmin)
    assert not any(r['process_incarnation']==owner['process_incarnation'] for r in baseline['owner_epochs'])
    assert not any(r['role_instance_id']==role['role_instance_id'] for r in baseline['role_instances'])
    claim=copy.deepcopy(baseline);claim['owner_epochs'].append(owner);claim['role_instances'].append(role)
    observed=at
    for state in states:
        stamp=state['observed_us'];assert type(stamp) is int and observed<=stamp<=clock['after_us'] and stamp<at+20_000_000
        observed=stamp
        expected=copy.deepcopy(claim)
        if state['stage'] in ('partial','terminal','checked'):apply_target(expected,mode,at,xmin)
        elif state['stage']=='legacy':
            actual=only(expected['owner_epochs'],role='control',epoch=owner['epoch'])
            actual.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest='\\x'+sha(bytes.fromhex(backend['incarnation'])))
            only(expected['role_instances'],role_instance_id=role['role_instance_id']).update(retired_at_us=at)
            old=only(expected['owner_epochs'],role='ingest',epoch=1)
            only(expected['role_instances'],role_instance_id=old['role_instance_id']).update(xmin=xmin)
        assert set(state)=={'event','stage','transaction_id','observed_us','rows'} and state['transaction_id']==xid
        same(state['rows'],expected)
    wanted=[backend,states[0]]
    if mode not in ('missing-copy','missing-gap','legacy-grants'):
        wanted.append(dict(event='classified',stage='ready',terminal_at=None))
    if mode=='immediate-before':wanted.append(dict(event='immediate'))
    if len(states)>1:
        wanted.append(states[1])
    if len(states)>2:
        wanted += [dict(event='classified',stage='complete',terminal_at=at),states[2]]
    if mode in POSITIVE:wanted.append(dict(event='rolled-back'))
    elif mode in ('producer-only','domain-only','role-only','wrong-common-time','wrong-domain-digest'):
        wanted.append(dict(event='before-commit'))
    assert events==wanted
    if mode in POSITIVE:
        assert response['returncode']==0 and response['stderr']==''
        sqlstate='00000'
    elif mode=='role-truncate':
        assert response['returncode']==3 and len(re.findall(r'ERROR:',response['stderr']))==1
        assert re.search(r'ERROR:\s+0A000: cannot truncate a table referenced in a foreign key constraint\s*\n',response['stderr'])
        assert 'references "role_instances"' in response['stderr'] and 'PL/pgSQL function' not in response['stderr']
        sqlstate='0A000'
    else:
        message,function=ERRORS[mode]
        assert response['returncode']==3 and len(re.findall(r'ERROR:',response['stderr']))==1
        assert re.search(r'ERROR:\s+23514: '+re.escape(message)+r'\s*\n',response['stderr'])
        assert 'function '+function+' line ' in response['stderr']
        sqlstate='23514'
    absence_checked(case['backend_absence'],backend,database)
    return dict(name=mode,status='PASS',observed_states=len(states),sqlstate=sqlstate,control_tables=72,native_authority=False)


def verify(*,seed,clones,cases):
    assert [c['mode'] for c in cases]==list(CASES)
    assert len(clones)==3 and len({c['database'] for c in clones})==3
    checked=[]
    for case in cases:
        clone=only(clones,database=case['database'])
        checked.append(verify_case(case,seed=seed,clone=clone))
    assert all(a['server_window']['after_us']<=b['server_window']['before_us'] for a,b in zip(cases,cases[1:]))
    for clone in clones:
        assert clone['native_after']==clone['native']==seed['native']
        prior=clone['grants']['privileges_before']
        revoke='REVOKE DELETE ON control.role_instances FROM bigbrotr_control;\nREVOKE TRUNCATE ON '+','.join('control.'+t for t in sorted(prior) if not prior[t])+' FROM bigbrotr_control;\n'
        assert clone['restored']==dict(statement=revoke,response=dict(returncode=0,stdout='',stderr='',outcome='PROCESS_EXITED'),
            catalog=clone['grants']['before'],privileges=prior,errors=[])
        legacy=[c for c in cases if c['database']==clone['database'] and c['mode']=='legacy-grants']
        if legacy:
            assert clone['legacy_restore']==dict(statement='\n'.join(retirement.GRANTS)+'\n',
                response=dict(returncode=0,stdout='',stderr='',outcome='PROCESS_EXITED'),catalog=clone['grants']['after'])
        else:assert clone['legacy_restore'] is None
    return dict(status='PASS',attempts=len(cases),positive=3,refusals=15,observed_states=sum(c['observed_states'] for c in checked),
        control_tables=72,native_rows=70,native_sequence=85,original_inputs=6,catalog_restored=True,cases=checked)


def negative_controls(**context):
    verify(**context)
    checks=[]
    def reject(name,mutate):
        value=copy.deepcopy(context);mutate(value)
        try:verify(**value)
        except (AssertionError,KeyError,ValueError,TypeError,IndexError,StopIteration):checks.append(name);return
        raise AssertionError('guard counterexample accepted: '+name)
    def case(c,mode='complete-rollback'):return only(c['cases'],mode=mode)
    def change_events(c,change,mode='complete-rollback'):
        row=case(c,mode);change(row['events'])
        import json
        row['response']['stdout']=''.join(json.dumps(e)+'\n' for e in row['events'])
    reject('missing-attempt',lambda c:c['cases'].pop())
    reject('unknown-wrapper',lambda c:case(c)['response'].update(outcome='UNKNOWN_TIMEOUT',returncode=None))
    reject('missing-clock',lambda c:case(c)['server_window'].pop('after_us'))
    reject('lost-capture',lambda c:case(c)['capture_errors'].append('after:TimeoutExpired'))
    reject('native-write',lambda c:c['clones'][0]['native_after'].update(sequence=86))
    reject('original-input-change',lambda c:case(c)['contents_after'].pop(next(iter(c['seed']['contents']))))
    reject('source-xmin-change',lambda c:only(case(c)['after']['source_streams'],owner_epoch=2).update(xmin='999999999'))
    reject('gap-recorder-change',lambda c:only(case(c)['after']['acquisition_gaps'],producer_epoch=2).update(issuer_epoch=13))
    reject('unrestored-privilege',lambda c:c['clones'][0]['restored']['privileges'].update(role_instances=True))
    reject('legacy-new-function-grant',lambda c:only(case(c,'legacy-grants')['catalog_before']['retirement']['functions'],name=retirement.CATALOG_FUNCTIONS[1]).update(execute=True))
    reject('unproved-missing-copy',lambda c:case(c,'missing-copy')['operator']['before']['producer_outcomes'].pop())
    reject('generic-error',lambda c:case(c,'producer-only')['response'].update(stderr='ERROR: 23514: unrelated\n'))
    reject('wrong-truncate-boundary',lambda c:case(c,'role-truncate')['response'].update(stderr='ERROR: 23514: activated target role history cannot be truncated\n'))
    reject('wrong-db-command',lambda c:case(c)['command'].__setitem__(-1,'wrong_database'))
    reject('backend-present',lambda c:case(c)['backend_absence']['samples'][-1]['present'].append(dict(pid=1,
        backend_start=case(c)['events'][0]['backend_start'],database=case(c)['database'],user='bigbrotr_control')))
    reject('replica-case',lambda c:change_events(c,lambda e:e[0].update(replication_role='replica')))
    reject('superuser-case',lambda c:change_events(c,lambda e:e[0].update(superuser=True)))
    reject('split-target-xmin',lambda c:change_events(c,lambda e:only(e[-2]['rows']['role_instances'],
        role_instance_id=only(e[-2]['rows']['owner_epochs'],role='ingest',epoch=2)['role_instance_id']).update(xmin='999999999')))
    reject('changed-old-source',lambda c:change_events(c,lambda e:only(e[-2]['rows']['source_streams'],owner_epoch=1).update(last_complete_sequence=0)))
    def retime(c):
        def edit(events):
            for event in events:
                if event['event']=='state' and event['stage']!='baseline':
                    apply_target(event['rows'],'complete-rollback',events[0]['fixture_time']+1,event['transaction_id'])
        change_events(c,edit)
    reject('coherent-wrong-terminal-time',retime)
    def rehash(c):
        row=case(c);row['statement']+='-- altered\n';row['statement_sha256']=sha(row['statement'].encode())
    reject('rehashed-unreviewed-script',rehash)
    return dict(status='PASS',rejected=len(checks),controls=checks)
