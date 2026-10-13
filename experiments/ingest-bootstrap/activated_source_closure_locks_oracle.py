"""Exact two-stream cuts and independently checked relational lock orders."""
import copy
import json
import re

import activated_source_closure_oracle as source
from activated_recovery_faults_oracle import backend_checked, failed_control, bounded_number
from activated_recovery_oracle import inside_window, sha
from initial_succession_oracle import only, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import instant

CASES=('installed-source-job-first','installed-source-source-first')
JOB,ACTOR,REQUEST,DIGEST='\\x'+'fa'*16,'\\x'+'f9'*16,'\\x'+'fb'*16,'\\x'+'fc'*32
GUARDS={
    'require_complete_job_input':'eb45c1dbdbdd820198a5566e628b7e0e52f3e96cbab4993d7e6b10629cc1b271',
    'guard_source_closure_bounds':'b66398eca431bee08e1f6886942b0dfcd2ee164d61f1e2dfccc7651b9063a047',
    'lock_job_admission':'236d8704b19bd1f76e3fcceec55d4ea0945f3fb827cc62fdc9ecbd7c3dde31f4'}


def body_checked(rows):
    assert len(rows)==len(GUARDS) and {r['name'] for r in rows}==GUARDS.keys()
    for row in rows:
        assert row==dict(name=row['name'],source=row['source'],language='plpgsql',security_definer=False,
            configuration=['search_path=pg_catalog' if row['name']=='lock_job_admission' else 'search_path=pg_catalog, control'])
        assert sha(row['source'].encode())==GUARDS[row['name']]


def exact_job(before, private):
    assert set(private)=={'transaction_id','jobs','job_input_bounds','job_admissions'}
    xid=private['transaction_id'];assert isinstance(xid,str) and xid.isdecimal() and 0<int(xid)<2**64
    xmin=str(int(xid)%2**32);fresh_xmin(xmin,before)
    d=only(before['datasets'])
    assert d['topology_generation']==1
    old=only(before['topology_streams'],topology_generation=1,owner_epoch=1)
    new=only(before['topology_streams'],topology_generation=1,owner_epoch=2)
    assert old['handoff_last_complete_sequence']==12 and new['handoff_last_complete_sequence'] is None
    job=dict(job_id=JOB,dataset_id=d['dataset_id'],principal_id=ACTOR,request_key=REQUEST,request_digest=DIGEST,
        kind='export',config_revision=d['config_revision'],policy_revision=d['policy_revision'],created_at_us=1,
        state='accepted',job_revision=1,checkpoint_revision=0,reason_code=None,result_manifest_digest=None,
        owner_resource_id=None,owner_epoch=None,owner_role='control',owner_incarnation=None,completed_at_us=None,
        input_topology_generation=1,input_complete=True,export_job_protocol=None,acquisition_job_protocol=None,xmin=xmin)
    bounds=[dict(job_id=JOB,dataset_id=d['dataset_id'],topology_generation=1,shard_id=0,owner_epoch=epoch,
        last_complete_sequence=cut,xmin=xmin) for epoch,cut in [(1,12),(2,6)]]
    admission=dict(job_id=JOB,admission_ordinal=max(r['admission_ordinal'] for r in before['job_admissions'])+1,xmin=xmin)
    wanted=dict(jobs=[job],job_input_bounds=bounds,job_admissions=[admission])
    same(wanted,{k:private[k] for k in wanted})
    assert all(r['job_id']!=JOB for r in before['jobs'])
    return wanted


def session_checked(value, database, clock):
    backend=value['backend']
    assert set(backend)=={'pid','backend_start','database','user','application','isolation','synchronous_commit'}
    assert type(backend['pid']) is int and backend['pid']>0 and backend['database']==database
    assert backend['user']=='postgres' and backend['isolation']=='read committed' and backend['synchronous_commit']=='on'
    inside_window(instant(backend['backend_start']),clock)
    assert value['identity']==dict(session_replication_role='origin',isolation='read committed',user='postgres',role='postgres')
    return backend


def relation(observations, waiter, holder, database, clock):
    row,=observations
    assert row['pid']==waiter['pid'] and instant(row['backend_start'])==instant(waiter['backend_start'])
    assert row['database']==database and row['user']==waiter['user']
    assert row['wait_event_type']=='Lock' and isinstance(row['wait_event'],str) and row['wait_event']
    assert holder['pid'] in row['blockers'] and row['pid']!=holder['pid']
    assert all(type(pid) is int and pid>0 for pid in row['blockers'])
    assert instant(row['holder_start'])==instant(holder['backend_start']) and row['holder_user']==holder['user']
    inside_window(row['observed_at_us'],clock)
    assert max(instant(row['backend_start']),instant(row['holder_start']))<=row['observed_at_us']
    assert row['waiting_locks']
    for lock in row['waiting_locks']:
        assert set(lock)=={'type','mode','relation','transaction'}
        assert lock['type'] in ('transactionid','tuple','relation','advisory')
        assert isinstance(lock['mode'],str) and lock['mode']
    return row


def fixture_checked(value,before,setup,current,backend,clock,job_first):
    assert value['kind']=='privileged-relational-job' and value['product_job_acceptance'] is False
    assert not any(k in value for k in ('schedule_error','invoke_error','clock_error'))
    assert value['cleanup_errors']==[]
    window(value['window']);assert value['window']['before_us']<=clock['before_us']<=clock['after_us']<=value['window']['after_us']
    assert value['guard_bodies_before']==value['guard_bodies_after'];body_checked(value['guard_bodies_before'])
    assert value['barrier_objects_before']==value['barrier_objects_after']==dict(schema=False,functions=[])
    database=backend['absence']['database']
    job=value['job'];identity=session_checked(job,database,value['window'])
    assert identity['application']==('installed-source-job-first' if job_first else 'installed-source-source-first')+'_job'
    rows=exact_job(before,job['private'])
    actions=job['actions'];expected=['begin','insert','seal','force-constraints','commit'] if job_first else ['begin','insert','seal','commit','rollback']
    assert [a['name'] for a in actions]==expected
    for a in actions:
        assert set(a)=={'name','statement','response'}
        response=a['response'];assert set(response)=={'sqlstate','rows','lines'}
        assert response['sqlstate']==('23514' if not job_first and a['name']=='commit' else '00000')
        assert type(response['rows']) is int and response['rows']>=0
        if a['name'] in ('insert','seal'):assert response['rows']==1
        if a['name']=='begin':assert a['statement']=='BEGIN;'
        if a['name']=='force-constraints':assert a['statement']=='SET CONSTRAINTS ALL IMMEDIATE;'
        if a['name'] in ('commit','rollback'):assert a['statement']==a['name'].upper()+';'
        if a['name']=='rollback':
            # A deferred COMMIT refusal already ends the transaction. The
            # explicit cleanup ROLLBACK can therefore report this exact warning.
            assert response['lines']==[] or len(response['lines'])==1 and re.fullmatch(r'WARNING:\s+25P01',response['lines'][0])
        elif response['sqlstate']=='00000':assert response['lines']==[]
        else:assert any('23514' in line for line in response['lines'])
    # SQL identity is frozen independently; expected rows are separately literal.
    assert sha(actions[1]['statement'].encode())==INSERT_SHA
    assert sha(actions[2]['statement'].encode())==SEAL_SHA
    owner=dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],user='bigbrotr_control')
    if job_first:
        assert value['barrier'] is None and set(value['locks'])=={'job_blocks_cli'}
        same(before,value['uncommitted_external'])
        observed=relation(value['locks']['job_blocks_cli'],owner,identity,database,clock)
        assert 'control.datasets' in observed['query'] and 'FOR UPDATE' in observed['query']
        assert value['catalog_terminal']==setup['catalog_before']
        session_ids=[identity]
    else:
        assert 'uncommitted_external' not in value and set(value['locks'])=={'source_commit','source_blocks_job'}
        barrier=value['barrier'];holder=session_checked(barrier,database,value['window'])
        assert holder['application']=='installed-source-source-first_barrier'
        assert sha(barrier['statement'].encode())==BARRIER_SHA and sha(barrier['remove_statement'].encode())==REMOVE_SHA
        assert barrier['lock']==dict(sqlstate='00000',rows=1,lines=[''])
        assert barrier['unlock']==dict(sqlstate='00000',rows=1,lines=['t'])
        obj=barrier['objects'];assert obj['schema'] is True
        function,=obj['functions']
        assert function==dict(name='wait_for_observer',source=BARRIER_BODY,language='plpgsql',security_definer=False,configuration=None)
        installed=copy.deepcopy(setup['catalog_before'])
        installed['triggers'].append(dict(table='source_streams',name='source_commit_barrier',enabled='O',internal=False,
            function='source_closure_fixture.wait_for_observer()'))
        installed['triggers'].sort(key=lambda r:(r['table'],r['name']))
        installed['constraints'].append(dict(table='source_streams',name='source_commit_barrier',type='t',validated=True,deferrable=True,deferred=True))
        installed['constraints'].sort(key=lambda r:(r['table'],r['name']))
        assert barrier['installed_catalog']==value['catalog_terminal']==installed
        assert barrier['remove_response']['returncode']==0 and barrier['remove_response']['stdout']==barrier['remove_response']['stderr']==''
        observed=relation(value['locks']['source_commit'],owner,holder,database,clock)
        assert observed['query'].strip()=='COMMIT' and observed['wait_event']=='advisory'
        other=relation(value['locks']['source_blocks_job'],identity,owner,database,clock)
        assert other['query'].strip()==actions[1]['statement'] and observed['observed_at_us']<=other['observed_at_us']
        session_ids=[holder,identity]
    assert len(value['sessions'])==len(session_ids)
    for ended,wanted in zip(value['sessions'],session_ids):
        assert set(ended)=={'backend','role','name','wrapper_exit','absence'}
        assert ended['backend']==wanted and ended['role']=='postgres' and ended['name']==wanted['application']
        assert type(ended['wrapper_exit']) is int and ended['absence']=='0'
    diagnostic=value['diagnostic'];target=backend['exact']['expected'] if job_first else {k:identity[k] for k in ('pid','backend_start','database')}
    assert diagnostic['backend']==target and diagnostic['returncode']==0 and diagnostic['observed_sqlstate'] in (None,'23514')
    assert instant(diagnostic['since'])==value['window']['before_us'] and instant(diagnostic['until'])==value['window']['after_us']
    error,=diagnostic['errors'];context,=diagnostic['contexts']
    function,message=(('guard_source_closure_bounds','source closure is below an already recorded source cut') if job_first else ('require_complete_job_input','initial job cut exceeds authoritative source closure'))
    assert re.search(r'\['+str(target['pid'])+r'\] ERROR:  (?:23514: )?'+re.escape(message)+'$',error)
    assert re.search(r'\['+str(target['pid'])+r'\] CONTEXT:  PL/pgSQL function (?:control\.)?'+function+r'\(\) line [1-9][0-9]* at RAISE',context)
    for line in (error,context):inside_window(instant(line.split()[0].replace('Z','+00:00')),value['window'])
    return rows


def verify(before,after,native_before,native_after,inputs_before,inputs_after,input_contents,response,mode,
        *,seed,setup,server_window,backend,fixture):
    assert mode in CASES and seed['name']=='off'
    job_first=mode.endswith('job-first');window(server_window)
    known=source.science(seed)
    assert len(before)==len(after)==72 and setup['operator'] is None and setup['diagnostic'] is None
    same(source.migrated(seed,setup,server_window),before);source.catalogs(setup,'first')
    assert native_before==seed['native'] and native(native_after)==known['closed'] and native_after['sequence']==85
    assert inputs_before==inputs_after==seed['inputs'] and input_contents==seed['contents']
    for name,raw in input_contents.items():
        value=bytes.fromhex(raw);assert inputs_after[name]==dict(bytes=len(value),sha256=sha(value))
    assert only(before['owner_epochs'],role='ingest',epoch=2)['lease_expires_at_us']<server_window['before_us']
    projected,current=(failed_control(before,after,'outcome',server_window) if job_first else control_delta(before,after,'normal',server_window))
    wanted=copy.deepcopy(before)
    producer=only(wanted['producer_epochs'],producer_epoch=2)
    actual=only(projected['producer_epochs'],producer_epoch=2);fresh_xmin(actual['xmin'],before)
    assert actual['xmin']!=current['xmin'];producer.update(state='closing',xmin=actual['xmin'])
    if not job_first:
        stream=only(wanted['source_streams'],owner_epoch=2);actual=only(projected['source_streams'],owner_epoch=2)
        fresh_xmin(actual['xmin'],before);assert actual['xmin'] not in (current['xmin'],producer['xmin'])
        inside_window(actual['closed_at_us'],server_window)
        assert current['lease_expires_at_us']-20_000_000<=actual['closed_at_us']<=current['released_at_us']
        stream.update(state='closed',last_complete_sequence=5,closure_digest='\\x'+known['digest'],closed_at_us=actual['closed_at_us'],xmin=actual['xmin'])
    database=backend_checked(backend,current,server_window)
    assert response['terminal']['config']=='/lab/'+database+'.toml'
    bounded_number(response['seconds'],120)
    job=fixture_checked(fixture,before,setup,current,backend,server_window,job_first)
    if job_first:
        assert only(job['jobs'])['xmin'] not in (current['xmin'],producer['xmin'])
        for table,rows in job.items():wanted[table].extend(rows)
    same(wanted,projected)
    intent=known['intent']
    report=dict(format='bigbrotr-ingest-recovery/1',state='activated-source-closed',dataset_id=intent['dataset_id'][2:],
        owner_resource_id=intent['owner_resource_id'][2:],producer_slot_id=intent['producer_slot_id'][2:],producer_epoch='2',
        issued_through='2',reserved_through='2',resolved_requests='2',source_closure=dict(last_complete_sequence='5',sha256=known['digest']),
        native='closed',owner_release='committed',ready=False)
    source.response_checked(response,mode,report,'producer operation unavailable: Control(Backend)' if job_first else None)
    return dict(status='PASS',mode=mode,observed_states=2,control_tables=72,native_rows=70,native_sequence=85,
        native_writes=1,retained_inputs=6,source_closed=not job_first,job6_committed=job_first,
        source_cut=5,source_digest=known['digest'],authenticated_job=False,workflow_retired=False)


def negative_controls(**context):
    verify(**context);checks=[]
    def reject(name,mutate):
        c=copy.deepcopy(context);mutate(c)
        try:verify(**c)
        except (AssertionError,KeyError,ValueError,TypeError,IndexError):checks.append(name);return
        raise AssertionError('accepted lock counterexample: '+name)
    reject('native-write-extra',lambda c:c['native_after'].update(sequence=86))
    reject('native-close-absent',lambda c:c['native_after'].update(rows=c['seed']['native']['rows']))
    reject('old-input-corrupt',lambda c:c['input_contents'].update({next(iter(c['input_contents'])):'00'}))
    reject('original-outcome-rewritten',lambda c:only(c['after']['producer_outcomes'],producer_epoch=2,request_sequence=2).update(xmin='99999999'))
    reject('source1-rewritten',lambda c:only(c['after']['source_streams'],owner_epoch=1).update(xmin='99999999'))
    reject('activation-rewritten',lambda c:only(c['after']['initial_ingest_activations']).update(activated_by_epoch=99))
    reject('catalog-guard-disabled',lambda c:c['setup']['catalog_after']['triggers'][0].update(enabled='D'))
    reject('fixture-origin-bypass',lambda c:c['fixture']['job']['identity'].update(session_replication_role='replica'))
    reject('source1-cut-wrong',lambda c:only(c['fixture']['job']['private']['job_input_bounds'],owner_epoch=1).update(last_complete_sequence=13))
    reject('source2-cut-vacuous',lambda c:only(c['fixture']['job']['private']['job_input_bounds'],owner_epoch=2).update(last_complete_sequence=5))
    def changed_cut(c):
        only(c['fixture']['job']['private']['job_input_bounds'],owner_epoch=2)['last_complete_sequence']=5
        for row in c['after']['job_input_bounds']:
            if row['job_id']==JOB and row['owner_epoch']==2:row['last_complete_sequence']=5
    reject('coherent-vacuous-cut',changed_cut)
    reject('input-stream-omitted',lambda c:c['fixture']['job']['private']['job_input_bounds'].pop())
    reject('authenticated-export-invented',lambda c:only(c['fixture']['job']['private']['jobs']).update(export_job_protocol=1))
    key=next(iter(context['fixture']['locks']))
    reject('blocker-tuple-wrong',lambda c:c['fixture']['locks'][key][0].update(holder_start='2000-01-01T00:00:00+00:00'))
    reject('blocker-not-observed',lambda c:c['fixture']['locks'][key][0].update(blockers=[]))
    reject('waiter-tuple-wrong',lambda c:c['fixture']['locks'][key][0].update(backend_start='2000-01-01T00:00:00+00:00'))
    reject('wrong-diagnostic',lambda c:c['fixture']['diagnostic'].update(errors=['unrelated error']))
    reject('backend-not-excluded',lambda c:c['backend']['exact'].update(present=True,stdout='t\n'))
    reject('CLI-unjoined',lambda c:c['response']['terminal'].update(reaped=False))
    reject('fixture-session-not-excluded',lambda c:c['fixture']['sessions'][0].update(absence='1'))
    if context['mode'].endswith('job-first'):
        reject('durable-job-missing',lambda c:c['after']['jobs'].remove(only(c['after']['jobs'],job_id=JOB)))
        reject('unexpected-source-close',lambda c:only(c['after']['source_streams'],owner_epoch=2).update(state='closed'))
    else:
        reject('closure-digest-corrupt',lambda c:only(c['after']['source_streams'],owner_epoch=2).update(closure_digest='\\x'+'00'*32))
        reject('job-rollback-lost',lambda c:c['after']['jobs'].append(copy.deepcopy(only(c['fixture']['job']['private']['jobs']))))
        reject('barrier-not-removed',lambda c:c['fixture']['barrier_objects_after'].update(schema=True))
        reject('unrelated-rollback-warning',lambda c:c['fixture']['job']['actions'][-1]['response'].update(lines=['WARNING:  99999']))
    return dict(status='PASS',count=len(checks),rejected=checks)


# Reviewed fixed SQL; expectations above separately enumerate every row.
INSERT_SHA = 'f461319c4fad5464daa12dbc4cb8aec6ba15aa3c8a922ee4908e69cfd61f1d27'
SEAL_SHA = '03340bacfdb45f0e78a0ffc0a8d8f40d2bc35d7a902c87e9d16667c5cce5d1db'
BARRIER_SHA = '0873d921322791e94e3a1b98890cf7138d29bd08d43753a3f7f594df1d8c1685'
REMOVE_SHA = 'b7b4f4f884e2b2abd855f17ffe24bce8f80de8859eb79a8b25a7f432676ae440'
BARRIER_BODY = ' BEGIN\n            PERFORM pg_advisory_xact_lock(180023441, 20261002);\n            RETURN NEW;\n        END '
