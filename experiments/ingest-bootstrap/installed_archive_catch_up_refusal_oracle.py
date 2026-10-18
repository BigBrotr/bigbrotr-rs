"""Independent retained-state expectations for one installed refusal.

The original nine phases reuse their literal science reducer. Corruption is
modeled separately from application effects; failed Control is retained active.
Observed identifiers, times and xmins parameterize only explicitly named rows.
"""
import copy
import json
import math
from pathlib import Path
import re
import stat
import tomllib

from installed_archive_catch_up_client import argv
from installed_archive_catch_up_fault_oracle import (prefix,control,receipts_for,
    normal,Model,only,same,sha,be,binary,clock,fresh_xmin,instant,cut_digest,COUNTS)

CASES=('current-join','orphan-history','family-counts','missing-input','frontier-cancel','frontier-control-loss')
WAIT_CASES=CASES[-2:]
ERRORS={
    'current-join':'error: Archive catch-up refused; preserve original state: InvalidMetadata\n',
    'orphan-history':'error: Archive catch-up refused; preserve original state: InvalidMetadata\n',
    'family-counts':'error: Archive catch-up refused; preserve original state: "retained frontier family counts differ from actual original prefix"\n',
    'missing-input':'error: Archive catch-up refused; preserve original state: "issued attempt has no original input linkage"\n',
    'frontier-cancel':'error: initial Ingest cancelled; original attempt may need recovery\n',
    'frontier-control-loss':'error: Archive catch-up refused; preserve original state: Backend\n',
}


def process_checked(response,case,config,name):
    assert response['outcome']=='PROCESS_EXITED' and type(response['returncode']) is int and response['returncode']==4
    assert response['stdout']=='' and response['stderr']==ERRORS[case]
    t=response['terminal']
    assert t['command']==argv('catchup',Path(config)) and t['config']==config and t['prefix']=='/lab/'+name
    assert t['reaped'] is True and t['forced'] is False and t['error'] is None and t['exit_code']==4
    assert all(type(t[k]) is int and t[k]>0 for k in ('pid','start_ticks'))
    assert type(t['elapsed_seconds']) in (int,float) and math.isfinite(t['elapsed_seconds']) and 0<t['elapsed_seconds']<=105


def mutation_checked(value,case,previous):
    before,after=value['before'],value['after']
    semantic=lambda state:{k:v for k,v in state.items() if k not in ('physical','local_records')}
    assert semantic(before)==semantic(previous)
    for area in ('data','state'):assert before['physical'][area]==previous['physical'][area]
    assert value['case']==case and value['schema_before']==value['schema_after']
    schema=value['schema_before'];assert set(schema)=={'constraints','triggers'} and schema['constraints'] and schema['triggers']
    assert all(r['tgenabled'] in ('O','A') for r in schema['triggers'])
    expected=copy.deepcopy(before);wanted=expected['control'];consumer=only(wanted['consumers']);catalog=consumer['consumer_id']
    if case in CASES[:3]:
        response=value['response'];assert response['returncode']==0 and not response['stderr']
        lines=response['stdout'].splitlines();assert len(lines)==2 and lines[1]=='origin'
        tx=json.loads(lines[0]);assert set(tx)=={'xid','role','replication_role','pid'}
        assert tx['role']=='postgres' and tx['replication_role']=='replica' and type(tx['pid']) is int and tx['pid']>0
        fresh_xmin(tx['xid'],wanted)
        target="decode('"+catalog[2:]+"','hex')"
        if case=='current-join':
            change=f'UPDATE control.consumers SET current_revision=current_revision+1 WHERE consumer_id={target}'
            consumer.update(current_revision=consumer['current_revision']+1,xmin=tx['xid'])
        elif case=='orphan-history':
            change=f'DELETE FROM control.consumers WHERE consumer_id={target}'
            wanted['consumers']=[]
        else:
            change=f'UPDATE control.archive_frontier_snapshots SET dictionary_count=dictionary_count-1,payload_count=payload_count+1 WHERE consumer_id={target} AND frontier_revision=(SELECT current_revision FROM control.consumers WHERE consumer_id={target})'
            row=only(wanted['archive_frontier_snapshots'],frontier_revision=consumer['current_revision'])
            assert row['dictionary_count']==8 and row['payload_count']==1
            row.update(dictionary_count=7,payload_count=2,xmin=tx['xid'])
        assert value['sql']=="BEGIN; SET LOCAL session_replication_role=replica;\n"+change+";\n"+"""SELECT json_build_object('xid',pg_current_xact_id()::text,'role',current_user,
 'replication_role',current_setting('session_replication_role'),'pid',pg_backend_pid());
COMMIT;
SELECT current_setting('session_replication_role');
"""
        assert value['quarantine'] is None
    elif case=='missing-input':
        assert value['sql'] is None and value['response'] is None
        first=min(wanted['archive_attempts'],key=lambda row:row['ordinal']);name=first['attempt_id'][2:]+'.inputs'
        q=value['quarantine'];raw=bytes.fromhex(before['retained']['state/'+name]);item=before['physical']['state']['files'][name]
        assert q['filename']==name and q['hex']==raw.hex() and q['bytes']==len(raw) and q['sha256']==sha(raw)
        assert Path(q['path']).name=='catch-up-refusal-missing-input-quarantined.inputs'
        assert stat.S_ISREG(q['mode']) and stat.S_IMODE(q['mode'])==item['mode']==0o600
        assert {k:q[k] for k in ('device','inode','links')}=={k:item[k] for k in ('device','inode','links')}
        del expected['retained']['state/'+name];del expected['local_records'][name];del expected['physical']['state']['files'][name]
    else:assert value['sql'] is value['response'] is value['quarantine'] is None
    same(expected['control'],after['control'])
    for key in ('catalog','system_roles','native','inputs','contents','retained','local_records'):
        assert expected[key]==after[key],key
    for area in ('data','state'):assert expected['physical'][area]==after['physical'][area]
    return after


def schedule_checked(phase):
    schedule=phase['schedule'];case=phase['step'];window=phase['window'];after=phase['after']
    if case not in WAIT_CASES:assert schedule is None;return
    assert schedule['mode']==case and schedule['status']=='OBSERVED_CLOSED_AND_JOINED' and not schedule['cleanup_errors']
    holder=schedule['holder'];h=holder['identity'];b=schedule['blocked']
    assert h['database']==phase['database'] and h['user']=='bigbrotr_migrator' and h['application']=='bb_catchup_refusal_holder'
    assert h['isolation']=='read committed' and h['synchronous_commit']=='on'
    assert type(h['pid']) is int and h['pid']>0 and holder['exit']==0
    for key in ('begin','held','rollback'):assert holder[key]==dict(sqlstate='00000',rows=0,lines=[])
    assert holder['absence']==[dict(pid=h['pid'],backend_start=h['backend_start'],database=phase['database'],present=False)]
    current=max((r for r in after['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    assert b['pid']==current['control_backend_pid'] and instant(b['backend_start'])==instant(current['control_backend_start'])
    assert b['database']==phase['database'] and b['user']=='bigbrotr_control' and b['state']=='active'
    assert b['control_epoch']==current['epoch'] and b['control_incarnation']==current['process_incarnation'] and b['control_lease']==current['lease_expires_at_us']
    assert b['wait_type']=='Lock' and b['wait_event']=='relation' and b['blockers']==[h['pid']]
    assert instant(b['holder_start'])==instant(h['backend_start'])
    assert b['waiting_locks']==[dict(relation='control.archive_frontier_snapshots',mode='RowExclusiveLock',granted=False)]
    # Match the actual query, not just a generic blocked Control session.
    assert b['query']=="INSERT INTO control.archive_frontier_snapshots(operation_id,consumer_id,frontier_revision,dataset_id,topology_generation,catalog_id,owner_epoch,owner_incarnation,snapshot_id,request_digest,dictionary_count,payload_count,eligibility_count,receive_count,provenance_count) VALUES ($1::bytea,$2::bytea,$3::text::numeric,$4::bytea,$5::text::numeric,$2::bytea,$6::text::numeric,$7::bytea,$8::bigint,$9::bytea,$10::text::numeric,$11::text::numeric,$12::text::numeric,$13::text::numeric,$14::text::numeric)"
    times=[b['server_us'],schedule['action_before_us'],schedule['action_after_us'],schedule['terminal_observed_us'],schedule['release_before_us'],schedule['release_after_us']]
    for at in times:clock(at,window)
    assert times==sorted(times) and current['acquired_at_us']<=instant(b['query_start'])<=times[0]
    assert times[2]-instant(b['query_start'])<1_000_000 and times[2]<current['lease_expires_at_us']
    assert schedule['terminal_before_release']==phase['response'] and times[3]-times[2]<=6_500_000
    # This is an observed server-clock envelope, not internal Rust telemetry.
    if case=='frontier-cancel':
        t=phase['response']['terminal'];identity=schedule['signal']
        assert identity=={k:t[k] for k in ('pid','start_ticks','command','config','prefix')}
        assert schedule['action_response']=='SIGTERM_SENT_TO_EXACT_PROCESS' and 'termination_sql' not in schedule
    else:
        assert schedule['action_response']=='t' and 'signal' not in schedule
        assert schedule['termination_sql']==f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid={b['pid']} AND backend_start='{b['backend_start']}'::timestamptz AND datname='{phase['database']}' AND usename='bigbrotr_control' AND {h['pid']}=ANY(pg_blocking_pids(pid))"


def journal_checked(before,after,science,revision):
    """Original literal custody/input formats plus ONE uncommitted cut intent."""
    normal.retained(after,science)
    files=after['physical']['state']['files'];records=after['local_records']
    assert set(records)=={name for name,meta in files.items() if meta['sha256'] is not None}
    expected=set(before['physical']['state']['files']);initial=json.loads(bytes.fromhex(after['retained']['data/.bigbrotr-initialization/intent.json']))
    binding=sha(bytes.fromhex(records['coordinator.json']))
    for name,raw in before['local_records'].items():
        assert records[name]==raw and files[name]==before['physical']['state']['files'][name]
    for row in sorted(after['control']['archive_attempts'],key=lambda r:r['ordinal'])[4:]:
        identity=row['attempt_id'][2:];lock=files[identity+'/custody.lock'];manifest=json.loads(bytes.fromhex(records[identity+'/intent.json']))
        assert manifest==dict(format='bigbrotr-local-archive-custody/1',boot_id=initial['boot_id'],lock_device=lock['device'],lock_inode=lock['inode'],
            dataset_id=row['dataset_id'][2:],catalog_id=row['catalog_id'][2:],catalog_root=initial['catalog_root'],attempt_id=list(binary(row['attempt_id'])),
            coordinator_incarnation=row['coordinator_incarnation'][2:],worker_incarnation=row['worker_incarnation'][2:],publication_ordinal=row['ordinal'],
            topology_generation='2',batch_digest=row['batch_digest'][2:])
        assert records[identity+'/custody.lock']=='' and row['coordinator_binding_sha256']=='\\x'+binding
        expected.update((identity,identity+'/intent.json',identity+'/custody.lock',identity+'.inputs'))
    candidates=set(files)-expected;name,=candidates
    assert re.fullmatch('frontier-[0-9a-f]{32}\\.intent',name)
    raw=bytes.fromhex(records[name]);operation=bytes.fromhex(name[9:-7]);pg=after['control'];consumer=only(pg['consumers'])
    assert 214<=len(raw)<=65536 and operation!=bytes(16)
    receipts=receipts_for(pg,science)[:4];ds=pg['datasets'][0]['dataset_id'];catalog=consumer['consumer_id']
    digest=cut_digest(ds,1,receipts,[(1,12),(2,0)],8)
    assert raw[:8]==b'BBFRONT\x01' and raw[8:40]==bytes.fromhex(binding)
    assert raw[40:88]==binary(ds)+binary(catalog)+operation and raw[88:104]==be(revision)+be(8)
    assert raw[104:136]==bytes.fromhex(digest) and raw[136:176]==b''.join(be(n) for n in (8,1,1,1,1))
    n=int.from_bytes(raw[176:180],'big');assert len(raw)==180+n+32 and raw[-32:]==bytes.fromhex(sha(b'bigbrotr/archive-frontier-intent/1\0'+raw[:-32]))
    expectation=dict(dataset_id=ds[2:],topology_generation='1',bounds=[dict(shard_id=0,owner_epoch=e,complete_through=c) for e,c in ((1,12),(2,0))],
        receipts=[dict(r,dataset_id=r['dataset_id'].lower(),topology_generation=str(r['topology_generation']),batch_digest=r['batch_digest'].lower(),attempt_id=list(bytes.fromhex(r['attempt_id']))) for r in receipts],snapshot_id=8)
    expectation['receipts']=[{k:r[k] for k in ('dataset_id','schema_version','topology_generation','batch_digest','record_count','input_ranges','attempt_id','publication_ordinal')} for r in expectation['receipts']]
    assert raw[180:-32]==json.dumps(expectation,separators=(',',':')).encode()
    assert not any(binary(s['operation_id'])==operation for s in pg['archive_frontier_snapshots'])
    for name,raw in records.items():
        item=files[name];assert item['mode']==0o600 and item['links']==1 and item['sha256']==sha(bytes.fromhex(raw))


def application_checked(phase,science,revision):
    before,after=phase['before'],phase['after'];case=phase['step'];wait=case in WAIT_CASES
    model=Model(before['control'],before['native'],before['inputs'],phase['window'])
    control(model,after['control'],failed=True)
    if wait:
        wanted=model.pg;issuer=model.control;template=only(wanted['archive_attempts'],ordinal=1)
        for ordinal in (5,6,7):
            actual=only(after['control']['archive_attempts'],ordinal=ordinal);batch=science['batches'][ordinal-1]
            attempt=model.identity(actual['attempt_id'][2:]);worker=model.identity(actual['worker_incarnation'][2:])
            clock(actual['issued_at_us'],phase['window']);fresh_xmin(actual['xmin'],wanted)
            assert issuer['acquired_at_us']<=actual['issued_at_us']<issuer['lease_expires_at_us']
            wanted['archive_attempts'].append(dict(template,attempt_id=attempt,ordinal=ordinal,expected_predecessor=ordinal-1,
                topology_generation=2,batch_digest='\\x'+batch['archive_digest'],coordinator_incarnation=issuer['process_incarnation'],worker_incarnation=worker,
                issuer_epoch=issuer['epoch'],issuer_incarnation=issuer['process_incarnation'],issued_at_us=actual['issued_at_us'],xmin=actual['xmin']))
        from installed_archive_catch_up_catalog import verify_catalog
        verify_catalog(phase,science)  # Shared corrected native stats envelope, no copied count assumption.
        normal.roles(before,after,'catchup')
        journal_checked(before,after,science,revision)
    else:
        assert before['catalog']==after['catalog'] and before['retained']==after['retained'] and before['local_records']==after['local_records']
        assert before['physical']['data']==after['physical']['data'] and before['physical']['state']==after['physical']['state']
        normal.roles(before,after,'catchup-retry' if case=='family-counts' else case)
    same(model.pg,after['control'])  # All75 tables, including unchanged Archive owner/frontier/history.
    assert all(before[k]==after[k] for k in ('native','inputs','contents'))


def verify(value):
    assert value['status'] in ('RUNNING','PASS') and value['case'] in CASES
    revision,science,previous,count=prefix(value)
    phase=value['refusal'];case=value['case'];assert phase['step']==case and phase['operation']=='catchup'
    prepared=mutation_checked(phase['mutation'],case,previous);assert prepared==phase['before']
    assert phase['database']==value['database'] and phase['config']==value['config']
    assert phase['config_text']==value['steps'][-1]['config_text']
    config=tomllib.loads(phase['config_text'])
    assert config['postgres']['control']['endpoint']==config['postgres']['catalog']['endpoint']=='127.0.0.1:5432'
    process_checked(phase['response'],case,phase['config'],'catch-up-refusal-'+case)
    window=phase['window'];assert all(type(window[k]) is int for k in ('before_us','after_us')) and 0<window['after_us']-window['before_us']<=120_000_000
    assert phase['fixture']==dict(errors=[],snapshots=[])
    for state in (phase['mutation']['before'],prepared,phase['after']):
        assert len(state['control'])==75 and set(state['control'])==set(value['initial_state']['control'])
        for area in ('store','data','state','scratch'):
            assert {k:v for k,v in state['physical'][area].items() if k!='files'}=={k:v for k,v in value['initial_state']['physical'][area].items() if k!='files'}
        assert set(state['contents'])==set(state['inputs'])
        for name,raw in state['contents'].items():assert state['inputs'][name]==dict(bytes=len(bytes.fromhex(raw)),sha256=sha(bytes.fromhex(raw)))
    current=max((r for r in phase['after']['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    assert phase['backend']['exact']==[dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=value['database'],present=False)]
    assert phase['backend']['all']=={value['database']:'0',value['setup_resource']['catalog_database']:'0'}
    schedule_checked(phase);application_checked(phase,science,revision)
    if case in WAIT_CASES:
        response=value['readback'];assert response['returncode']==0 and not response['stderr']
        assert json.loads(response['stdout'])==dict(format='bigbrotr-independent-installed-archive-read/1',tables={**science['tables'],'archive_commits':receipts_for(phase['after']['control'],science)})
    else:assert 'readback' not in value
    return dict(status='PASS',case=case,expected_refusal=True,exit_code=4,installed_processes=11,control_tables=75,
        snapshots=count+3,new_archive_attempts=3 if case in WAIT_CASES else 0,new_frontier_revisions=0,
        original_frontier_revision=revision,control_released=False,same_physical_roots=True,
        source_scope='independent whole-state reducer; wait timing is observed, no internal timer claim')


def negative_controls(value):
    verify(value);rejected=[]
    def check(label,mutate):
        changed=copy.deepcopy(value);mutate(changed)
        try:verify(changed)
        except (AssertionError,KeyError,ValueError,TypeError,IndexError,StopIteration):rejected.append(label)
        else:raise AssertionError('control accepted: '+label)
    check('forced-process',lambda v:v['refusal']['response']['terminal'].update(forced=True))
    check('unknown-process',lambda v:v['refusal']['response'].update(outcome='OUTCOME_UNKNOWN'))
    check('wrong-error',lambda v:v['refusal']['response'].update(stderr='error: CommitUnknown\n'))
    check('unrelated-SQL',lambda v:v['refusal']['after']['control']['datasets'][0].update(instance_id='\\x'+'ab'*16))
    check('native-write',lambda v:v['refusal']['after']['native'].update(sequence=v['refusal']['after']['native']['sequence']+1))
    check('backend-live',lambda v:v['refusal']['backend']['exact'][0].update(present=True))
    check('Control-normalized',lambda v:max((r for r in v['refusal']['after']['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch']).update(state='released'))
    check('old-frontier-rewritten',lambda v:v['refusal']['after']['control']['archive_frontier_snapshots'][0].update(xmin='4294967000'))
    check('scope-obligation-added',lambda v:v['refusal']['after']['control']['jobs'].append(dict(job_id='unexpected')))
    if value['case'] in WAIT_CASES:
        check('foreign-waiter',lambda v:v['refusal']['schedule']['blocked'].update(pid=v['refusal']['schedule']['blocked']['pid']+1))
        check('wrong-wait-query',lambda v:v['refusal']['schedule']['blocked'].update(query='COMMIT'))
        check('holder-unjoined',lambda v:v['refusal']['schedule']['holder'].update(exit=1))
        check('no-real-action',lambda v:v['refusal']['schedule'].update(action_response=''))
        check('late-action',lambda v:v['refusal']['schedule'].update(action_after_us=v['refusal']['schedule']['blocked']['server_us']+2_000_000))
        check('release-before-terminal',lambda v:v['refusal']['schedule'].update(release_before_us=v['refusal']['schedule']['terminal_observed_us']-1))
        check('retained-intent-lost',lambda v:v['refusal']['after']['local_records'].pop(next(n for n in v['refusal']['after']['local_records'] if n.startswith('frontier-'))))
    elif value['case']=='missing-input':
        check('lost-original',lambda v:v['refusal']['mutation']['quarantine'].update(hex='00'))
    else:
        check('guard-catalog-changed',lambda v:v['refusal']['mutation']['schema_after']['triggers'][0].update(tgenabled='D'))
    return dict(status='PASS',rejected=rejected,scope='assembled-context mutations, not independent engine or isolated-predicate coverage')
