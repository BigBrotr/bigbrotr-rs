"""Independent science and complete SQL transitions for four NEW-owner faults.

Reuses only independent initial/network science and literal formats. No product
reducer, Rust codec, fabricated intermediate authority, or expected-from-after
row projection is used. Fresh IDs/times/xmins parameterize explicit row changes.
"""
import copy
import json
import math
from pathlib import Path, PurePosixPath
import re
import tomllib

import installed_archive_catch_up_oracle as normal
from installed_archive_catch_up_oracle import (Model, only, same, sha, be, binary,
    clock, fresh_xmin, instant, exclusion, archive_science, cut_digest, COUNTS)
from installed_archive_catch_up_client import argv
from installed_archive_catch_up_catalog import verify_file_counts

CASES=('publication-loss','frontier-loss','resolution','exact-retry')
TARGETS={'publication-loss':'publication','frontier-loss':'control-frontier'}
ERRORS={'publication-loss':'error: initial Archive refused; preserve original state: OutcomeUnknown\n',
    'frontier-loss':'error: Archive catch-up refused; preserve original state: CommitUnknown\n'}


def wire_checked(wire,target,source,client,*,local_proxy=None,mode='after-complete'):
    assert wire['status'] in ('UNKNOWN','OBSERVED_CLOSED_AND_JOINED')
    assert mode in ('before-forward','after-complete')
    assert wire['target']==target and wire['mode']==mode
    p=wire['process'];assert p['exit']==0 and p['overflow'] is False and p['readers_joined'] is True and not p['stderr']
    if local_proxy is None:
        assert mode=='after-complete'
        assert p['command']==['docker','exec','-i',client,'python3','-B','/lab/archive-catch-up-commit-proxy.py',
            '--target',target,'--mode','after-complete','--profile','retained-catch-up']
    else:
        # A second maintained owner runs this same proxy on the host. Its
        # source root/interpreter are independently pinned by that owner; the
        # route, actual roles and argv remain a closed profile here.
        assert client is None and target=='publication'
        executable,path=local_proxy
        assert Path(executable).is_absolute() and Path(path).is_absolute()
        assert Path(path).parts[-3:]==('experiments','archive-engine','native_commit_proxy.py')
        assert p['command']==[executable,'-B',path,'--target','publication','--mode',mode,
            '--profile','retained-catch-up','--upstream-port','15432',
            '--catalog-manager','bigbrotr_catalog','--catalog-reader','bigbrotr_archive_read',
            '--catalog-database','bigbrotr_catalog']
    events=[json.loads(s) for s in p['stdout'].splitlines()]
    assert [e['event'] for e in events]==['ready','fault','terminal']
    ready,fault,end=events;assert ready==wire['ready'] and ready['source_sha256']==source
    assert re.fullmatch('[0-9a-f]{64}',source) and type(ready['port']) is int and 1024<=ready['port']<=65535
    for event in events:
        assert event['target']==target and event['mode']==mode
        for key in ('connections','bytes','frames','data_queries','data_commits','server_complete','client_complete','faults'):
            assert type(event[key]) is int and event[key]>=0
        assert event['profile']=='retained-catch-up' and event['frame_limit']==131072 and event['connection_limit']==64
        assert event['connections']<=64 and event['bytes']<=64*1024**2 and event['frames']<=131072
        for key in ('owner_requests','owner_forwarded','owner_complete','guard_rejections','serialization_rejections',
            'auth_finals_omitted','coordinator_inserts','pin_claim_injections','session_mutations_complete',
            'reader_queries','capture_verify_requests','resume_authority_requests','resume_expiry_rejections','resume_expiry_idle'):
            assert event[key]==0
        assert event['rejection_sequence']==event['pin_rejections']==[]
        if local_proxy is not None:
            assert [event[k] for k in ('upstream_host','upstream_port','catalog_manager','catalog_reader',
                'catalog_database','armed')]==['127.0.0.1',15432,'bigbrotr_catalog','bigbrotr_archive_read',
                'bigbrotr_catalog',True]
    assert all(ready[k]==0 for k in ('connections','bytes','frames','data_queries','data_commits','server_complete','client_complete','faults'))
    for event in (fault,end):
        assert [event[k] for k in ('data_queries','data_commits','server_complete','client_complete','faults')]==[1,1,int(mode=='after-complete'),0,1]
    assert type(fault['backend_pid']) is int and fault['backend_pid']>0 and end['unexpected']==0
    assert all(fault[k]<=end[k] for k in ('connections','bytes','frames','frontend_queries'))
    return fault['backend_pid']


def common(phase,previous,baseline,database,catalog,*,loss=False):
    before,after=phase['before'],phase['after'];response=phase['response'];window=phase['window'];fixture=phase['fixture']
    project=lambda s:{k:v for k,v in s.items() if k not in ('physical','local_records')}
    assert project(before)==project(previous)
    if 'local_records' in previous:assert before['local_records']==previous['local_records']
    assert phase['database']==database and set(before['control'])==set(after['control'])==set(baseline['control']) and len(after['control'])==75
    assert all(type(window[k]) is int and window[k]>0 for k in ('before_us','after_us'))
    assert 0<window['after_us']-window['before_us']<=120_000_000
    assert response['outcome']=='PROCESS_EXITED' and type(response['returncode']) is int and response['returncode']==(4 if loss else 0)
    t=response['terminal'];assert t['reaped'] is True and t['forced'] is False and t['error'] is None and t['exit_code']==response['returncode']
    assert t['command']==argv(phase['operation'],Path(phase['config'])) and t['config']==phase['config']
    assert t['prefix']=='/lab/catch-up-'+phase['step']
    assert all(type(t[k]) is int and t[k]>0 for k in ('pid','start_ticks'))
    assert type(t['elapsed_seconds']) in (int,float) and math.isfinite(t['elapsed_seconds']) and 0<t['elapsed_seconds']<=105
    assert not fixture['errors'] and fixture.get('client_joined',True) is True
    if loss:assert not response['stdout'] and response['stderr']==ERRORS[phase['step']]
    else:assert not response['stderr']
    for state in (before,after):
        for area in ('store','data','state','scratch'):
            assert {k:v for k,v in state['physical'][area].items() if k!='files'}=={k:v for k,v in baseline['physical'][area].items() if k!='files'}
        assert set(state['contents'])==set(state['inputs'])
        for name,raw in state['contents'].items():
            data=bytes.fromhex(raw);assert state['inputs'][name]==dict(bytes=len(data),sha256=sha(data))
    for snap in fixture['snapshots']:
        assert set(snap['control'])==set(baseline['control'])
        clock(snap['before_us'],window);clock(snap['after_us'],window);assert snap['before_us']<=snap['after_us']
    current=max((r for r in after['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    assert phase['backend']['exact']==[dict(pid=current['control_backend_pid'],backend_start=current['control_backend_start'],database=database,present=False)]
    assert phase['backend']['all']=={database:'0',catalog:'0'}


def prefix(value):
    baseline=value['initial_state'];revision=normal.initial(value);previous=baseline;science=None;count=1
    assert [p['step'] for p in value['steps']]==list(normal.STEPS[:9])
    for p in value['steps']:
        common(p,previous,baseline,value['database'],value['setup_resource']['catalog_database'])
        before,after=p['before'],p['after'];step=p['step'];reports=[json.loads(s) for s in p['response']['stdout'].splitlines()]
        assert p['config']==value['config']
        if step=='live3':model=normal.family(p);science=archive_science(after['native'])
        else:
            model=Model(before['control'],before['native'],before['inputs'],p['window'])
            if step=='live2':model.live2(after['control'],reports[0]);normal.live2_observations(model,p,reports)
            else:
                assert len(reports)==1;model.finite(after['control'],step);normal.finite_report(model,step,reports[0])
            assert after['native']==model.native and after['inputs']==model.inputs and after['contents']==before['contents']
        if step in ('live2','live3'):assert len(reports)==2 and p['fixture']['initial_report']==reports[0]
        assert before['catalog']==after['catalog'] and before['retained']==after['retained']
        assert all(before['physical'][a]==after['physical'][a] for a in ('data','state'))
        normal.roles(before,after,step);previous=after;count+=2+len(p['fixture']['snapshots'])
    assert science is not None
    return revision,science,previous,count


def control(model,observed,failed):
    """Actual new Control, including fencing an unreleased failed predecessor."""
    wanted=model.pg;previous=max((r for r in wanted['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
    old=copy.deepcopy(previous);actual=only(observed['owner_epochs'],role='control',epoch=old['epoch']+1)
    role=only(observed['role_instances'],role_instance_id=actual['role_instance_id'])
    rid=model.identity(role['role_instance_id'][2:]);inc=model.identity(actual['process_incarnation'][2:])
    for at in (role['created_at_us'],actual['acquired_at_us'],actual['lease_expires_at_us']-6_000_000):clock(at,model.window)
    assert model.window['before_us']<=instant(actual['control_backend_start'])<=role['created_at_us']<=actual['acquired_at_us']<=actual['lease_expires_at_us']-6_000_000
    assert type(actual['control_backend_pid']) is int and actual['control_backend_pid']>0
    fresh_xmin(actual['xmin'],wanted);fresh_xmin(role['xmin'],wanted)
    if old['state']=='active':
        closed=only(observed['owner_epochs'],role='control',epoch=old['epoch'])
        priorrole=only(wanted['role_instances'],role_instance_id=old['role_instance_id'])
        r=only(observed['role_instances'],role_instance_id=old['role_instance_id'])
        for at in (closed['released_at_us'],r['retired_at_us']):clock(at,model.window)
        assert closed['released_at_us']<=r['retired_at_us']<=role['created_at_us']
        fresh_xmin(closed['xmin'],wanted);assert r['xmin']==closed['xmin']
        if failed:assert role['xmin']==closed['xmin']
        digest=sha(b'bigbrotr/control-exclusion/1\0'+binary(old['dataset_id'])+binary(old['resource_id'])+be(old['epoch'])+binary(old['process_incarnation'])+binary(inc))
        previous.update(state='released',released_at_us=closed['released_at_us'],exclusion_method='storage_fenced',exclusion_digest='\\x'+digest,xmin=closed['xmin'])
        priorrole.update(retired_at_us=r['retired_at_us'],xmin=closed['xmin'])
    else:assert old['state']=='released'
    model.control=dict(old,epoch=old['epoch']+1,role_instance_id=rid,process_incarnation=inc,
        acquired_at_us=actual['acquired_at_us'],lease_expires_at_us=actual['lease_expires_at_us'],
        state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,
        control_backend_pid=actual['control_backend_pid'],control_backend_start=actual['control_backend_start'],xmin=actual['xmin'])
    if not failed:
        for at in (actual['released_at_us'],role['retired_at_us']):clock(at,model.window)
        assert actual['lease_expires_at_us']-6_000_000<=actual['released_at_us']<=role['retired_at_us']
        assert actual['released_at_us']<actual['lease_expires_at_us'] and role['xmin']==actual['xmin']
        model.control.update(state='released',released_at_us=actual['released_at_us'],exclusion_method='storage_fenced',exclusion_digest=exclusion(model.control))
    assert actual==model.control
    wanted['owner_epochs'].append(model.control)
    wanted['role_instances'].append(dict(role_instance_id=rid,dataset_id=model.ds['dataset_id'],role='control',created_at_us=role['created_at_us'],
        retired_at_us=None if failed else role['retired_at_us'],xmin=role['xmin']))


def receipts_for(state,science):
    return [dict(dataset_id=state['datasets'][0]['dataset_id'][2:].upper(),schema_version=1,topology_generation=b['topology'],
        batch_digest=b['archive_digest'].upper(),record_count=b['last']-b['first']+1,
        input_ranges=[dict(shard_id=0,owner_epoch=b['epoch'],first_sequence=b['first'],last_sequence=b['last'])],
        attempt_id=a['attempt_id'][2:].upper(),publication_ordinal=a['ordinal'])
        for a,b in zip(sorted(state['archive_attempts'],key=lambda r:r['ordinal']),science['batches'])]


def frontier(model,observed,science,receipts,revisions,initial_revision):
    if not revisions:return
    wanted=model.pg;original=copy.deepcopy(wanted);issuer=model.control;ds=model.ds['dataset_id'];consumer=only(wanted['consumers']);catalog=consumer['consumer_id']
    previous=max((r for r in wanted['owner_epochs'] if r['role']=='archive'),key=lambda r:r['epoch']);old=copy.deepcopy(previous)
    assert old['state']=='active'
    owner_epoch=old['epoch']+1;first=revisions[0];last=revisions[-1]
    r=only(observed['consumer_frontier_revisions'],frontier_revision=first);tx=r['xmin'];fresh_xmin(tx,wanted)
    offset=first-initial_revision;topology=1 if offset==1 else 2
    bounds=[(1,12),(2,0)]+([] if offset==1 else [(3,0)])
    digest=cut_digest(ds,topology,receipts[:4],bounds,8)
    closed=only(observed['owner_epochs'],role='archive',epoch=old['epoch'])
    role=only(wanted['role_instances'],role_instance_id=old['role_instance_id']);actual_role=only(observed['role_instances'],role_instance_id=role['role_instance_id'])
    for at in (closed['released_at_us'],actual_role['retired_at_us']):clock(at,model.window)
    excluded=sha(b'bigbrotr/archive-frontier-owner-exclusion/1\0'+binary(ds)+binary(catalog)+be(old['epoch'])+binary(old['process_incarnation'])
        +binary(issuer['resource_id'])+be(issuer['epoch'])+binary(issuer['process_incarnation'])+bytes.fromhex(digest))
    previous.update(state='released',released_at_us=closed['released_at_us'],exclusion_method='storage_fenced',exclusion_digest='\\x'+excluded,xmin=tx)
    role.update(retired_at_us=actual_role['retired_at_us'],xmin=tx)
    actual=only(observed['owner_epochs'],role='archive',epoch=owner_epoch);rid=model.identity(actual['role_instance_id'][2:]);newrole=only(observed['role_instances'],role_instance_id=rid)
    for at in (newrole['created_at_us'],actual['acquired_at_us'],actual['lease_expires_at_us']-6_000_000):clock(at,model.window)
    assert issuer['acquired_at_us']<=closed['released_at_us']<=actual_role['retired_at_us']<=newrole['created_at_us']<=actual['acquired_at_us']
    assert actual['lease_expires_at_us']<=issuer['lease_expires_at_us']
    finaltx=only(observed['consumer_frontier_revisions'],frontier_revision=last)['xmin']
    wanted['owner_epochs'].append(dict(old,epoch=owner_epoch,role_instance_id=rid,process_incarnation=issuer['process_incarnation'],
        acquired_at_us=actual['acquired_at_us'],lease_expires_at_us=actual['lease_expires_at_us'],state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,xmin=finaltx))
    wanted['role_instances'].append(dict(role_instance_id=rid,dataset_id=ds,role='archive',created_at_us=newrole['created_at_us'],retired_at_us=None,xmin=tx))
    wanted['archive_owner_sessions'].append(dict(only(wanted['archive_owner_sessions'],owner_epoch=old['epoch']),owner_epoch=owner_epoch,
        owner_incarnation=issuer['process_incarnation'],issuer_epoch=issuer['epoch'],issuer_incarnation=issuer['process_incarnation'],xmin=tx))
    previous_at=actual['acquired_at_us'];used_transactions=set()
    for revision in revisions:
        offset=revision-initial_revision;topology=1 if offset==1 else 2
        bounds=[(1,12),(2,0)]+([] if offset==1 else [(3,10 if offset==3 else 0)])
        counts=[13,2,2,2,3] if offset==3 else [8,1,1,1,1];selected=7 if offset==3 else 4
        r=only(observed['consumer_frontier_revisions'],frontier_revision=revision);at,tx=r['created_at_us'],r['xmin']
        clock(at,model.window);fresh_xmin(tx,original)
        assert previous_at<=at<actual['lease_expires_at_us'] and tx not in used_transactions
        previous_at=at;used_transactions.add(tx)
        if issuer['released_at_us'] is not None:assert at<=issuer['released_at_us']
        wanted['consumer_frontier_revisions'].append(dict(consumer_id=catalog,frontier_revision=revision,dataset_id=ds,consumer_role='archive',
            topology_generation=topology,previous_revision=revision-1,owner_resource_id=catalog,owner_epoch=owner_epoch,owner_incarnation=issuer['process_incarnation'],created_at_us=at,sealed=True,xmin=tx))
        snap=only(observed['archive_frontier_snapshots'],frontier_revision=revision);operation=model.identity(snap['operation_id'][2:])
        wanted['archive_frontier_snapshots'].append(dict(operation_id=operation,consumer_id=catalog,frontier_revision=revision,dataset_id=ds,topology_generation=topology,
            catalog_id=catalog,owner_epoch=owner_epoch,owner_incarnation=issuer['process_incarnation'],snapshot_id=8,request_digest='\\x'+cut_digest(ds,topology,receipts[:selected],bounds,8),**dict(zip(COUNTS,counts)),xmin=tx))
        for epoch,cut in bounds:
            batch=next((b for b in science['batches'] if b['epoch']==epoch and b['last']==cut),None)
            wanted['consumer_frontiers'].append(dict(consumer_id=catalog,frontier_revision=revision,dataset_id=ds,topology_generation=topology,
                shard_id=0,owner_epoch=epoch,last_complete_sequence=cut,attestation_kind='archive-batch' if cut else None,attestation_digest='\\x'+batch['archive_digest'] if cut else None,xmin=tx))
        for attempt in sorted(wanted['archive_attempts'],key=lambda r:r['ordinal'])[:selected]:
            wanted['archive_frontier_batches'].append(dict(consumer_id=catalog,frontier_revision=revision,attempt_id=attempt['attempt_id'],xmin=tx))
    consumer.update(current_revision=last,current_topology_generation=2 if last>initial_revision+1 else 1,xmin=tx)


def catalog_growth(phase,science):
    before,after=phase['before'],phase['after'];a,b=before['catalog'],after['catalog']
    assert set(a)==set(b) and len(a)==32
    mutable={'bigbrotr_archive.attempts','bigbrotr_archive.owner',*[f'public.ducklake_{s}' for s in
        ('snapshot','snapshot_changes','data_file','file_column_stats','table_stats','table_column_stats')]}
    for table in a.keys()-mutable:assert a[table]==b[table],table
    start=len(a['bigbrotr_archive.attempts']);end=len(b['bigbrotr_archive.attempts'])
    assert (start,end) in ((4,5),(5,7))
    for row in a['bigbrotr_archive.attempts']:assert row in b['bigbrotr_archive.attempts']
    for ordinal in range(start+1,end+1):
        row=only(b['bigbrotr_archive.attempts'],ordinal=ordinal)
        issued=only(after['control']['archive_attempts'],ordinal=ordinal)
        assert row==dict({k:issued[k] for k in ('ordinal','attempt_id','coordinator_incarnation','worker_incarnation','topology_generation','batch_digest')},
            login_name='bb_archive_'+issued['attempt_id'][2:],login_oid=row['login_oid'],xmin=row['xmin'])
        assert str(row['login_oid']).isdecimal() and int(row['login_oid'])>0 and str(row['xmin']).isdecimal()
    owner=only(b['bigbrotr_archive.owner'])
    assert owner==dict(only(a['bigbrotr_archive.owner']),ordinal=end,enabled=end==5,xmin=owner['xmin'])
    for table in ('snapshot','snapshot_changes','data_file','file_column_stats'):
        for row in a['public.ducklake_'+table]:assert row in b['public.ducklake_'+table],table
    assert sorted(r['snapshot_id'] for r in b['public.ducklake_snapshot'])==list(range(end+2))
    assert sorted(r['snapshot_id'] for r in b['public.ducklake_snapshot_changes'])==list(range(end+2))
    tables={r['table_name']:r for r in b['public.ducklake_table']}
    expected={6:{'dictionaries':4,'archive_commits':1},7:{'dictionaries':1,'events':1,'event_eligibility_facts':1,
        'event_receive_facts':1,'provenance_facts':1,'archive_commits':1},8:{'provenance_facts':1,'archive_commits':1}}
    # The immutable native layout names, rather than fixed generated table IDs,
    # associate each physical file with its independently expected family count.
    expected_paths=set(before['physical']['data']['files']);newids=set()
    last=only(a['public.ducklake_snapshot'],snapshot_id=start+1)
    for snapshot in range(start+2,end+2):
        counts=expected[snapshot]
        row=only(b['public.ducklake_snapshot'],snapshot_id=snapshot);clock(instant(row['snapshot_time']),phase['window'])
        assert row==dict(last,snapshot_id=snapshot,snapshot_time=row['snapshot_time'],next_file_id=row['next_file_id'],xmin=row['xmin'])
        assert row['next_file_id']>last['next_file_id'] and row['xmin']!=last['xmin']
        records=[r for r in b['public.ducklake_data_file'] if r['begin_snapshot']==snapshot]
        assert len(records)==len(counts) and {r['table_id'] for r in records}=={tables[t]['table_id'] for t in counts}
        change=only(b['public.ducklake_snapshot_changes'],snapshot_id=snapshot)
        assert change==dict(snapshot_id=snapshot,changes_made=','.join('inserted_into_table:'+str(tables[t]['table_id'])
            for t in sorted(counts,key=lambda t:tables[t]['table_id'])),author=None,commit_message=None,commit_extra_info=None,xmin=row['xmin'])
        for name,count in counts.items():
            table=tables[name];record=only(records,table_id=table['table_id']);fid=record['data_file_id'];newids.add(fid)
            assert last['next_file_id']<=fid<row['next_file_id'] and record['xmin']==row['xmin']
            assert re.fullmatch(r'prepared-[0-9a-f]{32}/data\.parquet',record['path'])
            assert record['record_count']==count and record['row_id_start']==sum(r['record_count'] for r in b['public.ducklake_data_file']
                if r['table_id']==table['table_id'] and r['begin_snapshot']<snapshot)
            expected_record=dict(data_file_id=fid,table_id=table['table_id'],begin_snapshot=snapshot,end_snapshot=None,file_order=None,
                path=record['path'],path_is_relative=True,file_format='parquet',record_count=count,file_size_bytes=record['file_size_bytes'],
                footer_size=record['footer_size'],row_id_start=record['row_id_start'],partition_id=None,encryption_key=None,
                mapping_id=record['mapping_id'],partial_max=None,xmin=row['xmin'])
            assert record==expected_record and 0<record['footer_size']<record['file_size_bytes']<=16*1024**2
            assert any(m['mapping_id']==record['mapping_id'] and m['table_id']==table['table_id'] for m in b['public.ducklake_column_mapping'])
            schema=only(b['public.ducklake_schema'],schema_id=table['schema_id'])
            path=str(PurePosixPath(schema['path'])/table['path']/record['path']);expected_paths.update((path,str(PurePosixPath(path).parent)))
            metadata=after['physical']['data']['files'][path]
            assert metadata['mode']==0o600 and metadata['links']==1 and re.fullmatch('[0-9a-f]{64}',metadata['sha256'])
        last=row
    added=sum(len(expected[n]) for n in range(start+2,end+2))
    assert len(b['public.ducklake_data_file'])==len(a['public.ducklake_data_file'])+added and len(newids)==added
    assert set(after['physical']['data']['files'])==expected_paths
    for name,old in before['physical']['data']['files'].items():
        current=after['physical']['data']['files'][name]
        assert {k:v for k,v in old.items() if k!='links'}=={k:v for k,v in current.items() if k!='links'}
        assert current['links']==old['links'] if old['sha256'] is not None else current['links']>=old['links']
    counts=dict(zip(science['tables'],[12,1,1,1,1] if end==5 else [13,2,2,2,3]))|{'archive_commits':end}
    assert len(b['public.ducklake_table_stats'])==6
    for name,count in counts.items():
        tid=tables[name]['table_id'];row=only(b['public.ducklake_table_stats'],table_id=tid)
        data=[r for r in b['public.ducklake_data_file'] if r['table_id']==tid]
        latest=max(data,key=lambda r:r['begin_snapshot'])
        assert row==dict(table_id=tid,record_count=count,next_row_id=count,file_size_bytes=sum(r['file_size_bytes'] for r in data),xmin=latest['xmin'])
    columns={(r['table_id'],r['column_id']):r for r in b['public.ducklake_column']}
    for row in b['public.ducklake_file_column_stats']:
        if row['data_file_id'] not in newids:continue
        file=only(b['public.ducklake_data_file'],data_file_id=row['data_file_id'])
        assert (row['table_id'],row['column_id']) in columns and row['table_id']==file['table_id'] and row['xmin']==file['xmin']
        assert set(row)=={'data_file_id','table_id','column_id','column_size_bytes','value_count','null_count','min_value','max_value','contains_nan','extra_stats','xmin'}
        verify_file_counts(row,file)
        assert row['contains_nan'] is None and row['extra_stats'] is None
    keys=lambda rows,key:{tuple(r[k] for k in key) for r in rows}
    assert len(keys(b['public.ducklake_file_column_stats'],('data_file_id','column_id')))==len(b['public.ducklake_file_column_stats'])
    assert keys(a['public.ducklake_table_column_stats'],('table_id','column_id'))==keys(b['public.ducklake_table_column_stats'],('table_id','column_id'))
    for row in b['public.ducklake_table_column_stats']:
        old=only(a['public.ducklake_table_column_stats'],table_id=row['table_id'],column_id=row['column_id'])
        assert set(row)==set(old) and row['contains_nan']==old['contains_nan'] and row['extra_stats']==old['extra_stats']
        assert row['contains_null'] in (old['contains_null'],True)
        assert row['xmin']==only(b['public.ducklake_table_stats'],table_id=row['table_id'])['xmin']

    # Pinned DuckLake MergeStats compares BLOB statistics as their escaped
    # strings, not byte values. Match that metadata reduction here; it is not
    # a scientific BLOB range. Actual values are independently read in full.
    def scalar(value,kind):
        if kind.startswith(('uint','int')):return int(value)
        return value
    for row in b['public.ducklake_table_column_stats']:
        records=[r for r in b['public.ducklake_file_column_stats'] if r['table_id']==row['table_id'] and r['column_id']==row['column_id']]
        kind=columns[row['table_id'],row['column_id']]['column_type']
        for field,reduce in [('min_value',min),('max_value',max)]:
            values=[r[field] for r in records if r[field] is not None]
            assert row[field]==(reduce(values,key=lambda v:scalar(v,kind)) if values else None)
        assert row['contains_null']==any(r['null_count']>0 for r in records)


def catalog_roles(phase):
    before,after=phase['before'],phase['after'];wanted=copy.deepcopy(before['system_roles']);observed=after['system_roles']
    attempts=after['catalog']['bigbrotr_archive.attempts'];oldattempts=before['catalog']['bigbrotr_archive.attempts']
    end=len(attempts);names={r['login_name'] for r in attempts};oldnames={r['login_name'] for r in oldattempts}
    for row in wanted['roles']:
        if row['rolname'] in oldnames:
            row.update(xmin=only(observed['roles'],oid=row['oid'])['xmin'],rolcanlogin=False,credential_present=False)
    additions=[r for r in observed['roles'] if r['oid'] not in {p['oid'] for p in wanted['roles']}]
    added=[r for r in attempts if r['login_name'] not in oldnames]
    assert len(additions)==len(added)==end-len(oldattempts)
    assert {(r['rolname'],r['oid']) for r in additions}=={(r['login_name'],r['login_oid']) for r in added}
    template=only(before['system_roles']['roles'],rolname=oldattempts[0]['login_name'])
    for row in additions:
        login=end==5 and row['rolname']==only(attempts,ordinal=5)['login_name']
        assert row==dict(template,oid=row['oid'],rolname=row['rolname'],xmin=row['xmin'],rolcanlogin=login,credential_present=login)
        assert re.fullmatch('[1-9][0-9]*',row['xmin'])
    wanted['roles']+=additions;wanted['roles'].sort(key=lambda r:int(r['oid']))
    newmembers=[r for r in observed['members'] if r not in wanted['members']]
    assert len(newmembers)==len(additions) and {r['roleid'] for r in newmembers}=={r['oid'] for r in additions}
    # PostgreSQL 16 assigns each membership its own row OID.
    assert len({r['oid'] for r in observed['members']})==len(observed['members'])
    oldoids={r['oid'] for r in wanted['members']}
    template=next(r for r in wanted['members'] if r['roleid'] in {p['login_oid'] for p in oldattempts})
    for row in newmembers:
        assert str(row['oid']).isdecimal() and int(row['oid'])>0 and row['oid'] not in oldoids
        assert str(row['xmin']).isdecimal() and int(row['xmin'])>0
        assert {k:v for k,v in row.items() if k not in ('oid','roleid','xmin')}=={k:v for k,v in template.items() if k not in ('oid','roleid','xmin')}
    wanted['members']+=newmembers;wanted['members'].sort(key=lambda r:tuple(int(r[k]) for k in ('roleid','member','grantor')))
    assert wanted==observed


def journals(state,science,initial_revision,original,*,virgin=False):
    """Literal retained input, local manifest, and BBFRONT version1 science."""
    assert type(virgin) is bool
    if virgin:
        assert initial_revision==0 and not original['control']['archive_attempts']
        assert len(science['batches'])==8 and len(state['control']['archive_frontier_snapshots'])==1
    normal.retained(state,science)
    files=state['physical']['state']['files'];records=state['local_records'];pg=state['control']
    assert set(records)=={name for name,value in files.items() if value['sha256'] is not None}
    assert len(files)<=128
    for name,raw in records.items():
        item=files[name];assert item['mode']==0o600 and item['links']==1 and item['sha256']==sha(bytes.fromhex(raw))
    coordinator=json.loads(bytes.fromhex(records['coordinator.json']));initialization=bytes.fromhex(state['retained']['data/.bigbrotr-initialization/intent.json'])
    initial=json.loads(initialization)
    assert coordinator['format']=='bigbrotr-local-coordinator/1'
    assert coordinator['initialization_sha256']==list(bytes.fromhex(sha(initialization)))
    assert coordinator['state_device']==state['physical']['state']['device'] and coordinator['state_inode']==state['physical']['state']['inode']
    binding=sha(bytes.fromhex(records['coordinator.json']))
    expected_paths=set(original['physical']['state']['files'])
    receipts=receipts_for(pg,science)
    for attempt in pg['archive_attempts']:
        identity=attempt['attempt_id'][2:];lock=files[identity+'/custody.lock'];raw=records[identity+'/intent.json']
        manifest=json.loads(bytes.fromhex(raw))
        assert manifest==dict(format='bigbrotr-local-archive-custody/1',boot_id=initial['boot_id'],lock_device=lock['device'],lock_inode=lock['inode'],
            dataset_id=attempt['dataset_id'][2:],catalog_id=attempt['catalog_id'][2:],catalog_root=initial['catalog_root'],
            attempt_id=list(binary(attempt['attempt_id'])),coordinator_incarnation=attempt['coordinator_incarnation'][2:],
            worker_incarnation=attempt['worker_incarnation'][2:],publication_ordinal=attempt['ordinal'],topology_generation=str(attempt['topology_generation']),batch_digest=attempt['batch_digest'][2:])
        assert records[identity+'/custody.lock']==''
        assert attempt['coordinator_binding_sha256']=='\\x'+binding
        if attempt['ordinal']>(0 if virgin else 4):expected_paths.update((identity,identity+'/custody.lock',identity+'/intent.json',identity+'.inputs'))
    for snap in pg['archive_frontier_snapshots']:
        revision=snap['frontier_revision']
        if revision<=initial_revision:continue
        operation=binary(snap['operation_id']);name='frontier-'+operation.hex()+'.intent';expected_paths.add(name)
        raw=bytes.fromhex(records[name]);assert 214<=len(raw)<=65536 and raw[:8]==b'BBFRONT\x01'
        assert raw[8:40]==bytes.fromhex(binding) and raw[40:56]==binary(snap['dataset_id']) and raw[56:72]==binary(snap['catalog_id']) and raw[72:88]==operation
        assert raw[88:96]==be(revision-1) and raw[96:104]==be(9 if virgin else 8) and raw[104:136]==binary(snap['request_digest'])
        assert raw[136:176]==b''.join(be(snap[k]) for k in COUNTS)
        n=int.from_bytes(raw[176:180],'big');assert len(raw)==180+n+32 and raw[-32:]==bytes.fromhex(sha(b'bigbrotr/archive-frontier-intent/1\0'+raw[:-32]))
        offset=revision-initial_revision;bounds=[(1,12),(2,0)]+([] if offset==1 else [(3,10 if offset==3 else 0)])
        selected=receipts[:7 if offset==3 else 4]
        if virgin:
            assert revision==1 and snap['topology_generation']==2
            bounds=[(1,27),(2,1),(3,0)];selected=receipts;assert len(selected)==8
        expected=dict(dataset_id=snap['dataset_id'][2:],topology_generation=str(snap['topology_generation']),
            bounds=[dict(shard_id=0,owner_epoch=e,complete_through=c) for e,c in bounds],
            receipts=[dict(r,dataset_id=r['dataset_id'].lower(),topology_generation=str(r['topology_generation']),batch_digest=r['batch_digest'].lower(),attempt_id=list(bytes.fromhex(r['attempt_id']))) for r in selected],snapshot_id=9 if virgin else 8)
        # JSON field order is Rust declaration order, different from C API rows.
        expected['receipts']=[{k:r[k] for k in ('dataset_id','schema_version','topology_generation','batch_digest','record_count','input_ranges','attempt_id','publication_ordinal')} for r in expected['receipts']]
        assert raw[180:180+n]==json.dumps(expected,separators=(',',':')).encode()
    assert set(files)==expected_paths
    for name,item in original['physical']['state']['files'].items():assert files[name]==item


def archive_phase(phase,science,initial_revision,original):
    before,after=phase['before'],phase['after'];observed=after['control'];case=phase['step'];failed=case in TARGETS
    model=Model(before['control'],before['native'],before['inputs'],phase['window']);control(model,observed,failed)
    wanted=model.pg;issuer=model.control;oldcount=len(wanted['archive_attempts']);end=5 if case=='publication-loss' else 7
    assert len(observed['archive_attempts'])==end
    for ordinal in range(oldcount+1,end+1):
        actual=only(observed['archive_attempts'],ordinal=ordinal);batch=science['batches'][ordinal-1]
        attempt=model.identity(actual['attempt_id'][2:]);worker=model.identity(actual['worker_incarnation'][2:])
        clock(actual['issued_at_us'],phase['window']);fresh_xmin(actual['xmin'],wanted)
        assert issuer['acquired_at_us']<=actual['issued_at_us']<issuer['lease_expires_at_us']
        if issuer['released_at_us'] is not None:assert actual['issued_at_us']<=issuer['released_at_us']
        wanted['archive_attempts'].append(dict(only(wanted['archive_attempts'],ordinal=1),attempt_id=attempt,ordinal=ordinal,expected_predecessor=ordinal-1,
            topology_generation=2,batch_digest='\\x'+batch['archive_digest'],coordinator_incarnation=issuer['process_incarnation'],worker_incarnation=worker,
            issuer_epoch=issuer['epoch'],issuer_incarnation=issuer['process_incarnation'],issued_at_us=actual['issued_at_us'],xmin=actual['xmin']))
    receipts=receipts_for(wanted,science)
    revisions=([initial_revision+1] if case=='frontier-loss' else [initial_revision+2,initial_revision+3] if case=='resolution' else [])
    frontier(model,observed,science,receipts,revisions,initial_revision);same(wanted,observed)
    assert all(before[k]==after[k] for k in ('native','inputs','contents'))
    if case in TARGETS:catalog_growth(phase,science)
    else:
        assert before['catalog']==after['catalog']
        assert before['physical']['data']==after['physical']['data']
    catalog_roles(phase)
    if case=='frontier-loss':
        login=only(before['catalog']['bigbrotr_archive.attempts'],ordinal=5)['login_name']
        prior=only(before['system_roles']['roles'],rolname=login)
        sealed=only(after['system_roles']['roles'],rolname=login)
        assert prior['rolcanlogin'] is True and prior['credential_present'] is True
        assert sealed['rolcanlogin'] is False and sealed['credential_present'] is False
    for name,raw in before['retained'].items():assert after['retained'][name]==raw
    for name,raw in before['local_records'].items():
        assert after['local_records'][name]==raw and after['physical']['state']['files'][name]==before['physical']['state']['files'][name]
    journals(after,science,initial_revision,original)
    if not failed:
        snapshot=only(observed['archive_frontier_snapshots'],frontier_revision=initial_revision+3)
        assert json.loads(phase['response']['stdout'])==dict(format='bigbrotr-archive-catch-up/1',dataset_id=model.ds['dataset_id'][2:],
            catalog_id=only(wanted['consumers'])['consumer_id'][2:],terminal_epoch=3,issued_attempts_before=7,new_attempts=0,
            frontier_revision=initial_revision+3,snapshot_id=8,operation_id=list(binary(snapshot['operation_id'])),request_digest=snapshot['request_digest'][2:],
            family_counts=[13,2,2,2,3],new_frontier_revisions=2 if case=='resolution' else 0)
    if case=='exact-retry':assert before['local_records']==after['local_records'] and before['physical']['state']==after['physical']['state']
    return receipts


def verify(value):
    assert value['status'] in ('RUNNING','PASS') and [p['step'] for p in value['faults']]==list(CASES)
    revision,science,previous,count=prefix(value);original=previous;baseline=value['initial_state']
    direct=tomllib.loads(value['steps'][-1]['config_text'])
    assert direct['postgres']['control']['endpoint']==direct['postgres']['catalog']['endpoint']=='127.0.0.1:5432'
    assert re.fullmatch('bigbrotr-acquisition-binding-[0-9a-f]{7}',value['client'])
    for phase in value['faults']:
        case=phase['step'];common(phase,previous,baseline,value['database'],value['setup_resource']['catalog_database'],loss=case in TARGETS)
        assert phase['operation']=='catchup' and not phase['fixture']['snapshots']
        config=copy.deepcopy(direct)
        if case in TARGETS:
            wire=phase['wire'];assert wire['status']=='OBSERVED_CLOSED_AND_JOINED'
            pid=wire_checked(wire,TARGETS[case],value['proxy_sha256'],value['client'])
            section='catalog' if case=='publication-loss' else 'control';config['postgres'][section]['endpoint']='127.0.0.1:'+str(wire['ready']['port'])
            if case=='frontier-loss':assert pid==phase['backend']['exact'][0]['pid']
        else:assert phase['wire'] is None
        assert tomllib.loads(phase['config_text'])==config
        receipts=archive_phase(phase,science,revision,original)
        previous=phase['after'];count+=2
    assert value['readback']['returncode']==0 and not value['readback']['stderr']
    assert json.loads(value['readback']['stdout'])==dict(format='bigbrotr-independent-installed-archive-read/1',tables={**science['tables'],'archive_commits':receipts})
    return dict(status='PASS',installed_processes=14,continuation_processes=13,control_tables=75,snapshots=count,
        scientific_records=22,archive_receipts=7,family_counts=[13,2,2,2,3],initial_frontier_revision=revision,final_frontier_revision=revision+3,
        final_snapshot=8,publication_receipt_reused=True,first_bridge_operation_reused=True,exact_retry=True,same_physical_roots=True)


def negative_controls(value):
    verify(value);rejected=[]
    def check(label,mutation):
        altered=copy.deepcopy(value);mutation(altered)
        try:verify(altered)
        except (AssertionError,KeyError,ValueError,TypeError,IndexError,StopIteration):rejected.append(label)
        else:raise AssertionError('negative accepted: '+label)
    check('unknown-process',lambda v:v['faults'][0]['response'].__setitem__('outcome','OUTCOME_UNKNOWN'))
    check('forced-process',lambda v:v['faults'][0]['response']['terminal'].__setitem__('forced',True))
    check('unknown-proxy',lambda v:v['faults'][0]['wire'].__setitem__('status','UNKNOWN'))
    check('proxy-reader-alive',lambda v:v['faults'][0]['wire']['process'].__setitem__('readers_joined',False))
    check('client-id-instead-of-name',lambda v:v.__setitem__('client','0'*64))
    check('proxy-client-mismatch',lambda v:v['faults'][0]['wire']['process']['command'].__setitem__(3,v['client']+'-other'))
    def missing_commit(v):
        process=v['faults'][0]['wire']['process'];events=[json.loads(s) for s in process['stdout'].splitlines()]
        for e in events:e['server_complete']=0
        process['stdout']='\n'.join(map(json.dumps,events))+'\n'
    check('missing-after-complete',missing_commit)
    check('wrong-root',lambda v:v['faults'][0]['after']['physical']['data'].__setitem__('inode',0))
    check('duplicate-attempt',lambda v:v['faults'][1]['after']['control']['archive_attempts'].append(copy.deepcopy(v['faults'][1]['after']['control']['archive_attempts'][-1])))
    check('missing-zero-stream',lambda v:v['faults'][1]['after']['control']['consumer_frontiers'].pop())
    check('original-attempt-xmin',lambda v:only(v['faults'][2]['after']['control']['archive_attempts'],ordinal=5).__setitem__('xmin','1'))
    def original_operation(v):
        before=v['faults'][1]['before']['control']['archive_frontier_snapshots']
        original,=[r for r in v['faults'][1]['after']['control']['archive_frontier_snapshots'] if r not in before]
        only(v['faults'][2]['after']['control']['archive_frontier_snapshots'],operation_id=original['operation_id'])['xmin']='1'
    check('original-operation-xmin',original_operation)
    check('old-input-bytes',lambda v:v['faults'][2]['after']['retained'].__setitem__(next(k for k in v['faults'][2]['after']['retained'] if k.endswith('.inputs')),'00'))
    check('frontier-journal-bytes',lambda v:v['faults'][2]['after']['local_records'].__setitem__(next(k for k in v['faults'][2]['after']['local_records'] if k.startswith('frontier-')),'00'))
    check('native-sequence',lambda v:v['faults'][2]['after']['native'].__setitem__('sequence',0))
    check('retry-added-revision',lambda v:v['faults'][3]['after']['control']['consumer_frontier_revisions'].append(copy.deepcopy(v['faults'][3]['after']['control']['consumer_frontier_revisions'][-1])))
    for table in sorted(value['initial_state']['control']):
        def mutation(v,t=table):
            rows=v['faults'][3]['after']['control'][t]
            if rows:rows[0]['xmin']='0'
            else:rows.append(dict(unexpected=True))
        check('unrelated-'+table,mutation)
    return dict(status='PASS',count=len(rejected),rejected=rejected)
