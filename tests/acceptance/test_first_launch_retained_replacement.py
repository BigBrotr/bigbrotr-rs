"""Unique/grouped retained verifier controls; no engines or product runs."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import test_first_launch_retained as retained
from test_first_launch_retained import (ROOT,
    retained_successor_fixture, compacted_archive_fixture)
from cases import first_launch_retained as case
from cases import first_launch_retained_oracle as check
from cases import first_launch_index_oracle as index
from cases import first_launch_restart as source
from installed_archive_catch_up_fault_oracle import wire_checked, ERRORS
import installed_archive_export_oracle as exports


def catalog_fixture(science,attempts):
    """Literal append-only file counts from independent frozen science."""
    names=(*science['tables'],'archive_commits')
    tables=[dict(table_name=name,table_id=n,schema_id=1,path=name+'/') for n,name in enumerate(names,1)]
    catalog={'public.ducklake_table':tables,'public.ducklake_schema':[dict(schema_id=1,path='main/')],
        'public.ducklake_snapshot':[dict(snapshot_id=n,xmin=str(100+n)) for n in range(len(science['batches'])+2)],
        'public.ducklake_data_file':[],'public.ducklake_snapshot_changes':[],
        'public.ducklake_file_column_stats':[],'public.ducklake_table_stats':[],
        'public.ducklake_table_column_stats':[],'bigbrotr_archive.owner':[dict(enabled=False,ordinal=2,xmin='90')],
        'bigbrotr_archive.attempts':[]}
    files={'.initial':dict(mode=0o600,inode=1,device=1,links=1,sha256='d'*64)}
    for snapshot,batch in enumerate(science['batches'],2):
        counts={'archive_commits':1}
        for name,rows in science['tables'].items():
            count=sum(r['source_record_id']['owner_epoch']==batch['epoch'] and
                batch['first']<=r['source_record_id']['sequence']<=batch['last'] for r in rows)
            if count:counts[name]=count
        for name,count in counts.items():
            fid=len(catalog['public.ducklake_data_file'])+1
            table=index.only(tables,table_name=name)
            relative='prepared-'+f'{fid:032x}'+'/data.parquet'
            catalog['public.ducklake_data_file'].append(dict(data_file_id=fid,table_id=table['table_id'],
                begin_snapshot=snapshot,end_snapshot=None,path_is_relative=True,file_format='parquet',
                record_count=count,xmin=str(100+snapshot),path=relative,file_size_bytes=100,footer_size=10))
            path=Path('main')/name/relative
            for parent in path.parents:
                if str(parent)!='.':files.setdefault(str(parent),dict(mode=0o700,inode=100+len(files),device=1,links=2,sha256=None))
            files[str(path)]=dict(mode=0o600,inode=100+len(files),device=1,links=1,sha256=f'{fid:064x}')
    for attempt in attempts:
        row={k:attempt[k] for k in ('ordinal','attempt_id','coordinator_incarnation','worker_incarnation','topology_generation','batch_digest')}
        catalog['bigbrotr_archive.attempts'].append(dict(row,login_name='bb_archive_'+attempt['attempt_id'][2:],
            login_oid=str(1000+attempt['ordinal']),xmin=str(2000+attempt['ordinal'])))
    return catalog,files


def fault_fixture(root,*,phase='unique'):
    science=check.compacted_archive_science(retained_successor_fixture(root)[-1])
    prefix,parked,before,final,report,_,_,_=compacted_archive_fixture(science)
    owner=before['owner_epochs'][0]
    owner.update(dataset_id=before['datasets'][0]['dataset_id'],role_instance_id='\\x'+'01'*16,state='released',
        released_at_us=1000,exclusion_method='storage_fenced',index_publication_protocol=None,
        control_fence_version=1,control_backend_pid=124,control_backend_start='2026-10-29T01:00:00+00:00',xmin='80')
    owner['exclusion_digest']=check.control_exclusion(owner,owner)
    before['role_instances']=[dict(role_instance_id=owner['role_instance_id'],dataset_id=owner['dataset_id'],
        role='control',created_at_us=owner['acquired_at_us'],retired_at_us=owner['released_at_us'],xmin='80')]
    claimed=retained.ClosedReclamationControls().claimed_control
    if phase=='grouped-second':
        first=claimed(before,failed=True);control=first['owner_epochs'][-1]
        first['archive_attempts'].append(dict(final['archive_attempts'][3],coordinator_incarnation=control['process_incarnation'],
            issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],issuer_incarnation=control['process_incarnation'],
            issued_at_us=control['acquired_at_us']+1,xmin='9001'))
        before=first
    after=claimed(before,failed=True)
    control=after['owner_epochs'][-1]
    ordinal=5 if phase=='grouped-second' else 4
    if phase=='grouped-second':
        old=before['owner_epochs'][-1]
        fenced=index.only(after['owner_epochs'],role_instance_id=old['role_instance_id'])
        fenced.update(state='released',released_at_us=old['lease_expires_at_us']+10,exclusion_method='storage_fenced',
            exclusion_digest=check.control_exclusion(old,control),xmin='9000')
        index.only(after['role_instances'],role_instance_id=old['role_instance_id']).update(
            retired_at_us=old['lease_expires_at_us']+15,xmin='9000')
    replacement=dict(final['archive_attempts'][3],coordinator_incarnation=control['process_incarnation'],
        ordinal=ordinal,expected_predecessor=ordinal-1,attempt_id='\\x'+f'{ordinal:032x}',worker_incarnation='\\x'+f'{ordinal+100:032x}',
        issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],issuer_incarnation=control['process_incarnation'],
        issued_at_us=control['acquired_at_us']+1,xmin='9002' if phase=='grouped-second' else '9001')
    after['archive_attempts'].append(replacement)
    count,cut=(2,3) if phase=='grouped-first' else (3,7)
    selected=dict(batches=science['batches'][:count],tables={name:[r for r in rows
        if r['source_record_id']['owner_epoch']==1 and r['source_record_id']['sequence']<=cut]
        for name,rows in science['tables'].items()})
    published={'unique':(1,2,4),'grouped-first':(1,2),'grouped-second':(1,2,5)}[phase]
    receipts=source.publication_receipts(after,selected,published_ordinals=published)
    census=dict(format='bigbrotr-independent-installed-archive-read/1',tables={**copy.deepcopy(selected['tables']),'archive_commits':receipts})
    previous=dict(batches=selected['batches'][:2],tables={name:[r for r in rows
        if r['source_record_id']['sequence']<=3] for name,rows in selected['tables'].items()})
    old_catalog,old_files=catalog_fixture(previous,[r for r in before['archive_attempts'] if r['ordinal']!=3])
    if phase=='grouped-second':old_catalog['bigbrotr_archive.owner']=[dict(enabled=True,ordinal=4,xmin='9002')]
    catalog,files=catalog_fixture(selected,[r for r in after['archive_attempts'] if r['ordinal']!=3])
    catalog['bigbrotr_archive.owner']=[dict(enabled=True,ordinal=ordinal,xmin='9003')]
    return dict(prefix=prefix,parked=parked,before=before,after=after,catalog_before=old_catalog,
        catalog_after=catalog,census=census,science=science,phase=phase),selected,files,old_files


def wire_fixture(local=True,*,mode='after-complete'):
    zero=dict(connections=0,bytes=0,frames=0,data_queries=0,data_commits=0,server_complete=0,client_complete=0,
        faults=0,owner_requests=0,owner_forwarded=0,owner_complete=0,guard_rejections=0,serialization_rejections=0,
        auth_finals_omitted=0,coordinator_inserts=0,pin_claim_injections=0,session_mutations_complete=0,
        reader_queries=0,capture_verify_requests=0,resume_authority_requests=0,resume_expiry_rejections=0,
        resume_expiry_idle=0,rejection_sequence=[],pin_rejections=[],frontend_queries=0)
    common=dict(target='publication',mode=mode,profile='retained-catch-up',frame_limit=131072,connection_limit=64)
    if local:common.update(upstream_host='127.0.0.1',upstream_port=15432,catalog_manager='bigbrotr_catalog',
        catalog_reader='bigbrotr_archive_read',catalog_database='bigbrotr_catalog',armed=True)
    ready=dict(common,**zero,event='ready',port=23456,source_sha256='a'*64)
    fault=dict(common,**dict(zero,connections=1,bytes=100,frames=10,data_queries=1,data_commits=1,server_complete=int(mode=='after-complete'),
        faults=1,frontend_queries=1),event='fault',backend_pid=123)
    end=dict(fault,event='terminal',unexpected=0)
    path=str(ROOT/'experiments/archive-engine/native_commit_proxy.py')
    command=([sys.executable,'-B',path,'--target','publication','--mode',mode,
        '--profile','retained-catch-up','--upstream-port','15432','--catalog-manager','bigbrotr_catalog',
        '--catalog-reader','bigbrotr_archive_read','--catalog-database','bigbrotr_catalog'] if local else
        ['docker','exec','-i','bigbrotr-acquisition-binding-1234567','python3','-B','/lab/archive-catch-up-commit-proxy.py',
            '--target','publication','--mode','after-complete','--profile','retained-catch-up'])
    return dict(status='OBSERVED_CLOSED_AND_JOINED',target='publication',mode=mode,ready=ready,
        process=dict(exit=0,overflow=False,readers_joined=True,stderr='',command=command,
            stdout='\n'.join(json.dumps(r) for r in (ready,fault,end))+'\n'))


def custody_fixture(state,science,ordinals=(3,4)):
    initialization=json.dumps(dict(boot_id='boot-fixture',catalog_root='/var/lib/bigbrotr/archive/')).encode()
    coordinator=b'independent coordinator bytes';binding='\\x'+hashlib.sha256(coordinator).hexdigest()
    records={'coordinator.json':coordinator.hex()};files={}
    for ordinal in ordinals:
        row=index.only(state['archive_attempts'],ordinal=ordinal);row['coordinator_binding_sha256']=binding
        ident=index.raw(row['attempt_id'],16).hex();lock=dict(device=1,inode=ordinal,mode=0o600,links=1)
        files[ident+'/custody.lock']=lock
        intent=dict(format='bigbrotr-local-archive-custody/1',boot_id='boot-fixture',lock_device=1,lock_inode=ordinal,
            catalog_root='/var/lib/bigbrotr/archive/',attempt_id=list(index.raw(row['attempt_id'],16)),publication_ordinal=ordinal,topology_generation='0')
        intent.update({k:index.raw(row[k],size).hex() for k,size in (
            ('dataset_id',16),('catalog_id',16),('coordinator_incarnation',16),('worker_incarnation',16),('batch_digest',32))})
        records[ident+'/intent.json']=json.dumps(intent).encode().hex();records[ident+'/custody.lock']=''
        # Use the already independently checked original A encoder,
        # replacing only the literal identity/ordinal prefix for B.
        original=dict(row,ordinal=3)
        body=index.retained_input_bytes(original,science['batches'][2],hashlib.sha256(initialization).digest())[:-32]
        if ordinal!=3:body=body[:120]+index.be(ordinal)+body[128:]
        records[ident+'.inputs']=(body+hashlib.sha256(body).digest()).hex()
    for name,raw in records.items():files.setdefault(name,{}).update(sha256=hashlib.sha256(bytes.fromhex(raw)).hexdigest())
    return initialization,records,files


def compacted_history_fixture(science):
    before=compacted_archive_fixture(science,schedule='grouped-publication-loss')[3]
    dataset=before['datasets'][0]['dataset_id'];principal='\\x'+'ee'*16
    before['local_accounts']=[dict(principal_id=principal,dataset_id=dataset,username='alice',
        credential_revision=1,authentication_revision=1,grant_revision=4,xmin='90')]
    before['browser_sessions']=[dict(session_id='\\x'+'ed'*16,principal_id=principal,revoked_at_us=1000)]
    before['login_buckets']=[dict(dataset_id=dataset,kind=kind,bucket=value[0],attempts=1,
        window_started_at_us=1000,xmin='91') for kind,value in source.login_buckets().items()]
    before['management_audit']=[dict(audit_id='\\x'+'01'*16,action='dataset.initialize',immutable='original audit',xmin='92')]
    for table in ('snapshot_leases','archive_snapshot_pins','retention_claims'):
        before[table]=[dict(lease_id='old-export',purpose='export',state='active',xmin='93')]
    claimed=retained.ClosedReclamationControls().claimed_control
    startup=claimed(before,failed=True);after=claimed(before)
    owner=index.only(after['owner_epochs'],role='control',epoch=3)
    issued_session=dict(session_id='\\x'+'ec'*16,dataset_id=dataset,principal_id=principal,
        credential_revision=1,authentication_revision=1,token_digest='\\x'+'eb'*32,csrf_digest='\\x'+'ea'*32,
        issued_at_us=1305,expires_at_us=3600,idle_window_us=500,last_seen_at_us=1305,idle_expires_at_us=1805,
        revoked_at_us=None,xmin='5000')
    # The real client logs in, GETs /me, then publishes its setup marker. That
    # mandatory touch commits separately from the session/issue-audit INSERTs.
    session=dict(issued_session,last_seen_at_us=1315,idle_expires_at_us=1815,xmin='5001')
    startup['browser_sessions']=[session]
    after['browser_sessions']=[dict(session,last_seen_at_us=1370,idle_expires_at_us=1870,revoked_at_us=1380,xmin='5002')]
    for state in (startup,after):state['login_buckets']=[dict(r,attempts=2,xmin='4999') for r in before['login_buckets']]
    def audit(action,ident,at,tx):return dict(audit_id='\\x'+ident*16,dataset_id=dataset,actor_id=principal,
        action=action,outcome='accepted',created_at_us=at,config_revision=1,policy_revision=1,subject_id=session['session_id'],
        reason=None,requester_database_role=None,requester_os_uid=None,xmin=tx)
    startup['management_audit'].append(audit('browser.session.issue','02',1306,'5000'))
    after['management_audit']=copy.deepcopy(startup['management_audit'])+[audit('browser.session.revoke','03',1381,'5002')]
    catalog=index.only(after['consumers'],consumer_role='archive')['consumer_id']
    for number,at in enumerate((1310,1340,1360),1):
        ident='\\x'+f'{number+500:032x}';tx=str(5100+number)
        lease=dict(lease_id=ident,dataset_id=dataset,consumer_id=catalog,frontier_revision=4,topology_generation=1,
            holder_kind='role_instance',holder_job_id=None,holder_role_instance_id=owner['role_instance_id'],
            holder_role='control',holder_incarnation=owner['process_incarnation'],archive_pin_protocol=1,
            state='released',lease_revision=2,acquired_at_us=at,updated_at_us=at+2,expires_at_us=10000,
            released_at_us=at+2,release_reason='reader_joined',xmin=tx)
        pin=dict(lease_id=ident,dataset_id=dataset,consumer_id=catalog,frontier_revision=4,purpose='reader',
            issuer_role='control',issuer_resource=owner['resource_id'],issuer_epoch=3,
            issuer_incarnation=owner['process_incarnation'],requested_ttl_us=30_000_000,xmin='4900')
        claims=[dict(claim_id='\\x'+f'{number*10+epoch:032x}',lease_id=ident,dataset_id=dataset,topology_generation=1,
            shard_id=0,owner_epoch=epoch,first_sequence=1,last_sequence=cut,purpose='reader',state='released',
            created_at_us=at+1,released_at_us=at+2,release_reason='reader_joined',xmin=tx) for epoch,cut in ((1,312),(2,10))]
        for state in ((startup,after) if number==1 else (after,)):
            state['snapshot_leases'].append(copy.deepcopy(lease));state['archive_snapshot_pins'].append(copy.deepcopy(pin))
            state['retention_claims'].extend(copy.deepcopy(claims))
    return before,after,startup


def second_export_fixture(root,science):
    """Literal second-job SQL/files over the existing independent full82 fixture."""
    before,after,startup=compacted_history_fixture(science)
    scope=dict(dataset_id='22'*16,dictionary_kind=4,allocation_domain='68'*16,ordinal='3')
    initialized=dict(private_scope=scope);catalog='55'*16;bx='\\x'
    old=json.loads((ROOT/'tests/fixtures/ingest/import-event.jsonl').read_text())
    new=next(r['event'] for r in json.loads((ROOT/'tests/fixtures/acquisition/selector-events.json').read_text())['events'] if r['name']=='included')
    destination=root/'destination';destination.mkdir();(destination/'destination.json').write_text('{}')
    homes=[];verdicts=[]
    # Build both portable artifacts from fixture fields and literal contract
    # values, never from the validator or a candidate serializer.
    for n,event in enumerate((old,new),1):
        home=root/str(n);home.mkdir();homes.append(home)
        (home/'destination').mkdir();(home/'destination/destination.json').write_text('{}')
        case.save(home/'event.jsonl',event);job=f'{240+n:02x}'*16;artifact=f'{160+n:02x}'*16
        directory=destination/artifact;directory.mkdir()
        encoded=(json.dumps({k:event[k] for k in ('id','pubkey','created_at','kind','tags','content','sig')},ensure_ascii=False,separators=(',',':'))+'\n').encode()
        (directory/'events.jsonl').write_bytes(encoded);sha=hashlib.sha256(encoded).hexdigest()
        manifest=dict(format='bigbrotr-signed-events-export/1',artifact_id=artifact,job_id=job,
            visibility='private',fidelity='full-signed-events',origin='retained-scoped-archive',
            source=dict(cut=dict(dataset_id='22'*16,topology_generation='1',bounds=[
                dict(stream=dict(shard_id=0,owner_epoch='1'),last_complete_sequence='312'),
                dict(stream=dict(shard_id=0,owner_epoch='2'),last_complete_sequence='10')]),
                catalog_id=catalog,frontier_revision='4',snapshot_id='68'),selection=dict(event_id=event['id'],scope=scope),
            accepted_revisions=dict(configuration='1',policy='1'),included_families=['signed-events'],
            excluded_families=['internal-payload-envelopes','eligibility-facts','first-local-receive','relay-provenance',
                'imported-claims','measurements','documents','dictionaries','coverage','derived-results','deployment-state'],
            files=[dict(file_id='events',path='events.jsonl',format='nostr-jsonl/1',bytes=str(len(encoded)),rows='1',sha256=sha)])
        raw=(json.dumps(manifest,ensure_ascii=False,separators=(',',':'))+'\n').encode()
        (directory/'manifest.json').write_bytes(raw);msha=hashlib.sha256(raw).hexdigest()
        verdicts.append(dict(job_id=job,artifact_id=artifact,jsonl_bytes=len(encoded),jsonl_sha256=sha,manifest_sha256=msha,
            manifest=manifest,export_pin_retained=True,all_fresh_readers_released=True,original_acquisition_and_publication_unchanged=True))
        case.save(home/'export-client-result.json',dict(status='PASS',job_id=job))
    first,second=verdicts;home=homes[1];case.save(home/'conflict-event.jsonl',old)
    case.save(home/'export-input.json',dict(scope=scope,catalog_id=catalog,destination_id='d3'*16,
        retained_compacted_epoch2=True,retained_second_export=True,startup_quiescence=True))
    principal=before['local_accounts'][0]['principal_id'];dataset=bx+'22'*16
    for state in (before,startup,after):
        for number,row in enumerate(state['jobs'],1):
            row.setdefault('kind','acquisition');row.setdefault('job_id','\\x'+f'{number:032x}')
        state['jobs'].append(dict(job_id=bx+first['job_id'],kind='export',state='succeeded',completed_at_us=1000,result_manifest_digest=bx+first['manifest_sha256'],xmin='94'))
        state['export_jobs']=[dict(job_id=bx+first['job_id'],request_key=bx+'d1'*16,event_id=bx+old['id'],pin_lease_id='old-export',
            catalog_id=bx+catalog,frontier_revision=4,destination_id=bx+'d3'*16,destination_binding_sha256=bx+hashlib.sha256(b'{}').hexdigest())]
        state['export_execution_units']=[dict(job_id=bx+first['job_id'],immutable='old unit',xmin='95')]
        state['artifacts']=[dict(job_id=bx+first['job_id'],artifact_id=bx+first['artifact_id'],manifest_sha256=bx+first['manifest_sha256'],
            manifest_bytes=(destination/first['artifact_id']/'manifest.json').stat().st_size,total_bytes=first['jsonl_bytes']+(destination/first['artifact_id']/'manifest.json').stat().st_size,
            file_count=1,completed_at_us=1000,verification_pin_id='first-reader',xmin='94')]
        state['artifact_files']=[dict(artifact_id=bx+first['artifact_id'],byte_count=353,row_count=1,sha256=bx+first['jsonl_sha256'],xmin='94')]
        state['archive_snapshot_pins'].append(dict(lease_id='first-reader',purpose='reader',xmin='96'))
        state['snapshot_leases'].append(dict(lease_id='first-reader',state='released',xmin='97'))
        state['scope_grants']=[dict(principal_id=principal,dataset_id=dataset,allocation_domain=bx+'68'*16,ordinal=3,
            action=action,enabled=True,generation=1,xmin='98') for action in ('history.read','export','private-export')]
        state['scope_grant_changes']=copy.deepcopy(state['scope_grants'])
    owner=after['owner_epochs'][-1];session=after['browser_sessions'][0];binding=hashlib.sha256(b'{}').hexdigest()
    # The second client touches only its initial /me. Job acceptance/status,
    # retries and logout leave that touch and idle expiry unchanged.
    for field in ('last_seen_at_us','idle_expires_at_us'):
        session[field]=startup['browser_sessions'][0][field]
    key='d2'*16;job=bx+second['job_id'];artifact=bx+second['artifact_id'];created=1341;completed=1361
    allocation,execution=[r['lease_id'] for r in after['snapshot_leases'] if r.get('acquired_at_us') in (1340,1360)]
    export_pin=bx+'da'*16
    source_pin=next(r for r in after['archive_snapshot_pins'] if r['lease_id']==allocation)
    source_lease=next(r for r in after['snapshot_leases'] if r['lease_id']==allocation)
    for row in after['archive_snapshot_pins']:
        if row['lease_id'] in (allocation,execution):row.update(requested_ttl_us=5_000_000,export_job_id=None)
    after['archive_snapshot_pins'].append(dict(source_pin,lease_id=export_pin,purpose='export',requested_ttl_us=30_000_000,export_job_id=job,xmin='5200'))
    after['snapshot_leases'].append(dict(source_lease,lease_id=export_pin,acquired_at_us=created,updated_at_us=created,
        state='active',lease_revision=1,released_at_us=None,release_reason=None,xmin='5200'))
    after['retention_claims'].extend(dict(claim_id=bx+f'{900+epoch:032x}',lease_id=export_pin,dataset_id=dataset,topology_generation=1,
        shard_id=0,owner_epoch=epoch,first_sequence=1,last_sequence=cut,purpose='export',state='active',created_at_us=created,
        released_at_us=None,release_reason=None,xmin='5200') for epoch,cut in ((1,312),(2,10)))
    be=lambda v,w=8:int(v).to_bytes(w,'big');r=bytes.fromhex
    digest=hashlib.sha256(b'bigbrotr/export-event/1\0'+r('22'*16)+r(principal[2:])+r(key)+r('22'*16)+be(4,2)+r('68'*16)+be(3)
        +r(new['id'])+be(100,2)+r(catalog)+be(4)+r('d3'*16)+r(binding)+b'\x01\x01'+be(1)+be(1)).hexdigest()
    after['jobs'].append(dict(job_id=job,dataset_id=dataset,principal_id=principal,request_key=bx+key,request_digest=bx+digest,
        kind='export',config_revision=1,policy_revision=1,created_at_us=created,input_topology_generation=1,input_complete=True,
        export_job_protocol=1,acquisition_job_protocol=None,state='succeeded',job_revision=4,checkpoint_revision=1,owner_role='control',
        owner_resource_id=owner['resource_id'],owner_epoch=3,owner_incarnation=owner['process_incarnation'],completed_at_us=completed,
        reason_code=None,result_manifest_digest=bx+second['manifest_sha256'],xmin='5202'))
    after['export_jobs'].append(dict(job_id=job,dataset_id=dataset,actor_id=principal,request_key=bx+key,format='signed-events-jsonl',visibility='private',
        dictionary_kind=4,allocation_domain=bx+'68'*16,ordinal=3,event_id=bx+new['id'],page_size=100,catalog_id=bx+catalog,frontier_revision=4,
        destination_id=bx+'d3'*16,destination_binding_sha256=bx+binding,pin_lease_id=export_pin,allocation_pin_id=allocation,
        session_id=session['session_id'],actor_credential_revision=1,actor_authentication_revision=1,actor_grant_revision=4,xmin='5200'))
    after['job_admissions'].append(dict(job_id=job,admission_ordinal=4,xmin='5200'))
    after['job_input_bounds'].extend(dict(job_id=job,dataset_id=dataset,topology_generation=1,shard_id=0,owner_epoch=epoch,
        last_complete_sequence=cut,xmin='5200') for epoch,cut in ((1,312),(2,10)))
    after['export_execution_units'].append(dict(job_id=job,artifact_id=artifact,max_output_bytes=256*1024**2,max_temporary_bytes=512*1024**2,
        reservation_transaction='5201',reserved_at_us=1350,xmin='5201'))
    after['artifacts'].append(dict(artifact_id=artifact,job_id=job,dataset_id=dataset,principal_id=principal,destination_id=bx+'d3'*16,
        destination_binding_sha256=bx+binding,manifest_format='bigbrotr-signed-events-export/1',manifest_bytes=(destination/second['artifact_id']/'manifest.json').stat().st_size,
        manifest_sha256=bx+second['manifest_sha256'],file_count=1,total_bytes=(destination/second['artifact_id']/'manifest.json').stat().st_size+358,
        verification_pin_id=execution,completed_at_us=completed,xmin='5202'))
    after['artifact_files'].append(dict(artifact_id=artifact,file_id='events',relative_path='events.jsonl',format='nostr-jsonl/1',byte_count=358,row_count=1,sha256=bx+second['jsonl_sha256'],xmin='5202'))
    audit_id=bx+'d4'*16
    after['management_commands'].append(dict(actor_id=principal,request_key=bx+key,dataset_id=dataset,action='job.export.accept',request_digest=bx+digest,
        expected_config_revision=1,policy_revision=1,audit_id=audit_id,accepted_at_us=created,authority_mode=None,document_digest=None,xmin='5200'))
    after['management_audit'].append(dict(audit_id=audit_id,dataset_id=dataset,actor_id=principal,action='job.export.accept',outcome='accepted',created_at_us=created,
        config_revision=1,subject_id=bx+key,policy_revision=1,reason=None,requester_database_role=None,requester_os_uid=None,xmin='5200'))
    body=dict(kind='export',format='signed-events-jsonl',query=dict(version=1,resource='event_history',mode='snapshot',scope=scope,filters=[dict(ids=[new['id']])]),
        allocation=dict(catalog_id=catalog,frontier_revision='4'),destination=dict(id='d3'*16,binding_sha256=binding),expected_config_revision='1',expected_policy_revision='1')
    receipt=dict(format='bigbrotr-job-acceptance/1',command_key=key,job_id=second['job_id'],accepted_at_us=str(created),created=True,execution='unobserved',authority='acceptance-only')
    case.save(home/'export-client-result.json',dict(status='PASS',job_id=second['job_id'],history=None,completed=dict(format='bigbrotr-job-status/1',job_id=second['job_id'],kind='export',state='succeeded',
        job_revision='4',checkpoint_revision='1',created_at_us=str(created),completed_at_us=str(completed),observed_at_us='1365',reason_code=None,authority='observation-only')))
    case.save(home/'export-observations.json',[dict(case='accept-export',status=202,command=body,key=key,receipt=receipt),
        *[dict(case=label,status=200,receipt=dict(receipt,created=False)) for label in ('second-export-identical-retry','second-export-completed-retry')],
        dict(case='second-export-changed-event-conflict',status=409,key=key,command=dict(body,query=dict(body['query'],filters=[dict(ids=[old['id']])])),
            error=dict(status=409,code='command_conflict',retry_class='check_state'))])
    return before,after,startup,homes,initialized,catalog,destination,verdicts


class UnboundReplacementControls(unittest.TestCase):
    def test_first_export_rejects_all_unselected_inventory_default_and_retained(self):
        # Reuse the literal original353-byte artifact and full82-table fixture.
        # This control isolates the first-job whole-inventory boundary; it does
        # not claim another installed execution or full SQL lifecycle proof.
        for retained_profile in (False,True):
            with self.subTest(retained=retained_profile),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);science=check.compacted_archive_science(retained_successor_fixture(root)[-1])
                after,_,_,homes,initialized,catalog,destination,verdicts=second_export_fixture(root,science)
                before=copy.deepcopy(after);first=verdicts[0];job_id='\\x'+first['job_id']
                owner=max((r for r in after['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
                for table in ('owner_epochs','role_instances'):
                    before[table]=[r for r in before[table] if r.get('role_instance_id')!=owner['role_instance_id']]
                before['jobs']=[r for r in before['jobs'] if r['kind']!='export']
                for table in ('export_jobs','export_execution_units','artifacts','artifact_files','job_input_bounds'):
                    before[table]=[]
                session=after['browser_sessions'][0]
                after['browser_sessions']=[dict(session,session_id='\\x'+'ec'*16,issued_at_us=1105,revoked_at_us=1190)]
                after['jobs'][-1].update(checkpoint_revision=1,input_complete=True,
                    owner_resource_id=owner['resource_id'],owner_epoch=owner['epoch'])
                after['job_admissions'].append(dict(job_id=job_id,admission_ordinal=3 if retained_profile else 2))
                after['export_execution_units']=[dict(job_id=job_id,artifact_id='\\x'+first['artifact_id'],
                    max_output_bytes=256*1024**2,max_temporary_bytes=512*1024**2)]
                cuts=[(1,312),(2,10)] if retained_profile else [(1,12)]
                after['job_input_bounds']=[dict(job_id=job_id,shard_id=0,owner_epoch=epoch,last_complete_sequence=cut) for epoch,cut in cuts]
                initialized['topology_generation']='0'
                manifest=copy.deepcopy(first['manifest'])
                if not retained_profile:
                    manifest['source'].update(snapshot_id='5',cut=dict(dataset_id='22'*16,topology_generation='0',
                        bounds=[dict(stream=dict(shard_id=0,owner_epoch='1'),last_complete_sequence='12')]))
                raw=(json.dumps(manifest,ensure_ascii=False,separators=(',',':'))+'\n').encode()
                (destination/first['artifact_id']/'manifest.json').write_bytes(raw)
                digest='\\x'+hashlib.sha256(raw).hexdigest()
                after['artifacts'][0].update(manifest_bytes=len(raw),total_bytes=len(raw)+353,manifest_sha256=digest)
                after['jobs'][-1]['result_manifest_digest']=digest
                def verify(state):
                    return exports.validate(before,state,homes[0],initialized,catalog,expected_principal='ee'*16,
                        expected_table_count=82,expected_tables=before,destination_root=destination,
                        initial_cut=312 if retained_profile else 12,retained_compacted_epoch2=retained_profile)
                self.assertEqual(verify(after)['jsonl_bytes'],353)
                for table in ('export_jobs','export_execution_units','artifacts','artifact_files','job_input_bounds'):
                    changed=copy.deepcopy(after);extra=dict(changed[table][0])
                    extra['artifact_id' if table=='artifact_files' else 'job_id']='\\x'+'fe'*16
                    changed[table].append(extra)
                    with self.subTest(extra_unselected=table),self.assertRaises((AssertionError,ValueError)):
                        verify(changed)

    def test_second_export_full_state_and_both_standalone_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);science=check.compacted_archive_science(retained_successor_fixture(root)[-1])
            before,after,startup,homes,initialized,catalog,destination,verdicts=second_export_fixture(root,science)
            def verify(state):
                verdict=exports.validate(before,state,homes[1],initialized,catalog,expected_principal='ee'*16,
                    expected_table_count=82,expected_tables=before,destination_root=destination,initial_cut=312,
                    retained_compacted_epoch2=True,retained_second_export=True)
                view=exports.second_job_rows(before,state,homes[1],initialized,catalog,verdict)
                check.compacted_history_control(before,view,startup)
                return verdict
            self.assertEqual(verify(after),verdicts[1])
            for home,verdict in zip(homes,verdicts):
                self.assertEqual(exports.verify_artifact(after,home,initialized,catalog,destination_root=destination),verdict)
            for table in ('jobs','export_jobs','job_admissions','job_input_bounds','export_execution_units','artifacts','artifact_files',
                    'management_commands','scope_grants','scope_grant_changes','local_accounts','producer_outcomes','producer_requests',
                    'producer_checkpoints','archive_attempts','archive_frontier_batches','consumer_frontiers','snapshot_leases',
                    'archive_snapshot_pins','retention_claims','management_audit','owner_epochs','role_instances'):
                for operation in ('rewrite','delete'):
                    state=copy.deepcopy(after)
                    if not before[table]:continue
                    original=before[table][0];target=next(r for r in state[table] if r==original)
                    if operation=='rewrite':target['xmin']='999999'
                    else:state[table].remove(target)
                    with self.subTest(old_table=table,operation=operation),self.assertRaises((AssertionError,ValueError)):
                        verify(state)
            for table,field,value in (('jobs','checkpoint_revision',2),('jobs','request_digest','\\x'+'00'*32),
                    ('export_jobs','event_id',before['export_jobs'][0]['event_id']),('export_jobs','actor_grant_revision',3),
                    ('export_jobs','catalog_id','\\x'+'00'*16),('job_admissions','admission_ordinal',5),
                    ('job_input_bounds','last_complete_sequence',12),('export_execution_units','max_output_bytes',1),
                    ('artifacts','manifest_sha256','\\x'+'00'*32),('artifact_files','row_count',0),
                    ('management_commands','request_key','\\x'+'d1'*16),('management_audit','action','unrelated')):
                state=copy.deepcopy(after);target=next(r for r in state[table] if r not in before[table]);target[field]=value
                with self.subTest(new_table=table,field=field),self.assertRaises((AssertionError,ValueError)):
                    verify(state)
            for table in ('snapshot_leases','archive_snapshot_pins','retention_claims'):
                for purpose in ('reader','export'):
                    state=copy.deepcopy(after)
                    ids={p['lease_id'] for p in state['archive_snapshot_pins'] if p.get('purpose')==purpose and p not in before['archive_snapshot_pins']}
                    target=next(r for r in state[table] if r['lease_id'] in ids and r not in startup[table])
                    field,value={'snapshot_leases':('lease_revision',99),'archive_snapshot_pins':('issuer_epoch',99),'retention_claims':('last_sequence',99)}[table]
                    target[field]=value
                    with self.subTest(pin_table=table,purpose=purpose),self.assertRaises((AssertionError,ValueError)):
                        verify(state)
            for table in ('jobs','export_jobs','export_execution_units','artifacts','artifact_files','job_admissions',
                    'management_commands','management_audit','snapshot_leases','archive_snapshot_pins','retention_claims'):
                state=copy.deepcopy(after);state[table].append(copy.deepcopy(state[table][-1]))
                with self.subTest(extra_retry_work=table),self.assertRaises((AssertionError,ValueError)):
                    verify(state)
            for table in ('jobs','export_jobs','export_execution_units','artifacts','artifact_files'):
                state=copy.deepcopy(after);state[table].append(copy.deepcopy(before[table][0]))
                with self.subTest(duplicated_old_row=table),self.assertRaises((AssertionError,ValueError)):
                    verify(state)

    def test_second_export_uses_last_explicit_touch_and_existing_auth_expiries(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);science=check.compacted_archive_science(retained_successor_fixture(root)[-1])
            before,after,startup,homes,initialized,catalog,_,verdicts=second_export_fixture(root,science)
            session=after['browser_sessions'][0];job=after['jobs'][-1]
            self.assertLess(session['last_seen_at_us'],job['created_at_us'])
            self.assertLess(session['last_seen_at_us'],job['completed_at_us'])
            for field in ('last_seen_at_us','idle_expires_at_us'):
                self.assertEqual(session[field],startup['browser_sessions'][0][field])
            view=exports.second_job_rows(before,after,homes[1],initialized,catalog,verdicts[1])
            check.compacted_history_control(before,view,startup)
            for field,value in (('issued_at_us',1342),('last_seen_at_us',1342),('last_seen_at_us',1370)):
                changed=copy.deepcopy(after);changed['browser_sessions'][0][field]=value
                with self.subTest(pre_auth_or_unissued_touch=(field,value)),self.assertRaises(AssertionError):
                    exports.second_job_rows(before,changed,homes[1],initialized,catalog,verdicts[1])
            observed=int(json.loads((homes[1]/'export-client-result.json').read_text())['completed']['observed_at_us'])
            for boundary in ('idle','absolute'):
                for delta in (0,1):
                    changed,setup=copy.deepcopy((after,startup));expires=observed-delta
                    for state in (changed,setup):
                        row=state['browser_sessions'][0]
                        if boundary=='idle':row['idle_window_us']=expires-row['last_seen_at_us']
                        else:row['expires_at_us']=expires
                        row['idle_expires_at_us']=min(row['expires_at_us'],row['last_seen_at_us']+row['idle_window_us'])
                    # Other full82/session relationships remain valid. The
                    # authenticated terminal200 specifically needs this strict
                    # observation-before-expiry bound, without refreshing it.
                    check.compacted_history_control(before,dict(view,browser_sessions=changed['browser_sessions']),setup)
                    with self.subTest(expiry=boundary,at_or_after=delta),self.assertRaises(AssertionError):
                        exports.second_job_rows(before,changed,homes[1],initialized,catalog,verdicts[1])

    def test_second_export_rejects_retry_conflict_and_standalone_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);science=check.compacted_archive_science(retained_successor_fixture(root)[-1])
            before,after,startup,homes,initialized,catalog,destination,verdicts=second_export_fixture(root,science)
            path=homes[1]/'export-observations.json';original=path.read_bytes()
            for label,field,value in (('accept-export','key','d1'*16),('second-export-identical-retry','status',202),
                    ('second-export-completed-retry','receipt',{}),('second-export-changed-event-conflict','status',200),
                    ('second-export-changed-event-conflict','error',dict(status=409,code='other',retry_class='check_state'))):
                observations=json.loads(original);next(r for r in observations if r['case']==label)[field]=value
                case.save(path,observations)
                with self.subTest(label=label,field=field),self.assertRaises((AssertionError,ValueError)):
                    exports.second_job_rows(before,after,homes[1],initialized,catalog,verdicts[1])
            path.write_bytes(original)
            for home,verdict in zip(homes,verdicts):
                for name in ('events.jsonl','manifest.json'):
                    path=destination/verdict['artifact_id']/name;original=path.read_bytes()
                    for changed in (b' '+original,original[:-1],bytes([original[0]^1])+original[1:]):
                        path.write_bytes(changed)
                        with self.subTest(artifact=home.name,file=name),self.assertRaises(AssertionError):
                            exports.verify_artifact(after,home,initialized,catalog,destination_root=destination)
                    path.write_bytes(original)
            # Mutating both the expected fixture and JSONL cannot hide a bad
            # signature behind a matching event ID and updated file digest.
            home=homes[1];event=json.loads((home/'event.jsonl').read_text());event['sig']='00'*64
            case.save(home/'event.jsonl',event)
            selected=destination/verdicts[1]['artifact_id']
            encoded=(json.dumps({k:event[k] for k in ('id','pubkey','created_at','kind','tags','content','sig')},separators=(',',':'))+'\n').encode()
            (selected/'events.jsonl').write_bytes(encoded)
            manifest=json.loads((selected/'manifest.json').read_text());sha=hashlib.sha256(encoded).hexdigest()
            manifest['files'][0]['sha256']=sha
            raw=(json.dumps(manifest,separators=(',',':'))+'\n').encode();(selected/'manifest.json').write_bytes(raw)
            state=copy.deepcopy(after);msha=hashlib.sha256(raw).hexdigest()
            state['artifact_files'][-1]['sha256']='\\x'+sha
            state['artifacts'][-1]['manifest_sha256']=state['jobs'][-1]['result_manifest_digest']='\\x'+msha
            with self.assertRaisesRegex(AssertionError,'BIP340'):
                exports.verify_artifact(state,home,initialized,catalog,destination_root=destination)
            # This assertion establishes the discriminator: all other file and
            # metadata relationships still pass if independent crypto is omitted.
            with patch.object(exports._signed,'equation_verify',return_value=True):
                exports.verify_artifact(state,home,initialized,catalog,destination_root=destination)
            public=bytes.fromhex('F9308A019258C31049344F85F89D5229B531C845836F99B08601F113BCE036F9')
            signature=bytes.fromhex('E907831F80848D1069A5371B402410364BDF1C5F8307B0084C55F1CE2DCA821525F66A4A85EA8B71E482A74F382D2CE5EBEEE8FDB2172F477DF4900D310536C0')
            self.assertTrue(exports._signed.equation_verify(bytes(32),public,signature))
            self.assertFalse(exports._signed.equation_verify(b'\x01'+bytes(31),public,signature))

    def test_history_login_me_startup_sequence_has_a_separate_touch_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            science=check.compacted_archive_science(retained_successor_fixture(Path(directory))[-1])
            before,after,startup=compacted_history_fixture(science)
            def verify(state,setup):source.history_unchanged(before,state,startup=setup,retained_compacted_epoch2=True)
            issued=index.only(startup['management_audit'],action='browser.session.issue')
            session=index.only(startup['browser_sessions']);revoked=index.only(after['browser_sessions'])
            self.assertLess(session['issued_at_us'],issued['created_at_us'])
            self.assertLess(issued['created_at_us'],session['last_seen_at_us'])
            self.assertEqual((issued['xmin'],session['xmin'],revoked['xmin']),('5000','5001','5002'))
            verify(after,startup)
            # Separate transactions may share a microsecond timestamp. The real
            # /me UPDATE is still required and must retain its distinct xmin.
            same_clock,same_start=copy.deepcopy((after,startup))
            same_start['browser_sessions'][0].update(last_seen_at_us=1305,idle_expires_at_us=1805)
            for state in (same_clock,same_start):state['management_audit'][1]['created_at_us']=1305
            verify(same_clock,same_start)
            def missing_touch(final,setup):
                setup['browser_sessions'][0].update(last_seen_at_us=1305,idle_expires_at_us=1805,xmin='5000')
                for state in (final,setup):state['management_audit'][1]['created_at_us']=1305
            def expired_initial_idle(final,setup):
                for state in (final,setup):
                    row=state['browser_sessions'][0];row['idle_window_us']=5
                    row['idle_expires_at_us']=row['last_seen_at_us']+5
            def both_issue(final,setup,field,value):
                for state in (final,setup):state['management_audit'][1][field]=value
            for label,mutate in {
                    'missing-me-touch':missing_touch,
                    'retrograde-startup':lambda f,s:s['browser_sessions'][0].update(last_seen_at_us=1304,idle_expires_at_us=1804),
                    'retrograde-final':lambda f,s:f['browser_sessions'][0].update(last_seen_at_us=1314,idle_expires_at_us=1814),
                    'outside-owner-window':lambda f,s:s['browser_sessions'][0].update(last_seen_at_us=1401,idle_expires_at_us=1901),
                    'expired-initial-idle':expired_initial_idle,
                    'wrong-startup-idle':lambda f,s:s['browser_sessions'][0].update(idle_expires_at_us=1816),
                    'wrong-final-idle':lambda f,s:f['browser_sessions'][0].update(idle_expires_at_us=1871),
                    'changed-issued-time':lambda f,s:f['browser_sessions'][0].update(issued_at_us=1306),
                    'changed-token':lambda f,s:f['browser_sessions'][0].update(token_digest='\\x'+'00'*32),
                    'changed-credential':lambda f,s:s['browser_sessions'][0].update(credential_revision=2),
                    'changed-original-audit':lambda f,s:f['management_audit'][0].update(xmin='rewritten'),
                    'changed-issue-audit':lambda f,s:f['management_audit'][1].update(xmin='5003'),
                    'issue-after-me':lambda f,s:both_issue(f,s,'created_at_us',1316),
                    'issue-before-login':lambda f,s:both_issue(f,s,'created_at_us',1304),
                    'issue-wrong-principal':lambda f,s:both_issue(f,s,'actor_id','\\x'+'00'*16),
                    'issue-reused-old-xmin':lambda f,s:both_issue(f,s,'xmin','92'),
                    'missing-touch-transaction':lambda f,s:s['browser_sessions'][0].update(xmin='5000'),
                    'malformed-touch-xmin':lambda f,s:s['browser_sessions'][0].update(xmin='05001'),
                    'out-of-range-touch-xmin':lambda f,s:s['browser_sessions'][0].update(xmin=str(2**32)),
                    'wrong-revoke-transaction':lambda f,s:f['management_audit'][-1].update(xmin='5003'),
                    'wrong-revoke-subject':lambda f,s:f['management_audit'][-1].update(subject_id='wrong'),
                    'revoke-audit-before-revoke':lambda f,s:f['management_audit'][-1].update(created_at_us=1379),
            }.items():
                final,setup=copy.deepcopy((after,startup));mutate(final,setup)
                with self.subTest(label=label),self.assertRaises((AssertionError,ValueError)):verify(final,setup)

    def test_export_and_history_terminal_captures_follow_actual_join_and_keep_startup(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);runtime=object.__new__(case.ReclamationRuntime)
            runtime.reclamation_output=root;runtime.home=root/'home';runtime.home.mkdir()
            runtime.state=root/'instance';(runtime.state/'data/export').mkdir(parents=True)
            for path in (runtime.home/'event.jsonl',runtime.home/'fixture-profile.json',runtime.state/'data/export/destination.json'):
                path.write_text('{}')
            runtime.config=Mock();runtime.config.read_text.return_value='config';runtime.set_value=Mock(return_value='config')
            runtime.source_unchanged=Mock();runtime.wait_login_capacity=Mock();runtime.finite=Mock();runtime.wait=Mock()
            runtime.initialized=dict(private_scope={});runtime.ids=dict(catalog='catalog');runtime.enrolled=dict(principal_id='principal')
            runtime.tables=set();runtime.events=[dict(id='first')];runtime.new_event=dict(id='second')
            runtime.archive_schedule='grouped-publication-loss';runtime.complete_index_state={}
            before=dict(phase='before');export=dict(phase='export');second=dict(phase='second');startup=dict(phase='startup');final=dict(phase='history')
            runtime.archive_retry_state=before;runtime.final_native={};runtime.all_input_files={}
            runtime.native_capture=Mock(return_value={})
            census={name:{} for name in ('.artifact-second.lock','second','second/events.jsonl','second/manifest.json')}
            file_captures=iter(({},census,census))
            runtime.files=Mock(side_effect=lambda path:next(file_captures) if path==runtime.state/'data/export' else {})
            verdicts=[dict(jsonl_bytes=353,job_id='first',artifact_id='first'),dict(jsonl_bytes=358,job_id='second',artifact_id='second')]
            runtime.export_validate=Mock(side_effect=verdicts);runtime.restart_checks=Mock()
            order=[];captures=iter(((before,{'before':True}),(export,{'export':True}),
                (export,{'second-before':True}),(second,{'second-after':True}),
                (second,{'history-before':True}),(final,{'history-after':True})))
            def capture(output,label):order.append('capture:'+label);return next(captures)
            runtime.capture=Mock(side_effect=capture)
            runtime.launch=Mock(side_effect=lambda label,*args:dict(label=label))
            def stop(entry,output):order.append('joined:'+entry['label']);return {'State':{'ExitCode':0}}
            runtime.stop_container=Mock(side_effect=stop)
            runtime.backend_absence=Mock(side_effect=lambda output:order.append('backends:'+output.name))
            runtime.verify_archive_preservation=Mock(side_effect=lambda output,label,*args:order.append('preserve:'+label))
            def client(owner,output,entry,**options):
                if options.get('successor'):
                    case.save(output/'history-client-result.json',dict(status='PASS'))
                return startup
            def reread(*args,**kwargs):order.append('reread:'+args[1].name);return verdicts[args[1].name=='second-export']
            with patch.object(case.baseline,'startup_client',side_effect=client),patch.object(case.live_oracle,'publication_unchanged'), \
                    patch.object(exports,'second_job_rows',return_value=second),patch.object(case.oracle,'compacted_history_control') as control, \
                    patch.object(exports,'verify_artifact',side_effect=reread):
                runtime.compacted_history_export()
            runtime.restart_checks.history_unchanged.assert_called_once_with(second,final,startup=startup,retained_compacted_epoch2=True)
            control.assert_called_once_with(export,second,startup)
            self.assertTrue(runtime.export_validate.call_args.kwargs['retained_second_export'])
            for joined,captured,preserved in (('compacted-signed-export','export-after','export'),
                    ('compacted-second-signed-export','after','second-export'),('compacted-history-restart','history-after','history')):
                self.assertLess(order.index('joined:'+joined),order.index('capture:'+captured))
                self.assertLess(order.index('capture:'+captured),order.index('preserve:'+preserved))
            self.assertEqual(runtime.verify_archive_preservation.call_args.args[-1],{'history-after':True})
            self.assertLess(order.index('capture:history-after'),order.index('reread:export'))
            self.assertLess(order.index('capture:history-after'),order.index('reread:second-export'))

    def test_recovery_preserves_the_complete_control_inventory_and_historical_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            science=check.compacted_archive_science(retained_successor_fixture(Path(directory))[-1])
            for schedule in ('uninterrupted','unbound-publication-loss','grouped-publication-loss'):
                prefix,parked,before,after,report,old_catalog,catalog,_=compacted_archive_fixture(science,schedule=schedule)
                def verify(state):check.compacted_archive(prefix,parked,before,state,report,science,old_catalog,catalog,schedule=schedule)
                verify(after)
                for table in ('producer_outcomes','producer_checkpoints','jobs','principal_grants'):
                    for corruption in ('change','delete'):
                        changed=copy.deepcopy(after)
                        if corruption=='change':changed[table][0]['xmin']='rewritten'
                        else:changed[table].pop()
                        with self.subTest(schedule=schedule,table=table,corruption=corruption),self.assertRaises(AssertionError):verify(changed)
                for label,mutate in {
                        'missing-table':lambda s:s.pop('producer_requests'),
                        'old-control':lambda s:s['owner_epochs'][0].update(xmin='rewritten'),
                        'new-control-contract':lambda s:index.only(s['owner_epochs'],role='control',epoch=2).update(archive_frontier_protocol=1),
                        'archive-fence':lambda s:index.only(s['owner_epochs'],role='archive',epoch=1).update(exclusion_digest='\\x'+'00'*32),
                        'archive-session':lambda s:s['archive_owner_sessions'][0].update(xmin='rewritten'),
                        'extra-archive-role':lambda s:s['role_instances'].append(dict(index.only(s['role_instances'],role='archive',role_instance_id='\\x'+'a5'*16),role_instance_id='extra')),
                        'old-publication':lambda s:s['archive_frontier_snapshots'][0].update(xmin='rewritten'),
                        'bridge-authority':lambda s:index.only(s['consumer_frontier_revisions'],consumer_role='archive',frontier_revision=3).update(owner_epoch=1),
                        'final-clock':lambda s:index.only(s['consumer_frontier_revisions'],consumer_role='archive',frontier_revision=4).update(created_at_us=99999999),
                        'final-membership-xmin':lambda s:s['archive_frontier_batches'][-1].update(xmin='rewritten'),
                        'new-attempt-issuer':lambda s:s['archive_attempts'][-1].update(issuer_epoch=1),
                }.items():
                    changed=copy.deepcopy(after);mutate(changed)
                    with self.subTest(schedule=schedule,label=label),self.assertRaises((AssertionError,ValueError)):verify(changed)

    def test_retained_history_preserves_old_control_and_requires_exact_joined_readers(self):
        with tempfile.TemporaryDirectory() as directory:
            science=check.compacted_archive_science(retained_successor_fixture(Path(directory))[-1])
            before,after,startup=compacted_history_fixture(science)
            def verify(state,setup=startup):source.history_unchanged(before,state,startup=setup,retained_compacted_epoch2=True)
            verify(after)
            startup_ids={r['lease_id'] for r in startup['snapshot_leases']}-{r['lease_id'] for r in before['snapshot_leases']}
            for operation in ('delete-complete-probe','rewrite-complete-probe'):
                changed=copy.deepcopy(after)
                for table in ('snapshot_leases','archive_snapshot_pins','retention_claims'):
                    if operation=='delete-complete-probe':changed[table]=[r for r in changed[table] if r['lease_id'] not in startup_ids]
                    else:
                        for row in changed[table]:
                            if row['lease_id'] in startup_ids:row['xmin']='5999'
                with self.subTest(operation=operation),self.assertRaises(AssertionError):verify(changed)
            for table in ('snapshot_leases','archive_snapshot_pins','retention_claims'):
                for selected in ('all','xmin'):
                    changed=copy.deepcopy(after)
                    if selected=='all':changed[table]=[r for r in changed[table] if r['lease_id'] not in startup_ids]
                    else:next(r for r in changed[table] if r['lease_id'] in startup_ids)['xmin']='rewritten'
                    with self.subTest(startup_table=table,corruption=selected),self.assertRaises((AssertionError,ValueError)):
                        verify(changed)
            for label,mutate in {
                    'changed-outcome':lambda s:s['producer_outcomes'][0].update(xmin='rewritten'),
                    'deleted-checkpoint':lambda s:s['producer_checkpoints'].clear(),
                    'changed-account':lambda s:s['local_accounts'][0].update(grant_revision=99),
                    'old-audit':lambda s:s['management_audit'][0].update(xmin='rewritten'),
                    'old-export-claim':lambda s:s['retention_claims'][0].update(xmin='rewritten'),
                    'lost-startup-reader':lambda s:s['snapshot_leases'].pop(1),
                    'extra-reader':lambda s:s['archive_snapshot_pins'].append(dict(s['archive_snapshot_pins'][-1],lease_id='extra')),
                    'missing-claim':lambda s:s['retention_claims'].pop(),
                    'wrong-cut':lambda s:s['retention_claims'][-1].update(last_sequence=9),
                    'wrong-holder':lambda s:s['snapshot_leases'][-1].update(holder_incarnation='wrong'),
                    'unjoined-reader':lambda s:s['snapshot_leases'][-1].update(release_reason='expired'),
                    'changed-session-identity':lambda s:s['browser_sessions'][0].update(credential_revision=2),
                    'second-login':lambda s:s['login_buckets'][0].update(attempts=3),
                    'missing-revoke-audit':lambda s:s['management_audit'].pop(),
                    'changed-owner-role':lambda s:s['role_instances'][-1].update(created_at_us=1),
            }.items():
                changed=copy.deepcopy(after);mutate(changed)
                with self.subTest(label=label),self.assertRaises((AssertionError,ValueError)):verify(changed)
            with self.assertRaises(AssertionError):verify(after,None)
            with self.assertRaises(AssertionError):source.history_unchanged(before,after,startup=startup)

    def test_retry_export_and_history_reobserve_group_custody_logins_and_failed_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);runtime=object.__new__(case.ReclamationRuntime)
            value,_,files,_=fault_fixture(root,phase='grouped-second')
            original=copy.deepcopy(files)
            parent='main/dictionaries/prepared-'+'f'*32;path=parent+'/data.parquet'
            files[parent]=dict(mode=0o700,inode=9000,device=1,links=2,sha256=None)
            files[path]=dict(mode=0o600,inode=9001,device=1,links=1,sha256='e'*64)
            runtime.failed_archive_files=check.failed_archive_output(value['catalog_after'],files,original)
            runtime.state=root;runtime.caught_up_files=files;runtime.caught_up_catalog=value['catalog_after']
            _,records,metadata=custody_fixture(value['after'],value['science'],(3,4,5))
            baseline=dict(custody=dict(records=records,files=metadata,initialization='original'),logins={
                '4':dict(rolname='B4',oid='104',rolcanlogin=False,credential_present=False),
                '5':dict(rolname='C5',oid='105',rolcanlogin=False,credential_present=False)})
            runtime.retained_archive_evidence=baseline
            for boundary in ('retry','export','history'):
                runtime.files=Mock(return_value=copy.deepcopy(files));runtime.archive_evidence=Mock(return_value=copy.deepcopy(baseline))
                runtime.verify_archive_preservation(root,boundary,{},copy.deepcopy(value['catalog_after']))
                for corruption in ('input-delete','input-rewrite','B-journal','C-journal','lock-delete','lock-inode',
                        'C-lock-inode','lock-busy','B-login','C-login','failed-file','registration'):
                    current=copy.deepcopy(baseline);objects=copy.deepcopy(files);catalog=copy.deepcopy(value['catalog_after'])
                    target=next(n for n in records if n.endswith('.inputs'))
                    lock=next(n for n in metadata if n.endswith('/custody.lock'))
                    b=index.only(value['after']['archive_attempts'],ordinal=4)['attempt_id'][2:]
                    c=index.only(value['after']['archive_attempts'],ordinal=5)['attempt_id'][2:]
                    if corruption=='input-delete':current['custody']['records'].pop(target)
                    elif corruption=='input-rewrite':current['custody']['records'][target]='00'
                    elif corruption in ('B-journal','C-journal'):current['custody']['records'][(b if corruption=='B-journal' else c)+'/intent.json']='00'
                    elif corruption=='lock-delete':current['custody']['files'].pop(lock)
                    elif corruption=='lock-inode':current['custody']['files'][lock]['inode']+=1
                    elif corruption=='C-lock-inode':current['custody']['files'][c+'/custody.lock']['inode']+=1
                    elif corruption in ('B-login','C-login'):current['logins']['4' if corruption=='B-login' else '5']['rolcanlogin']=True
                    elif corruption=='failed-file':objects[path]['sha256']='0'*64
                    elif corruption=='registration':catalog['public.ducklake_data_file'][0]['path']='prepared-'+'f'*32+'/data.parquet'
                    runtime.files=Mock(return_value=objects);runtime.archive_evidence=Mock(return_value=current)
                    if corruption=='lock-busy':runtime.archive_evidence.side_effect=ValueError('original custody lock busy')
                    with self.subTest(boundary=boundary,corruption=corruption),self.assertRaises((AssertionError,ValueError)):
                        runtime.verify_archive_preservation(root,boundary,{},catalog)

    def test_schedule_selection_preserves_unique_default_and_refuses_unknown(self):
        for selected in (None,'uninterrupted','unbound-publication-loss','grouped-publication-loss','unknown',''):
            with self.subTest(selected=selected),tempfile.TemporaryDirectory() as directory:
                args=retained.arguments(Path(directory));args.retained_archive_schedule=selected
                inputs={}
                if selected in ('unknown',''):
                    with self.assertRaisesRegex(ValueError,'declared compacted Archive schedule'):
                        case.ReclamationRuntime(args,inputs)
                else:
                    runtime=case.ReclamationRuntime(args,inputs)
                    self.assertEqual(runtime.archive_schedule,selected or 'unbound-publication-loss')
                    self.assertEqual(inputs['retained_archive_schedule'],runtime.archive_schedule)

    def test_grouped_recovery_requires_both_original_logins_to_remain_sealed(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime=object.__new__(case.ReclamationRuntime);runtime.archive_schedule='grouped-publication-loss'
            runtime.caught_up={};runtime.unbound_custody=dict(records={},files={})
            runtime.archive_replacement_custody=Mock(return_value=runtime.unbound_custody)
            runtime.unbound_catalog=runtime.caught_up_catalog={table:[] for table in (
                'bigbrotr_archive.attempts','public.ducklake_snapshot','public.ducklake_snapshot_changes',
                'public.ducklake_data_file','public.ducklake_file_column_stats')}
            runtime.archive_science=runtime.caught_up_files=runtime.unbound_files={}
            runtime.unbound_login=dict(rolname='original-C5',oid='105',rolcanlogin=True,credential_present=True)
            runtime.failed_replacement_login=dict(rolname='original-B4',oid='104',rolcanlogin=False,credential_present=False)
            sealed={4:runtime.failed_replacement_login,5:dict(runtime.unbound_login,rolcanlogin=False,credential_present=False)}
            runtime.replacement_login=Mock(side_effect=lambda state,ordinal:sealed[ordinal])
            with patch.object(case.oracle,'archive_file_membership'):
                runtime.verify_unbound_reuse(Path(directory))
                for ordinal in (4,5):
                    original=sealed[ordinal];sealed[ordinal]=dict(original,rolcanlogin=True,credential_present=True)
                    with self.subTest(ordinal=ordinal),self.assertRaises((AssertionError,ValueError)):
                        runtime.verify_unbound_reuse(Path(directory))
                    sealed[ordinal]=original

    def test_grouped_faults_require_distinct_guard_and_native_inventories(self):
        for phase,native_ordinals,guard_ordinals in (
                ('grouped-first',[1,2],[1,2,4]),('grouped-second',[1,2,5],[1,2,4,5])):
            with self.subTest(phase=phase),tempfile.TemporaryDirectory() as directory:
                value,selected,*_=fault_fixture(Path(directory),phase=phase)
                self.assertEqual(check.unbound_archive_fault(**value),selected)
                self.assertEqual([r['publication_ordinal'] for r in value['census']['tables']['archive_commits']],native_ordinals)
                self.assertEqual([r['ordinal'] for r in value['catalog_after']['bigbrotr_archive.attempts']],guard_ordinals)
                changes={
                    'omitted-B':lambda v:v['after']['archive_attempts'].pop(3),
                    'missing-guard-B':lambda v:v['catalog_after']['bigbrotr_archive.attempts'].pop(2),
                    'native-B':lambda v:v['census']['tables']['archive_commits'].append(dict(
                        v['census']['tables']['archive_commits'][-1],publication_ordinal=4,
                        attempt_id=v['after']['archive_attempts'][3]['attempt_id'][2:].upper())),
                    'changed-body':lambda v:v['census']['tables']['dictionaries'][0].update(recorded_at=0),
                    'changed-B-xmin':lambda v:v['after']['archive_attempts'][3].update(xmin='rewritten'),
                    'changed-B-guard':lambda v:v['catalog_after']['bigbrotr_archive.attempts'][2].update(xmin='rewritten'),
                    'wrong-issued':lambda v:v['after']['archive_attempts'].append(dict(v['after']['archive_attempts'][-1],ordinal=69)),
                    'guard-as-native':lambda v:v['census']['tables'].update(archive_commits=v['catalog_after']['bigbrotr_archive.attempts']),
                }
                # The new B guard row's xmin is an observed scalar at the first
                # cut; its exact original identity is required at the second.
                if phase=='grouped-first':
                    changes.pop('changed-B-guard');changes.pop('changed-B-xmin')
                for name,mutate in changes.items():
                    changed=copy.deepcopy(value);mutate(changed)
                    with self.subTest(refusal=name),self.assertRaises((AssertionError,KeyError,ValueError)):
                        check.unbound_archive_fault(**changed)

    def test_failed_output_empty_or_nonempty_is_preserved_and_never_science(self):
        with tempfile.TemporaryDirectory() as directory:
            value,selected,files,original=fault_fixture(Path(directory),phase='grouped-first')
            self.assertEqual(check.failed_archive_output(value['catalog_after'],files,original),{})
            check.archive_file_membership(value['catalog_after'],selected,files,original,failed_output={})
            parent='main/dictionaries/prepared-'+'f'*32;name=parent+'/data.parquet'
            files[parent]=dict(mode=0o700,inode=10001,device=1,links=2,sha256=None)
            files[name]=dict(mode=0o600,inode=10002,device=1,links=1,sha256='e'*64)
            baseline=check.failed_archive_output(value['catalog_after'],files,original)
            self.assertEqual(set(baseline),{parent,name})
            check.archive_file_membership(value['catalog_after'],selected,files,original,failed_output=baseline)
            with self.assertRaises(AssertionError):check.archive_file_membership(value['catalog_after'],selected,files,original)
            for label in ('delete','rewrite','inode','registration','unanchored'):
                catalog,current=copy.deepcopy((value['catalog_after'],files))
                if label=='delete':current.pop(name)
                elif label=='rewrite':current[name]['sha256']='0'*64
                elif label=='inode':current[name]['inode']+=1
                elif label=='registration':catalog['public.ducklake_data_file'][0]['path']='prepared-'+'f'*32+'/data.parquet'
                else:current[parent+'/other']=dict(current[name])
                with self.subTest(label=label),self.assertRaises((AssertionError,KeyError)):
                    check.archive_file_membership(catalog,selected,current,original,failed_output=baseline)

    def test_grouped_final_and_shared_startup_require_the_literal69_schedule(self):
        with tempfile.TemporaryDirectory() as directory:
            science=check.compacted_archive_science(retained_successor_fixture(Path(directory))[-1])
            prefix,parked,before,after,report,old_catalog,catalog,census=compacted_archive_fixture(science,schedule='grouped-publication-loss')
            def verify(a=after,r=report,c=catalog):
                check.compacted_archive(prefix,parked,before,a,r,science,old_catalog,c,schedule='grouped-publication-loss')
                index.startup_selection(a,retained_compacted_epoch2=True,retained_archive_schedule='grouped-publication-loss')
            verify()
            source.publication(after,census,science,revision=4,snapshot=68,topology=1,cuts=[(1,312),(2,10)],
                published_ordinals=(1,2,*range(5,70)))
            self.assertEqual((len(after['archive_attempts']),len(catalog['bigbrotr_archive.attempts']),len(census['tables']['archive_commits'])),(69,68,67))
            for label in ('omit-B','old68','C-identity','B-xmin','C-xmin','B-guard-xmin','wrong-report','B-selected'):
                a,r,c=copy.deepcopy((after,report,catalog))
                if label=='omit-B':a['archive_attempts'].pop(3)
                elif label=='old68':a['archive_attempts'].pop()
                elif label=='C-identity':a['archive_attempts'][4]['attempt_id']='\\x'+'ef'*16
                elif label=='B-xmin':a['archive_attempts'][3]['xmin']='rewritten'
                elif label=='C-xmin':a['archive_attempts'][4]['xmin']='rewritten'
                elif label=='B-guard-xmin':c['bigbrotr_archive.attempts'][2]['xmin']='rewritten'
                elif label=='wrong-report':r.update(issued_attempts_before=4,new_attempts=65)
                else:index.only(a['archive_frontier_batches'],frontier_revision=4,attempt_id=a['archive_attempts'][4]['attempt_id'])['attempt_id']=a['archive_attempts'][3]['attempt_id']
                with self.subTest(label=label),self.assertRaises((AssertionError,ValueError)):
                    verify(a,r,c)
            for label in ('omit-B','old-default','native4'):
                a=copy.deepcopy(after);options=dict(retained_compacted_epoch2=True,retained_archive_schedule='grouped-publication-loss')
                if label=='omit-B':a['archive_attempts'].pop(3)
                elif label=='old-default':options.pop('retained_archive_schedule')
                else:index.only(a['archive_frontier_batches'],frontier_revision=4,attempt_id=a['archive_attempts'][4]['attempt_id'])['attempt_id']=a['archive_attempts'][3]['attempt_id']
                with self.subTest(startup=label),self.assertRaises(AssertionError):index.startup_selection(a,**options)

    def test_grouped_custody_requires_all_original_headers_and_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            value,*_=fault_fixture(Path(directory),phase='grouped-second')
            state,science=value['after'],value['science'];initialization,records,files=custody_fixture(state,science,(3,4,5))
            check.replacement_custody(state,science,records,files,initialization.hex(),ordinals=(3,4,5))
            for ordinal in (3,4,5):
                ident=index.raw(index.only(state['archive_attempts'],ordinal=ordinal)['attempt_id'],16).hex()
                for suffix in ('.inputs','/intent.json','/custody.lock'):
                    changed=copy.deepcopy(records);changed.pop(ident+suffix)
                    with self.subTest(ordinal=ordinal,missing=suffix),self.assertRaises((AssertionError,KeyError)):
                        check.replacement_custody(state,science,changed,files,initialization.hex(),ordinals=(3,4,5))
                for offset in (40,120,-40):
                    raw=bytearray.fromhex(records[ident+'.inputs']);raw[offset]^=1;raw[-32:]=hashlib.sha256(raw[:-32]).digest()
                    changed=dict(records,**{ident+'.inputs':raw.hex()});metadata=copy.deepcopy(files)
                    metadata[ident+'.inputs']['sha256']=hashlib.sha256(raw).hexdigest()
                    with self.subTest(ordinal=ordinal,changed=offset),self.assertRaises(AssertionError):
                        check.replacement_custody(state,science,changed,metadata,initialization.hex(),ordinals=(3,4,5))

    def test_before_forward_wire_is_explicit_and_cannot_replace_after_complete(self):
        local=(sys.executable,str(ROOT/'experiments/archive-engine/native_commit_proxy.py'))
        before=wire_fixture(mode='before-forward')
        wire_checked(before,'publication','a'*64,None,local_proxy=local,mode='before-forward')
        with self.assertRaises(AssertionError):wire_checked(before,'publication','a'*64,None,local_proxy=local)
        with self.assertRaises(AssertionError):wire_checked(before,'publication','a'*64,None,mode='before-forward')
        for key,value in (('server_complete',1),('client_complete',1),('faults',0),('unexpected',1)):
            changed=copy.deepcopy(before);events=[json.loads(line) for line in changed['process']['stdout'].splitlines()]
            events[-1][key]=value;changed['process']['stdout']='\n'.join(map(json.dumps,events))+'\n'
            with self.subTest(key=key),self.assertRaises(AssertionError):
                wire_checked(changed,'publication','a'*64,None,local_proxy=local,mode='before-forward')

    def test_selected_schedule_dispatches_only_declared_cuts(self):
        for schedule,phases in (('uninterrupted',[]),('unbound-publication-loss',['unique']),
                ('grouped-publication-loss',['grouped-first','grouped-second'])):
            with self.subTest(schedule=schedule),tempfile.TemporaryDirectory() as directory:
                runtime=object.__new__(case.ReclamationRuntime)
                runtime.reclamation_output=runtime.state=Path(directory);runtime.archive_schedule=schedule
                runtime.schema=retained.schema;runtime.prefix_catalog={};runtime.complete_index_state={};runtime.final_science={}
                runtime.compacted_bridge={}
                runtime.custody_before={};runtime.all_input_files={};runtime.parked_files={}
                runtime.capture=Mock(return_value=({},{}));runtime.ch=Mock(return_value=[])
                runtime.custody=Mock(return_value={});runtime.files=Mock(return_value={})
                runtime.finite=Mock(side_effect=[dict(published_revision_after=68,new_batches=0)]*2+[ValueError('fresh recovery reached')])
                observed=[]
                def fault(output,before,**options):
                    phase=options.get('phase','unique');self.assertEqual(before,{'phase':observed[-1]} if observed else {})
                    observed.append(phase);return dict(phase=phase)
                runtime.unbound_archive_publication_loss=Mock(side_effect=fault)
                with patch.object(case.live_oracle,'no_work_rebind'),patch.object(case.live_oracle,'append_only_history'), \
                        patch.object(case.oracle,'compacted_archive_science',return_value={}):
                    with self.assertRaisesRegex(ValueError,'fresh recovery reached'):runtime.compacted_consumers()
                self.assertEqual(observed,phases)

    def test_fault_science_exact_native_ordinals_and_unchanged_frontier(self):
        with tempfile.TemporaryDirectory() as directory:
            value,selected,files,old_files=fault_fixture(Path(directory))
            self.assertEqual(check.unbound_archive_fault(**value),selected)
            check.archive_file_membership(value['catalog_after'],selected,files,old_files)
            changes={
                'native3':lambda v:v['census']['tables']['archive_commits'].append(dict(v['census']['tables']['archive_commits'][-1],publication_ordinal=3)),
                'duplicate4':lambda v:v['census']['tables']['archive_commits'].append(copy.deepcopy(v['census']['tables']['archive_commits'][-1])),
                'changed-body':lambda v:v['census']['tables']['dictionaries'][-1].update(recorded_at=0),
                'premature-frontier3':lambda v:v['after']['consumers'][0].update(current_revision=3),
                'missing-current-cut':lambda v:v['after']['consumer_frontiers'].pop(),
                'changed-A3':lambda v:v['after']['archive_attempts'][2].update(xmin='rewritten'),
                'same-identity':lambda v:v['after']['archive_attempts'][3].update(attempt_id=v['after']['archive_attempts'][2]['attempt_id']),
                'same-worker':lambda v:v['after']['archive_attempts'][3].update(worker_incarnation=v['after']['archive_attempts'][2]['worker_incarnation']),
                'wrong-attribution':lambda v:v['after']['archive_attempts'][3].update(issuer_epoch=1),
                'wrong-topology':lambda v:v['after']['archive_attempts'][3].update(topology_generation=1),
                'later-publish':lambda v:v['after']['archive_attempts'].append(dict(v['after']['archive_attempts'][3],ordinal=5)),
            }
            for name,mutate in changes.items():
                changed=copy.deepcopy(value);mutate(changed)
                with self.subTest(name=name),self.assertRaises((AssertionError,ValueError)):
                    check.unbound_archive_fault(**changed)
            for name,mutate in {
                    'missing-file':lambda c,f:f.pop(next(n for n in f if n.endswith('.parquet'))),
                    'orphan-file':lambda c,f:f.update(orphan=dict(f['.initial'])),
                    'wrong-count':lambda c,f:c['public.ducklake_data_file'][-1].update(record_count=99),
                    'wrong-old-bytes':lambda c,f:f['.initial'].update(sha256='0'*64),
                    'missing-batch':lambda c,f:c['public.ducklake_data_file'].pop(),
            }.items():
                c,f=copy.deepcopy((value['catalog_after'],files));mutate(c,f)
                with self.subTest(files=name),self.assertRaises((AssertionError,KeyError,ValueError)):
                    check.archive_file_membership(c,selected,f,old_files)

    def test_recovery_schedule_is_declared_and_preserves_B4_identity_xmin(self):
        with tempfile.TemporaryDirectory() as directory:
            science=check.compacted_archive_science(retained_successor_fixture(Path(directory))[-1])
            prefix,parked,before,after,report,old_catalog,catalog,census=compacted_archive_fixture(science)
            before['archive_attempts'].append(copy.deepcopy(after['archive_attempts'][3]))
            old_catalog['bigbrotr_archive.attempts'].append(copy.deepcopy(catalog['bigbrotr_archive.attempts'][2]))
            report.update(issued_attempts_before=4,new_attempts=64)
            check.compacted_archive(prefix,parked,before,after,report,science,old_catalog,catalog,schedule='unbound-publication-loss')
            for change in ('B-identity','B-xmin','B-import-xmin','another-replacement','wrong-report','missing-zero-stream'):
                a,r,c=copy.deepcopy((after,report,catalog))
                if change=='B-identity':a['archive_attempts'][3]['attempt_id']='\\x'+'ef'*16
                elif change=='B-xmin':a['archive_attempts'][3]['xmin']='rewritten'
                elif change=='B-import-xmin':c['bigbrotr_archive.attempts'][2]['xmin']='rewritten'
                elif change=='another-replacement':a['archive_attempts'].append(dict(a['archive_attempts'][3],ordinal=69))
                elif change=='wrong-report':r.update(issued_attempts_before=3,new_attempts=65)
                else:a['consumer_frontiers']=[row for row in a['consumer_frontiers'] if not (row['frontier_revision']==3 and row['owner_epoch']==2)]
                with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                    check.compacted_archive(prefix,parked,before,a,r,science,old_catalog,c,schedule='unbound-publication-loss')
            with self.assertRaises(AssertionError):
                check.compacted_archive(prefix,parked,before,after,report,science,old_catalog,catalog)

    def test_exact_A3_B4_headers_and_custody_are_mandatory(self):
        with tempfile.TemporaryDirectory() as directory:
            value,*_=fault_fixture(Path(directory));state=value['after'];science=value['science']
            initialization,records,files=custody_fixture(state,science)
            check.replacement_custody(state,science,records,files,initialization.hex())
            ident=index.raw(index.only(state['archive_attempts'],ordinal=3)['attempt_id'],16).hex()
            for target in (ident+'/intent.json',ident+'/custody.lock',ident+'.inputs'):
                changed=copy.deepcopy(records);changed.pop(target)
                with self.subTest(missing=target),self.assertRaises((AssertionError,KeyError)):
                    check.replacement_custody(state,science,changed,files,initialization.hex())
            ident=index.raw(index.only(state['archive_attempts'],ordinal=4)['attempt_id'],16).hex()
            raw=bytearray.fromhex(records[ident+'.inputs']);raw[-40]^=1
            raw[-32:]=hashlib.sha256(raw[:-32]).digest()
            changed=dict(records,**{ident+'.inputs':raw.hex()});metadata=copy.deepcopy(files)
            metadata[ident+'.inputs']['sha256']=hashlib.sha256(raw).hexdigest()
            with self.assertRaises(AssertionError):check.replacement_custody(state,science,changed,metadata,initialization.hex())

    def test_local_wire_pins_route_and_custody_and_default_docker_mode_stays_strict(self):
        local_proxy=(sys.executable,str(ROOT/'experiments/archive-engine/native_commit_proxy.py'))
        direct='[postgres.control]\nendpoint="127.0.0.1:15432"\n[postgres.catalog]\nendpoint="127.0.0.1:15432"\ndatabase="bigbrotr_catalog"\nuser="bigbrotr_catalog"\nread_user="bigbrotr_archive_read"\n'
        pins={'experiments/archive-engine/native_commit_proxy.py':'a'*64,'other.py':'b'*64};candidate=dict(manifest='c'*64,binary='d'*64)
        identity=dict(pid=42,ppid=1,start_ticks=11)
        value=dict(wire=wire_fixture(),direct_config=direct,routed_config=direct.replace('endpoint="127.0.0.1:15432"\ndatabase',
            'endpoint="127.0.0.1:23456"\ndatabase'),restored_config=direct,stdout='',stderr=ERRORS['publication-loss'],wait_exit='4',
            terminal=dict(Running=False,Pid=0,ExitCode=4,OOMKilled=False),parent=dict(identity,pid=43),parent_absent=True,
            proxy_absent=True,proxy_identity=identity,proxy_terminal_identity=identity,
            backends={'bigbrotr_control':'0','bigbrotr_catalog':'0'},source_pins_before=pins,source_pins_after=pins,
            source_root=str(ROOT),candidate_before=candidate,candidate_after=candidate)
        def verify(v):check.unbound_fault_process(v,'a'*64,local_proxy,source_pins=pins,candidate=candidate)
        verify(value)
        changes={
            'wrong-proxy-source':lambda v:v['wire']['ready'].update(source_sha256='0'*64),
            'wrong-command':lambda v:v['wire']['process']['command'].__setitem__(0,'/other/python3'),
            'wrong-config-route':lambda v:v.update(routed_config=direct),
            'wrong-control-route':lambda v:v.update(routed_config=v['routed_config'].replace('15432','23456')),
            'unknown-parent':lambda v:v.update(parent_absent=False),
            'unknown-proxy':lambda v:v.update(proxy_absent=False),
            'proxy-reader-alive':lambda v:v['wire']['process'].update(readers_joined=False),
            'proxy-pid-reused':lambda v:v.update(proxy_terminal_identity=dict(v['proxy_identity'],start_ticks=12)),
            'driver-alive':lambda v:v['backends'].update(bigbrotr_catalog='1'),
            'positive-report':lambda v:v.update(stdout='{}\n'),
            'stale-source':lambda v:v.update(source_pins_before={},source_pins_after={}),
            'stale-candidate':lambda v:v.update(candidate_before={},candidate_after={}),
        }
        for name,mutate in changes.items():
            changed=copy.deepcopy(value);mutate(changed)
            with self.subTest(name=name),self.assertRaises((AssertionError,KeyError,ValueError)):
                verify(changed)
        for key,replacement in (('upstream_port',5432),('catalog_reader','wrong_reader'),('server_complete',0),('client_complete',1)):
            changed=copy.deepcopy(value);events=[json.loads(line) for line in changed['wire']['process']['stdout'].splitlines()]
            for event in events:event[key]=replacement
            changed['wire']['ready']=events[0];changed['wire']['process']['stdout']='\n'.join(map(json.dumps,events))+'\n'
            with self.subTest(event=key),self.assertRaises(AssertionError):verify(changed)
        default=wire_fixture(local=False);client='bigbrotr-acquisition-binding-1234567'
        wire_checked(default,'publication','a'*64,client)
        for mutation in (lambda v:v['process']['command'].__setitem__(3,'wrong-client'),
                lambda v:v['process'].update(readers_joined=False),lambda v:v['ready'].update(source_sha256='0'*64)):
            changed=copy.deepcopy(default);mutation(changed)
            with self.assertRaises(AssertionError):wire_checked(changed,'publication','a'*64,client)
        with self.assertRaises(AssertionError):wire_checked(value['wire'],'publication','a'*64,client)

    def test_extra_proxy_cleanup_is_independent_of_the_original_two_proxy_bound(self):
        runtime=object.__new__(case.ReclamationRuntime);runtime.resources=dict(cleanup_errors=[])
        runtime.reclamation_proxy_handles=[Mock(),Mock()];runtime.unbound_proxy_handle=Mock();runtime.grouped_proxy_handle=Mock()
        with patch.object(case.RetainedRuntime,'cleanup') as cleanup:runtime.cleanup()
        self.assertEqual(len(runtime.reclamation_proxy_handles),2)
        for proxy in [*runtime.reclamation_proxy_handles,runtime.unbound_proxy_handle,runtime.grouped_proxy_handle]:proxy.stop.assert_called_once()
        cleanup.assert_called_once()


if __name__=='__main__':unittest.main()
