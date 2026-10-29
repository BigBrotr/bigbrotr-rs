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


class UnboundReplacementControls(unittest.TestCase):
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
                    with self.subTest(ordinal=ordinal),self.assertRaises(AssertionError):
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
