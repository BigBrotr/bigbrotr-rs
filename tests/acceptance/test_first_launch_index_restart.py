"""Backlog/rebind mutation and custody controls; no product or engine execution."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from cases import first_launch as base
from cases import first_launch_index as initial
from cases import first_launch_index_oracle as index
from cases import first_launch_index_restart as case
from cases import first_launch_index_restart_oracle as check
from test_first_launch import arguments
from test_first_launch_index import metadata_fixture, schema, ROOT, set_value


def restart_fixture(root):
    state,rows,science=metadata_fixture(root)
    for table in base.expected_tables(ROOT): state.setdefault(table,[])
    own=index.only(state['owner_epochs'],role='index')
    control=index.only(state['owner_epochs'],role='control')
    control.update(state='released',released_at_us=11000,role_instance_id='\\x'+'ab'*16,
        dataset_id=own['dataset_id'],index_publication_protocol=None,xmin='110')
    own.update(state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,xmin='110')
    state['role_instances'][0].update(xmin='100')
    state['role_instances'].append(dict(role_instance_id=control['role_instance_id'],role='control',
        dataset_id=own['dataset_id'],retired_at_us=11000,xmin='110'))
    state['index_owner_sessions'][0]['xmin']='100'
    state['consumers'].append(dict(consumer_id='\\x'+'ac'*16,consumer_role='archive',current_revision=4))
    state['consumer_frontiers'].append(dict(consumer_id='\\x'+'ac'*16,frontier_revision=4,last_complete_sequence=12))
    state['producer_requests']=[dict(request_sequence=n) for n in range(1,5)]
    state['producer_outcomes']=[dict(request_sequence=n) for n in range(1,5)]
    before=copy.deepcopy(state)
    fourth=index.only(before['projection_plans'],frontier_revision=4)
    fourth_chunks={r['chunk_id'] for r in before['projection_plan_chunks'] if r['plan_digest']==fourth['plan_digest']}
    before['projection_plans'].remove(fourth)
    before['projection_plan_chunks']=[r for r in before['projection_plan_chunks'] if r['plan_digest']!=fourth['plan_digest']]
    for table in ('projection_chunks','projection_chunk_bounds'):
        before[table]=[r for r in before[table] if r['chunk_id'] not in fourth_chunks]
    for table in ('index_frontier_plans','consumer_frontier_revisions','consumer_frontiers'):
        before[table]=[r for r in before[table] if r['consumer_id']=='\\x'+'ac'*16 or r['frontier_revision']<=2]
    for row in before['projection_chunks']:
        if row['chunk_ordinal']==3: row.update(state='prepared',committed_at_us=None,engine_receipt_digest=None,xmin='103')
    index.only(before['consumers'],consumer_role='index').update(current_revision=2,xmin='102')
    identity,_,custody=index.retained(root)
    binding=tuple(bytes.fromhex(identity[n]) for n in ('dataset','resource','generation','storage_incarnation','table_identity_digest'))+(custody,1)
    def add_epoch(value,epoch):
        previous=index.only(value['owner_epochs'],role='index',epoch=epoch-1)
        session=copy.deepcopy(value['index_owner_sessions'][0])
        current=copy.deepcopy(previous)
        incarnation='\\x'+f'{epoch+120:02x}'*16
        role='\\x'+f'{epoch+130:02x}'*16
        transaction=str(200+(epoch-2)*100)
        current.update(epoch=epoch,process_incarnation=incarnation,role_instance_id=role,state='active',
            released_at_us=None,exclusion_method=None,exclusion_digest=None,xmin=transaction)
        prior_control=max((r for r in value['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        fresh_control=dict(prior_control,epoch=prior_control['epoch']+1,process_incarnation=incarnation,
            role_instance_id='\\x'+f'{epoch+140:02x}'*16,released_at_us=20000+epoch,xmin=transaction)
        session.update(owner_epoch=epoch,owner_incarnation=incarnation,issuer_epoch=fresh_control['epoch'],
            issuer_incarnation=incarnation,previous_epoch=epoch-1,recovered_revision=2 if epoch==2 else 4,
            recovered_plan=value['projection_plans'][1 if epoch==2 else 3]['plan_digest'],
            recovered_pending_plan=value['projection_plans'][2]['plan_digest'] if epoch==2 else None,xmin=transaction)
        plans=[dict(p,members=[r for r in value['projection_plan_chunks'] if r['plan_digest']==p['plan_digest']]) for p in value['projection_plans']]
        session['recovery_digest']='\\x'+index.recovery_digest(binding,previous,plans,2 if epoch==2 else 4,3 if epoch==2 else None).hex()
        previous.update(state='released',released_at_us=19000+epoch,exclusion_method='storage_fenced',
                        exclusion_digest=session['recovery_digest'],xmin=transaction)
        index.only(value['role_instances'],role_instance_id=previous['role_instance_id']).update(retired_at_us=19000+epoch,xmin=transaction)
        value['owner_epochs'] += [fresh_control,current]
        value['index_owner_sessions'].append(session)
        value['role_instances'] += [dict(role_instance_id=r['role_instance_id'],dataset_id=r['dataset_id'],role=r['role'],
            retired_at_us=None if r['role']=='index' else 20000+epoch,xmin=transaction) for r in (fresh_control,current)]
        return current
    current=add_epoch(state,2)
    for plan in state['projection_plans']:
        if plan['frontier_revision']==4: plan.update(origin_epoch=2,origin_incarnation=current['process_incarnation'])
    for row in state['projection_chunks']:
        if row['chunk_ordinal']>=3:
            row.update(xmin=str(200+row['chunk_ordinal']),committed_at_us=20000+row['chunk_ordinal'])
        if row['chunk_ordinal']==4: row.update(owner_epoch=2,owner_incarnation=current['process_incarnation'])
    for table in ('index_frontier_plans','consumer_frontier_revisions','consumer_frontiers'):
        for row in state[table]:
            if row['consumer_id']!='\\x'+'ac'*16 and row['frontier_revision']>=3:
                n=row['frontier_revision'];row['xmin']=str(200+n)
                if table!='consumer_frontiers': row.update(owner_epoch=2,owner_incarnation=current['process_incarnation'])
                if table=='index_frontier_plans': row['committed_at_us']=20000+n
                if table=='consumer_frontier_revisions': row['created_at_us']=20000+n
    index.only(state['consumers'],consumer_role='index')['xmin']='204'
    retried=copy.deepcopy(state);add_epoch(retried,3)
    return before,state,retried,rows,science


class RestartIndexControls(unittest.TestCase):
    def test_prefix_dispatch_policy_is_declared_and_accepted_before_owner_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime=case.RestartIndexRuntime(arguments(Path(temporary)),{})
            runtime.config=Path(temporary)/'instance.toml'
            runtime.home=Path(temporary)/'client';runtime.home.mkdir()
            runtime.set_value=set_value
            original=initial.recipe((ROOT/'config/recipes/acceptance.toml').read_text(),set_value)
            runtime.config.write_text(original)
            with patch.object(initial.IndexRuntime,'prepare_state'):
                runtime.prepare_state()
            configured=tomllib.loads(runtime.config.read_text())
            expected=tomllib.loads(original);expected.setdefault('archive',{})['max_publish_delay']='1s'
            self.assertEqual(configured,expected)
            self.assertEqual(runtime.inputs['initial_recipe_sha256'],base.digest(runtime.config))
            document=dict(format='bigbrotr-product-policy/2',settings={
                'indexer.enabled':True,'sync.provenance':'local_first_witness',
                'indexer.batch_max_rows':4,'archive.max_publish_delay':1_000_000})
            state=dict(datasets=[dict(config_revision=1,policy_revision=1)],
                config_revisions=[dict(revision=1,document=copy.deepcopy(document),document_digest='synthetic')],
                policy_revisions=[dict(revision=1,config_revision=1,document=copy.deepcopy(document),document_digest='synthetic')])
            output=runtime.root/'initial';output.mkdir()
            base.save(output/'before-control.json',state)
            with patch.object(initial.IndexRuntime,'initial_launch',return_value='observed') as launch:
                self.assertEqual(runtime.initial_launch(),'observed')
                launch.assert_called_once_with()
            for value in (30_000_000,True,'1000000',None):
                changed=copy.deepcopy(state)
                for table in ('config_revisions','policy_revisions'):
                    changed[table][0]['document']['settings']['archive.max_publish_delay']=value
                base.save(output/'before-control.json',changed)
                with self.subTest(value=value),patch.object(initial.IndexRuntime,'initial_launch') as launch:
                    with self.assertRaises(AssertionError):runtime.initial_launch()
                    launch.assert_not_called()
            changed=copy.deepcopy(state)
            changed['policy_revisions'][0]['document']['settings']['archive.max_publish_delay']=30_000_000
            base.save(output/'before-control.json',changed)
            with patch.object(initial.IndexRuntime,'initial_launch') as launch:
                with self.assertRaises(AssertionError):runtime.initial_launch()
                launch.assert_not_called()
            base.save(output/'before-control.json',state)
            for text in (original,set_value(original,'archive','max_publish_delay','30s')):
                runtime.config.write_text(text)
                with patch.object(initial.IndexRuntime,'initial_launch') as launch:
                    with self.assertRaises((AssertionError,KeyError)):runtime.initial_launch()
                    launch.assert_not_called()

    def test_source_closure_changes_only_the_original_source_and_finite_control(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);before,_,_,_,science=restart_fixture(root)
            science['manifest']=next(value for cf,key,value in science['native'] if cf=='default' and key==b'\x01\x01')
            before['producer_epochs']=[dict(producer_slot_id='\\x'+'33'*16,producer_epoch=1,
                state='open',reserved_through_request=4,retired_at_us=None,closure_digest=None,xmin='100')]
            before['source_streams']=[dict(dataset_id='\\x'+'22'*16,owner_resource_id='\\x'+'aa'*16,
                owner_epoch=1,shard_id=0,state='open',last_complete_sequence=None,closure_digest=None,closed_at_us=None,xmin='100')]
            after=copy.deepcopy(before)
            old=index.only(before['owner_epochs'],role='control')
            control=dict(old,epoch=old['epoch']+1,process_incarnation='\\x'+'ba'*16,
                role_instance_id='\\x'+'bc'*16,released_at_us=20000,xmin='201')
            after['owner_epochs'].append(control)
            after['role_instances'].append(dict(role_instance_id=control['role_instance_id'],role='control',dataset_id=old['dataset_id']))
            digest=check.closure(science['manifest'],'\\x'+'33'*16,1,science['inputs'],science['receipts'],12)
            after['producer_epochs'][0].update(state='closing',xmin='199')
            after['source_streams'][0].update(state='closed',last_complete_sequence=12,closure_digest='\\x'+digest,closed_at_us=19000,xmin='200')
            report=dict(format='bigbrotr-ingest-recovery/1',state='source-closed',dataset_id='22'*16,
                owner_resource_id='aa'*16,producer_slot_id='33'*16,producer_epoch='1',issued_through='4',reserved_through='4',
                resolved_requests='4',native='closed',owner_release='committed',ready=False,
                source_closure=dict(last_complete_sequence='12',sha256=digest))
            check.source_closed(before,after,report,science)
            for table,field,value in [('source_streams','closure_digest','\\x'+'00'*32),
                    ('source_streams','last_complete_sequence',13),('source_streams','owner_epoch',2),
                    ('producer_epochs','state','open'),('producer_epochs','state','retired'),
                    ('producer_epochs','producer_slot_id','\\x'+'44'*16),('producer_epochs','producer_epoch',2),
                    ('producer_epochs','reserved_through_request',5),('producer_epochs','retired_at_us',19000),
                    ('producer_epochs','closure_digest','\\x'+'00'*32),('producer_epochs','xmin','100'),
                    ('producer_epochs','xmin','200'),('producer_outcomes','request_sequence',99)]:
                changed=copy.deepcopy(after);changed[table][0][field]=value
                with self.subTest(table=table,field=field),self.assertRaises(AssertionError):
                    check.source_closed(before,changed,report,science)
            for state in ('closing','retired'):
                changed=copy.deepcopy(before);changed['producer_epochs'][0]['state']=state
                with self.subTest(prior_state=state),self.assertRaises(AssertionError):
                    check.source_closed(changed,after,report,science)
            changed=copy.deepcopy(after);changed['producer_epochs'].append(copy.deepcopy(changed['producer_epochs'][0]))
            with self.assertRaises((AssertionError,ValueError)):check.source_closed(before,changed,report,science)
            with self.assertRaises(AssertionError):check.source_closed(before,after,dict(report,ready=True),science)

    def test_custody_retains_the_actual_lock_and_refuses_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime=case.RestartIndexRuntime(arguments(Path(temporary).resolve()),{})
            root=runtime.state/'data/index';root.mkdir(parents=True,mode=0o700)
            metadata_fixture(root)
            directory=root/'.bigbrotr-index-custody';directory.chmod(0o700)
            lock=directory/'custody.lock';lock.write_bytes(b'');lock.chmod(0o600)
            identity=base.load(directory/'identity.json')
            for path,field in ((root,'root_identity'),(directory,'journal_identity'),(lock,'lock_identity')):
                metadata=path.stat();identity[field]=dict(device=metadata.st_dev,inode=metadata.st_ino)
            base.save(directory/'identity.json',identity)
            observed=runtime.custody()
            self.assertEqual(observed['lock']['bytes'],0)
            lock.rename(directory/'retained-original.lock')
            lock.write_bytes(b'');lock.chmod(0o600)
            # Hide the retained original outside the closed custody inventory;
            # the new same-name inode still cannot satisfy the original binding.
            (directory/'retained-original.lock').rename(root/'retained-original.lock')
            with self.assertRaises(AssertionError):runtime.custody()

    def test_complete_history_keeps_origins_publishers_and_session_recovery_commitments(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);before,after,retry,rows,science=restart_fixture(root)
            check.backlog(before,root)
            check.recovered(before,after,root)
            check.recovered(after,retry,root,retry=True)
            self.assertEqual(index.publication(retry,retry,rows,science,schema,root,owner_count=3)['committed_chunks'],40)
            self.assertEqual([r['origin_epoch'] for r in retry['projection_plans']],[1,1,1,2])
            self.assertEqual([r['owner_epoch'] for r in retry['index_frontier_plans']],[1,1,2,2])
            with self.assertRaises((AssertionError,ValueError)):
                index.publication(retry,retry,rows,science,schema,root)

    def test_new_history_cannot_be_normalized_or_reassigned(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);before,after,retry,rows,science=restart_fixture(root)
            mutations=(
                lambda s:s['projection_plans'][2].update(origin_epoch=2),
                lambda s:s['projection_plans'][3].update(origin_epoch=1),
                lambda s:s['projection_chunks'][-1].update(owner_epoch=1),
                lambda s:s['index_frontier_plans'][2].update(owner_epoch=1),
                lambda s:s['consumer_frontier_revisions'][2].update(owner_epoch=1),
                lambda s:s['consumer_frontiers'][2].update(owner_epoch=2),
                lambda s:s['index_owner_sessions'][1].update(recovered_revision=3),
                lambda s:s['index_owner_sessions'][1].update(recovered_pending_plan=None),
                lambda s:s['index_owner_sessions'][2].update(recovered_pending_plan=s['projection_plans'][2]['plan_digest']),
                lambda s:s['index_owner_sessions'][1].update(recovery_digest='\\x'+'00'*32),
                lambda s:s['index_owner_sessions'][1].update(issuer_incarnation='\\x'+'00'*16),
                lambda s:s['projection_generations'][0].update(owner_epoch=2),
                lambda s:index.only(s['owner_epochs'],role='index',epoch=1).update(exclusion_method='lease_expired'),
                lambda s:index.only(s['owner_epochs'],role='index',epoch=2).update(exclusion_digest='\\x'+'00'*32),
                lambda s:s['index_owner_sessions'].pop(),
            )
            for change in mutations:
                changed=copy.deepcopy(retry);change(changed)
                with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                    index.publication(changed,changed,rows,science,schema,root,owner_count=3)

    def test_backlog_requires_real_prepared_third_and_absent_fourth(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);before,after,_,_,_=restart_fixture(root)
            for change in (
                lambda s:s['projection_plans'].pop(),
                lambda s:s['projection_plans'][2].update(source_first=5),
                lambda s:s['projection_chunks'][-1].update(state='committed'),
                lambda s:s['index_frontier_plans'].append(after['index_frontier_plans'][2]),
                lambda s:index.only(s['consumers'],consumer_role='index').update(current_revision=3),
                lambda s:s['producer_outcomes'].pop(),
            ):
                changed=copy.deepcopy(before);change(changed)
                with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                    check.backlog(changed,root)
            with self.assertRaises(AssertionError): check.backlog(after,root)

    def test_every_control_table_has_preservation_or_explicit_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);before,after,retry,_,_=restart_fixture(root)
            for table in base.expected_tables(ROOT)-{'owner_epochs','role_instances','index_owner_sessions'}:
                changed=copy.deepcopy(retry)
                changed[table].append(dict(unrelated='mutation'))
                with self.subTest(table=table),self.assertRaises((AssertionError,ValueError,KeyError)):
                    check.recovered(after,changed,root,retry=True)
            for table in ('projection_plans','projection_plan_chunks','projection_chunks','projection_chunk_bounds','index_frontier_plans'):
                changed=copy.deepcopy(after);changed[table][0]['xmin']='999'
                with self.subTest(prefix_table=table),self.assertRaises((AssertionError,ValueError)):
                    check.recovered(before,changed,root)
            changed=copy.deepcopy(after);changed['index_owner_sessions'][1]['xmin']='203'
            with self.assertRaises(AssertionError):check.recovered(before,changed,root)

    def test_cli_report_refuses_ready_ack_missing_pending_or_false_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);_,state,_,_,_=restart_fixture(root)
            gen=state['projection_generations'][0]
            report=dict(format='bigbrotr-initial-index-catch-up/1',state='complete',dataset_id=gen['dataset_id'][2:],
                generation_id=gen['generation_id'][2:],storage_incarnation=gen['storage_incarnation'][2:],
                table_identity_digest=gen['table_identity_digest'][2:],previous_owner_epoch=1,owner_epoch=2,
                published_revision_before=2,published_revision_after=4,source_through=12,published_through=12,
                recovered_pending=True,new_batches=1,ready=False,acknowledgement='not-performed',
                ingest_retirement='not-performed',topology_transition='not-performed')
            check.catch_up_report(report,state)
            for field,value in [('ready',True),('acknowledgement','performed'),('published_revision_before',4),
                    ('recovered_pending',False),('new_batches',0),('owner_epoch',3),('new_batches',True)]:
                changed=dict(report,**{field:value})
                with self.subTest(field=field),self.assertRaises(AssertionError):check.catch_up_report(changed,state)
            with self.assertRaises(AssertionError):check.catch_up_report(report,state,retry=True)

    def test_absolute_client_gate_is_never_restarted(self):
        gate=dict(format='bigbrotr-installed-index-prefix-gate/1',started_ns=1_000_000_000,deadline_ns=16_000_000_000)
        self.assertEqual(case.gate_deadline(gate,15.999),16)
        for now in (.999,16,17):
            with self.subTest(now=now),self.assertRaises(ValueError):case.gate_deadline(gate,now)
        for change in ({'deadline_ns':17_000_000_000},{'started_ns':True},{'extra':1}):
            with self.assertRaises(ValueError):case.gate_deadline(dict(gate,**change),2)
        with tempfile.TemporaryDirectory() as temporary:
            runtime=case.RestartIndexRuntime(arguments(Path(temporary)),{})
            runtime.prefix_deadline=16
            with patch.object(case.time,'monotonic',side_effect=[15,16]),patch.object(initial.IndexRuntime,'command') as command:
                with self.assertRaises(ValueError):runtime.command(['observed'],timeout=30)
                command.assert_called_once_with(['observed'],timeout=1)

    def test_pause_checks_custody_before_mutation_and_unpause_precedes_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime=case.RestartIndexRuntime(arguments(Path(temporary)),{})
            runtime.ch_entry=dict(id='a'*64)
            with patch.object(runtime,'inspect',side_effect=ValueError('foreign mount')),patch.object(runtime,'docker') as docker:
                with self.assertRaises(ValueError):runtime.pause_clickhouse()
                docker.assert_not_called()
            runtime.resources['index_restart_pause']=dict(id='a'*64,status='paused',events=['pausing','paused'])
            paused=dict(Id='a'*64,State=dict(Running=True,Paused=True))
            resumed=dict(Id='a'*64,State=dict(Running=True,Paused=False))
            order=[]
            with patch.object(runtime,'inspect',side_effect=[paused,resumed]), \
                    patch.object(runtime,'docker',side_effect=lambda *a,**k:order.append(a)), \
                    patch.object(initial.IndexRuntime,'cleanup',side_effect=lambda:order.append(('cleanup',))):
                runtime.cleanup()
            self.assertEqual(order,[('unpause','a'*64),('cleanup',)])
            self.assertEqual(runtime.resources['index_restart_pause']['events'],['pausing','paused','unpaused'])

    def test_ambiguous_pause_is_unpaused_and_original_failure_survives_cleanup_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime=case.RestartIndexRuntime(arguments(Path(temporary)),{})
            runtime.ch_entry=dict(id='a'*64)
            running=dict(Id='a'*64,State=dict(Running=True,Paused=False))
            with patch.object(runtime,'inspect',return_value=running),patch.object(runtime,'docker',side_effect=TimeoutError):
                with self.assertRaises(TimeoutError):runtime.pause_clickhouse()
            self.assertEqual(runtime.resources['index_restart_pause']['status'],'pausing')
            with patch.object(runtime,'inspect',side_effect=ValueError('foreign ID')),patch.object(runtime,'docker') as docker, \
                    patch.object(initial.IndexRuntime,'cleanup') as cleanup:
                runtime.cleanup();cleanup.assert_called_once();docker.assert_not_called()
            self.assertEqual(runtime.resources['cleanup_errors'],[dict(resource='paused-clickhouse',error='ValueError')])
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);args=arguments(root);args.case='p01-first-launch-index-restart';checks=[]
            with patch.object(case,'RestartIndexRuntime') as runtime:
                runtime.return_value.index_catch_up.side_effect=AssertionError('original recovery failure')
                runtime.return_value.cleanup.side_effect=ValueError('unpause failed')
                base.first_launch(args,checks,{})
                runtime.return_value.index_retry.assert_not_called()
                runtime.return_value.ordinary.assert_not_called()
            self.assertEqual(base.load(root/'failure.json')['reason'],'original recovery failure')
            result={c['name']:c['result'] for c in checks}
            self.assertEqual(result['first_launch_index_catch_up'],'FAIL')
            self.assertEqual(result['first_launch_cleanup'],'FAIL')
            self.assertEqual(result['first_launch_index_exact_retry'],'NOT_RUN')

    def test_missing_candidate_never_launches_an_engine(self):
        with tempfile.TemporaryDirectory() as temporary:
            evidence=Path(temporary)/'result'
            result=subprocess.run([sys.executable,str(ROOT/'tests/acceptance/run.py'),'p01-first-launch-index-restart',
                '--source-root',str(ROOT),'--candidate-manifest',str(Path(temporary)/'missing'),
                '--evidence',str(evidence)],capture_output=True,text=True,timeout=5)
            self.assertEqual(result.returncode,2,result.stderr)
            report=base.load(evidence/'result.json')
            self.assertEqual(report['counts'],dict(collected=11,executed=1,failed=0,skipped=0,not_run=10))
            self.assertFalse(list((evidence/'commands').iterdir()))


if __name__ == '__main__': unittest.main()
