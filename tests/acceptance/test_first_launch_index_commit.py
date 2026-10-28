"""Literal publication fault histories and raw replay multiplicity; no engines."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from cases import first_launch as base
from cases import first_launch_index as initial
from cases import first_launch_index_oracle as index
from cases import first_launch_index_commit as case
from cases import first_launch_index_commit_oracle as oracle
from test_first_launch import arguments
from test_first_launch_index import ROOT, schema, set_value
from test_first_launch_index_restart import restart_fixture

from test_first_launch_index_query_trace import TRACE_SERVER, TRACE_USERS, TRACE_PROVISION


def commit_fixture(root, mode):
    before,complete,_,rows,science=restart_fixture(root)
    held=copy.deepcopy(before)
    for table in oracle.OWNERS:held[table]=copy.deepcopy(complete[table])
    control=index.only(held['owner_epochs'],role='control',epoch=10)
    control.update(state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,
        control_backend_pid=7331,control_backend_start='2026-10-28T00:00:00+00:00',acquired_at_us=20000,lease_expires_at_us=50000)
    index.only(held['owner_epochs'],role='index',epoch=2).update(lease_expires_at_us=50000)
    index.only(held['role_instances'],role_instance_id=control['role_instance_id']).update(retired_at_us=None)
    failed=copy.deepcopy(held)
    if mode=='after-complete':
        for table in ('index_frontier_plans','consumer_frontier_revisions','consumer_frontiers'):
            failed[table]+=[copy.deepcopy(r) for r in complete[table] if r['frontier_revision']==3]
        for row in failed['projection_chunks']:
            if row['chunk_ordinal']==3:row.update(index.only(complete['projection_chunks'],chunk_id=row['chunk_id']))
        index.only(failed['consumers'],consumer_role='index').update(current_revision=3,xmin='203')
    identity,_,custody=index.retained(root)
    binding=tuple(bytes.fromhex(identity[n]) for n in ('dataset','resource','generation','storage_incarnation','table_identity_digest'))+(custody,1)

    def add_epoch(value,epoch,published,pending):
        previous=index.only(value['owner_epochs'],role='index',epoch=epoch-1)
        prior_control=max((r for r in value['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        incarnation='\\x'+f'{epoch+120:02x}'*16;transaction=str(epoch*100)
        current=dict(previous,epoch=epoch,process_incarnation=incarnation,role_instance_id='\\x'+f'{epoch+130:02x}'*16,
            state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,xmin=transaction)
        fresh=dict(prior_control,epoch=prior_control['epoch']+1,process_incarnation=incarnation,
            role_instance_id='\\x'+f'{epoch+140:02x}'*16,state='released',released_at_us=epoch*10000+500,xmin=str(epoch*100+5))
        fresh.update(exclusion_method='storage_fenced',exclusion_digest=oracle.control_exclusion(fresh,fresh))
        if prior_control['state']=='active':
            prior_control.update(state='released',released_at_us=epoch*10000,exclusion_method='storage_fenced',
                exclusion_digest=oracle.control_exclusion(prior_control,fresh),xmin=transaction)
            index.only(value['role_instances'],role_instance_id=prior_control['role_instance_id']).update(retired_at_us=epoch*10000,xmin=transaction)
        plans=[dict(p,members=[r for r in value['projection_plan_chunks'] if r['plan_digest']==p['plan_digest']]) for p in value['projection_plans']]
        recovery='\\x'+index.recovery_digest(binding,previous,plans,published,pending).hex()
        session=dict(value['index_owner_sessions'][0],owner_epoch=epoch,owner_incarnation=incarnation,
            issuer_epoch=fresh['epoch'],issuer_incarnation=incarnation,previous_epoch=epoch-1,recovered_revision=published,
            recovered_plan=value['projection_plans'][published-1]['plan_digest'],
            recovered_pending_plan=value['projection_plans'][pending-1]['plan_digest'] if pending else None,
            recovery_digest=recovery,xmin=transaction)
        previous.update(state='released',released_at_us=epoch*10000+1,exclusion_method='storage_fenced',exclusion_digest=recovery,xmin=transaction)
        index.only(value['role_instances'],role_instance_id=previous['role_instance_id']).update(retired_at_us=epoch*10000+1,xmin=transaction)
        value['owner_epochs'] += [fresh,current];value['index_owner_sessions'].append(session)
        value['role_instances'] += [dict(role_instance_id=r['role_instance_id'],dataset_id=r['dataset_id'],role=r['role'],
            retired_at_us=None if r['role']=='index' else r['released_at_us'],xmin=r['xmin']) for r in (fresh,current)]
        return current

    caught=copy.deepcopy(complete)
    for table in oracle.OWNERS:caught[table]=copy.deepcopy(failed[table])
    current=add_epoch(caught,3,2 if mode=='before-forward' else 3,3 if mode=='before-forward' else None)
    for plan in caught['projection_plans']:
        if plan['frontier_revision']==4:plan.update(origin_epoch=3,origin_incarnation=current['process_incarnation'])
    for row in caught['projection_chunks']:
        n=row['chunk_ordinal']
        if n==4:row.update(owner_epoch=3,owner_incarnation=current['process_incarnation'])
        if n==4 or n==3 and mode=='before-forward':row.update(xmin=str(300+n),committed_at_us=30000+n)
    for table in ('index_frontier_plans','consumer_frontier_revisions','consumer_frontiers'):
        for row in caught[table]:
            n=row['frontier_revision']
            if row['consumer_id']=='\\x'+'ac'*16 or not (n==4 or n==3 and mode=='before-forward'):continue
            row['xmin']=str(300+n)
            if table!='consumer_frontiers':row.update(owner_epoch=3,owner_incarnation=current['process_incarnation'])
            if table=='index_frontier_plans':row['committed_at_us']=30000+n
            if table=='consumer_frontier_revisions':row['created_at_us']=30000+n
    index.only(caught['consumers'],consumer_role='index')['xmin']='304'
    retried=copy.deepcopy(caught);add_epoch(retried,4,4,None)
    if mode=='before-forward':rows['dictionaries']=[r for r in rows['dictionaries'] for _ in range(2 if 4<=r['source'][3]<=7 else 1)]
    event=dict(mode=mode,target='control-index-publication',hold_index_publication=True,backend_pid=7331,
        index_consumer=index.only(before['consumers'],consumer_role='index')['consumer_id'][2:],index_revision=3,index_previous_revision=2,index_topology=0,
        data_queries=1,data_commits=0,server_complete=0,client_complete=0,faults=0)
    evidence=dict(mode=mode,selected=dict(event,event='index-publication-update-complete'),held=dict(event,event='held',stage='index-publication-update'),
        fault=dict(event,event='fault',data_commits=1,server_complete=int(mode=='after-complete'),faults=1),
        exit_code=4,commit_unknown=True,success_record=False,owner_release_claimed=False,parent_after=None,backend_absent=True,
        custody_held=dict(busy=True,identity={'inode':123}),custody_joined=dict(busy=False,identity={'inode':123}),
        allow_ns=2000000,held_started_ns=1000000,operation_limit_us=5000000)
    return before,held,failed,caught,retried,rows,science,evidence


class IndexCommitControls(unittest.TestCase):
    def test_operation_cap_is_the_unchanged_deployed_default_not_managed_policy(self):
        field=base.load(ROOT/'contracts/config/schema.json')['properties']['runtime']['properties']['renewal_timeout']
        self.assertEqual((field['default'],field['x-bigbrotr-deployment-locked'],field['x-bigbrotr-reload']),('5s',True,'restart'))
        with tempfile.TemporaryDirectory() as directory:
            runtime=case.IndexCommitRuntime(arguments(Path(directory)),{})
            runtime.config=Path(directory)/'instance.toml'
            runtime.home=Path(directory)/'client';runtime.home.mkdir()
            runtime.set_value=set_value
            private=runtime.state/'clickhouse/private';private.mkdir(parents=True)
            for name,text in (('server.xml',TRACE_SERVER),('users.xml',TRACE_USERS),('provision.sql',TRACE_PROVISION)):
                (private/name).write_text(text)
            original=initial.recipe((ROOT/'config/recipes/acceptance.toml').read_text(),set_value)
            runtime.config.write_text(original)
            with patch.object(initial.IndexRuntime,'prepare_state'):
                runtime.prepare_state()
            expected=tomllib.loads(original)
            expected.setdefault('archive',{})['max_publish_delay']='1s'
            expected.setdefault('runtime',{})['renewal_timeout']='5s'
            self.assertEqual(tomllib.loads(runtime.config.read_text()),expected)
            self.assertEqual(runtime.inputs['initial_recipe_sha256'],base.digest(runtime.config))
            self.assertEqual(case.publication_operation_limit(expected),5_000_000)
            # Missing, larger, smaller and differently typed deployed values
            # cannot borrow a managed-policy field or reach a fault launch.
            for value in (None,'10s','500ms',5_000_000):
                changed=copy.deepcopy(expected)
                if value is None:del changed['runtime']['renewal_timeout']
                else:changed['runtime']['renewal_timeout']=value
                with self.subTest(value=value),self.assertRaises(ValueError):
                    case.publication_operation_limit(changed)
                runtime.config.write_text(original if value is None else set_value(original,'runtime','renewal_timeout',value))
                with self.subTest(value=value),patch.object(runtime,'source_unchanged'),patch.object(runtime,'launch') as launch:
                    with self.assertRaises(ValueError):runtime.index_commit_fault()
                    launch.assert_not_called()

    def test_both_histories_have_independent_publication_and_recovery_tuples(self):
        for mode in oracle.MODES:
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory);before,held,failed,caught,retry,rows,science,evidence=commit_fixture(root,mode)
                owner=index.only(held['owner_epochs'],role='control',state='active')
                backend=dict(pid=owner['control_backend_pid'],backend_start=owner['control_backend_start'],usename='bigbrotr_control',datname='bigbrotr_control')
                oracle.held(before,held,root,backend,21000)
                oracle.fault(held,failed,evidence,root)
                oracle.recovered(failed,caught,root,mode);oracle.recovered(caught,retry,root,mode,retry=True)
                result=index.publication(retry,retry,rows,science,schema,root,owner_count=4,publication_fault=mode)
                self.assertEqual(result['committed_chunks'],40)
                self.assertEqual(len(rows['dictionaries']),12 if mode=='before-forward' else 8)
                self.assertEqual([r['origin_epoch'] for r in retry['projection_plans']],[1,1,1,3])
                self.assertEqual([r['owner_epoch'] for r in retry['index_frontier_plans']],[1,1,3 if mode=='before-forward' else 2,3])

    def test_wrong_target_counters_success_or_unjoined_custody_refuse(self):
        changes=(lambda e:e['fault'].update(index_consumer='00'*16),lambda e:e['fault'].update(index_revision=4),
            lambda e:e['fault'].update(index_topology=1),lambda e:e['fault'].update(index_previous_revision=1),
            lambda e:e['selected'].update(backend_pid=999),lambda e:e['fault'].update(data_commits=2),
            lambda e:e['fault'].update(server_complete=1),lambda e:e['fault'].update(client_complete=1),
            lambda e:e['fault'].update(event='held'),
            lambda e:e.update(success_record=True),lambda e:e.update(owner_release_claimed=True),
            lambda e:e.update(backend_absent=False),lambda e:e.update(parent_after={'pid':1}),
            lambda e:e['custody_joined'].update(busy=True),lambda e:e.update(allow_ns=5001000000))
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);_,held,failed,_,_,_,_,evidence=commit_fixture(root,'before-forward')
            for change in changes:
                value=copy.deepcopy(evidence);change(value)
                with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                    oracle.fault(held,failed,value,root)

    def test_partial_publication_wrong_publisher_and_plan4_continuation_refuse(self):
        for mode in oracle.MODES:
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory);_,held,failed,caught,_,_,_,evidence=commit_fixture(root,mode)
                changes=(lambda s:index.only(s['consumers'],consumer_role='index').update(current_revision=4),
                    lambda s:s['projection_plans'].append(copy.deepcopy(caught['projection_plans'][-1])),
                    lambda s:s['projection_chunks'][-1].update(state='committed' if mode=='before-forward' else 'prepared'),
                    lambda s:s['projection_chunks'][-1].update(xmin='999'),
                    lambda s:s['index_frontier_plans'][-1].update(owner_epoch=1 if mode=='after-complete' else 2),
                    lambda s:s['index_owner_sessions'][-1].update(recovered_revision=3),
                    lambda s:s['index_owner_sessions'][-1].update(recovered_pending_plan=None))
                for change in changes:
                    value=copy.deepcopy(failed);change(value)
                    with self.subTest(mode=mode,change=change),self.assertRaises((AssertionError,ValueError)):
                        oracle.fault(held,value,evidence,root)

    def test_recovery_requires_successor_bound_control_exclusion_and_preserved_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);_,_,failed,caught,retry,_,_,_=commit_fixture(root,'after-complete')
            for change in (lambda s:index.only(s['owner_epochs'],role='control',epoch=10).update(exclusion_digest='\\x'+'00'*32),
                    lambda s:index.only(s['owner_epochs'],role='control',epoch=10).update(state='active'),
                    lambda s:s['index_frontier_plans'][2].update(xmin='999'),
                    lambda s:s['index_frontier_plans'][2].update(owner_epoch=3),
                    lambda s:s['projection_plans'][2].update(origin_epoch=3)):
                value=copy.deepcopy(caught);change(value)
                with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                    oracle.recovered(failed,value,root,'after-complete')
            for table in base.expected_tables(ROOT)-oracle.OWNERS:
                value=copy.deepcopy(retry);value[table].append({'unexpected':'write'})
                with self.subTest(table=table),self.assertRaises((AssertionError,ValueError,KeyError)):
                    oracle.recovered(caught,value,root,'after-complete',retry=True)

    def test_raw_copies_cannot_be_hidden_added_or_changed(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);_,_,_,_,state,rows,science,_=commit_fixture(root,'before-forward')
            for change in (lambda r:r['dictionaries'].pop(),lambda r:r['dictionaries'].append(copy.deepcopy(r['dictionaries'][3])),
                    lambda r:r['dictionaries'][3].update(label='different immutable copy')):
                value=copy.deepcopy(rows);change(value)
                with self.subTest(change=change),self.assertRaises(AssertionError):
                    index.publication(state,state,value,science,schema,root,owner_count=4,publication_fault='before-forward')

    def test_cli_reports_resolve_the_observed_head_without_claiming_readiness(self):
        for mode in oracle.MODES:
            with tempfile.TemporaryDirectory() as directory:
                _,_,_,state,_,_,_,_=commit_fixture(Path(directory),mode)
                gen=state['projection_generations'][0]
                value=dict(format='bigbrotr-initial-index-catch-up/2',state='complete',dataset_id=gen['dataset_id'][2:],
                    generation_id=gen['generation_id'][2:],storage_incarnation=gen['storage_incarnation'][2:],
                    table_identity_digest=gen['table_identity_digest'][2:],previous_owner_epoch=2,owner_epoch=3,
                    published_revision_before=2 if mode=='before-forward' else 3,published_revision_after=4,
                    source_through=12,published_through=12,recovered_pending=mode=='before-forward',new_batches=1,
                    source_epoch=1,source_cut=dict(topology_generation=0,bounds=[dict(shard_id=0,owner_epoch=1,last_complete_sequence=12)]),
                    published_cut=dict(topology_generation=0,bounds=[dict(shard_id=0,owner_epoch=1,last_complete_sequence=12)]),
                    index_handoff_revision=None,published_handoff=False,
                    ready=False,acknowledgement='not-performed',ingest_retirement='not-performed',topology_transition='not-performed')
                oracle.report(value,state,mode)
                for change in (dict(ready=True),dict(recovered_pending=mode!='before-forward'),dict(new_batches=0),
                               dict(published_revision_before=4),dict(owner_epoch=2),dict(acknowledgement='performed')):
                    with self.subTest(mode=mode,change=change),self.assertRaises(AssertionError):
                        oracle.report(dict(value,**change),state,mode)
                retry=dict(value,previous_owner_epoch=3,owner_epoch=4,published_revision_before=4,recovered_pending=False,new_batches=0)
                oracle.report(retry,state,mode,retry=True)

    def test_missing_candidate_never_starts_engines_and_fault_failure_stays_failed(self):
        for name in case.CASES:
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                result=subprocess.run([sys.executable,'-B',str(ROOT/'tests/acceptance/run.py'),name,'--source-root',str(ROOT),
                    '--candidate-manifest',str(root/'missing'),'--evidence',str(root/'evidence')],capture_output=True,text=True,timeout=5)
                report=json.loads((root/'evidence/result.json').read_text())
                self.assertEqual((result.returncode,report['result'],len(report['checks'])),(2,'NOT_RUN',12))
                self.assertFalse(list((root/'evidence/commands').iterdir()))
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory);args=arguments(root);args.case=name;checks=[]
                with patch.object(case,'IndexCommitRuntime') as runtime:
                    runtime.return_value.index_commit_fault.side_effect=ValueError('unresolved')
                    base.first_launch(args,checks,{})
                    runtime.return_value.index_catch_up.assert_not_called()
                    runtime.return_value.cleanup.assert_called_once()
                self.assertEqual(base.load(root/'failure.json')['stage'],'index_publication_commit_loss')


if __name__=='__main__':unittest.main()
