"""Independent live-boundary controls; no product, engine or Docker execution."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from cases import first_launch as base
from cases import first_launch_index_live as case
from cases import first_launch_index_live_oracle as live
from cases import first_launch_index_oracle as index
from cases import first_launch_index_topology_oracle as topology
from test_first_launch import arguments
from test_first_launch_index import ROOT
from test_first_launch_index_topology import recovery_fixture


def roles_fixture():
    state=dict(datasets=[dict(dataset_id='\\x'+'11'*16)],
        owner_epochs=[dict(role='control',state='active',process_incarnation='\\x'+'22'*16)],
        producer_epochs=[dict(producer_epoch=2,state='open')],
        source_streams=[dict(owner_epoch=2,state='open',last_complete_sequence=None)],
        projection_generations=[dict(generation_id='\\x'+'33'*16,storage_incarnation='\\x'+'44'*16)])
    cut=dict(dataset_id='11'*16,topology_generation='1',bounds=[
        dict(shard_id='0',owner_epoch='1',last_complete_sequence='12'),
        dict(shard_id='0',owner_epoch='2',last_complete_sequence='10')])
    observation=dict(reason='current',freshness='fresh',time=dict(observed_at_us='200',age_us='100'))
    value=dict(format='bigbrotr-role-status/1',authority='observation-only',observed_at_us='200',
        identity=dict(dataset_id='11'*16,process_incarnation='22'*16,config_revision='1',policy_revision='1'),
        source=dict(cut=cut,**copy.deepcopy(observation)),
        roles=[dict(role=role,state='ready' if role=='control' else 'idle',reason='current')
               for role in ('control','ingest','sync','index')],
        capabilities=[dict(capability=kind,state='ready',**copy.deepcopy(observation),
            published=dict(revision='8',cut=copy.deepcopy(cut)) if kind=='index_publication' else None,
            generation='33'*16 if kind=='index_publication' else None,
            storage_incarnation='44'*16 if kind=='index_publication' else None)
            for kind in ('signed_event_admission','finite_acquisition','index_publication')])
    return state,value


def bridge_fixture():
    _,binding,owners,_,_=recovery_fixture()
    generation=dict(zip(('dataset_id','owner_resource_id','generation_id'),
                        ['\\x'+value.hex() for value in binding[:3]]))
    publisher=dict(owners[1],acquired_at_us=100,lease_expires_at_us=1000)
    common=dict(consumer_id=generation['owner_resource_id'],dataset_id=generation['dataset_id'],
                topology_generation=1,frontier_revision=5,xmin='7')
    handoff=dict(common,expected_revision=4,generation_id=generation['generation_id'],
        handoff_digest='\\x93e6288fd8a0f5fbd46932a47483376e3bdb3952f3be25ad9764337130bf3586',
        ingest_resource='\\x'+'77'*16,succession_digest='\\x'+'88'*32,source_shard=0,
        predecessor_through=12,owner_epoch=2,owner_incarnation=publisher['process_incarnation'],committed_at_us=123)
    old=dict(common,frontier_revision=4,topology_generation=0,shard_id=0,owner_epoch=1,
             last_complete_sequence=12,attestation_kind='projection-chunk',attestation_digest='\\x'+'99'*32,xmin='6')
    state=dict(index_frontier_handoffs=[handoff],initial_ingest_successions=[dict(
        owner_resource_id=handoff['ingest_resource'],handoff_sha256=handoff['succession_digest'])],
        consumer_frontier_revisions=[dict(common,consumer_role='index',previous_revision=4,
            owner_resource_id=common['consumer_id'],owner_epoch=2,
            owner_incarnation=publisher['process_incarnation'],sealed=True,created_at_us=123)],
        consumer_frontiers=[old,dict(old,**common),dict(common,shard_id=0,owner_epoch=2,
            last_complete_sequence=0,attestation_kind=None,attestation_digest=None)],
        index_frontier_plans=[],projection_plans=[])
    return state,binding,publisher,generation


def later_topology_fixture():
    ds,resource,slot='\\x'+'11'*16,'\\x'+'55'*16,'\\x'+'77'*16
    before={name:[] for name in base.expected_tables(ROOT,{})}
    before.update(datasets=[dict(dataset_id=ds,topology_generation=1,config_revision=1,policy_revision=1,xmin='1')],
        consumers=[dict(consumer_role='index',consumer_id='index',current_revision=8)],
        owner_epochs=[dict(role='control',epoch=10,process_incarnation='old-control'),
                      dict(role='ingest',epoch=2,resource_id=resource,process_incarnation='old-ingest')],
        producer_epochs=[dict(producer_epoch=2,state='retired',reserved_through_request=3,producer_slot_id=slot)],
        source_streams=[dict(owner_epoch=2,state='closed',last_complete_sequence=10,closure_digest='\\x'+'99'*32)],
        index_frontier_plans=[dict(frontier_revision=8,xmin='8')],archive_attempts=[dict(attempt_id='old')])
    after=copy.deepcopy(before);after['datasets'][0].update(topology_generation=2,xmin='2')
    incarnation='\\x'+'33'*16;control_role='\\x'+'c0'*16;ingest_role='\\x'+'d0'*16
    after['owner_epochs'] += [dict(dataset_id=ds,role='control',epoch=11,state='active',released_at_us=None,
        process_incarnation=incarnation,role_instance_id=control_role),dict(dataset_id=ds,role='ingest',epoch=3,
        state='active',resource_id=resource,process_incarnation=incarnation,role_instance_id=ingest_role)]
    after['role_instances']=[dict(dataset_id=ds,role=role,role_instance_id=key,retired_at_us=None)
                             for role,key in (('control',control_role),('ingest',ingest_role))]
    after['producer_epochs'].append(dict(dataset_id=ds,producer_epoch=3,owner_epoch=3,state='open',
        reserved_through_request=0,outstanding_request_limit=32,producer_slot_id=slot,producer_role='ingest',
        owner_role='ingest',producer_role_instance_id=ingest_role,producer_incarnation=incarnation,owner_incarnation=incarnation))
    after['source_streams'].append(dict(dataset_id=ds,owner_epoch=3,state='open',last_complete_sequence=None,
        owner_resource_id=resource,owner_incarnation=incarnation))
    after['topology_generations']=[dict(dataset_id=ds,generation=2,previous_generation=1,sealed=True)]
    after['topology_streams']=[dict(dataset_id=ds,shard_id=0,owner_epoch=epoch,handoff_last_complete_sequence=last)
                              for epoch,last in ((1,12),(2,10),(3,None))]
    after['allocation_domains']=[dict(dataset_id=ds,dictionary_kind=n,allocation_domain=f'new-{n}',
        owner_epoch=3,owner_resource_id=resource,owner_incarnation=incarnation,state='active') for n in range(1,10)]
    after['ingest_successions']=[dict(dataset_id=ds,target_epoch=3,previous_epoch=2,previous_topology=1,
        target_topology=2,terminal_kind=1,issued_through=3,last_complete_sequence=10,closure_digest='\\x'+'99'*32,
        target_incarnation=incarnation,target_role_instance_id=ingest_role,owner_resource_id=resource,
        producer_slot_id=slot,target_manifest_sha256='manifest3',handoff_sha256='handoff3')]
    after['ingest_activations']=[dict(dataset_id=ds,target_epoch=3,target_incarnation=incarnation,
        target_role_instance_id=ingest_role,target_manifest_sha256='manifest3',handoff_sha256='handoff3',
        activated_by_epoch=11,activated_by_incarnation=incarnation)]
    report=dict(format='bigbrotr-ingest-successor/1',state='activated',dataset_id='11'*16,
        owner_resource_id='55'*16,producer_slot_id='77'*16,producer_epoch='3',process_incarnation='33'*16,
        role_instance_id='d0'*16,config_revision='1',policy_revision='1',outstanding_request_limit='32',
        acquisition='not-started',ready=False,predecessor=dict(producer_epoch='2',resolved_requests='3',
            last_complete_sequence='10',sha256='99'*32))
    return before,after,report


class LiveIndexTopologyControls(unittest.TestCase):
    def test_genuine_later_topology_preserves_science_and_stops_only_the_new_control(self):
        before,after,report=later_topology_fixture()
        live.later_topology(before,after,report)
        stopped=copy.deepcopy(after)
        stopped['owner_epochs'][-2].update(state='released',released_at_us=123)
        stopped['role_instances'][0]['retired_at_us']=123
        live.later_topology(before,stopped,report,stopped=True)
        mutations=(
            lambda s:s['datasets'][0].update(topology_generation=1),
            lambda s:s['producer_requests'].append(dict(producer_epoch=3)),
            lambda s:s['producer_epochs'][-1].update(reserved_through_request=1),
            lambda s:s['index_owner_sessions'].append(dict(owner_epoch=5)),
            lambda s:s['index_frontier_plans'][0].update(xmin='changed'),
            lambda s:s['archive_attempts'][0].update(attempt_id='changed'),
            lambda s:s['topology_streams'][1].update(handoff_last_complete_sequence=9),
            lambda s:s['ingest_successions'][0].update(issued_through=2),
            lambda s:s['owner_epochs'][-1].update(process_incarnation='unrelated-process'),
        )
        for number,change in enumerate(mutations):
            wrong=copy.deepcopy(after);change(wrong)
            with self.subTest(mutation=number),self.assertRaises((AssertionError,ValueError)):
                live.later_topology(before,wrong,report)

    def test_historical_sql_requires_actual_unprivileged_login_and_exact_topology_refusal(self):
        consumer,dataset='\\x'+'55'*16,'\\x'+'11'*16
        identity=dict(event='runtime_login',current_user='bigbrotr_control',session_user='bigbrotr_control',
            database='bigbrotr_control',dataset_id='11'*16,client_addr='127.0.0.1',read_only='on',
            role_flags=dict(rolcanlogin=True,rolsuper=False,rolcreatedb=False,rolcreaterole=False,
                rolinherit=False,rolreplication=False,rolbypassrls=False),memberships=0,
            database_temp=False,schema_create=False,dataset_topology=2)
        metadata=dict(event='historical_metadata',consumer_id='55'*16,frontier_revision=5,status='PASS')
        def output(actor):return json.dumps(actor)+'\n'+json.dumps(metadata)+'\nauthority_sqlstate 23514\n'
        error='ERROR:  23514: Index handoff publication requires current initial successor topology\nCONTEXT: retained function\n'
        live.historical_sql_result(output(identity),error,0,consumer,dataset)
        for key,wrong in [('current_user','postgres'),('session_user','postgres'),('read_only','off'),
                          ('client_addr',None),('memberships',1),('database_temp',True)]:
            with self.subTest(field=key),self.assertRaises(AssertionError):
                live.historical_sql_result(output(dict(identity,**{key:wrong})),error,0,consumer,dataset)
        privileged=copy.deepcopy(identity);privileged['role_flags']['rolsuper']=True
        with self.assertRaises(AssertionError):live.historical_sql_result(output(privileged),error,0,consumer,dataset)
        for wrong in ('ERROR:  23514: owner is unavailable\n',error.replace('23514','42501'),'',error+error):
            with self.subTest(error=wrong),self.assertRaises(AssertionError):
                live.historical_sql_result(output(identity),wrong,0,consumer,dataset)
        sql=live.historical_sql(consumer,dataset)
        self.assertIn('BEGIN READ ONLY;',sql);self.assertIn('ROLLBACK;',sql)
        self.assertLess(sql.index('require_index_handoff_metadata'),sql.index('require_index_handoff_authority'))
        self.assertIn('-h 127.0.0.1',live.CONTROL_LOGIN);self.assertIn('-U bigbrotr_control',live.CONTROL_LOGIN)
        self.assertIn('read -r live_index_password < /run/secrets/control',live.CONTROL_LOGIN)

    def test_original_and_bridge_rows_survive_later_publication_with_exact_mvcc(self):
        initial={name:[] for name in index.INDEX_TABLES}
        initial.update(consumers=[dict(consumer_role='index',consumer_id='index',current_revision=4)],
            index_frontier_plans=[dict(frontier_revision=4,owner_epoch=1,owner_incarnation='old-publisher',xmin='4')],
            projection_chunks=[dict(chunk_id='old-chunk',owner_epoch=1,owner_incarnation='old-publisher',xmin='4')],
            consumer_frontier_revisions=[dict(consumer_id='index',frontier_revision=4,owner_epoch=1,xmin='4')],
            consumer_frontiers=[dict(consumer_id='index',frontier_revision=4,last_complete_sequence=12,
                attestation_digest='old-attestation',xmin='4')])
        bridge=copy.deepcopy(initial);bridge['consumers'][0]['current_revision']=5
        bridge['index_frontier_handoffs']=[dict(frontier_revision=5,owner_epoch=2,xmin='5')]
        bridge['consumer_frontier_revisions'].append(dict(consumer_id='index',frontier_revision=5,owner_epoch=2,xmin='5'))
        bridge['consumer_frontiers'].append(dict(consumer_id='index',frontier_revision=5,last_complete_sequence=0,
            attestation_digest=None,xmin='5'))
        live.append_only_history(initial,bridge)
        current=copy.deepcopy(bridge);current['consumers'][0]['current_revision']=8
        current['index_frontier_plans'].append(dict(frontier_revision=8,owner_epoch=2,xmin='8'))
        live.append_only_history(initial,current);live.append_only_history(bridge,current)
        mutations=(('index_frontier_plans','owner_incarnation','later-publisher'),
            ('index_frontier_plans','xmin','8'),('projection_chunks','owner_epoch',2),
            ('projection_chunks','xmin','8'),('consumer_frontier_revisions','xmin','8'),
            ('consumer_frontiers','attestation_digest','changed-attestation'),
            ('consumer_frontiers','xmin','8'))
        for table,key,value in mutations:
            wrong=copy.deepcopy(current);wrong[table][0][key]=value
            with self.subTest(table=table,key=key),self.assertRaises(AssertionError):
                live.append_only_history(bridge,wrong)
        wrong=copy.deepcopy(bridge);wrong['consumer_frontiers'][0]['attestation_digest']='changed-at-bridge'
        with self.assertRaises(AssertionError):live.append_only_history(initial,wrong)
        wrong=copy.deepcopy(current);wrong['index_frontier_handoffs'][0]['xmin']='8'
        with self.assertRaises(AssertionError):live.append_only_history(bridge,wrong)

    def test_startup_binds_retained_identity_and_exact_receipt_free_zero_bridge(self):
        generation=dict(dataset_id='\\x'+'11'*16,generation_id='\\x'+'33'*16,
            storage_incarnation='\\x'+'44'*16,table_identity_digest='\\x'+'55'*32)
        value=dict(format='bigbrotr-live-initial-index/1',state='bridged',generation_id='33'*16,
            storage_incarnation='44'*16,table_identity_digest='55'*32,previous_owner_epoch=1,owner_epoch=2,
            published_revision_before=4,published_revision_after=5,ready=False,
            published_cut=dict(dataset_id='11'*16,topology_generation='1',bounds=[
                dict(shard_id='0',owner_epoch='1',last_complete_sequence='12'),
                dict(shard_id='0',owner_epoch='2',last_complete_sequence='0')]))
        live.startup_index(value,generation)
        for field,wrong in [('generation_id','00'*16),('storage_incarnation','00'*16),
                ('table_identity_digest','00'*32),('owner_epoch',True),('published_revision_after',4),
                ('ready',True),('state','ready')]:
            with self.subTest(field=field),self.assertRaises(AssertionError):
                live.startup_index(dict(value,**{field:wrong}),generation)
        wrong=copy.deepcopy(value);wrong['published_cut']['bounds'].pop()
        with self.assertRaises(AssertionError):live.startup_index(wrong,generation)

    def test_ready_requires_full_ordered_vector_current_identity_and_original_age(self):
        state,value=roles_fixture()
        live.ready_roles(value,state,190,210)
        mutations=(
            lambda v:v['source']['cut']['bounds'].pop(0),
            lambda v:v['source']['cut']['bounds'].reverse(),
            lambda v:v['source']['cut']['bounds'][0].update(last_complete_sequence='11'),
            lambda v:v['capabilities'][2]['published']['cut']['bounds'][1].update(last_complete_sequence='0'),
            lambda v:v['capabilities'][2].update(published=None),
            lambda v:v['capabilities'][2].update(generation='00'*16),
            lambda v:v['capabilities'][2]['time'].update(age_us='30000000'),
            lambda v:v['identity'].update(process_incarnation='00'*16),
            lambda v:v.update(authority='publication-authority'),
            lambda v:v['roles'].append(dict(role='archive',state='idle',reason='current')),
        )
        for number,change in enumerate(mutations):
            wrong=copy.deepcopy(value);change(wrong)
            with self.subTest(mutation=number),self.assertRaises((AssertionError,ValueError)):
                live.ready_roles(wrong,state,190,210)
        for table,key,value in [('producer_epochs','state','closing'),('source_streams','state','closed')]:
            wrong=copy.deepcopy(state);wrong[table][0][key]=value
            with self.subTest(table=table),self.assertRaises(AssertionError):
                live.ready_roles(roles_fixture()[1],wrong,190,210)

    def test_bridge_zero_is_stored_atomic_and_never_a_physical_batch(self):
        state,binding,publisher,generation=bridge_fixture()
        topology.bridge_history(state,binding,publisher,generation)
        mutations=(
            lambda s:s['consumer_frontiers'][2].update(last_complete_sequence=1),
            lambda s:s['consumer_frontiers'][2].update(attestation_kind='projection-chunk'),
            lambda s:s['consumer_frontiers'][2].update(xmin='8'),
            lambda s:s['index_frontier_handoffs'][0].update(owner_incarnation='\\x'+'00'*16),
            lambda s:s['consumer_frontier_revisions'][0].update(created_at_us=124),
            lambda s:s['projection_plans'].append(dict(frontier_revision=5)),
            lambda s:s['index_frontier_plans'].append(dict(frontier_revision=5)),
        )
        for number,change in enumerate(mutations):
            wrong=copy.deepcopy(state);change(wrong)
            with self.subTest(mutation=number),self.assertRaises((AssertionError,ValueError)):
                topology.bridge_history(wrong,binding,publisher,generation)

    def test_successor_enables_retained_index_without_initializer(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime=case.LiveTopologyIndexRuntime(arguments(Path(temporary)),{})
            with patch.object(runtime,'launch') as launch:
                runtime.successor_launch()
            self.assertEqual(launch.call_args.args,('successor','app','serve','--file','/etc/bigbrotr/instance.toml',
                '--activate-initial-successor','--roles','control,ingest,sync,index'))

    def test_dispatch_retains_failure_and_does_not_run_finite_history_after_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);args=arguments(root);args.case='p01-first-launch-index-live-topology';checks=[]
            with patch.object(case,'LiveTopologyIndexRuntime') as runtime:
                runtime.return_value.restart_ingest.side_effect=AssertionError('original live bridge refusal')
                base.first_launch(args,checks,{})
                runtime.return_value.restart_archive.assert_not_called()
                runtime.return_value.index_epoch2_catch_up.assert_not_called()
                runtime.return_value.cleanup.assert_called_once()
            self.assertEqual(base.load(root/'failure.json')['reason'],'original live bridge refusal')
            self.assertEqual([c['name'] for c in checks],['first_launch_'+name for name in case.STAGES])
            self.assertEqual({c['name']:c['result'] for c in checks}['first_launch_archive_catch_up'],'NOT_RUN')

    def test_missing_candidate_refuses_before_any_engine_command(self):
        with tempfile.TemporaryDirectory() as temporary:
            evidence=Path(temporary)/'result'
            result=subprocess.run([sys.executable,str(ROOT/'tests/acceptance/run.py'),'p01-first-launch-index-live-topology',
                '--source-root',str(ROOT),'--candidate-manifest',str(Path(temporary)/'missing'),
                '--evidence',str(evidence)],capture_output=True,text=True,timeout=5)
            self.assertEqual(result.returncode,2,result.stderr)
            report=base.load(evidence/'result.json')
            self.assertEqual(report['counts'],dict(collected=16,executed=1,failed=0,skipped=0,not_run=15))
            self.assertFalse(list((evidence/'commands').iterdir()))


if __name__=='__main__':unittest.main()
