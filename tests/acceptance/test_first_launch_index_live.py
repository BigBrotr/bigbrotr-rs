"""Independent live-boundary controls; no product, engine or Docker execution."""
import copy
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from cases import first_launch as base
from cases import first_launch_index_live as case
from cases import first_launch_index_live_oracle as live
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


class LiveIndexTopologyControls(unittest.TestCase):
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
            self.assertEqual(report['counts'],dict(collected=15,executed=1,failed=0,skipped=0,not_run=14))
            self.assertFalse(list((evidence/'commands').iterdir()))


if __name__=='__main__':unittest.main()
