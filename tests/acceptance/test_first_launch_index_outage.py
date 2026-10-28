"""Stopped-backend custody and exact retained-prefix controls; no engines."""
import copy
import errno
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from cases import first_launch as base
from cases import first_launch_index as initial
from cases import first_launch_index_outage as case
from cases import first_launch_index_outage_oracle as check
from cases import first_launch_archive_outage_oracle as outage
from cases import first_launch_index_oracle as index
from cases import first_launch_retained_native as native
from test_first_launch import arguments, ROOT
from test_first_launch_archive_outage import preservation_fixture
from test_first_launch_index_restart import restart_fixture
from test_first_launch_index import schema
from test_first_launch_restart import fixture


def inverse_fixture():
    before,live,_=preservation_fixture()
    live['archive_attempts'].append(dict(added=2,xmin='50'))
    live['archive_frontier_snapshots']=[dict(added=n,xmin='50') for n in range(2)]
    live['archive_frontier_batches']=[dict(added=n,xmin='50') for n in range(7)]
    live['projection_plans'].pop()
    for table in ('projection_plan_chunks','projection_chunks','projection_chunk_bounds'):
        live[table]=live[table][:30]
    live['index_frontier_plans']=copy.deepcopy(before['index_frontier_plans'])
    live['consumers'][0].update(current_revision=4,xmin='50')
    live['consumers'][1]=copy.deepcopy(before['consumers'][1])
    live['owner_epochs'][2].update(lease_expires_at_us=20000,xmin='51')
    return before,live


def roles_fixture():
    state=dict(datasets=[dict(dataset_id='\\x'+'22'*16,config_revision=1,policy_revision=1)],
        owner_epochs=[dict(role='control',state='active',process_incarnation='\\x'+'77'*16)],
        projection_generations=[dict(generation_id='\\x'+'66'*16,storage_incarnation='\\x'+'88'*16)])
    cut=dict(dataset_id='22'*16,topology_generation='0',bounds=[dict(shard_id='0',owner_epoch='1',last_complete_sequence='12')])
    stamp=dict(observed_at_us='123456789',age_us='1000')
    value=dict(format='bigbrotr-role-status/1',authority='observation-only',observed_at_us='123456789',
        identity=dict(dataset_id='22'*16,process_incarnation='77'*16,config_revision='1',policy_revision='1'),
        source=dict(cut=cut,freshness='fresh',time=copy.deepcopy(stamp),reason='current'),
        roles=[dict(role=r,state='ready' if r=='control' else 'degraded' if r=='index' else 'idle',
            reason='dependency_unavailable' if r=='index' else 'current') for r in ('control','ingest','sync','archive','index')],
        capabilities=[])
    for kind in ('signed_event_admission','finite_acquisition','archive_publication','index_publication'):
        failed=kind=='index_publication'
        published=dict(revision='2' if failed else '4',cut=copy.deepcopy(cut)) if kind.endswith('_publication') else None
        if failed:published['cut']['bounds'][0]['last_complete_sequence']='3'
        value['capabilities'].append(dict(capability=kind,state='unavailable' if failed else 'ready',
            reason='dependency_unavailable' if failed else 'current',freshness='fresh',time=copy.deepcopy(stamp),
            published=published,generation='66'*16 if failed else None,storage_incarnation='88'*16 if failed else None))
    return state,value


class IndexOutageControls(unittest.TestCase):
    def test_inverse_all82_transition_preserves_index_head_and_unrelated_rows(self):
        before,live=inverse_fixture()
        outage.prefix_preservation(before,live,index_unavailable=True)
        mutations=(lambda v:v['projection_chunks'].pop(),lambda v:v['projection_plans'].append(dict(extra=True)),
            lambda v:v['index_frontier_plans'].append(dict(extra=True)),
            lambda v:v['consumers'][1].update(current_revision=3),
            lambda v:v['consumers'][0].update(current_revision=3),
            lambda v:v['archive_frontier_batches'].pop(),lambda v:v['archive_attempts'].pop(),
            lambda v:v['datasets'][0].update(dataset_id='foreign'),
            lambda v:v['producer_requests'][0].update(xmin='new'),
            lambda v:v['owner_epochs'][0].update(control_backend_pid=999),
            lambda v:v['browser_sessions'][0].update(idle_expires_at_us=999))
        for number,mutate in enumerate(mutations):
            invalid=copy.deepcopy(live);mutate(invalid)
            with self.subTest(mutation=number),self.assertRaises(AssertionError):
                outage.prefix_preservation(before,invalid,index_unavailable=True)
        with self.assertRaises(AssertionError):outage.prefix_preservation(before,live)

    def test_stopped_index_roles_retain_original_cut_generation_and_age(self):
        state,value=roles_fixture()
        index.live_roles(value,state,123456000,123457000,index_unavailable=True)
        mutations=(lambda v:v['capabilities'][-1].update(state='ready',reason='current'),
            lambda v:v['capabilities'][-1].update(state='building',reason='verification_in_flight'),
            lambda v:v['capabilities'][-1].update(reason='remote_outcome_unknown'),
            lambda v:v['capabilities'][-1]['published'].update(revision='3'),
            lambda v:v['capabilities'][-1]['published']['cut']['bounds'][0].update(last_complete_sequence='7'),
            lambda v:v['capabilities'][-1].update(generation='99'*16),
            lambda v:v['capabilities'][2].update(state='unavailable'),
            lambda v:v['roles'][0].update(state='degraded'),
            lambda v:v['roles'][-1].update(state='idle',reason='current'))
        for number,mutate in enumerate(mutations):
            invalid=copy.deepcopy(value);mutate(invalid)
            with self.subTest(mutation=number),self.assertRaises(AssertionError):
                index.live_roles(invalid,state,123456000,123457000,index_unavailable=True)
        first=dict(before_ns=1_000_000_000,after_ns=1_000_100_000)
        last=dict(before_ns=3_000_000_000,after_ns=3_000_100_000)
        aged=copy.deepcopy(value);aged['capabilities'][-1]['time']['age_us']='2001000'
        outage.retained_age(value,aged,first,last,capability='index_publication')
        with self.assertRaises(AssertionError):outage.retained_age(value,value,first,last,capability='index_publication')
        value['capabilities'][-1].update(freshness='stale',time=dict(observed_at_us='123456789',age_us='30000000'))
        index.live_roles(value,state,123456000,123457000,index_unavailable=True)

    def test_exact_pending_plan_and_prefix_physical_bytes_are_independent(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);state,_,_,rows,science=restart_fixture(root)
            physical={name:[row for row in values if row['source'][3]<=3] for name,values in rows.items()}
            result=check.publication(state,physical,science,schema,root)
            self.assertEqual((result['index_lag_records'],result['prepared_chunks'],result['physical_rows']),(9,10,3))
            for mutate in (lambda v:v['dictionaries'].pop(),lambda v:v['dictionaries'].append(rows['dictionaries'][3]),
                           lambda v:v['dictionaries'][0].update(payload_digest='00'*32)):
                invalid=copy.deepcopy(physical);mutate(invalid)
                with self.assertRaises(AssertionError):check.publication(state,invalid,science,schema,root)
            for table in ('projection_plans','projection_chunks','projection_plan_chunks','projection_chunk_bounds'):
                invalid=copy.deepcopy(state);invalid[table].pop()
                with self.subTest(table=table),self.assertRaises((AssertionError,ValueError)):
                    check.publication(invalid,physical,science,schema,root)

    def test_all49_native_bytes_reject_missing_extra_duplicate_or_corrupt_rows(self):
        science=fixture()[3].first
        dump=dict(event='dump-state',sequence=61,rows=[dict(cf=cf,key=k.hex(),value=v.hex()) for cf,k,v in science['native']])
        check.native_rows(dump,science)
        for mutate in (lambda d:d['rows'].pop(),lambda d:d['rows'].append(copy.deepcopy(d['rows'][0])),
            lambda d:d['rows'][0].update(value='00'),lambda d:d.update(sequence=True),lambda d:d.update(sequence=128)):
            invalid=copy.deepcopy(dump);mutate(invalid)
            with self.assertRaises(AssertionError):check.native_rows(invalid,science)

    def test_stop_requires_owned_running_identity_before_mutation_and_terminal_proof_after(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime=object.__new__(case.IndexOutageRuntime);runtime.ch_entry={'id':'owned'}
            runtime.resources={};runtime.record_resources=Mock();runtime.docker=Mock()
            runtime.inspect=Mock(return_value=dict(Id='owned',State=dict(Running=True,Paused=False)))
            runtime.stopped_backend=Mock(return_value={'id':'owned','state':'stopped'})
            runtime.stop_backend(Path(temporary))
            runtime.docker.assert_called_once_with('stop','--time','5','owned',timeout=10)
            runtime.stopped_backend.assert_called_once()
            self.assertEqual(runtime.resources['index_backend_stop']['status'],'stopped-retained')
            for failure in (ValueError('foreign identity'),ValueError('ambiguous mount')):
                runtime.docker.reset_mock();runtime.inspect.side_effect=failure
                with self.assertRaises(ValueError):runtime.stop_backend(Path(temporary))
                runtime.docker.assert_not_called()
            runtime.inspect.side_effect=None;runtime.inspect.return_value['State']['Running']=False
            runtime.docker.reset_mock()
            with self.assertRaises(ValueError):runtime.stop_backend(Path(temporary))
            runtime.docker.assert_not_called()

    def test_stopped_backend_rejects_live_pid_oom_or_any_open_endpoint(self):
        runtime=object.__new__(case.IndexOutageRuntime);runtime.ch_entry={'id':'owned'}
        info=dict(Id='owned',State=dict(Running=False,Paused=False,Pid=0,ExitCode=0,OOMKilled=False))
        runtime.inspect=Mock(return_value=info)
        connection=Mock();connection.__enter__=Mock(return_value=connection);connection.__exit__=Mock()
        connection.connect_ex.return_value=errno.ECONNREFUSED
        with patch.object(case.socket,'socket',return_value=connection):
            self.assertEqual(runtime.stopped_backend()['refusal_errno'],{'18123':errno.ECONNREFUSED,'19000':errno.ECONNREFUSED})
            connection.connect_ex.return_value=0
            with self.assertRaises(ValueError):runtime.stopped_backend()
            connection.connect_ex.return_value=errno.ECONNREFUSED
            for field,value in (('Running',True),('Paused',True),('Pid',123),('ExitCode',1),('OOMKilled',True)):
                runtime.inspect.return_value=copy.deepcopy(info);runtime.inspect.return_value['State'][field]=value
                with self.subTest(field=field),self.assertRaises(ValueError):runtime.stopped_backend()

    def test_native_census_refuses_prejoin_before_creating_observer(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);(root/'initial').mkdir();base.save(root/'initial/terminal-container.json',dict(id='writer'))
            runtime=Mock(root=root);runtime.docker.return_value.stdout=json.dumps([dict(State=dict(Running=True,Pid=123,ExitCode=0,OOMKilled=False))])
            with self.assertRaisesRegex(ValueError,'positively joined'):native.census(runtime,root)
            runtime.docker.assert_called_once_with('inspect','writer')
            runtime.proxy_process.assert_not_called()

    def test_dispatch_uses_only_eight_ordinary_stages_and_preserves_failure(self):
        for failing in (False,True):
            with tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);args=arguments(root);args.case='p01-first-launch-index-outage';checks=[]
                with patch.object(case,'IndexOutageRuntime') as runtime:
                    if failing:runtime.return_value.verify_index.side_effect=AssertionError('original outage proof')
                    base.first_launch(args,checks,{})
                    runtime.return_value.index_restart_baseline.assert_not_called()
                    runtime.return_value.index_catch_up.assert_not_called()
                    runtime.return_value.cleanup.assert_called_once()
                    if failing:runtime.return_value.ordinary.assert_not_called()
                    else:runtime.return_value.ordinary.assert_called_once()
                self.assertEqual(len(checks),8)
                if failing:
                    self.assertEqual(base.load(root/'failure.json')['reason'],'original outage proof')
                    self.assertEqual(checks[-2]['result'],'NOT_RUN')
                self.assertIs(case.IndexOutageRuntime.ordinary_client,initial.IndexRuntime.baseline_client)

    def test_native_cleanup_failure_does_not_skip_owned_application_cleanup(self):
        runtime=object.__new__(case.IndexOutageRuntime);runtime.resources=dict(cleanup_errors=[])
        with patch.object(native,'stop',side_effect=ValueError('original observer cleanup')),patch.object(case.RestartIndexRuntime,'cleanup') as cleanup:
            runtime.cleanup()
        cleanup.assert_called_once()
        self.assertEqual(runtime.resources['cleanup_errors'],[dict(resource='native-observer',error='ValueError')])

    def test_cli_accepts_observer_for_outage_and_missing_candidate_starts_no_resources(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            result=subprocess.run([sys.executable,'-B',str(ROOT/'tests/acceptance/run.py'),
                'p01-first-launch-index-outage','--source-root',str(ROOT),'--candidate-manifest',str(root/'missing'),
                '--native-observer-build',str(root/'observer'),'--evidence',str(root/'evidence')],capture_output=True,text=True,timeout=5)
            self.assertEqual(result.returncode,2,result.stderr)
            report=base.load(root/'evidence/result.json')
            self.assertEqual(report['counts'],dict(collected=8,executed=1,failed=0,skipped=0,not_run=7))
            self.assertFalse(list((root/'evidence/commands').iterdir()))


if __name__=='__main__':unittest.main()
