"""Literal timeline and refusal controls; not installed/engine qualification."""
import copy
import os
import subprocess
import sys
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import tomllib
import unittest

from cases import first_launch_index_pin as case
from test_first_launch_index import set_value


def timeline():
    dataset,catalog,lease='\\x'+'22'*16,'\\x'+'55'*16,'\\x'+'66'*16
    parent=dict(pid=100,ppid=90,start_ticks=200)
    worker=dict(pid=101,ppid=100,start_ticks=250,sha256='ab'*32)
    owner=dict(dataset_id=dataset,resource_id='\\x'+'77'*16,epoch=2,role_instance_id='\\x'+'88'*16,
        process_incarnation='\\x'+'99'*16,control_backend_pid=301,control_backend_start='2026-10-27T00:00:00+00:00',
        acquired_at_us=90_000_000,lease_expires_at_us=103_000_000,state='active')
    original=dict(lease_id=lease,dataset_id=dataset,consumer_id=catalog,frontier_revision=4,topology_generation=0,
        holder_kind='role_instance',holder_job_id=None,holder_role_instance_id=owner['role_instance_id'],
        holder_role='control',holder_incarnation=owner['process_incarnation'],acquired_at_us=100_100_000,
        updated_at_us=100_100_000,expires_at_us=103_000_000,archive_pin_protocol=1,state='active',
        lease_revision=1,released_at_us=None,release_reason=None)
    pin=dict(lease_id=lease,dataset_id=dataset,consumer_id=catalog,frontier_revision=4,
        issuer_resource=owner['resource_id'],issuer_epoch=2,issuer_incarnation=owner['process_incarnation'],
        issuer_role='control',purpose='reader',requested_ttl_us=29_500_000)
    claim=dict(claim_id='\\x'+'aa'*16,lease_id=lease,dataset_id=dataset,topology_generation=0,shard_id=0,
        owner_epoch=1,first_sequence=1,last_sequence=12,purpose='reader',state='active',
        released_at_us=None,release_reason=None)
    reader=dict(pid=302,backend_start='2026-10-27T00:00:01+00:00',backend_start_us=100_300_000,
        usename='bigbrotr_archive_read',datname='bigbrotr_catalog',application_name='bigbrotr-archive-cut',
        wait_event_type='Lock',blocking_pids=[303],waiting_on_relation=True)
    blocker=dict(pid=303,backend_start='2026-10-27T00:00:02+00:00',relation=900)
    samples=[]
    for now in (100_500_000,102_100_000,103_100_000):
        samples.append(dict(now_us=now,started_ns=(now-90_000_000)*1000-50_000_000,
            finished_ns=(now-90_000_000)*1000+50_000_000,owner=copy.deepcopy(owner),
            control_backend=dict(pid=301,backend_start=owner['control_backend_start']),
            parent=parent.copy(),worker=worker.copy(),lease=copy.deepcopy(original),pin=pin.copy(),
            claims=[claim.copy()],reader=reader.copy()))
    for sample in samples: sample['blocker']=blocker.copy()
    samples[1]['reader'].update(wait_event_type='Lock',blocking_pids=[303])
    samples[1]['owner']['lease_expires_at_us']=104_000_000
    samples[2]['worker']=None
    samples[2]['owner']['lease_expires_at_us']=106_050_000
    samples[2]['lease'].update(state='released',lease_revision=2,released_at_us=102_990_000,
        updated_at_us=102_990_000,release_reason='reader_joined')
    samples[2]['claims'][0].update(state='released',released_at_us=102_990_000,release_reason='reader_joined')
    # A read-only PG backend can linger at the lock after positive local reap.
    samples[2]['reader'].update(wait_event_type='Lock',blocking_pids=[303])
    roles=[dict(observed_at_us=str(now),capabilities=[dict(state='unavailable',reason='dependency_unavailable')]*2)
           for now in (103_150_000,103_200_000)]
    gate_reader=dict(reader,pid=305,backend_start='2026-10-27T00:00:00.9+00:00',backend_start_us=100_150_000,
        wait_event_type='Client',blocking_pids=[],waiting_on_relation=False)
    gate=copy.deepcopy(samples[0]);gate.update(now_us=100_250_000,started_ns=10_200_000_000,
        finished_ns=10_300_000_000,reader=gate_reader.copy(),blocker=None)
    return dict(samples=samples,gate_sample=gate,gate_reader=gate_reader,discovery_observations=1,
        arm_clock_us=100_000_000,baseline_owner=owner,parent=parent,worker=worker,reader=reader,blocker=blocker,
        post_expiry_roles=roles,post_clock_us=103_300_000,gate_terminal=[],reader_terminal=[],blocker_terminal=[],phase_started_ns=1_000_000_000)


class ShortPinControls(unittest.TestCase):
    @unittest.skipUnless(sys.platform == 'linux', 'actual process custody observation requires Linux /proc')
    def test_owned_process_identity_survives_poll_and_disappears_only_after_join(self):
        process=subprocess.Popen([sys.executable,'-c','import time; time.sleep(5)'],
            stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            first=case.process_identity(process.pid)
            self.assertEqual(first['ppid'],os.getpid())
            self.assertGreater(first['start_ticks'],0)
            self.assertEqual(case.process_identity(process.pid),first)
        finally:
            process.terminate()
            process.wait(timeout=2)
        self.assertIsNone(case.process_identity(process.pid))

    def test_literal_shorter_pin_and_same_authority_renewal_is_accepted(self):
        result=case.verify_schedule(timeline())
        self.assertEqual(result['status'],'PASS')
        self.assertEqual(result['original_pin_lifetime_us'],2_900_000)
        self.assertEqual(result['blocked_observations'],2)
        self.assertEqual(result['unknown_cleanup'],'NOT_RUN')

    def test_precheck_gate_and_actual_waiter_are_distinct_and_require_relation_proof(self):
        observed=timeline()
        self.assertNotEqual(observed['gate_reader']['pid'],observed['reader']['pid'])
        self.assertEqual(case.verify_schedule(observed)['status'],'PASS')
        self.assertIsNone(case.select_waiter([],observed['blocker'],100_100_000))
        self.assertEqual(case.select_waiter([observed['reader']],observed['blocker'],100_100_000),observed['reader'])
        mutations=(dict(pid=True),dict(usename='other'),dict(datname='other'),dict(application_name='other'),
            dict(backend_start_us=100_099_999),dict(wait_event_type='Client'),dict(blocking_pids=[]),
            dict(waiting_on_relation=False),dict(waiting_on_relation=1))
        for change in mutations:
            with self.subTest(change=change),self.assertRaises(ValueError):
                case.select_waiter([dict(observed['reader'],**change)],observed['blocker'],100_100_000)
        with self.assertRaises(ValueError):
            case.select_waiter([observed['reader'],dict(observed['reader'],pid=306)],observed['blocker'],100_100_000)
        for change in (
            lambda e:e['samples'][0].update(reader=e['gate_reader']),
            lambda e:e['samples'][1]['reader'].update(pid=306),
            lambda e:e['samples'][1]['reader'].update(backend_start='reused'),
            lambda e:e['samples'][1]['reader'].update(backend_start_us=100_400_000),
            lambda e:e['samples'][1]['reader'].update(waiting_on_relation=False),
            lambda e:e['samples'][0]['reader'].update(backend_start_us=101_000_000),
            lambda e:e['gate_sample'].update(worker=None),
            lambda e:e['samples'][0].update(worker=None),
            lambda e:e['worker'].update(ppid=999),
            lambda e:e['gate_sample']['reader'].update(pid=306),
            lambda e:e.update(gate_terminal=[e['gate_reader']]),
            lambda e:e.update(discovery_observations=94),
        ):
            changed=copy.deepcopy(observed);change(changed)
            with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                case.verify_schedule(changed)

    def test_discovery_refuses_reused_blocker_before_binding_reader(self):
        evidence=timeline()
        runtime=case.ShortPinRuntime.__new__(case.ShortPinRuntime)
        value=dict(blocker=dict(evidence['blocker'],backend_start='reused'),readers=[evidence['reader']])
        with patch.object(runtime,'bounded_sql',return_value=value),patch.object(runtime,'worker') as worker:
            with self.assertRaises(AssertionError):
                runtime.sample(evidence['gate_sample']['lease']['lease_id'],None,evidence['parent'],evidence['worker'],
                    999999999,evidence['blocker'],acquired=100_100_000)
            worker.assert_not_called()

    def test_deadline_and_lifecycle_counterexamples_refuse(self):
        changes = (
            lambda e:e['samples'].pop(1),
            lambda e:e.update(arm_clock_us=100_200_000),
            lambda e:e['samples'][1]['reader'].update(blocking_pids=[]),
            lambda e:e['samples'][1]['blocker'].update(backend_start='reused'),
            lambda e:e['baseline_owner'].update(epoch=1),
            lambda e:e['samples'][1]['reader'].update(application_name='bigbrotr-archive-event'),
            lambda e:e['samples'][1]['reader'].update(backend_start='reused'),
            lambda e:e['samples'][1]['worker'].update(start_ticks=251),
            lambda e:e['samples'][-1].update(worker=e['worker']),
            lambda e:e['samples'][-1]['parent'].update(start_ticks=201),
            lambda e:e['samples'][-1]['owner'].update(epoch=3),
            lambda e:e['samples'][-1]['owner'].update(control_backend_pid=304),
            lambda e:e['samples'][-1]['control_backend'].update(backend_start='reused'),
            lambda e:e['samples'][-1]['owner'].update(lease_expires_at_us=103_200_000),
            lambda e:e['samples'][-1]['owner'].update(lease_expires_at_us=105_900_000),
            lambda e:e['samples'][-1]['owner'].update(lease_expires_at_us=109_000_000),
            lambda e:e['samples'][-1]['lease'].update(expires_at_us=106_000_000),
            lambda e:e['samples'][-1]['lease'].update(lease_revision=3),
            lambda e:e['samples'][-1]['lease'].update(release_reason='old_reader_excluded'),
            lambda e:e['samples'][-1]['lease'].update(released_at_us=104_500_000,updated_at_us=104_500_000),
            lambda e:e['samples'][-1]['lease'].update(released_at_us=102_000_000,updated_at_us=102_000_000),
            lambda e:e['samples'][1].update(worker=None),
            lambda e:e['samples'][1].update(now_us=101_000_000,started_ns=10_950_000_000,finished_ns=11_050_000_000),
            lambda e:e['samples'][-1]['claims'][0].update(state='active'),
            lambda e:e['samples'][0]['claims'][0].update(last_sequence=11),
            lambda e:e['samples'][0]['pin'].update(requested_ttl_us=2_000_000),
            lambda e:e['samples'][0]['pin'].update(purpose='export'),
            lambda e:e['samples'][0]['pin'].update(issuer_incarnation='foreign'),
            lambda e:e['samples'][0]['lease'].update(holder_incarnation='foreign'),
            lambda e:e['samples'][-1].update(now_us=104_300_000),
            lambda e:e['samples'][-1].update(started_ns=20_000_000_000,finished_ns=20_100_000_000),
            lambda e:e['post_expiry_roles'][0]['capabilities'][0].update(state='ready',reason='current'),
            lambda e:e['post_expiry_roles'][0].update(observed_at_us='102999999'),
            lambda e:e['post_expiry_roles'][0].update(observed_at_us='103300001'),
            lambda e:e['post_expiry_roles'][0].update(capabilities=[]),
            lambda e:e.update(reader_terminal=[e['reader']]),
            lambda e:e.update(blocker_terminal=[e['blocker']]),
        )
        for i,change in enumerate(changes):
            observed=copy.deepcopy(timeline());change(observed)
            with self.subTest(counterexample=i),self.assertRaises((AssertionError,ValueError)):
                case.verify_schedule(observed)

    def test_integer_aliases_do_not_impersonate_pin_or_authority_fields(self):
        for row,key in (('owner','epoch'),('lease','lease_revision'),('lease','expires_at_us'),('pin','requested_ttl_us')):
            for replacement in (True,1.0):
                observed=timeline();observed['samples'][0][row][key]=replacement
                with self.subTest(row=row,key=key,replacement=replacement),self.assertRaises(ValueError):
                    case.verify_schedule(observed)

    def test_closed_profile_changes_only_declared_deployment_fields(self):
        original='''[postgres.catalog]\nendpoint="127.0.0.1:15432"\n[ runtime ]\n'''.replace('[ runtime ]','[runtime]')
        original+='lease_ttl="60s"\nheartbeat_interval="10s"\nrenewal_timeout="5s"\nrenewal_safety_margin="5s"\nauthorization_ttl="5s"\ngraceful_shutdown_timeout="30s"\n[sync]\nprovenance="local_first_witness"\n'
        original=original.replace('=',' = ')
        configured=tomllib.loads(case.profile(original,set_value,12345))
        self.assertEqual(configured['runtime'],dict(lease_ttl='3s',heartbeat_interval='500ms',
            renewal_timeout='500ms',renewal_safety_margin='500ms',authorization_ttl='5s',graceful_shutdown_timeout='30s'))
        self.assertEqual(configured['sync'],dict(provenance='local_first_witness'))
        self.assertEqual(configured['postgres']['catalog']['endpoint'],'127.0.0.1:12345')
        for port in (True,0,65536,'12345'):
            with self.assertRaises(ValueError):case.profile(original,set_value,port)

    def test_new_case_dispatch_keeps_missing_prerequisite_stages_not_run(self):
        from cases import first_launch as base
        runtime=MagicMock()
        runtime.preflight.side_effect=base.MissingPrerequisite('synthetic prerequisite absence')
        checks=[]
        with tempfile.TemporaryDirectory() as directory:
            args=SimpleNamespace(case='p01-first-launch-index-short-pin',evidence=Path(directory))
            with patch.object(case,'ShortPinRuntime',return_value=runtime) as selected, \
                    patch('cases.first_launch_index.IndexRuntime',side_effect=AssertionError('wrong runtime')):
                base.first_launch(args,checks,{})
            selected.assert_called_once()
        self.assertEqual(len(checks),8)
        self.assertEqual([c['result'] for c in checks],['NOT_RUN']*7+['PASS'])
        runtime.ordinary.assert_not_called()
        runtime.cleanup.assert_called_once()
        self.assertEqual(args.case_limits,case.LIMITS)

    def test_closed_phase_inventory_and_original_active_budget_remain_required(self):
        runtime=case.ShortPinRuntime.__new__(case.ShortPinRuntime)
        runtime.pin_verdict=case.verify_schedule(timeline())
        phases=list(case.PHASES)
        timing=dict(mode=case.MODE,original_limit_seconds=40,export_limit_seconds=12,
            overall_limit_seconds=130,original_active_ns=10_000_000_000,
            phases=[dict(phase=p,started_ns=i*2_000_000_000,finished_ns=i*2_000_000_000+1_000_000_000,limit_seconds=30)
                    for i,p in enumerate(phases)])
        self.assertEqual(runtime.ordinary_verdict(phases,timing,None)['status'],'PASS')
        for change in (lambda t:t.update(overall_limit_seconds=190),lambda t:t.update(original_active_ns=40_000_000_001),
                       lambda t:t['phases'].pop(),lambda t:t['phases'][-1].update(phase='restored')):
            changed=copy.deepcopy(timing);change(changed)
            with self.assertRaises(ValueError):runtime.ordinary_verdict(phases,changed,None)
        with self.assertRaises(ValueError):runtime.ordinary_verdict(phases,timing,dict(status='restored'))


if __name__ == '__main__':
    unittest.main()
