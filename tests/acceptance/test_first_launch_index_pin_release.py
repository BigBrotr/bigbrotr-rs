"""Literal COMMIT-side and custody refusal controls; no engine qualification."""
import copy
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from cases import first_launch as launch
from cases import first_launch_index_pin_release as case
from cases import first_launch_index_pin_release_oracle as oracle
from test_first_launch import arguments
from test_first_launch_index_pin import timeline
from test_first_launch_index import ordinary_fixture


def observation(mode):
    source = timeline(); gate = source['gate_sample']
    owner = gate['owner']; owner.update(released_at_us=None,role='control')
    before = {name:[] for name in launch.expected_tables(Path(__file__).resolve().parents[2])}
    before['owner_epochs'] = [dict(owner,xmin='900')]
    # The original export pin remains active. Reader-only selection and exact
    # preservation must not reinterpret it as the new selected probe.
    before['snapshot_leases'] = [dict(lease_id='old-export',state='active')]
    before['archive_snapshot_pins'] = [dict(lease_id='old-export',purpose='export')]
    before['retention_claims'] = [dict(lease_id='old-export',purpose='export',state='active')]
    after = copy.deepcopy(before)
    after['snapshot_leases'].append(dict(gate['lease'],xmin='1000'))
    after['archive_snapshot_pins'].append(dict(gate['pin'],xmin='1000'))
    after['retention_claims'] += [dict(gate['claims'][0],xmin='1000')]
    for table in oracle.PIN_TABLES:
        before[table] = copy.deepcopy(after[table])
    versions = dict(lease=copy.deepcopy(gate['lease']),claims=copy.deepcopy(gate['claims']),lease_xmin='1000',claim_xmins=['1000'],
        pin=copy.deepcopy(gate['pin']),pin_xmin='1000')
    final = copy.deepcopy(versions)
    if mode == 'after-complete':
        after['snapshot_leases'][-1].update(state='released',lease_revision=2,updated_at_us=100_500_000,
            released_at_us=100_500_000,release_reason='reader_joined',xmin='1001')
        after['retention_claims'][-1].update(state='released',released_at_us=100_499_999,release_reason='reader_joined',xmin='1001')
        final.update(lease={k:v for k,v in after['snapshot_leases'][-1].items() if k != 'xmin'},
            claims=[{k:v for k,v in after['retention_claims'][-1].items() if k != 'xmin'}],
            lease_xmin='1001',claim_xmins=['1001'])
    event = dict(backend_pid=owner['control_backend_pid'],hold_pin_release=True,target='control-pin-release',mode=mode)
    evidence = dict(mode=mode,gate=gate,baseline_owner=dict(owner,xmin='900'),parent=source['parent'],worker=source['worker'],
        arm_clock_us=100_000_000,release_gate_clock_us=100_300_000,
        setup_started_ns=500_000_000,setup_finished_ns=10_350_000_000,
        custody=dict(identity={'inode':123},busy=True),
        held=dict(started_ns=10_400_000_000,finished_ns=10_401_000_000,parent=source['parent'],worker=None,
            custody=dict(identity={'inode':123},busy=True),control_backend_pid=owner['control_backend_pid']),
        terminal_custody=dict(identity={'inode':123},busy=False),allow_ns=10_402_000_000,
        started_ns=1_000_000_000,finished_ns=11_000_000_000,
        owner_backend=dict(pid=owner['control_backend_pid'],backend_start=owner['control_backend_start'],usename='bigbrotr_control',datname='bigbrotr_control'),
        update=dict(event,data_queries=1,data_commits=0,server_complete=0,client_complete=0,faults=0),
        hold_event=dict(event,stage='pin-release-update',data_queries=1,data_commits=0,server_complete=0,client_complete=0,faults=0),
        fault=dict(event,data_queries=1,data_commits=1,server_complete=int(mode == 'after-complete'),client_complete=0,faults=1),
        terminal=dict(exit_code=4,parent=None,worker=None,backend_count=0,http='connection-refused',
            commit_unknown=True,owner_release_claimed=False),versions=dict(before=versions,after=final))
    return evidence,before,after


def setup_observation():
    """Completed startup fixture also used by the unchanged baseline controls."""
    evidence,before,after = observation('after-complete')
    for table in oracle.PIN_TABLES:
        before[table] = [copy.deepcopy(after[table][0])]
    owner = copy.deepcopy(evidence['baseline_owner']); principal='\\x'+'ee'*16
    previous = dict(owner,epoch=1,state='released',released_at_us=90_000_000,role_instance_id='old-role')
    before['owner_epochs']=[previous]
    before['role_instances']=[dict(role_instance_id='old-role',role='control',dataset_id=owner['dataset_id'],retired_at_us=90_000_000)]
    before['browser_sessions']=[dict(session_id='old-session',principal_id=principal,revoked_at_us=90_000_000)]
    before['management_audit']=[dict(audit_id='old-audit')]
    before['login_buckets']=[dict(dataset_id=owner['dataset_id'],kind=kind,bucket=pair[0],attempts=1,window_started_at_us=70_000_000)
                             for kind,pair in oracle.login_buckets().items()]
    after['owner_epochs']=[copy.deepcopy(previous),owner]
    after['role_instances']=copy.deepcopy(before['role_instances'])+[
        dict(role_instance_id=owner['role_instance_id'],role='control',dataset_id=owner['dataset_id'],retired_at_us=None)]
    after['browser_sessions']=[dict(session_id='fresh-session',principal_id=principal,revoked_at_us=None,
        issued_at_us=90_100_000,last_seen_at_us=90_100_000)]
    after['management_audit']=copy.deepcopy(before['management_audit'])+[dict(action='browser.session.issue',outcome='accepted',
        actor_id=principal,subject_id='fresh-session',dataset_id=owner['dataset_id'],config_revision=1,policy_revision=1)]
    after['login_buckets']=[dict(row,attempts=2) for row in before['login_buckets']]
    return before,after,principal,90_000_000,101_000_000


def first_probe_setup():
    before,after,principal,start,end = setup_observation()
    _,active,_ = observation('before-forward')
    for table in oracle.PIN_TABLES:
        after[table] = copy.deepcopy(active[table])
    science,roles = ordinary_fixture()
    for name in ('datasets','consumers','archive_frontier_snapshots','consumer_frontiers','archive_frontier_batches','archive_attempts'):
        before[name] = copy.deepcopy(science[name]); after[name] = copy.deepcopy(science[name])
    roles['identity']['process_incarnation'] = after['owner_epochs'][-1]['process_incarnation'][2:]
    roles['observed_at_us'] = '100900000'
    for cap in roles['capabilities']:
        cap.update(state='building',reason='verification_in_flight',freshness='unknown',time=None,published=None)
    return before,after,principal,start,end,roles


class PinReleaseControls(unittest.TestCase):
    def test_both_cli_cases_retain_not_run_without_candidate_or_engines(self):
        source=Path(__file__).resolve().parents[2]
        for name in case.CASES:
            with tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary)
                result=subprocess.run([sys.executable,'-B',str(source/'tests/acceptance/run.py'),name,
                    '--source-root',str(source),'--candidate-manifest',str(root/'absent.json'),
                    '--evidence',str(root/'evidence')],capture_output=True,text=True,timeout=5)
                report=json.loads((root/'evidence/result.json').read_text())
                self.assertEqual((result.returncode,report['result'],len(report['checks'])),(2,'NOT_RUN',9))
                self.assertFalse(list((root/'evidence/commands').iterdir()))

    def test_distinct_setup_preserves_history_and_requires_exactly_first_active_probe(self):
        before,after,principal,start,end,_=first_probe_setup()
        self.assertEqual(oracle.setup(before,after,principal,start,end)['epoch'],2)
        for change in (
            lambda b,a:a['owner_epochs'][0].update(released_at_us=99_000_000),
            lambda b,a:a['owner_epochs'][-1].update(epoch=1),
            lambda b,a:a['role_instances'][0].update(retired_at_us=None),
            lambda b,a:a['browser_sessions'][0].update(last_seen_at_us=90_100_001),
            lambda b,a:a['management_audit'][-1].update(action='unexpected.write'),
            lambda b,a:a['login_buckets'][0].update(attempts=3),
            lambda b,a:a['snapshot_leases'][-1].update(state='released'),
            lambda b,a:a['snapshot_leases'][-1].update(lease_revision=2),
            lambda b,a:a['snapshot_leases'][-1].update(expires_at_us=end),
            lambda b,a:a['archive_snapshot_pins'][-1].update(xmin='999'),
            lambda b,a:a['archive_snapshot_pins'][-1].update(purpose='export'),
            lambda b,a:[a[t][-1].update(consumer_id='foreign') for t in ('snapshot_leases','archive_snapshot_pins')],
            lambda b,a:a['snapshot_leases'].append(dict(a['snapshot_leases'][-1],lease_id='second')),
            lambda b,a:a['retention_claims'][-1].update(last_sequence=11),
            lambda b,a:a['retention_claims'].append(dict(lease_id='unbound',purpose='reader')),
            lambda b,a:a['jobs'].append(dict(kind='export',state='active')),
        ):
            before,after,principal,start,end,_=first_probe_setup(); change(before,after)
            with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                oracle.setup(before,after,principal,start,end)

    def test_first_probe_status_requires_actual_current_selection_and_empty_cache_in_flight(self):
        _,after,_,start,end,roles=first_probe_setup()
        oracle.setup_roles(roles,after,start,end)
        for change in (
            lambda a,r:r['capabilities'][0].update(reason='not_observed'),
            lambda a,r:r['capabilities'][0].update(state='ready',reason='current'),
            lambda a,r:r['capabilities'][0].update(state='unavailable',reason='dependency_unavailable'),
            lambda a,r:r['identity'].update(process_incarnation='00'*16),
            lambda a,r:a['archive_frontier_snapshots'][0].update(snapshot_id=4),
            lambda a,r:a['consumer_frontiers'][0].update(last_complete_sequence=11),
        ):
            _,after,_,start,end,roles=first_probe_setup(); change(after,roles)
            with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                oracle.setup_roles(roles,after,start,end)

    def test_real_commit_sides_keep_distinct_sql_and_same_unknown_product_outcome(self):
        for mode,state in (('before-forward','active'),('after-complete','released')):
            with self.subTest(mode=mode):
                result = oracle.verify(*observation(mode))
                self.assertEqual((result['status'],result['product'],result['independently_observed_pin']),('PASS','CommitUnknown',state))
                self.assertEqual(result['unknown_native_termination'],'NOT_RUN')
                observed=observation(mode)
                observed[2]['owner_epochs'][0].update(lease_expires_at_us=104_000_000,xmin='901')
                self.assertEqual(oracle.verify(*observed)['status'],'PASS')

    def test_no_fault_or_timeout_or_successful_release_can_substitute_for_commit_unknown(self):
        changes = (
            lambda e,b,a:e['fault'].update(data_commits=0),
            lambda e,b,a:e['fault'].update(server_complete=1),
            lambda e,b,a:e['fault'].update(client_complete=1),
            lambda e,b,a:e['fault'].update(backend_pid=999),
            lambda e,b,a:e['fault'].update(hold_pin_release=False),
            lambda e,b,a:e['hold_event'].update(data_commits=1),
            lambda e,b,a:e['terminal'].update(exit_code=0),
            lambda e,b,a:e['terminal'].update(commit_unknown=False),
            lambda e,b,a:e['terminal'].update(owner_release_claimed=True),
            lambda e,b,a:e['terminal'].update(backend_count=1),
            lambda e,b,a:e['terminal'].update(http='ready'),
            lambda e,b,a:e.update(allow_ns=e['held']['started_ns']+500_000_000),
            lambda e,b,a:e.update(finished_ns=e['started_ns']+30_000_000_000),
            lambda e,b,a:e.update(setup_finished_ns=e['setup_started_ns']+30_000_000_000),
            lambda e,b,a:e.update(setup_finished_ns=e['held']['started_ns']+1),
            lambda e,b,a:e.update(release_gate_clock_us=e['gate']['lease']['expires_at_us']),
        )
        for change in changes:
            observed = observation('before-forward'); change(*observed)
            with self.subTest(change=change),self.assertRaises((ValueError,AssertionError)):
                oracle.verify(*observed)

    def test_native_and_pin_attribution_and_custody_are_independent_requirements(self):
        changes = (
            lambda e,b,a:e['held'].update(worker=e['worker']),
            lambda e,b,a:e['held']['custody'].update(busy=False),
            lambda e,b,a:e['held']['custody']['identity'].update(inode=124),
            lambda e,b,a:e['terminal_custody'].update(busy=True),
            lambda e,b,a:e['gate'].update(worker=None),
            lambda e,b,a:e['worker'].update(ppid=999),
            lambda e,b,a:e['gate']['reader'].update(backend_start_us=e['gate']['lease']['acquired_at_us']-1),
            lambda e,b,a:e['gate']['pin'].update(purpose='export'),
            lambda e,b,a:e['gate']['pin'].update(issuer_epoch=999),
            lambda e,b,a:e['gate']['lease'].update(holder_role_instance_id='foreign'),
            lambda e,b,a:e['held'].update(control_backend_pid=999),
            lambda e,b,a:e['gate']['control_backend'].update(backend_start='reused'),
            lambda e,b,a:e['owner_backend'].update(usename='wrong'),
            lambda e,b,a:e['gate']['claims'][0].update(last_sequence=11),
            lambda e,b,a:b['snapshot_leases'].pop(),
            lambda e,b,a:b['snapshot_leases'][-1].update(xmin='999'),
            lambda e,b,a:b['archive_snapshot_pins'][-1].update(issuer_epoch=1),
            lambda e,b,a:b['retention_claims'][-1].update(last_sequence=11),
        )
        for change in changes:
            observed = observation('before-forward'); change(*observed)
            with self.subTest(change=change),self.assertRaises((ValueError,AssertionError)):
                oracle.verify(*observed)

    def test_raw_rows_versions_and_every_earlier_table_are_preserved(self):
        changes = (
            lambda e,b,a:e['versions']['after'].update(lease_xmin='2000'),
            lambda e,b,a:a['archive_snapshot_pins'][-1].update(xmin='2000'),
            lambda e,b,a:a['owner_epochs'][0].update(xmin='2000'),
            lambda e,b,a:a['snapshot_leases'][0].update(state='released'),
            lambda e,b,a:a['retention_claims'][0].update(state='released'),
            lambda e,b,a:a['owner_epochs'][0].update(state='released',released_at_us=100_600_000),
            lambda e,b,a:a['owner_epochs'][0].update(control_backend_pid=999),
            lambda e,b,a:a['owner_epochs'].append(dict(a['owner_epochs'][0],epoch=3)),
            lambda e,b,a:a['archive_snapshot_pins'][-1].update(frontier_revision=5),
            lambda e,b,a:a['browser_sessions'].append(dict(session_id='new-work')),
            lambda e,b,a:a['snapshot_leases'][-1].update(updated_at_us=100_400_000),
        )
        for change in changes:
            observed = observation('before-forward'); change(*observed)
            with self.subTest(change=change),self.assertRaises((ValueError,AssertionError)):
                oracle.verify(*observed)
        for change in (
            lambda e,b,a:e['versions']['after'].update(claim_xmins=['1002']),
            lambda e,b,a:e['versions']['after'].update(lease_xmin='1000',claim_xmins=['1000']),
            lambda e,b,a:a['snapshot_leases'][-1].update(lease_revision=3),
            lambda e,b,a:a['retention_claims'][-1].update(state='active'),
        ):
            observed = observation('after-complete'); change(*observed)
            with self.subTest(change=change),self.assertRaises((ValueError,AssertionError)):
                oracle.verify(*observed)

    def test_actual_flock_is_busy_until_its_owned_holder_releases_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'custody.lock'; path.touch(mode=0o600)
            fd = os.open(path,os.O_RDONLY)
            try:
                fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                held = case.custody(path)
                self.assertTrue(held['busy'])
                fcntl.flock(fd,fcntl.LOCK_UN)
                self.assertFalse(case.custody(path,held['identity'])['busy'])
                path.unlink(); path.touch(mode=0o600)
                with self.assertRaises(AssertionError):
                    case.custody(path,held['identity'])
            finally:
                os.close(fd)

    def test_dispatch_preserves_original_journey_and_failed_fault_stage(self):
        for name in case.CASES:
            with tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary); args=arguments(root); args.case=name
                checks=[]
                with patch.object(case,'PinReleaseRuntime') as runtime:
                    runtime.return_value.pin_release_uncertainty.side_effect=ValueError('unknown retained')
                    launch.first_launch(args,checks,{})
                    runtime.return_value.ordinary.assert_called_once()
                    runtime.return_value.cleanup.assert_called_once()
                outcomes={row['name']:row['result'] for row in checks}
                self.assertEqual(outcomes['first_launch_ordinary_history_export'],'PASS')
                self.assertEqual(outcomes['first_launch_pin_release_uncertainty'],'FAIL')
                self.assertEqual(outcomes['first_launch_cleanup'],'PASS')
                self.assertEqual(len(outcomes),9)


if __name__ == '__main__':
    unittest.main()
