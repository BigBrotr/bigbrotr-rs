"""Closed startup-quiescence and unchanged-deadline controls; no engines."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from cases import first_launch_index as index
from cases import first_launch_index_oracle as oracle
from cases import first_launch_index_baseline as baseline
from cases import first_launch_restart as restart
from cases.first_launch_index_pin import ShortPinRuntime, process_identity
from cases.first_launch_index_pin_release import PinReleaseRuntime
from cases.first_launch_index_restart import RestartIndexRuntime
from test_first_launch_index import ordinary_fixture
from test_first_launch_index_pin_release import setup_observation
import installed_archive_export_client as client


def startup_observation():
    before,after,_,start,end = setup_observation()
    science,roles = ordinary_fixture()
    for name in ('datasets','consumers','archive_frontier_snapshots','consumer_frontiers','archive_frontier_batches','archive_attempts'):
        before[name]=copy.deepcopy(science[name]); after[name]=copy.deepcopy(science[name])
    owner=after['owner_epochs'][-1]
    roles['identity']['process_incarnation']=owner['process_incarnation'][2:]
    roles['observed_at_us']='100900000'
    for cap in roles['capabilities']:
        cap.update(state='building',reason='not_observed',freshness='unknown',time=None,published=None)
    return before,after,roles,start,end,None


def successor_observation(provenance=True):
    before,after,roles,start,end,worker = startup_observation()
    selected = restart.profile(provenance)
    for state in (before,after):
        catalog = state['consumers'][0]['consumer_id']
        state['consumers'][0].update(current_revision=6,current_topology_generation=1)
        snapshot = state['archive_frontier_snapshots'][0]
        snapshot.update(frontier_revision=6,topology_generation=1,snapshot_id=selected['snapshot'],
            **dict(zip(restart.COUNTS,selected['counts'])))
        bound = state['consumer_frontiers'][0]
        bound.update(frontier_revision=6,topology_generation=1,last_complete_sequence=selected['initial_cut'])
        state['consumer_frontiers'].append(dict(bound,owner_epoch=2,last_complete_sequence=selected['successor_cut']))
        for member in state['archive_frontier_batches']: member['frontier_revision']=6
        for n in range(5,5+selected['successor_requests']):
            attempt = dict(state['archive_attempts'][0],attempt_id='successor-'+str(n),ordinal=n)
            state['archive_attempts'].append(attempt)
            state['archive_frontier_batches'].append(dict(consumer_id=catalog,frontier_revision=6,attempt_id=attempt['attempt_id']))
    after['snapshot_leases'][-1]['frontier_revision']=6
    after['archive_snapshot_pins'][-1]['frontier_revision']=6
    claim = after['retention_claims'][-1]
    claim.update(topology_generation=1,last_sequence=selected['initial_cut'])
    after['retention_claims'].append(dict(claim,owner_epoch=2,last_sequence=selected['successor_cut']))
    return before,after,roles,start,end,worker


class BaselineControls(unittest.TestCase):
    def test_successor_startup_requires_both_exact_claims_and_same_fresh_owner(self):
        for provenance in (True,False):
            values = successor_observation(provenance)
            self.assertFalse(oracle.startup_reader_pending(*values[:2],successor=provenance))
            self.assertEqual(oracle.ordinary_startup(*values,successor=provenance)['epoch'],2)
            changes = (
                lambda a:a['retention_claims'].pop(),
                lambda a:a['retention_claims'][-1].update(owner_epoch=1),
                lambda a:a['retention_claims'][-1].update(last_sequence=1),
                lambda a:a['retention_claims'][-1].update(xmin='other'),
                lambda a:a['retention_claims'][-1].update(state='active'),
                lambda a:a['archive_snapshot_pins'][-1].update(issuer_epoch=1),
                lambda a:a['archive_frontier_snapshots'][0].update(snapshot_id=99),
                lambda a:a['archive_frontier_batches'].pop(),
            )
            for change in changes:
                changed = list(successor_observation(provenance)); change(changed[1])
                with self.subTest(provenance=provenance,change=change),self.assertRaises((AssertionError,ValueError)):
                    oracle.ordinary_startup(*changed,successor=provenance)
            active = copy.deepcopy(values[1])
            active['snapshot_leases'][-1].update(state='active',lease_revision=1,released_at_us=None,release_reason=None)
            for claim in active['retention_claims'][-2:]: claim.update(state='active',released_at_us=None,release_reason=None)
            self.assertTrue(oracle.startup_reader_pending(values[0],active,successor=provenance))
            active['retention_claims'].pop()
            with self.assertRaises(AssertionError): oracle.startup_reader_pending(values[0],active,successor=provenance)

    def test_successor_client_uses_fresh_session_and_original_thirty_second_window(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            (output/'ordinary-setup.json').write_text(json.dumps(dict(phase='setup',started_ns=10_000_000_000,deadline_ns=40_000_000_000)))
            (output/'before-control.json').write_text('{}')
            owner = Mock(source=output,root=output,environment={},processes=[])
            child = Mock(pid=123,returncode=0)
            with patch.object(baseline.subprocess,'Popen',return_value=child) as spawn,patch.object(baseline,'stop_process') as stop,\
                 patch.object(baseline.time,'monotonic',return_value=10),patch.object(baseline,'startup_quiescence',return_value=({'complete':True},)) as observe:
                self.assertEqual(baseline.startup_client(owner,output,{},successor=True),{'complete':True})
            self.assertEqual(spawn.call_args.args[0][-2:],['history',str(output)])
            self.assertEqual(observe.call_args.args[-1],40)
            self.assertEqual(observe.call_args.kwargs,dict(successor=True,session_path=output/'read-session.private'))
            child.wait.assert_called_once_with(timeout=30)
            stop.assert_called_once_with(child)
            self.assertEqual(owner.processes,[])
            self.assertEqual(json.loads((output/'successor-history-client-process.json').read_text())['limit_seconds'],30)

    def test_history_keeps_verified_startup_rows_and_exactly_two_new_readers(self):
        before,startup,*_ = successor_observation()
        after = copy.deepcopy(startup)
        after['owner_epochs'][-1].update(state='released',released_at_us=102_000_000)
        after['browser_sessions'][-1]['revoked_at_us']=102_000_000
        for n in (1,2):
            for table in ('snapshot_leases','archive_snapshot_pins','retention_claims'):
                selected = [r for r in startup[table] if r['lease_id']!='old-export']
                after[table] += [dict(r,lease_id='history-'+str(n)) for r in selected]
        restart.history_unchanged(before,after,startup=startup)
        for change in (
            lambda a:a['archive_snapshot_pins'].append(dict(a['archive_snapshot_pins'][-1],lease_id='unexpected-third')),
            lambda a:a['retention_claims'][1].update(xmin='rewritten-startup'),
            lambda a:a['snapshot_leases'][1].update(state='active'),
        ):
            changed=copy.deepcopy(after);change(changed)
            with self.assertRaises(AssertionError):restart.history_unchanged(before,changed,startup=startup)

    def test_not_observed_needs_independent_completed_reader_and_pending_is_closed(self):
        before,after,*_=startup_observation()
        self.assertFalse(oracle.startup_reader_pending(before,after))
        absent=copy.deepcopy(after)
        for table in ('snapshot_leases','archive_snapshot_pins','retention_claims'):
            absent[table]=copy.deepcopy(before[table])
        self.assertTrue(oracle.startup_reader_pending(before,absent))
        active=copy.deepcopy(after)
        active['snapshot_leases'][-1].update(state='active',lease_revision=1,released_at_us=None,release_reason=None)
        active['retention_claims'][-1].update(state='active',released_at_us=None,release_reason=None)
        self.assertTrue(oracle.startup_reader_pending(before,active))
        for change in (
            lambda a:a['retention_claims'].pop(),
            lambda a:a['retention_claims'][-1].update(state='released'),
            lambda a:a['archive_snapshot_pins'][-1].update(purpose='export'),
            lambda a:a['snapshot_leases'][-1].update(holder_role_instance_id='foreign'),
            lambda a:a['snapshot_leases'][-1].update(lease_revision=2),
        ):
            invalid=copy.deepcopy(active);change(invalid)
            with self.subTest(change=change),self.assertRaises(AssertionError):
                oracle.startup_reader_pending(before,invalid)
        # Completed inconsistent rows are never converted into a retry: the
        # unchanged strict completion oracle still rejects them immediately.
        values=list(startup_observation());values[1]['retention_claims'][-1]['last_sequence']=11
        self.assertFalse(oracle.startup_reader_pending(values[0],values[1]))
        with self.assertRaises(AssertionError):oracle.ordinary_startup(*values)

    def test_fresh_cache_needs_completed_probe_and_never_claims_ready(self):
        self.assertEqual(oracle.ordinary_startup(*startup_observation())['epoch'],2)
        changes=(
            lambda b,a,r,s,e,w:r['capabilities'][0].update(state='ready',reason='current'),
            lambda b,a,r,s,e,w:r['capabilities'][0].update(reason='verification_in_flight'),
            lambda b,a,r,s,e,w:r['capabilities'][0].update(state='unavailable',reason='dependency_unavailable'),
            lambda b,a,r,s,e,w:a['snapshot_leases'][-1].update(state='active'),
            lambda b,a,r,s,e,w:a['retention_claims'][-1].update(last_sequence=11),
            lambda b,a,r,s,e,w:a['retention_claims'][-1].update(xmin='unexpected-transaction'),
            lambda b,a,r,s,e,w:a['owner_epochs'][-1].update(lease_expires_at_us=e),
            lambda b,a,r,s,e,w:a['owner_epochs'][-1].update(epoch=1),
            lambda b,a,r,s,e,w:a['archive_frontier_snapshots'][0].update(snapshot_id=4),
            lambda b,a,r,s,e,w:[a.__setitem__(t,copy.deepcopy(b[t])) for t in ('snapshot_leases','archive_snapshot_pins','retention_claims')],
        )
        for change in changes:
            values=startup_observation();change(*values)
            with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                oracle.ordinary_startup(*values)
        values=list(startup_observation());values[-1]=dict(pid=999)
        with self.assertRaises(AssertionError):oracle.ordinary_startup(*values)

    def test_both_unphased_index_callers_share_original_budget_helper(self):
        self.assertIs(PinReleaseRuntime.ordinary_client,index.IndexRuntime.baseline_client)
        self.assertIs(RestartIndexRuntime.ordinary_client,index.IndexRuntime.baseline_client)
        self.assertIsNot(ShortPinRuntime.ordinary_client,index.IndexRuntime.baseline_client)

    def test_setup_phase_is_capped_by_existing_journey_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            home=Path(directory);(home/'ordinary-setup.continue').touch()
            with patch.object(client.time,'monotonic_ns',side_effect=[10_000_000_000,10_001_000_000]):
                observed=client.phase(home,'setup',enclosing_deadline_ns=11_000_000_000)
            marker=json.loads((home/'ordinary-setup.json').read_text())
            self.assertEqual(marker['deadline_ns'],11_000_000_000)
            self.assertLess(observed['finished_ns'],marker['deadline_ns'])
            (home/'ordinary-setup.json').unlink()
            with patch.object(client.time,'monotonic_ns',side_effect=[10_000_000_000,11_000_000_000]),self.assertRaises(AssertionError):
                client.phase(home,'setup',enclosing_deadline_ns=11_000_000_000)

    def test_failure_classification_does_not_capture_credentials_or_arbitrary_text(self):
        value={'error':{'status':429,'code':'capacity_or_rate_limited','retry_class':'retry_later',
            'message':'private diagnostic','session':'private token','csrf':'private proof'},'token':'private'}
        self.assertEqual(client.error_summary(value),{'status':429,'code':'capacity_or_rate_limited','retry_class':'retry_later'})
        self.assertEqual(client.error_summary({'error':{'status':True,'code':'private token','retry_class':'x'*65}}),{})
        self.assertIsNone(client.error_summary({'unrecognized':'private'}))

    @unittest.skipUnless(sys.platform == 'linux', 'actual process custody observation requires Linux /proc')
    def test_unknown_child_is_not_mistaken_for_an_empty_native_inventory(self):
        parent=process_identity(os.getpid())
        child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(5)'],
            stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            with self.assertRaises(ValueError):
                ShortPinRuntime.worker(object(),parent,allow_absent=True)
        finally:
            child.terminate();child.wait(timeout=2)


if __name__ == '__main__':unittest.main()
