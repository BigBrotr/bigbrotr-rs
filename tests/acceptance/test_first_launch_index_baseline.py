"""Closed startup-quiescence and unchanged-deadline controls; no engines."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from cases import first_launch_index as index
from cases import first_launch_index_oracle as oracle
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


class BaselineControls(unittest.TestCase):
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
