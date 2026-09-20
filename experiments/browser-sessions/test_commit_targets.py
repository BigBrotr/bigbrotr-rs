"""Discriminate fixture targets without SQL values or credentials in receipts."""
import importlib.util
from pathlib import Path
import unittest

path = Path(__file__).resolve().parents[1] / 'archive-engine/native_commit_proxy.py'
spec = importlib.util.spec_from_file_location('commit_proxy', path)
proxy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proxy)

class Targets(unittest.TestCase):
    def test_session_mutation_only(self):
        issue = b'INSERT INTO control.browser_sessions(session_id,dataset_id) VALUES($1,$2)'
        revoke = b'UPDATE control.browser_sessions SET revoked_at_us=GREATEST(issued_at_us,42) WHERE session_id=$1'
        for target, query in [('control-session-issue', issue), ('control-session-revoke', revoke)]:
            self.assertTrue(proxy.control_target_query(target, query))
            for other in [b'INSERT INTO control.owner_epochs(resource_id) VALUES($1)',
                          b'UPDATE control.browser_sessions SET last_seen_at_us=$3',
                          b'SELECT revoked_at_us FROM control.browser_sessions',
                          b'DELETE FROM control.browser_sessions',
                          revoke if target.endswith('issue') else issue]:
                self.assertFalse(proxy.control_target_query(target, other))
    def test_existing_targets_remain_exact(self):
        self.assertTrue(proxy.control_target_query('control-pin', b'INSERT INTO control.archive_snapshot_pins(pin_id) VALUES($1)'))
        self.assertFalse(proxy.control_target_query('control-pin', b'INSERT INTO control.archive_pin_recoveries(pin_id) VALUES($1)'))

if __name__ == '__main__':
    unittest.main()
