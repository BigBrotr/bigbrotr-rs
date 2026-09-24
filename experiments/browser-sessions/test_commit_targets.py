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

    def test_export_does_not_target_reader_or_owner_commits(self):
        self.assertTrue(proxy.control_target_query('control-export-accept', b'INSERT INTO control.export_jobs(job_id,dataset_id) VALUES($1,$2)'))
        for query in [b'INSERT INTO control.owner_epochs(resource_id) VALUES($1)',
                      b'INSERT INTO control.archive_snapshot_pins(lease_id) VALUES($1)',
                      b'INSERT INTO control.jobs(job_id) VALUES($1)',
                      b'SELECT job_id FROM control.export_jobs WHERE request_key=$1']:
            self.assertFalse(proxy.control_target_query('control-export-accept', query))


class SessionCompletions(unittest.TestCase):
    def observer(self):
        return proxy.SessionCommandObserver('control-session-issue')

    def prepare(self, observer, name, selected):
        query = (b'INSERT INTO control.browser_sessions(session_id) VALUES($1)' if selected
                 else b'INSERT INTO control.management_audit(audit_id) VALUES($1)')
        observer.frontend(b'P', name + b'\0' + query + b'\0\0\0')

    def execute(self, observer, name):
        observer.frontend(b'B', b'\0' + name + b'\0' + b'\0' * 6)
        observer.frontend(b'E', b'\0' * 5)

    def test_audit_completion_does_not_count_as_session(self):
        observer = self.observer()
        self.prepare(observer, b'issue', True)
        self.prepare(observer, b'audit', False)
        self.execute(observer, b'issue')
        self.execute(observer, b'audit')
        observer.frontend(b'S', b'')
        self.assertTrue(observer.backend(b'C', b'INSERT 0 1\0'))
        self.assertFalse(observer.backend(b'C', b'INSERT 0 1\0'))
        observer.backend(b'Z', b'T')
        self.assertFalse(observer.pending)

    def test_prepare_without_execute_does_not_attribute_audit(self):
        observer = self.observer()
        self.prepare(observer, b'issue', True)
        self.prepare(observer, b'audit', False)
        self.execute(observer, b'audit')
        observer.frontend(b'S', b'')
        self.assertFalse(observer.backend(b'C', b'INSERT 0 1\0'))
        observer.backend(b'Z', b'T')

    def test_failed_cycle_preserves_next_pipelined_cycle(self):
        observer = self.observer()
        self.prepare(observer, b'issue', True)
        self.prepare(observer, b'audit', False)
        self.execute(observer, b'issue')
        observer.frontend(b'S', b'')
        self.execute(observer, b'audit')
        observer.frontend(b'S', b'')
        self.assertFalse(observer.backend(b'E', b'C23514\0\0'))
        observer.backend(b'Z', b'E')
        self.assertFalse(observer.backend(b'C', b'INSERT 0 1\0'))
        observer.backend(b'Z', b'T')
        self.assertFalse(observer.pending)

    def test_error_before_execute_and_sync_ignores_rest_of_failed_cycle(self):
        observer = self.observer()
        self.prepare(observer, b'issue', True)
        observer.frontend(b'B', b'\0issue\0' + b'\0' * 6)
        self.assertFalse(observer.backend(b'E', b'C23514\0\0'))
        observer.frontend(b'E', b'\0' * 5)
        observer.frontend(b'S', b'')
        observer.backend(b'Z', b'E')
        self.assertFalse(observer.pending)
        self.execute(observer, b'issue')
        observer.frontend(b'S', b'')
        self.assertTrue(observer.backend(b'C', b'INSERT 0 1\0'))
        observer.backend(b'Z', b'T')

    def test_simple_multi_statement_and_zero_rows_are_not_target_success(self):
        observer = self.observer()
        observer.frontend(b'Q', b'BEGIN; SELECT 1\0')
        self.assertFalse(observer.backend(b'C', b'BEGIN\0'))
        self.assertFalse(observer.backend(b'C', b'SELECT 1\0'))
        observer.backend(b'Z', b'T')
        self.prepare(observer, b'issue', True)
        self.execute(observer, b'issue')
        observer.frontend(b'S', b'')
        self.assertFalse(observer.backend(b'C', b'INSERT 0 0\0'))
        observer.backend(b'Z', b'T')

if __name__ == '__main__':
    unittest.main()
