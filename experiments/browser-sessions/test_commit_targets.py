"""Discriminate fixture targets without SQL values or credentials in receipts."""
import importlib.util
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import unittest

path = Path(__file__).resolve().parents[1] / 'archive-engine/native_commit_proxy.py'
spec = importlib.util.spec_from_file_location('commit_proxy', path)
proxy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proxy)

class Targets(unittest.TestCase):
    def test_progress_targets_only_exact_update_prefix(self):
        target = 'control-producer-progress'
        query = b'UPDATE control.acquisition_progress SET scope_id=$5::bytea, progress_revision=$6::text::numeric'
        self.assertTrue(proxy.control_target_query(target, query))
        self.assertTrue(proxy.control_target_query(target, query.lower()))
        for other in [b'INSERT INTO control.acquisition_progress(dataset_id) VALUES($1)',
                      b'UPDATE control.acquisition_progress SET progress_revision=$6::text::numeric',
                      b'UPDATE control.acquisition_progress SET scope_id=$5::bytea_extra,',
                      b'UPDATE control.producer_checkpoints SET scope_id=$5::bytea,',
                      b'SELECT scope_id FROM control.acquisition_progress',
                      b"SELECT 'UPDATE control.acquisition_progress SET scope_id=$5::bytea,'"]:
            self.assertFalse(proxy.control_target_query(target, other))

    def test_progress_mode_guard_and_target_only_receipt_fields(self):
        for mode in ['pass', 'before-forward', 'after-complete']:
            observer = proxy.Proxy(mode, 'control-producer-progress')
            stream = io.StringIO()
            with redirect_stdout(stream):
                observer.emit('ready')
            event = json.loads(stream.getvalue())
            self.assertEqual(event['target_occurrence'], 2)
            self.assertEqual(event['progress_updates_complete'], 0)
            self.assertIsNone(event['backend_pid'])
        for mode in ['hold-data', 'hold-commit', 'hold-reply', 'omit-auth-final']:
            with self.assertRaisesRegex(ValueError, '^producer-progress-mode$'):
                proxy.Proxy(mode, 'control-producer-progress')
        original = proxy.Proxy('pass')
        self.assertFalse({'target_occurrence', 'progress_updates_complete', 'backend_pid'} & original.report.keys())

    def test_cursor_retention_does_not_target_lifecycle_or_session(self):
        target = 'control-cursor-retention'
        self.assertIn(target, proxy.CONTROL_TARGETS)
        self.assertTrue(proxy.control_target_query(target,
            b'UPDATE control.cursor_key_lifecycle SET decrypt_until_us=$3::bigint WHERE dataset_id=$1::bytea'))
        for query in [b'INSERT INTO control.cursor_key_lifecycle(dataset_id) VALUES($1)',
                      b'UPDATE control.cursor_key_lifecycle SET revoked_revision=$2',
                      b'SELECT decrypt_until_us FROM control.cursor_key_lifecycle',
                      b'INSERT INTO control.cursor_key_transitions(dataset_id) VALUES($1)',
                      b'INSERT INTO control.owner_epochs(resource_id) VALUES($1)',
                      b'INSERT INTO control.browser_sessions(session_id) VALUES($1)']:
            self.assertFalse(proxy.control_target_query(target, query))

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

    def test_export_completion_does_not_target_reservation_or_reader(self):
        target = 'control-export-complete'
        self.assertTrue(proxy.control_target_query(target,
            b'INSERT INTO control.artifacts(artifact_id,job_id) VALUES($1,$2)'))
        for query in [b'INSERT INTO control.export_jobs(job_id) VALUES($1)',
                      b'INSERT INTO control.export_execution_units(job_id) VALUES($1)',
                      b'INSERT INTO control.archive_snapshot_pins(lease_id) VALUES($1)',
                      b'INSERT INTO control.artifact_files(artifact_id) VALUES($1)',
                      b'SELECT artifact_id FROM control.artifacts',
                      b"SELECT 'INSERT INTO control.artifacts('",
                      b'UPDATE control.jobs SET state=$1']:
            self.assertFalse(proxy.control_target_query(target, query))

    def test_export_pause_does_not_target_start_reader_release_or_completion(self):
        target = 'control-export-pause'
        self.assertTrue(proxy.control_target_query(target,
            b"UPDATE control.jobs SET state='paused',job_revision=job_revision+1,reason_code=$3::text WHERE job_id=$1::bytea"))
        for query in [b'UPDATE control.jobs SET state=$3::text,job_revision=job_revision+1',
                      b"UPDATE control.jobs SET state='succeeded',job_revision=job_revision+1",
                      b'INSERT INTO control.export_execution_units(job_id) VALUES($1)',
                      b'INSERT INTO control.artifacts(artifact_id) VALUES($1)',
                      b"UPDATE control.snapshot_leases SET state='released' WHERE lease_id=$1",
                      b'INSERT INTO control.archive_snapshot_pins(lease_id) VALUES($1)',
                      b'SELECT job_id FROM control.jobs']:
            self.assertFalse(proxy.control_target_query(target, query))


class SessionCompletions(unittest.TestCase):
    def test_progress_cached_execute_excludes_other_update_and_failed_commands(self):
        observer = proxy.SessionCommandObserver('control-producer-progress')
        for name, query in [(b'progress', b'UPDATE control.acquisition_progress SET scope_id=$5::bytea, progress_revision=$6'),
                            (b'other', b'UPDATE control.jobs SET state=$1')]:
            observer.frontend(b'P', name + b'\0' + query + b'\0\0\0')
            observer.frontend(b'S', b'')
            observer.backend(b'1', b'')
            observer.backend(b'Z', b'T')
        self.execute(observer, b'other')
        self.execute(observer, b'progress')
        observer.frontend(b'S', b'')
        self.assertFalse(observer.backend(b'C', b'UPDATE 1\0'))
        self.assertFalse(observer.backend(b'C', b'UPDATE 0\0'))
        observer.backend(b'Z', b'T')
        self.execute(observer, b'progress')
        observer.frontend(b'S', b'')
        self.assertFalse(observer.backend(b'E', b'C23514\0\0'))
        observer.backend(b'Z', b'E')
        # The cached statement survives transaction boundaries and an error.
        observer.frontend(b'Q', b'ROLLBACK\0')
        observer.backend(b'C', b'ROLLBACK\0')
        observer.backend(b'Z', b'I')
        self.execute(observer, b'progress')
        observer.frontend(b'S', b'')
        self.assertTrue(observer.backend(b'C', b'UPDATE 1\0'))
        observer.backend(b'Z', b'T')
        self.assertFalse(observer.pending)

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
