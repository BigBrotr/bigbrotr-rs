"""Discriminating controls for the installed, positively scheduled stop oracle."""
import copy
from pathlib import Path
import subprocess
import sys
import unittest

from cases.first_launch_renewal_stop import (EFFECT_TABLES, RENEWAL_QUERY,
                                           select_waiter, verify_terminal)


class RenewalStopOracle(unittest.TestCase):
    def setUp(self):
        self.backend = dict(pid=17, backend_start='2026-10-29T10:00:00+00:00', usename='bigbrotr_control')
        self.waiter = dict(self.backend, query=RENEWAL_QUERY, wait_event_type='Lock', blocking_pids=[23])
        self.effects = dict(ingest_owner=[dict(state='active', role='ingest', xmin='11')],
                            **{name: [] for name in EFFECT_TABLES})
        self.effects['producer_epochs'] = [dict(producer_epoch='1')]
        self.initial = dict(state='initialized')
        self.stopped = dict(state='stopped', native='closed', owner_release='committed')

    def test_actual_phase_requires_same_backend_query_and_exclusive_blocker(self):
        self.assertIsNone(select_waiter([], self.backend, 23))
        self.assertEqual(select_waiter([self.waiter], self.backend, 23), self.waiter)
        for key, value in [('pid', 19), ('backend_start', 'another-incarnation'),
                           ('usename', 'postgres'), ('query', 'SELECT 1'),
                           ('wait_event_type', 'Client'), ('blocking_pids', [23, 29])]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                select_waiter([dict(self.waiter, **{key: value})], self.backend, 23)
        with self.assertRaises(ValueError):
            select_waiter([self.waiter, self.waiter], self.backend, 23)

    def test_known_sql_requires_actual_stopped_native_close_and_committed_release(self):
        def check(code=0, records=None, stderr='', state='released'):
            verify_terminal('known', code, records or [self.initial, self.stopped], stderr,
                            [dict(state=state)], self.effects, copy.deepcopy(self.effects))
        check()
        for change in [dict(code=4), dict(stderr='cancelled'), dict(state='active'),
                       dict(records=[self.initial]),
                       dict(records=[self.initial, dict(self.stopped, native='unknown')]),
                       dict(records=[self.initial, dict(self.stopped, owner_release='unknown')])]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                check(**change)

    def test_held_sql_requires_deadline_refusal_and_preserves_unreleased_owner(self):
        def check(code=4, records=None, stderr='initial Ingest deadline', state='active'):
            verify_terminal('deadline', code, records or [self.initial], stderr,
                            [dict(state=state)], self.effects, copy.deepcopy(self.effects))
        check()
        for change in [dict(code=0), dict(code=137), dict(stderr='worker not joined'),
                       dict(stderr='initial Ingest cancelled'), dict(state='released'),
                       dict(records=[self.initial, self.stopped])]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                check(**change)

    def test_missing_capture_or_following_renewal_job_or_scientific_write_cannot_pass(self):
        for key in self.effects:
            missing = copy.deepcopy(self.effects)
            del missing[key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                verify_terminal('known', 0, [self.initial, self.stopped], '',
                                [dict(state='released')], missing, missing)
            changed = copy.deepcopy(self.effects)
            changed[key].append(dict(unexpected='write'))
            with self.subTest(changed=key), self.assertRaises(ValueError):
                verify_terminal('known', 0, [self.initial, self.stopped], '',
                                [dict(state='released')], self.effects, changed)

    def test_schedule_is_explicit_and_cannot_change_another_installed_case(self):
        runner = Path(__file__).parent/'run.py'
        for case, extra in [('p01-first-launch-initial-renewal-stop', []),
                            ('p01-first-launch-tutorial', ['--renewal-stop-outcome', 'known'])]:
            with self.subTest(case=case):
                result = subprocess.run([sys.executable, '-B', str(runner), case,
                    '--source-root', '/nonexistent/bigbrotr-source', '--evidence', '/nonexistent/evidence',
                    *extra], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 2)
                self.assertIn('--renewal-stop-outcome is required only', result.stderr)


if __name__ == '__main__':
    unittest.main()
