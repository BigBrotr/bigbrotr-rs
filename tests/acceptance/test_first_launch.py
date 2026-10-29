"""Bounded controls for first-launch custody and terminal verdicts; no engines."""
import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from cases import first_launch as case


ROOT = Path(__file__).resolve().parents[2]


def arguments(root):
    return SimpleNamespace(evidence=root, source_root=ROOT,
                           candidate_manifest=root / 'missing.json', compose_network=None)


class FirstLaunchControls(unittest.TestCase):
    def test_missing_input_records_not_run_without_docker(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            command = [sys.executable, str(ROOT / 'tests/acceptance/run.py'), 'p01-first-launch',
                '--candidate-manifest', str(root / 'absent.json'), '--source-root', str(ROOT),
                '--evidence', str(root / 'evidence')]
            result = subprocess.run(command, capture_output=True, text=True, timeout=5)
            report = json.loads((root / 'evidence/result.json').read_text())
            self.assertEqual(result.returncode, 2)
            self.assertEqual(report['result'], 'NOT_RUN')
            self.assertFalse(list((root / 'evidence/commands').iterdir()))
            self.assertIn('NOT_RUN: independent native/C API census', report['limits'][1])
            self.assertEqual((root / 'evidence').stat().st_mode & 0o777, 0o700)

    def exercise_failure(self, failure, *, cleanup_failure=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checks, inputs = [], {}
            with patch.object(case, 'Runtime') as runtime:
                runtime.return_value.initial.side_effect = failure
                if cleanup_failure:
                    runtime.return_value.cleanup.side_effect = ValueError('custody mismatch')
                case.first_launch(arguments(root), checks, inputs)
                runtime.return_value.cleanup.assert_called_once()
                runtime.return_value.ordinary.assert_not_called()
            return {row['name']: row for row in checks}, json.loads((root / 'failure.json').read_text())

    def test_wrong_observation_keeps_failure_and_unreached_stage(self):
        checks, failure = self.exercise_failure(AssertionError('wrong signed observation'))
        self.assertEqual(checks['first_launch_signed_acquisition']['result'], 'FAIL')
        self.assertEqual(checks['first_launch_ordinary_history_export']['result'], 'NOT_RUN')
        self.assertEqual(checks['first_launch_cleanup']['result'], 'PASS')
        self.assertEqual(failure['stage'], 'signed_acquisition')

    def test_sigterm_is_failure_and_runs_cleanup(self):
        def interrupt():
            signal.raise_signal(signal.SIGTERM)
        old_handler = signal.getsignal(signal.SIGTERM)
        checks, failure = self.exercise_failure(interrupt)
        self.assertEqual(failure['error_type'], 'InterruptedError')
        self.assertEqual(checks['first_launch_signed_acquisition']['result'], 'FAIL')
        self.assertEqual(signal.getsignal(signal.SIGTERM), old_handler)

    def test_cleanup_failure_cannot_become_success(self):
        checks, _ = self.exercise_failure(ValueError('wrong observation'), cleanup_failure=True)
        self.assertEqual(checks['first_launch_cleanup']['result'], 'FAIL')

    def container(self, state):
        entry = dict(name='project-initial', service='app', id='a' * 64)
        info = dict(Name='/project-initial', Id='a' * 64,
            Config=dict(Labels={case.LABEL: 'owner', 'com.docker.compose.project': 'project',
                                'com.docker.compose.service': 'app'}),
            Mounts=[dict(Type='bind', Destination=target, Source=str(state / source)) for target, source in
                    (('/var/lib/bigbrotr', 'data'), ('/etc/bigbrotr', 'config'), ('/run/secrets', 'secrets/runtime'))])
        return entry, info

    def test_container_custody_refuses_foreign_identity_and_secret_mounts(self):
        state = Path('/owned/instance')
        entry, info = self.container(state)
        self.assertIs(case.check_container(info, entry, 'owner', 'project', state), info)
        changes = [lambda row: row.update(Id='b' * 64),
                   lambda row: row['Config']['Labels'].update({case.LABEL: 'foreign'}),
                   lambda row: row['Mounts'][0].update(Source='/unrelated'),
                   lambda row: row['Mounts'].append(dict(Type='bind', Destination='/run/operator', Source='/owned/secrets'))]
        for change in changes:
            changed = copy.deepcopy(info)
            change(changed)
            with self.subTest(change=change), self.assertRaises(ValueError):
                case.check_container(changed, entry, 'owner', 'project', state)

    def test_borrowed_bridge_is_inspected_never_removed(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = case.Runtime(arguments(Path(temporary)), {})
            network = dict(Name='borrowed', Id='a' * 64, Driver='bridge', Scope='local', Labels={},
                           Containers={'unrelated': {'Name': 'unrelated'}})
            runtime.resources['network'] = dict(name='borrowed', id=network['Id'], borrowed=True)
            with patch.object(runtime, 'docker', return_value=SimpleNamespace(stdout=json.dumps([network]))) as docker:
                runtime.cleanup()
                docker.assert_called_once_with('network', 'inspect', network['Id'])
            self.assertEqual(json.loads((runtime.root / 'network-after.json').read_text()), network)
            for key, value in (('Driver', 'overlay'), ('Scope', 'swarm'), ('Name', 'other')):
                with self.subTest(key=key), self.assertRaises(ValueError):
                    case.check_network(dict(network, **{key: value}), 'borrowed')

    def test_explicit_remote_context_cannot_hide_behind_local_docker_host(self):
        local = 'unix:///var/run/docker.sock'
        context = dict(Name='selected', Endpoints=dict(docker=dict(Host='ssh://remote')))
        with self.assertRaises(ValueError):
            case.local_docker_endpoint(dict(DOCKER_CONTEXT='selected', DOCKER_HOST=local), context)
        self.assertEqual(case.local_docker_endpoint(dict(DOCKER_HOST=local), context), local)
        context['Endpoints']['docker']['Host'] = local
        self.assertEqual(case.local_docker_endpoint(dict(DOCKER_CONTEXT='selected', DOCKER_HOST='tcp://remote'), context), local)

    def test_cleanup_never_stops_container_with_wrong_custody(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = case.Runtime(arguments(Path(temporary)), {})
            entry, info = self.container(runtime.state)
            runtime.resources['containers'].append(entry)
            with patch.object(runtime, 'inspect', side_effect=ValueError('foreign owner')), \
                    patch.object(runtime, 'stop_container') as stop:
                with self.assertRaises(ValueError):
                    runtime.cleanup()
                stop.assert_not_called()
            self.assertTrue(runtime.resources['cleanup_errors'])

    @unittest.skipUnless(sys.platform.startswith('linux'), 'process-group observation requires Linux')
    def test_command_timeout_joins_its_descendant(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = case.Runtime(arguments(root), {})
            pid_file = root / 'child.pid'
            program = ('import pathlib,subprocess,sys,time; '
                       'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(120)"]); '
                       'pathlib.Path(sys.argv[1]).write_text(str(p.pid)); time.sleep(120)')
            with self.assertRaises(subprocess.TimeoutExpired):
                runtime.command([sys.executable, '-c', program, str(pid_file)], timeout=.3)
            child = int(pid_file.read_text())
            state = Path('/proc') / str(child) / 'stat'
            self.assertTrue(not state.exists() or state.read_text().split()[2] == 'Z')
            self.assertEqual(runtime.processes, [])
            receipt, = runtime.commands.glob('*.json')
            self.assertEqual(json.loads(receipt.read_text())['status'], 'joined')

    def test_current_table_inventory_is_derived_from_migrations(self):
        expected = case.expected_tables(ROOT)
        self.assertEqual(len(expected), 82)
        self.assertIn('index_frontier_handoffs', expected)
        self.assertIn('producer_requests', expected)
        self.assertIn('schema_versions', expected)


class FirstLaunchWaitControls(unittest.TestCase):
    def test_positive_observation_retains_value_and_live_process_requirements(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = case.Runtime(arguments(Path(temporary)), {})
            now = [100.0]
            value = {'observed': 'current'}
            observations = iter((False, value))
            client = SimpleNamespace(poll=lambda: None)
            def sleep(seconds):
                now[0] += seconds
            with patch.object(case.time, 'monotonic', side_effect=lambda: now[0]), \
                    patch.object(case.time, 'sleep', side_effect=sleep), \
                    patch.object(runtime, 'inspect', return_value={'State': {'Running': True}}):
                self.assertIs(runtime.wait(lambda: next(observations), 'current', 1,
                                           entry={'id': 'owned'}, client=client), value)
                self.assertIs(runtime.wait(lambda: value, 'no process requirement', 1), value)

    def test_expired_wait_does_not_start_a_predicate(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = case.Runtime(arguments(Path(temporary)), {})
            predicate = Mock(return_value=True)
            for seconds in (0, -1):
                with self.subTest(seconds=seconds), \
                        patch.object(case.time, 'monotonic', return_value=100), \
                        self.assertRaisesRegex(ValueError, 'timeout: current'):
                    runtime.wait(predicate, 'current', seconds)
            predicate.assert_not_called()

    def test_late_positive_and_negative_predicates_cannot_extend_wait(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = case.Runtime(arguments(Path(temporary)), {})
            for result in (True, False):
                for finished in (101.0, 101.1):
                    now = [100.0]
                    def predicate():
                        now[0] = finished
                        return result
                    with self.subTest(result=result, finished=finished), \
                            patch.object(case.time, 'monotonic', side_effect=lambda: now[0]), \
                            self.assertRaisesRegex(ValueError, 'timeout: current'):
                        runtime.wait(predicate, 'current', 1)

    def test_positive_observation_refuses_owner_or_client_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = case.Runtime(arguments(Path(temporary)), {})
            for stopped in ('owner', 'client_zero', 'client_error'):
                live = {'owner': True, 'client': None}
                def predicate():
                    live['owner'] = stopped != 'owner'
                    live['client'] = {'owner': None, 'client_zero': 0, 'client_error': 1}[stopped]
                    return True
                client = SimpleNamespace(poll=lambda: live['client'])
                inspect = lambda entry: {'State': {'Running': live['owner']}}
                with self.subTest(stopped=stopped), \
                        patch.object(case.time, 'monotonic', return_value=100), \
                        patch.object(runtime, 'inspect', side_effect=inspect), \
                        self.assertRaisesRegex(ValueError, '(owner|client) exited before current'):
                    runtime.wait(predicate, 'current', 1, entry={'id': 'owned'}, client=client)

    def test_final_process_observation_consumes_the_same_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = case.Runtime(arguments(Path(temporary)), {})
            for slow in ('inspect', 'poll'):
                now = [100.0]
                def inspect(entry):
                    if slow == 'inspect': now[0] = 101.0
                    return {'State': {'Running': True}}
                def poll():
                    if slow == 'poll': now[0] = 101.0
                    return None
                with self.subTest(slow=slow), \
                        patch.object(case.time, 'monotonic', side_effect=lambda: now[0]), \
                        patch.object(runtime, 'inspect', side_effect=inspect), \
                        self.assertRaisesRegex(ValueError, 'timeout: current'):
                    runtime.wait(lambda: True, 'current', 1, entry={'id': 'owned'},
                                 client=SimpleNamespace(poll=poll))

    def test_expired_sleep_does_not_start_another_predicate(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = case.Runtime(arguments(Path(temporary)), {})
            now = [100.0]
            predicate = Mock(return_value=False)
            def sleep(seconds):
                now[0] = 101.0
            with patch.object(case.time, 'monotonic', side_effect=lambda: now[0]), \
                    patch.object(case.time, 'sleep', side_effect=sleep), \
                    self.assertRaisesRegex(ValueError, 'timeout: current'):
                runtime.wait(predicate, 'current', 1)
            predicate.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
