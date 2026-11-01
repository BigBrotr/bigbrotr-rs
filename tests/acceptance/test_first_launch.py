"""Bounded controls for first-launch custody and terminal verdicts; no engines."""
import copy
import json
import os
from pathlib import Path
import signal
import socket
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
    def check_port_preflight(self, root, port):
        candidate = root / 'candidate.json'
        candidate.write_text('{}')
        args = arguments(root)
        args.candidate_manifest = candidate
        runtime = case.Runtime(args, {})
        runtime.docker = Mock(side_effect=RuntimeError('ports accepted'))
        with patch.object(case.platform, 'system', return_value='Linux'), \
                patch.object(case.platform, 'machine', return_value='x86_64'), \
                patch.object(case.os, 'geteuid', return_value=1000), \
                patch.object(case.shutil, 'which', return_value='/usr/bin/docker'), \
                patch.object(case.shutil, 'disk_usage', return_value=SimpleNamespace(free=100 * 1024**3)), \
                patch.object(case, 'PORTS', (port,)):
            runtime.preflight()

    def test_preflight_refuses_live_listener(self):
        with tempfile.TemporaryDirectory() as temporary, socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(('127.0.0.1', 0))
            listener.listen()
            port = listener.getsockname()[1]
            with self.assertRaisesRegex(case.MissingPrerequisite, f'port {port} unavailable'):
                self.check_port_preflight(Path(temporary), port)

    @unittest.skipUnless(sys.platform.startswith('linux'), 'Linux TIME_WAIT bind semantics')
    def test_preflight_accepts_closed_journey_time_wait(self):
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(('127.0.0.1', 0))
            listener.listen()
            port = listener.getsockname()[1]
            with socket.create_connection(('127.0.0.1', port)) as client:
                connection, _ = listener.accept()
                connection.close()
                self.assertEqual(client.recv(1), b'')
        with socket.socket() as exclusive, tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(OSError):
                exclusive.bind(('127.0.0.1', port))
            with self.assertRaisesRegex(RuntimeError, 'ports accepted'):
                self.check_port_preflight(Path(temporary), port)

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

    def test_failure_trace_records_only_relative_source_locations(self):
        def fail(argument='private-argument-sentinel'):
            local = 'private-local-sentinel'
            raise AssertionError('original reason')
        checks, failure = self.exercise_failure(fail)
        self.assertEqual(failure['reason'], 'original reason')
        self.assertEqual(checks['first_launch_signed_acquisition']['result'], 'FAIL')
        self.assertEqual(checks['first_launch_ordinary_history_export']['result'], 'NOT_RUN')
        self.assertEqual(checks['first_launch_cleanup']['result'], 'PASS')
        self.assertTrue(failure['traceback'])
        for frame in failure['traceback']:
            self.assertEqual(set(frame), {'file', 'line', 'function'})
            self.assertFalse(Path(frame['file']).is_absolute())
            self.assertNotIn('..', Path(frame['file']).parts)
            self.assertIs(type(frame['line']), int)
        self.assertEqual(failure['traceback'][-1]['file'], 'tests/acceptance/test_first_launch.py')
        self.assertEqual(failure['traceback'][-1]['function'], 'fail')
        self.assertNotIn('sentinel', json.dumps(failure))
        self.assertNotIn('AssertionError(', json.dumps(failure['traceback']))

    def test_failure_trace_omits_frames_outside_source_tree(self):
        with tempfile.TemporaryDirectory() as temporary:
            namespace = {}
            exec(compile("def external():\n    raise ValueError('original reason')\n",
                         str(Path(temporary) / 'private-location.py'), 'exec'), namespace)
            _, failure = self.exercise_failure(namespace['external'])
            self.assertTrue(failure['traceback'])
            self.assertNotIn(temporary, json.dumps(failure))
            self.assertNotIn('private-location', json.dumps(failure))
            self.assertNotIn('external', [frame['function'] for frame in failure['traceback']])

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
        self.assertEqual(len(expected), 83)
        self.assertIn('index_frontier_handoffs', expected)
        self.assertIn('producer_requests', expected)
        self.assertIn('schema_versions', expected)

    def test_current_bootstrap_refuses_predecessor_or_changed_ledger_before_control_setup(self):
        current = case.expected_ledger(ROOT)
        self.assertEqual(len(current), 59)
        self.assertIn(801, [row['migration'] for row in current])
        last, = [row for row in current if row['migration'] == 42]
        self.assertEqual(last['checksum'], 'fd6b43739a8e2f5639d00ef72ae9bf9edaad8bd9a2e93b81922329bb0e3e2a43')
        case.validate_current_ledger(ROOT, current)
        changed = copy.deepcopy(current)
        changed[0]['checksum'] = '00'*32
        incompatible = copy.deepcopy(current)
        incompatible[0]['writer_floor'] = 2
        for ledger in ([row for row in current if row['migration'] != 42], changed, incompatible):
            with self.subTest(ledger_rows=len(ledger)), tempfile.TemporaryDirectory() as temporary:
                runtime = case.Runtime(arguments(Path(temporary)), {})
                runtime.manifest = dict(artifact=dict(build_info={'build': 'bound'}))
                runtime.finite = Mock(side_effect=[{'build': 'bound'}, {'result': 'verified'}, {}, {}, {}])
                runtime.launch = Mock(return_value={'id': 'owned-pg'})
                runtime.inspect = Mock()
                runtime.wait = Mock()
                runtime.docker = Mock()
                runtime.sql = Mock(side_effect=['160015', json.dumps(ledger)])
                with self.assertRaisesRegex(ValueError, 'applied Control ledger differs'):
                    runtime.bootstrap()
                calls = runtime.finite.call_args_list
                self.assertEqual([call.args[0] for call in calls],
                    ['build-info', 'verify-worker', 'config-validate', 'migration-plan', 'migration-apply'])
                for call in calls[-2:]:
                    self.assertEqual(call.args[-2:], ('--target', '43'))
                self.assertEqual(runtime.docker.call_count, 1)
                self.assertEqual(case.load(runtime.root/'bootstrap/applied-control-ledger.json'), ledger)

    def test_fresh_prefix_profile_refuses_old_profile_foreign_identity_or_retirement(self):
        initialized = dict(private_scope=dict(dataset_id='12'*16), producer_slot_id='34'*16)
        head = dict(dataset_id='\\x'+'12'*16, producer_slot_id='\\x'+'34'*16,
            producer_epoch=1, prefix_profile=True, progress_format=1, complete_through_request=4)
        head.update({'prefix_'+slot+'_'+field:None for slot in ('active','prepared')
            for field in ('bytes','sha256','generation','retired','source')})
        case.validate_initial_prefix_profile({'producer_checkpoints':[head]}, initialized)
        for delta in ({'prefix_profile':False}, {'producer_slot_id':'\\x'+'56'*16},
                      {'progress_format':2}, {'complete_through_request':3},
                      {'prefix_active_generation':1}, {'prefix_prepared_sha256':'\\x'+'78'*32}):
            with self.subTest(delta=delta), self.assertRaises(ValueError):
                case.validate_initial_prefix_profile({'producer_checkpoints':[dict(head, **delta)]}, initialized)

    def test_prefix_runtime_role_refuses_privileged_login_helpers_or_guards(self):
        profile = dict(current_user='bigbrotr_control', session_user='bigbrotr_control',
            superuser=False, owns_objects=False, private_execute=dict(
                producer_prefix_deletion=False, finalize_producer_prefix=False), guards=[
                dict(table='producer_checkpoint_dependencies', function='guard_producer_prefix_dependency',
                     trigger='producer_checkpoint_dependencies_immutable', type=27, security_definer=False, enabled='O'),
                dict(table='producer_outcomes', function='guard_producer_outcome',
                     trigger='producer_outcome_guard', type=31, security_definer=False, enabled='O'),
                dict(table='producer_outcomes', function='guard_producer_outcome',
                     trigger='producer_outcome_truncate_guard', type=34, security_definer=False, enabled='O'),
                dict(table='producer_requests', function='guard_producer_request',
                     trigger='producer_request_guard', type=31, security_definer=False, enabled='O'),
                dict(table='producer_requests', function='guard_producer_request',
                     trigger='producer_request_truncate_guard', type=34, security_definer=False, enabled='O')])
        case.validate_prefix_runtime_role(profile)
        changes = [lambda p: p.update(current_user='postgres'),
            lambda p: p.update(session_user='postgres'), lambda p: p.update(superuser=True),
            lambda p: p.update(owns_objects=True),
            lambda p: p['private_execute'].update(producer_prefix_deletion=True),
            lambda p: p['private_execute'].update(finalize_producer_prefix=True),
            lambda p: p['guards'][0].update(security_definer=True),
            lambda p: p['guards'][1].update(enabled='D'),
            lambda p: p['guards'][2].update(type=31),
            lambda p: p['guards'][1].update(trigger='different'), lambda p: p['guards'].pop()]
        for change in changes:
            changed = copy.deepcopy(profile)
            change(changed)
            with self.subTest(change=change), self.assertRaises(ValueError):
                case.validate_prefix_runtime_role(changed)

    def test_prefix_refusals_require_populated_unchanged_targets_and_exact_errors(self):
        tables = ('producer_requests', 'producer_outcomes', 'producer_checkpoint_dependencies', 'producer_checkpoints')
        before = dict(current_user='bigbrotr_control', session_user='bigbrotr_control',
            rows={table: [dict(target=1)] for table in tables})
        errors = [name + ' 42501' for name in (*tables[:3], 'finalize_producer_prefix')]
        def result(first=before, last=before, states=errors):
            return '\n'.join([json.dumps(first), *states, json.dumps(last)]) + '\n'
        self.assertTrue(case.validate_prefix_refusals(result())['target_rows_and_checkpoint_unchanged'])
        for state in ('00000', '23514', '25006', '57014'):
            for index in range(4):
                changed = errors.copy()
                changed[index] = changed[index].replace('42501', state)
                with self.subTest(state=state, index=index), self.assertRaises(ValueError):
                    case.validate_prefix_refusals(result(states=changed))
        for table in tables:
            changed = copy.deepcopy(before)
            changed['rows'][table][0]['target'] = 2
            with self.subTest(table=table), self.assertRaises(ValueError):
                case.validate_prefix_refusals(result(last=changed))
            changed['rows'][table] = []
            with self.subTest(empty=table), self.assertRaises(ValueError):
                case.validate_prefix_refusals(result(first=changed, last=changed))
        with self.assertRaises(ValueError):
            case.validate_prefix_refusals(result(states=errors[:-1]))
        changed = dict(before, session_user='postgres')
        with self.assertRaises(ValueError):
            case.validate_prefix_refusals(result(first=changed, last=changed))

    def test_runtime_control_probe_uses_verified_tcp_login_and_mounted_secret(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = case.Runtime(arguments(Path(temporary)), {})
            runtime.pg = dict(id='owned-pg')
            statement = 'SELECT current_user;'
            with patch.object(runtime, 'inspect') as inspect, patch.object(runtime, 'docker') as docker:
                runtime.runtime_control_sql(statement, 'probe')
            inspect.assert_called_once_with(runtime.pg)
            args, options = docker.call_args
            self.assertEqual(args[:6], ('exec', '-i', 'owned-pg', 'sh', '-eu', '-c'))
            self.assertIn('PGPASSWORD="$(cat /run/secrets/control)"', args[6])
            self.assertIn('-h 127.0.0.1 -U bigbrotr_control -d bigbrotr_control', args[6])
            self.assertNotIn('postgres', args[6])
            self.assertEqual(options, dict(data=statement, timeout=15, label='probe'))


class BackendAbsenceControls(unittest.TestCase):
    def sample(self,database,backends=None):
        backends=[] if backends is None else backends
        return dict(database=database,observer_pid=100,observer_backend_start='2026-11-01T03:09:21.390000+00:00',
            count=len(backends),backends=backends)

    def backend(self,kind='client backend'):
        return dict(pid=200,backend_start='2026-11-01T03:09:20.000000+00:00',backend_type=kind,
            usename='bigbrotr_control',application_name='',state='idle',wait_event_type='Client',
            wait_event='ClientRead',xact_start=None,leader_pid=None)

    def observe(self,samples,*,failure=None):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=object.__new__(case.Runtime)
            runtime.sql=Mock(side_effect=lambda database,statement:samples[database]
                if isinstance(samples[database],str) else json.dumps(samples[database]))
            runtime.wait=Mock(side_effect=AssertionError('unexpected backend retry'))
            if failure:
                with self.assertRaisesRegex(ValueError,failure):runtime.backend_absence(root)
            else:runtime.backend_absence(root)
            self.assertEqual(case.load(root/'backend-absence.json'),samples)
            self.assertEqual([call.args[0] for call in runtime.sql.call_args_list],
                ['bigbrotr_control','bigbrotr_catalog'])
            runtime.wait.assert_not_called()
            statements=[call.args[1] for call in runtime.sql.call_args_list]
            self.assertEqual(statements[0],statements[1])
            return statements[0]

    def test_zero_sample_counts_and_metadata_share_one_projection_without_query_or_credentials(self):
        samples={database:self.sample(database) for database in ('bigbrotr_control','bigbrotr_catalog')}
        statement=self.observe(samples)
        self.assertEqual(statement.count('pg_catalog.pg_stat_activity'),1)
        self.assertIn('activity AS MATERIALIZED',statement)
        self.assertIn("'count',(SELECT count(*) FROM others)",statement)
        self.assertIn("jsonb_agg(to_jsonb(b) ORDER BY b.pid)",statement)
        self.assertIn('FROM others b',statement)
        self.assertIn('WHERE datname=current_database()',statement)
        self.assertIn('WHERE pid<>pg_backend_pid()',statement)
        self.assertNotRegex(statement.lower(),r'\b(query|query_id|password|client_addr|limit)\b')
        self.assertNotIn("backend_type=",statement)

    def test_any_other_backend_refuses_and_preserves_metadata_including_maintenance_and_unknown_types(self):
        for database in ('bigbrotr_control','bigbrotr_catalog'):
            for kind in ('client backend','autovacuum worker','unknown extension worker'):
                with self.subTest(database=database,kind=kind):
                    samples={name:self.sample(name) for name in ('bigbrotr_control','bigbrotr_catalog')}
                    backend=self.backend(kind)
                    if kind!='client backend':
                        backend.update(usename=None,state=None,wait_event_type=None,wait_event=None,
                            xact_start='2026-11-01T03:09:21+00:00')
                    samples[database]=self.sample(database,[backend])
                    self.observe(samples,failure='database backend remains')

    def test_malformed_or_inconsistent_sample_cannot_pass_and_raw_observation_survives(self):
        zero=self.sample('bigbrotr_control')
        present=self.sample('bigbrotr_control',[self.backend()])
        mutations=[dict(zero,database='other'),dict(zero,observer_pid=True),dict(zero,observer_pid=0),
            dict(zero,observer_backend_start='not a timestamp'),dict(zero,observer_backend_start='2026-11-01T03:09:21'),
            dict(zero,count='0'),dict(zero,count=False),dict(zero,count=-1),dict(zero,backends={}),
            dict(present,count=0),dict(zero,count=1),dict(present,backends=[dict(self.backend(),pid=100)]),
            dict(present,backends=[dict(self.backend(),pid=True)]),
            dict(present,backends=[dict(self.backend(),backend_start=None)]),
            dict(present,backends=[dict(self.backend(),leader_pid='100')]),
            dict(present,backends=[dict(self.backend(),xact_start='not a timestamp')]),
            dict(present,backends=[dict(self.backend(),wait_event_type=1)]),
            dict(present,backends=[dict(self.backend(),backend_type=None)]),
            dict(present,backends=[dict(self.backend(),unexpected='field')]),
            dict(present,count=2,backends=[self.backend(),dict(self.backend(),backend_start='2026-11-01T03:09:21+00:00')]),
            dict(zero,unexpected='field'),'{invalid JSON']
        for number,sample in enumerate(mutations):
            with self.subTest(mutation=number):
                self.observe(dict(bigbrotr_control=sample,bigbrotr_catalog=self.sample('bigbrotr_catalog')),
                    failure='backend|database')


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
                        patch.object(runtime, 'docker', side_effect=ValueError('log retrieval failed')) as docker, \
                        self.assertRaisesRegex(ValueError, '(owner|client) exited before current'):
                    runtime.wait(predicate, 'current', 1, entry={'id': 'owned'}, client=client)
                if stopped == 'owner':
                    docker.assert_called_once_with('logs', 'owned', check=False, timeout=5, label='exited-owner')
                else:
                    docker.assert_not_called()

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
