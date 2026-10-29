"""Offline live-client wiring controls; no installed or engine qualification."""
import argparse
import base64
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
HELPERS = ROOT / 'experiments/ingest-bootstrap'
sys.path.insert(0, str(HELPERS))
import installed_live_client as live


# Independent expected identity/actions, never inferred from /me.
PRINCIPAL = 'ab' * 16
ACTIONS = ['audit.read', 'budgets.change', 'data.read', 'policy.change',
    'principals.manage', 'roles.control', 'status.read']
LEGACY_ACTIONS = ['data.read', 'policy.change', 'principals.manage', 'roles.control', 'status.read']
PASSWORD = 'Independent supplied password 2026 '
SESSION = dict(format='bigbrotr-cli-session/1', origin='http://127.0.0.1:18081',
    session='cd' * 32, csrf='ef' * 32)


class LiveClientControls(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name).resolve()
        self.directory = self.home / 'journey'
        self.directory.mkdir(mode=0o700)
        self.profile_directory = self.home / 'profile'
        self.profile_directory.mkdir()
        self.secret = self.home / 'owner.private'
        self.secret.write_text(PASSWORD)
        self.secret.chmod(0o600)
        self.profile = dict(format='bigbrotr-installed-archive-fixture/1',
            product_binary=sys.executable, password_ref='file:' + str(self.secret),
            expected_principal_id=PRINCIPAL, expected_actions=ACTIONS)
        (self.profile_directory / 'fixture-profile.json').write_text(json.dumps(self.profile))

    def exercise(self, *, profile=True, config='1', policy='1', changed_me=None, leak=None, live_index_proof=False):
        commands, posts = [], []
        self.timeline=[]
        logged_out = False
        me = dict(principal_id=PRINCIPAL if profile else '10' * 16,
            authority='observation-only', config_revision=config, policy_revision=policy,
            stored_actions=ACTIONS if profile else LEGACY_ACTIONS)
        me.update(changed_me or {})
        (self.directory / 'visibility.json').write_text(json.dumps({'ordinal': '3'}))
        for barrier in ('prepared', 'submitted', 'commands-finished'):
            (self.directory / (barrier + '.continue')).touch()
        if live_index_proof:
            (self.directory / 'index-proof.continue').touch()

        def cli(command, **kwargs):
            nonlocal logged_out
            commands.append(command)
            self.timeline.append(('cli',command[2]))
            self.assertEqual(kwargs, dict(env={}, capture_output=True, timeout=5))
            if command[2] == 'login':
                session = Path(command[command.index('--session-output') + 1])
                session.write_text(json.dumps(SESSION))
                session.chmod(0o600)
            else:
                self.assertEqual(command[2], 'logout')
                logged_out = True
            return subprocess.CompletedProcess(command, 0, json.dumps({'message': leak} if leak else {}).encode(), b'')

        def request(method, path, body=None, headers=None):
            self.timeline.append(('request',method,path))
            if method == 'GET' and path == '/management/v1/me':
                return (200, json.dumps(me).encode()) if headers and not logged_out else (401, b'{}')
            if method == 'GET':
                self.assertEqual(path, '/management/v1/jobs/' + '45' * 16)
                value = dict(format='bigbrotr-job-status/1', job_id='45' * 16,
                    kind='acquisition', state='queued', job_revision='2', checkpoint_revision='0',
                    created_at_us='100', completed_at_us=None, observed_at_us='101',
                    reason_code=None, authority='observation-only')
                return 200, json.dumps(value).encode()
            self.assertEqual(method, 'POST')
            posts.append((path, copy.deepcopy(body)))
            value = dict(created=True, command_key=headers['Idempotency-Key'])
            if path.endswith('/relay-endpoints'):
                value.update(endpoint_id='12' * 16)
            elif path.endswith('/relay-enrollments'):
                value.update(generation='1', enabled=True, access_scope_id='23' * 16)
            elif path.endswith('/acquisition-scopes'):
                value.update(scope_id='34' * 16)
            else:
                self.assertEqual(path, '/management/v1/jobs')
                value.update(job_id='45' * 16, accepted_at_us='100', execution='unobserved',
                    authority='acceptance-only')
            return 202, json.dumps(value).encode()

        original_publish=live.publish
        def publish(directory,name,value):
            self.timeline.append(('publish',name))
            return original_publish(directory,name,value)
        with patch.object(live.subprocess, 'run', cli), patch.object(live, 'request', request), \
                patch.object(live,'publish',publish), \
                patch.object(live, 'relay_listener', return_value=object()), \
                patch.object(live, 'relay', side_effect=lambda directory, listener, finished, errors: finished.wait(2)):
            options=dict(live_index_proof=True) if live_index_proof else {}
            result = live.journey(self.directory, self.profile_directory if profile else None, config, policy,**options)
        return result, commands, posts

    def test_supplied_identity_binary_password_and_independent_revisions_propagate(self):
        result, commands, posts = self.exercise(config='1', policy='3')
        self.assertEqual([command[:3] for command in commands],
            [[sys.executable, 'access', 'login'], [sys.executable, 'access', 'logout']])
        self.assertEqual(commands[0][commands[0].index('--password-ref') + 1], self.profile['password_ref'])
        self.assertEqual([path.rsplit('/', 1)[-1] for path, _ in posts],
            ['relay-endpoints', 'relay-enrollments', 'acquisition-scopes', 'jobs'])
        for _, body in posts:
            self.assertEqual(body['expected_config_revision'], '1')
            self.assertEqual(body['expected_policy_revision'], '3')
        self.assertEqual(result['me']['principal_id'], PRINCIPAL)
        self.assertEqual(result['me']['stored_actions'], ACTIONS)
        self.assertFalse((self.directory / 'password.private').exists())
        self.assertFalse((self.directory / 'index-proof.json').exists())
        self.assertNotIn('live_index_proof',result)
        self.assertEqual(self.secret.read_text(), PASSWORD)
        for path in self.directory.glob('*.json'):
            for secret in (PASSWORD, SESSION['session'], SESSION['csrf']):
                self.assertNotIn(secret, path.read_text())

    def test_live_proof_is_opt_in_after_queued_job_and_before_original_logout(self):
        result,commands,posts=self.exercise(live_index_proof=True)
        queued=self.timeline.index(('request','GET','/management/v1/jobs/'+'45'*16))
        proof=self.timeline.index(('publish','index-proof-phase.json'))
        logout=self.timeline.index(('cli','logout'))
        self.assertLess(queued,proof);self.assertLess(proof,logout)
        self.assertLess(logout,self.timeline.index(('publish','commands-finished.json')))
        self.assertEqual([c[2] for c in commands],['login','logout'])
        self.assertEqual(posts[-1][1]['deadline_us'],'120000000')
        for name,seconds in (('prepared',25),('submitted',25),('index-proof',45),('commands-finished',25)):
            phase=json.loads((self.directory/(name+'-phase.json')).read_text())
            released=json.loads((self.directory/(name+'-phase-released.json')).read_text())
            self.assertEqual(phase['deadline_monotonic_ns']-phase['started_monotonic_ns'],seconds*1_000_000_000)
            self.assertLess(released['accepted_monotonic_ns'],phase['deadline_monotonic_ns'])
        self.assertEqual(result['live_index_proof']['phase'],'index-proof')
        self.assertEqual(result['unauthenticated'],401)

    def test_child_deadline_refuses_even_a_present_or_late_release_flag(self):
        for clocked,name,seconds in ((False,'submitted',25),(True,'index-proof',45)):
            for late_stat in (False,True):
                directory=self.home/(str(clocked)+'-'+str(late_stat));directory.mkdir()
                (directory/(name+'.continue')).touch()
                deadline=100+seconds*1_000_000_000
                ticks=[100,200,deadline] if late_stat else [100,deadline]
                with self.subTest(clocked=clocked,late_stat=late_stat), \
                        patch.object(live.time,'monotonic_ns',side_effect=ticks), \
                        self.assertRaisesRegex(AssertionError,name):
                    live.barrier(directory,name,[],[],phase_clock=clocked)
                self.assertFalse((directory/(name+'-phase-released.json')).exists())

    def test_index_phase_requires_opt_in_and_publishes_its_actual_start(self):
        from cases.first_launch_index_live_oracle import phase_release
        with self.assertRaises(AssertionError):live.barrier(self.directory,'index-proof',[],[])
        self.assertFalse((self.directory/'index-proof.json').exists())
        (self.directory/'index-proof.continue').touch()
        with patch.object(live.time,'monotonic_ns',side_effect=[100,200,300]):
            released=live.barrier(self.directory,'index-proof',[],[],phase_clock=True)
        phase=json.loads((self.directory/'index-proof-phase.json').read_text())
        self.assertEqual(phase,dict(format='bigbrotr-installed-live-phase/1',phase='index-proof',
            started_monotonic_ns=100,deadline_monotonic_ns=45_000_000_100))
        self.assertEqual(released['accepted_monotonic_ns'],300)
        phase_release(phase,released,400)

    def test_observed_identity_actions_or_revisions_cannot_replace_expectations(self):
        for changed in (dict(principal_id='10' * 16), dict(stored_actions=LEGACY_ACTIONS),
                dict(stored_actions=ACTIONS + ['unknown.action']), dict(config_revision='2'),
                dict(policy_revision='2')):
            with self.subTest(changed=changed), self.assertRaises(AssertionError):
                self.exercise(changed_me=changed)
            self.assertFalse((self.directory / 'command-1.json').exists())

    def test_profile_secret_in_cli_result_is_refused_before_result_publication(self):
        for number, password in enumerate((PASSWORD, 'Independent "quoted" \\ password 2026 \u00e8')):
            self.directory = self.home / ('leak-' + str(number))
            self.directory.mkdir(mode=0o700)
            self.secret.write_text(password)
            with self.subTest(number=number), self.assertRaisesRegex(AssertionError, 'sensitive bytes'):
                self.exercise(leak=password)
            self.assertFalse((self.directory / 'client-result.json').exists())

    def test_cli_failure_does_not_print_sensitive_stderr(self):
        with patch.object(live.subprocess, 'run', return_value=subprocess.CompletedProcess(
                [], 1, b'', PASSWORD.encode())):
            with self.assertRaisesRegex(AssertionError, '^private CLI command refused$'):
                live.access('login', binary=sys.executable)

    def test_failure_and_timeout_keep_original_bytes_in_exclusive_private_evidence(self):
        stdout, stderr = b'\x00private session bytes', PASSWORD.encode()
        for timeout in (False, True):
            directory = self.home / ('failed-' + str(timeout))
            directory.mkdir(mode=0o700)
            result = subprocess.CompletedProcess([], 3, stdout, stderr)
            error = subprocess.TimeoutExpired(['private-command'], 5, output=stdout, stderr=stderr)
            with patch.object(live.subprocess, 'run', side_effect=error if timeout else None,
                    return_value=result), self.assertRaisesRegex(AssertionError, '^private CLI command (refused|timed out)$'):
                live.access('login', binary=sys.executable, failure_directory=directory)
            evidence = directory / 'login-failure.private'
            self.assertEqual(evidence.stat().st_mode & 0o777, 0o600)
            retained = json.loads(evidence.read_text())
            self.assertEqual(base64.b64decode(retained['stdout_base64']), stdout)
            self.assertEqual(base64.b64decode(retained['stderr_base64']), stderr)
            self.assertEqual(retained['exit_code'], None if timeout else 3)
            self.assertEqual(retained['timed_out'], timeout)
            original = evidence.read_bytes()
            with patch.object(live.subprocess, 'run', return_value=result), self.assertRaises(FileExistsError):
                live.access('login', binary=sys.executable, failure_directory=directory)
            self.assertEqual(evidence.read_bytes(), original)

    def test_nonprivate_evidence_directory_refuses_before_cli_execution(self):
        self.directory.chmod(0o755)
        with patch.object(live.subprocess, 'run') as command, self.assertRaisesRegex(AssertionError, 'private evidence'):
            live.access('login', binary=sys.executable, failure_directory=self.directory)
        command.assert_not_called()

    def test_legacy_journey_keeps_binary_password_identity_and_revision_defaults(self):
        # An unrelated profile in the journey directory must not opt in.
        shutil.copy2(self.profile_directory / 'fixture-profile.json', self.directory)
        result, commands, posts = self.exercise(profile=False, config='2', policy='2')
        self.assertEqual(result['me']['principal_id'], '10' * 16)
        self.assertTrue(all(command[0] == '/build/bigbrotr' for command in commands))
        secret = self.directory / 'password.private'
        self.assertEqual(secret.read_text(), 'Independent owner password 2026')
        self.assertEqual(secret.stat().st_mode & 0o777, 0o600)
        self.assertTrue(all(body['expected_config_revision'] == body['expected_policy_revision'] == '2'
            for _, body in posts))
        with self.assertRaises(FileExistsError):
            live.journey(self.directory)

    def test_explicit_missing_profile_refuses_both_commands_before_any_side_effect(self):
        for missing in (self.home / 'absent', self.home):
            for argv in (['journey', str(self.directory)],
                    ['serve', str(self.home / 'config.toml'), str(self.home / 'serve')]):
                with self.subTest(missing=missing, command=argv[0]), \
                        patch.object(live.subprocess, 'run') as run, patch.object(live.subprocess, 'Popen') as popen:
                    with self.assertRaisesRegex(AssertionError, 'explicit fixture profile required'):
                        live.main([*argv, '--profile', str(missing)])
                    run.assert_not_called()
                    popen.assert_not_called()
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_serve_selects_profile_binary_and_preserves_command(self):
        config = self.home / 'config.toml'
        child = SimpleNamespace(pid=os.getpid(), returncode=0, wait=lambda timeout: 0)
        original = Path.read_text
        def read_text(path, *args, **kwargs):
            if str(path) == f'/proc/{child.pid}/stat':
                return ' '.join(['0'] * 21 + ['123'])
            return original(path, *args, **kwargs)
        for name, binary, options in (('profile', sys.executable, ['--profile', str(self.profile_directory)]),
                ('legacy', '/build/bigbrotr', [])):
            prefix = self.home / ('serve-' + name)
            with self.subTest(name=name), patch.object(live.subprocess, 'Popen', return_value=child) as popen, \
                    patch.object(Path, 'read_text', read_text):
                live.main(['serve', str(config), str(prefix), *options])
            command = [binary, 'serve', '--file', str(config),
                '--activate-initial-successor', '--roles', 'control,ingest,sync']
            self.assertEqual(popen.call_args.args[0], command)
            self.assertEqual(popen.call_args.kwargs['env'], {})
            self.assertEqual(json.loads(Path(str(prefix) + '.pid.json').read_text())['command'], command)
            self.assertTrue(json.loads(Path(str(prefix) + '.terminal.json').read_text())['reaped'])

    def test_cli_defaults_and_explicit_revision_values(self):
        with patch.object(live, 'journey') as journey:
            live.main(['journey', str(self.directory)])
            journey.assert_called_once_with(self.directory, None, '2', '2')
            journey.reset_mock()
            live.main(['journey', str(self.directory), '--profile', str(self.profile_directory),
                '--expected-config-revision', '1', '--expected-policy-revision', '3'])
            journey.assert_called_once_with(self.directory, self.profile_directory, '1', '3')
            journey.reset_mock()
            live.main(['journey',str(self.directory),'--live-index-proof'])
            journey.assert_called_once_with(self.directory,None,'2','2',live_index_proof=True)

    def test_noncanonical_or_overflow_revisions_refuse_before_journey(self):
        invalid = ('0', '01', '+1', '-1', '1.0', '1e0', ' 1', '1 ', '\u0661', '18446744073709551616')
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                live.revision(value)
            for flag in ('--expected-config-revision', '--expected-policy-revision'):
                with self.subTest(value=value, flag=flag), patch.object(live, 'journey') as journey, \
                        patch('sys.stderr'):
                    with self.assertRaises(SystemExit) as error:
                        live.main(['journey', str(self.directory), flag + '=' + value])
                    self.assertEqual(error.exception.code, 2)
                    journey.assert_not_called()
        self.assertEqual(live.revision('18446744073709551615'), '18446744073709551615')

    def test_legacy_two_helper_package_remains_importable_without_profile_module(self):
        copied = self.home / 'legacy-helpers'
        copied.mkdir()
        for name in ('installed_live_client.py', 'installed_sync_client.py'):
            shutil.copy2(HELPERS / name, copied)
        result = subprocess.run([sys.executable, '-B', str(copied / 'installed_live_client.py'), '--help'],
            cwd=copied, env={}, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(b'journey', result.stdout)


if __name__ == '__main__':
    unittest.main()
