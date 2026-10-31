"""Offline controls for explicit profiles; these do not qualify a real install."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import installed_archive_client as archive
import installed_archive_export_client as export
from installed_archive_profile import absolute_path, load, password_file


# Frozen independently from postgres/enrollment.rs ADMIN_ACTIONS; never from /me.
OWNER_ACTIONS = ['audit.read', 'budgets.change', 'data.read', 'policy.change',
    'principals.manage', 'roles.control', 'status.read']


class ProfileTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.home = Path(self.temporary.name).resolve(strict=True)
        self.secret = self.home / 'owner.private'
        self.secret.write_text('Independent supplied password 2026 ')
        self.secret.chmod(0o600)
        self.profile = dict(format='bigbrotr-installed-archive-fixture/1',
            product_binary=sys.executable, password_ref='file:' + str(self.secret),
            expected_principal_id='ab' * 16, expected_actions=OWNER_ACTIONS)

    def save(self, value=None):
        (self.home / 'fixture-profile.json').write_text(json.dumps(self.profile if value is None else value))

    def test_absent_and_explicit_independent_expectations(self):
        self.assertIsNone(load(self.home))
        self.save()
        self.assertEqual(load(self.home), self.profile)
        self.assertEqual(password_file(self.profile['password_ref']), 'Independent supplied password 2026 ')
        self.secret.chmod(0o400)
        self.assertEqual(load(self.home), self.profile)

    def test_reject_profile_shape_and_identity(self):
        for patch_value in ({'format': 'other'}, {'expected_principal_id': '00' * 16},
                {'expected_principal_id': 'AB' * 16}, {'expected_principal_id': 12},
                {'expected_actions': []}, {'expected_actions': ['status.read', 'data.read']},
                {'expected_actions': ['status.read', 'status.read']}, {'expected_actions': [12]},
                {'extra': True}, {'product_binary': 'relative'}, {'product_binary': str(self.home)},
                {'product_binary': str(self.secret)}, {'product_binary': str(self.home / 'absent')},
                {'password_ref': str(self.secret)}, {'password_ref': 'file:relative'}):
            with self.subTest(patch=patch_value):
                self.save(dict(self.profile, **patch_value))
                with self.assertRaises(AssertionError):
                    load(self.home)
        del self.profile['expected_actions']
        self.save()
        with self.assertRaises(AssertionError):
            load(self.home)

    def test_reject_paths_and_profile_bound(self):
        for value in ('relative', '/', '/trailing/', '/a/../b', '/a/./b', '/a\nb', '/a\x7fb'):
            with self.subTest(path=value), self.assertRaises(AssertionError):
                absolute_path(value)
        (self.home / 'fixture-profile.json').write_bytes(b' ' * 16385)
        with self.assertRaises(AssertionError):
            load(self.home)
        (self.home / 'fixture-profile.json').unlink()
        (self.home / 'fixture-profile.json').symlink_to(self.home / 'absent')
        with self.assertRaises(AssertionError):
            load(self.home)

    def test_reject_unprotected_or_invalid_password(self):
        reference = self.profile['password_ref']
        for mode in (0o644, 0o640, 0o4600):
            self.secret.chmod(mode)
            with self.subTest(mode=mode), self.assertRaises(AssertionError):
                password_file(reference)
        self.secret.chmod(0o600)
        for raw in (b'short', b'Password with newline\n', b'x' * 1025, b'Invalid UTF8 password\xff'):
            self.secret.write_bytes(raw)
            with self.subTest(kind=len(raw)), self.assertRaises((AssertionError, UnicodeError)):
                password_file(reference)
        with patch('installed_archive_profile.os.geteuid', return_value=self.secret.stat().st_uid + 1):
            with self.assertRaises(AssertionError):
                password_file(reference)

    def test_reject_symlink_password_components(self):
        link = self.home / 'link'
        link.symlink_to(self.secret)
        with self.assertRaises(OSError):
            password_file('file:' + str(link))
        link.unlink()
        link.symlink_to(self.home, target_is_directory=True)
        with self.assertRaises(OSError):
            password_file('file:' + str(link / self.secret.name))

    def wire(self, module, me, *, stop_at_grant=False):
        requests, commands = [], []
        class ReachedGrant(Exception):
            pass
        class Connection:
            def __init__(self, *args, **kwargs):
                pass
            def request(self, method, path, body, headers):
                requests.append((method, path, None if body is None else json.loads(body)))
                self.path = path
                if stop_at_grant and method == 'PUT':
                    raise ReachedGrant()
            def getresponse(self):
                if self.path == '/management/v1/me':
                    value, status = me, 200
                elif self.path.endswith('/reauthenticate'):
                    value, status = {'proof_id': 'cd' * 16}, 201
                else:
                    value, status = {}, 403
                class Response:
                    def read(self, limit):
                        return json.dumps(value).encode()
                response = Response()
                response.status = status
                return response
            def close(self):
                pass
        def cli(args, **kwargs):
            commands.append(args)
            target = Path(args[args.index('--session-output') + 1])
            target.write_text(json.dumps(dict(format='bigbrotr-cli-session/1',
                origin='http://127.0.0.1:18081', session='opaque-session', csrf='opaque-csrf')))
            target.chmod(0o600)
            return subprocess.CompletedProcess(args, 0, b'{}', b'')
        with patch.object(module.http.client, 'HTTPConnection', Connection), patch.object(module.subprocess, 'run', cli):
            with self.assertRaises(ReachedGrant if stop_at_grant else FileNotFoundError):
                module.run(self.home)
        return requests, commands

    def test_archive_cli_wiring_preserves_legacy_and_checks_explicit_actions(self):
        original = self.home
        for name, actions in [('legacy', ['policy.change', 'roles.control', 'status.read']),
                ('archive-export', ['data.read', 'policy.change', 'principals.manage', 'roles.control', 'status.read'])]:
            self.home = original / name
            self.home.mkdir()
            _, commands = self.wire(archive, dict(principal_id='10' * 16, stored_actions=actions))
            self.assertEqual(commands[0][0], '/build/bigbrotr')
            self.assertEqual((self.home / 'password.private').read_text(), 'Independent owner password 2026')
            self.assertEqual((self.home / 'password.private').stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                archive.run(self.home)
        self.home = original
        self.save()
        _, commands = self.wire(archive, dict(principal_id='ab' * 16, stored_actions=OWNER_ACTIONS))
        self.assertEqual(commands[0][0], sys.executable)
        self.assertEqual(commands[0][commands[0].index('--password-ref') + 1], self.profile['password_ref'])
        self.assertFalse((self.home / 'password.private').exists())
        for me in (dict(principal_id='10' * 16, stored_actions=OWNER_ACTIONS),
                dict(principal_id='ab' * 16, stored_actions=['status.read'])):
            with self.assertRaises(AssertionError):
                self.wire(archive, me)

    def test_export_uses_supplied_principal_and_exact_reauthentication_password(self):
        self.save()
        (self.home / 'export-input.json').write_text(json.dumps(dict(scope={}, catalog_id='12' * 16, destination_id='34' * 16)))
        (self.home / 'event.jsonl').write_text(json.dumps({'id': '56' * 32}))
        (self.home / 'destination').mkdir()
        (self.home / 'destination/destination.json').write_text('{}')
        requests, commands = self.wire(export, dict(principal_id='ab' * 16, stored_actions=OWNER_ACTIONS), stop_at_grant=True)
        self.assertEqual(commands[0][0], sys.executable)
        self.assertEqual(commands[0][commands[0].index('--password-ref') + 1], self.profile['password_ref'])
        self.assertEqual(requests[-2][2]['password'], 'Independent supplied password 2026 ')
        self.assertEqual(requests[-2][2]['command']['principal_id'], 'ab' * 16)
        self.assertEqual(requests[-1][1], '/management/v1/principals/' + 'ab' * 16 + '/grants')
        self.assertNotIn('Independent supplied password', (self.home / 'export-observations.json').read_text())
        for me in (dict(principal_id='10' * 16, stored_actions=OWNER_ACTIONS),
                dict(principal_id='ab' * 16, stored_actions=['status.read'])):
            with self.assertRaises(AssertionError):
                self.wire(export, me)
        (self.home / 'fixture-profile.json').unlink()
        requests, commands = self.wire(export, dict(principal_id='10' * 16,
            stored_actions=['data.read', 'policy.change', 'principals.manage', 'roles.control', 'status.read']), stop_at_grant=True)
        self.assertEqual(commands[0][0], '/build/bigbrotr')
        self.assertEqual(commands[0][commands[0].index('--password-ref') + 1], 'file:' + str(self.home / 'password.private'))
        self.assertEqual(requests[-2][2]['password'], 'Independent owner password 2026')
        self.assertEqual(requests[-1][1], '/management/v1/principals/' + '10' * 16 + '/grants')

    def test_optional_readiness_phase_is_absolute_and_rejects_missing_or_late_release(self):
        with self.assertRaises(AssertionError):export.phase(self.home,'other')
        # An absent parent cannot turn the client barrier into successful status.
        with patch.object(export.time,'monotonic_ns',side_effect=[10,30_000_000_010]):
            with self.assertRaisesRegex(AssertionError,'timed out'):export.phase(self.home,'setup')
        marker=json.loads((self.home/'ordinary-setup.json').read_text())
        self.assertEqual(marker['deadline_ns'],30_000_000_010)
        (self.home/'ordinary-positive.continue').write_text('synthetic release')
        with patch.object(export.time,'monotonic_ns',side_effect=[10,30_000_000_010]):
            with self.assertRaisesRegex(AssertionError,'exceeded deadline'):export.phase(self.home,'positive')
        (self.home/'ordinary-narrow.continue').write_text('synthetic release')
        with patch.object(export.time,'monotonic_ns',side_effect=[100,200]):
            result=export.phase(self.home,'narrow',lambda:dict(independent='response'))
        self.assertEqual(result,dict(phase='narrow',started_ns=100,finished_ns=200,limit_seconds=30))
        self.assertEqual(json.loads((self.home/'ordinary-narrow.json').read_text())['payload'],dict(independent='response'))

    def test_closed_second_export_uses_distinct_key_exact_retries_and_changed_event_conflict(self):
        self.save()
        (self.home/'destination').mkdir();(self.home/'destination/destination.json').write_text('{}')
        (self.home/'event.jsonl').write_text(json.dumps(dict(id='56'*32)))
        (self.home/'conflict-event.jsonl').write_text(json.dumps(dict(id='78'*32)))
        (self.home/'export-input.json').write_text(json.dumps(dict(scope={},catalog_id='12'*16,destination_id='34'*16,
            retained_compacted_epoch2=True,retained_second_export=True,startup_quiescence=True)))
        receipt=dict(format='bigbrotr-job-acceptance/1',command_key='d2'*16,job_id='90'*16,
            accepted_at_us='100',created=True,execution='unobserved',authority='acceptance-only')
        for failure in (None,'retry-creates-work','conflict-succeeds','wrong-conflict-code'):
            requests=[];posts=[];me_calls=[]
            class Connection:
                def __init__(self,*args,**kwargs):pass
                def request(self,method,path,body,headers):
                    self.method,self.path=method,path
                    requests.append((method,path,None if body is None else json.loads(body),headers))
                def getresponse(self):
                    if self.path=='/management/v1/me':
                        me_calls.append(True);status=200 if len(me_calls)==1 else 401
                        value=dict(principal_id='ab'*16,stored_actions=OWNER_ACTIONS)
                    elif self.path=='/management/v1/jobs':
                        posts.append(requests[-1]);number=len(posts)
                        if number==1:status,value=202,receipt
                        elif number<4:
                            status,value=200,dict(receipt,created=failure=='retry-creates-work')
                        else:
                            status=200 if failure=='conflict-succeeds' else 409
                            value=dict(error=dict(status=status,code='other' if failure=='wrong-conflict-code' else 'command_conflict',retry_class='check_state'))
                    else:
                        assert self.path=='/management/v1/jobs/'+receipt['job_id']
                        status,value=200,dict(kind='export',state='succeeded',checkpoint_revision='1')
                    class Response:
                        def read(self,limit):return json.dumps(value).encode()
                    response=Response();response.status=status;return response
                def close(self):pass
            def cli(args,**kwargs):
                if 'login' in args:
                    target=Path(args[args.index('--session-output')+1])
                    target.write_text(json.dumps(dict(session='opaque',csrf='csrf')))
                return subprocess.CompletedProcess(args,0,b'{}',b'')
            with self.subTest(failure=failure),patch.object(export.http.client,'HTTPConnection',Connection), \
                    patch.object(export.subprocess,'run',cli),patch.object(export,'phase',return_value={}), \
                    patch.object(export.time,'monotonic_ns',return_value=10):
                if failure:
                    with self.assertRaises(AssertionError):export.run(self.home)
                else:
                    result=export.run(self.home)
                    self.assertEqual(result['job_id'],receipt['job_id']);self.assertIsNone(result['history'])
                    self.assertEqual(len(posts),4)
                    self.assertEqual([p[3]['Idempotency-Key'] for p in posts],['d2'*16]*4)
                    self.assertEqual([p[2]['query']['filters'][0]['ids'] for p in posts],[['56'*32]]*3+[['78'*32]])
                    self.assertFalse(any(method=='PUT' or path=='/v1/resources/event_history' for method,path,*_ in requests))

    def test_full_first_export_keeps_d1_through_all_grants_default_and_retained(self):
        self.save()
        (self.home/'destination').mkdir();(self.home/'destination/destination.json').write_text('{}')
        event=dict(id='56'*32);(self.home/'event.jsonl').write_text(json.dumps(event))
        receipt=dict(format='bigbrotr-job-acceptance/1',command_key='d1'*16,job_id='90'*16,
            accepted_at_us='100',created=True,execution='unobserved',authority='acceptance-only')
        for retained in (False,True):
            selected=dict(scope={},catalog_id='12'*16,destination_id='34'*16)
            if retained:selected.update(retained_compacted_epoch2=True,startup_quiescence=True)
            (self.home/'export-input.json').write_text(json.dumps(selected))
            state=dict(grants=0,accepted=False,logged_out=False);requests=[]
            class Connection:
                def __init__(self,*args,**kwargs):pass
                def request(self,method,path,body,headers):
                    self.method,self.path=method,path
                    self.body=None if body is None else json.loads(body)
                    requests.append((method,path,self.body,headers))
                def getresponse(self):
                    if self.path=='/management/v1/me':
                        status=401 if state['logged_out'] else 200
                        value=dict(principal_id='ab'*16,stored_actions=OWNER_ACTIONS)
                    elif self.path.endswith('/reauthenticate'):
                        status,value=201,dict(proof_id='cd'*16)
                    elif self.method=='PUT':
                        state['grants']+=1
                        status,value=200,dict(created=True,scope_generation='1',account_grant_revision=str(state['grants']+1))
                    elif self.path=='/v1/resources/event_history':
                        status=200 if state['grants']==3 else 403
                        value=dict(items=[event],next_cursor=None,view_stamp=dict(input_generations=[dict(kind='archive',
                            catalog_id='12'*16,frontier_revision='4',snapshot_id='68' if retained else '5')]))
                    elif self.path=='/management/v1/jobs':
                        if state['grants']<3:status,value=403,{}
                        elif not state['accepted']:
                            state['accepted']=True;status,value=202,receipt
                        else:status,value=200,dict(receipt,created=False)
                    else:
                        assert self.path=='/management/v1/jobs/'+receipt['job_id']
                        status,value=200,dict(kind='export',state='succeeded',checkpoint_revision='1')
                    class Response:
                        def read(self,limit):return json.dumps(value).encode()
                    response=Response();response.status=status;return response
                def close(self):pass
            def cli(args,**kwargs):
                if 'login' in args:
                    Path(args[args.index('--session-output')+1]).write_text(json.dumps(dict(session='opaque',csrf='csrf')))
                else:state['logged_out']=True
                return subprocess.CompletedProcess(args,0,b'{}',b'')
            with self.subTest(retained=retained),patch.object(export.http.client,'HTTPConnection',Connection), \
                    patch.object(export.subprocess,'run',cli),patch.object(export,'phase',return_value={}) as barrier, \
                    patch.object(export.time,'monotonic_ns',return_value=10):
                self.assertEqual(export.run(self.home)['status'],'PASS')
                self.assertEqual(state['grants'],3);self.assertTrue(state['logged_out'])
                self.assertEqual(barrier.call_count,int(retained))
                jobs=[r for r in requests if r[:2]==('POST','/management/v1/jobs')]
                self.assertEqual(len(jobs),6)
                self.assertEqual([r[3]['Idempotency-Key'] for r in jobs],['d1'*16]*6)
                grants=[r for r in requests if r[0]=='PUT']
                self.assertEqual([r[3]['Idempotency-Key'] for r in grants],[key*16 for key in ('c1','c2','c3')])
                self.assertEqual([r[2]['command']['action'] for r in grants],['history.read','export','private-export'])
                observations=json.loads((self.home/'export-observations.json').read_text())
                accepted,=[r for r in observations if r['case']=='accept-export']
                self.assertEqual(accepted['receipt'],receipt)
                self.assertEqual(accepted['key'],receipt['command_key'])


if __name__ == '__main__':
    unittest.main()
