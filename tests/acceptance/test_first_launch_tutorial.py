"""Tutorial location, owned sockets, complete peer and capability controls."""
import base64
import copy
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from cases import first_launch as base
from cases import first_launch_tutorial as case
from test_first_launch import ROOT, arguments


def peer_fixture(root):
    for n in range(1,4):
        before=dict(number=n,connected_at_us=n*1000,request_at_us=n*1000+1,
            clock_resolution_us=1,request=['REQ',f'{n:032x}'+'0'*16,dict(kinds=[1],limit=3)])
        after=dict(before,closed_at_us=n*1000+100,
            frames=['signed','forged-signature','duplicate','EOSE'] if n<=2 else [],
            completion='request-and-websocket-close' if n<=2 else 'quiet-eof')
        base.save(root/f'relay-open-{n:03}.json',before);base.save(root/f'relay-closed-{n:03}.json',after)


class TutorialControls(unittest.TestCase):
    def test_external_paths_accept_declared_checkout_but_refuse_relative_linked_or_checkout_destinations(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve();args=arguments(root/'evidence');args.candidate_manifest=root/'missing.json'
            with patch.object(case.Path,'cwd',return_value=root):
                value=case.canonical_inputs(args)
                self.assertEqual(value['source'],str(ROOT))
                for changed in (dict(evidence=Path('relative')),dict(source_root=Path('relative')),
                                dict(candidate_manifest=Path('relative')),dict(evidence=ROOT/'new-evidence')):
                    selected=SimpleNamespace(**vars(args));vars(selected).update(changed)
                    with self.subTest(changed=changed),self.assertRaises((ValueError,OSError)):
                        case.canonical_inputs(selected)
                link=root/'linked';link.symlink_to(root,target_is_directory=True)
                args.evidence=link/'evidence'
                with self.assertRaises(ValueError):case.canonical_inputs(args)
                args.evidence=root/'final-link';args.evidence.symlink_to(ROOT/'.research/uncreated-tutorial-evidence')
                with self.assertRaises(ValueError):case.canonical_inputs(args)
            args.evidence=root/'evidence'
            with patch.object(case.Path,'cwd',return_value=ROOT):
                with self.assertRaises(ValueError):case.canonical_inputs(args)

    def test_ambiguous_git_visibility_is_not_outside_checkout_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve();args=arguments(root/'evidence');args.candidate_manifest=root/'missing'
            with patch.object(case.Path,'cwd',return_value=root),patch.object(case.subprocess,'run',
                    return_value=SimpleNamespace(returncode=128,stderr='fatal: permission denied')):
                with self.assertRaises(ValueError):case.canonical_inputs(args)

    @unittest.skipUnless(sys.platform.startswith('linux'),'owned listener observation requires Linux /proc')
    def test_real_owned_listener_requires_exact_loopback_address_and_does_not_invent_absence(self):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0));listener.listen(1);port=listener.getsockname()[1]
            actual=case.owned_listeners(os.getpid())
            case.loopback_listener(actual,port)
            self.assertEqual(actual['pid'],os.getpid())
            with socket.socket() as extra:
                extra.bind(('0.0.0.0',0));extra.listen(1)
                with self.assertRaises(ValueError):case.loopback_listener(case.owned_listeners(os.getpid()),port)
        with self.assertRaises((ValueError,OSError)):case.owned_listeners(2**31-1)
        with self.assertRaises(ValueError):case.owned_listeners(True)
        with patch.object(case.Path,'iterdir',side_effect=PermissionError('inaccessible owned descriptors')):
            with self.assertRaises(PermissionError):case.owned_listeners(os.getpid())

    def test_capability_refusal_rejects_false_ready_usage_and_unrelated_errors(self):
        original=subprocess.CompletedProcess([],3,'','error: '+case.UNWIRED+'\n')
        case.refusal(original,case.UNWIRED)
        for result in (subprocess.CompletedProcess([],0,'{"ready":true}',''),
                subprocess.CompletedProcess([],3,'{"state":"ready"}','error: '+case.UNWIRED+'\n'),
                subprocess.CompletedProcess([],2,'','usage: missing option'),
                subprocess.CompletedProcess([],3,'','error: file unavailable\n')):
            with self.assertRaises(ValueError):case.refusal(result,case.UNWIRED)

    def test_negative_driver_changes_only_declared_inputs_and_never_bootstraps_after_wrong_refusal(self):
        from test_first_launch_index import set_value
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=object.__new__(case.TutorialRuntime)
            runtime.root=root;runtime.state=root/'state';runtime.state.mkdir()
            runtime.bundle=root/'bundle';runtime.config=root/'instance.toml'
            original=(ROOT/'config/recipes/acceptance.toml').read_text();runtime.config.write_text(original)
            runtime.set_value=set_value;runtime.files=Mock(return_value={})
            seen=[]
            def command(argv,**options):
                document=tomllib.loads(Path(argv[argv.index('--file')+1]).read_text());seen.append(options['label'])
                if options['label']=='unwired-static':
                    expected=tomllib.loads(original)
                    expected['topology']['local_roles']+=['monitor'];expected['monitor']['enabled']=True
                    self.assertEqual(document,expected)
                    return subprocess.CompletedProcess(argv,0,'{"signing":false}','')
                if options['label']=='unwired-role':
                    self.assertEqual(argv[-2:],['--roles','control,monitor'])
                    return subprocess.CompletedProcess(argv,3,'','error: '+case.UNWIRED+'\n')
                self.assertEqual(options['label'],'active-write-refusal')
                expected=tomllib.loads(original);expected['monitor']['active_write']={'enabled':True}
                self.assertEqual(document,expected)
                return subprocess.CompletedProcess(argv,3,'','error: '+case.ACTIVE_WRITE+'\n')
            runtime.command=command
            connection=Mock();connection.__enter__=Mock(return_value=connection);connection.__exit__=Mock()
            with patch.object(case.Runtime,'bootstrap') as bootstrap,patch.object(case.socket,'socket',return_value=connection):
                runtime.bootstrap();bootstrap.assert_called_once()
            self.assertEqual(seen,['unwired-static','unwired-role','active-write-refusal'])
            self.assertEqual(runtime.config.read_text(),original)
            failed=root/'failure';failed.mkdir();runtime.root=failed
            runtime.command=Mock(return_value=subprocess.CompletedProcess([],0,'{"signing":false}',''))
            with patch.object(case.Runtime,'bootstrap') as bootstrap:
                with self.assertRaises(ValueError):runtime.bootstrap()
                bootstrap.assert_not_called()

    def test_listener_driver_binds_owned_pid_and_actual_postgres_loopback_mapping(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=object.__new__(case.TutorialRuntime)
            runtime.pg={'id':'pg'};entry={'id':'app'}
            mapping={'5432/tcp':[dict(HostIp='127.0.0.1',HostPort='15432')]}
            app=dict(State=dict(Pid=123,Running=True))
            pg=dict(Id='pg',State=dict(Running=True),HostConfig=dict(PortBindings=copy.deepcopy(mapping)),
                    NetworkSettings=dict(Ports=copy.deepcopy(mapping)))
            runtime.inspect=Mock(side_effect=[app,app,pg])
            connection=Mock();connection.__enter__=Mock(return_value=connection);connection.__exit__=Mock()
            connection.getpeername.return_value=('127.0.0.1',15432)
            observation=dict(pid=123,start_ticks=456,listeners=[dict(address='127.0.0.1',port=18081,family='tcp')])
            with patch.object(case,'owned_listeners',return_value=observation), \
                    patch.object(case.socket,'create_connection',return_value=connection):
                runtime.listener_observation(entry,root)
                for invalid in (dict(app,State=dict(Pid=124,Running=True)),):
                    runtime.inspect=Mock(side_effect=[app,invalid])
                    with self.assertRaises(ValueError):runtime.listener_observation(entry,root)
                wrong=copy.deepcopy(pg);wrong['NetworkSettings']['Ports']['5432/tcp'][0]['HostIp']='0.0.0.0'
                runtime.inspect=Mock(side_effect=[app,app,wrong])
                with self.assertRaises(ValueError):runtime.listener_observation(entry,root)

    def test_peer_evidence_is_nonempty_complete_and_bound_to_every_accepted_connection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);peer_fixture(root)
            self.assertEqual(case.transcript(root)['connections'],3)
            for marker in ('relay-error.json','relay-shutdown.json'):
                path=root/marker;base.save(path,dict(error='incomplete frame'))
                with self.subTest(marker=marker),self.assertRaises(ValueError):case.transcript(root)
                path.unlink()
            path=root/'relay-closed-003.json';raw=path.read_bytes();path.unlink()
            with self.assertRaises(ValueError):case.transcript(root)
            path.write_bytes(raw)
            last=base.load(path)
            for change in (dict(completion='incomplete-handshake'),dict(request=['EVENT',{}]),
                           dict(frames=['unexpected']),dict(number=4)):
                base.save(path,dict(last,**change))
                with self.subTest(change=change),self.assertRaises(ValueError):case.transcript(root)
            base.save(path,last)
            (root/'relay-closed-002.json').rename(root/'relay-closed-004.json')
            with self.assertRaises(ValueError):case.transcript(root)

    def test_real_peer_rejects_masked_outbound_event_and_cannot_report_zero_publication(self):
        sys.path.insert(0,str(ROOT/'experiments/ingest-bootstrap'))
        from installed_sync_client import relay
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);errors=[];finished=threading.Event()
            with socket.socket() as listener:
                listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(.1)
                thread=threading.Thread(target=relay,args=(root,listener,finished,errors,True));thread.start()
                try:
                    with socket.create_connection(listener.getsockname(),timeout=1) as connection:
                        key=base64.b64encode(bytes(range(16))).decode()
                        connection.sendall(('GET /installed-fixture HTTP/1.1\r\nHost: 127.0.0.1:7447\r\n'
                            'Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n'
                            'Sec-WebSocket-Key: '+key+'\r\n\r\n').encode())
                        header=b''
                        while not header.endswith(b'\r\n\r\n'):
                            self.assertLess(len(header),4096);header+=connection.recv(1)
                        self.assertTrue(header.startswith(b'HTTP/1.1 101'))
                        raw=json.dumps(['EVENT',dict(id='00'*32)],separators=(',',':')).encode();mask=b'abcd'
                        self.assertLess(len(raw),126)
                        connection.sendall(bytes((0x81,0x80|len(raw)))+mask+bytes(c^mask[n%4] for n,c in enumerate(raw)))
                    thread.join(4);self.assertFalse(thread.is_alive())
                    self.assertTrue(errors);self.assertTrue((root/'relay-error.json').is_file())
                    with self.assertRaises(ValueError):case.transcript(root)
                finally:
                    finished.set();thread.join(4)

    def test_documentation_only_product_revision_can_reuse_reviewed_code_but_changed_code_refuses(self):
        runtime=object.__new__(case.TutorialRuntime);runtime.source=ROOT;runtime.inputs={}
        revision=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True).strip()
        runtime.manifest=dict(artifact=dict(source=dict(revision=revision)))
        runtime.command=lambda command,**_:subprocess.run(command,capture_output=True,text=True,check=True)
        with patch.object(case.Runtime,'candidate'):
            runtime.candidate()
        self.assertEqual(runtime.inputs['nostr_source_exclusion']['product_revision'],revision)
        self.assertEqual(runtime.inputs['nostr_source_exclusion']['reviewed_revision'],case.REVIEWED_PRODUCT)
        runtime.command=Mock(return_value=SimpleNamespace(stdout='0'*40+'\n'))
        with patch.object(case.Runtime,'candidate'):
            with self.assertRaisesRegex(ValueError,'changed product code'):runtime.candidate()

    def test_dispatch_preserves_original_seven_stages_and_cleanup_after_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);args=arguments(root);args.case='p01-first-launch-tutorial';checks=[]
            with patch.object(case,'TutorialRuntime') as runtime:
                runtime.return_value.initial.side_effect=ValueError('incomplete real peer')
                base.first_launch(args,checks,{})
                runtime.return_value.cleanup.assert_called_once()
                runtime.return_value.ordinary.assert_not_called()
            self.assertEqual(len(checks),7)
            self.assertEqual(checks[4]['result'],'FAIL');self.assertEqual(checks[5]['result'],'NOT_RUN')
            self.assertEqual(base.load(root/'failure.json')['reason'],'incomplete real peer')

    def test_cli_outside_checkout_missing_candidate_is_not_run_without_docker(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve()
            result=subprocess.run([sys.executable,'-B',str(ROOT/'tests/acceptance/run.py'),
                'p01-first-launch-tutorial','--source-root',str(ROOT),'--candidate-manifest',str(root/'missing'),
                '--evidence',str(root/'evidence')],cwd=root,capture_output=True,text=True,timeout=5)
            self.assertEqual(result.returncode,2,result.stderr)
            report=base.load(root/'evidence/result.json')
            self.assertEqual(report['result'],'NOT_RUN');self.assertEqual(report['counts']['collected'],7)
            self.assertFalse(list((root/'evidence/commands').iterdir()))


if __name__=='__main__':unittest.main()
