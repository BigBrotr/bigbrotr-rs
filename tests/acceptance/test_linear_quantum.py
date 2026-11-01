"""Independent quantum evidence/refusal controls; no product or engine runs."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from cases import first_launch_quantum_oracle as check
from installed_sync_client import quantum_status


def fixture(*,pending=False):
    bx=lambda digit:'\\x'+digit*32
    predicate=b'["REQ","000000000000000000000000000000000000000000000000",{"kinds":[1],"since":0,"until":86399,"limit":3}]'
    ident='ab'*24;frame=json.dumps(['EOSE',ident],separators=(',',':')).encode()
    job=dict(dataset_id=bx('1'),job_id=bx('2'),created_at_us=200_000_999_999,
        owner_resource_id=bx('3'),owner_epoch=1,owner_incarnation=bx('4'))
    row=dict(quantum_id=bx('5'),quantum_profile=1,quantum_number=1,dataset_id=bx('1'),job_id=bx('2'),scope_id=bx('6'),
        producer_slot_id=bx('7'),producer_epoch=1,issuer_role='control',issuer_resource=bx('3'),
        issuer_epoch=1,issuer_incarnation=bx('4'),interval_lower=0,interval_upper=86400,horizon=200000,
        request_predicate='\\x'+predicate.hex(),predicate_sha256='\\x'+hashlib.sha256(predicate).hexdigest(),
        maximum_dependencies=192,checkpoint_before=3,terminal_kind=None if pending else 'eose',
        completed_at_us=None if pending else 200_002_000_000,dependencies=None if pending else '\\x',
        effect_records=None if pending else '\\x',terminal_frame=None if pending else '\\x'+frame.hex(),
        request_id=None if pending else '\\x'+ident.encode().hex(),terminal_sequence=None if pending else 1,
        retained_events=None if pending else 0,classified_prefix=None if pending else 0,
        checkpoint_after=None if pending else 3,continuation_upper=None if pending else 86400,
        result_class=None if pending else 'linear_observed',gap_reason=None,teardown_complete=None if pending else True,xmin='11')
    row['charged_bytes']=(1024+len(predicate)+2097152+3072+192*256 if pending else 1024+len(predicate)+len(frame))
    state=dict(jobs=[job],acquisition_jobs=[dict(job_id=job['job_id'],scope_id=row['scope_id'])],
        acquisition_quanta=[row],producer_requests=[],producer_checkpoints=[])
    wire=dict(number=1,request=['REQ',ident,dict(kinds=[1],since=0,until=86399,limit=3)],frames=['EOSE'],completion='request-and-websocket-close')
    return state,wire


class LinearQuantumControls(unittest.TestCase):
    def test_proxy_ready_failure_retains_custody_and_cleanup_joins_original_process(self):
        from cases import first_launch_linear_quantum as case
        vectors=(('normal',0,None,None,None,False,[]),
            ('already-exited',9,None,None,None,True,['join','observation']),
            ('closed-stdin',0,ValueError('write to closed file'),None,None,False,['control']),
            ('wait-error',-15,None,TimeoutError('proxy join deadline'),None,False,['join','observation']),
            ('stop-error',0,None,None,OSError('proxy stop failed'),False,['stop']))
        for name,code,control_error,wait_error,stop_error,overflow,operations in vectors:
            with self.subTest(name=name),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary)
                runtime=case.LinearQuantumRuntime(SimpleNamespace(evidence=root,
                    source_root=Path(__file__).resolve().parents[2],linear_quantum_fault='gap-write'),
                    dict(acceptance_files_sha256={}))
                runtime.home=root/'home';runtime.home.mkdir()
                original='[postgres.control]\nendpoint = "127.0.0.1:15432"\n'
                runtime.config=root/'instance.toml';runtime.config.write_text(original)
                order=[]
                child=SimpleNamespace(pid=123,returncode=code if name=='already-exited' else None)
                child.poll=lambda:child.returncode
                def wait(timeout):
                    if wait_error:raise wait_error
                    child.returncode=code
                    return code
                def stop():
                    child.returncode=code
                    if stop_error:raise stop_error
                stdout=b'proxy stopped\xff\n';stderr=b'proxy exited early\n' if name=='already-exited' else b''
                process=SimpleNamespace(process=child,wait=Mock(side_effect=wait),stop=Mock(side_effect=stop),
                    threads=[SimpleNamespace(is_alive=lambda:False)],stdout=stdout,stderr=stderr,overflow=overflow)
                def launch(*args,**kwargs):
                    self.assertEqual(order,['bootstrap']);order.append('proxy')
                    return process
                runtime.proxy_process=Mock(side_effect=launch)
                pending=dict(pid=123,status='running',port=None,upstream_endpoint='127.0.0.1:15432',
                    direct_recipe_sha256=hashlib.sha256(original.encode()).hexdigest())
                def ready(route,event,timeout):
                    self.assertIs(route,process);self.assertEqual((event,timeout),('ready',10))
                    self.assertEqual(order,['bootstrap','proxy'])
                    self.assertEqual(json.loads((root/'resources.json').read_text())['quantum_proxy'],pending)
                    self.assertEqual(json.loads((root/'quantum-proxy-process.json').read_text()),dict(pid=123))
                    raise TimeoutError('proxy ready deadline')
                runtime.proxy_event=ready
                terminal=dict(exit=code,stdout=stdout.decode(errors='replace'),stderr=stderr.decode(),
                    overflow=overflow,remote_exclusion='UNKNOWN')
                runtime.proxy_send=Mock(side_effect=control_error);runtime.proxy_observation=Mock(return_value=terminal)
                def base_bootstrap():
                    runtime.proxy_process.assert_not_called()
                    self.assertFalse(hasattr(runtime,'route'))
                    self.assertEqual(runtime.config.read_text(),original)
                    order.append('bootstrap')
                with patch.object(case.Runtime,'bootstrap',side_effect=base_bootstrap) as bootstrap:
                    with self.assertRaisesRegex(TimeoutError,'proxy ready deadline'):runtime.bootstrap()
                    bootstrap.assert_called_once_with()
                runtime.proxy_process.assert_called_once()
                self.assertEqual(runtime.proxy_process.call_args.kwargs,dict(input_pipe=True))
                if operations:
                    original=control_error or wait_error or stop_error
                    with self.assertRaises(type(original) if original else ValueError) as raised:
                        runtime.stop_quantum_proxy()
                    if original:self.assertIs(raised.exception,original)
                else:runtime.stop_quantum_proxy()
                if name=='already-exited':runtime.proxy_send.assert_not_called()
                else:runtime.proxy_send.assert_called_once_with(process,'stop\n')
                process.wait.assert_called_once_with(5);process.stop.assert_called_once_with()
                runtime.proxy_observation.assert_called_once_with(process)
                self.assertEqual((root/'quantum-proxy.stdout').read_bytes(),stdout)
                self.assertEqual((root/'quantum-proxy.stderr').read_bytes(),stderr)
                self.assertEqual(json.loads((root/'quantum-proxy.json').read_text()),terminal)
                self.assertEqual(json.loads((root/'resources.json').read_text())['quantum_proxy'],
                    dict(pending,status='joined',exit_code=code))
                self.assertEqual(json.loads((root/'quantum-proxy-process.json').read_text()),
                    dict(pid=123,exit_code=code,overflow=overflow,streams_joined=True))
                failures=json.loads((root/'resources.json').read_text())['cleanup_errors']
                self.assertEqual([item['operation'] for item in failures],operations)
                self.assertTrue(all(item['resource']=='quantum-proxy' and item['id']==123 for item in failures))
                if operations:
                    with self.assertRaisesRegex(ValueError,'owned resource cleanup incomplete'):runtime.cleanup()
                else:runtime.cleanup()
                process.wait.assert_called_once_with(5);process.stop.assert_called_once_with()

    def test_fault_route_records_direct_recipe_and_refuses_an_undeclared_upstream(self):
        from cases import first_launch_linear_quantum as case
        for endpoint in ('127.0.0.1:15432','127.0.0.1:25432'):
            with self.subTest(endpoint=endpoint),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary)
                runtime=case.LinearQuantumRuntime(SimpleNamespace(evidence=root,
                    source_root=Path(__file__).resolve().parents[2],linear_quantum_fault='plan-before'),
                    dict(acceptance_files_sha256={}))
                runtime.home=root/'home';runtime.home.mkdir()
                original='[postgres.control]\nendpoint = "'+endpoint+'"\n'
                runtime.config=root/'instance.toml';runtime.config.write_text(original)
                routed='[postgres.control]\nendpoint = "127.0.0.1:34567"\n'
                runtime.set_value=Mock(return_value=routed)
                runtime.proxy_process=Mock(return_value=SimpleNamespace(process=SimpleNamespace(pid=123)))
                def ready(route,event,timeout):
                    bootstrap.assert_called_once_with()
                    self.assertEqual(runtime.config.read_text(),original)
                    saved=json.loads((root/'resources.json').read_text())['quantum_proxy']
                    self.assertEqual((saved['upstream_endpoint'],saved['direct_recipe_sha256']),
                        ('127.0.0.1:15432',hashlib.sha256(original.encode()).hexdigest()))
                    return dict(port=34567,source_sha256=case.digest(runtime.source/'experiments/archive-engine/native_commit_proxy.py'))
                runtime.proxy_event=ready
                with patch.object(case.Runtime,'bootstrap') as bootstrap:
                    if endpoint!='127.0.0.1:15432':
                        with self.assertRaisesRegex(ValueError,'declared direct Control upstream'):runtime.bootstrap()
                        bootstrap.assert_called_once_with();runtime.proxy_process.assert_not_called()
                        runtime.set_value.assert_not_called();self.assertEqual(runtime.config.read_text(),original)
                        continue
                    runtime.bootstrap();bootstrap.assert_called_once_with()
                self.assertEqual(runtime.proxy_process.call_args.args[0][3:],[
                    '--mode','before-forward','--target','control-quantum-plan','--upstream-port','15432'])
                self.assertEqual(runtime.proxy_process.call_args.kwargs,dict(input_pipe=True))
                runtime.set_value.assert_called_once_with(original,'postgres.control','endpoint','127.0.0.1:34567')
                self.assertEqual(runtime.config.read_text(),routed)
                self.assertEqual(runtime.inputs['initial_recipe_sha256'],hashlib.sha256(routed.encode()).hexdigest())
                saved=json.loads((root/'resources.json').read_text())['quantum_proxy']
                self.assertEqual(saved['port'],34567)
                self.assertEqual(saved['direct_recipe_sha256'],hashlib.sha256(original.encode()).hexdigest())

    def test_control_expiry_consumes_frozen_sixty_second_lease_instead_of_fixed_twenty_seconds(self):
        from cases import first_launch_linear_quantum as case
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=object.__new__(case.LinearQuantumRuntime)
            runtime.config=root/'instance.toml';runtime.config.write_text('[runtime]\n')
            runtime.initialized=dict(dataset_id='11'*16,owner_resource_id='44'*16)  # Ingest is distinct.
            owner=dict(dataset_id='\\x'+'11'*16,resource_id='\\x'+'22'*16,role='control',epoch=2,
                process_incarnation='\\x'+'33'*16,state='active',acquired_at_us=999_000_000,
                lease_expires_at_us=1_060_000_000,xmin='7')
            job_owner=['\\x'+'11'*16,'\\x'+'22'*16,2,'\\x'+'33'*16]
            terminal=dict(Id='dead-app',State=dict(Running=False,Pid=0,OOMKilled=False))
            clock=[100.0]
            def sql(database,statement):
                self.assertEqual(database,'bigbrotr_control')
                self.assertIn('clock_timestamp()',statement);self.assertIn('active_unexpired',statement)
                now=1_000_000_000+round((clock[0]-100)*1_000_000)
                return json.dumps(dict(observed_at_us=now,owners=[owner],job_owners=[job_owner],active_unexpired=int(now<1_060_000_000)))
            runtime.sql=Mock(side_effect=sql)
            runtime.wait=Mock(wraps=runtime.wait)
            def advance(seconds):clock[0]+=seconds
            with patch.object(case.time,'monotonic',side_effect=lambda:clock[0]), \
                    patch.object(case.time,'sleep',side_effect=advance):
                runtime.freeze_control_lease(terminal,root)
                frozen=json.loads((root/'dead-control-lease.json').read_text())
                self.assertEqual((frozen['remaining_us'],frozen['deadline_monotonic']),(60_000_000,165.0))
                # Ten seconds of native verification consumes the same deadline.
                clock[0]=110.0
                runtime.wait_control_expiry(root)
            self.assertEqual(runtime.wait.call_args.args[1:],('dead original Control lease expiry',55.0))
            witness=json.loads((root/'dead-control-expiry.json').read_text())
            self.assertEqual(witness['status'],'PASS')
            self.assertGreater(witness['finished_monotonic']-witness['started_monotonic'],20)
            self.assertLessEqual(witness['finished_monotonic'],165.0)
            self.assertEqual(witness['last_observation']['owners'],[owner])
            self.assertEqual(witness['last_observation']['active_unexpired'],0)
            self.assertGreaterEqual(witness['last_observation']['observed_at_us'],1_060_000_000)

    def test_dead_control_expiry_refuses_changed_lease_or_out_of_profile_wait(self):
        from cases import first_launch_linear_quantum as case
        for name in ('beyond-maximum','changed-lease','clock-regressed','budget-exhausted','wrong-profile'):
            with self.subTest(name=name),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=object.__new__(case.LinearQuantumRuntime)
                runtime.config=root/'instance.toml'
                runtime.config.write_text('[runtime]\nlease_ttl = "'+('61s' if name=='wrong-profile' else '60s')+'"\n')
                runtime.initialized=dict(dataset_id='11'*16,owner_resource_id='44'*16)
                owner=dict(dataset_id='\\x'+'11'*16,resource_id='\\x'+'22'*16,role='control',epoch=2,
                    process_incarnation='\\x'+'33'*16,state='active',
                    lease_expires_at_us=1_060_000_001 if name=='beyond-maximum' else 1_060_000_000,xmin='7')
                terminal=dict(Id='dead-app',State=dict(Running=False,Pid=0,OOMKilled=False))
                sample=dict(observed_at_us=1_000_000_000,owners=[owner],
                    job_owners=[['\\x'+'11'*16,'\\x'+'22'*16,2,'\\x'+'33'*16]],active_unexpired=1)
                runtime.sql=Mock(return_value=json.dumps(sample));runtime.wait=Mock()
                clock=[100.0]
                with patch.object(case.time,'monotonic',side_effect=lambda:clock[0]):
                    if name in ('beyond-maximum','wrong-profile'):
                        with self.assertRaisesRegex(ValueError,'configured maximum|declared 60s'):
                            runtime.freeze_control_lease(terminal,root)
                        runtime.wait.assert_not_called()
                        continue
                    runtime.freeze_control_lease(terminal,root)
                    if name=='changed-lease':sample['owners']=[dict(owner,lease_expires_at_us=1_060_000_001)]
                    elif name=='clock-regressed':sample['observed_at_us']=999_999_999
                    else:clock[0]=166.0
                    runtime.sql.return_value=json.dumps(sample)
                    with self.assertRaisesRegex(ValueError,'lease changed|clock regressed|budget exhausted'):
                        runtime.wait_control_expiry(root)
                runtime.wait.assert_not_called()
                witness=json.loads((root/'dead-control-expiry.json').read_text())
                self.assertEqual(witness['status'],'FAIL')
                self.assertEqual(witness['deadline_monotonic'],165.0)

    def test_empty_terminal_has_charged_metadata_and_no_fabricated_effect_or_dependency(self):
        state,wire=fixture();check.shape(state);check.journey(state,[wire],1,{})
        self.assertEqual(state['acquisition_quanta'][0]['charged_bytes'],1189)
        self.assertEqual(state['producer_requests'],[])
        for field,value in [('charged_bytes',1188),('dependencies','\\x'+'00'*72),
                ('retained_events',1),('predicate_sha256','\\x'+'00'*32),('continuation_upper',0)]:
            changed=copy.deepcopy(state);changed['acquisition_quanta'][0][field]=value
            with self.subTest(field=field),self.assertRaises(AssertionError):check.journey(changed,[wire],1,{})

    def test_pending_reservation_survives_observation_and_cannot_be_refunded_or_relabelled(self):
        state,_=fixture(pending=True);check.shape(state)
        self.assertEqual(state['acquisition_quanta'][0]['charged_bytes'],2150506)
        for field,value in [('charged_bytes',1024),('horizon',200001),('issuer_epoch',2),('quantum_number',2)]:
            changed=copy.deepcopy(state);changed['acquisition_quanta'][0][field]=value
            with self.subTest(field=field),self.assertRaises(AssertionError):check.shape(changed)
        terminal,_=fixture();check.transition(state,terminal)
        changed=copy.deepcopy(terminal);changed['acquisition_quanta'][0]['scope_id']='\\x'+'88'*16
        with self.assertRaises(AssertionError):check.transition(state,changed)
        changed=copy.deepcopy(terminal);changed['acquisition_quanta'][0]['xmin']='12'
        with self.assertRaises(AssertionError):check.transition(terminal,changed)

    def test_wholly_preexisting_dependency_union_is_exact_and_deduplicated(self):
        ds=bytes.fromhex('11'*16)
        first=[[ds,0,1,7],bytes.fromhex('aa'*32)]
        second=[[ds,0,1,8],bytes.fromhex('bb'*32)]
        expected=bytes.fromhex('11'*16+'0000000000000000'+'0000000000000001'+'0000000000000007'+'aa'*32+
            '11'*16+'0000000000000000'+'0000000000000001'+'0000000000000008'+'bb'*32)
        self.assertEqual(check.reference_bytes([second,first,first]),expected)
        self.assertNotEqual(check.reference_bytes([second]),expected)
        self.assertNotEqual(check.reference_bytes([]),expected)

    def test_commit_proxy_targets_are_exact_and_do_not_match_reads_or_other_work(self):
        import importlib.util
        from pathlib import Path
        path=Path(__file__).resolve().parents[2]/'experiments/archive-engine/native_commit_proxy.py'
        spec=importlib.util.spec_from_file_location('linear_commit_proxy_control',path)
        proxy=importlib.util.module_from_spec(spec);spec.loader.exec_module(proxy)
        for target,query in [('control-quantum-plan',b'INSERT INTO control.acquisition_quanta(quantum_id,quantum_number,dataset_id)'),
                ('control-quantum-terminal',b'UPDATE control.acquisition_quanta SET terminal_kind=$2,terminal_frame=$3')]:
            self.assertTrue(proxy.control_target_query(target,query))
            for other in (b'SELECT * FROM control.acquisition_quanta',query.replace(b'acquisition_quanta',b'acquisition_progress'),
                    b'UPDATE control.jobs SET state=paused',b'INSERT INTO control.producer_requests('):
                self.assertFalse(proxy.control_target_query(target,other))

    def test_status_preserves_pending_and_rejects_false_coverage_or_hidden_proof_fields(self):
        status=dict(format='bigbrotr-linear-quantum-status/1',quantum_number='1',scope_id='66'*16,
            lower='0',upper='86400',horizon='200000',result='pending',terminal=None,gap=None,
            continuation_upper=None,retained_events=None,classified_prefix=None,coverage='pending')
        observed=dict(linear_quantum=status);quantum_status(observed,'66'*16,'200000999999',known_plan=True);self.assertEqual(observed,dict(linear_quantum=status))
        for field,value in [('coverage','complete'),('result','linear_observed'),('horizon','200001'),('dependencies',[])]:
            changed=dict(status,**{field:value})
            with self.subTest(field=field),self.assertRaises((AssertionError,TypeError)):
                quantum_status(dict(linear_quantum=changed),'66'*16,'200000999999',known_plan=True)


    def test_known_plan_cannot_disappear_and_bounded_scope_freezes_its_own_horizon(self):
        with self.assertRaisesRegex(AssertionError,'known durable plan'):
            quantum_status({},'66'*16,'200000999999',known_plan=True)
        value=dict(format='bigbrotr-linear-quantum-status/1',quantum_number='1',scope_id='66'*16,
            lower='0',upper='86400',horizon='86400',result='pending',terminal=None,gap=None,
            continuation_upper=None,retained_events=None,classified_prefix=None,coverage='pending')
        selector=dict(version=1,filters=[{'kinds':[1],'until':86399}],history_since=0,ephemeral='retain')
        observed=dict(linear_quantum=value)
        self.assertIs(quantum_status(observed,'66'*16,'200000999999',known_plan=True,selector=selector),value)
        with self.assertRaises(AssertionError):
            quantum_status(dict(linear_quantum=dict(value,horizon='200000')),'66'*16,'200000999999',known_plan=True,selector=selector)

    def test_malformed_eose_requires_a_gap_exact_window_predicate_and_teardown(self):
        state,wire=fixture();row=state['acquisition_quanta'][0];job=state['jobs'][0]
        row.update(terminal_sequence=3,retained_events=2,classified_prefix=0,
            result_class='interrupted',gap_reason='unclassified_suffix')
        wire.update(scenario='malformed_eose',frames=['EVENT','EVENT','EOSE'],
            terminal_frame_hex=check.raw(row['terminal_frame']).hex(),
            teardown=dict(request_close=True,websocket_close=True,ack_sent=True,eof=True),completion='request-and-websocket-close')
        event=dict(created_at=1)
        check.shape(state);check.scenario(row,wire,job,event,b'')
        mutations=[dict(result_class='linear_observed',gap_reason=None),dict(gap_reason='closed'),
            dict(teardown_complete=False),dict(horizon=199999),dict(terminal_sequence=2)]
        for changes in mutations:
            changed=dict(row,**changes)
            with self.subTest(changes=changes),self.assertRaises(AssertionError):check.scenario(changed,wire,job,event,b'')
        changed=dict(row)
        predicate=check.raw(row['request_predicate']).replace(b'"since":0',b'"since":1')
        changed.update(request_predicate='\\x'+predicate.hex(),predicate_sha256='\\x'+hashlib.sha256(predicate).hexdigest())
        altered=dict(state,acquisition_quanta=[changed]);check.shape(altered)
        with self.assertRaises(AssertionError):check.scenario(changed,wire,job,event,b'')

    def test_closed_terminal_requires_original_reason_text_and_no_synthetic_close_req(self):
        state,wire=fixture();row=state['acquisition_quanta'][0];job=state['jobs'][0]
        frame=check.wire_json(['CLOSED',wire['request'][1],'rate-limited'])
        old=len(check.raw(row['terminal_frame']))
        row.update(terminal_kind='closed',terminal_frame='\\x'+frame.hex(),result_class='interrupted',gap_reason='closed',
            charged_bytes=row['charged_bytes']+len(frame)-old)
        wire.update(scenario='closed',frames=['CLOSED'],terminal_frame_hex=frame.hex(),
            teardown=dict(request_close=False,websocket_close=True,ack_sent=True,eof=True),completion='request-and-websocket-close')
        check.scenario(row,wire,job,dict(created_at=1),b'')
        altered=dict(row,terminal_frame='\\x'+check.wire_json(['CLOSED',wire['request'][1],'other-reason']).hex())
        with self.assertRaises(AssertionError):check.scenario(altered,wire,job,dict(created_at=1),b'')
        altered_wire=dict(wire,teardown=dict(wire['teardown'],request_close=True))
        with self.assertRaises(AssertionError):check.scenario(row,altered_wire,job,dict(created_at=1),b'')

    def test_reservation_boundary_vectors_include_quanta_and_missing_head_allowance(self):
        # Independent arithmetic for the actual rollback-only SQL trigger probe;
        # executing that probe remains a root-owned PostgreSQL qualification.
        retained=16777216+65536
        vectors=((38912,2097152,14510080),(33792,2097152,14515200),(33792,0,16612352))
        for metadata,quanta,new_input in vectors:
            self.assertEqual(retained+metadata+quanta+new_input+65536,33554432)
            self.assertGreater(retained+metadata+quanta+new_input+1+65536,33554432)
            old_metadata=metadata if metadata==38912 else 32768
            self.assertLessEqual(retained+old_metadata+new_input+1+65536,33554432)

    def test_source_rollback_observer_requires_exact_query_completion_and_handles_cached_execute(self):
        import importlib.util
        from pathlib import Path
        path=Path(__file__).resolve().parents[2]/'experiments/archive-engine/native_commit_proxy.py'
        spec=importlib.util.spec_from_file_location('linear_rollback_proxy_control',path)
        proxy=importlib.util.module_from_spec(spec);spec.loader.exec_module(proxy)
        query=proxy.CONTROL_TARGETS['control-quantum-source-rollback']
        self.assertTrue(proxy.control_target_query('control-quantum-source-rollback',b'\n '+query.lower()))
        self.assertFalse(proxy.control_target_query('control-quantum-source-rollback',query+b' WHERE false'))
        observer=proxy.SessionCommandObserver('control-quantum-source-rollback',command_complete=b'SELECT 1\0')
        observer.frontend(b'P',b'q\0'+query+b'\0\0\0')
        for eligible,expected in ((True,True),(False,False)):
            observer.frontend(b'B',b'\0q\0');observer.frontend(b'E',bytes(5),eligible=eligible);observer.frontend(b'S',b'')
            self.assertEqual(observer.backend(b'C',b'SELECT 1\0'),expected)
            self.assertFalse(observer.backend(b'Z',b'T'))
        observer.frontend(b'Q',b'ROLLBACK\0')
        self.assertFalse(observer.backend(b'C',b'ROLLBACK\0'));self.assertFalse(observer.backend(b'Z',b'I'))

    def test_terminal_authority_refusal_must_be_returned_after_relay_delivery_not_timeout(self):
        report=dict(stage='quantum-terminal',reason='operation-error',entered_us=10,returned_us=20,dispatch_deadline_us=30)
        text='initial Sync unavailable: Source(Unavailable); sync_dispatch='+json.dumps(report)
        self.assertEqual(check.terminal_authority_diagnostic(text),report)
        for fields in (dict(stage='relay-collection'),dict(returned_us=None),dict(returned_us=30)):
            with self.subTest(fields=fields),self.assertRaises(AssertionError):
                check.terminal_authority_diagnostic(text.split('sync_dispatch=')[0]+'sync_dispatch='+json.dumps(dict(report,**fields)))
        with self.assertRaises(AssertionError):check.terminal_authority_diagnostic(text.replace('Source(Unavailable)','deadline'))

    def test_empty_terminal_withdrawal_retains_exact_plan_charge_and_science(self):
        before,wire=fixture(pending=True)
        before.update(producer_requests=[dict(request_sequence=n) for n in (1,2,3)],
            producer_outcomes=[dict(request_sequence=n) for n in (1,2,3)],acquisition_progress=[])
        after=copy.deepcopy(before)
        wire.update(scenario='empty',terminal_frame_hex=check.wire_json(['EOSE',wire['request'][1]]).hex(),
            teardown=dict(request_close=True,websocket_close=True,ack_sent=True,eof=True),completion='request-and-websocket-close')
        completed=dict(server_ready=True,backend_pid=42)
        held=dict(stage='source-rollback-reply',backend_pid=42,source_queries_complete=1,source_server_rollback=1,source_client_rollback=0)
        delivered=dict(backend_pid=42,source_server_rollback=1,source_client_rollback=1)
        check.empty_authority(before,after,wire,completed,held,delivered)
        for table,change in [('acquisition_quanta',lambda rows:rows[0].update(charged_bytes=1024)),
                ('acquisition_quanta',lambda rows:rows[0].update(continuation_upper=1)),
                ('producer_outcomes',lambda rows:rows.pop())]:
            altered=copy.deepcopy(after);change(altered[table])
            with self.subTest(table=table),self.assertRaises(AssertionError):
                check.empty_authority(before,altered,wire,completed,held,delivered)
        with self.assertRaises(AssertionError):
            check.empty_authority(before,after,wire,dict(completed,server_ready=False),held,delivered)
        with self.assertRaises(AssertionError):
            check.empty_authority(before,after,wire,completed,held,dict(delivered,source_client_rollback=0))
