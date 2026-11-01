"""Independent quantum evidence/refusal controls; no product or engine runs."""
import copy
import base64
import hashlib
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from cases import first_launch_quantum_oracle as check
from installed_sync_client import quantum_status


def fixture(*,pending=False,abandoned=False,producer_epoch=1):
    bx=lambda digit:'\\x'+digit*32
    predicate=b'["REQ","000000000000000000000000000000000000000000000000",{"kinds":[1],"since":0,"until":86399,"limit":3}]'
    ident='ab'*24;frame=json.dumps(['EOSE',ident],separators=(',',':')).encode()
    job=dict(dataset_id=bx('1'),job_id=bx('2'),created_at_us=200_000_999_999,
        owner_resource_id=bx('3'),owner_epoch=1,owner_incarnation=bx('4'),owner_role='control',
        kind='acquisition',acquisition_job_protocol=1,state='queued',job_revision=2,checkpoint_revision=0,
        result_manifest_digest=None,reason_code=None,completed_at_us=None)
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
    state=dict(jobs=[job],acquisition_jobs=[dict(dataset_id=job['dataset_id'],job_id=job['job_id'],scope_id=row['scope_id'])],
        acquisition_quanta=[row],producer_requests=[],producer_checkpoints=[],acquisition_gaps=[],
        owner_epochs=[dict(dataset_id=job['dataset_id'],resource_id=bx('3'),epoch=1,role='control',
            process_incarnation=bx('4'),state='active',exclusion_method=None)],
        producer_epochs=[dict(dataset_id=job['dataset_id'],producer_slot_id=bx('7'),producer_epoch=1)])
    if abandoned:
        state['owner_epochs'][0].update(state='released',exclusion_method='storage_fenced')
        state['owner_epochs'].append(dict(dataset_id=bx('1'),resource_id=bx('3'),epoch=2,role='control',
            process_incarnation=bx('8'),state='released',exclusion_method='joined'))
        row['producer_epoch']=state['producer_epochs'][0]['producer_epoch']=producer_epoch
        reason=('initial_producer_abandoned' if producer_epoch==1 else
            'activated_initial_producer_abandoned' if producer_epoch==2 else 'ingest_successor_abandoned')
        job.update(state='cancelled',job_revision=4,reason_code=reason,completed_at_us=200_003_000_000,
            owner_epoch=2,owner_incarnation=bx('8'))
        state['acquisition_gaps']=[dict(job_id=bx('2'),dataset_id=bx('1'),producer_slot_id=bx('7'),
            producer_epoch=producer_epoch,source_owner_epoch=producer_epoch,owner_role='control',
            previous_job_revision=2,previous_owner_resource=bx('3'),previous_owner_epoch=1,
            previous_owner_incarnation=bx('4'),issuer_resource=bx('3'),issuer_epoch=2,
            issuer_incarnation=bx('8'),abandoned_at_us=200_003_000_000,reason=reason)]
    wire=dict(number=1,request=['REQ',ident,dict(kinds=[1],since=0,until=86399,limit=3)],frames=['EOSE'],completion='request-and-websocket-close')
    return state,wire


def successor_fixture(provenance):
    from test_first_launch_restart import fixture as signed_science
    old,new,_,encoded,_=signed_science(provenance)
    first=encoded.first;science=dict(receipts=list(encoded.receipts.values()))
    state,wire=fixture();row=state['acquisition_quanta'][0]
    for rows in state.values():
        for value in rows:
            if 'dataset_id' in value:value['dataset_id']=encoded.m.ds['dataset_id']
    row['producer_epoch']=state['producer_epochs'][0]['producer_epoch']=2
    wire['frames']=['distinct','predecessor','distinct-resend','EOSE']
    frames=[(new,2,2),(old,2,3) if provenance else (old,5,0),(new,5,0)]
    records=b''.join(number.to_bytes(2,'big')+number.to_bytes(8,'big')+
        hashlib.sha256(check.wire_json(event)).digest()+kind.to_bytes(2,'big')+
        request.to_bytes(8,'big')+event['created_at'].to_bytes(8,'big')+b'\x01'+bytes.fromhex(event['id'])
        for number,(event,kind,request) in enumerate(frames,1))
    refs=check.receipt_refs(science['receipts'][1])
    refs+=check.receipt_refs(science['receipts'][2]) if provenance else (
        check.receipt_refs(first['receipts'][3])+check.receipt_refs(science['receipts'][0]))
    dependencies=check.reference_bytes(refs)
    times=[event['created_at'] for event,_,_ in frames]
    gap=None if all(later<=earlier for earlier,later in zip(times,times[1:])) else 'ordering'
    row.update(terminal_sequence=4,retained_events=3,classified_prefix=3,effect_records='\\x'+records.hex(),
        dependencies='\\x'+dependencies.hex(),checkpoint_before=0,checkpoint_after=4,
        gap_reason=gap,result_class='interrupted' if gap else 'linear_observed',
        continuation_upper=row['interval_upper'] if gap else min(times)+1)
    row['charged_bytes']=1024+len(check.raw(row['request_predicate']))+len(check.raw(row['terminal_frame']))+len(records)+256*(len(dependencies)//72)
    return state,wire,old,new,science,first


def signed_pages_fixture():
    from test_first_launch_restart import fixture as signed_science
    event,_,_,encoded,_=signed_science()
    science=encoded.first;state,_=fixture();rows=[];wires=[]
    for values in state.values():
        for value in values:
            if 'dataset_id' in value:value['dataset_id']=encoded.m.ds['dataset_id']
    original=state['acquisition_quanta'][0]
    refs=check.reference_bytes(check.receipt_refs(science['receipts'][3]))
    for number,upper in ((1,86400),(2,event['created_at']+1)):
        ident=f'{number:048x}';frame=check.wire_json(['EOSE',ident])
        predicate=check.wire_json(['REQ','0'*48,dict(kinds=[1],since=0,until=upper-1,limit=3)])
        bodies=(event,dict(event,sig='00'*64),event)
        kinds=((2,4),(6,0),(5,0)) if number==1 else ((5,0),(6,0),(5,0))
        records=b''.join(ordinal.to_bytes(2,'big')+ordinal.to_bytes(8,'big')+
            hashlib.sha256(check.wire_json(body)).digest()+kind.to_bytes(2,'big')+
            request.to_bytes(8,'big')+body['created_at'].to_bytes(8,'big')+b'\x01'+bytes.fromhex(body['id'])
            for ordinal,(body,(kind,request)) in enumerate(zip(bodies,kinds),1))
        rows.append(dict(original,quantum_id='\\x'+f'{number:032x}',quantum_number=number,
            interval_upper=upper,request_predicate='\\x'+predicate.hex(),predicate_sha256='\\x'+hashlib.sha256(predicate).hexdigest(),
            terminal_frame='\\x'+frame.hex(),request_id='\\x'+ident.encode().hex(),terminal_sequence=4,
            retained_events=3,classified_prefix=3,dependencies='\\x'+refs.hex(),effect_records='\\x'+records.hex(),
            continuation_upper=event['created_at']+1,charged_bytes=1024+len(predicate)+len(frame)+len(records)+256*(len(refs)//72)))
        wires.append(dict(number=number,request=['REQ',ident,dict(kinds=[1],since=0,until=upper-1,limit=3)],
            frames=['signed','forged-signature','duplicate','EOSE'],completion='request-and-websocket-close'))
    state['acquisition_quanta']=rows
    return state,wires,event,science


class LinearQuantumControls(unittest.TestCase):
    def test_finite_scenarios_keep_declared_scope_identity_and_continued_bounds(self):
        import linear_quantum_transport as peer
        root=Path(__file__).resolve().parents[2]
        for failure in (None,'stale-upper','wrong-scope'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as temporary:
                directory=Path(temporary)
                (directory/'event.jsonl').write_bytes((root/'tests/fixtures/ingest/import-event.jsonl').read_bytes())
                (directory/'linear-quantum-profile.json').write_text(json.dumps(peer.PROFILE))
                class Connection:
                    def __init__(self,number):
                        self.number=number;self.frames=0
                        self.header=bytearray(('GET /installed-fixture HTTP/1.1\r\nHost: 127.0.0.1:7447\r\n'
                            'Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n'
                            'Sec-WebSocket-Key: '+base64.b64encode(bytes(16)).decode()+'\r\n\r\n').encode())
                    def __enter__(self):return self
                    def __exit__(self,*args):pass
                    def settimeout(self,value):pass
                    def sendall(self,value):pass
                    def shutdown(self,value):pass
                    def recv(self,count):
                        result=bytes(self.header[:count]);del self.header[:count];return result
                    def frame(self):
                        self.frames+=1;ident=f'{self.number:032x}'+'0'*16
                        if self.frames==1:
                            scope=peer.TURN_SCOPES[self.number-1]
                            upper=86400 if self.number==1 or scope else 2
                            if failure=='stale-upper' and self.number==3:upper=86400
                            if failure=='wrong-scope' and self.number==2:scope=0
                            return 1,check.wire_json(['REQ',ident,dict(kinds=[1],since=scope,until=upper-1,limit=3)])
                        if self.frames==2 and self.number!=6:return 1,check.wire_json(['CLOSE',ident])
                        return 8,b''
                listener=Mock();listener.accept.side_effect=[(Connection(number),('127.0.0.1',12345)) for number in range(1,9)]
                errors=[];uppers={0:{1:86400},1:{1:86400}}
                with patch.object(peer,'read_frame',side_effect=lambda connection:connection.frame()), \
                        patch.object(peer,'send_frame'),patch.object(peer.time,'sleep'):
                    peer.relay(directory,listener,threading.Event(),errors,expected_uppers=uppers)
                listener.close.assert_called_once()
                self.assertEqual(bool(errors),failure is not None)
                self.assertEqual((directory/'linear-peer-complete.json').exists(),failure is None)
                if failure is None:
                    wires=[json.loads(path.read_text()) for path in sorted(directory.glob('relay-closed-*.json'))]
                    self.assertEqual([wire['scenario'] for wire in wires],list(peer.SCENARIOS))
                    self.assertEqual([wire['request'][2]['until'] for wire in wires],[86399,86399,1,86399,86399,86399,86399,86399])
                    self.assertEqual([wire['scope_number'] for wire in wires],[0,1,0,1,1,1,1,1])

    def test_signed_pages_overlap_once_then_refuse_an_extra_plan_or_socket(self):
        state,wires,event,science=signed_pages_fixture()
        check.initial(state,wires,event,science)
        self.assertEqual([row['interval_upper'] for row in state['acquisition_quanta']],[86400,2])
        for change in ('upper','predicate','third-plan','third-socket'):
            changed=copy.deepcopy(state);transcript=copy.deepcopy(wires)
            if change=='upper':changed['acquisition_quanta'][1]['interval_upper']=86400
            elif change=='predicate':
                predicate=check.raw(changed['acquisition_quanta'][1]['request_predicate']).replace(b'"until":1',b'"until":86399')
                changed['acquisition_quanta'][1]['request_predicate']='\\x'+predicate.hex()
                changed['acquisition_quanta'][1]['predicate_sha256']='\\x'+hashlib.sha256(predicate).hexdigest()
            else:
                changed['acquisition_quanta'].append(dict(changed['acquisition_quanta'][1],
                    quantum_id='\\x'+f'{3:032x}',quantum_number=3))
                if change=='third-socket':transcript.append(dict(transcript[-1],number=3))
            with self.subTest(change=change),self.assertRaises(AssertionError):check.initial(changed,transcript,event,science)

    def test_successful_empty_defers_without_an_extra_plan(self):
        state,wire=fixture();check.journey(state,[wire],1,{})
        extra=copy.deepcopy(state['acquisition_quanta'][0]);extra.update(quantum_id='\\x'+'9'*32,quantum_number=2)
        state['acquisition_quanta'].append(extra)
        with self.assertRaisesRegex(AssertionError,'stopped bounded traversal'):check.journey(state,[wire],1,{})

    def test_closed_page_retries_exact_interval_before_successful_overlap(self):
        state,wires,event,science=signed_pages_fixture()
        first,second=state['acquisition_quanta']
        closed=check.wire_json(['CLOSED',wires[0]['request'][1],'fixture setup interruption'])
        first['charged_bytes']+=len(closed)-len(check.raw(first['terminal_frame']))
        first.update(terminal_kind='closed',terminal_frame='\\x'+closed.hex(),gap_reason='closed',
            result_class='interrupted',continuation_upper=86400)
        wires[0]['frames'][-1]='CLOSED'
        predicate=check.raw(first['request_predicate'])
        second['charged_bytes']+=len(predicate)-len(check.raw(second['request_predicate']))
        second.update(interval_upper=86400,request_predicate='\\x'+predicate.hex(),
            predicate_sha256='\\x'+hashlib.sha256(predicate).hexdigest())
        wires[1]['request'][2]['until']=86399
        pending=fixture(pending=True)[0]['acquisition_quanta'][0]
        predicate=check.wire_json(['REQ','0'*48,dict(kinds=[1],since=0,until=1,limit=3)])
        pending.update(dataset_id=first['dataset_id'],quantum_number=3,quantum_id='\\x'+f'{3:032x}',
            interval_upper=2,request_predicate='\\x'+predicate.hex(),predicate_sha256='\\x'+hashlib.sha256(predicate).hexdigest(),
            charged_bytes=1024+len(predicate)+2097152+3072+192*256)
        state['acquisition_quanta'].append(pending)
        check.initial(state,wires,event,science)
        changed=copy.deepcopy(state);changed['acquisition_quanta'][1]['interval_upper']=2
        with self.assertRaises(AssertionError):check.initial(changed,wires,event,science)

    def test_installed_stall_validator_requires_both_terminal_pages_and_exact_bounds(self):
        from installed_sync import validate_stall
        state,wires,event,_=signed_pages_fixture();rows=state['acquisition_quanta'];job=state['jobs'][0]['job_id'][2:]
        self.assertEqual(validate_stall(rows,wires,event,job)['coverage'],'pending')
        for field,value in (('interval_upper',86400),('terminal_kind',None),('classified_prefix',2),
                ('continuation_upper',1),('request_id',rows[0]['request_id'])):
            changed=copy.deepcopy(rows);changed[-1][field]=value
            with self.subTest(field=field),self.assertRaises(AssertionError):validate_stall(changed,wires,event,job)
        with self.assertRaises(AssertionError):validate_stall(rows[:1],wires[:1],event,job)

    def test_status_uses_declared_query_ordinal_bounds_and_rejects_an_old_upper(self):
        status=dict(format='bigbrotr-linear-quantum-status/1',quantum_number='2',scope_id='66'*16,
            lower='0',upper='2',horizon='200000',result='linear_observed',terminal='eose',gap=None,
            continuation_upper='2',retained_events=3,classified_prefix=3,coverage='pending')
        quantum_status(dict(linear_quantum=status),'66'*16,'200000999999',known_plan=True,expected_uppers={1:86400,2:2})
        with self.assertRaises(AssertionError):
            quantum_status(dict(linear_quantum=dict(status,upper='86400')),'66'*16,'200000999999',
                known_plan=True,expected_uppers={1:86400,2:2})
        with self.assertRaises(KeyError):
            quantum_status(dict(linear_quantum=dict(status,quantum_number='3')),'66'*16,'200000999999',
                known_plan=True,expected_uppers={1:86400,2:2})

    def test_successor_on_off_quantum_requires_exact_effects_and_dependency_union(self):
        for provenance in (True,False):
            state,wire,old,new,science,first=successor_fixture(provenance)
            def verify(value):
                check.successor(value,[wire],old,new,science,before={'acquisition_quanta':[]},
                    provenance=provenance,first_science=first)
            verify(state)
            for change in ('classification','event-digest','dependency-digest','canonical-event-id','event-time'):
                changed=copy.deepcopy(state);row=changed['acquisition_quanta'][0]
                key='dependencies' if change=='dependency-digest' else 'effect_records'
                body=bytearray(check.raw(row[key]))
                at=40 if change=='dependency-digest' else 93+dict(classification=43,**{'event-digest':10,'canonical-event-id':61,'event-time':52})[change]
                body[at]^=1;row[key]='\\x'+body.hex()
                with self.subTest(provenance=provenance,change=change),self.assertRaises(AssertionError):verify(changed)
            old_format=copy.deepcopy(state);records=check.raw(old_format['acquisition_quanta'][0]['effect_records'])
            old_format['acquisition_quanta'][0]['effect_records']='\\x'+b''.join(records[at:at+61] for at in range(0,len(records),93)).hex()
            old_format['acquisition_quanta'][0]['charged_bytes']-=96
            with self.assertRaises(AssertionError):verify(old_format)
            if not provenance:
                row=state['acquisition_quanta'][0];raw=check.raw(row['dependencies'])
                parts=[raw[at:at+72] for at in range(0,len(raw),72)]
                coordinates=lambda part:(int.from_bytes(part[24:32],'big'),int.from_bytes(part[32:40],'big'))
                self.assertEqual({coordinates(part) for part in parts},
                    {(1,n) for n in range(3,9)}|{(2,n) for n in range(1,6)})
                for missing in ((1,4),(1,8),(2,1),(2,5)):
                    changed=copy.deepcopy(state);current=changed['acquisition_quanta'][0]
                    current['dependencies']='\\x'+b''.join(part for part in parts if coordinates(part)!=missing).hex()
                    current['charged_bytes']-=256
                    with self.subTest(missing=missing),self.assertRaises(AssertionError):verify(changed)

    def test_successor_old_frame_declaration_excludes_new_event_dependencies(self):
        for provenance in (True,False):
            state,wire,old,new,science,first=successor_fixture(provenance)
            with patch.object(check,'journey') as journey:
                check.successor(state,[wire],old,new,science,before={'acquisition_quanta':[]},
                    provenance=provenance,first_science=first)
            declared=journey.call_args.args[3][1]
            self.assertEqual(declared[1][:3],(old,2,3) if provenance else (old,5,0))
            if provenance:
                self.assertEqual(check.reference_bytes(declared[1][3]),check.reference_bytes(check.receipt_refs(science['receipts'][2])))
            else:
                refs=declared[1][3]
                self.assertEqual(len(refs),7)
                self.assertEqual({(source[2],source[3]) for source,_ in refs},
                    {(1,n) for n in range(3,9)}|{(2,1)})
                with self.assertRaises(AssertionError):
                    check.successor(state,[wire],old,new,science,before={'acquisition_quanta':[]},
                        provenance=False)

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
                    return dict(runtime.quantum_proxy_binding,event='ready',port=34567)
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

    def test_fault_profiles_keep_plan_default_and_bound_body_terminal_and_authority_captures(self):
        from cases import first_launch_linear_quantum as case
        vectors=(('plan-before','before-forward','plan','default',20000,16),
            ('plan-after','after-complete','plan','default',20000,16),
            ('terminal-before','before-forward','terminal','retained-catch-up',131072,64),
            ('terminal-after','after-complete','terminal','retained-catch-up',131072,64),
            ('gap-write','hold-reply','plan','retained-catch-up',131072,64),
            ('stale-authority','hold-reply','plan','retained-catch-up',131072,64),
            ('empty-terminal-authority','hold-source-rollback-reply','source-rollback','retained-catch-up',131072,64))
        for fault,mode,target,profile,frames,connections in vectors:
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary)
                runtime=case.LinearQuantumRuntime(SimpleNamespace(evidence=root,
                    source_root=Path(__file__).resolve().parents[2],linear_quantum_fault=fault),
                    dict(acceptance_files_sha256={}))
                runtime.home=root/'home';runtime.home.mkdir()
                runtime.config=root/'instance.toml'
                runtime.config.write_text('[postgres.control]\nendpoint = "127.0.0.1:15432"\n')
                runtime.set_value=Mock(return_value='[postgres.control]\nendpoint = "127.0.0.1:34567"\n')
                source=runtime.source/'experiments/archive-engine/native_commit_proxy.py'
                binding=dict(source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),mode=mode,
                    target='control-quantum-'+target,profile=profile,frame_limit=frames,connection_limit=connections,
                    connection_limit_scope='lifetime',wire_limit=67108864,lifetime_seconds=120,
                    upstream_host='127.0.0.1',upstream_port=15432)
                ready=dict(binding,event='ready',port=34567)
                runtime.proxy_process=Mock(return_value=SimpleNamespace(process=SimpleNamespace(pid=123)))
                runtime.proxy_event=Mock(return_value=ready)
                with patch.object(case.Runtime,'bootstrap'):runtime.bootstrap()
                expected=[case.sys.executable,'-B',str(source),'--mode',mode,'--target','control-quantum-'+target,
                    '--upstream-port','15432']
                if profile!='default':expected+=['--profile','retained-catch-up']
                runtime.proxy_process.assert_called_once_with(expected,input_pipe=True)
                self.assertEqual(runtime.inputs['quantum_proxy'],binding)
                self.assertEqual(json.loads((root/'quantum-proxy-ready.json').read_text()),ready)

    def test_quantum_ready_and_terminal_emit_actual_profile_limits_source_and_route(self):
        import importlib.util
        path=Path(__file__).resolve().parents[2]/'experiments/archive-engine/native_commit_proxy.py'
        spec=importlib.util.spec_from_file_location('linear_profile_proxy_control',path)
        proxy=importlib.util.module_from_spec(spec);spec.loader.exec_module(proxy)
        source=hashlib.sha256(path.read_bytes()).hexdigest()
        for target,mode in (('control-quantum-plan','hold-reply'),('control-quantum-terminal','before-forward'),
                ('control-quantum-source-rollback','hold-source-rollback-reply')):
            for profile,frames,connections in (('default',20000,16),('retained-catch-up',131072,64)):
                with self.subTest(target=target,profile=profile):
                    route=proxy.Proxy(mode,target,profile=profile,upstream_port=15432)
                    expected=dict(source_sha256=source,mode=mode,target=target,profile=profile,
                        frame_limit=frames,connection_limit=connections,connection_limit_scope='lifetime',
                        wire_limit=67108864,lifetime_seconds=120,upstream_host='127.0.0.1',upstream_port=15432)
                    for event in ('ready','terminal'):
                        with patch('builtins.print') as output:route.emit(event)
                        actual=json.loads(output.call_args.args[0])
                        self.assertEqual({key:actual[key] for key in expected},expected)
        # Shared proxy callers outside these three quantum targets retain their
        # original report shape as well as their original transport behavior.
        with patch('builtins.print') as output:proxy.Proxy('pass').emit('terminal',unexpected=0)
        actual=json.loads(output.call_args.args[0])
        self.assertTrue(set(actual).isdisjoint(('profile','frame_limit','connection_limit',
            'connection_limit_scope','wire_limit','lifetime_seconds','source_sha256','upstream_host','upstream_port')))

    def test_ready_and_terminal_refuse_substituted_profile_identity_or_budget(self):
        from cases import first_launch_linear_quantum as case
        runtime=object.__new__(case.LinearQuantumRuntime)
        runtime.quantum_proxy_binding=dict(source_sha256='ab'*32,mode='before-forward',
            target='control-quantum-terminal',profile='retained-catch-up',frame_limit=131072,
            connection_limit=64,connection_limit_scope='lifetime',wire_limit=67108864,lifetime_seconds=120,
            upstream_host='127.0.0.1',upstream_port=15432)
        substitutions=dict(source_sha256='cd'*32,mode='after-complete',target='control-quantum-plan',
            profile='default',frame_limit=20000,connection_limit=16,connection_limit_scope='active',
            wire_limit=67108865,lifetime_seconds=180,upstream_host='localhost',upstream_port=5432)
        for event in ('ready','terminal'):
            valid=dict(runtime.quantum_proxy_binding,event=event,frames=20001,connections=1,bytes=1212047,unexpected=0)
            runtime.validate_quantum_proxy(valid,event)
            for key,value in substitutions.items():
                for changed in (dict(valid,**{key:value}),{field:value for field,value in valid.items() if field!=key}):
                    with self.subTest(event=event,key=key),self.assertRaisesRegex(ValueError,'binding differs'):
                        runtime.validate_quantum_proxy(changed,event)
            with self.assertRaisesRegex(ValueError,'binding differs'):
                runtime.validate_quantum_proxy(dict(valid,upstream_port=True),event)
        for key,value in (('frames',131073),('connections',65),('bytes',67108865),('frames',0),('unexpected',1)):
            with self.subTest(key=key,value=value),self.assertRaisesRegex(ValueError,'terminal observation'):
                runtime.validate_quantum_proxy(dict(valid,**{key:value}),'terminal')

    def test_joined_proxy_requires_terminal_binding_and_retains_refused_summary(self):
        from cases import first_launch_linear_quantum as case
        for failure in (None,'profile','missing','duplicate'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary)
                runtime=case.LinearQuantumRuntime(SimpleNamespace(evidence=root,
                    source_root=Path(__file__).resolve().parents[2]),dict(acceptance_files_sha256={}))
                runtime.quantum_proxy_binding=dict(source_sha256='ab'*32,mode='before-forward',
                    target='control-quantum-terminal',profile='retained-catch-up',frame_limit=131072,
                    connection_limit=64,connection_limit_scope='lifetime',wire_limit=67108864,lifetime_seconds=120,
                    upstream_host='127.0.0.1',upstream_port=15432)
                ready=dict(runtime.quantum_proxy_binding,event='ready',port=34567)
                terminal=dict(runtime.quantum_proxy_binding,event='terminal',frames=20001,connections=1,bytes=1212047,unexpected=0)
                if failure=='profile':terminal['profile']='default'
                events=[ready]+([] if failure=='missing' else [terminal])
                if failure=='duplicate':events.append(terminal)
                stdout=('\n'.join(json.dumps(event) for event in events)+'\n').encode()
                route=SimpleNamespace(process=SimpleNamespace(pid=123,poll=lambda:0),
                    wait=Mock(return_value=0),stop=Mock(),threads=[SimpleNamespace(is_alive=lambda:False)],
                    stdout=stdout,stderr=b'',overflow=False)
                runtime.route=route;runtime.quantum_proxy_ready=ready
                runtime.resources['quantum_proxy']=dict(pid=123,status='running',port=34567)
                runtime.proxy_send=Mock()
                observation=dict(exit=0,stdout=stdout.decode(),stderr='',overflow=False,remote_exclusion='UNKNOWN')
                runtime.proxy_observation=Mock(return_value=observation)
                if failure:
                    with self.assertRaises(ValueError):runtime.stop_quantum_proxy()
                else:runtime.stop_quantum_proxy()
                route.wait.assert_called_once_with(5);route.stop.assert_called_once_with()
                runtime.proxy_send.assert_not_called()
                self.assertEqual((root/'quantum-proxy.stdout').read_bytes(),stdout)
                self.assertEqual(json.loads((root/'quantum-proxy.json').read_text()),observation)
                self.assertEqual(runtime.resources['quantum_proxy']['status'],'joined')
                self.assertEqual([value['operation'] for value in runtime.resources['cleanup_errors']],
                    ['observation'] if failure else [])
                if failure not in ('missing','duplicate'):
                    self.assertEqual(json.loads((root/'quantum-proxy-terminal.json').read_text()),terminal)

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

    def test_expired_database_lease_cannot_bypass_exhausted_frozen_host_budget(self):
        from cases import first_launch_linear_quantum as case
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=object.__new__(case.LinearQuantumRuntime)
            owner=dict(epoch=2,lease_expires_at_us=1_060_000_000)
            frozen=dict(observed_at_us=1_000_000_000,owners=[owner],job_owners=[['control',2]])
            runtime.dead_control_lease=dict(deadline_monotonic=165.0,original_owner=owner,observation=frozen)
            expired=dict(frozen,observed_at_us=1_070_000_000,active_unexpired=0)
            runtime.sql=Mock(return_value=json.dumps(expired));runtime.wait=Mock()
            with patch.object(case.time,'monotonic',return_value=170.0):
                with self.assertRaisesRegex(ValueError,'budget exhausted'):runtime.wait_control_expiry(root)
            runtime.sql.assert_not_called();runtime.wait.assert_not_called()
            witness=json.loads((root/'dead-control-expiry.json').read_text())
            self.assertEqual((witness['status'],witness['observations'],witness['deadline_monotonic']),('FAIL',0,165.0))

    def test_first_expiry_sql_observation_cannot_cross_frozen_deadline(self):
        from cases import first_launch_linear_quantum as case
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=object.__new__(case.LinearQuantumRuntime)
            owner=dict(epoch=2,lease_expires_at_us=1_060_000_000)
            frozen=dict(observed_at_us=1_000_000_000,owners=[owner],job_owners=[['control',2]])
            runtime.dead_control_lease=dict(deadline_monotonic=165.0,original_owner=owner,observation=frozen)
            expired=dict(frozen,observed_at_us=1_070_000_000,active_unexpired=0)
            clock=[164.0]
            def sql(database,statement):
                clock[0]=166.0
                return json.dumps(expired)
            runtime.sql=Mock(side_effect=sql);runtime.wait=Mock()
            with patch.object(case.time,'monotonic',side_effect=lambda:clock[0]):
                with self.assertRaisesRegex(ValueError,'budget exhausted'):runtime.wait_control_expiry(root)
            runtime.sql.assert_called_once();runtime.wait.assert_not_called()
            witness=json.loads((root/'dead-control-expiry.json').read_text())
            self.assertEqual((witness['status'],witness['observations'],witness['deadline_monotonic']),('FAIL',1,165.0))
            self.assertEqual(witness['last_observation'],expired)

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

    def test_abandonment_changes_terminal_job_owner_and_preserves_original_quantum_issuer(self):
        for epoch in (1,2,3):
            for pending in (False,True):
                with self.subTest(epoch=epoch,pending=pending):
                    state,_=fixture(pending=pending,abandoned=True,producer_epoch=epoch)
                    before=copy.deepcopy(state)
                    check.shape(state)
                    self.assertEqual(state,before)
                    self.assertEqual(state['acquisition_quanta'][0]['issuer_epoch'],1)
                    self.assertEqual(state['jobs'][0]['owner_epoch'],2)
        # A terminal job that kept its original owner needs no replacement gap.
        state,_=fixture()
        state['jobs'][0].update(state='cancelled',reason_code='fixture_cancelled',completed_at_us=200_003_000_000)
        state['owner_epochs'][0].update(state='released',exclusion_method='joined')
        check.shape(state)

    def test_active_job_cannot_use_another_retained_control_owner_or_clear_its_owner(self):
        historical,_=fixture(abandoned=True)
        for state_name in ('queued','running'):
            changed=copy.deepcopy(historical)
            changed['jobs'][0].update(state=state_name,job_revision=2,reason_code=None,completed_at_us=None)
            changed['acquisition_gaps']=[]
            with self.subTest(state=state_name),self.assertRaises(AssertionError):check.shape(changed)
        changed=copy.deepcopy(historical)
        changed['jobs'][0].update(owner_resource_id=None,owner_epoch=None,owner_incarnation=None)
        with self.assertRaises(AssertionError):check.shape(changed)
        changed=copy.deepcopy(historical);changed['acquisition_gaps']=[]
        with self.assertRaises(AssertionError):check.shape(changed)

    def test_historical_transfer_requires_exact_gap_issuer_revision_and_completed_cancellation(self):
        state,_=fixture(abandoned=True)
        mutations=(('acquisition_quanta','issuer_epoch',2),('acquisition_quanta','issuer_incarnation','\\x'+'8'*32),
            ('acquisition_gaps','previous_owner_resource','\\x'+'9'*32),('acquisition_gaps','previous_owner_epoch',2),
            ('acquisition_gaps','previous_owner_incarnation','\\x'+'8'*32),('acquisition_gaps','issuer_epoch',1),
            ('acquisition_gaps','issuer_incarnation','\\x'+'4'*32),('acquisition_gaps','previous_job_revision',3),
            ('acquisition_gaps','dataset_id','\\x'+'9'*32),('acquisition_gaps','producer_slot_id','\\x'+'9'*32),
            ('acquisition_gaps','producer_epoch',2),('acquisition_gaps','source_owner_epoch',2),
            ('acquisition_gaps','reason','activated_initial_producer_abandoned'),('acquisition_gaps','owner_role','ingest'),
            ('jobs','job_revision',3),('jobs','checkpoint_revision',1),('jobs','reason_code','other_reason'),
            ('jobs','completed_at_us',200_004_000_000),('owner_epochs','exclusion_method','joined'))
        for table,field,value in mutations:
            changed=copy.deepcopy(state);changed[table][0][field]=value
            with self.subTest(table=table,field=field),self.assertRaises(AssertionError):check.shape(changed)
        # REQUEST_CANCELLATION is transaction-internal. SQL0027's deferred gap
        # INSERT check requires COMPLETE_CANCELLATION before a capture can see it.
        changed=copy.deepcopy(state)
        changed['jobs'][0].update(state='cancel_requested',job_revision=3,completed_at_us=None)
        with self.assertRaises(AssertionError):check.shape(changed)
        changed=copy.deepcopy(state)
        changed['acquisition_quanta'][0].update(issuer_epoch=2,issuer_incarnation='\\x'+'8'*32)
        with self.assertRaises(AssertionError):check.shape(changed)

    def test_quantum_requires_exact_retained_control_and_producer_scope_bindings(self):
        state,_=fixture()
        mutations=(('owner_epochs','dataset_id','\\x'+'9'*32),('owner_epochs','resource_id','\\x'+'9'*32),
            ('owner_epochs','epoch',2),('owner_epochs','role','ingest'),('owner_epochs','process_incarnation','\\x'+'9'*32),
            ('producer_epochs','dataset_id','\\x'+'9'*32),('producer_epochs','producer_slot_id','\\x'+'9'*32),
            ('producer_epochs','producer_epoch',2),('acquisition_jobs','dataset_id','\\x'+'9'*32),
            ('acquisition_jobs','scope_id','\\x'+'9'*32),('jobs','owner_role','ingest'))
        for table,field,value in mutations:
            changed=copy.deepcopy(state);changed[table][0][field]=value
            with self.subTest(table=table,field=field),self.assertRaises(AssertionError):check.shape(changed)
        for table in ('owner_epochs','producer_epochs'):
            for rows in ([],state[table]*2):
                changed=copy.deepcopy(state);changed[table]=rows
                with self.subTest(table=table,rows=len(rows)),self.assertRaises(AssertionError):check.shape(changed)

    def test_shared_capture_saves_invalid_raw_control_before_validation_without_catalog_query(self):
        from cases.first_launch import Runtime
        for failure in ('inventory','quantum'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=object.__new__(Runtime)
                state,_=fixture();state['owner_epochs']=[]
                runtime.pg=dict(id='owned-pg');runtime.inspect=Mock()
                runtime.rows=Mock(return_value=state);runtime.catalog_rows=Mock()
                runtime.tables=set(state)|({'missing_table'} if failure=='inventory' else set())
                with self.assertRaises(ValueError if failure=='inventory' else AssertionError):
                    runtime.capture(root,'invalid')
                self.assertEqual(json.loads((root/'invalid-control.json').read_text()),state)
                runtime.rows.assert_called_once_with(dict(postgres='owned-pg'),'bigbrotr_control')
                runtime.catalog_rows.assert_not_called()
                self.assertFalse((root/'invalid-catalog.json').exists())

    def test_stopped_quantum_history_preserves_pending_rows_versions_and_cardinality_without_row_order(self):
        before,_=fixture()
        pending=fixture(pending=True)[0]['acquisition_quanta'][0]
        pending.update(quantum_id='\\x'+'9'*32,quantum_number=2)
        before['acquisition_quanta'].append(pending)
        check.shape(before)
        reordered=copy.deepcopy(before)
        reordered['acquisition_quanta']=[dict(reversed(list(row.items()))) for row in reversed(reordered['acquisition_quanta'])]
        check.unchanged(before,reordered)
        completed=copy.deepcopy(before)
        completed['acquisition_quanta'][1]=dict(before['acquisition_quanta'][0],quantum_id=pending['quantum_id'],quantum_number=2)
        check.shape(completed)  # A well-shaped terminal cannot rewrite a stopped pending plan.
        with self.assertRaisesRegex(AssertionError,'stopped quantum history'):check.unchanged(before,completed)
        for change in ('deleted','added','duplicated','xmin','charge','issuer'):
            changed=copy.deepcopy(before);rows=changed['acquisition_quanta']
            if change=='deleted':rows.pop()
            elif change=='added':rows.append(dict(pending,quantum_id='\\x'+'a'*32,quantum_number=3))
            elif change=='duplicated':rows.append(copy.deepcopy(pending))
            elif change=='xmin':rows[1]['xmin']='12'
            elif change=='charge':rows[1]['charged_bytes']-=1
            else:rows[1]['issuer_epoch']=2
            with self.subTest(change=change),self.assertRaisesRegex(AssertionError,'stopped quantum history'):
                check.unchanged(before,changed)

    def test_restart_retirement_refuses_changed_stopped_quanta_before_later_recovery(self):
        from cases.first_launch import Runtime
        for change in ('unchanged','completion','xmin','charge','deleted','added'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=object.__new__(Runtime)
                before,_=fixture(pending=change!='added')
                after,_=fixture(pending=change not in ('added','completion'),abandoned=True)
                rows=after['acquisition_quanta']
                if change=='xmin':rows[0]['xmin']='12'
                elif change=='charge':rows[0]['charged_bytes']-=1
                elif change=='deleted':rows.clear()
                elif change=='added':
                    pending=fixture(pending=True)[0]['acquisition_quanta'][0]
                    rows.append(dict(pending,quantum_id='\\x'+'9'*32,quantum_number=2))
                runtime.root=runtime.restart_output=root
                runtime.config=root/'instance.toml';runtime.config.write_text('current fixture config\n')
                (root/'ordinary').mkdir();(root/'ordinary/initial-config.toml').write_text('initial fixture config\n')
                runtime.restart_before=before;runtime.first_science={};runtime.initial_cut=12
                runtime.finite=Mock(side_effect=[dict(acquisition_abandonment=dict(jobs='1',
                    reason='initial_producer_abandoned',coverage='unknown')),{}])
                runtime.capture=Mock(return_value=(after,{}))
                runtime.restart_checks=SimpleNamespace(retired=Mock(side_effect=RuntimeError('later recovery reached')))
                if change=='unchanged':
                    with self.assertRaisesRegex(RuntimeError,'later recovery reached'):runtime.restart_ingest()
                    runtime.restart_checks.retired.assert_called_once()
                else:
                    with self.assertRaisesRegex(AssertionError,'stopped quantum history'):runtime.restart_ingest()
                    runtime.restart_checks.retired.assert_not_called()
                runtime.capture.assert_called_once_with(root,'retired-initial')

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
