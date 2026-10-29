"""Closed saturation/oracle/custody refusal controls; no engines or product runs."""
import copy
import base64
import errno
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from test_first_launch_restart import fixture
from test_first_launch_index import ROOT, schema, metadata_fixture
from cases import first_launch_retained as case
from cases import first_launch_retained_oracle as check
from cases import first_launch_retained_native as native
from cases.first_launch_archive_outage import ArchiveOutageRuntime
from cases import first_launch as base
from test_first_launch import arguments
from cases import first_launch_index_oracle as index
from cases import first_launch_restart as source
from installed_retained_transport import corpus, RetainedEvents, PROFILE
from installed_sync_client import publish
import installed_sync_client as wire
from installed_archive_oracle import expected
from installed_sync_oracle import decode
from installed_progress_replay import head_digest, native_reference, certification_digest
from bootstrap_oracle import cbor


def saturation_fixture(root):
    """Independent literal workload/SQL shape; no product output supplies values."""
    encoded=fixture()[3]
    state,report=copy.deepcopy(encoded.initial_state),encoded.initial_report
    events=corpus(ROOT/'tests/fixtures/acquisition/retained-requests.json')
    wires=[]
    for n in range(1,23):
        values=events[(n-1)*3:n*3] if n<=20 else [events[60],events[61],events[0]] if n==21 else [events[0]]
        at=1500+n*1000
        wires.append(dict(number=n,request=['REQ',f'{n:032x}'+'0000000000000001',{'kinds':[1],'limit':3}],
            connected_at_us=at,request_at_us=at+10,sent_at_us=at+100,closed_at_us=at+400,
            clock_resolution_us=1,completion='request-and-websocket-close',retained_quiet=False,
            retained_phase='admission' if n<=20 else 'saturation' if n==21 else 'proof-only',
            frames=['signed']*len(values)+['EOSE'],event_ids=[e['id'] for e in values],
            event_json_sha256=[source.sha(json.dumps(e,ensure_ascii=False,separators=(',',':')).encode()) for e in values]))
    parameters=[]
    for n,event in enumerate(events[:61]):
        wire=wires[n//3];received=wire['sent_at_us']+10+n%3
        body=[bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],1,[],event['content']]
        parameters.append((event,received,received+100,1,source.sha(b'bigbrotr/body/1\0'+cbor(body))))
    wanted=expected(state,report,events[0],2000,parameters[0][1],parameters[0][2],1,
        native_options=dict(acknowledge=True,retained_boundary=True,additional_events=parameters[1:]))
    dump=dict(event='dump-state',sequence=1081,rows=[dict(cf=cf,key=k.hex(),value=v.hex()) for cf,k,v in wanted['native']])
    inputs=root/'producer-inputs-v1';inputs.mkdir()
    for n,value in enumerate(wanted['inputs'],1):
        (inputs/f'{report["producer_slot_id"]}-0000000000000001-{n:016x}.cbor').write_bytes(value)
    control=dict(role='control',resource_id='\\x'+'ca'*16,epoch=1,process_incarnation='\\x'+'bb'*16,
        acquired_at_us=1000,lease_expires_at_us=1_000_000,released_at_us=30000)
    state['owner_epochs'].append(control)
    common=dict(dataset_id=state['datasets'][0]['dataset_id'],producer_slot_id='\\x'+report['producer_slot_id'],producer_epoch=1)
    state.update(producer_epochs=[dict(common,reserved_through_request=64,outstanding_request_limit=32)],
        jobs=[dict(job_id='\\x'+'da'*16,request_key='\\x'+'b1'*16,created_at_us=1500,state='queued',checkpoint_revision=0)],
        acquisition_jobs=[dict(job_id='\\x'+'da'*16,scope_id='\\x'+'db'*16)],
        acquisition_scopes=[dict(scope_id='\\x'+'db'*16,endpoint_id='\\x'+'dc'*16,access_scope_id='\\x'+'dd'*16)],
        relay_enrollments=[dict(endpoint_id='\\x'+'dc'*16,access_scope_id='\\x'+'dd'*16,route_id='\\x'+'de'*16)],
        job_admissions=[dict(job_id='\\x'+'da'*16)],producer_requests=[],producer_outcomes=[])
    job,detail,attribution=source.attribution(state,'b1')
    manifest=next(v for cf,k,v in wanted['native'] if cf=='default' and k==b'\x01\x01')
    shared=dict(common,manifest_sha256='\\x'+source.sha(manifest))
    fields=('progress_request_id','progress_event_ordinal','progress_wire_sequence','progress_input_sha256')
    for n,(original,receipt) in enumerate(zip(wanted['inputs'],wanted['receipts']),1):
        binding=source.request_binding(events[n-4],wires[(n-4)//3],(n-4)%3+1) if n>=4 else dict.fromkeys(fields)
        provenance=attribution if n>=4 else dict.fromkeys(attribution)
        state['producer_requests'].append(dict(shared,request_sequence=n,request_digest='\\x'+source.sha(original),
            input_bytes=len(original),input_profile=1,issuer_role='control',issuer_resource=control['resource_id'],
            issuer_epoch=1,issuer_incarnation=control['process_incarnation'],**provenance,**binding,reserved_at_us=2000+n,xmin=str(n)))
        state['producer_outcomes'].append(dict(shared,request_sequence=n,request_digest='\\x'+source.sha(original),
            outcome_kind=1,outcome_bytes='\\x'+receipt.hex(),outcome_sha256='\\x'+source.sha(receipt),policy_epoch=1,
            recorder_role='control',recorder_resource=control['resource_id'],recorder_epoch=1,
            recorder_incarnation=control['process_incarnation'],recorded_at_us=2100+n,xmin=str(n)))
    values={(cf,k.hex()):v.hex() for cf,k,v in wanted['native']}
    ds=bytes.fromhex(common['dataset_id'][2:])
    dependencies=[]
    for n in range(1,313):
        first=1 if n<=2 else 2 if n==3 else 3 if n<=7 else 4+(n-8)//5
        value=bytes.fromhex(values['facts',(b'\x01\x20'+ds+bytes(4)+(1).to_bytes(8,'big')+n.to_bytes(8,'big')).hex()])
        dependencies.append(dict(common,source_shard=0,source_epoch=1,source_sequence=n,
            payload_digest='\\x'+value[2:34].hex(),xmin=str(first)))
    state['producer_checkpoint_dependencies']=dependencies
    before=copy.deepcopy(state)
    domains={r['dictionary_kind']:bytes.fromhex(r['allocation_domain'][2:]) for r in state['allocation_domains']}
    ref=lambda kind,n=1:[ds,kind,domains[kind],n]
    for value,proof in ((before,False),(state,True)):
        wire=wires[21 if proof else 20];event=events[0 if proof else 60]
        current=dict(common,job_id=job['job_id'],scope_id=detail['scope_id'],progress_revision=62 if proof else 61,
            request_id='\\x'+wire['request'][1].encode().hex(),event_ordinal=1,wire_sequence=1,
            input_sha256='\\x'+source.sha(json.dumps(event,ensure_ascii=False,separators=(',',':')).encode()),
            classification=5 if proof else 2,mutation_request=None if proof else 64,
            classified_events=62 if proof else 61,successful_events=62 if proof else 61,rejected_events=0,
            observed_at_us=wire['sent_at_us']+10,clock_resolution_us=1,clock_anomaly_flags=0,xmin='65' if proof else '64')
        current['intent_digest']='\\x'+(certification_digest(event,ref(4,3),ref(2),ref(2),[ref(1),ref(3),ref(5)],current,wire).hex()
            if proof else source.sha(wanted['inputs'][63]))
        head=dict(shared,progress_format=1,checkpoint_revision=65 if proof else 64,complete_through_request=64,
            progress_jobs=1,dependency_count=312,issuer_resource=control['resource_id'],issuer_epoch=1,
            issuer_incarnation=control['process_incarnation'],committed_at_us=wire['closed_at_us']+10,
            xmin=current['xmin'],checkpoint_digest='\\x'+'00'*32)
        head['checkpoint_digest']='\\x'+head_digest(head,[current],dependencies,lambda row:native_reference(row,values)).hex()
        value.update(acquisition_progress=[current],producer_checkpoints=[head])
    return state,before,report,events,wires,dump,wanted


class RetainedBoundaryControls(unittest.TestCase):
    def finite_relay(self,root,*,bad_round=None,release=True,failure='protocol',retained=True):
        for source_name,target_name in (('tests/fixtures/acquisition/retained-requests.json','retained-requests.json'),
                ('tests/fixtures/ingest/import-event.jsonl','event.jsonl')):
            (root/target_name).write_bytes((ROOT/source_name).read_bytes())
        if retained:base.save(root/'retained-request-profile.json',PROFILE)
        test=self;finished=threading.Event()
        class Connection:
            def __init__(self,number):
                self.number=number;self.ident=f'{number:032x}'+'0000000000000001';self.frame_number=0
                key=base64.b64encode(bytes(16)).decode()
                self.header=bytearray(('GET /installed-fixture HTTP/1.1\r\nHost: 127.0.0.1:7447\r\n'
                    'Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n'
                    'Sec-WebSocket-Key: '+key+'\r\n\r\n').encode())
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def settimeout(self,value):test.assertEqual(value,3)
            def sendall(self,value):pass
            def shutdown(self,value):pass
            def recv(self,count):
                if self.number==bad_round and failure=='socket':raise EOFError('peer closed')
                if not self.header and not retained and self.number==3:finished.set()
                result=bytes(self.header[:count]);del self.header[:count];return result
            def frame(self):
                self.frame_number+=1
                if self.frame_number==1:
                    selector={'kinds':[1],'limit':2 if self.number==bad_round and failure=='protocol' else 3}
                    return 1,json.dumps(['REQ',self.ident,selector]).encode()
                return (1,json.dumps(['CLOSE',self.ident]).encode()) if self.frame_number==2 else (8,b'')
        listener=Mock();accepted=[];closed=[]
        def accept():
            number=len(accepted)+1
            if number==22 and release:
                base.save(root/'retained-proof.continue',dict(format='bigbrotr-retained-request-proof-release/1',
                    boundary_request=f'{21:032x}'+'0000000000000001',classified_events=61,
                    complete_through_request=64,checkpoint_revision=64))
            accepted.append(number)
            test.assertLessEqual(number,129)  # A regression may attempt129; never hang this control.
            return Connection(number),('127.0.0.1',12345)
        def close():
            test.assertFalse((root/'relay-finite-complete.json').exists())
            closed.append(True)
        listener.accept.side_effect=accept;listener.close.side_effect=close;errors=[]
        with patch.object(wire,'read_frame',side_effect=lambda connection:connection.frame()), \
                patch.object(wire,'send_frame'):
            wire.relay(root,listener,finished,errors,True)
        return listener,accepted,closed,errors

    def test_finite_peer_closes_after128_without_accept129_and_preserves_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);listener,accepted,closed,errors=self.finite_relay(root)
            self.assertEqual((accepted,closed,errors),(list(range(1,129)),[True],[]))
            self.assertEqual(listener.accept.call_count,128)
            self.assertFalse((root/'relay-open-129.json').exists())
            receipt=base.load(root/'relay-finite-complete.json')
            wires=[base.load(p) for p in sorted(root.glob('relay-closed-*.json'))]
            events=corpus(root/'retained-requests.json')
            check.finite_peer(receipt,wires,events,base.digest(root/'relay-closed-128.json'))
            self.assertEqual(sum(w.get('retained_phase')=='proof-only' for w in wires),1)
            for change in (lambda v:v.update(connections=129),lambda v:v.update(listener_closed=False),
                    lambda v:v.update(proof_sent=False),lambda v:v.update(last_closed_sha256='0'*64),
                    lambda v:v.update(completed_monotonic_ns=0)):
                invalid=copy.deepcopy(receipt);change(invalid)
                with self.assertRaises(AssertionError):
                    check.finite_peer(invalid,wires,events,base.digest(root/'relay-closed-128.json'))
            runtime=case.RetainedRuntime(arguments(root),{});runtime.home=root;runtime.events=events
            (root/'initial').mkdir()
            connection=Mock();connection.connect_ex.return_value=errno.ECONNREFUSED
            with patch.object(case.socket,'socket') as socket:
                socket.return_value.__enter__.return_value=connection
                self.assertTrue(runtime.initial_source_quiet())
            self.assertEqual(base.load(root/'initial/retained-peer-completion.json')['quiet_cancellation'],'NOT_RUN')
            connection.connect_ex.return_value=0
            with patch.object(case.socket,'socket') as socket:
                socket.return_value.__enter__.return_value=connection
                with self.assertRaisesRegex(ValueError,'listener remains reachable'):runtime.initial_source_quiet()
            base.save(root/'relay-open-129.json',dict(number=129,retained_quiet=True))
            with self.assertRaisesRegex(ValueError,'extra/missing connection'):runtime.initial_source_quiet()

    def test_prelimit_protocol_failure_or_missing_proof_is_not_finite_completion(self):
        for bad_round,release,failure in ((17,True,'protocol'),(17,True,'socket'),(None,False,'protocol')):
            with self.subTest(bad_round=bad_round,failure=failure),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);listener,accepted,closed,errors=self.finite_relay(root,
                    bad_round=bad_round,release=release,failure=failure)
                self.assertEqual(closed,[True]);self.assertEqual(len(errors),1)
                self.assertTrue(errors[0].startswith('EOFError:' if failure=='socket' else 'AssertionError:'))
                self.assertTrue((root/'relay-error.json').exists())
                self.assertFalse((root/'relay-finite-complete.json').exists())
                self.assertEqual(listener.accept.call_count,17 if bad_round else 128)

    def test_unrelated_relay_keeps_its_original_quiet_receive_and_no_finite_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);listener,accepted,closed,errors=self.finite_relay(root,retained=False)
            self.assertEqual((accepted,closed,errors),([1,2,3],[True],[]))
            self.assertEqual(base.load(root/'relay-closed-003.json')['completion'],'quiet-eof')
            self.assertFalse((root/'relay-finite-complete.json').exists())

    def test_default_quiet_source_and_retained_quiet_receive_keep_old_meaning(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);ordinary=object.__new__(base.Runtime);ordinary.home=root
            retained=object.__new__(case.RetainedRuntime);retained.home=root
            self.assertFalse(ordinary.initial_source_quiet());self.assertFalse(retained.initial_source_quiet())
            base.save(root/'relay-open-003.json',dict(number=3))
            self.assertTrue(ordinary.initial_source_quiet());self.assertFalse(retained.initial_source_quiet())
            base.save(root/'relay-open-003.json',dict(number=3,retained_quiet=True))
            self.assertTrue(retained.initial_source_quiet())
            base.save(root/'relay-closed-003.json',dict(number=3))
            self.assertFalse(ordinary.initial_source_quiet());self.assertFalse(retained.initial_source_quiet())

    def prefix_runtime(self,root,clock=100):
        runtime=case.RetainedRuntime(arguments(root),{})
        runtime.home=root/'client';runtime.home.mkdir()
        runtime.trace_output=root/'trace';runtime.trace_output.mkdir()
        runtime.ch_entry=dict(id='owned-clickhouse')
        runtime.prefix_deadline=15
        runtime.ch=Mock(return_value=[dict(cutoff_us=clock)])
        runtime.collect_query_lifecycle=Mock()
        output=root/'prefix';output.mkdir()
        base.save(output/'index-prefix-verdict.json',dict(status='PASS',source_through=3,
            archive_revision=2,index_revision=2))
        return runtime,output

    def test_prefix_clock_precedes_fault_and_release_without_live_log_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime,output=self.prefix_runtime(Path(directory))
            def fault(*args):
                scope=base.load(runtime.trace_output/'prefix-scope.json')
                self.assertEqual((scope['scope'],scope['query_start_through_us']),('prefix_started_queries',100))
                self.assertEqual(scope['prefix_verdict_sha256'],base.digest(output/'index-prefix-verdict.json'))
                self.assertEqual(scope['clickhouse_container'],'owned-clickhouse')
                self.assertEqual(scope['all_query_coverage'],scope['termination_or_exclusion'])
                self.assertEqual(scope['termination_or_exclusion'],'NOT_PROVEN')
                self.assertEqual(scope['earlier_code202_cause'],'UNKNOWN')
                self.assertFalse(scope['source_released']);self.assertFalse(scope['catalog_fault'])
                self.assertEqual(runtime.trace_start_through_us,100)
                self.assertFalse(getattr(runtime,'trace_collected',False))
                self.assertFalse((runtime.home/'index-prefix.continue').exists())
                return 'existing fault'
            with patch.object(case.time,'monotonic',return_value=10), \
                    patch.object(ArchiveOutageRuntime,'establish_prefix_fault',side_effect=fault) as owner:
                self.assertEqual(runtime.establish_prefix_fault({},output),'existing fault')
                owner.assert_called_once_with({},output)
            runtime.ch.assert_called_once_with('SELECT toUnixTimestamp64Micro(now64(6)) AS cutoff_us',
                'provisioner',rows=True)
            runtime.collect_query_lifecycle.assert_not_called()

    def test_unverified_released_expired_or_malformed_prefix_cannot_reach_fault(self):
        for kind in ('failed','wrong-cut','released','expired','clock-shape','missing-clock',
                     'bool-clock','zero-clock','negative-clock','large-clock','string-clock'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as directory:
                runtime,output=self.prefix_runtime(Path(directory))
                proof=base.load(output/'index-prefix-verdict.json')
                if kind=='failed':proof['status']='FAIL'
                if kind=='wrong-cut':proof['source_through']=4
                base.save(output/'index-prefix-verdict.json',proof)
                if kind=='released':(runtime.home/'index-prefix.continue').touch()
                if kind=='expired':runtime.prefix_deadline=10
                if kind=='clock-shape':runtime.ch.return_value=[dict(cutoff_us=100),dict(cutoff_us=101)]
                clock={'missing-clock':None,'bool-clock':True,'zero-clock':0,'negative-clock':-1,
                    'large-clock':2**63,'string-clock':'100'}
                if kind in clock:runtime.ch.return_value=[dict(cutoff_us=clock[kind])]
                with patch.object(case.time,'monotonic',return_value=10), \
                        patch.object(ArchiveOutageRuntime,'establish_prefix_fault') as fault:
                    with self.assertRaises(ValueError):runtime.establish_prefix_fault({},output)
                    fault.assert_not_called()
                self.assertFalse(hasattr(runtime,'trace_start_through_us'))
                self.assertFalse((runtime.trace_output/'prefix-scope.json').exists())
                runtime.collect_query_lifecycle.assert_not_called()

    def test_prefix_scope_persistence_failure_keeps_original_unfiltered_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime,output=self.prefix_runtime(Path(directory))
            with patch.object(case.time,'monotonic',return_value=10), \
                    patch.object(case,'save',side_effect=OSError('scope storage')), \
                    patch.object(ArchiveOutageRuntime,'establish_prefix_fault') as fault:
                with self.assertRaisesRegex(OSError,'scope storage'):runtime.establish_prefix_fault({},output)
                fault.assert_not_called()
            self.assertFalse(hasattr(runtime,'trace_start_through_us'))
            with patch.object(base.Runtime,'stop_container',return_value='joined') as stop:
                self.assertEqual(runtime.stop_container(dict(service='clickhouse')),'joined')
                stop.assert_called_once()
            runtime.collect_query_lifecycle.assert_called_once_with('terminal')

    def test_driver_captures_boundary_before_atomic_proof_release(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);state,before,report,events,wires,dump,wanted=saturation_fixture(root)
            home=root/'client';home.mkdir();output=root/'output';output.mkdir()
            (home/'retained-request-deadline.json').write_text(json.dumps(dict(
                format='bigbrotr-retained-request-gate/1',started_ns=1_000_000_000,deadline_ns=66_000_000_000)))
            (home/'relay-closed-021.json').write_text(json.dumps(wires[20]))
            runtime=object.__new__(case.RetainedRuntime)
            runtime.home,runtime.state,runtime.client=home,root,Mock()
            runtime.events,runtime.prefix_catalog=events,{}
            runtime.sql=Mock(return_value='1');order=[]
            def capture(*args):
                self.assertFalse((home/'retained-proof.continue').exists())
                order.append('captured');return before,{}
            runtime.capture=Mock(side_effect=capture)
            runtime.files=Mock(return_value={str(n):str(n) for n in range(64)})
            def release(directory,name,value):
                self.assertEqual(order,['captured'])
                self.assertFalse((directory/name).exists())
                publish(directory,name,value)
                self.assertEqual(json.loads((directory/name).read_text()),value)
                self.assertFalse((directory/'retained-proof.continue.partial').exists())
                order.append('released')
            runtime.publish_peer=Mock(side_effect=release)
            runtime.wait=lambda predicate,*args:self.assertTrue(predicate())
            with patch.object(case.time,'monotonic',return_value=10):
                runtime.initial_archive_observation({},output)
            self.assertEqual(order,['captured','released'])
            (home/'retained-proof.continue').unlink()
            runtime.publish_peer.reset_mock();before['acquisition_progress'][0]['event_ordinal']=2
            with patch.object(case.time,'monotonic',return_value=10),self.assertRaises(AssertionError):
                runtime.initial_archive_observation({},output)
            runtime.publish_peer.assert_not_called()

    def test_complete_literal_science_and_native_boundary_refusals(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);state,before,report,events,wires,dump,wanted=saturation_fixture(root)
            actual=check.science(state,before,report,events,wires,dump,root,30000)
            self.assertEqual((len(actual['native']),len(actual['inputs']),len(actual['batches'])),(829,64,64))
            self.assertEqual([actual['batches'][n]['last'] for n in (0,1,2,63)],[2,3,7,312])
            mutations=(lambda d:d.update(sequence=1082),lambda d:d['rows'].pop(),
                lambda d:d['rows'].append(copy.deepcopy(d['rows'][-1])),
                lambda d:next(r for r in d['rows'] if r['cf']=='receipts' and r['key'].startswith('0131')).update(
                    value='0131'+(65).to_bytes(8,'big').hex()*2+bytes(8).hex()))
            for mutate in mutations:
                changed=copy.deepcopy(dump);mutate(changed)
                with self.assertRaises(AssertionError):check.science(state,before,report,events,wires,changed,root,30000)
            extra=root/'producer-inputs-v1'/f'{report["producer_slot_id"]}-0000000000000001-0000000000000041.cbor'
            extra.write_bytes(b'unissued overflow')
            with self.assertRaises(AssertionError):check.science(state,before,report,events,wires,dump,root,30000)

    def test_intermediate_progress_cannot_be_hidden_by_later_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);state,before,report,events,wires,dump,_=saturation_fixture(root)
            for change in (lambda v:v['acquisition_progress'][0].update(event_ordinal=2),
                lambda v:v['acquisition_progress'][0].update(input_sha256=source.request_binding(events[61],wires[20],2)['progress_input_sha256']),
                lambda v:v['producer_checkpoints'][0].update(checkpoint_revision=65),
                lambda v:v['producer_epochs'][0].update(reserved_through_request=65)):
                invalid=copy.deepcopy(before);change(invalid)
                with self.assertRaises(AssertionError):check.science(state,invalid,report,events,wires,dump,root,30000)

    def test_current_scope_does_not_silently_expand_old_encoder(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,before,report,events,wires,dump,wanted=saturation_fixture(Path(temporary))
            rows=index.expected_rows(wanted,bytes.fromhex('66'*16),schema,counts=(68,61,61,61,61,61,61,0,61,61))
            self.assertEqual(sum(map(len,rows.values())),556)
            with self.assertRaises(AssertionError):index.expected_rows(wanted,bytes.fromhex('66'*16),schema)
            encoded=fixture()[3]
            with self.assertRaises(AssertionError):expected(encoded.initial_state,report,events[0],2000,3000,4000,
                native_options=dict(acknowledge=True,retained_boundary=True,additional_events=[]))

    def test_late_plan_and_physical_rows_cannot_escape_the64_plan_profile(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);*_,wanted=saturation_fixture(root)
            custody=root/'index';custody.mkdir()
            state,actual,_=metadata_fixture(custody,science=wanted,retained_boundary=True)
            result=index.publication(state,state,actual,wanted,schema,custody,retained_boundary=True)
            self.assertEqual((result['frontier_revision'],result['source_cut'],result['committed_chunks']),(64,312,640))
            with self.assertRaises(AssertionError):index.validate_metadata(state,custody,complete=True)
            for mutate in (lambda s:s['projection_plans'][-1].update(source_last=313),
                lambda s:s['index_frontier_plans'][-1].update(engine_receipt_digest='\\x'+'00'*32),
                lambda s:s['projection_chunks'][-1].update(owner_epoch=2)):
                invalid=copy.deepcopy(state);mutate(invalid)
                with self.assertRaises(AssertionError):index.publication(invalid,invalid,actual,wanted,schema,custody,retained_boundary=True)
            invalid=copy.deepcopy(actual);invalid['events'][-1]['content']='changed late signed content'
            with self.assertRaises(AssertionError):index.publication(state,state,invalid,wanted,schema,custody,retained_boundary=True)

    def test_nonready_history_preserves_actual_freshness_semantics(self):
        ds='22'*16;generation='66'*16;storage='88'*16;incarnation='77'*16
        state=dict(datasets=[dict(dataset_id='\\x'+ds,config_revision=1,policy_revision=1)],
            owner_epochs=[dict(role='control',state='active',process_incarnation='\\x'+incarnation)],
            projection_generations=[dict(generation_id='\\x'+generation,storage_incarnation='\\x'+storage)],
            consumers=[dict(consumer_role='index',consumer_id='index')],
            consumer_frontiers=[dict(consumer_id='index',frontier_revision=2,last_complete_sequence=3)])
        cut=lambda n:dict(dataset_id=ds,topology_generation='0',bounds=[dict(shard_id='0',owner_epoch='1',last_complete_sequence=str(n))])
        clock=dict(observed_at_us='123456789',age_us='1000')
        value=dict(format='bigbrotr-role-status/1',authority='observation-only',observed_at_us='123456789',
            identity=dict(dataset_id=ds,process_incarnation=incarnation,config_revision='1',policy_revision='1'),
            source=dict(cut=cut(312),freshness='fresh',time=clock,reason='current'),
            roles=[dict(role=role,state='ready' if role=='control' else 'idle',reason='current')
                for role in ('control','ingest','sync','archive','index')],
            capabilities=[dict(capability=kind,state='ready',reason='current',freshness='fresh',time=clock,
                published=dict(revision='2',cut=cut(3)) if kind.endswith('_publication') else None,
                generation=generation if kind=='index_publication' else None,
                storage_incarnation=storage if kind=='index_publication' else None)
                for kind in ('signed_event_admission','finite_acquisition','archive_publication','index_publication')])
        value['roles'][3].update(state='degraded',reason='dependency_unavailable')
        value['capabilities'][2].update(state='unavailable',reason='dependency_unavailable')
        value['roles'][4].update(state='starting',reason='verification_in_flight')
        value['capabilities'][3].update(state='building',reason='verification_in_flight')
        check_roles=lambda v:index.live_roles(v,state,123456000,123457000,revision=64,sequence=312,
            archive_unavailable=True,retained_boundary=True)
        check_roles(value)
        for state_name,reason,age,freshness in (('stale','verification_stale','1','fresh'),
            ('building','behind_source','30000000','stale'),('ready','current','1000','fresh')):
            invalid=copy.deepcopy(value)
            invalid['roles'][4].update(state='starting' if state_name=='building' else 'degraded',reason=reason)
            invalid['capabilities'][3].update(state=state_name,reason=reason,freshness=freshness,
                time=dict(observed_at_us='123456789',age_us=age))
            with self.assertRaises(AssertionError):check_roles(invalid)

    def test_native_artifact_requires_exact_candidate_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);binary=root/'ingest_bootstrap_probe';binary.write_bytes(b'declared fixture');binary.chmod(0o700)
            source_manifest=dict(revision='c'*40,tree='d'*40)
            (root/'source-manifest.json').write_text(json.dumps(source_manifest))
            manifest_hash=source.sha((root/'source-manifest.json').read_bytes())
            parent=dict(source_manifest_sha256=manifest_hash,build_info={'source':'same'},native_toolchain={'tools':'fixed'})
            (root/'build-result.json').write_text(json.dumps(parent))
            receipt=dict(format='bigbrotr-ingest-bootstrap-probe-build/1',profile='release',
                command=['cargo','build','--locked','--release','-p','bigbrotr','--example','ingest_bootstrap_probe'],
                source_revision=source_manifest['revision'],source_tree=source_manifest['tree'],
                source_manifest_sha256=manifest_hash,parent_build_receipt_sha256=source.sha((root/'build-result.json').read_bytes()),
                binary_sha256=source.sha(binary.read_bytes()),native_toolchain=parent['native_toolchain'])
            (root/'ingest-bootstrap-probe-build.json').write_text(json.dumps(receipt))
            candidate=dict(artifact=dict(source=dict(source_manifest,manifest_sha256=manifest_hash),build_info=parent['build_info']))
            native.artifact(root,candidate)
            candidate['artifact']['source']['manifest_sha256']='e'*64
            with self.assertRaises(ValueError):native.artifact(root,candidate)

    def test_native_ownership_rejects_writable_or_foreign_mounts_before_stop(self):
        runtime=SimpleNamespace(owner='owner',state=Path('/owned/state'),
            resources=dict(image=dict(id='image')),native_paths={'ingest_bootstrap_probe':Path('/owned/probe')},record_resources=Mock())
        entry=dict(name='owned',id='id')
        info=dict(Name='/owned',Id='id',Image='image',State={'Status':'running'},
            Config=dict(Labels={native.LABEL:'owner'},Entrypoint=['/observer/probe'],
                Cmd=['dump-state','/observer/store'],User=f'{os.geteuid()}:{os.getegid()}'),
            Mounts=[dict(Destination='/observer/probe',Source='/owned/probe',RW=False,Type='bind'),
                dict(Destination='/observer/store',Source='/owned/state/data/ingest',RW=False,Type='bind')],
            HostConfig=dict(NetworkMode='none',ReadonlyRootfs=True,Memory=512*native.MIB,MemorySwap=512*native.MIB,
                NanoCpus=1_000_000_000,PidsLimit=64,CapDrop=['ALL'],SecurityOpt=['no-new-privileges:true'],RestartPolicy={'Name':'no'}))
        native.identity(info,runtime,entry);runtime.record_resources.assert_not_called()
        for change in (lambda v:v['Mounts'][1].update(RW=True),lambda v:v['Mounts'][1].update(Source='/foreign'),
            lambda v:v['Config']['Labels'].update({native.LABEL:'foreign'}),lambda v:v.update(Id='reused')):
            invalid=copy.deepcopy(info);change(invalid)
            with self.assertRaises(ValueError):native.identity(invalid,runtime,entry)

    def test_transport_has_one_bound_proof_and_exact_empty_round_cap(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            (root/'retained-request-profile.json').write_text(json.dumps(PROFILE))
            (root/'retained-requests.json').write_bytes((ROOT/'tests/fixtures/acquisition/retained-requests.json').read_bytes())
            (root/'event.jsonl').write_bytes((ROOT/'tests/fixtures/ingest/import-event.jsonl').read_bytes())
            peer=RetainedEvents(root)
            for n in range(1,21):self.assertEqual(len(peer.select(str(n),n)[1]),3)
            selected=peer.select('boundary',21)
            self.assertEqual(selected[1],[peer.events[60],peer.events[61],peer.events[0]])
            self.assertEqual(peer.select('22',22),('empty',[]))
            release=dict(format='bigbrotr-retained-request-proof-release/1',boundary_request='wrong',
                classified_events=61,complete_through_request=64,checkpoint_revision=64)
            (root/'retained-proof.continue').write_text(json.dumps(release))
            with self.assertRaises(AssertionError):peer.select('23',23)
            release['boundary_request']='boundary';(root/'retained-proof.continue').write_text(json.dumps(release))
            self.assertEqual(peer.select('23',23),('proof-only',[peer.events[0]]))
            for n in range(99):self.assertEqual(peer.select(str(24+n),24+n),('empty',[]))
            self.assertIsNone(peer.select('123',123))

    def test_absolute_client_deadline_does_not_reset(self):
        declaration=dict(format='bigbrotr-retained-request-gate/1',started_ns=1_000_000_000,deadline_ns=66_000_000_000)
        self.assertEqual(case.gate(declaration,65.9),66)
        for value,now in ((declaration,66),(dict(declaration,deadline_ns=67_000_000_000),2)):
            with self.assertRaises(ValueError):case.gate(value,now)

    def test_native_stop_survives_resource_persistence_failure(self):
        runtime=SimpleNamespace(resources=dict(native_observer=dict(name='owned',id='id')),
            record_resources=Mock(side_effect=OSError('resource capture unavailable')),root=Path('/unused'))
        active=dict(Id='id',State=dict(Running=True,Pid=123))
        stopped=dict(Id='id',State=dict(Running=False,Pid=0))
        runtime.docker=Mock(side_effect=[SimpleNamespace(returncode=0,stdout=json.dumps([active])),
            SimpleNamespace(returncode=0),SimpleNamespace(returncode=0,stdout=json.dumps([stopped]))])
        with patch.object(native,'identity',side_effect=lambda value,*_:value):
            with self.assertRaisesRegex(OSError,'resource capture'):native.stop(runtime)
        self.assertEqual(runtime.docker.call_args_list[1].args,('stop','--time','5','id'))
        self.assertEqual(runtime.docker.call_args_list[2].args,('inspect','id'))

    def test_native_observer_original_error_survives_output_failure_and_still_joins(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);(root/'initial').mkdir()
            (root/'initial/terminal-container.json').write_text(json.dumps(dict(id='writer')))
            process=SimpleNamespace(wait=Mock(side_effect=ValueError('original observer failure')),stop=Mock(),
                stdout=bytearray(b'partial'),stderr=bytearray(),overflow=False,
                process=SimpleNamespace(pid=123,returncode=1))
            (root/'state/data/ingest').mkdir(parents=True)
            runtime=SimpleNamespace(root=root,state=root/'state',project='owned',owner='owner',
                native_paths={'ingest_bootstrap_probe':root/'probe'},native_hashes={'ingest_bootstrap_probe':'hash'},
                resources=dict(image=dict(id='image'),cleanup_errors=[]),record_resources=Mock(),
                backend_absence=Mock(),proxy_process=Mock(return_value=process))
            writer=dict(State=dict(Running=False,Pid=0,ExitCode=0,OOMKilled=False))
            runtime.docker=Mock(side_effect=[SimpleNamespace(stdout=json.dumps([writer])),
                SimpleNamespace(returncode=1),SimpleNamespace(stdout='observer'),
                SimpleNamespace(stdout=json.dumps([{'Id':'observer'}]))])
            with patch.object(native,'digest',return_value='hash'),patch.object(native,'identity'), \
                    patch.object(native,'stop') as stop,patch.object(Path,'write_bytes',side_effect=OSError('capture failed')):
                with self.assertRaisesRegex(ValueError,'original observer failure'):native.census(runtime,root)
            process.stop.assert_called_once();stop.assert_called_once_with(runtime)
            self.assertEqual(runtime.proxy_process.call_args.kwargs,dict(limit=1024*1024))
            self.assertEqual(runtime.resources['cleanup_errors'],[dict(resource='native-observer-output',error='OSError')])



class ClosedReclamationControls(unittest.TestCase):
    def claimed_control(self,before,*,failed=False):
        after=copy.deepcopy(before)
        prior=max((r for r in before['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        epoch=prior['epoch']+1;at=max(prior['acquired_at_us'],prior['released_at_us'] or prior['lease_expires_at_us'])+100
        owner=dict(prior,epoch=epoch,role_instance_id='\\x'+f'{epoch+30:02x}'*16,
            process_incarnation='\\x'+f'{epoch+60:02x}'*16,acquired_at_us=at,lease_expires_at_us=at+1_000_000,
            state='active' if failed else 'released',released_at_us=None if failed else at+100,
            exclusion_method=None if failed else 'storage_fenced',exclusion_digest=None,xmin=str(epoch),control_backend_pid=123+epoch)
        if not failed:owner['exclusion_digest']=check.control_exclusion(owner,owner)
        after['owner_epochs'].append(owner)
        after['role_instances'].append(dict(role_instance_id=owner['role_instance_id'],dataset_id=owner['dataset_id'],
            role='control',created_at_us=at,retired_at_us=None if failed else at+100,xmin=str(epoch)))
        return after

    def reclaimed_fixture(self,root):
        state,_,report,events,wires,dump,science=saturation_fixture(root)
        science['manifest']=next(v for cf,k,v in science['native'] if cf=='default' and k==b'\x01\x01')
        state['role_instances']=[]
        for owner in state['owner_epochs']:
            owner.setdefault('epoch',1);owner.setdefault('role_instance_id','\\x'+'99'*16)
        owner=index.only(state['owner_epochs'],role='control')
        owner.update(dataset_id=state['datasets'][0]['dataset_id'],role_instance_id='\\x'+'01'*16,
            state='released',index_publication_protocol=None,control_fence_version=1,control_backend_pid=124,
            control_backend_start='2026-10-29T01:00:00+00:00',exclusion_method='storage_fenced',xmin='1')
        owner['exclusion_digest']=check.control_exclusion(owner,owner)
        state['role_instances'].append(dict(role_instance_id=owner['role_instance_id'],role='control',dataset_id=owner['dataset_id'],
            created_at_us=owner['acquired_at_us'],retired_at_us=owner['released_at_us'],xmin='1'))
        producer=state['producer_epochs'][0]
        producer.update(state='retired',owner_resource_id='\\x'+'aa'*16)
        producer['closure_digest']='\\x'+source.closure(science['manifest'],producer['producer_slot_id'],1,science['inputs'],science['receipts'],312)
        slot=bytes.fromhex(report['producer_slot_id']);marker=(b'\x01\x31'+slot+index.be(1)).hex()
        before=copy.deepcopy(dump);before['sequence']=1082
        next(r for r in before['rows'] if r['cf']=='receipts' and r['key']==marker)['value']=(b'\x02\x31'+index.be(64)*2+index.be(0)+b'\x01').hex()
        head=state['producer_checkpoints'][0]
        floor=(b'\x03\x31'+index.be(64)*3+b'\x01'+hashlib.sha256(science['manifest']).digest()+index.be(312)
            +bytes.fromhex(producer['closure_digest'][2:])+index.be(65)+bytes.fromhex(head['checkpoint_digest'][2:]))
        after=copy.deepcopy(before);after['sequence']=1147
        after['rows']=[r for r in after['rows'] if not (r['cf']=='receipts' and r['key'].startswith('0130'))]
        next(r for r in after['rows'] if r['cf']=='receipts' and r['key']==marker)['value']=floor.hex()
        pg=self.claimed_control(state)
        result=dict(format='bigbrotr-ingest-recovery/1',state='initial-reclaimed',dataset_id=state['datasets'][0]['dataset_id'][2:],
            owner_resource_id='aa'*16,producer_slot_id=slot.hex(),producer_epoch='1',retired_requests='64',removed_input_files='64',
            native_sequence='1147',recovery_basis='closed-producer-floor-v3',native='compacted',owner_release='committed',ready=False)
        return state,pg,before,after,science,result,floor

    def test_reclamation_exact_floor_batch_science_and_postgres_preservation(self):
        with tempfile.TemporaryDirectory() as directory:
            value=self.reclaimed_fixture(Path(directory))
            self.assertEqual(check.reclaimed(*value[:6]),value[6])
            self.assertEqual(len(value[6]),139)
            for label in ('sequence','pending','fact','floor','receipt'):
                changed=copy.deepcopy(value[3])
                if label=='sequence':changed['sequence']-=1
                elif label=='pending':changed['rows']=[r for r in changed['rows'] if not (r['cf']=='facts' and r['key'].startswith('0121'))]
                elif label in ('fact','floor'):
                    row=next(r for r in changed['rows'] if r['key'].startswith('0131' if label=='floor' else '0120'))
                    row['value']=row['value'][:-2]+f'{int(row["value"][-2:],16)^1:02x}'
                else:changed['rows'].append(copy.deepcopy(next(r for r in value[2]['rows'] if r['key'].startswith('0130'))))
                with self.subTest(label=label),self.assertRaises(AssertionError):
                    check.reclaimed(value[0],value[1],value[2],changed,value[4],value[5])
            corrupt=copy.deepcopy(value[1]);corrupt['producer_outcomes'][0]['outcome_sha256']='\\x'+'00'*32
            with self.assertRaises(AssertionError):check.reclaimed(value[0],corrupt,*value[2:6])

    def test_reclamation_retry_cannot_rewrite_native_or_claim_another_removal(self):
        with tempfile.TemporaryDirectory() as directory:
            state,pg,before,after,science,result,_=self.reclaimed_fixture(Path(directory))
            retry_pg=self.claimed_control(pg)
            retry=dict(result,removed_input_files='0')
            check.reclaimed(pg,retry_pg,after,after,science,retry,retry=True)
            with self.assertRaises(AssertionError):check.reclaimed(pg,retry_pg,after,after,science,result,retry=True)
            changed=copy.deepcopy(after);changed['sequence']+=1
            with self.assertRaises(AssertionError):check.reclaimed(pg,retry_pg,after,changed,science,retry,retry=True)

    def test_retirement_cannot_promote_corrupt_observed_science_into_gc_expectations(self):
        with tempfile.TemporaryDirectory() as directory:
            state,pg,before,after,science,result,_=self.reclaimed_fixture(Path(directory))
            check.retired_initial_native(science,before)
            for prefix in ('0120','0121'):
                corrupt_before,corrupt_after=copy.deepcopy(before),copy.deepcopy(after)
                key=next(r['key'] for r in corrupt_before['rows'] if r['cf']=='facts' and r['key'].startswith(prefix))
                for dump in (corrupt_before,corrupt_after):
                    row=next(r for r in dump['rows'] if r['cf']=='facts' and r['key']==key)
                    row['value']=row['value'][:-2]+f'{int(row["value"][-2:],16)^1:02x}'
                # Corruption survives byte-exact into the observed post-GC
                # baseline. Only the earlier independent science rejects it.
                with self.subTest(prefix=prefix),self.assertRaises(AssertionError):
                    check.reclaimed(state,pg,corrupt_before,corrupt_after,science,result)
            changed=copy.deepcopy(before);changed['sequence']+=1
            with self.assertRaises(AssertionError):check.retired_initial_native(science,changed)

    def test_live_publication_requires_open_producer_and_source_under_active_owners(self):
        state=dict(owner_epochs=[dict(role='control',state='active',resource_id='control',epoch=9,
            process_incarnation='control-index'),dict(role='index',epoch=2,state='active',process_incarnation='control-index'),
            dict(role='ingest',epoch=2,state='active',resource_id='ingest',process_incarnation='native-target',
                 role_instance_id='native-role')],
            initial_ingest_successions=[dict(target_epoch=2,owner_resource_id='ingest',
                target_incarnation='native-target',target_role_instance_id='native-role')],
            initial_ingest_activations=[dict(target_epoch=2,owner_resource_id='ingest',target_incarnation='native-target',
                target_role_instance_id='native-role',activated_by_resource='control',activated_by_epoch=9,
                activated_by_incarnation='control-index')],
            index_owner_sessions=[dict(owner_epoch=2,issuer_role='control',issuer_resource='control',
                issuer_epoch=9,issuer_incarnation='control-index')],
            producer_epochs=[dict(producer_epoch=2,state='open')],
            source_streams=[dict(owner_epoch=1,state='closed',last_complete_sequence=312),
                dict(owner_epoch=2,state='open',last_complete_sequence=None)],
            consumers=[dict(consumer_role='index',consumer_id='index',current_revision=68,current_topology_generation=1)],
            consumer_frontiers=[dict(consumer_id='index',frontier_revision=68,owner_epoch=epoch,last_complete_sequence=cut)
                for epoch,cut in ((1,312),(2,10))])
        check.live_compacted_publication(state)
        for mutate in (lambda pg:pg['producer_epochs'][0].update(state='active'),
            lambda pg:pg['source_streams'][1].update(state='closed',last_complete_sequence=10),
            lambda pg:pg['source_streams'][1].update(last_complete_sequence=10),
            lambda pg:pg['owner_epochs'][1].update(state='open'),
            lambda pg:pg['consumer_frontiers'][0].update(last_complete_sequence=12),
            lambda pg:pg['index_owner_sessions'][0].update(issuer_incarnation='unrelated-control')):
            changed=copy.deepcopy(state);mutate(changed)
            with self.assertRaises(AssertionError):check.live_compacted_publication(changed)

    def test_compacted_consumer_uses_the_declared_index_database(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime=object.__new__(case.ReclamationRuntime)
            runtime.reclamation_output=Path(directory);runtime.schema=schema
            runtime.finite=Mock(return_value=dict(published_revision_after=68,new_batches=0))
            runtime.capture=Mock(return_value=({},{}))
            runtime.ch=Mock(side_effect=ValueError('declared Index reader reached'))
            with self.assertRaisesRegex(ValueError,'declared Index reader reached'):runtime.compacted_consumers()
            self.assertIn('FROM serving_fixture.',runtime.ch.call_args.args[0])

    def test_fresh_control_binds_resource_epoch_role_disposition_and_fault_backend(self):
        with tempfile.TemporaryDirectory() as directory:
            before=self.reclaimed_fixture(Path(directory))[0]
            released=self.claimed_control(before);failed=self.claimed_control(before,failed=True)
            owner=check.control_only(before,failed,claim='failed',backend_pid=125)
            self.assertEqual(owner['state'],'active')
            check.control_only(before,before,claim='none')
            for target,field,value in (('owner','dataset_id','wrong'),('owner','resource_id','wrong'),
                ('owner','epoch',9),('owner','control_fence_version',None),('owner','control_backend_start',None),
                ('owner','state','active'),('role','dataset_id','wrong'),('role','retired_at_us',None)):
                changed=copy.deepcopy(released)
                row=changed['owner_epochs'][-1] if target=='owner' else changed['role_instances'][-1]
                row[field]=value
                with self.subTest(target=target,field=field),self.assertRaises(AssertionError):check.control_only(before,changed)
            with self.assertRaises(AssertionError):check.control_only(before,failed)
            with self.assertRaises(AssertionError):check.control_only(before,failed,claim='failed',backend_pid=999)
            with self.assertRaises(AssertionError):check.control_only(before,released,claim='none')

    def test_refusal_and_unknown_commit_require_the_selected_branch_and_exact_counters(self):
        for label,(claim,error) in check.REFUSALS.items():
            self.assertEqual(check.reclamation_refusal(label,'','error: '+error+'\n'),claim)
            for stdout,stderr in (('success','error: '+error+'\n'),('','error: file unavailable\n')):
                with self.assertRaises(AssertionError):check.reclamation_refusal(label,stdout,stderr)
        for mode in ('before-forward','after-complete'):
            fault=dict(event='fault',mode=mode,target='control-closed-reclamation',profile='retained-catch-up',
                data_queries=1,data_commits=1,faults=1,server_complete=int(mode=='after-complete'),client_complete=0,backend_pid=125)
            ended=dict(fault,event='terminal',unexpected=0)
            terminal=dict(exit=0,stderr='',overflow=False,stdout=json.dumps(fault)+'\n'+json.dumps(ended)+'\n')
            check.reclamation_fault(fault,terminal,mode)
            for field,value in (('unexpected',1),('data_queries',2),('data_commits',2),('client_complete',1),('server_complete',2)):
                wrong=dict(ended,**{field:value});changed=dict(terminal,stdout=json.dumps(fault)+'\n'+json.dumps(wrong)+'\n')
                with self.assertRaises(AssertionError):check.reclamation_fault(fault,changed,mode)

    def test_proxy_cleanup_always_stops_and_preserves_the_original_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime=object.__new__(case.ReclamationRuntime);runtime.config=Mock();runtime.resources=dict(cleanup_errors=[])
            runtime.config.write_text.side_effect=OSError('restore failure')
            runtime.proxy_send=Mock(side_effect=OSError('send failure'))
            proxy=Mock();proxy.wait.side_effect=TimeoutError('join failure');proxy.stop.side_effect=RuntimeError('stop failure')
            runtime.proxy_observation=Mock(return_value=dict(stdout='',stderr='',overflow=False))
            with patch.object(case,'save',side_effect=OSError('persistence failure')):
                with self.assertRaisesRegex(ValueError,'original reclamation failure'):
                    try:raise ValueError('original reclamation failure')
                    finally:runtime.finish_reclamation_proxy(proxy,'original',Path(directory))
            proxy.stop.assert_called_once();proxy.wait.assert_called_once_with(5)
            self.assertEqual([r['resource'] for r in runtime.resources['cleanup_errors']],
                ['reclamation-config-restore','reclamation-proxy-stop-command','reclamation-proxy-join',
                 'reclamation-proxy-output','reclamation-proxy-stop'])
            runtime.resources['cleanup_errors']=[];runtime.config.write_text.side_effect=None
            runtime.proxy_send.side_effect=None;proxy.wait.side_effect=None;proxy.wait.return_value=0;proxy.stop.side_effect=None
            with patch.object(case,'save',side_effect=OSError('persistence failure')):
                with self.assertRaisesRegex(OSError,'persistence failure'):runtime.finish_reclamation_proxy(proxy,'original',Path(directory))
            self.assertEqual(proxy.stop.call_count,2)

    def test_reclamation_fault_wait_preserves_unexpected_child_failure(self):
        expected='error: producer operation unavailable: CommitUnknown\n'
        state=dict(Running=False,ExitCode=4,Pid=0,OOMKilled=False)
        failures=[(state,'','error: producer operation unavailable: Native(InsufficientReservation)\n'),
            (dict(state,ExitCode=0),'',expected),(dict(state,Pid=1),'',expected),
            (dict(state,OOMKilled=True),'',expected),(state,'unexpected success\n',expected)]
        for changed,stdout,stderr in failures:
            with self.subTest(state=changed,stdout=stdout,stderr=stderr),tempfile.TemporaryDirectory() as directory:
                output=Path(directory);runtime=object.__new__(case.ReclamationRuntime)
                runtime.proxy_event=Mock(side_effect=AssertionError('fixture event deadline'))
                runtime.inspect=Mock(return_value=dict(State=changed))
                runtime.docker=Mock(return_value=SimpleNamespace(stdout=stdout,stderr=stderr))
                proxy=object();entry=dict(id='a'*64)
                with patch.object(case.time,'monotonic',return_value=0):
                    with self.assertRaisesRegex(ValueError,'unexpected reclamation child result before proxy fault'):
                        runtime.wait_reclamation_fault(proxy,entry,output)
                runtime.proxy_event.assert_called_once_with(proxy,'fault',1)
                runtime.docker.assert_called_once_with('logs',entry['id'],timeout=30)
                self.assertEqual((output/'serve.stdout').read_text(),stdout)
                self.assertEqual((output/'serve.stderr').read_text(),stderr)
                self.assertEqual(base.load(output/'serve-terminal.json'),dict(container_id=entry['id'],state=changed))

    def test_reclamation_fault_wait_allows_delayed_fault_after_expected_child_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory);runtime=object.__new__(case.ReclamationRuntime)
            fault=dict(event='fault',faults=1)
            runtime.proxy_event=Mock(side_effect=[AssertionError('fixture event deadline'),fault])
            runtime.inspect=Mock(return_value=dict(State=dict(Running=False,ExitCode=4,Pid=0,OOMKilled=False)))
            runtime.docker=Mock(return_value=SimpleNamespace(stdout='',stderr='error: producer operation unavailable: CommitUnknown\n'))
            proxy=object();entry=dict(id='a'*64)
            with patch.object(case.time,'monotonic',return_value=0):
                self.assertIs(runtime.wait_reclamation_fault(proxy,entry,output),fault)
            self.assertEqual(runtime.proxy_event.call_count,2)
            runtime.inspect.assert_called_once_with(entry)
            self.assertEqual(runtime.docker.call_count,1)
            self.assertEqual((output/'serve.stderr').read_text(),'error: producer operation unavailable: CommitUnknown\n')

    def test_reclamation_fault_wait_keeps_one110s_deadline(self):
        runtime=object.__new__(case.ReclamationRuntime);now=[0]
        def event(*args):
            now[0]+=1
            raise AssertionError('fixture event deadline')
        runtime.proxy_event=Mock(side_effect=event)
        runtime.inspect=Mock(return_value=dict(State=dict(Running=True)))
        runtime.docker=Mock()
        with patch.object(case.time,'monotonic',side_effect=lambda:now[0]):
            with self.assertRaisesRegex(ValueError,'reclamation fault deadline'):
                runtime.wait_reclamation_fault(object(),dict(id='a'*64),Path('unused'))
        self.assertEqual(runtime.proxy_event.call_count,110)
        self.assertEqual(runtime.inspect.call_count,109)
        self.assertTrue(all(call.args[2]==1 for call in runtime.proxy_event.call_args_list))
        runtime.docker.assert_not_called()

    def test_reclamation_fault_wait_refuses_deadline_crossing_observations(self):
        for boundary in ('event','inspect','logs'):
            with self.subTest(boundary=boundary),tempfile.TemporaryDirectory() as directory:
                output=Path(directory);runtime=object.__new__(case.ReclamationRuntime);now=[0]
                def event(*args):
                    if boundary=='event':
                        now[0]=110
                        return dict(event='fault',faults=1)
                    raise AssertionError('fixture event deadline')
                def inspect(entry):
                    if boundary=='inspect':now[0]=110
                    return dict(State=dict(Running=False,ExitCode=4,Pid=0,OOMKilled=False))
                def logs(*args,**kwargs):
                    now[0]=110
                    return SimpleNamespace(stdout='',stderr='error: producer operation unavailable: CommitUnknown\n')
                runtime.proxy_event=Mock(side_effect=event);runtime.inspect=Mock(side_effect=inspect)
                runtime.docker=Mock(side_effect=logs)
                with patch.object(case.time,'monotonic',side_effect=lambda:now[0]):
                    with self.assertRaisesRegex(ValueError,'reclamation fault deadline'):
                        runtime.wait_reclamation_fault(object(),dict(id='a'*64),output)
                if boundary=='event':runtime.inspect.assert_not_called()
                if boundary!='logs':runtime.docker.assert_not_called()
                else:self.assertTrue((output/'serve-terminal.json').is_file())

    def test_reclamation_fault_wait_propagates_other_proxy_failures(self):
        for failure in (AssertionError(),AssertionError('proxy overflow'),json.JSONDecodeError('bad record','{',1)):
            with self.subTest(failure=type(failure).__name__,args=failure.args):
                runtime=object.__new__(case.ReclamationRuntime)
                runtime.proxy_event=Mock(side_effect=failure);runtime.inspect=Mock();runtime.docker=Mock()
                with patch.object(case.time,'monotonic',return_value=0):
                    with self.assertRaises(type(failure)) as caught:
                        runtime.wait_reclamation_fault(object(),dict(id='a'*64),Path('unused'))
                self.assertIs(caught.exception,failure)
                runtime.inspect.assert_not_called();runtime.docker.assert_not_called()

    def test_outer_cleanup_retries_every_retained_proxy_even_after_stop_failure(self):
        runtime=object.__new__(case.ReclamationRuntime);runtime.resources=dict(cleanup_errors=[])
        first,second=Mock(),Mock();first.stop.side_effect=OSError('first stop')
        runtime.reclamation_proxy_handles=[first,second]
        with patch.object(case.RetainedRuntime,'cleanup') as cleanup:runtime.cleanup()
        first.stop.assert_called_once();second.stop.assert_called_once();cleanup.assert_called_once()

    def test_retained_bridge_and_startup_keep_exact_old_history_and_identity(self):
        from test_first_launch_index_live import bridge_fixture
        state,binding,publisher,generation=bridge_fixture()
        generation.update(storage_incarnation='\\x'+binding[3].hex(),table_identity_digest='\\x'+binding[4].hex())
        wanted='1dba3e1ed7c5d9673d58296d9d3dca5053fdfd04852c66bcd2818e95466237c8'
        self.assertEqual(case.topology.handoff_digest(binding,bytes.fromhex('77'*16),bytes.fromhex('88'*32),
            retained_boundary=True).hex(),wanted)
        for table in ('index_frontier_handoffs','consumer_frontier_revisions','consumer_frontiers'):
            for row in state[table]:
                if 'frontier_revision' in row:row['frontier_revision']+=60
                if 'expected_revision' in row:row['expected_revision']+=60
                if 'previous_revision' in row:row['previous_revision']+=60
                if row.get('last_complete_sequence')==12:row['last_complete_sequence']=312
        state['index_frontier_handoffs'][0].update(predecessor_through=312,handoff_digest='\\x'+wanted)
        case.topology.bridge_history(state,binding,publisher,generation,retained_boundary=True)
        for field,value in (('handoff_digest','\\x'+'00'*32),('expected_revision',4),('predecessor_through',12)):
            changed=copy.deepcopy(state);changed['index_frontier_handoffs'][0][field]=value
            with self.assertRaises(AssertionError):case.topology.bridge_history(changed,binding,publisher,generation,retained_boundary=True)
        startup=dict(format='bigbrotr-live-initial-index/1',state='bridged',generation_id=generation['generation_id'][2:],
            storage_incarnation=generation['storage_incarnation'][2:],table_identity_digest=generation['table_identity_digest'][2:],
            previous_owner_epoch=1,owner_epoch=2,published_revision_before=64,published_revision_after=65,
            published_cut=dict(dataset_id=generation['dataset_id'][2:],topology_generation='1',bounds=[
                dict(stream=dict(shard_id=0,owner_epoch=str(epoch)),last_complete_sequence=str(cut)) for epoch,cut in ((1,312),(2,0))]),ready=False)
        case.live_oracle.startup_index(startup,generation,retained_boundary=True)
        for field,value in (('published_revision_after',5),('storage_incarnation','00'*16),('owner_epoch',3)):
            with self.assertRaises(AssertionError):case.live_oracle.startup_index(dict(startup,**{field:value}),generation,retained_boundary=True)
        before={t:[] for t in index.INDEX_TABLES};before.update(consumers=[dict(consumer_role='index',consumer_id='index')],
            consumer_frontier_revisions=[],consumer_frontiers=[],index_frontier_plans=[dict(frontier_revision=64,engine_receipt_digest='receipt',xmin='64')])
        changed=copy.deepcopy(before);changed['index_frontier_plans'][0]['engine_receipt_digest']='changed-during-activation'
        with self.assertRaises(AssertionError):case.live_oracle.append_only_history(before,changed)

    def test_retained_recovery_digest_seals_the_earlier_bridge_before_batch66(self):
        from test_first_launch_index_topology import recovery_fixture
        state,binding,owners,_,handoff=recovery_fixture()
        digests=['e5429bcf4fdc199a2cfaf2844c791d5f9b0ec73fff9d1b1186f33b5bad002b61',
            '041278638277dc63a34af6c905b3e2a86019ada5ebe65c3b331e20bd2ea460ab']
        handoff['handoff_digest']='\\x'+'1dba3e1ed7c5d9673d58296d9d3dca5053fdfd04852c66bcd2818e95466237c8'
        commitments=[(n,bytes([n])*32,bytes([n+16])*32) for n in list(range(1,65))+[66,67,68]]
        for n,digest in enumerate(digests):
            owners[n]['exclusion_digest']='\\x'+digest
            head=64 if n==0 else 68
            state['index_owner_sessions'][n+1].update(recovered_revision=head,recovered_plan='\\x'+bytes([head]).hex()*32,recovery_digest='\\x'+digest)
        case.topology.recovery_sessions(state,binding,owners,commitments,handoff,retained_boundary=True)
        for mutation in ('bridge','owner','receipt'):
            changed=copy.deepcopy(state);hs=copy.deepcopy(handoff);cs=copy.deepcopy(commitments)
            if mutation=='bridge':hs['handoff_digest']='\\x'+'00'*32
            elif mutation=='owner':changed['index_owner_sessions'][-1]['owner_incarnation']='\\x'+'ff'*16
            else:cs[-1]=(68,cs[-1][1],bytes(32))
            with self.assertRaises(AssertionError):case.topology.recovery_sessions(changed,binding,owners,cs,hs,retained_boundary=True)

    def test_retained_finite_report_requires_exact_compacted_basis_and_no_new_work(self):
        generation=dict(dataset_id='\\x'+'11'*16,generation_id='\\x'+'22'*16,
            storage_incarnation='\\x'+'33'*16,table_identity_digest='\\x'+'44'*32)
        state=dict(projection_generations=[generation],index_owner_sessions=[])
        cut=dict(topology_generation=1,bounds=[dict(shard_id=0,owner_epoch=epoch,last_complete_sequence=last)
            for epoch,last in ((1,312),(2,10))])
        for epoch in (3,4):
            report=dict(format='bigbrotr-initial-index-catch-up/2',state='complete',dataset_id='11'*16,generation_id='22'*16,
                storage_incarnation='33'*16,table_identity_digest='44'*32,previous_owner_epoch=epoch-1,owner_epoch=epoch,
                published_revision_before=68,published_revision_after=68,source_epoch=2,source_through=10,published_through=10,
                source_cut=cut,published_cut=cut,index_handoff_revision=65,published_handoff=False,recovered_pending=False,new_batches=0,
                ready=False,acknowledgement='not-performed',ingest_retirement='not-performed',
                topology_transition='existing-ingest-handoff-verified',recovery_basis='closed-producer-floor-v3',compacted_requests='64')
            # This isolates the report parser; other controls separately verify
            # the typed history and its exact delegated recovery commitments.
            with patch.object(case.live_oracle.restart,'unchanged_except'),patch.object(case.live_oracle.restart,'owner_changes'), \
                    patch.object(case.topology,'published_history',return_value={}):
                case.live_oracle.no_work_rebind(state,state,report,{},schema,{},owner_epoch=epoch,retained_boundary=True)
                for field,value in (('compacted_requests','65'),('recovery_basis','original-inputs'),('index_handoff_revision',5),
                    ('published_revision_after',8),('new_batches',1),('ready',True)):
                    with self.assertRaises(AssertionError):case.live_oracle.no_work_rebind(state,state,dict(report,**{field:value}),
                        {},schema,{},owner_epoch=epoch,retained_boundary=True)

    def test_retained_epoch_two_profile_counts_all_322_scientific_and_572_index_rows(self):
        from initial_succession_oracle import successor_manifest
        from installed_family_science import NetworkScience
        from installed_archive_catch_up_model import archive_science
        from transition_replay import native_rows
        with tempfile.TemporaryDirectory() as directory:
            state,_,_,_,_,dump,first=saturation_fixture(Path(directory))
            previous=next(v for cf,k,v in first['native'] if cf=='default' and k==b'\x01\x01')
            fresh=[bytes([120+k])*16 for k in range(1,10)]
            manifest=successor_manifest(previous,bytes.fromhex('cc'*16),fresh)
            model=SimpleNamespace(ds=state['datasets'][0],slot=state['producer_epochs'][0]['producer_slot_id'],
                legacy=dict(previous_manifest='\\x'+previous.hex()),inputs={},native=copy.deepcopy(dump))
            new=next(row['event'] for row in base.load(ROOT/'tests/fixtures/acquisition/selector-events.json')['events'] if row['name']=='included')
            encoded=NetworkScience(model,dict(epoch=2,topology=1,manifest=manifest,domains=['\\x'+d.hex() for d in fresh]),
                acknowledge=True,new_event=new,endpoint=source.ENDPOINT)
            encoded.network(True,50000,51000,52000,1,(51001,52001))
            native_rows_value=dict(rows=native_rows(encoded.values))
            science=archive_science(native_rows_value,live_epoch=2,retained_epoch2=True)
            self.assertEqual(sum(map(len,science['tables'].values())),322)
            self.assertEqual([len(rows) for rows in science['tables'].values()],[73,62,62,62,63])
            self.assertEqual(len(science['batches']),67)
            projected=index.expected_rows(science,bytes.fromhex('66'*16),schema,counts=case.topology.RETAINED_COUNTS)
            self.assertEqual(sum(map(len,projected.values())),572)
            self.assertEqual((len(first['inputs']),min(encoded.inputs)),(64,1))
            invalid=copy.deepcopy(native_rows_value)
            invalid['rows']=[r for r in invalid['rows'] if not (r['cf']=='facts' and r['key'].startswith('0120') and r['key'].endswith('000000000000000a'))]
            with self.assertRaises(AssertionError):archive_science(invalid,live_epoch=2,retained_epoch2=True)

    def test_spent_control_fencing_is_exact_without_excusing_scientific_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            before=self.reclaimed_fixture(Path(directory))[0]
            old=index.only(before['owner_epochs'],role='control')
            role=index.only(before['role_instances'],role_instance_id=old['role_instance_id'])
            old.update(state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None)
            role['retired_at_us']=None
            after=self.claimed_control(before)
            successor=max((r for r in after['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
            fenced=index.only(after['owner_epochs'],role_instance_id=old['role_instance_id'])
            fenced.update(state='released',released_at_us=old['lease_expires_at_us']+10,exclusion_method='storage_fenced',
                exclusion_digest=check.control_exclusion(old,successor),xmin='2')
            index.only(after['role_instances'],role_instance_id=role['role_instance_id']).update(
                retired_at_us=old['lease_expires_at_us']+15,xmin='2')
            check.control_only(before,after)
            for missing in ('owner-fence','role-retirement'):
                changed=copy.deepcopy(after)
                if missing=='owner-fence':
                    current=index.only(changed['owner_epochs'],role_instance_id=old['role_instance_id'])
                    current.clear();current.update(old)
                current=index.only(changed['role_instances'],role_instance_id=role['role_instance_id'])
                current.clear();current.update(role)
                with self.subTest(missing=missing),self.assertRaises(AssertionError):check.control_only(before,changed)
            for mutate in (lambda pg:pg['producer_requests'][0].update(request_sequence=63),
                lambda pg:index.only(pg['owner_epochs'],role_instance_id=old['role_instance_id']).update(exclusion_digest='\\x'+'00'*32),
                lambda pg:index.only(pg['role_instances'],role_instance_id=role['role_instance_id']).update(retired_at_us=old['lease_expires_at_us'])):
                changed=copy.deepcopy(after);mutate(changed)
                with self.assertRaises(AssertionError):check.control_only(before,changed)

    def test_new_case_keeps_declared_existing_workload_and_client_bounds(self):
        self.assertIn('p01-first-launch-closed-reclamation',base.RESTART_CASES)
        self.assertEqual((case.ReclamationRuntime.source_cut,case.ReclamationRuntime.publication_revision,case.ReclamationRuntime.request_count),(312,64,64))
        self.assertIn('closed_reclamation',case.RECLAMATION_STAGES)
        self.assertEqual(case.RECLAMATION_STAGES[-1],'cleanup')

    def test_reclamation_dispatch_requires_every_stage_and_preserves_first_failure(self):
        for failing in (False,True):
            with tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);args=arguments(root);args.case='p01-first-launch-closed-reclamation';checks=[]
                with patch.object(case,'ReclamationRuntime') as runtime:
                    if failing:runtime.return_value.closed_reclamation.side_effect=AssertionError('unconfirmed closed reclamation')
                    base.first_launch(args,checks,{})
                    runtime.return_value.cleanup.assert_called_once()
                    runtime.return_value.restart_baseline.assert_not_called()
                    runtime.return_value.ordinary.assert_not_called()
                    for name in ('compacted_successor','compacted_consumers','compacted_history_export'):
                        if failing:getattr(runtime.return_value,name).assert_not_called()
                        else:getattr(runtime.return_value,name).assert_called_once()
                self.assertEqual(len(checks),11)
                rows={row['name']:row['result'] for row in checks}
                self.assertEqual(rows['first_launch_cleanup'],'PASS')
                if failing:
                    self.assertEqual(base.load(root/'failure.json')['reason'],'unconfirmed closed reclamation')
                    self.assertEqual(rows['first_launch_closed_reclamation'],'FAIL')
                    for name in ('compacted_successor','compacted_consumers','compacted_history_export'):
                        self.assertEqual(rows['first_launch_'+name],'NOT_RUN')
                else:self.assertEqual(set(rows.values()),{'PASS'})

if __name__=='__main__':
    unittest.main()
