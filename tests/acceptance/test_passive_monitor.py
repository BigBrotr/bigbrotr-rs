"""Passive Monitor fixture/oracle controls; no product or database execution."""
import base64
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from cases import first_launch as base
from cases import first_launch_passive_monitor as case
from cases import first_launch_passive_oracle as oracle

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'experiments/ingest-bootstrap'))
sys.path.insert(0,str(ROOT/'experiments/control-migrations'))
if 'bootstrap_oracle' not in sys.modules:
    spec = importlib.util.spec_from_file_location('bootstrap_oracle',ROOT/'experiments/ingest-bootstrap/oracle.py')
    module = importlib.util.module_from_spec(spec); sys.modules[spec.name] = module; spec.loader.exec_module(module)
from installed import set_value
from installed_sync_client import exact
import installed_passive_monitor_client as peer
from installed_sync_oracle import decode
from installed_archive_catch_up_oracle import cut_digest

spec = importlib.util.spec_from_file_location('passive_test_reference',ROOT/'experiments/identity-contract/probe.py')
encoder = importlib.util.module_from_spec(spec); sys.modules[spec.name] = encoder; spec.loader.exec_module(encoder)


def sample():
    dataset = '01'*16
    def ref(kind, ordinal=1):
        return dict(dataset_id=dataset,dictionary_kind=kind,allocation_domain=f'{kind:02x}'*16,ordinal=str(ordinal))
    refs = dict(endpoint_id=ref(1),observer_id=ref(2),route_id=ref(6),access_scope_id=ref(3),
        method_id=ref(5),clock_id=ref(2,2),visibility_scope_id=ref(4,3))
    moment = lambda utc,elapsed:dict(observed_at=str(utc),elapsed_us=str(elapsed))
    event = json.loads((ROOT/'tests/fixtures/ingest/import-event.jsonl').read_text())
    ident = 'ab'*16+'0000000000000001'
    request = ['REQ',ident,dict(kinds=[1],limit=1)]
    messages = [['EVENT',ident,event],['EOSE',ident]]
    payloads = [json.dumps(v,ensure_ascii=False,separators=(',',':')).encode() for v in messages]
    # Independent fixed single-frame recipe: EVENT long header, EOSE short,
    # empty Close; client REQ/CLOSE each short+mask and empty masked Close.
    counters = dict(rx_physical_frames=3,rx_messages=3,event_candidates=1,
        rx_ws_bytes=sum(map(len,payloads))+4+2+2,rx_payload_bytes=sum(map(len,payloads)),
        tx_ws_bytes=len(json.dumps(request,separators=(',',':')).encode())+6+
            len(json.dumps(['CLOSE',ident],separators=(',',':')).encode())+6+6)
    wire = dict(counters,clock_resolution_us=1)
    digest = '4522a214e60794e362dd633d59223278667ad98c90905346c889dea69694b334'
    report = dict(attempt_id='03'*16,effective_authority_us='4000000',measurement=dict(
        dataset_id=dataset,shard_id=0,owner_epoch='1',sequence='10',payload_digest=digest))
    payload = dict(attempt_id='03'*16,subject_kind=0,**{k:refs[k] for k in ('endpoint_id','observer_id','route_id','access_scope_id','method_id')},
        profile=oracle.PROFILE+[4000000],clock_quality=dict(clock_id=refs['clock_id'],resolution_us='1',uncertainty_us=None,anomaly_flags=0),
        state_kind=2,prior_unknown=None,observed=dict(execution=dict(destination=1,result=0,error=None,observed_at='101'),
            open=dict(tcp_connected=moment(102,1),tls_completed=None,websocket_opened=moment(103,2)),
            read=dict(request_flushed=moment(104,3),first_valid_event=dict(event_id=event['id'],received=moment(105,4),verified=moment(106,5)),
                eose=moment(107,6),eose_prefix_verified=True,auth_observed=False,correlated_closed=False),counters=counters),
        ended=moment(108,7),termination=dict(kind=0,phase=None,failure=None,limit_code=None),teardown=1)
    fact = dict(source_record_id={k:v for k,v in report['measurement'].items() if k != 'payload_digest'},record_schema_version=1,
        record_kind='measurement',visibility_scope_id=refs['visibility_scope_id'],payload_digest=digest,recorded_at='109',payload=payload)
    document = dict(format='bigbrotr-archive-measurement/1',state='present',audience='trusted-local-operator',catalog_id='09'*16,
        frontier_revision='4',topology_generation='0',policy_revision='1',cut=dict(format='bigbrotr-native-cut/2',snapshot_id=5,
            request_digest='42'*32,family_counts=[9,0,0,0,0,1]),fact=fact)
    # A separately spelled literal array is cross-checked with the maintained
    # Node reference before freezing the digest; no Rust implementation oracle.
    r = lambda kind,n=1:[bytes.fromhex(dataset),kind,bytes([kind])*16,n]
    expected = [[bytes.fromhex(dataset),0,1,10],'measurement',1,r(4,3),109,
        [bytes([3])*16,[[0,r(1)],r(2),r(6),r(3),r(5)],
         [1,300000000,15000000,10000000,1,1,1,1,1,1,0,2097152,262144,64,32,3,4194304,65536,2000000,9,4096,64,0,4000000],
         [r(2,2),1,None,0],[2,[[[1,[0],101],[[102,1],None,[103,2]],
            [[104,3],[bytes.fromhex(event['id']),[105,4],[106,5]],[107,6],True,False,False],
            [3,3,1,counters['rx_ws_bytes'],counters['rx_payload_bytes'],counters['tx_ws_bytes']]], [108,7],[0],1]]]]
    return document,report,refs,wire,event,expected


def send_masked(connection, payload, opcode=1):
    payload = payload if isinstance(payload,bytes) else json.dumps(payload,separators=(',',':')).encode()
    assert len(payload) < 126
    mask = b'unit'
    connection.sendall(bytes([0x80|opcode,0x80|len(payload)])+mask+
        bytes(value^mask[index%4] for index,value in enumerate(payload)))


def server_frame(connection):
    first,second = exact(connection,2); assert not second&128
    size = second if second < 126 else int.from_bytes(exact(connection,2),'big')
    return first&15,exact(connection,size)


def literal_client(address, *, wrong_command=None):
    with socket.create_connection(address,timeout=1) as connection:
        connection.settimeout(1)
        key=base64.b64encode(bytes(range(16)))
        connection.sendall(b'GET /installed-fixture HTTP/1.1\r\nHost: 127.0.0.1:7447\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: '+key+b'\r\n\r\n')
        header=bytearray()
        while not header.endswith(b'\r\n\r\n'):header.extend(exact(connection,1))
        ident='ab'*16+'0000000000000001'
        send_masked(connection,wrong_command if wrong_command is not None else ['REQ',ident,dict(kinds=[1],limit=1)])
        if wrong_command is not None:
            assert connection.recv(1) == b''
            return
        opcode,event=server_frame(connection);assert opcode == 1
        assert json.loads(event)[:2] == ['EVENT',ident]
        assert json.loads(server_frame(connection)[1]) == ['EOSE',ident]
        send_masked(connection,['CLOSE',ident]);send_masked(connection,b'',8)
        assert server_frame(connection) == (8,b'')
        connection.shutdown(socket.SHUT_WR);assert connection.recv(1) == b''


def retained_fixture():
    document,report,refs,wire,event,expected=sample()
    initialized=dict(private_scope=refs['visibility_scope_id'],producer_slot_id='44'*16,recovery_generation='45'*16)
    source=dict(endpoint_id='71'*16,access_scope_id='72'*16,enrollment_generation='1',route_id='73'*16)
    selection=dict(format='bigbrotr-monitor-once-v1',**source,visibility='private')
    report.update(format='bigbrotr-monitor-once/1',state='completed',result='admitted',profile=1,ready=False,
        source=source,context=refs,request_id=dict(producer_slot_id='44'*16,producer_epoch='1',request_sequence='4'))
    dataset=bytes.fromhex('01'*16);slot=bytes.fromhex('44'*16)
    r=lambda n:[dataset,4,bytes([4])*16,n]
    first=[['dictionary',1,[r(n),1,'visibility',1,{'class':name}]] for n,name in [(1,'public'),(2,'private')]]
    second=[['dictionary',1,[r(3),1,'visibility',1,{'class':'private'}]]]
    originals=[b'bigbrotr/producer-request/1\0'+encoder.cbor([[slot,1,n],'admit-evidence',1,[1,scope,entries]])
        for n,scope,entries in [(1,r(1),first),(2,r(3),second)]]
    originals += [oracle.dictionary_request(refs,'44'*16,encoder.cbor),
        b'bigbrotr/producer-request/1\0'+encoder.cbor([[slot,1,4],'admit-evidence',1,[1,r(3),'measurement/1',expected[5]]])]
    report['request_digest']=hashlib.sha256(originals[3]).hexdigest()
    state={name:[] for name in oracle.EMPTY_TABLES}
    state['allocation_domains']=[dict(dataset_id='\\x'+'01'*16,dictionary_kind=kind,allocation_domain='\\x'+f'{kind:02x}'*16)
        for kind in range(1,10)]
    owner=dict(role='control',resource_id='\\x'+'51'*16,epoch=1,process_incarnation='\\x'+'52'*16)
    state['owner_epochs']=[owner]
    context=b''
    for key,kind,ordinal in [('endpoint_id',1,1),('observer_id',2,1),('route_id',6,1),('access_scope_id',3,1),
            ('method_id',5,1),('clock_id',2,2),('visibility_scope_id',4,3)]:
        context += dataset+kind.to_bytes(2,'big')+bytes([kind])*16+ordinal.to_bytes(8,'big')
    state['measurement_dispatches']=[dict(dataset_id='\\x'+'01'*16,attempt_id='\\x'+'03'*16,profile_version=1,
        context_bytes='\\x'+context.hex(),authority_ceiling_us=4000000,created_at_us=90,dispatch_deadline_us=4000090,
        config_revision=1,policy_revision=1,endpoint_id='\\x'+'71'*16,access_scope_id='\\x'+'72'*16,
        route_id='\\x'+'73'*16,enrollment_generation=1,issuer_resource=owner['resource_id'],issuer_epoch=1,
        issuer_incarnation=owner['process_incarnation'],issuer_role='control')]
    state['producer_requests']=[];state['producer_outcomes']=[]
    facts={n:[[dataset,0,1,n],bytes([n])*32] for n in range(1,10)}
    facts[10]=[[dataset,0,1,10],bytes.fromhex(report['measurement']['payload_digest'])]
    receipts=[]
    for n,(original,(first,last)) in enumerate(zip(originals,[(1,2),(3,3),(4,9),(10,10)]),1):
        digest=hashlib.sha256(original).digest()
        common=dict(producer_slot_id='\\x'+'44'*16,producer_epoch=1,request_sequence=n,request_digest='\\x'+digest.hex(),manifest_sha256='\\x'+'46'*32)
        request=dict(common,input_bytes=len(original),input_profile=1,acquisition_job_id=None,acquisition_scope_id=None,
            dataset_id='\\x'+'01'*16,issuer_role='control',issuer_resource=owner['resource_id'],issuer_epoch=1,issuer_incarnation=owner['process_incarnation'],
            monitor_attempt_id='\\x'+'03'*16 if n==4 else None,monitor_resolution=False if n==4 else None,
            relay_endpoint_id='\\x'+'71'*16 if n==4 else None,relay_access_scope_id='\\x'+'72'*16 if n==4 else None,
            relay_route_id='\\x'+'73'*16 if n==4 else None,relay_enrollment_generation=1 if n==4 else None)
        dependencies=list(range(first,last+1)) if n<=2 else list(range(3,last+1))
        receipt=[1,[slot,1,n],digest,[dataset,0,1,bytes.fromhex('45'*16),1],
            [facts[k] for k in range(first,last+1)],[facts[k] for k in dependencies]]
        encoded=b'bigbrotr/receipt/1\0'+encoder.cbor(receipt)
        state['producer_requests'].append(request)
        state['producer_outcomes'].append(dict(common,outcome_kind=1,policy_epoch=1,outcome_bytes='\\x'+encoded.hex(),
            dataset_id='\\x'+'01'*16,recorder_role='control',recorder_resource=owner['resource_id'],recorder_epoch=1,recorder_incarnation=owner['process_incarnation'],
            outcome_sha256='\\x'+hashlib.sha256(encoded).hexdigest()))
        receipts.append(receipt)
    catalog='09'*16
    state.update(consumers=[dict(consumer_role='archive',consumer_id='\\x'+catalog,current_revision=4,current_topology_generation=0)],
        consumer_frontiers=[dict(consumer_id='\\x'+catalog,frontier_revision=4,shard_id=0,owner_epoch=1,last_complete_sequence=10)],
        archive_frontier_snapshots=[dict(frontier_revision=4,snapshot_id=5,topology_generation=0,
            consumer_id='\\x'+catalog,catalog_id='\\x'+catalog,dataset_id='\\x'+'01'*16,
            **dict(zip(oracle.COUNT_COLUMNS,[9,0,0,0,0,1])))],archive_attempts=[],archive_frontier_batches=[])
    expectations=[]
    for n,(receipt,(first,last)) in enumerate(zip(receipts,[(1,2),(3,3),(4,9),(10,10)]),1):
        digest=hashlib.sha256(b'bigbrotr/archive-batch/1\0'+encoder.cbor([dataset,1,0,[[0,1,first,last]],receipt[4]])).hexdigest()
        attempt=dict(ordinal=n,attempt_id='\\x'+f'{n+80:02x}'*16,expected_predecessor=n-1,
            batch_digest='\\x'+digest,schema_version=1,topology_generation=0,dataset_id='\\x'+dataset.hex(),catalog_id='\\x'+catalog)
        state['archive_attempts'].append(attempt)
        state['archive_frontier_batches'].append(dict(frontier_revision=4,attempt_id=attempt['attempt_id']))
        expectations.append(dict(dataset_id=dataset.hex(),schema_version=1,topology_generation=0,batch_digest=digest,
            record_count=last-first+1,input_ranges=[dict(shard_id=0,owner_epoch=1,first_sequence=first,last_sequence=last)],
            attempt_id=attempt['attempt_id'][2:],publication_ordinal=n))
    state['archive_frontier_snapshots'][0]['request_digest']='\\x'+cut_digest('\\x'+dataset.hex(),0,expectations,[(1,10)],5)
    return state,initialized,report,refs,selection,expected[5],originals,receipts


class PassiveMonitorControls(unittest.TestCase):
    def test_missing_candidate_preserves_monitor_stage_names_and_not_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = subprocess.run([sys.executable,'-B',str(ROOT/'tests/acceptance/run.py'),'p04-passive-monitor',
                '--source-root',str(ROOT),'--candidate-manifest',str(root/'absent'), '--evidence',str(root/'evidence')],
                capture_output=True,text=True,timeout=5)
            report = json.loads((root/'evidence/result.json').read_text())
            self.assertEqual((result.returncode,report['result']),(2,'NOT_RUN'))
            self.assertEqual([r['name'] for r in report['checks']],['first_launch_'+s for s in case.STAGES])
            self.assertFalse(list((root/'evidence/commands').iterdir()))
            self.assertNotIn('signed_acquisition',json.dumps(report['checks']))
            self.assertNotIn('ordinary_history_export',json.dumps(report['checks']))

    def test_monitor_failure_does_not_mark_restart_inspection_or_cleanup_pass_falsely(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); checks=[]
            args=SimpleNamespace(case='p04-passive-monitor',evidence=root,source_root=ROOT)
            with patch.object(case,'PassiveMonitorRuntime') as runtime:
                runtime.return_value.passive_measurement.side_effect=ValueError('refused/no-I/O')
                base.first_launch(args,checks,{})
                runtime.return_value.cleanup.assert_called_once()
                runtime.return_value.restarted_measurement_inspection.assert_not_called()
            self.assertEqual([r['result'] for r in checks],['PASS']*4+['FAIL','NOT_RUN','PASS'])
            self.assertEqual(json.loads((root/'failure.json').read_text())['stage'],'passive_measurement')

    def test_recipe_uses_only_explicit_passive_profile_and_keeps_original_resource_limits(self):
        import tomllib
        original=(ROOT/'config/recipes/acceptance.toml').read_text()
        before=tomllib.loads(original); after=tomllib.loads(case.deployment_recipe(original,set_value))
        self.assertEqual(after['topology']['local_roles'],['control','ingest','monitor','archive'])
        self.assertFalse(after['sync']['enabled']); self.assertTrue(after['monitor']['enabled'])
        self.assertEqual(after['monitor']['checks'],['open','read'])
        self.assertEqual(after['monitor']['store_checks'],['open','read'])
        self.assertEqual((after['monitor']['check_timeout'],after['monitor']['retries']),('15s',1))
        self.assertEqual(after['monitor']['endpoints'],dict(mode='fixed',urls=[case.ENDPOINT]))
        self.assertFalse(after['archive']['reader']['enabled'])
        self.assertEqual(after['runtime'],before['runtime']); self.assertEqual(after['network'],before['network'])

    def test_selection_is_single_atomic_protected_file_with_real_id_spelling(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'monitor-once.json'
            selection=dict(format='bigbrotr-monitor-once-v1',endpoint_id='71'*16,access_scope_id='72'*16,
                route_id='73'*16,enrollment_generation='1',visibility='private')
            case.publish_selection(path,selection)
            self.assertEqual(json.loads(path.read_text()),selection)
            self.assertEqual(path.stat().st_mode&0o777,0o600)
            self.assertEqual(path.stat().st_nlink,1)
            with self.assertRaises(ValueError):case.publish_selection(path,selection)
            self.assertEqual([p.name for p in path.parent.iterdir()],['monitor-once.json'])

    def test_typed_original_has_independent_frozen_bytes_and_digest(self):
        document,report,refs,wire,event,expected=sample()
        value,encoded=oracle.scientific(document,report,refs,wire,event,100,200,encoder.cbor)
        self.assertEqual(encoded,b'bigbrotr/fact/1\0'+encoder.cbor(expected))
        self.assertEqual(hashlib.sha256(encoded).hexdigest(),report['measurement']['payload_digest'])
        self.assertEqual(value,expected[5])

    def test_missing_wrong_digest_no_io_counts_refs_and_timing_cannot_pass(self):
        document,report,refs,wire,event,_=sample()
        changes=[lambda d:d.update(state='absent'),lambda d:d['fact'].update(payload_digest='00'*32),
            lambda d:d['fact']['payload'].update(state_kind=0,observed=None),
            lambda d:d['fact']['payload']['observed']['execution'].update(destination=0),
            lambda d:d['fact']['payload']['observed']['counters'].update(rx_messages=2),
            lambda d:d['fact']['payload']['observed']['counters'].update(rx_physical_frames=2),
            lambda d:d['fact']['payload']['observed']['counters'].update(rx_ws_bytes=0),
            lambda d:d['fact']['payload']['observed']['counters'].update(event_candidates=0),
            lambda d:d['fact']['payload']['observed']['read'].update(auth_observed=True),
            lambda d:d['fact']['payload']['observed']['read']['first_valid_event']['verified'].update(elapsed_us='0'),
            lambda d:d['fact']['payload']['route_id'].update(ordinal='2'),
            lambda d:d['fact']['payload']['profile'].__setitem__(13,65)]
        for change in changes:
            bad=copy.deepcopy(document);change(bad)
            with self.subTest(change=change),self.assertRaises((AssertionError,TypeError,ValueError)):
                oracle.scientific(bad,report,refs,wire,event,100,200,encoder.cbor)
        for missing in (None,{},dict(document,fact=None)):
            with self.assertRaises((AssertionError,TypeError,ValueError)):
                oracle.scientific(missing,report,refs,wire,event,100,200,encoder.cbor)

    def test_partial_or_refused_inspection_output_cannot_mean_success(self):
        document,*_=sample()
        completion=dict(format='bigbrotr-archive-measurement-completion/1',state='complete',owner_release='committed',policy_revision='1')
        science=json.dumps(document)+'\n'; complete=json.dumps(completion)+'\n'
        self.assertEqual(case.inspection_records(science+complete,'',0),document)
        for stdout,stderr,code in [(science,'',0),('', 'refused',4),(science+complete,'',4),
                (science+complete,'unresolved',0),(science+complete+complete,'',0)]:
            with self.assertRaises(ValueError):case.inspection_records(stdout,stderr,code)

    def test_exact_pg_dispatch_original_input_outcome_and_publication_counts(self):
        state,initialized,report,refs,selection,measurement,originals,receipts=retained_fixture()
        self.assertEqual(oracle.completed(report,state,initialized,selection),refs)
        self.assertEqual(oracle.retained(state,initialized,report,refs,measurement,originals,encoder.cbor,decode),receipts)
        cut=oracle.publication(state,initialized,'09'*16,receipts,encoder.cbor,cut_digest)
        self.assertEqual(cut['family_counts'],[9,0,0,0,0,1])
        for change in [lambda s:s['jobs'].append({}),lambda s:s['producer_outcomes'].pop(),
                lambda s:s['producer_requests'][3].update(monitor_resolution=True),
                lambda s:s['measurement_dispatches'][0].update(context_bytes='\\x'+'00'*294)]:
            bad=copy.deepcopy(state);change(bad)
            with self.assertRaises((AssertionError,ValueError)):oracle.completed(report,bad,initialized,selection)
        for field,value in [('dictionary_count',8),('measurement_count',0),('payload_count',1),('snapshot_id',4)]:
            bad=copy.deepcopy(state);bad['archive_frontier_snapshots'][0][field]=value
            with self.assertRaises((AssertionError,ValueError)):
                oracle.publication(bad,initialized,'09'*16,receipts,encoder.cbor,cut_digest)
        for bad in [originals[:3], originals[:3]+[originals[3]+b'\x00']]:
            with self.assertRaises((AssertionError,ValueError)):
                oracle.retained(state,initialized,report,refs,measurement,bad,encoder.cbor,decode)

    def test_selection_uses_actual_hex_ids_and_decimal_generation_not_numeric_placeholders(self):
        state,initialized,report,refs,selection,*_=retained_fixture()
        state['measurement_dispatches']=[]
        state['producer_requests']=state['producer_requests'][:2];state['producer_outcomes']=state['producer_outcomes'][:2]
        state['relay_enrollments']=[dict(endpoint_id='\\x'+'71'*16,access_scope_id='\\x'+'72'*16,
            route_id='\\x'+'73'*16,generation=1,enabled=True)]
        observations=[dict(path='relay-endpoints',key='91'*16,status=202,
            input=dict(url=case.ENDPOINT,expected_config_revision='1',expected_policy_revision='1'),
            body=dict(endpoint_id='71'*16,command_key='91'*16,created=True)),
            dict(path='relay-enrollments',key='92'*16,status=202,
            input=dict(endpoint_id='71'*16,visibility=initialized['private_scope'],route={'class':'private','cidr':'127.0.0.1/32'},
                enabled=True,expected_generation=None,expected_config_revision='1',expected_policy_revision='1'),
            body=dict(endpoint_id='71'*16,access_scope_id='72'*16,route_id='73'*16,generation='1',enabled=True,command_key='92'*16,created=True))]
        self.assertEqual(oracle.source_selection(observations,state,initialized),selection)
        for key,value in [('generation',1),('access_scope_id',1),('route_id','0')]:
            bad=copy.deepcopy(observations);bad[1]['body'][key]=value
            with self.assertRaises((AssertionError,TypeError,ValueError)):
                oracle.source_selection(bad,state,initialized)

    def test_actual_pin_requires_exact_scope_and_positive_atomic_joined_release(self):
        old=dict(role='control',epoch=1,state='released',resource_id='\\x'+'51'*16)
        owner=dict(old,epoch=2,dataset_id='\\x'+'01'*16,process_incarnation='\\x'+'52'*16,
            role_instance_id='\\x'+'53'*16,acquired_at_us=100,released_at_us=110)
        before=dict(owner_epochs=[old],snapshot_leases=[],archive_snapshot_pins=[],retention_claims=[])
        lease_id='\\x'+'54'*16;catalog='09'*16
        pin=dict(lease_id=lease_id,consumer_id='\\x'+catalog,frontier_revision=4,purpose='reader',
            issuer_resource=owner['resource_id'],issuer_epoch=2,issuer_incarnation=owner['process_incarnation'])
        lease=dict(lease_id=lease_id,consumer_id='\\x'+catalog,frontier_revision=4,archive_pin_protocol=1,
            holder_kind='role_instance',holder_job_id=None,holder_role_instance_id=owner['role_instance_id'],holder_role='control',
            holder_incarnation=owner['process_incarnation'],state='released',lease_revision=2,release_reason='reader_joined',
            acquired_at_us=101,released_at_us=108,updated_at_us=108,xmin='42')
        claim=dict(lease_id=lease_id,purpose='reader',state='released',release_reason='reader_joined',
            released_at_us=107,xmin='42',dataset_id=owner['dataset_id'],topology_generation=0,shard_id=0,
            owner_epoch=1,first_sequence=1,last_sequence=10)
        after=dict(owner_epochs=[old,owner],archive_snapshot_pins=[pin],snapshot_leases=[lease],retention_claims=[claim])
        oracle.inspection_pin(before,after,catalog)
        for table,key,value in [('snapshot_leases','state','active'),('snapshot_leases','release_reason','expired'),
                ('retention_claims','last_sequence',9),('retention_claims','xmin','41')]:
            bad=copy.deepcopy(after);bad[table][0][key]=value
            with self.assertRaises((AssertionError,ValueError)):oracle.inspection_pin(before,bad,catalog)

    def test_real_socket_peer_serves_one_passive_exchange_and_joins_its_owned_listener(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            (directory/'event.jsonl').write_bytes((ROOT/'tests/fixtures/ingest/import-event.jsonl').read_bytes())
            listener=peer.relay_listener(('127.0.0.1',0)); address=listener.getsockname()
            finished=threading.Event();errors=[]
            worker=threading.Thread(target=peer.relay,args=(directory,listener,finished,errors));worker.start()
            try:
                literal_client(address)
                deadline=time.monotonic()+1
                while not (directory/'relay-complete.json').exists():
                    self.assertFalse(errors);self.assertLess(time.monotonic(),deadline);time.sleep(.01)
                self.assertTrue(worker.is_alive())
                self.assertTrue(json.loads((directory/'relay-complete.json').read_text())['listening'])
            finally:
                finished.set();worker.join(1)
            self.assertFalse(worker.is_alive());self.assertFalse(errors)
            ended=json.loads((directory/'listener-ended.json').read_text())
            self.assertEqual((ended['connections'],ended['completed'],ended['listener_closed']),(1,True,True))
            self.assertTrue(ended['final_queue_empty'])

    def test_literal_peer_rejects_auth_publish_and_wrong_filter_without_completion(self):
        for command in (['AUTH',{}],['EVENT',{}],['REQ','ab'*16+'0000000000000001',dict(kinds=[1],limit=3)]):
            with self.subTest(command=command),tempfile.TemporaryDirectory() as temporary:
                directory=Path(temporary)
                (directory/'event.jsonl').write_bytes((ROOT/'tests/fixtures/ingest/import-event.jsonl').read_bytes())
                listener=peer.relay_listener(('127.0.0.1',0));address=listener.getsockname()
                finished=threading.Event();errors=[]
                worker=threading.Thread(target=peer.relay,args=(directory,listener,finished,errors));worker.start()
                try:
                    literal_client(address,wrong_command=command)
                finally:
                    finished.set();worker.join(1)
                self.assertFalse(worker.is_alive());self.assertTrue(errors)
                self.assertFalse((directory/'relay-complete.json').exists())

    def test_second_connection_after_completion_still_refuses_the_owned_session(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            (directory/'event.jsonl').write_bytes((ROOT/'tests/fixtures/ingest/import-event.jsonl').read_bytes())
            listener=peer.relay_listener(('127.0.0.1',0));address=listener.getsockname()
            finished=threading.Event();errors=[]
            worker=threading.Thread(target=peer.relay,args=(directory,listener,finished,errors));worker.start()
            try:
                literal_client(address)
                deadline=time.monotonic()+1
                while not (directory/'relay-complete.json').exists():
                    self.assertFalse(errors);self.assertLess(time.monotonic(),deadline);time.sleep(.01)
                with socket.create_connection(address,timeout=1) as extra:
                    extra.settimeout(1);self.assertEqual(extra.recv(1),b'')
                worker.join(1)
                self.assertFalse(worker.is_alive());self.assertTrue(errors)
            finally:
                finished.set();worker.join(1)
            self.assertEqual(json.loads((directory/'listener-ended.json').read_text())['connections'],2)

    def test_queued_second_connection_is_refused_even_when_owner_finished_before_worker_resumes(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            (directory/'event.jsonl').write_bytes((ROOT/'tests/fixtures/ingest/import-event.jsonl').read_bytes())
            listener=peer.relay_listener(('127.0.0.1',0));address=listener.getsockname()
            finished=threading.Event();paused=threading.Event();resume=threading.Event();errors=[]
            original_publish=peer.publish
            def pause_after_completion(path,name,value):
                original_publish(path,name,value)
                if name == 'relay-complete.json':
                    paused.set()
                    assert resume.wait(1), 'test did not resume the paused peer'
            with patch.object(peer,'publish',side_effect=pause_after_completion):
                worker=threading.Thread(target=peer.relay,args=(directory,listener,finished,errors));worker.start()
                try:
                    literal_client(address)
                    self.assertTrue(paused.wait(1))
                    self.assertTrue(worker.is_alive())
                    self.assertGreaterEqual(listener.fileno(),0)
                    self.assertTrue(json.loads((directory/'relay-complete.json').read_text())['listening'])
                    # TCP connect completes into the real backlog while the
                    # peer cannot accept. Finish wins before its next loop turn.
                    with socket.create_connection(address,timeout=1) as extra:
                        extra.settimeout(1)
                        finished.set();resume.set();worker.join(1)
                        self.assertFalse(worker.is_alive());self.assertTrue(errors)
                        self.assertIn('queued another connection',errors[0])
                        self.assertEqual(extra.recv(1),b'')
                finally:
                    finished.set();resume.set();worker.join(1)
            ended=json.loads((directory/'listener-ended.json').read_text())
            self.assertEqual((ended['connections'],ended['completed'],ended['listener_closed']),(2,True,True))
            self.assertFalse(ended['final_queue_empty'])


if __name__ == '__main__':
    unittest.main()
