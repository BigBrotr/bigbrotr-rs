"""Closed saturation/oracle/custody refusal controls; no engines or product runs."""
import copy
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
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


if __name__=='__main__':
    unittest.main()
