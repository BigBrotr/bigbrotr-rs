"""Source-only false-PASS controls; no installed ownership qualification."""
import copy
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import socket
import threading
import time
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from test_first_launch import ROOT
from test_first_launch_restart import fixture as signed_fixture,current42_head
from test_linear_quantum import fixture as quantum_fixture
from test_linear_capped_quantum import corpus_fixture
from cases import first_launch_capped_oracle as check
from cases import first_launch_capped_quantum as case
from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_restart as source
from cases.first_launch_quantum_science import initial_science
from installed_sync_oracle import expected,decode
from initial_succession_oracle import successor_manifest,successor_rows
from installed_family_science import NetworkScience
from installed_progress_replay import head_digest,native_reference,certification_digest
from bootstrap_oracle import cbor
from transition_replay import native_rows
from installed_sync_client import exact,publish
import linear_capped_transport as peer

ACCEPTED=200_000_999_999
COMPLETED=200_001_020_000
UPPERS=(86400,24,23,2,1)
PAGES=((0,1),(1,),(2,),(2,),())


def projection(state,science,events,wires,epoch):
    """Encode the literal closed SQL fixture with independent typed utilities."""
    job=state['jobs'][0];detail=state['acquisition_jobs'][0]
    q=state['acquisition_quanta'][0 if epoch==1 else 1]
    owner=source.only(state['owner_epochs'],role='control',resource_id=q['issuer_resource'],epoch=q['issuer_epoch'])
    common=dict(dataset_id=job['dataset_id'],producer_slot_id=state['producer_epochs'][0]['producer_slot_id'],producer_epoch=epoch)
    source_fields=dict(relay_endpoint_id=state['acquisition_scopes'][0]['endpoint_id'],
        relay_access_scope_id=state['acquisition_scopes'][0]['access_scope_id'],relay_enrollment_generation=1,
        relay_route_id=state['relay_enrollments'][0]['route_id'],acquisition_scope_id=detail['scope_id'],acquisition_job_id=job['job_id'])
    bindings={4:source.request_binding(events[0],wires[0],1),5:source.request_binding(events[1],wires[0],2)} if epoch==1 else {
        2:source.request_binding(events[1],wires[1],1),3:source.request_binding(events[2],wires[2],1)}
    values={(cf,key.hex()):value.hex() for cf,key,value in science['native']}
    for n,(original,receipt) in enumerate(zip(science['inputs'],science['receipts']),1):
        tx=str(epoch*100+n)
        binding=bindings.get(n)
        state['producer_requests'].append(dict(common,manifest_sha256='\\x'+source.sha(science['manifest']),request_sequence=n,
            request_digest='\\x'+source.sha(original),input_bytes=len(original),input_profile=1,
            monitor_attempt_id=None,monitor_resolution=None,issuer_role='control',
            issuer_resource=owner['resource_id'],issuer_epoch=owner['epoch'],issuer_incarnation=owner['process_incarnation'],
            **(source_fields if binding else dict.fromkeys(source_fields)),**(binding or dict.fromkeys(source.PROGRESS_FIELDS)),
            reserved_at_us=wires[0 if epoch==1 else 1]['sent_at_us']+10,xmin=tx))
        state['producer_outcomes'].append(dict(common,manifest_sha256='\\x'+source.sha(science['manifest']),request_sequence=n,
            request_digest='\\x'+source.sha(original),outcome_kind=1,outcome_bytes='\\x'+receipt.hex(),outcome_sha256='\\x'+source.sha(receipt),
            policy_epoch=1,recorder_role='control',recorder_resource=owner['resource_id'],recorder_epoch=owner['epoch'],
            recorder_incarnation=owner['process_incarnation'],recorded_at_us=wires[0 if epoch==1 else 1]['sent_at_us']+50,xmin=tx))
    wire=wires[0 if epoch==1 else 3];event=events[1 if epoch==1 else 2]
    progress=dict(common,job_id=job['job_id'],scope_id=detail['scope_id'],progress_revision=2 if epoch==1 else 3,
        request_id='\\x'+wire['request'][1].encode().hex(),event_ordinal=2 if epoch==1 else 1,wire_sequence=2 if epoch==1 else 1,
        input_sha256='\\x'+source.sha(quantum.wire_json(event)),classification=2 if epoch==1 else 5,mutation_request=5 if epoch==1 else None,
        classified_events=2 if epoch==1 else 3,successful_events=2 if epoch==1 else 3,rejected_events=0,
        observed_at_us=wire['sent_at_us']+10,clock_resolution_us=1,clock_anomaly_flags=0,xmin='105' if epoch==1 else '204')
    if epoch==1:progress['intent_digest']='\\x'+source.sha(science['inputs'][4])
    else:
        old=source.raw(source.only(state['allocation_domains'],owner_epoch=1,dictionary_kind=4)['allocation_domain'])
        domains={row['dictionary_kind']:source.raw(row['allocation_domain']) for row in state['allocation_domains'] if row['owner_epoch']==2}
        ref=lambda kind:[source.raw(job['dataset_id']),kind,domains[kind],1]
        progress['intent_digest']='\\x'+certification_digest(event,[source.raw(job['dataset_id']),4,old,3],ref(2),ref(2),
            [ref(1),ref(3),ref(5)],progress,wire).hex()
    coordinates=([(1,n,n and (1 if n<=2 else 2 if n==3 else 3 if n<=7 else 4 if n<=12 else 5)) for n in range(1,18)] if epoch==1 else
        [(1,3,1),(1,4,2),(1,13,2),(1,14,2),(1,15,2),(1,16,2)]+[(2,n,1 if n<=4 else 2 if n==5 else 3) for n in range(1,11)])
    dependencies=[]
    for source_epoch,sequence,request in coordinates:
        fact=bytes.fromhex(values['facts',(b'\x01\x20'+source.raw(job['dataset_id'])+bytes(4)+source.be(source_epoch)+source.be(sequence)).hex()])
        dependencies.append(dict(common,source_shard=0,source_epoch=source_epoch,source_sequence=sequence,
            payload_digest='\\x'+fact[2:34].hex(),xmin=str(epoch*100+request)))
    head=dict(common,progress_format=1,manifest_sha256='\\x'+source.sha(science['manifest']),checkpoint_revision=5 if epoch==1 else 4,
        complete_through_request=5 if epoch==1 else 3,progress_jobs=1,dependency_count=len(dependencies),
        issuer_resource=owner['resource_id'],issuer_epoch=owner['epoch'],issuer_incarnation=owner['process_incarnation'],
        committed_at_us=wire['closed_at_us']+10,xmin=progress['xmin'],checkpoint_digest='\\x'+'00'*32)
    head['checkpoint_digest']='\\x'+head_digest(head,[progress],dependencies,lambda row:native_reference(row,values)).hex()
    state['acquisition_progress'].append(progress);state['producer_checkpoint_dependencies']+=dependencies
    state['producer_checkpoints'].append(current42_head(head,initial=epoch==1))


def fixture(root):
    events=corpus_fixture(root)[:3];encoded=signed_fixture()[3]
    before=copy.deepcopy(encoded.initial_state);report=encoded.initial_report
    skeleton,_=quantum_fixture();template=skeleton['acquisition_quanta'][0]
    ds=before['datasets'][0]['dataset_id'];slot='\\x'+report['producer_slot_id']
    before['datasets'][0].update(config_revision=1,policy_revision=1,topology_generation=0)
    before['owner_epochs'][0].update(dataset_id=ds,epoch=1,state='active',exclusion_method=None,role_instance_id='\\x'+'a0'*16,xmin='1')
    owner=dict(skeleton['owner_epochs'][0],dataset_id=ds,acquired_at_us=1000,lease_expires_at_us=300_000_000_000,role_instance_id='\\x'+'a1'*16,xmin='1')
    before['owner_epochs'].append(owner)
    before['role_instances']=[dict(dataset_id=ds,role=row['role'],role_instance_id=row['role_instance_id'],created_at_us=1000,retired_at_us=None,xmin='1') for row in before['owner_epochs']]
    for row in before['allocation_domains']:row.update(owner_epoch=1,state='active',xmin='1')
    job=dict(skeleton['jobs'][0],dataset_id=ds,request_key='\\x'+'b1'*16,created_at_us=ACCEPTED,
        config_revision=1,policy_revision=1,principal_id='\\x'+'10'*16,xmin='2')
    detail=dict(dataset_id=ds,job_id=job['job_id'],scope_id=template['scope_id'],deadline_at_us=ACCEPTED+120_000_000)
    before.update(jobs=[job],acquisition_jobs=[detail],acquisition_gaps=[],acquisition_continuations=[],
        producer_epochs=[dict(dataset_id=ds,producer_slot_id=slot,producer_epoch=1,owner_resource_id=before['owner_epochs'][0]['resource_id'],
            owner_epoch=1,owner_incarnation=before['owner_epochs'][0]['process_incarnation'],state='open',reserved_through_request=5,
            outstanding_request_limit=32,closure_digest=None,xmin='3')],
        source_streams=[dict(dataset_id=ds,shard_id=0,owner_epoch=1,state='open',last_complete_sequence=None,closure_digest=None,xmin='3')],
        relay_endpoints=[],network_routes=[],relay_enrollments=[dict(route_id='\\x'+'73'*16,endpoint_id='\\x'+'71'*16,access_scope_id='\\x'+'72'*16)],
        acquisition_scopes=[dict(scope_id=detail['scope_id'],endpoint_id='\\x'+'71'*16,access_scope_id='\\x'+'72'*16)],
        acquisition_selectors=[],acquisition_scope_commands=[],job_admissions=[],config_revisions=[],policy_revisions=[],
        producer_requests=[],producer_outcomes=[],producer_checkpoints=[],producer_checkpoint_dependencies=[],acquisition_progress=[],acquisition_quanta=[])
    wires=[]
    for number,(upper,page) in enumerate(zip(UPPERS,PAGES),1):
        at=200_001_000_000+number*1000;ident=f'{number:032x}'+'0'*16;values=[events[index] for index in page]
        wires.append(dict(number=number,scope_number=0,scope_turn=number,
            request=['REQ',ident,dict(kinds=[1],since=0,until=upper-1,limit=3)],connected_at_us=at,request_at_us=at+10,
            sent_at_us=at+100,closed_at_us=at+400,clock_resolution_us=1,frames=['signed']*len(values)+['EOSE'],
            event_ids=[event['id'] for event in values],event_json_hex=[quantum.wire_json(event).hex() for event in values],
            completion='request-and-websocket-close'))
    body_digest=lambda event:source.sha(b'bigbrotr/body/1\0'+cbor([bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],1,event['tags'],event['content']]))
    native,inputs,receipts=expected(before,report,events[0],200_001_000_000,wires[0]['sent_at_us']+10,wires[0]['sent_at_us']+50,1,
        expected_body_digest=body_digest(events[0]),additional_events=[(events[1],wires[0]['sent_at_us']+10,wires[0]['sent_at_us']+50,1,body_digest(events[1]))],
        acknowledge=True,finite_event_count=2)
    first=dict(native=native,inputs=inputs,receipts=receipts,manifest=next(value for cf,key,value in native if cf=='default' and key==b'\x01\x01'))
    first_dump=dict(event='dump-state',sequence=85,rows=[dict(cf=cf,key=key.hex(),value=value.hex()) for cf,key,value in native])
    def quantum_row(number,epoch,effects,issuer):
        wire=wires[number-1];records=b'';references=[]
        for ordinal,(event,classification,request,deps) in enumerate(effects,1):
            references+=deps
            records+=(source.be(ordinal,2)+source.be(ordinal)+hashlib.sha256(quantum.wire_json(event)).digest()+source.be(classification,2)
                +source.be(request)+source.be(event['created_at'])+b'\x01'+bytes.fromhex(event['id']))
        refs=quantum.reference_bytes(references);predicate=quantum.wire_json(['REQ','0'*48,wire['request'][2]])
        terminal=quantum.wire_json(['EOSE',wire['request'][1]])
        return dict(template,dataset_id=ds,job_id=job['job_id'],scope_id=detail['scope_id'],producer_slot_id=slot,producer_epoch=epoch,
            manifest_sha256='\\x'+source.sha(first['manifest'] if epoch==1 else manifest),enrollment_generation=1,route_id='\\x'+'73'*16,
            issuer_resource=issuer['resource_id'],issuer_epoch=issuer['epoch'],issuer_incarnation=issuer['process_incarnation'],
            quantum_id='\\x'+f'{number:032x}',quantum_number=number,query_number=number,interval_upper=UPPERS[number-1],
            disposition=('overlap','saturated_second','overlap','saturated_second','window_closed')[number-1],
            continuation_upper=(24,23,2,1,86400)[number-1],gap_reason='saturated_second' if number in (2,4) else None,
            gap_lower=23 if number==2 else 1 if number==4 else None,gap_upper=24 if number==2 else 2 if number==4 else None,
            classified_before=(0,0,1,2,3)[number-1],checkpoint_before=(3,1,2,3,4)[number-1],checkpoint_after=(5,2,3,4,4)[number-1],
            request_predicate='\\x'+predicate.hex(),predicate_sha256='\\x'+source.sha(predicate),terminal_frame='\\x'+terminal.hex(),
            request_id='\\x'+wire['request'][1].encode().hex(),retained_events=len(effects),classified_prefix=len(effects),
            created_at_us=wire['connected_at_us']-1,terminal_received_at_us=wire['sent_at_us']+200,completed_at_us=wire['closed_at_us']+10,
            terminal_sequence=len(effects)+1,effect_records='\\x'+records.hex(),dependencies='\\x'+refs.hex(),
            charged_bytes=1024+len(predicate)+len(terminal)+len(records)+256*(len(refs)//72))
    refs=lambda n:quantum.receipt_refs(receipts[n-1])
    before['acquisition_quanta']=[quantum_row(1,1,[(events[0],2,4,refs(4)),(events[1],2,5,refs(5))],owner)]
    projection(before,first,events,wires,1)
    before['acquisition_quanta'][0]['checkpoint_sha256']=before['producer_checkpoints'][0]['checkpoint_digest']
    sealed=copy.deepcopy(before)
    old_control=source.only(sealed['owner_epochs'],role='control');old_control.update(state='released',exclusion_method='storage_fenced',released_at_us=200_001_001_440,xmin='9')
    source.only(sealed['role_instances'],role='control').update(retired_at_us=200_001_001_440,xmin='9')
    sealing=dict(old_control,epoch=2,process_incarnation='\\x'+'77'*16,role_instance_id='\\x'+'a2'*16)
    sealed['owner_epochs'].append(sealing)
    sealed['role_instances'].append(dict(dataset_id=ds,role='control',role_instance_id=sealing['role_instance_id'],created_at_us=200_001_001_441,retired_at_us=200_001_001_451,xmin='10'))
    closure='\\x'+source.closure(first['manifest'],slot,1,inputs,receipts,17)
    sealed['source_streams'][0].update(state='closed',last_complete_sequence=17,closure_digest=closure,xmin='10')
    seal=dict(continuation_id='\\x'+'88'*16,dataset_id=ds,job_id=job['job_id'],previous_job_revision=2,previous_continuation=None,
        previous_owner_resource=owner['resource_id'],previous_owner_epoch=owner['epoch'],previous_owner_incarnation=owner['process_incarnation'],
        scope_id=detail['scope_id'],enrollment_generation=1,route_id='\\x'+'73'*16,producer_slot_id=slot,producer_epoch=1,
        manifest_sha256='\\x'+source.sha(first['manifest']),shard_id=0,source_sequence=17,source_digest=closure,
        checkpoint_revision=5,complete_through_request=5,checkpoint_sha256=before['producer_checkpoints'][0]['checkpoint_digest'],last_quantum_number=1,
        sealing_resource=sealing['resource_id'],sealing_epoch=sealing['epoch'],sealing_incarnation=sealing['process_incarnation'],sealed_at_us=200_001_001_450,
        **dict.fromkeys(check.ADOPTION_FIELDS),xmin='10')
    sealed['acquisition_continuations']=[seal]
    sealed['producer_epochs'][0]['state']='closing'
    retired=copy.deepcopy(sealed);retired['producer_epochs'][0].update(state='retired',closure_digest=closure,retired_at_us=200_001_001_470,xmin='11')
    retired['owner_epochs'][0].update(state='released',exclusion_method='storage_fenced',exclusion_digest=closure,released_at_us=200_001_001_470,xmin='11')
    source.only(retired['role_instances'],role='ingest').update(retired_at_us=200_001_001_470,xmin='11')
    for row in retired['allocation_domains']:row.update(state='closed',closure_digest=closure,closed_at_us=200_001_001_470,xmin='11')
    after=copy.deepcopy(retired);after['datasets'][0]['topology_generation']=1
    new_control=dict(sealing,epoch=4,process_incarnation='\\x'+'99'*16,role_instance_id='\\x'+'a4'*16)
    after['owner_epochs'].append(new_control)
    after['role_instances'].append(dict(dataset_id=ds,role='control',role_instance_id=new_control['role_instance_id'],created_at_us=200_001_001_480,retired_at_us=None,xmin='12'))
    domains=[bytes([120+k])*16 for k in range(1,10)];incarnation=bytes.fromhex('cc'*16)
    manifest=successor_manifest(first['manifest'],incarnation,domains)
    handoff=b'\x01\x0a'+source.raw(slot)+source.be(1)+source.be(5)+source.be(17)+source.raw(closure)+source.be(266,2)+first['manifest']+source.be(266,2)+manifest
    intent=dict(dataset_id=ds,producer_slot_id=slot,target_epoch=2,owner_resource_id=before['owner_epochs'][0]['resource_id'],target_incarnation='\\x'+incarnation.hex(),
        target_role_instance_id='\\x'+'cd'*16,
        previous_manifest='\\x'+first['manifest'].hex(),target_manifest='\\x'+manifest.hex(),previous_manifest_sha256='\\x'+source.sha(first['manifest']),
        target_manifest_sha256='\\x'+source.sha(manifest),handoff='\\x'+handoff.hex(),handoff_sha256='\\x'+source.sha(handoff),
        issued_through=5,last_complete_sequence=17,closure_digest=closure,config_revision=1,policy_revision=1)
    after['initial_ingest_successions']=[intent]
    after['owner_epochs'].append(dict(before['owner_epochs'][0],epoch=2,process_incarnation=intent['target_incarnation'],
        role_instance_id=intent['target_role_instance_id']))
    after['role_instances'].append(dict(dataset_id=ds,role='ingest',role_instance_id=intent['target_role_instance_id'],created_at_us=200_001_001_500,retired_at_us=None,xmin='12'))
    after['source_streams'].append(dict(dataset_id=ds,shard_id=0,owner_epoch=2,state='open',last_complete_sequence=None,closure_digest=None))
    after['initial_ingest_activations']=[dict(dataset_id=ds,producer_slot_id=slot,target_epoch=2,target_manifest_sha256=intent['target_manifest_sha256'],
        handoff_sha256=intent['handoff_sha256'],activated_by_resource=new_control['resource_id'],activated_by_epoch=4,
        activated_by_incarnation=new_control['process_incarnation'],activated_at_us=200_001_001_500)]
    after['producer_epochs'].append(dict(after['producer_epochs'][0],producer_epoch=2,owner_epoch=2,
        owner_incarnation=intent['target_incarnation'],state='open',reserved_through_request=3,closure_digest=None))
    after['allocation_domains'] += [dict(dictionary_kind=k,allocation_domain='\\x'+value.hex(),owner_epoch=2) for k,value in enumerate(domains,1)]
    after['acquisition_continuations'][0].update(target_epoch=2,target_manifest_sha256=intent['target_manifest_sha256'],activation_sha256=intent['handoff_sha256'],
        adopting_resource=new_control['resource_id'],adopting_epoch=4,adopting_incarnation=new_control['process_incarnation'],adopted_job_revision=3,
        adopted_at_us=200_001_001_600,adoption_transaction='12',xmin='12')
    after['jobs'][0].update(job_revision=3,owner_epoch=4,owner_incarnation=new_control['process_incarnation'],xmin='12')
    closed={(cf,key):value for cf,key,value in native}
    closed['receipts',b'\x01\x31'+source.raw(slot)+source.be(1)]=b'\x02\x31'+source.be(5)+source.be(5)+source.be(0)+b'\x01'
    initial=successor_rows(closed,manifest,source.raw(slot),handoff)
    model=SimpleNamespace(ds=after['datasets'][0],slot=slot,legacy=intent,inputs={},native=dict(event='dump-state',sequence=91,
        rows=[dict(cf=cf,key=key.hex(),value=value.hex()) for (cf,key),value in sorted(initial.items())]))
    science=NetworkScience(model,dict(epoch=2,topology=1,manifest=manifest,domains=['\\x'+value.hex() for value in domains]),
        acknowledge=True,new_event=events[2],endpoint='ws://127.0.0.1:7447/installed-fixture',previous_event=events[1],previous_coordinates=(3,4,13,14,15,16))
    science.network(True,wires[1]['connected_at_us']-10,wires[2]['sent_at_us']+10,wires[2]['sent_at_us']+50,1,
        (wires[1]['sent_at_us']+10,wires[1]['sent_at_us']+50,1),historical_first=True)
    second=dict(native=[(row['cf'],bytes.fromhex(row['key']),bytes.fromhex(row['value'])) for row in native_rows(science.values)],
        inputs=list(science.inputs.values()),receipts=list(science.receipts.values()),manifest=manifest,handoff=handoff)
    refs=lambda n:quantum.receipt_refs(second['receipts'][n-1])
    effects=([(events[1],2,2,refs(2))],[(events[2],2,3,refs(3))],[(events[2],5,0,refs(3))],[])
    after['acquisition_quanta'] += [quantum_row(n,2,values,new_control) for n,values in enumerate(effects,2)]
    projection(after,second,events,wires,2)
    final_head=source.only(after['producer_checkpoints'],producer_epoch=2)
    final_progress=source.only(after['acquisition_progress'],producer_epoch=2)
    for number in (2,3):
        wire=wires[number-1];original=second['inputs'][number-1];event=events[1 if number==2 else 2]
        progress=dict(final_progress,progress_revision=number-1,classified_events=number-1,successful_events=number-1,
            request_id='\\x'+wire['request'][1].encode().hex(),input_sha256='\\x'+source.sha(quantum.wire_json(event)),
            intent_digest='\\x'+source.sha(original),classification=2,mutation_request=number,
            observed_at_us=decode(original.split(b'\0',1)[1])[3][5][0])
        references={tuple(ref):value for receipt in second['receipts'][:number] for ref,value in quantum.receipt_refs(receipt)}
        dependencies=[dict(dataset_id=ds,producer_slot_id=slot,producer_epoch=2,source_shard=ref[1],source_epoch=ref[2],source_sequence=ref[3],
            payload_digest='\\x'+value.hex(),xmin='1') for ref,value in sorted(references.items())]
        head={key:value for key,value in final_head.items() if not key.startswith('prefix_')}
        head.update(checkpoint_revision=number,complete_through_request=number,dependency_count=len(dependencies))
        digest='\\x'+head_digest(head,[progress],dependencies,lambda row:native_reference(row,science.values)).hex()
        after['acquisition_quanta'][number-1]['checkpoint_sha256']=digest
    for number in (4,5):after['acquisition_quanta'][number-1]['checkpoint_sha256']=final_head['checkpoint_digest']
    started=dict(producer_epoch='2',process_incarnation=incarnation.hex(),activated_at_us='200001001500',
        owner_resource_id=intent['owner_resource_id'][2:],producer_slot_id=slot[2:],dataset_id=ds[2:])
    return before,sealed,retired,after,report,events,wires,first,second,first_dump,model.native,started


class CappedAdoptionControls(unittest.TestCase):
    def successor_runtime(self,root,*,trace=False,role='actual_control',database='actual_database'):
        runtime=object.__new__(case.CappedAdoptionRuntime)
        runtime.args=SimpleNamespace(linear_quantum_trace_successor_sql=trace)
        runtime.config=root/'instance.toml'
        runtime.config.write_text('[postgres.control]\nuser = '+json.dumps(role)+'\ndatabase = '+json.dumps(database)+'\n')
        runtime.home=root;runtime.client=Mock()
        dataset='\\x'+'11'*16;job='\\x'+'12'*16;digest='ab'*32
        document=dict(format='bigbrotr-product-policy/3',settings={'runtime.pressure_stable_duration':30_000_000})
        runtime.adoption_before=dict(datasets=[dict(dataset_id=dataset,config_revision=1,policy_revision=1)],
            jobs=[dict(job_id=job,dataset_id=dataset,created_at_us=1_000_000,config_revision=1,policy_revision=1)],
            acquisition_jobs=[dict(job_id=job,dataset_id=dataset,deadline_at_us=121_000_000)],
            config_revisions=[dict(dataset_id=dataset,revision=1,document=copy.deepcopy(document),document_digest='\\x'+digest)],
            policy_revisions=[dict(dataset_id=dataset,revision=1,config_revision=1,document=copy.deepcopy(document),document_digest='\\x'+digest)])
        runtime.adoption_retired=copy.deepcopy(runtime.adoption_before)
        runtime.adoption_started=dict(state='activated',producer_epoch='2',dataset_id=dataset[2:],
            config_revision='1',policy_revision='1',config_sha256=digest,policy_sha256=digest,activated_at_us='64000000')
        runtime.sql=Mock(return_value='65000000')
        runtime.launch=Mock(return_value='successor')
        return runtime

    def test_successor_trace_quotes_exact_login_and_commits_redacted_defaults_before_launch(self):
        role=r'''control"login'\tail''';database=r'''owned"db'\path'''
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=self.successor_runtime(root,trace=True,role=role,database=database)
            observed=dict(role=role,database=database,settings=[
                'log_parameter_max_length=0','log_parameter_max_length_on_error=0','log_statement=all'])
            runtime.sql=Mock(return_value=json.dumps(observed))
            def launch(*args):
                runtime.sql.assert_called_once()
                self.assertEqual(json.loads((root/'sql-trace-configured.json').read_text()),
                    dict(scope='verified_login_defaults_only',before_successor_launch=True,**observed))
                return 'successor'
            runtime.launch.side_effect=launch
            self.assertEqual(runtime.launch_adoption_successor(root),'successor')
            actual_database,statement=runtime.sql.call_args.args
            self.assertEqual(actual_database,database)
            self.assertIn(r'''ALTER ROLE "control""login'\tail" IN DATABASE "owned""db'\path" SET log_parameter_max_length=0;''',statement)
            self.assertIn(r'''r.rolname=E'control"login''\\tail' AND d.datname=E'owned"db''\\path';''',statement)
            self.assertEqual(statement.count('ALTER ROLE '),3)
            self.assertLess(statement.index('BEGIN;'),statement.index('SET log_parameter_max_length=0;'))
            self.assertLess(statement.index('SET log_parameter_max_length_on_error=0;'),statement.index("SET log_statement='all';"))
            self.assertLess(statement.index('COMMIT;'),statement.index('FROM pg_db_role_setting'))
            self.assertIn("WHERE split_part(value,'=',1) IN ('log_statement','log_parameter_max_length','log_parameter_max_length_on_error')",statement)
            self.assertNotIn('current_setting(',statement)
            runtime.launch.assert_called_once_with('adoption-successor','app','serve','--file','/etc/bigbrotr/instance.toml',
                '--activate-initial-successor','--roles','control,ingest,sync')

    def test_successor_trace_refuses_wrong_or_missing_redaction_defaults_and_login(self):
        valid=dict(role='actual_control',database='actual_database',settings=[
            'log_parameter_max_length=0','log_parameter_max_length_on_error=0','log_statement=all'])
        changes=[dict(role='observer'),dict(database='observer_database')]
        for index in range(3):
            missing=list(valid['settings']);missing.pop(index);changes.append(dict(settings=missing))
            wrong=list(valid['settings']);wrong[index]=('log_parameter_max_length=10',
                'log_parameter_max_length_on_error=-1','log_statement=none')[index]
            changes.append(dict(settings=wrong))
        changes.append(dict(settings=valid['settings']+[valid['settings'][0]]))
        for changed in changes:
            with self.subTest(changed=changed),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=self.successor_runtime(root,trace=True)
                runtime.sql=Mock(return_value=json.dumps(dict(valid,**changed)))
                with self.assertRaisesRegex(ValueError,'defaults differ or expose bind values'):
                    runtime.launch_adoption_successor(root)
                runtime.launch.assert_not_called();self.assertFalse((root/'sql-trace-configured.json').exists())

    def test_successor_trace_is_opt_in_and_rejects_invalid_identifiers_before_sql(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=self.successor_runtime(root)
            runtime.sql=Mock();runtime.launch_adoption_successor(root);runtime.sql.assert_not_called()
            self.assertFalse((root/'sql-trace-configured.json').exists())
        for role in ('','bad\0role','r'*64):
            with self.subTest(role=role),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=self.successor_runtime(root,trace=True,role=role)
                runtime.sql=Mock()
                with self.assertRaisesRegex(ValueError,'PostgreSQL identifier'):runtime.launch_adoption_successor(root)
                runtime.sql.assert_not_called();runtime.launch.assert_not_called()

    def test_failed_successor_wait_retains_original_exception_and_one_readonly_snapshot_with_trace_on_or_off(self):
        for trace in (False,True):
            with self.subTest(trace=trace),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=self.successor_runtime(root,trace=trace)
                original=ValueError('original settled wait failure');runtime.wait=Mock(side_effect=original)
                snapshot=dict(format='bigbrotr-adoption-successor-wait-diagnostic/1',jobs=dict(count=1,rows=[]),
                    seals=dict(count=1,rows=[]),quanta=dict(count=1,rows=[]),application_backends=dict(count=1,
                        rows=[dict(pid=123,backend_start='2026-11-03T00:00:00Z',state='idle',wait_event_type='Client',
                            wait_event='ClientRead',query_sha256='ab'*32)]))
                runtime.sql=Mock(side_effect=['65000000',json.dumps(snapshot)])
                with self.assertRaises(ValueError) as raised:runtime.wait_adoption_successor('successor',root)
                self.assertIs(raised.exception,original)
                self.assertEqual(runtime.wait.call_args.args[1:],('five globally numbered settled acquisition queries',50.,'successor',runtime.client))
                self.assertEqual(runtime.wait.call_count,1);self.assertEqual(runtime.sql.call_count,2)
                database,statement=runtime.sql.call_args.args;self.assertEqual(database,'actual_database')
                self.assertIn('BEGIN READ ONLY;',statement);self.assertIn("SET LOCAL statement_timeout='3s';",statement)
                self.assertIn("encode(sha256(convert_to(a.query,'UTF8')),'hex') AS query_sha256",statement)
                self.assertIn("a.usename=E'actual_control' AND a.datname=E'actual_database'",statement)
                for bound in (1,3,6,16):self.assertIn('LIMIT '+str(bound),statement)
                self.assertNotIn('SELECT *',statement)
                self.assertNotRegex(statement,r'\b(?:INSERT|UPDATE|DELETE|ALTER|CREATE|DROP)\b')
                self.assertEqual(json.loads((root/'wait-failure-snapshot.json').read_text()),snapshot)
                self.assertFalse((root/'wait-failure-diagnostic-error.json').exists())

    def test_snapshot_or_error_receipt_failure_cannot_mask_original_wait_failure(self):
        for save_error in (False,True):
            with self.subTest(save_error=save_error),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=self.successor_runtime(root)
                original=RuntimeError('original wait failure');runtime.wait=Mock(side_effect=original)
                runtime.sql=Mock(side_effect=['65000000',OSError('private diagnostic failure detail')])
                original_save=case.save
                def save(path,value):
                    if save_error and path.name!='wait-observation-budget.json':raise OSError('private receipt failure')
                    return original_save(path,value)
                with patch.object(case,'save',side_effect=save):
                    with self.assertRaises(RuntimeError) as raised:runtime.wait_adoption_successor('successor',root)
                self.assertIs(raised.exception,original);self.assertEqual(runtime.sql.call_count,2)
                if not save_error:
                    self.assertEqual(json.loads((root/'wait-failure-diagnostic-error.json').read_text()),dict(error='OSError'))
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=self.successor_runtime(root)
            runtime.wait=Mock();runtime.capture_successor_wait_failure=Mock()
            runtime.wait_adoption_successor('successor',root);runtime.capture_successor_wait_failure.assert_not_called()

    def test_successor_observation_uses_exact_accepted_microseconds_and_existing_settlement_allowance(self):
        for stable,seconds in ((30_000_000,50.),(1,20.000001),(1_234_567,21.234567),(35_000_000,55.)):
            with self.subTest(stable=stable),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=self.successor_runtime(root)
                for kind in ('config','policy'):
                    runtime.adoption_retired[kind+'_revisions'][0]['document']['settings']['runtime.pressure_stable_duration']=stable
                before=copy.deepcopy((runtime.adoption_before,runtime.adoption_retired,runtime.adoption_started))
                runtime.wait=Mock()
                with patch.object(case.time,'monotonic_ns',side_effect=[1_000_000_000,1_001_000_000]):
                    runtime.wait_adoption_successor('successor',root)
                self.assertEqual(runtime.wait.call_args.args[1:],('five globally numbered settled acquisition queries',seconds,'successor',runtime.client))
                runtime.sql.assert_called_once_with('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint')
                budget=json.loads((root/'wait-observation-budget.json').read_text())
                self.assertEqual((budget['stable_duration_us'],budget['settlement_allowance_us'],budget['observation_duration_us']),
                    (stable,20_000_000,int(seconds*1_000_000)))
                self.assertEqual((budget['observed_database_at_us'],budget['clock_call_elapsed_us'],budget['remaining_us']),
                    (65_000_000,1000,55_999_000))
                self.assertTrue(budget['fits_original_deadline']);self.assertEqual(budget['original_deadline_at_us'],121_000_000)
                self.assertEqual((runtime.adoption_before,runtime.adoption_retired,runtime.adoption_started),before)

    def test_successor_observation_refuses_unaccepted_disagreeing_or_malformed_timing_before_wait(self):
        for change in ('zero','negative','boolean','float','string','u64-overflow','missing-stable','disagree',
                'missing-config','duplicate-policy','wrong-format','wrong-dataset','wrong-digest','wrong-revision','new-deadline'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=self.successor_runtime(root);state=runtime.adoption_retired
                values={'zero':0,'negative':-1,'boolean':True,'float':30_000_000.,'string':'30000000','u64-overflow':2**64}
                if change in values:
                    for kind in ('config','policy'):
                        state[kind+'_revisions'][0]['document']['settings']['runtime.pressure_stable_duration']=values[change]
                elif change=='missing-stable':del state['policy_revisions'][0]['document']['settings']['runtime.pressure_stable_duration']
                elif change=='disagree':state['policy_revisions'][0]['document']['settings']['runtime.pressure_stable_duration']=1
                elif change=='missing-config':state['config_revisions']=[]
                elif change=='duplicate-policy':state['policy_revisions']*=2
                elif change=='wrong-format':state['config_revisions'][0]['document']['format']='bigbrotr-product-policy/2'
                elif change=='wrong-dataset':state['policy_revisions'][0]['dataset_id']='\\x'+'22'*16
                elif change=='wrong-digest':runtime.adoption_started['config_sha256']='cd'*32
                elif change=='wrong-revision':state['datasets'][0]['config_revision']=2
                else:state['acquisition_jobs'][0]['deadline_at_us']+=1
                runtime.wait=Mock()
                with self.assertRaisesRegex(ValueError,'successor observation prerequisite: accepted timing profile'):
                    runtime.wait_adoption_successor('successor',root)
                runtime.sql.assert_not_called();runtime.wait.assert_not_called()

    def test_successor_observation_must_fit_remaining_original_deadline_including_clock_roundtrip(self):
        for stable,now in ((36_000_000,65_000_000),(35_999_999,65_000_000),(30_000_000,100_000_000)):
            with self.subTest(stable=stable,now=now),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);runtime=self.successor_runtime(root);runtime.sql.return_value=str(now)
                for kind in ('config','policy'):
                    runtime.adoption_retired[kind+'_revisions'][0]['document']['settings']['runtime.pressure_stable_duration']=stable
                runtime.wait=Mock()
                with patch.object(case.time,'monotonic_ns',side_effect=[1_000_000_000,1_001_000_000]),\
                        self.assertRaisesRegex(ValueError,'cannot fit original remaining job deadline'):
                    runtime.wait_adoption_successor('successor',root)
                runtime.wait.assert_not_called();self.assertEqual(runtime.sql.call_count,1)
                budget=json.loads((root/'wait-observation-budget.json').read_text())
                self.assertFalse(budget['fits_original_deadline']);self.assertEqual(budget['original_deadline_at_us'],121_000_000)

    def test_trace_cli_rejects_unrelated_cases_and_accepts_only_declared_adoption(self):
        spec=importlib.util.spec_from_file_location('adoption_trace_runner_control',ROOT/'tests/acceptance/run.py')
        runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        invalid=(('p01-t01','--binary',sys.executable),('p03-linear-quantum',),
            ('p03-linear-quantum','--linear-quantum-scenario','interruptions'),
            ('p03-linear-quantum','--linear-quantum-scenario','capped-continuation'),
            ('p03-linear-quantum','--linear-quantum-scenario','window-succession','--linear-quantum-fault','gap-write'),
            ('p03-linear-quantum','--linear-quantum-scenario','settled-owner-adoption','--linear-quantum-fault','plan-after'))
        for prefix in invalid:
            with self.subTest(prefix=prefix),tempfile.TemporaryDirectory() as temporary:
                evidence=Path(temporary)/'evidence'
                argv=['run.py',*prefix,'--linear-quantum-trace-successor-sql','--source-root',str(ROOT),'--evidence',str(evidence)]
                with patch.object(sys,'argv',argv),patch.object(sys,'stderr'),self.assertRaises(SystemExit) as raised:runner.main()
                self.assertEqual(raised.exception.code,2);self.assertFalse(evidence.exists())
        for trace in (False,True):
            with self.subTest(valid_trace=trace),tempfile.TemporaryDirectory() as temporary:
                evidence=Path(temporary)/'evidence'
                argv=['run.py','p03-linear-quantum','--linear-quantum-scenario','settled-owner-adoption',
                    '--linear-quantum-fault','plan-before','--source-root',str(ROOT),'--evidence',str(evidence)]
                if trace:argv.append('--linear-quantum-trace-successor-sql')
                with patch.object(sys,'argv',argv),patch.object(sys,'stdout'),patch('cases.first_launch.first_launch') as launched:
                    self.assertEqual(runner.main(),2)
                self.assertEqual(launched.call_args.args[0].linear_quantum_trace_successor_sql,trace)

    def test_distinct_retained_job_union_preserves_64_across_categories_and_owner_move(self):
        # Captured metadata semantics only. No connected SQL65 refusal is run.
        bx=lambda n:'\\x'+f'{n:032x}'
        dataset,slot,resource,incarnation=map(bx,(1,2,3,4))
        producer=dict(dataset_id=dataset,producer_slot_id=slot,producer_epoch=1,owner_resource_id=bx(5))
        registration=dict(producer_slot_id=slot,registered_by_resource_id=resource,registered_by_epoch=7,registered_by_incarnation=incarnation)
        claims=[dict(dataset_id=dataset,job_id=bx(n),kind='acquisition',state='queued',owner_role='control',
            owner_resource_id=resource,owner_epoch=7,owner_incarnation=incarnation) for n in range(100,164)]
        state=dict(producer_epochs=[producer],producer_slots=[registration],jobs=claims,
            acquisition_gaps=[dict(dataset_id=dataset,producer_slot_id=slot,producer_epoch=1,job_id=bx(n)) for n in range(100,132)],
            acquisition_continuations=[dict(dataset_id=dataset,producer_slot_id=slot,producer_epoch=1,job_id=bx(n),
                adopted_at_us=None if n%2 else 10) for n in range(100,164)])
        self.assertEqual(len(check.adoption_cardinality(state,slot,1)),64)
        for job in claims:job.update(owner_epoch=8,owner_incarnation=bx(6))
        self.assertEqual(len(check.adoption_cardinality(state,slot,1)),64)  # Finalized seals still count.
        foreign=dict(dataset_id=dataset,producer_slot_id=bx(99),producer_epoch=1,job_id=bx(164),adopted_at_us=10)
        state['acquisition_continuations'].append(foreign)
        self.assertEqual(len(check.adoption_cardinality(state,slot,1)),64)
        foreign['producer_slot_id']=slot
        with self.assertRaisesRegex(AssertionError,'exceeds64'):check.adoption_cardinality(state,slot,1)
        state['acquisition_continuations'].pop()
        state['jobs'].append(dict(claims[0],job_id=bx(164),state='cancelled',owner_epoch=7,owner_incarnation=incarnation))
        with self.assertRaisesRegex(AssertionError,'exceeds64'):check.adoption_cardinality(state,slot,1)
        state['jobs'].pop()
        for epoch in (2,3):
            current=copy.deepcopy(state);current['producer_epochs'][0]['producer_epoch']=epoch
            current['producer_slots'][0]['registered_by_epoch']=999
            for table in ('acquisition_gaps','acquisition_continuations'):
                for row in current[table]:row['producer_epoch']=epoch
            current['initial_ingest_activations' if epoch==2 else 'ingest_activations']=[dict(
                owner_resource_id=producer['owner_resource_id'],target_epoch=epoch,activated_by_resource=resource,
                activated_by_epoch=7,activated_by_incarnation=incarnation)]
            self.assertEqual(len(check.adoption_cardinality(current,slot,epoch)),64)

    def test_finalized_later_zero_query_segment_is_required_in_historical_owner_chain(self):
        # Literal lineage metadata only; no later successor process/native proof.
        with tempfile.TemporaryDirectory() as temporary:
            before,_,_,state,_,_,_,_,_,_,_,_=fixture(Path(temporary))
            state['acquisition_quanta']=state['acquisition_quanta'][:1]
            for table in ('producer_requests','producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies','acquisition_progress'):
                state[table]=[row for row in state[table] if row['producer_epoch']==1]
            initial=state['initial_ingest_successions'][0];manifest=source.raw(initial['target_manifest'])
            parent=source.raw(initial['handoff']);slot=initial['producer_slot_id']
            closure='\\x'+source.closure(manifest,slot,2,[],[],0,parent)
            producer=source.only(state['producer_epochs'],producer_epoch=2)
            producer.update(state='retired',reserved_through_request=0,closure_digest=closure)
            stream=source.only(state['source_streams'],owner_epoch=2)
            stream.update(state='closed',last_complete_sequence=0,closure_digest=closure)
            previous=source.only(state['owner_epochs'],role='control',epoch=4)
            current=dict(previous,epoch=5,process_incarnation='\\x'+'a5'*16)
            state['owner_epochs'].append(current)
            target=bytearray(manifest);target[2:4]=source.be(3,2);target[26:34]=source.be(2);target[54:62]=source.be(3);target[62:78]=bytes.fromhex('a6'*16)
            for k in range(1,10):target[106+(k-1)*18:122+(k-1)*18]=bytes([160+k])*16
            target=bytes(target)
            handoff=b'\x02\x0a\x01'+source.raw(slot)+source.be(2)+source.be(0)+source.be(0)+source.raw(closure)+hashlib.sha256(parent).digest()+source.be(266,2)+manifest+source.be(266,2)+target
            intent=dict(initial,target_epoch=3,target_incarnation='\\x'+'a6'*16,previous_manifest='\\x'+manifest.hex(),target_manifest='\\x'+target.hex(),
                previous_manifest_sha256='\\x'+source.sha(manifest),target_manifest_sha256='\\x'+source.sha(target),handoff='\\x'+handoff.hex(),
                handoff_sha256='\\x'+source.sha(handoff),issued_through=0,last_complete_sequence=0,closure_digest=closure)
            state['ingest_successions']=[intent]
            state['ingest_activations']=[dict(dataset_id=intent['dataset_id'],producer_slot_id=slot,target_epoch=3,
                target_manifest_sha256=intent['target_manifest_sha256'],handoff_sha256=intent['handoff_sha256'],
                activated_by_resource=current['resource_id'],activated_by_epoch=5,activated_by_incarnation=current['process_incarnation'],activated_at_us=200_001_010_100)]
            state['producer_epochs'].append(dict(producer,producer_epoch=3,owner_epoch=3,owner_incarnation=intent['target_incarnation'],state='open',closure_digest=None))
            prior=state['acquisition_continuations'][0]
            seal=dict(prior,continuation_id='\\x'+'ab'*16,previous_continuation=prior['continuation_id'],previous_job_revision=3,
                previous_owner_epoch=4,previous_owner_incarnation=previous['process_incarnation'],producer_epoch=2,manifest_sha256=intent['previous_manifest_sha256'],
                source_sequence=0,source_digest=closure,checkpoint_revision=0,complete_through_request=0,checkpoint_sha256=None,last_quantum_number=1,
                sealed_at_us=200_001_010_000,target_epoch=3,target_manifest_sha256=intent['target_manifest_sha256'],activation_sha256=intent['handoff_sha256'],
                adopting_epoch=5,adopting_incarnation=current['process_incarnation'],adopted_job_revision=4,adopted_at_us=200_001_010_200,adoption_transaction='14',xmin='14')
            state['acquisition_continuations'].append(seal)
            state['jobs'][0].update(owner_epoch=5,owner_incarnation=current['process_incarnation'],job_revision=4,xmin='14')
            quantum.shape(state)
            for name in ('missing-zero-segment','reset-global-cut','wrong-parent','invented-checkpoint'):
                changed=copy.deepcopy(state)
                if name=='missing-zero-segment':changed['acquisition_continuations'].pop()
                elif name=='reset-global-cut':changed['acquisition_continuations'][1]['last_quantum_number']=0
                elif name=='wrong-parent':changed['acquisition_continuations'][1]['previous_continuation']=None
                else:changed['acquisition_continuations'][1]['checkpoint_revision']=1
                with self.subTest(name=name),self.assertRaises((AssertionError,ValueError)):quantum.shape(changed)

    def test_manifest_full_history_and_complete_independent_native_science(self):
        check.adoption_manifest(json.loads((ROOT/'tests/fixtures/acquisition/settled-owner-adoption.json').read_text()))
        with tempfile.TemporaryDirectory() as temporary:
            before,sealed,retired,after,report,events,wires,first,second,dump,successor,started=fixture(Path(temporary))
            encoded=initial_science(before,report,events,wires[:1],dump,first['inputs'],COMPLETED,
                mutations=((0,4,0,8),(1,5,0,13)),first_accepted_at_us=ACCEPTED,native_sequence=85)
            self.assertEqual(encoded,first)
            closed=copy.deepcopy(dump);closed['sequence']=86
            marker=next(row for row in closed['rows'] if row['cf']=='receipts' and row['key']=='0131'+report['producer_slot_id']+'0000000000000001')
            marker['value']=(b'\x02\x31'+source.be(5)+source.be(5)+source.be(0)+b'\x01').hex()
            check.adoption_closed_native(first,'\\x'+report['producer_slot_id'],closed)
            for name in ('extra-ACK','missing-close','changed-acknowledgement'):
                changed=copy.deepcopy(closed)
                if name=='extra-ACK':changed['sequence']=87
                elif name=='missing-close':changed['rows']=copy.deepcopy(dump['rows'])
                else:
                    row=next(row for row in changed['rows'] if row['cf']=='receipts' and row['key']==marker['key'])
                    row['value']=(b'\x02\x31'+source.be(5)+source.be(4)+source.be(0)+b'\x01').hex()
                with self.subTest(name=name),self.assertRaises(AssertionError):check.adoption_closed_native(first,'\\x'+report['producer_slot_id'],changed)
            # 85 old puts +1 close +5 handoff +22 dictionary +8 witness +18 new body.
            self.assertEqual(successor['sequence'],139)
            actual=check.adoption_successor_science(after,started,events,wires,successor,second['inputs'],first,COMPLETED,before=before)
            self.assertEqual(actual,second)
            check.adoption_history(before,wires[:1],events,first)
            check.adoption_transition(before,sealed,retired,after,first)
            check.adoption_history(after,wires,events,first,second)
            check.adoption_progress(before,report,events,wires,first,COMPLETED,epoch=1)
            check.adoption_progress(after,report,events,wires,second,COMPLETED,epoch=2)

    def test_early_known_page_short_count_gap_loss_and_extra_wire_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            before,sealed,retired,after,report,events,wires,first,second,*_=fixture(Path(temporary))
            for name in ('known-page-stop','gap23','gap1','no-empty','sixth','reset-query-number','changed-horizon','changed-budget',
                    'known-body-as-proof','forged-original-input-digest','forged-original-wire-sequence'):
                state=copy.deepcopy(after);transcript=copy.deepcopy(wires)
                if name=='known-page-stop':state['acquisition_quanta']=state['acquisition_quanta'][:2];transcript=transcript[:2]
                elif name=='no-empty':state['acquisition_quanta'].pop();transcript.pop()
                elif name=='sixth':transcript.append(copy.deepcopy(transcript[-1]))
                elif name=='reset-query-number':state['acquisition_quanta'][1]['query_number']=1
                elif name=='changed-horizon':
                    for row in state['acquisition_quanta']:row['horizon']=172800
                elif name=='changed-budget':
                    for row in state['acquisition_quanta']:row['query_budget']=64
                elif name in ('known-body-as-proof','forged-original-input-digest','forged-original-wire-sequence'):
                    row=state['acquisition_quanta'][1 if name=='known-body-as-proof' else 0]
                    values=bytearray(quantum.raw(row['effect_records']))
                    if name=='known-body-as-proof':values[42:52]=source.be(5,2)+source.be(0)
                    elif name=='forged-original-input-digest':values[10]^=1
                    else:values[2:10]=source.be(2)
                    row['effect_records']='\\x'+values.hex()
                else:
                    row=state['acquisition_quanta'][1 if name=='gap23' else 3]
                    row.update(disposition='overlap',gap_reason=None,gap_lower=None,gap_upper=None,continuation_upper=row['interval_upper'])
                with self.subTest(name=name),self.assertRaises((AssertionError,ValueError)):
                    check.adoption_history(state,transcript,events,first,second)

    def test_seal_without_adoption_foreign_activation_or_nonatomic_job_cas_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            before,sealed,retired,after,_,_,_,first,*_=fixture(Path(temporary))
            for name in ('missing-seal','unadopted','wrong-owner','wrong-target','wrong-handoff','wrong-source','wrong-cut','pending-old',
                    'wrong-checkpoint','unretired-old','live-old-control','expired-original-deadline','wrong-activation','different-cas',
                    'changed-principal','new-deadline','old-xmin','continuation-xmin','original-request-digest','original-request-sequence','old-lease-rewritten','seal-rewritten'):
                state=copy.deepcopy(after);intermediate=copy.deepcopy(sealed)
                seal=state['acquisition_continuations'][0]
                if name=='missing-seal':state['acquisition_continuations']=[]
                elif name=='unadopted':seal.update(dict.fromkeys(check.ADOPTION_FIELDS))
                elif name=='wrong-owner':seal['previous_owner_incarnation']='\\x'+'ff'*16
                elif name=='wrong-target':seal['target_manifest_sha256']='\\x'+'ff'*32
                elif name=='wrong-handoff':seal['activation_sha256']='\\x'+'ff'*32
                elif name=='wrong-source':seal['source_digest']='\\x'+'ff'*32
                elif name=='wrong-cut':seal['last_quantum_number']=0
                elif name=='pending-old':state['acquisition_quanta'][0]['terminal_kind']=None
                elif name=='wrong-checkpoint':seal['checkpoint_sha256']='\\x'+'ff'*32
                elif name=='unretired-old':state['producer_epochs'][0]['state']='open'
                elif name=='live-old-control':source.only(state['owner_epochs'],role='control',epoch=1)['state']='active'
                elif name=='expired-original-deadline':seal['adopted_at_us']=state['acquisition_jobs'][0]['deadline_at_us']
                elif name=='wrong-activation':state['initial_ingest_activations'][0]['activated_by_epoch']=3
                elif name=='different-cas':state['jobs'][0]['xmin']='13'
                elif name=='changed-principal':state['jobs'][0]['principal_id']='\\x'+'ff'*16
                elif name=='new-deadline':state['acquisition_jobs'][0]['deadline_at_us']+=1
                elif name=='old-xmin':state['producer_outcomes'][0]['xmin']='999'
                elif name=='continuation-xmin':seal['xmin']='13'
                elif name=='original-request-digest':state['producer_requests'][3]['request_digest']='\\x'+'ff'*32
                elif name=='original-request-sequence':state['producer_requests'][3]['progress_wire_sequence']=2
                elif name=='old-lease-rewritten':source.only(state['owner_epochs'],role='control',epoch=1)['lease_expires_at_us']+=1
                else:intermediate['acquisition_continuations'][0]['sealed_at_us']-=1
                with self.subTest(name=name),self.assertRaises((AssertionError,ValueError,TypeError)):
                    check.adoption_transition(before,intermediate,retired,state,first)

    def test_exact_old_prefix_and_completed_lifecycle_boundaries_refuse_additions_or_rewrites(self):
        with tempfile.TemporaryDirectory() as temporary:
            before,sealed,retired,after,_,_,_,first,*_=fixture(Path(temporary))
            for table in ('producer_requests','producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies','acquisition_progress','acquisition_quanta'):
                for stage in ('sealed','retired','final'):
                    snapshots=[copy.deepcopy(value) for value in (sealed,retired,after)]
                    selected=snapshots[('sealed','retired','final').index(stage)]
                    extra=copy.deepcopy(selected[table][0])
                    if table in ('producer_requests','producer_outcomes'):extra['request_sequence']=6
                    selected[table].append(extra)
                    with self.subTest(table=table,stage=stage),self.assertRaises(AssertionError):
                        check.adoption_transition(before,*snapshots,first)
            for name in ('sealed-open','unreleased-ingest','wrong-ingest-digest','unclosed-domain','wrong-domain-digest','missing-domain',
                    'closed-source-xmin','retired-producer-xmin','retired-ingest-xmin','retired-domain-time','old-control-xmin',
                    'ingest-role-time','control-role-xmin','domain-retirement-time','retirement-role-xmin'):
                snapshots=[copy.deepcopy(value) for value in (sealed,retired,after)];s,r,a=snapshots
                if name=='sealed-open':s['producer_epochs'][0]['state']='open'
                elif name=='unreleased-ingest':a['owner_epochs'][0]['state']='active'
                elif name=='wrong-ingest-digest':a['owner_epochs'][0]['exclusion_digest']='\\x'+'ff'*32
                elif name=='unclosed-domain':a['allocation_domains'][0]['state']='active'
                elif name=='wrong-domain-digest':a['allocation_domains'][0]['closure_digest']='\\x'+'ff'*32
                elif name=='missing-domain':a['allocation_domains'].pop(0)
                elif name=='closed-source-xmin':a['source_streams'][0]['xmin']='999'
                elif name=='retired-producer-xmin':a['producer_epochs'][0]['xmin']='999'
                elif name=='retired-ingest-xmin':a['owner_epochs'][0]['xmin']='999'
                elif name=='retired-domain-time':a['allocation_domains'][0]['closed_at_us']+=1
                elif name=='ingest-role-time':source.only(a['role_instances'],role_instance_id=a['owner_epochs'][0]['role_instance_id'])['retired_at_us']+=1
                elif name=='control-role-xmin':source.only(a['role_instances'],role_instance_id=before['owner_epochs'][1]['role_instance_id'])['xmin']='999'
                elif name=='domain-retirement-time':
                    r['allocation_domains'][0]['closed_at_us']+=1;a['allocation_domains'][0]['closed_at_us']+=1
                elif name=='retirement-role-xmin':
                    for value in (r,a):source.only(value['role_instances'],role_instance_id=value['owner_epochs'][0]['role_instance_id'])['xmin']='12'
                else:source.only(a['owner_epochs'],role='control',epoch=1)['xmin']='999'
                with self.subTest(name=name),self.assertRaises((AssertionError,ValueError)):
                    check.adoption_transition(before,s,r,a,first)

    def test_each_literal_page_binds_source_manifest_checkpoint_and_adoption_time(self):
        with tempfile.TemporaryDirectory() as temporary:
            _,_,_,after,_,events,wires,first,second,*_=fixture(Path(temporary))
            for number in range(1,6):
                for field,value in (('manifest_sha256','\\x'+'ff'*32),('route_id','\\x'+'ff'*16),('enrollment_generation',2),
                        ('checkpoint_sha256','\\x'+'ff'*32)):
                    changed=copy.deepcopy(after);changed['acquisition_quanta'][number-1][field]=value
                    with self.subTest(page=number,field=field),self.assertRaises(AssertionError):
                        check.adoption_history(changed,wires,events,first,second)
            for name in ('plan-before-adoption','wire-before-plan','past-original-deadline'):
                changed=copy.deepcopy(after);transcript=copy.deepcopy(wires)
                adopted=changed['acquisition_continuations'][0]['adopted_at_us']
                if name=='plan-before-adoption':changed['acquisition_quanta'][1]['created_at_us']=adopted-1
                elif name=='wire-before-plan':transcript[1]['connected_at_us']=adopted-1
                else:changed['acquisition_quanta'][-1]['completed_at_us']=changed['acquisition_jobs'][0]['deadline_at_us']
                with self.subTest(name=name),self.assertRaises(AssertionError):check.adoption_history(changed,transcript,events,first,second)

    def test_progress_and_head_transactions_bind_original_outcome_and_later_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            before,_,_,after,report,events,wires,first,second,*_=fixture(Path(temporary))
            for epoch,baseline,science in ((1,before,first),(2,after,second)):
                for name in ('head-only','progress-only','both-wrong-boundary'):
                    changed=copy.deepcopy(baseline)
                    head=source.only(changed['producer_checkpoints'],producer_epoch=epoch)
                    progress=source.only(changed['acquisition_progress'],producer_epoch=epoch)
                    if name=='head-only':head['xmin']='999'
                    elif name=='progress-only':progress['xmin']='999'
                    else:head['xmin']=progress['xmin']='106' if epoch==1 else '203'
                    with self.subTest(epoch=epoch,name=name),self.assertRaises(AssertionError):
                        check.adoption_progress(changed,report,events,wires,science,COMPLETED,epoch=epoch)

    def test_native_page_reference_full_fact_receipt_and_original_input_corruption_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            before,sealed,retired,after,report,events,wires,first,second,dump,successor,started=fixture(Path(temporary))
            for name in ('missing-old-page-fact','corrupt-old-page-fact','new-witness','receipt','sequence','input'):
                native=copy.deepcopy(successor);inputs=list(second['inputs'])
                if name=='sequence':native['sequence']+=1
                elif name=='input':inputs[1]=inputs[1][:-1]+bytes([inputs[1][-1]^1])
                else:
                    row=next(row for row in native['rows'] if row['cf']==('receipts' if name=='receipt' else 'facts')
                        and row['key'].startswith('0130' if name=='receipt' else '0120')
                        and (name=='receipt' or row['key'].endswith(source.be(5 if name=='new-witness' else 14).hex())))
                    if name=='missing-old-page-fact':native['rows'].remove(row)
                    else:
                        value=bytearray.fromhex(row['value']);value[-1]^=1;row['value']=value.hex()
                with self.subTest(name=name),self.assertRaises((AssertionError,ValueError,KeyError)):
                    check.adoption_successor_science(after,started,events,wires,native,inputs,first,COMPLETED,before=before)

    def test_real_peer_five_responses_keep_listener_available_and_refuse_a_sixth(self):
        def send(connection,opcode,value):
            body=quantum.wire_json(value) if opcode==1 else b'';mask=b'\x01\x02\x03\x04'
            header=bytes([0x80|opcode,0x80|len(body)]) if len(body)<126 else bytes([0x80|opcode,0xfe])+len(body).to_bytes(2,'big')
            connection.sendall(header+mask+bytes(byte^mask[n%4] for n,byte in enumerate(body)))
        def receive(connection):
            first,second=exact(connection,2);self.assertFalse(second&128)
            size=second&127
            if size==126:size=int.from_bytes(exact(connection,2),'big')
            self.assertLess(size,65536)
            raw=exact(connection,size)
            return first&15,json.loads(raw) if first&15==1 else raw
        for extra in (False,True):
            with self.subTest(extra=extra),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);events=corpus_fixture(root)[:3]
                publish(root,'linear-capped-adoption-profile.json',peer.ADOPTION_PROFILE)
                listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(.05)
                address=listener.getsockname();finished=threading.Event();errors=[]
                worker=threading.Thread(target=peer.relay,args=(root,listener,finished,errors),kwargs=dict(adoption=True));worker.start()
                try:
                    for number,(upper,page) in enumerate(zip(UPPERS,PAGES),1):
                        with socket.create_connection(address,timeout=3) as connection:
                            connection.settimeout(3)
                            connection.sendall(('GET /installed-fixture HTTP/1.1\r\nHost: 127.0.0.1:7447\r\n'
                                'Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n'
                                'Sec-WebSocket-Key: '+base64.b64encode(bytes(16)).decode()+'\r\n\r\n').encode())
                            header=bytearray()
                            while not header.endswith(b'\r\n\r\n'):
                                self.assertLess(len(header),4096);header.extend(exact(connection,1))
                            self.assertTrue(header.startswith(b'HTTP/1.1 101 Switching Protocols\r\n'))
                            ident=f'{number:032x}'+'0'*16
                            send(connection,1,['REQ',ident,dict(kinds=[1],since=0,until=upper-1,limit=3)])
                            for index in page:self.assertEqual(receive(connection),(1,['EVENT',ident,events[index]]))
                            self.assertEqual(receive(connection),(1,['EOSE',ident]))
                            send(connection,1,['CLOSE',ident]);send(connection,8,None)
                            self.assertEqual(receive(connection),(8,b''));self.assertEqual(connection.recv(1),b'')
                    completion=root/'capped-adoption-peer-complete.json';deadline=time.monotonic()+1
                    while not completion.exists() and time.monotonic()<deadline:time.sleep(.01)
                    self.assertTrue(completion.exists(),errors);self.assertTrue(json.loads(completion.read_text())['listening'])
                    self.assertTrue(worker.is_alive());self.assertGreaterEqual(listener.fileno(),0)
                    check.adoption_wires([json.loads(path.read_text()) for path in sorted(root.glob('relay-closed-*.json'))],events)
                    if extra:
                        with socket.create_connection(address,timeout=3):pass
                        worker.join(3);self.assertFalse(worker.is_alive())
                finally:
                    finished.set();worker.join(4);self.assertFalse(worker.is_alive())
                self.assertEqual(listener.fileno(),-1)
                self.assertEqual(len(errors),int(extra))
                joined=json.loads((root/'capped-adoption-peer-joined.json').read_text())
                self.assertEqual((joined['requests'],joined['errors']),(6 if extra else 5,int(extra)))

    def test_client_accepts_only_one_job_and_observes_its_original_time_after_host_gate(self):
        from linear_capped_client import run_adoption,adoption_status
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);scope='12'*16;job='13'*16;session=root/'session.private';saved=dict(session='private')
            session.write_text(json.dumps(saved));publish(root,'visibility.json',dict(ordinal='3'))
            for name in ('prepared','adoption-old-cut','commands-finished'):(root/(name+'.continue')).write_text('released\n')
            final=dict(format='bigbrotr-job-status/1',job_id=job,kind='acquisition',state='queued',job_revision='3',checkpoint_revision='0',
                created_at_us=str(ACCEPTED),completed_at_us=None,observed_at_us=str(COMPLETED),reason_code=None,authority='observation-only',
                linear_quantum=dict(format='bigbrotr-linear-quantum-status/2',quantum_number='5',scope_id=scope,entry_kind='request',
                    disposition='window_closed',lower='0',upper='1',window_end='86400',horizon='86400',window_width='86400',query_budget=128,
                    queries_spent=5,attempted_prefix='86400',gap_count=2,pending_work='settled',pending_interval=None,result='linear_observed',
                    terminal='eose',gap=None,continuation_upper='86400',retained_events=0,classified_prefix=0,coverage='pending'))
            calls=[]
            def request(method,path,body=None,headers=None):
                calls.append((method,path,body))
                if method=='GET':return (401,{}) if path.endswith('/me') else (200,final)
                key=headers['Idempotency-Key'];answer=dict(created=True,command_key=key)
                if path.endswith('/relay-endpoints'):answer.update(endpoint_id='11'*16)
                elif path.endswith('/relay-enrollments'):answer.update(access_scope_id='14'*16,generation='1',enabled=True)
                elif path.endswith('/acquisition-scopes'):answer.update(scope_id=scope)
                elif path.endswith('/jobs'):answer.update(job_id=job,accepted_at_us=str(ACCEPTED),execution='unobserved',authority='acceptance-only')
                return 202,answer
            def relay(directory,listener,finished,errors,**kwargs):
                self.assertEqual(kwargs,dict(adoption=True));finished.wait(3)
            with patch('installed_sync_client.relay_listener',return_value=Mock()),patch.object(peer,'relay',side_effect=relay):
                result=run_adoption(root,request,Mock(return_value={}),{},session,saved,{})
            self.assertEqual(result['job_status'],final)
            accepted=[body for method,path,body in calls if method=='POST' and path.endswith('/jobs')]
            self.assertEqual(len(accepted),1);self.assertEqual(accepted[0]['deadline_us'],'120000000')
            self.assertEqual([path for method,path,_ in calls if method=='GET'],['/management/v1/jobs/'+job,'/management/v1/me'])
            self.assertEqual(len(json.loads((root/'adoption-old-cut.json').read_text())),4)
            commands=json.loads((root/'adoption-old-cut.json').read_text())
            with patch('installed_sync.validate_source') as checked_source,patch('installed_sync.validate_queued') as checked_job:
                check.adoption_source({}, {},commands,principal='actor')
                checked_source.assert_called_once_with({}, {},commands[:3],selector_document=check.ADOPTION_SELECTOR)
                checked_job.assert_called_once_with({},commands,ordinal=1,key='b1'*16,total_jobs=1,expected_principal='actor')
                for name in ('new-selector','new-deadline','replacement-job'):
                    changed=copy.deepcopy(commands)
                    if name=='new-selector':changed[2]['input']['selector']['filters'][0]['since']=23
                    elif name=='new-deadline':changed[3]['input']['deadline_us']='240000000'
                    else:changed.append(copy.deepcopy(changed[-1]))
                    with self.subTest(name=name),self.assertRaises(AssertionError):check.adoption_source({}, {},changed,principal='actor')
            for key,value in (('job_revision','2'),('created_at_us',str(ACCEPTED+1))):
                changed=copy.deepcopy(final);changed[key]=value
                with self.subTest(key=key),self.assertRaises(AssertionError):
                    adoption_status(changed,dict(scope_id=scope),dict(job_id=job,accepted_at_us=str(ACCEPTED)))
            for key,value in (('coverage','complete'),('gap_count',1),('retained_events',1),('queries_spent',4)):
                changed=copy.deepcopy(final);changed['linear_quantum'][key]=value
                with self.subTest(key=key),self.assertRaises(AssertionError):
                    adoption_status(changed,dict(scope_id=scope),dict(job_id=job,accepted_at_us=str(ACCEPTED)))


if __name__=='__main__':unittest.main()
