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
import tomllib
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


def unsettled_fixture(root):
    before,_,_,_,report,events,wires,first,_,dump,*_=fixture(root)
    job=before['jobs'][0];detail=before['acquisition_jobs'][0];dataset=job['dataset_id']
    detail.update(actor_id=job['principal_id'],request_key=job['request_key'])
    job.update(input_complete=True,acquisition_job_protocol=1)
    before['local_accounts']=[dict(dataset_id=dataset,principal_id=job['principal_id'],active=True)]
    before['principal_grants']=[dict(dataset_id=dataset,principal_id=job['principal_id'],action='roles.control',
        resource_kind='dataset',resource_id=dataset)]
    document=dict(format='bigbrotr-product-policy/3',settings={'sync.enabled':True})
    for kind in ('config','policy'):
        before[kind+'_revisions']=[dict(dataset_id=dataset,revision=1,document=copy.deepcopy(document),document_digest='\\x'+'ab'*32)]
    before['acquisition_scopes'][0]['policy_revision']=1
    before['relay_enrollments'][0].update(dataset_id=dataset,enabled=True,generation=1)
    old=source.only(before['owner_epochs'],role='control');old['lease_expires_at_us']=ACCEPTED+60_000_000
    pending=copy.deepcopy(before['acquisition_quanta'][0]);predicate=quantum.wire_json(['REQ','0'*48,dict(kinds=[1],since=0,until=23,limit=3)])
    pending.update(quantum_id='\\x'+'02'*16,quantum_number=2,query_number=2,interval_upper=24,
        checkpoint_before=5,classified_before=2,created_at_us=wires[0]['closed_at_us']+20,
        request_predicate='\\x'+predicate.hex(),predicate_sha256='\\x'+source.sha(predicate),xmin='106')
    for key in ('terminal_kind','terminal_frame','request_id','terminal_sequence','terminal_received_at_us','checkpoint_sha256',
            'teardown_complete','retained_events','classified_prefix','effect_records','dependencies','checkpoint_after',
            'result_class','gap_reason','continuation_upper','disposition','gap_lower','gap_upper','completed_at_us'):
        pending[key]=None
    pending['charged_bytes']=1024+len(predicate)+2*1024*1024+3*1024+256*pending['maximum_dependencies']
    before['acquisition_quanta'].append(pending)
    before_us=ACCEPTED+61_000_000;after_us=before_us+1_000_000;after=copy.deepcopy(before)
    previous=source.only(after['owner_epochs'],role='control')
    previous.update(state='released',released_at_us=before_us+10,exclusion_method='storage_fenced',exclusion_digest='\\x'+'ef'*32,xmin='110')
    source.only(after['role_instances'],role_instance_id=previous['role_instance_id']).update(retired_at_us=before_us+10,xmin='110')
    current=dict(old,epoch=old['epoch']+1,process_incarnation='\\x'+'ec'*16,role_instance_id='\\x'+'ed'*16,
        acquired_at_us=before_us+20,lease_expires_at_us=before_us+5_000_000,xmin='110')
    after['owner_epochs'].append(current)
    after['role_instances'].append(dict(dataset_id=dataset,role='control',role_instance_id=current['role_instance_id'],
        created_at_us=before_us+20,retired_at_us=None,xmin='110'))
    after['producer_epochs'][0].update(state='closing',xmin='111')
    after['source_streams'][0].update(state='closed',closed_at_us=before_us+500_000,last_complete_sequence=17,
        closure_digest='\\x'+source.closure(first['manifest'],'\\x'+report['producer_slot_id'],1,first['inputs'],first['receipts'],17),xmin='112')
    closed=dict(event='dump-state',sequence=86,rows=[dict(cf=cf,key=key.hex(),value=value.hex())
        for cf,key,value in check.adoption_closed_native(first,'\\x'+report['producer_slot_id'])])
    return before,after,report,events,wires[:1],first,dump,closed,before_us,after_us


def root_configuration_report(document_sha256,value='5000000'):
    """Declared config-show serialization; this fixture never resolves defaults."""
    return dict(format='bigbrotr-config-static/1',status='valid-static',document_sha256=document_sha256,
        initial_policy=dict(format='bigbrotr-product-policy/3',status='candidate-only',sha256='ab'*32),
        settings={'runtime.authorization_ttl':dict(value=value,source='schema-default',default_from=None,
            default_add_us='0',reload='live-revocation-barrier',deployment_locked=True,applicability='selected')})


def seal_fault_fixture(root,mode):
    """Same declared old prefix; the pending Q2 fixture is deliberately removed."""
    before,after,report,events,wires,first,dump,closed,start,end=unsettled_fixture(root)
    before['acquisition_quanta'].pop();after['acquisition_quanta'].pop()
    issuer=source.only(after['owner_epochs'],role='control',epoch=2)
    issuer.update(control_backend_pid=7331,lease_expires_at_us=start+60_000_000)
    if mode=='after-complete':
        seal=fixture(root)[1]['acquisition_continuations'][0]
        seal.update(sealing_resource=issuer['resource_id'],sealing_epoch=issuer['epoch'],
            sealing_incarnation=issuer['process_incarnation'],sealed_at_us=start+750_000,xmin='113')
        after['acquisition_continuations']=[seal]
    return before,after,report,events,wires,first,dump,closed,start,end


def seal_retry_fixture(root,mode):
    from initial_succession_oracle import exclusion
    original,failed,initialized,events,wires,first,_,closed,start,end=seal_fault_fixture(root,mode)
    for rows in failed.values():
        for row in rows:row.setdefault('xmin','1')
    for owner in failed['owner_epochs']:
        if owner['role']=='control':
            owner.update(control_fence_version=1,control_backend_start='control-'+str(owner['epoch']),
                archive_frontier_protocol=None,index_publication_protocol=None)
            owner.setdefault('control_backend_pid',7000+owner['epoch'])
    previous=max((row for row in failed['owner_epochs'] if row['role']=='control'),key=lambda row:row['epoch'])
    lower=end+1_000_000;upper=lower+1_000_000
    after=copy.deepcopy(failed)
    current=dict(previous,epoch=previous['epoch']+1,role_instance_id='\\x'+'fa'*16,process_incarnation='\\x'+'fb'*16,
        acquired_at_us=lower+100,lease_expires_at_us=lower+500_000+60_000_000,state='released',released_at_us=upper-10,
        exclusion_method='storage_fenced',exclusion_digest=None,control_backend_pid=7444,control_backend_start='retry-start',xmin='203')
    current['exclusion_digest']=exclusion(current,current['process_incarnation'])
    source.only(after['owner_epochs'],role='control',epoch=previous['epoch']).update(state='released',
        released_at_us=lower+10,exclusion_method='storage_fenced',exclusion_digest=exclusion(previous,current['process_incarnation']),xmin='201')
    source.only(after['role_instances'],role_instance_id=previous['role_instance_id']).update(retired_at_us=lower+20,xmin='201')
    after['owner_epochs'].append(current)
    after['role_instances'].append(dict(dataset_id=current['dataset_id'],role='control',role_instance_id=current['role_instance_id'],
        created_at_us=lower+50,retired_at_us=upper-9,xmin='203'))
    if mode=='before-forward':
        seal=fixture(root)[1]['acquisition_continuations'][0]
        seal.update(sealing_resource=current['resource_id'],sealing_epoch=current['epoch'],sealing_incarnation=current['process_incarnation'],
            sealed_at_us=lower+250_000,xmin='202')
        after['acquisition_continuations']=[seal]
    producer=source.only(failed['producer_epochs']);job=source.only(failed['jobs'])
    report=dict(format='bigbrotr-ingest-recovery/1',state='acquisition-sealed',dataset_id=job['dataset_id'][2:],
        owner_resource_id=producer['owner_resource_id'][2:],producer_slot_id=producer['producer_slot_id'][2:],producer_epoch='1',
        issued_through='5',reserved_through='5',resolved_requests='5',native='closed',owner_release='committed',ready=False,
        acquisition_continuation=dict(sealed_jobs='1',execution='not-authorized'),
        source_closure=dict(last_complete_sequence='17',sha256=failed['source_streams'][0]['closure_digest'][2:]))
    return original,failed,after,initialized,events,wires,first,closed,lower,upper,report


def adoption_fault_fixture(root,mode):
    """Declared5-request/17-record cut and fresh active-zero SQL/native lifetime."""
    from initial_succession_oracle import exclusion
    before,sealed,report,events,wires,first,_,closed,start,end=seal_fault_fixture(root,'after-complete')
    job=before['jobs'][0];ds=job['dataset_id'];slot='\\x'+report['producer_slot_id']
    for state in (before,sealed):
        for rows in state.values():
            for row in rows:row.setdefault('xmin','1')
        state['topology_generations']=[dict(dataset_id=ds,generation=0,previous_generation=None,created_at_us=1000,sealed=True,xmin='1')]
        state['topology_streams']=[dict(dataset_id=ds,topology_generation=0,shard_id=0,owner_epoch=1,handoff_last_complete_sequence=None,xmin='1')]
    old_control=source.only(before['owner_epochs'],role='control')
    changed=source.only(sealed['owner_epochs'],role='control',epoch=old_control['epoch'])
    sealer=source.only(sealed['owner_epochs'],role='control',epoch=2)
    changed['exclusion_digest']=exclusion(changed,sealer['process_incarnation'])
    changed_role=source.only(sealed['role_instances'],role_instance_id=changed['role_instance_id']);changed_role['xmin']=changed['xmin']
    sealer.update(state='released',released_at_us=end,exclusion_method='storage_fenced',
        exclusion_digest=exclusion(sealer,sealer['process_incarnation']),control_backend_start='fresh-start',
        control_fence_version=1,archive_frontier_protocol=None,index_publication_protocol=None,
        lease_expires_at_us=sealer['acquired_at_us']+60_000_000,xmin='115')
    source.only(sealed['role_instances'],role_instance_id=sealer['role_instance_id']).update(retired_at_us=end+1,xmin='115')
    def control(state,epoch,role,inc,at,tx,*,released=False):
        owner=dict(sealer,epoch=epoch,role_instance_id='\\x'+role*16,process_incarnation='\\x'+inc*16,
            acquired_at_us=at,lease_expires_at_us=at+60_000_000,state='released' if released else 'active',
            released_at_us=at+500_000 if released else None,exclusion_method='storage_fenced' if released else None,
            exclusion_digest=None,control_backend_pid=7400+epoch,control_backend_start='fresh-'+str(epoch),xmin=tx)
        if released:owner['exclusion_digest']=exclusion(owner,owner['process_incarnation'])
        state['owner_epochs'].append(owner)
        state['role_instances'].append(dict(dataset_id=ds,role='control',role_instance_id=owner['role_instance_id'],
            created_at_us=at-1,retired_at_us=at+500_001 if released else None,xmin=tx))
        return owner
    retired=copy.deepcopy(sealed);retirer=control(retired,3,'d1','d2',end+1000,'122',released=True)
    at=end+300_000;digest=sealed['source_streams'][0]['closure_digest']
    retired['producer_epochs'][0].update(state='retired',retired_at_us=at,closure_digest=digest,xmin='121')
    ingest=source.only(retired['owner_epochs'],role='ingest')
    ingest.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest=digest,xmin='121')
    source.only(retired['role_instances'],role_instance_id=ingest['role_instance_id']).update(retired_at_us=at,xmin='121')
    for row in retired['allocation_domains']:row.update(state='closed',closed_at_us=at,closure_digest=digest,xmin='121')
    begun=end+1_000_000;returned=begun+1_000_000;after=copy.deepcopy(retired)
    current=control(after,4,'d3','d4',begun+1000,'130')
    current.update(lease_expires_at_us=current['lease_expires_at_us']+20_000,xmin='133')
    created=begun+10_000;expires=begun+1000+60_000_000;inc='\\x'+'cf'*16;role='\\x'+'ce'*16;tx='131'
    domains=[bytes([150+k])*16 for k in range(1,10)]
    manifest=successor_manifest(first['manifest'],source.raw(inc),domains)
    handoff=(b'\x01\x0a'+source.raw(slot)+source.be(1)+source.be(5)+source.be(17)+source.raw(digest)
        +source.be(len(first['manifest']),2)+first['manifest']+source.be(len(manifest),2)+manifest)
    after['owner_epochs'].append(dict(ingest,epoch=2,role_instance_id=role,process_incarnation=inc,acquired_at_us=created,
        lease_expires_at_us=expires,state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,xmin=tx))
    after['role_instances'].append(dict(dataset_id=ds,role='ingest',role_instance_id=role,created_at_us=created,retired_at_us=None,xmin=tx))
    after['producer_epochs'].append(dict(retired['producer_epochs'][0],producer_epoch=2,producer_role_instance_id=role,
        producer_incarnation=inc,owner_epoch=2,owner_incarnation=inc,state='open',reserved_through_request=0,
        opened_at_us=created,retired_at_us=None,closure_digest=None,xmin=tx))
    after['source_streams'].append(dict(retired['source_streams'][0],owner_epoch=2,owner_incarnation=inc,state='open',opened_at_us=created,
        closed_at_us=None,last_complete_sequence=None,closure_digest=None,xmin=tx))
    for kind,domain in enumerate(domains,1):
        old=source.only(retired['allocation_domains'],dictionary_kind=kind)
        after['allocation_domains'].append(dict(old,allocation_domain='\\x'+domain.hex(),owner_epoch=2,owner_incarnation=inc,
            state='active',created_at_us=created,closed_at_us=None,closure_digest=None,xmin=tx))
    after['topology_generations'].append(dict(retired['topology_generations'][0],generation=1,previous_generation=0,
        created_at_us=created,sealed=True,xmin=tx))
    after['topology_streams'] += [dict(retired['topology_streams'][0],topology_generation=1,owner_epoch=epoch,
        handoff_last_complete_sequence=17 if epoch==1 else None,xmin=tx) for epoch in (1,2)]
    after['datasets'][0].update(topology_generation=1,xmin=tx)
    intent=dict(dataset_id=ds,owner_resource_id=ingest['resource_id'],target_epoch=2,owner_role='ingest',
        target_incarnation=inc,target_role_instance_id=role,producer_slot_id=slot,producer_epoch=2,shard_id=0,
        previous_topology=0,target_topology=1,issued_through=5,last_complete_sequence=17,closure_digest=digest,
        previous_manifest='\\x'+first['manifest'].hex(),target_manifest='\\x'+manifest.hex(),
        previous_manifest_sha256='\\x'+source.sha(first['manifest']),target_manifest_sha256='\\x'+source.sha(manifest),
        handoff='\\x'+handoff.hex(),handoff_sha256='\\x'+source.sha(handoff),config_revision=1,policy_revision=1,
        config_digest='\\x'+'ab'*32,policy_digest='\\x'+'ab'*32,outstanding_request_limit=retired['producer_epochs'][0]['outstanding_request_limit'],
        created_at_us=created,lease_expires_at_us=expires,prepared_by_resource=current['resource_id'],prepared_by_epoch=current['epoch'],
        prepared_by_role='control',prepared_by_incarnation=current['process_incarnation'],xmin=tx)
    after['initial_ingest_successions']=[intent]
    keys=('dataset_id','owner_resource_id','target_epoch','target_incarnation','target_role_instance_id','producer_slot_id','producer_epoch',
        'previous_manifest_sha256','target_manifest_sha256','handoff_sha256','prepared_by_resource','prepared_by_epoch','prepared_by_incarnation',
        'config_revision','policy_revision','config_digest','policy_digest')
    after['initial_ingest_activations']=[dict({key:intent[key] for key in keys},activated_by_resource=current['resource_id'],
        activated_by_epoch=current['epoch'],activated_by_incarnation=current['process_incarnation'],activated_at_us=begun+20_000,
        activation_transaction='132',xmin='132')]
    if mode=='after-complete':
        after['acquisition_continuations'][0].update(target_epoch=2,target_manifest_sha256='\\x'+source.sha(manifest),
            activation_sha256='\\x'+source.sha(handoff),adopting_resource=current['resource_id'],adopting_epoch=current['epoch'],
            adopting_incarnation=current['process_incarnation'],adopted_job_revision=3,adopted_at_us=begun+30_000,
            adoption_transaction='134',xmin='134')
        after['jobs'][0].update(job_revision=3,owner_resource_id=current['resource_id'],owner_epoch=current['epoch'],
            owner_incarnation=current['process_incarnation'],xmin='134')
    values=successor_rows({(row['cf'],bytes.fromhex(row['key'])):bytes.fromhex(row['value']) for row in closed['rows']},
        manifest,source.raw(slot),handoff)
    dump=dict(event='dump-state',sequence=91,rows=[dict(cf=cf,key=key.hex(),value=value.hex()) for (cf,key),value in sorted(values.items())])
    return before,sealed,retired,after,report,events,wires,first,dump,begun,returned,dict(backend_pid=7404)


def authority_fixture(root,profile):
    original,sealed,retired,after,report,events,wires,first,dump,start,end,fault=adoption_fault_fixture(root,'before-forward')
    eligible=start-500_000;prepared=copy.deepcopy(retired)
    grant=source.only(retired['principal_grants']);shadow=dict(grant,action='status.read')
    for state in (retired,prepared,after):state['principal_grants'].append(copy.deepcopy(shadow))
    if profile=='actor-grant':
        prepared['principal_grants'].remove(grant);after['principal_grants'].remove(grant)
    else:
        delta=retired['acquisition_jobs'][0]['deadline_at_us']+1_000_000-start
        for table,rows in after.items():
            for row in rows:
                fresh=(table=='owner_epochs' and row['state']=='active' or table=='role_instances' and row['retired_at_us'] is None
                    or table in ('initial_ingest_successions','initial_ingest_activations') or table=='allocation_domains' and row['owner_epoch']==2
                    or table=='producer_epochs' and row['producer_epoch']==2 or table=='source_streams' and row['owner_epoch']==2
                    or table=='topology_generations' and row['generation']==1)
                if fresh:
                    for key in ('acquired_at_us','lease_expires_at_us','created_at_us','opened_at_us','activated_at_us'):
                        if key in row:row[key]+=delta
        start+=delta;end+=delta
    observed=dict(event='continuation-authority-refused',classification='acquisition-continuation-authority',sqlstate='BB001',
        reason='acquisition job requires current authority and original remaining duration',backend_pid=fault['backend_pid'],
        role='bigbrotr_control',database='bigbrotr_control',executed=True,transaction='T')
    return retired,prepared,after,report,events,wires,first,dump,eligible,start,end,observed


def integrity_fixture(root):
    original,after,report,events,wires,first,_,closed,start,end=seal_fault_fixture(root,'before-forward')
    seeded=copy.deepcopy(original);old,changed=check.integrity_effect_seed(original,events)
    changed=dict(changed,xmin='107');seeded['acquisition_quanta']=[changed]
    after['acquisition_quanta']=[copy.deepcopy(changed)]
    common=dict(backend_pid=7331,role='bigbrotr_control',database='bigbrotr_control',executed=True,transaction='T')
    inserted=dict(common,event='integrity-seal-insert-complete');result=dict(common,event='continuation-coverage-result',value=False)
    return original,seeded,after,report,events,wires,first,closed,start,end,inserted,result


class CappedAdoptionControls(unittest.TestCase):
    def test_exact_seal_retry_requires_real_fencing_released_owner_and_preserves_the_whole_existing_seal(self):
        for mode in ('before-forward','after-complete'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as temporary:
                _,failed,after,initialized,events,wires,first,closed,start,end,report=seal_retry_fixture(Path(temporary),mode)
                def verify(state=after,answer=report,prior=failed):
                    check.adoption_seal_retry(prior,state,first,wires,events,start,end,end+5_000_000,mode,answer)
                verify();check.adoption_closed_native(first,'\\x'+initialized['producer_slot_id'],closed)
                previous=source.only(failed['owner_epochs'],role='control',epoch=2)
                current=source.only(after['owner_epochs'],role='control',epoch=3)
                self.assertLess(current['acquired_at_us'],previous['lease_expires_at_us'])
                self.assertNotEqual(current['lease_expires_at_us'],current['acquired_at_us']+60_000_000)
                mutations=[('owner_epochs',2,'state','active'),('owner_epochs',2,'exclusion_digest','\\x'+'ff'*32),
                    ('owner_epochs',2,'lease_expires_at_us',end),('owner_epochs',3,'state','active'),
                    ('owner_epochs',3,'lease_expires_at_us',end),('owner_epochs',3,'exclusion_digest','\\x'+'ff'*32),
                    ('role_instances',3,'retired_at_us',None),('jobs',0,'job_revision',3),('jobs',0,'xmin','204'),
                    ('acquisition_jobs',0,'deadline_at_us',ACCEPTED+121_000_000),('source_streams',0,'xmin','204'),
                    ('source_streams',0,'closed_at_us',end),('producer_epochs',0,'xmin','204'),
                    ('acquisition_quanta',0,'xmin','204'),('producer_requests',0,'xmin','204'),
                    ('producer_outcomes',0,'outcome_kind',2),('acquisition_continuations',0,'sealing_epoch',2 if mode=='before-forward' else 3),
                    ('acquisition_continuations',0,'sealed_at_us',start+1),('acquisition_continuations',0,'xmin','204')]
                if mode=='before-forward':mutations.pop() # A new seal has an opaque fresh transaction.
                for table,index,key,value in mutations:
                    changed=copy.deepcopy(after);changed[table][index][key]=value
                    with self.subTest(table=table,key=key),self.assertRaises(AssertionError):verify(changed)
                for table in ('owner_epochs','role_instances','producer_requests'):
                    changed=copy.deepcopy(after);changed[table].append(copy.deepcopy(changed[table][-1]))
                    with self.subTest(extra=table),self.assertRaises((AssertionError,ValueError)):verify(changed)
                for key,value in (('state','source-closed'),('ready',True),('resolved_requests','4'),('owner_release','unobserved')):
                    with self.subTest(report=key),self.assertRaises(AssertionError):verify(answer=dict(report,**{key:value}))
                changed=copy.deepcopy(failed);source.only(changed['owner_epochs'],role='control',epoch=2)['lease_expires_at_us']=start
                with self.assertRaises(AssertionError):verify(prior=changed)

    def test_seal_retry_parser_is_explicit_and_rejects_all_other_profiles(self):
        spec=importlib.util.spec_from_file_location('seal_retry_runner_control',ROOT/'tests/acceptance/run.py')
        runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        common=['run.py','p03-linear-quantum','--linear-quantum-scenario','settled-owner-adoption','--linear-quantum-fault','plan-before']
        for mode in ('before-forward','after-complete'):
            with tempfile.TemporaryDirectory() as temporary:
                with patch.object(sys,'argv',common+['--linear-quantum-seal-fault',mode,'--linear-quantum-seal-retry','exact',
                        '--source-root',str(ROOT),'--evidence',str(Path(temporary)/'evidence')]),patch.object(sys,'stdout'),\
                        patch('cases.first_launch.first_launch') as launched:
                    self.assertEqual(runner.main(),2)
                self.assertEqual(launched.call_args.args[0].linear_quantum_seal_retry,'exact')
        for prefix in (['run.py','p01-t01','--binary',sys.executable],common,common[:-1]+['plan-after'],
                common+['--linear-quantum-adoption-fault','before-forward'],common+['--linear-quantum-authority-refusal','actor-grant'],
                common+['--linear-quantum-integrity-refusal','workflow-effect'],common+['--linear-quantum-trace-successor-sql']):
            with tempfile.TemporaryDirectory() as temporary:
                evidence=Path(temporary)/'evidence'
                tail=['--linear-quantum-seal-retry','exact','--source-root',str(ROOT),'--evidence',str(evidence)]
                if prefix!=common:tail+=['--linear-quantum-seal-fault','before-forward']
                with patch.object(sys,'argv',prefix+tail),patch.object(sys,'stderr'),self.assertRaises(SystemExit):runner.main()
                self.assertFalse(evidence.exists())

    def test_retry_checks_exclusion_restores_direct_route_and_joins_success_before_native_proof(self):
        from installed import set_value
        for mode in ('before-forward','after-complete'):
            for failure in (None,'proxy','observer','old-config','config','authorization','diagnostic','report','native','history'):
                with self.subTest(mode=mode,failure=failure),tempfile.TemporaryDirectory() as temporary:
                    root=Path(temporary);_,failed,after,initialized,events,wires,first,closed,start,end,report=seal_retry_fixture(root,mode)
                    runtime=object.__new__(case.CappedAdoptionRuntime);runtime.root=root;runtime.home=root/'peer';runtime.home.mkdir()
                    runtime.config=root/'instance.toml';runtime.config.write_text('[postgres.control]\nendpoint = "127.0.0.1:33333"\n')
                    runtime.set_value=set_value;runtime.args=SimpleNamespace(linear_quantum_seal_retry='exact')
                    runtime.state=root/'state';runtime.adoption_refused=failed;runtime.adoption_first=first;runtime.initialized=initialized
                    runtime.adoption_refusal_dump=copy.deepcopy(closed);runtime.events=events;runtime.inputs={}
                    runtime.resources=dict(seal_proxy=dict(status='active' if failure=='proxy' else 'joined'),native_observer=dict(id='observer'))
                    failed_output=root/'seal-commit-fault';failed_output.mkdir()
                    publish(failed_output,'terminal-container.json',dict(id='failed',state=dict(ExitCode=4,Pid=0,Running=False,OOMKilled=False)))
                    publish(failed_output,'refused-control.json',failed)
                    old_sha256='00'*32 if failure=='old-config' else case.digest(runtime.config)
                    publish(failed_output,'authorization-interval.json',dict(root_config_document_sha256=old_sha256,
                        root_config_after_recovery_sha256=old_sha256))
                    publish(root,'native-observer-terminal.json',dict(id='observer',state=dict(ExitCode=0,Pid=1 if failure=='observer' else 0,Running=False,OOMKilled=False)))
                    publish(failed_output,'native-observer-process.json',dict(exit_code=0,overflow=False))
                    publish(root,'seal-proxy-terminal.json',dict(event='terminal'));runtime.validate_seal_fault_terminal=Mock()
                    def absence(output):publish(output,'backend-absence.json',dict(absent=True,cut=runtime.backend_absence.call_count))
                    runtime.backend_absence=Mock(side_effect=absence)
                    def finite(*args):
                        self.assertEqual(tomllib.loads(runtime.config.read_text())['postgres']['control']['endpoint'],'127.0.0.1:15432')
                        (root/'bootstrap').mkdir()
                        observed=root_configuration_report(case.digest(runtime.config));publish(root/'bootstrap','seal-retry-root-config.stdout',observed)
                        if failure=='config':runtime.config.write_text(runtime.config.read_text()+'\n# changed\n')
                        return observed
                    runtime.finite=Mock(side_effect=finite);runtime.sql=Mock(side_effect=[str(start),str(end),str(end+5_000_000)])
                    runtime.launch=Mock(return_value=dict(id='retry'));runtime.docker=Mock(return_value=SimpleNamespace(stdout='0\n'))
                    runtime.inspect=Mock(return_value=dict(Id='retry',State=dict(ExitCode=0,Pid=0,Running=False,OOMKilled=False)))
                    def logs(entry,output):
                        (output/'serve.stdout').write_text(json.dumps(report)+'\n')
                        (output/'serve.stderr').write_text('diagnostic' if failure=='diagnostic' else '')
                        return [dict(report,state='source-closed') if failure=='report' else report]
                    runtime.logs=Mock(side_effect=logs)
                    if failure=='history':after['jobs'][0]['xmin']='999'
                    runtime.capture=Mock(return_value=(after,{}));runtime.wires=Mock(return_value=wires)
                    if failure=='native':closed['sequence']=87
                    def census(*args,**kwargs):
                        self.assertEqual(kwargs['writer_terminal'],root/'seal-commit-retry/terminal-container.json')
                        self.assertFalse((runtime.home/'seal-commit-retry.continue').exists())
                        self.assertGreaterEqual(runtime.backend_absence.call_count,2);return closed
                    directory=runtime.state/'data/ingest/producer-inputs-v1';directory.mkdir(parents=True)
                    for number,data in enumerate(first['inputs'],1):
                        (directory/f"{initialized['producer_slot_id']}-0000000000000001-{number:016x}.cbor").write_bytes(data)
                    runtime.adoption_original_files={'inputs':'exact'};runtime.files=Mock(return_value=runtime.adoption_original_files)
                    runtime.client=Mock(pid=12345);runtime.client.poll.return_value=None
                    runtime.owned_listeners=Mock(return_value=['listener']);runtime.loopback_listener=Mock()
                    with patch.object(case.native,'census',side_effect=census),patch.object(case.time,'CLOCK_BOOTTIME',7,create=True),\
                            patch.object(case.time,'clock_gettime_ns',side_effect=[1_000_000_000,6_000_000_000 if failure=='authorization' else 2_000_000_000]):
                        if failure is None:runtime.retry_seal(mode)
                        else:
                            with self.assertRaises((AssertionError,ValueError)):runtime.retry_seal(mode)
                    self.assertFalse((runtime.home/'seal-commit-retry.continue').exists())
                    if failure in ('proxy','observer','old-config'):runtime.launch.assert_not_called();runtime.finite.assert_not_called()
                    elif failure is None:
                        self.assertEqual(runtime.adoption_retry_dump,closed)
                        prior=root/'seal-commit-retry/prior-backend/backend-absence.json'
                        receipt=case.load(root/'seal-commit-retry/excluded-prior-owners.json')
                        self.assertEqual(receipt['backend_absence_sha256'],case.digest(prior))
                        self.assertNotEqual(case.digest(prior),case.digest(root/'seal-commit-retry/backend-absence.json'))
                        runtime.launch.assert_called_once_with('retry-seal-acquisition','app','ingest','recover','--file','/etc/bigbrotr/instance.toml','--seal-acquisition')
                        runtime.finite.assert_called_once_with('seal-retry-root-config','config','show','--file','/etc/bigbrotr/instance.toml')

    def test_integrity_admin_seed_is_captured_before_write_and_restores_origin(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);original,seeded,_,report,events,_,_,_,start,*_=integrity_fixture(root)
            old,changed=check.integrity_effect_seed(original,events)
            text='\n'.join(json.dumps(value) for value in (dict(stage='before',mode='origin',row=old),
                dict(stage='changed',count=1,exact_one=1,row=seeded['acquisition_quanta'][0]),dict(stage='restored',mode='origin')))
            runtime=object.__new__(case.CappedAdoptionRuntime);runtime.root=root;runtime.adoption_before=original;runtime.events=events
            runtime.args=SimpleNamespace(linear_quantum_integrity_refusal='workflow-effect')
            def sql(database,query):
                self.assertEqual(database,'bigbrotr_control')
                self.assertEqual(json.loads((root/'continuation-integrity-refusal/seed-original-control.json').read_text()),original)
                return text if 'SET LOCAL' in query else str(start)
            runtime.sql=Mock(side_effect=sql);runtime.capture=Mock(return_value=(seeded,{}))
            runtime.prepare_integrity_refusal()
            self.assertEqual(runtime.sql.call_count,2);runtime.capture.assert_called_once_with(root/'continuation-integrity-refusal','seeded')
            self.assertEqual(json.loads((root/'continuation-integrity-refusal/seed-observation.json').read_text())['setup_kind'],'fixture-admin-replica')

    def test_integrity_selector_only_accepts_the_declared_profile_and_keeps_separate_pass_route(self):
        spec=importlib.util.spec_from_file_location('integrity_runner_control',ROOT/'tests/acceptance/run.py')
        runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        common=['run.py','p03-linear-quantum','--linear-quantum-scenario','settled-owner-adoption','--linear-quantum-fault','plan-before']
        with tempfile.TemporaryDirectory() as temporary:
            tail=['--source-root',str(ROOT),'--evidence',str(Path(temporary)/'evidence'),'--linear-quantum-integrity-refusal','workflow-effect']
            with patch.object(sys,'argv',common+tail),patch.object(sys,'stdout'),patch('cases.first_launch.first_launch') as launched:self.assertEqual(runner.main(),2)
            self.assertEqual(launched.call_args.args[0].linear_quantum_integrity_refusal,'workflow-effect')
        for prefix in (['run.py','p03-linear-quantum'],common[:-1]+['plan-after'],common+['--linear-quantum-trace-successor-sql'],
                common+['--linear-quantum-seal-fault','before-forward'],common+['--linear-quantum-adoption-fault','after-complete'],
                common+['--linear-quantum-authority-refusal','actor-grant']):
            with tempfile.TemporaryDirectory() as temporary:
                evidence=Path(temporary)/'evidence'
                with patch.object(sys,'argv',prefix+['--source-root',str(ROOT),'--evidence',str(evidence),'--linear-quantum-integrity-refusal','workflow-effect']),\
                        patch.object(sys,'stderr'),self.assertRaises(SystemExit) as raised:runner.main()
                self.assertEqual(raised.exception.code,2);self.assertFalse(evidence.exists())
        from installed import set_value
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=object.__new__(case.CappedAdoptionRuntime);runtime.root=root;runtime.source=ROOT
            runtime.config=root/'instance.toml';runtime.config.write_text('[postgres.control]\nendpoint = "127.0.0.1:15432"\n')
            runtime.set_value=set_value;runtime.quantum_fault='plan-before';runtime.route=Mock();old=runtime.route
            runtime.resources=dict(quantum_proxy=dict(status='joined',pid=42),cleanup_errors=[]);runtime.inputs={};runtime.record_resources=Mock()
            route=Mock();route.process.pid=43;route.process.poll.return_value=0;route.wait.return_value=0
            route.threads=[];route.stderr=b'';route.overflow=False;runtime.proxy_process=Mock(return_value=route)
            runtime.proxy_event=Mock(side_effect=lambda *args:dict(runtime.integrity_proxy_binding,event='ready',port=12348))
            runtime.start_integrity_proxy();self.assertIs(runtime.route,old);self.assertFalse(hasattr(runtime,'seal_route'))
            self.assertEqual(runtime.inputs['integrity_proxy']['mode'],'pass');self.assertNotIn('commit_ordinal',runtime.inputs['integrity_proxy'])
            terminal=dict(runtime.integrity_proxy_binding,event='terminal',unexpected=0,frames=0,connections=0,bytes=0)
            route.stdout=(json.dumps(runtime.integrity_proxy_ready)+'\n'+json.dumps(terminal)+'\n').encode()
            runtime.proxy_observation=Mock(return_value=dict(stdout=route.stdout.decode(),stderr='',overflow=False))
            runtime.stop_integrity_proxy();self.assertEqual(runtime.resources['cleanup_errors'],[])
            with self.assertRaises(ValueError):runtime.validate_integrity_refusal_terminal(terminal)

    def test_integrity_seed_is_one_fixture_byte_and_exact_coverage_join_refuses_missing_request4(self):
        with tempfile.TemporaryDirectory() as temporary:
            original,seeded,after,report,events,wires,first,closed,start,end,inserted,result=integrity_fixture(Path(temporary))
            check.adoption_history(original,wires,events,first)
            old,changed=check.integrity_effect_seed(original,events)
            self.assertEqual([i for i,(a,b) in enumerate(zip(source.raw(old['effect_records']),source.raw(changed['effect_records']))) if a!=b],[51])
            self.assertEqual(check.integrity_workflow_coverage(original),dict(covered_requests=[4,5],missing_requests=[]))
            self.assertEqual(check.integrity_workflow_coverage(seeded),dict(covered_requests=[5],missing_requests=[4]))
            check.integrity_seed_rows(original,seeded,events)
            check.integrity_refusal(original,seeded,after,first,wires,events,start,end,end+5_000_000,inserted,result)
            check.adoption_closed_native(first,'\\x'+report['producer_slot_id'],closed)
            check.adoption_progress(after,report,events,wires,first,end+5_000_000,epoch=1)
            with self.assertRaises((AssertionError,ValueError)):check.adoption_history(seeded,wires,events,first)
            text='\n'.join(json.dumps(value) for value in (dict(stage='before',mode='origin',row=old),
                dict(stage='changed',count=1,exact_one=1,row=seeded['acquisition_quanta'][0]),dict(stage='restored',mode='origin')))
            self.assertEqual(check.integrity_seed_receipt(text,old,changed),seeded['acquisition_quanta'][0])
            sql=check.integrity_seed_sql(old)
            self.assertIn('SET LOCAL session_replication_role=replica',sql);self.assertIn('set_byte(effect_records,51,5)',sql)
            self.assertIn("xmin::text='"+old['xmin']+"'",sql);self.assertNotIn('ALTER TABLE',sql);self.assertNotIn('charged_bytes=',sql)
            for table,index,key,value in (('acquisition_quanta',0,'charged_bytes',old['charged_bytes']-93),
                    ('acquisition_quanta',0,'effect_records','\\x'+source.raw(changed['effect_records'])[93:].hex()),
                    ('acquisition_quanta',0,'checkpoint_after',6),('jobs',0,'xmin','999'),('producer_requests',3,'request_sequence',5)):
                bad=copy.deepcopy(seeded);bad[table][index][key]=value
                with self.subTest(table=table,key=key),self.assertRaises((AssertionError,ValueError)):check.integrity_seed_rows(original,bad,events)
            for key,value in (('value',True),('executed',False),('backend_pid',7332),('role','postgres'),('transaction','I')):
                with self.subTest(marker=key),self.assertRaises(AssertionError):
                    check.integrity_refusal(original,seeded,after,first,wires,events,start,end,end+5_000_000,inserted,dict(result,**{key:value}))
            for key,value in (('mode','replica'),('row',changed)):
                rows=[json.loads(line) for line in text.splitlines()];rows[0][key]=value
                with self.assertRaises(AssertionError):check.integrity_seed_receipt('\n'.join(json.dumps(row) for row in rows),old,changed)

    def test_integrity_runtime_keeps_peer_until_executed_false_and_exact_native_closure(self):
        for failure in (None,'unexecuted','true','commit','pid','native','diagnostic'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);original,seeded,after,report,events,wires,first,closed,start,end,inserted,result=integrity_fixture(root)
                runtime=object.__new__(case.CappedAdoptionRuntime);runtime.root=root;runtime.home=root/'peer';runtime.home.mkdir()
                runtime.args=SimpleNamespace()
                runtime.state=root/'state';runtime.config=root/'instance.toml';runtime.config.write_text('[runtime]\n')
                runtime.adoption_before=original;runtime.integrity_seeded=seeded;runtime.adoption_first=first
                runtime.initialized=report;runtime.events=events;runtime.integrity_route=Mock()
                document_sha256=case.digest(runtime.config)
                def finite(*args):
                    (root/'bootstrap').mkdir();observed=root_configuration_report(document_sha256)
                    publish(root/'bootstrap','integrity-root-config.stdout',observed);return observed
                runtime.finite=Mock(side_effect=finite)
                directory=runtime.state/'data/ingest/producer-inputs-v1';directory.mkdir(parents=True)
                for number,data in enumerate(first['inputs'],1):
                    (directory/f"{report['producer_slot_id']}-0000000000000001-{number:016x}.cbor").write_bytes(data)
                runtime.adoption_original_files={'old-inputs':'exact'};runtime.files=Mock(return_value=runtime.adoption_original_files)
                runtime.sql=Mock(side_effect=[str(start),str(end),str(end+5_000_000)])
                runtime.launch=Mock(return_value=dict(id='ab'*32));runtime.docker=Mock(return_value=SimpleNamespace(stdout='4\n'))
                runtime.inspect=Mock(return_value=dict(Id='ab'*32,State=dict(ExitCode=4,Pid=0,Running=False,OOMKilled=False)))
                def logs(entry,output):
                    (output/'serve.stdout').write_text('')
                    (output/'serve.stderr').write_text('error: producer operation unavailable: Control('
                        +('Backend' if failure=='diagnostic' else 'InvalidMetadata')+')\n')
                runtime.logs=Mock(side_effect=logs)
                runtime.integrity_proxy_binding=dict(mode='pass',target='control-acquisition-coverage',frame_limit=131072,
                    connection_limit=64,wire_limit=64*1024*1024)
                terminal=dict(runtime.integrity_proxy_binding,event='terminal',unexpected=0,frames=30,connections=1,bytes=1024,
                    integrity_inserts_complete=int(failure!='unexecuted'),coverage_selects_complete=1,coverage_false=int(failure!='true'),
                    coverage_true=int(failure=='true'),coverage_unpaired=0,integrity_commits=int(failure=='commit'),integrity_server_commits=0,
                    integrity_rollbacks=0,integrity_errors=0,data_queries=1,data_commits=0,server_complete=0,client_complete=0,faults=0)
                if failure=='pid':result['backend_pid']=7332
                if failure=='true':result['value']=True
                runtime.proxy_event=Mock(side_effect=[inserted,result])
                def stop_proxy():
                    publish(root,'integrity-proxy-terminal.json',terminal)
                    publish(root,'integrity-proxy.json',dict(stdout='\n'.join(json.dumps(value) for value in (inserted,result))))
                runtime.stop_integrity_proxy=Mock(side_effect=stop_proxy);runtime.backend_absence=Mock()
                runtime.capture=Mock(return_value=(after,{}));runtime.wires=Mock(return_value=wires)
                if failure=='native':closed['sequence']=87
                def census(*args,**kwargs):
                    runtime.stop_integrity_proxy.assert_called_once();runtime.backend_absence.assert_called_once()
                    self.assertFalse((runtime.home/'continuation-integrity-refusal.continue').exists());return closed
                runtime.client=Mock(pid=12345);runtime.client.poll.return_value=None;runtime.processes=[runtime.client]
                runtime.owned_listeners=Mock(return_value=['listener']);runtime.loopback_listener=Mock()
                (runtime.home/'client.stderr').write_text('')
                publish(runtime.home,'client-result.json',dict(status='PASS',completion='continuation-integrity-refusal',job_status=None))
                def joined(*,timeout):
                    self.assertTrue((runtime.home/'continuation-integrity-refusal.continue').exists());return 0
                runtime.client.wait.side_effect=joined
                with patch.object(case.native,'census',side_effect=census),patch.object(case,'stop_process'),\
                        patch.object(case.time,'CLOCK_BOOTTIME',7,create=True),patch.object(case.time,'clock_gettime_ns',side_effect=[1_000_000_000,2_000_000_000]):
                    if failure is None:runtime.failed_seal(integrity=True)
                    else:
                        with self.assertRaises((AssertionError,ValueError)):runtime.failed_seal(integrity=True)
                runtime.finite.assert_called_once_with('integrity-root-config','config','show','--file','/etc/bigbrotr/instance.toml')
                runtime.launch.assert_called_once_with('refuse-integrity-seal','app','ingest','recover','--file','/etc/bigbrotr/instance.toml','--seal-acquisition')
                if failure is None:runtime.client.wait.assert_called_once_with(timeout=6)
                else:runtime.client.wait.assert_not_called()

    def test_authority_selector_requires_only_its_explicit_plan_before_profile(self):
        spec=importlib.util.spec_from_file_location('authority_runner_control',ROOT/'tests/acceptance/run.py')
        runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        common=['run.py','p03-linear-quantum','--linear-quantum-scenario','settled-owner-adoption','--linear-quantum-fault','plan-before']
        for profile in ('actor-grant','original-expiry'):
            with tempfile.TemporaryDirectory() as temporary:
                tail=['--source-root',str(ROOT),'--evidence',str(Path(temporary)/'evidence'),'--linear-quantum-authority-refusal',profile]
                with patch.object(sys,'argv',common+tail),patch.object(sys,'stdout'),patch('cases.first_launch.first_launch') as launched:
                    self.assertEqual(runner.main(),2)
                self.assertEqual(launched.call_args.args[0].linear_quantum_authority_refusal,profile)
        for prefix in (['run.py','p01-t01','--binary',sys.executable],['run.py','p03-linear-quantum'],common[:-1]+['plan-after'],
                common+['--linear-quantum-trace-successor-sql'],common+['--linear-quantum-seal-fault','before-forward'],
                common+['--linear-quantum-adoption-fault','after-complete']):
            with tempfile.TemporaryDirectory() as temporary:
                evidence=Path(temporary)/'evidence'
                with patch.object(sys,'argv',prefix+['--source-root',str(ROOT),'--evidence',str(evidence),
                        '--linear-quantum-authority-refusal','actor-grant']),patch.object(sys,'stderr'),self.assertRaises(SystemExit) as raised:runner.main()
                self.assertEqual(raised.exception.code,2);self.assertFalse(evidence.exists())

    def test_authority_route_keeps_original_proxy_and_untriggered_cleanup_honest(self):
        from installed import set_value
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=object.__new__(case.CappedAdoptionRuntime)
            runtime.root=root;runtime.source=ROOT;runtime.config=root/'instance.toml'
            runtime.config.write_text('[postgres.control]\nendpoint = "127.0.0.1:15432"\n');runtime.set_value=set_value
            runtime.quantum_fault='plan-before';original=Mock();runtime.route=original
            old=dict(status='joined',pid=42,port=9999);runtime.resources=dict(quantum_proxy=old.copy(),cleanup_errors=[])
            runtime.inputs=dict(quantum_proxy=dict(original=True));runtime.record_resources=Mock()
            route=Mock();route.process.pid=43;route.process.poll.return_value=0;route.wait.return_value=0
            route.threads=[];route.stderr=b'';route.overflow=False;runtime.proxy_process=Mock(return_value=route)
            runtime.proxy_event=Mock(side_effect=lambda *args:dict(runtime.authority_proxy_binding,event='ready',port=12347))
            runtime.start_authority_proxy();self.assertIs(runtime.route,original);self.assertEqual(runtime.resources['quantum_proxy'],old)
            self.assertFalse(hasattr(runtime,'adoption_route'));self.assertFalse(hasattr(runtime,'seal_route'))
            self.assertEqual(runtime.inputs['authority_proxy']['mode'],'pass');self.assertNotIn('commit_ordinal',runtime.inputs['authority_proxy'])
            self.assertEqual(tomllib.loads(runtime.config.read_text())['postgres']['control']['endpoint'],'127.0.0.1:12347')
            terminal=dict(runtime.authority_proxy_binding,event='terminal',unexpected=0,frames=0,connections=0,bytes=0,
                **dict.fromkeys(('authority_execute_errors','authority_refusals','authority_successes','data_queries','data_commits',
                    'server_complete','client_complete','faults'),0))
            route.stdout=(json.dumps(runtime.authority_proxy_ready)+'\n'+json.dumps(terminal)+'\n').encode()
            runtime.proxy_observation=Mock(return_value=dict(stdout=route.stdout.decode(),stderr='',overflow=False))
            runtime.stop_authority_proxy();self.assertEqual(runtime.resources['authority_proxy']['status'],'joined')
            self.assertEqual(runtime.resources['cleanup_errors'],[])
            with self.assertRaises(ValueError):runtime.validate_authority_refusal_terminal(terminal)

    def test_authority_preparation_deletes_only_exact_grant_or_observes_original_pg_deadline(self):
        for profile in ('actor-grant','original-expiry'):
            with self.subTest(profile=profile),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);original,prepared,after,report,events,wires,first,dump,eligible,start,end,marker=authority_fixture(root,profile)
                runtime=object.__new__(case.CappedAdoptionRuntime);runtime.root=root;runtime.adoption_retired=original
                runtime.args=SimpleNamespace(linear_quantum_authority_refusal=profile);runtime.client=Mock()
                deadline=original['acquisition_jobs'][0]['deadline_at_us']
                grant=source.only(original['principal_grants'],action='roles.control')
                answers=[str(eligible),json.dumps([grant]),str(start)] if profile=='actor-grant' else [
                    str(eligible),str(deadline-1),str(deadline+1),str(start)]
                runtime.sql=Mock(side_effect=answers);runtime.capture=Mock(return_value=(prepared,{}))
                def waited(test,label,seconds,**kwargs):
                    self.assertEqual(label,'genuine original acquisition deadline expiry');self.assertEqual(kwargs,dict(client=runtime.client))
                    self.assertAlmostEqual(seconds,(deadline-eligible)/1_000_000+5)
                    self.assertFalse(test());self.assertTrue(test())
                runtime.wait=Mock(side_effect=waited)
                with patch.object(case.time,'monotonic',return_value=1):runtime.prepare_authority_refusal()
                self.assertEqual(runtime.authority_eligible_us,eligible);self.assertEqual(runtime.authority_prepared,prepared)
                if profile=='actor-grant':
                    runtime.wait.assert_not_called();sql=runtime.sql.call_args_list[1].args[1]
                    self.assertIn("action='roles.control' AND resource_kind='dataset'",sql)
                    self.assertIn("principal_id=decode('"+original['jobs'][0]['principal_id'][2:]+"','hex')",sql)
                    self.assertIn("resource_id=decode('"+original['jobs'][0]['dataset_id'][2:]+"','hex')",sql)
                    removed=json.loads((root/'continuation-authority-refusal/removed-grant.json').read_text())
                    self.assertEqual(removed['removed'],[grant])
                else:
                    runtime.wait.assert_called_once();witness=json.loads((root/'continuation-authority-refusal/expiry-wait.json').read_text())
                    self.assertEqual((witness['status'],witness['original_deadline_at_us'],witness['last_observed_at_us']),('PASS',deadline,deadline+1))
                    self.assertFalse(any('DELETE' in call.args[1] or 'UPDATE' in call.args[1] for call in runtime.sql.call_args_list))

    def test_authority_startup_needs_real_joined_helper_error_and_live_root_authority(self):
        for profile in ('actor-grant','original-expiry'):
            for failure in (None,'diagnostic','unexecuted','pid','elapsed'):
                with self.subTest(profile=profile,failure=failure),tempfile.TemporaryDirectory() as temporary:
                    root=Path(temporary);original,prepared,after,report,events,wires,first,dump,eligible,start,end,marker=authority_fixture(root,profile)
                    runtime=object.__new__(case.CappedAdoptionRuntime);runtime.root=root;runtime.home=root/'peer';runtime.home.mkdir()
                    runtime.state=root/'state';runtime.config=root/'instance.toml';runtime.config.write_text('[runtime]\n')
                    runtime.args=SimpleNamespace(linear_quantum_adoption_fault=None,linear_quantum_trace_successor_sql=False)
                    runtime.adoption_retired=original;runtime.authority_prepared=prepared;runtime.authority_eligible_us=eligible
                    runtime.adoption_first=first;runtime.initialized=report;runtime.events=events;runtime.authority_route=Mock()
                    document_sha256=case.digest(runtime.config)
                    def finite(*args):
                        (root/'bootstrap').mkdir();observed=root_configuration_report(document_sha256)
                        publish(root/'bootstrap','continuation-root-config.stdout',observed);return observed
                    runtime.finite=Mock(side_effect=finite)
                    directory=runtime.state/'data/ingest/producer-inputs-v1';directory.mkdir(parents=True)
                    for number,data in enumerate(first['inputs'],1):
                        (directory/f"{report['producer_slot_id']}-0000000000000001-{number:016x}.cbor").write_bytes(data)
                    runtime.adoption_original_files={'old-inputs':'exact'};runtime.files=Mock(return_value=runtime.adoption_original_files)
                    runtime.sql=Mock(side_effect=[str(start),str(end),str(end+5_000_000)])
                    runtime.launch=Mock(return_value=dict(id='ab'*32));runtime.docker=Mock(return_value=SimpleNamespace(stdout='4\n'))
                    runtime.inspect=Mock(return_value=dict(Id='ab'*32,State=dict(ExitCode=4,Pid=0,Running=False,OOMKilled=False)))
                    def logs(entry,output):
                        (output/'serve.stdout').write_text('')
                        (output/'serve.stderr').write_text('error: initial native batch unavailable: '
                            +('Quantum(CommitUnknown)' if failure=='diagnostic' else 'Control(Backend)')+'\n')
                    runtime.logs=Mock(side_effect=logs)
                    runtime.authority_proxy_binding=dict(mode='pass',target='control-acquisition-authority',frame_limit=131072,
                        connection_limit=64,wire_limit=64*1024*1024)
                    terminal=dict(runtime.authority_proxy_binding,event='terminal',unexpected=0,frames=30,connections=1,bytes=1024,
                        authority_execute_errors=int(failure!='unexecuted'),authority_refusals=int(failure!='unexecuted'),authority_successes=0,
                        data_queries=0,data_commits=0,server_complete=0,client_complete=0,faults=0)
                    observed=dict(terminal,**marker)
                    if failure=='pid':observed['backend_pid']=9999
                    runtime.proxy_event=Mock(return_value=observed)
                    def stop_proxy():
                        publish(root,'authority-proxy-terminal.json',terminal)
                        publish(root,'authority-proxy.json',dict(stdout=json.dumps(observed)))
                    runtime.stop_authority_proxy=Mock(side_effect=stop_proxy);runtime.backend_absence=Mock()
                    runtime.capture=Mock(return_value=(after,{}));runtime.wires=Mock(return_value=wires)
                    def census(*args,**kwargs):
                        runtime.stop_authority_proxy.assert_called_once();runtime.backend_absence.assert_called_once()
                        self.assertFalse((runtime.home/'continuation-authority-refusal.continue').exists());return dump
                    runtime.client=Mock(pid=12345);runtime.client.poll.return_value=None;runtime.processes=[runtime.client]
                    runtime.wait=Mock();runtime.owned_listeners=Mock(return_value=['listener']);runtime.loopback_listener=Mock()
                    (runtime.home/'client.stderr').write_text('')
                    publish(runtime.home,'client-result.json',dict(status='PASS',completion='continuation-authority-refusal',job_status=None))
                    def joined(*,timeout):
                        self.assertTrue((runtime.home/'continuation-authority-refusal.continue').exists())
                        self.assertTrue((root/'continuation-authority-refusal/original-duration.json').exists());return 0
                    runtime.client.wait.side_effect=joined
                    with patch.object(case.native,'census',side_effect=census),patch.object(case,'stop_process'),\
                            patch.object(case.time,'CLOCK_BOOTTIME',7,create=True),patch.object(case.time,'clock_gettime_ns',
                                side_effect=[1_000_000_000,6_000_000_000 if failure=='elapsed' else 2_000_000_000]):
                        if failure is None:runtime.failed_adoption(authority=profile)
                        else:
                            with self.assertRaises((AssertionError,ValueError)):runtime.failed_adoption(authority=profile)
                    runtime.finite.assert_called_once_with('continuation-root-config','config','show','--file','/etc/bigbrotr/instance.toml')
                    runtime.wait.assert_not_called()
                    if failure is None:runtime.client.wait.assert_called_once_with(timeout=6)
                    else:runtime.client.wait.assert_not_called()

    def test_continuation_authority_discriminates_only_the_declared_grant_or_original_expiry(self):
        for profile in ('actor-grant','original-expiry'):
            with self.subTest(profile=profile),tempfile.TemporaryDirectory() as temporary:
                original,prepared,after,report,events,wires,first,dump,eligible,start,end,observed=authority_fixture(Path(temporary),profile)
                def verify(state=after,marker=observed,begun=start):
                    check.continuation_authority_refusal(original,prepared,state,first,wires,events,dump,eligible,begun,end,end+5_000_000,profile,marker)
                verify();check.adoption_progress(after,report,events,wires,first,end+5_000_000,epoch=1)
                for key,value in (('sqlstate','23514'),('reason','other guard'),('backend_pid',9999),('role','postgres'),
                        ('database','other'),('executed',False),('transaction','I')):
                    with self.subTest(marker=key),self.assertRaises(AssertionError):verify(marker=dict(observed,**{key:value}))
                for table,index,key,value in (('local_accounts',0,'active',False),('jobs',0,'job_revision',3),
                        ('acquisition_jobs',0,'deadline_at_us',original['acquisition_jobs'][0]['deadline_at_us']+1),
                        ('acquisition_continuations',0,'xmin','99999'),('producer_epochs',1,'reserved_through_request',1),
                        ('producer_outcomes',0,'xmin','99999'),('config_revisions',0,'document_digest','\\x'+'ff'*32),
                        ('owner_epochs',4,'lease_expires_at_us',end),('owner_epochs',5,'lease_expires_at_us',end)):
                    changed=copy.deepcopy(after);changed[table][index][key]=value
                    with self.subTest(table=table,key=key),self.assertRaises((AssertionError,ValueError)):verify(changed)
                changed=copy.deepcopy(prepared)
                changed['principal_grants']=[]
                with self.assertRaises(AssertionError):check.continuation_authority_setup(original,changed,profile,eligible,start)
                deadline=original['acquisition_jobs'][0]['deadline_at_us']
                with self.assertRaises(AssertionError):
                    check.continuation_authority_setup(original,prepared,profile,eligible,deadline if profile=='actor-grant' else deadline-1)
                for table in ('local_accounts','policy_revisions','source_streams','acquisition_jobs'):
                    changed=copy.deepcopy(prepared);changed[table][0]['xmin']='99999'
                    with self.subTest(prepared=table),self.assertRaises(AssertionError):
                        check.continuation_authority_setup(original,changed,profile,eligible,start)
                changed=copy.deepcopy(after);changed['acquisition_continuations'][0]['target_epoch']=2
                with self.assertRaises((AssertionError,ValueError)):verify(changed)
                with self.assertRaises(AssertionError):
                    check.continuation_authority_refusal(original,prepared,after,first,wires,events,dict(dump,sequence=92),eligible,start,end,end+5_000_000,profile,observed)
                check.continuation_authority_error('','error: initial native batch unavailable: Control(Backend)\n')
                with self.assertRaises(AssertionError):check.continuation_authority_error('','error: initial native batch unavailable: Quantum(CommitUnknown)\n')

    def test_adoption_proxy_has_separate_route_file_binding_and_clean_unreached_cleanup(self):
        from installed import set_value
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=object.__new__(case.CappedAdoptionRuntime)
            runtime.root=root;runtime.source=ROOT;runtime.config=root/'instance.toml'
            runtime.config.write_text('[postgres.control]\nendpoint = "127.0.0.1:15432"\n');runtime.set_value=set_value
            runtime.args=SimpleNamespace(linear_quantum_adoption_fault='before-forward');runtime.quantum_fault='plan-before'
            original=Mock();runtime.route=original;old=dict(status='joined',pid=42,port=9999)
            runtime.resources=dict(quantum_proxy=old.copy(),cleanup_errors=[]);runtime.inputs=dict(quantum_proxy=dict(original=True))
            runtime.record_resources=Mock();route=Mock();route.process.pid=43;route.process.poll.return_value=0
            route.wait.return_value=0;route.threads=[];route.stderr=b'';route.overflow=False
            runtime.proxy_process=Mock(return_value=route)
            runtime.proxy_event=Mock(side_effect=lambda *args:dict(runtime.adoption_proxy_binding,event='ready',port=12346))
            runtime.start_adoption_proxy()
            self.assertIs(runtime.route,original);self.assertEqual(runtime.resources['quantum_proxy'],old)
            self.assertEqual(runtime.inputs['quantum_proxy'],dict(original=True));self.assertFalse(hasattr(runtime,'seal_route'))
            self.assertEqual(runtime.resources['adoption_proxy']['pid'],43)
            self.assertEqual(tomllib.loads(runtime.config.read_text())['postgres']['control']['endpoint'],'127.0.0.1:12346')
            self.assertEqual(runtime.inputs['initial_recipe_sha256'],case.digest(runtime.config))
            terminal=dict(runtime.adoption_proxy_binding,event='terminal',unexpected=0,frames=0,connections=0,bytes=0,
                **dict.fromkeys(('adoption_cas_complete','data_queries','data_commits','matched_commits','selected_commits','skipped_commits',
                    'selected_server_complete','selected_client_complete','server_complete','client_complete','faults'),0))
            route.stdout=(json.dumps(runtime.adoption_proxy_ready)+'\n'+json.dumps(terminal)+'\n').encode()
            runtime.proxy_observation=Mock(return_value=dict(stdout=route.stdout.decode(),stderr='',overflow=False))
            runtime.stop_adoption_proxy()
            self.assertEqual(runtime.resources['adoption_proxy']['status'],'joined');self.assertEqual(runtime.resources['cleanup_errors'],[])
            with self.assertRaises(ValueError):runtime.validate_adoption_fault_terminal(terminal)
            self.assertTrue((root/'adoption-proxy-process.json').exists());self.assertTrue((root/'adoption-proxy-terminal.json').exists())

    def test_adoption_fault_requires_confirmed_prefix_exact_activation_and_atomic_job_seal_cut(self):
        for mode in ('before-forward','after-complete'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as temporary:
                before,sealed,retired,after,report,events,wires,first,dump,start,end,fault=adoption_fault_fixture(Path(temporary),mode)
                check.adoption_recovery_prefix(before,sealed,retired,first)
                check.adoption_fault(retired,after,first,wires,events,dump,start,end,end+5_000_000,mode,fault)
                check.adoption_progress(after,report,events,wires,first,end+5_000_000,epoch=1)
                reordered=lambda state:{table:[dict(reversed(list(row.items()))) for row in reversed(rows)]
                    for table,rows in reversed(list(state.items()))}
                check.adoption_recovery_prefix(before,reordered(sealed),reordered(retired),first)
                check.adoption_fault(retired,reordered(after),first,wires,events,dump,start,end,end+5_000_000,mode,fault)
                duplicate=copy.deepcopy(after);duplicate['producer_outcomes'].append(copy.deepcopy(duplicate['producer_outcomes'][0]))
                with self.assertRaises((AssertionError,ValueError)):
                    check.adoption_fault(retired,duplicate,first,wires,events,dump,start,end,end+5_000_000,mode,fault)
                mutations=[('jobs',0,'job_revision',4),('acquisition_continuations',0,'sealed_at_us',start),
                    ('producer_epochs',1,'reserved_through_request',1),('producer_epochs',1,'state','retired'),
                    ('source_streams',1,'last_complete_sequence',0),('allocation_domains',9,'xmin','999'),
                    ('initial_ingest_successions',0,'issued_through',4),('initial_ingest_successions',0,'prepared_by_epoch',2),
                    ('initial_ingest_successions',0,'outstanding_request_limit',1),('initial_ingest_activations',0,'activation_transaction','131'),
                    ('initial_ingest_activations',0,'target_incarnation',retired['owner_epochs'][0]['process_incarnation']),
                    ('topology_streams',1,'handoff_last_complete_sequence',18),('topology_generations',1,'sealed',1),
                    ('owner_epochs',4,'state','released'),('owner_epochs',4,'control_backend_pid',9999),
                    ('owner_epochs',4,'xmin',retired['jobs'][0]['xmin']),('role_instances',4,'xmin',retired['jobs'][0]['xmin']),
                    ('owner_epochs',5,'lease_expires_at_us',end),('role_instances',5,'retired_at_us',end),
                    ('datasets',0,'topology_generation',2)]
                if mode=='after-complete':
                    mutations += [('acquisition_continuations',0,key,None) for key in check.ADOPTION_FIELDS]
                    mutations += [('acquisition_continuations',0,key,value) for key,value in (
                        ('target_manifest_sha256','\\x'+'ff'*32),('activation_sha256','\\x'+'ff'*32),('adopting_epoch',2),
                        ('adoption_transaction','135'),('adopted_at_us',start),('xmin','135'),('sealing_transaction','imaginary'))]
                for table,index,key,value in mutations:
                    changed=copy.deepcopy(after);changed[table][index][key]=value
                    with self.subTest(table=table,key=key),self.assertRaises((AssertionError,ValueError)):
                        check.adoption_fault(retired,changed,first,wires,events,dump,start,end,end+5_000_000,mode,fault)
                # Every preexisting captured table is conserved, including xmins.
                for table,rows in retired.items():
                    if not rows:continue
                    changed=copy.deepcopy(after);changed[table][0]['xmin']='9999'
                    with self.subTest(old_table=table),self.assertRaises((AssertionError,ValueError)):
                        check.adoption_fault(retired,changed,first,wires,events,dump,start,end,end+5_000_000,mode,fault)
                for broken in (dict(dump,sequence=86),dict(dump,sequence=92),dict(dump,rows=dump['rows'][:-1])):
                    with self.assertRaises((AssertionError,ValueError)):check.adoption_fault(retired,after,first,wires,events,broken,start,end,end+5_000_000,mode,fault)
                with self.assertRaises((AssertionError,ValueError)):check.adoption_fault(retired,after,first,wires*2,events,dump,start,end,end+5_000_000,mode,fault)
                with self.assertRaises((AssertionError,ValueError)):check.adoption_fault(retired,after,first,wires,events,dump,start,end,end+5_000_000,mode,dict(backend_pid=9999))
                half=copy.deepcopy(after)
                if mode=='after-complete':half['jobs']=copy.deepcopy(retired['jobs'])
                else:half['jobs'][0].update(job_revision=3,owner_epoch=4,xmin='134')
                with self.assertRaises((AssertionError,ValueError)):check.adoption_fault(retired,half,first,wires,events,dump,start,end,end+5_000_000,mode,fault)
                for table in ('producer_requests','producer_outcomes','producer_checkpoints','acquisition_quanta'):
                    extra=copy.deepcopy(after);extra[table].append(dict(extra[table][0],producer_epoch=2))
                    with self.subTest(extra_epoch2=table),self.assertRaises((AssertionError,ValueError)):
                        check.adoption_fault(retired,extra,first,wires,events,dump,start,end,end+5_000_000,mode,fault)
                for table in ('owner_epochs','role_instances','source_streams','allocation_domains','jobs','acquisition_continuations'):
                    altered=copy.deepcopy(retired);altered[table][0]['xmin']='99999'
                    with self.subTest(prefix_table=table),self.assertRaises((AssertionError,ValueError)):check.adoption_recovery_prefix(before,sealed,altered,first)
                check.adoption_fault_error('','error: initial native batch unavailable: Quantum(CommitUnknown)\n')
                for stdout,stderr in (('{}','error: initial native batch unavailable: Quantum(CommitUnknown)\n'),
                        ('','error: producer operation unavailable: CommitUnknown\n'),
                        ('','error: initial native batch unavailable: Quantum(CommitUnknown)\nextra')):
                    with self.assertRaises((AssertionError,ValueError)):check.adoption_fault_error(stdout,stderr)

    def test_adoption_fault_cli_refuses_other_schedules_and_competing_fault_or_trace(self):
        spec=importlib.util.spec_from_file_location('adoption_fault_runner_control',ROOT/'tests/acceptance/run.py')
        runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        common=['run.py','p03-linear-quantum','--linear-quantum-scenario','settled-owner-adoption','--linear-quantum-fault','plan-before']
        for mode in ('before-forward','after-complete'):
            with tempfile.TemporaryDirectory() as temporary:
                tail=['--source-root',str(ROOT),'--evidence',str(Path(temporary)/'evidence'),'--linear-quantum-adoption-fault',mode]
                with patch.object(sys,'argv',common+tail),patch.object(sys,'stdout'),patch('cases.first_launch.first_launch') as launched:
                    self.assertEqual(runner.main(),2)
                self.assertEqual(launched.call_args.args[0].linear_quantum_adoption_fault,mode)
        for prefix in (['run.py','p01-t01','--binary',sys.executable],['run.py','p03-linear-quantum'],
                common[:-1]+['plan-after'],common+['--linear-quantum-trace-successor-sql'],
                common+['--linear-quantum-seal-fault','before-forward']):
            with tempfile.TemporaryDirectory() as temporary:
                evidence=Path(temporary)/'evidence'
                with patch.object(sys,'argv',prefix+['--source-root',str(ROOT),'--evidence',str(evidence),
                        '--linear-quantum-adoption-fault','before-forward']),patch.object(sys,'stderr'),self.assertRaises(SystemExit) as raised:
                    runner.main()
                self.assertEqual(raised.exception.code,2);self.assertFalse(evidence.exists())

    def test_failed_adoption_startup_retains_exact_error_clocks_and_custody_before_peer_release(self):
        for mode in ('before-forward','after-complete'):
            for failure in (None,'diagnostic','exit','elapsed','file','native','extra-input','pid','proxy'):
                with self.subTest(mode=mode,failure=failure),tempfile.TemporaryDirectory() as temporary:
                    root=Path(temporary);before,sealed,retired,after,report,events,wires,first,dump,start,end,fault=adoption_fault_fixture(root,mode)
                    runtime=object.__new__(case.CappedAdoptionRuntime);runtime.root=root;runtime.home=root/'peer';runtime.home.mkdir()
                    runtime.state=root/'state';runtime.config=root/'instance.toml';runtime.config.write_text('[runtime]\n')
                    runtime.args=SimpleNamespace(linear_quantum_adoption_fault=mode,linear_quantum_trace_successor_sql=False)
                    document_sha256=case.digest(runtime.config);runtime.adoption_retired=retired;runtime.adoption_first=first
                    runtime.initialized=report;runtime.events=events;runtime.adoption_route=Mock()
                    def finite(*args):
                        (root/'bootstrap').mkdir();observed=root_configuration_report(document_sha256)
                        publish(root/'bootstrap','adoption-root-config.stdout',observed);return observed
                    runtime.finite=Mock(side_effect=finite)
                    directory=runtime.state/'data/ingest/producer-inputs-v1';directory.mkdir(parents=True)
                    for number,data in enumerate(first['inputs'],1):
                        (directory/f"{report['producer_slot_id']}-0000000000000001-{number:016x}.cbor").write_bytes(data)
                    runtime.adoption_original_files={'old-inputs':'exact'}
                    runtime.files=Mock(return_value={'extra-epoch2-input':'unexpected'} if failure=='extra-input' else runtime.adoption_original_files)
                    runtime.sql=Mock(side_effect=[str(start),str(end),str(end+10_000_000)])
                    runtime.launch=Mock(return_value=dict(id='ab'*32))
                    def waited(*args,**kwargs):
                        if failure=='file':runtime.config.write_text('[runtime]\n# changed during startup\n')
                        return SimpleNamespace(stdout='4\n')
                    runtime.docker=Mock(side_effect=waited)
                    runtime.inspect=Mock(return_value=dict(Id='ab'*32,State=dict(ExitCode=0 if failure=='exit' else 4,Pid=0,Running=False,OOMKilled=False)))
                    def logs(entry,output):
                        (output/'serve.stdout').write_text('')
                        (output/'serve.stderr').write_text('error: initial native batch unavailable: Quantum('
                            +('DeliveryUnknown' if failure=='diagnostic' else 'CommitUnknown')+')\n')
                    runtime.logs=Mock(side_effect=logs)
                    runtime.adoption_proxy_binding=dict(mode=mode,target='control-acquisition-adoption',frame_limit=131072,
                        connection_limit=64,wire_limit=64*1024*1024)
                    terminal=dict(runtime.adoption_proxy_binding,event='terminal',unexpected=0,frames=30,connections=1,bytes=1024,
                        adoption_cas_complete=1,data_queries=1,data_commits=1,matched_commits=1,selected_commits=1,skipped_commits=0,
                        selected_server_complete=int(mode=='after-complete'),selected_client_complete=0,
                        server_complete=int(mode=='after-complete'),client_complete=0,faults=int(failure!='proxy'))
                    selected=dict(terminal,event='fault',backend_pid=9999 if failure=='pid' else fault['backend_pid'])
                    runtime.proxy_event=Mock(return_value=selected)
                    def stop_proxy():
                        publish(root,'adoption-proxy-terminal.json',terminal)
                        publish(root,'adoption-proxy.json',dict(stdout='\n'.join(json.dumps(value) for value in (
                            dict(event='adoption-cas-complete',backend_pid=selected['backend_pid']),selected))))
                    runtime.stop_adoption_proxy=Mock(side_effect=stop_proxy)
                    runtime.backend_absence=Mock();runtime.capture=Mock(return_value=(after,{}));runtime.wires=Mock(return_value=wires)
                    if failure=='native':dump['sequence']=92
                    def census(*args,**kwargs):
                        runtime.stop_adoption_proxy.assert_called_once();runtime.backend_absence.assert_called_once()
                        self.assertFalse((runtime.home/'adoption-commit-fault.continue').exists());return dump
                    runtime.client=Mock(pid=12345);runtime.client.poll.return_value=None;runtime.processes=[runtime.client]
                    runtime.wait=Mock();runtime.owned_listeners=Mock(return_value=['listener']);runtime.loopback_listener=Mock()
                    (runtime.home/'client.stderr').write_text('')
                    publish(runtime.home,'client-result.json',dict(status='PASS',completion='adoption-commit-fault',job_status=None))
                    def joined(*,timeout):
                        self.assertTrue((runtime.home/'adoption-commit-fault.continue').exists())
                        self.assertTrue((root/'adoption-commit-fault/original-duration.json').exists())
                        runtime.capture.assert_called_once();runtime.loopback_listener.assert_called_once();return 0
                    runtime.client.wait.side_effect=joined
                    with patch.object(case.native,'census',side_effect=census),patch.object(case,'stop_process'),\
                            patch.object(case.time,'CLOCK_BOOTTIME',7,create=True),patch.object(case.time,'clock_gettime_ns',
                                side_effect=[1_000_000_000,6_000_000_000 if failure=='elapsed' else 2_000_000_000]):
                        if failure is None:runtime.failed_adoption()
                        else:
                            with self.assertRaises((AssertionError,ValueError)):runtime.failed_adoption()
                    runtime.finite.assert_called_once_with('adoption-root-config','config','show','--file','/etc/bigbrotr/instance.toml')
                    runtime.launch.assert_called_once_with('adoption-successor','app','serve','--file','/etc/bigbrotr/instance.toml',
                        '--activate-initial-successor','--roles','control,ingest,sync')
                    runtime.wait.assert_not_called()
                    if failure is None:
                        self.assertEqual(runtime.processes,[]);runtime.client.wait.assert_called_once_with(timeout=6)
                    else:
                        runtime.client.wait.assert_not_called();self.assertFalse((runtime.home/'adoption-commit-fault.continue').exists())
    def test_seal_fault_parser_accepts_only_its_explicit_negative_schedule(self):
        spec=importlib.util.spec_from_file_location('seal_fault_runner_control',ROOT/'tests/acceptance/run.py')
        runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        common=['run.py','p03-linear-quantum','--linear-quantum-scenario','settled-owner-adoption','--linear-quantum-fault','plan-before']
        for mode in ('before-forward','after-complete'):
            with tempfile.TemporaryDirectory() as temporary:
                tail=['--source-root',str(ROOT),'--evidence',str(Path(temporary)/'evidence'),'--linear-quantum-seal-fault',mode]
                with patch.object(sys,'argv',common+tail),patch.object(sys,'stdout'),patch('cases.first_launch.first_launch') as launched:
                    self.assertEqual(runner.main(),2)
                self.assertEqual(launched.call_args.args[0].linear_quantum_seal_fault,mode)
        for prefix in (['run.py','p01-t01','--binary',sys.executable],['run.py','p03-linear-quantum'],
                ['run.py','p03-linear-quantum','--linear-quantum-scenario','capped-continuation'],
                common[:-1]+['plan-after'],common+['--linear-quantum-trace-successor-sql']):
            with tempfile.TemporaryDirectory() as temporary:
                evidence=Path(temporary)/'evidence'
                with patch.object(sys,'argv',prefix+['--source-root',str(ROOT),'--evidence',str(evidence),
                        '--linear-quantum-seal-fault','before-forward']),patch.object(sys,'stderr'),self.assertRaises(SystemExit) as raised:
                    runner.main()
                self.assertEqual(raised.exception.code,2);self.assertFalse(evidence.exists())

    def test_seal_proxy_keeps_original_proxy_custody_and_binds_rewritten_root_before_show(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);runtime=object.__new__(case.CappedAdoptionRuntime)
            runtime.root=root;runtime.source=ROOT;runtime.config=root/'instance.toml'
            from installed import set_value
            runtime.set_value=set_value
            runtime.config.write_text('[postgres.control]\nendpoint = "127.0.0.1:15432"\n')
            runtime.args=SimpleNamespace(linear_quantum_seal_fault='after-complete');runtime.quantum_fault='plan-before'
            original=Mock();runtime.route=original;old=dict(status='joined',pid=42,port=9999)
            runtime.resources=dict(quantum_proxy=old.copy());runtime.inputs=dict(quantum_proxy=dict(original=True))
            runtime.record_resources=Mock();route=Mock();route.process.pid=43
            runtime.proxy_process=Mock(return_value=route)
            def ready(selected,event,timeout):
                self.assertIs(selected,route);self.assertEqual((event,timeout),('ready',10))
                return dict(runtime.seal_proxy_binding,event='ready',port=12345)
            runtime.proxy_event=Mock(side_effect=ready)
            runtime.start_seal_proxy()
            self.assertIs(runtime.route,original);self.assertEqual(runtime.resources['quantum_proxy'],old)
            self.assertEqual(runtime.inputs['quantum_proxy'],dict(original=True))
            self.assertEqual(runtime.resources['seal_proxy']['pid'],43)
            self.assertEqual(tomllib.loads(runtime.config.read_text())['postgres']['control']['endpoint'],'127.0.0.1:12345')
            self.assertEqual(runtime.inputs['initial_recipe_sha256'],case.digest(runtime.config))
            command=runtime.proxy_process.call_args.args[0]
            self.assertEqual(command[command.index('--target')+1],'control-acquisition-seal')
            self.assertEqual(command[command.index('--profile')+1],'retained-catch-up')
            terminal=dict(runtime.seal_proxy_binding,event='terminal',unexpected=0,frames=30,connections=1,bytes=1024,
                seal_inserts_complete=1,data_queries=1,data_commits=1,matched_commits=1,selected_commits=1,
                skipped_commits=0,selected_server_complete=1,selected_client_complete=0,server_complete=1,client_complete=0,faults=1)
            runtime.validate_seal_fault_terminal(terminal)
            for key,value in (('source_sha256','ff'*32),('lifetime_seconds',180),('wire_limit',128*1024*1024),
                    ('target','control-quantum-plan'),('seal_inserts_complete',0),('selected_server_complete',0),
                    ('selected_client_complete',1),('faults',0),('unexpected',1),('matched_commits',2)):
                with self.subTest(key=key),self.assertRaises(ValueError):runtime.validate_seal_fault_terminal(dict(terminal,**{key:value}))
            runtime.stop_owned_proxy=Mock();runtime.stop_seal_proxy()
            runtime.stop_owned_proxy.assert_called_once_with(route,'seal_proxy','seal-proxy',runtime.seal_proxy_ready,runtime.validate_seal_proxy)

    def test_unreached_seal_target_can_join_cleanly_without_claiming_a_fault_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime=object.__new__(case.CappedAdoptionRuntime);runtime.root=Path(temporary)
            runtime.seal_proxy_binding=dict(mode='before-forward',target='control-acquisition-seal',frame_limit=131072,
                connection_limit=64,wire_limit=64*1024*1024)
            ready=dict(runtime.seal_proxy_binding,event='ready',port=12345)
            terminal=dict(runtime.seal_proxy_binding,event='terminal',unexpected=0,frames=0,connections=0,bytes=0,
                **dict.fromkeys(('seal_inserts_complete','data_queries','data_commits','matched_commits','selected_commits',
                    'skipped_commits','selected_server_complete','selected_client_complete','server_complete','client_complete','faults'),0))
            runtime.seal_proxy_ready=ready;route=Mock();runtime.seal_route=route
            route.process.poll.return_value=0;route.process.pid=123;route.wait.return_value=0
            route.threads=[];route.stdout=(json.dumps(ready)+'\n'+json.dumps(terminal)+'\n').encode()
            route.stderr=b'';route.overflow=False
            runtime.resources=dict(seal_proxy=dict(status='running'),cleanup_errors=[]);runtime.record_resources=Mock()
            runtime.proxy_observation=Mock(return_value=dict(stdout=route.stdout.decode(),stderr='',overflow=False))
            runtime.stop_seal_proxy()
            self.assertEqual(runtime.resources['seal_proxy'],dict(status='joined',exit_code=0))
            self.assertEqual(runtime.resources['cleanup_errors'],[]);route.stop.assert_called_once()
            with self.assertRaisesRegex(ValueError,'lacks framed Control traffic'):runtime.validate_seal_fault_terminal(terminal)

    def test_seal_commit_unknown_requires_exact_zero_or_one_unadopted_seal_and_conserves_every_old_row(self):
        for mode in ('before-forward','after-complete'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as temporary:
                before,after,report,events,wires,first,_,closed,start,end=seal_fault_fixture(Path(temporary),mode)
                fault=dict(backend_pid=7331)
                def verify(state=after,selected=fault):
                    check.adoption_seal_fault(before,state,first,wires,events,start,end,end+5_000_000,mode,selected)
                verify();check.adoption_closed_native(first,'\\x'+report['producer_slot_id'],closed)
                mutations=[('jobs',0,'job_revision',3),('jobs',0,'xmin','new'),('acquisition_jobs',0,'deadline_at_us',ACCEPTED+121_000_000),
                    ('acquisition_quanta',0,'xmin','new'),('producer_requests',0,'xmin','new'),('producer_outcomes',0,'outcome_kind',2),
                    ('producer_epochs',0,'state','retired'),('source_streams',0,'last_complete_sequence',18),
                    ('owner_epochs',2,'state','released'),('owner_epochs',2,'control_backend_pid',7332)]
                if mode=='after-complete':
                    mutations += [('acquisition_continuations',0,key,value) for key,value in (
                        ('previous_job_revision',3),('previous_continuation','\\x'+'ff'*16),('previous_owner_epoch',2),
                        ('scope_id','\\x'+'ff'*16),('enrollment_generation',2),('route_id','\\x'+'ff'*16),
                        ('producer_epoch',2),('source_sequence',18),('source_digest','\\x'+'ff'*32),('checkpoint_revision',6),
                        ('complete_through_request',4),('last_quantum_number',2),('manifest_sha256','\\x'+'ff'*32),
                        ('checkpoint_sha256','\\x'+'ff'*32),('shard_id',1),('sealing_epoch',1),('sealed_at_us',start),
                        ('sealing_incarnation','\\x'+'ff'*16),('continuation_id','\\x00'),('sealing_transaction','imaginary'))]
                    mutations += [('acquisition_continuations',0,key,1) for key in check.ADOPTION_FIELDS]
                for table,index,key,value in mutations:
                    state=copy.deepcopy(after);state[table][index][key]=value
                    with self.subTest(table=table,key=key),self.assertRaises(AssertionError):verify(state)
                state=copy.deepcopy(after)
                state['acquisition_continuations']=[] if mode=='after-complete' else [dict(unexpected=True)]
                with self.assertRaises(AssertionError):verify(state)
                with self.assertRaises(AssertionError):verify(selected=dict(backend_pid=7332))
                check.adoption_seal_fault_error('','error: producer operation unavailable: CommitUnknown\n')
                for stdout,stderr in (('report','error: producer operation unavailable: CommitUnknown\n'),
                        ('','error: producer operation unavailable: Control(InvalidMetadata)\n'),
                        ('','error: producer operation unavailable: CommitUnknown\nextra')):
                    with self.assertRaises(AssertionError):check.adoption_seal_fault_error(stdout,stderr)

    def test_seal_fault_command_joins_proxy_and_backend_then_checks_native_and_state_before_peer_release(self):
        for mode in ('before-forward','after-complete'):
            for failure,retry in ((None,False),(None,True),('retry',True),('diagnostic',False),('counter',False),('backend',False),('native',False),('authorization',False)):
                with self.subTest(mode=mode,failure=failure,retry=retry),tempfile.TemporaryDirectory() as temporary:
                    root=Path(temporary);before,after,report,events,wires,first,_,closed,start,end=seal_fault_fixture(root,mode)
                    runtime=object.__new__(case.CappedAdoptionRuntime);runtime.root=root;runtime.home=root/'peer';runtime.home.mkdir()
                    runtime.state=root/'state';runtime.config=root/'instance.toml';runtime.config.write_text('[runtime]\n')
                    document_sha256=case.digest(runtime.config);runtime.adoption_before=before;runtime.adoption_first=first
                    runtime.initialized=report;runtime.events=events;runtime.seal_route=Mock();runtime.record_resources=Mock()
                    runtime.args=SimpleNamespace(linear_quantum_seal_retry='exact' if retry else None)
                    def finite(*args):
                        (root/'bootstrap').mkdir();observed=root_configuration_report(document_sha256)
                        publish(root/'bootstrap','seal-root-config.stdout',observed);return observed
                    runtime.finite=Mock(side_effect=finite)
                    directory=runtime.state/'data/ingest/producer-inputs-v1';directory.mkdir(parents=True)
                    for number,data in enumerate(first['inputs'],1):
                        (directory/f"{report['producer_slot_id']}-0000000000000001-{number:016x}.cbor").write_bytes(data)
                    runtime.adoption_original_files={'old-inputs':'exact'};runtime.files=Mock(return_value=runtime.adoption_original_files)
                    runtime.sql=Mock(side_effect=[str(start),str(end),str(end+5_000_000)])
                    runtime.launch=Mock(return_value=dict(id='ab'*32));runtime.docker=Mock(return_value=SimpleNamespace(stdout='4\n'))
                    runtime.inspect=Mock(return_value=dict(Id='ab'*32,State=dict(ExitCode=4,Pid=0,Running=False,OOMKilled=False)))
                    def logs(entry,output):
                        (output/'serve.stdout').write_text('')
                        (output/'serve.stderr').write_text('error: producer operation unavailable: '
                            +('Control(InvalidMetadata)' if failure=='diagnostic' else 'CommitUnknown')+'\n')
                    runtime.logs=Mock(side_effect=logs)
                    runtime.seal_proxy_binding=dict(mode=mode,target='control-acquisition-seal',frame_limit=131072,
                        connection_limit=64,wire_limit=64*1024*1024)
                    terminal=dict(runtime.seal_proxy_binding,event='terminal',unexpected=0,frames=30,connections=1,bytes=1024,
                        seal_inserts_complete=1,data_queries=1,data_commits=1,matched_commits=1,selected_commits=1,skipped_commits=0,
                        selected_server_complete=int(mode=='after-complete'),selected_client_complete=0,
                        server_complete=int(mode=='after-complete'),client_complete=0,faults=int(failure!='counter'))
                    selected=dict(terminal,event='fault',backend_pid=7332 if failure=='backend' else 7331)
                    runtime.proxy_event=Mock(return_value=selected)
                    def stop_proxy():
                        runtime.validate_seal_proxy(terminal,'terminal')
                        publish(root,'seal-proxy-terminal.json',terminal)
                        publish(root,'seal-proxy.json',dict(stdout='\n'.join(json.dumps(value) for value in (
                            dict(event='seal-insert-complete',backend_pid=selected['backend_pid']),selected))))
                    runtime.stop_seal_proxy=Mock(side_effect=stop_proxy)
                    runtime.backend_absence=Mock();runtime.capture=Mock(return_value=(after,{}));runtime.wires=Mock(return_value=wires)
                    if failure=='native':closed['sequence']=87
                    def census(*args,**kwargs):
                        runtime.stop_seal_proxy.assert_called_once();runtime.backend_absence.assert_called_once()
                        self.assertFalse((runtime.home/'seal-commit-fault.continue').exists())
                        return closed
                    runtime.client=Mock(pid=12345);runtime.client.poll.return_value=None;runtime.processes=[runtime.client]
                    runtime.owned_listeners=Mock(return_value=['listener']);runtime.loopback_listener=Mock()
                    (runtime.home/'client.stderr').write_text('')
                    completion='seal-commit-retry' if retry else 'seal-commit-fault'
                    publish(runtime.home,'client-result.json',dict(status='PASS',completion=completion,job_status=None,
                        **(dict(seal_retry='exact',gate_seconds=110) if retry else {})))
                    def retry_once(selected):
                        self.assertEqual(selected,mode)
                        self.assertTrue((root/'seal-commit-fault/original-duration.json').exists())
                        self.assertFalse((runtime.home/'seal-commit-fault.continue').exists())
                        self.assertFalse((runtime.home/'seal-commit-retry.continue').exists())
                        self.assertEqual(runtime.adoption_refusal_dump,closed)
                        runtime.capture.assert_called_once();runtime.stop_seal_proxy.assert_called_once()
                        if failure=='retry':raise ValueError('retry proof refused')
                    runtime.retry_seal=Mock(side_effect=retry_once)
                    def joined(*,timeout):
                        self.assertTrue((runtime.home/(completion+'.continue')).exists())
                        self.assertTrue((root/'seal-commit-fault/original-duration.json').exists())
                        runtime.capture.assert_called_once();runtime.loopback_listener.assert_called_once()
                        if retry:runtime.retry_seal.assert_called_once_with(mode)
                        return 0
                    runtime.client.wait.side_effect=joined
                    with patch.object(case.native,'census',side_effect=census),patch.object(case,'stop_process'),\
                            patch.object(case.time,'CLOCK_BOOTTIME',7,create=True),patch.object(case.time,'clock_gettime_ns',
                                side_effect=[1_000_000_000,6_000_000_000 if failure=='authorization' else 2_000_000_000]):
                        if failure is None:runtime.failed_seal(mode)
                        else:
                            with self.assertRaises((AssertionError,ValueError)):runtime.failed_seal(mode)
                    runtime.finite.assert_called_once_with('seal-root-config','config','show','--file','/etc/bigbrotr/instance.toml')
                    runtime.launch.assert_called_once_with('fault-seal-commit','app','ingest','recover','--file',
                        '/etc/bigbrotr/instance.toml','--seal-acquisition')
                    if failure is None:
                        self.assertEqual(runtime.processes,[]);runtime.client.wait.assert_called_once_with(timeout=6)
                    else:
                        runtime.client.wait.assert_not_called();self.assertFalse((runtime.home/(completion+'.continue')).exists())
                    if not retry:runtime.retry_seal.assert_not_called()

    def test_root_authorization_requires_actual_show_types_file_and_nonroot_policy_binding(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);state,*_=unsettled_fixture(root)
            config=root/'root.toml';config.write_text('[runtime]\n')
            self.assertNotIn('authorization_ttl',tomllib.loads(config.read_text())['runtime'])
            for kind in ('config','policy'):self.assertNotIn('runtime.authorization_ttl',state[kind+'_revisions'][0]['document']['settings'])
            for literal,value in ((None,'5000000'),('1us','1'),('7s','7000000')):
                config.write_text('[runtime]\n'+('' if literal is None else 'authorization_ttl="'+literal+'"\n'))
                document_sha256=case.digest(config)
                observed=root_configuration_report(document_sha256,value)
                if literal is not None:observed['settings']['runtime.authorization_ttl']['source']='file'
                self.assertEqual(check.adoption_root_authorization(observed,document_sha256,state),int(value))
            config.write_text('[runtime]\n');document_sha256=case.digest(config)
            for fault in ('number','boolean','zero','leading-zero','fraction','overflow','source','locked','reload','unselected',
                    'dependent','offset','file','seed','missing','unknown','validate-only','policy-root','accepted-digest'):
                observed=root_configuration_report(document_sha256);selected=observed['settings']['runtime.authorization_ttl']
                accepted=copy.deepcopy(state)
                if fault in ('number','boolean','zero','leading-zero','fraction','overflow'):
                    selected['value']={'number':5_000_000,'boolean':True,'zero':'0','leading-zero':'05000000',
                        'fraction':'5000000.0','overflow':str(2**64)}[fault]
                elif fault=='source':selected['source']='accepted-policy'
                elif fault=='locked':selected['deployment_locked']=False
                elif fault=='reload':selected['reload']='restart'
                elif fault=='unselected':selected['applicability']='staged-unselected'
                elif fault=='dependent':selected['default_from']='another.key'
                elif fault=='offset':selected['default_add_us']=0
                elif fault=='file':observed['document_sha256']='cd'*32
                elif fault=='seed':observed['initial_policy']['sha256']='cd'*32
                elif fault=='missing':del selected['value']
                elif fault=='unknown':selected['extra']=True
                elif fault=='validate-only':del observed['settings']
                elif fault=='policy-root':accepted['policy_revisions'][0]['document']['settings']['runtime.authorization_ttl']=5_000_000
                else:accepted['config_revisions'][0]['document_digest']='\\x'+'cd'*32
                with self.subTest(fault=fault),self.assertRaises((AssertionError,KeyError,TypeError,ValueError)):
                    check.adoption_root_authorization(observed,document_sha256,accepted)

    def test_unsettled_plan_refusal_keeps_exact_history_progress_inputs_and_permitted_native_close(self):
        with tempfile.TemporaryDirectory() as temporary:
            before,after,report,events,wires,first,dump,closed,start,end=unsettled_fixture(Path(temporary))
            check.adoption_unsettled_refusal(before,after,first,wires,events,start,end,end)
            # Independent diagnostics may outlast the failed owner's lease;
            # authority is required at the immediate command-return clock cut.
            check.adoption_unsettled_refusal(before,after,first,wires,events,start,end,end+5_000_000)
            self.assertEqual(check.adoption_unsettled_history(before,wires,events,first),dict(queries_spent=2,
                attempted_prefix=0,gap_count=0,pending_work='request_pending',pending_interval=['0','24']))
            self.assertEqual(initial_science(before,report,events,wires,dump,first['inputs'],COMPLETED,
                mutations=((0,4,0,8),(1,5,0,13)),first_accepted_at_us=ACCEPTED,native_sequence=85),first)
            check.adoption_progress(after,report,events,wires,first,end,epoch=1)
            check.adoption_closed_native(first,'\\x'+report['producer_slot_id'],closed)
            check.adoption_unsettled_error('','error: producer operation unavailable: Control(InvalidMetadata)\n')

    def test_unsettled_history_refuses_wrong_pending_plan_or_second_wire(self):
        with tempfile.TemporaryDirectory() as temporary:
            before,_,_,events,wires,first,*_=unsettled_fixture(Path(temporary))
            for name in ('missing','extra','wire','epoch','query','interval','horizon','budget','checkpoint','classified','terminal','disposition','identity'):
                state=copy.deepcopy(before);transcript=copy.deepcopy(wires);pending=state['acquisition_quanta'][1]
                if name=='missing':state['acquisition_quanta'].pop()
                elif name=='extra':state['acquisition_quanta'].append(copy.deepcopy(pending))
                elif name=='wire':transcript.append(copy.deepcopy(wires[0]))
                elif name=='epoch':pending['producer_epoch']=2
                elif name=='query':pending['query_number']=1
                elif name=='interval':pending['interval_upper']=23
                elif name=='horizon':pending['horizon']=172800
                elif name=='budget':pending['query_budget']=64
                elif name=='checkpoint':pending['checkpoint_before']=4
                elif name=='classified':pending['classified_before']=1
                elif name=='terminal':pending['terminal_kind']='eose'
                elif name=='disposition':pending['disposition']='overlap'
                else:pending['quantum_id']=state['acquisition_quanta'][0]['quantum_id']
                with self.subTest(name=name),self.assertRaises((AssertionError,ValueError)):
                    check.adoption_unsettled_history(state,transcript,events,first)

    def test_unsettled_refusal_rejects_alternate_ineligibility_or_undeclared_effects(self):
        with tempfile.TemporaryDirectory() as temporary:
            baseline,closed,_,events,wires,first,_,_,start,end=unsettled_fixture(Path(temporary))
            for name in ('actor','grant','policy','source','expiry','old-owner-live','fresh-owner-expired','seal','activation',
                    'authorization-in-policy',
                    'retirement','closure-cut','closure-digest','deadline','job-xmin','old-row-xmin','pending-xmin','input','role','source-identity'):
                before=copy.deepcopy(baseline);after=copy.deepcopy(closed);finish=end
                if name=='actor':before['local_accounts'][0]['active']=False
                elif name=='grant':before['principal_grants']=[]
                elif name=='policy':before['policy_revisions'][0]['document']['settings']['sync.enabled']=False
                elif name=='authorization-in-policy':before['policy_revisions'][0]['document']['settings']['runtime.authorization_ttl']=5_000_000
                elif name=='source':before['relay_enrollments'][0]['enabled']=False
                elif name=='expiry':finish=after['acquisition_jobs'][0]['deadline_at_us']
                elif name=='old-owner-live':source.only(before['owner_epochs'],role='control')['lease_expires_at_us']=start+1
                elif name=='fresh-owner-expired':source.only(after['owner_epochs'],role='control',state='active')['lease_expires_at_us']=end
                elif name=='seal':after['acquisition_continuations']=[{}]
                elif name=='activation':after['initial_ingest_activations']=[{}]
                elif name=='retirement':after['producer_epochs'][0]['state']='retired'
                elif name=='closure-cut':after['source_streams'][0]['last_complete_sequence']=18
                elif name=='closure-digest':after['source_streams'][0]['closure_digest']='\\x'+'ff'*32
                elif name=='deadline':after['acquisition_jobs'][0]['deadline_at_us']+=1
                elif name=='job-xmin':after['jobs'][0]['xmin']='999'
                elif name=='old-row-xmin':after['acquisition_quanta'][0]['xmin']='999'
                elif name=='pending-xmin':after['acquisition_quanta'][1]['xmin']='999'
                elif name=='input':after['producer_requests'][3]['request_digest']='\\x'+'ff'*32
                elif name=='role':after['role_instances'].pop()
                else:after['source_streams'][0]['shard_id']=1
                with self.subTest(name=name),self.assertRaises((AssertionError,ValueError,KeyError)):
                    check.adoption_unsettled_refusal(before,after,first,wires,events,start,end,finish)
            for stdout,stderr in (('{}','error: producer operation unavailable: Control(InvalidMetadata)\n'),
                    ('','error: database constraint 23514\n'),('','error: producer operation unavailable: Control(Unavailable)\n'),('','')):
                with self.subTest(stdout=stdout,stderr=stderr),self.assertRaises(AssertionError):check.adoption_unsettled_error(stdout,stderr)

    def test_failed_seal_requires_natural_exact_refusal_and_captures_before_releasing_peer(self):
        for fault in ('none','exit','error','native','authorization-duration','clock','clock-reversed',
                'show-change','pre-recovery-change','recovery-change'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);before,after,report,events,wires,first,_,closed,start,end=unsettled_fixture(root)
                runtime=object.__new__(case.CappedAdoptionRuntime);runtime.root=root
                runtime.args=SimpleNamespace()
                runtime.home=root/'peer';runtime.home.mkdir();runtime.state=root/'state'
                runtime.config=root/'instance.toml';runtime.config.write_text('[runtime]\n')
                document_sha256=case.digest(runtime.config)
                def finite(*args):
                    observed=root_configuration_report(document_sha256)
                    (root/'bootstrap').mkdir()
                    publish(root/'bootstrap','unsettled-root-config.stdout',observed)
                    if fault=='show-change':runtime.config.write_text('[runtime]\n# changed during show\n')
                    return observed
                runtime.finite=Mock(side_effect=finite)
                directory=runtime.state/'data/ingest/producer-inputs-v1';directory.mkdir(parents=True)
                for number,data in enumerate(first['inputs'],1):
                    (directory/f"{report['producer_slot_id']}-0000000000000001-{number:016x}.cbor").write_bytes(data)
                (runtime.home/'client.stderr').write_text('')
                publish(runtime.home,'client-result.json',dict(status='PASS',completion='unsettled-seal-refusal',job_status=None))
                runtime.adoption_before=before;runtime.adoption_first=first;runtime.initialized=report;runtime.events=events
                runtime.adoption_original_files={'old-inputs':'exact'};runtime.files=Mock(return_value=runtime.adoption_original_files)
                clocks=iter([str(start),str(end),str(end+5_000_000)])
                def sql(*args):
                    value=next(clocks)
                    if fault=='pre-recovery-change' and value==str(start):runtime.config.write_text('[runtime]\n# changed before recovery\n')
                    return value
                runtime.sql=Mock(side_effect=sql);runtime.capture=Mock(return_value=(after,{}))
                runtime.wires=Mock(return_value=wires);runtime.backend_absence=Mock()
                runtime.launch=Mock(return_value=dict(id='ab'*32));runtime.docker=Mock(return_value=SimpleNamespace(stdout='4\n'))
                if fault=='recovery-change':
                    def wait(*args,**kwargs):
                        runtime.config.write_text('[runtime]\n# changed during recovery\n')
                        return SimpleNamespace(stdout='4\n')
                    runtime.docker.side_effect=wait
                runtime.inspect=Mock(return_value=dict(Id='ab'*32,State=dict(ExitCode=0 if fault=='exit' else 4,
                    Pid=0,Running=False,OOMKilled=False)))
                def logs(entry,output):
                    (output/'serve.stdout').write_text('')
                    (output/'serve.stderr').write_text('error: producer operation unavailable: Control('
                        +('Unavailable' if fault=='error' else 'InvalidMetadata')+')\n')
                runtime.logs=Mock(side_effect=logs);runtime.client=Mock(pid=12345);runtime.client.poll.return_value=None
                runtime.processes=[runtime.client];runtime.owned_listeners=Mock(return_value=['listener']);runtime.loopback_listener=Mock()
                def joined(*,timeout):
                    self.assertEqual(timeout,6)
                    self.assertTrue((runtime.home/'unsettled-seal-refusal.continue').exists())
                    self.assertTrue((root/'unsettled-seal-refusal/original-duration.json').exists())
                    runtime.capture.assert_called_once();runtime.loopback_listener.assert_called_once()
                    return 0
                runtime.client.wait.side_effect=joined
                if fault=='native':closed['sequence']=87
                samples=[1_000_000_000,6_000_000_000] if fault=='authorization-duration' else [
                    2_000_000_000,1_000_000_000] if fault=='clock-reversed' else [1_000_000_000,2_000_000_000]
                with patch.object(case.native,'census',return_value=closed) as census,patch.object(case,'stop_process'),\
                        patch.object(case.time,'CLOCK_BOOTTIME',None if fault=='clock' else 7,create=True),\
                        patch.object(case.time,'clock_gettime_ns',side_effect=samples) as measured:
                    if fault=='none':runtime.refuse_unsettled_seal()
                    elif fault in ('authorization-duration','clock-reversed'):
                        with self.assertRaisesRegex(ValueError,'unsettled seal refusal prerequisite: recovery attempt exceeded resolved root authorization interval'):
                            runtime.refuse_unsettled_seal()
                    elif fault=='clock':
                        with self.assertRaisesRegex(ValueError,'unsettled seal refusal prerequisite: Linux CLOCK_BOOTTIME'):
                            runtime.refuse_unsettled_seal()
                    elif fault.endswith('change'):
                        with self.assertRaisesRegex(ValueError,'unsettled seal refusal prerequisite: root file changed'):
                            runtime.refuse_unsettled_seal()
                    else:
                        with self.assertRaises((AssertionError,ValueError)):runtime.refuse_unsettled_seal()
                    runtime.finite.assert_called_once_with('unsettled-root-config','config','show','--file','/etc/bigbrotr/instance.toml')
                    if fault in ('clock','show-change','pre-recovery-change'):
                        runtime.launch.assert_not_called();runtime.docker.assert_not_called();measured.assert_not_called()
                    else:
                        runtime.launch.assert_called_once_with('refuse-unsettled-seal','app','ingest','recover','--file',
                            '/etc/bigbrotr/instance.toml','--seal-acquisition')
                        runtime.docker.assert_called_once_with('wait','ab'*32,timeout=180)
                        self.assertEqual([observed.args for observed in measured.call_args_list],[(7,),(7,)])
                    if fault=='authorization-duration':
                        self.assertEqual(json.loads((root/'unsettled-seal-refusal/authorization-interval.json').read_text()),
                            dict(clock='CLOCK_BOOTTIME',elapsed_ns=5_000_000_000,root_authorization_ttl_us=5_000_000,
                                command_returned_at_us=end,root_config_document_sha256=document_sha256,
                                root_config_after_recovery_sha256=document_sha256,
                                root_config_show_sha256=case.digest(root/'bootstrap/unsettled-root-config.stdout'),root_setting_source='schema-default'))
                    if fault=='none':
                        self.assertEqual(runtime.processes,[])
                        census.assert_called_once_with(runtime,root/'unsettled-seal-refusal',expected_writer_exit=4,
                            writer_terminal=root/'unsettled-seal-refusal/terminal-container.json')
                    else:
                        runtime.client.wait.assert_not_called();self.assertFalse((runtime.home/'unsettled-seal-refusal.continue').exists())

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
        for fault,trace in (('plan-before',False),('plan-before',True),('plan-after',False)):
            with self.subTest(fault=fault,valid_trace=trace),tempfile.TemporaryDirectory() as temporary:
                evidence=Path(temporary)/'evidence'
                argv=['run.py','p03-linear-quantum','--linear-quantum-scenario','settled-owner-adoption',
                    '--linear-quantum-fault',fault,'--source-root',str(ROOT),'--evidence',str(evidence)]
                if trace:argv.append('--linear-quantum-trace-successor-sql')
                with patch.object(sys,'argv',argv),patch.object(sys,'stdout'),patch('cases.first_launch.first_launch') as launched:
                    self.assertEqual(runner.main(),2)
                self.assertEqual(launched.call_args.args[0].linear_quantum_trace_successor_sql,trace)
                self.assertEqual(launched.call_args.args[0].linear_quantum_fault,fault)

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
        for prefix,extra in ((1,False),(5,False),(5,True)):
            with self.subTest(prefix=prefix,extra=extra),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);events=corpus_fixture(root)[:3]
                publish(root,'linear-capped-adoption-profile.json',peer.ADOPTION_PROFILE)
                listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(.05)
                address=listener.getsockname();finished=threading.Event();errors=[]
                worker=threading.Thread(target=peer.relay,args=(root,listener,finished,errors),kwargs=dict(adoption=True));worker.start()
                try:
                    for number,(upper,page) in enumerate(list(zip(UPPERS,PAGES))[:prefix],1):
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
                    completion=root/'capped-adoption-peer-complete.json'
                    if prefix==5:
                        deadline=time.monotonic()+1
                        while not completion.exists() and time.monotonic()<deadline:time.sleep(.01)
                        self.assertTrue(completion.exists(),errors);self.assertTrue(json.loads(completion.read_text())['listening'])
                    else:self.assertFalse(completion.exists())
                    self.assertTrue(worker.is_alive());self.assertGreaterEqual(listener.fileno(),0)
                    deadline=time.monotonic()+1
                    while not (root/f'relay-closed-{prefix:03}.json').exists() and time.monotonic()<deadline:time.sleep(.01)
                    check.adoption_wires([json.loads(path.read_text()) for path in sorted(root.glob('relay-closed-*.json'))],events,old_cut=prefix==1)
                    if extra:
                        with socket.create_connection(address,timeout=3):pass
                        worker.join(3);self.assertFalse(worker.is_alive())
                finally:
                    finished.set();worker.join(4);self.assertFalse(worker.is_alive())
                self.assertEqual(listener.fileno(),-1)
                self.assertEqual(len(errors),int(extra))
                joined=json.loads((root/'capped-adoption-peer-joined.json').read_text())
                self.assertEqual((joined['requests'],joined['errors']),(prefix+int(extra),int(extra)))

    def test_client_accepts_only_one_job_and_observes_its_original_time_after_host_gate(self):
        from linear_capped_client import run_adoption,adoption_status
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);scope='12'*16;job='13'*16;session=root/'session.private';saved=dict(session='private')
            session.write_text(json.dumps(saved));publish(root,'visibility.json',dict(ordinal='3'))
            publish(root,'linear-quantum-fault.json','plan-before')
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

    def test_negative_client_keeps_peer_until_its_host_gate_and_makes_no_successor_call(self):
        from linear_capped_client import run_adoption
        for fault,seal,adoption,authority,integrity,gate in (('plan-after',None,None,None,None,'unsettled-seal-refusal'),
                ('plan-before','before-forward',None,None,None,'seal-commit-fault'),('plan-before','after-complete',None,None,None,'seal-commit-fault'),
                ('plan-before',None,'before-forward',None,None,'adoption-commit-fault'),('plan-before',None,'after-complete',None,None,'adoption-commit-fault'),
                ('plan-before',None,None,'actor-grant',None,'continuation-authority-refusal'),
                ('plan-before',None,None,'original-expiry',None,'continuation-authority-refusal'),
                ('plan-before',None,None,None,'workflow-effect','continuation-integrity-refusal'),
                ('plan-before','before-forward',None,None,None,'seal-commit-retry'),
                ('plan-before','after-complete',None,None,None,'seal-commit-retry')):
            with tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);session=root/'session.private';saved=dict(session='private')
                session.write_text(json.dumps(saved));publish(root,'visibility.json',dict(ordinal='3'))
                publish(root,'linear-quantum-fault.json',fault);(root/'prepared.continue').write_text('prepared\n')
                if seal is not None:publish(root,'linear-quantum-seal-fault.json',seal)
                if adoption is not None:publish(root,'linear-quantum-adoption-fault.json',adoption)
                if authority is not None:publish(root,'linear-quantum-authority-refusal.json',authority)
                if integrity is not None:publish(root,'linear-quantum-integrity-refusal.json',integrity)
                if gate=='seal-commit-retry':
                    publish(root,'linear-quantum-seal-retry.json','exact')
                    (root/'seal-commit-fault.continue').write_text('old failed observation gate is insufficient\n')
                calls=[];result=[];errors=[];peer_started=threading.Event();peer_joined=threading.Event();cli=Mock()
                def request(method,path,body=None,headers=None):
                    calls.append((method,path,body));self.assertEqual(method,'POST')
                    answer=dict(created=True,command_key=headers['Idempotency-Key'])
                    if path.endswith('/relay-endpoints'):answer['endpoint_id']='11'*16
                    elif path.endswith('/relay-enrollments'):answer.update(access_scope_id='14'*16,generation='1',enabled=True)
                    elif path.endswith('/acquisition-scopes'):answer['scope_id']='12'*16
                    else:answer.update(job_id='13'*16,accepted_at_us=str(ACCEPTED),execution='unobserved',authority='acceptance-only')
                    return 202,answer
                def relay(directory,listener,finished,relay_errors,**kwargs):
                    peer_started.set();finished.wait(3);peer_joined.set()
                def run():
                    try:result.append(run_adoption(root,request,cli,{},session,saved,{}))
                    except BaseException as error:errors.append(error)
                with patch('installed_sync_client.relay_listener',return_value=Mock()),patch.object(peer,'relay',side_effect=relay):
                    worker=threading.Thread(target=run);worker.start()
                    try:
                        deadline=time.monotonic()+1
                        while not (root/(gate+'.json')).exists() and time.monotonic()<deadline:time.sleep(.01)
                        self.assertTrue((root/(gate+'.json')).exists(),errors)
                        self.assertTrue(worker.is_alive());self.assertTrue(peer_started.is_set());self.assertFalse(peer_joined.is_set())
                        self.assertEqual(len(calls),4);cli.assert_not_called()
                        self.assertFalse((root/'linear-status-observations.json').exists());self.assertFalse((root/'commands-finished.json').exists())
                    finally:
                        (root/(gate+'.continue')).write_text('actual failed seal evidence complete\n')
                        worker.join(4)
                self.assertFalse(worker.is_alive());self.assertFalse(errors);self.assertTrue(peer_joined.is_set())
                self.assertEqual(len(calls),4);cli.assert_not_called()
                self.assertEqual(result[0]['completion'],gate);self.assertIsNone(result[0]['job_status'])
                if authority is not None:self.assertEqual(result[0]['gate_seconds'],140 if authority=='original-expiry' else 110)
                if integrity is not None:self.assertEqual(result[0]['integrity_refusal'],'workflow-effect');self.assertEqual(result[0]['gate_seconds'],110)
                if gate=='seal-commit-retry':self.assertEqual(result[0]['seal_retry'],'exact');self.assertEqual(result[0]['gate_seconds'],110)


if __name__=='__main__':unittest.main()
