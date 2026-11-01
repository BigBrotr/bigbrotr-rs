"""Capped continuation false-PASS controls; no product or engine execution."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock,patch

from test_first_launch import ROOT,arguments
from test_first_launch_restart import fixture as signed_fixture
from test_linear_quantum import fixture as quantum_fixture
from cases import first_launch_capped_oracle as check
from cases import first_launch_capped_quantum as case
from cases import first_launch as base
from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_restart as source
from installed_sync_oracle import expected
from installed_sync_oracle import decode
from bootstrap_oracle import cbor
import linear_capped_transport as peer


def corpus_fixture(root):
    for name,target in (('tests/fixtures/acquisition/selector-events.json','selector-events.json'),
            ('tests/fixtures/ingest/import-event.jsonl','event.jsonl'),
            ('tests/fixtures/acquisition/linear-window-events.json','linear-window-events.json')):
        (root/target).write_bytes((ROOT/name).read_bytes())
    return peer.corpus(root)


def fixture(root):
    events=corpus_fixture(root);encoded=signed_fixture()[3]
    state=copy.deepcopy(encoded.initial_state);report=encoded.initial_report
    control,_=quantum_fixture();template=control['acquisition_quanta'][0]
    dataset=state['datasets'][0]['dataset_id']
    for historical in state['owner_epochs']:
        historical.setdefault('dataset_id',dataset);historical.setdefault('epoch',1)
    owner=copy.deepcopy(control['owner_epochs'][0]);owner.update(dataset_id=dataset,acquired_at_us=1000,lease_expires_at_us=300_000_000_000)
    state['owner_epochs'].append(owner)
    jobs=[];details=[];commands=[]
    endpoint='71'*16;access='72'*16
    def command(path,key,value,body):
        commands.append(dict(path=path,key=key*16,status=202,input=dict(value,expected_config_revision='1',expected_policy_revision='1'),
            body=dict(body,created=True,command_key=key*16)))
    visibility=dict(report['private_scope'])
    command('relay-endpoints','91',dict(url='WS://127.0.0.1:7447/a/../installed-fixture'),dict(endpoint_id=endpoint))
    command('relay-enrollments','92',dict(endpoint_id=endpoint,visibility=visibility),dict(access_scope_id=access))
    for number,(key,accepted) in enumerate((('81',200_000_999_999),('82',200_001_002_500))):
        job_id=key*16;scope_id=f'{number+91:02x}'*16
        job=dict(control['jobs'][0],dataset_id=dataset,job_id='\\x'+job_id,request_key='\\x'+('b1' if number==0 else 'b2')*16,
            created_at_us=accepted)
        jobs.append(job);details.append(dict(dataset_id=dataset,job_id=job['job_id'],scope_id='\\x'+scope_id))
        command('acquisition-scopes','a1' if number==0 else 'a2',dict(endpoint_id=endpoint,access_scope_id=access,
            visibility=visibility,selector=copy.deepcopy(check.SELECTORS[number])),dict(scope_id=scope_id))
        command('jobs','b1' if number==0 else 'b2',dict(kind='acquisition',scope_id=scope_id,deadline_us='120000000'),
            dict(job_id=job_id,accepted_at_us=str(accepted)))
    wires=[]
    page_indices=((0,1),(1,),(0,1),(3,2),(2,),())
    uppers=(86400,24,86400,24,2,1);turns=((0,1),(0,2),(1,1),(1,2),(1,3),(1,4))
    for number,(scope,turn) in enumerate(turns,1):
        at=200_001_000_000+number*1000;values=[events[index] for index in page_indices[number-1]]
        ident=f'{number:032x}'+'0'*16
        wires.append(dict(number=number,scope_number=scope,scope_turn=turn,
            request=['REQ',ident,dict(kinds=[1],since=23 if scope==0 else 0,until=uppers[number-1]-1,limit=3)],
            connected_at_us=at,request_at_us=at+10,sent_at_us=at+100,closed_at_us=at+400,clock_resolution_us=1,
            frames=['signed']*len(values)+['EOSE'],event_ids=[event['id'] for event in values],
            event_json_hex=[quantum.wire_json(event).hex() for event in values],completion='request-and-websocket-close'))
    parameters=[]
    for event,wire in ((events[0],wires[0]),(events[1],wires[0]),(events[3],wires[3]),(events[2],wires[3])):
        body=[bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],1,event['tags'],event['content']]
        parameters.append((event,wire['sent_at_us']+10,wire['sent_at_us']+50,1,source.sha(b'bigbrotr/body/1\0'+cbor(body))))
    first=parameters[0]
    native,inputs,receipts=expected(state,report,events[0],200_001_000_000,first[1],first[2],1,
        expected_body_digest=first[4],additional_events=parameters[1:],acknowledge=True,finite_event_count=4)
    dump=dict(event='dump-state',sequence=121,rows=[dict(cf=cf,key=key.hex(),value=value.hex()) for cf,key,value in native])
    state.update(jobs=jobs,acquisition_jobs=details,producer_requests=[],producer_checkpoints=[],acquisition_gaps=[],acquisition_quanta=[],
        producer_epochs=[dict(dataset_id=dataset,producer_slot_id='\\x'+report['producer_slot_id'],producer_epoch=1)])
    recipe=(((0,2,4),(1,2,5)),((1,5,0),),((0,5,0),(1,5,0)),((3,2,6),(2,2,7)),((2,5,0),),())
    native_requests={0:4,1:5,3:6,2:7}
    checkpoint=3;classified={0:0,1:0}
    for number,(wire,effects) in enumerate(zip(wires,recipe),1):
        scope,turn=turns[number-1];job=jobs[scope]
        records=b'';references=[]
        for ordinal,(index,kind,request) in enumerate(effects,1):
            event=events[index];native_request=native_requests[index]
            references+=quantum.receipt_refs(receipts[native_request-1])
            records+=(ordinal.to_bytes(2,'big')+ordinal.to_bytes(8,'big')+hashlib.sha256(quantum.wire_json(event)).digest()
                +kind.to_bytes(2,'big')+request.to_bytes(8,'big')+event['created_at'].to_bytes(8,'big')+b'\x01'+bytes.fromhex(event['id']))
        refs=quantum.reference_bytes(references)
        predicate=quantum.wire_json(['REQ','0'*48,wire['request'][2]])
        terminal=quantum.wire_json(['EOSE',wire['request'][1]])
        row=dict(template,dataset_id=dataset,job_id=job['job_id'],scope_id=details[scope]['scope_id'],
            producer_slot_id='\\x'+report['producer_slot_id'],quantum_id='\\x'+f'{number+(number>2):032x}',quantum_number=turn,query_number=turn,
            interval_lower=0,interval_upper=uppers[number-1],horizon=86400,window_end=86400,
            classified_before=classified[scope],
            request_predicate='\\x'+predicate.hex(),predicate_sha256='\\x'+source.sha(predicate),
            terminal_frame='\\x'+terminal.hex(),request_id='\\x'+wire['request'][1].encode().hex(),terminal_sequence=len(effects)+1,
            retained_events=len(effects),classified_prefix=len(effects),effect_records='\\x'+records.hex(),dependencies='\\x'+refs.hex(),
            checkpoint_before=checkpoint,checkpoint_after=checkpoint+len(effects),continuation_upper=(24,23,24,2,1,86400)[number-1],
            disposition=('overlap','saturated_second','overlap','overlap','saturated_second','window_closed')[number-1],
            gap_reason='saturated_second' if number in (2,5) else None,
            gap_lower=23 if number==2 else 1 if number==5 else None,
            gap_upper=24 if number==2 else 2 if number==5 else None,
            charged_bytes=1024+len(predicate)+len(terminal)+len(records)+256*(len(refs)//72))
        checkpoint+=len(effects);classified[scope]+=len(effects);state['acquisition_quanta'].append(row)
        if number==2:
            local=dict(row,quantum_id='\\x'+f'{3:032x}',quantum_number=3,entry_kind='selector_exclusion',
                interval_upper=23,query_number=2,classified_before=3,maximum_dependencies=0,
                disposition='selector_excluded',result_class='selector_excluded',charged_bytes=1024,
                checkpoint_before=6,checkpoint_after=6)
            local.update(dict.fromkeys(('request_predicate','predicate_sha256','terminal_kind','terminal_frame','request_id',
                'terminal_sequence','terminal_received_at_us','teardown_complete','retained_events','classified_prefix',
                'effect_records','dependencies','gap_reason','continuation_upper','gap_lower','gap_upper')))
            state['acquisition_quanta'].append(local)
    return state,report,events,wires,dump,inputs,receipts,commands


class CappedQuantumControls(unittest.TestCase):
    def test_four_event_encoder_requires_its_explicit_bounded_profile(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,report,events,wires,_,_,_,_=fixture(Path(temporary))
            parameters=[]
            for index,wire_index in ((0,0),(1,0),(3,3),(2,3)):
                event,wire=events[index],wires[wire_index]
                body=[bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],1,event['tags'],event['content']]
                parameters.append((event,wire['sent_at_us']+10,wire['sent_at_us']+50,1,source.sha(b'bigbrotr/body/1\0'+cbor(body))))
            first=parameters[0]
            def encode(**changes):
                options=dict(expected_body_digest=first[4],additional_events=parameters[1:],acknowledge=True,finite_event_count=4)
                options.update(changes)
                return expected(state,report,first[0],200_001_000_000,first[1],first[2],1,**options)
            rows,inputs,_=encode();self.assertEqual((len(rows),len(inputs)),(89,7))
            variants=[dict(finite_event_count=value) for value in (None,3,True,4.0,'4')]
            variants+=[dict(additional_events=parameters[1:][:count]) for count in (0,1,2)]
            variants+=[dict(additional_events=parameters),dict(additional_events=[parameters[1]]*3),
                dict(acknowledge=False),dict(provenance=False),dict(preparation_hole=True),dict(retained_boundary=True)]
            for fields in variants:
                with self.subTest(fields=fields),self.assertRaises(AssertionError):encode(**fields)
            prefix,prefix_inputs,_=encode(finite_event_count=None,additional_events=parameters[1:3])
            self.assertEqual((len(prefix),len(prefix_inputs)),(76,6))
            previous={(cf,key):value for cf,key,value in prefix};current={(cf,key):value for cf,key,value in rows}
            added=current.keys()-previous.keys();changed={key for key in previous if previous[key]!=current[key]}
            # One additional signed event contributes13 new keys,4 overwritten
            # mutable heads/counters and one subsequent ACK overwrite. Together
            # these18 sequence increments extend the declared six-request103.
            self.assertEqual((len(added),len(changed)),(13,4))
            self.assertEqual({(cf,key[:2]) for cf,key in changed},
                {('default',b'\x01\x02'),('default',b'\x01\x03'),('default',b'\x01\x0b'),('receipts',b'\x01\x31')})
            self.assertEqual(103+len(added)+len(changed)+1,121)

    def test_source_acceptance_projects_each_scope_but_preserves_both_jobs_and_true_ordinals(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,report,_,_,_,_,_,commands=fixture(Path(temporary))
            actor='\\x'+'10'*16;session='\\x'+'20'*16;dataset=state['datasets'][0]['dataset_id']
            owner=source.only(state['owner_epochs'],role='control')
            state['local_accounts']=[dict(principal_id=actor,credential_revision=1,authentication_revision=1,grant_revision=1)]
            state['browser_sessions']=[dict(session_id=session,principal_id=actor,dataset_id=dataset)]
            state.update(jobs=[],acquisition_jobs=[],job_admissions=[],management_commands=[],management_audit=[],
                acquisition_scopes=[],acquisition_selectors=[],acquisition_scope_commands=[])
            for number in range(2):
                scoped,accepted=commands[2+number*2:4+number*2]
                key=accepted['key'];job='\\x'+accepted['body']['job_id'];scope='\\x'+scoped['body']['scope_id']
                at=int(accepted['body']['accepted_at_us']);audit='\\x'+f'{number+31:02x}'*16;selector='\\x'+f'{number+41:02x}'*16
                digest='\\x'+source.sha(b'bigbrotr/acquisition-job/1\0'+source.raw(dataset)+source.raw(actor)+bytes.fromhex(key)
                    +source.raw(scope)+(120000000).to_bytes(8,'big')+(1).to_bytes(8,'big')*2)
                accepted['body'].update(format='bigbrotr-job-acceptance/1',execution='unobserved',authority='acceptance-only')
                state['acquisition_scopes'].append(dict(scope_id=scope,selector_id=selector))
                state['acquisition_selectors'].append(dict(selector_id=selector))
                state['acquisition_scope_commands'].append(dict(scope_id=scope))
                state['jobs'].append(dict(job_id=job,dataset_id=dataset,principal_id=actor,request_key='\\x'+key,
                    request_digest=digest,kind='acquisition',config_revision=1,policy_revision=1,created_at_us=at,state='queued',
                    job_revision=2,checkpoint_revision=0,reason_code=None,result_manifest_digest=None,
                    owner_resource_id=owner['resource_id'],owner_epoch=owner['epoch'],owner_role='control',
                    owner_incarnation=owner['process_incarnation'],completed_at_us=None,input_topology_generation=None,
                    input_complete=True,export_job_protocol=None,acquisition_job_protocol=1))
                state['acquisition_jobs'].append(dict(job_id=job,dataset_id=dataset,actor_id=actor,request_key='\\x'+key,
                    scope_id=scope,duration_us=120000000,deadline_at_us=at+120000000,session_id=session,
                    actor_credential_revision=1,actor_authentication_revision=1,actor_grant_revision=1))
                state['job_admissions'].append(dict(job_id=job,admission_ordinal=number+1))
                state['management_commands'].append(dict(dataset_id=dataset,actor_id=actor,request_key='\\x'+key,
                    request_digest=digest,action='job.acquisition.accept',accepted_at_us=at,expected_config_revision=1,
                    policy_revision=1,audit_id=audit,authority_mode=None,document_digest=None))
                state['management_audit'].append(dict(dataset_id=dataset,actor_id=actor,audit_id=audit,
                    action='job.acquisition.accept',outcome='accepted',subject_id='\\x'+key,created_at_us=at,
                    config_revision=1,policy_revision=1,reason=None,requester_os_uid=None,requester_database_role=None))
            seen=[]
            def source_view(view,*args,**kwargs):
                self.assertEqual(len(view['acquisition_scopes']),1)
                self.assertEqual([len(view[table]) for table in ('jobs','acquisition_jobs','job_admissions')],[2,2,2])
                self.assertEqual([row['admission_ordinal'] for row in view['job_admissions']],[1,2])
                seen.append(view['acquisition_scopes'][0]['scope_id'])
            with patch('installed_sync.validate_source',side_effect=source_view):
                check.source_rows(state,report,commands,principal=actor[2:])
                self.assertEqual(seen,[row['scope_id'] for row in state['acquisition_scopes']])
                self.assertEqual([row['job_revision'] for row in state['jobs']],[2,2])
                for change in ('foreign-job','foreign-scope','wrong-digest','wrong-ordinal'):
                    value=copy.deepcopy(state)
                    if change=='foreign-job':value['acquisition_jobs'][1]['job_id']=value['jobs'][0]['job_id']
                    elif change=='foreign-scope':value['acquisition_jobs'][1]['scope_id']=value['acquisition_scopes'][0]['scope_id']
                    elif change=='wrong-digest':value['jobs'][1]['request_digest']='\\x'+'00'*32
                    else:value['job_admissions'][1]['admission_ordinal']=1
                    with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                        check.source_rows(value,report,commands,principal=actor[2:])

    def test_existing_encoder_reproduces_all_four_events_and_seven_original_requests(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,report,events,wires,dump,inputs,receipts,commands=fixture(Path(temporary))
            science=check.science(state,report,events,wires,dump,inputs,200_001_010_000)
            self.assertEqual((len(science['native']),len(science['inputs']),len(science['receipts'])),(89,7,7))
            self.assertEqual(science['receipts'],receipts)
            check.quanta(state,wires,events,receipts,commands)
            self.assertEqual(len(state['acquisition_quanta']),7)
            self.assertEqual(sum(row['terminal_kind'] is not None for row in state['acquisition_quanta']),6)
            local,=[row for row in state['acquisition_quanta'] if row['entry_kind']=='selector_exclusion']
            self.assertEqual((local['interval_lower'],local['interval_upper'],local['window_end'],local['query_number']),(0,23,86400,2))
            for change in ('native-id','native-time','native-receipt','native-sequence','missing-row','extra-input','changed-input'):
                changed=copy.deepcopy(dump);originals=list(inputs)
                if change=='extra-input':originals.append(inputs[-1])
                elif change=='changed-input':
                    value=decode(originals[-1].split(b'\0',1)[1]);value[3][2][-1]=bytes(64)
                    originals[-1]=b'bigbrotr/producer-request/1\0'+cbor(value)
                elif change=='missing-row':changed['rows'].pop()
                elif change=='native-sequence':changed['sequence']=103
                elif change=='native-receipt':
                    row=next(row for row in changed['rows'] if row['cf']=='receipts' and row['key'].startswith('0130'))
                    body=bytearray.fromhex(row['value']);body[-1]^=1;row['value']=body.hex()
                else:
                    row=next(row for row in changed['rows'] if row['cf']=='facts' and row['key'].startswith('0120') and row['key'].endswith(f'{9:016x}'))
                    value=decode(bytes.fromhex(row['value'])[34:].split(b'\0',1)[1])
                    if change=='native-id':value[5][0]=bytes(32)
                    else:value[5][2]=1
                    preimage=b'bigbrotr/fact/1\0'+cbor(value)
                    row['value']=(b'\x01\x20'+hashlib.sha256(preimage).digest()+preimage).hex()
                with self.subTest(change=change),self.assertRaises(AssertionError):
                    check.science(state,report,events,wires,changed,originals,200_001_010_000)

    def test_all_known_page_cannot_end_early_or_hide_missing_older_extra_plan_or_changed_effect(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,_,events,wires,_,_,receipts,commands=fixture(Path(temporary))
            check.quanta(state,wires,events,receipts,commands)
            for change in ('early-all-known','wrong-upper','missing-older','extra-quantum','missing-exclusion',
                    'missing-empty-query','event-id','event-time','dependencies'):
                value=copy.deepcopy(state);transcript=copy.deepcopy(wires)
                if change=='early-all-known':value['acquisition_quanta']=value['acquisition_quanta'][:4];transcript=transcript[:3]
                elif change=='wrong-upper':value['acquisition_quanta'][4]['interval_upper']=86400
                elif change=='missing-older':transcript[3]['event_ids'][-1]=events[1]['id']
                elif change=='extra-quantum':value['acquisition_quanta'].append(dict(value['acquisition_quanta'][-1],quantum_number=5))
                elif change=='missing-exclusion':value['acquisition_quanta'].pop(2)
                elif change=='missing-empty-query':value['acquisition_quanta'].pop();transcript.pop()
                else:
                    row=value['acquisition_quanta'][4];field='dependencies' if change=='dependencies' else 'effect_records'
                    body=bytearray(quantum.raw(row[field]));body[40 if change=='dependencies' else 93+(61 if change=='event-id' else 52)]^=1
                    row[field]='\\x'+body.hex()
                with self.subTest(change=change),self.assertRaises(AssertionError):check.quanta(value,transcript,events,receipts,commands)

    def test_literal_final_statuses_require_pending_coverage_and_both_stopped_jobs(self):
        with tempfile.TemporaryDirectory() as temporary:
            *_,commands=fixture(Path(temporary));declared=check.declarations(commands);statuses=[]
            for number,value in enumerate(declared):
                local=number==0
                statuses.append(dict(job_id=value['job_id'],linear_quantum=dict(format='bigbrotr-linear-quantum-status/2',
                    quantum_number='3' if local else '4',scope_id=value['scope_id'],lower='0',upper='23' if local else '1',
                    horizon='86400',result='selector_excluded' if local else 'linear_observed',terminal=None if local else 'eose',gap=None,
                    continuation_upper=None if local else '86400',retained_events=None if local else 0,
                    classified_prefix=None if local else 0,coverage='pending',entry_kind='selector_exclusion' if local else 'request',
                    disposition='selector_excluded' if local else 'window_closed',window_end='86400',window_width='86400',
                    query_budget=128,queries_spent=2 if local else 4,attempted_prefix='86400',gap_count=1,
                    pending_work='settled',pending_interval=None)))
            check.statuses(statuses,commands)
            for field,value in (('upper','86400'),('coverage','complete'),('quantum_number','1'),
                    ('format','bigbrotr-linear-quantum-status/1'),('gap_count',0),('queries_spent',3),
                    ('pending_interval',['0','1']),('attempted_prefix','1'),('disposition','overlap')):
                changed=copy.deepcopy(statuses);changed[1]['linear_quantum'][field]=value
                with self.subTest(field=field),self.assertRaises(AssertionError):check.statuses(changed,commands)
            for field in ('entry_kind','window_end','window_width','query_budget','queries_spent','attempted_prefix',
                    'gap_count','pending_work','pending_interval','disposition'):
                changed=copy.deepcopy(statuses);changed[0]['linear_quantum'].pop(field)
                with self.subTest(missing=field),self.assertRaises((AssertionError,KeyError)):check.statuses(changed,commands)

    def test_two_job_typed_progress_requires_all_eight_classifications_and_native_dependencies(self):
        import installed_progress_replay as typed
        from test_first_launch_restart import current42_head
        with tempfile.TemporaryDirectory() as temporary:
            state,report,events,wires,dump,inputs,receipts,commands=fixture(Path(temporary))
            science=check.science(state,report,events,wires,dump,inputs,200_001_010_000)
            ds=source.raw(state['datasets'][0]['dataset_id']);domains={row['dictionary_kind']:source.raw(row['allocation_domain']) for row in state['allocation_domains']}
            ref=lambda kind,ordinal=1:[ds,kind,domains[kind],ordinal]
            common=dict(dataset_id='\\x'+ds.hex(),producer_slot_id='\\x'+report['producer_slot_id'],producer_epoch=1)
            state['acquisition_scopes']=[dict(scope_id=detail['scope_id'],endpoint_id='\\x'+'71'*16,access_scope_id='\\x'+'72'*16)
                for detail in state['acquisition_jobs']]
            state['relay_enrollments']=[dict(endpoint_id='\\x'+'71'*16,access_scope_id='\\x'+'72'*16,route_id='\\x'+'73'*16)]
            state['producer_outcomes']=[dict(request_sequence=n,xmin=str(n)) for n in range(1,8)]
            jobs=[]
            for number,(wire,event,count) in enumerate(((wires[1],events[1],3),(wires[4],events[2],5))):
                job=state['jobs'][number];detail=state['acquisition_jobs'][number]
                progress=dict(common,job_id=job['job_id'],scope_id=detail['scope_id'],progress_revision=count,
                    request_id='\\x'+wire['request'][1].encode().hex(),event_ordinal=1,wire_sequence=1,
                    input_sha256='\\x'+source.sha(quantum.wire_json(event)),classification=5,mutation_request=None,
                    classified_events=count,successful_events=count,rejected_events=0,observed_at_us=wire['sent_at_us']+20,
                    clock_resolution_us=1,clock_anomaly_flags=0,xmin=str(7+number))
                progress['intent_digest']='\\x'+typed.certification_digest(event,ref(4,3),ref(2),ref(2),[ref(1),ref(3),ref(5)],progress,wire).hex()
                jobs.append(progress)
            jobs.sort(key=lambda row:row['job_id'])
            native={(cf,key.hex()):value.hex() for cf,key,value in science['native']};dependencies=[]
            for n in range(1,28):
                first=1 if n<=2 else 2 if n==3 else 3 if n<=7 else 4+(n-8)//5
                value=bytes.fromhex(native['facts',(b'\x01\x20'+ds+bytes(4)+source.be(1)+source.be(n)).hex()])
                dependencies.append(dict(common,source_shard=0,source_epoch=1,source_sequence=n,payload_digest='\\x'+value[2:34].hex(),xmin=str(first)))
            owner=source.only(state['owner_epochs'],role='control')
            head=dict(common,progress_format=1,manifest_sha256='\\x'+source.sha(science['manifest']),checkpoint_revision=11,
                complete_through_request=7,progress_jobs=2,dependency_count=27,issuer_resource=owner['resource_id'],issuer_epoch=owner['epoch'],
                issuer_incarnation=owner['process_incarnation'],committed_at_us=wires[4]['closed_at_us']+10,xmin='8',checkpoint_digest='\\x'+'00'*32)
            head['checkpoint_digest']='\\x'+typed.head_digest(head,jobs,dependencies,lambda row:typed.native_reference(row,native)).hex()
            state.update(producer_checkpoints=[current42_head(head,initial=True)],acquisition_progress=jobs,producer_checkpoint_dependencies=dependencies)
            check.progress(state,report,wires,events,science,200_001_010_000)
            for change in ('all-known-count','dependency','head','missing-job'):
                value=copy.deepcopy(state)
                if change=='all-known-count':value['acquisition_progress'][1]['classified_events']=3
                elif change=='dependency':value['producer_checkpoint_dependencies'].pop()
                elif change=='head':value['producer_checkpoints'][0]['complete_through_request']=5
                else:value['acquisition_progress'].pop()
                with self.subTest(change=change),self.assertRaises((AssertionError,ValueError)):
                    check.progress(value,report,wires,events,science,200_001_010_000)

    def test_sequential_client_does_not_submit_b_before_host_releases_positive_prepopulation(self):
        from linear_capped_client import continue_after_prepopulation
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);order=[]
            def barrier(name,**kwargs):
                self.assertEqual(name,'capped-prepopulated');self.assertEqual(order,[]);order.append('released')
            def status(**kwargs):
                self.assertEqual(order,['released']);self.assertEqual(kwargs,dict(known_plan=True));return dict(job_id='A')
            def call(path,value,key):
                self.assertTrue(order and order[0]=='released');order.append(path)
                return dict(scope_id='B') if path=='acquisition-scopes' else dict(execution='unobserved',authority='acceptance-only')
            continue_after_prepopulation(directory,call,barrier,status,dict(endpoint_id='endpoint'),dict(access_scope_id='access'),{}, {})
            self.assertEqual(order,['released','acquisition-scopes','jobs'])
            call=Mock()
            with self.assertRaisesRegex(AssertionError,'not qualified'):
                continue_after_prepopulation(directory,call,Mock(side_effect=AssertionError('not qualified')),status,{}, {},{}, {})
            call.assert_not_called()

    def test_per_mutation_source_attribution_reuses_exact_request_oracle(self):
        from test_first_launch_retained import successor_authority_fixture
        _,state,manifest,_,_,science,_,_,_,bindings,observations=successor_authority_fixture()
        owner=source.only(state['owner_epochs'],role='control',epoch=9)
        values={2:observations,3:observations}
        source.requests(state,2,manifest,list(science.inputs.values()),list(science.receipts.values()),owner,bindings,values)
        changed=copy.deepcopy(values);changed[3]['acquisition_job_id']='foreign'
        with self.assertRaises(AssertionError):
            source.requests(state,2,manifest,list(science.inputs.values()),list(science.receipts.values()),owner,bindings,changed)

    def test_cli_refuses_irrelevant_scenario_and_fault_before_creating_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            variants=(('p01-t01','--linear-quantum-scenario','capped-continuation','--binary',sys.executable),
                ('p03-linear-quantum','--linear-quantum-scenario','capped-continuation','--linear-quantum-fault','plan-before'),
                ('p03-linear-quantum','--linear-quantum-scenario','unknown'))
            for number,variant in enumerate(variants):
                evidence=root/str(number)
                result=subprocess.run([sys.executable,'-B',str(ROOT/'tests/acceptance/run.py'),*variant,
                    '--source-root',str(ROOT),'--evidence',str(evidence)],capture_output=True,text=True,timeout=5)
                self.assertEqual(result.returncode,2,result.stderr);self.assertFalse(evidence.exists())

    def test_dispatch_selects_capped_runtime_and_keeps_cleanup_after_unqualified_work(self):
        with tempfile.TemporaryDirectory() as temporary:
            args=arguments(Path(temporary));args.case='p03-linear-quantum';args.linear_quantum_scenario='capped-continuation';checks=[]
            with patch.object(case,'CappedQuantumRuntime') as runtime:
                runtime.return_value.initial.side_effect=ValueError('capped work incomplete')
                base.first_launch(args,checks,{})
                runtime.return_value.cleanup.assert_called_once();runtime.return_value.verify_linear_quantum.assert_not_called()
                runtime.return_value.ordinary.assert_not_called()
            self.assertEqual(len(checks),7)
            self.assertEqual(base.load(args.evidence/'failure.json')['reason'],'capped work incomplete')

    def test_corpus_pins_refuse_changed_full_source_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);corpus_fixture(root);peer.corpus(root)
            for name in ('selector-events.json','event.jsonl','linear-window-events.json'):
                path=root/name;original=path.read_bytes();path.write_bytes(original+b'\n')
                with self.subTest(name=name),self.assertRaises(AssertionError):peer.corpus(root)
                path.write_bytes(original)

    def test_delivered_prepopulation_gate_refuses_missing_proof_outcome_or_changed_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            state,report,events,wires,_,inputs,receipts,commands=fixture(Path(temporary))
            state['acquisition_quanta']=state['acquisition_quanta'][:3]
            slot='\\x'+report['producer_slot_id']
            state['producer_requests']=[dict(producer_slot_id=slot,producer_epoch=1,request_sequence=n,
                request_digest='\\x'+source.sha(value),input_bytes=len(value)) for n,value in enumerate(inputs[:5],1)]
            state['producer_outcomes']=[dict(request_sequence=n,request_digest='\\x'+source.sha(inputs[n-1]),outcome_kind=1,
                outcome_bytes='\\x'+value.hex(),outcome_sha256='\\x'+source.sha(value)) for n,value in enumerate(receipts[:5],1)]
            state['producer_checkpoints']=[dict(producer_slot_id=slot,producer_epoch=1,complete_through_request=5,
                checkpoint_revision=6,prefix_profile=True,progress_jobs=1,dependency_count=17)]
            state['acquisition_progress']=[dict(classified_events=3,successful_events=3,rejected_events=0)]
            with patch.object(check,'source_rows'):
                check.prepopulation(state,wires[:2],events,commands[:4],inputs[:5],report,principal='11'*16)
                for change in ('missing-proof','bad-outcome','bad-input','incomplete-checkpoint'):
                    value=copy.deepcopy(state);originals=list(inputs[:5])
                    if change=='missing-proof':value['acquisition_quanta'].pop()
                    elif change=='bad-outcome':value['producer_outcomes'][-1]['outcome_kind']=2
                    elif change=='bad-input':originals[-1]=inputs[3]
                    else:value['producer_checkpoints'][0]['complete_through_request']=4
                    with self.subTest(change=change),self.assertRaises(AssertionError):
                        check.prepopulation(value,wires[:2],events,commands[:4],originals,report,principal='11'*16)

    def test_finite_peer_caps_every_page_and_keeps_listener_until_owned_join(self):
        for failure in (None,'wrong-upper','seventh-socket'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);corpus_fixture(root);base.save(root/'linear-capped-profile.json',peer.PROFILE)
                finished=threading.Event();accepted=[]
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
                        value=bytes(self.header[:count]);del self.header[:count];return value
                    def frame(self):
                        self.frames+=1;ident=f'{self.number:032x}'+'0'*16
                        if self.frames==1:
                            scope=0 if self.number<=2 else 1
                            upper=(86400,24,86400,24,2,1)[self.number-1]
                            if failure=='wrong-upper' and self.number==4:upper=86400
                            return 1,quantum.wire_json(['REQ',ident,dict(kinds=[1],since=23 if scope==0 else 0,until=upper-1,limit=3)])
                        return (1,quantum.wire_json(['CLOSE',ident])) if self.frames==2 else (8,b'')
                def accept():
                    number=len(accepted)+1
                    if number==7 and failure!='seventh-socket':finished.set();raise socket.timeout()
                    accepted.append(number);return Connection(number),('127.0.0.1',12345)
                listener=Mock();listener.accept.side_effect=accept;listener.getsockopt.return_value=1;errors=[]
                with patch.object(peer,'read_frame',side_effect=lambda connection:connection.frame()),patch.object(peer,'send_frame'):
                    peer.relay(root,listener,finished,errors)
                listener.close.assert_called_once()
                self.assertEqual(bool(errors),failure is not None)
                if failure is None:
                    wires=[base.load(path) for path in sorted(root.glob('relay-closed-*.json'))]
                    check.wires_checked(wires,peer.corpus(root))
                    completion=base.load(root/'capped-peer-complete.json')
                    self.assertTrue(completion['listening']);self.assertFalse(completion['listener_closed'])
                    joined=base.load(root/'capped-peer-joined.json')
                    self.assertEqual((joined['requests'],joined['errors'],joined['listener_closed']),(6,0,True))

    def test_real_local_socket_peer_serves_six_literal_pages_and_joins_owned_listener(self):
        from installed_sync_client import exact
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);events=corpus_fixture(root)
            base.save(root/'linear-capped-profile.json',peer.PROFILE)
            listener=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(.05)
            address=listener.getsockname();finished=threading.Event();errors=[]
            worker=threading.Thread(target=peer.relay,args=(root,listener,finished,errors));worker.start()
            def send(connection,opcode,value):
                body=quantum.wire_json(value) if opcode==1 else b'';mask=b'\x01\x02\x03\x04'
                header=bytes([0x80|opcode,0x80|len(body)]) if len(body)<126 else bytes([0x80|opcode,0xfe])+len(body).to_bytes(2,'big')
                connection.sendall(header+mask+bytes(byte^mask[n%4] for n,byte in enumerate(body)))
            def receive(connection):
                first,second=exact(connection,2);self.assertFalse(second&128)
                length=second&127
                if length==126:length=int.from_bytes(exact(connection,2),'big')
                self.assertLess(length,65536)
                body=exact(connection,length)
                return first&15,json.loads(body) if first&15==1 else body
            try:
                for number,(upper,indices) in enumerate(zip((86400,24,86400,24,2,1),((0,1),(1,),(0,1),(3,2),(2,),())),1):
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
                        send(connection,1,['REQ',ident,dict(kinds=[1],since=23 if number<=2 else 0,until=upper-1,limit=3)])
                        for index in indices:self.assertEqual(receive(connection),(1,['EVENT',ident,events[index]]))
                        self.assertEqual(receive(connection),(1,['EOSE',ident]))
                        send(connection,1,['CLOSE',ident]);send(connection,8,None)
                        self.assertEqual(receive(connection),(8,b''));self.assertEqual(connection.recv(1),b'')
                completion=root/'capped-peer-complete.json';deadline=time.monotonic()+1
                while not completion.exists() and time.monotonic()<deadline:time.sleep(.01)
                self.assertTrue(completion.exists());self.assertTrue(base.load(completion)['listening'])
                self.assertEqual(listener.getsockopt(socket.SOL_SOCKET,socket.SO_ACCEPTCONN),1)
            finally:
                finished.set();worker.join(4)
                self.assertFalse(worker.is_alive());self.assertEqual(listener.fileno(),-1)
            self.assertEqual(errors,[])
            wires=[base.load(path) for path in sorted(root.glob('relay-closed-*.json'))]
            check.wires_checked(wires,events)
            self.assertEqual(base.load(root/'capped-peer-joined.json')['requests'],6)


if __name__=='__main__':unittest.main()
