"""Independent five-page expectations and the existing complete native encoder."""
import hashlib
import re

from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_restart as source

BOUNDS=(86400,24,86400,24,2)
TURNS=((0,1),(0,2),(1,1),(1,2),(1,3))
PAGES=((0,1),(1,),(0,1),(1,2),(2,))
SELECTORS=(dict(version=1,filters=[dict(kinds=[1],since=23)],history_since=0,ephemeral='retain'),
    dict(version=1,filters=[dict(kinds=[1])],history_since=0,ephemeral='retain'))
EVENTS=((24,'dddc1fd9c72975cf5b5768daf466d01bb6949c1d8acc07cf0f3c88b2c638aba8'),
    (23,'32be96c6ec271705225aad466eebc4f8352c65960e619164be86eae0d442a791'),
    (1,'8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849'))


def wires_checked(wires,events,*,prepopulated=False):
    assert [(event['created_at'],event['id']) for event in events]==list(EVENTS)
    assert len(wires)==(2 if prepopulated else 5),'missing capped work or extra socket'
    nonces=set();previous=None
    for number,wire in enumerate(wires,1):
        scope,turn=TURNS[number-1];values=[events[index] for index in PAGES[number-1]]
        assert (wire['number'],wire['scope_number'],wire['scope_turn'])==(number,scope,turn)
        request=wire['request'];ident=request[1]
        assert request==['REQ',ident,dict(kinds=[1],since=23 if scope==0 else 0,until=BOUNDS[number-1]-1,limit=3)]
        assert re.fullmatch('[0-9a-f]{48}',ident) and int(ident[:32],16) and ident[:32] not in nonces
        nonces.add(ident[:32])
        assert wire['frames']==['signed']*len(values)+['EOSE'] and 0<len(values)<=2
        assert wire['completion']=='request-and-websocket-close'
        assert wire['event_ids']==[event['id'] for event in values]
        assert wire['event_json_hex']==[quantum.wire_json(event).hex() for event in values]
        assert wire['connected_at_us']<=wire['request_at_us']<=wire['sent_at_us']<=wire['closed_at_us']
        assert 0<=wire['closed_at_us']-wire['request_at_us']<3_000_000
        assert previous is None or previous<=wire['connected_at_us']
        assert type(wire['clock_resolution_us']) is int and wire['clock_resolution_us']>=1
        previous=wire['closed_at_us']


def declarations(commands,*,prepopulated=False):
    paths=('relay-endpoints','relay-enrollments','acquisition-scopes','jobs','acquisition-scopes','jobs')
    keys=('91','92','a1','b1','a2','b2')
    assert len(commands)==(4 if prepopulated else 6)
    for row,path,key in zip(commands,paths,keys):
        assert row['path']==path and row['key']==key*16 and row['status']==202
        assert row['body']['created'] is True and row['body']['command_key']==key*16
        assert row['input']['expected_config_revision']==row['input']['expected_policy_revision']=='1'
    result=[]
    for number in range(1 if prepopulated else 2):
        scoped,accepted=commands[2+number*2:4+number*2]
        assert scoped['input']['selector']==SELECTORS[number]
        assert scoped['input']['endpoint_id']==commands[0]['body']['endpoint_id']
        assert scoped['input']['access_scope_id']==commands[1]['body']['access_scope_id']
        assert scoped['input']['visibility']==commands[1]['input']['visibility']
        assert accepted['input']['scope_id']==scoped['body']['scope_id']
        assert accepted['input']['kind']=='acquisition' and accepted['input']['deadline_us']=='120000000'
        result.append(dict(scope_id=scoped['body']['scope_id'],job_id=accepted['body']['job_id'],
            accepted_at_us=accepted['body']['accepted_at_us']))
    return result


def source_rows(state,report,commands,*,principal,prepopulated=False):
    from installed_sync import validate_source,validate_queued
    declared=declarations(commands,prepopulated=prepopulated)
    assert len(state['acquisition_scopes'])==len(state['acquisition_selectors'])==len(declared)
    assert len(state['acquisition_scope_commands'])==len(declared)
    for number,value in enumerate(declared):
        view=dict(state)
        view['acquisition_scopes']=[row for row in state['acquisition_scopes'] if row['scope_id']=='\\x'+value['scope_id']]
        selector=view['acquisition_scopes'][0]['selector_id']
        view['acquisition_selectors']=[row for row in state['acquisition_selectors'] if row['selector_id']==selector]
        view['acquisition_scope_commands']=[row for row in state['acquisition_scope_commands'] if row['scope_id']=='\\x'+value['scope_id']]
        observations=commands[:2]+[commands[2+number*2]]
        validate_source(view,report,observations,nonces=('91','92','a1' if number==0 else 'a2'),selector_document=SELECTORS[number])
        validate_queued(state,commands,ordinal=number+1,key=('b1' if number==0 else 'b2')*16,
            total_jobs=len(declared),expected_principal=principal)
    return declared


def quanta(state,wires,events,receipts,commands,*,prepopulated=False):
    wires_checked(wires,events,prepopulated=prepopulated);quantum.shape(state)
    declared=declarations(commands,prepopulated=prepopulated)
    assert len(state['acquisition_quanta'])==(2 if prepopulated else 5),'missing capped quantum or extra plan'
    assert all(row['terminal_kind'] is not None for row in state['acquisition_quanta'])
    refs=lambda n:quantum.receipt_refs(receipts[n-1])
    a,b,c=events
    expected=[{1:[(a,2,4,refs(4)),(b,2,5,refs(5))],2:[(b,5,0,refs(5))]}]
    if not prepopulated:
        expected.append({1:[(a,5,0,refs(4)),(b,5,0,refs(5))],2:[(b,5,0,refs(5)),(c,2,6,refs(6))],3:[(c,5,0,refs(6))]})
    for number,value in enumerate(declared):
        rows=[row for row in state['acquisition_quanta'] if row['job_id']=='\\x'+value['job_id']]
        assert len(rows)==(2 if number==0 else 3)
        job=source.only(state['jobs'],job_id='\\x'+value['job_id'])
        assert job['created_at_us']==int(value['accepted_at_us'])
        assert all(row['scope_id']=='\\x'+value['scope_id'] for row in rows)
        view=dict(state,acquisition_quanta=rows)
        quantum.journey(view,[wire for wire in wires if wire['scope_number']==number],1,expected[number],selector=SELECTORS[number])


def prepopulation(state,wires,events,commands,inputs,report,*,principal):
    from installed_sync_oracle import decode
    source_rows(state,report,commands,principal=principal,prepopulated=True)
    assert len(inputs)==len(state['producer_requests'])==len(state['producer_outcomes'])==5
    outcomes=sorted(state['producer_outcomes'],key=lambda row:row['request_sequence'])
    requests=sorted(state['producer_requests'],key=lambda row:row['request_sequence'])
    receipts=[]
    for number,(request,outcome,original) in enumerate(zip(requests,outcomes,inputs),1):
        assert request['request_sequence']==outcome['request_sequence']==number
        assert request['request_digest']==outcome['request_digest']=='\\x'+source.sha(original)
        assert request['input_bytes']==len(original) and outcome['outcome_kind']==1
        receipt=source.raw(outcome['outcome_bytes']);receipts.append(receipt)
        assert outcome['outcome_sha256']=='\\x'+source.sha(receipt)
        assert receipt.startswith(b'bigbrotr/receipt/1\0')
        returned=decode(receipt.split(b'\0',1)[1])
        assert returned[:3]==[1,[bytes.fromhex(report['producer_slot_id']),1,number],hashlib.sha256(original).digest()]
    for number,event in ((4,events[0]),(5,events[1])):
        value=decode(inputs[number-1].split(b'\0',1)[1])
        assert value[:3]==[[bytes.fromhex(report['producer_slot_id']),1,number],'admit',1]
        assert value[3][2]==[bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],1,
            event['tags'],event['content'],bytes.fromhex(event['sig'])]
        source.clock(value[3][5][0],wires[0]['sent_at_us'],wires[0]['closed_at_us'])
    quanta(state,wires,events,receipts,commands,prepopulated=True)
    head,=state['producer_checkpoints'];progress,=state['acquisition_progress']
    assert head['complete_through_request']==5 and head['checkpoint_revision']==6 and head['prefix_profile'] is True
    assert progress['classified_events']==progress['successful_events']==3 and progress['rejected_events']==0
    return dict(status='PASS',scope='delivered_sql_input_and_checkpoint_before_B',terminal_quanta=2,
        mutation_requests=5,classified_events=3,native_census='post_join_only')


def science(state,report,events,wires,dump,inputs,completed):
    from installed_sync_oracle import expected,decode
    from bootstrap_oracle import cbor
    assert len(inputs)==6 and dump['event']=='dump-state' and dump['sequence']==103
    actual=[(row['cf'],bytes.fromhex(row['key']),bytes.fromhex(row['value'])) for row in dump['rows']]
    facts={int.from_bytes(key[-8:],'big'):value for cf,key,value in actual if cf=='facts' and key[:2]==b'\x01\x20'}
    recorded=lambda n:decode(facts[n][34:].split(b'\0',1)[1])[4]
    accepted=int(declarations_from_state(state)[0]['created_at_us'])
    dictionary=source.clock(recorded(4),accepted,wires[0]['connected_at_us'])
    parameters=[]
    for event,request,wire,sequence in ((events[0],4,wires[0],8),(events[1],5,wires[0],13),(events[2],6,wires[3],18)):
        received=decode(inputs[request-1].split(b'\0',1)[1])[3][5][0]
        source.clock(received,wire['sent_at_us'],wire['closed_at_us'])
        at=source.clock(recorded(sequence),received,completed)
        body=[bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],1,event['tags'],event['content']]
        parameters.append((event,received,at,wire['clock_resolution_us'],source.sha(b'bigbrotr/body/1\0'+cbor(body))))
    first=parameters[0]
    rows,wanted_inputs,receipts=expected(state,report,first[0],dictionary,first[1],first[2],first[3],
        expected_body_digest=first[4],additional_events=parameters[1:],acknowledge=True)
    assert actual==rows,'complete independently encoded capped native rows'
    assert inputs==wanted_inputs,'all six independently encoded original inputs'
    return dict(native=rows,inputs=wanted_inputs,receipts=receipts,
        manifest=next(value for cf,key,value in rows if cf=='default' and key==b'\x01\x01'))


def declarations_from_state(state):
    return [source.only(state['jobs'],request_key='\\x'+key*16) for key in ('b1','b2')]


def statuses(values,commands):
    from installed_sync_client import quantum_status
    declared=declarations(commands)
    assert len(values)==2 and {value['job_id'] for value in values}=={value['job_id'] for value in declared}
    for number,value in enumerate(declared):
        observed,=[row for row in values if row['job_id']==value['job_id']]
        upper=24 if number==0 else 2;count=2 if number==0 else 3
        actual=quantum_status(observed,value['scope_id'],value['accepted_at_us'],known_plan=True,selector=SELECTORS[number],
            expected_uppers={1:86400,2:24,**({3:2} if number else {})})
        assert actual==dict(format='bigbrotr-linear-quantum-status/1',quantum_number=str(count),scope_id=value['scope_id'],
            lower='0',upper=str(upper),horizon=str(int(value['accepted_at_us'])//1_000_000),result='linear_observed',
            terminal='eose',gap=None,continuation_upper=str(upper),retained_events=1,classified_prefix=1,coverage='pending')


def progress(state,report,wires,events,science,completed):
    import installed_progress_replay as typed
    from installed_progress_context_oracle import current_snapshot
    ds=source.raw(state['datasets'][0]['dataset_id'])
    domains={row['dictionary_kind']:source.raw(row['allocation_domain']) for row in state['allocation_domains']}
    ref=lambda kind,ordinal=1:[ds,kind,domains[kind],ordinal]
    owner=source.only(state['owner_epochs'],role='control')
    common=dict(dataset_id='\\x'+ds.hex(),producer_slot_id='\\x'+report['producer_slot_id'],producer_epoch=1)
    jobs=[]
    for number,(key,wire,event,count) in enumerate((('b1',wires[1],events[1],3),('b2',wires[4],events[2],5))):
        job,detail,_=source.attribution(state,key)
        actual=source.only(state['acquisition_progress'],job_id=job['job_id'])
        expected=dict(common,job_id=job['job_id'],scope_id=detail['scope_id'],progress_revision=count,
            request_id='\\x'+wire['request'][1].encode().hex(),event_ordinal=1,wire_sequence=1,
            input_sha256='\\x'+source.sha(quantum.wire_json(event)),classification=5,mutation_request=None,
            classified_events=count,successful_events=count,rejected_events=0,observed_at_us=actual['observed_at_us'],
            clock_resolution_us=wire['clock_resolution_us'],clock_anomaly_flags=4 if wire['clock_resolution_us']>1 else 0,xmin=actual['xmin'])
        source.clock(expected['observed_at_us'],wire['sent_at_us'],wire['closed_at_us'])
        expected['intent_digest']='\\x'+typed.certification_digest(event,ref(4,3),ref(2),ref(2),[ref(1),ref(3),ref(5)],expected,wire).hex()
        jobs.append(expected)
    native={(cf,key.hex()):value.hex() for cf,key,value in science['native']}
    dependencies=[]
    for n in range(1,23):
        first=1 if n<=2 else 2 if n==3 else 3 if n<=7 else 4+(n-8)//5
        key=b'\x01\x20'+ds+bytes(4)+source.be(1)+source.be(n)
        value=bytes.fromhex(native['facts',key.hex()])
        dependencies.append(dict(common,source_shard=0,source_epoch=1,source_sequence=n,payload_digest='\\x'+value[2:34].hex(),
            xmin=source.only(state['producer_outcomes'],request_sequence=first)['xmin']))
    head,=state['producer_checkpoints']
    source.clock(head['committed_at_us'],wires[4]['sent_at_us'],completed)
    assert head['committed_at_us']<owner['lease_expires_at_us']
    expected_head=dict(common,progress_format=1,manifest_sha256='\\x'+source.sha(science['manifest']),checkpoint_revision=11,
        complete_through_request=6,progress_jobs=2,dependency_count=22,issuer_resource=owner['resource_id'],issuer_epoch=owner['epoch'],
        issuer_incarnation=owner['process_incarnation'],committed_at_us=head['committed_at_us'],xmin=head['xmin'],checkpoint_digest='\\x'+'00'*32)
    jobs.sort(key=lambda row:row['job_id'])
    expected_head['checkpoint_digest']='\\x'+typed.head_digest(expected_head,jobs,dependencies,lambda row:typed.native_reference(row,native)).hex()
    current_snapshot(head,sorted(state['acquisition_progress'],key=lambda row:row['job_id']),
        sorted(state['producer_checkpoint_dependencies'],key=lambda row:(row['source_shard'],row['source_epoch'],row['source_sequence'])),
        expected_head,jobs,dependencies,native,prefix_profile=True)
