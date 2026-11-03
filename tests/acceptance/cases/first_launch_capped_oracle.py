"""Independent six-page expectations and the existing complete native encoder."""
import hashlib
import re
import copy

from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_restart as source

BOUNDS=(86400,24,86400,24,2,1)
TURNS=((0,1),(0,2),(1,1),(1,2),(1,3),(1,4))
PAGES=((0,1),(1,),(0,1),(3,2),(2,),())
SELECTORS=(dict(version=1,filters=[dict(kinds=[1],since=23,until=86399)],history_since=0,ephemeral='retain'),
    dict(version=1,filters=[dict(kinds=[1],until=86399)],history_since=0,ephemeral='retain'))
EVENTS=((24,'dddc1fd9c72975cf5b5768daf466d01bb6949c1d8acc07cf0f3c88b2c638aba8'),
    (23,'32be96c6ec271705225aad466eebc4f8352c65960e619164be86eae0d442a791'),
    (1,'8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849'),
    (23,'18c15755c92def447b33546307951bf25dab36ca8063b852e99c1705b4431ada'))
ENTRIES=((('request',0,86400,86400),('request',0,24,86400),
          ('selector_exclusion',0,23,86400,'selector_excluded')),
         (('request',0,86400,86400),('request',0,24,86400),
          ('request',0,2,86400),('request',0,1,86400)))

ADOPTION_SELECTOR=dict(version=1,filters=[dict(kinds=[1],until=86399)],history_since=0,ephemeral='retain')
ADOPTION_ENTRIES=tuple(('request',0,upper,86400) for upper in (86400,24,23,2,1))
ADOPTION_PAGES=((0,1),(1,),(2,),(2,),())
ADOPTION_FIELDS={'target_epoch','target_manifest_sha256','activation_sha256','adopting_resource','adopting_epoch',
    'adopting_incarnation','adopted_job_revision','adopted_at_us','adoption_transaction'}


def wires_checked(wires,events,*,prepopulated=False):
    assert [(event['created_at'],event['id']) for event in events]==list(EVENTS)
    assert len(wires)==(2 if prepopulated else 6),'missing capped work or extra socket'
    nonces=set();previous=None
    for number,wire in enumerate(wires,1):
        scope,turn=TURNS[number-1];values=[events[index] for index in PAGES[number-1]]
        assert (wire['number'],wire['scope_number'],wire['scope_turn'])==(number,scope,turn)
        request=wire['request'];ident=request[1]
        assert request==['REQ',ident,dict(kinds=[1],since=23 if scope==0 else 0,until=BOUNDS[number-1]-1,limit=3)]
        assert re.fullmatch('[0-9a-f]{48}',ident) and int(ident[:32],16) and ident[:32] not in nonces
        nonces.add(ident[:32])
        assert wire['frames']==['signed']*len(values)+['EOSE'] and 0<=len(values)<=2
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
        validate_queued(view,commands,ordinal=number+1,key=('b1' if number==0 else 'b2')*16,
            total_jobs=len(declared),expected_principal=principal)
    return declared


def quanta(state,wires,events,receipts,commands,*,prepopulated=False):
    wires_checked(wires,events,prepopulated=prepopulated);quantum.shape(state)
    declared=declarations(commands,prepopulated=prepopulated)
    assert len(state['acquisition_quanta'])==(3 if prepopulated else 7),'missing capped quantum or extra plan'
    assert all(row['disposition'] is not None for row in state['acquisition_quanta'])
    refs=lambda n:quantum.receipt_refs(receipts[n-1])
    a,b,c,alternate=events
    expected=[{1:[(a,2,4,refs(4)),(b,2,5,refs(5))],2:[(b,5,0,refs(5))]}]
    if not prepopulated:
        expected.append({1:[(a,5,0,refs(4)),(b,5,0,refs(5))],2:[(alternate,2,6,refs(6)),(c,2,7,refs(7))],3:[(c,5,0,refs(7))],4:[]})
    for number,value in enumerate(declared):
        rows=[row for row in state['acquisition_quanta'] if row['job_id']=='\\x'+value['job_id']]
        assert len(rows)==(3 if number==0 else 4)
        job=source.only(state['jobs'],job_id='\\x'+value['job_id'])
        assert job['created_at_us']==int(value['accepted_at_us'])
        assert all(row['scope_id']=='\\x'+value['scope_id'] for row in rows)
        view=dict(state,acquisition_quanta=rows)
        quantum.journey(view,[wire for wire in wires if wire['scope_number']==number],1,expected[number],selector=SELECTORS[number],entries=ENTRIES[number])


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
    return dict(status='PASS',scope='delivered_sql_input_and_checkpoint_before_B',settled_history_entries=3,terminal_requests=2,local_exclusions=1,
        mutation_requests=5,classified_events=3,native_census='post_join_only')


def science(state,report,events,wires,dump,inputs,completed):
    from cases.first_launch_quantum_science import initial_science
    return initial_science(state,report,events,wires,dump,inputs,completed,
        mutations=((0,4,0,8),(1,5,0,13),(3,6,3,18),(2,7,3,23)),
        first_accepted_at_us=int(declarations_from_state(state)[0]['created_at_us']),native_sequence=121)


def declarations_from_state(state):
    return [source.only(state['jobs'],request_key='\\x'+key*16) for key in ('b1','b2')]


def statuses(values,commands):
    from installed_sync_client import quantum_status
    declared=declarations(commands)
    assert len(values)==2 and {value['job_id'] for value in values}=={value['job_id'] for value in declared}
    for number,value in enumerate(declared):
        observed,=[row for row in values if row['job_id']==value['job_id']]
        local=number==0
        actual=quantum_status(observed,value['scope_id'],value['accepted_at_us'],known_plan=True,selector=SELECTORS[number],
            expected_entries=dict(enumerate(ENTRIES[number],1)))
        assert actual==dict(format='bigbrotr-linear-quantum-status/2',quantum_number='3' if local else '4',
            scope_id=value['scope_id'],lower='0',upper='23' if local else '1',horizon='86400',
            result='selector_excluded' if local else 'linear_observed',terminal=None if local else 'eose',gap=None,
            continuation_upper=None if local else '86400',retained_events=None if local else 0,
            classified_prefix=None if local else 0,coverage='pending',
            entry_kind='selector_exclusion' if local else 'request',
            disposition='selector_excluded' if local else 'window_closed',window_end='86400',window_width='86400',
            query_budget=128,queries_spent=2 if local else 4,attempted_prefix='86400',gap_count=1,
            pending_work='settled',pending_interval=None)


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
    for n in range(1,28):
        first=1 if n<=2 else 2 if n==3 else 3 if n<=7 else 4+(n-8)//5
        key=b'\x01\x20'+ds+bytes(4)+source.be(1)+source.be(n)
        value=bytes.fromhex(native['facts',key.hex()])
        dependencies.append(dict(common,source_shard=0,source_epoch=1,source_sequence=n,payload_digest='\\x'+value[2:34].hex(),
            xmin=source.only(state['producer_outcomes'],request_sequence=first)['xmin']))
    head,=state['producer_checkpoints']
    source.clock(head['committed_at_us'],wires[4]['sent_at_us'],completed)
    assert head['committed_at_us']<owner['lease_expires_at_us']
    expected_head=dict(common,progress_format=1,manifest_sha256='\\x'+source.sha(science['manifest']),checkpoint_revision=11,
        complete_through_request=7,progress_jobs=2,dependency_count=27,issuer_resource=owner['resource_id'],issuer_epoch=owner['epoch'],
        issuer_incarnation=owner['process_incarnation'],committed_at_us=head['committed_at_us'],xmin=head['xmin'],checkpoint_digest='\\x'+'00'*32)
    jobs.sort(key=lambda row:row['job_id'])
    expected_head['checkpoint_digest']='\\x'+typed.head_digest(expected_head,jobs,dependencies,lambda row:typed.native_reference(row,native)).hex()
    current_snapshot(head,sorted(state['acquisition_progress'],key=lambda row:row['job_id']),
        sorted(state['producer_checkpoint_dependencies'],key=lambda row:(row['source_shard'],row['source_epoch'],row['source_sequence'])),
        expected_head,jobs,dependencies,native,prefix_profile=True)


def adoption_manifest(value):
    """Frozen human-readable expectations, independent of product output."""
    assert value['format']=='bigbrotr-settled-owner-adoption-expectation/1'
    assert value['qualification'].startswith('NOT_RUN:')
    assert value['selector']==ADOPTION_SELECTOR
    assert (value['window_width'],value['horizon'],value['query_budget'],value['requested_limit'],value['relay_limit'])==(86400,86400,128,3,2)
    assert [(row['created_at'],row['id']) for row in value['signed_inputs']]==list(EVENTS[:3])
    assert value['old_cut']==dict(last_quantum_number=1,next_plan_commit='before-forward',next_quantum_absent=True,producer_epoch=1)
    for number,(row,entry,page) in enumerate(zip(value['requests'],ADOPTION_ENTRIES,ADOPTION_PAGES),1):
        gap=[23,24] if number==2 else [1,2] if number==4 else None
        assert row==dict(number=number,producer_epoch=1 if number==1 else 2,interval=list(entry[1:3]),
            events=[EVENTS[index][0] for index in page],disposition=('overlap','saturated_second','overlap','saturated_second','window_closed')[number-1],
            continuation_upper=(24,23,2,1,86400)[number-1],gap=gap)
    assert len(value['requests'])==5
    assert value['final']==dict(same_job=True,job_revision=3,queries_spent=5,attempted_prefix=86400,gap_count=2,
        pending_work='settled',pending_interval=None,coverage='pending',unique_bodies=[24,23,1],event_observations=5,sixth_wire_forbidden=True)


def adoption_wires(wires,events,*,old_cut=False):
    assert [(event['created_at'],event['id']) for event in events]==list(EVENTS[:3])
    assert len(wires)==(1 if old_cut else 5),'missing required capped request or extra source wire'
    nonces=set();previous=None
    for number,(wire,entry,page) in enumerate(zip(wires,ADOPTION_ENTRIES,ADOPTION_PAGES),1):
        values=[events[index] for index in page];ident=wire['request'][1]
        assert (wire['number'],wire['scope_number'],wire['scope_turn'])==(number,0,number)
        assert wire['request']==['REQ',ident,dict(kinds=[1],since=0,until=entry[2]-1,limit=3)]
        assert re.fullmatch('[0-9a-f]{48}',ident) and int(ident[:32],16) and ident[:32] not in nonces
        nonces.add(ident[:32])
        assert wire['frames']==['signed']*len(values)+['EOSE']
        assert wire['event_ids']==[event['id'] for event in values]
        assert wire['event_json_hex']==[quantum.wire_json(event).hex() for event in values]
        assert wire['completion']=='request-and-websocket-close'
        assert wire['connected_at_us']<=wire['request_at_us']<=wire['sent_at_us']<=wire['closed_at_us']
        assert 0<=wire['closed_at_us']-wire['request_at_us']<3_000_000
        assert previous is None or previous<=wire['connected_at_us']
        assert type(wire['clock_resolution_us']) is int and wire['clock_resolution_us']>=1
        previous=wire['closed_at_us']


def adoption_source(state,report,commands,*,principal):
    from installed_sync import validate_source,validate_queued
    assert len(commands)==4
    for row,path,key in zip(commands,('relay-endpoints','relay-enrollments','acquisition-scopes','jobs'),('91','92','a1','b1')):
        assert (row['path'],row['key'],row['status'])==(path,key*16,202)
        assert row['body']['created'] is True and row['body']['command_key']==key*16
        assert row['input']['expected_config_revision']==row['input']['expected_policy_revision']=='1'
    assert commands[2]['input']['selector']==ADOPTION_SELECTOR
    assert commands[3]['input']['scope_id']==commands[2]['body']['scope_id']
    assert commands[3]['input']['deadline_us']=='120000000'
    validate_source(state,report,commands[:3],selector_document=ADOPTION_SELECTOR)
    validate_queued(state,commands,ordinal=1,key='b1'*16,total_jobs=1,expected_principal=principal)


def adoption_cardinality(state,slot,epoch):
    """Captured-state union only; connected SQL65 refusal remains NOT_RUN.

    Immutable original Control attribution chooses owned claims. Pending and
    finalized seals retain an old producer's obligation after the job moves.
    Overlap among claims, gaps and seals is one JobId, never multiple charges.
    """
    producer=source.only(state['producer_epochs'],producer_slot_id=slot,producer_epoch=epoch)
    if epoch==1:
        registrar=source.only(state['producer_slots'],producer_slot_id=slot)
        original=(registrar['registered_by_resource_id'],registrar['registered_by_epoch'],registrar['registered_by_incarnation'])
    else:
        activation=source.only(state['initial_ingest_activations'] if epoch==2 else state['ingest_activations'],
            owner_resource_id=producer['owner_resource_id'],target_epoch=epoch)
        original=(activation['activated_by_resource'],activation['activated_by_epoch'],activation['activated_by_incarnation'])
    tracked={row['job_id'] for row in state['jobs'] if row['dataset_id']==producer['dataset_id'] and row['kind']=='acquisition'
        and row['owner_role']=='control' and (row['owner_resource_id'],row['owner_epoch'],row['owner_incarnation'])==original}
    for table in ('acquisition_gaps','acquisition_continuations'):
        tracked.update(row['job_id'] for row in state.get(table,[]) if (row['dataset_id'],row['producer_slot_id'],row['producer_epoch'])==(
            producer['dataset_id'],slot,epoch))
    assert all(len(source.raw(value))==16 for value in tracked)
    assert len(tracked)<=64,'retained producer job union exceeds64'
    return sorted(tracked)


def adoption_history(state,wires,events,first,second=None):
    old_cut=second is None
    adoption_wires(wires,events,old_cut=old_cut)
    assert len(state['jobs'])==len(state['acquisition_jobs'])==1 and not state['acquisition_gaps']
    oldrefs=lambda n:quantum.receipt_refs(first['receipts'][n-1])
    expected={1:[(events[0],2,4,oldrefs(4)),(events[1],2,5,oldrefs(5))]}
    if not old_cut:
        refs=lambda n:quantum.receipt_refs(second['receipts'][n-1])
        expected.update({2:[(events[1],2,2,refs(2))],3:[(events[2],2,3,refs(3))],4:[(events[2],5,0,refs(3))],5:[]})
    rows=sorted(state['acquisition_quanta'],key=lambda row:row['quantum_number'])
    assert len(rows)==(1 if old_cut else 5)
    assert [row['producer_epoch'] for row in rows]==([1] if old_cut else [1,2,2,2,2])
    from installed_sync_oracle import decode
    slot='\\x'+decode(first['inputs'][0].split(b'\0',1)[1])[0][0].hex()
    for row in rows:
        manifest=first['manifest'] if row['producer_epoch']==1 else second['manifest']
        assert (row['producer_slot_id'],row['manifest_sha256'],row['enrollment_generation'],row['route_id'])==(
            slot,'\\x'+source.sha(manifest),1,state['relay_enrollments'][0]['route_id'])
        wire=wires[row['quantum_number']-1]
        assert state['jobs'][0]['created_at_us']<=row['created_at_us']<=wire['connected_at_us']
        assert wire['sent_at_us']<=row['terminal_received_at_us']<=wire['closed_at_us']
        assert row['terminal_received_at_us']<=row['completed_at_us']<state['acquisition_jobs'][0]['deadline_at_us']
        if row['producer_epoch']==2:
            seal,=state['acquisition_continuations']
            assert seal['adopted_at_us']<=row['created_at_us']<=wire['connected_at_us']
            assert wire['closed_at_us']<state['acquisition_jobs'][0]['deadline_at_us']
    assert [row['classified_before'] for row in rows]==([0] if old_cut else [0,0,1,2,3])
    assert [(row['checkpoint_before'],row['checkpoint_after']) for row in rows]==([(3,5)] if old_cut else [(3,5),(1,2),(2,3),(3,4),(4,4)])
    result=quantum.journey(state,wires,None,expected,selector=ADOPTION_SELECTOR,
        entries=ADOPTION_ENTRIES[:1] if old_cut else ADOPTION_ENTRIES)
    assert result==dict(queries_spent=1 if old_cut else 5,attempted_prefix=0 if old_cut else 86400,
        gap_count=0 if old_cut else 2,pending_work='query_ready' if old_cut else 'settled',
        pending_interval=['0','24'] if old_cut else None)
    adoption_page_checkpoints(state,wires,events,first,second)
    return result


def adoption_page_checkpoints(state,wires,events,first,second=None):
    """Rebuild the declared intermediate C2/C3, and bind old C5/new C4.

    These digests use complete independently encoded inputs/receipts/native
    references. Retained SQL counters or effect bytes never choose the snapshot.
    """
    import installed_progress_replay as typed
    from installed_sync_oracle import decode
    old=source.only(state['acquisition_quanta'],quantum_number=1)
    assert old['checkpoint_sha256']==source.only(state['producer_checkpoints'],producer_epoch=1)['checkpoint_digest']
    if second is None:return
    job=state['jobs'][0];detail=state['acquisition_jobs'][0]
    values={(cf,key.hex()):value.hex() for cf,key,value in second['native']}
    common=dict(dataset_id=job['dataset_id'],producer_slot_id=old['producer_slot_id'],producer_epoch=2)
    for number,event in ((2,events[1]),(3,events[2])):
        row=source.only(state['acquisition_quanta'],quantum_number=number);wire=wires[number-1]
        original=second['inputs'][number-1]
        received=decode(original.split(b'\0',1)[1])[3][5][0]
        progress=dict(common,job_id=job['job_id'],scope_id=detail['scope_id'],progress_revision=number-1,
            request_id='\\x'+wire['request'][1].encode().hex(),event_ordinal=1,wire_sequence=1,
            input_sha256='\\x'+source.sha(quantum.wire_json(event)),intent_digest='\\x'+source.sha(original),
            classification=2,mutation_request=number,classified_events=number-1,successful_events=number-1,rejected_events=0,
            observed_at_us=received,clock_resolution_us=wire['clock_resolution_us'],clock_anomaly_flags=4 if wire['clock_resolution_us']>1 else 0,
            xmin='1')
        references={tuple(reference):digest for receipt in second['receipts'][:number] for reference,digest in quantum.receipt_refs(receipt)}
        dependencies=[dict(common,source_shard=reference[1],source_epoch=reference[2],source_sequence=reference[3],payload_digest='\\x'+digest.hex(),xmin='1')
            for reference,digest in sorted(references.items())]
        head=dict(common,progress_format=1,manifest_sha256='\\x'+source.sha(second['manifest']),checkpoint_revision=number,
            complete_through_request=number,progress_jobs=1,dependency_count=len(dependencies),issuer_resource=row['issuer_resource'],
            issuer_epoch=row['issuer_epoch'],issuer_incarnation=row['issuer_incarnation'],committed_at_us=0,xmin='1',checkpoint_digest='\\x'+'00'*32)
        wanted='\\x'+typed.head_digest(head,[progress],dependencies,lambda dependency:typed.native_reference(dependency,values)).hex()
        assert row['checkpoint_sha256']==wanted,'retained intermediate page checkpoint differs'
    current=source.only(state['producer_checkpoints'],producer_epoch=2)['checkpoint_digest']
    assert all(source.only(state['acquisition_quanta'],quantum_number=n)['checkpoint_sha256']==current for n in (4,5))


def adoption_unsettled_history(state,wires,events,first):
    """The acknowledged Q1 is followed by one committed, unwired Q2 plan."""
    rows=sorted(state['acquisition_quanta'],key=lambda row:row['quantum_number'])
    assert len(rows)==2 and len(wires)==1
    settled,pending=rows
    # Keep the positive single-page oracle exact; the pending row is checked
    # separately and again by the complete two-entry traversal below.
    adoption_history(dict(state,acquisition_quanta=[settled]),wires,events,first)
    keys=('dataset_id','job_id','scope_id','producer_slot_id','producer_epoch','manifest_sha256',
        'enrollment_generation','route_id','issuer_role','issuer_resource','issuer_epoch','issuer_incarnation')
    source.exact({key:pending[key] for key in keys},{key:settled[key] for key in keys},'pending Q2 changed original attribution')
    assert (pending['quantum_number'],pending['query_number'],pending['producer_epoch'],pending['interval_lower'],
        pending['interval_upper'],pending['window_end'],pending['horizon'],pending['window_width'],pending['query_budget'],
        pending['checkpoint_before'],pending['classified_before'])==(2,2,1,0,24,86400,86400,86400,128,5,2)
    assert pending['quantum_id']!=settled['quantum_id']
    assert settled['completed_at_us']<=pending['created_at_us']<state['acquisition_jobs'][0]['deadline_at_us']
    assert pending['terminal_kind'] is pending['disposition'] is None
    expected={1:[(events[0],2,4,quantum.receipt_refs(first['receipts'][3])),
        (events[1],2,5,quantum.receipt_refs(first['receipts'][4]))]}
    result=quantum.journey(state,wires,1,expected,selector=ADOPTION_SELECTOR,entries=ADOPTION_ENTRIES[:2])
    assert result==dict(queries_spent=2,attempted_prefix=0,gap_count=0,pending_work='request_pending',pending_interval=['0','24'])
    return result


def adoption_root_authorization(report,document_sha256,state):
    """Bind the offline resolver's deployment root; accepted policy excludes it."""
    assert report['format']=='bigbrotr-config-static/1' and report['status']=='valid-static'
    assert re.fullmatch('[0-9a-f]{64}',document_sha256) and report['document_sha256']==document_sha256
    setting=report['settings']['runtime.authorization_ttl']
    assert set(setting)=={'value','source','default_from','default_add_us','reload','deployment_locked','applicability'}
    assert setting['deployment_locked'] is True and setting['reload']=='live-revocation-barrier'
    assert setting['applicability']=='selected' and setting['source'] in ('schema-default','recipe','file')
    assert setting['default_from'] is None and setting['default_add_us']=='0'
    value=setting['value'];assert type(value) is str and re.fullmatch('[1-9][0-9]*',value)
    authorization_us=source.clock(int(value),1,2**64-1)
    seed=report['initial_policy']
    assert seed['format']=='bigbrotr-product-policy/3' and seed['status']=='candidate-only'
    assert re.fullmatch('[0-9a-f]{64}',seed['sha256'])
    for kind in ('config','policy'):
        revision=source.only(state[kind+'_revisions'])
        assert revision['document_digest']=='\\x'+seed['sha256'],'resolved non-root policy differs from accepted identity'
        assert 'runtime.authorization_ttl' not in revision['document']['settings'],'deployment root leaked into accepted policy'
    return authorization_us


def adoption_seal_eligibility(state,observed_at_us,*,job_revision=2):
    """The original actor, accepted documents, source and deadline still allow sealing."""
    job=source.only(state['jobs']);detail=source.only(state['acquisition_jobs']);dataset=source.only(state['datasets'])
    source.clock(observed_at_us,job['created_at_us'],detail['deadline_at_us']-1)
    assert detail['deadline_at_us']-job['created_at_us']==120_000_000
    assert job['state']=='queued' and job['job_revision']==job_revision and job['input_complete'] is True and job['acquisition_job_protocol']==1
    assert (job['dataset_id'],job['principal_id'],job['request_key'])==(detail['dataset_id'],detail['actor_id'],detail['request_key'])
    assert job['dataset_id']==dataset['dataset_id']
    actor=source.only(state['local_accounts'],dataset_id=job['dataset_id'],principal_id=detail['actor_id'])
    assert actor['active'] is True
    source.only(state['principal_grants'],dataset_id=job['dataset_id'],principal_id=detail['actor_id'],
        action='roles.control',resource_kind='dataset',resource_id=job['dataset_id'])
    for kind in ('config','policy'):
        revision=source.only(state[kind+'_revisions'],dataset_id=job['dataset_id'],revision=job[kind+'_revision'])
        assert dataset[kind+'_revision']==job[kind+'_revision']==1
        assert revision['document']['format']=='bigbrotr-product-policy/3'
        assert revision['document']['settings']['sync.enabled'] is True
        assert 'runtime.authorization_ttl' not in revision['document']['settings']
    scope=source.only(state['acquisition_scopes'],scope_id=detail['scope_id'])
    assert scope['policy_revision']==job['policy_revision']
    enrollment=source.only(state['relay_enrollments'],dataset_id=job['dataset_id'],
        endpoint_id=scope['endpoint_id'],access_scope_id=scope['access_scope_id'])
    assert enrollment['enabled'] is True and enrollment['generation']==1


def adoption_unsettled_refusal(before,after,first,wires,events,before_us,returned_us,after_us):
    """No seal or job effect; only authenticated source close and fresh Control custody."""
    adoption_unsettled_history(before,wires,events,first);adoption_unsettled_history(after,wires,events,first)
    adoption_failed_seal_closure(before,after,first,before_us,returned_us,after_us)


def adoption_failed_seal_closure(before,after,first,before_us,returned_us,after_us,*,seal_count=0):
    """Shared failed-command conservation; a retained seal is checked separately."""
    adoption_seal_eligibility(before,before_us);adoption_seal_eligibility(after,after_us)
    assert before_us<=returned_us<=after_us
    assert set(after)==set(before)
    mutable={'datasets','owner_epochs','role_instances','producer_epochs','source_streams'}
    if seal_count:mutable.add('acquisition_continuations')
    canonical=lambda rows:sorted(quantum.wire_json(dict(sorted(row.items()))) for row in rows)
    for table in set(before)-mutable:
        assert canonical(before[table])==canonical(after[table]),'failed seal changed retained '+table
    assert not before['acquisition_continuations'] and len(after['acquisition_continuations'])==seal_count
    for table in ('acquisition_gaps','initial_ingest_successions','initial_ingest_activations',
            'ingest_successions','ingest_activations'):
        assert not after.get(table),'unsettled plan acquired lifecycle authority: '+table
    old=source.only(before['datasets']);new=source.only(after['datasets'])
    source.exact({k:v for k,v in old.items() if k!='xmin'},{k:v for k,v in new.items() if k!='xmin'},'failed seal changed dataset')
    producer=source.only(before['producer_epochs']);closed=source.only(after['producer_epochs'])
    assert producer['producer_epoch']==1 and producer['state']=='open' and closed['state']=='closing'
    source.exact({k:v for k,v in producer.items() if k not in ('state','xmin')},
        {k:v for k,v in closed.items() if k not in ('state','xmin')},'failed seal changed producer')
    stream=source.only(before['source_streams']);closed_stream=source.only(after['source_streams'])
    assert stream['state']=='open' and closed_stream['state']=='closed' and closed_stream['last_complete_sequence']==17
    digest='\\x'+source.closure(first['manifest'],producer['producer_slot_id'],1,first['inputs'],first['receipts'],17)
    assert closed_stream['closure_digest']==digest
    source.clock(closed_stream['closed_at_us'],before_us,returned_us)
    ignored={'state','closed_at_us','last_complete_sequence','closure_digest','xmin'}
    source.exact({k:v for k,v in stream.items() if k not in ignored},
        {k:v for k,v in closed_stream.items() if k not in ignored},'failed seal changed source identity')
    job=source.only(before['jobs'])
    previous=source.only(before['owner_epochs'],role='control',resource_id=job['owner_resource_id'],epoch=job['owner_epoch'])
    assert previous['state']=='active' and previous['lease_expires_at_us']<=before_us
    for owner in before['owner_epochs']:
        current=source.only(after['owner_epochs'],resource_id=owner['resource_id'],role=owner['role'],epoch=owner['epoch'])
        if owner is previous:
            assert current['state']=='released' and current['exclusion_method']=='storage_fenced'
            assert len(source.raw(current['exclusion_digest']))==32
            source.clock(current['released_at_us'],before_us,returned_us)
            ignored={'state','released_at_us','exclusion_method','exclusion_digest','xmin'}
            source.exact({k:v for k,v in owner.items() if k not in ignored},
                {k:v for k,v in current.items() if k not in ignored},'failed seal rewrote old Control identity')
        else:source.exact(current,owner,'failed seal changed another old owner')
    fresh=[row for row in after['owner_epochs'] if not any((row['resource_id'],row['role'],row['epoch'])==(
        old['resource_id'],old['role'],old['epoch']) for old in before['owner_epochs'])]
    current,=fresh
    assert current['role']=='control' and current['resource_id']==previous['resource_id'] and current['dataset_id']==job['dataset_id']
    assert current['epoch']==max(row['epoch'] for row in before['owner_epochs'] if row['role']=='control')+1
    assert current['state']=='active' and current['process_incarnation'] not in {row['process_incarnation'] for row in before['owner_epochs']}
    source.clock(current['acquired_at_us'],before_us,closed_stream['closed_at_us'])
    assert returned_us<current['lease_expires_at_us']
    assert len(after['owner_epochs'])==len(before['owner_epochs'])+1
    for role in before['role_instances']:
        new=source.only(after['role_instances'],role_instance_id=role['role_instance_id'])
        if role['role_instance_id']==previous['role_instance_id']:
            source.clock(new['retired_at_us'],before_us,returned_us)
            source.exact({k:v for k,v in role.items() if k not in ('retired_at_us','xmin')},
                {k:v for k,v in new.items() if k not in ('retired_at_us','xmin')},'failed seal changed old Control role')
        else:source.exact(new,role,'failed seal changed another old role')
    fresh_role=source.only(after['role_instances'],role_instance_id=current['role_instance_id'])
    assert fresh_role['dataset_id']==job['dataset_id'] and fresh_role['role']=='control' and fresh_role['retired_at_us'] is None
    source.clock(fresh_role['created_at_us'],before_us,returned_us)
    assert current['role_instance_id'] not in {row['role_instance_id'] for row in before['role_instances']}
    assert len(after['role_instances'])==len(before['role_instances'])+1
    return current


def adoption_seal_fault(before,after,first,wires,events,before_us,returned_us,after_us,mode,fault):
    """An executed seal INSERT loses one COMMIT reply; the caller stays unknown."""
    assert mode in ('before-forward','after-complete')
    adoption_history(before,wires,events,first);adoption_history(after,wires,events,first)
    count=int(mode=='after-complete')
    issuer=adoption_failed_seal_closure(before,after,first,before_us,returned_us,after_us,seal_count=count)
    assert type(fault['backend_pid']) is int and fault['backend_pid']>0
    assert issuer['control_backend_pid']==fault['backend_pid'],'seal fault did not select the fresh Control backend'
    if count:adoption_unadopted_seal(before,after,first,issuer,returned_us)


def adoption_unadopted_seal(before,after,first,issuer,latest_us):
    """Exact seal component; caller separately qualifies issuer custody and time cut."""
    seal=source.only(after['acquisition_continuations']);job=source.only(before['jobs'])
    detail=source.only(before['acquisition_jobs']);head=source.only(before['producer_checkpoints'])
    stream=source.only(after['source_streams'])
    assert len(source.raw(seal['continuation_id']))==16
    assert type(seal['xmin']) is str and re.fullmatch('[0-9]+',seal['xmin'])
    source.clock(seal['sealed_at_us'],stream['closed_at_us'],latest_us)
    assert seal['sealed_at_us']<detail['deadline_at_us']
    expected=dict(continuation_id=seal['continuation_id'],dataset_id=job['dataset_id'],job_id=job['job_id'],
        previous_job_revision=2,previous_continuation=None,previous_owner_resource=job['owner_resource_id'],
        previous_owner_epoch=job['owner_epoch'],previous_owner_incarnation=job['owner_incarnation'],scope_id=detail['scope_id'],
        enrollment_generation=1,route_id=source.only(before['relay_enrollments'])['route_id'],
        producer_slot_id=head['producer_slot_id'],producer_epoch=1,manifest_sha256='\\x'+source.sha(first['manifest']),shard_id=0,
        source_sequence=17,source_digest=stream['closure_digest'],checkpoint_revision=5,complete_through_request=5,
        checkpoint_sha256=head['checkpoint_digest'],last_quantum_number=1,sealing_resource=issuer['resource_id'],
        sealing_epoch=issuer['epoch'],sealing_incarnation=issuer['process_incarnation'],sealed_at_us=seal['sealed_at_us'],
        **dict.fromkeys(ADOPTION_FIELDS),xmin=seal['xmin'])
    source.exact(seal,expected,'unknown COMMIT retained a different seal')


def adoption_seal_fault_error(stdout,stderr):
    assert stdout=='' and stderr=='error: producer operation unavailable: CommitUnknown\n'


def adoption_unsettled_error(stdout,stderr):
    assert stdout=='' and stderr=='error: producer operation unavailable: Control(InvalidMetadata)\n'


def adoption_xid(value,*,bits=32):
    assert type(value) is str and re.fullmatch('[1-9][0-9]*',value)
    result=int(value);assert result<1<<bits
    return result


def adoption_exact_tables(wanted,actual):
    assert set(wanted)==set(actual),'table census differs'
    canonical=lambda rows:sorted(quantum.wire_json(dict(sorted(row.items()))) for row in rows)
    for table in wanted:
        assert canonical(wanted[table])==canonical(actual[table]),table


def adoption_new_control(before,after,lower,upper,*,active):
    """Exact new Control shape with opaque fresh IDs and source-bounded renewal clocks."""
    from initial_succession_oracle import exclusion
    previous=max((row for row in before['owner_epochs'] if row['role']=='control'),key=lambda row:row['epoch'])
    current=source.only(after['owner_epochs'],role='control',epoch=previous['epoch']+1)
    role=source.only(after['role_instances'],role_instance_id=current['role_instance_id'])
    old_ids=set(re.findall(r'\\x[0-9a-f]{32}(?![0-9a-f])',quantum.wire_json(before).decode()))
    selected=(current['role_instance_id'],current['process_incarnation'])
    assert len(set(selected))==2
    for value in selected:
        assert re.fullmatch(r'\\x[0-9a-f]{32}',value) and any(source.raw(value)) and value not in old_ids
    assert type(current['control_backend_pid']) is int and current['control_backend_pid']>0
    assert type(current['control_backend_start']) is str and current['control_backend_start']
    source.clock(role['created_at_us'],lower,current['acquired_at_us'])
    source.clock(current['acquired_at_us'],lower,upper)
    # acquire/renew each add the configured60s SQL lease to their own PG cut;
    # preparation separately captures the then-current deadline, not a new TTL.
    source.clock(current['lease_expires_at_us']-60_000_000,current['acquired_at_us'],upper)
    adoption_xid(current['xmin']);adoption_xid(role['xmin'])
    old_xids={row['xmin'] for rows in before.values() for row in rows}
    assert current['xmin'] not in old_xids and role['xmin'] not in old_xids,'new Control reused an old transaction'
    expected=dict(previous,epoch=previous['epoch']+1,role_instance_id=current['role_instance_id'],
        process_incarnation=current['process_incarnation'],acquired_at_us=current['acquired_at_us'],
        lease_expires_at_us=current['lease_expires_at_us'],state='active' if active else 'released',released_at_us=None,
        exclusion_method=None,exclusion_digest=None,control_fence_version=1,control_backend_pid=current['control_backend_pid'],
        control_backend_start=current['control_backend_start'],archive_frontier_protocol=None,index_publication_protocol=None,xmin=current['xmin'])
    retired_at=None
    if not active:
        source.clock(current['released_at_us'],current['acquired_at_us'],upper)
        retired_at=source.clock(role['retired_at_us'],current['released_at_us'],upper)
        assert current['xmin']==role['xmin']
        expected.update(released_at_us=current['released_at_us'],exclusion_method='storage_fenced',
            exclusion_digest=exclusion(current,current['process_incarnation']))
    source.exact(current,expected,'new Control identity/custody differs')
    source.exact(role,dict(dataset_id=previous['dataset_id'],role='control',role_instance_id=current['role_instance_id'],
        created_at_us=role['created_at_us'],retired_at_us=retired_at,xmin=role['xmin']),'new Control role differs')
    return current,role


def adoption_recovery_prefix(before,sealed,retired,first):
    """Confirmed seal/retire effects only, including every unrelated row/xmin."""
    from initial_succession_oracle import exclusion
    deadline=source.only(before['acquisition_jobs'])['deadline_at_us']-1
    job=source.only(before['jobs']);producer=source.only(before['producer_epochs'])
    slot=producer['producer_slot_id'];digest='\\x'+source.closure(first['manifest'],slot,1,first['inputs'],first['receipts'],17)
    assert not before['acquisition_continuations']
    for prior,after,retiring in ((before,sealed,False),(sealed,retired,True)):
        current,role=adoption_new_control(prior,after,job['created_at_us'],deadline,active=False)
        wanted=copy.deepcopy(prior)
        for owner in prior['owner_epochs']:
            if owner['role']!='control' or owner['state']=='released':continue
            assert not retiring and owner['state']=='active'
            changed=source.only(after['owner_epochs'],resource_id=owner['resource_id'],epoch=owner['epoch'])
            changed_role=source.only(after['role_instances'],role_instance_id=owner['role_instance_id'])
            source.clock(changed['released_at_us'],owner['lease_expires_at_us'],current['acquired_at_us'])
            source.clock(changed_role['retired_at_us'],changed['released_at_us'],current['acquired_at_us'])
            adoption_xid(changed['xmin']);assert changed_role['xmin']==changed['xmin']
            wanted['owner_epochs'][wanted['owner_epochs'].index(owner)]=dict(owner,state='released',
                released_at_us=changed['released_at_us'],exclusion_method='storage_fenced',
                exclusion_digest=exclusion(owner,current['process_incarnation']),xmin=changed['xmin'])
            old_role=source.only(prior['role_instances'],role_instance_id=owner['role_instance_id'])
            wanted['role_instances'][wanted['role_instances'].index(old_role)]=dict(old_role,retired_at_us=changed_role['retired_at_us'],xmin=changed_role['xmin'])
        wanted['owner_epochs'].append(current);wanted['role_instances'].append(role)
        dataset=source.only(wanted['datasets']);observed=source.only(after['datasets'])
        adoption_xid(observed['xmin']);dataset['xmin']=observed['xmin']
        old=source.only(wanted['producer_epochs']);new=source.only(after['producer_epochs'])
        adoption_xid(new['xmin'])
        if not retiring:
            assert old['state']=='open'
            old.update(state='closing',xmin=new['xmin'])
            stream=source.only(wanted['source_streams']);closed=source.only(after['source_streams'])
            source.clock(closed['closed_at_us'],current['acquired_at_us'],current['released_at_us'])
            adoption_xid(closed['xmin'])
            stream.update(state='closed',closed_at_us=closed['closed_at_us'],last_complete_sequence=17,closure_digest=digest,xmin=closed['xmin'])
            wanted['acquisition_continuations']=copy.deepcopy(after['acquisition_continuations'])
            adoption_unadopted_seal(before,after,first,current,current['released_at_us'])
        else:
            assert old['state']=='closing'
            at=source.clock(new['retired_at_us'],current['acquired_at_us'],current['released_at_us']);tx=new['xmin']
            old.update(state='retired',retired_at_us=at,closure_digest=digest,xmin=tx)
            ingest=source.only(wanted['owner_epochs'],role='ingest',resource_id=producer['owner_resource_id'],epoch=1)
            ingest.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest=digest,xmin=tx)
            source.only(wanted['role_instances'],role_instance_id=ingest['role_instance_id']).update(retired_at_us=at,xmin=tx)
            assert len(wanted['allocation_domains'])==9
            for domain in wanted['allocation_domains']:domain.update(state='closed',closed_at_us=at,closure_digest=digest,xmin=tx)
        adoption_exact_tables(wanted,after)


def adoption_activated_native(before,state,first,dump=None):
    """Independent five-put activation from the exact closed86 cut, no epoch2 work."""
    from initial_succession_oracle import successor_manifest,successor_rows
    intent=source.only(state['initial_ingest_successions'])
    domains=[source.only(state['allocation_domains'],owner_epoch=2,dictionary_kind=k)['allocation_domain'] for k in range(1,10)]
    source.fresh_successor(before,intent,domains)
    manifest=successor_manifest(first['manifest'],source.raw(intent['target_incarnation']),[source.raw(value) for value in domains])
    slot=intent['producer_slot_id'];digest=source.closure(first['manifest'],slot,1,first['inputs'],first['receipts'],17)
    handoff=(b'\x01\x0a'+source.raw(slot)+source.be(1)+source.be(5)+source.be(17)+bytes.fromhex(digest)
        +source.be(len(first['manifest']),2)+first['manifest']+source.be(len(manifest),2)+manifest)
    assert source.raw(intent['previous_manifest'])==first['manifest'] and source.raw(intent['target_manifest'])==manifest
    assert source.raw(intent['handoff'])==handoff
    initial=successor_rows({(cf,key):value for cf,key,value in adoption_closed_native(first,slot)},manifest,source.raw(slot),handoff)
    wanted=dict(event='dump-state',sequence=91,rows=[dict(cf=cf,key=key.hex(),value=value.hex()) for (cf,key),value in sorted(initial.items())])
    if dump is not None:source.exact(dump,wanted,'failed adoption changed activation native cut')
    return manifest,handoff,wanted


def adoption_fault(before,after,first,wires,events,dump,before_us,returned_us,after_us,mode,fault):
    """Fresh activation remains; only the atomic job/seal adoption may commit."""
    assert mode in ('before-forward','after-complete') and before_us<=returned_us<=after_us
    revision=3 if mode=='after-complete' else 2
    adoption_seal_eligibility(before,before_us);adoption_seal_eligibility(after,after_us,job_revision=revision)
    adoption_history(before,wires,events,first);adoption_history(after,wires,events,first)
    wanted,current,activation,manifest,handoff,expected=adoption_startup_cut(before,after,first,dump,before_us,returned_us)
    assert type(fault['backend_pid']) is int and fault['backend_pid']==current['control_backend_pid']
    job=source.only(before['jobs'])
    if mode=='after-complete':
        seal=source.only(after['acquisition_continuations']);old_seal=source.only(before['acquisition_continuations'])
        tx=seal['xmin'];assert adoption_xid(tx)==adoption_xid(seal['adoption_transaction'],bits=64)&0xffffffff
        assert tx not in (old_seal['xmin'],job['xmin'],activation['xmin'],expected['xmin'])
        source.clock(seal['adopted_at_us'],activation['activated_at_us'],returned_us)
        wanted['acquisition_continuations']=[dict(old_seal,target_epoch=2,target_manifest_sha256='\\x'+source.sha(manifest),
            activation_sha256='\\x'+source.sha(handoff),adopting_resource=current['resource_id'],adopting_epoch=current['epoch'],
            adopting_incarnation=current['process_incarnation'],adopted_job_revision=3,adopted_at_us=seal['adopted_at_us'],
            adoption_transaction=seal['adoption_transaction'],xmin=tx)]
        wanted['jobs']=[dict(job,job_revision=3,owner_resource_id=current['resource_id'],owner_epoch=current['epoch'],
            owner_incarnation=current['process_incarnation'],xmin=tx)]
    adoption_exact_tables(wanted,after)


def adoption_startup_cut(before,after,first,dump,before_us,returned_us):
    """Exact active-zero prepare/activation layout, shared by declared startup refusals."""
    current,role=adoption_new_control(before,after,before_us,returned_us,active=True)
    assert returned_us<current['lease_expires_at_us']
    manifest,handoff,_=adoption_activated_native(before,after,first,dump)
    intent=source.only(after['initial_ingest_successions']);activation=source.only(after['initial_ingest_activations'])
    job=source.only(before['jobs']);old_ingest=source.only(before['owner_epochs'],role='ingest',epoch=1)
    producer=source.only(before['producer_epochs']);slot=producer['producer_slot_id'];dataset=job['dataset_id']
    created=source.clock(intent['created_at_us'],current['acquired_at_us'],activation['activated_at_us'])
    source.clock(activation['activated_at_us'],created,returned_us)
    expires=intent['lease_expires_at_us'];assert returned_us<expires<=current['lease_expires_at_us']
    source.clock(expires-60_000_000,current['acquired_at_us'],created)
    tx=intent['xmin'];adoption_xid(tx)
    assert all(tx!=row['xmin'] for rows in before.values() for row in rows)
    wanted=copy.deepcopy(before);wanted['owner_epochs'].append(current);wanted['role_instances'].append(role)
    inc=intent['target_incarnation'];target_role=intent['target_role_instance_id']
    assert inc not in (current['process_incarnation'],current['role_instance_id']) and target_role not in (current['process_incarnation'],current['role_instance_id'])
    wanted['owner_epochs'].append(dict(old_ingest,epoch=2,role_instance_id=target_role,process_incarnation=inc,
        acquired_at_us=created,lease_expires_at_us=expires,state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,xmin=tx))
    wanted['role_instances'].append(dict(dataset_id=dataset,role='ingest',role_instance_id=target_role,created_at_us=created,retired_at_us=None,xmin=tx))
    wanted['producer_epochs'].append(dict(producer,producer_epoch=2,producer_role_instance_id=target_role,producer_incarnation=inc,
        owner_epoch=2,owner_incarnation=inc,state='open',reserved_through_request=0,opened_at_us=created,retired_at_us=None,closure_digest=None,xmin=tx))
    old=source.only(before['source_streams'])
    wanted['source_streams'].append(dict(old,owner_epoch=2,owner_incarnation=inc,state='open',opened_at_us=created,
        closed_at_us=None,last_complete_sequence=None,closure_digest=None,xmin=tx))
    for kind in range(1,10):
        old=source.only(before['allocation_domains'],dictionary_kind=kind)
        domain=source.only(after['allocation_domains'],owner_epoch=2,dictionary_kind=kind)['allocation_domain']
        wanted['allocation_domains'].append(dict(old,allocation_domain=domain,owner_epoch=2,owner_incarnation=inc,state='active',
            created_at_us=created,closed_at_us=None,closure_digest=None,xmin=tx))
    old=source.only(before['topology_generations'])
    wanted['topology_generations'].append(dict(old,generation=1,previous_generation=0,created_at_us=created,sealed=True,xmin=tx))
    old=source.only(before['topology_streams'])
    wanted['topology_streams'] += [dict(old,topology_generation=1,owner_epoch=epoch,handoff_last_complete_sequence=17 if epoch==1 else None,xmin=tx) for epoch in (1,2)]
    wanted['datasets']=[dict(source.only(before['datasets']),topology_generation=1,xmin=tx)]
    config=source.only(before['config_revisions']);policy=source.only(before['policy_revisions'])
    expected=dict(dataset_id=dataset,owner_resource_id=old_ingest['resource_id'],target_epoch=2,owner_role='ingest',
        target_incarnation=inc,target_role_instance_id=target_role,producer_slot_id=slot,producer_epoch=2,shard_id=0,
        previous_topology=0,target_topology=1,issued_through=5,last_complete_sequence=17,
        closure_digest=producer['closure_digest'],previous_manifest='\\x'+first['manifest'].hex(),target_manifest='\\x'+manifest.hex(),
        previous_manifest_sha256='\\x'+source.sha(first['manifest']),target_manifest_sha256='\\x'+source.sha(manifest),
        handoff='\\x'+handoff.hex(),handoff_sha256='\\x'+source.sha(handoff),config_revision=1,policy_revision=1,
        config_digest=config['document_digest'],policy_digest=policy['document_digest'],outstanding_request_limit=producer['outstanding_request_limit'],
        created_at_us=created,lease_expires_at_us=expires,prepared_by_resource=current['resource_id'],prepared_by_epoch=current['epoch'],
        prepared_by_role='control',prepared_by_incarnation=current['process_incarnation'],xmin=tx)
    wanted['initial_ingest_successions']=[expected]
    fields=('dataset_id','owner_resource_id','target_epoch','target_incarnation','target_role_instance_id','producer_slot_id','producer_epoch',
        'previous_manifest_sha256','target_manifest_sha256','handoff_sha256','prepared_by_resource','prepared_by_epoch','prepared_by_incarnation',
        'config_revision','policy_revision','config_digest','policy_digest')
    activation_tx=activation['xmin'];assert adoption_xid(activation_tx)==adoption_xid(activation['activation_transaction'],bits=64)&0xffffffff
    assert activation_tx!=tx
    wanted['initial_ingest_activations']=[dict({key:expected[key] for key in fields},activated_by_resource=current['resource_id'],
        activated_by_epoch=current['epoch'],activated_by_incarnation=current['process_incarnation'],activated_at_us=activation['activated_at_us'],
        activation_transaction=activation['activation_transaction'],xmin=activation_tx)]
    return wanted,current,activation,manifest,handoff,expected


def adoption_fault_error(stdout,stderr):
    assert stdout=='' and stderr=='error: initial native batch unavailable: Quantum(CommitUnknown)\n'


def continuation_authority_setup(original,prepared,profile,eligible_us,observed_us):
    """Only one deliberate grant removal or genuine expiry differs from eligible input."""
    assert profile in ('actor-grant','original-expiry') and eligible_us<=observed_us
    adoption_seal_eligibility(original,eligible_us)
    job=source.only(original['jobs']);detail=source.only(original['acquisition_jobs'])
    wanted=copy.deepcopy(original)
    if profile=='actor-grant':
        grant=source.only(wanted['principal_grants'],dataset_id=job['dataset_id'],principal_id=job['principal_id'],
            action='roles.control',resource_kind='dataset',resource_id=job['dataset_id'])
        wanted['principal_grants'].remove(grant)
        source.clock(observed_us,job['created_at_us'],detail['deadline_at_us']-1)
    else:source.clock(observed_us,detail['deadline_at_us'],(1<<63)-1)
    adoption_exact_tables(wanted,prepared)


def continuation_authority_refusal(original,prepared,after,first,wires,events,dump,eligible_us,
        before_us,returned_us,after_us,profile,observed):
    """Prove the job-specific guard refused while fresh global startup authority held."""
    assert before_us<=returned_us<=after_us
    continuation_authority_setup(original,prepared,profile,eligible_us,before_us)
    job=source.only(original['jobs']);deadline=source.only(original['acquisition_jobs'])['deadline_at_us']
    if profile=='actor-grant':source.clock(after_us,job['created_at_us'],deadline-1)
    else:source.clock(after_us,deadline,(1<<63)-1)
    adoption_history(prepared,wires,events,first);adoption_history(after,wires,events,first)
    wanted,current,*_=adoption_startup_cut(prepared,after,first,dump,before_us,returned_us)
    expected=dict(event='continuation-authority-refused',classification='acquisition-continuation-authority',
        sqlstate='BB001',reason='acquisition job requires current authority and original remaining duration',
        backend_pid=current['control_backend_pid'],role='bigbrotr_control',database='bigbrotr_control',executed=True,transaction='T')
    source.exact({key:observed.get(key) for key in expected},expected,'actual helper refusal/backend differs')
    adoption_exact_tables(wanted,after)


def continuation_authority_error(stdout,stderr):
    assert stdout=='' and stderr=='error: initial native batch unavailable: Control(Backend)\n'


def adoption_transition(before,sealed,retired,after,first):
    """Allow only the declared old lifecycle and one exact queued-owner CAS."""
    frozen=('producer_requests','producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies',
        'acquisition_progress','acquisition_quanta','acquisition_jobs','job_admissions','relay_endpoints','network_routes',
        'relay_enrollments','acquisition_scopes','acquisition_selectors','acquisition_scope_commands',
        'config_revisions','policy_revisions')
    producer_tables=set(frozen[:6])
    canonical=lambda rows:sorted(quantum.wire_json(dict(sorted(row.items()))) for row in rows)
    slot=before['producer_checkpoints'][0]['producer_slot_id']
    for snapshot in (sealed,retired,after):
        for table in frozen:
            if table in producer_tables:
                old=lambda state:[row for row in state[table] if (row['producer_slot_id'],row['producer_epoch'])==(slot,1)]
                assert canonical(old(snapshot))==canonical(old(before)),'old producer projection changed: '+table
                if snapshot is not after:assert canonical(snapshot[table])==canonical(before[table]),'unexpected pre-successor row: '+table
            else:assert canonical(snapshot[table])==canonical(before[table]),'source/acceptance table changed: '+table
        for table in ('principals','principal_grants','local_accounts','local_credentials','management_commands','management_audit'):
            assert all(row in snapshot[table] for row in before.get(table,[])),'old authority/acceptance row changed: '+table
        lifecycle=(('owner_epochs',('resource_id','epoch'),{'state','released_at_us','exclusion_method','exclusion_digest','xmin'}),
            ('producer_epochs',('producer_slot_id','producer_epoch'),{'state','retired_at_us','closure_digest','xmin'}),
            ('source_streams',('dataset_id','shard_id','owner_epoch'),{'state','closed_at_us','last_complete_sequence','closure_digest','xmin'}),
            ('allocation_domains',('allocation_domain',),{'state','closed_at_us','closure_digest','xmin'}),
            ('datasets',('dataset_id',),{'topology_generation','xmin'}))
        for table,keys,mutable in lifecycle:
            for old in before[table]:
                new=source.only(snapshot[table],**{key:old[key] for key in keys})
                assert {k:v for k,v in old.items() if k not in mutable}=={k:v for k,v in new.items() if k not in mutable}, \
                    'undeclared old lifecycle rewrite: '+table
                if table=='owner_epochs' and old['state']=='released':assert new==old,'already released owner was rewritten'
        for old in before.get('role_instances',[]):
            new=source.only(snapshot['role_instances'],role_instance_id=old['role_instance_id'])
            assert {k:v for k,v in old.items() if k not in ('retired_at_us','xmin')}=={
                k:v for k,v in new.items() if k not in ('retired_at_us','xmin')},'old role identity changed'
            if old['retired_at_us'] is not None:assert new==old,'already retired role was rewritten'
    original,=before['jobs'];job,=after['jobs']
    assert sealed['jobs']==retired['jobs']==before['jobs'],'sealing/retirement changed the job owner or state'
    assert original['state']==job['state']=='queued' and original['job_revision']==2 and job['job_revision']==3
    mutable={'owner_resource_id','owner_epoch','owner_incarnation','job_revision','xmin'}
    assert {k:v for k,v in original.items() if k not in mutable}=={k:v for k,v in job.items() if k not in mutable}
    assert not before.get('acquisition_continuations') and len(sealed['acquisition_continuations'])==1
    seal,=sealed['acquisition_continuations'];adopted,=after['acquisition_continuations']
    assert all(seal[key] is None for key in ADOPTION_FIELDS),'seal fabricated adoption'
    assert retired['acquisition_continuations']==sealed['acquisition_continuations'],'retirement rewrote the seal'
    assert {k:v for k,v in seal.items() if k not in ADOPTION_FIELDS|{'xmin'}}=={
        k:v for k,v in adopted.items() if k not in ADOPTION_FIELDS|{'xmin'}},'immutable seal fields changed'
    assert adopted['xmin']!=seal['xmin'] and all(adopted[key] is not None for key in ADOPTION_FIELDS)
    head,=before['producer_checkpoints'];slot=head['producer_slot_id']
    digest='\\x'+source.closure(first['manifest'],slot,1,first['inputs'],first['receipts'],17)
    assert (seal['job_id'],seal['scope_id'],seal['previous_job_revision'],seal['previous_continuation'])==(
        original['job_id'],before['acquisition_jobs'][0]['scope_id'],2,None)
    assert (seal['previous_owner_resource'],seal['previous_owner_epoch'],seal['previous_owner_incarnation'])==(
        original['owner_resource_id'],original['owner_epoch'],original['owner_incarnation'])
    assert (seal['producer_slot_id'],seal['producer_epoch'],seal['manifest_sha256'],seal['shard_id'],seal['source_sequence'],seal['source_digest'])==(
        slot,1,'\\x'+source.sha(first['manifest']),0,17,digest)
    assert (seal['last_quantum_number'],seal['checkpoint_revision'],seal['complete_through_request'],seal['checkpoint_sha256'])==(
        1,5,5,head['checkpoint_digest'])
    assert before['acquisition_quanta'][0]['completed_at_us']<=seal['sealed_at_us']
    assert (seal['enrollment_generation'],seal['route_id'])==(1,before['relay_enrollments'][0]['route_id'])
    issuer=source.only(sealed['owner_epochs'],role='control',resource_id=seal['sealing_resource'],epoch=seal['sealing_epoch'],process_incarnation=seal['sealing_incarnation'])
    assert issuer['epoch']>original['owner_epoch'] and issuer['state']=='released'
    for snapshot,producer_state in ((sealed,'closing'),(retired,'retired'),(after,'retired')):
        producer=source.only(snapshot['producer_epochs'],producer_epoch=1)
        stream=source.only(snapshot['source_streams'],owner_epoch=1)
        assert producer['state']==producer_state and stream['state']=='closed'
        assert stream['last_complete_sequence']==17 and stream['closure_digest']==digest
        if producer_state=='retired':
            assert producer['closure_digest']==digest
            ingest=source.only(snapshot['owner_epochs'],role='ingest',resource_id=producer['owner_resource_id'],epoch=producer['owner_epoch'],
                process_incarnation=producer['owner_incarnation'])
            assert ingest['state']=='released' and ingest['exclusion_method']=='storage_fenced' and ingest['exclusion_digest']==digest
            assert ingest['released_at_us']==producer['retired_at_us']
            role=source.only(snapshot['role_instances'],role_instance_id=ingest['role_instance_id'])
            assert role['dataset_id']==producer['dataset_id'] and role['role']=='ingest'
            assert role['retired_at_us']==producer['retired_at_us']
            domains=[row for row in snapshot['allocation_domains'] if row['owner_epoch']==1]
            assert len(domains)==9 and {row['dictionary_kind'] for row in domains}==set(range(1,10))
            assert all(row['state']=='closed' and row['closure_digest']==digest and row['closed_at_us']==producer['retired_at_us'] for row in domains)
            assert all(row['xmin']==producer['xmin'] for row in (ingest,role,*domains)),'retirement lifecycle did not commit atomically'
        assert not snapshot['acquisition_gaps'],'continuation became whole-job abandonment'
    assert source.only(sealed['source_streams'],owner_epoch=1)==source.only(retired['source_streams'],owner_epoch=1)==source.only(after['source_streams'],owner_epoch=1), \
        'completed source closure was rewritten'
    for table,key in (('producer_epochs','producer_epoch'),('allocation_domains','owner_epoch')):
        assert canonical(row for row in retired[table] if row[key]==1)==canonical(row for row in after[table] if row[key]==1), \
            'completed retirement was rewritten: '+table
    retired_ingest=source.only(retired['producer_epochs'],producer_epoch=1)
    fields=dict(role='ingest',resource_id=retired_ingest['owner_resource_id'],epoch=retired_ingest['owner_epoch'])
    assert source.only(retired['owner_epochs'],**fields)==source.only(after['owner_epochs'],**fields),'retired old Ingest owner was rewritten'
    retired_role=source.only(retired['role_instances'],role_instance_id=source.only(retired['owner_epochs'],**fields)['role_instance_id'])
    assert retired_role==source.only(after['role_instances'],role_instance_id=retired_role['role_instance_id']),'retired old Ingest role was rewritten'
    fields=dict(role='control',resource_id=original['owner_resource_id'],epoch=original['owner_epoch'])
    old_control=source.only(sealed['owner_epochs'],**fields)
    assert old_control['state']=='released' and old_control['exclusion_method']=='storage_fenced'
    assert old_control==source.only(retired['owner_epochs'],**fields)==source.only(after['owner_epochs'],**fields), \
        'excluded original Control owner was rewritten'
    old_role=source.only(sealed['role_instances'],role_instance_id=old_control['role_instance_id'])
    assert old_role['retired_at_us'] is not None and old_role['retired_at_us']<=seal['sealed_at_us']
    assert old_role==source.only(retired['role_instances'],role_instance_id=old_role['role_instance_id'])==source.only(after['role_instances'],role_instance_id=old_role['role_instance_id']), \
        'excluded original Control role was rewritten'
    quantum.continuation_owner(after,before['acquisition_quanta'][0],job)


def adoption_closed_native(first,slot,dump=None):
    """Existing close barrier replaces one tracker; it adds no scientific fact.

    All five requests were already acknowledged at the observed old cut, so
    positive recovery's ACK followthrough must make no additional native write.
    """
    rows={(cf,key):value for cf,key,value in first['native']}
    key=('receipts',b'\x01\x31'+source.raw(slot)+source.be(1))
    assert rows[key]==b'\x01\x31'+source.be(5)+source.be(5)+source.be(0)
    rows[key]=b'\x02\x31'+source.be(5)+source.be(5)+source.be(0)+b'\x01'
    result=sorted((cf,key,value) for (cf,key),value in rows.items())
    if dump is not None:
        assert dump==dict(event='dump-state',sequence=86,
            rows=[dict(cf=cf,key=key.hex(),value=value.hex()) for cf,key,value in result]),'old close barrier made undeclared native changes'
    return result


def adoption_successor_science(state,started,events,wires,dump,inputs,first,completed,*,before):
    """Complete independent successor rows, including its new known23 witness."""
    from types import SimpleNamespace
    from initial_succession_oracle import successor_manifest,successor_rows
    from installed_family_science import NetworkScience,decode
    from transition_replay import native_rows
    intent,=state['initial_ingest_successions']
    domains=[source.only(state['allocation_domains'],owner_epoch=2,dictionary_kind=k)['allocation_domain'] for k in range(1,10)]
    source.fresh_successor(before,intent,domains)
    ingest=source.only(state['owner_epochs'],role='ingest',resource_id=intent['owner_resource_id'],epoch=2,
        process_incarnation=intent['target_incarnation'])
    assert ingest['role_instance_id']==intent['target_role_instance_id']
    assert (started['owner_resource_id'],started['producer_slot_id'],started['dataset_id'])==(
        intent['owner_resource_id'][2:],intent['producer_slot_id'][2:],intent['dataset_id'][2:])
    stream=source.only(state['source_streams'],owner_epoch=2,shard_id=0,dataset_id=intent['dataset_id'])
    assert stream['state']=='open' and stream['last_complete_sequence'] is stream['closure_digest'] is None
    manifest=successor_manifest(first['manifest'],source.raw(intent['target_incarnation']),[source.raw(value) for value in domains])
    digest=source.closure(first['manifest'],intent['producer_slot_id'],1,first['inputs'],first['receipts'],17)
    handoff=(b'\x01\x0a'+source.raw(intent['producer_slot_id'])+source.be(1)+source.be(5)+source.be(17)+bytes.fromhex(digest)
        +source.be(len(first['manifest']),2)+first['manifest']+source.be(len(manifest),2)+manifest)
    assert source.raw(intent['previous_manifest'])==first['manifest'] and source.raw(intent['target_manifest'])==manifest
    assert source.raw(intent['handoff'])==handoff and intent['handoff_sha256']=='\\x'+source.sha(handoff)
    assert started['process_incarnation']==intent['target_incarnation'][2:] and started['producer_epoch']=='2'
    closed=adoption_closed_native(first,intent['producer_slot_id'])
    initial=successor_rows({(cf,key):value for cf,key,value in closed},manifest,source.raw(intent['producer_slot_id']),handoff)
    model=SimpleNamespace(ds=source.only(state['datasets']),slot=intent['producer_slot_id'],legacy=intent,inputs={},
        native=dict(event='dump-state',sequence=91,rows=[dict(cf=cf,key=key.hex(),value=value.hex()) for (cf,key),value in sorted(initial.items())]))
    science=NetworkScience(model,dict(epoch=2,topology=1,manifest=manifest,domains=domains),acknowledge=True,
        new_event=events[2],endpoint='ws://127.0.0.1:7447/installed-fixture',previous_event=events[1],previous_coordinates=(3,4,13,14,15,16))
    assert len(inputs)==3
    facts={int.from_bytes(bytes.fromhex(row['key'])[-8:],'big'):bytes.fromhex(row['value']) for row in dump['rows']
        if row['cf']=='facts' and row['key'].startswith('0120') and int(row['key'][44:60],16)==2}
    at=lambda n:decode(facts[n][34:].split(b'\0',1)[1])[4]
    received=lambda n:decode(inputs[n-1].split(b'\0',1)[1])[3][5][0]
    old=source.clock(received(2),wires[1]['sent_at_us'],wires[1]['closed_at_us'])
    new=source.clock(received(3),wires[2]['sent_at_us'],wires[2]['closed_at_us'])
    dictionary=source.clock(at(1),int(started.get('activated_at_us',wires[0]['closed_at_us'])),wires[1]['connected_at_us'])
    science.network(True,dictionary,new,source.clock(at(6),new,completed),wires[2]['clock_resolution_us'],
        (old,source.clock(at(5),old,completed),wires[1]['clock_resolution_us']),historical_first=True)
    wanted=native_rows(science.values)
    assert dump==dict(event='dump-state',sequence=model.native['sequence'],rows=wanted),'complete independently encoded successor native rows/sequence'
    assert inputs==list(science.inputs.values()),'successor original inputs differ'
    return dict(native=[(row['cf'],bytes.fromhex(row['key']),bytes.fromhex(row['value'])) for row in wanted],
        inputs=inputs,receipts=list(science.receipts.values()),manifest=manifest,handoff=handoff)


def adoption_progress(state,report,events,wires,science,completed,*,epoch):
    """Literal per-producer frontiers; old and new heads are never relabelled."""
    import installed_progress_replay as typed
    from installed_progress_context_oracle import current_snapshot
    from installed_sync_oracle import decode
    assert epoch in (1,2)
    job,detail,attribution=source.attribution(state,'b1')
    first=source.only(state['acquisition_quanta'],quantum_number=1 if epoch==1 else 2)
    owner=source.only(state['owner_epochs'],role='control',resource_id=first['issuer_resource'],epoch=first['issuer_epoch'],
        process_incarnation=first['issuer_incarnation'])
    bindings={4:source.request_binding(events[0],wires[0],1),5:source.request_binding(events[1],wires[0],2)} if epoch==1 else {
        2:source.request_binding(events[1],wires[1],1),3:source.request_binding(events[2],wires[2],1)}
    source.requests(state,epoch,science['manifest'],science['inputs'],science['receipts'],owner,bindings,attribution)
    ds=source.raw(job['dataset_id']);slot='\\x'+report['producer_slot_id']
    common=dict(dataset_id=job['dataset_id'],producer_slot_id=slot,producer_epoch=epoch)
    actual=source.only(state['acquisition_progress'],producer_epoch=epoch)
    head=source.only(state['producer_checkpoints'],producer_epoch=epoch)
    assert head['xmin']==actual['xmin'],'progress and checkpoint did not commit atomically'
    if epoch==1:
        assert head['xmin']==source.only(state['producer_outcomes'],producer_epoch=1,request_sequence=5)['xmin']
    else:
        assert int(typed.xid(head['xmin']))>max(int(typed.xid(row['xmin'])) for row in state['producer_outcomes'] if row['producer_epoch']==2), \
            'final proof checkpoint must follow all successor mutation outcomes'
    wire=wires[0] if epoch==1 else wires[3];event=events[1] if epoch==1 else events[2]
    received=(decode(science['inputs'][4].split(b'\0',1)[1])[3][5][0] if epoch==1 else actual['observed_at_us'])
    source.clock(received,wire['sent_at_us'],wire['closed_at_us'])
    expected=dict(common,job_id=job['job_id'],scope_id=detail['scope_id'],progress_revision=2 if epoch==1 else 3,
        request_id='\\x'+wire['request'][1].encode().hex(),event_ordinal=2 if epoch==1 else 1,wire_sequence=2 if epoch==1 else 1,
        input_sha256='\\x'+source.sha(quantum.wire_json(event)),classification=2 if epoch==1 else 5,
        mutation_request=5 if epoch==1 else None,classified_events=2 if epoch==1 else 3,successful_events=2 if epoch==1 else 3,
        rejected_events=0,observed_at_us=received,clock_resolution_us=wire['clock_resolution_us'],
        clock_anomaly_flags=4 if wire['clock_resolution_us']>1 else 0,xmin=actual['xmin'])
    if epoch==1:expected['intent_digest']='\\x'+source.sha(science['inputs'][4])
    else:
        old=source.raw(source.only(state['allocation_domains'],owner_epoch=1,dictionary_kind=4)['allocation_domain'])
        domains={row['dictionary_kind']:source.raw(row['allocation_domain']) for row in state['allocation_domains'] if row['owner_epoch']==2}
        ref=lambda kind:[ds,kind,domains[kind],1]
        expected['intent_digest']='\\x'+typed.certification_digest(event,[ds,4,old,3],ref(2),ref(2),[ref(1),ref(3),ref(5)],expected,wire).hex()
    values={(cf,key.hex()):value.hex() for cf,key,value in science['native']}
    coordinates=[(1,n) for n in range(1,18)] if epoch==1 else [(1,n) for n in (3,4,13,14,15,16)]+[(2,n) for n in range(1,11)]
    dependencies=[]
    for source_epoch,sequence in coordinates:
        request=(1 if sequence<=2 else 2 if sequence==3 else 3 if sequence<=7 else 4 if sequence<=12 else 5) if epoch==1 else (
            1 if (source_epoch,sequence)==(1,3) or source_epoch==2 and sequence<=4 else 2 if source_epoch==1 or sequence==5 else 3)
        fact=bytes.fromhex(values['facts',(b'\x01\x20'+ds+bytes(4)+source.be(source_epoch)+source.be(sequence)).hex()])
        dependencies.append(dict(common,source_shard=0,source_epoch=source_epoch,source_sequence=sequence,payload_digest='\\x'+fact[2:34].hex(),
            xmin=source.only(state['producer_outcomes'],producer_epoch=epoch,request_sequence=request)['xmin']))
    source.clock(head['committed_at_us'],wire['sent_at_us'],completed)
    expected_head=dict(common,progress_format=1,manifest_sha256='\\x'+source.sha(science['manifest']),checkpoint_revision=5 if epoch==1 else 4,
        complete_through_request=5 if epoch==1 else 3,progress_jobs=1,dependency_count=len(dependencies),
        issuer_resource=owner['resource_id'],issuer_epoch=owner['epoch'],issuer_incarnation=owner['process_incarnation'],
        committed_at_us=head['committed_at_us'],xmin=head['xmin'],checkpoint_digest='\\x'+'00'*32)
    expected_head['checkpoint_digest']='\\x'+typed.head_digest(expected_head,[expected],dependencies,lambda row:typed.native_reference(row,values)).hex()
    actual_dependencies=sorted((row for row in state['producer_checkpoint_dependencies'] if row['producer_epoch']==epoch),
        key=lambda row:(row['source_shard'],row['source_epoch'],row['source_sequence']))
    current_snapshot(head,[actual],actual_dependencies,expected_head,[expected],dependencies,values,prefix_profile=epoch==1)
