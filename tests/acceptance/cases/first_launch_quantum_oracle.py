"""Independent durable linear obligations over the existing socket/science captures.

This validator reads evidence only; it supplies neither a native capability nor
permission to resume an old owner. No engine result is inferred from its tests.
"""
import hashlib
import json


def raw(value):
    assert isinstance(value,str) and value.startswith('\\x')
    return bytes.fromhex(value[2:])


def wire_json(value):
    return json.dumps(value,ensure_ascii=False,separators=(',',':')).encode()


def effects(value):
    value=raw(value);assert len(value)%61==0 and len(value)<=3*61
    result=[]
    for at in range(0,len(value),61):
        part=value[at:at+61]
        result.append((int.from_bytes(part[:2],'big'),int.from_bytes(part[2:10],'big'),part[10:42],
            int.from_bytes(part[42:44],'big'),int.from_bytes(part[44:52],'big'),
            int.from_bytes(part[52:60],'big'),part[60]))
    assert [value[0] for value in result]==sorted({value[0] for value in result})
    return result


def shape(state):
    """Bound every retained record and independently recompute its logical charge."""
    rows=state['acquisition_quanta'];totals={};jobs={}
    for row in rows:
        assert row['quantum_profile']==1
        key=(row['producer_slot_id'],row['producer_epoch']);totals[key]=totals.get(key,0)+row['charged_bytes']
        job=next(value for value in state['jobs'] if value['job_id']==row['job_id'])
        detail=next(value for value in state['acquisition_jobs'] if value['job_id']==row['job_id'])
        assert row['dataset_id']==job['dataset_id']==detail['dataset_id'] and row['scope_id']==detail['scope_id']
        assert job['kind']=='acquisition' and job['acquisition_job_protocol']==1
        assert row['issuer_role']==job['owner_role']=='control'
        owner_key=lambda owner:(owner['dataset_id'],owner['resource_id'],owner['epoch'],owner['role'],owner['process_incarnation'])
        issuer=(row['dataset_id'],row['issuer_resource'],row['issuer_epoch'],'control',row['issuer_incarnation'])
        current=(job['dataset_id'],job['owner_resource_id'],job['owner_epoch'],'control',job['owner_incarnation'])
        original=[owner for owner in state['owner_epochs'] if owner_key(owner)==issuer]
        assert len(original)==1 and all(value is not None for value in current)
        assert sum(owner_key(owner)==current for owner in state['owner_epochs'])==1
        assert sum((producer['producer_slot_id'],producer['producer_epoch'],producer['dataset_id'])
            ==(row['producer_slot_id'],row['producer_epoch'],row['dataset_id'])
            for producer in state['producer_epochs'])==1
        gaps=[gap for gap in state['acquisition_gaps'] if gap['job_id']==row['job_id']]
        if gaps:
            # SQL0027/0036/0038 transfer only the terminal cancellation write.
            # SQL0043 retains the quantum's original Control issuer unchanged.
            assert len(gaps)==1
            gap=gaps[0]
            reason=('initial_producer_abandoned' if row['producer_epoch']==1 else
                'activated_initial_producer_abandoned' if row['producer_epoch']==2 else 'ingest_successor_abandoned')
            assert 1<=row['producer_epoch']<=255 and gap['owner_role']=='control'
            assert (gap['dataset_id'],gap['producer_slot_id'],gap['producer_epoch'],gap['source_owner_epoch'])==(
                row['dataset_id'],row['producer_slot_id'],row['producer_epoch'],row['producer_epoch'])
            assert (gap['dataset_id'],gap['previous_owner_resource'],gap['previous_owner_epoch'],
                'control',gap['previous_owner_incarnation'])==issuer
            assert (gap['dataset_id'],gap['issuer_resource'],gap['issuer_epoch'],
                'control',gap['issuer_incarnation'])==current
            assert original[0]['state']=='released' and original[0]['exclusion_method']=='storage_fenced'
            assert job['state']=='cancelled' and job['job_revision']==gap['previous_job_revision']+2
            assert job['checkpoint_revision']==0 and job['result_manifest_digest'] is None
            assert job['reason_code']==gap['reason']==reason
            assert job['completed_at_us']==gap['abandoned_at_us']>=job['created_at_us']
        else:
            assert issuer==current
        assert 0<=row['interval_lower']<row['interval_upper']<=row['horizon']<=job['created_at_us']//1_000_000
        predicate=raw(row['request_predicate']);command=json.loads(predicate)
        assert command[:2]==['REQ','0'*48] and len(command)>=3
        assert all(part['limit']==3 and row['interval_lower']<=part['since']<=part['until']<row['interval_upper'] for part in command[2:])
        assert hashlib.sha256(predicate).digest()==raw(row['predicate_sha256'])
        assert 0<=row['maximum_dependencies']<=768
        jobs.setdefault(row['job_id'],[]).append(row)
        if row['terminal_kind'] is None:
            charge=1024+len(predicate)+2*1024*1024+3*1024+256*row['maximum_dependencies']
            assert row['completed_at_us'] is None and row['dependencies'] is None and row['effect_records'] is None
        else:
            records=effects(row['effect_records']);refs=raw(row['dependencies'])
            assert len(refs)%72==0 and len(refs)//72<=row['maximum_dependencies']
            assert 0<=row['classified_prefix']<=row['retained_events']<=3
            assert all(1<=part[0]<=row['retained_events'] and part[1]>0 and part[3] in range(2,7) and part[6] in (0,1) for part in records)
            assert sum(part[6] for part in records)==row['classified_prefix']
            assert [part[0] for part in records if part[6]]==list(range(1,row['classified_prefix']+1))
            assert row['checkpoint_before']<=row['checkpoint_after']
            assert row['interval_lower']<row['continuation_upper']<=row['interval_upper']
            assert (row['result_class']=='interrupted')==(row['gap_reason'] is not None)
            frame=raw(row['terminal_frame'])
            if row['terminal_kind'] in ('eose','closed'):
                terminal=json.loads(frame)
                assert terminal[0].lower()==row['terminal_kind'] and terminal[1].encode()==raw(row['request_id'])
                assert row['terminal_sequence']>0
            else:assert frame==b'' and row['terminal_sequence'] is None
            charge=1024+len(predicate)+len(frame)+len(raw(row['effect_records']))+256*(len(refs)//72)
        assert charge==row['charged_bytes']
    for rows in jobs.values():
        assert sorted(row['quantum_number'] for row in rows)==list(range(1,len(rows)+1))
        pending=[row for row in rows if row['terminal_kind'] is None]
        assert len(pending)<=1 and (not pending or pending[0]['quantum_number']==len(rows))
    for (slot,epoch),charge in totals.items():
        inputs=sum(row['input_bytes']+65536 for row in state['producer_requests'] if (row['producer_slot_id'],row['producer_epoch'])==(slot,epoch))
        heads=[row for row in state['producer_checkpoints'] if (row['producer_slot_id'],row['producer_epoch'])==(slot,epoch)]
        head=heads[0] if heads else dict(progress_jobs=0,dependency_count=0,prefix_profile=epoch==1)
        assert inputs+charge+1024+1024*head['progress_jobs']+256*head['dependency_count']+(32768 if head['prefix_profile'] else 0)<=32*1024*1024


def transition(before,after):
    shape(after)
    current={row['quantum_id']:row for row in after['acquisition_quanta']}
    mutable={'charged_bytes','terminal_kind','terminal_frame','request_id','terminal_sequence','terminal_received_at_us',
        'teardown_complete','retained_events','classified_prefix','effect_records','dependencies','checkpoint_after',
        'checkpoint_sha256','result_class','gap_reason','continuation_upper','completed_at_us','xmin'}
    for old in before['acquisition_quanta']:
        assert old['quantum_id'] in current,'retained quantum history deleted'
        new=current[old['quantum_id']]
        if old['terminal_kind'] is not None:assert new==old
        else:assert {k:v for k,v in new.items() if k not in mutable}=={k:v for k,v in old.items() if k not in mutable}


def unchanged(before,after):
    """Every stopped obligation survives, including pending fields and row versions."""
    canonical=lambda row:json.dumps(row,sort_keys=True,allow_nan=False,separators=(',',':'))
    assert sorted(map(canonical,before['acquisition_quanta']))==sorted(map(canonical,after['acquisition_quanta'])), \
        'stopped quantum history changed'


def reference_bytes(references):
    return b''.join(source[0]+b''.join(number.to_bytes(8,'big') for number in source[1:])+digest
        for source,digest in sorted({(tuple(source),digest) for source,digest in references}))


def receipt_refs(receipt):
    from installed_family_science import decode
    return decode(receipt.split(b'\0',1)[1])[5]


def journey(state,wires,epoch,expected):
    """expected maps wire number to literal (event, classification, request, refs).

    Membership comes from the independent science encoder and declared workload;
    no observed counter, effect record or dependency selects an expectation.
    """
    shape(state)
    rows=sorted((row for row in state['acquisition_quanta'] if row['producer_epoch']==epoch),key=lambda row:row['quantum_number'])
    assert rows and len(wires)<=len(rows)<=len(wires)+1
    for number,wire in enumerate(wires,1):
        row=rows[number-1]
        job=next(job for job in state['jobs'] if job['job_id']==row['job_id'])
        window(row,job,wire)
        assert row['request_predicate']=='\\x'+wire_json(['REQ','0'*48,*wire['request'][2:]]).hex()
        assert row['quantum_number']==number
        if row['terminal_kind'] is None:
            assert number==len(rows);continue  # Unknown/local teardown remains pending.
        assert raw(row['request_id'])==wire['request'][1].encode()
        declared=expected.get(number,[])
        records=effects(row['effect_records'])
        wanted=[];refs=[]
        for ordinal,(event,kind,request,dependencies) in enumerate(declared,1):
            wanted.append((ordinal,ordinal,hashlib.sha256(wire_json(event)).digest(),kind,request,event['created_at'],1))
            refs.extend(dependencies)
        assert records==wanted
        assert raw(row['dependencies'])==reference_bytes(refs)
        if 'EOSE' in wire['frames']:
            assert row['retained_events']==len(wire['frames'])-1
            assert row['terminal_kind']=='eose' and json.loads(raw(row['terminal_frame']))==['EOSE',wire['request'][1]]
            assert row['terminal_sequence']==len(wire['frames'])
        else:assert not declared and row['terminal_kind'] in ('deadline','cancelled','disconnected')
        good=[event['created_at'] for event,kind,_,_ in declared if kind in (2,3,5)]
        ordered=all(later<=earlier for earlier,later in zip(good,good[1:]))
        continuous=row['terminal_kind']=='eose' and len(declared)==row['retained_events'] and ordered
        if 'EOSE' in wire['frames']:
            assert row['teardown_complete'] is True and wire['completion']=='request-and-websocket-close'
            gap='unclassified_suffix' if len(declared)<row['retained_events'] else 'ordering' if not ordered else None
            assert row['gap_reason']==gap and row['result_class']==('interrupted' if gap else 'linear_observed')
        else:
            assert row['teardown_complete'] is False and row['gap_reason']=='teardown' and row['result_class']=='interrupted'
        assert row['continuation_upper']==(min(good)+1 if good and continuous else row['interval_upper'])


def initial(state,wires,event,science,*,retained_events=None):
    receipts=science['receipts'];refs=lambda request:receipt_refs(receipts[request-1])
    if retained_events is None:
        forged=dict(event,sig='00'*64)
        expected={1:[(event,2,4,refs(4)),(forged,6,0,[]),(event,5,0,refs(4))],
            2:[(event,5,0,refs(4)),(forged,6,0,[]),(event,5,0,refs(4))]}
    else:
        assert len(retained_events)==62 and len(receipts)==64
        expected={number:[(value,2,4+(number-1)*3+index,refs(4+(number-1)*3+index))
            for index,value in enumerate(retained_events[(number-1)*3:number*3])] for number in range(1,21)}
        expected[21]=[(retained_events[60],2,64,refs(64))]
        proof=[wire['number'] for wire in wires if wire.get('retained_phase')=='proof-only']
        assert len(proof)==1
        expected[proof[0]]=[(retained_events[0],5,0,refs(4))]
    journey(state,wires,1,expected)


def successor(state,wires,old,new,science,*,before):
    assert all(row in state['acquisition_quanta'] for row in before['acquisition_quanta']), 'old quantum history changed'
    refs=lambda request:receipt_refs(science['receipts'][request-1])
    journey(state,wires,2,{1:[(new,2,2,refs(2)),(old,2,3,refs(3)),(new,5,0,refs(2))]})


def window(row,job,wire=None,*,selector=None,accepted_at_us=None):
    from installed_sync_client import quantum_window
    if accepted_at_us is not None:assert job['created_at_us']==int(accepted_at_us)
    lower,upper,horizon,filters=quantum_window(selector,job['created_at_us'])
    assert (row['interval_lower'],row['interval_upper'],row['horizon'])==(lower,upper,horizon)
    command=json.loads(raw(row['request_predicate']))
    assert command[:2]==['REQ','0'*48] and command[2:]==filters
    if wire is not None:
        assert wire['request'][0]=='REQ' and wire['request'][2:]==filters
        assert raw(row['request_predicate'])==wire_json(['REQ','0'*48,*wire['request'][2:]])
    return upper


# Literal peer scenarios; the cap fixtures deliberately omit the WebSocket close
# acknowledgement, making teardown failure an observed input rather than an
# unchecked alternative selected from the product's own result.
SCENARIO_RESULTS={
    'slow_eose':('eose',1,1,True,'linear_observed',None,2),
    'malformed_eose':('eose',2,0,True,'interrupted','unclassified_suffix',3),
    'frame_cap_eose':('frame_cap',0,0,False,'interrupted','teardown',None),
    'event_cap_eose':('event_cap',3,3,False,'interrupted','teardown',None),
    'closed':('closed',0,0,True,'interrupted','closed',1),
    'deadline':('deadline',0,0,False,'interrupted','teardown',None),
    'empty':('eose',0,0,True,'linear_observed',None,1),
    'proof':('eose',1,1,True,'linear_observed',None,2),
    'body_closed':('closed',1,1,True,'interrupted','closed',2),
}


def scenario(row,wire,job,event,refs,*,selector=None,accepted_at_us=None):
    upper=window(row,job,wire,selector=selector,accepted_at_us=accepted_at_us)
    terminal,retained,classified,teardown,result,gap,sequence=SCENARIO_RESULTS[wire['scenario']]
    assert (row['terminal_kind'],row['retained_events'],row['classified_prefix'],row['teardown_complete'],
        row['result_class'],row['gap_reason'],row['terminal_sequence'])==(terminal,retained,classified,teardown,result,gap,sequence)
    ident=wire['request'][1]
    assert raw(row['request_id'])==ident.encode()
    if terminal in ('eose','closed'):
        expected=['EOSE',ident] if terminal=='eose' else ['CLOSED',ident,'rate-limited']
        assert wire['terminal_frame_hex']==wire_json(expected).hex()
        assert raw(row['terminal_frame'])==wire_json(expected)
    else:assert raw(row['terminal_frame'])==b''
    close=wire['teardown']
    assert set(close)=={'request_close','websocket_close','ack_sent','eof'} and close['eof'] is True
    if teardown:
        assert close==dict(request_close=terminal!='closed',websocket_close=True,ack_sent=True,eof=True)
        assert wire['completion']=='request-and-websocket-close'
    else:
        assert close['ack_sent'] is False and wire['completion']=='interrupted-socket-close'
        if terminal=='deadline':assert close['request_close'] is close['websocket_close'] is False
    if wire['scenario']=='frame_cap_eose':assert wire['frames']==['NOTICE']*33+['EOSE']
    if wire['scenario']=='event_cap_eose':assert wire['frames']==['EVENT']*4+['EOSE']
    if wire['scenario']=='malformed_eose':assert wire['frames']==['EVENT','EVENT','EOSE']
    records=effects(row['effect_records'])
    assert len(records)==classified
    assert raw(row['dependencies'])==(refs if classified else b'')
    for ordinal,record in enumerate(records,1):
        kind,request=(2,4) if wire['scenario'] in ('slow_eose','body_closed') else (5,0)
        assert record==(ordinal,ordinal,hashlib.sha256(wire_json(event)).digest(),kind,request,event['created_at'],1)
    assert row['continuation_upper']==(event['created_at']+1 if classified and gap is None else upper)
    return classified


def terminal_authority_diagnostic(text):
    assert 'initial Sync unavailable: Source(Unavailable); sync_dispatch=' in text
    report,_=json.JSONDecoder().raw_decode(text.split('sync_dispatch=',1)[1])
    assert report['stage']=='quantum-terminal' and report['reason']=='operation-error'
    assert report['returned_us'] is not None and report['entered_us']<=report['returned_us']<report['dispatch_deadline_us']
    return report


def empty_authority(before,after,wire,completed,held,delivered):
    assert completed['server_ready'] is True and completed['backend_pid']==held['backend_pid']==delivered['backend_pid']
    assert held['stage']=='source-rollback-reply' and held['source_queries_complete']==held['source_server_rollback']==1
    assert held['source_client_rollback']==0 and delivered['source_server_rollback']==delivered['source_client_rollback']==1
    assert wire['scenario']=='empty' and wire['frames']==['EOSE']
    assert wire['terminal_frame_hex']==wire_json(['EOSE',wire['request'][1]]).hex()
    assert wire['teardown']==dict(request_close=True,websocket_close=True,ack_sent=True,eof=True)
    assert wire['completion']=='request-and-websocket-close'
    plan,=before['acquisition_quanta']
    assert plan['terminal_kind'] is plan['continuation_upper'] is plan['result_class'] is plan['gap_reason'] is None
    assert plan['charged_bytes']==1024+len(raw(plan['request_predicate']))+2097152+3072+256*plan['maximum_dependencies']
    for table,old in before.items():
        canonical=lambda row:json.dumps(row,sort_keys=True,separators=(',',':'))
        assert sorted(after[table],key=canonical)==sorted(old,key=canonical),'empty terminal changed retained '+table
    assert len(after['producer_requests'])==len(after['producer_outcomes'])==3 and not after['acquisition_progress']
