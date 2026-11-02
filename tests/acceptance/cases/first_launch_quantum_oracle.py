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
    value=raw(value);assert len(value)%93==0 and len(value)<=3*93
    result=[]
    for at in range(0,len(value),93):
        part=value[at:at+93]
        result.append((int.from_bytes(part[:2],'big'),int.from_bytes(part[2:10],'big'),part[10:42],
            int.from_bytes(part[42:44],'big'),int.from_bytes(part[44:52],'big'),
            int.from_bytes(part[52:60],'big'),part[60],part[61:93]))
    assert [value[0] for value in result]==sorted({value[0] for value in result})
    return result


def continuation_owner(state,row,job):
    """Authenticate a historical issuer through complete finalized segments.

    A seal alone is insufficient. Each segment needs the closed old source,
    actual committed succession/activation and one exact job-owner CAS. Native
    page authentication remains the separate complete native-census oracle.
    """
    from cases.first_launch_restart import only
    seals=sorted((value for value in state.get('acquisition_continuations',[]) if value['job_id']==job['job_id'] and value['adopted_at_us'] is not None),
        key=lambda value:value['previous_job_revision'])
    assert seals and len({value['continuation_id'] for value in seals})==len(seals)
    owner=lambda value,prefix:(value[prefix+'resource'],value[prefix+'epoch'],value[prefix+'incarnation'])
    prior=None;covered=False
    for seal in seals:
        assert seal['dataset_id']==job['dataset_id']==row['dataset_id'] and seal['scope_id']==row['scope_id']
        assert seal['previous_continuation']==(None if prior is None else prior['continuation_id'])
        assert seal['previous_job_revision']==(2 if prior is None else prior['adopted_job_revision'])
        if prior is not None:
            assert owner(seal,'previous_owner_')==owner(prior,'adopting_')
            assert seal['producer_epoch']==prior['target_epoch'] and seal['manifest_sha256']==prior['target_manifest_sha256']
            assert seal['last_quantum_number']>=prior['last_quantum_number']
        assert seal['adopted_job_revision']==seal['previous_job_revision']+1
        assert seal['target_epoch']==seal['producer_epoch']+1
        assert seal['sealed_at_us']<=seal['adopted_at_us']<only(state['acquisition_jobs'],job_id=job['job_id'])['deadline_at_us']
        assert all(seal[key] is not None for key in ('target_manifest_sha256','activation_sha256','adoption_transaction'))
        previous=only(state['owner_epochs'],dataset_id=job['dataset_id'],role='control',
            resource_id=seal['previous_owner_resource'],epoch=seal['previous_owner_epoch'],process_incarnation=seal['previous_owner_incarnation'])
        assert previous['state']=='released' and previous['exclusion_method']=='storage_fenced'
        current=only(state['owner_epochs'],dataset_id=job['dataset_id'],role='control',
            resource_id=seal['adopting_resource'],epoch=seal['adopting_epoch'],process_incarnation=seal['adopting_incarnation'])
        assert current['epoch']>previous['epoch'] and current['resource_id']==previous['resource_id']
        producer=only(state['producer_epochs'],dataset_id=job['dataset_id'],producer_slot_id=seal['producer_slot_id'],producer_epoch=seal['producer_epoch'])
        stream=only(state['source_streams'],dataset_id=job['dataset_id'],shard_id=seal['shard_id'],owner_epoch=seal['producer_epoch'])
        assert producer['state']=='retired' and stream['state']=='closed'
        assert producer['reserved_through_request']==seal['complete_through_request']<=64
        assert stream['last_complete_sequence']==seal['source_sequence']
        assert producer['closure_digest']==stream['closure_digest']==seal['source_digest']
        heads=[value for value in state['producer_checkpoints'] if (value['producer_slot_id'],value['producer_epoch'])==(
            seal['producer_slot_id'],seal['producer_epoch'])]
        if seal['checkpoint_revision']==0:
            assert not heads and seal['complete_through_request']==0 and seal['checkpoint_sha256'] is None
        else:
            head,=heads
            assert (head['checkpoint_revision'],head['complete_through_request'],head['checkpoint_digest'])==(
                seal['checkpoint_revision'],seal['complete_through_request'],seal['checkpoint_sha256'])
        initial=seal['target_epoch']==2
        intent=only(state['initial_ingest_successions'] if initial else state['ingest_successions'],
            producer_slot_id=seal['producer_slot_id'],target_epoch=seal['target_epoch'])
        activation=only(state['initial_ingest_activations'] if initial else state['ingest_activations'],
            producer_slot_id=seal['producer_slot_id'],target_epoch=seal['target_epoch'])
        assert intent['dataset_id']==activation['dataset_id']==seal['dataset_id']
        assert intent['previous_manifest_sha256']==seal['manifest_sha256']
        assert intent['target_manifest_sha256']==activation['target_manifest_sha256']==seal['target_manifest_sha256']
        assert intent['handoff_sha256']==activation['handoff_sha256']==seal['activation_sha256']
        assert raw(seal['manifest_sha256'])==hashlib.sha256(raw(intent['previous_manifest'])).digest()
        assert raw(seal['target_manifest_sha256'])==hashlib.sha256(raw(intent['target_manifest'])).digest()
        assert raw(seal['activation_sha256'])==hashlib.sha256(raw(intent['handoff'])).digest()
        assert (intent['issued_through'],intent['last_complete_sequence'],intent['closure_digest'])==(
            seal['complete_through_request'],seal['source_sequence'],seal['source_digest'])
        assert (intent['config_revision'],intent['policy_revision'])==(job['config_revision'],job['policy_revision'])
        assert (activation['activated_by_resource'],activation['activated_by_epoch'],activation['activated_by_incarnation'])==owner(seal,'adopting_')
        assert activation['activated_at_us']<=seal['adopted_at_us']
        target=only(state['producer_epochs'],dataset_id=job['dataset_id'],producer_slot_id=seal['producer_slot_id'],producer_epoch=seal['target_epoch'])
        assert (target['owner_resource_id'],target['owner_epoch'],target['owner_incarnation'])==(
            intent['owner_resource_id'],intent['target_epoch'],intent['target_incarnation'])
        lower=0 if prior is None else prior['last_quantum_number']
        segment=[value for value in state['acquisition_quanta'] if value['job_id']==job['job_id']
            and (value['producer_slot_id'],value['producer_epoch'])==(seal['producer_slot_id'],seal['producer_epoch'])]
        assert all(lower<value['quantum_number']<=seal['last_quantum_number'] and value['disposition'] is not None
            and value['completed_at_us'] is not None and (value['entry_kind']!='request' or value['terminal_kind'] is not None)
            for value in segment),'a pending/unknown quantum cannot be sealed'
        assert max((value['quantum_number'] for value in segment),default=lower)==seal['last_quantum_number']
        if (row['producer_slot_id'],row['producer_epoch'])==(seal['producer_slot_id'],seal['producer_epoch']):
            assert lower<row['quantum_number']<=seal['last_quantum_number']
            assert (row['issuer_resource'],row['issuer_epoch'],row['issuer_incarnation'])==owner(seal,'previous_owner_')
            covered=True
        prior=seal
    assert covered,'historical issuer has no finalized segment'
    assert (job['owner_resource_id'],job['owner_epoch'],job['owner_incarnation'])==owner(prior,'adopting_')
    assert job['job_revision']==prior['adopted_job_revision'] and job['state']=='queued'
    assert int(job['xmin'])==int(prior['adoption_transaction'])%(2**32),'adoption and owner CAS were not atomic'


def shape(state):
    """Bound every retained record and independently recompute its logical charge."""
    rows=state['acquisition_quanta'];totals={};jobs={}
    for row in rows:
        assert type(row['quantum_profile']) is int and row['quantum_profile']==2
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
        elif issuer!=current:
            continuation_owner(state,row,job)
        integer_fields=('quantum_number','producer_epoch','issuer_epoch','interval_lower','interval_upper',
            'window_end','horizon','window_width','query_budget','query_number','maximum_dependencies',
            'checkpoint_before','classified_before','charged_bytes')
        assert all(type(row[key]) is int for key in integer_fields)
        assert 1<=row['query_budget']<=128 and 0<=row['query_number']<=row['query_budget']
        assert row['window_width']>0 and 0<=row['horizon']<=job['created_at_us']//1_000_000
        jobs.setdefault(row['job_id'],[]).append(row)
        if row['entry_kind']=='selector_exclusion':
            assert row['disposition'] in ('selector_excluded','empty_target')
            assert row['result_class']==row['disposition'] and row['completed_at_us'] is not None
            assert row['checkpoint_after']==row['checkpoint_before'] and row['maximum_dependencies']==0
            assert all(row[key] is None for key in ('request_predicate','predicate_sha256','terminal_kind',
                'terminal_frame','request_id','terminal_sequence','terminal_received_at_us','teardown_complete',
                'retained_events','classified_prefix','effect_records','dependencies','gap_reason',
                'continuation_upper','gap_lower','gap_upper'))
            if row['disposition']=='empty_target':
                assert row['interval_lower']==row['interval_upper']==row['window_end']==row['horizon']
            else:assert 0<=row['interval_lower']<row['interval_upper']<=row['window_end']<=row['horizon']
            charge=1024
        else:
            assert row['entry_kind']=='request' and row['query_number']>0
            assert 0<=row['interval_lower']<row['interval_upper']<=row['window_end']<=row['horizon']
            predicate=raw(row['request_predicate']);command=json.loads(predicate)
            assert command[:2]==['REQ','0'*48] and len(command)>=3
            assert all(type(part.get('limit')) is int and part['limit']==3
                and type(part.get('since')) is int and type(part.get('until')) is int
                and row['interval_lower']<=part['since']<=part['until']<row['interval_upper'] for part in command[2:])
            assert hashlib.sha256(predicate).digest()==raw(row['predicate_sha256'])
            assert 0<=row['maximum_dependencies']<=768
            if row['terminal_kind'] is None:
                charge=1024+len(predicate)+2*1024*1024+3*1024+256*row['maximum_dependencies']
                assert row['completed_at_us'] is None
                assert all(row[key] is None for key in ('terminal_frame','request_id','terminal_sequence',
                    'terminal_received_at_us','checkpoint_sha256','teardown_complete','retained_events','classified_prefix','effect_records','dependencies',
                    'checkpoint_after','result_class','gap_reason','continuation_upper','disposition','gap_lower','gap_upper'))
            else:
                records=effects(row['effect_records']);refs=raw(row['dependencies'])
                assert len(refs)%72==0 and len(refs)//72<=row['maximum_dependencies']
                assert type(row['classified_prefix']) is int and type(row['retained_events']) is int
                assert 0<=row['classified_prefix']<=row['retained_events']<=3
                assert all(1<=part[0]<=row['retained_events'] and part[1]>0 and part[3] in range(2,7) and part[6] in (0,1) for part in records)
                assert sum(part[6] for part in records)==row['classified_prefix']
                assert [part[0] for part in records if part[6]]==list(range(1,row['classified_prefix']+1))
                assert row['checkpoint_before']<=row['checkpoint_after'] and row['completed_at_us'] is not None
                disposition=row['disposition'];continuation=row['continuation_upper']
                assert type(continuation) is int
                if disposition=='retry':
                    assert continuation==row['interval_upper'] and row['result_class']=='interrupted'
                    assert row['gap_reason'] is not None and row['gap_reason']!='saturated_second'
                elif disposition=='overlap':
                    assert row['interval_lower']<continuation<=row['interval_upper'] and row['gap_reason'] is None
                elif disposition=='window_closed':
                    assert continuation==row['window_end'] and row['gap_reason'] is None and row['retained_events']==0
                else:
                    assert disposition=='saturated_second' and row['gap_reason']=='saturated_second'
                    assert type(row['gap_lower']) is int and type(row['gap_upper']) is int
                    assert row['interval_lower']<=row['gap_lower']<row['interval_upper']
                    assert continuation==row['gap_lower'] and row['gap_upper']==row['gap_lower']+1
                if disposition!='saturated_second':assert row['gap_lower'] is row['gap_upper'] is None
                assert row['result_class'] in ('linear_observed','relay_reported_exhaustion','interrupted')
                assert (row['result_class']=='interrupted')==(disposition=='retry')
                frame=raw(row['terminal_frame'])
                if row['terminal_kind'] in ('eose','closed'):
                    terminal=json.loads(frame)
                    assert terminal[0].lower()==row['terminal_kind'] and terminal[1].encode()==raw(row['request_id'])
                    assert row['terminal_sequence']>0
                else:assert frame==b'' and row['terminal_sequence'] is None
                charge=1024+len(predicate)+len(frame)+len(raw(row['effect_records']))+256*(len(refs)//72)
        assert charge==row['charged_bytes']
    for rows in jobs.values():
        rows=sorted(rows,key=lambda row:row['quantum_number'])
        assert [row['quantum_number'] for row in rows]==list(range(1,len(rows)+1))
        frozen={(row['horizon'],row['window_width'],row['query_budget']) for row in rows};assert len(frozen)==1
        assert len(rows)<=3*rows[0]['query_budget']+1
        spent=0
        for row in rows:
            spent+=row['entry_kind']=='request'
            assert row['query_number']==spent<=row['query_budget']
        pending=[row for row in rows if row['entry_kind']=='request' and row['terminal_kind'] is None]
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
        'checkpoint_sha256','result_class','gap_reason','continuation_upper','disposition','gap_lower','gap_upper','completed_at_us','xmin'}
    for old in before['acquisition_quanta']:
        assert old['quantum_id'] in current,'retained quantum history deleted'
        new=current[old['quantum_id']]
        if old['entry_kind']=='selector_exclusion' or old['terminal_kind'] is not None:assert new==old
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


def journey(state,wires,epoch,expected,*,selector=None,entries=None,width=86400,budget=128):
    """Validate literal entry geometry and independently declared event effects.

    Request tuples are ('request',a,c,b); local tuples append their disposition.
    Effects are keyed by per-job request ordinal, independently of local entries.
    No SQL effect, counter, cursor or gap chooses an expected scientific value.
    """
    from installed_sync_client import COMMON_SELECTOR,frozen_horizon
    selector=selector or COMMON_SELECTOR
    shape(state)
    assert epoch is None or type(epoch) is int
    rows=sorted((row for row in state['acquisition_quanta'] if epoch is None or row['producer_epoch']==epoch),key=lambda row:row['quantum_number'])
    assert rows
    job=next(job for job in state['jobs'] if job['job_id']==rows[0]['job_id'])
    horizon=frozen_horizon(selector,job['created_at_us']);origin=selector['history_since']
    first_end=min(origin+width,horizon)
    entries=(('request',origin,first_end,first_end),) if entries is None else tuple(entries)
    assert len(rows)==len(entries),'declared entry count differs: missing work or extra plan'
    request_count=sum(entry[0]=='request' for entry in entries)
    assert len(wires)<=request_count<=len(wires)+1
    assert all(type(n) is int and 1<=n<=request_count for n in expected)
    assert all(len(entry) in (4,5) and all(type(value) is int for value in entry[1:4]) for entry in entries)
    lower=origin;end=min(origin+width,horizon);upper=max(lower,end)
    boundary=None;ids=set();spent=0;attempted=min(origin,horizon);gaps=0;stopped=False;pending=False
    for number,(row,entry) in enumerate(zip(rows,entries),1):
        assert not stopped,'stopped bounded traversal retained another entry'
        kind,a,c,b=entry[:4]
        assert row['quantum_number']==number and row['job_id']==job['job_id']
        assert (row['entry_kind'],row['interval_lower'],row['interval_upper'],row['window_end'])==(kind,a,c,b)
        assert (row['horizon'],row['window_width'],row['query_budget'])==(horizon,width,budget)
        if kind=='selector_exclusion':
            assert len(entry)==5 and row['query_number']==spent
            disposition=entry[4];assert row['disposition']==disposition
            if disposition=='empty_target':
                assert origin>=horizon and (a,c,b)==(horizon,horizon,horizon) and spent==0
                stopped=True
            else:
                assert disposition=='selector_excluded' and a==lower and a<c<=b<=horizon
                assert all(max(a,part.get('since',a))>=min(c,part.get('until',c-1)+1) for part in selector['filters']), \
                    'local exclusion intersects accepted selector time'
                if upper<end:assert (c,b)==(upper,end),'excluded older remainder changed original window end'
                else:assert c==b and b>=end and (b==horizon or (b-origin)%width==0)
                attempted=b;lower=b;end=min(b+width,horizon);upper=end;stopped=b==horizon
            boundary=None;ids.clear();continue
        assert kind=='request' and len(entry)==4 and (a,c,b)==(lower,upper,end)
        assert spent<budget,'query budget authorized an extra request'
        spent+=1;assert row['query_number']==spent
        wire=wires[spent-1] if spent<=len(wires) else None
        window(row,job,wire,selector=selector,expected_lower=a,expected_upper=c,window_end=b,width=width,budget=budget)
        if wire is None or row['terminal_kind'] is None:
            assert row['terminal_kind'] is None and number==len(rows)
            pending=True;continue
        assert raw(row['request_id'])==wire['request'][1].encode()
        declared=expected.get(spent,[]);wanted=[];refs=[]
        for ordinal,(event,classification,request,dependencies) in enumerate(declared,1):
            wanted.append((ordinal,ordinal,hashlib.sha256(wire_json(event)).digest(),classification,request,
                event['created_at'],1,bytes.fromhex(event['id'])))
            refs.extend(dependencies)
        assert effects(row['effect_records'])==wanted
        assert raw(row['dependencies'])==reference_bytes(refs)
        terminal=wire['frames'][-1] if wire['frames'] else None
        if terminal in ('EOSE','CLOSED'):
            assert row['retained_events']==len(wire['frames'])-1
            assert row['terminal_kind']==terminal.lower() and row['teardown_complete'] is True
            assert wire['completion']=='request-and-websocket-close'
            frame=['EOSE',wire['request'][1]] if terminal=='EOSE' else ['CLOSED',wire['request'][1],'fixture setup interruption']
            assert json.loads(raw(row['terminal_frame']))==frame and row['terminal_sequence']==len(wire['frames'])
        else:
            assert not declared and row['terminal_kind'] in ('deadline','cancelled','disconnected')
            assert row['teardown_complete'] is False
        good=[event for event,classification,_,_ in declared if classification in (2,3,5)]
        ordered=all(later['created_at']<=earlier['created_at'] for earlier,later in zip(good,good[1:]))
        gap=('teardown' if not row['teardown_complete'] else 'unclassified_suffix' if len(declared)<row['retained_events']
            else 'ordering' if not ordered else 'closed' if terminal=='CLOSED' else None)
        continuation=c;disposition='retry';gap_span=None
        if gap is not None:boundary=None;ids.clear()
        elif row['retained_events']==0:
            disposition='window_closed';continuation=b
            attempted=b;lower=b;end=min(b+width,horizon);upper=end;stopped=b==horizon
            boundary=None;ids.clear()
        elif not good:
            gap='no_verified_boundary';boundary=None;ids.clear()
        else:
            oldest=min(event['created_at'] for event in good)
            current={event['id'] for event in good if event['created_at']==oldest}
            if oldest!=boundary:boundary=oldest;ids.clear()
            if current<=ids:
                disposition='saturated_second';gap='saturated_second';gap_span=(oldest,oldest+1)
                continuation=oldest;gaps+=1;boundary=None;ids.clear()
                if oldest==lower:
                    attempted=b;lower=b;end=min(b+width,horizon);upper=end;stopped=b==horizon
                else:upper=oldest
            else:
                disposition='overlap';continuation=oldest+1;upper=continuation;ids|=current
        result='interrupted' if disposition=='retry' else 'linear_observed'
        assert (row['disposition'],row['continuation_upper'],row['result_class'],row['gap_reason'])==(disposition,continuation,result,gap)
        assert (row['gap_lower'],row['gap_upper'])==(gap_span if gap_span else (None,None))
    return dict(queries_spent=spent,attempted_prefix=attempted,gap_count=gaps,
        pending_work='request_pending' if pending else 'settled' if stopped else 'budget_deferred' if spent==budget else 'query_ready',
        pending_interval=None if stopped else [str(lower),str(upper)])


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
    if retained_events is None:
        from installed_sync_client import INITIAL_ENTRIES
        entries=INITIAL_ENTRIES
    else:
        proof_number=proof[0]
        entries=tuple(('request',0,86400 if n<=proof_number else 2,86400) for n in range(1,129))
    journey(state,wires,1,expected,entries=entries)


def successor(state,wires,old,new,science,*,before,provenance,first_science=None):
    assert all(row in state['acquisition_quanta'] for row in before['acquisition_quanta']), 'old quantum history changed'
    assert type(provenance) is bool
    assert len(science['receipts'])==(3 if provenance else 2)
    refs=lambda request:receipt_refs(science['receipts'][request-1])
    if provenance:
        old_effect=(old,2,3,refs(3))
    else:
        assert first_science is not None and len(first_science['receipts'])==4
        # ExistingProof retains the stored event/receive descriptors and this
        # input's current observer. The new event's receipt is not its proof.
        dependencies=receipt_refs(first_science['receipts'][3])+refs(1)
        dependencies=[[list(source),digest] for source,digest in sorted(
            {(tuple(source),digest) for source,digest in dependencies})]
        old_effect=(old,5,0,dependencies)
    journey(state,wires,2,{1:[(new,2,2,refs(2)),old_effect,(new,5,0,refs(2))]},
        entries=tuple(('request',0,86400,86400) for _ in wires))


def window(row,job,wire=None,*,selector=None,accepted_at_us=None,expected_upper=None,expected_lower=None,window_end=None,width=86400,budget=128):
    from installed_sync_client import quantum_window
    if accepted_at_us is not None:assert job['created_at_us']==int(accepted_at_us)
    lower,upper,horizon,filters=quantum_window(selector,job['created_at_us'],expected_upper=expected_upper,
        expected_lower=expected_lower,window_end=window_end,width=width)
    assert (row['interval_lower'],row['interval_upper'],row['horizon'])==(lower,upper,horizon)
    declared_end=min(lower+width,horizon) if window_end is None else window_end
    assert type(width) is int and type(budget) is int
    assert all(type(row[key]) is int for key in ('window_end','window_width','query_budget'))
    assert (row['window_end'],row['window_width'],row['query_budget'])==(declared_end,width,budget)
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
    'proof':('eose',1,1,True,'linear_observed','saturated_second',2),
    'body_closed':('closed',1,1,True,'interrupted','closed',2),
}


def scenarios(state,wires,event,refs,declarations):
    """Validate the two literal interruption recipes under any legal interleaving."""
    recipes=(('slow_eose','proof','empty'),
        ('malformed_eose','frame_cap_eose','event_cap_eose','closed','deadline','empty'))
    shape(state)
    rows=state['acquisition_quanta']
    assert len(rows)==len(wires)==9 and all(row['terminal_kind'] is not None for row in rows)
    assert len(declarations)==2
    by_request={raw(row['request_id']).decode():row for row in rows}
    assert len(by_request)==9 and len({wire['request'][1] for wire in wires})==9
    turns={0:0,1:0};totals={}
    for number,wire in enumerate(wires,1):
        assert wire['number']==number and len(wire['request'])==3
        scope=wire['request'][2].get('since')
        assert type(scope) is int and scope in (0,1) and (number!=1 or scope==0)
        turns[scope]+=1;ordinal=turns[scope]
        assert ordinal<=len(recipes[scope]) and wire['scenario']==recipes[scope][ordinal-1]
        assert wire['scope_number']==scope and wire['quantum_number']==ordinal
        upper=86400 if scope==1 else (86400,2,1)[ordinal-1]
        assert wire['request'][2]==dict(kinds=[1],since=scope,until=upper-1,limit=3)
        row=by_request[wire['request'][1]]
        job=next(job for job in state['jobs'] if job['job_id']==row['job_id'])
        declared=declarations[job['job_id'][2:]]
        assert row['scope_id']=='\\x'+declared['scope_id'] and row['quantum_number']==ordinal
        assert declared['selector']['filters'][0].get('since',0)==scope
        classified=scenario(row,wire,job,event,refs,selector=declared['selector'],
            accepted_at_us=declared['accepted_at_us'],expected_upper=upper)
        totals[row['job_id']]=totals.get(row['job_id'],0)+classified
    assert turns=={0:3,1:6}
    return totals


def scenario(row,wire,job,event,refs,*,selector=None,accepted_at_us=None,expected_upper=None):
    upper=window(row,job,wire,selector=selector,accepted_at_us=accepted_at_us,expected_upper=expected_upper)
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
        assert record==(ordinal,ordinal,hashlib.sha256(wire_json(event)).digest(),kind,request,event['created_at'],1,bytes.fromhex(event['id']))
    disposition=('saturated_second' if wire['scenario']=='proof' else 'window_closed' if wire['scenario']=='empty'
        else 'overlap' if wire['scenario']=='slow_eose' else 'retry')
    continuation=(event['created_at'] if disposition=='saturated_second' else row['window_end'] if disposition=='window_closed'
        else event['created_at']+1 if disposition=='overlap' else upper)
    assert row['disposition']==disposition and row['continuation_upper']==continuation
    assert (row['gap_lower'],row['gap_upper'])==((event['created_at'],event['created_at']+1) if disposition=='saturated_second' else (None,None))
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
