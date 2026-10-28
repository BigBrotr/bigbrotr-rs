"""Literal finite64 boundary: independent input/native/typed progress science."""
import hashlib
import json
import re

from cases import first_launch_index_oracle as index
from cases import first_launch_restart as source


def boundary(state, wire, event, *, proof=False):
    """The live gate must precede proof release; a final row cannot replace it."""
    assert type(proof) is bool
    producer, = state['producer_epochs']
    assert producer['producer_epoch']==1 and producer['reserved_through_request']==64
    assert producer['outstanding_request_limit']==32
    for table in ('producer_requests','producer_outcomes'):
        assert sorted(row['request_sequence'] for row in state[table])==list(range(1,65))
        assert all(row['producer_epoch']==1 for row in state[table])
    progress, = state['acquisition_progress']
    expected = dict(progress_revision=62 if proof else 61,classified_events=62 if proof else 61,
        successful_events=62 if proof else 61,rejected_events=0,
        request_id='\\x'+wire['request'][1].encode().hex(),event_ordinal=1,wire_sequence=1,
        input_sha256='\\x'+hashlib.sha256(json.dumps(event,ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),
        classification=5 if proof else 2,mutation_request=None if proof else 64)
    index.exact({key:progress[key] for key in expected},expected)
    head, = state['producer_checkpoints']
    assert head['complete_through_request']==64 and head['checkpoint_revision']==(65 if proof else 64)
    assert head['progress_jobs']==1 and head['dependency_count']==312 and head['xmin']==progress['xmin']
    if not proof:
        assert head['xmin']==index.only(state['producer_outcomes'],request_sequence=64)['xmin']
    return head


def wires_checked(wires, events):
    assert 22<=len(wires)<=128
    previous=None;nonces=set();proof=[];empties=0
    for number,wire in enumerate(wires,1):
        assert wire['number']==number and wire['request'][0]=='REQ'
        assert wire['request'][2]=={'kinds':[1],'limit':3}
        ident=wire['request'][1]
        assert re.fullmatch('[0-9a-f]{48}',ident) and int(ident[:32],16)
        assert ident[:32] not in nonces;nonces.add(ident[:32])
        assert wire['connected_at_us']<=wire['request_at_us']<=wire['closed_at_us']
        assert previous is None or previous<=wire['connected_at_us']
        previous=wire['closed_at_us']
        assert 0<=wire['closed_at_us']-wire['request_at_us']<3_000_000
        assert type(wire['clock_resolution_us']) is int and wire['clock_resolution_us']>=1
        if number<=20:
            phase,values='admission',events[(number-1)*3:number*3]
        elif number==21:
            phase,values='saturation',[events[60],events[61],events[0]]
        elif wire.get('retained_phase')=='proof-only':
            phase,values='proof-only',[events[0]];proof.append(wire)
        elif wire.get('retained_phase')=='empty':
            phase,values='empty',[];empties+=1
        else:
            assert wire['retained_quiet'] is True and wire['frames']==[] and wire['completion']=='quiet-eof'
            continue
        assert wire['retained_quiet'] is False and wire['retained_phase']==phase
        assert wire['completion']=='request-and-websocket-close'
        assert wire['frames']==['signed']*len(values)+['EOSE']
        assert wire['event_ids']==[event['id'] for event in values]
        assert wire['event_json_sha256']==[hashlib.sha256(json.dumps(event,ensure_ascii=False,
            separators=(',',':')).encode()).hexdigest() for event in values]
        assert wire['request_at_us']<=wire['sent_at_us']<=wire['closed_at_us']
    assert len(proof)==1 and empties<=100
    return wires[20],proof[0]


def progress(state, wanted, events, wire, control, completed, *, proof):
    from installed_progress_context_oracle import typed
    from installed_sync_oracle import decode
    head=boundary(state,wire,events[0 if proof else 60],proof=proof)
    producer, = state['producer_epochs']
    job, = state['jobs']; detail, = state['acquisition_jobs']
    assert job['state']=='queued' and job['checkpoint_revision']==0 and job['request_key']=='\\x'+'b1'*16
    assert detail['job_id']==job['job_id']
    actual, = state['acquisition_progress']
    common={key:producer[key] for key in ('dataset_id','producer_slot_id','producer_epoch')}
    expected=dict(common,job_id=job['job_id'],scope_id=detail['scope_id'],
        progress_revision=62 if proof else 61,request_id='\\x'+wire['request'][1].encode().hex(),
        event_ordinal=1,wire_sequence=1,input_sha256='\\x'+source.sha(json.dumps(
            events[0 if proof else 60],ensure_ascii=False,separators=(',',':')).encode()),
        classification=5 if proof else 2,mutation_request=None if proof else 64,
        classified_events=62 if proof else 61,successful_events=62 if proof else 61,rejected_events=0,
        observed_at_us=actual['observed_at_us'],clock_resolution_us=wire['clock_resolution_us'],
        clock_anomaly_flags=4 if wire['clock_resolution_us']>1 else 0,xmin=actual['xmin'])
    dataset=index.raw(common['dataset_id'],16)
    domains={r['dictionary_kind']:index.raw(r['allocation_domain'],16) for r in state['allocation_domains']}
    ref=lambda kind,ordinal=1:[dataset,kind,domains[kind],ordinal]
    if proof:
        intent=typed.certification_digest(events[0],ref(4,3),ref(2),ref(2),
            [ref(1),ref(3),ref(5)],expected,wire).hex()
        assert all(actual['xmin']!=r['xmin'] for r in state['producer_outcomes'])
    else:
        intent=source.sha(wanted['inputs'][63])
        observation=decode(wanted['inputs'][63].split(b'\0',1)[1])[3][5]
        assert [expected['observed_at_us'],expected['clock_resolution_us'],expected['clock_anomaly_flags']]==[
            observation[0],observation[2][1],observation[2][3]]
    expected['intent_digest']='\\x'+intent
    source.clock(head['committed_at_us'],wire['sent_at_us'],completed)
    assert expected['observed_at_us']<=head['committed_at_us']<control['lease_expires_at_us']
    values={(cf,key.hex()):value.hex() for cf,key,value in wanted['native']}
    dependencies=[]
    receipts=[decode(receipt.split(b'\0',1)[1])[5] for receipt in wanted['receipts']]
    for n in range(1,313):
        key=b'\x01\x20'+dataset+index.be(0,4)+index.be(1)+index.be(n)
        value=bytes.fromhex(values['facts',key.hex()])
        reference=[[dataset,0,1,n],value[2:34]]
        first=next(k for k,refs in enumerate(receipts,1) if reference in refs)
        dependencies.append(dict(common,source_shard=0,source_epoch=1,source_sequence=n,
            payload_digest='\\x'+value[2:34].hex(),
            xmin=index.only(state['producer_outcomes'],request_sequence=first)['xmin']))
    expected_head=dict(common,progress_format=1,manifest_sha256='\\x'+source.sha(bytes.fromhex(values['default','0101'])),
        checkpoint_revision=65 if proof else 64,complete_through_request=64,progress_jobs=1,dependency_count=312,
        issuer_resource=control['resource_id'],issuer_epoch=control['epoch'],issuer_incarnation=control['process_incarnation'],
        committed_at_us=head['committed_at_us'],xmin=head['xmin'],checkpoint_digest='\\x'+'00'*32)
    expected_head['checkpoint_digest']='\\x'+typed.head_digest(expected_head,[expected],dependencies,
        lambda row:typed.native_reference(row,values)).hex()
    return typed.exact_snapshot(head,state['acquisition_progress'],sorted(state['producer_checkpoint_dependencies'],
        key=lambda row:(row['source_shard'],row['source_epoch'],row['source_sequence'])),
        expected_head,[expected],dependencies,values)


def science(state, before_proof, report, events, wires, dump, store, completed):
    from installed_archive_oracle import expected
    from installed_sync_oracle import decode
    from bootstrap_oracle import cbor
    saturation,proof=wires_checked(wires,events)
    original=source.originals(store,report['producer_slot_id'],1,64)
    actual=[(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in dump['rows']]
    assert dump['event']=='dump-state' and dump['sequence']==1081
    assert len(actual)==829 and actual==sorted(actual) and len({(cf,key) for cf,key,_ in actual})==829
    facts={int.from_bytes(key[-8:],'big'):value for cf,key,value in actual if cf=='facts' and key[:2]==b'\x01\x20'}
    def recorded(n):
        return decode(facts[n][34:].split(b'\0',1)[1])[4]
    job,_,attribution=source.attribution(state,'b1')
    dictionary_time=source.clock(recorded(4),job['created_at_us'],wires[0]['connected_at_us'])
    parameters=[];bindings={}
    for n,event in enumerate(events[:61]):
        wire=wires[n//3] if n<60 else saturation
        ordinal=n%3+1
        received=decode(original[n+3].split(b'\0',1)[1])[3][5][0]
        source.clock(received,wire['sent_at_us'],wire['closed_at_us'])
        at=source.clock(recorded(8+n*5),received,completed)
        body=[bytes.fromhex(event['id']),bytes.fromhex(event['pubkey']),event['created_at'],event['kind'],event['tags'],event['content']]
        digest=source.sha(b'bigbrotr/body/1\0'+cbor(body))
        parameters.append((event,received,at,wire['clock_resolution_us'],digest))
        bindings[n+4]=source.request_binding(event,wire,ordinal)
    first=parameters[0]
    wanted=expected(state,report,events[0],dictionary_time,first[1],first[2],first[3],
        native_options=dict(acknowledge=True,retained_boundary=True,additional_events=parameters[1:]))
    index.exact([(cf,k.hex(),v.hex()) for cf,k,v in actual],
                [(cf,k.hex(),v.hex()) for cf,k,v in wanted['native']])
    assert original==wanted['inputs'], 'all64 independently encoded original inputs'
    control=index.only(state['owner_epochs'],role='control')
    manifest=next(v for cf,k,v in wanted['native'] if cf=='default' and k==b'\x01\x01')
    source.requests(state,1,manifest,wanted['inputs'],wanted['receipts'],control,bindings,attribution)
    progress(before_proof,wanted,events,saturation,control,completed,proof=False)
    progress(state,wanted,events,proof,control,completed,proof=True)
    for table in ('producer_requests','producer_outcomes','producer_epochs','producer_checkpoint_dependencies',
                  'jobs','acquisition_jobs','job_admissions'):
        index.exact(before_proof[table],state[table])
    for batch in wanted['batches']:
        batch.update(epoch=1,topology=0)
    return wanted
