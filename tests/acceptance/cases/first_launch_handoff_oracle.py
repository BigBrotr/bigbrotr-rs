"""Declared zero-query epoch2 handoff; complete native and relational expectations."""
import copy
from types import SimpleNamespace

from cases import first_launch_capped_oracle as base
from cases import first_launch_quantum_oracle as quantum
from cases import first_launch_restart as source


def attribution_science(before,state,started,events,first,dump,inputs,lower,upper):
    from installed_family_science import NetworkScience,decode
    from transition_replay import native_rows
    manifest,handoff,activated=base.adoption_activated_native(before,state,first)
    # Only the event clock is observed; input/receipt/native bytes are encoded.
    assert len(inputs)==1
    fact=next(row for row in dump['rows'] if row['cf']=='facts' and row['key']==(
        b'\x01\x20'+source.raw(state['datasets'][0]['dataset_id'])+bytes(4)+source.be(2)+source.be(1)).hex())
    at=source.clock(decode(bytes.fromhex(fact['value'])[34:].split(b'\0',1)[1])[4],lower,upper)
    domains=[source.only(state['allocation_domains'],owner_epoch=2,dictionary_kind=k)['allocation_domain'] for k in range(1,10)]
    model=SimpleNamespace(ds=source.only(state['datasets']),slot='\\x'+started['producer_slot_id'],
        legacy=dict(previous_manifest='\\x'+first['manifest'].hex()),inputs={},native=copy.deepcopy(activated))
    science=NetworkScience(model,dict(epoch=2,topology=1,manifest=manifest,domains=domains),acknowledge=True,
        new_event=events[2],endpoint='ws://127.0.0.1:7447/installed-fixture',previous_event=events[1],previous_coordinates=(3,4,13,14,15,16))
    assert science.attribution(at)==4
    assert dump==model.native and dump['sequence']==113
    assert inputs==list(science.inputs.values())
    return dict(native=[(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in native_rows(science.values)],
        inputs=inputs,receipts=list(science.receipts.values()),manifest=manifest,handoff=handoff,dump=dump)


def attribution_rows(state,science,current,lower,upper):
    import installed_progress_replay as typed
    from installed_progress_context_oracle import current_snapshot
    _,_,attribution=source.attribution(state,'b1')
    source.requests(state,2,science['manifest'],science['inputs'],science['receipts'],current,{},attribution)
    request=source.only(state['producer_requests'],producer_epoch=2)
    outcome=source.only(state['producer_outcomes'],producer_epoch=2)
    source.clock(request['reserved_at_us'],lower,outcome['recorded_at_us']);source.clock(outcome['recorded_at_us'],lower,upper)
    assert not [row for row in state['acquisition_progress'] if row['producer_epoch']==2]
    head=source.only(state['producer_checkpoints'],producer_epoch=2)
    assert head['xmin']==outcome['xmin']
    common=dict(dataset_id=head['dataset_id'],producer_slot_id=head['producer_slot_id'],producer_epoch=2)
    refs=quantum.receipt_refs(science['receipts'][0])
    deps=[dict(common,source_shard=ref[1],source_epoch=ref[2],source_sequence=ref[3],payload_digest='\\x'+digest.hex(),xmin=outcome['xmin']) for ref,digest in refs]
    assert [(d['source_epoch'],d['source_sequence']) for d in deps]==[(1,3),(2,1),(2,2),(2,3),(2,4)]
    values={(cf,key.hex()):value.hex() for cf,key,value in science['native']}
    expected=dict(common,progress_format=1,manifest_sha256='\\x'+source.sha(science['manifest']),checkpoint_revision=1,
        complete_through_request=1,progress_jobs=0,dependency_count=5,issuer_resource=current['resource_id'],
        issuer_epoch=current['epoch'],issuer_incarnation=current['process_incarnation'],committed_at_us=head['committed_at_us'],
        xmin=outcome['xmin'],checkpoint_digest='\\x'+'00'*32)
    source.clock(head['committed_at_us'],outcome['recorded_at_us'],upper)
    expected['checkpoint_digest']='\\x'+typed.head_digest(expected,[],deps,lambda d:typed.native_reference(d,values)).hex()
    actual=sorted((r for r in state['producer_checkpoint_dependencies'] if r['producer_epoch']==2),key=lambda r:(r['source_shard'],r['source_epoch'],r['source_sequence']))
    current_snapshot(head,[],actual,expected,[],deps,values,prefix_profile=False)
    return request,outcome,head,actual


def zero_query_cut(before,after,first,started,wires,events,dump,inputs,lower,upper,fault):
    """Successful rev3 adoption plus exactly attribution1; the first plan rolled back."""
    base.adoption_seal_eligibility(before,lower);base.adoption_seal_eligibility(after,upper,job_revision=3)
    base.adoption_history(after,wires,events,first)
    wanted,current,activation,manifest,handoff,prepared=base.adoption_startup_cut(before,after,first,None,lower,upper,renewed_ingest=True)
    assert fault['backend_pid']==current['control_backend_pid'] and type(fault['backend_pid']) is int
    job=source.only(before['jobs']);seal=source.only(after['acquisition_continuations']);old=source.only(before['acquisition_continuations'])
    tx=seal['xmin'];assert base.adoption_xid(tx)==base.adoption_xid(seal['adoption_transaction'],bits=64)&0xffffffff
    source.clock(seal['adopted_at_us'],activation['activated_at_us'],upper)
    assert tx not in {r['xmin'] for rows in before.values() for r in rows}|{prepared['xmin'],activation['xmin']}
    wanted['jobs']=[dict(job,job_revision=3,owner_resource_id=current['resource_id'],owner_epoch=current['epoch'],owner_incarnation=current['process_incarnation'],xmin=tx)]
    wanted['acquisition_continuations']=[dict(old,target_epoch=2,target_manifest_sha256='\\x'+source.sha(manifest),activation_sha256='\\x'+source.sha(handoff),
        adopting_resource=current['resource_id'],adopting_epoch=current['epoch'],adopting_incarnation=current['process_incarnation'],
        adopted_job_revision=3,adopted_at_us=seal['adopted_at_us'],adoption_transaction=seal['adoption_transaction'],xmin=tx)]
    science=attribution_science(before,after,started,events,first,dump,inputs,activation['activated_at_us'],upper)
    request,outcome,head,deps=attribution_rows(after,science,current,activation['activated_at_us'],upper)
    assert tx!=request['xmin']!=outcome['xmin'] and outcome['recorded_at_us']>=seal['adopted_at_us']
    for name,rows in (('producer_requests',[request]),('producer_outcomes',[outcome]),('producer_checkpoints',[head]),('producer_checkpoint_dependencies',deps)):
        wanted[name]+=rows
    producer=source.only(wanted['producer_epochs'],producer_epoch=2)
    producer.update(reserved_through_request=1,xmin=request['xmin'])
    base.adoption_exact_tables(wanted,after)
    return science


def closed_attribution(science,slot,dump=None):
    rows={(cf,key):value for cf,key,value in science['native']};key=('receipts',b'\x01\x31'+source.raw(slot)+source.be(2))
    assert rows[key]==b'\x01\x31'+source.be(1)+source.be(1)+source.be(0)
    rows[key]=b'\x02\x31'+source.be(1)+source.be(1)+source.be(0)+b'\x01'
    result=dict(event='dump-state',sequence=114,rows=[dict(cf=cf,key=key.hex(),value=value.hex()) for (cf,key),value in sorted(rows.items())])
    if dump is not None:source.exact(dump,result,'zero-query closure made undeclared native changes')
    return result


def fenced_control(before,after,lower,upper):
    from initial_succession_oracle import exclusion
    current,role=base.adoption_new_control(before,after,lower,upper,active=False)
    wanted=copy.deepcopy(before)
    for old in before['owner_epochs']:
        if old['role']!='control' or old['state']=='released':continue
        assert old['state']=='active' and current['acquired_at_us']<old['lease_expires_at_us']
        changed=source.only(after['owner_epochs'],role='control',epoch=old['epoch'])
        old_role=source.only(before['role_instances'],role_instance_id=old['role_instance_id'])
        retired=source.only(after['role_instances'],role_instance_id=old['role_instance_id'])
        source.clock(changed['released_at_us'],lower,current['acquired_at_us'])
        source.clock(retired['retired_at_us'],changed['released_at_us'],current['acquired_at_us'])
        xid=changed['xmin'];base.adoption_xid(xid);assert retired['xmin']==xid and xid!=current['xmin']
        assert xid not in {r['xmin'] for rows in before.values() for r in rows}
        wanted['owner_epochs'][wanted['owner_epochs'].index(old)]=dict(old,state='released',released_at_us=changed['released_at_us'],
            exclusion_method='storage_fenced',exclusion_digest=exclusion(old,current['process_incarnation']),xmin=xid)
        wanted['role_instances'][wanted['role_instances'].index(old_role)]=dict(old_role,retired_at_us=retired['retired_at_us'],xmin=xid)
    wanted['owner_epochs'].append(current);wanted['role_instances'].append(role)
    return wanted,current


def epoch2_recovery(before,sealed,retired,science,bounds,reports):
    """Close/seal and then retire epoch2, preserving its zero-query adopted obligation."""
    job=source.only(before['jobs']);slot=source.only(before['producer_epochs'],producer_epoch=2)['producer_slot_id']
    digest='\\x'+source.closure(science['manifest'],slot,2,science['inputs'],science['receipts'],4,science['handoff'])
    for index,(prior,after) in enumerate(((before,sealed),(sealed,retired))):
        lower,upper=bounds[index];base.adoption_seal_eligibility(prior,lower,job_revision=3);base.adoption_seal_eligibility(after,upper,job_revision=3)
        wanted,current=fenced_control(prior,after,lower,upper)
        producer=source.only(wanted['producer_epochs'],producer_epoch=2);actual=source.only(after['producer_epochs'],producer_epoch=2)
        if index==0:
            producer.update(state='closing',xmin=actual['xmin']);base.adoption_xid(actual['xmin'])
            stream=source.only(wanted['source_streams'],owner_epoch=2);closed=source.only(after['source_streams'],owner_epoch=2)
            source.clock(closed['closed_at_us'],current['acquired_at_us'],current['released_at_us'])
            stream.update(state='closed',closed_at_us=closed['closed_at_us'],last_complete_sequence=4,closure_digest=digest,xmin=closed['xmin'])
            seal=source.only(after['acquisition_continuations'],producer_epoch=2);first=source.only(prior['acquisition_continuations'])
            head=source.only(prior['producer_checkpoints'],producer_epoch=2)
            source.clock(seal['sealed_at_us'],closed['closed_at_us'],current['released_at_us']);base.adoption_xid(seal['xmin'])
            assert len(source.raw(seal['continuation_id']))==16 and seal['continuation_id']!=first['continuation_id']
            expected=dict(continuation_id=seal['continuation_id'],dataset_id=job['dataset_id'],job_id=job['job_id'],previous_job_revision=3,
                previous_continuation=first['continuation_id'],previous_owner_resource=job['owner_resource_id'],previous_owner_epoch=job['owner_epoch'],
                previous_owner_incarnation=job['owner_incarnation'],scope_id=source.only(prior['acquisition_jobs'])['scope_id'],enrollment_generation=1,
                route_id=source.only(prior['relay_enrollments'])['route_id'],producer_slot_id=slot,producer_epoch=2,manifest_sha256='\\x'+source.sha(science['manifest']),
                shard_id=0,source_sequence=4,source_digest=digest,checkpoint_revision=1,complete_through_request=1,checkpoint_sha256=head['checkpoint_digest'],
                last_quantum_number=1,sealing_resource=current['resource_id'],sealing_epoch=current['epoch'],sealing_incarnation=current['process_incarnation'],
                sealed_at_us=seal['sealed_at_us'],**dict.fromkeys(base.ADOPTION_FIELDS),xmin=seal['xmin'])
            source.exact(seal,expected,'zero-query seal lost its workflow predecessor/cut')
            wanted['acquisition_continuations'].append(expected)
        else:
            at=source.clock(actual['retired_at_us'],current['acquired_at_us'],current['released_at_us']);tx=actual['xmin'];base.adoption_xid(tx)
            producer.update(state='retired',retired_at_us=at,closure_digest=digest,xmin=tx)
            ingest=source.only(wanted['owner_epochs'],role='ingest',epoch=2)
            ingest.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest=digest,xmin=tx)
            source.only(wanted['role_instances'],role_instance_id=ingest['role_instance_id']).update(retired_at_us=at,xmin=tx)
            for row in wanted['allocation_domains']:
                if row['owner_epoch']==2:row.update(state='closed',closed_at_us=at,closure_digest=digest,xmin=tx)
        base.adoption_exact_tables(wanted,after)
        report=reports[index]
        expected=dict(format='bigbrotr-ingest-recovery/1',state='activated-acquisition-sealed' if index==0 else 'activated-initial-retired',
            dataset_id=job['dataset_id'][2:],owner_resource_id=producer['owner_resource_id'][2:],producer_slot_id=slot[2:],producer_epoch='2',
            issued_through='1',reserved_through='1',resolved_requests='1',source_closure=dict(last_complete_sequence='4',sha256=digest[2:]),
            native='closed',owner_release='committed',ready=False)
        if index==0:expected['acquisition_continuation']=dict(sealed_jobs='1',execution='not-authorized')
        else:expected['ingest_retirement']=dict(allocation_domains='9',acquisition_gaps='0',owner_release='committed',native_acknowledged='1',native_retired='0')
        source.exact(report,expected,'epoch2 lifecycle report differs')


def epoch3_native(retired,state,science,dump=None):
    from initial_succession_oracle import successor_rows
    prior=source.only(retired['initial_ingest_successions']);intent=source.only(state['ingest_successions'])
    domains=[source.raw(source.only(state['allocation_domains'],owner_epoch=3,dictionary_kind=k)['allocation_domain']) for k in range(1,10)]
    old=source.raw(prior['target_manifest']);inc=source.raw(intent['target_incarnation'])
    manifest=bytearray(old);manifest[2:4]=source.be(3,2);manifest[26:34]=source.be(2);manifest[54:62]=source.be(3);manifest[62:78]=inc
    for k,domain in enumerate(domains):manifest[106+k*18:122+k*18]=domain
    manifest=bytes(manifest);stream=source.only(retired['source_streams'],owner_epoch=2)
    handoff=(b'\x02\x0a\x01'+source.raw(prior['producer_slot_id'])+source.be(2)+source.be(1)+source.be(4)
        +source.raw(stream['closure_digest'])+bytes.fromhex(source.sha(source.raw(prior['handoff'])))
        +source.be(266,2)+old+source.be(266,2)+manifest)
    source.exact(intent['target_manifest'],'\\x'+manifest.hex(),'epoch3 manifest differs')
    source.exact(intent['handoff'],'\\x'+handoff.hex(),'epoch3 lineage differs')
    closed=closed_attribution(science,prior['producer_slot_id'])
    rows=successor_rows({(r['cf'],bytes.fromhex(r['key'])):bytes.fromhex(r['value']) for r in closed['rows']},manifest,source.raw(prior['producer_slot_id']),handoff,repeatable=True)
    expected=dict(event='dump-state',sequence=119,rows=[dict(cf=cf,key=key.hex(),value=value.hex()) for (cf,key),value in sorted(rows.items())])
    if dump is not None:source.exact(dump,expected,'epoch3 activation native differs')
    return manifest,handoff,expected


def final_rows(before,after,first,attribution,started,wires,events,dump,inputs,lower,upper,*,preparation_ttl_us):
    """New epoch3 activation/adoption/work only; every epoch1/2 byte and xmin retained."""
    base.adoption_seal_eligibility(before,lower,job_revision=3);base.adoption_seal_eligibility(after,upper,job_revision=4)
    current,control_role=base.adoption_new_control(before,after,lower,upper,active=False)
    manifest,handoff,activated=epoch3_native(before,after,attribution)
    science=base.adoption_network_science(after,started,events,wires,dump,inputs,first,upper,manifest,handoff,activated,epoch=3)
    assert dump['sequence']==167
    base.adoption_history(after,wires,events,first,science,successor_epoch=3)
    base.adoption_progress(after,dict(producer_slot_id=started['producer_slot_id']),events,wires,science,upper,epoch=3)
    intent=source.only(after['ingest_successions']);activation=source.only(after['ingest_activations'])
    old_intent=source.only(before['initial_ingest_successions']);dataset=source.only(before['datasets'])
    producer=source.only(before['producer_epochs'],producer_epoch=2);stream=source.only(before['source_streams'],owner_epoch=2)
    created=source.clock(intent['created_at_us'],current['acquired_at_us'],activation['activated_at_us'])
    source.clock(activation['activated_at_us'],created,current['released_at_us'])
    assert current['lease_expires_at_us']-60_000_000>activation['activated_at_us']
    source.clock(preparation_ttl_us,1,2**64-1)
    # successor_family::Work::plan passes the root authorization TTL here;
    # ordinary live SQL renewals independently retain runtime.lease_ttl60s.
    assert intent['lease_expires_at_us']==created+preparation_ttl_us
    assert activation['activated_at_us']<intent['lease_expires_at_us']
    tx=intent['xmin'];assert base.adoption_xid(tx)==base.adoption_xid(intent['preparation_transaction'],bits=64)&0xffffffff
    activation_tx=activation['xmin'];assert base.adoption_xid(activation_tx)==base.adoption_xid(activation['activation_transaction'],bits=64)&0xffffffff
    old_xids={r['xmin'] for rows in before.values() for r in rows}
    assert tx not in old_xids and activation_tx not in old_xids|{tx}
    inc=intent['target_incarnation'];role_id=intent['target_role_instance_id']
    old_ids={source.raw(r['allocation_domain']) for r in before['allocation_domains']}
    domains=[source.only(after['allocation_domains'],owner_epoch=3,dictionary_kind=k)['allocation_domain'] for k in range(1,10)]
    fresh=[source.raw(v) for v in (*domains,inc,role_id)]
    assert len(set(fresh))==11 and all(len(v)==16 and any(v) and v not in old_ids for v in fresh)
    assert inc not in {r['process_incarnation'] for r in before['owner_epochs']}|{current['process_incarnation'],current['role_instance_id']}
    assert role_id not in {r['role_instance_id'] for r in before['role_instances']}|{current['process_incarnation'],current['role_instance_id']}
    wanted=copy.deepcopy(before);wanted['owner_epochs'].append(current);wanted['role_instances'].append(control_role)
    ingest=source.only(after['owner_epochs'],role='ingest',epoch=3)
    base.adoption_xid(ingest['xmin']);assert ingest['xmin'] not in old_xids|{tx,activation_tx}
    source.clock(ingest['lease_expires_at_us']-60_000_000,created,current['released_at_us'])
    assert ingest['lease_expires_at_us']-60_000_000>activation['activated_at_us']
    assert current['released_at_us']<ingest['lease_expires_at_us']
    old_owner=source.only(before['owner_epochs'],role='ingest',epoch=2)
    wanted['owner_epochs'].append(dict(old_owner,epoch=3,role_instance_id=role_id,process_incarnation=inc,acquired_at_us=created,
        lease_expires_at_us=ingest['lease_expires_at_us'],state='active',released_at_us=None,exclusion_method=None,exclusion_digest=None,xmin=ingest['xmin']))
    wanted['role_instances'].append(dict(dataset_id=dataset['dataset_id'],role='ingest',role_instance_id=role_id,created_at_us=created,retired_at_us=None,xmin=tx))
    reservation=source.only(after['producer_requests'],producer_epoch=3,request_sequence=3)
    wanted['producer_epochs'].append(dict(producer,producer_epoch=3,producer_role_instance_id=role_id,producer_incarnation=inc,
        owner_epoch=3,owner_incarnation=inc,state='open',reserved_through_request=3,opened_at_us=created,retired_at_us=None,closure_digest=None,xmin=reservation['xmin']))
    wanted['source_streams'].append(dict(stream,owner_epoch=3,owner_incarnation=inc,state='open',opened_at_us=created,
        closed_at_us=None,last_complete_sequence=None,closure_digest=None,xmin=tx))
    for k,domain in enumerate(domains,1):
        old=source.only(before['allocation_domains'],owner_epoch=2,dictionary_kind=k)
        wanted['allocation_domains'].append(dict(old,allocation_domain=domain,owner_epoch=3,owner_incarnation=inc,state='active',
            created_at_us=created,closed_at_us=None,closure_digest=None,xmin=tx))
    wanted['topology_generations'].append(dict(dataset_id=dataset['dataset_id'],generation=2,previous_generation=1,created_at_us=created,sealed=True,xmin=tx))
    for epoch,cut in ((1,17),(2,4),(3,None)):
        wanted['topology_streams'].append(dict(dataset_id=dataset['dataset_id'],topology_generation=2,shard_id=0,owner_epoch=epoch,handoff_last_complete_sequence=cut,xmin=tx))
    wanted['datasets']=[dict(dataset,topology_generation=2,xmin=tx)]
    expected=dict(dataset_id=dataset['dataset_id'],owner_resource_id=producer['owner_resource_id'],owner_role='ingest',target_epoch=3,
        previous_epoch=2,terminal_kind=1,target_incarnation=inc,target_role_instance_id=role_id,producer_slot_id=producer['producer_slot_id'],producer_epoch=3,
        shard_id=0,previous_topology=1,target_topology=2,issued_through=1,last_complete_sequence=4,closure_digest=stream['closure_digest'],
        previous_manifest=old_intent['target_manifest'],target_manifest='\\x'+manifest.hex(),previous_manifest_sha256='\\x'+source.sha(attribution['manifest']),
        target_manifest_sha256='\\x'+source.sha(manifest),handoff='\\x'+handoff.hex(),handoff_sha256='\\x'+source.sha(handoff),
        parent_handoff_sha256='\\x'+source.sha(attribution['handoff']),config_revision=1,policy_revision=1,
        config_digest=source.only(before['config_revisions'])['document_digest'],policy_digest=source.only(before['policy_revisions'])['document_digest'],
        outstanding_request_limit=32,created_at_us=created,lease_expires_at_us=created+preparation_ttl_us,prepared_by_resource=current['resource_id'],
        prepared_by_epoch=current['epoch'],prepared_by_role='control',prepared_by_incarnation=current['process_incarnation'],preparation_transaction=intent['preparation_transaction'],xmin=tx)
    wanted['ingest_successions']=[expected]
    wanted['ingest_activations']=[dict({k:expected[k] for k in ('dataset_id','owner_resource_id','target_epoch','target_incarnation','target_role_instance_id','producer_slot_id',
        'producer_epoch','target_manifest_sha256','handoff_sha256')},activated_by_resource=current['resource_id'],activated_by_epoch=current['epoch'],
        activated_by_incarnation=current['process_incarnation'],activated_at_us=activation['activated_at_us'],activation_transaction=activation['activation_transaction'],xmin=activation_tx)]
    old_seal=source.only(before['acquisition_continuations'],producer_epoch=2);seal=source.only(after['acquisition_continuations'],producer_epoch=2)
    adoption_tx=seal['xmin'];assert base.adoption_xid(adoption_tx)==base.adoption_xid(seal['adoption_transaction'],bits=64)&0xffffffff
    assert adoption_tx not in old_xids|{tx,activation_tx}
    source.clock(seal['adopted_at_us'],activation['activated_at_us'],wires[1]['connected_at_us'])
    assert seal['adopted_at_us']<intent['lease_expires_at_us']
    source.exact({k:started[k] for k in ('dataset_id','owner_resource_id','producer_slot_id','process_incarnation','role_instance_id',
        'prepared_at_us','lease_expires_at_us','config_revision','policy_revision')},dict(dataset_id=dataset['dataset_id'][2:],
        owner_resource_id=producer['owner_resource_id'][2:],producer_slot_id=producer['producer_slot_id'][2:],process_incarnation=inc[2:],role_instance_id=role_id[2:],
        prepared_at_us=str(created),lease_expires_at_us=str(created+preparation_ttl_us),config_revision='1',policy_revision='1'),'epoch3 startup attribution differs')
    wanted['acquisition_continuations'][wanted['acquisition_continuations'].index(old_seal)]=dict(old_seal,target_epoch=3,target_manifest_sha256='\\x'+source.sha(manifest),
        activation_sha256='\\x'+source.sha(handoff),adopting_resource=current['resource_id'],adopting_epoch=current['epoch'],adopting_incarnation=current['process_incarnation'],
        adopted_job_revision=4,adopted_at_us=seal['adopted_at_us'],adoption_transaction=seal['adoption_transaction'],xmin=adoption_tx)
    wanted['jobs']=[dict(source.only(before['jobs']),job_revision=4,owner_resource_id=current['resource_id'],owner_epoch=current['epoch'],owner_incarnation=current['process_incarnation'],xmin=adoption_tx)]
    for table in ('producer_requests','producer_outcomes','producer_checkpoints','producer_checkpoint_dependencies','acquisition_progress','acquisition_quanta'):
        wanted[table]+=[r for r in after[table] if r['producer_epoch']==3]
    base.adoption_exact_tables(wanted,after)
    return science
