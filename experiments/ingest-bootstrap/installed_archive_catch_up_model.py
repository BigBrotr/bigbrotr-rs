"""Independent fixed three-lifetime reducer for the same-root Archive journey.

Only fresh identities, clocks, transaction IDs and lease observations parameterize
rows. No ignored operational tables, native rebaseline, or product codec is used.
"""
import copy
import hashlib
import json
import re

from initial_succession_oracle import only, same, be, binary, bytea
from installed_family_replay import (Model as Family, clock, fresh_xmin, exclusion,
    leases, admissions, native_map, native_rows, digest_bytes)
from installed_cycle_model import Model as Cycle
from installed_family_science import NetworkScience, decode
from installed_archive_oracle import TABLES, DESCRIPTOR_FIELDS, reference, source, clock as fact_clock
from bootstrap_oracle import cbor

sha=lambda b:hashlib.sha256(b).hexdigest()


class Model(Cycle):
    def __init__(self,before,native,inputs,window):
        self.pg,self.native,self.inputs=copy.deepcopy((before,native,inputs))
        self.ds=only(self.pg['datasets']); producer=max(self.pg['producer_epochs'],key=lambda r:r['producer_epoch'])
        self.resource,self.slot=producer['owner_resource_id'],producer['producer_slot_id']
        self.legacy=next(iter(self.pg['initial_ingest_successions']),None)
        self.window=window; self.ids=set(re.findall(r'\\\\x([0-9a-f]{32})"',json.dumps(before)))

    def change_at(self,actual,field,lower):
        at,tx=actual[field],actual['xmin']; clock(at,self.window); fresh_xmin(tx,self.pg)
        assert max(lower,self.control['acquired_at_us'])<=at<=self.control['released_at_us']<self.control['lease_expires_at_us']
        return at,tx

    def closure(self,epoch):
        producer=only(self.pg['producer_epochs'],producer_epoch=epoch)
        issued=producer['reserved_through_request']; values=native_map(self.native)
        if epoch==1:
            # The native head advances; the initial handoff preserves epoch 1.
            manifest=(binary(only(self.pg['initial_ingest_successions'])['previous_manifest'])
                if self.pg['initial_ingest_successions'] else bytes.fromhex(values['default','0101']))
        else:
            manifest=binary(only(self.pg[
                'initial_ingest_successions' if epoch==2 else 'ingest_successions'],target_epoch=epoch)['target_manifest'])
        prefix=b'bigbrotr/initial-source-closure/1\0' if epoch==1 else (
            b'bigbrotr/activated-initial-source-closure/1\0' if epoch==2 else b'bigbrotr/ingest-successor-source-closure/1\0')
        raw=prefix
        if epoch>1:
            intent=only(self.pg['initial_ingest_successions' if epoch==2 else 'ingest_successions'],target_epoch=epoch)
            raw+=digest_bytes(binary(intent['handoff']))
        raw+=digest_bytes(manifest)+binary(self.slot)+be(epoch)+be(issued)
        cut=(12,0,10)[epoch-1]
        for n in range(1,issued+1):
            request=only(self.pg['producer_requests'],producer_epoch=epoch,request_sequence=n)
            outcome=only(self.pg['producer_outcomes'],producer_epoch=epoch,request_sequence=n)
            assert request['request_digest']==outcome['request_digest']
            assert outcome['outcome_sha256']==bytea(digest_bytes(binary(outcome['outcome_bytes'])))
            raw+=be(n)+binary(request['request_digest'])+binary(outcome['outcome_sha256'])
        return issued,cut,sha(raw+be(cut))

    def close(self,observed,epoch):
        producer=only(self.pg['producer_epochs'],producer_epoch=epoch)
        issued,cut,digest=self.closure(epoch)
        if producer['state']=='open':
            tx=only(observed['producer_epochs'],producer_epoch=epoch)['xmin']; fresh_xmin(tx,self.pg)
            producer.update(state='closing',xmin=tx)
            values=native_map(self.native); key=('receipts',(b'\x01\x31'+binary(self.slot)+be(epoch)).hex())
            assert values[key]==(b'\x01\x31'+be(issued)+bytes(16)).hex()
            values[key]=(b'\x02\x31'+be(issued)+bytes(16)+b'\x01').hex()
            self.native.update(rows=native_rows(values),sequence=self.native['sequence']+1)
        else: assert producer['state']=='closing'
        row=only(self.pg['source_streams'],owner_epoch=epoch)
        assert row['state']=='open'
        at,tx=self.change_at(only(observed['source_streams'],owner_epoch=epoch),'closed_at_us',row['opened_at_us'])
        row.update(state='closed',closed_at_us=at,last_complete_sequence=cut,closure_digest='\\x'+digest,xmin=tx)
        return issued,cut,digest

    def abandon(self,observed,epoch,digest,cut,key):
        source_row=only(self.pg['source_streams'],owner_epoch=epoch)
        assert source_row['state']=='closed'
        activator=(only(self.pg['producer_slots'])['registered_by_epoch'] if epoch==1 else
            only(self.pg['initial_ingest_activations' if epoch==2 else 'ingest_activations'],target_epoch=epoch)['activated_by_epoch'])
        claims=[r for r in self.pg['jobs'] if r['kind']=='acquisition' and r['owner_epoch']==activator]
        assert len(claims)==(0 if epoch==2 else 1)
        if not claims: return
        job,=claims; assert job['request_key']=='\\x'+key*16 and job['state']=='queued' and job['job_revision']==2
        assert job['checkpoint_revision']==0 and job['input_complete'] and job['result_manifest_digest'] is None
        reason=('initial_producer_abandoned','activated_initial_producer_abandoned','ingest_successor_abandoned')[epoch-1]
        gap=only(observed['acquisition_gaps'],job_id=job['job_id'])
        at,tx=self.change_at(gap,'abandoned_at_us',source_row['closed_at_us'])
        self.pg['acquisition_gaps'].append(dict(xmin=tx,job_id=job['job_id'],reason=reason,shard_id=0,
            dataset_id=self.ds['dataset_id'],owner_role='control',issuer_epoch=self.control['epoch'],producer_epoch=epoch,
            abandoned_at_us=at,issuer_resource=self.control['resource_id'],producer_slot_id=self.slot,
            issuer_incarnation=self.control['process_incarnation'],source_owner_epoch=epoch,previous_owner_epoch=job['owner_epoch'],
            previous_job_revision=2,source_closure_digest='\\x'+digest,last_complete_sequence=cut,
            previous_owner_resource=job['owner_resource_id'],previous_owner_incarnation=job['owner_incarnation']))
        job.update(state='cancelled',job_revision=4,reason_code=reason,completed_at_us=at,
            owner_resource_id=self.control['resource_id'],owner_epoch=self.control['epoch'],
            owner_incarnation=self.control['process_incarnation'],xmin=tx)

    def retire(self,observed,epoch):
        producer=only(self.pg['producer_epochs'],producer_epoch=epoch)
        source_row=only(self.pg['source_streams'],owner_epoch=epoch)
        assert producer['state']=='closing' and source_row['state']=='closed'
        at,tx=self.change_at(only(observed['producer_epochs'],producer_epoch=epoch),'retired_at_us',source_row['closed_at_us'])
        digest=source_row['closure_digest']
        producer.update(state='retired',retired_at_us=at,closure_digest=digest,xmin=tx)
        owner=only(self.pg['owner_epochs'],role='ingest',epoch=epoch)
        owner.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest=digest,xmin=tx)
        only(self.pg['role_instances'],role_instance_id=owner['role_instance_id']).update(retired_at_us=at,xmin=tx)
        domains=[r for r in self.pg['allocation_domains'] if r['owner_epoch']==epoch]; assert len(domains)==9
        for row in domains: row.update(state='closed',closed_at_us=at,closure_digest=digest,xmin=tx)
        if epoch==3:
            receipt=only(observed['ingest_terminal_transactions'],target_epoch=epoch)
            xid=receipt['terminal_transaction']; assert re.fullmatch('[1-9][0-9]*',xid) and int(xid)%2**32==int(tx)
            self.pg['ingest_terminal_transactions'].append(dict(owner_resource_id=self.resource,target_epoch=epoch,
                terminal_transaction=xid,control_resource=self.control['resource_id'],control_epoch=self.control['epoch'],
                control_incarnation=self.control['process_incarnation'],xmin=tx))

    def finite(self,observed,operation):
        self.completed_control(observed)
        epoch=int(operation[-1]); name=operation[:-1]
        if name in ('source','abandon'):
            source_row=only(self.pg['source_streams'],owner_epoch=epoch)
            issued,cut,digest=self.close(observed,epoch) if source_row['state']=='open' else self.closure(epoch)
            if name=='abandon': self.abandon(observed,epoch,digest,cut,'b1' if epoch==1 else 'e4')
        else:
            assert name=='retire'; self.retire(observed,epoch)
        same(self.pg,observed)

    def live2(self,observed,report):
        """Schema2 bytes are constructed from actual fresh identities, never adopted."""
        self.completed_control(observed)
        source_row=only(self.pg['source_streams'],owner_epoch=1)
        p=only(self.pg['producer_epochs']); assert p['state']=='retired' and source_row['last_complete_sequence']==12
        intent=only(observed['initial_ingest_successions']); tx=intent['xmin']; fresh_xmin(tx,self.pg)
        at,deadline=intent['created_at_us'],intent['lease_expires_at_us'];clock(at,self.window)
        assert self.control['acquired_at_us']<=at<deadline<=at+6_000_000
        role=self.identity(intent['target_role_instance_id'][2:]);inc=self.identity(intent['target_incarnation'][2:])
        domains=[self.identity(only(observed['allocation_domains'],owner_epoch=2,dictionary_kind=k)['allocation_domain'][2:]) for k in range(1,10)]
        values=native_map(self.native); old=bytes.fromhex(values['default','0101']); target=bytearray(old)
        target[2:4]=be(2,2);target[26:34]=be(1);target[54:62]=be(2);target[62:78]=binary(inc)
        for k,domain in enumerate(domains,1): target[106+(k-1)*18:122+(k-1)*18]=binary(domain)
        target=bytes(target)
        handoff=b'\x01\x0a'+binary(self.slot)+be(1)+be(4)+be(12)+binary(source_row['closure_digest'])+be(len(old),2)+old+be(len(target),2)+target
        owner=only(self.pg['owner_epochs'],role='ingest',epoch=1)
        actual_owner=only(observed['owner_epochs'],role='ingest',epoch=2)
        clock(actual_owner['lease_expires_at_us']-6_000_000,self.window)
        assert actual_owner['lease_expires_at_us']>=deadline
        fresh_xmin(actual_owner['xmin'],self.pg)
        self.pg['owner_epochs'].append(dict(owner,epoch=2,role_instance_id=role,process_incarnation=inc,
            acquired_at_us=at,lease_expires_at_us=actual_owner['lease_expires_at_us'],state='active',released_at_us=None,
            exclusion_method=None,exclusion_digest=None,xmin=actual_owner['xmin']))
        self.pg['role_instances'].append(dict(role_instance_id=role,dataset_id=self.ds['dataset_id'],role='ingest',
            created_at_us=at,retired_at_us=None,xmin=tx))
        self.pg['producer_epochs'].append(dict(p,producer_epoch=2,producer_role_instance_id=role,producer_incarnation=inc,
            owner_epoch=2,owner_incarnation=inc,state='open',reserved_through_request=0,opened_at_us=at,
            retired_at_us=None,closure_digest=None,xmin=tx))
        self.pg['source_streams'].append(dict(source_row,owner_epoch=2,owner_incarnation=inc,state='open',
            opened_at_us=at,closed_at_us=None,last_complete_sequence=None,closure_digest=None,xmin=tx))
        for k,domain in enumerate(domains,1):
            self.pg['allocation_domains'].append(dict(only(self.pg['allocation_domains'],owner_epoch=1,dictionary_kind=k),
                allocation_domain=domain,owner_epoch=2,owner_incarnation=inc,state='active',created_at_us=at,closed_at_us=None,
                closure_digest=None,xmin=tx))
        self.pg['topology_generations'].append(dict(only(self.pg['topology_generations']),generation=1,previous_generation=0,created_at_us=at,xmin=tx))
        old_stream=only(self.pg['topology_streams'])
        self.pg['topology_streams'] += [dict(old_stream,topology_generation=1,owner_epoch=e,handoff_last_complete_sequence=12 if e==1 else None,xmin=tx) for e in (1,2)]
        self.ds.update(topology_generation=1,xmin=tx)
        policy=only(self.pg['policy_revisions']);config=only(self.pg['config_revisions'])
        expected=dict(dataset_id=self.ds['dataset_id'],owner_resource_id=self.resource,target_epoch=2,owner_role='ingest',
            target_incarnation=inc,target_role_instance_id=role,producer_slot_id=self.slot,producer_epoch=2,shard_id=0,
            previous_topology=0,target_topology=1,issued_through=4,last_complete_sequence=12,closure_digest=source_row['closure_digest'],
            previous_manifest=bytea(old),target_manifest=bytea(target),handoff=bytea(handoff),
            previous_manifest_sha256=bytea(digest_bytes(old)),target_manifest_sha256=bytea(digest_bytes(target)),
            handoff_sha256=bytea(digest_bytes(handoff)),config_revision=1,policy_revision=1,
            config_digest=config['document_digest'],policy_digest=policy['document_digest'],outstanding_request_limit=32,
            created_at_us=at,lease_expires_at_us=deadline,prepared_by_resource=self.control['resource_id'],
            prepared_by_epoch=self.control['epoch'],prepared_by_role='control',prepared_by_incarnation=self.control['process_incarnation'],xmin=tx)
        self.pg['initial_ingest_successions'].append(expected)
        activation=only(observed['initial_ingest_activations']);ax=activation['xmin'];fresh_xmin(ax,self.pg)
        clock(activation['activated_at_us'],self.window); assert at<=activation['activated_at_us']<deadline
        assert int(activation['activation_transaction'])%2**32==int(ax)
        fields=('dataset_id','owner_resource_id','target_epoch','target_incarnation','target_role_instance_id','producer_slot_id',
            'producer_epoch','previous_manifest_sha256','target_manifest_sha256','handoff_sha256','prepared_by_resource',
            'prepared_by_epoch','prepared_by_incarnation','config_revision','policy_revision','config_digest','policy_digest')
        self.pg['initial_ingest_activations'].append(dict({k:expected[k] for k in fields},activated_by_resource=self.control['resource_id'],
            activated_by_epoch=self.control['epoch'],activated_by_incarnation=self.control['process_incarnation'],
            activated_at_us=activation['activated_at_us'],activation_transaction=activation['activation_transaction'],xmin=ax))
        values['default','0101']=target.hex()
        values['default',(b'\x01\x0a'+binary(self.ds['dataset_id'])+binary(self.resource)+be(1)).hex()]=handoff.hex()
        values['default',(b'\x01\x02'+binary(self.ds['dataset_id'])+bytes(4)+be(2)).hex()]=(b'\x01\x02'+be(0)).hex()
        values['receipts',(b'\x01\x31'+binary(self.slot)+be(2)).hex()]=(b'\x01\x31'+bytes(24)).hex()
        self.native.update(rows=native_rows(values),sequence=self.native['sequence']+4)
        same(self.pg,observed)
        assert report['producer_epoch']=='2' and report['state']=='activated' and report['ready'] is False
        assert report['producer_slot_id']==self.slot[2:] and report['dataset_id']==self.ds['dataset_id'][2:]


def archive_science(native,*,compacted=False,live_epoch=3,provenance=True):
    """Translate independently checked native facts, not Archive output, to rows."""
    assert type(compacted) is bool
    assert type(provenance) is bool and (provenance or (not compacted and live_epoch == 2))
    assert type(live_epoch) is int and live_epoch in (2,3)
    assert not compacted or live_epoch==3
    tables={t:[] for t in TABLES}; facts={}
    for row in native['rows']:
        key,value=bytes.fromhex(row['key']),bytes.fromhex(row['value'])
        if row['cf']!='facts' or not key.startswith(b'\x01\x20'): continue
        preimage=value[34:]; assert preimage.startswith(b'bigbrotr/fact/1\0')
        digest=digest_bytes(preimage); assert value[:34]==b'\x01\x20'+digest
        identity,family,version,visibility,at,payload=decode(preimage.split(b'\0',1)[1])
        epoch,sequence=identity[2:]; assert key==b'\x01\x20'+identity[0]+be(identity[1],4)+be(epoch)+be(sequence)
        facts[epoch,sequence]=digest
        item=dict(source_record_id=source(identity),record_schema_version=version,record_kind=family,
            visibility_scope_id=reference(visibility),payload_digest=digest.hex().upper(),recorded_at=at)
        if family=='dictionary':
            ref,normalization,schema,descriptor_version,fields=payload; descriptor=dict.fromkeys(DESCRIPTOR_FIELDS)
            for k,v in fields.items():
                assert k in descriptor
                descriptor[k]=reference(v) if k in ('method_id','visibility_scope_id') and v is not None else v.hex().upper() if isinstance(v,bytes) else v
            item.update(reference=reference(ref),normalization_version=normalization,descriptor_schema=schema,
                descriptor_version=descriptor_version,descriptor_value=descriptor);table='dictionaries'
        elif family=='event-payload':
            event,pubkey,created,kind,tags,content,ordinal=payload
            item.update(event_id=event.hex().upper(),pubkey=pubkey.hex().upper(),created_at=created,kind=kind,tags=tags,
                content=content,event_ordinal=reference(ordinal));table='events'
        elif family=='event-eligibility':
            event,body,sig,eligible,origin,lineage=payload;assert lineage is None
            item.update(event_id=event.hex().upper(),body_digest=body.hex().upper(),sig=sig.hex().upper(),first_eligible_at=eligible,
                origin=origin,import_lineage=None);table='event_eligibility_facts'
        elif family=='event-receive':
            event,received,observer,quality,corrects=payload;assert corrects is None
            item.update(event_id=event.hex().upper(),received_at=received,observer_id=reference(observer),clock_quality=fact_clock(quality),corrects=None);table='event_receive_facts'
        else:
            assert family=='provenance';event,endpoint,observer,access,received,quality,verification,corrects=payload;assert corrects is None
            item.update(event_id=event.hex().upper(),endpoint_id=reference(endpoint),observer_id=reference(observer),access_scope_id=reference(access),
                received_at=received,clock_quality=fact_clock(quality),verification_method_id=reference(verification),corrects=None);table='provenance_facts'
        tables[table].append(item)
    for table in tables:
        tables[table].sort(key=lambda r:tuple(r['source_record_id'][k] for k in ('dataset_id','shard_id','owner_epoch','sequence')))
    expected_facts=({(1,n) for n in range(1,28)}|{(2,1)} if compacted else
        {(1,n) for n in range(1,13 if provenance else 9)}|{(live_epoch,n) for n in range(1,11 if provenance else 6)})
    assert set(facts)==expected_facts
    assert [len(tables[t]) for t in TABLES]==([12,4,4,4,4] if compacted else
        [13,2,2,2,3] if provenance else [7,2,2,2,0])
    ds=bytes.fromhex(tables['events'][0]['source_record_id']['dataset_id']);batches=[]
    ranges_by_epoch=([(1,2,[(1,2),(3,3),(4,7),(8,12),(13,17),(18,22),(23,27)]),(2,2,[(1,1)])]
        if compacted else [(1,0,[(1,2),(3,3),(4,7),(8,12)]),(live_epoch,live_epoch-1,[(1,4),(5,9),(10,10)])])
    if not provenance:
        ranges_by_epoch=[(1,0,[(1,2),(3,3),(4,4),(5,8)]),(2,1,[(1,1),(2,5)])]
    for epoch,topology,ranges in ranges_by_epoch:
        for first,last in ranges:
            refs=[[[ds,0,epoch,n],facts[epoch,n]] for n in range(first,last+1)]
            batches.append(dict(epoch=epoch,topology=topology,first=first,last=last,
                archive_digest=sha(b'bigbrotr/archive-batch/1\0'+cbor([ds,1,topology,[[0,epoch,first,last]],refs])),
                admission_digest=sha(b'bigbrotr/admission-batch/1\0'+cbor([ds,0,epoch,first,last,refs]))))
    return dict(tables=tables,batches=batches)
