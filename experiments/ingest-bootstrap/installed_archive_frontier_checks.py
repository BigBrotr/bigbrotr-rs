"""Synthetic oracle controls only; these do not stand in for installed publication."""
import copy
import hashlib
import struct
from installed_archive_frontier import validate, cut_digest


def check(wanted):
    sql = lambda n: '\\x' + bytes([n]).hex()*16
    row = lambda **fields: dict(xmin='101', **fields)
    dataset, control_id, catalog_id = sql(1), sql(2), sql(6)
    old = [row(role='control', resource_id=control_id, epoch=n, role_instance_id=sql(30+n)) for n in (1,2)]
    control = row(role='control', resource_id=control_id, epoch=3, role_instance_id=sql(33),
        process_incarnation=sql(3), acquired_at_us=100, lease_expires_at_us=10000)
    ingest = row(role='ingest', resource_id=sql(4), epoch=1, role_instance_id=sql(34))
    archive = row(role='archive', resource_id=catalog_id, epoch=1, role_instance_id=sql(35),
        dataset_id=dataset, acquired_at_us=290, lease_expires_at_us=10000,
        control_fence_version=None, control_backend_pid=None, control_backend_start=None,
        process_incarnation=sql(3), archive_frontier_protocol=1, state='active', released_at_us=None,
        exclusion_method=None, exclusion_digest=None)
    roles = [row(role=o['role'], role_instance_id=o['role_instance_id']) for o in [*old,control,ingest,archive]]
    roles[-1].update(dataset_id=dataset, created_at_us=289, retired_at_us=None)
    static = {k: [] for k in ('bigbrotr_archive.installation','bigbrotr_archive.scientific_schema',
        'public.ducklake_schema','public.ducklake_table','public.ducklake_column')}
    setup = dict(control=dict(owner_epochs=old, role_instances=roles[:2]), catalog=static,
        resource=dict(catalog_id=catalog_id[2:], binding_sha256='77'*32))
    state = dict(datasets=[dict(dataset_id=dataset)], owner_epochs=[*old,control,ingest,archive],
        role_instances=roles, topology_streams=[dict(shard_id=0,owner_epoch=1,topology_generation=0)])
    attempts, native, receipts = [], [], []
    for n,b in enumerate(wanted['batches'],1):
        common = dict(ordinal=n, attempt_id=sql(10+n), coordinator_incarnation=sql(20),
            worker_incarnation=sql(20+n), topology_generation=0, batch_digest='\\x'+b['archive_digest'])
        attempts.append(row(**common, dataset_id=dataset, catalog_id=catalog_id,
            expected_predecessor=n-1, schema_version=1, issuer_resource=control_id, issuer_epoch=3,
            issuer_role='control', issuer_incarnation=sql(3), issued_at_us=200+n,
            coordinator_binding_sha256='\\x'+'77'*32, persistence_protocol=1))
        native.append(row(**common, login_name='writer_'+str(n), login_oid=100+n))
        receipts.append(dict(dataset_id=dataset[2:].upper(),schema_version=1,topology_generation=0,
            batch_digest=b['archive_digest'].upper(),record_count=b['last']-b['first']+1,
            input_ranges=[dict(shard_id=0,owner_epoch=1,first_sequence=b['first'],last_sequence=b['last'])],
            attempt_id=sql(10+n)[2:].upper(), publication_ordinal=n))
    state['archive_attempts'] = attempts
    state['archive_owner_sessions'] = [row(dataset_id=dataset,catalog_id=catalog_id,owner_epoch=1,
        owner_role='archive',owner_incarnation=sql(3),coordinator_binding_sha256='\\x'+'77'*32,
        persistence_protocol=1,issuer_resource=control_id,issuer_epoch=3,issuer_role='control',issuer_incarnation=sql(3))]
    catalog = {**static, 'bigbrotr_archive.attempts':native,
        'bigbrotr_archive.owner':[row(singleton=True,ordinal=4,enabled=False)],
        'public.ducklake_snapshot':[row(snapshot_id=n) for n in range(6)]}
    # A second independent fixed-width recipe checks the one-stream cut digest.
    def literal_cut(selected, through, snapshot):
        wire = bytearray(b'bigbrotr/archive-cut-inspection/1\0' + bytes([1])*16)
        wire += struct.pack('>QQIQQQ',0,1,0,1,through,len(selected))
        for r in selected:
            wire += bytes.fromhex(r['dataset_id']) + struct.pack('>HQ',1,0)
            wire += bytes.fromhex(r['batch_digest']) + struct.pack('>Q',r['record_count'])
            wire += bytes.fromhex(r['attempt_id']) + struct.pack('>QQ',r['publication_ordinal'],1)
            bound = r['input_ranges'][0]
            wire += struct.pack('>IQQQ',0,1,bound['first_sequence'],bound['last_sequence'])
        return hashlib.sha256(wire + struct.pack('>BQ',1,snapshot)).hexdigest()
    for n,b in enumerate(wanted['batches'],1):
        assert cut_digest(dataset[2:],0,receipts[:n],b['last'],n+1) == literal_cut(receipts[:n],b['last'],n+1)
    # Both one final revision and all four complete intermediate frontiers pass.
    for cuts in ([4], [1,2,3,4]):
        state['consumers'] = [row(consumer_id=catalog_id,dataset_id=dataset,consumer_role='archive',
            created_at_us=300,current_revision=len(cuts),current_topology_generation=0)]
        state['consumer_frontier_revisions'], state['archive_frontier_snapshots'] = [], []
        state['consumer_frontiers'], state['archive_frontier_batches'] = [], []
        for rev,count in enumerate(cuts,1):
            b = wanted['batches'][count-1]
            state['consumer_frontier_revisions'].append(row(consumer_id=catalog_id,frontier_revision=rev,
                dataset_id=dataset,consumer_role='archive',topology_generation=0,previous_revision=rev-1 or None,
                owner_resource_id=catalog_id,owner_epoch=1,owner_incarnation=sql(3),created_at_us=300+rev,sealed=True))
            state['archive_frontier_snapshots'].append(row(operation_id=sql(40+rev),consumer_id=catalog_id,
                frontier_revision=rev,dataset_id=dataset,topology_generation=0,catalog_id=catalog_id,
                owner_epoch=1,owner_incarnation=sql(3),snapshot_id=count+1,
                request_digest='\\x'+literal_cut(receipts[:count],b['last'],count+1),
                dictionary_count=8 if count==4 else b['last'],payload_count=int(count==4),
                eligibility_count=int(count==4),receive_count=int(count==4),provenance_count=int(count==4)))
            state['consumer_frontiers'].append(row(consumer_id=catalog_id,frontier_revision=rev,
                dataset_id=dataset,topology_generation=0,shard_id=0,owner_epoch=1,last_complete_sequence=b['last'],
                attestation_kind='archive-batch',attestation_digest='\\x'+b['archive_digest']))
            state['archive_frontier_batches'] += [row(consumer_id=catalog_id,frontier_revision=rev,
                attempt_id=a['attempt_id']) for a in attempts[:count]]
        assert validate(state,catalog,setup,wanted) == receipts
    rejected = 0
    faults = ('digest','unsealed','partial','counts','missing-batch','foreign-owner','old-xmin','extra-native-attempt')
    for fault in faults:
        bad, native_bad = copy.deepcopy(state), copy.deepcopy(catalog)
        if fault=='digest': bad['archive_attempts'][0]['batch_digest']='\\x'+'00'*32
        elif fault=='unsealed': bad['consumer_frontier_revisions'][-1]['sealed']=False
        elif fault=='partial': bad['consumer_frontiers'][-1]['last_complete_sequence']=11
        elif fault=='counts': bad['archive_frontier_snapshots'][-1]['payload_count']=0
        elif fault=='missing-batch': bad['archive_frontier_batches'].pop()
        elif fault=='foreign-owner': bad['owner_epochs'].append(dict(archive,resource_id=sql(99)))
        elif fault=='old-xmin': bad['owner_epochs'][0]['xmin']='999'
        else: native_bad['bigbrotr_archive.attempts'].append(dict(native[-1],ordinal=5))
        try: validate(bad,native_bad,setup,wanted)
        except (AssertionError, ValueError): rejected += 1
        else: raise AssertionError('corrupt oracle fixture accepted: '+fault)
    assert rejected == len(faults)
    return dict(complete_frontier_schedules=2,cut_digest_crosschecks=4,frontier_negative_controls=rejected)
