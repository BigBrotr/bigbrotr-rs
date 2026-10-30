"""Literal signed bytes, complete manifest and independent Control relationships."""
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys

# Reuse the independent research BIP340 equation, with no native dependency.
_spec = importlib.util.spec_from_file_location(
    "export_signed_fixture", Path(__file__).resolve().parents[1] / "identity-contract/signed_fixture.py")
_signed = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _signed
_spec.loader.exec_module(_signed)


def validate(before, after, home, initialized, catalog_id, *, expected_principal='10'*16,
             expected_table_count=69, expected_tables=None, destination_root=None, initial_cut=12, retained_compacted_epoch2=False, retained_second_export=False):
    """Placement/identity expectations come from the caller's independent setup.

    A newer schema supplies its expected inventory explicitly; observed table
    names must not be used to manufacture that expectation.
    """
    assert re.fullmatch('[0-9a-f]{32}', expected_principal)
    assert type(retained_compacted_epoch2) is bool
    assert type(retained_second_export) is bool and (not retained_second_export or retained_compacted_epoch2)
    assert type(initial_cut) is int and initial_cut in ((312,) if retained_compacted_epoch2 else (8,12)), 'closed recipe cut required'
    frontier,snapshot,topology=(4,68,'1') if retained_compacted_epoch2 else (4,5,initialized['topology_generation'])
    source_bounds=[(1,312),(2,10)] if retained_compacted_epoch2 else [(1,initial_cut)]
    principal = '\\x' + expected_principal
    assert type(expected_table_count) is int and expected_table_count > 0
    assert expected_table_count == 69 or expected_tables is not None
    if expected_tables is not None:
        assert not isinstance(expected_tables, str)
        expected_tables = tuple(expected_tables)
        assert len(expected_tables) == len(set(expected_tables)) == expected_table_count
        assert all(re.fullmatch('[a-z][a-z0-9_]*', name) for name in expected_tables)
        assert set(before) == set(expected_tables)
    if destination_root is None:
        destination_root = home / 'destination'
    else:
        destination_root = Path(destination_root)
        assert destination_root.is_absolute()
    mutable = {'owner_epochs', 'role_instances', 'browser_sessions', 'login_buckets',
        'local_accounts', 'management_audit', 'management_commands', 'scope_grants',
        'scope_grant_changes', 'snapshot_leases', 'archive_snapshot_pins', 'retention_claims',
        'jobs', 'job_input_bounds', 'job_admissions', 'export_jobs',
        'export_execution_units', 'artifacts', 'artifact_files'}
    assert set(before) == set(after) and len(after) == expected_table_count
    assert all(before[t] == after[t] for t in before.keys() - mutable)
    for table in ('owner_epochs', 'role_instances', 'jobs', 'job_admissions',
        'management_audit', 'management_commands'):
        assert all(r in after[table] for r in before[table]), table
    # Login removes the actor's previously revoked session. Audit history stays
    # immutable; session retention must not be confused with scientific history.
    old_session, = before['browser_sessions']
    session, = after['browser_sessions']
    assert old_session['revoked_at_us'] is not None and session['revoked_at_us'] is not None
    assert old_session['session_id'] != session['session_id']
    assert old_session['principal_id'] == session['principal_id'] == principal
    assert session['issued_at_us'] >= old_session['revoked_at_us']
    owners = [r for r in after['owner_epochs'] if r not in before['owner_epochs']]
    owner, = owners
    assert owner['role'] == 'control' and owner['state'] == 'released'
    assert owner['released_at_us'] is not None
    assert not after['reauthentication_proofs']
    assert len(after['scope_grants']) == len(after['scope_grant_changes']) == 3
    scope = initialized['private_scope']
    for grant in after['scope_grants']:
        assert grant['enabled'] and grant['generation'] == 1
        assert grant['dataset_id'] == '\\x' + scope['dataset_id']
        assert grant['principal_id'] == principal
        assert grant['allocation_domain'] == '\\x' + scope['allocation_domain'] and grant['ordinal'] == 3
    assert {g['action'] for g in after['scope_grants']} == {'history.read', 'export', 'private-export'}
    account,=after['local_accounts']
    assert account['grant_revision'] == 4
    result = json.loads((home / 'export-client-result.json').read_text())
    assert len([r for r in before['jobs'] if r['kind']=='export']) == int(retained_second_export)
    assert len([r for r in after['jobs'] if r['kind']=='export']) == 1+int(retained_second_export)
    if retained_second_export:
        job=next_row(after['jobs'],'job_id','\\x'+result['job_id'])
    else:
        job,=[r for r in after['jobs'] if r['kind']=='export']
    assert job['job_id'] == '\\x' + result['job_id'] and job['state'] == 'succeeded'
    assert job['checkpoint_revision'] == 1 and job['input_complete']
    assert job['owner_resource_id'] == owner['resource_id'] and job['owner_epoch'] == owner['epoch']
    admission, = [r for r in after['job_admissions'] if r['job_id'] == job['job_id']]
    assert admission['admission_ordinal'] == (4 if retained_second_export else 3 if retained_compacted_epoch2 else 2)
    if retained_second_export:
        detail,unit,artifact = (next_row(after[t],'job_id',job['job_id']) for t in
            ('export_jobs','export_execution_units','artifacts'))
        file=next_row(after['artifact_files'],'artifact_id',artifact['artifact_id'])
        bounds=[r for r in after['job_input_bounds'] if r['job_id']==job['job_id']]
    else:
        # First/default export retains the original closed whole inventory.
        # Only the explicit second job may select among validated prior rows.
        detail,=after['export_jobs'];unit,=after['export_execution_units']
        artifact,=after['artifacts'];file,=after['artifact_files']
        bounds=after['job_input_bounds']
    assert detail['job_id'] == unit['job_id'] == artifact['job_id'] == job['job_id']
    assert file['artifact_id'] == unit['artifact_id'] == artifact['artifact_id']
    assert detail['frontier_revision'] == frontier and detail['catalog_id'] == '\\x' + catalog_id
    assert unit['max_output_bytes'] == 256*1024**2 and unit['max_temporary_bytes'] == 512*1024**2
    bounds=sorted(bounds,key=lambda row:row['owner_epoch'])
    assert [(r['owner_epoch'],r['last_complete_sequence']) for r in bounds]==source_bounds
    assert all(r['job_id']==job['job_id'] and r['shard_id']==0 for r in bounds)
    verdict=verify_artifact(after,home,initialized,catalog_id,destination_root=destination_root,
        initial_cut=initial_cut,retained_compacted_epoch2=retained_compacted_epoch2)
    pins = {r['lease_id']: r for r in after['archive_snapshot_pins']}
    leases = {r['lease_id']: r for r in after['snapshot_leases']}
    assert pins[detail['pin_lease_id']]['purpose'] == 'export'
    assert leases[detail['pin_lease_id']]['state'] == 'active'
    assert pins[artifact['verification_pin_id']]['purpose'] == 'reader'
    assert all(leases[key]['state'] == 'released' for key, pin in pins.items() if pin['purpose'] == 'reader')
    if retained_second_export:
        second_job_rows(before,after,home,initialized,catalog_id,verdict)
    return verdict


def next_row(rows,key,value):
    row,=[r for r in rows if r[key]==value]
    return row


def second_job_rows(before,after,home,initialized,catalog_id,verdict):
    """Validate every extra export row before projecting the existing history checks.

    Only this closed second job is removed. Its two released operation readers
    remain, so the maintained full82-table history/session oracle still checks
    startup, authentication, all prior rows and the ordinary Control release.
    """
    bx='\\x';scope=initialized['private_scope'];dataset=bx+scope['dataset_id']
    result=json.loads((home/'export-client-result.json').read_text())
    spec=json.loads((home/'export-input.json').read_text())
    event=json.loads((home/'event.jsonl').read_text())
    original=json.loads((home/'conflict-event.jsonl').read_text())
    assert spec['retained_second_export'] is spec['retained_compacted_epoch2'] is spec['startup_quiescence'] is True
    assert event['id']!=original['id']
    assert event['id']=='32be96c6ec271705225aad466eebc4f8352c65960e619164be86eae0d442a791'
    assert verdict['jsonl_bytes']==358 and verdict['jsonl_sha256']=='492cfca9c1834e722bfa814407f961ab7aa91911c4801f985f1c71df1269dbc0'
    account,=before['local_accounts'];actor=account['principal_id']
    session,=after['browser_sessions']
    owner,=[r for r in after['owner_epochs'] if r not in before['owner_epochs']]
    exact=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'))
    tables=('jobs','export_jobs','job_admissions','job_input_bounds','export_execution_units',
        'artifacts','artifact_files','management_commands')
    added={t:[r for r in after[t] if exact(r) not in {exact(old) for old in before[t]}] for t in tables}
    for table in tables:
        assert {exact(r) for r in before[table]} <= {exact(r) for r in after[table]}, ('old export row/xmin changed',table)
        assert len(added[table])==(2 if table=='job_input_bounds' else 1), ('unexpected export work',table)
        assert len(after[table])==len(before[table])+len(added[table]), ('extra old export row',table)
    for table in ('local_accounts','scope_grants','scope_grant_changes','reauthentication_proofs'):
        assert exact(before[table])==exact(after[table]), table
    job,=added['jobs'];detail,=added['export_jobs'];unit,=added['export_execution_units']
    artifact,=added['artifacts'];file,=added['artifact_files'];command,=added['management_commands']
    admission,=added['job_admissions']
    job_id=bx+result['job_id'];artifact_id=bx+verdict['artifact_id'];key='d2'*16
    def identity(value):
        assert re.fullmatch(r'\\x[0-9a-f]{32}',value) and int(value[2:],16)>0
    identity(job_id);identity(artifact_id)
    assert job_id not in {r['job_id'] for r in before['jobs']}
    assert artifact_id not in {r['artifact_id'] for r in before['artifacts']}
    old_detail,=before['export_jobs']
    assert old_detail['event_id']==bx+original['id'] and old_detail['request_key']==bx+'d1'*16
    binding=hashlib.sha256((home/'destination/destination.json').read_bytes()).hexdigest()
    assert old_detail['destination_id']==bx+spec['destination_id'] and old_detail['destination_binding_sha256']==bx+binding
    body=dict(kind='export',format='signed-events-jsonl',query=dict(version=1,resource='event_history',
        mode='snapshot',scope=scope,filters=[dict(ids=[event['id']])]),
        allocation=dict(catalog_id=catalog_id,frontier_revision='4'),
        destination=dict(id=spec['destination_id'],binding_sha256=binding),
        expected_config_revision='1',expected_policy_revision='1')
    observations=json.loads((home/'export-observations.json').read_text())
    def observed(label):return next_row(observations,'case',label)
    accepted=observed('accept-export')
    created,completed=job['created_at_us'],job['completed_at_us']
    receipt=dict(format='bigbrotr-job-acceptance/1',command_key=key,job_id=result['job_id'],
        accepted_at_us=str(created),created=True,execution='unobserved',authority='acceptance-only')
    assert accepted==dict(case='accept-export',status=202,command=body,key=key,receipt=receipt)
    for label in ('second-export-identical-retry','second-export-completed-retry'):
        assert observed(label)==dict(case=label,status=200,receipt=dict(receipt,created=False))
    conflict=observed('second-export-changed-event-conflict')
    assert conflict==dict(case='second-export-changed-event-conflict',status=409,key=key,
        command=dict(body,query=dict(body['query'],filters=[dict(ids=[original['id']])])),
        error=dict(status=409,code='command_conflict',retry_class='check_state'))
    assert not {r['case'] for r in observations}&{'signed-history','no-implicit-scope-authority',
        'enable-history.read','enable-export','enable-private-export'}
    assert result['history'] is None
    status=result['completed']
    assert status==dict(format='bigbrotr-job-status/1',job_id=result['job_id'],kind='export',state='succeeded',
        job_revision='4',checkpoint_revision='1',created_at_us=str(created),completed_at_us=str(completed),
        observed_at_us=status['observed_at_us'],reason_code=None,authority='observation-only')
    assert created<=unit['reserved_at_us']<completed<=int(status['observed_at_us'])<=session['revoked_at_us']
    # Only explicit /me touches idle time. Job traffic authenticates the
    # existing lifetime; its successful status observation must precede expiry.
    assert session['issued_at_us']<=session['last_seen_at_us']<=created
    assert int(status['observed_at_us'])<min(session['expires_at_us'],session['idle_expires_at_us'])
    def same(row,**expected):assert exact(row)==exact(dict(expected,xmin=row['xmin'])), ('closed export row',row)
    raw=bytes.fromhex;be=lambda value,width=8:int(value).to_bytes(width,'big')
    request_digest=hashlib.sha256(b'bigbrotr/export-event/1\0'+raw(scope['dataset_id'])+raw(actor[2:])+raw(key)
        +raw(scope['dataset_id'])+be(4,2)+raw(scope['allocation_domain'])+be(scope['ordinal'])
        +raw(event['id'])+be(100,2)+raw(catalog_id)+be(4)+raw(spec['destination_id'])+raw(binding)
        +b'\x01\x01'+be(1)+be(1)).hexdigest()
    same(job,job_id=job_id,dataset_id=dataset,principal_id=actor,request_key=bx+key,request_digest=bx+request_digest,
        kind='export',config_revision=1,policy_revision=1,created_at_us=created,input_topology_generation=1,
        input_complete=True,export_job_protocol=1,acquisition_job_protocol=None,state='succeeded',job_revision=4,
        checkpoint_revision=1,owner_role='control',owner_resource_id=owner['resource_id'],owner_epoch=owner['epoch'],
        owner_incarnation=owner['process_incarnation'],completed_at_us=completed,reason_code=None,
        result_manifest_digest=bx+verdict['manifest_sha256'])
    same(detail,job_id=job_id,dataset_id=dataset,actor_id=actor,request_key=bx+key,format='signed-events-jsonl',
        visibility='private',dictionary_kind=4,allocation_domain=bx+scope['allocation_domain'],ordinal=3,
        event_id=bx+event['id'],page_size=100,catalog_id=bx+catalog_id,frontier_revision=4,
        destination_id=bx+spec['destination_id'],destination_binding_sha256=bx+binding,
        pin_lease_id=detail['pin_lease_id'],allocation_pin_id=detail['allocation_pin_id'],session_id=session['session_id'],
        actor_credential_revision=account['credential_revision'],actor_authentication_revision=account['authentication_revision'],
        actor_grant_revision=4)
    same(admission,job_id=job_id,admission_ordinal=4)
    assert sorted((r['shard_id'],r['owner_epoch'],r['last_complete_sequence']) for r in added['job_input_bounds'])==[(0,1,312),(0,2,10)]
    for row in added['job_input_bounds']:
        same(row,job_id=job_id,dataset_id=dataset,topology_generation=1,shard_id=0,
            owner_epoch=row['owner_epoch'],last_complete_sequence=row['last_complete_sequence'])
    audit=next_row(after['management_audit'],'audit_id',command['audit_id'])
    identity(audit['audit_id'])
    assert audit['audit_id'] not in {r['audit_id'] for r in before['management_audit']}
    same(audit,audit_id=audit['audit_id'],dataset_id=dataset,actor_id=actor,action='job.export.accept',outcome='accepted',
        created_at_us=created,config_revision=1,subject_id=bx+key,policy_revision=1,reason=None,
        requester_database_role=None,requester_os_uid=None)
    same(command,actor_id=actor,request_key=bx+key,dataset_id=dataset,action='job.export.accept',request_digest=bx+request_digest,
        expected_config_revision=1,policy_revision=1,audit_id=audit['audit_id'],accepted_at_us=created,
        authority_mode=None,document_digest=None)
    same(unit,job_id=job_id,artifact_id=artifact_id,max_output_bytes=256*1024**2,max_temporary_bytes=512*1024**2,
        reservation_transaction=unit['xmin'],reserved_at_us=unit['reserved_at_us'])
    same(artifact,artifact_id=artifact_id,job_id=job_id,dataset_id=dataset,principal_id=actor,
        destination_id=bx+spec['destination_id'],destination_binding_sha256=bx+binding,
        manifest_format='bigbrotr-signed-events-export/1',manifest_bytes=artifact['manifest_bytes'],
        manifest_sha256=bx+verdict['manifest_sha256'],file_count=1,total_bytes=artifact['manifest_bytes']+verdict['jsonl_bytes'],
        verification_pin_id=artifact['verification_pin_id'],completed_at_us=completed)
    same(file,artifact_id=artifact_id,file_id='events',relative_path='events.jsonl',format='nostr-jsonl/1',
        byte_count=verdict['jsonl_bytes'],row_count=1,sha256=bx+verdict['jsonl_sha256'])
    assert job['xmin']==artifact['xmin']==file['xmin']
    assert artifact['total_bytes']<=unit['max_output_bytes']
    acceptance=[detail,command,audit,admission,*added['job_input_bounds']]
    ids=(detail['allocation_pin_id'],detail['pin_lease_id'],artifact['verification_pin_id'])
    assert len(set(ids))==3 and not set(ids)&{r['lease_id'] for r in before['snapshot_leases']}
    for ident,retained in zip(ids,(False,True,False)):
        identity(ident)
        pin=next_row(after['archive_snapshot_pins'],'lease_id',ident)
        lease=next_row(after['snapshot_leases'],'lease_id',ident)
        claims=[r for r in after['retention_claims'] if r['lease_id']==ident]
        purpose,state=('export','active') if retained else ('reader','released')
        ttl=pin['requested_ttl_us']
        assert ttl==30_000_000 if retained else 0<ttl<=5_000_000
        same(pin,lease_id=ident,dataset_id=dataset,consumer_id=bx+catalog_id,frontier_revision=4,
            issuer_role='control',issuer_resource=owner['resource_id'],issuer_epoch=owner['epoch'],
            issuer_incarnation=owner['process_incarnation'],purpose=purpose,requested_ttl_us=ttl,
            export_job_id=job_id if retained else None)
        acquired,expires=lease['acquired_at_us'],lease['expires_at_us']
        released=None if retained else lease['released_at_us']
        assert owner['acquired_at_us']<=acquired<expires<=owner['lease_expires_at_us'] and expires-acquired<=ttl
        assert lease['updated_at_us']==(acquired if retained else released)
        same(lease,lease_id=ident,dataset_id=dataset,consumer_id=bx+catalog_id,frontier_revision=4,topology_generation=1,
            holder_kind='role_instance',holder_role_instance_id=owner['role_instance_id'],holder_role='control',
            holder_incarnation=owner['process_incarnation'],holder_job_id=None,acquired_at_us=acquired,
            updated_at_us=lease['updated_at_us'],expires_at_us=expires,archive_pin_protocol=1,lease_revision=1 if retained else 2,
            state=state,released_at_us=released,release_reason=None if retained else 'reader_joined')
        assert sorted((r['shard_id'],r['owner_epoch'],r['first_sequence'],r['last_sequence']) for r in claims)==[(0,1,1,312),(0,2,1,10)]
        for claim in claims:
            identity(claim['claim_id'])
            assert sum(r.get('claim_id')==claim['claim_id'] for r in after['retention_claims'])==1
            same(claim,claim_id=claim['claim_id'],lease_id=ident,dataset_id=dataset,topology_generation=1,shard_id=0,
                owner_epoch=claim['owner_epoch'],first_sequence=1,last_sequence=claim['last_sequence'],purpose=purpose,state=state,
                created_at_us=claim['created_at_us'],released_at_us=None if retained else claim['released_at_us'],
                release_reason=None if retained else 'reader_joined')
            assert acquired<=claim['created_at_us']<expires and claim['xmin']==lease['xmin']
            if not retained:assert claim['created_at_us']<=claim['released_at_us']<=released
        if retained:
            assert created<=acquired<unit['reserved_at_us']
            acceptance += [pin,lease,*claims]
        else:
            consumed=created if ident==detail['allocation_pin_id'] else completed
            assert acquired<=consumed<expires and consumed<=released<=owner['released_at_us']
            assert pin['xmin']!=lease['xmin']
    assert {r['xmin'] for r in acceptance}=={detail['xmin']}
    assert len({detail['xmin'],unit['xmin'],job['xmin'],owner['xmin']})==4
    projected=dict(after,**{table:before[table] for table in tables})
    projected['management_audit']=[r for r in after['management_audit'] if r is not audit]
    for table in ('archive_snapshot_pins','snapshot_leases','retention_claims'):
        projected[table]=[r for r in after[table] if r['lease_id']!=detail['pin_lease_id']]
    return projected


def verify_artifact(state,home,initialized,catalog_id,*,destination_root,initial_cut=312,retained_compacted_epoch2=True):
    """Independently reread a selected standalone artifact, including after restart."""
    frontier,snapshot,topology=(4,68,'1') if retained_compacted_epoch2 else (4,5,initialized['topology_generation'])
    source_bounds=[(1,312),(2,10)] if retained_compacted_epoch2 else [(1,initial_cut)]
    supplied_scope=initialized['private_scope']
    assert set(supplied_scope)=={'dataset_id','dictionary_kind','allocation_domain','ordinal'}
    scope={key:supplied_scope[key] for key in ('dataset_id','dictionary_kind','allocation_domain','ordinal')}
    result=json.loads((home/'export-client-result.json').read_text())
    assert result['status']=='PASS'
    job=next_row(state['jobs'],'job_id','\\x'+result['job_id'])
    artifact=next_row(state['artifacts'],'job_id',job['job_id'])
    file=next_row(state['artifact_files'],'artifact_id',artifact['artifact_id'])
    original = json.loads((home / 'event.jsonl').read_text())
    assert set(original)=={'id','pubkey','created_at','kind','tags','content','sig'}
    message=hashlib.sha256(_signed.canonical(original)).digest()
    assert message.hex()==original['id']
    assert _signed.equation_verify(message,bytes.fromhex(original['pubkey']),bytes.fromhex(original['sig'])), 'independent BIP340 signature'
    # Protocol field order is frozen here; no product serializer participates.
    encoded = (json.dumps({key: original[key] for key in (
        'id', 'pubkey', 'created_at', 'kind', 'tags', 'content', 'sig')},
        ensure_ascii=False, separators=(',', ':')) + '\n').encode()
    destination = destination_root / artifact['artifact_id'][2:]
    actual = (destination / 'events.jsonl').read_bytes()
    assert actual == encoded and len(actual.splitlines()) == 1
    file_hash = hashlib.sha256(encoded).hexdigest()
    assert file['byte_count'] == len(encoded) and file['row_count'] == 1
    assert file['sha256'] == '\\x' + file_hash
    manifest = dict(format='bigbrotr-signed-events-export/1', artifact_id=artifact['artifact_id'][2:],
        job_id=result['job_id'], visibility='private', fidelity='full-signed-events',
        origin='retained-scoped-archive', source=dict(cut=dict(dataset_id=scope['dataset_id'],
            topology_generation=topology,
            bounds=[dict(stream=dict(shard_id=0,owner_epoch=str(epoch)),last_complete_sequence=str(cut)) for epoch,cut in source_bounds]),
            catalog_id=catalog_id, frontier_revision=str(frontier), snapshot_id=str(snapshot)),
        selection=dict(event_id=original['id'], scope=scope),
        accepted_revisions=dict(configuration='1', policy='1'), included_families=['signed-events'],
        excluded_families=['internal-payload-envelopes', 'eligibility-facts', 'first-local-receive',
            'relay-provenance', 'imported-claims', 'measurements', 'documents', 'dictionaries',
            'coverage', 'derived-results', 'deployment-state'],
        files=[dict(file_id='events', path='events.jsonl', format='nostr-jsonl/1',
            bytes=str(len(encoded)), rows='1', sha256=file_hash)])
    raw = (destination / 'manifest.json').read_bytes()
    assert raw == (json.dumps(manifest,ensure_ascii=False,separators=(',',':'))+'\n').encode(), 'literal complete manifest bytes'
    manifest_hash = hashlib.sha256(raw).hexdigest()
    assert artifact['manifest_sha256'] == job['result_manifest_digest'] == '\\x' + manifest_hash
    assert artifact['manifest_bytes'] == len(raw) and artifact['total_bytes'] == len(raw) + len(encoded)
    assert artifact['file_count'] == 1 and artifact['completed_at_us'] == job['completed_at_us']
    detail=next_row(state['export_jobs'],'job_id',job['job_id'])
    assert detail['event_id']=='\\x'+original['id'] and detail['catalog_id']=='\\x'+catalog_id and detail['frontier_revision']==frontier
    for ident,purpose,status in ((detail['pin_lease_id'],'export','active'),
            (artifact['verification_pin_id'],'reader','released')):
        pin=next_row(state['archive_snapshot_pins'],'lease_id',ident)
        lease=next_row(state['snapshot_leases'],'lease_id',ident)
        assert pin['purpose']==purpose and lease['state']==status
    leases={r['lease_id']:r for r in state['snapshot_leases']}
    assert all(leases[r['lease_id']]['state']=='released' for r in state['archive_snapshot_pins'] if r['purpose']=='reader')
    assert not any(p.name.endswith('.partial') for p in destination.rglob('*'))
    return dict(job_id=result['job_id'], artifact_id=artifact['artifact_id'][2:],
        jsonl_bytes=len(encoded), jsonl_sha256=file_hash, manifest_sha256=manifest_hash,
        manifest=manifest, export_pin_retained=True, all_fresh_readers_released=True,
        original_acquisition_and_publication_unchanged=True)
