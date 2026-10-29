"""Literal signed bytes, complete manifest and independent Control relationships."""
import hashlib
import json
from pathlib import Path
import re


def validate(before, after, home, initialized, catalog_id, *, expected_principal='10'*16,
             expected_table_count=69, expected_tables=None, destination_root=None, initial_cut=12, retained_compacted_epoch2=False):
    """Placement/identity expectations come from the caller's independent setup.

    A newer schema supplies its expected inventory explicitly; observed table
    names must not be used to manufacture that expectation.
    """
    assert re.fullmatch('[0-9a-f]{32}', expected_principal)
    assert type(retained_compacted_epoch2) is bool
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
    assert after['local_accounts'][0]['grant_revision'] == 4
    result = json.loads((home / 'export-client-result.json').read_text())
    job, = [r for r in after['jobs'] if r['kind'] == 'export']
    assert job['job_id'] == '\\x' + result['job_id'] and job['state'] == 'succeeded'
    assert job['checkpoint_revision'] == 1 and job['input_complete']
    assert job['owner_resource_id'] == owner['resource_id'] and job['owner_epoch'] == owner['epoch']
    admission, = [r for r in after['job_admissions'] if r['job_id'] == job['job_id']]
    assert admission['admission_ordinal'] == (3 if retained_compacted_epoch2 else 2)
    detail, = after['export_jobs']; unit, = after['export_execution_units']
    artifact, = after['artifacts']; file, = after['artifact_files']
    assert detail['job_id'] == unit['job_id'] == artifact['job_id'] == job['job_id']
    assert file['artifact_id'] == unit['artifact_id'] == artifact['artifact_id']
    assert detail['frontier_revision'] == frontier and detail['catalog_id'] == '\\x' + catalog_id
    assert unit['max_output_bytes'] == 256*1024**2 and unit['max_temporary_bytes'] == 512*1024**2
    bounds=sorted(after['job_input_bounds'],key=lambda row:row['owner_epoch'])
    assert [(r['owner_epoch'],r['last_complete_sequence']) for r in bounds]==source_bounds
    assert all(r['job_id']==job['job_id'] and r['shard_id']==0 for r in bounds)
    original = json.loads((home / 'event.jsonl').read_text())
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
    assert json.loads(raw) == manifest
    manifest_hash = hashlib.sha256(raw).hexdigest()
    assert artifact['manifest_sha256'] == job['result_manifest_digest'] == '\\x' + manifest_hash
    assert artifact['manifest_bytes'] == len(raw) and artifact['total_bytes'] == len(raw) + len(encoded)
    assert artifact['file_count'] == 1 and artifact['completed_at_us'] == job['completed_at_us']
    pins = {r['lease_id']: r for r in after['archive_snapshot_pins']}
    leases = {r['lease_id']: r for r in after['snapshot_leases']}
    assert pins[detail['pin_lease_id']]['purpose'] == 'export'
    assert leases[detail['pin_lease_id']]['state'] == 'active'
    assert pins[artifact['verification_pin_id']]['purpose'] == 'reader'
    assert all(leases[key]['state'] == 'released' for key, pin in pins.items() if pin['purpose'] == 'reader')
    assert not any(p.name.endswith('.partial') for p in destination.rglob('*'))
    return dict(job_id=result['job_id'], artifact_id=artifact['artifact_id'][2:],
        jsonl_bytes=len(encoded), jsonl_sha256=file_hash, manifest_sha256=manifest_hash,
        manifest=manifest, export_pin_retained=True, all_fresh_readers_released=True,
        original_acquisition_and_publication_unchanged=True)
