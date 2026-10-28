"""Closed Archive-only refusal: independent source bytes and retained prefix.

No application output chooses expected facts, ranges or row membership. Generated
identities and bounded clock scalars parameterize the existing literal encoders.
"""
import hashlib

from cases import first_launch_index_oracle as index
from cases import first_launch_restart as source


ARCHIVE_TABLES = ('archive_frontier_snapshots', 'archive_frontier_batches')


def archived_prefix(before, after):
    """Two published attempts remain exact; the third is only an issued intent."""
    attempts = sorted(after['archive_attempts'], key=lambda row: row['ordinal'])
    index.exact([row['ordinal'] for row in attempts], [1, 2, 3])
    index.exact(attempts[:2], sorted(before['archive_attempts'], key=lambda row: row['ordinal']))
    third = attempts[2]
    owner = index.only(before['owner_epochs'], role='control', state='active')
    consumer = index.only(before['consumers'], consumer_role='archive')
    for field, value in dict(dataset_id=consumer['dataset_id'], catalog_id=consumer['consumer_id'],
            expected_predecessor=2, schema_version=1, topology_generation=0,
            issuer_role='control', issuer_resource=owner['resource_id'],
            issuer_epoch=owner['epoch'], issuer_incarnation=owner['process_incarnation']).items():
        index.exact(third[field], value)
    source.clock(third['issued_at_us'], owner['acquired_at_us'],
                 index.only(after['owner_epochs'], role='control', resource_id=owner['resource_id'], epoch=owner['epoch'])['lease_expires_at_us'])
    assert len({row['attempt_id'] for row in attempts}) == 3
    assert len({row['worker_incarnation'] for row in attempts}) == 3
    for field in ('attempt_id', 'worker_incarnation', 'coordinator_incarnation'):
        assert any(index.raw(third[field], 16))
    assert third['coordinator_incarnation'] == owner['process_incarnation']
    assert third['worker_incarnation'] != third['coordinator_incarnation']
    assert third['persistence_protocol'] == 1
    assert third['coordinator_binding_sha256'] == attempts[1]['coordinator_binding_sha256']
    for table in ARCHIVE_TABLES:
        index.exact(after[table], before[table])
    for table in ('consumers', 'consumer_frontier_revisions', 'consumer_frontiers'):
        selected = lambda state: [row for row in state[table] if row['consumer_id'] == consumer['consumer_id']]
        index.exact(selected(after), selected(before))
    assert consumer['current_revision'] == 2 and consumer['current_topology_generation'] == 0
    assert index.only(before['consumer_frontiers'], consumer_id=consumer['consumer_id'],
                      frontier_revision=2)['last_complete_sequence'] == 3
    return third


def science(state, report, event, wires, rows, store, completed):
    from installed_archive_oracle import expected
    from installed_progress_context_oracle import archive_request_progress, verify_archive_progress
    from installed_sync_oracle import decode
    job, _, attribution = source.attribution(state, 'b1')
    original = source.originals(store, report['producer_slot_id'], 1, 4)
    dictionary, = [row for row in rows['dictionaries'] if row['source'][1:] == [0, 1, 4]]
    payload, = rows['events']
    dictionary_time = source.clock(dictionary['recorded_at'], job['created_at_us'], wires[0]['connected_at_us'])
    received = decode(original[3].split(b'\0', 1)[1])[3][5][0]
    source.clock(received, wires[0]['sent_at_us'], wires[0]['closed_at_us'])
    recorded = source.clock(payload['recorded_at'], received, completed)
    wanted = expected(state, report, event, dictionary_time, received, recorded,
                      wires[0]['clock_resolution_us'], native_options=dict(acknowledge=True))
    assert original == wanted['inputs'], 'original admission bytes differ'
    manifest = next(value for cf, key, value in wanted['native'] if cf == 'default' and key == b'\x01\x01')
    owner = index.only(state['owner_epochs'], role='control')
    source.requests(state, 1, manifest, wanted['inputs'], wanted['receipts'], owner,
                    {4: archive_request_progress(event, wires)}, attribution)
    verify_archive_progress(state, wires, completed, event, wanted['native'], wanted['inputs'],
                            wanted['receipts'], owner)
    for batch in wanted['batches']:
        batch.update(epoch=1, topology=0)
    return wanted


def retained_input(row, batch, initialization):
    assert (batch['first'], batch['last']) == (4, 7)
    assert row['ordinal'] == 3 and row['batch_digest'] == '\\x' + batch['archive_digest']
    return index.retained_input_bytes(row, batch, initialization)


def archive_science(prefix, census, wanted):
    selected = dict(batches=wanted['batches'][:2], tables={name: [row for row in rows
        if row['source_record_id']['sequence'] <= 3] for name, rows in wanted['tables'].items()})
    source.publication(prefix, census, selected, revision=2, snapshot=3, topology=0, cuts=[(1, 3)])


def parked_roles(value, state, before_us, after_us):
    index.live_roles(value, state, before_us, after_us, archive_unavailable=True)


def retained_age(prefix, parked, first, last):
    """The same BOOTTIME observation ages through two HTTP sampling brackets."""
    for bracket in (first, last):
        assert set(bracket) == {'before_ns', 'after_ns'}
        assert all(type(n) is int and n >= 0 for n in bracket.values())
        assert bracket['before_ns'] <= bracket['after_ns']
    assert first['after_ns'] <= last['before_ns']
    old = int(index.only(prefix['capabilities'], capability='archive_publication')['time']['age_us'])
    new = int(index.only(parked['capabilities'], capability='archive_publication')['time']['age_us'])
    # Both durations truncate to integer microseconds. One microsecond covers
    # that rounding; the request brackets provide the actual scheduling bounds.
    lower = (last['before_ns'] - first['after_ns']) // 1000 - 1
    upper = (last['after_ns'] - first['before_ns'] + 999) // 1000 + 1
    assert lower <= new - old <= upper, 'Archive observation was refreshed or changed'


def prefix_preservation(before, after):
    assert set(before) == set(after) and len(before) == 82
    additions = dict(acquisition_selectors=1, acquisition_scopes=1, acquisition_scope_commands=1,
        jobs=1, acquisition_jobs=1, job_admissions=1, management_commands=2, management_audit=2,
        producer_requests=2, producer_outcomes=2, producer_checkpoint_dependencies=9,
        acquisition_progress=1, archive_attempts=1, projection_plans=2, projection_plan_chunks=20,
        projection_chunks=20, projection_chunk_bounds=20, index_frontier_plans=2,
        consumer_frontier_revisions=2, consumer_frontiers=2)
    appended = set(additions)
    mutable = {'owner_epochs', 'producer_epochs', 'producer_checkpoints', 'consumers'}
    assert appended | mutable <= set(before)
    for table in set(before) - appended - mutable:
        index.exact(before[table], after[table])
    for table in appended:
        assert all(row in after[table] for row in before[table]), 'prefix row changed: ' + table
        assert len(after[table]) == len(before[table]) + additions[table], 'unexpected append count: ' + table
    assert len(before['owner_epochs']) == len(after['owner_epochs'])
    for old in before['owner_epochs']:
        new = index.only(after['owner_epochs'], resource_id=old['resource_id'], epoch=old['epoch'])
        allowed = {'lease_expires_at_us', 'xmin'} if old['state'] == 'active' and old['role'] in ('control', 'ingest', 'index') else set()
        index.exact({k:v for k,v in old.items() if k not in allowed},
                    {k:v for k,v in new.items() if k not in allowed})
        assert new['lease_expires_at_us'] >= old['lease_expires_at_us']
    old, = before['producer_epochs']; new, = after['producer_epochs']
    assert old['reserved_through_request'] == 2 and new['reserved_through_request'] == 4
    ignored = {'reserved_through_request', 'xmin'}
    index.exact({k:v for k,v in old.items() if k not in ignored},
                {k:v for k,v in new.items() if k not in ignored})
    assert len(before['consumers']) == len(after['consumers']) == 2
    for old in before['consumers']:
        new = index.only(after['consumers'], consumer_id=old['consumer_id'])
        if old['consumer_role'] == 'archive':
            index.exact(old, new)
        else:
            assert old['consumer_role'] == 'index' and old['current_revision'] == 2 and new['current_revision'] == 4
            ignored = {'current_revision', 'xmin'}
            index.exact({k:v for k,v in old.items() if k not in ignored},
                        {k:v for k,v in new.items() if k not in ignored})
    old, = before['producer_checkpoints']; new, = after['producer_checkpoints']
    assert old['complete_through_request'] == 2 and old['checkpoint_revision'] == 2
    assert new['complete_through_request'] == 4 and new['checkpoint_revision'] == 9
    for field in ('dataset_id', 'producer_slot_id', 'producer_epoch', 'progress_format',
                  'manifest_sha256', 'issuer_resource', 'issuer_epoch', 'issuer_incarnation'):
        index.exact(old[field], new[field])
    # The complete replacement head/digest/dependency membership is separately
    # checked by the existing independent six-classification progress oracle.


def terminal_preservation(live, after, observed_at_us, *, archive_renewal=False):
    assert type(archive_renewal) is bool
    renewing = ('ingest', 'index', 'archive') if archive_renewal else ('ingest', 'index')
    assert set(live) == set(after) and len(live) == 82
    mutable = {'owner_epochs', 'role_instances', 'browser_sessions', 'management_audit'}
    for table in set(live) - mutable:
        index.exact(live[table], after[table])
    for table in ('owner_epochs', 'role_instances'):
        assert len(live[table]) == len(after[table])
        keys = ('role_instance_id',) if table == 'role_instances' else ('resource_id', 'epoch')
        for row in live[table]:
            other = index.only(after[table], **{key: row[key] for key in keys})
            active_control = row['role'] == 'control' and (row.get('state') == 'active'
                if table == 'owner_epochs' else row['retired_at_us'] is None)
            changed = ({'xmin', 'retired_at_us'} if active_control else set()) if table == 'role_instances' else (
                {'xmin', 'state', 'released_at_us', 'lease_expires_at_us', 'exclusion_method', 'exclusion_digest'}
                if active_control else {'xmin', 'lease_expires_at_us'} if row['state'] == 'active' and row['role'] in renewing else set())
            index.exact({k:v for k,v in row.items() if k not in changed},
                        {k:v for k,v in other.items() if k not in changed})
            if table == 'owner_epochs' and not active_control and 'lease_expires_at_us' in changed:
                assert other['lease_expires_at_us'] >= row['lease_expires_at_us']
            if active_control and table == 'owner_epochs':
                assert other['state'] == 'released' and other['exclusion_method'] == 'storage_fenced'
                exclusion = hashlib.sha256(b'bigbrotr/control-exclusion/1\0' + index.raw(row['dataset_id'],16)
                    + index.raw(row['resource_id'],16) + index.be(row['epoch'])
                    + index.raw(row['process_incarnation'],16)*2).digest()
                assert index.raw(other['exclusion_digest'],32) == exclusion
                source.clock(other['released_at_us'], row['acquired_at_us'], other['lease_expires_at_us'])
            elif active_control:
                owner = index.only(after['owner_epochs'], role_instance_id=row['role_instance_id'])
                source.clock(other['retired_at_us'], owner['released_at_us'], observed_at_us)
                assert other['xmin'] == owner['xmin']
    assert len(live['browser_sessions']) == len(after['browser_sessions']) == 1
    session, = live['browser_sessions']; terminal, = after['browser_sessions']
    fields = {'xmin', 'revoked_at_us'}
    index.exact({k:v for k,v in session.items() if k not in fields},
                {k:v for k,v in terminal.items() if k not in fields})
    assert session['revoked_at_us'] is None and type(terminal['revoked_at_us']) is int
    assert all(row in after['management_audit'] for row in live['management_audit'])
    logout, = [row for row in after['management_audit'] if row not in live['management_audit']]
    index.exact({k:v for k,v in logout.items() if k not in ('xmin', 'audit_id', 'created_at_us')},
        dict(dataset_id=session['dataset_id'], actor_id=session['principal_id'],
             action='browser.session.revoke', outcome='accepted', config_revision=1, policy_revision=1,
             subject_id=session['session_id'], reason=None, requester_database_role=None, requester_os_uid=None))
    control = index.only(live['owner_epochs'], role='control', state='active')
    released = index.only(after['owner_epochs'], resource_id=control['resource_id'], epoch=control['epoch'])
    source.clock(terminal['revoked_at_us'], session['issued_at_us'], logout['created_at_us'])
    source.clock(logout['created_at_us'], terminal['revoked_at_us'], released['released_at_us'])
    assert logout['xmin'] == terminal['xmin']
