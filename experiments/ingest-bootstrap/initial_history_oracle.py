"""Independent observations of original history beneath a prepared successor.

No product codecs, SQL or process control. The harness separately establishes
that the preparing process finished and starts a new process with configuration
only. A transcript cannot by itself prove process identity or native exclusion.
"""
import copy
import json

from bootstrap_oracle import cbor
from initial_succession_oracle import (
    be, binary, bytea, digest, encoded, ended, expected, native_rows, only, same,
)


# Canonical contents of the previously qualified ac644ef normal prepared seed.
# These bind all 71 tables/52 native rows/four inputs, including original xmin.
# The harness also pins the enclosing artifact bytes and their producing run.
SEED_CONTROL = 'ccec0bb1c953efaf663e2c07b7d6f6281c9e249388015e816816bb24c5178126'
SEED_NATIVE = 'af55bf86c2004867e8f2fe3f977929cd05e87ccaff96b74f17c0af2b628b08d3'
SEED_INPUTS = '5b925ffacfdebc27efde1c8b577757581692b4359272b708779a1b97bf82e7ea'
SEED_SEQUENCE = 62
FAILURES = {
    'history-missing-intent': ('history-load', None, 'Control(InvalidMetadata)'),
    'history-corrupt-intent': ('history-load', None, 'Control(InvalidMetadata)'),
    'history-missing-handoff': ('history-load', None, 'Native(StateMismatch)'),
    'history-nonempty-target': ('history-load', None, 'Native(StateMismatch)'),
    'history-missing-input': ('request', 4, 'Native(Input(Missing))'),
    'history-missing-outcome': ('history-load', None, 'Control(Backend)'),
}


def rows_dump(rows):
    return dict(event='dump', rows=[dict(cf=cf, key=key.hex(), value=value.hex())
        for (cf, key), value in sorted(rows.items())])


def seed_expected(seed):
    """Check the frozen seed and reconstruct original receipts independently."""
    assert set(seed) == {'control', 'native', 'inputs'}
    pg, native, inputs = seed['control'], seed['native'], seed['inputs']
    assert len(pg) == 71 and len(native['rows']) == 52 and len(inputs) == 4
    assert digest(encoded({k: sorted(v, key=encoded) for k, v in pg.items()})) == SEED_CONTROL
    assert digest(encoded(native)) == SEED_NATIVE
    assert digest(encoded(inputs)) == SEED_INPUTS
    intent = only(pg['initial_ingest_successions'])
    old = binary(intent['previous_manifest'])
    ds, resource, slot = (binary(intent[k]) for k in
        ('dataset_id', 'owner_resource_id', 'producer_slot_id'))
    handoff_key = ('default', b'\x01\x0a' + ds + resource + be(1))
    target_source = ('default', b'\x01\x02' + ds + be(0, 4) + be(2))
    old_rows = native_rows(native['rows']).copy()
    for key in (handoff_key, target_source, ('receipts', b'\x01\x31' + slot + be(2))):
        del old_rows[key]
    old_rows['default', b'\x01\x01'] = old
    # Project the historical identity for the existing independent handoff-byte
    # oracle. This projection does not assert that PostgreSQL was rolled back.
    previous = copy.deepcopy(pg)
    previous['datasets'][0]['topology_generation'] = 0
    previous['owner_epochs'] = [r for r in previous['owner_epochs']
        if r['role'] != 'ingest' or r['epoch'] == 1]
    for table, field in [('producer_epochs', 'producer_epoch'),
                         ('source_streams', 'owner_epoch'), ('allocation_domains', 'owner_epoch')]:
        previous[table] = [r for r in previous[table] if r[field] == 1]
    plan, checked_old, wanted_native = expected(previous, rows_dump(old_rows), inputs,
        intent['created_at_us'])
    assert checked_old == old_rows and wanted_native == native_rows(native['rows'])
    for field in ('previous_manifest', 'target_manifest', 'handoff', 'closure_digest'):
        assert intent[field] == '\\x' + plan[field]
    for field in ('previous_manifest', 'target_manifest', 'handoff'):
        assert intent[field + '_sha256'] == '\\x' + digest(bytes.fromhex(plan[field]))
    assert intent['issued_through'] == 4 and intent['last_complete_sequence'] == 12

    facts = {}
    for sequence in range(1, 13):
        source = ds + be(0, 4) + be(1) + be(sequence)
        value = old_rows['facts', b'\x01\x20' + source]
        assert value[:2] == b'\x01\x20' and digest(value[34:]) == value[2:34].hex()
        facts[sequence] = [[ds, 0, 1, sequence], value[2:34]]
    # Original bootstrap/public+private, private scope append, four observation
    # dictionaries, then the signed event plus provenance: fixed source sets.
    batches = [(1, 2), (3, 3), (4, 7), (8, 12)]
    dependencies = [[1, 2], [3], list(range(3, 8)), list(range(3, 13))]
    receipts = []
    for sequence, ((first, last), closure) in enumerate(zip(batches, dependencies), 1):
        request = only(pg['producer_requests'], request_sequence=sequence)
        outcome = only(pg['producer_outcomes'], request_sequence=sequence)
        receipt = b'bigbrotr/receipt/1\0' + cbor([1, [slot, 1, sequence],
            binary(request['request_digest']), [ds, 0, 1, old[78:94], 1],
            [facts[n] for n in range(first, last + 1)], [facts[n] for n in closure]])
        assert old_rows['receipts', b'\x01\x30' + slot + be(1) + be(sequence)] == receipt
        assert outcome['outcome_bytes'] == bytea(receipt)
        assert outcome['outcome_sha256'] == '\\x' + digest(receipt)
        assert outcome['outcome_kind'] == outcome['policy_epoch'] == 1
        assert outcome['manifest_sha256'] == request['manifest_sha256'] == '\\x' + digest(old)
        assert outcome['request_digest'] == request['request_digest']
        receipts.append(receipt.hex())

    owner = only(pg['owner_epochs'], role='ingest', epoch=1)
    producer = only(pg['producer_epochs'], producer_epoch=1)
    registrar = only(pg['producer_slots'])
    control = only(pg['owner_epochs'], resource_id=registrar['registered_by_resource_id'],
        epoch=registrar['registered_by_epoch'])
    config = only(pg['config_revisions'], revision=1)
    policy = only(pg['policy_revisions'], revision=1)
    registration = dict(event='successor-history-registration', dataset_id=ds.hex(),
        instance_id=only(pg['datasets'])['instance_id'][2:],
        control_resource=control['resource_id'][2:], control_epoch=control['epoch'],
        control_role_instance=control['role_instance_id'][2:],
        control_incarnation=control['process_incarnation'][2:],
        ingest_resource=resource.hex(), ingest_role_instance=owner['role_instance_id'][2:],
        ingest_incarnation=owner['process_incarnation'][2:], shard_id=0,
        config_revision=1, policy_revision=1, config_digest=config['document_digest'][2:],
        policy_digest=policy['document_digest'][2:], previous_topology=None,
        allocation_domains=sorted([[r['dictionary_kind'], r['allocation_domain'][2:]]
            for r in pg['allocation_domains'] if r['owner_epoch'] == 1]),
        producer_slot=slot.hex(), window=producer['outstanding_request_limit'],
        created_at_us=owner['acquired_at_us'], lease_expires_at_us=owner['lease_expires_at_us'])
    proof = dict(event='successor-history-proof', issued_through=4,
        last_complete_sequence=12, **{k: plan[k] for k in
            ('closure_digest', 'previous_manifest', 'target_manifest', 'handoff')})
    return dict(registration=registration, proof=proof, receipts=receipts,
        handoff_key=handoff_key, target_source=target_source, batches=batches)


def preparation(original, native, inputs, mode, seed, known):
    """Permit exactly the named isolated corruption before the actual read."""
    wanted = copy.deepcopy(seed['control'])
    rows = native_rows(seed['native']['rows']).copy()
    originals = copy.deepcopy(seed['inputs'])
    if mode == 'history-missing-intent':
        wanted['initial_ingest_successions'] = []
    elif mode == 'history-corrupt-intent':
        changed = only(original['initial_ingest_successions'])
        previous = only(wanted['initial_ingest_successions'])
        assert changed['xmin'].isdecimal() and all(changed['xmin'] != r['xmin']
            for table in seed['control'].values() for r in table)
        wanted['initial_ingest_successions'] = [dict(previous,
            closure_digest='\\x' + 'ab' * 32, xmin=changed['xmin'])]
    elif mode == 'history-missing-outcome':
        wanted['producer_outcomes'].remove(only(wanted['producer_outcomes'], request_sequence=3))
    elif mode == 'history-missing-handoff':
        del rows[known['handoff_key']]
    elif mode == 'history-nonempty-target':
        rows[known['target_source']] = b'\x01\x02' + be(1)
    elif mode == 'history-missing-input':
        slot = known['registration']['producer_slot']
        del originals[slot + '-0000000000000001-0000000000000004.cbor']
    else:
        assert mode == 'history-normal'
    same(wanted, original)
    assert native == rows_dump(rows), 'native setup has unintended effects'
    assert inputs == originals, 'input setup has unintended effects'
    return SEED_SEQUENCE + int(mode in ('history-missing-handoff', 'history-nonempty-target'))


def claim(before, after, planned):
    """Exactly one fresh Control, preserving the finished preparing process."""
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'),
        key=lambda r: r['epoch'])
    assert previous['state'] == 'released'
    current = only(after['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
    role, incarnation = '\\x' + planned['role'], '\\x' + planned['incarnation']
    for value in (planned['role'], planned['incarnation']):
        assert len(value) == 32 and bytes.fromhex(value).hex() == value and int(value, 16) != 0
    assert not any(r['role_instance_id'] == role for r in before['role_instances'])
    assert not any(r['process_incarnation'] == incarnation for r in before['owner_epochs'])
    actual_role = only(after['role_instances'], role_instance_id=role)
    assert isinstance(current['control_backend_pid'], int) and current['control_backend_pid'] > 0
    assert isinstance(current['control_backend_start'], str) and current['control_backend_start']
    assert previous['released_at_us'] <= actual_role['created_at_us'] <= current['acquired_at_us']
    tx = current['xmin']
    assert tx.isdecimal() and all(tx != r['xmin'] for table in before.values() for r in table)
    wanted = copy.deepcopy(before)
    wanted['owner_epochs'].append(dict(previous, epoch=previous['epoch'] + 1,
        role_instance_id=role, process_incarnation=incarnation, state='active',
        acquired_at_us=current['acquired_at_us'], lease_expires_at_us=current['acquired_at_us'] + 20_000_000,
        released_at_us=None, exclusion_method=None, exclusion_digest=None,
        archive_frontier_protocol=None, control_fence_version=1,
        control_backend_pid=current['control_backend_pid'],
        control_backend_start=current['control_backend_start'], xmin=tx))
    wanted['role_instances'].append(dict(role_instance_id=role,
        dataset_id=previous['dataset_id'], role='control', created_at_us=actual_role['created_at_us'],
        retired_at_us=None, xmin=tx))
    same(wanted, after)
    return current


def refresh(before, after, control):
    current = only(after['owner_epochs'], resource_id=control['resource_id'], epoch=control['epoch'])
    assert current['lease_expires_at_us'] >= control['lease_expires_at_us']
    assert current['xmin'].isdecimal() and all(current['xmin'] != r['xmin']
        for table in before.values() for r in table)
    wanted = copy.deepcopy(before)
    wanted['owner_epochs'][wanted['owner_epochs'].index(control)] = dict(control,
        lease_expires_at_us=current['lease_expires_at_us'], xmin=current['xmin'])
    same(wanted, after)
    return current


def verify(events, original, native, inputs, mode, *, seed):
    """Classify every phase, including explicit renewals; return evidence only."""
    assert mode == 'history-normal' or mode in FAILURES
    known = seed_expected(seed)
    native_sequence = preparation(original, native, inputs, mode, seed, known)
    position = snapshots = 0
    current = original
    resolved = []

    def take(event, stage=None, phase=None):
        nonlocal position
        assert position < len(events), (event, stage, 'missing event')
        value = events[position]
        position += 1
        assert (value['event'], value.get('stage'), value.get('phase')) == (event, stage, phase), value
        return value

    def snapshot(stage):
        nonlocal snapshots
        event = take('successor-state', stage)
        assert set(event) == {'event', 'stage', 'state', 'rows'}
        state = event['state']
        assert set(state) == {'native_rows', 'native_sha256', 'sequence',
            'control_sha256', 'control_tables', 'originals'}
        assert event['rows'] == native['rows'] and native_rows(event['rows']) == native_rows(native['rows']), stage
        assert state['native_rows'] == len(native['rows'])
        assert state['native_sha256'] == digest(encoded(event['rows']))
        assert type(state['sequence']) is int and state['sequence'] == native_sequence, stage
        tables = state['control_tables']
        assert state['control_sha256'] == digest(encoded(tables))
        pg = {name: json.loads(rows) for name, rows in tables}
        assert len(pg) == len(tables) == 71 and list(pg) == sorted(original)
        assert len(state['originals']) == len(inputs) and dict(state['originals']) == inputs
        assert [name for name, _ in state['originals']] == sorted(inputs)
        snapshots += 1
        return pg

    planned = take('successor-control', 'history', 'claim-planned')
    assert set(planned) == {'event', 'stage', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['lease_ttl_us'] == '20000000'
    same(current, snapshot('history-before-claim'))
    claimed = take('successor-control', 'history', 'claimed')
    after = snapshot('history-after-claim')
    control = claim(current, after, planned)
    assert claimed == dict(event='successor-control', stage='history', phase='claimed',
        resource=control['resource_id'][2:], epoch=control['epoch'],
        role=control['role_instance_id'][2:], incarnation=control['process_incarnation'][2:])
    current = after
    same(current, snapshot('history-before-read'))
    normal = mode == 'history-normal'
    if normal or mode == 'history-missing-input':
        assert take('successor-history-registration') == known['registration']
        same(current, snapshot('history-loaded'))
        for sequence in range(1, 5):
            after = snapshot(f'history-refreshed-{sequence}')
            control = refresh(current, after, control)
            current = after
            if mode == 'history-missing-input' and sequence == 4:
                break
            assert take('successor-history-request') == dict(event='successor-history-request',
                sequence=sequence, outcome='durable', receipt=known['receipts'][sequence - 1], rejection=None)
            resolved.append(sequence)
            same(current, snapshot(f'history-resolved-{sequence}'))
        if normal:
            assert take('successor-history-proof') == known['proof']
            same(current, snapshot('history-proof'))
    stage, sequence, error = ('complete', None, None) if normal else FAILURES[mode]
    assert take('successor-history-result', stage) == dict(event='successor-history-result',
        stage=stage, sequence=sequence, error=error)
    same(current, snapshot('history-after-read'))
    same(current, snapshot('history-before-control-end'))
    phase = 'released-and-joined' if normal else 'dropped-and-joined'
    assert take('successor-control', 'history', phase) == dict(event='successor-control',
        stage='history', phase=phase, resource=control['resource_id'][2:], epoch=control['epoch'])
    after = snapshot('history-after-control-end')
    ended(current, after, control, normal)
    same(after, snapshot('history-final'))
    assert take('successor-history-complete') == dict(event='successor-history-complete',
        native='closed', control=phase, admission=False, success=normal)
    assert position == len(events), 'unclassified trailing event'
    return dict(status='PASS', mode=mode, snapshots=snapshots, control_tables=71,
        native_rows=len(native['rows']), native_sequence=native_sequence, read_sequence_delta=0,
        original_inputs=len(inputs), resolved_requests=resolved,
        original_batches=[list(known['batches'][n - 1]) for n in resolved],
        control_epoch=control['epoch'], registrar_epoch=known['registration']['control_epoch'],
        prepared_by_epoch=only(seed['control']['initial_ingest_successions'])['prepared_by_epoch'],
        control_refreshes=4 if resolved else 0, historical_rows_and_xmin_preserved=True,
        target_incarnation_inherited=False, admission=False, read_only=True,
        result_error=error, proof_reconstructed=normal,
        process_boundary='established separately by the owning harness',
        previous_manifest_sha256=digest(bytes.fromhex(known['proof']['previous_manifest'])),
        target_manifest_sha256=digest(bytes.fromhex(known['proof']['target_manifest'])),
        handoff_sha256=digest(bytes.fromhex(known['proof']['handoff'])))


def negative_controls(events, original, native, inputs, mode, *, seed):
    """Alter captures and refresh hashes, so semantic checks must reject them."""
    verify(events, original, native, inputs, mode, seed=seed)
    rejected = []

    def check(name, mutate):
        changed = copy.deepcopy(events)
        mutate(changed)
        try:
            verify(changed, original, native, inputs, mode, seed=seed)
        except (AssertionError, KeyError, ValueError, TypeError):
            rejected.append(name)
        else:
            raise AssertionError('oracle accepted perturbation: ' + name)

    def final(changed):
        return only(changed, event='successor-state', stage='history-final')

    def sql(changed, mutate, stage='history-final'):
        event = only(changed, event='successor-state', stage=stage)
        state = event['state']
        pg = {name: json.loads(rows) for name, rows in state['control_tables']}
        mutate(pg)
        state['control_tables'] = [[name, encoded(pg[name]).decode()] for name in sorted(pg)]
        state['control_sha256'] = digest(encoded(state['control_tables']))

    def native_bit(changed):
        event = final(changed)
        row = only(event['rows'], cf='default', key='0101')
        value = bytearray.fromhex(row['value'])
        value[-1] ^= 1
        row['value'] = value.hex()
        event['state']['native_sha256'] = digest(encoded(event['rows']))

    check('native-byte-with-refreshed-hash', native_bit)
    check('historical-row-xmin', lambda e: sql(e,
        lambda pg: only(pg['producer_epochs'], producer_epoch=1).update(xmin='999999999')))
    check('historical-registrar-rewritten', lambda e: sql(e,
        lambda pg: pg['producer_slots'][0].update(registered_by_epoch=10)))
    check('successor-reservation-created', lambda e: sql(e,
        lambda pg: only(pg['producer_epochs'], producer_epoch=2).update(reserved_through_request=1)))
    check('original-input-digest', lambda e: final(e)['state']['originals'][0][1].update(sha256='00' * 32))
    check('native-sequence', lambda e: final(e)['state'].update(sequence=final(e)['state']['sequence'] + 1))
    check('missing-snapshot', lambda e: e.pop(next(i for i, value in enumerate(e)
        if value['event'] == 'successor-state')))
    check('terminal-admission-claim', lambda e: only(e, event='successor-history-complete').update(admission=True))
    check('wrong-current-control', lambda e: only(e, event='successor-control', phase='claimed').update(epoch=3))
    if mode != 'history-missing-intent':
        check('intent-attribution-rewritten', lambda e: sql(e,
            lambda pg: pg['initial_ingest_successions'][0].update(prepared_by_epoch=10)))
    if mode in ('history-normal', 'history-missing-input'):
        check('historical-registration-impersonation', lambda e:
            only(e, event='successor-history-registration').update(control_epoch=10))
        check('original-outcome-bytes', lambda e:
            only(e, event='successor-history-request', sequence=1).update(receipt='00'))
        check('missing-request-observation', lambda e: e.pop(next(i for i, value in enumerate(e)
            if value['event'] == 'successor-history-request')))
        check('read-rewrites-historical-copy', lambda e: sql(e,
            lambda pg: pg['producer_outcomes'][0].update(xmin='999999999'), 'history-resolved-1'))
        check('refresh-changes-historical-source', lambda e: sql(e,
            lambda pg: only(pg['source_streams'], owner_epoch=1).update(last_complete_sequence=13),
            'history-refreshed-1'))
    if mode == 'history-normal':
        check('wrong-positive-source-bound', lambda e:
            only(e, event='successor-history-proof').update(last_complete_sequence=11))
        check('wrong-positive-closure', lambda e:
            only(e, event='successor-history-proof').update(closure_digest='00' * 32))
    else:
        check('unrelated-error-accepted-as-refusal', lambda e:
            only(e, event='successor-history-result').update(error='DeliveryUnknown'))
    if mode == 'history-missing-outcome':
        check('missing-copy-synthesized', lambda e: sql(e,
            lambda pg: pg['producer_outcomes'].append(copy.deepcopy(
                only(seed['control']['producer_outcomes'], request_sequence=3)))))
    return dict(status='PASS', rejected=rejected)
