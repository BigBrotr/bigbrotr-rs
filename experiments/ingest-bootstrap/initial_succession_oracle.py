"""Independent finite oracle for the retained signed initial succession probe.

Pure observations only: no SQL, native APIs, product codecs, or process control.
The caller supplies the independently verified retired seed and full transcript.
Prepared metadata is never interpreted as admission or replacement-process use.
"""
import copy
import hashlib
import json


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def be(value, width=8):
    return value.to_bytes(width, 'big')


def binary(value):
    assert value.startswith('\\x')
    return bytes.fromhex(value[2:])


def bytea(value):
    return '\\x' + value.hex()


def only(rows, **fields):
    row, = [r for r in rows if all(r[k] == v for k, v in fields.items())]
    return row


def same(before, after):
    assert before.keys() == after.keys(), 'table census differs'
    for name in before:
        assert sorted(before[name], key=encoded) == sorted(after[name], key=encoded), name


def exclusion(owner, successor):
    return bytea(hashlib.sha256(b'bigbrotr/control-exclusion/1\0'
        + binary(owner['dataset_id']) + binary(owner['resource_id']) + be(owner['epoch'])
        + binary(owner['process_incarnation']) + binary(successor)).digest())


def native_rows(rows):
    assert all(set(r) == {'cf', 'key', 'value'} for r in rows)
    result = {(r['cf'], bytes.fromhex(r['key'])): bytes.fromhex(r['value']) for r in rows}
    assert len(result) == len(rows), 'duplicate native key'
    assert list(result) == sorted(result), 'native census order differs'
    return result


def expected(original, native, inputs, timestamp):
    """Derive literal target bytes and fields from the frozen, already signed seed."""
    assert native['event'] == 'dump' and len(native['rows']) == 49
    old_rows = native_rows(native['rows'])
    old = old_rows['default', b'\x01\x01']
    assert len(old) == 266 and old[:10] == bytes.fromhex('01010001000100010001')
    dataset = only(original['datasets'])
    producer = only(original['producer_epochs'])
    owner = only(original['owner_epochs'], role='ingest')
    source = only(original['source_streams'])
    slot = binary(producer['producer_slot_id'])
    ds, resource = binary(dataset['dataset_id']), binary(owner['resource_id'])
    assert old[10:26] == ds and old[26:34] == be(0) and old[34:50] == resource
    assert old[50:62] == be(0, 4) + be(1)
    assert old[62:78] == binary(owner['process_incarnation']) and old[94:104] == be(1) + b'\x01\x09'
    assert dataset['config_revision'] == dataset['policy_revision'] == 1
    assert dataset['topology_generation'] == 0
    assert producer['producer_epoch'] == producer['owner_epoch'] == owner['epoch'] == 1
    assert producer['state'] == 'retired' and producer['reserved_through_request'] == 4
    assert producer['outstanding_request_limit'] == 32
    assert owner['state'] == 'released' and owner['exclusion_method'] == 'storage_fenced'
    assert source['state'] == 'closed' and source['last_complete_sequence'] == 12
    assert len(original['allocation_domains']) == 9
    assert len(original['producer_requests']) == len(original['producer_outcomes']) == 4
    assert len(inputs) == 4 and not original['producer_input_bindings']
    assert old_rows['default', b'\x01\x02' + ds + be(0, 4) + be(1)] == b'\x01\x02' + be(12)
    assert old_rows['receipts', b'\x01\x31' + slot + be(1)] == b'\x02\x31' + be(4) + be(0) + be(0) + b'\x01'
    closure = hashlib.sha256(b'bigbrotr/initial-source-closure/1\0' + hashlib.sha256(old).digest()
        + slot + be(1) + be(4))
    for sequence in range(1, 5):
        filename = slot.hex() + '-0000000000000001-' + f'{sequence:016x}.cbor'
        item = inputs[filename]
        assert set(item) == {'bytes', 'sha256'} and item['bytes'] > 0
        request = only(original['producer_requests'], request_sequence=sequence)
        assert request['request_digest'] == '\\x' + item['sha256']
        assert request['input_bytes'] == item['bytes']
        receipt = old_rows['receipts', b'\x01\x30' + slot + be(1) + be(sequence)]
        closure.update(be(sequence) + bytes.fromhex(item['sha256']) + hashlib.sha256(receipt).digest())
    closure.update(be(12))
    closure = closure.digest()
    assert bytea(closure) == producer['closure_digest'] == source['closure_digest'] == owner['exclusion_digest']
    assert isinstance(timestamp, int) and timestamp >= producer['retired_at_us']
    next_manifest = bytearray(old)
    next_manifest[2:4] = be(2, 2)
    next_manifest[26:34] = be(1)
    next_manifest[54:62] = be(2)
    next_manifest[62:78] = bytes([202]) * 16
    domains = []
    for kind in range(1, 10):
        offset = 104 + (kind - 1) * 18
        previous = only(original['allocation_domains'], dictionary_kind=kind)
        assert old[offset:offset + 18] == be(kind, 2) + binary(previous['allocation_domain'])
        assert previous['state'] == 'closed' and previous['closure_digest'] == bytea(closure)
        domain = bytes([160 + kind]) * 16
        next_manifest[offset + 2:offset + 18] = domain
        domains.append([kind, domain.hex()])
    target = bytes(next_manifest)
    handoff = (b'\x01\x0a' + slot + be(1) + be(4) + be(12) + closure
        + be(len(old), 2) + old + be(len(target), 2) + target)
    assert len(handoff) == 610
    new_rows = dict(old_rows)
    new_rows['default', b'\x01\x01'] = target
    new_rows['default', b'\x01\x0a' + ds + resource + be(1)] = handoff
    new_rows['default', b'\x01\x02' + ds + be(0, 4) + be(2)] = b'\x01\x02' + be(0)
    new_rows['receipts', b'\x01\x31' + slot + be(2)] = b'\x01\x31' + be(0) * 3
    assert len(new_rows) == 52
    config = only(original['config_revisions'], revision=1)
    policy = only(original['policy_revisions'], revision=1)
    plan = dict(event='successor-plan', dataset=ds.hex(), instance=dataset['instance_id'][2:],
        resource=resource.hex(), producer_slot=slot.hex(), previous_epoch=1, target_epoch=2,
        shard=0, previous_topology=0, target_topology=1, target_incarnation='ca' * 16,
        recovery_generation=old[78:94].hex(), target_role='c9' * 16, created_at_us=timestamp,
        lease_expires_at_us=timestamp + 60_000_000, window=32, policy_revision=1, config_revision=1,
        policy_digest=policy['document_digest'][2:], config_digest=config['document_digest'][2:],
        domains=domains, issued_through=4, last_complete_sequence=12, closure_digest=closure.hex(),
        previous_manifest=old.hex(), target_manifest=target.hex(), handoff=handoff.hex())
    return plan, old_rows, new_rows


def claim(before, after, ordinal):
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    current = only(after['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
    role = only(after['role_instances'], role_instance_id='\\x' + f'{210 + ordinal:02x}' * 16)
    assert current['role_instance_id'] == role['role_instance_id']
    assert current['process_incarnation'] == '\\x' + f'{220 + ordinal:02x}' * 16
    assert not any(r['role_instance_id'] == current['role_instance_id'] for r in before['role_instances'])
    assert not any(r['process_incarnation'] == current['process_incarnation'] for r in before['owner_epochs'])
    assert isinstance(current['control_backend_pid'], int) and current['control_backend_pid'] > 0
    assert isinstance(current['control_backend_start'], str) and current['control_backend_start']
    assert role['created_at_us'] <= current['acquired_at_us']
    assert current['lease_expires_at_us'] == current['acquired_at_us'] + 20_000_000
    tx = current['xmin']
    assert tx.isdecimal() and all(tx != r['xmin'] for rows in before.values() for r in rows)
    wanted = copy.deepcopy(before)
    if previous['state'] != 'released':
        assert previous['state'] == 'active' and previous['released_at_us'] is None
        released = only(after['owner_epochs'], resource_id=previous['resource_id'], epoch=previous['epoch'])
        old_role = only(before['role_instances'], role_instance_id=previous['role_instance_id'])
        retired = only(after['role_instances'], role_instance_id=previous['role_instance_id'])
        assert old_role['retired_at_us'] is None
        assert previous['acquired_at_us'] <= released['released_at_us'] <= retired['retired_at_us'] <= role['created_at_us']
        wanted['owner_epochs'][wanted['owner_epochs'].index(previous)] = dict(previous,
            state='released', released_at_us=released['released_at_us'], exclusion_method='storage_fenced',
            exclusion_digest=exclusion(previous, current['process_incarnation']), xmin=tx)
        wanted['role_instances'][wanted['role_instances'].index(old_role)] = dict(old_role,
            retired_at_us=retired['retired_at_us'], xmin=tx)
    expected_owner = dict(previous, epoch=previous['epoch'] + 1,
        role_instance_id=role['role_instance_id'], process_incarnation=current['process_incarnation'],
        acquired_at_us=current['acquired_at_us'], lease_expires_at_us=current['acquired_at_us'] + 20_000_000,
        state='active', released_at_us=None, exclusion_method=None, exclusion_digest=None,
        archive_frontier_protocol=None, control_fence_version=1,
        control_backend_pid=current['control_backend_pid'], control_backend_start=current['control_backend_start'], xmin=tx)
    wanted['owner_epochs'].append(expected_owner)
    wanted['role_instances'].append(dict(role_instance_id=current['role_instance_id'],
        dataset_id=previous['dataset_id'], role='control', created_at_us=role['created_at_us'], retired_at_us=None, xmin=tx))
    same(wanted, after)
    return current


def renewed(before, after, owner, timestamp):
    current = only(after['owner_epochs'], resource_id=owner['resource_id'], epoch=owner['epoch'])
    assert current['xmin'] != owner['xmin'] and current['xmin'].isdecimal()
    assert owner['acquired_at_us'] <= current['lease_expires_at_us'] - 20_000_000 <= timestamp
    wanted = copy.deepcopy(before)
    wanted['owner_epochs'][wanted['owner_epochs'].index(owner)] = dict(owner,
        lease_expires_at_us=current['lease_expires_at_us'], xmin=current['xmin'])
    same(wanted, after)
    return current


def ended(before, after, owner, release):
    if not release:
        same(before, after)
        assert only(after['owner_epochs'], resource_id=owner['resource_id'], epoch=owner['epoch']) == owner
        return
    current = only(after['owner_epochs'], resource_id=owner['resource_id'], epoch=owner['epoch'])
    role = only(before['role_instances'], role_instance_id=owner['role_instance_id'])
    retired = only(after['role_instances'], role_instance_id=owner['role_instance_id'])
    tx = current['xmin']
    assert tx.isdecimal() and tx != owner['xmin'] and tx != role['xmin']
    assert owner['acquired_at_us'] <= current['released_at_us'] <= retired['retired_at_us']
    wanted = copy.deepcopy(before)
    wanted['owner_epochs'][wanted['owner_epochs'].index(owner)] = dict(owner,
        state='released', released_at_us=current['released_at_us'], exclusion_method='storage_fenced',
        exclusion_digest=exclusion(owner, owner['process_incarnation']), xmin=tx)
    wanted['role_instances'][wanted['role_instances'].index(role)] = dict(role,
        retired_at_us=retired['retired_at_us'], xmin=tx)
    same(wanted, after)


def prepared(before, after, plan, control):
    assert len(before) == 71 and before['initial_ingest_successions'] == []
    intent = only(after['initial_ingest_successions'])
    tx = intent['xmin']
    assert tx.isdecimal() and all(tx != r['xmin'] for rows in before.values() for r in rows)
    ds = '\\x' + plan['dataset']
    resource = '\\x' + plan['resource']
    role, inc = '\\x' + plan['target_role'], '\\x' + plan['target_incarnation']
    timestamp = plan['created_at_us']
    wanted = copy.deepcopy(before)
    old_owner = only(before['owner_epochs'], role='ingest')
    old_role = only(before['role_instances'], role_instance_id=old_owner['role_instance_id'])
    wanted['owner_epochs'].append(dict(old_owner, epoch=2, role_instance_id=role, process_incarnation=inc,
        acquired_at_us=timestamp, lease_expires_at_us=plan['lease_expires_at_us'], state='active',
        released_at_us=None, exclusion_method=None, exclusion_digest=None, xmin=tx))
    wanted['role_instances'].append(dict(old_role, role_instance_id=role, created_at_us=timestamp, retired_at_us=None, xmin=tx))
    old = only(before['producer_epochs'])
    wanted['producer_epochs'].append(dict(old, producer_epoch=2, producer_role_instance_id=role,
        producer_incarnation=inc, owner_epoch=2, owner_incarnation=inc, state='open', reserved_through_request=0,
        outstanding_request_limit=32, opened_at_us=timestamp, retired_at_us=None, closure_digest=None, xmin=tx))
    old = only(before['source_streams'])
    wanted['source_streams'].append(dict(old, owner_epoch=2, owner_incarnation=inc, state='open',
        opened_at_us=timestamp, closed_at_us=None, last_complete_sequence=None, closure_digest=None, xmin=tx))
    for kind, domain in plan['domains']:
        old = only(before['allocation_domains'], dictionary_kind=kind)
        wanted['allocation_domains'].append(dict(old, allocation_domain='\\x' + domain, owner_epoch=2,
            owner_incarnation=inc, state='active', created_at_us=timestamp, closed_at_us=None, closure_digest=None, xmin=tx))
    old = only(before['topology_generations'])
    wanted['topology_generations'].append(dict(old, generation=1, previous_generation=0, created_at_us=timestamp, sealed=True, xmin=tx))
    old = only(before['topology_streams'])
    wanted['topology_streams'] += [dict(old, topology_generation=1, owner_epoch=epoch,
        handoff_last_complete_sequence=12 if epoch == 1 else None, xmin=tx) for epoch in (1, 2)]
    wanted['datasets'] = [dict(only(before['datasets']), topology_generation=1, xmin=tx)]
    wanted['initial_ingest_successions'] = [dict(dataset_id=ds, owner_resource_id=resource,
        target_epoch=2, owner_role='ingest', target_incarnation=inc, target_role_instance_id=role,
        producer_slot_id='\\x' + plan['producer_slot'], producer_epoch=2, shard_id=0,
        previous_topology=0, target_topology=1, issued_through=4, last_complete_sequence=12,
        closure_digest='\\x' + plan['closure_digest'], previous_manifest='\\x' + plan['previous_manifest'],
        target_manifest='\\x' + plan['target_manifest'], handoff='\\x' + plan['handoff'],
        previous_manifest_sha256='\\x' + digest(bytes.fromhex(plan['previous_manifest'])),
        target_manifest_sha256='\\x' + digest(bytes.fromhex(plan['target_manifest'])),
        handoff_sha256='\\x' + digest(bytes.fromhex(plan['handoff'])), config_revision=1, policy_revision=1,
        config_digest='\\x' + plan['config_digest'], policy_digest='\\x' + plan['policy_digest'],
        outstanding_request_limit=32, created_at_us=timestamp, lease_expires_at_us=plan['lease_expires_at_us'],
        prepared_by_resource=control['resource_id'], prepared_by_epoch=control['epoch'],
        prepared_by_role='control', prepared_by_incarnation=control['process_incarnation'], xmin=tx)]
    same(wanted, after)


def verify(events, original, native, inputs, mode):
    """Verify every ordered snapshot and return finite evidence, never authority."""
    commit_fault = mode in ('successor-commit-before', 'successor-commit-after')
    assert commit_fault or mode in ('successor-normal', 'successor-gap', 'successor-gap-only', 'successor-old-schema', 'successor-rollback')
    plans = [e for e in events if e['event'] == 'successor-plan']
    observed_plan, = plans
    plan, old_rows, new_rows = expected(original, native, inputs, observed_plan['created_at_us'])
    assert observed_plan == plan, 'product plan differs from independent bytes/fields'
    assert len(original) == (70 if mode == 'successor-old-schema' else 71)
    position = 0
    snapshots = 0
    initial_sequence = None
    completed_native = False
    current_pg = original
    control = None

    def take(event, stage=None, phase=None):
        nonlocal position
        assert position < len(events), (event, stage, 'missing event')
        value = events[position]
        position += 1
        assert (value['event'], value.get('stage'), value.get('phase')) == (event, stage, phase), value
        return value

    def snapshot(stage):
        nonlocal snapshots, initial_sequence
        event = take('successor-state', stage)
        assert set(event) == {'event', 'stage', 'state', 'rows'}
        state = event['state']
        assert set(state) == {'native_rows', 'native_sha256', 'sequence', 'control_sha256', 'control_tables', 'originals'}
        assert native_rows(event['rows']) == (new_rows if completed_native else old_rows), stage
        assert state['native_rows'] == len(event['rows'])
        assert state['native_sha256'] == digest(encoded(event['rows']))
        assert isinstance(state['sequence'], int) and state['sequence'] >= 0
        if initial_sequence is None:
            initial_sequence = state['sequence']
        assert state['sequence'] == initial_sequence + (4 if completed_native else 0), stage
        assert state['control_sha256'] == digest(encoded(state['control_tables']))
        pg = {name: json.loads(rows) for name, rows in state['control_tables']}
        assert len(pg) == len(state['control_tables']) == len(original)
        assert list(pg) == sorted(original)
        assert len(state['originals']) == len(inputs) and dict(state['originals']) == inputs
        assert [name for name, _ in state['originals']] == sorted(inputs)
        snapshots += 1
        return pg

    def acquire(stage, ordinal):
        nonlocal current_pg, control
        event = take('successor-control', stage, 'claim-planned')
        assert event == dict(event='successor-control', stage=stage, phase='claim-planned',
            role=f'{210 + ordinal:02x}' * 16, incarnation=f'{220 + ordinal:02x}' * 16, lease_ttl_us='20000000')
        same(current_pg, snapshot(stage + '-before-claim'))
        event = take('successor-control', stage, 'claimed')
        after = snapshot(stage + '-after-claim')
        control = claim(current_pg, after, ordinal)
        assert event == dict(event='successor-control', stage=stage, phase='claimed',
            resource=control['resource_id'][2:], epoch=control['epoch'],
            role=control['role_instance_id'][2:], incarnation=control['process_incarnation'][2:])
        current_pg = after

    def end(stage, release, *, failed_driver=False):
        nonlocal current_pg
        same(current_pg, snapshot(stage + '-before-control-end'))
        if failed_driver:
            assert not release
            event = take('successor-driver', stage)
            assert event['failed'] is True and event['joined'] is True
            assert event == dict(event='successor-driver',
                stage=stage, failed=True, joined=True)
        phase = 'released-and-joined' if release else 'dropped-and-joined'
        assert take('successor-control', stage, phase) == dict(event='successor-control', stage=stage,
            phase=phase, resource=control['resource_id'][2:], epoch=control['epoch'])
        after = snapshot(stage + '-after-control-end')
        ended(current_pg, after, control, release)
        current_pg = after

    def backend(present, previous=None):
        event = take('successor-backend', 'initial')
        database = event['database']
        # The runner separately binds this name to its actual cloned database.
        # SQL table rows do not contain datname, so it cannot be derived here.
        assert isinstance(database, str) and 1 <= len(database) <= 63
        assert database.isascii() and database.replace('_', '').isalnum()
        assert event['present'] is present
        wanted = dict(event='successor-backend', stage='initial', database=database,
            pid=control['control_backend_pid'], backend_start=control['control_backend_start'],
            control_epoch=control['epoch'], present=present)
        assert event == wanted
        if previous is not None:
            assert event == dict(previous, present=False), 'backend absence used another tuple'
        return event

    def attempt(stage, *, new_intent=False, native_commit=False, error=None, validations=4, conflict=False):
        nonlocal current_pg, completed_native
        assert take('successor-attempt', stage) == dict(event='successor-attempt', stage=stage,
            role=('cb' * 16 if conflict else plan['target_role']), control_epoch=control['epoch'])
        assert take('successor-result', stage) == dict(event='successor-result', stage=stage,
            control_epoch=control['epoch'], validations=validations, error=error)
        if error is not None:
            assert take('successor-session-poisoned') == dict(event='successor-session-poisoned', control_epoch=control['epoch'])
        if native_commit:
            assert not completed_native
            completed_native = True
        after = snapshot(stage)
        if new_intent:
            prepared(current_pg, after, plan, control)
        else:
            same(current_pg, after)
        current_pg = after

    acquire('initial', 1)
    assert take('successor-plan') == plan
    after = snapshot('before-prepare')
    control = renewed(current_pg, after, control, plan['created_at_us'])
    current_pg = after
    if commit_fault:
        previous_backend = backend(True)
        committed = mode == 'successor-commit-after'
        # Client uncertainty never permits native dispatch, including when the
        # independent observer already sees the complete SQL preparation.
        attempt('commit-unknown', new_intent=committed, error='CommitUnknown', validations=2)
        end('unknown', False, failed_driver=True)
        excluded_backend = backend(False, previous_backend)
        # Only a new actual claim may proceed; the retained local plan and its
        # root/sequence proof stay in this same process through both branches.
        acquire('completion', 2)
        attempt('completed', new_intent=not committed, native_commit=True)
    elif mode == 'successor-normal':
        attempt('prepared', new_intent=True, native_commit=True)
        attempt('same-control-retry')
        end('first', True)
        acquire('fresh', 2)
        attempt('fresh-control-retry')
    elif mode == 'successor-gap-only':
        attempt('prepared-native-old', new_intent=True, error='Control(InvalidPolicy)', validations=3)
    elif mode == 'successor-gap':
        attempt('prepared-native-old', new_intent=True, error='Control(InvalidPolicy)', validations=3)
        end('gap', False)
        acquire('conflict', 2)
        attempt('conflict-refused', error='Control(InvalidMetadata)', validations=2, conflict=True)
        end('conflict', False)
        acquire('completion', 3)
        attempt('completed', native_commit=True)
    else:
        old_schema = mode == 'successor-old-schema'
        attempt('old-schema-refused' if old_schema else 'rollback-refused',
            error='Control(IncompatibleDeployment)' if old_schema else 'Control(Backend)',
            validations=0 if old_schema else 2)
    release = completed_native
    end('final', release)
    same(current_pg, snapshot('final'))
    assert take('successor-complete') == dict(event='successor-complete', mode=mode, native='closed',
        control='released-and-joined' if release else 'dropped-and-joined', admission=False)
    assert position == len(events), 'unclassified trailing events'
    evidence = dict(status='PASS', mode=mode, snapshots=snapshots, control_tables=len(original),
        original_native_rows=49, final_native_rows=52 if completed_native else 49,
        native_sequence_delta=4 if completed_native else 0, original_inputs=len(inputs),
        previous_manifest_sha256=digest(bytes.fromhex(plan['previous_manifest'])),
        target_manifest_sha256=digest(bytes.fromhex(plan['target_manifest'])),
        handoff_sha256=digest(bytes.fromhex(plan['handoff'])),
        prepared_only=True, admission=False, retained_same_process_plan=True)
    if commit_fault:
        evidence.update(commit_unknown=True, native_rows_after_unknown=49,
            preparation_after_unknown='committed' if committed else 'absent',
            excluded_backend=excluded_backend,
            exclusion_observed_before_fresh_claim=True,
            intent_prepared_by_epoch=only(current_pg['initial_ingest_successions'])['prepared_by_epoch'])
    return evidence


def negative_controls(events, original, native, inputs, mode):
    """Perturb real captures; refresh their hashes so semantic comparisons reject."""
    verify(events, original, native, inputs, mode)
    rejected = []

    def check(name, mutate):
        changed = copy.deepcopy(events)
        mutate(changed)
        try:
            verify(changed, original, native, inputs, mode)
        except (AssertionError, KeyError, ValueError, TypeError):
            rejected.append(name)
        else:
            raise AssertionError('oracle accepted perturbation: ' + name)

    def final(changed):
        return only(changed, event='successor-state', stage='final')

    def sql(changed, mutate, *, all_prepared=False):
        selected = ([e for e in changed if e['event'] == 'successor-state']
            if all_prepared else [final(changed)])
        for event in selected:
            state = event['state']
            pg = {name: json.loads(rows) for name, rows in state['control_tables']}
            if all_prepared and not pg.get('initial_ingest_successions'):
                continue
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

    check('native-byte', native_bit)
    check('historical-slot-registrar', lambda e: sql(e, lambda pg: pg['producer_slots'][0].update(registered_by_epoch=999)))
    check('historical-row-xmin', lambda e: sql(e, lambda pg: pg['source_streams'][0].update(xmin='999999999')))
    check('original-input-digest', lambda e: final(e)['state']['originals'][0][1].update(sha256='00' * 32))
    check('native-sequence', lambda e: final(e)['state'].update(sequence=final(e)['state']['sequence'] + 1))
    check('missing-snapshot', lambda e: e.pop(next(i for i, v in enumerate(e) if v['event'] == 'successor-state')))
    check('product-plan-encoding', lambda e: only(e, event='successor-plan').update(target_manifest='00' * 266))
    if mode in ('successor-normal', 'successor-gap', 'successor-gap-only', 'successor-commit-before', 'successor-commit-after'):
        check('intent-attribution', lambda e: sql(e, lambda pg: pg['initial_ingest_successions'][0].update(prepared_by_epoch=3), all_prepared=True))
        check('intent-xmin-rewrite', lambda e: sql(e, lambda pg: pg['initial_ingest_successions'][0].update(xmin='999999999')))
        check('old-topology-rewrite', lambda e: sql(e, lambda pg: only(pg['topology_streams'], topology_generation=0).update(handoff_last_complete_sequence=12), all_prepared=True))
        check('missing-successor-producer', lambda e: sql(e, lambda pg: pg['producer_epochs'].remove(only(pg['producer_epochs'], producer_epoch=2)), all_prepared=True))
        check('fabricated-successor-bound', lambda e: sql(e, lambda pg: only(pg['source_streams'], owner_epoch=2).update(last_complete_sequence=0), all_prepared=True))
    if mode in ('successor-commit-before', 'successor-commit-after'):
        def unknown_native(changed):
            observation = only(changed, event='successor-state', stage='commit-unknown')
            completed = final(changed)
            observation['rows'] = copy.deepcopy(completed['rows'])
            for field in ('native_rows', 'native_sha256', 'sequence'):
                observation['state'][field] = completed['state'][field]

        def wrong_preparer(changed):
            # Exchange the two actual valid Control tuples, rather than merely
            # corrupting an identifier. Only one owns immutable preparation.
            stage = 'initial-after-claim' if mode == 'successor-commit-before' else 'completion-after-claim'
            event = only(changed, event='successor-state', stage=stage)
            pg = {name: json.loads(rows) for name, rows in event['state']['control_tables']}
            owner = max((r for r in pg['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
            # At completion both owner tuples exist. Preserve the earlier
            # uncertainty observations so this specifically tests retry effects.
            offset = next(i for i, e in enumerate(changed)
                if e['event'] == 'successor-state' and e['stage'] == 'completed')
            sql(changed[offset:], lambda tables: tables['initial_ingest_successions'][0].update(
                prepared_by_resource=owner['resource_id'], prepared_by_epoch=owner['epoch'],
                prepared_by_incarnation=owner['process_incarnation']), all_prepared=True)

        check('unknown-native-dispatched', unknown_native)
        check('wrong-valid-preparing-Control', wrong_preparer)
        check('backend-still-present', lambda e: only(e, event='successor-backend', present=False).update(present=True))
        check('backend-identity-substitution', lambda e: only(e, event='successor-backend', present=False).update(
            pid=only(e, event='successor-backend', present=False)['pid'] + 1))
        check('unjoined-failed-driver', lambda e: only(e, event='successor-driver').update(joined=False))
    return dict(status='PASS', rejected=rejected)
