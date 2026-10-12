"""Independent full-state oracle for retiring the exact empty initial attempt.

Only observations and fixed bytes: no engine, product codec, or SQL execution.
The harness separately owns processes, source preservation and the SQL fault.
A follow-up recursively verifies its predecessor before accepting that baseline.
"""
import copy
import json

from initial_history_oracle import claim, inside_window, refresh, rows_dump, seed_expected
from initial_succession_oracle import be, binary, digest, encoded, ended, native_rows, only, same


MIGRATION_SHA256 = '52819215086abb15c06b25ebb536b16955256e4707aaf0f1a89060c66fc96b8a'
SUCCESS = ('empty-normal', 'empty-terminal-retry', 'empty-fresh-completion', 'empty-inspect')
FAILURES = {
    'empty-native-first-failure': ('retirement', None, 'Control(Backend)'),
    'empty-cancelled': ('retirement', None, 'Native(AuthorityUnavailable)'),
    'empty-missing-input': ('request', 4, 'Native(Input(Missing))'),
    'empty-missing-outcome': ('empty-load', None, 'Control(Backend)'),
    'empty-nonempty-target': ('empty-load', None, 'Native(StateMismatch)'),
    'empty-missing-intent': ('empty-load', None, 'Control(InvalidMetadata)'),
    'empty-old-schema': ('empty-load', None, 'Control(IncompatibleDeployment)'),
}
FOLLOWUPS = {'empty-terminal-retry': 'empty-normal',
    'empty-fresh-completion': 'empty-native-first-failure'}


def control_digest(rows):
    return digest(encoded({name: sorted(value, key=encoded) for name, value in rows.items()}))


def final_state(events):
    event = only(events, event='successor-state', stage='empty-final')
    return {name: json.loads(rows) for name, rows in event['state']['control_tables']}


def known_empty(seed):
    assert set(seed) == {'control', 'native', 'inputs', 'sequence'} and seed['sequence'] == 62
    known = seed_expected({key: seed[key] for key in ('control', 'native', 'inputs')})
    intent = only(seed['control']['initial_ingest_successions'])
    slot = binary(intent['producer_slot_id'])
    closure = digest(b'bigbrotr/initial-empty-attempt-closure/1\0'
        + bytes.fromhex(digest(binary(intent['handoff'])))
        + bytes.fromhex(digest(binary(intent['target_manifest'])))
        + slot + be(2) + be(0) + be(0))
    marker = ('receipts', b'\x01\x31' + slot + be(2))
    opened = native_rows(seed['native']['rows'])
    assert opened[marker] == b'\x01\x31' + be(0) * 3
    closed = dict(opened)
    closed[marker] = b'\x02\x31' + be(0) * 3 + b'\x01'
    assert len(closed[marker]) == 27 and len(closed) == len(opened) == 52
    known.update(empty_digest=closure, marker=marker, opened=opened, closed=closed)
    return known


def migration(before, after, server_window):
    """Only one independently pinned migration ledger row may be added."""
    row = only(after['schema_versions'], migration=31)
    assert not any(r['migration'] == 31 for r in before['schema_versions'])
    assert row['xmin'].isdecimal() and all(row['xmin'] != r['xmin']
        for table in before.values() for r in table)
    assert type(row['applied_at_us']) is int
    assert max(r['applied_at_us'] for r in before['schema_versions']) <= row['applied_at_us'] <= server_window['before_us']
    expected = dict(name='0031_initial_empty_retirement.sql', xmin=row['xmin'],
        engine='postgres-control', checksum='\\x' + MIGRATION_SHA256, migration=31,
        applied_by='bigbrotr_migrator', reader_max=1, reader_min=1, writer_floor=1,
        applied_at_us=row['applied_at_us'])
    assert row == expected
    wanted = copy.deepcopy(before)
    wanted['schema_versions'].append(expected)
    return wanted


def preparation(original, native, inputs, mode, seed, known, server_window, predecessor):
    if mode in FOLLOWUPS:
        assert predecessor is not None and set(predecessor) == {
            'events', 'original', 'native', 'inputs', 'mode', 'seed', 'server_window'}
        assert predecessor['mode'] == FOLLOWUPS[mode] and predecessor['seed'] == seed
        checked = verify(**predecessor)
        assert checked['status'] == 'PASS'
        same(final_state(predecessor['events']), original)
        final = only(predecessor['events'], event='successor-state', stage='empty-final')
        assert native == dict(event='dump', rows=final['rows'])
        assert inputs == predecessor['inputs'] == seed['inputs']
        assert final['state']['sequence'] == 63 and native_rows(native['rows']) == known['closed']
        return 63
    assert predecessor is None
    wanted = copy.deepcopy(seed['control'])
    if mode != 'empty-old-schema':
        wanted = migration(wanted, original, server_window)
    rows = dict(known['opened'])
    originals = copy.deepcopy(seed['inputs'])
    sequence = seed['sequence']
    if mode == 'empty-missing-intent':
        wanted['initial_ingest_successions'] = []
    elif mode == 'empty-missing-outcome':
        wanted['producer_outcomes'].remove(only(wanted['producer_outcomes'], request_sequence=3))
    elif mode == 'empty-missing-input':
        del originals[known['registration']['producer_slot'] + '-0000000000000001-0000000000000004.cbor']
    elif mode == 'empty-nonempty-target':
        rows[known['target_source']] = b'\x01\x02' + be(1)
        sequence += 1
    same(wanted, original)
    assert native == rows_dump(rows), 'unintended native setup delta'
    assert inputs == originals, 'unintended input setup delta'
    return sequence


def terminal(before, after, known, server_window):
    """One transaction changes exactly thirteen target rows in five families."""
    intent = only(before['initial_ingest_successions'])
    producer = only(after['producer_epochs'], producer_epoch=2)
    time, tx = producer['retired_at_us'], producer['xmin']
    inside_window(time, server_window)
    assert time >= intent['created_at_us']
    assert tx.isdecimal() and all(tx != r['xmin'] for table in before.values() for r in table)
    closure = '\\x' + known['empty_digest']
    wanted = copy.deepcopy(before)
    row = only(wanted['producer_epochs'], producer_epoch=2)
    assert row['state'] == 'open' and row['reserved_through_request'] == 0
    row.update(state='retired', retired_at_us=time, closure_digest=closure, xmin=tx)
    row = only(wanted['source_streams'], owner_epoch=2)
    assert row['state'] == 'open' and row['last_complete_sequence'] is None
    row.update(state='closed', closed_at_us=time, last_complete_sequence=0, closure_digest=closure, xmin=tx)
    domains = [r for r in wanted['allocation_domains'] if r['owner_epoch'] == 2]
    assert len(domains) == 9
    for row in domains:
        assert row['state'] == 'active'
        row.update(state='closed', closed_at_us=time, closure_digest=closure, xmin=tx)
    row = only(wanted['owner_epochs'], role='ingest', epoch=2)
    assert row['state'] == 'active'
    row.update(state='released', released_at_us=time, exclusion_method='storage_fenced',
        exclusion_digest=closure, xmin=tx)
    row = only(wanted['role_instances'], role_instance_id=intent['target_role_instance_id'])
    assert row['retired_at_us'] is None
    row.update(retired_at_us=time, xmin=tx)
    same(wanted, after)
    return time, tx


def verify(events, original, native, inputs, mode, *, seed, server_window, predecessor=None):
    assert mode in SUCCESS or mode in FAILURES
    assert set(server_window) == {'before_us', 'after_us'}
    assert all(type(v) is int and v > 0 for v in server_window.values())
    assert 0 <= server_window['after_us'] - server_window['before_us'] <= 120_000_000
    known = known_empty(seed)
    initial_sequence = preparation(original, native, inputs, mode, seed, known, server_window, predecessor)
    position = snapshots = 0
    current, expected_native = original, native_rows(native['rows'])
    sequence = initial_sequence
    resolved = []
    success = mode in SUCCESS
    initially_closing = mode in FOLLOWUPS
    loaded = success or mode in ('empty-cancelled', 'empty-native-first-failure', 'empty-missing-input')
    retired_at = retirement_tx = None

    def take(event, stage=None, phase=None):
        nonlocal position
        assert position < len(events), (event, stage, 'missing event')
        value = events[position]
        position += 1
        assert (value['event'], value.get('stage'), value.get('phase')) == (event, stage, phase), value
        return value

    def snapshot(suffix):
        nonlocal snapshots
        event = take('successor-state', 'empty-' + suffix)
        assert set(event) == {'event', 'stage', 'state', 'rows'}
        state = event['state']
        assert set(state) == {'native_rows', 'native_sha256', 'sequence',
            'control_sha256', 'control_tables', 'originals'}
        assert native_rows(event['rows']) == expected_native, suffix
        assert state['native_rows'] == len(event['rows'])
        assert state['native_sha256'] == digest(encoded(event['rows']))
        assert type(state['sequence']) is int and state['sequence'] == sequence, suffix
        tables = state['control_tables']
        assert state['control_sha256'] == digest(encoded(tables))
        pg = {name: json.loads(rows) for name, rows in tables}
        assert len(pg) == len(tables) == 71 and list(pg) == sorted(original)
        assert len(state['originals']) == len(inputs) and dict(state['originals']) == inputs
        assert [name for name, _ in state['originals']] == sorted(inputs)
        snapshots += 1
        return pg

    def policy(value, phase, ordinal, control):
        dataset = only(original['datasets'])
        config = only(original['config_revisions'], revision=dataset['config_revision'])
        row = only(original['policy_revisions'], revision=dataset['policy_revision'])
        assert dataset['config_revision'] == dataset['policy_revision'] == 1
        assert value == dict(event='successor-policy', stage='empty', phase=phase,
            sequence=ordinal, control_epoch=control['epoch'], config_revision=1, policy_revision=1,
            config_digest=config['document_digest'][2:], policy_digest=row['document_digest'][2:])

    planned = take('successor-control', 'empty', 'claim-planned')
    assert set(planned) == {'event', 'stage', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['lease_ttl_us'] == '20000000'
    same(current, snapshot('before-claim'))
    claimed = take('successor-control', 'empty', 'claimed')
    acquired = take('successor-policy', 'empty', 'acquired')
    backend = take('successor-backend', 'empty')
    after = snapshot('after-claim')
    previous = max((r for r in current['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    control = claim(current, after, planned, server_window, exclude_previous=previous['state'] == 'active')
    assert claimed == dict(event='successor-control', stage='empty', phase='claimed',
        resource=control['resource_id'][2:], epoch=control['epoch'],
        role=control['role_instance_id'][2:], incarnation=control['process_incarnation'][2:])
    policy(acquired, 'acquired', None, control)
    database = backend.get('database')
    assert isinstance(database, str) and 1 <= len(database) <= 63
    assert database.isascii() and database.replace('_', '').isalnum()
    assert backend == dict(event='successor-backend', stage='empty', database=database,
        pid=control['control_backend_pid'], backend_start=control['control_backend_start'],
        control_epoch=control['epoch'], present=True)
    current = after
    same(current, snapshot('before-read'))
    if loaded:
        assert take('successor-empty-registration') == dict(known['registration'], event='successor-empty-registration')
        same(current, snapshot('loaded'))
        same(current, snapshot('source-started'))
        for ordinal in range(1, 5):
            policy(take('successor-policy', 'empty', 'refreshed'), 'refreshed', ordinal, control)
            after = snapshot(f'refreshed-{ordinal}')
            control = refresh(current, after, control, server_window)
            current = after
            if mode == 'empty-missing-input' and ordinal == 4:
                break
            assert take('successor-empty-request') == dict(event='successor-empty-request',
                sequence=ordinal, outcome='durable', receipt=known['receipts'][ordinal - 1], rejection=None)
            resolved.append(ordinal)
            same(current, snapshot(f'resolved-{ordinal}'))
        if mode != 'empty-missing-input':
            assert take('successor-empty-proof') == dict(known['proof'], event='successor-empty-proof',
                empty_closure_digest=known['empty_digest'], initially_closing=initially_closing)
            same(current, snapshot('proof'))
            if mode != 'empty-inspect':
                if mode == 'empty-cancelled':
                    assert take('successor-empty-cancellation') == dict(event='successor-empty-cancellation', cancelled=True)
                same(current, snapshot('before-retirement'))
                dispatch_success = mode in ('empty-normal', 'empty-fresh-completion', 'empty-terminal-retry')
                receipt = take('successor-empty-retirement') if dispatch_success else None
                if mode != 'empty-cancelled':
                    expected_native = known['closed']
                    sequence = 63
                after = snapshot('after-retirement')
                if dispatch_success:
                    if mode == 'empty-terminal-retry':
                        same(current, after)
                        row = only(current['producer_epochs'], producer_epoch=2)
                        retired_at, retirement_tx = row['retired_at_us'], row['xmin']
                    else:
                        retired_at, retirement_tx = terminal(current, after, known, server_window)
                    expected_receipt = dict(event='successor-empty-retirement', attempt='initial',
                        retired_at_us=retired_at, closure_digest=known['empty_digest'],
                        handoff=known['proof']['handoff'], admission=False)
                    assert receipt == expected_receipt
                    current = after
                    same(current, snapshot('before-retry'))
                    assert take('successor-empty-retirement') == dict(expected_receipt, attempt='retry')
                    same(current, snapshot('after-retry'))
                else:
                    same(current, after)
    stage, ordinal, error = ('complete', None, None) if success else FAILURES[mode]
    assert take('successor-empty-result', stage) == dict(event='successor-empty-result',
        stage=stage, sequence=ordinal, error=error)
    if not success:
        reusable = mode == 'empty-cancelled'
        assert take('successor-empty-control-reuse') == dict(event='successor-empty-control-reuse',
            available=reusable, error=None if reusable else 'Unavailable')
        if reusable:
            policy(take('successor-policy', 'empty', 'reuse'), 'reuse', None, control)
            after = snapshot('after-reuse')
            control = refresh(current, after, control, server_window)
            current = after
        else:
            same(current, snapshot('after-reuse'))
    same(current, snapshot('after-read'))
    same(current, snapshot('before-control-end'))
    assert take('successor-driver', 'empty') == dict(event='successor-driver', stage='empty', failed=False, joined=True)
    assert take('successor-backend', 'empty') == dict(backend, present=False)
    phase = 'released-and-joined' if success else 'dropped-and-joined'
    assert take('successor-control', 'empty', phase) == dict(event='successor-control', stage='empty',
        phase=phase, resource=control['resource_id'][2:], epoch=control['epoch'])
    after = snapshot('after-control-end')
    ended(current, after, control, success)
    if success:
        inside_window(only(after['owner_epochs'], role='control', epoch=control['epoch'])['released_at_us'], server_window)
        inside_window(only(after['role_instances'], role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
    same(after, snapshot('final'))
    command = {'empty-inspect': 'empty-inspect', 'empty-cancelled': 'empty-cancelled'}.get(mode, 'empty-retire')
    assert take('successor-empty-complete') == dict(event='successor-empty-complete', command=command,
        native='closed', control=phase, admission=False, success=success)
    assert position == len(events), 'unclassified trailing event'
    return dict(status='PASS', mode=mode, snapshots=snapshots, control_tables=71,
        resolved_requests=resolved, empty_closure_digest=known['empty_digest'],
        native_sequence_delta=sequence - initial_sequence, native_rows=len(expected_native),
        retired_at_us=retired_at, retirement_transaction=retirement_tx,
        target_rows_changed=13 if mode in ('empty-normal', 'empty-fresh-completion') else 0,
        original_rows_and_xmin_preserved=True, original_inputs=len(inputs),
        immutable_intent_preserved=mode != 'empty-missing-intent',
        terminal_retry_unchanged=mode in ('empty-normal', 'empty-terminal-retry', 'empty-fresh-completion'),
        admission=False, result_error=error, control_epoch=control['epoch'],
        historical_registrar=known['registration']['control_epoch'],
        historical_preparer=only(seed['control']['initial_ingest_successions'])['prepared_by_epoch'],
        server_clock_window=server_window,
        qualification='process separation, fixture fault and backend query execution are established by the harness',
        final=dict(control_sha256=control_digest(after), native_sha256=digest(encoded(rows_dump(expected_native))),
            inputs_sha256=digest(encoded(inputs)), sequence=sequence,
            control_epoch=control['epoch'], control_state='released' if success else 'active'))


def negative_controls(events, original, native, inputs, mode, **context):
    """Rehash changed captures: full semantics, not stale checksums, must refuse."""
    verify(events, original, native, inputs, mode, **context)
    rejected = []

    def check(name, mutate):
        changed = copy.deepcopy(events)
        mutate(changed)
        try:
            verify(changed, original, native, inputs, mode, **context)
        except (AssertionError, KeyError, ValueError, TypeError):
            rejected.append(name)
        else:
            raise AssertionError('oracle accepted perturbation: ' + name)

    def snapshot(changed, suffix='final'):
        return only(changed, event='successor-state', stage='empty-' + suffix)

    def sql(changed, mutate, suffix='final'):
        state = snapshot(changed, suffix)['state']
        pg = {name: json.loads(rows) for name, rows in state['control_tables']}
        mutate(pg)
        state['control_tables'] = [[name, encoded(pg[name]).decode()] for name in sorted(pg)]
        state['control_sha256'] = digest(encoded(state['control_tables']))

    def native_byte(changed, suffix='final', producer=False):
        event = snapshot(changed, suffix)
        if producer:
            slot = only(context['seed']['control']['producer_slots'])['producer_slot_id'][2:]
            row = only(event['rows'], cf='receipts', key='0131' + slot + be(2).hex())
        else:
            row = only(event['rows'], cf='default', key='0101')
        value = bytearray.fromhex(row['value'])
        value[-1] ^= 1
        row['value'] = value.hex()
        event['state']['native_sha256'] = digest(encoded(event['rows']))

    check('native-byte-refreshed-hash', native_byte)
    check('producer-marker-refreshed-hash', lambda e: native_byte(e, producer=True))
    check('native-extra-write', lambda e: snapshot(e)['state'].update(sequence=snapshot(e)['state']['sequence'] + 1))
    check('original-outcome-xmin', lambda e: sql(e, lambda pg: pg['producer_outcomes'][0].update(xmin='999999999')))
    check('original-source-cut', lambda e: sql(e, lambda pg: only(pg['source_streams'], owner_epoch=1).update(last_complete_sequence=13)))
    check('original-registrar', lambda e: sql(e, lambda pg: pg['producer_slots'][0].update(registered_by_epoch=999)))
    check('target-reservation', lambda e: sql(e, lambda pg: only(pg['producer_epochs'], producer_epoch=2).update(reserved_through_request=1)))
    check('topology-handoff-rewritten', lambda e: sql(e, lambda pg: only(pg['topology_streams'], owner_epoch=2).update(handoff_last_complete_sequence=0)))
    check('original-input-hash', lambda e: snapshot(e)['state']['originals'][0][1].update(sha256='00' * 32))
    check('current-policy-digest', lambda e: only(e, event='successor-policy', phase='acquired').update(policy_digest='00' * 32))
    check('admission-claimed', lambda e: only(e, event='successor-empty-complete').update(admission=True))
    check('backend-absence-another-tuple', lambda e: only(e, event='successor-backend', present=False).update(backend_start='different'))
    check('backend-still-present', lambda e: only(e, event='successor-backend', present=False).update(present=True))
    check('driver-not-joined', lambda e: only(e, event='successor-driver').update(joined=False))
    check('driver-error-misclassified', lambda e: only(e, event='successor-driver').update(failed=True))
    check('missing-phase', lambda e: e.pop(next(i for i, v in enumerate(e) if v['event'] == 'successor-state')))
    if mode != 'empty-missing-intent':
        check('intent-attribution', lambda e: sql(e, lambda pg: pg['initial_ingest_successions'][0].update(prepared_by_epoch=999)))
        check('intent-xmin', lambda e: sql(e, lambda pg: pg['initial_ingest_successions'][0].update(xmin='999999999')))
    if any(e['event'] == 'successor-empty-request' for e in events):
        check('original-receipt', lambda e: only(e, event='successor-empty-request', sequence=1).update(receipt='00'))
        check('request-read-rewrites-PG', lambda e: sql(e, lambda pg: pg['producer_outcomes'][0].update(xmin='999999999'), 'resolved-1'))

        def future_renewals(changed):
            epoch = only(changed, event='successor-control', phase='claimed')['epoch']
            active = False
            for event in changed:
                if event['event'] != 'successor-state':
                    continue
                active |= event['stage'] == 'empty-refreshed-1'
                if active:
                    sql(changed, lambda pg: only(pg['owner_epochs'], role='control', epoch=epoch)
                        .update(lease_expires_at_us=(1 << 63) - 1), event['stage'][6:])
        check('consistently-future-renewals', future_renewals)
    if any(e['event'] == 'successor-empty-proof' for e in events):
        check('original-closure', lambda e: only(e, event='successor-empty-proof').update(closure_digest='00' * 32))
        check('empty-closure', lambda e: only(e, event='successor-empty-proof').update(empty_closure_digest='00' * 32))
        check('wrong-initial-native-state', lambda e: only(e, event='successor-empty-proof').update(initially_closing=mode not in FOLLOWUPS))
    if mode in ('empty-normal', 'empty-fresh-completion'):
        for field, table, selected, key, value in (
            ('source-digest', 'source_streams', {'owner_epoch': 2}, 'closure_digest', '\\x' + '00' * 32),
            ('source-cut', 'source_streams', {'owner_epoch': 2}, 'last_complete_sequence', 1),
            ('domain-time', 'allocation_domains', {'owner_epoch': 2, 'dictionary_kind': 1}, 'closed_at_us', 0),
            ('owner-xmin', 'owner_epochs', {'role': 'ingest', 'epoch': 2}, 'xmin', '999999999'),
            ('producer-only-closing', 'producer_epochs', {'producer_epoch': 2}, 'state', 'closing'),
        ):
            check('terminal-' + field, lambda e, t=table, s=selected, k=key, v=value:
                sql(e, lambda pg: only(pg[t], **s).update({k: v}), 'after-retirement'))
        check('retry-rewrites-PG', lambda e: sql(e, lambda pg: only(pg['producer_epochs'], producer_epoch=2)
            .update(xmin='999999999'), 'after-retry'))
        check('retirement-result-wrong-time', lambda e: only(e, event='successor-empty-retirement', attempt='initial')
            .update(retired_at_us=0))
    if mode == 'empty-normal':
        def early_native(changed):
            before = snapshot(changed, 'before-retirement')
            after = snapshot(changed, 'after-retirement')
            before['rows'] = copy.deepcopy(after['rows'])
            for field in ('native_rows', 'native_sha256', 'sequence'):
                before['state'][field] = after['state'][field]
        check('native-before-dispatch', early_native)
    if mode == 'empty-native-first-failure':
        def no_native_close(changed):
            rows = context['seed']['native']['rows']
            active = False
            for event in changed:
                if event['event'] != 'successor-state':
                    continue
                active |= event['stage'] == 'empty-after-retirement'
                if active:
                    event['rows'] = copy.deepcopy(rows)
                    event['state'].update(native_rows=len(rows), native_sha256=digest(encoded(rows)), sequence=62)
        check('native-first-failure-without-native-effect', no_native_close)
    if mode not in SUCCESS:
        check('unrelated-refusal', lambda e: only(e, event='successor-empty-result').update(error='DeliveryUnknown'))
        check('wrong-poisoning-result', lambda e: only(e, event='successor-empty-control-reuse').update(available=mode != 'empty-cancelled'))
    if mode in FOLLOWUPS:
        altered = copy.deepcopy(context)
        prior = altered['predecessor']
        only(prior['events'], event='successor-empty-proof')['empty_closure_digest'] = '00' * 32
        try:
            verify(events, original, native, inputs, mode, **altered)
        except AssertionError:
            rejected.append('predecessor-proof-not-trusted')
        else:
            raise AssertionError('oracle trusted an invalid predecessor transcript')
    return dict(status='PASS', rejected=rejected)
