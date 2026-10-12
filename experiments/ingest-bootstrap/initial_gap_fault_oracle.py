"""Independent policy and delivery-loss observations for an existing initial gap.

No SQL, native product codec or process control. The owning harness separately
proves the selected wire boundary, process exit and immutable seed custody.
Each fresh process must begin at the exact final state qualified here before it.
"""
import copy
import json

from initial_gap_oracle import known_gap
from initial_history_oracle import claim, inside_window, refresh, rows_dump
from initial_succession_oracle import digest, encoded, ended, native_rows, only, same


MODES = ('gap-policy', 'gap-loss-before', 'gap-loss-after', 'prepared-recover')


def control_digest(rows):
    return digest(encoded({name: sorted(value, key=encoded) for name, value in rows.items()}))


def policy_setup(before, after):
    """Exactly two copied revisions and the matching dataset-head update.

This is privileged isolated fixture setup, not authenticated product activation.
Schema version and the two different documents/digests retain their own meaning.
"""
    assert len(before) == len(after) == 71
    dataset = only(before['datasets'])
    assert dataset['config_revision'] == dataset['policy_revision'] == 1
    current = only(after['datasets'])
    tx = current['xmin']
    assert tx.isdecimal() and all(tx != r['xmin'] for table in before.values() for r in table)
    wanted = copy.deepcopy(before)
    only(wanted['datasets']).update(config_revision=2, policy_revision=2, xmin=tx)
    for table in ('config_revisions', 'policy_revisions'):
        previous = only(before[table])
        assert previous['revision'] == 1 and previous['schema_version'] == 2
        assert previous['document']['format'] == 'bigbrotr-product-policy/2'
        next_row = dict(previous, revision=2, previous_revision=1,
            accepted_at_us=previous['accepted_at_us'] + 1, xmin=tx)
        if table == 'policy_revisions':
            assert previous['config_revision'] == 1
            next_row['config_revision'] = 2
        wanted[table].append(next_row)
    same(wanted, after)
    return dict(status='PASS', transaction_id=tx, changed_tables=[
        'config_revisions', 'datasets', 'policy_revisions'],
        historical_revisions_preserved=True, product_policy_activation=False)


def verify(events, original, native, inputs, mode, *, seed, retired, server_window, prior=None):
    assert mode in MODES
    assert set(server_window) == {'before_us', 'after_us'}
    assert all(type(v) is int and v > 0 for v in server_window.values())
    assert 0 <= server_window['after_us'] - server_window['before_us'] <= 120_000_000
    known, old_rows, new_rows = known_gap(seed, retired)
    assert inputs == seed['inputs'] and len(inputs) == 4
    actual = native_rows(native['rows'])
    assert native == rows_dump(actual)
    assert actual in (old_rows, new_rows), 'unrecognized prepared native state'
    advanced = only(original['datasets'])['policy_revision'] == 2
    if mode == 'prepared-recover':
        assert prior is not None and prior['status'] == 'PASS'
        assert prior['mode'] in MODES[:3]
        anchor = prior['final']
        assert control_digest(original) == anchor['control_sha256'], 'prior PostgreSQL state differs'
        assert digest(encoded(native)) == anchor['native_sha256'], 'prior native state differs'
        assert digest(encoded(inputs)) == anchor['inputs_sha256'], 'prior original inputs differ'
        assert advanced == (prior['mode'] == 'gap-policy')
    else:
        assert prior is None and actual == old_rows
        if mode == 'gap-policy':
            policy_setup(seed['control'], original)
        else:
            same(seed['control'], original)
            assert not advanced
    sequence = seed['sequence'] + (4 if actual == new_rows else 0)
    assert seed['sequence'] == 58
    if prior is not None:
        assert sequence == prior['final']['sequence']
    route = 'gap' if actual == old_rows else 'history'
    assert mode == 'prepared-recover' or route == 'gap'
    loss = mode in ('gap-loss-before', 'gap-loss-after')
    success = not loss
    position = snapshots = 0
    current = original
    completed = actual == new_rows

    def take(event, stage=None, phase=None):
        nonlocal position
        assert position < len(events), (event, stage, 'missing event')
        value = events[position]
        position += 1
        assert (value['event'], value.get('stage'), value.get('phase')) == (event, stage, phase), value
        return value

    def snapshot(suffix):
        nonlocal snapshots
        stage = route + '-' + suffix
        event = take('successor-state', stage)
        assert set(event) == {'event', 'stage', 'state', 'rows'}
        state = event['state']
        assert set(state) == {'native_rows', 'native_sha256', 'sequence',
            'control_sha256', 'control_tables', 'originals'}
        assert native_rows(event['rows']) == (new_rows if completed else old_rows), stage
        assert state['native_rows'] == len(event['rows'])
        assert state['native_sha256'] == digest(encoded(event['rows']))
        assert type(state['sequence']) is int and state['sequence'] == seed['sequence'] + 4 * completed, stage
        tables = state['control_tables']
        assert state['control_sha256'] == digest(encoded(tables))
        pg = {name: json.loads(rows) for name, rows in tables}
        assert len(pg) == len(tables) == 71 and list(pg) == sorted(original)
        assert len(state['originals']) == len(inputs) and dict(state['originals']) == inputs
        assert [name for name, _ in state['originals']] == sorted(inputs)
        snapshots += 1
        return pg

    def policy(value, phase, ordinal, control):
        head = only(original['datasets'])
        config = only(original['config_revisions'], revision=head['config_revision'])
        policy_row = only(original['policy_revisions'], revision=head['policy_revision'])
        assert head['config_revision'] == head['policy_revision'] == (2 if advanced else 1)
        assert value == dict(event='successor-policy',
            stage=route, phase=phase, sequence=ordinal, control_epoch=control['epoch'],
            config_revision=head['config_revision'], policy_revision=head['policy_revision'],
            config_digest=config['document_digest'][2:], policy_digest=policy_row['document_digest'][2:])

    manifest = known['proof']['previous_manifest' if route == 'gap' else 'target_manifest']
    assert take('successor-recovery-selection') == dict(event='successor-recovery-selection',
        route=route, owner_epoch=1 if route == 'gap' else 2,
        key_schema=1 if route == 'gap' else 2, manifest=manifest, admission=False)
    planned = take('successor-control', route, 'claim-planned')
    assert set(planned) == {'event', 'stage', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['lease_ttl_us'] == '20000000'
    same(current, snapshot('before-claim'))
    claimed = take('successor-control', route, 'claimed')
    # Policy/backend events precede the full after-claim state; verify them
    # against its actual row only once the complete observation is available.
    acquired = take('successor-policy', route, 'acquired')
    backend = take('successor-backend', route)
    after = snapshot('after-claim')
    previous = max((r for r in current['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    if prior is not None:
        assert previous['epoch'] == prior['final']['control_epoch']
        assert previous['state'] == prior['final']['control_state']
    control = claim(current, after, planned, server_window, exclude_previous=previous['state'] == 'active')
    assert claimed == dict(event='successor-control', stage=route, phase='claimed',
        resource=control['resource_id'][2:], epoch=control['epoch'],
        role=control['role_instance_id'][2:], incarnation=control['process_incarnation'][2:])
    policy(acquired, 'acquired', None, control)
    assert isinstance(backend.get('database'), str) and backend['database']
    assert backend == dict(event='successor-backend', stage=route, pid=control['control_backend_pid'],
        backend_start=control['control_backend_start'], database=backend['database'],
        control_epoch=control['epoch'], present=True)
    current = after
    same(current, snapshot('before-read'))
    assert take(f'successor-{route}-registration') == dict(known['registration'], event=f'successor-{route}-registration')
    same(current, snapshot('loaded'))
    if route == 'gap':
        same(current, snapshot('source-started'))
    for ordinal in range(1, 5):
        policy(take('successor-policy', route, 'refreshed'), 'refreshed', ordinal, control)
        after = snapshot(f'refreshed-{ordinal}')
        control = refresh(current, after, control, server_window)
        current = after
        assert take(f'successor-{route}-request') == dict(event=f'successor-{route}-request',
            sequence=ordinal, outcome='durable', receipt=known['receipts'][ordinal - 1], rejection=None)
        same(current, snapshot(f'resolved-{ordinal}'))
    assert take(f'successor-{route}-proof') == dict(known['proof'], event=f'successor-{route}-proof')
    same(current, snapshot('proof'))
    if route == 'gap':
        same(current, snapshot('before-completion'))
        if success:
            assert take('successor-gap-completion') == dict(event='successor-gap-completion',
                handoff=known['proof']['handoff'], admission=False)
        completed = mode != 'gap-loss-before'
        same(current, snapshot('after-completion'))
    error = {'gap-loss-before': 'Control(Backend)', 'gap-loss-after': 'DeliveryUnknown'}.get(mode)
    result_stage = 'completion' if loss else 'complete'
    assert take(f'successor-{route}-result', result_stage) == dict(event=f'successor-{route}-result',
        stage=result_stage, sequence=None, error=error)
    if loss:
        assert take('successor-session-poisoned', route) == dict(event='successor-session-poisoned',
            stage=route, control_epoch=control['epoch'])
    same(current, snapshot('after-read'))
    same(current, snapshot('before-control-end'))
    assert take('successor-driver', route) == dict(event='successor-driver', stage=route, failed=loss, joined=True)
    assert take('successor-backend', route) == dict(backend, present=False)
    phase = 'released-and-joined' if success else 'dropped-and-joined'
    assert take('successor-control', route, phase) == dict(event='successor-control', stage=route,
        phase=phase, resource=control['resource_id'][2:], epoch=control['epoch'])
    after = snapshot('after-control-end')
    ended(current, after, control, success)
    if success:
        inside_window(only(after['owner_epochs'], resource_id=control['resource_id'], epoch=control['epoch'])['released_at_us'], server_window)
        inside_window(only(after['role_instances'], role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
    same(after, snapshot('final'))
    assert take(f'successor-{route}-complete') == dict(event=f'successor-{route}-complete', native='closed',
        control=phase, admission=False, success=success)
    assert position == len(events), 'unclassified trailing event'
    final_native = rows_dump(new_rows if completed else old_rows)
    return dict(status='PASS', mode=mode, route=route, snapshots=snapshots, control_tables=71,
        resolved_requests=[1, 2, 3, 4], current_policy_revision=2 if advanced else 1,
        historical_policy_revision=1, registrar_epoch=known['registration']['control_epoch'],
        prepared_by_epoch=only(seed['control']['initial_ingest_successions'])['prepared_by_epoch'],
        sql_history_and_intent_xmin_preserved=True, admission=False, result_error=error,
        native_sequence_delta=4 if route == 'gap' and completed else 0,
        server_clock_window=server_window,
        qualification='wire cut and genuine process separation established independently by harness',
        final=dict(control_sha256=control_digest(after), native_sha256=digest(encoded(final_native)),
            inputs_sha256=digest(encoded(inputs)), sequence=seed['sequence'] + 4 * completed,
            control_epoch=control['epoch'], control_state='released' if success else 'active'))


def negative_controls(events, original, native, inputs, mode, **context):
    verify(events, original, native, inputs, mode, **context)
    rejected = []
    route = events[0]['route']

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
        return only(changed, event='successor-state', stage=route + '-' + suffix)

    def sql(changed, mutate, suffix='final'):
        state = snapshot(changed, suffix)['state']
        pg = {name: json.loads(rows) for name, rows in state['control_tables']}
        mutate(pg)
        state['control_tables'] = [[name, encoded(pg[name]).decode()] for name in sorted(pg)]
        state['control_sha256'] = digest(encoded(state['control_tables']))

    def native_bit(changed):
        event = snapshot(changed)
        row = only(event['rows'], cf='default', key='0101')
        value = bytearray.fromhex(row['value'])
        value[-1] ^= 1
        row['value'] = value.hex()
        event['state']['native_sha256'] = digest(encoded(event['rows']))

    check('selection-not-actual-manifest', lambda e: e[0].update(route='history' if route == 'gap' else 'gap'))
    check('native-byte-refreshed-hash', native_bit)
    check('native-extra-write', lambda e: snapshot(e)['state'].update(sequence=snapshot(e)['state']['sequence'] + 1))
    check('intent-original-preparer', lambda e: sql(e, lambda pg: pg['initial_ingest_successions'][0].update(prepared_by_epoch=10)))
    check('intent-xmin', lambda e: sql(e, lambda pg: pg['initial_ingest_successions'][0].update(xmin='999999999')))
    check('registrar-attribution', lambda e: sql(e, lambda pg: pg['producer_slots'][0].update(registered_by_epoch=10)))
    check('original-input', lambda e: snapshot(e)['state']['originals'][0][1].update(sha256='00' * 32))
    check('original-outcome', lambda e: only(e, event=f'successor-{route}-request', sequence=1).update(receipt='00'))
    check('positive-closure', lambda e: only(e, event=f'successor-{route}-proof').update(closure_digest='00' * 32))
    check('successor-admission', lambda e: only(e, event=f'successor-{route}-complete').update(admission=True))
    check('successor-reservation', lambda e: sql(e, lambda pg: only(pg['producer_epochs'], producer_epoch=2).update(reserved_through_request=1)))
    check('current-policy-digest', lambda e: only(e, event='successor-policy', phase='acquired').update(policy_digest='00' * 32))
    check('missing-snapshot', lambda e: e.pop(next(i for i, v in enumerate(e) if v['event'] == 'successor-state')))
    check('backend-identity-reused', lambda e: only(e, event='successor-backend', present=False).update(backend_start='different backend'))
    check('backend-still-present', lambda e: only(e, event='successor-backend', present=False).update(present=True))
    check('driver-not-joined', lambda e: only(e, event='successor-driver').update(joined=False))

    def future_renewals(changed):
        epoch = only(changed, event='successor-control', phase='claimed')['epoch']
        active = False
        for event in changed:
            if event['event'] != 'successor-state':
                continue
            active |= event['stage'] == route + '-refreshed-1'
            if active:
                sql(changed, lambda pg: only(pg['owner_epochs'], role='control', epoch=epoch)
                    .update(lease_expires_at_us=(1 << 63) - 1), event['stage'][len(route) + 1:])
    check('consistently-future-renewals', future_renewals)
    if mode == 'gap-policy' or (context.get('prior') or {}).get('mode') == 'gap-policy':
        check('current-policy-confused-with-historical', lambda e: [v.update(config_revision=1, policy_revision=1)
            for v in e if v['event'] == 'successor-policy'])
    if mode == 'gap-policy':
        for name, mutate in (
            ('policy-setup-conflates-documents', lambda pg: only(pg['policy_revisions'], revision=2)
                .update(document_digest=only(pg['config_revisions'], revision=2)['document_digest'])),
            ('policy-setup-rewrites-predecessor', lambda pg: only(pg['config_revisions'], revision=1)
                .update(xmin='999999999')),
            ('policy-setup-rebinds-intent', lambda pg: pg['initial_ingest_successions'][0].update(policy_revision=2)),
            ('policy-setup-extra-write', lambda pg: pg['producer_outcomes'][0].update(xmin='999999999')),
        ):
            changed = copy.deepcopy(original)
            mutate(changed)
            try:
                policy_setup(context['seed']['control'], changed)
            except AssertionError:
                rejected.append(name)
            else:
                raise AssertionError('oracle accepted perturbation: ' + name)
    if mode == 'prepared-recover':
        for field in ('control_sha256', 'native_sha256', 'inputs_sha256'):
            altered = dict(context, prior=copy.deepcopy(context['prior']))
            altered['prior']['final'][field] = '00' * 32
            try:
                verify(events, original, native, inputs, mode, **altered)
            except AssertionError:
                rejected.append('prior-' + field)
            else:
                raise AssertionError('oracle accepted a changed prior anchor: ' + field)
    if mode in ('gap-loss-before', 'gap-loss-after'):
        check('unrelated-error', lambda e: only(e, event='successor-gap-result').update(error='Control(InvalidMetadata)'))
        check('failed-driver-called-clean', lambda e: only(e, event='successor-driver').update(failed=False))
        def opposite_native(changed):
            _, old, new = known_gap(context['seed'], context['retired'])
            rows = rows_dump(old if mode == 'gap-loss-after' else new)['rows']
            active = False
            for event in changed:
                if event['event'] != 'successor-state':
                    continue
                active |= event['stage'] == 'gap-after-completion'
                if active:
                    event['rows'] = copy.deepcopy(rows)
                    event['state'].update(native_rows=len(rows), native_sha256=digest(encoded(rows)),
                        sequence=context['seed']['sequence'] + (0 if mode == 'gap-loss-after' else 4))
        check('consistently-wrong-native-loss-outcome', opposite_native)
    return dict(status='PASS', rejected=rejected)
