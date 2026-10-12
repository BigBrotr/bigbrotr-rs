"""Independent finite completion oracle, consuming exact full observations.

The harness proves real preparation, process exit and backend absence separately.
Portable intent bytes are expected data, never authority or a positive witness.
"""
import copy
import json

from initial_succession_oracle import (
    digest, encoded, ended, expected, native_rows, only, same,
)
from initial_history_oracle import claim, inside_window, refresh, retained_expected, rows_dump

FAILURES = {
    'gap-missing-intent': ('gap-load', None, 'Control(InvalidMetadata)'),
    'gap-corrupt-intent': ('gap-load', None, 'Control(InvalidMetadata)'),
    'gap-target-input': ('gap-load', None, 'Native(RequestInput(Conflict))'),
    'gap-missing-input': ('request', 4, 'Native(Input(Missing))'),
    'gap-missing-outcome': ('gap-load', None, 'Control(Backend)'),
    'gap-cancelled': ('completion', None, 'Native(AuthorityUnavailable)'),
}


def known_gap(seed, retired):
    """Derive the exact four native writes from the independently checked seed."""
    intent = only(seed['control']['initial_ingest_successions'])
    plan, old_rows, new_rows = expected(retired['control'], retired['native'],
        retired['inputs'], intent['created_at_us'])
    assert seed['native'] == retired['native'] and seed['inputs'] == retired['inputs']
    for field in ('previous_manifest', 'target_manifest', 'handoff', 'closure_digest'):
        assert intent[field] == '\\x' + plan[field]
    for field in ('previous_manifest', 'target_manifest', 'handoff'):
        assert intent[field + '_sha256'] == '\\x' + digest(bytes.fromhex(plan[field]))
    # This projection exists only inside the independent oracle. The actual
    # engine must still be epoch1 until the observed completion boundary.
    known = retained_expected(seed['control'], rows_dump(new_rows), seed['inputs'])
    for field in ('registration', 'proof'):
        known[field]['event'] = known[field]['event'].replace('history', 'gap')
    return known, old_rows, new_rows


def preparation(original, native, inputs, mode, seed, known):
    wanted = copy.deepcopy(seed['control'])
    originals = dict(seed['inputs'])
    if mode == 'gap-missing-intent':
        wanted['initial_ingest_successions'] = []
    elif mode == 'gap-corrupt-intent':
        changed = only(original['initial_ingest_successions'])
        assert changed['xmin'].isdecimal() and all(changed['xmin'] != r['xmin']
            for table in seed['control'].values() for r in table)
        only(wanted['initial_ingest_successions']).update(closure_digest='\\x' + 'ab' * 32,
            xmin=changed['xmin'])
    elif mode == 'gap-missing-outcome':
        wanted['producer_outcomes'].remove(only(wanted['producer_outcomes'], request_sequence=3))
    elif mode in ('gap-missing-input', 'gap-target-input'):
        slot = known['registration']['producer_slot']
        if mode == 'gap-missing-input':
            del originals[slot + '-0000000000000001-0000000000000004.cbor']
        else:
            originals[slot + '-0000000000000002-0000000000000001.cbor'] = originals[
                slot + '-0000000000000001-0000000000000001.cbor']
    else:
        assert mode in ('gap-normal', 'gap-cancelled')
    same(wanted, original)
    assert native == seed['native'], 'fixture changed native rows'
    assert inputs == originals, 'unexpected input setup delta'


def verify(events, original, native, inputs, mode, *, seed, retired, server_window):
    assert mode == 'gap-normal' or mode in FAILURES
    assert set(server_window) == {'before_us', 'after_us'}
    assert all(type(v) is int and v > 0 for v in server_window.values())
    assert 0 <= server_window['after_us'] - server_window['before_us'] <= 120_000_000
    known, old_rows, new_rows = known_gap(seed, retired)
    preparation(original, native, inputs, mode, seed, known)
    position = snapshots = 0
    current = original
    completed = False
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

    planned = take('successor-control', 'gap', 'claim-planned')
    assert set(planned) == {'event', 'stage', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['lease_ttl_us'] == '20000000'
    same(current, snapshot('gap-before-claim'))
    claimed = take('successor-control', 'gap', 'claimed')
    after = snapshot('gap-after-claim')
    control = claim(current, after, planned, server_window, exclude_previous=True)
    assert claimed == dict(event='successor-control', stage='gap', phase='claimed',
        resource=control['resource_id'][2:], epoch=control['epoch'],
        role=control['role_instance_id'][2:], incarnation=control['process_incarnation'][2:])
    current = after
    same(current, snapshot('gap-before-read'))
    normal = mode == 'gap-normal'
    loaded = mode in ('gap-normal', 'gap-missing-input', 'gap-cancelled')
    if loaded:
        assert take('successor-gap-registration') == known['registration']
        same(current, snapshot('gap-loaded'))
        same(current, snapshot('gap-source-started'))
        for sequence in range(1, 5):
            after = snapshot(f'gap-refreshed-{sequence}')
            control = refresh(current, after, control, server_window)
            current = after
            if mode == 'gap-missing-input' and sequence == 4:
                break
            assert take('successor-gap-request') == dict(event='successor-gap-request',
                sequence=sequence, outcome='durable', receipt=known['receipts'][sequence - 1], rejection=None)
            resolved.append(sequence)
            same(current, snapshot(f'gap-resolved-{sequence}'))
        if normal or mode == 'gap-cancelled':
            assert take('successor-gap-proof') == known['proof']
            same(current, snapshot('gap-proof'))
            if mode == 'gap-cancelled':
                assert take('successor-gap-cancellation') == dict(event='successor-gap-cancellation', cancelled=True)
            same(current, snapshot('gap-before-completion'))
            if normal:
                assert take('successor-gap-completion') == dict(event='successor-gap-completion',
                    handoff=known['proof']['handoff'], admission=False)
                completed = True
            same(current, snapshot('gap-after-completion'))
    stage, sequence, error = ('complete', None, None) if normal else FAILURES[mode]
    assert take('successor-gap-result', stage) == dict(event='successor-gap-result',
        stage=stage, sequence=sequence, error=error)
    same(current, snapshot('gap-after-read'))
    same(current, snapshot('gap-before-control-end'))
    phase = 'released-and-joined' if normal else 'dropped-and-joined'
    assert take('successor-control', 'gap', phase) == dict(event='successor-control', stage='gap',
        phase=phase, resource=control['resource_id'][2:], epoch=control['epoch'])
    after = snapshot('gap-after-control-end')
    ended(current, after, control, normal)
    if normal:
        inside_window(only(after['owner_epochs'], resource_id=control['resource_id'],
            epoch=control['epoch'])['released_at_us'], server_window)
        inside_window(only(after['role_instances'],
            role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
    same(after, snapshot('gap-final'))
    assert take('successor-gap-complete') == dict(event='successor-gap-complete', native='closed',
        control=phase, admission=False, success=normal)
    assert position == len(events), 'unclassified trailing event'
    return dict(status='PASS', mode=mode, snapshots=snapshots, control_tables=71,
        original_native_rows=49, final_native_rows=52 if completed else 49,
        native_sequence_delta=4 if completed else 0, input_files=len(inputs), resolved_requests=resolved,
        control_epoch=control['epoch'], registrar_epoch=known['registration']['control_epoch'],
        prepared_by_epoch=only(seed['control']['initial_ingest_successions'])['prepared_by_epoch'],
        sql_history_and_intent_xmin_preserved=True, original_attribution_preserved=True,
        target_incarnation_inherited=False, admission=False, result_error=error,
        completion=completed, server_clock_window=server_window,
        handoff_sha256=digest(bytes.fromhex(known['proof']['handoff'])))


def negative_controls(events, original, native, inputs, mode, **context):
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

    def snapshot(changed, stage='gap-final'):
        return only(changed, event='successor-state', stage=stage)

    def sql(changed, mutate, stage='gap-final'):
        state = snapshot(changed, stage)['state']
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

    check('native-byte-refreshed-hash', native_bit)
    check('historical-row-xmin', lambda e: sql(e,
        lambda pg: only(pg['producer_epochs'], producer_epoch=1).update(xmin='999999999')))
    check('old-Control-unfenced', lambda e: sql(e, lambda pg:
        only(pg['owner_epochs'], role='control', epoch=context['seed']['control']['initial_ingest_successions'][0]['prepared_by_epoch']).update(state='active'), 'gap-after-claim'))
    check('historical-registrar-rewritten', lambda e: sql(e,
        lambda pg: pg['producer_slots'][0].update(registered_by_epoch=9)))
    check('successor-reservation', lambda e: sql(e,
        lambda pg: only(pg['producer_epochs'], producer_epoch=2).update(reserved_through_request=1)))
    check('original-input-digest', lambda e: snapshot(e)['state']['originals'][0][1].update(sha256='00' * 32))
    check('native-sequence', lambda e: snapshot(e)['state'].update(sequence=snapshot(e)['state']['sequence'] + 1))
    check('missing-snapshot', lambda e: e.pop(next(i for i, value in enumerate(e) if value['event'] == 'successor-state')))
    check('admission-claim', lambda e: only(e, event='successor-gap-complete').update(admission=True))
    if mode != 'gap-missing-intent':
        check('intent-attribution', lambda e: sql(e,
            lambda pg: pg['initial_ingest_successions'][0].update(prepared_by_epoch=9)))
        check('intent-xmin', lambda e: sql(e,
            lambda pg: pg['initial_ingest_successions'][0].update(xmin='999999999')))
    if any(e['event'] == 'successor-gap-request' for e in events):
        def future_renewals(changed):
            epoch = only(changed, event='successor-control', phase='claimed')['epoch']
            active = False
            for event in changed:
                if event['event'] != 'successor-state':
                    continue
                active |= event['stage'] == 'gap-refreshed-1'
                if active:
                    sql(changed, lambda pg: only(pg['owner_epochs'], role='control',
                        epoch=epoch).update(lease_expires_at_us=(1 << 63) - 1), event['stage'])
        check('consistently-future-renewals', future_renewals)
        check('original-outcome', lambda e: only(e, event='successor-gap-request', sequence=1).update(receipt='00'))
        check('request-read-writes', lambda e: sql(e,
            lambda pg: pg['producer_outcomes'][0].update(xmin='999999999'), 'gap-resolved-1'))
    if mode in ('gap-normal', 'gap-cancelled'):
        check('closure-digest', lambda e: only(e, event='successor-gap-proof').update(closure_digest='00' * 32))
        check('incomplete-positive-bound', lambda e: only(e, event='successor-gap-proof').update(last_complete_sequence=11))
    if mode == 'gap-normal':
        def early(changed):
            before = snapshot(changed, 'gap-before-completion')
            after = snapshot(changed, 'gap-after-completion')
            before['rows'] = copy.deepcopy(after['rows'])
            for field in ('native_rows', 'native_sha256', 'sequence'):
                before['state'][field] = after['state'][field]
        check('native-before-completion', early)
        check('completion-writes-PG', lambda e: sql(e,
            lambda pg: pg['initial_ingest_successions'][0].update(xmin='999999999'), 'gap-after-completion'))
    else:
        check('unrelated-refusal', lambda e: only(e, event='successor-gap-result').update(error='DeliveryUnknown'))
    return dict(status='PASS', rejected=rejected)
