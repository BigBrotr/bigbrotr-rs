"""Exact observations of old recovery routes against a retained activation.

Frozen complete seeds and raw snapshots are never repaired for an old reducer.
The gap route refuses native epoch2 before SQL; it does not exercise the receipt
veto. Process identity, stopped source custody and external captures belong to
the harness. Only the empty probe directly observes unavailable Control reuse.
"""
import copy
from datetime import datetime, timezone
import json
import math
import re

import initial_history_oracle as history
import initial_live_oracle as live

CASES = ('activated-history', 'activated-gap', 'activated-empty', 'prepared-history')
ACTIVE_CONTROL = 'd2a1ee5282209dbc7c95c4a2cf9a96d0844ae73a73a5f181511334ededde38f1'
ACTIVE_NATIVE = '4bffbc4052917a1e836c640440c30fdd9f8c7a1f652e8783d808612f7a006fe0'
REFUSAL = 'activated initial writer is not an unactivated preparation'
FUNCTIONS = {'activated-history': 'require_prepared_initial_successor(bytea)',
    'activated-empty': 'lock_initial_empty_retirement(bytea,bytea)'}


def baseline(original, native, inputs, mode, seed, window):
    assert set(seed) == {'control', 'native', 'inputs', 'sequence'} and seed['sequence'] == 62
    assert native == seed['native'] and inputs == seed['inputs']
    assert native['event'] == 'dump' and len(native['rows']) == 52 and len(inputs) == 4
    assert live.digest(live.encoded(inputs)) == history.SEED_INPUTS
    wanted = copy.deepcopy(seed['control'])
    known = None
    if mode == 'prepared-history':
        known = history.seed_expected({k: seed[k] for k in ('control', 'native', 'inputs')})
        assert max(r['migration'] for r in wanted['schema_versions'] if r['migration'] < 100) == 30
        tx = live.only(original['schema_versions'], migration=31)['xmin']
        live.fresh_xmin(tx, wanted)
        for number in (31, 32):
            row = live.only(original['schema_versions'], migration=number)
            name, checksum = live.MIGRATIONS[number]
            assert type(row['applied_at_us']) is int
            assert max(r['applied_at_us'] for r in wanted['schema_versions']) <= row['applied_at_us'] <= window['before_us']
            assert row == dict(name=name, checksum='\\x' + checksum, migration=number,
                engine='postgres-control', applied_by='bigbrotr_migrator', reader_min=1,
                reader_max=1, writer_floor=1, applied_at_us=row['applied_at_us'], xmin=tx)
            wanted['schema_versions'].append(row)
        wanted[live.ACTIVATIONS] = []
    else:
        assert live.digest(live.encoded({k: sorted(v, key=live.encoded)
            for k, v in wanted.items()})) == ACTIVE_CONTROL, 'activated seed differs'
        assert live.digest(live.encoded(native)) == ACTIVE_NATIVE
        activation = live.only(wanted[live.ACTIVATIONS])
        assert activation['prepared_by_epoch'] == activation['activated_by_epoch'] == 8
        assert activation['activation_transaction'] == activation['xmin'] == '26894'
        assert activation['activated_at_us'] == 1791874536878587
    live.same(wanted, original)
    assert len(original) == 72
    intent = live.only(original['initial_ingest_successions'])
    assert intent['prepared_by_epoch'] == 8
    producer = live.only(original['producer_epochs'], producer_epoch=2)
    assert producer['state'] == 'open' and producer['reserved_through_request'] == 0
    assert not any(r['producer_epoch'] == 2 for r in original['producer_requests'])
    source = live.only(original['source_streams'], owner_epoch=2)
    assert source['state'] == 'open' and source['last_complete_sequence'] is None
    previous = max((r for r in original['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    assert previous['epoch'] == (9 if known else 8) and previous['state'] == 'released'
    rows = live.native_rows(native['rows'])
    slot = live.binary(intent['producer_slot_id'])
    ds = live.binary(intent['dataset_id'])
    assert rows['default', b'\x01\x01'] == live.binary(intent['target_manifest'])
    assert rows['default', b'\x01\x02' + ds + live.be(0, 4) + live.be(2)] == b'\x01\x02' + live.be(0)
    assert rows['receipts', b'\x01\x31' + slot + live.be(2)] == b'\x01\x31' + live.be(0) * 3
    assert rows['default', b'\x01\x0a' + ds + live.binary(intent['owner_resource_id']) + live.be(1)] == live.binary(intent['handoff'])
    return known


def backend_checked(backend, control):
    assert set(backend) == {'exact', 'absence'}
    exact, absence = backend['exact'], backend['absence']
    expected = exact['expected']
    database = expected['database']
    assert isinstance(database, str) and re.fullmatch('[a-z][a-z0-9_]{0,62}', database)
    assert expected == dict(pid=control['control_backend_pid'],
        backend_start=control['control_backend_start'], database=database, control_epoch=control['epoch'])
    instant = datetime.fromisoformat(expected['backend_start'])
    assert instant.tzinfo is not None
    assert instant <= datetime.fromtimestamp(control['acquired_at_us'] / 1_000_000, timezone.utc)
    assert exact == dict(expected=expected, returncode=0, stdout='f\n', stderr='', present=False)
    assert absence['database'] == database and absence['user'] == 'bigbrotr_control'
    assert absence['status'] == 'OBSERVED_ABSENT' and absence['limit_seconds'] == 5
    assert set(absence) == {'database', 'user', 'limit_seconds', 'status', 'attempts', 'seconds'}
    samples = absence['attempts']
    assert type(absence['seconds']) in (int, float) and math.isfinite(absence['seconds'])
    assert samples and 0 <= samples[-1]['seconds'] <= absence['seconds']
    previous = 0
    for sample in samples:
        assert set(sample) == {'seconds', 'count', 'backends'}
        assert type(sample['seconds']) in (int, float) and previous <= sample['seconds'] <= 5
        previous = sample['seconds']
        assert type(sample['count']) is int and sample['count'] == len(sample['backends'])
        assert sample['count'] >= 0
        pids = []
        for row in sample['backends']:
            # The direct observer may also linger after its joined driver.
            # This census proves final absence, not transient tuple ownership.
            assert set(row) == {'pid', 'backend_start'}
            assert type(row['pid']) is int and row['pid'] > 0
            assert datetime.fromisoformat(row['backend_start']).tzinfo is not None
            pids.append(row['pid'])
        assert pids == sorted(set(pids))
    assert samples[-1]['count'] == 0 and samples[-1]['backends'] == []
    return expected


def diagnostic_checked(diagnostic, mode, backend, window):
    if mode not in FUNCTIONS:
        assert diagnostic is None
        return
    assert set(diagnostic) == {'backend', 'since', 'until', 'returncode',
        'observed_sqlstate', 'errors', 'contexts'}
    assert diagnostic['backend'] == backend and diagnostic['returncode'] == 0
    def instant(us):
        return datetime.fromtimestamp(us / 1_000_000, timezone.utc).isoformat()
    assert diagnostic['since'] == instant(window['before_us'])
    assert diagnostic['until'] == instant(window['after_us'])
    error, = diagnostic['errors']
    context, = diagnostic['contexts']
    prefix = r'\[' + str(backend['pid']) + r'\] '
    match = re.search(prefix + r'ERROR:  (?:(?P<sqlstate>[0-9A-Z]{5}): )?'
        + re.escape(REFUSAL) + r'$', error)
    assert match, 'different SQL refusal'
    assert match['sqlstate'] in (None, '23514')
    assert diagnostic['observed_sqlstate'] == match['sqlstate']
    assert re.search(prefix + r'CONTEXT:  PL/pgSQL function (?:control\.)?'
        + re.escape(FUNCTIONS[mode]) + r' line [0-9]+ at RAISE', context), 'different SQL guard'
    for line in (error, context):
        at = datetime.fromisoformat(line.split()[0].replace('Z', '+00:00'))
        assert datetime.fromisoformat(diagnostic['since']) <= at <= datetime.fromisoformat(diagnostic['until'])


def verify(events, original, native, inputs, mode, *, seed, server_window, backend, diagnostic):
    assert mode in CASES
    live.check_window(server_window)
    known = baseline(original, native, inputs, mode, seed, server_window)
    positive = mode == 'prepared-history'
    route = 'history' if mode in ('activated-history', 'prepared-history') else mode.removeprefix('activated-')
    capture = live.Capture(events, original, live.native_rows(native['rows']), inputs)
    capture.sequence = 62
    def snap(suffix):
        return capture.snapshot(route + '-' + suffix)
    planned = capture.take('successor-control', route, 'claim-planned')
    assert set(planned) == {'event', 'stage', 'phase', 'role', 'incarnation', 'lease_ttl_us'}
    assert planned['lease_ttl_us'] == '20000000' and planned['role'] != planned['incarnation']
    live.same(original, snap('before-claim'))
    claimed = capture.take('successor-control', route, 'claimed')
    policy = capture.take('successor-policy', route, 'acquired') if route == 'empty' else None
    present = capture.take('successor-backend', route) if route == 'empty' else None
    pg = snap('after-claim')
    control = history.claim(original, pg, planned, server_window)
    assert control['epoch'] == (10 if positive else 9)
    assert claimed == dict(event='successor-control', stage=route, phase='claimed',
        resource=control['resource_id'][2:], epoch=control['epoch'],
        role=control['role_instance_id'][2:], incarnation=control['process_incarnation'][2:])
    expected_backend = backend_checked(backend, control)
    if route == 'empty':
        assert present == dict(event='successor-backend', stage=route, **expected_backend, present=True)
        assert policy == dict(event='successor-policy', stage=route, phase='acquired', sequence=None,
            control_epoch=9, config_revision=1, policy_revision=1,
            config_digest=live.only(original['config_revisions'], revision=1)['document_digest'][2:],
            policy_digest=live.only(original['policy_revisions'], revision=1)['document_digest'][2:])
    live.same(pg, snap('before-read'))
    if positive:
        assert capture.take('successor-history-registration') == known['registration']
        live.same(pg, snap('loaded'))
        for sequence in range(1, 5):
            after = snap(f'refreshed-{sequence}')
            previous = control
            control = history.refresh(pg, after, control, server_window)
            assert control['lease_expires_at_us'] - 20_000_000 < previous['lease_expires_at_us']
            pg = after
            assert capture.take('successor-history-request') == dict(event='successor-history-request',
                sequence=sequence, outcome='durable', receipt=known['receipts'][sequence - 1], rejection=None)
            live.same(pg, snap(f'resolved-{sequence}'))
        assert capture.take('successor-history-proof') == known['proof']
        live.same(pg, snap('proof'))
    stage = 'complete' if positive else route + '-load'
    error = None if positive else 'Control(InvalidInput)' if route == 'gap' else 'Control(Backend)'
    assert capture.take('successor-' + route + '-result', stage) == dict(
        event='successor-' + route + '-result', stage=stage, sequence=None, error=error)
    if route == 'empty':
        assert capture.take('successor-empty-control-reuse') == dict(
            event='successor-empty-control-reuse', available=False, error='Unavailable')
        live.same(pg, snap('after-reuse'))
    live.same(pg, snap('after-read'))
    live.same(pg, snap('before-control-end'))
    if route == 'empty':
        assert capture.take('successor-driver', route) == dict(
            event='successor-driver', stage=route, failed=False, joined=True)
        assert capture.take('successor-backend', route) == dict(
            event='successor-backend', stage=route, **expected_backend, present=False)
    phase = 'released-and-joined' if positive else 'dropped-and-joined'
    assert capture.take('successor-control', route, phase) == dict(event='successor-control',
        stage=route, phase=phase, resource=control['resource_id'][2:], epoch=control['epoch'])
    after = snap('after-control-end')
    live.ended(pg, after, control, positive)
    if positive:
        released = live.only(after['owner_epochs'], role='control', epoch=control['epoch'])
        live.fresh_xmin(released['xmin'], pg)
        live.clock(released['released_at_us'], server_window)
        live.clock(live.only(after['role_instances'], role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
        assert released['released_at_us'] >= control['lease_expires_at_us'] - 20_000_000
    live.same(after, snap('final'))
    completion = dict(event='successor-' + route + '-complete', native='closed',
        control=phase, admission=False, success=positive)
    if route == 'empty':
        completion['command'] = 'empty-retire'
    assert capture.take(completion['event']) == completion
    assert capture.position == len(events), 'unclassified trailing event'
    diagnostic_checked(diagnostic, mode, expected_backend, server_window)
    return dict(status='PASS', mode=mode, snapshots=capture.snapshots, control_tables=72,
        native_rows=52, native_sequence=62, native_writes=0, original_inputs=4,
        activation_preserved=not positive, original_attribution_preserved=True,
        current_control_epoch=control['epoch'], preparer_epoch=8, registrar_epoch=3,
        proof_reconstructed=positive, resolved_requests=4 if positive else 0,
        result_error=error, receipt_veto_observed=mode in FUNCTIONS,
        gap_boundary='native epoch2 rejected before transaction' if route == 'gap' else None,
        reuse_unavailable_observed=route == 'empty', driver_result_emitted=route == 'empty',
        backend_absent=expected_backend, admission=False, writer_adoption=False,
        process_custody='separately established by owning harness')


def negative_controls(events, original, native, inputs, mode, *, seed, server_window, backend, diagnostic):
    context = dict(events=events, original=original, native=native, inputs=inputs, mode=mode,
        seed=seed, server_window=server_window, backend=backend, diagnostic=diagnostic)
    verify(**context)
    rejected = []
    def check(name, change):
        changed = copy.deepcopy(context)
        change(changed)
        try:
            verify(**changed)
        except (AssertionError, KeyError, ValueError, TypeError, StopIteration):
            rejected.append(name)
        else:
            raise AssertionError('oracle accepted perturbation: ' + name)
    route = 'history' if mode in ('activated-history', 'prepared-history') else mode.removeprefix('activated-')
    def final(c):
        return live.only(c['events'], event='successor-state', stage=route + '-final')
    def sql(c, change, stage=None):
        event = final(c) if stage is None else live.only(c['events'], event='successor-state', stage=stage)
        pg = {t: json.loads(v) for t, v in event['state']['control_tables']}
        change(pg)
        event['state']['control_tables'] = [[t, live.encoded(pg[t]).decode()] for t in sorted(pg)]
        event['state']['control_sha256'] = live.digest(live.encoded(event['state']['control_tables']))
    def native_bit(c):
        event = final(c)
        row = live.only(event['rows'], cf='default', key='0101')
        raw = bytearray.fromhex(row['value']); raw[-1] ^= 1; row['value'] = raw.hex()
        event['state']['native_sha256'] = live.digest(live.encoded(event['rows']))
    check('native-byte-rehashed', native_bit)
    check('native-sequence-changed', lambda c: final(c)['state'].update(sequence=63))
    check('original-input-changed', lambda c: final(c)['state']['originals'][0][1].update(sha256='00' * 32))
    check('old-original-xmin', lambda c: sql(c, lambda p:
        live.only(p['producer_outcomes'], producer_epoch=1, request_sequence=1).update(xmin='999999999')))
    check('target-reservation-created', lambda c: sql(c, lambda p:
        live.only(p['producer_epochs'], producer_epoch=2).update(reserved_through_request=1)))
    check('target-incarnation-adopted', lambda c: sql(c, lambda p:
        live.only(p['owner_epochs'], role='ingest', epoch=2).update(process_incarnation='\\x' + 'ff' * 16)))
    check('intent-attribution-rewritten', lambda c: sql(c, lambda p:
        live.only(p['initial_ingest_successions']).update(prepared_by_epoch=9)))
    check('new-control-wrong-epoch', lambda c:
        live.only(c['events'], event='successor-control', phase='claimed').update(epoch=3))
    check('missing-snapshot', lambda c: c['events'].pop(next(i for i, e in enumerate(c['events']) if e['event'] == 'successor-state')))
    check('backend-still-present', lambda c: c['backend']['exact'].update(stdout='t\n', present=True))
    check('backend-tuple-substitution', lambda c: c['backend']['exact']['expected'].update(pid=1))
    check('global-absence-not-observed', lambda c: c['backend']['absence'].update(status='INCOMPLETE'))
    def lingering_backend(c):
        observed = c['backend']['exact']['expected']
        c['backend']['absence']['attempts'][-1].update(count=1,
            backends=[{k: observed[k] for k in ('pid', 'backend_start')}])
    check('global-final-backend-present', lingering_backend)
    def duplicate_backend(c):
        row = {k: c['backend']['exact']['expected'][k] for k in ('pid', 'backend_start')}
        c['backend']['absence']['attempts'].insert(0,
            dict(seconds=0, count=2, backends=[row, copy.deepcopy(row)]))
    check('global-duplicate-backend-tuple', duplicate_backend)
    check('native-not-closed', lambda c: live.only(c['events'], event='successor-' + route + '-complete').update(native='open'))
    check('admission-claimed', lambda c: live.only(c['events'], event='successor-' + route + '-complete').update(admission=True))
    if mode == 'prepared-history':
        check('original-receipt-changed', lambda c: live.only(c['events'], event='successor-history-request', sequence=1).update(receipt='00'))
        check('proof-closure-changed', lambda c: live.only(c['events'], event='successor-history-proof').update(closure_digest='00' * 32))
        def future_lease(c):
            active = False
            for event in c['events']:
                if event['event'] != 'successor-state':
                    continue
                active |= event['stage'] == 'history-refreshed-1'
                if active:
                    sql(c, lambda p: live.only(p['owner_epochs'], role='control', epoch=10).update(
                        lease_expires_at_us=(1 << 63) - 1), event['stage'])
        check('consistently-future-renewals', future_lease)
    else:
        check('activation-xmin-rewritten', lambda c: sql(c, lambda p: live.only(p[live.ACTIVATIONS]).update(xmin='999999999')))
        check('different-loader-refusal', lambda c: live.only(c['events'], event='successor-' + route + '-result').update(error='DeliveryUnknown'))
        def remove_activation(c):
            c['original'][live.ACTIVATIONS] = []
            c['seed']['control'][live.ACTIVATIONS] = []
            for event in c['events']:
                if event['event'] == 'successor-state':
                    sql(c, lambda p: p[live.ACTIVATIONS].clear(), event['stage'])
        check('coherent-activation-erasure', remove_activation)
    if mode in FUNCTIONS:
        check('different-server-message', lambda c: c['diagnostic']['errors'].__setitem__(0,
            c['diagnostic']['errors'][0].replace(REFUSAL, 'unrelated failure')))
        check('different-server-function', lambda c: c['diagnostic']['contexts'].__setitem__(0,
            c['diagnostic']['contexts'][0].replace(FUNCTIONS[mode], 'unrelated()')))
        check('server-backend-mismatch', lambda c: c['diagnostic']['backend'].update(pid=1))
    if route == 'empty':
        check('poisoned-control-reused', lambda c: live.only(c['events'], event='successor-empty-control-reuse').update(available=True, error=None))
        check('driver-not-joined', lambda c: live.only(c['events'], event='successor-driver').update(joined=False))
    return dict(status='PASS', rejected=rejected)
