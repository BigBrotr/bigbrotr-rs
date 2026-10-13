"""Independent rejected-request/source-zero seed and installed recovery evidence.

The finite producer performs real activation/admission. Literal input and rejection
bytes define the new effects; no product encoder or observed receipt supplies the
expectation. This qualifies neither an installed import nor network provenance.
"""
import copy
import json
import math

import activated_recovery_oracle as recovery
import initial_live_oracle as live
from bootstrap_oracle import cbor
from initial_live_routes_oracle import backend_checked as finite_backend
from initial_succession_oracle import claim, ended, only, prepared, renewed, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import catalog_identity, instant


MODE = 'installed-activated-source-zero'
CASES = (MODE,)
PROBE = 'successor-live-rejected'
INVALID_SIGNATURE = bytes.fromhex('9544d1fab8095f14d43137f841a400f0648cd614515f53bb5545749e373056381ce2963d43fdb5d10d9294846da6fc895d1fdfaf3417399a5c2699414877c180')


def rejected_science(plan, prepared_rows, inputs):
    science = live.Science(plan, prepared_rows, inputs)
    original = b'bigbrotr/producer-request/1\0' + cbor([[science.slot, 2, 1], 'admit', 1,
        [1, science.scope, [*live.BODY, INVALID_SIGNATURE], 'import', science.lineage, None]])
    rejection = (b'bigbrotr/rejection/1\0' + science.slot + live.be(2) + live.be(1)
        + live.sha(original) + science.ds + live.be(1) + b'\x04')
    science.rows['receipts', b'\x01\x30' + science.slot + live.be(2) + live.be(1)] = rejection
    science.rows['receipts', b'\x01\x31' + science.slot + live.be(2)] = b'\x01\x31' + live.be(1) + live.be(0) * 2
    name = science.slot.hex() + '-0000000000000002-0000000000000001.cbor'
    science.inputs[name] = dict(bytes=len(original), sha256=live.digest(original))
    science.sequence = 64
    return science, name, original, rejection


def rejected_sql(before, after, science, original, rejection, control, clock):
    request = only(after['producer_requests'], producer_epoch=2, request_sequence=1)
    outcome = only(after['producer_outcomes'], producer_epoch=2, request_sequence=1)
    fresh_xmin(request['xmin'], before)
    fresh_xmin(outcome['xmin'], before)
    assert request['xmin'] != outcome['xmin']
    for value in (request['reserved_at_us'], outcome['recorded_at_us']):
        live.clock(value, clock)
    assert only(before[live.ACTIVATIONS])['activated_at_us'] <= request['reserved_at_us'] <= outcome['recorded_at_us']
    assert outcome['recorded_at_us'] < min(control['lease_expires_at_us'], science.plan['lease_expires_at_us'])
    identity = dict(producer_slot_id=live.bytea(science.slot), producer_epoch=2, request_sequence=1,
        dataset_id=live.bytea(science.ds), request_digest=live.bytea(live.sha(original)),
        manifest_sha256=live.bytea(live.sha(bytes.fromhex(science.plan['target_manifest']))))
    wanted_request = dict(identity, input_bytes=len(original), input_profile=1,
        issuer_resource=control['resource_id'], issuer_epoch=8, issuer_role='control',
        issuer_incarnation=control['process_incarnation'], reserved_at_us=request['reserved_at_us'],
        relay_endpoint_id=None, relay_access_scope_id=None, relay_enrollment_generation=None,
        relay_route_id=None, acquisition_scope_id=None, acquisition_job_id=None, xmin=request['xmin'])
    wanted_outcome = dict(identity, outcome_kind=3, policy_epoch=1, outcome_bytes=live.bytea(rejection),
        outcome_sha256=live.bytea(live.sha(rejection)), recorder_resource=control['resource_id'],
        recorder_epoch=8, recorder_role='control', recorder_incarnation=control['process_incarnation'],
        recorded_at_us=outcome['recorded_at_us'], xmin=outcome['xmin'])
    wanted = copy.deepcopy(before)
    producer = only(wanted['producer_epochs'], producer_epoch=2)
    assert producer['reserved_through_request'] == 0
    producer.update(reserved_through_request=1, xmin=request['xmin'])
    wanted['producer_requests'].append(wanted_request)
    wanted['producer_outcomes'].append(wanted_outcome)
    same(wanted, after)


def seed_state(events, original, native, inputs, *, retired, server_window, backend, response,
        after, native_after, inputs_after, input_contents, catalog_before, catalog_after):
    live.check_window(server_window)
    live.migrated(retired, original, native, inputs, server_window)
    catalog_identity(catalog_before)
    assert catalog_before == catalog_after
    assert response['returncode'] == 0 and response['stderr'] == ''
    assert [json.loads(line) for line in response['stdout'].splitlines()] == events
    observed = only(events, event='successor-plan')
    live.clock(observed['created_at_us'], server_window)
    plan, old_rows, prepared_rows = live.minted_plan(original, native, inputs, observed)
    capture = live.Capture(events, original, old_rows, inputs)
    assert capture.take('successor-control', 'initial', 'claim-planned') == dict(event='successor-control',
        stage='initial', phase='claim-planned', role='d3' * 16, incarnation='dd' * 16, lease_ttl_us='20000000')
    assert plan['target_role'] != 'd3' * 16 and plan['target_incarnation'] != 'dd' * 16
    same(original, capture.snapshot('initial-before-claim'))
    claimed = capture.take('successor-control', 'initial', 'claimed')
    current = capture.snapshot('initial-after-claim')
    control = claim(original, current, 1)
    assert control['epoch'] == 8
    assert claimed == dict(event='successor-control', stage='initial', phase='claimed',
        resource=control['resource_id'][2:], epoch=8, role='d3' * 16, incarnation='dd' * 16)
    live.clock(control['acquired_at_us'], server_window)
    live.clock(instant(control['control_backend_start']), server_window)
    live.clock(only(current['role_instances'], role_instance_id=control['role_instance_id'])['created_at_us'], server_window)
    assert capture.take('successor-plan') == plan
    following = capture.snapshot('before-prepare')
    control = renewed(current, following, control, plan['created_at_us'])
    fresh_xmin(control['xmin'], current)
    live.clock(control['lease_expires_at_us'] - 20_000_000, server_window)
    current = following
    capture.native, capture.sequence = prepared_rows, 62
    following = capture.snapshot('prepared')
    # Only the independently checked empty activation table is excluded from
    # this older71-table preparation reducer; all72 tables remain captured.
    assert current[live.ACTIVATIONS] == following[live.ACTIVATIONS] == []
    prepared({k: v for k, v in current.items() if k != live.ACTIVATIONS},
        {k: v for k, v in following.items() if k != live.ACTIVATIONS}, plan, control)
    current = following
    physical = capture.take('successor-backend', 'initial')
    assert physical == dict(event='successor-backend', stage='initial',
        **backend['exact']['expected'], present=True)
    assert capture.take('live-activation-start') == dict(event='live-activation-start', mode=PROBE, control_epoch=8)
    result = capture.take('live-activation-result')
    assert result == dict(event='live-activation-result', writer=True, error=None,
        activated_at_us=result['activated_at_us'])
    following = capture.snapshot('activated-zero')
    live.activated(current, following, plan, control, server_window, result['activated_at_us'])
    current = following
    science, filename, raw, rejection = rejected_science(plan, prepared_rows, inputs)
    assert capture.take('live-rejected-input') == dict(event='live-rejected-input', request=1,
        fixture='E_public_invalid_signature', event_id=live.EVENT_ID.hex(), supplied_signature=INVALID_SIGNATURE.hex(),
        preimage=raw.hex(), request_digest=live.digest(raw), provenance=False)
    capture.native, capture.sequence, capture.inputs = science.rows, 64, science.inputs
    following = capture.snapshot('rejected')
    rejected_sql(current, following, science, raw, rejection, control, server_window)
    current = following
    assert capture.take('live-rejection') == dict(event='live-rejection', request=1,
        reason='Signature', outcome=rejection.hex(), validations=4)
    same(current, capture.snapshot('final-before-control-end'))
    assert capture.take('successor-control', 'final', 'released-and-joined') == dict(event='successor-control',
        stage='final', phase='released-and-joined', resource=control['resource_id'][2:], epoch=8)
    following = capture.snapshot('final-after-control-end')
    ended(current, following, control, True)
    released = only(following['owner_epochs'], role='control', epoch=8)
    fresh_xmin(released['xmin'], current)
    live.clock(released['released_at_us'], server_window)
    assert outcome_time(current) <= released['released_at_us']
    live.clock(only(following['role_instances'], role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
    current = following
    assert capture.take('successor-backend', 'initial') == dict(physical, present=False)
    same(current, capture.snapshot('final'))
    assert capture.take('live-complete') == dict(event='live-complete', mode=PROBE, native='closed',
        control='released-and-joined', writer=False, admission=True)
    assert capture.position == len(events)
    same(current, after)
    assert recovery.native(native_after) == science.rows and native_after['sequence'] == 64
    assert inputs_after == science.inputs and len(inputs_after) == 5
    assert set(input_contents) == set(inputs_after) and input_contents[filename] == raw.hex()
    for name, value in input_contents.items():
        value = bytes.fromhex(value)
        assert inputs_after[name] == dict(bytes=len(value), sha256=live.digest(value))
    assert len(science.rows) == 53
    finite_backend(backend, released)
    assert math.isfinite(backend['absence']['seconds']) and 0 <= backend['absence']['seconds'] <= 120
    return science, capture.snapshots


def outcome_time(rows):
    return only(rows['producer_outcomes'], producer_epoch=2, request_sequence=1)['recorded_at_us']


def verify_seed(**context):
    science, count = seed_state(**context)
    return dict(status='PASS', snapshots=count, control_tables=72, native_rows=53, native_sequence=64,
        retained_inputs=5, issued=1, reserved=1, source_sequence=0, rejection='Signature',
        installed_import=False, network_provenance=False)


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed_context, setup, server_window, backend, predecessor=None):
    assert mode in (MODE, MODE + '-retry')
    window(server_window)
    science, _ = seed_state(**seed_context)
    assert seed_context['server_window']['after_us'] <= server_window['before_us']
    seed = dict(control=seed_context['after'])
    if mode == MODE:
        assert predecessor is None
        wanted = recovery.migrated(seed, setup, server_window['before_us'])
        assert native_before == seed_context['native_after']
    else:
        assert predecessor is not None and predecessor['mode'] == MODE
        verify(**predecessor)
        assert predecessor['seed_context'] == seed_context
        assert predecessor['setup'] == setup
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        wanted = predecessor['after']
        assert native_before == predecessor['native_after']
    assert setup['policy'] is None and setup['removed_copy'] is None
    assert setup['catalog_before'] == setup['catalog_after'] == setup['migration']['catalog_after']
    same(wanted, before)
    assert inputs_before == inputs_after == seed_context['inputs_after']
    assert input_contents == seed_context['input_contents']
    closed = dict(science.rows)
    key = ('receipts', b'\x01\x31' + science.slot + live.be(2))
    closed[key] = b'\x02\x31' + live.be(1) + live.be(0) * 2 + b'\x01'
    assert native(native_after) == closed and native_after['sequence'] == 65
    projected, control = control_delta(before, after, 'normal', server_window)
    wanted = copy.deepcopy(before)
    producer = only(wanted['producer_epochs'], producer_epoch=2)
    if mode == MODE:
        assert producer['state'] == 'open'
        tx = only(projected['producer_epochs'], producer_epoch=2)['xmin']
        fresh_xmin(tx, before)
        assert tx != control['xmin']
        producer.update(state='closing', xmin=tx)
    else:
        assert producer['state'] == 'closing'
    same(wanted, projected)
    report = dict(format='bigbrotr-ingest-recovery/1', state='activated-positive-outcomes-resolved',
        dataset_id=science.ds.hex(), owner_resource_id=science.plan['resource'], producer_slot_id=science.slot.hex(),
        producer_epoch='2', issued_through='1', reserved_through='1', resolved_requests='1',
        source_closure='not-performed', native='closed', owner_release='committed', ready=False)
    assert response['outcome'] == 'PROCESS_EXITED' and response['returncode'] == 0 and response['stderr'] == ''
    assert not any(k in response for k in ('output_limit_exceeded', 'orchestration_error', 'terminal_error', 'clock_error'))
    terminal = response['terminal']
    assert terminal['exit_code'] == 0 and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert math.isfinite(terminal['elapsed_seconds']) and 0 <= terminal['elapsed_seconds'] <= 50
    assert terminal['prefix'] == '/lab/' + mode
    assert terminal['command'] == ['/build/bigbrotr', 'ingest', 'recover', '--file', terminal['config'], '--activated-successor']
    def unique(pairs):
        assert len(pairs) == len(dict(pairs))
        return dict(pairs)
    lines = response['stdout'].splitlines()
    assert len(lines) == 1 and recovery.encoded(json.loads(lines[0], object_pairs_hook=unique)) == recovery.encoded(report)
    recovery.backend_checked(backend, control, response, server_window)
    previous = predecessor if predecessor is not None else seed_context
    old = previous['backend']['exact']['expected']
    assert backend['absence']['database'] == previous['backend']['absence']['database']
    assert (control['control_backend_pid'], control['control_backend_start']) != (old['pid'], old['backend_start'])
    assert previous['server_window']['after_us'] <= instant(control['control_backend_start'])
    if predecessor:
        prior = predecessor['response']['terminal']
        assert (terminal['pid'], terminal['start_ticks']) != (prior['pid'], prior['start_ticks'])
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72, native_rows=53,
        native_sequence=65, retained_inputs=5, issued=1, reserved=1, source_sequence=0,
        resolved_requests=1, new_native_writes=int(mode == MODE), source_closed=False, ready=False)


def seed_negative_controls(**context):
    verify_seed(**context)
    rejected = []
    def check(name, mutate):
        value = copy.deepcopy(context)
        mutate(value)
        value['response']['stdout'] = ''.join(json.dumps(e) + '\n' for e in value['events'])
        try:
            verify_seed(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError):
            rejected.append(name)
            return
        raise AssertionError('accepted source-zero counterexample: ' + name)
    def pg(value, stage, mutate):
        event = only(value['events'], event='successor-state', stage=stage)
        tables = {k: json.loads(v) for k, v in event['state']['control_tables']}
        mutate(tables)
        event['state']['control_tables'] = [[k, live.encoded(tables[k]).decode()] for k in sorted(tables)]
        event['state']['control_sha256'] = live.digest(live.encoded(event['state']['control_tables']))
    def raw(value, prefix):
        event = only(value['events'], event='successor-state', stage='rejected')
        row = next(r for r in event['rows'] if r['key'].startswith(prefix))
        row['value'] = row['value'][:-2] + ('00' if row['value'][-2:] != '00' else '01')
        event['state']['native_sha256'] = live.digest(live.encoded(event['rows']))
    check('new-source-record', lambda c: raw(c, '0102' + only(c['events'], event='successor-plan')['dataset'] + live.be(0, 4).hex() + live.be(2).hex()))
    check('rejection-reason', lambda c: only(c['events'], event='live-rejection').update(reason='EventId'))
    check('wrong-signature-input', lambda c: only(c['events'], event='live-rejected-input').update(supplied_signature=live.PUBLIC_SIG.hex()))
    check('wrong-preimage', lambda c: only(c['events'], event='live-rejected-input').update(preimage='00'))
    check('rejection-copy-kind', lambda c: pg(c, 'rejected', lambda p: only(p['producer_outcomes'], producer_epoch=2).update(outcome_kind=1)))
    check('old-science-mutated', lambda c: raw(c, '0120'))
    check('old-sql-xmin-mutated', lambda c: pg(c, 'final', lambda p: only(p['source_streams'], owner_epoch=1).update(xmin='99999999')))
    check('invented-source-closure', lambda c: pg(c, 'rejected', lambda p: only(p['source_streams'], owner_epoch=2).update(last_complete_sequence=0)))
    check('reservation-missing', lambda c: pg(c, 'rejected', lambda p: p['producer_requests'].pop()))
    check('backend-present', lambda c: c['backend']['exact'].update(present=True, stdout='t\n'))
    check('original-input-mutated', lambda c: c['input_contents'].update({next(iter(c['inputs'])): '00'}))
    check('native-extra-write', lambda c: c['native_after'].update(sequence=65))
    return dict(status='PASS', rejected=rejected, count=len(rejected))


def negative_controls(**context):
    verify(**context)
    rejected = []
    def check(name, mutate):
        value = copy.deepcopy(context)
        mutate(value)
        try:
            verify(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError):
            rejected.append(name)
            return
        raise AssertionError('accepted installed source-zero counterexample: ' + name)
    check('extra-native-write', lambda c: c['native_after'].update(sequence=66))
    check('barrier-not-durable', lambda c: c['native_after'].update(rows=c['seed_context']['native_after']['rows']))
    check('rejection-recorder-rewritten', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2).update(recorder_epoch=9))
    check('rejection-bytes-rewritten', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2).update(outcome_bytes='\\x00'))
    check('source-closed', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(state='closed'))
    check('lost-old-row', lambda c: c['after']['producer_outcomes'].pop(0))
    check('original-input-mutated', lambda c: c['input_contents'].update({next(iter(c['input_contents'])): '00'}))
    check('not-joined', lambda c: c['response']['terminal'].update(reaped=False))
    check('wrong-cli', lambda c: c['response']['terminal']['command'].__setitem__(-1, '--retire-empty-attempt'))
    check('backend-present', lambda c: c['backend']['exact'].update(present=True, stdout='t\n'))
    check('catalog-drift', lambda c: c['setup']['catalog_after']['triggers'].pop())
    return dict(status='PASS', rejected=rejected, count=len(rejected))
