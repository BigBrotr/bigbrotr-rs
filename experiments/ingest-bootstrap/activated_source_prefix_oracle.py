"""Three distinct incomplete-prefix refusals; no repair inferred from failure.

Reservation ambiguity is checked from the complete finite transcript. The two
operator faults preserve exact preimages, and every installed result compares
all 72 tables, original inputs and native rows. No product codec supplies an
expected receipt, source cut, or error classification.
"""
import copy
import json
import math

import activated_recovery_oracle as recovery
import activated_source_closure_oracle as closure
import activated_source_closure_faults_oracle as publication
import initial_live_oracle as live
from bootstrap_oracle import cbor
from activated_recovery_faults_oracle import (COUNTERS, PROXY_SHA256, backend_checked,
    failed_control)
from activated_source_durable_oracle import migrated_seed
from initial_live_routes_oracle import backend_checked as finite_backend
from initial_succession_oracle import claim, ended, only, prepared, renewed, same
from installed_empty_oracle import exclusion, fresh_identity, fresh_xmin, native, window
from installed_live_oracle import catalog_identity, instant

CASES = ('installed-source-reservation-tail', 'installed-source-retained-tail',
    'installed-source-copy-conflict')
PROBE = 'successor-live-reservation-after'
CLOSED_CONTEXT = '39f2342b25a073ee6494e1affc658e2a3b2459d507a709a703967d922828bef1'
TRUNCATE = """BEGIN;
SET LOCAL session_replication_role=replica;
WITH bindings AS (
 DELETE FROM control.producer_input_bindings WHERE producer_epoch=2 AND request_sequence=2
 RETURNING to_jsonb(producer_input_bindings)||jsonb_build_object('xmin',xmin::text) AS row
), outcomes AS (
 DELETE FROM control.producer_outcomes WHERE producer_epoch=2 AND request_sequence=2
 RETURNING to_jsonb(producer_outcomes)||jsonb_build_object('xmin',xmin::text) AS row
), requests AS (
 DELETE FROM control.producer_requests WHERE producer_epoch=2 AND request_sequence=2
 RETURNING to_jsonb(producer_requests)||jsonb_build_object('xmin',xmin::text) AS row
), producer AS (
 UPDATE control.producer_epochs SET reserved_through_request=1
 WHERE producer_epoch=2 AND reserved_through_request=2 AND state='open'
 RETURNING to_jsonb(producer_epochs)||jsonb_build_object('xmin',xmin::text) AS row
)
SELECT json_build_object('bindings',(SELECT coalesce(json_agg(b), '[]') FROM bindings b),
 'outcomes',(SELECT json_agg(o) FROM outcomes o), 'requests',(SELECT json_agg(r) FROM requests r),
 'producer',(SELECT json_agg(p) FROM producer p));
COMMIT;
"""
CONFLICT = """BEGIN;
SET LOCAL session_replication_role=replica;
UPDATE control.producer_outcomes
SET outcome_bytes=set_byte(outcome_bytes,octet_length(outcome_bytes)-1,get_byte(outcome_bytes,octet_length(outcome_bytes)-1)#1),
 outcome_sha256=sha256(set_byte(outcome_bytes,octet_length(outcome_bytes)-1,get_byte(outcome_bytes,octet_length(outcome_bytes)-1)#1))
WHERE producer_epoch=2 AND request_sequence=2
RETURNING (to_jsonb(producer_outcomes)||jsonb_build_object('xmin',xmin::text))::text;
COMMIT;
"""


def exact_state(value, control, raw, originals, contents):
    assert set(value) == {'control', 'native', 'inputs', 'contents'}
    same(value['control'], control)
    assert value['native'] == raw and value['inputs'] == originals and value['contents'] == contents


def wire_checked(wire):
    assert wire['status'] == 'PASS' and wire['proxy_sha256'] == PROXY_SHA256
    assert wire['exit_code'] == wire['wrapper_exit'] == 0
    assert wire['forced_wrapper_stop'] is False and wire['stderr_nonempty'] is False
    events = wire['events']
    assert [e['event'] for e in events] == ['ready', 'fault', 'receipt']
    assert events[0] == wire['ready'] and events[-1] == wire['receipt']
    elapsed = 0
    for index, event in enumerate(events):
        assert event['format'] == 'bigbrotr-pg-fault-proxy/1'
        assert event['mode'] == 'after-complete' and event['commit_number'] == 14
        assert event['held'] is False and event['hold_action'] is None
        assert type(event['elapsed_ns']) is int and elapsed <= event['elapsed_ns'] <= 35_000_000_000
        elapsed = event['elapsed_ns']
        assert all(type(event[k]) is int for k in COUNTERS)
        assert [event[k] for k in COUNTERS] == ([0] * 5 if not index else [14, 14, 14, 13, 1])
        if index:
            assert event['reason'] == 'after-complete'
    ready = events[0]
    assert ready['listen_host'] == '127.0.0.1' and ready['upstream_port'] == 5432
    assert type(ready['listen_port']) is int and 1 <= ready['listen_port'] <= 65535
    assert ready['listen_port'] != 5432 and ready['proxy_sha256'] == PROXY_SHA256
    assert ready['deadline_seconds'] == 30 and ready['max_frame_bytes'] == 1024 ** 2
    assert ready['max_wire_bytes'] == 64 * 1024 ** 2 and ready['max_messages'] == 20000
    assert type(ready['pid']) is int and ready['pid'] > 0
    final = events[-1]
    assert set(final['frames']) == set(final['tags']) == {'frontend', 'backend'}
    for direction, tags in final['tags'].items():
        assert all(len(k) == 2 and bytes.fromhex(k).hex() == k and type(n) is int and n > 0 for k, n in tags.items())
        assert final['frames'][direction] == sum(tags.values()) > 0
    assert sum(final['frames'].values()) <= ready['max_messages']
    assert 0 < final['declared_wire_bytes'] <= ready['max_wire_bytes']
    assert final['tags']['frontend']['51'] >= 14 and final['tags']['backend']['43'] >= 14


def seed_state(events, original, native_before, inputs, *, retired, server_window,
        response, wire, backend, after, native_after, inputs_after, input_contents,
        catalog_before, catalog_after):
    live.check_window(server_window)
    live.migrated(retired, original, native_before, inputs, server_window)
    catalog_identity(catalog_before)
    assert catalog_before == catalog_after
    assert response['returncode'] == 0 and response['stderr'] == ''
    assert 'orchestration_error' not in response
    assert type(response['seconds']) in (int, float) and math.isfinite(response['seconds']) and 0 <= response['seconds'] <= 120
    assert [json.loads(line) for line in response['stdout'].splitlines()] == events
    wire_checked(wire)
    observed = only(events, event='successor-plan')
    live.clock(observed['created_at_us'], server_window)
    plan, old, target = live.minted_plan(original, native_before, inputs, observed)
    cap = live.Capture(events, original, old, inputs)
    assert cap.take('successor-control', 'initial', 'claim-planned') == dict(event='successor-control',
        stage='initial', phase='claim-planned', role='d3' * 16, incarnation='dd' * 16, lease_ttl_us='20000000')
    assert plan['target_role'] != 'd3' * 16 and plan['target_incarnation'] != 'dd' * 16
    same(original, cap.snapshot('initial-before-claim'))
    acquired = cap.take('successor-control', 'initial', 'claimed')
    current = cap.snapshot('initial-after-claim')
    control = claim(original, current, 1)
    assert control['epoch'] == 8
    assert acquired == dict(event='successor-control', stage='initial', phase='claimed',
        resource=control['resource_id'][2:], epoch=8, role='d3' * 16, incarnation='dd' * 16)
    for at in (control['acquired_at_us'], instant(control['control_backend_start']),
            only(current['role_instances'], role_instance_id=control['role_instance_id'])['created_at_us']):
        live.clock(at, server_window)
    assert cap.take('successor-plan') == plan
    following = cap.snapshot('before-prepare')
    control = renewed(current, following, control, plan['created_at_us'])
    fresh_xmin(control['xmin'], current)
    live.clock(control['lease_expires_at_us'] - 20_000_000, server_window)
    current = following
    cap.native, cap.sequence = target, 62
    following = cap.snapshot('prepared')
    assert current[live.ACTIVATIONS] == following[live.ACTIVATIONS] == []
    prepared({k:v for k,v in current.items() if k != live.ACTIVATIONS},
        {k:v for k,v in following.items() if k != live.ACTIVATIONS}, plan, control)
    current = following
    physical = cap.take('successor-backend', 'initial')
    assert physical == dict(event='successor-backend', stage='initial', **backend['exact']['expected'], present=True)
    assert cap.take('live-activation-start') == dict(event='live-activation-start', mode=PROBE, control_epoch=8)
    event = cap.take('live-activation-result')
    assert event == dict(event='live-activation-result', writer=True, error=None, activated_at_us=event['activated_at_us'])
    following = cap.snapshot('activated-zero')
    live.activated(current, following, plan, control, server_window, event['activated_at_us'])
    current = following
    ds, slot = (bytes.fromhex(plan[k]) for k in ('dataset', 'producer_slot'))
    previous = bytes.fromhex(plan['previous_manifest'])
    scope = [ds, 4, previous[104 + 3 * 18 + 2:104 + 4 * 18], 3]
    lineage = [ds, 9, bytes.fromhex(dict(plan['domains'])[9]), 1]
    descriptor = [1, 'dataset-lineage', 1, dict(format='bigbrotr-signed-fixture',
        format_version=1, source_dataset_id=None, source_artifact_digest=live.CORPUS_DIGEST)]
    raw = b'bigbrotr/producer-request/1\0' + cbor([[slot, 2, 1], 'admit-evidence', 1,
        [1, scope, [['dictionary', 1, [lineage, *descriptor]]]]])
    assert cap.take('live-reservation-start') == dict(event='live-reservation-start', request=1,
        preimage=raw.hex(), request_digest=live.digest(raw), commit_number=14)
    assert cap.take('live-reservation-result') == dict(event='live-reservation-result', request=1,
        error='Reservation(CommitUnknown)', native_dispatch=False)
    filename = slot.hex() + '-0000000000000002-0000000000000001.cbor'
    cap.inputs = dict(inputs, **{filename:dict(bytes=len(raw), sha256=live.digest(raw))})
    following = cap.snapshot('reservation-unknown')
    row = only(following['producer_requests'], producer_epoch=2, request_sequence=1)
    tx, at = row['xmin'], row['reserved_at_us']
    fresh_xmin(tx, current); live.clock(at, server_window)
    assert event['activated_at_us'] <= at < min(control['lease_expires_at_us'], plan['lease_expires_at_us'])
    wanted = copy.deepcopy(current)
    only(wanted['producer_epochs'], producer_epoch=2).update(reserved_through_request=1, xmin=tx)
    wanted['producer_requests'].append(dict(producer_slot_id=live.bytea(slot), producer_epoch=2,
        request_sequence=1, dataset_id=live.bytea(ds), request_digest=live.bytea(live.sha(raw)),
        input_bytes=len(raw), input_profile=1, manifest_sha256=live.bytea(live.sha(bytes.fromhex(plan['target_manifest']))),
        issuer_resource=control['resource_id'], issuer_epoch=8, issuer_role='control',
        issuer_incarnation=control['process_incarnation'], reserved_at_us=at, relay_endpoint_id=None,
        relay_access_scope_id=None, relay_enrollment_generation=None, relay_route_id=None,
        acquisition_scope_id=None, acquisition_job_id=None, xmin=tx))
    same(wanted, following); current = following
    assert cap.take('successor-session-poisoned') == dict(event='successor-session-poisoned', control_epoch=8)
    same(current, cap.snapshot('unknown-before-control-end'))
    assert cap.take('successor-driver', 'unknown') == dict(event='successor-driver', stage='unknown', failed=True, joined=True)
    assert cap.take('successor-control', 'unknown', 'dropped-and-joined') == dict(event='successor-control',
        stage='unknown', phase='dropped-and-joined', resource=control['resource_id'][2:], epoch=8)
    same(current, cap.snapshot('unknown-after-control-end'))
    assert cap.take('successor-backend', 'initial') == dict(physical, present=False)
    same(current, cap.snapshot('final'))
    assert cap.take('live-complete') == dict(event='live-complete', mode=PROBE, native='closed',
        control='dropped-and-joined', writer=False, admission=False)
    assert cap.position == len(events)
    same(current, after)
    assert native(native_after) == target and native_after['sequence'] == 62 and len(target) == 52
    assert inputs_after == cap.inputs and len(inputs_after) == 5
    assert set(input_contents) == set(inputs_after) and input_contents[filename] == raw.hex()
    for name, value in input_contents.items():
        value = bytes.fromhex(value)
        assert inputs_after[name] == dict(bytes=len(value), sha256=live.digest(value))
    finite_backend(backend, control)
    assert math.isfinite(backend['absence']['seconds']) and 0 <= backend['absence']['seconds'] <= 120
    return dict(status='PASS', snapshots=cap.snapshots, native_rows=52, native_sequence=62,
        issued=0, reserved=1, source_cut=0, retained_inputs=5, native_dispatch=False, commit_number=14)


def failed_fresh_control(before, after, clock):
    """The failed finite owner's physical absence precedes a new fenced claim.

    ControlSession::acquire fences and retires the old identity and inserts the
    new identity in one transaction. Freshness is relative to the original
    capture, not an intermediate state containing those same transaction rows.
"""
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r:r['epoch'])
    assert previous['state'] == 'active'
    current = only(after['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
    fenced = only(after['owner_epochs'], role='control', epoch=previous['epoch'])
    old_role = only(before['role_instances'], role_instance_id=previous['role_instance_id'])
    retired = only(after['role_instances'], role_instance_id=previous['role_instance_id'])
    role = only(after['role_instances'], role_instance_id=current['role_instance_id'])
    fresh_xmin(fenced['xmin'], before)
    assert fenced['xmin'] == retired['xmin'] == current['xmin'] == role['xmin']
    for at in (fenced['released_at_us'], retired['retired_at_us']): live.clock(at, clock)
    assert fenced['released_at_us'] <= retired['retired_at_us'] <= current['acquired_at_us']
    assert retired['retired_at_us'] <= role['created_at_us']
    assert fenced == dict(previous, state='released', released_at_us=fenced['released_at_us'],
        exclusion_method='storage_fenced', exclusion_digest=exclusion(previous, current['process_incarnation']), xmin=fenced['xmin'])
    assert retired == dict(old_role, retired_at_us=retired['retired_at_us'], xmin=fenced['xmin'])
    for field in ('role_instance_id', 'process_incarnation'): fresh_identity(current[field])
    assert not any(r['role_instance_id'] == current['role_instance_id'] for r in before['role_instances'])
    assert not any(r['process_incarnation'] == current['process_incarnation'] for r in before['owner_epochs'])
    assert type(current['control_backend_pid']) is int and current['control_backend_pid'] > 0
    for at in (instant(current['control_backend_start']), role['created_at_us'], current['acquired_at_us']):
        live.clock(at, clock)
    assert instant(current['control_backend_start']) <= role['created_at_us'] <= current['acquired_at_us']
    assert current == dict(previous, epoch=previous['epoch'] + 1,
        role_instance_id=current['role_instance_id'], process_incarnation=current['process_incarnation'],
        acquired_at_us=current['acquired_at_us'], lease_expires_at_us=current['acquired_at_us'] + 20_000_000,
        state='active', released_at_us=None, exclusion_method=None, exclusion_digest=None,
        archive_frontier_protocol=None, control_fence_version=1, control_backend_pid=current['control_backend_pid'],
        control_backend_start=current['control_backend_start'], xmin=fenced['xmin'])
    assert role == dict(role_instance_id=current['role_instance_id'], dataset_id=previous['dataset_id'],
        role='control', created_at_us=role['created_at_us'], retired_at_us=None, xmin=fenced['xmin'])
    projected = copy.deepcopy(after)
    projected['owner_epochs'].remove(current)
    projected['role_instances'].remove(role)
    projected['owner_epochs'][projected['owner_epochs'].index(fenced)] = previous
    projected['role_instances'][projected['role_instances'].index(retired)] = old_role
    return projected, current


def mutation_checked(value, before, expected, original_native, originals, contents, mode):
    exact_state(value['preimage'], expected, original_native, originals, contents)
    wanted, raw = copy.deepcopy(expected), copy.deepcopy(original_native)
    operation = value['operation']
    assert operation['authority'] == 'privileged-isolated-fixture' and operation['product_transition'] is False
    answer = operation['response']
    assert answer['returncode'] == 0 and answer['stderr'] == '' and answer['outcome'] == 'PROCESS_EXITED'
    observed = json.loads(answer['stdout'])
    if mode == CASES[1]:
        assert operation['kind'] == 'retained-target-tail' and operation['statement'] == TRUNCATE
        slot = bytes.fromhex(only(wanted['producer_epochs'], producer_epoch=2)['producer_slot_id'][2:])
        receipt = ('receipts', b'\x01\x30' + slot + live.be(2) * 2)
        marker = ('receipts', b'\x01\x31' + slot + live.be(2))
        rows = native(raw)
        changes = [dict(cf=receipt[0], key=receipt[1].hex(), before=rows[receipt].hex(), after=None),
            dict(cf=marker[0], key=marker[1].hex(), before=rows[marker].hex(), after=(b'\x01\x31' + live.be(1) + bytes(16)).hex())]
        probe = operation['native_response']
        assert probe['returncode'] == 0 and probe['stderr'] == ''
        events = [json.loads(line) for line in probe['stdout'].splitlines()]
        assert events == operation['native_events'] and len(events) == 2
        for phase, count, seq in [('before', 70, 84), ('after', 69, 86)]:
            if phase == 'after':
                del rows[receipt]; rows[marker] = bytes.fromhex(changes[1]['after'])
            event = events[int(phase == 'after')]
            assert event == dict(event='activated-source-tail-corruption', phase=phase,
                mutation='retained-target-tail', changes=changes, rows=event['rows'], sequence=seq)
            assert live.native_rows(event['rows']) == rows and len(rows) == count
        raw.update(rows=events[-1]['rows'], sequence=86)
        exact_state(value['native_after'], expected, raw, originals, contents)
        removed = {}
        for field, table in [('bindings', 'producer_input_bindings'), ('outcomes', 'producer_outcomes'), ('requests', 'producer_requests')]:
            removed[field] = [r for r in wanted[table] if r['producer_epoch'] == 2 and r['request_sequence'] == 2]
            assert len(removed[field]) == (0 if field == 'bindings' else 1)
            wanted[table] = [r for r in wanted[table] if r not in removed[field]]
        producer = only(wanted['producer_epochs'], producer_epoch=2)
        tx = only(before['producer_epochs'], producer_epoch=2)['xmin']; fresh_xmin(tx, expected)
        producer.update(reserved_through_request=1, xmin=tx)
        # CTE RETURNING has one deliberately named JSON column per deleted row.
        assert observed == {**{k:[{'row':r} for r in v] for k,v in removed.items()}, 'producer':[{'row':producer}]}
    else:
        assert mode == CASES[2] and operation['kind'] == 'outcome-conflict' and operation['statement'] == CONFLICT
        outcome = only(wanted['producer_outcomes'], producer_epoch=2, request_sequence=2)
        altered = bytearray.fromhex(outcome['outcome_bytes'][2:]); altered[-1] ^= 1
        tx = only(before['producer_outcomes'], producer_epoch=2, request_sequence=2)['xmin']
        fresh_xmin(tx, expected)
        outcome.update(outcome_bytes='\\x' + altered.hex(), outcome_sha256='\\x' + recovery.sha(altered), xmin=tx)
        assert observed == outcome
    exact_state(value['after'], wanted, raw, originals, contents)
    same(wanted, before)
    return raw


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed, setup, server_window, backend, mutation, seed_context=None):
    assert mode in CASES and len(before) == len(after) == 72
    window(server_window)
    if mode == CASES[0]:
        assert seed is None and mutation is None
        seed_state(**seed_context)
        assert seed_context['server_window']['after_us'] <= server_window['before_us']
        expected = migrated_seed(dict(after=seed_context['after']), setup, server_window)
        closure.catalogs(setup, 'first')
        same(expected, before)
        assert native_before == seed_context['native_after']
        originals, contents = seed_context['inputs_after'], seed_context['input_contents']
        projected, control = failed_fresh_control(before, after, server_window)
        assert (control['control_backend_pid'], control['control_backend_start']) != (
            seed_context['backend']['exact']['expected']['pid'], seed_context['backend']['exact']['expected']['backend_start'])
        error = 'producer operation unavailable: Control(InvalidMetadata)'
    else:
        assert seed_context is None
        if mode == CASES[1]:
            closure.science(seed)
            assert seed['name'] == 'off'
            expected = closure.migrated(seed, setup, server_window)
            closure.catalogs(setup, 'first')
            original_native, originals, contents = seed['native'], seed['inputs'], seed['contents']
        else:
            assert recovery.sha(recovery.encoded(seed)) == CLOSED_CONTEXT
            publication.verify(**seed)
            expected, original_native = seed['after'], seed['native_after']
            originals, contents = seed['inputs_after'], seed['input_contents']
            reapply = setup['reapply']
            same(expected, reapply['before']); same(expected, reapply['after'])
            assert reapply['physical_before'] == reapply['physical_after'] and set(reapply['physical_before']) == set(expected)
            assert reapply['response']['returncode'] == 0 and reapply['response']['stderr'] == ''
            catalog = seed['setup']['catalog_after']
            assert reapply['catalog_before'] == reapply['catalog_after'] == setup['catalog_before'] == setup['catalog_after'] == catalog
            assert seed['server_window']['after_us'] <= server_window['before_us']
        raw = mutation_checked(mutation, before, expected, original_native, originals, contents, mode)
        assert native_before == raw
        projected, control = failed_control(before, after, 'outcome', server_window)
        error = ('initial dictionary batch failed: StateMismatch' if mode == CASES[1]
            else 'producer operation unavailable: Outcome(Conflict)')
    assert inputs_before == inputs_after == originals and input_contents == contents
    for name, raw in contents.items():
        raw = bytes.fromhex(raw)
        assert originals[name] == dict(bytes=len(raw), sha256=recovery.sha(raw))
    wanted, raw = copy.deepcopy(before), native(native_before)
    writes = int(mode == CASES[1])
    if writes:
        producer = only(wanted['producer_epochs'], producer_epoch=2)
        tx = only(projected['producer_epochs'], producer_epoch=2)['xmin']; fresh_xmin(tx, before)
        assert tx not in (control['xmin'], only(after['role_instances'], role_instance_id=control['role_instance_id'])['xmin'])
        producer.update(state='closing', xmin=tx)
        slot = bytes.fromhex(producer['producer_slot_id'][2:])
        raw['receipts', b'\x01\x31' + slot + live.be(2)] = b'\x02\x31' + live.be(1) + bytes(16) + b'\x01'
    same(wanted, projected)
    assert native(native_after) == raw and native_after['sequence'] == native_before['sequence'] + writes
    database = backend_checked(backend, control, server_window)
    if mode == CASES[0]:
        assert database == seed_context['backend']['exact']['expected']['database']
    assert response['terminal']['config'] == '/lab/' + database + '.toml'
    closure.response_checked(response, mode, {}, error)
    assert setup['diagnostic'] is None
    return dict(status='PASS', observed_states=2, control_tables=72, native_rows=len(raw),
        native_sequence=native_after['sequence'], native_writes=writes, retained_inputs=len(originals),
        error=error, source_unchanged=True, repaired_copies=0, installed_poison_telemetry=False)


def negative_controls(**context):
    verify(**context)
    checks = []
    def reject(name, edit):
        value = copy.deepcopy(context); edit(value)
        try: verify(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError): checks.append(name); return
        raise AssertionError('prefix counterexample accepted: ' + name)
    reject('native-extra-write', lambda c:c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    reject('wrong-refusal', lambda c:c['response'].update(stderr='error: producer operation unavailable: Control(Backend)\n'))
    reject('unjoined-process', lambda c:c['response']['terminal'].update(reaped=False))
    reject('backend-present', lambda c:c['backend']['exact'].update(present=True, stdout='t\n'))
    reject('source2-rewritten', lambda c:only(c['after']['source_streams'], owner_epoch=2).update(xmin='999999999'))
    reject('old-copy-rewritten', lambda c:only(c['after']['producer_outcomes'], producer_epoch=1, request_sequence=1).update(xmin='999999999'))
    reject('activation-reattributed', lambda c:only(c['after']['initial_ingest_activations']).update(activated_by_epoch=99))
    reject('input-bytes-corrupt', lambda c:c['input_contents'].update({next(iter(c['input_contents'])):'00'}))
    def change_fact(c):
        row = next(r for r in c['native_after']['rows'] if r['cf'] == 'facts')
        data = bytearray.fromhex(row['value']); data[-1] ^= 1; row['value'] = data.hex()
    reject('retained-fact-corrupt', change_fact)
    if context['mode'] == CASES[0]:
        reject('failed-owner-not-fenced', lambda c:only(c['after']['owner_epochs'], role='control', epoch=8).update(state='active'))
        def split_claim(c):
            current = only(c['after']['owner_epochs'], role='control', epoch=9)
            current['xmin'] = '999999998'
            only(c['after']['role_instances'], role_instance_id=current['role_instance_id'])['xmin'] = current['xmin']
        reject('fencing-and-claim-split-transactions', split_claim)
        reject('old-role-retired-separate-transaction', lambda c:only(c['after']['role_instances'],
            role_instance_id=only(c['before']['owner_epochs'], role='control', epoch=8)['role_instance_id']).update(xmin='999999997'))
        reject('reservation-lost', lambda c:c['after']['producer_requests'].remove(only(c['after']['producer_requests'], producer_epoch=2)))
        def other_database(c):
            db = 'bb_foreign_prefix'
            c['backend']['exact']['expected']['database'] = db
            c['backend']['absence']['database'] = db
            c['response']['terminal']['config'] = '/lab/' + db + '.toml'
            c['response']['terminal']['command'][4] = '/lab/' + db + '.toml'
        reject('coherent-foreign-database', other_database)
        reject('finite-orchestration-error', lambda c:c['seed_context']['response'].update(orchestration_error='RuntimeError'))
        def seed_event(c, event, update):
            s = c['seed_context']; update(only(s['events'], event=event))
            s['response']['stdout'] = ''.join(json.dumps(e) + '\n' for e in s['events'])
        reject('coherent-finite-driver-success', lambda c:seed_event(c, 'successor-driver', lambda e:e.update(failed=False)))
        reject('coherent-reservation-native-dispatch', lambda c:seed_event(c, 'live-reservation-result', lambda e:e.update(native_dispatch=True)))
        def delivered(c):
            w = c['seed_context']['wire']
            for e in w['events'][1:]: e['client_commit_complete'] = 14
            w['receipt'] = copy.deepcopy(w['events'][-1])
        reject('coherent-COMMIT14-delivered', delivered)
    else:
        reject('operator-authority-misclaimed', lambda c:c['mutation']['operation'].update(product_transition=True))
        reject('operator-preimage-corrupt', lambda c:only(c['mutation']['preimage']['control']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(policy_epoch=99))
        reject('operator-SQL-replaced', lambda c:c['mutation']['operation'].update(statement='SELECT 1;'))
        if context['mode'] == CASES[1]:
            reject('native-closing-absent', lambda c:c.update(native_after=copy.deepcopy(c['native_before'])))
            reject('source-prefix-laundered', lambda c:only(c['after']['source_streams'], owner_epoch=2).update(last_complete_sequence=1, state='closed'))
        else:
            reject('conflicting-copy-repaired', lambda c:only(c['after']['producer_outcomes'], producer_epoch=2, request_sequence=2).update(
                only(c['seed']['after']['producer_outcomes'], producer_epoch=2, request_sequence=2)))
    return dict(status='PASS', rejected=checks, count=len(checks))
