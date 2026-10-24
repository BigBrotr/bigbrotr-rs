"""Independent real-network AlreadyDurable seed and installed source2 closure.

Every new scientific byte is independently encoded from the frozen signed event
and original facts. Authentication identities and bounded clock/xmin scalars are
parameters, never substitutes for full SQL/native/input comparison. The finite
job stays queued and its browser session remains unrevoked.
"""
import copy
import json
import math
import re

import activated_recovery_oracle as recovery
import activated_source_closure_oracle as closure
import initial_live_oracle as live
from bootstrap_oracle import cbor
from initial_live_routes_oracle import backend_checked as finite_backend
from initial_succession_oracle import binary, claim, ended, only, prepared, renewed, same
from installed_empty_oracle import control_delta, fresh_xmin, native, window
from installed_live_oracle import catalog_identity, instant
from installed_sync_oracle import decode

MODE = 'installed-source-already-durable'
CASES = (MODE,)
PROBE = 'successor-live-already-durable'
ACTOR = '\\x' + '10' * 16
REQUEST_KEY = '\\x' + 'a6' * 16
SCOPE = '\\xa39723d7c49af2c99498fb2f59c03b16'
SIGNED = {'pubkey': 'f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9', 'created_at': 1, 'kind': 1, 'tags': [], 'content': 'import then network', 'id': '8a0096a3a84f1460c11a432355666cb9fd377cca00a5a1e08ef0071c9c0b9849', 'sig': 'ab9e62de93391621735c773f4b1c40f41029d7d49369993a63a226c4ce14605859b112dd261b5fa4f8a2b82bbac0a67323f39c4d007555711547bfd25560b4a6'}


def authenticated_job(before, after, report, control, clock, *, modern=False):
    """One actual login/accept/claim; no endpoint, scope or workflow mutation."""
    assert type(modern) is bool
    epoch=control['epoch'] if modern else 8
    scope='\\x422b7d8b33d4b55977806fb711e2d082' if modern else SCOPE
    refreshed = copy.deepcopy(before)
    changed = only(after['owner_epochs'], role='control', epoch=epoch)
    refreshed['owner_epochs'][refreshed['owner_epochs'].index(control)] = changed
    control = renewed(before, refreshed, control, report['accepted_at_us'])
    fresh_xmin(control['xmin'], before)
    refreshed_at = control['lease_expires_at_us'] - 20_000_000
    live.clock(refreshed_at, clock)
    wanted = copy.deepcopy(refreshed)
    ds = only(before['datasets'])['dataset_id']
    session = only(after['browser_sessions'])
    sid, stx, issued = session['session_id'], session['xmin'], session['issued_at_us']
    assert len(binary(sid)) == 16 and sid not in {r['session_id'] for r in before['browser_sessions']}
    assert all(r['revoked_at_us'] is not None or r['expires_at_us'] <= issued for r in before['browser_sessions'])
    fresh_xmin(stx, before); live.clock(issued, clock)
    activation = only(before[live.ACTIVATIONS])['activated_at_us']
    assert activation <= issued <= refreshed_at
    for key in ('token_digest', 'csrf_digest'):
        assert len(binary(session[key])) == 32 and any(binary(session[key]))
        assert session[key] not in {r[key] for r in before['browser_sessions']}
    assert session['token_digest'] != session['csrf_digest']
    wanted['browser_sessions'] = [dict(dataset_id=ds, session_id=sid, principal_id=ACTOR,
        credential_revision=1, authentication_revision=1, token_digest=session['token_digest'],
        csrf_digest=session['csrf_digest'], issued_at_us=issued, expires_at_us=issued + 28_800_000_000,
        idle_window_us=1_800_000_000, last_seen_at_us=issued, idle_expires_at_us=issued + 1_800_000_000,
        revoked_at_us=None, xmin=stx)]
    bucket = only(after['login_buckets'], kind='account')
    at, btx = bucket['window_started_at_us'], bucket['xmin']
    fresh_xmin(btx, before); live.clock(at, clock)
    assert activation <= at <= issued and btx != stx
    wanted['login_buckets'] = [dict(dataset_id=ds, kind=kind, bucket=n, window_started_at_us=at,
        attempts=1, xmin=btx) for kind, n in [('account', 97), ('peer', 13)]]

    def audit(action, subject, tx, at=None, aid=None):
        actual = only(after['management_audit'], action=action, subject_id=subject)
        value = actual['created_at_us'] if at is None else at
        assert len(binary(actual['audit_id'])) == 16
        assert actual['audit_id'] not in {r['audit_id'] for r in wanted['management_audit']}
        live.clock(value, clock)
        wanted['management_audit'].append(dict(dataset_id=ds, actor_id=ACTOR,
            audit_id=actual['audit_id'] if aid is None else aid, action=action, outcome='accepted',
            subject_id=subject, created_at_us=value, config_revision=1, policy_revision=1,
            reason=None, requester_database_role=None, requester_os_uid=None, xmin=tx))
        return value

    assert issued <= audit('browser.session.issue', sid, stx) <= refreshed_at
    command = only(after['management_commands'], actor_id=ACTOR, request_key=REQUEST_KEY)
    tx, at = command['xmin'], command['accepted_at_us']
    fresh_xmin(tx, before); live.clock(at, clock)
    assert refreshed_at <= at < control['lease_expires_at_us']
    digest = live.sha(b'bigbrotr/acquisition-job/1\0' + binary(ds) + binary(ACTOR)
        + binary(REQUEST_KEY) + binary(scope) + live.be(30_000_000) + live.be(1) * 2)
    wanted['management_commands'].append(dict(dataset_id=ds, actor_id=ACTOR, request_key=REQUEST_KEY,
        request_digest=live.bytea(digest), action='job.acquisition.accept', accepted_at_us=at,
        expected_config_revision=1, policy_revision=1, audit_id=command['audit_id'],
        authority_mode=None, document_digest=None, xmin=tx))
    audit('job.acquisition.accept', REQUEST_KEY, tx, at, command['audit_id'])
    job = only(after['jobs'], request_key=REQUEST_KEY)
    assert len(binary(job['job_id'])) == 16 and job['job_id'] not in {r['job_id'] for r in before['jobs']}
    fresh_xmin(job['xmin'], before)
    assert len({btx, stx, tx, job['xmin'], control['xmin']}) == 5
    owner = dict(owner_resource_id=control['resource_id'], owner_epoch=epoch,
        owner_role='control', owner_incarnation=control['process_incarnation'])
    wanted['jobs'].append(dict(only(before['jobs'], kind='acquisition'), job_id=job['job_id'],
        request_key=REQUEST_KEY, request_digest=live.bytea(digest), state='queued', job_revision=2,
        config_revision=1, policy_revision=1, created_at_us=at, completed_at_us=None,
        reason_code=None, xmin=job['xmin'], **owner))
    wanted['job_admissions'].append(dict(job_id=job['job_id'],
        admission_ordinal=max(r['admission_ordinal'] for r in before['job_admissions']) + 1, xmin=tx))
    wanted['acquisition_jobs'].append(dict(only(before['acquisition_jobs']), job_id=job['job_id'],
        request_key=REQUEST_KEY, scope_id=scope, duration_us=30_000_000, deadline_at_us=at + 30_000_000,
        session_id=sid, actor_credential_revision=1, actor_authentication_revision=1, actor_grant_revision=1 if modern else 4, xmin=tx))
    assert report == dict(event='live-durable-job', job=job['job_id'][2:], scope=scope[2:],
        accepted_at_us=at, deadline_at_us=at + 30_000_000, control_epoch=epoch, request_key=REQUEST_KEY[2:],
        provenance=True, existing_dictionaries=True)
    same(wanted, after)
    return control


def durable_science(plan, prepared_rows, inputs, contents, report, receive_clock, clock):
    ds, slot = (bytes.fromhex(plan[k]) for k in ('dataset', 'producer_slot'))
    assert set(receive_clock) == {'clock', 'resolution_ns', 'resolution_us'}
    ns, resolution = receive_clock['resolution_ns'], receive_clock['resolution_us']
    assert receive_clock['clock'] == 'CLOCK_REALTIME'
    assert type(ns) is int and 0 < ns <= 1_000_000_000
    assert type(resolution) is int and resolution == (ns + 999) // 1000
    oldname = slot.hex() + '-0000000000000001-0000000000000004.cbor'
    old = bytes.fromhex(contents[oldname])
    assert live.digest(old) == '19820e2cea792c8780de0d8542baa76dabbdb611ab96237eed2230a228d82977'
    original = decode(old.removeprefix(b'bigbrotr/producer-request/1\0'))
    scope, signed, observation = original[3][1], original[3][2], original[3][5]
    body = [bytes.fromhex(SIGNED[k]) for k in ('id', 'pubkey')]
    body += [1, 1, [], 'import then network', bytes.fromhex(SIGNED['sig'])]
    assert original == [[slot, 1, 4], 'admit', 1, [1, scope, body, 'network', None, observation]]
    previous = bytes.fromhex(plan['previous_manifest'])
    assert scope == [ds, 4, previous[104 + 3 * 18 + 2:104 + 4 * 18], 3]
    at = report['received_at_us']; live.clock(at, clock)
    assert at > observation[0] == report['predecessor_received_at_us']
    sample = [at, observation[1], [observation[2][0], resolution, None, 4 if resolution > 1 else 0], observation[3]]
    raw = b'bigbrotr/producer-request/1\0' + cbor([[slot, 2, 1], 'admit', 1,
        [1, scope, body, 'network', None, sample]])
    refs = []
    for n in range(3, 13):
        value = prepared_rows['facts', b'\x01\x20' + ds + live.be(0, 4) + live.be(1) + live.be(n)]
        assert value[:2] == b'\x01\x20' and live.sha(value[34:]) == value[2:34]
        refs.append([[ds, 0, 1, n], value[2:34]])
    receipt = b'bigbrotr/receipt/1\0' + cbor([2, [slot, 2, 1], live.sha(raw),
        [ds, 1, 1, bytes.fromhex(plan['recovery_generation']), 1], None, refs])
    wanted = dict(prepared_rows)
    wanted['receipts', b'\x01\x30' + slot + live.be(2) + live.be(1)] = receipt
    wanted['receipts', b'\x01\x31' + slot + live.be(2)] = b'\x01\x31' + live.be(1) + live.be(0) * 2
    filename = slot.hex() + '-0000000000000002-0000000000000001.cbor'
    originals = dict(inputs)
    originals[filename] = dict(bytes=len(raw), sha256=live.digest(raw))
    assert report == dict(event='live-durable-input', request=1, event_id=SIGNED['id'],
        preimage=raw.hex(), request_digest=live.digest(raw), provenance=True, wire_sequence=1,
        wire_request=report['wire_request'], received_at_us=at, predecessor_received_at_us=observation[0])
    digest = live.digest(b'bigbrotr/activated-initial-source-closure/1\0'
        + live.sha(bytes.fromhex(plan['handoff'])) + live.sha(bytes.fromhex(plan['target_manifest']))
        + slot + live.be(2) + live.be(1) + live.be(1) + live.sha(raw) + live.sha(receipt) + live.be(0))
    return dict(plan=plan, ds=ds, slot=slot, rows=wanted, inputs=originals, digest=digest), filename, raw, receipt


def admitted(before, after, science, raw, receipt, control, report, clock, *, modern=False):
    assert type(modern) is bool
    epoch=control['epoch'] if modern else 8
    scope_id='\\x422b7d8b33d4b55977806fb711e2d082' if modern else SCOPE
    request = only(after['producer_requests'], producer_epoch=2, request_sequence=1)
    outcome = only(after['producer_outcomes'], producer_epoch=2, request_sequence=1)
    fresh_xmin(request['xmin'], before); fresh_xmin(outcome['xmin'], before)
    assert request['xmin'] != outcome['xmin']
    for at in (request['reserved_at_us'], outcome['recorded_at_us']):
        live.clock(at, clock)
    job = only(before['acquisition_jobs'], request_key=REQUEST_KEY)
    assert report['received_at_us'] <= request['reserved_at_us'] <= outcome['recorded_at_us']
    assert outcome['recorded_at_us'] < min(control['lease_expires_at_us'],
        science['plan']['lease_expires_at_us'], job['deadline_at_us'])
    scope = only(before['acquisition_scopes'], scope_id=scope_id)
    enrollment = only(before['relay_enrollments'], endpoint_id=scope['endpoint_id'], access_scope_id=scope['access_scope_id'])
    identity = dict(producer_slot_id=live.bytea(science['slot']), producer_epoch=2, request_sequence=1,
        dataset_id=live.bytea(science['ds']), request_digest=live.bytea(live.sha(raw)),
        manifest_sha256=live.bytea(live.sha(bytes.fromhex(science['plan']['target_manifest']))))
    wanted = copy.deepcopy(before)
    only(wanted['producer_epochs'], producer_epoch=2).update(reserved_through_request=1, xmin=request['xmin'])
    wanted['producer_requests'].append(dict(identity, input_bytes=len(raw), input_profile=1,
        issuer_resource=control['resource_id'], issuer_epoch=epoch, issuer_role='control',
        issuer_incarnation=control['process_incarnation'], reserved_at_us=request['reserved_at_us'],
        relay_endpoint_id=scope['endpoint_id'], relay_access_scope_id=scope['access_scope_id'],
        relay_enrollment_generation=enrollment['generation'], relay_route_id=enrollment['route_id'],
        acquisition_scope_id=scope_id, acquisition_job_id=job['job_id'], xmin=request['xmin'],
        **(dict(progress_event_ordinal=None,progress_input_sha256=None,progress_request_id=None,progress_wire_sequence=None) if modern else {})))
    wanted['producer_outcomes'].append(dict(identity, outcome_kind=1 if modern else 2, policy_epoch=1,
        outcome_bytes=live.bytea(receipt), outcome_sha256=live.bytea(live.sha(receipt)),
        recorder_resource=control['resource_id'], recorder_epoch=epoch, recorder_role='control',
        recorder_incarnation=control['process_incarnation'], recorded_at_us=outcome['recorded_at_us'], xmin=outcome['xmin']))
    same(wanted, after)

def seed_state(events, original, native, inputs, *, retired, server_window, backend, response,
        after, native_after, inputs_after, input_contents, catalog_before, catalog_after, receive_clock):
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
    job = capture.take('live-durable-job')
    following = capture.snapshot('durable-job-claimed')
    control = authenticated_job(current, following, job, control, server_window)
    current = following
    wire = capture.take('live-durable-wire')
    subscription = wire['request'][1]
    assert re.fullmatch(r'[0-9a-f]{32}0000000000000001', subscription)
    assert int(subscription[:32], 16) != 0
    assert wire == dict(event='live-durable-wire', request=['REQ', subscription, dict(kinds=[1], limit=1)],
        signed_event=SIGNED, events=1, eose=True)
    assert capture.take('live-durable-peer-closed') == dict(event='live-durable-peer-closed', eof=True)
    same(current, capture.snapshot('durable-received'))
    observation = capture.take('live-durable-input')
    assert observation['wire_request'] == subscription
    assert job['accepted_at_us'] <= observation['received_at_us'] < job['deadline_at_us']
    science, filename, raw, receipt = durable_science(plan, prepared_rows, inputs, input_contents,
        observation, receive_clock, server_window)
    capture.native, capture.sequence, capture.inputs = science['rows'], 64, science['inputs']
    following = capture.snapshot('already-durable')
    admitted(current, following, science, raw, receipt, control, observation, server_window)
    current = following
    assert capture.take('live-already-durable') == dict(event='live-already-durable', request=1,
        receipt=receipt.hex(), dependencies=10, source_cut=0, peer_joined=True, job_complete=False, session_revoked=False)
    same(current, capture.snapshot('final-before-control-end'))
    assert capture.take('successor-control', 'final', 'released-and-joined') == dict(event='successor-control',
        stage='final', phase='released-and-joined', resource=control['resource_id'][2:], epoch=8)
    following = capture.snapshot('final-after-control-end')
    ended(current, following, control, True)
    released = only(following['owner_epochs'], role='control', epoch=8)
    fresh_xmin(released['xmin'], current)
    live.clock(released['released_at_us'], server_window)
    assert only(current['producer_outcomes'], producer_epoch=2)['recorded_at_us'] <= released['released_at_us']
    live.clock(only(following['role_instances'], role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
    current = following
    assert capture.take('successor-backend', 'initial') == dict(physical, present=False)
    same(current, capture.snapshot('final'))
    assert capture.take('live-complete') == dict(event='live-complete', mode=PROBE, native='closed',
        control='released-and-joined', writer=False, admission=True)
    assert capture.position == len(events)
    same(current, after)
    assert recovery.native(native_after) == science['rows'] and native_after['sequence'] == 64
    assert inputs_after == science['inputs'] and len(inputs_after) == 5
    assert set(input_contents) == set(inputs_after) and input_contents[filename] == raw.hex()
    for name, value in input_contents.items():
        value = bytes.fromhex(value)
        assert inputs_after[name] == dict(bytes=len(value), sha256=live.digest(value))
    assert len(science['rows']) == 53
    finite_backend(backend, released)
    assert math.isfinite(backend['absence']['seconds']) and 0 <= backend['absence']['seconds'] <= 120
    return science, capture.snapshots


def verify_seed(**context):
    science, count = seed_state(**context)
    return dict(status='PASS', snapshots=count, control_tables=72, native_rows=53, native_sequence=64,
        retained_inputs=5, issued=1, reserved=1, source_sequence=0, outcome='AlreadyDurable',
        source_closure_sha256=science['digest'], actual_network=True, job_state='queued',
        job_complete=False, session_revoked=False, installed_scheduler=False)


def migrated_seed(seed_context, setup, clock):
    wanted = recovery.migrated(dict(control=seed_context['after']), setup, clock['before_us'])
    upgrade = setup['source_upgrade']
    same(wanted, upgrade['before'])
    assert setup['source_migration_sha256'] == closure.MIGRATION
    added = only(upgrade['after']['schema_versions'], migration=35)
    fresh_xmin(added['xmin'], wanted)
    assert type(added['applied_at_us']) is int
    assert max(r['applied_at_us'] for r in wanted['schema_versions']) <= added['applied_at_us'] <= clock['before_us']
    assert added == dict(only(wanted['schema_versions'], migration=34), migration=35,
        name='0035_activated_source_closure.sql', checksum='\\x' + closure.MIGRATION,
        applied_at_us=added['applied_at_us'], xmin=added['xmin'])
    wanted['schema_versions'].append(added)
    same(wanted, upgrade['after'])
    assert upgrade['after'] == upgrade['reapplied']
    assert upgrade['physical_before'] == upgrade['physical_after'] and set(upgrade['physical_before']) == set(wanted)
    assert len(upgrade['responses']) == 2
    assert all(r['returncode'] == 0 and r['stderr'] == '' for r in upgrade['responses'])
    closure.catalog34(upgrade['catalog_before'])
    old, new = upgrade['catalog_before'], upgrade['catalog_ungranted']
    assert set(old) == set(new) and all(old[k] == new[k] for k in old if k != 'functions')
    expected = {r['name']: r for r in old['functions']}
    actual = {r['name']: r for r in new['functions']}
    assert len(expected) == len(old['functions']) == 15
    assert len(actual) == len(new['functions']) == 16 and actual.keys() == expected.keys() | {closure.SOURCE_FUNCTION}
    for name, value in actual.items():
        if name in closure.FUNCTIONS:
            assert set(value) == {'name', 'definition', 'execute'}
            assert live.digest(value['definition'].encode()) == closure.FUNCTIONS[name]
            assert value['execute'] is (name != closure.SOURCE_FUNCTION)
        else:
            assert value == expected[name]
    return wanted


def verify(before, after, native_before, native_after, inputs_before, inputs_after, input_contents,
        response, mode, *, seed_context, setup, server_window, backend, predecessor=None):
    assert mode in (MODE, MODE + '-retry')
    window(server_window)
    science, _ = seed_state(**seed_context)
    assert seed_context['server_window']['after_us'] <= server_window['before_us']
    if mode == MODE:
        assert predecessor is None
        wanted = migrated_seed(seed_context, setup, server_window)
        assert native_before == seed_context['native_after']
    else:
        assert predecessor is not None and predecessor['mode'] == MODE
        verify(**predecessor)
        assert predecessor['seed_context'] == seed_context and predecessor['setup'] == setup
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        wanted = predecessor['after']
        assert native_before == predecessor['native_after']
    closure.catalogs(setup, 'first')
    same(wanted, before)
    assert inputs_before == inputs_after == seed_context['inputs_after']
    assert input_contents == seed_context['input_contents']
    closed = dict(science['rows'])
    key = ('receipts', b'\x01\x31' + science['slot'] + live.be(2))
    closed[key] = b'\x02\x31' + live.be(1) + live.be(0) * 2 + b'\x01'
    assert native(native_after) == closed and native_after['sequence'] == 65
    projected, control = control_delta(before, after, 'normal', server_window)
    wanted = copy.deepcopy(before)
    producer = only(wanted['producer_epochs'], producer_epoch=2)
    source = only(wanted['source_streams'], owner_epoch=2)
    if mode == MODE:
        assert producer['state'] == source['state'] == 'open'
        tx = only(projected['producer_epochs'], producer_epoch=2)['xmin']
        fresh_xmin(tx, before)
        assert tx != control['xmin']
        producer.update(state='closing', xmin=tx)
        changed = only(projected['source_streams'], owner_epoch=2)
        live.clock(changed['closed_at_us'], server_window)
        assert control['lease_expires_at_us'] - 20_000_000 <= changed['closed_at_us'] <= control['released_at_us']
        fresh_xmin(changed['xmin'], before)
        assert changed['xmin'] not in (control['xmin'], producer['xmin'])
        source.update(state='closed', last_complete_sequence=0, closure_digest='\\x' + science['digest'],
            closed_at_us=changed['closed_at_us'], xmin=changed['xmin'])
    else:
        assert producer['state'] == 'closing' and source['state'] == 'closed'
    same(wanted, projected)
    report = dict(format='bigbrotr-ingest-recovery/1', state='activated-source-closed',
        dataset_id=science['ds'].hex(), owner_resource_id=science['plan']['resource'],
        producer_slot_id=science['slot'].hex(), producer_epoch='2', issued_through='1',
        reserved_through='1', resolved_requests='1', source_closure=dict(last_complete_sequence='0', sha256=science['digest']),
        native='closed', owner_release='committed', ready=False)
    closure.response_checked(response, mode, report, None)
    recovery.backend_checked(backend, control, response, server_window)
    previous = predecessor if predecessor is not None else seed_context
    old = previous['backend']['exact']['expected']
    assert backend['absence']['database'] == previous['backend']['absence']['database']
    assert (control['control_backend_pid'], control['control_backend_start']) != (old['pid'], old['backend_start'])
    assert previous['server_window']['after_us'] <= instant(control['control_backend_start'])
    if predecessor:
        prior = predecessor['response']['terminal']
        terminal = response['terminal']
        assert (terminal['pid'], terminal['start_ticks']) != (prior['pid'], prior['start_ticks'])
    return dict(status='PASS', mode=mode, observed_states=2, control_tables=72, native_rows=53,
        native_sequence=65, retained_inputs=5, issued=1, reserved=1, source_sequence=0,
        source_closure_sha256=science['digest'], resolved_requests=1, new_native_writes=int(mode == MODE),
        source_closed=True, job_state='queued', job_complete=False, session_revoked=False, ready=False)


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
        raise AssertionError('accepted durable seed counterexample: ' + name)
    def pg(value, stage, mutate):
        event = only(value['events'], event='successor-state', stage=stage)
        tables = {k: json.loads(v) for k, v in event['state']['control_tables']}
        mutate(tables)
        event['state']['control_tables'] = [[k, live.encoded(tables[k]).decode()] for k in sorted(tables)]
        event['state']['control_sha256'] = live.digest(live.encoded(event['state']['control_tables']))
    def row(value, prefix):
        event = only(value['events'], event='successor-state', stage='already-durable')
        raw = next(r for r in event['rows'] if r['key'].startswith(prefix))
        raw['value'] = raw['value'][:-2] + ('00' if raw['value'][-2:] != '00' else '01')
        event['state']['native_sha256'] = live.digest(live.encoded(event['rows']))
    check('new-source-fact', lambda c: row(c, '0102' + only(c['events'], event='successor-plan')['dataset'] + live.be(0, 4).hex() + live.be(2).hex()))
    check('original-science-mutated', lambda c: row(c, '0120'))
    check('invented-batch', lambda c: only(c['events'], event='live-already-durable').update(dependencies=11))
    check('wrong-wire-event', lambda c: only(c['events'], event='live-durable-wire')['signed_event'].update(content='different'))
    check('unjoined-peer', lambda c: only(c['events'], event='live-durable-peer-closed').update(eof=False))
    check('wrong-preimage', lambda c: only(c['events'], event='live-durable-input').update(preimage='00'))
    check('wrong-clock-resolution', lambda c: c['receive_clock'].update(resolution_us=c['receive_clock']['resolution_us'] + 1))
    check('job-completed', lambda c: pg(c, 'already-durable', lambda p: only(p['jobs'], request_key=REQUEST_KEY).update(state='succeeded')))
    check('job-adopted-old-control', lambda c: pg(c, 'durable-job-claimed', lambda p: only(p['jobs'], request_key=REQUEST_KEY).update(owner_epoch=5)))
    check('session-revoked', lambda c: pg(c, 'final', lambda p: only(p['browser_sessions']).update(revoked_at_us=c['server_window']['after_us'])))
    check('unbound-acquisition', lambda c: pg(c, 'already-durable', lambda p: only(p['producer_requests'], producer_epoch=2).update(acquisition_job_id=None)))
    check('outcome-wrong-kind', lambda c: pg(c, 'already-durable', lambda p: only(p['producer_outcomes'], producer_epoch=2).update(outcome_kind=1)))
    check('old-sql-xmin-mutated', lambda c: pg(c, 'final', lambda p: only(p['source_streams'], owner_epoch=1).update(xmin='99999999')))
    check('backend-present', lambda c: c['backend']['exact'].update(present=True, stdout='t\n'))
    check('original-input-mutated', lambda c: c['input_contents'].update({next(iter(c['inputs'])): '00'}))
    check('extra-native-write', lambda c: c['native_after'].update(sequence=65))
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
        raise AssertionError('accepted durable source closure counterexample: ' + name)
    check('extra-native-write', lambda c: c['native_after'].update(sequence=66))
    check('barrier-not-durable', lambda c: c['native_after'].update(rows=c['seed_context']['native_after']['rows']))
    check('outcome-recorder-rewritten', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2).update(recorder_epoch=9))
    check('receipt-rewritten', lambda c: only(c['after']['producer_outcomes'], producer_epoch=2).update(outcome_bytes='\\x00'))
    check('wrong-source-cut', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(last_complete_sequence=1))
    check('wrong-closure-digest', lambda c: only(c['after']['source_streams'], owner_epoch=2).update(closure_digest='\\x' + '00' * 32))
    check('old-row-lost', lambda c: c['after']['producer_outcomes'].pop(0))
    check('job-closed-with-source', lambda c: only(c['after']['jobs'], request_key=REQUEST_KEY).update(state='cancelled'))
    check('session-revoked-with-source', lambda c: only(c['after']['browser_sessions']).update(revoked_at_us=c['server_window']['after_us']))
    check('original-input-mutated', lambda c: c['input_contents'].update({next(iter(c['input_contents'])): '00'}))
    check('not-joined', lambda c: c['response']['terminal'].update(reaped=False))
    check('wrong-cli', lambda c: c['response']['terminal']['command'].__setitem__(-1, '--activated-successor'))
    check('backend-present', lambda c: c['backend']['exact'].update(present=True, stdout='t\n'))
    check('catalog-drift', lambda c: c['setup']['catalog_after']['triggers'].pop())
    return dict(status='PASS', rejected=rejected, count=len(rejected))
