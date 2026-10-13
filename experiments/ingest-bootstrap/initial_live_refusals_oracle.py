"""Independent observations of first-manifest and live renewal refusals.

Every raw snapshot is verified in its actual order. A locally delivered writer
does not permit a different Control to renew it, and an unknown COMMIT is never
reported as rollback. This finite import fixture supplies no network provenance,
installed consumer, replacement-process adoption or generic restart evidence.
"""
import copy
from datetime import datetime, timezone
import json
import re

import initial_live_oracle as live
import initial_live_guards_oracle as guards
import initial_live_routes_oracle as routes


CASES = ('successor-live-manifest', 'successor-live-renew-control',
    'successor-live-renew-expired', 'successor-live-renew-before', 'successor-live-renew-after')
MANIFEST_CASES = ('producer-request-wrong-manifest', 'input-binding-wrong-manifest')
MANIFEST_ERROR = 'request requires its activated target manifest and actual activating Control'
EXPIRED_ERROR = 'active initial writer scheduling authority expired'
# Independently reviewed invoker SQL: request3 is unbound, binding4 reaches the
# early manifest veto without claiming an eligible running import job.
TEMPLATE = 'fd95ff08a9c11e98b2057a8b3aad46283486d3db193d3bb8ed2a4952111f9c0f'
TRIGGER = dict(table='owner_epochs', name='live_manifest_fixture_refresh', enabled='O',
    internal=False, function='live_manifest_fixture.exercise()')
REMOVE = ("BEGIN; SET LOCAL lock_timeout='5s'; "
    'DROP TRIGGER live_manifest_fixture_refresh ON control.owner_epochs; '
    'DROP FUNCTION live_manifest_fixture.exercise(); COMMIT;')


def manifest_sql(fixture, control):
    statement = fixture['install_sql']
    assert fixture['install_sha256'] == live.digest(statement.encode())
    normalized = statement
    for value, placeholder in {
        ','.join("'" + n + "'" for n in MANIFEST_CASES): '__NAMES__',
        MANIFEST_ERROR: '__ERROR__', str(guards.EVENT_BYTES): '__EVENT_BYTES__',
        guards.EVENT_SHA: '__EVENT_SHA__',
        "decode('" + control['resource_id'][2:] + "','hex')": '__RESOURCE__',
        "decode('" + control['process_incarnation'][2:] + "','hex')": '__INCARNATION__',
    }.items():
        assert value in normalized
        normalized = normalized.replace(value, placeholder)
    assert live.digest(normalized.encode()) == TEMPLATE, 'unreviewed manifest fixture SQL'
    assert fixture['remove_sql'] == REMOVE
    function = fixture['fixture_function']
    body = statement.split('$fixture$')
    assert len(body) == 3
    header = ("CREATE OR REPLACE FUNCTION live_manifest_fixture.exercise()\n"
        " RETURNS trigger\n LANGUAGE plpgsql\n SET search_path TO 'pg_catalog'\n")
    assert function == dict(security_definer=False,
        definition=header + 'AS $function$' + body[1] + '$function$\n')


def attempted_request(pg, control, plan, at, index):
    if index == 1:
        old = live.only(pg['producer_requests'], producer_epoch=1, request_sequence=3)
        source_fields = ('relay_endpoint_id', 'relay_access_scope_id', 'relay_enrollment_generation',
            'relay_route_id', 'acquisition_scope_id', 'acquisition_job_id')
        assert all(old[f] is None for f in source_fields)
        assert old['input_bytes'] == 814
        assert old['request_digest'] == '\\x08c9b935c7c101c704f7c741847f26baad8063438fea836aff1678e4070240e5'
        result = {k: v for k, v in old.items() if k != 'xmin'}
        result.update(producer_epoch=2, request_sequence=1, issuer_resource=control['resource_id'],
            issuer_epoch=8, issuer_incarnation=control['process_incarnation'], reserved_at_us=at)
        return result
    old = live.only(pg['producer_requests'], producer_epoch=1, request_sequence=4)
    job = live.only(pg['jobs'], job_id=old['acquisition_job_id'])
    assert job['dataset_id'] == '\\x' + plan['dataset']
    return dict(job_id=job['job_id'], dataset_id=job['dataset_id'],
        source_namespace=live.bytea(live.sha(b'live-manifest-source/1')), shard_id=0,
        source_bytes=guards.EVENT_BYTES, source_sha256='\\x' + guards.EVENT_SHA,
        parser_profile=1, batch_first_record=1, batch_records=1, obligation_index=0,
        obligation_kind=1, logical_record=1, byte_start=0, byte_end=guards.EVENT_BYTES,
        record_sha256='\\x' + guards.EVENT_SHA, producer_slot_id='\\x' + plan['producer_slot'],
        producer_epoch=2, request_sequence=1, request_digest=old['request_digest'],
        input_bytes=old['input_bytes'], input_profile=1,
        manifest_sha256=live.bytea(live.sha(bytes.fromhex(plan['previous_manifest']))),
        issuer_resource=control['resource_id'], issuer_epoch=8, issuer_incarnation=control['process_incarnation'])


def manifest(capture, pg, control, plan, receipt, fixture, backend, window):
    raw = fixture['manifest']
    assert raw['status'] == 'CAPTURED' and raw['forced_wrapper_stop'] is False
    assert raw['cleanup_errors'] == [] and raw['control'] == control
    live.same(pg, raw['before'])
    manifest_sql(raw, control)
    assert raw['catalog_before'] == raw['catalog_removed'] == fixture['catalog_before']
    expected_catalog = copy.deepcopy(raw['catalog_before'])
    assert not any(t['name'] == TRIGGER['name'] for t in expected_catalog['triggers'])
    expected_catalog['triggers'].append(TRIGGER)
    expected_catalog['triggers'].sort(key=lambda t: (t['table'], t['name']))
    assert raw['catalog_installed'] == expected_catalog
    gates = raw['gates']
    assert [(g['gate'], g['phase']) for g in gates] == [
        ('manifest-install', 'waiting'), ('manifest-install', 'released'),
        ('manifest-remove', 'waiting'), ('manifest-remove', 'released')]
    for g in gates:
        assert set(g) == {'gate', 'phase', 'server_us'}
        live.clock(g['server_us'], window)
    moments = [raw[k] for k in ('install_before_us', 'install_after_us', 'remove_before_us', 'remove_after_us')]
    for moment in moments:
        live.clock(moment, window)
    assert moments == sorted(moments)
    assert receipt['activated_at_us'] <= gates[0]['server_us'] <= moments[0] <= moments[1] <= gates[1]['server_us']
    assert gates[2]['server_us'] <= moments[2] <= moments[3] <= gates[3]['server_us']
    assert moments[0] < control['lease_expires_at_us']
    guards.gate(capture, 'manifest-install')
    assert capture.take('live-manifest-result') == dict(event='live-manifest-result', cases=2, passed=True)
    after = capture.snapshot('manifest-refreshed')
    current = guards.refreshed(pg, after, control, window)
    live.same(after, raw['after_refresh'])
    refresh_at = current['lease_expires_at_us'] - 20_000_000
    assert gates[1]['server_us'] <= refresh_at, 'manifest refresh predates gate release'
    assert raw['run'] == dict(singleton=True, status='complete', executions=1, xmin=current['xmin'])
    assert len(raw['cases']) == 2
    outer, = {r['transaction_id'] for r in raw['cases']}
    assert isinstance(outer, str) and outer.isascii() and outer.isdecimal() and 0 < int(outer) < 1 << 64
    assert int(outer) % (1 << 32) == int(current['xmin'])
    assert int(outer) > int(receipt['activation_transaction']), 'activation was not previously committed'
    previous = refresh_at
    for index, (name, row) in enumerate(zip(MANIFEST_CASES, raw['cases'], strict=True), 1):
        detail = row['detail']
        assert set(detail) == {'receipt', 'attempted', 'remaining_us', 'attempted_at_us', 'rolled_back', 'expected_function'}
        assert detail['receipt'] == receipt, 'fixture rewrote committed activation'
        at, observed, remaining = detail['attempted_at_us'], row['observed_at_us'], detail['remaining_us']
        live.clock(at, window)
        live.clock(observed, window)
        assert previous <= at <= observed <= gates[2]['server_us']
        ceiling = min(current['lease_expires_at_us'], plan['lease_expires_at_us'])
        assert type(remaining) is int and 0 < remaining
        assert ceiling - at <= remaining <= ceiling - previous
        previous = observed
        assert detail['attempted'] == attempted_request(pg, current, plan, at, index)
        assert detail['attempted']['manifest_sha256'] == receipt['previous_manifest_sha256']
        assert detail['attempted']['manifest_sha256'] != receipt['target_manifest_sha256']
        assert detail['rolled_back'] is True and detail['expected_function'] == 'veto_unactivated_producer()'
        context = row['context']
        assert isinstance(context, str) and re.search(
            r'PL/pgSQL function (?:control\.)?veto_unactivated_producer\(\) line [0-9]+ at RAISE', context)
        assert 'live_manifest_fixture.exercise()' in context
        assert row == dict(case_index=index, name=name, status='PASS', sqlstate='23514',
            message=MANIFEST_ERROR, context=context, backend_pid=current['control_backend_pid'],
            backend_start=current['control_backend_start'], database_name=backend['database'],
            database_user='bigbrotr_control', replication_role='origin', control_epoch=8,
            transaction_id=outer, observed_at_us=observed, detail=detail, xmin=current['xmin'])
    guards.gate(capture, 'manifest-remove')
    live.same(after, capture.snapshot('manifest-clean'))
    live.same(after, raw['clean'])
    return after, current, gates[3]['server_us']


def plan_checked(original, native, inputs, observed, expired):
    # The shared independent byte recipe has a fixed60s scheduling parameter.
    # Check the selected5s field explicitly; no event or snapshot is rewritten.
    standard = dict(observed, lease_expires_at_us=observed['created_at_us'] + 60_000_000)
    plan, old, target = live.minted_plan(original, native, inputs, standard)
    plan['lease_expires_at_us'] = plan['created_at_us'] + (5_000_000 if expired else 60_000_000)
    assert observed == plan, 'unexpected initial target deadline'
    return plan, old, target


def acquire(capture, before, stage, ordinal, window):
    role, incarnation = f'{210 + ordinal:02x}' * 16, f'{220 + ordinal:02x}' * 16
    assert capture.take('successor-control', stage, 'claim-planned') == dict(
        event='successor-control', stage=stage, phase='claim-planned',
        role=role, incarnation=incarnation, lease_ttl_us='20000000')
    live.same(before, capture.snapshot(stage + '-before-claim'))
    event = capture.take('successor-control', stage, 'claimed')
    after = capture.snapshot(stage + '-after-claim')
    owner = live.claim(before, after, ordinal)
    assert owner['epoch'] == 7 + ordinal
    assert event == dict(event='successor-control', stage=stage, phase='claimed',
        resource=owner['resource_id'][2:], epoch=owner['epoch'], role=role, incarnation=incarnation)
    live.clock(owner['acquired_at_us'], window)
    role_row = live.only(after['role_instances'], role_instance_id=owner['role_instance_id'])
    live.clock(role_row['created_at_us'], window)
    return after, owner


def backend_present(capture, control, stage):
    event = capture.take('successor-backend', stage)
    database = event['database']
    assert isinstance(database, str) and re.fullmatch('[a-z][a-z0-9_]{0,62}', database)
    assert event == dict(event='successor-backend', stage=stage, database=database,
        pid=control['control_backend_pid'], backend_start=control['control_backend_start'],
        control_epoch=control['epoch'], present=True)
    return event


def end(capture, before, control, backend, stage, release, failed, clean_driver, window, lower):
    live.same(before, capture.snapshot(stage + '-before-control-end'))
    if failed:
        assert capture.take('successor-driver', stage) == dict(
            event='successor-driver', stage=stage, failed=True, joined=True)
    phase = 'released-and-joined' if release else 'dropped-and-joined'
    assert capture.take('successor-control', stage, phase) == dict(
        event='successor-control', stage=stage, phase=phase,
        resource=control['resource_id'][2:], epoch=control['epoch'])
    after = capture.snapshot(stage + '-after-control-end')
    live.ended(before, after, control, release)
    if release:
        owner = live.only(after['owner_epochs'], role='control', epoch=control['epoch'])
        role = live.only(after['role_instances'], role_instance_id=control['role_instance_id'])
        live.fresh_xmin(owner['xmin'], before)
        live.clock(owner['released_at_us'], window)
        live.clock(role['retired_at_us'], window)
        assert lower <= owner['released_at_us'] <= role['retired_at_us']
    if clean_driver:
        assert capture.take('successor-driver', stage) == dict(
            event='successor-driver', stage=stage, failed=False, joined=True)
    assert capture.take('successor-backend', backend['stage']) == dict(backend, present=False)
    return after


def scheduling(capture, phase, plan, control, window):
    event = capture.take('live-renewal-clock', phase=phase)
    now = event['now_us']
    live.clock(now, window)
    assert event == dict(event='live-renewal-clock', phase=phase, control_epoch=8,
        now_us=now, ingest_lease_expires_at_us=plan['lease_expires_at_us'],
        control_lease_expires_at_us=control['lease_expires_at_us'])
    return now


def catalog_checked(fixture):
    before = fixture['catalog_before']
    assert set(before) == {'session_replication_role', 'triggers', 'constraints', 'functions', 'tables'}
    assert before['session_replication_role'] == 'origin'
    assert len(before['triggers']) == 1045 and len(before['constraints']) == 721
    assert len({(t['table'], t['name']) for t in before['triggers']}) == 1045
    assert all(t['enabled'] in ('O', 'A') for t in before['triggers'])
    assert all(c['validated'] is True for c in before['constraints'])
    assert len(before['functions']) == 14 and all(f['execute'] is True for f in before['functions'])
    assert all(v is True for v in before['tables'].values())
    pinned = dict(before, triggers=[t for t in before['triggers'] if not t['internal']])
    assert live.digest(live.encoded(pinned)) == guards.CATALOG, 'product catalog changed'
    assert fixture['catalog_after'] == before, 'catalog not restored'


def external(fixture, controls, backends, mode, window):
    assert set(fixture) == {'manifest', 'wire', 'backend', 'diagnostic', 'catalog_before', 'catalog_after'}
    catalog_checked(fixture)
    observed = fixture['backend']
    assert set(observed) == {'exact', 'absence'}
    assert len(observed['exact']) == len(controls) == len(backends)
    for control, backend, exact in zip(controls, backends, observed['exact'], strict=True):
        expected = routes.backend_checked(dict(exact=exact, absence=observed['absence']), control)
        assert backend == dict(event='successor-backend', stage=backend['stage'], **expected, present=True)
    assert observed['absence']['seconds'] <= 120
    if mode == CASES[2]:
        diagnostic = fixture['diagnostic']
        expected = observed['exact'][0]['expected']
        assert set(diagnostic) == {'backend', 'since', 'until', 'returncode',
            'observed_sqlstate', 'errors', 'contexts'}
        assert diagnostic['backend'] == expected and diagnostic['returncode'] == 0
        for field, clock in (('since', 'before_us'), ('until', 'after_us')):
            assert diagnostic[field] == datetime.fromtimestamp(window[clock] / 1_000_000, timezone.utc).isoformat()
        error, = diagnostic['errors']
        context, = diagnostic['contexts']
        prefix = r'\[' + str(expected['pid']) + r'\] '
        found = re.search(prefix + r'ERROR:  (?:(?P<code>[0-9A-Z]{5}): )?'
            + re.escape(EXPIRED_ERROR) + r'$', error)
        assert found and found['code'] in (None, '23514'), 'different expiry refusal'
        assert diagnostic['observed_sqlstate'] == found['code']
        assert re.search(prefix + r'CONTEXT:  PL/pgSQL function (?:control\.)?'
            r'require_active_initial_writer\(bytea\) line [0-9]+ at RAISE', context)
        for line in (error, context):
            at = datetime.fromisoformat(line.split()[0].replace('Z', '+00:00'))
            assert datetime.fromisoformat(diagnostic['since']) <= at <= datetime.fromisoformat(diagnostic['until'])
    else:
        assert fixture['diagnostic'] is None
    wire_checked(fixture['wire'], mode)


def wire_checked(wire, mode):
    if mode not in CASES[3:]:
        assert wire == dict(status='NOT_RUN')
        return
    reason = 'before-forward' if mode == CASES[3] else 'after-complete'
    assert wire['status'] == 'PASS' and wire['exit_code'] == wire['wrapper_exit'] == 0
    assert wire['forced_wrapper_stop'] is False and wire['stderr_nonempty'] is False
    ready, fault, receipt = wire['events']
    assert ready == wire['ready'] and receipt == wire['receipt']
    assert [e['event'] for e in wire['events']] == ['ready', 'fault', 'receipt']
    assert ready['listen_host'] == '127.0.0.1' and ready['upstream_port'] == 5432
    assert type(ready['listen_port']) is int and 0 < ready['listen_port'] < 65536
    assert ready['proxy_sha256'] == wire['proxy_sha256']
    assert wire['proxy_sha256'] == '6e737de8223818d0b05b47cb25f16038233d43181b12aec97bd13e3b44e3f829'
    assert ready['commit_number'] == receipt['commit_number'] == 15
    assert fault['reason'] == receipt['reason'] == reason
    expected = [15, 14, 14, 14, 1] if mode == CASES[3] else [15, 15, 15, 14, 1]
    fields = ('commits_seen', 'commits_forwarded', 'server_commit_complete', 'client_commit_complete', 'faults')
    assert [receipt[k] for k in fields] == [fault[k] for k in fields] == expected, 'different COMMIT boundary'
    for event in wire['events']:
        assert event['format'] == 'bigbrotr-pg-fault-proxy/1' and event['mode'] == reason
        assert event['commit_number'] == 15 and event['held'] is False and event['hold_action'] is None
        assert type(event['elapsed_ns']) is int and event['elapsed_ns'] >= 0
        assert all(type(event[k]) is int for k in fields)
    assert [e['elapsed_ns'] for e in wire['events']] == sorted(e['elapsed_ns'] for e in wire['events'])
    assert [ready[k] for k in fields] == [0] * 5


def verify(events, original, native, inputs, mode, *, seed, server_window, fixture):
    assert mode in CASES
    live.check_window(server_window)
    live.migrated(seed, original, native, inputs, server_window)
    observed = live.only(events, event='successor-plan')
    live.clock(observed['created_at_us'], server_window)
    plan, old, new = plan_checked(original, native, inputs, observed, mode == CASES[2])
    capture = live.Capture(events, original, old, inputs)
    pg, control = acquire(capture, original, 'initial', 1, server_window)
    assert plan['target_role'] != control['role_instance_id'][2:]
    assert plan['target_incarnation'] != control['process_incarnation'][2:]
    assert capture.take('successor-plan') == plan
    after = capture.snapshot('before-prepare')
    control = guards.refreshed(pg, after, control, server_window)
    assert control['lease_expires_at_us'] - 20_000_000 <= plan['created_at_us']
    pg = after
    capture.native, capture.sequence = new, 62
    after = capture.snapshot('prepared')
    assert pg[live.ACTIVATIONS] == after[live.ACTIVATIONS] == []
    live.prepared({k: v for k, v in pg.items() if k != live.ACTIVATIONS},
        {k: v for k, v in after.items() if k != live.ACTIVATIONS}, plan, control)
    pg = after
    backend = backend_present(capture, control, 'initial')
    controls, backends = [control], [backend]
    assert capture.take('live-activation-start') == dict(event='live-activation-start', mode=mode, control_epoch=8)
    result = capture.take('live-activation-result')
    assert result == dict(event='live-activation-result', writer=True, error=None, activated_at_us=result['activated_at_us'])
    after = capture.snapshot('activated-zero')
    receipt = live.activated(pg, after, plan, control, server_window, result['activated_at_us'])
    pg, lower = after, receipt['activated_at_us']
    fault = mode in CASES[3:]
    if mode == CASES[0]:
        pg, control, lower = manifest(capture, pg, control, plan, receipt, fixture, backend, server_window)
        science = live.Science(plan, new, inputs)
        for stage, n in [('dictionary', 1), ('new-event', 2), ('same-request', 2), ('same-id-alternate-signature', 3)]:
            receipt_bytes = science.advance(stage)
            expected = dict(event='live-receipt', stage=stage, request=n, receipt=receipt_bytes.hex())
            if stage != 'dictionary':
                expected.update(event_id=live.EVENT_ID.hex(), provenance=True,
                    supplied_signature=(live.ALTERNATE_SIG if n == 3 else live.PUBLIC_SIG).hex())
            assert capture.take('live-receipt', stage) == expected
            capture.native, capture.inputs, capture.sequence = science.rows, science.inputs, science.sequence
            after = capture.snapshot(stage)
            if stage == 'same-request':
                live.same(pg, after)
            else:
                live.admitted(pg, after, science, n, control, server_window)
                outcome = live.only(after['producer_outcomes'], producer_epoch=2, request_sequence=n)
                request = live.only(after['producer_requests'], producer_epoch=2, request_sequence=n)
                assert lower <= request['reserved_at_us']
                lower = outcome['recorded_at_us']
            pg = after
    else:
        assert fixture['manifest'] is None
        if mode == CASES[1]:
            live.same(pg, capture.snapshot('live-renew-before-switch'))
            pg = end(capture, pg, control, backend, 'renewal-previous', True, False, True, server_window, lower)
            old_role = live.only(pg['role_instances'], role_instance_id=control['role_instance_id'])
            lower = old_role['retired_at_us']
            pg, control = acquire(capture, pg, 'renewal-fresh', 2, server_window)
            assert lower <= control['acquired_at_us']
            backend = backend_present(capture, control, 'renewal-fresh')
            assert (backend['pid'], backend['backend_start']) != (backends[0]['pid'], backends[0]['backend_start']), 'fresh Control reused absent backend'
            assert datetime.fromisoformat(backend['backend_start']) >= datetime.fromtimestamp(
                lower / 1_000_000, timezone.utc), 'fresh backend predates previous driver closure'
            controls.append(control)
            backends.append(backend)
        else:
            after = capture.snapshot('live-control-refreshed')
            control = guards.refreshed(pg, after, control, server_window)
            assert control['lease_expires_at_us'] - 20_000_000 >= lower, 'refresh predates activation'
            pg = after
            lower = control['lease_expires_at_us'] - 20_000_000
            if mode == CASES[2]:
                before = scheduling(capture, 'before-wait', plan, control, server_window)
                expired = scheduling(capture, 'expired', plan, control, server_window)
                assert lower <= before < plan['lease_expires_at_us'] <= expired < control['lease_expires_at_us']
                live.same(pg, capture.snapshot('live-renew-expired'))
                lower = expired
        live.same(pg, capture.snapshot('live-renew-before'))
        error = 'InvalidInput' if mode == CASES[1] else 'Backend' if mode == CASES[2] else 'CommitUnknown'
        assert capture.take('live-renewal-result') == dict(event='live-renewal-result', mode=mode,
            control_epoch=control['epoch'], commit_started=fault, error=error)
        after = capture.snapshot('live-renew-refused')
        if mode == CASES[4]:
            target = live.only(after['owner_epochs'], role='ingest', epoch=2)
            live.fresh_xmin(target['xmin'], pg)
            wanted = copy.deepcopy(pg)
            live.only(wanted['owner_epochs'], role='ingest', epoch=2).update(
                lease_expires_at_us=control['lease_expires_at_us'], xmin=target['xmin'])
            assert lower < plan['lease_expires_at_us']
            live.same(wanted, after)
        else:
            live.same(pg, after)
        pg = after
        if mode == CASES[1]:
            assert capture.take('live-renewal-usable') == dict(event='live-renewal-usable', control_epoch=9, usable=True)
            after = capture.snapshot('live-renew-control-usable')
            control = guards.refreshed(pg, after, control, server_window)
            assert lower <= control['lease_expires_at_us'] - 20_000_000
            pg = after
            lower = control['lease_expires_at_us'] - 20_000_000
        else:
            if mode == CASES[2]:
                after_refusal = scheduling(capture, 'after-refusal', plan, control, server_window)
                assert lower <= after_refusal < control['lease_expires_at_us']
                lower = after_refusal
            assert capture.take('successor-session-poisoned') == dict(event='successor-session-poisoned', control_epoch=8)
            live.same(pg, capture.snapshot('live-renew-unavailable'))
    release = mode in CASES[:2]
    pg = end(capture, pg, control, backend, 'unknown' if fault else 'final', release,
        fault, mode in CASES[1:3], server_window, lower)
    live.same(pg, capture.snapshot('final'))
    assert capture.take('live-complete') == dict(event='live-complete', mode=mode,
        native='closed', control='released-and-joined' if release else 'dropped-and-joined',
        writer=False, admission=mode == CASES[0])
    assert capture.position == len(events), 'unclassified trailing event'
    external(fixture, controls, backends, mode, server_window)
    target = live.only(pg['owner_epochs'], role='ingest', epoch=2)
    return dict(status='PASS', mode=mode, snapshots=capture.snapshots, control_tables=72,
        sql_cases=2 if mode == CASES[0] else 0, activated=True, activation_delivered=True,
        activation_transaction=receipt['activation_transaction'], activated_at_us=receipt['activated_at_us'],
        final_native_rows=len(capture.native), final_native_sequence=capture.sequence, final_inputs=len(capture.inputs),
        original_requests_preserved=4, new_requests=3 if mode == CASES[0] else 0,
        current_ingest_deadline_us=target['lease_expires_at_us'], renewal_unknown=fault,
        renewal_committed=mode == CASES[4], backend_absent=[dict(b, present=False) for b in backends],
        catalog_restored=True, finite_import_only=True, raw_running_import=False,
        network_provenance_off=False, installed_import=False, fresh_process_adoption=False)


def negative_controls(events, original, native, inputs, mode, *, seed, server_window, fixture):
    """Reject coherent state substitutions even after snapshot hashes are fixed."""
    verify(events, original, native, inputs, mode, seed=seed, server_window=server_window, fixture=fixture)
    rejected = []

    def check(name, change, message=None):
        e, f = copy.deepcopy(events), copy.deepcopy(fixture)
        change(e, f)
        try:
            verify(e, original, native, inputs, mode, seed=seed, server_window=server_window, fixture=f)
        except (AssertionError, KeyError, ValueError, TypeError, StopIteration) as error:
            if message is not None:
                assert isinstance(error, AssertionError) and str(error) == message, (name, error)
            rejected.append(name)
        else:
            raise AssertionError('accepted altered capture: ' + name)

    def snapshot(e, stage):
        return live.only(e, event='successor-state', stage=stage)

    def sql(e, stage, change):
        event = snapshot(e, stage)
        pg = {k: json.loads(v) for k, v in event['state']['control_tables']}
        change(pg)
        event['state']['control_tables'] = [[k, live.encoded(pg[k]).decode()] for k in sorted(pg)]
        event['state']['control_sha256'] = live.digest(live.encoded(event['state']['control_tables']))

    def onward(e, stage, change):
        start = next(i for i, v in enumerate(e) if v['event'] == 'successor-state' and v['stage'] == stage)
        for event in e[start:]:
            if event['event'] == 'successor-state':
                sql(e, event['stage'], change)

    check('retained-original-xmin', lambda e, f: sql(e, 'final',
        lambda p: live.only(p['producer_outcomes'], producer_epoch=1, request_sequence=3).update(xmin='999999999')))
    def scientific(e, f):
        event = snapshot(e, 'final')
        row = next(r for r in event['rows'] if r['cf'] == 'facts')
        row['value'] = row['value'][:-2] + ('00' if row['value'][-2:] != '00' else '01')
        event['state']['native_sha256'] = live.digest(live.encoded(event['rows']))
    check('rehashed-original-scientific-bytes', scientific)
    check('original-input-replaced', lambda e, f: snapshot(e, 'final')['state']['originals'][0][1].update(sha256='00' * 32))
    check('native-sequence', lambda e, f: snapshot(e, 'final')['state'].update(sequence=63))
    check('activation-transaction-rewritten', lambda e, f: sql(e, 'activated-zero',
        lambda p: live.only(p[live.ACTIVATIONS]).update(activation_transaction='1')))
    check('poisoned-writer-adoption', lambda e, f: live.only(e, event='live-complete').update(writer=True))
    check('native-not-closed', lambda e, f: live.only(e, event='live-complete').update(native='open'))
    check('external-backend-wrong-incarnation', lambda e, f: f['backend']['exact'][0]['expected'].update(pid=1))
    check('external-backend-still-present', lambda e, f: f['backend']['exact'][0].update(present=True, stdout='t\n'))
    check('external-global-not-absent', lambda e, f: f['backend']['absence']['attempts'][-1].update(count=1))
    def catalog_substitution(e, f):
        for key in ('catalog_before', 'catalog_after'):
            f[key]['functions'][0]['definition'] += '-- changed\n'
        if f['manifest']:
            for key in ('catalog_before', 'catalog_installed', 'catalog_removed'):
                f['manifest'][key]['functions'][0]['definition'] += '-- changed\n'
    check('coherent-product-guard-substitution', catalog_substitution)
    if mode == CASES[0]:
        check('wrong-manifest-case-skipped', lambda e, f: f['manifest']['cases'].pop())
        check('different-actual-backend', lambda e, f: f['manifest']['cases'][0].update(backend_pid=1))
        check('privileged-fixture-actor', lambda e, f: f['manifest']['cases'][0].update(database_user='postgres'))
        check('different-manifest-veto', lambda e, f: f['manifest']['cases'][0].update(context='different guard'))
        check('different-sqlstate', lambda e, f: f['manifest']['cases'][1].update(sqlstate='23503'))
        check('current-target-manifest-substituted', lambda e, f: f['manifest']['cases'][0]['detail']['attempted'].update(
            manifest_sha256=f['manifest']['cases'][0]['detail']['receipt']['target_manifest_sha256']))
        check('binding-original-digest-replaced', lambda e, f: f['manifest']['cases'][1]['detail']['attempted'].update(record_sha256='\\x' + '00' * 32))
        check('request-retired-acquisition-source', lambda e, f: f['manifest']['cases'][0]['detail']['attempted'].update(acquisition_job_id='\\x' + '01' * 16))
        check('activation-created-in-guard', lambda e, f: f['manifest']['cases'][0]['detail']['receipt'].update(
            activation_transaction=f['manifest']['cases'][0]['transaction_id']))
        check('rollback-not-observed', lambda e, f: f['manifest']['cases'][0]['detail'].update(rolled_back=False))
        check('outer-transaction-misattributed', lambda e, f: f['manifest']['cases'][0].update(transaction_id='1'))
        def fixture_sql(e, f):
            g = f['manifest']
            g['install_sql'] = g['install_sql'].replace('SECURITY INVOKER', 'SECURITY DEFINER')
            g['install_sha256'] = live.digest(g['install_sql'].encode())
        check('rehashed-unreviewed-fixture', fixture_sql)
        check('manifest-trigger-retained', lambda e, f: f['manifest']['catalog_removed']['triggers'].append(TRIGGER))
        def backdated(e, f):
            g = f['manifest']
            expiry = g['gates'][1]['server_us'] - 1 + 20_000_000
            def change(p):
                live.only(p['owner_epochs'], role='control', epoch=8)['lease_expires_at_us'] = expiry
            onward(e, 'manifest-refreshed', change)
            for phase in ('after_refresh', 'clean'):
                change(g[phase])
            for row in g['cases']:
                d = row['detail']
                d['remaining_us'] = min(expiry, live.only(e, event='successor-plan')['lease_expires_at_us']) - d['attempted_at_us'] + 1
        check('coherently-backdated-manifest-refresh', backdated, 'manifest refresh predates gate release')
        check('same-request-not-identical', lambda e, f: live.only(e, event='live-receipt', stage='same-request').update(receipt='00'))
        check('alternate-signature-overwritten', lambda e, f: live.only(e, event='live-receipt', stage='same-id-alternate-signature').update(supplied_signature=live.PUBLIC_SIG.hex()))
        check('counterfactual-request-old-manifest', lambda e, f: sql(e, 'dictionary',
            lambda p: live.only(p['producer_requests'], producer_epoch=2).update(
                manifest_sha256=live.only(p[live.ACTIVATIONS])['previous_manifest_sha256'])))
    else:
        check('wrong-commit-started', lambda e, f: live.only(e, event='live-renewal-result').update(commit_started=mode not in CASES[3:]))
        check('wrong-refusal', lambda e, f: live.only(e, event='live-renewal-result').update(error='Unavailable'))
        check('refusal-admits-request', lambda e, f: live.only(e, event='live-complete').update(admission=True))
        if mode == CASES[1]:
            check('fresh-control-unusable', lambda e, f: live.only(e, event='live-renewal-usable').update(usable=False))
            check('fresh-control-substituted', lambda e, f: live.only(e, event='live-renewal-result').update(control_epoch=8))
            check('old-control-driver-not-joined', lambda e, f: live.only(e, event='successor-driver', stage='renewal-previous').update(joined=False))
            check('old-control-absence-missing', lambda e, f: e.pop(next(i for i, v in enumerate(e)
                if v['event'] == 'successor-backend' and v['control_epoch'] == 8 and v['present'] is False)))
            def backend_substitution(e, f, alias):
                old = live.only(e, event='successor-backend', control_epoch=8, present=True)
                current = live.only(e, event='successor-backend', control_epoch=9, present=True)
                pid = old['pid'] if alias else current['pid']
                start = old['backend_start']
                for event in e:
                    if event['event'] == 'successor-backend' and event['control_epoch'] == 9:
                        event.update(pid=pid, backend_start=start)
                onward(e, 'renewal-fresh-after-claim', lambda p: live.only(p['owner_epochs'], role='control', epoch=9).update(
                    control_backend_pid=pid, control_backend_start=start))
                f['backend']['exact'][1]['expected'].update(pid=pid, backend_start=start)
            check('coherent-absent-backend-resurrection', lambda e, f: backend_substitution(e, f, True),
                'fresh Control reused absent backend')
            check('coherent-backend-predates-previous-release', lambda e, f: backend_substitution(e, f, False),
                'fresh backend predates previous driver closure')
        else:
            check('poisoned-control-reusable', lambda e, f: live.only(e, event='successor-session-poisoned').update(control_epoch=9))
            def before_activation(e, f):
                expiry = live.only(e, event='live-activation-result')['activated_at_us'] - 1 + 20_000_000
                def change(p):
                    live.only(p['owner_epochs'], role='control', epoch=8)['lease_expires_at_us'] = expiry
                onward(e, 'live-control-refreshed', change)
                if mode == CASES[4]:
                    onward(e, 'live-renew-refused', lambda p: live.only(p['owner_epochs'], role='ingest', epoch=2).update(lease_expires_at_us=expiry))
                for event in e:
                    if event['event'] == 'live-renewal-clock':
                        event['control_lease_expires_at_us'] = expiry
            check('coherent-renewal-before-activation', before_activation, 'refresh predates activation')
            if mode == CASES[2]:
                check('target-not-expired', lambda e, f: live.only(e, event='live-renewal-clock', phase='expired').update(
                    now_us=live.only(e, event='successor-plan')['lease_expires_at_us'] - 1))
                check('control-also-expired', lambda e, f: live.only(e, event='live-renewal-clock', phase='after-refusal').update(
                    now_us=live.only(e, event='live-renewal-clock', phase='after-refusal')['control_lease_expires_at_us']))
                check('wrong-expiry-guard', lambda e, f: f['diagnostic'].update(contexts=['unrelated guard']))
                check('expiry-driver-misclassified', lambda e, f: live.only(e, event='successor-driver').update(failed=True))
            else:
                check('unknown-driver-misclassified', lambda e, f: live.only(e, event='successor-driver').update(failed=False))
                def wrong_wire(e, f):
                    f['wire']['receipt']['client_commit_complete'] = 15
                    f['wire']['events'][-1] = copy.deepcopy(f['wire']['receipt'])
                check('COMMIT-delivered-to-client', wrong_wire)
                check('wrong-COMMIT-ordinal', lambda e, f: f['wire']['receipt'].update(commit_number=13))
                def wrong_persistence(e, f):
                    def change(p):
                        i = live.only(p['owner_epochs'], role='ingest', epoch=2)
                        if mode == CASES[3]:
                            i.update(lease_expires_at_us=live.only(p['owner_epochs'], role='control', epoch=8)['lease_expires_at_us'], xmin='999999998')
                        else:
                            prepared_owner = live.only({k: json.loads(v) for k, v in snapshot(e, 'prepared')['state']['control_tables']}['owner_epochs'], role='ingest', epoch=2)
                            i.update(lease_expires_at_us=prepared_owner['lease_expires_at_us'], xmin=prepared_owner['xmin'])
                    onward(e, 'live-renew-refused', change)
                check('coherently-reversed-COMMIT-persistence', wrong_persistence)
    return dict(status='PASS', rejected=rejected)
