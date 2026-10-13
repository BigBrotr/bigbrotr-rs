"""Independent finite activation SQL-guard and typed renewal observations.

The real preparer's refresh invokes fourteen disposable SQL subtransactions.
Their diagnostics are not native authority. No old transcript is projected or
repaired: complete raw snapshots are checked before shared byte/row reducers.
"""
import copy
import json
import re

import initial_live_oracle as live


MODE = 'successor-live-guards'
CASES = (
    'activation-positive', 'request-immediate-before', 'request-immediate-after',
    'binding-immediate-after', 'checkpoint-zero', 'target-owner-lease',
    'target-role-retire', 'target-domain-close', 'target-source-close',
    'target-producer-close', 'control-expire', 'control-release',
    'control-role-retire', 'policy-head-advance',
)
ADMISSION = 'target admission requires a previously committed activation'
CHECKPOINT = 'target checkpoint requires a previously committed activation'
TARGET = 'activation authority and target must remain unchanged through its commit'
# Exact, independently reviewed invoker fixture template, before substitution.
# This pins SQL meaning, guard order and deliberate subtransaction rollback;
# changing the fixture requires review of this literal, not a generated update.
# Request cases select the original unbound dictionary request3 with STRICT
# cardinality; they must not fail first on request4's retired acquisition job.
TEMPLATE = '96ccafd10ab55be14bfb28bf8f737506551fad8bb189a95fe8cb6f38d5f49322'
# The five qualified2c142c6 catalogs agree on every product trigger, constraint,
# function definition and grant. Internal trigger names contain per-clone OIDs;
# retain and compare those exactly within this run instead of pinning their IDs.
CATALOG = '43d3d12896981233d86b7b0a9ad19354728592e1685ed34c014fff52dc89a78c'
EVENT_BYTES = 353
EVENT_SHA = 'c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae'
REMOVE = ("BEGIN; SET LOCAL lock_timeout='5s'; "
    'DROP TRIGGER live_guard_fixture_refresh ON control.owner_epochs; '
    'DROP FUNCTION live_guard_fixture.exercise(); COMMIT;')
TRIGGER = dict(table='owner_epochs', name='live_guard_fixture_refresh', enabled='O',
    internal=False, function='live_guard_fixture.exercise()')


def sql_definition(guards, control):
    statement = guards['install_sql']
    assert guards['install_sha256'] == live.digest(statement.encode())
    normalized = statement
    replacements = {
        ','.join("'" + name + "'" for name in CASES): '__NAMES__',
        ADMISSION: '__ADMISSION__', CHECKPOINT: '__CHECKPOINT__', TARGET: '__TARGET__',
        EVENT_SHA: '__EVENT_SHA__',
        "decode('" + control['resource_id'][2:] + "','hex')": '__RESOURCE__',
        "decode('" + control['process_incarnation'][2:] + "','hex')": '__INCARNATION__',
        str(EVENT_BYTES): '__EVENT_BYTES__',
    }
    for value, placeholder in replacements.items():
        assert value in normalized
        normalized = normalized.replace(value, placeholder)
    assert live.digest(normalized.encode()) == TEMPLATE, 'unreviewed SQL fixture'
    assert guards['remove_sql'] == REMOVE
    function = guards['fixture_function']
    assert set(function) == {'security_definer', 'definition'}
    assert function['security_definer'] is False
    body = statement.split('$fixture$')
    assert len(body) == 3
    header = ("CREATE OR REPLACE FUNCTION live_guard_fixture.exercise()\n"
        " RETURNS trigger\n LANGUAGE plpgsql\n SET search_path TO 'pg_catalog'\n")
    assert function['definition'] == header + 'AS $function$' + body[1] + '$function$\n'


def catalogs(guards):
    before = guards['catalog_before']
    assert set(before) == {'session_replication_role', 'triggers', 'constraints', 'functions', 'tables'}
    assert before['session_replication_role'] == 'origin'
    assert before['triggers'] and all(t['enabled'] in ('O', 'A') for t in before['triggers'])
    assert before['constraints'] and all(c['validated'] is True for c in before['constraints'])
    assert len(before['triggers']) == 1045 and len(before['constraints']) == 721
    assert len({(t['table'], t['name']) for t in before['triggers']}) == 1045
    pinned = dict(before, triggers=[t for t in before['triggers'] if not t['internal']])
    assert live.digest(live.encoded(pinned)) == CATALOG, 'product catalog differs from qualified schema32'
    functions = before['functions']
    assert len(functions) == 14 and all(f['execute'] is True for f in functions)
    assert all(before['tables'].values())
    assert live.only(before['constraints'], name='initial_activation_complete')['deferred'] is True
    assert live.only(before['constraints'], name='initial_activation_complete')['deferrable'] is True
    assert not any(t['name'] == TRIGGER['name'] for t in before['triggers'])
    wanted = copy.deepcopy(before)
    wanted['triggers'].append(TRIGGER)
    wanted['triggers'].sort(key=lambda t: (t['table'], t['name']))
    assert guards['catalog_installed'] == wanted, 'fixture altered a product guard'
    assert guards['catalog_removed'] == before, 'product catalog not restored'


def raw_cases(guards, plan, prior, current, database, window):
    assert guards['status'] == 'CAPTURED'
    assert guards['forced_wrapper_stop'] is False and guards['cleanup_errors'] == []
    sql_definition(guards, prior)
    catalogs(guards)
    moments = [guards[k] for k in ('install_before_us', 'install_after_us',
        'remove_before_us', 'remove_after_us')]
    for value in moments:
        live.clock(value, window)
    assert moments == sorted(moments)
    assert moments[0] < prior['lease_expires_at_us']
    assert guards['control'] == prior
    gates = guards['gates']
    assert [(g['gate'], g['phase']) for g in gates] == [
        ('install', 'waiting'), ('install', 'released'), ('remove', 'waiting'), ('remove', 'released')]
    assert all(set(g) == {'gate', 'phase', 'server_us'} for g in gates)
    for gate in gates:
        live.clock(gate['server_us'], window)
    assert [g['server_us'] for g in gates] == sorted(g['server_us'] for g in gates)
    assert gates[0]['server_us'] <= moments[0] <= moments[1] <= gates[1]['server_us']
    assert gates[2]['server_us'] <= moments[2] <= moments[3] <= gates[3]['server_us']
    refresh_at = current['lease_expires_at_us'] - 20_000_000
    assert gates[1]['server_us'] <= refresh_at, 'guard refresh predates gate release'
    assert guards['run'] == dict(singleton=True, status='complete', executions=1, xmin=current['xmin'])
    cases = guards['cases']
    assert len(cases) == 14
    outer, = {row['transaction_id'] for row in cases}
    assert isinstance(outer, str) and outer.isdecimal() and 0 < int(outer) < 1 << 64
    assert int(outer) % (1 << 32) == int(current['xmin'])
    intent = live.only(guards['before']['initial_ingest_successions'])
    fields = ('dataset_id', 'owner_resource_id', 'target_epoch', 'target_incarnation',
        'target_role_instance_id', 'producer_slot_id', 'producer_epoch', 'previous_manifest_sha256',
        'target_manifest_sha256', 'handoff_sha256', 'prepared_by_resource', 'prepared_by_epoch',
        'prepared_by_incarnation', 'config_revision', 'policy_revision', 'config_digest', 'policy_digest')
    receipt_base = {key: intent[key] for key in fields}
    receipt_base.update(activated_by_resource=current['resource_id'], activated_by_epoch=8,
        activated_by_incarnation=current['process_incarnation'], activation_transaction=outer)
    previous_time, subtransactions = refresh_at, set()
    for index, (name, row) in enumerate(zip(CASES, cases, strict=True), 1):
        detail = row['detail']
        assert set(detail) == {'receipt', 'remaining_us', 'constraint_order', 'rolled_back', 'expected_function'}
        assert detail['rolled_back'] is True
        assert detail['constraint_order'] == ('before-receipt' if index == 2 else 'after-receipt')
        receipt = detail['receipt']
        tx, at, observed = receipt['xmin'], receipt['activated_at_us'], row['observed_at_us']
        live.fresh_xmin(tx, guards['after_refresh'])
        assert int(tx) < 1 << 32
        assert tx not in subtransactions
        subtransactions.add(tx)
        live.clock(at, window)
        live.clock(observed, window)
        assert previous_time <= at <= observed <= moments[2]
        previous_time = observed
        remaining = detail['remaining_us']
        assert type(remaining) is int and 0 < remaining <= min(
            current['lease_expires_at_us'], plan['lease_expires_at_us']) - at
        assert receipt == dict(receipt_base, activated_at_us=at, xmin=tx)
        if index == 1:
            code = message = context = function = None
        else:
            code = '23514'
            message = ADMISSION if index in (2, 3, 4) else CHECKPOINT if index == 5 else TARGET
            function = ('veto_unactivated_producer()' if index in (2, 3, 4)
                else 'guard_initial_activation_checkpoint()' if index == 5 else 'guard_initial_activation_target()')
            context = row['context']
            assert isinstance(context, str)
            assert re.search(r'PL/pgSQL function (?:control\.)?' + re.escape(function)
                + r' line [0-9]+ at RAISE', context), 'refusal came from a different guard'
            assert 'live_guard_fixture.exercise()' in context
        assert detail['expected_function'] == function
        assert row == dict(case_index=index, name=name, status='PASS', sqlstate=code,
            message=message, context=context, backend_pid=current['control_backend_pid'],
            backend_start=current['control_backend_start'], database_name=database,
            database_user='bigbrotr_control', replication_role='origin', control_epoch=8,
            transaction_id=outer, observed_at_us=observed, detail=detail, xmin=current['xmin'])
    return outer


def refreshed(before, after, owner, window):
    current = live.renewed(before, after, owner, window['after_us'])
    live.fresh_xmin(current['xmin'], before)
    at = current['lease_expires_at_us'] - 20_000_000
    live.clock(at, window)
    assert at < owner['lease_expires_at_us'], 'refresh revived an expired owner'
    return current


def gate(capture, name):
    for phase in ('waiting', 'released'):
        assert capture.take('live-guards-gate', phase=phase) == dict(
            event='live-guards-gate', gate=name, phase=phase)


def verify(events, original, native, inputs, mode, *, seed, server_window, guards):
    assert mode == MODE
    live.check_window(server_window)
    live.migrated(seed, original, native, inputs, server_window)
    observed = live.only(events, event='successor-plan')
    live.clock(observed['created_at_us'], server_window)
    plan, old, new = live.minted_plan(original, native, inputs, observed)
    capture = live.Capture(events, original, old, inputs)
    planned = capture.take('successor-control', 'initial', 'claim-planned')
    assert planned == dict(event='successor-control', stage='initial', phase='claim-planned',
        role='d3' * 16, incarnation='dd' * 16, lease_ttl_us='20000000')
    assert plan['target_role'] != planned['role'] and plan['target_incarnation'] != planned['incarnation']
    live.same(original, capture.snapshot('initial-before-claim'))
    claimed = capture.take('successor-control', 'initial', 'claimed')
    pg = capture.snapshot('initial-after-claim')
    control = live.claim(original, pg, 1)
    assert control['epoch'] == 8
    assert claimed == dict(event='successor-control', stage='initial', phase='claimed',
        resource=control['resource_id'][2:], epoch=8, role='d3' * 16, incarnation='dd' * 16)
    live.clock(control['acquired_at_us'], server_window)
    live.clock(live.only(pg['role_instances'], role_instance_id=control['role_instance_id'])['created_at_us'], server_window)
    assert capture.take('successor-plan') == plan
    after = capture.snapshot('before-prepare')
    control = refreshed(pg, after, control, server_window)
    assert control['lease_expires_at_us'] - 20_000_000 <= plan['created_at_us']
    pg = after
    capture.native, capture.sequence = new, 62
    after = capture.snapshot('prepared')
    assert pg[live.ACTIVATIONS] == after[live.ACTIVATIONS] == []
    live.prepared({k: v for k, v in pg.items() if k != live.ACTIVATIONS},
        {k: v for k, v in after.items() if k != live.ACTIVATIONS}, plan, control)
    pg = after
    live.same(pg, guards['before'])
    prior_control = control
    gate(capture, 'install')
    assert capture.take('live-guards-result') == dict(event='live-guards-result', cases=14, passed=True)
    after = capture.snapshot('guards-refreshed')
    control = refreshed(pg, after, control, server_window)
    live.same(after, guards['after_refresh'])
    pg = after
    gate(capture, 'remove')
    after = capture.snapshot('guards-clean')
    live.same(pg, after)
    live.same(after, guards['clean'])
    backend = capture.take('successor-backend', 'initial')
    database = backend['database']
    assert isinstance(database, str) and re.fullmatch('[a-z][a-z0-9_]{0,62}', database)
    assert backend == dict(event='successor-backend', stage='initial', database=database,
        pid=control['control_backend_pid'], backend_start=control['control_backend_start'],
        control_epoch=8, present=True)
    guard_tx = raw_cases(guards, plan, prior_control, control, database, server_window)
    assert capture.take('live-activation-start') == dict(event='live-activation-start', mode=mode, control_epoch=8)
    activation = capture.take('live-activation-result')
    assert activation == dict(event='live-activation-result', writer=True, error=None,
        activated_at_us=activation['activated_at_us'])
    after = capture.snapshot('activated-zero')
    receipt = live.activated(pg, after, plan, control, server_window, activation['activated_at_us'])
    assert receipt['activated_at_us'] >= guards['remove_after_us']
    assert int(receipt['activation_transaction']) > int(guard_tx)
    assert receipt['xmin'] not in {r['detail']['receipt']['xmin'] for r in guards['cases']}
    pg = after
    after = capture.snapshot('live-control-refreshed')
    control = refreshed(pg, after, control, server_window)
    assert control['lease_expires_at_us'] - 20_000_000 >= receipt['activated_at_us'], 'postactivation refresh predates activation'
    pg = after
    assert capture.take('live-renewal-result') == dict(event='live-renewal-result', commit_started=True, error=None)
    after = capture.snapshot('live-ingest-renewed')
    previous_ingest = live.only(pg['owner_epochs'], role='ingest', epoch=2)
    ingest = live.only(after['owner_epochs'], role='ingest', epoch=2)
    live.fresh_xmin(ingest['xmin'], pg)
    assert control['lease_expires_at_us'] - 20_000_000 < previous_ingest['lease_expires_at_us']
    wanted = copy.deepcopy(pg)
    live.only(wanted['owner_epochs'], role='ingest', epoch=2).update(
        lease_expires_at_us=control['lease_expires_at_us'], xmin=ingest['xmin'])
    live.same(wanted, after)
    pg = after
    science = live.Science(plan, new, inputs)
    # Scientific bytes retain the immutable plan. The common admission reducer's
    # scheduling ceiling is the separately proved current Ingest deadline.
    science.plan = dict(plan, lease_expires_at_us=ingest['lease_expires_at_us'])
    for stage, n in [('dictionary', 1), ('new-event', 2), ('same-request', 2),
                     ('same-id-alternate-signature', 3)]:
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
        pg = after
    live.same(pg, capture.snapshot('final-before-control-end'))
    assert capture.take('successor-control', 'final', 'released-and-joined') == dict(
        event='successor-control', stage='final', phase='released-and-joined',
        resource=control['resource_id'][2:], epoch=8)
    after = capture.snapshot('final-after-control-end')
    live.ended(pg, after, control, True)
    released = live.only(after['owner_epochs'], role='control', epoch=8)
    live.fresh_xmin(released['xmin'], pg)
    live.clock(released['released_at_us'], server_window)
    live.clock(live.only(after['role_instances'], role_instance_id=control['role_instance_id'])['retired_at_us'], server_window)
    pg = after
    assert capture.take('successor-backend', 'initial') == dict(backend, present=False)
    live.same(pg, capture.snapshot('final'))
    assert capture.take('live-complete') == dict(event='live-complete', mode=mode,
        native='closed', control='released-and-joined', writer=False, admission=True)
    assert capture.position == len(events)
    return dict(status='PASS', mode=mode, snapshots=capture.snapshots, control_tables=72,
        sql_cases=14, positive_cases=1, named_refusals=13, guard_transaction=guard_tx,
        activation_transaction=receipt['activation_transaction'], activated_at_us=receipt['activated_at_us'],
        final_native_rows=69, final_native_sequence=84, final_inputs=7, original_requests_preserved=4,
        new_requests=3, current_ingest_deadline_us=ingest['lease_expires_at_us'],
        backend_absent=dict(backend, present=False), catalog_restored=True,
        finite_import_only=True, raw_running_import=False, product_policy_activation=False,
        network_provenance_off=False, installed_import=False, fresh_process_adoption=False)


def negative_controls(events, original, native, inputs, mode, *, seed, server_window, guards):
    verify(events, original, native, inputs, mode, seed=seed, server_window=server_window, guards=guards)
    rejected = []

    def check(name, mutate, message=None):
        e, g = copy.deepcopy(events), copy.deepcopy(guards)
        mutate(e, g)
        try:
            verify(e, original, native, inputs, mode, seed=seed, server_window=server_window, guards=g)
        except (AssertionError, KeyError, ValueError, TypeError, StopIteration) as error:
            if message is not None:
                assert isinstance(error, AssertionError) and str(error) == message, (name, error)
            rejected.append(name)
        else:
            raise AssertionError('oracle accepted perturbation: ' + name)

    def sql(e, stage, change):
        event = live.only(e, event='successor-state', stage=stage)
        pg = {k: json.loads(v) for k, v in event['state']['control_tables']}
        change(pg)
        event['state']['control_tables'] = [[k, live.encoded(pg[k]).decode()] for k in sorted(pg)]
        event['state']['control_sha256'] = live.digest(live.encoded(event['state']['control_tables']))

    check('sql-case-missing', lambda e, g: g['cases'].pop())
    check('wrong-actual-backend', lambda e, g: g['cases'][0].update(backend_pid=1))
    check('wrong-actual-database', lambda e, g: g['cases'][0].update(database_name='foreign'))
    check('privileged-case-actor', lambda e, g: g['cases'][0].update(database_user='postgres'))
    check('replica-bypass', lambda e, g: g['cases'][0].update(replication_role='replica'))
    check('outer-transaction-substituted', lambda e, g: g['cases'][0].update(transaction_id='1'))
    check('receipt-subtransaction-reused', lambda e, g: g['cases'][1]['detail']['receipt'].update(
        xmin=g['cases'][0]['detail']['receipt']['xmin']))
    check('receipt-subtransaction-is-parent', lambda e, g: g['cases'][0]['detail']['receipt'].update(xmin=g['run']['xmin']))
    check('caller-xid-not-overwritten', lambda e, g: g['cases'][0]['detail']['receipt'].update(activation_transaction='1'))
    check('historical-preparer-substituted', lambda e, g: g['cases'][0]['detail']['receipt'].update(activated_by_epoch=7))
    check('positive-case-skipped', lambda e, g: g['cases'][0]['detail'].update(remaining_us=0))
    check('wrong-named-refusal', lambda e, g: g['cases'][1].update(context='unrelated trigger'))
    check('wrong-refusal-code', lambda e, g: g['cases'][1].update(sqlstate='23503'))
    check('constraint-order-lost', lambda e, g: g['cases'][1]['detail'].update(constraint_order='after-receipt'))
    check('rollback-not-observed', lambda e, g: g['cases'][2]['detail'].update(rolled_back=False))
    check('fixture-repeat', lambda e, g: g['run'].update(executions=2))
    check('fixture-security-definer', lambda e, g: g['fixture_function'].update(security_definer=True))
    check('catalog-product-trigger-disabled', lambda e, g: g['catalog_installed']['triggers'][0].update(enabled='D'))
    check('catalog-not-restored', lambda e, g: g['catalog_removed']['triggers'].append(TRIGGER))
    def different_product_function(e, g):
        for phase in ('catalog_before', 'catalog_installed', 'catalog_removed'):
            g[phase]['functions'][0]['definition'] += '-- unchecked replacement\n'
    check('coherent-product-function-substitution', different_product_function)
    def different_sql(e, g):
        g['install_sql'] = g['install_sql'].replace('SECURITY INVOKER', 'SECURITY DEFINER')
        g['install_sha256'] = live.digest(g['install_sql'].encode())
    check('rehashed-unreviewed-sql', different_sql)
    check('remove-script-unreviewed', lambda e, g: g.update(remove_sql='COMMIT;'))
    check('future-case-clock', lambda e, g: g['cases'][0].update(observed_at_us=server_window['after_us'] + 1))
    def guard_backdated(e, g):
        expiry = g['control']['acquired_at_us'] + 20_000_000
        def change(p):
            live.only(p['owner_epochs'], role='control', epoch=8)['lease_expires_at_us'] = expiry
        for phase in ('after_refresh', 'clean'):
            change(g[phase])
        for stage in ('guards-refreshed', 'guards-clean', 'activated-zero'):
            sql(e, stage, change)
        plan = live.only(e, event='successor-plan')
        for case in g['cases']:
            detail = case['detail']
            detail['remaining_us'] = min(expiry, plan['lease_expires_at_us']) - detail['receipt']['activated_at_us'] - 1
    check('coherent-guard-refresh-backdated', guard_backdated, 'guard refresh predates gate release')
    def activation_backdated(e, g):
        expiry = live.only(e, event='live-activation-result')['activated_at_us'] - 1 + 20_000_000
        for stage in ('live-control-refreshed', 'live-ingest-renewed', 'dictionary',
                'new-event', 'same-request', 'same-id-alternate-signature',
                'final-before-control-end', 'final-after-control-end', 'final'):
            def change(p):
                live.only(p['owner_epochs'], role='control', epoch=8)['lease_expires_at_us'] = expiry
                if stage != 'live-control-refreshed':
                    live.only(p['owner_epochs'], role='ingest', epoch=2)['lease_expires_at_us'] = expiry
            sql(e, stage, change)
    check('coherent-postactivation-refresh-backdated', activation_backdated, 'postactivation refresh predates activation')
    check('renewal-not-committed', lambda e, g: live.only(e, event='live-renewal-result').update(commit_started=False))
    check('renewal-changes-intent', lambda e, g: sql(e, 'live-ingest-renewed',
        lambda p: live.only(p['initial_ingest_successions']).update(lease_expires_at_us=1)))
    check('renewal-wrong-deadline', lambda e, g: sql(e, 'live-ingest-renewed',
        lambda p: live.only(p['owner_epochs'], role='ingest', epoch=2).update(lease_expires_at_us=1)))
    check('future-control-renewal-clock', lambda e, g: sql(e, 'live-control-refreshed',
        lambda p: live.only(p['owner_epochs'], role='control', epoch=8).update(lease_expires_at_us=(1 << 63) - 1)))
    check('renewal-rewrites-activation', lambda e, g: sql(e, 'live-ingest-renewed',
        lambda p: live.only(p[live.ACTIVATIONS]).update(xmin='999999999')))
    check('guard-refresh-persists-activation', lambda e, g: sql(e, 'guards-refreshed',
        lambda p: p[live.ACTIVATIONS].append(g['cases'][0]['detail']['receipt'])))
    check('old-original-xmin-rewritten', lambda e, g: sql(e, 'final',
        lambda p: live.only(p['producer_outcomes'], producer_epoch=1, request_sequence=1).update(xmin='999999999')))
    check('same-request-receipt-changed', lambda e, g: live.only(e, event='live-receipt', stage='same-request').update(receipt='00'))
    check('wrong-backend-absence', lambda e, g: live.only(e, event='successor-backend', present=False).update(pid=1))
    check('native-owner-not-closed', lambda e, g: live.only(e, event='live-complete').update(native='open'))
    return dict(status='PASS', rejected=rejected)
