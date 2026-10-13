"""Schema36 HTTP acceptance/claim with independently validated recovery-ACL removal.

Expected native/request/receipt bytes and full SQL row deltas reuse semantic
reducers. No captured transcript is rewritten into another qualification mode.
"""
import copy
import json
import math
import re

from installed_live_oracle import (ACTIVATIONS, NetworkScience, add_http, admission_rows,
    backend_checked, catalog_identity, clock, decode, digest, encoded, fresh_xmin,
    native_plan, native_rows, only, policy_setup, renewed, same, sha, startup, wire_checked)
from initial_live_oracle import check_window

MODE = 'activated-acquisition-http'
REVOKE = """REVOKE ALL PRIVILEGES ON TABLE control.acquisition_gaps FROM bigbrotr_control;
REVOKE EXECUTE ON FUNCTION control.guard_acquisition_gap(),control.require_acquisition_gap_complete(),control.require_activated_initial_recovery(bytea),control.require_activated_initial_source(bytea) FROM bigbrotr_control;
"""
RESTORE = """GRANT SELECT,INSERT ON TABLE control.acquisition_gaps TO bigbrotr_control;
GRANT EXECUTE ON FUNCTION control.guard_acquisition_gap(),control.require_acquisition_gap_complete() TO bigbrotr_control;
"""
MIGRATIONS = {
    33: ('0033_activated_initial_recovery.sql', '4f22eb019e831126bf3f5c712d0ddf1e8213dfdfcdf9afaf5b38c80418cb5804'),
    34: ('0034_activated_control_retirement.sql', 'd3ccd19efd29a5c5db849c8bd21db45e4699076122d0bb6cd659fdd736d0b87a'),
    35: ('0035_activated_source_closure.sql', '80b5441cbb2de1f782ce6458e1c2b26a2f3730f51e61619af49f1e8d8adc375a'),
    36: ('0036_activated_acquisition_abandonment.sql', '19e2390b46d9eba7e4eac765eafc30fc7b2cac03e03c0918df64ed0b87642e54'),
}


def catalog36(value):
    normalized = copy.deepcopy(value)
    identities = set()
    for row in normalized['triggers']:
        key = row['table'], row['name']
        assert key not in identities
        identities.add(key)
        if row['internal']:
            assert re.fullmatch('RI_ConstraintTrigger_[ac]_[1-9][0-9]*', row['name'])
            row['name'] = row['name'].rsplit('_', 1)[0]
    normalized['triggers'].sort(key=encoded)
    # Qualified1d64cd7 schema36 catalog; new33/35 EXECUTE rights are set
    # false as migration REVOKE PUBLIC requires. Reviewed prior context SHA:
    # cf620280a0f43d526116ad504aca1fa95cbb46f74669db47f424aeb1ea5bc7b2.
    assert digest(encoded(normalized)) == '9b03d68945b089e57fa8ca9023d6ce5b61ce10d55dccdd2bb0a6ff7253484bc6'


def migration_checked(capture, migration):
    seed, setup, original, window = (capture[k] for k in ('seed', 'setup', 'original', 'server_window'))
    catalog_identity(migration['catalog_before'])
    assert setup['guards_before'] == {k:v for k,v in migration['catalog_before'].items() if k not in ('functions','tables')}
    policy_setup(seed, setup, migration['before'], window, False)
    check_window(migration['server_window'])
    assert setup['server_window']['after_us'] <= migration['server_window']['before_us']
    assert migration['server_window']['after_us'] <= window['before_us']
    assert len(migration['responses']) == 2
    for answer in migration['responses']:
        assert answer['returncode'] == 0 and answer['stderr'] == ''
    assert migration['physical_before'] == migration['physical_after']
    wanted = copy.deepcopy(migration['before'])
    template = only(wanted['schema_versions'], migration=32)
    tx = only(original['schema_versions'], migration=33)['xmin']
    fresh_xmin(tx, wanted)
    last = migration['server_window']['before_us']
    for number,(name,checksum) in MIGRATIONS.items():
        row = only(original['schema_versions'], migration=number)
        at = row['applied_at_us']
        clock(at, migration['server_window']); assert last <= at
        last = at
        wanted['schema_versions'].append(dict(template, migration=number, name=name,
            checksum='\\x'+checksum, applied_at_us=at, xmin=tx))
    for observed in (migration['after'], migration['reapplied'], original):
        same(wanted, observed)
    catalog36(migration['catalog_after'])


def acl_checked(capture, migration, acl):
    value = acl['revocation']
    assert value == capture['fixture']['after_activated']
    assert value['statement'] == REVOKE and value['activated'] == capture['snapshots'][0]
    assert value['before'] == migration['catalog_after']
    assert value['response'] == dict(returncode=0, stdout='', stderr='', outcome='PROCESS_EXITED')
    check_window(value['server_window'])
    assert capture['snapshots'][0]['observed_by_us'] <= value['server_window']['before_us']
    assert value['server_window']['after_us'] <= capture['snapshots'][1]['server_us']
    assert value['server_window']['after_us'] <= only(capture['snapshots'][1]['control']['browser_sessions'])['issued_at_us']
    wanted = copy.deepcopy(value['before'])
    for key in ('gap_select','gap_insert','gap_update','gap_delete','gap_truncate'):
        wanted['acquisition']['privileges'][key] = False
    for function in wanted['acquisition']['functions']:
        if function['name'] in ('control.guard_acquisition_gap()','control.require_acquisition_gap_complete()'):
            function['execute'] = False
    assert value['after'] == acl['final'] == wanted
    assert acl['restoration'] == dict(statement=RESTORE,
        response=dict(returncode=0, stdout='', stderr='', outcome='PROCESS_EXITED'),
        catalog=value['before'], errors=[])
    # The shared capture's original14-function catalog is a strict subset; none
    # of those live-admission permissions were removed by this fixture.
    base = copy.deepcopy(value['before']); base.pop('acquisition')
    base['functions'] = [f for f in base['functions'] if f['name'] not in (
        'control.require_activated_initial_recovery(bytea)','control.require_activated_initial_source(bytea)')]
    assert capture['catalog_before'] == capture['catalog_after'] == base


def verify(*, capture, migration, acl):
    mode, original, after, native_before, native_after, inputs_before, inputs_after = (
        capture[k] for k in ('mode','original','after','native_before','native_after','inputs_before','inputs_after'))
    input_contents, snapshots, response, journey, fixture, server_window, backend, seed = (
        capture[k] for k in ('input_contents','snapshots','response','journey','fixture','server_window','backend','seed'))
    assert mode == MODE and capture['predecessor'] is None
    check_window(server_window)
    assert len(original) == len(after) == 72
    migration_checked(capture, migration); acl_checked(capture, migration, acl)
    assert native_before['event'] == 'dump-state' and native_before['sequence'] == 58
    assert native_rows(native_before['rows']) == native_rows(seed['native']['rows'])
    assert inputs_before == seed['inputs'] and fixture['cleanup_errors'] == []
    terminal = response['terminal']
    assert response['outcome'] == 'PROCESS_EXITED' and terminal['reaped'] is True and terminal['forced'] is False
    assert response['returncode'] == terminal['exit_code']
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert 0 <= terminal['elapsed_seconds'] <= 60 and math.isfinite(terminal['elapsed_seconds'])
    database = backend['absence']['database']
    assert re.fullmatch('[a-z][a-z0-9_]{0,62}', database)
    assert terminal['config'] == '/lab/' + database + '.toml' and terminal['prefix'] == '/lab/' + mode
    assert terminal['command'] == ['/build/bigbrotr','serve','--file',terminal['config'],
        '--activate-initial-successor','--roles','control,ingest,sync']
    reports = [json.loads(line) for line in response['stdout'].splitlines()]
    assert response['returncode'] == 0 and response['stderr'] == '' and len(reports) == 2
    assert fixture['holder'] is None
    report, ended = reports
    assert ended == dict(format='bigbrotr-initial-successor/1', state='stopped', native='closed',
        owner_release='committed', source_closure='not-performed', producer_epoch='2', ready=False)
    assert [s['stage'] for s in snapshots] == ['activated', 'prepared', 'submitted', 'renewed', 'before-stop']
    previous_clock = server_window['before_us']
    for s in snapshots:
        assert set(s) == {'stage', 'server_us', 'observed_by_us', 'control', 'inputs'}
        assert previous_clock <= s['server_us'] <= s['observed_by_us'] <= server_window['after_us']
        previous_clock = s['observed_by_us']
        assert len(s['control']) == 72
    active = snapshots[0]['control']
    plan, prepared_rows = native_plan(seed, active, report)
    control = startup(original, active, dict(report, _plan=plan),
        dict(before_us=server_window['before_us'], after_us=snapshots[0]['observed_by_us']))
    activation = only(active[ACTIVATIONS])
    assert report == dict(format='bigbrotr-initial-successor/1', state='activated',
        dataset_id=plan['dataset'], owner_resource_id=plan['resource'], producer_slot_id=plan['producer_slot'],
        producer_epoch='2', process_incarnation=plan['target_incarnation'], topology_generation='1',
        config_revision='2', policy_revision='2', config_sha256=plan['config_digest'], policy_sha256=plan['policy_digest'],
        outstanding_request_limit='2', created_at_us=str(plan['created_at_us']),
        initial_lease_expires_at_us=str(plan['lease_expires_at_us']), activated_at_us=str(activation['activated_at_us']),
        predecessor=dict(producer_epoch='1', issued_through='4', reserved_through='4', resolved_requests='4',
            last_complete_sequence='12', sha256=plan['closure_digest']),
        native_profile=1, native_allowance_bytes=134217728, acquisition='finite-initial-successor', ready=False,
        listeners=[dict(kind='private-session-http', host='127.0.0.1', port=18081, profile='initial-source-commands')])
    first = wire_checked(journey, fixture, report, snapshots[1]['control'], server_window)
    science = NetworkScience(plan, prepared_rows, seed['inputs'])
    final_rows = native_rows(native_after['rows'])
    n = 1
    def fact_scalar(number):
        value = final_rows['facts', b'\x01\x20' + science.src_key(number)]
        assert value[:2] == b'\x01\x20' and value[34:].startswith(b'bigbrotr/fact/1\0')
        return decode(value[34:].split(b'\0', 1)[1])
    dictionary_at, recorded = fact_scalar(1)[4], fact_scalar(n + 1)[4]
    receive = fact_scalar(n + 4)
    received, resolution = receive[5][1], receive[5][3][1]
    assert type(resolution) is int and 0 < resolution <= 1_000_000
    assert resolution == first['clock_resolution_us']
    assert first['sent_at_us'] - resolution <= received <= first['closed_at_us'] + resolution
    # The peer's EOF timestamp need not precede the product's next task.
    # Receive precedes fact construction; both remain inside the wire/run bounds.
    assert received <= recorded
    for timestamp in (dictionary_at, recorded, received):
        clock(timestamp, server_window)
    assert activation['activated_at_us'] <= dictionary_at <= first['request_at_us'] <= recorded
    science.network(False, dictionary_at, received, recorded, resolution)
    assert final_rows == science.rows and native_after['event'] == 'dump-state'
    assert native_after['sequence'] == science.sequence
    assert inputs_after == science.inputs
    previous = active
    assert snapshots[0]['inputs'] == seed['inputs']
    for s in snapshots[1:] + [dict(stage='final', control=after, inputs=inputs_after,
            server_us=server_window['after_us'], observed_by_us=server_window['after_us'])]:
        phase = s['stage']
        state = s['control']
        window = dict(before_us=server_window['before_us'], after_us=s['observed_by_us'])
        wanted = copy.deepcopy(active)
        renewed(wanted, state, previous, window, stopped=phase == 'final')
        scope, enrollment = add_http(wanted, state, original, journey['result'], phase, window, control)
        if phase != 'prepared':
            admission_rows(wanted, state, science, scope, enrollment, control, window)
        same(wanted, state)
        if previous is not active:
            mutable = {'owner_epochs', 'role_instances', 'browser_sessions', 'producer_epochs'}
            for table in state:
                if table not in mutable:
                    assert all(row in state[table] for row in previous[table]), (phase, table, 'changed retained row')
            old_session, new_session = only(previous['browser_sessions']), only(state['browser_sessions'])
            changed_session_fields = {'last_seen_at_us', 'idle_expires_at_us', 'revoked_at_us', 'xmin'}
            assert {k: v for k, v in old_session.items() if k not in changed_session_fields} == {
                k: v for k, v in new_session.items() if k not in changed_session_fields}
        if phase in ('before-stop', 'final'):
            same({k: v for k, v in previous.items() if k not in ('owner_epochs', 'role_instances')},
                {k: v for k, v in state.items() if k not in ('owner_epochs', 'role_instances')})
        assert s['inputs'] == (seed['inputs'] if phase == 'prepared' else science.inputs)
        if phase not in ('prepared', 'submitted'):
            previous_session = only(previous['browser_sessions'])
            current_session = only(state['browser_sessions'])
            assert current_session['last_seen_at_us'] >= previous_session['last_seen_at_us']
        previous = state
    renewal = fixture['renewal']
    assert renewal['current'] == snapshots[3]
    target_initial = only(active['owner_epochs'], role='ingest', epoch=2)
    assert renewal['initial_target'] == target_initial
    assert renewal['initial_deadline_us'] == plan['lease_expires_at_us']
    assert plan['lease_expires_at_us'] < renewal['observed_at_us'] <= snapshots[3]['server_us']
    for role, epoch in [('ingest', 2), ('control', 8)]:
        current = only(snapshots[3]['control']['owner_epochs'], role=role, epoch=epoch)
        assert current['state'] == 'active' and current['lease_expires_at_us'] > snapshots[3]['observed_by_us']
    assert fixture['signal']['kind'] == 'SIGTERM'
    assert fixture['signal']['identity'] == {k: terminal[k] for k in ('pid', 'config', 'prefix', 'command', 'start_ticks')}
    assert fixture['client_terminal'] == journey['terminal']
    assert set(input_contents) == set(inputs_after)
    for name, content in input_contents.items():
        raw = bytes.fromhex(content)
        assert inputs_after[name] == dict(bytes=len(raw), sha256=digest(raw))
        if '-0000000000000002-' in name:
            assert raw == science.requests[int(name[-21:-5], 16)]
    backend_checked(backend, control, database, server_window)
    return dict(status='PASS', mode=mode, snapshots=len(snapshots), sql_tables=72,
        native_rows=len(native_after['rows']), native_sequence=native_after['sequence'],
        original_inputs=len(inputs_before), final_inputs=len(inputs_after), actual_cli=True,
        grants='recovery privileges absent during HTTP and claim',
        certification_per_resend='not-observed', completion='not-qualified')


def negative_controls(**context):
    verify(**context)
    rejected = []
    def case(name, mutate):
        value = copy.deepcopy(context); mutate(value)
        try: verify(**value)
        except (AssertionError, KeyError, ValueError, TypeError, IndexError, StopIteration):
            rejected.append(name); return
        raise AssertionError('HTTP privilege counterexample accepted: '+name)
    def cap(c): return c['capture']
    case('unreaped', lambda c: cap(c)['response']['terminal'].update(reaped=False))
    case('backend-present', lambda c: cap(c)['backend']['exact'][0].update(present=True))
    case('wrong-database', lambda c: cap(c)['response']['terminal'].update(config='/lab/wrong.toml'))
    case('native-extra-write', lambda c: cap(c)['native_after'].update(sequence=85))
    case('changed-original-input', lambda c: cap(c)['input_contents'].update({next(iter(cap(c)['seed']['inputs'])):'00'}))
    case('old-source-cut', lambda c: only(cap(c)['after']['source_streams'],owner_epoch=1).update(last_complete_sequence=13))
    case('recovery-gap-written', lambda c: cap(c)['after']['acquisition_gaps'].append(copy.deepcopy(cap(c)['after']['acquisition_gaps'][0])))
    case('schema-ledger-corrupt', lambda c: only(cap(c)['original']['schema_versions'],migration=36).update(checksum='\\x'+'00'*32))
    case('schema-body-corrupt', lambda c: c['migration']['catalog_after']['acquisition']['functions'][0].update(definition='SELECT 1'))
    def changed_acl(c, change):
        for catalog in (c['acl']['final'], c['acl']['revocation']['after'],
                cap(c)['fixture']['after_activated']['after']):
            change(catalog)
    case('gap-select-restored-early', lambda c: changed_acl(c,
        lambda catalog: catalog['acquisition']['privileges'].update(gap_select=True)))
    case('gap-function-restored-early', lambda c: changed_acl(c,
        lambda catalog: only(catalog['acquisition']['functions'],name='control.guard_acquisition_gap()').update(execute=True)))
    case('recovery-validator-granted', lambda c: changed_acl(c,
        lambda catalog: only(catalog['functions'],name='control.require_activated_initial_source(bytea)').update(execute=True)))
    case('grant-not-restored', lambda c: c['acl']['restoration']['catalog']['acquisition']['privileges'].update(gap_insert=False))
    def late_removal(c):
        at = only(cap(c)['snapshots'][1]['control']['browser_sessions'])['issued_at_us']+1
        c['acl']['revocation']['server_window']['after_us'] = at
        cap(c)['fixture']['after_activated']['server_window']['after_us'] = at
    case('privilege-removal-after-login', late_removal)
    case('http-accepted-wrong-body', lambda c: cap(c)['journey']['result']['observations'][3]['body'].update(authority='execution'))
    case('http-job-not-claimed', lambda c: only(cap(c)['after']['jobs'],request_key='\\x'+'f4'*16).update(state='accepted',job_revision=1))
    case('http-session-not-revoked', lambda c: only(cap(c)['after']['browser_sessions']).update(revoked_at_us=None))
    case('policy-drift', lambda c: only(cap(c)['after']['datasets']).update(policy_revision=1))
    case('old-window', lambda c: only(cap(c)['after']['producer_epochs'],producer_epoch=2).update(outstanding_request_limit=32))
    case('receipt-forgery', lambda c: only(cap(c)['after']['producer_outcomes'],producer_epoch=2,request_sequence=2).update(outcome_bytes='\\x00'))
    case('job-link-missing', lambda c: only(cap(c)['after']['producer_requests'],producer_epoch=2,request_sequence=2).update(acquisition_job_id=None))
    case('predecessor-wire-substitution', lambda c: cap(c)['journey']['closed'][0]['event_ids'].__setitem__(1,'00'*32))
    case('premature-renewal', lambda c: cap(c)['fixture']['renewal'].update(observed_at_us=cap(c)['fixture']['renewal']['initial_deadline_us']))
    case('quiet-socket-closed-before-signal', lambda c: cap(c)['fixture']['signal'].update(closed_before_signal=True))
    return dict(status='PASS', rejected=len(rejected), cases=rejected)
