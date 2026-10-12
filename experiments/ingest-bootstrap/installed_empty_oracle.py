"""Independent whole-state observations of the installed empty retirement CLI.

The CLI emits one terminal report, not the finite probe's intermediate events.
Only its measured before/after states are checked here. Aggregate Control changes
have a server-clock envelope; no per-statement renewal history is invented.
"""
import copy
import json

import initial_empty_oracle as empty
from initial_empty_locks_oracle import guards, identity, microseconds, sql_text
from initial_history_oracle import inside_window
from initial_succession_oracle import exclusion, native_rows, only, same


PREFIX = 'installed-empty-'
SUCCESS = ('normal', 'terminal-retry', 'fresh-completion')
NO_CLAIM = ('default', 'held', 'signal', 'deadline')
FOLLOWUPS = {'terminal-retry': 'normal', 'fresh-completion': 'native-first-failure'}
ERRORS = {
    'default': ('ingest registration failed: InvalidInput',),
    'held': ('native store inspection or exclusive open failed',),
    'missing-input': ('producer operation unavailable: Native(Input(Missing))',),
    'missing-outcome': ('producer operation unavailable: Control(Backend)',),
    'old-schema': ('producer operation unavailable: Control(IncompatibleDeployment)',),
    'native-first-failure': ('producer operation unavailable: Control(Backend)',),
    'signal': ('initial Ingest cancelled; original attempt may need recovery',),
    # The independent supervisor and the worker timeout race at the same bound.
    'deadline': ('initial Ingest deadline; original attempt may need recovery',
        'initial Ingest cancelled; original attempt may need recovery'),
}
DATASET_WAIT = ('SELECT substring(dataset_id FROM 1 FOR 17), '
    'substring(instance_id FROM 1 FOR 17) FROM control.datasets LIMIT 2 FOR UPDATE')
FAULT_INSTALL = """CREATE SCHEMA empty_retirement_fixture;
CREATE SEQUENCE empty_retirement_fixture.source_hit;
GRANT USAGE ON SCHEMA empty_retirement_fixture TO bigbrotr_control;
GRANT USAGE ON SEQUENCE empty_retirement_fixture.source_hit TO bigbrotr_control;
CREATE FUNCTION empty_retirement_fixture.reject_source() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
    PERFORM nextval('empty_retirement_fixture.source_hit');
    RAISE EXCEPTION 'qualification empty retirement source rollback' USING ERRCODE='P0001';
END $$;
CREATE TRIGGER fixture_empty_source_rollback BEFORE UPDATE ON control.source_streams
    FOR EACH ROW WHEN (NEW.owner_epoch=2 AND OLD.state='open' AND NEW.state='closing')
    EXECUTE FUNCTION empty_retirement_fixture.reject_source();"""
FAULT_REMOVE = """DROP TRIGGER fixture_empty_source_rollback ON control.source_streams;
DROP FUNCTION empty_retirement_fixture.reject_source();
DROP SEQUENCE empty_retirement_fixture.source_hit;
DROP SCHEMA empty_retirement_fixture;"""
OUTCOME_REMOVE = ('BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM '
    'control.producer_outcomes WHERE producer_epoch=1 AND request_sequence=3; COMMIT;')


def window(value):
    assert set(value) == {'before_us', 'after_us'}
    assert all(type(n) is int and n > 0 for n in value.values())
    assert 0 <= value['after_us'] - value['before_us'] <= 120_000_000


def native(value):
    assert set(value) == {'event', 'rows', 'sequence'} and value['event'] == 'dump-state'
    assert type(value['sequence']) is int and value['sequence'] >= 0
    return native_rows(value['rows'])


def fresh_xmin(value, before):
    assert isinstance(value, str) and value.isdecimal() and int(value) > 0
    assert all(value != r['xmin'] for rows in before.values() for r in rows)


def fresh_identity(value):
    assert isinstance(value, str) and value.startswith('\\x') and len(value) == 34
    assert bytes.fromhex(value[2:]).hex() == value[2:] and int(value[2:], 16) != 0


def baseline(before, native_before, inputs_before, mode, seed, known, server_window,
             predecessor):
    if mode in FOLLOWUPS:
        assert predecessor is not None and predecessor['mode'] == PREFIX + FOLLOWUPS[mode]
        assert predecessor['seed'] == seed
        verify(**predecessor)
        assert predecessor['server_window']['after_us'] <= server_window['before_us']
        same(predecessor['after'], before)
        assert native_before == predecessor['native_after']
        assert inputs_before == predecessor['inputs_after'] == seed['inputs']
        return
    assert predecessor is None
    wanted = copy.deepcopy(seed['control'])
    if mode != 'old-schema':
        wanted = empty.migration(wanted, before, server_window)
    if mode == 'missing-outcome':
        wanted['producer_outcomes'].remove(only(wanted['producer_outcomes'], request_sequence=3))
    originals = copy.deepcopy(seed['inputs'])
    if mode == 'missing-input':
        del originals[known['registration']['producer_slot'] + '-0000000000000001-0000000000000004.cbor']
    same(wanted, before)
    assert native(native_before) == known['opened'] and native_before['sequence'] == 62
    assert inputs_before == originals


def control_delta(before, after, mode, server_window):
    """Validate exact aggregate lifecycle fields before removing those rows.

This projection separates already checked Control writes from the thirteen
target rows. It neither synthesizes nor claims an intermediate SQL observation.
    """
    if mode in NO_CLAIM:
        return copy.deepcopy(after), None
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'),
        key=lambda r: r['epoch'])
    current = only(after['owner_epochs'], role='control', epoch=previous['epoch'] + 1)
    role = only(after['role_instances'], role_instance_id=current['role_instance_id'])
    for value in (current['role_instance_id'], current['process_incarnation']):
        fresh_identity(value)
    assert not any(r['role_instance_id'] == current['role_instance_id'] for r in before['role_instances'])
    assert not any(r['process_incarnation'] == current['process_incarnation'] for r in before['owner_epochs'])
    assert type(current['control_backend_pid']) is int and current['control_backend_pid'] > 0
    inside_window(microseconds(current['control_backend_start']), server_window)
    inside_window(role['created_at_us'], server_window)
    inside_window(current['acquired_at_us'], server_window)
    assert role['created_at_us'] <= current['acquired_at_us']
    assert microseconds(current['control_backend_start']) <= current['acquired_at_us']
    renewed = current['lease_expires_at_us'] - 20_000_000
    inside_window(renewed, server_window)
    assert renewed >= current['acquired_at_us']
    fresh_xmin(current['xmin'], before)
    fresh_xmin(role['xmin'], before)
    released = mode in SUCCESS
    if released:
        assert current['xmin'] == role['xmin']
        inside_window(current['released_at_us'], server_window)
        inside_window(role['retired_at_us'], server_window)
        assert renewed <= current['released_at_us'] <= role['retired_at_us']
    else:
        assert current['released_at_us'] is None and role['retired_at_us'] is None
        if mode in ('old-schema', 'missing-outcome'):
            assert current['xmin'] == role['xmin'] and renewed == current['acquired_at_us']
        else:
            assert current['xmin'] != role['xmin'], 'replay refresh must leave a different row version'
    wanted = dict(previous, epoch=previous['epoch'] + 1,
        role_instance_id=current['role_instance_id'], process_incarnation=current['process_incarnation'],
        acquired_at_us=current['acquired_at_us'], lease_expires_at_us=current['lease_expires_at_us'],
        state='released' if released else 'active', released_at_us=current['released_at_us'],
        exclusion_method='storage_fenced' if released else None,
        exclusion_digest=exclusion(current, current['process_incarnation']) if released else None,
        archive_frontier_protocol=None, control_fence_version=1,
        control_backend_pid=current['control_backend_pid'],
        control_backend_start=current['control_backend_start'], xmin=current['xmin'])
    assert current == wanted
    assert role == dict(role_instance_id=current['role_instance_id'], dataset_id=previous['dataset_id'],
        role='control', created_at_us=role['created_at_us'], retired_at_us=role['retired_at_us'], xmin=role['xmin'])
    projected = copy.deepcopy(after)
    projected['owner_epochs'].remove(current)
    projected['role_instances'].remove(role)
    if previous['state'] == 'active':
        assert mode == 'fresh-completion'
        fenced = only(after['owner_epochs'], role='control', epoch=previous['epoch'])
        old_role = only(before['role_instances'], role_instance_id=previous['role_instance_id'])
        retired = only(after['role_instances'], role_instance_id=previous['role_instance_id'])
        fresh_xmin(fenced['xmin'], before)
        assert fenced['xmin'] == retired['xmin'] and fenced['xmin'] != current['xmin']
        inside_window(fenced['released_at_us'], server_window)
        inside_window(retired['retired_at_us'], server_window)
        assert previous['acquired_at_us'] <= fenced['released_at_us'] <= retired['retired_at_us'] <= role['created_at_us']
        assert fenced == dict(previous, state='released', released_at_us=fenced['released_at_us'],
            exclusion_method='storage_fenced', exclusion_digest=exclusion(previous, current['process_incarnation']),
            xmin=fenced['xmin'])
        assert retired == dict(old_role, retired_at_us=retired['retired_at_us'], xmin=fenced['xmin'])
        projected['owner_epochs'][projected['owner_epochs'].index(fenced)] = previous
        projected['role_instances'][projected['role_instances'].index(retired)] = old_role
    else:
        assert previous['state'] == 'released' and previous['released_at_us'] <= role['created_at_us']
    return projected, current


def backend_observations(value, current, mode, server_window):
    assert set(value) == {'database', 'expected', 'observations', 'absence'}
    database = value['database']
    assert isinstance(database, str) and 0 < len(database) <= 63
    assert database.isascii() and database.replace('_', '').isalnum()
    expected, observations = value['expected'], value['observations']
    if mode in ('default', 'held'):
        assert expected == observations == []
    else:
        target, = expected
        observed, = observations
        assert set(target) == {'pid', 'backend_start', 'database', 'control_epoch'}
        assert target['database'] == database and type(target['pid']) is int and target['pid'] > 0
        inside_window(microseconds(target['backend_start']), server_window)
        if current is None:
            assert mode in ('signal', 'deadline') and target['control_epoch'] is None
        else:
            identity(target, dict(pid=current['control_backend_pid'],
                backend_start=current['control_backend_start'], database=database))
            assert target['control_epoch'] == current['epoch']
        assert observed == dict(target, present=False)
    absence = value['absence']
    assert absence['database'] == database and absence['user'] == 'bigbrotr_control'
    assert absence['status'] == 'OBSERVED_ABSENT' and absence['limit_seconds'] == 5
    assert 0 <= absence['seconds'] <= 5 and absence['attempts']
    for sample in absence['attempts']:
        assert 0 <= sample['seconds'] <= 5
        assert type(sample['count']) is int and sample['count'] == len(sample['backends'])
    assert absence['attempts'][-1]['count'] == 0


def expected_report(before, known, retired_at):
    intent = only(before['initial_ingest_successions'])
    return dict(format='bigbrotr-ingest-recovery/1', state='empty-attempt-retired',
        dataset_id=intent['dataset_id'][2:], owner_resource_id=intent['owner_resource_id'][2:],
        producer_slot_id=intent['producer_slot_id'][2:], producer_epoch='2', issued_through='0',
        reserved_through='0', resolved_original_requests='4',
        original_source_closure=dict(producer_epoch='1', issued_through='4', resolved_requests='4',
            last_complete_sequence='12', sha256=known['proof']['closure_digest']),
        empty_attempt_retirement=dict(owner_epoch='2', incarnation=intent['target_incarnation'][2:],
            retired_at_us=str(retired_at), sha256=known['empty_digest'], allocation_domains='9',
            native_acknowledged='0', native_retired='0', owner_release='committed'),
        native='closed', owner_release='committed', ready=False)


def response_checked(response, mode, wanted):
    assert response['outcome'] == 'PROCESS_EXITED'
    code = 0 if mode in SUCCESS else 4
    assert type(response['returncode']) is int and response['returncode'] == code
    assert 0 <= response['seconds'] <= 120
    terminal = response['terminal']
    assert terminal['exit_code'] == code and terminal['forced'] is False and terminal['reaped'] is True
    assert type(terminal['pid']) is int and terminal['pid'] > 0
    assert type(terminal['start_ticks']) is int and terminal['start_ticks'] > 0
    assert 0 <= terminal['elapsed_seconds'] <= 120
    config, prefix = terminal['config'], terminal['prefix']
    assert isinstance(config, str) and config.startswith('/lab/') and config.endswith('.toml')
    assert config.count('/') == 2 and '..' not in config
    assert isinstance(prefix, str) and prefix.startswith('/lab/' + PREFIX)
    assert prefix.count('/') == 2 and '..' not in prefix
    label = {'default': 'normal-default', 'terminal-retry': 'normal-terminal',
        'fresh-completion': 'native-first-failure-recovered'}.get(mode, mode)
    assert prefix == '/lab/' + PREFIX + label
    command = ['/build/bigbrotr', 'ingest', 'recover', '--file', config]
    if mode != 'default':
        command.append('--retire-empty-attempt')
    assert terminal['command'] == command
    if mode in SUCCESS:
        assert response['stderr'] == ''
        lines = response['stdout'].splitlines()
        assert len(lines) == 1
        def unique(pairs):
            assert len(pairs) == len(dict(pairs)), 'duplicate report field'
            return dict(pairs)
        report = json.loads(lines[0], object_pairs_hook=unique)
        assert json.dumps(report, sort_keys=True) == json.dumps(wanted, sort_keys=True)
    else:
        assert response['stdout'] == ''
        assert response['stderr'] in ['error: ' + error + '\n' for error in ERRORS[mode]]


def fixture_checked(setup, mode, before, inputs, known, backend, response, server_window, seed):
    assert set(setup) == {'guards_before', 'guards_after', 'removed_input', 'fixture'}
    catalog = setup['guards_before']
    assert setup['guards_after'] == catalog
    assert catalog['session_replication_role'] == 'origin'
    assert catalog['triggers'] and all(t['enabled'] in ('O', 'A') for t in catalog['triggers'])
    assert catalog['constraints'] and all(c['validated'] is True for c in catalog['constraints'])
    assert len({(t['table'], t['name']) for t in catalog['triggers']}) == len(catalog['triggers'])
    if mode != 'old-schema':
        guards(catalog)
    else:
        assert not any(t['name'].startswith('initial_empty_') for t in catalog['triggers'])
    removed = setup['removed_input']
    if mode == 'missing-input':
        name = known['registration']['producer_slot'] + '-0000000000000001-0000000000000004.cbor'
        assert set(removed) == {'name', 'bytes', 'sha256'} and removed['name'] == name
        assert name not in inputs and removed == dict(name=name, **seed['inputs'][name])
    else:
        assert removed is None
    fixture = setup['fixture']
    assert fixture['cleanup_errors'] == []
    expected_kind = {'native-first-failure': 'native-first-sql-failure',
        'fresh-completion': 'fault-removed', 'held': 'native-held',
        'signal': 'sql-wait', 'deadline': 'sql-wait', 'missing-outcome': 'missing-outcome'}.get(mode, 'none')
    assert fixture['kind'] == expected_kind
    fault_triggers = [t for t in catalog['triggers'] if t['name'].startswith('fixture_')]
    if mode == 'native-first-failure':
        artifact = fixture['installed']
        assert sql_text(artifact['statement']) == sql_text(FAULT_INSTALL)
        assert artifact['hit'] is None and artifact['guards'] == catalog
        assert fixture['hit'] == '1true'
        assert fault_triggers == [dict(table='source_streams', name='fixture_empty_source_rollback',
            enabled='O', internal=False, function='empty_retirement_fixture.reject_source()')]
    else:
        assert fault_triggers == []
    if mode == 'fresh-completion':
        artifact = fixture['removed']
        assert sql_text(artifact['statement']) == sql_text(FAULT_REMOVE)
        assert artifact['hit'] == '1true' and artifact['guards'] == catalog
    if mode == 'missing-outcome':
        assert fixture['statement'] == OUTCOME_REMOVE
    if mode == 'held':
        command = fixture['command']
        assert len(command) == 11 and command[:3] == ['docker', 'exec', '-i']
        assert isinstance(command[3], str) and command[3]
        assert command[4:8] == ['/build/ingest_bootstrap_probe', 'history-hold',
            backend['database'], '/run/secrets/application']
        assert command[8] == '/lab/installed-empty-held/store'
        dataset = only(before['datasets'])
        assert command[9:] == [dataset['dataset_id'][2:], dataset['instance_id'][2:]]
        assert fixture['ready'] == {'event': 'native-held'}
        assert json.loads(fixture['stdout']) == fixture['ready'] and fixture['stderr'] == ''
        assert fixture['alive_during_cli'] is True and fixture['exit_code'] == 0
        assert fixture['joined'] is True and fixture['forced'] is False
    if mode in ('signal', 'deadline'):
        holder, blocked = fixture['holder'], fixture['blocked']
        assert holder['database'] == backend['database'] and holder['user'] == 'postgres'
        assert holder['isolation'] == 'read committed' and holder['synchronous_commit'] == 'on'
        assert type(holder['pid']) is int and holder['pid'] > 0
        assert holder['application'] == response['terminal']['prefix'].split('/')[-1] + '_holder'
        assert microseconds(holder['backend_start']) <= server_window['before_us']
        assert fixture['lock_statement'] == 'BEGIN; LOCK TABLE control.datasets IN ACCESS EXCLUSIVE MODE;'
        assert fixture['lock_result'] == fixture['rollback'] == dict(sqlstate='00000', rows=0, lines=[])
        target, = backend['expected']
        identity(blocked, target)
        assert blocked['pid'] != holder['pid'] and blocked['user'] == 'bigbrotr_control'
        assert sql_text(blocked['query']) == DATASET_WAIT
        assert blocked['wait_event_type'] == 'Lock' and blocked['wait_event'] == 'relation'
        assert blocked['blockers'] == [holder['pid']]
        assert microseconds(blocked['holder_start']) == microseconds(holder['backend_start'])
        assert blocked['waiting_locks'] == [dict(type='relation', mode='RowShareLock',
            relation='control.datasets', granted=False)]
        inside_window(fixture['observed_at_us'], server_window)
        assert microseconds(blocked['backend_start']) <= fixture['observed_at_us']
        assert 0 <= fixture['seconds_after_observed_block'] <= 8
        assert type(fixture['application_backends_before_fixture_unlock']) is int
        remaining = fixture['remaining_backends_before_fixture_unlock']
        assert fixture['application_backends_before_fixture_unlock'] == len(remaining) <= 1
        if remaining:
            live, = remaining
            identity(live, target)
            assert live['user'] == 'bigbrotr_control' and sql_text(live['query']) == DATASET_WAIT
        assert fixture['released'] is True
        if mode == 'signal':
            assert fixture['signal'] == 'SIGTERM'
            assert fixture['signal_identity'] == {k: response['terminal'][k]
                for k in ('pid', 'start_ticks', 'config', 'prefix', 'command')}
        else:
            assert fixture['signal'] is None and 'signal_identity' not in fixture
            assert response['terminal']['elapsed_seconds'] >= 1


def verify(before, after, native_before, native_after, inputs_before, inputs_after, response, mode,
           *, seed, server_window, backend, predecessor=None, setup=None):
    assert mode.startswith(PREFIX)
    selected = mode[len(PREFIX):]
    assert selected in SUCCESS or selected in ERRORS
    window(server_window)
    known = empty.known_empty(seed)
    baseline(before, native_before, inputs_before, selected, seed, known, server_window, predecessor)
    assert inputs_after == inputs_before
    projected, control = control_delta(before, after, selected, server_window)
    retired_at = retirement_tx = None
    changed = selected in ('normal', 'fresh-completion')
    if changed:
        retired_at, retirement_tx = empty.terminal(before, projected, known, server_window)
        assert control['acquired_at_us'] <= retired_at <= control['released_at_us']
        assert retirement_tx != control['xmin']
    else:
        same(before, projected)
        if selected == 'terminal-retry':
            producer = only(before['producer_epochs'], producer_epoch=2)
            retired_at, retirement_tx = producer['retired_at_us'], producer['xmin']
    closes = selected in SUCCESS or selected == 'native-first-failure'
    assert native(native_after) == (known['closed'] if closes else native(native_before))
    assert native_after['sequence'] == (63 if closes else native_before['sequence'])
    backend_observations(backend, control, selected, server_window)
    assert response['terminal']['config'] == '/lab/' + backend['database'] + '.toml'
    response_checked(response, selected, expected_report(before, known, retired_at) if selected in SUCCESS else None)
    fixture_checked(setup, selected, before, inputs_before, known, backend, response, server_window, seed)
    if predecessor:
        assert backend['database'] == predecessor['backend']['database']
        old_process = predecessor['response']['terminal']
        process = response['terminal']
        assert (process['pid'], process['start_ticks']) != (old_process['pid'], old_process['start_ticks'])
        catalog = copy.deepcopy(predecessor['setup']['guards_after'])
        if selected == 'fresh-completion':
            catalog['triggers'] = [t for t in catalog['triggers'] if t['name'] != 'fixture_empty_source_rollback']
        assert setup['guards_before'] == catalog
    return dict(status='PASS', mode=mode, control_tables=71, native_rows=len(native_after['rows']),
        native_sequence_delta=native_after['sequence'] - native_before['sequence'],
        target_rows_changed=13 if changed else 0, original_inputs=len(inputs_after),
        historical_registrar=known['registration']['control_epoch'],
        historical_preparer=only(seed['control']['initial_ingest_successions'])['prepared_by_epoch'],
        control_epoch=None if control is None else control['epoch'],
        control_state=None if control is None else control['state'],
        retired_at_us=retired_at, retirement_transaction=retirement_tx,
        original_closure_digest=known['proof']['closure_digest'], empty_closure_digest=known['empty_digest'],
        original_rows_and_xmin_preserved=True, immutable_intent_preserved=True, admission=False,
        observed_states=2, control_clock='aggregate server-clock envelope; intermediate renewals not observed',
        qualification='actual CLI custody and injected schedules require the accompanying harness captures')


def negative_controls(before, after, native_before, native_after, inputs_before, inputs_after,
                      response, mode, *, seed, server_window, backend, predecessor=None, setup=None):
    """Perturb captured observations, never run a model of product execution."""
    context = dict(before=before, after=after, native_before=native_before, native_after=native_after,
        inputs_before=inputs_before, inputs_after=inputs_after, response=response, mode=mode,
        seed=seed, server_window=server_window, backend=backend, predecessor=predecessor, setup=setup)
    verify(**context)
    selected = mode[len(PREFIX):]
    rejected = []

    def check(name, mutate):
        changed = copy.deepcopy(context)
        mutate(changed)
        try:
            verify(**changed)
        except (AssertionError, ValueError):
            rejected.append(name)
        else:
            raise AssertionError('oracle accepted corrupted installed capture: ' + name)

    def pg(changed, table, filters, **changes):
        only(changed['after'][table], **filters).update(changes)

    check('historical-producer-xmin', lambda c: pg(c, 'producer_epochs', {'producer_epoch': 1}, xmin='4294967294'))
    check('intent-preparer', lambda c: pg(c, 'initial_ingest_successions', {}, prepared_by_epoch=999))
    check('intent-xmin', lambda c: pg(c, 'initial_ingest_successions', {}, xmin='4294967294'))
    check('original-outcome-xmin', lambda c: pg(c, 'producer_outcomes', {'request_sequence': 1}, xmin='4294967294'))
    check('native-sequence', lambda c: c['native_after'].update(sequence=c['native_after']['sequence'] + 1))
    check('native-value', lambda c: c['native_after']['rows'][0].update(value='00'))
    check('original-input-digest', lambda c: c['inputs_after'][sorted(c['inputs_after'])[0]].update(sha256='00' * 32))
    check('unknown-process-custody', lambda c: c['response'].update(outcome='OUTCOME_UNKNOWN'))
    check('forced-process', lambda c: c['response']['terminal'].update(forced=True))
    check('unreaped-process', lambda c: c['response']['terminal'].update(reaped=False))
    check('wrong-command', lambda c: c['response']['terminal']['command'].append('--retire-initial'))
    check('wrong-exit-status', lambda c: c['response'].update(returncode=9))
    check('diagnostic-contamination', lambda c: c['response'].update(stderr=c['response']['stderr'] + 'unrelated\n'))
    check('disabled-product-guard', lambda c: c['setup']['guards_after']['triggers'][0].update(enabled='D'))
    check('fixture-cleanup-unknown', lambda c: c['setup']['fixture']['cleanup_errors'].append('unknown'))
    check('unobserved-global-absence', lambda c: c['backend']['absence'].update(status='UNKNOWN'))
    def wrong_config(c):
        terminal = c['response']['terminal']
        terminal['config'] = '/lab/wrong_database.toml'
        terminal['command'][4] = terminal['config']
    check('wrong-database-config', wrong_config)
    if backend['expected']:
        check('backend-still-present', lambda c: c['backend']['observations'][0].update(present=True))
        check('foreign-backend-tuple', lambda c: c['backend']['observations'][0].update(pid=c['backend']['observations'][0]['pid'] + 1))
    if selected not in NO_CLAIM:
        epoch = max(r['epoch'] for r in after['owner_epochs'] if r['role'] == 'control')
        check('future-control-expiry', lambda c: pg(c, 'owner_epochs', {'role': 'control', 'epoch': epoch},
            lease_expires_at_us=(1 << 63) - 1))
        check('wrong-control-role', lambda c: pg(c, 'owner_epochs', {'role': 'control', 'epoch': epoch},
            role_instance_id='\\x' + 'ee' * 16))
        check('foreign-control-backend', lambda c: pg(c, 'owner_epochs', {'role': 'control', 'epoch': epoch},
            control_backend_pid=999999))
        if selected in SUCCESS:
            check('wrong-control-exclusion', lambda c: pg(c, 'owner_epochs', {'role': 'control', 'epoch': epoch},
                exclusion_digest='\\x' + '00' * 32))
        else:
            check('invented-owner-release', lambda c: pg(c, 'owner_epochs', {'role': 'control', 'epoch': epoch}, state='released'))
    if selected in SUCCESS:
        def report(c, section, field, value):
            record = json.loads(c['response']['stdout'])
            (record if section is None else record[section])[field] = value
            c['response']['stdout'] = json.dumps(record) + '\n'
        check('report-original-closure', lambda c: report(c, 'original_source_closure', 'sha256', '00' * 32))
        check('report-empty-closure', lambda c: report(c, 'empty_attempt_retirement', 'sha256', '00' * 32))
        check('report-retirement-time', lambda c: report(c, 'empty_attempt_retirement', 'retired_at_us', '0'))
        check('report-original-count', lambda c: report(c, None, 'resolved_original_requests', '3'))
        check('report-admission', lambda c: report(c, None, 'ready', True))
        check('report-ready-wrong-type', lambda c: report(c, None, 'ready', 0))
        check('target-owner-xmin', lambda c: pg(c, 'owner_epochs', {'role': 'ingest', 'epoch': 2}, xmin='4294967294'))
        check('target-domain-time', lambda c: pg(c, 'allocation_domains', {'owner_epoch': 2, 'dictionary_kind': 1}, closed_at_us=0))
        check('target-source-digest', lambda c: pg(c, 'source_streams', {'owner_epoch': 2}, closure_digest='\\x' + '00' * 32))
        check('target-producer-incomplete', lambda c: pg(c, 'producer_epochs', {'producer_epoch': 2}, state='closing'))
    else:
        check('false-success-output', lambda c: c['response'].update(stdout='{}\n'))
        check('unrelated-refusal', lambda c: c['response'].update(stderr='error: unrelated refusal\n'))
    if selected == 'native-first-failure':
        check('fault-never-fired', lambda c: c['setup']['fixture'].update(hit='1false'))
        check('missing-native-first-effect', lambda c: c.update(native_after=copy.deepcopy(c['native_before'])))
    if selected == 'fresh-completion':
        previous = max(r['epoch'] for r in before['owner_epochs'] if r['role'] == 'control')
        check('predecessor-not-fenced', lambda c: pg(c, 'owner_epochs', {'role': 'control', 'epoch': previous}, state='active'))
    if predecessor:
        check('predecessor-not-trusted', lambda c: c['predecessor']['backend']['observations'][0].update(present=True))
    if selected == 'missing-input':
        check('removed-input-identity', lambda c: c['setup']['removed_input'].update(sha256='00' * 32))
    if selected == 'held':
        check('native-holder-exited-early', lambda c: c['setup']['fixture'].update(alive_during_cli=False))
    if selected in ('signal', 'deadline'):
        check('unobserved-lock', lambda c: c['setup']['fixture']['blocked'].update(wait_event_type='IO'))
        check('wrong-blocker', lambda c: c['setup']['fixture']['blocked'].update(blockers=[]))
        check('unbounded-backends-before-unlock', lambda c: c['setup']['fixture'].update(application_backends_before_fixture_unlock=2))
        def foreign_waiter(c):
            value = c['setup']['fixture']
            value['remaining_backends_before_fixture_unlock'] = [dict(value['blocked'], pid=999999)]
            value['application_backends_before_fixture_unlock'] = 1
        check('foreign-live-waiter-before-unlock', foreign_waiter)
        if selected == 'signal':
            check('wrong-signalled-process', lambda c: c['setup']['fixture']['signal_identity'].update(pid=999999))
        else:
            check('not-a-deadline', lambda c: c['response']['terminal'].update(elapsed_seconds=.1))
    return dict(status='PASS', rejected=rejected)
