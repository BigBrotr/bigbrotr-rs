#!/usr/bin/env python3
"""Nine finite successor transitions on three fresh copies of terminal2 origins.

No original native store is opened. Every new process must positively close
native and join both drivers before the copied store may be inspected again.
"""
import datetime
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

sys.dont_write_bytecode = True
import history
from transition_replay import CASES, negative_controls, verify

schema, lab, SOURCE = history.schema, history.lab, history.SOURCE
read, digest, normalized = history.read, history.digest, history.normalized
projection, PINS = history.projection, history.PINS
capture, inputs_at = history.capture, history.inputs_at
observe_backend_absence, server_clock = history.observe_backend_absence, history.server_clock
census, content_census = history.census, history.content_census
catalog, configuration, dump = history.catalog, history.configuration, history.dump
LABEL = 'org.bigbrotr.succession-transition'
STATUS = 'PASS_SUCCESSOR_TRANSITION_FIRST_WAVE'
CLONES = ('activated', 'empty', 'activated-short')


@dataclass(frozen=True)
class Suite:
    directory: str
    label: str
    status: str
    client_prefix: str
    database_prefix: str
    clones: tuple
    cases: tuple
    scope: str
    not_run: tuple


FIRST_WAVE = Suite(
    directory='transition', label=LABEL, status=STATUS,
    client_prefix='bigbrotr-successor-transition-', database_prefix='bb_transition_',
    clones=CLONES, cases=CASES,
    scope='Finite prepare3, fresh empty retirement/retry3, prepare4 from both terminal2 origins and future-capacity refusal',
    not_run=('Gap completion requires a genuinely interrupted prepared seed',
             'Activation, writer delivery, installed succession and admitted new requests',
             'COMMIT loss, cancellation/expiry; PostgreSQL 255-epoch capacity; RSS qualification'))


def case_description(clone, name, database, config, predecessor):
    operation = 'empty-retry' if '-empty-retry3' in name else (
        'empty' if name.endswith('-empty3') else 'prepare')
    return dict(name=name, clone=clone, origin='empty' if clone == 'empty' else 'activated',
        operation=operation, predecessor=predecessor, database=database, config=config,
        metadata_bytes=132607 if clone == 'activated-short' else 262144, status='RUNNING')


def closed_and_joined(response, database, *, control_failed=False,
                      completion_event='lineage-transition-complete'):
    """Positive native/process telemetry, separate from scientific acceptance."""
    assert type(control_failed) is bool
    assert completion_event in ('lineage-transition-complete', 'lineage-live-complete',
                                'lineage-recovery-complete', 'lineage-source-complete',
                                'lineage-acquisition-complete', 'lineage-retirement-complete')
    assert response['returncode'] == 0
    events = [json.loads(line) for line in response['stdout'].splitlines()]
    assert not any(e.get('event') == 'lineage-history-timeout' for e in events)
    native, = [i for i, e in enumerate(events) if e == dict(event='lineage-history-native', state='closed')]
    control, = [i for i, e in enumerate(events) if e == dict(event='lineage-history-driver',
        driver='control', failed=control_failed, joined=True)]
    observer, = [i for i, e in enumerate(events) if e == dict(event='lineage-history-driver',
        driver='observer', failed=False, joined=True)]
    assert [e for e in events if e.get('event') == 'lineage-history-driver'] == [events[control], events[observer]]
    present, absent = [e for e in events if e.get('event') == 'lineage-history-backend']
    assert present['present'] is True and absent == dict(present, present=False)
    assert present['database'] == database and type(present['pid']) is int and present['pid'] > 0
    assert isinstance(present['backend_start'], str) and present['backend_start']
    absent_index = events.index(absent)
    complete = events[-1]
    assert complete['event'] == completion_event and complete['native'] == 'closed'
    assert complete['observer'] == 'joined' and complete['control'] in ('released-and-joined', 'dropped-and-joined')
    assert native < control < absent_index < observer < len(events) - 1
    return events


def external(pg, client, output, case, directory, response, *, control_failed=False,
             proxy_closed=True, completion_event='lineage-transition-complete'):
    """Persist each observation before rejecting missing custody or semantics."""
    name, database = case['name'], case['database']
    errors = []
    custody = 'UNKNOWN_PROCESS_OUTCOME'
    events = None
    try:
        assert proxy_closed is True, 'proxy custody not established'
        events = closed_and_joined(response, database, control_failed=control_failed,
                                   completion_event=completion_event)
        custody = 'OBSERVED_CLOSED_AND_JOINED'
    except BaseException as error:
        errors.append(dict(stage='probe-custody', error=repr(error)))
    absent = False
    try:
        observe_backend_absence(pg, database, output, name + '-control')
        backends = schema.absence(pg, database)
        lab.write_json(output / (name + '-backends.json'), backends)
        assert backends == []
        absent = True
    except BaseException as error:
        errors.append(dict(stage='backend-absence', error=repr(error)))
    # These reads can preserve provisional evidence even when no native owner
    # exclusion was established. The status below never relabels it as final.
    after = final_inputs = final_native = None
    for stage, function, suffix in [
        ('sql', lambda: schema.rows(pg, database), 'after'),
        ('inputs', lambda: inputs_at(directory / 'store'), 'after-inputs'),
    ]:
        try:
            value = function()
            lab.write_json(output / (name + '-' + suffix + '.json'), value)
            if stage == 'sql':
                after = value
            else:
                final_inputs = value
        except BaseException as error:
            errors.append(dict(stage=stage, error=repr(error)))
    if custody == 'OBSERVED_CLOSED_AND_JOINED' and absent:
        try:
            final_native = dump(client, output, name + '-after', directory)
        except BaseException as error:
            errors.append(dict(stage='native-capture', error=repr(error)))
    else:
        lab.write_json(output / (name + '-external-unavailable.json'), dict(
            status='NOT_RUN_UNKNOWN_CUSTODY', native='NOT_RUN', sql_and_inputs='provisional',
            cleanup='outer owner-checked client stop; no inferred rollback'))
    evidence = dict(status=custody, database=database, backend_absent=absent, errors=errors)
    lab.write_json(output / (name + '-custody.json'), evidence)
    case['custody'] = custody
    assert not errors, evidence
    return events, after, final_native, final_inputs


def execute(pg, client, output, case, directory, config, expected):
    name, database = case['name'], case['database']
    before = schema.rows(pg, database)
    lab.write_json(output / (name + '-before.json'), before)
    inputs = inputs_at(directory / 'store')
    lab.write_json(output / (name + '-inputs.json'), inputs)
    native = dump(client, output, name + '-before', directory)
    assert normalized(before) == normalized(expected[0])
    assert native == expected[1] and inputs == expected[2]
    window = dict(before_us=server_clock(pg, database))
    lab.write_json(output / (name + '-clock.json'), window)
    try:
        capture(client, output, name, 'successor-' + case['operation'], database,
            '/run/secrets/application', config, str(case['metadata_bytes']))
    finally:
        path = output / (name + '-response.json')
        if not path.exists():
            lab.write_json(path, dict(returncode=None, stdout='', stderr='',
                error='wrapper failed without terminal receipt', outcome='UNKNOWN'))
        response = read(path)
        case['returncode'] = response['returncode']
        try:
            window['after_us'] = server_clock(pg, database)
        except BaseException as error:
            window['error'] = repr(error)
        lab.write_json(output / (name + '-clock.json'), window)
        events, after, final_native, final_inputs = external(pg, client, output, case, directory, response)
    assert response['returncode'] == 0 and not response['stderr']
    checked = verify(events, before, native, inputs, case, window)
    controls = negative_controls(events, before, native, inputs, case, window)
    final, = [e for e in events if e['event'] == 'lineage-history-state' and e['stage'] == 'final']
    assert normalized(after) == normalized({t: json.loads(v) for t, v in final['state']['control_tables']})
    assert final_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence'])
    assert final_inputs == inputs
    assert checked['database'] == database
    case.update(status='PASS', oracle=checked, negative_controls=controls,
        response_bytes=len(response['stdout'].encode()), server_window=window)
    return after, final_native, final_inputs


def run_sequence(pg, client, output, clone, directory, expected, report):
    """The original nine-case sequence; resource ownership stays in run()."""
    mode, database, config = clone['name'], clone['database'], clone['config']
    names = ('activated-metadata-short',) if mode == 'activated-short' else tuple(
        mode + '-' + operation for operation in ('prepare3', 'empty3', 'empty-retry3', 'prepare4'))
    predecessor = None
    for label in names:
        case = case_description(mode, label, database, config, predecessor)
        report['cases'].append(case)
        lab.write_json(output / 'result.json', report)
        # The next process starts from the preceding complete external
        # state, never from a newly adopted or normalized baseline.
        expected = execute(pg, client, output, case, directory, config, expected)
        case['catalog_after'] = catalog(pg, database)
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = label
        lab.write_json(output / 'result.json', report)
    return predecessor


def legacy_installed_origin(basis, origin, retained):
    """Explicit old installed receipt shape; never a fallback for newer reports.

    The independently pinned whole verification binds its final state to the
    still-retained SQL/native/input endpoint. Live catalog and physical origin
    preservation remain the same outer checks as for succession origins.
    """
    assert origin['origin_format'] == 'legacy-installed'
    assert set(retained['client']) == {'id', 'name'}
    assert retained['client']['id'] == origin['client_id']
    group, = retained['cases']
    assert group['status'] == 'PASS'
    previous, = [c for c in group['cases'] if c['name'] == origin['source_clone']]
    assert previous['status'] == 'PASS'
    final, = [c for c in previous['attempts'] if c['name'] == origin['final_case']]
    assert final['status'] == 'PASS'
    assert final['verification_sha256'] == origin['verification_sha256']
    path = basis / origin['verification_file']
    assert digest(path) == origin['verification_sha256']
    context = read(path)
    assert normalized(context['after']) == normalized(read(basis / origin['snapshot']))
    assert context['native_after'] == read(basis / origin['native_file'])
    saved_inputs = read(basis / origin['inputs_file'])
    assert context['inputs_after'] == saved_inputs.get('inputs', saved_inputs)
    response = context['response']
    assert response['outcome'] == 'PROCESS_EXITED'
    assert response['returncode'] == 0 and not response['stderr']
    terminal = response['terminal']
    assert terminal['exit_code'] == 0 and terminal['reaped'] is True and terminal['forced'] is False
    assert terminal['config'] == '/lab/' + origin['database'] + '.toml'
    assert terminal['prefix'] == '/lab/' + origin['final_case']
    result, = [json.loads(line) for line in response['stdout'].splitlines()]
    assert result['native'] == 'closed' and result['owner_release'] == 'committed'
    assert result['state'] in ('activated-initial-retired', 'empty-attempt-retired')
    assert result['producer_epoch'] == '2' and result['ready'] is False
    assert origin['database'] in retained['databases']
    return origin['client_id']


def observe_origin_absence(pg, database, output, name):
    """Bounded all-session observation; no user/type exclusions or termination."""
    assert re.fullmatch(r'[a-z0-9_]+',database) and re.fullmatch(r'[a-z0-9-]+',name)
    started = time.monotonic()
    observation = dict(database=database,scope='all-database-sessions',limit_seconds=5,
        status='INCOMPLETE',attempts=[])
    path = output/(name+'-origin-backends.json')
    statement = "SELECT coalesce(json_agg(row_to_json(t) ORDER BY pid),'[]') FROM " \
        "(SELECT pid,backend_start,backend_type,usename,state FROM pg_catalog.pg_stat_activity " \
        "WHERE datname='"+database+"')t;"
    lab.write_json(path,observation)
    try:
        while True:
            remaining = 5-(time.monotonic()-started)
            if remaining <= 0:
                raise TimeoutError('All origin sessions absent was not observed within five seconds')
            # Connect to postgres so this observer is not itself in the target
            # set. lab.sql's default timeout exceeds this finite deadline.
            response = lab.docker('exec','-i',pg['postgres'],'psql','-X','-A','-t','-q',
                '--no-password','-v','ON_ERROR_STOP=1','-U','postgres','-d','postgres',
                data=statement,timeout=remaining)
            sample = json.loads(response.stdout)
            elapsed = time.monotonic()-started
            observation['attempts'].append(dict(seconds=elapsed,sessions=sample))
            lab.write_json(path,observation)
            assert type(sample) is list
            if sample == [] and elapsed <= 5:
                observation['status'] = 'OBSERVED_ABSENT'
                return observation
            remaining = 5-(time.monotonic()-started)
            if remaining > 0:
                time.sleep(min(.05,remaining))
    except BaseException as error:
        observation['error'] = type(error).__name__
        raise
    finally:
        observation['seconds'] = time.monotonic()-started
        lab.write_json(path,observation)


def run(root, build, *, suite=None, sequence=run_sequence, origin_specs=None,
        origin_for_clone=None, extra_grants=(), producer_progress=False, producer_ack=False,
        preparation_hole=False, compacted_reader=False, compacted_history=False,
        compacted_initial=False, initial_preparation=False, compacted_activated=False, compacted_terminal=False, empty_initial=False, later_empty=False, later_live=False, later_recovery=False, later_source=False, later_acquisition=False, later_retirement=False, product_source=None, ordinary_regression=False):
    assert type(ordinary_regression) is bool
    if ordinary_regression:
        assert not any((producer_progress, producer_ack, preparation_hole, compacted_reader,
            compacted_history, compacted_initial, initial_preparation, compacted_activated,
            compacted_terminal, empty_initial, later_empty, later_live, later_recovery, later_source, later_acquisition, later_retirement))
        from installed_prepared_model import PINS as ordinary_pins
        assert origin_specs == (ordinary_pins['open'],)
        assert origin_for_clone == {'open': 'open'}
        assert suite.clones == ('open',) and suite.cases == ('open-retire', 'open-retry')
    assert type(later_retirement) is bool and (not later_retirement or compacted_history) and not (later_retirement and (later_empty or later_live or later_recovery or later_source or later_acquisition))
    assert type(later_acquisition) is bool and (not later_acquisition or compacted_history) and not (later_acquisition and (later_empty or later_live or later_recovery or later_source))
    assert type(later_source) is bool and (not later_source or compacted_history) and not (later_source and (later_empty or later_live or later_recovery))
    assert type(later_recovery) is bool and (not later_recovery or compacted_history) and not (later_recovery and (later_empty or later_live))
    assert type(later_live) is bool and (not later_live or compacted_history) and not (later_live and later_empty)
    assert type(later_empty) is bool and (not later_empty or compacted_history)
    assert type(producer_progress) is bool
    assert type(producer_ack) is bool and (not producer_ack or producer_progress)
    assert type(preparation_hole) is bool and (not preparation_hole or producer_ack)
    assert type(compacted_reader) is bool
    assert type(compacted_history) is bool and (not compacted_history or compacted_reader)
    assert type(compacted_initial) is bool and (not compacted_initial or compacted_reader)
    assert type(initial_preparation) is bool and (not initial_preparation or compacted_initial)
    assert type(compacted_activated) is bool and (not compacted_activated or compacted_initial)
    assert not (compacted_activated and initial_preparation)
    assert type(empty_initial) is bool and (not empty_initial or compacted_initial)
    assert not (empty_initial and (initial_preparation or compacted_activated))
    assert type(compacted_terminal) is bool and (not compacted_terminal or compacted_activated)
    if initial_preparation or compacted_activated or empty_initial:
        from installed_compacted_reader_oracle import PREPARATION_RETAINED_ORIGINS, ACTIVATED_RETAINED_ORIGINS, TERMINAL_TAIL_ORIGIN, EMPTY_ORIGINS, EMPTY_PREPARED_ORIGINS
    assert not (compacted_initial and compacted_history)
    if compacted_reader:
        from installed_compacted_reader_oracle import GRANTS, HISTORY_GRANTS, JOURNEY_GRANTS, ACTIVATED_GRANTS, TERMINAL_GRANTS, EMPTY_GRANTS, qualification_checked
        assert preparation_hole and extra_grants == (EMPTY_GRANTS if empty_initial else TERMINAL_GRANTS if compacted_terminal else ACTIVATED_GRANTS if compacted_activated else JOURNEY_GRANTS if compacted_initial else HISTORY_GRANTS if compacted_history else GRANTS)
        assert re.fullmatch(r'[0-9a-f]{40}', product_source or '')
    else:
        assert product_source is None
    if producer_progress:
        from installed_progress_replay import require_frozen_profile
        progress_profile = require_frozen_profile(ack=producer_ack,preparation_hole=preparation_hole)
        migration = SOURCE / ('migrations/postgres/0040_producer_ack.sql' if producer_ack
            else 'migrations/postgres/0039_producer_progress.sql')
        assert digest(migration) == progress_profile['migration_sha256']
    if suite is None:
        suite = FIRST_WAVE
    explicit_origins = origin_specs is not None
    if explicit_origins:
        assert isinstance(origin_specs, tuple) and origin_specs
        assert len({o['name'] for o in origin_specs}) == len(origin_specs)
        assert len({o['database'] for o in origin_specs}) == len(origin_specs)
        assert set(origin_for_clone) == set(suite.clones)
        assert set(origin_for_clone.values()) == {o['name'] for o in origin_specs}
        for origin in origin_specs:
            fields = {'name', 'run', 'subdirectory', 'database', 'status', 'source',
                'owner', 'label', 'client_id', 'source_clone', 'final_case', 'catalog_sha256',
                'result_sha256', 'snapshot', 'snapshot_sha256', 'native_file', 'native_sha256',
                'inputs_file', 'inputs_sha256', 'store'}
            if 'origin_format' in origin:
                assert origin['origin_format'] in ('legacy-installed','initial-ack-hole')
                if origin['origin_format'] == 'initial-ack-hole':
                    assert preparation_hole
                fields |= {'origin_format', 'verification_file', 'verification_sha256'}
                assert origin['verification_file'] == origin['final_case'] + '-verification.json'
                assert re.fullmatch(r'[0-9a-f]{64}', origin['verification_sha256'])
            elif (initial_preparation and origin in PREPARATION_RETAINED_ORIGINS) or (compacted_activated and origin in ACTIVATED_RETAINED_ORIGINS) or (compacted_terminal and origin==TERMINAL_TAIL_ORIGIN) or (empty_initial and origin in EMPTY_ORIGINS):
                fields.add('verification_sha256')
                assert re.fullmatch(r'[0-9a-f]{64}', origin['verification_sha256'])
            assert set(origin) == fields
            assert all(type(value) is str and value for value in origin.values())
            for field in ('name', 'run', 'subdirectory', 'source_clone', 'final_case'):
                assert re.fullmatch(r'[a-z0-9-]+', origin[field]), field
            assert re.fullmatch(r'[a-z0-9_]+', origin['database'])
            assert origin['store'] == origin['source_clone'] + '/store'
            for field in ('snapshot', 'native_file', 'inputs_file'):
                assert Path(origin[field]).name == origin[field] and origin[field].endswith('.json')
            for field in ('result_sha256', 'snapshot_sha256', 'native_sha256', 'inputs_sha256',
                          'catalog_sha256', 'client_id'):
                assert re.fullmatch(r'[0-9a-f]{64}', origin[field]), field
            assert re.fullmatch(r'[0-9a-f]{40}', origin['source'])
            assert re.fullmatch(r'[0-9a-f]{32}', origin['owner'])
            assert origin['label'].startswith('org.bigbrotr.') and origin['status'].startswith('PASS_')
        pins = {o['name']: o for o in origin_specs}
    else:
        assert not preparation_hole
        assert origin_for_clone is None
        origin_specs, pins = schema.ORIGINS, PINS
        origin_for_clone = {mode: 'empty' if mode == 'empty' else 'activated' for mode in suite.clones}
    assert isinstance(extra_grants, tuple) and all(isinstance(s, str) and s for s in extra_grants)
    if preparation_hole and not compacted_reader:
        assert not extra_grants, 'qualified initial recovery already has its exact authority'
    assert SOURCE == root / 'source'
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
    built = read(build / 'build-result.json')
    component = read(build / 'component-result.json')
    qualification = read(build.parent / 'qualification.json')
    if ordinary_regression:
        from installed_prepared_model import ordinary_qualification
        ordinary_qualification(built, component, qualification)
    if compacted_reader:
        qualification_checked(built,component,qualification,product_source,
            initial_journey=compacted_initial or later_empty or later_live or later_recovery or later_source or later_acquisition or later_retirement,initial_preparation=initial_preparation,compacted_activated=compacted_activated,compacted_terminal=compacted_terminal,empty_initial=empty_initial or later_empty or later_live or later_recovery or later_source or later_acquisition or later_retirement,later_empty=later_empty,later_live=later_live,later_recovery=later_recovery,later_source=later_source,later_acquisition=later_acquisition,later_retirement=later_retirement)
    elif preparation_hole:
        from installed_progress_replay import preparation_hole_qualification
        preparation_hole_qualification(built,component,qualification)
    allowed = ('PASS_SUCCESSION_TRANSITION_COMPONENTS',
        'PASS_SUCCESSION_HISTORY_PROBE_BUILD', 'PASS_SUCCESSION_TRANSITION_PROBE_BUILD',
        'PASS_SUCCESSION_RECOVERY_COMPONENTS', 'PASS_SUCCESSION_INSTALLED_COMPONENTS')
    assert qualification['status'] in (('PASS_COMPACTED_PREFIX_COMPONENTS',) if compacted_reader or ordinary_regression else ('PASS_PRODUCER_ACK_COMPONENTS',) if producer_ack else ('PASS_PRODUCER_PROGRESS_COMPONENTS',)
        if producer_progress else allowed)
    assert component['status'] == 'PASS'
    assert component['source'] == built['source'] == qualification['source']
    if producer_progress and not compacted_reader:
        assert built['source'] == progress_profile['product_source'], 'unreviewed progress build source'
    for field in ('tree', 'source_manifest_sha256', 'lockfile_sha256'):
        assert component[field] == qualification[field] == built[field], field
    binaries = component['executables_sha256']
    assert {'bigbrotr', 'ingest_bootstrap_probe'} <= set(binaries)
    if producer_ack and not compacted_reader:
        from installed_progress_replay import ack_executables
        assert binaries==ack_executables(preparation_hole=preparation_hole)
        assert set(binaries)=={'bigbrotr','ingest_bootstrap_probe','native_components','sync_progress',
            'fact_codec','evidence_intent','control_schema','migrate_entrypoint'}
    assert all(digest(build / name) == value for name, value in binaries.items())
    assert binaries['bigbrotr'] == built['binary_sha256']
    manifest_path = build.parent / 'build/source-manifest.json'
    manifest = read(manifest_path)
    assert digest(manifest_path) == built['source_manifest_sha256']
    assert manifest['revision'] == built['source'] and manifest['tree'] == built['tree']
    changes = lab.command(['git', '-C', str(SOURCE), 'diff', '--name-only', built['source'], revision]).stdout.splitlines()
    assert all(name.startswith(('docs/', 'experiments/')) for name in changes), changes
    assert all(digest(SOURCE / name) == wanted for name, wanted in manifest['files_sha256'].items()
        if name.startswith(('apps/', 'crates/', 'migrations/')) or name in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'))
    pg_root = root.parent / 'relay-source-recheck-e06f580/pg'
    pg = lab.load(pg_root)
    assert pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608' and pg['state'] == 'stopped-retained'
    containers = schema.containers()
    borrowed, = [c for c in containers if c['Name'].lstrip('/') == pg['postgres']]
    assert borrowed['Id'] == '7177550705c49b14237e20790af411af58f62f22d110cd0dfa1afddaf78861ec'
    assert borrowed['State']['Status'] == 'exited' and not borrowed['State']['Pid'] and not borrowed['State']['OOMKilled']
    assert borrowed['HostConfig']['Memory'] == 1536 * 1024**2 and borrowed['HostConfig']['NanoCpus'] == 2_000_000_000
    assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in containers)
    assert shutil.disk_usage(root).free > 2 * 1024**3
    output = root / suite.directory
    output.mkdir(mode=0o700)
    owner = schema.secrets.token_hex(16)
    client = suite.client_prefix + revision[:7]
    assert not any(c['Name'].lstrip('/') == client for c in containers)
    tree = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD^{tree}']).stdout.strip()
    report = dict(status='RUNNING', source=revision, tree=tree, build=built, component=component, owner=owner,
        label=suite.label, postgres=borrowed['Id'], postgres_owner=pg['owner'], client=None, databases=[],
        started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), origins={}, clones=[], cases=[],
        free_bytes_before=shutil.disk_usage(root).free,
        scope=suite.scope, not_run=list(suite.not_run))
    if producer_ack:
        from installed_progress_context import record_source
        record_source(output,build,revision)
    lab.write_json(output / 'preflight-containers.json', containers)
    lab.write_json(output / 'result.json', report)
    origins = {}
    try:
        for origin in origin_specs:
            name, pin = origin['name'], pins[origin['name']]
            assert all(origin[k] == pin[k] for k in pin if k in origin)
            basis = root.parent / origin['run'] / origin.get('subdirectory', 'bindings')
            for filename, expected in [('result.json', pin['result_sha256']),
                    (pin['snapshot'], pin['snapshot_sha256']), (pin['native_file'], pin['native_sha256']),
                    (pin['inputs_file'], pin['inputs_sha256'])]:
                assert digest(basis / filename) == expected
            retained = read(basis / 'result.json')
            assert retained['status'] == origin['status']
            assert not any(retained[k] for k in ('cleanup_errors', 'missing_owned', 'unrelated_changed'))
            if explicit_origins:
                assert retained['source'] == origin['source']
                assert retained['owner'] == origin['owner'] and retained['label'] == origin['label']
                if origin.get('origin_format') == 'legacy-installed':
                    previous_client = legacy_installed_origin(basis, origin, retained)
                elif origin.get('origin_format') == 'initial-ack-hole':
                    from installed_progress_recovery_oracle import initial_ack_origin_report
                    previous_client = initial_ack_origin_report(basis,origin,retained)
                else:
                    assert type(retained['client']) is str and retained['client'] == origin['client_id']
                    assert not retained['unexpected_containers']
                    previous, = [c for c in retained['clones'] if c['name'] == origin['source_clone']]
                    assert previous['database'] == origin['database'] and previous['status'] == 'PASS'
                    assert previous['final_case'] == origin['final_case']
                    assert previous['directory'] + '/store' == origin['store']
                    final, = [c for c in retained['cases'] if c['name'] == origin['final_case']]
                    assert final['status'] == 'PASS' and final['custody'] == 'OBSERVED_CLOSED_AND_JOINED'
                    # These exact qualified endpoints end in their selected expected refusals.
                    assert final['returncode'] == (4 if (compacted_terminal and origin == TERMINAL_TAIL_ORIGIN) or (empty_initial and origin == EMPTY_PREPARED_ORIGINS[1]) else 0)
                    assert hashlib.sha256(previous['final_catalog'].encode()).hexdigest() == origin['catalog_sha256']
                    previous_client = retained['client']
            else:
                previous_client = retained['client']['id']
            old, = [c for c in containers if c['Id'] == previous_client]
            assert old['Config']['Labels'][retained['label']] == retained['owner']
            assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
            assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
                for m in c['Mounts']) for c in containers)
            store = basis / pin['store']
            files = census(store)
            assert len(files) <= 128 and sum(v['bytes'] for v in files.values()) < 4 * 1024**2
            inputs = inputs_at(store)
            frozen_inputs = read(basis / pin['inputs_file'])
            assert inputs == frozen_inputs.get('inputs', frozen_inputs)
            origins[name] = dict(store=store, basis=basis, files=files, inputs=inputs,
                native=read(basis / pin['native_file']), rows=read(basis / pin['snapshot']))
            report['origins'][name] = dict(origin, pins=pin, store=str(store), files=files,
                inputs=inputs, native=origins[name]['native'], previous_client=retained['client'])
            lab.write_json(output / (name + '-original.json'), origins[name]['rows'])
        lab.docker('start', pg['postgres'])
        deadline = time.monotonic() + 30
        while lab.docker('exec', pg['postgres'], 'pg_isready', '-U', 'postgres', check=False).returncode:
            assert time.monotonic() < deadline
            time.sleep(.2)
        assert lab.sql(pg, 'postgres', 'SHOW server_version_num;') == '160015'
        report['database_names_before'] = lab.sql(pg, 'postgres', 'SELECT datname FROM pg_database ORDER BY datname;').splitlines()
        for origin in origin_specs:
            name, database = origin['name'], origin['database']
            if preparation_hole:
                observe_origin_absence(pg,database,output,name)
            else:
                assert schema.absence(pg, database) == []
            observed = schema.rows(pg, database)
            assert normalized(observed) == normalized(origins[name]['rows'])
            origins[name]['catalog'] = catalog(pg, database)
            if explicit_origins:
                assert hashlib.sha256(origins[name]['catalog'].encode()).hexdigest() == origin['catalog_sha256']
            report['origins'][name].update(rows=observed, catalog=origins[name]['catalog'])
            lab.write_json(output / (name + '-original.json'), observed)
        ident = lab.docker('create', '--init', '--name', client, '--label', suite.label + '=' + owner,
            '--cpus', '2', '--memory', '1536m', '--memory-swap', '1536m', '--pids-limit', '128',
            '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--user', f'{os.getuid()}:{os.getgid()}', '--network', 'container:' + borrowed['Id'],
            '--tmpfs', '/tmp:rw,exec,nosuid,nodev,size=128m',
            '--mount', f'type=bind,src={output},dst=/lab',
            '--mount', f'type=bind,src={build},dst=/build,readonly',
            '--mount', f'type=bind,src={pg_root}/secrets,dst=/run/secrets,readonly',
            lab.CLIENT_IMAGE, 'python3', '-c', 'import time; time.sleep(1800)').stdout.strip()
        report['client'] = ident
        lab.write_json(output / 'result.json', report)
        lab.docker('start', ident)
        state = dict(pg, client=client)
        for mode in suite.clones:
            name = origin_for_clone[mode]
            original = origins[name]
            database = suite.database_prefix + revision[:7] + '_' + mode.replace('-', '_')
            assert database not in report['database_names_before']
            clone = dict(name=mode, origin=name, database=database, directory=mode, status='RUNNING', migrations=[])
            report['clones'].append(clone)
            lab.write_json(output / 'result.json', report)
            lab.sql(pg, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE {pins[name]["database"]};')
            report['databases'].append(database)
            copied = schema.rows(pg, database)
            lab.write_json(output / (mode + '-copy-before.json'), copied)
            assert normalized(copied) == normalized(original['rows'])
            assert catalog(pg, database) == original['catalog']
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(original['store'], directory / 'store')
            assert content_census(census(directory / 'store')) == content_census(original['files'])
            config = configuration(output, state, database, mode, copied)
            clone['config'] = config
            for action in ('plan', 'apply', 'apply'):
                try:
                    answer = lab.cli(state, 'migrate', action, '--file', config, '--user', 'bigbrotr_migrator',
                        '--password-ref', 'file:/run/secrets/migrator', '--target',
                        '40' if producer_ack else '39' if producer_progress else '38')
                except subprocess.TimeoutExpired as error:
                    def partial(value):
                        return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
                    clone['migrations'].append(dict(action=action, exit_code=None,
                        stdout=partial(error.stdout), stderr=partial(error.stderr)))
                    clone['migration_outcome'] = 'UNKNOWN_TIMEOUT'
                    lab.write_json(output / 'result.json', report)
                    raise
                clone['migrations'].append(dict(action=action, exit_code=answer.returncode, stdout=answer.stdout, stderr=answer.stderr))
                lab.write_json(output / 'result.json', report)
                assert answer.returncode == 0 and not answer.stderr
            migrated = schema.rows(pg, database)
            lab.write_json(output / (mode + '-migrated.json'), migrated)
            # The qualified initial origin already grants exact recovery rights;
            # successor setup would broaden its authority and change its catalog.
            statements = list(extra_grants) if compacted_reader else [] if preparation_hole else [
                (SOURCE / 'experiments/ingest-succession/schema_grants.sql').read_text(),
                'GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;', *extra_grants]
            clone['grants'] = []
            for statement in statements:
                clone['grants'].append(dict(statement=statement, stdout=lab.sql(pg, database, statement)))
            clone['catalog_before'] = catalog(pg, database)
            lab.write_json(output / 'result.json', report)
            expected = (migrated, original['native'], original['inputs'])
            predecessor = sequence(pg, client, output, clone, directory, expected, report)
            clone.update(status='PASS', final_case=predecessor, catalog_after=catalog(pg, database))
            assert clone['catalog_after'] == clone['catalog_before']
            lab.write_json(output / 'result.json', report)
        assert tuple(case['name'] for case in report['cases']) == suite.cases
        report['status'] = suite.status
    except BaseException as error:
        report.update(status='FAIL', error=repr(error))
        raise
    finally:
        errors = []
        # Never inspect a clone natively here: a failed wrapper may still own it.
        for clone in report['clones']:
            try:
                lab.write_json(output / (clone['name'] + '-final-sql.json'), schema.rows(pg, clone['database']))
                clone['final_backends'] = schema.absence(pg, clone['database'])
                clone['final_catalog'] = catalog(pg, clone['database'])
                assert clone['final_backends'] == []
            except BaseException as error:
                errors.append(dict(copy=clone['database'], error=repr(error)))
        for origin in origin_specs:
            name = origin['name']
            if name not in origins:
                continue
            try:
                original = origins[name]
                after_files = census(original['store'])
                preserved = dict(rows_unchanged=normalized(schema.rows(pg, origin['database'])) == normalized(original['rows']),
                    catalog_unchanged='catalog' in original and catalog(pg, origin['database']) == original['catalog'],
                    native_unchanged=content_census(after_files) == content_census(original['files']),
                    inputs_unchanged=inputs_at(original['store']) == original['inputs'],
                    physical_unchanged=after_files == original['files'])
                report['origins'][name]['preserved'] = preserved
                lab.write_json(output / (name + '-preserved.json'), preserved)
                assert all(preserved.values()) and schema.absence(pg, origin['database']) == []
            except BaseException as error:
                errors.append(dict(origin=name, error=repr(error)))
        try:
            names = lab.sql(pg, 'postgres', 'SELECT datname FROM pg_database ORDER BY datname;').splitlines()
            report['database_names_after'] = names
            assert set(names) == set(report['database_names_before']) | set(report['databases'])
            assert all(digest(build / name) == value for name, value in binaries.items())
        except BaseException as error:
            errors.append(dict(inventory_or_build=repr(error)))
        # A lost create reply can leave our uniquely named, labelled container
        # without its returned ID. Recover only that exact owned resource.
        if report['client'] is None:
            try:
                answer = lab.docker('inspect', client, check=False)
                if answer.returncode == 0:
                    item, = json.loads(answer.stdout)
                    assert item['Config']['Labels'].get(suite.label) == owner
                    report['client'] = item['Id']
            except BaseException as error:
                errors.append(dict(client_creation=repr(error)))
        for ident, label, expected in [(report['client'], suite.label, owner), (borrowed['Id'], lab.LABEL, pg['owner'])]:
            if ident is None:
                continue
            try:
                item, = json.loads(lab.docker('inspect', ident).stdout)
                assert item['Config']['Labels'][label] == expected
                if item['State']['Running']:
                    lab.docker('stop', '--time', '10', ident)
                item, = json.loads(lab.docker('inspect', ident).stdout)
                assert not item['State']['Running'] and not item['State']['Pid'] and not item['State']['OOMKilled']
                lab.write_json(output / ('postgres-terminal.json' if ident == borrowed['Id'] else 'client-terminal.json'), projection(item))
            except BaseException as error:
                errors.append(dict(container=ident, error=repr(error)))
        try:
            after = {c['Id']: projection(c) for c in schema.containers()}
            lab.write_json(output / 'terminal-containers.json', after)
            report['unrelated_changed'] = [c['Id'] for c in containers if c['Id'] != borrowed['Id'] and after.get(c['Id']) != projection(c)]
            report['unexpected_containers'] = sorted(set(after) - {c['Id'] for c in containers} - {report['client']})
            report['missing_owned'] = [ident for ident in (report['client'], borrowed['Id']) if ident is not None and ident not in after]
            logs = lab.docker('logs', '--since', report['started_at'], borrowed['Id'])
            (output / 'postgres.log').write_text(logs.stdout + logs.stderr)
        except BaseException as error:
            errors.append(dict(terminal_inventory=repr(error)))
        report.update(cleanup_errors=errors, finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            free_bytes_after=shutil.disk_usage(root).free)
        if errors or any(report.get(k) for k in ('missing_owned', 'unrelated_changed', 'unexpected_containers')):
            report['status'] = 'INCOMPLETE_CUSTODY'
        lab.write_json(output / 'result.json', report)
    assert report['status'] == suite.status
    print(json.dumps(dict(status=suite.status, cases=[c['name'] for c in report['cases']], root=str(root))))


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
