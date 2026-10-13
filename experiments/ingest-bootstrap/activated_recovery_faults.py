"""Four real installed activated-recovery COMMIT losses and fresh-process replay.

Each owned clone keeps its failed process, exact wire metadata and external state.
The installed CLI does not expose driver-result or reusable-session telemetry;
this fixture observes joined process custody and positive backend absence instead.
"""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import time
import tomllib

import lab
import activated_recovery as recovery
from activated_recovery_faults_oracle import verify, negative_controls
from activated_recovery_oracle import REMOVE_COPY, known
from fault_cases import Incomplete, Process, safe_event
from installed import set_value, wait_for
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_live import exact_absence
from initial_succession import census, digest


CASES = tuple('installed-activated-' + name for name in
    ('close-before', 'close-after', 'outcome-before', 'outcome-after'))


def proxy_config(output, config, mode, ready):
    """A separate deployment-root endpoint; accepted policy remains untouched."""
    direct = output / Path(config).name
    original = direct.read_text()
    endpoint = '127.0.0.1:' + str(ready['listen_port'])
    changed = set_value(original, 'postgres.control', 'endpoint', endpoint)
    before, after = tomllib.loads(original), tomllib.loads(changed)
    assert before['postgres']['control']['endpoint'] == '127.0.0.1:5432'
    expected = copy.deepcopy(before)
    expected['postgres']['control']['endpoint'] = endpoint
    assert after == expected
    path = output / (mode + '-proxy.toml')
    with path.open('x') as stream:
        stream.write(changed)
    return dict(direct_path=config, proxy_path='/lab/' + path.name,
        direct_text=original, proxy_text=changed, direct_sha256=digest(direct),
        proxy_sha256=digest(path), endpoint=endpoint)


def finish_proxy(proxy, wire):
    """Retain bounded safe metadata even when the expected boundary is absent."""
    try:
        wire['receipt'] = proxy.event('receipt', 5)
        wire['exit_code'] = proxy.wait(3)
    except Exception as error:
        wire.update(status='INCOMPLETE', error=type(error).__name__)
    finally:
        forced = proxy.process.poll() is None
        try:
            proxy.stop()
        except Exception as error:
            wire.update(status='INCOMPLETE', cleanup_error=type(error).__name__)
        events = []
        try:
            for line in proxy.stdout.splitlines():
                events.append(safe_event(json.loads(line)))
            wire['stdout'] = bytes(proxy.stdout).decode('utf-8')
        except (ValueError, TypeError, Incomplete) as error:
            wire.update(status='INCOMPLETE', metadata_error=type(error).__name__)
        wire.update(events=events, wrapper_exit=proxy.process.returncode,
            stderr_nonempty=bool(proxy.stderr), forced_wrapper_stop=forced,
            stdout_sha256=hashlib.sha256(proxy.stdout).hexdigest(),
            stdout_bytes=len(proxy.stdout), output_overflow=proxy.overflow,
            cleanup='Wrapper joined; exact backend observation and outer owned client stop establish remote custody')
        faults = [e for e in events if e['event'] == 'fault']
        if len(faults) == 1:
            wire['fault'] = faults[0]
        if forced or proxy.stderr or proxy.overflow:
            wire['status'] = 'INCOMPLETE'
        elif wire.get('exit_code') == 0 and wire['status'] != 'INCOMPLETE':
            # Transport complete is not semantic qualification of its counters.
            wire['status'] = 'CAPTURED'


def invoke(pg, client, output, database, config, mode, helper, proxy_source, fault=None):
    """The unchanged wrapper owns the real CLI; this owner only faults its wire."""
    prefix = output / mode
    proxy = None
    terminal = None
    response = dict(returncode=None, stdout='', stderr='', outcome='OUTCOME_UNKNOWN')
    clock = {}
    wire = None if fault is None else fault['wire']
    started = time.monotonic()
    try:
        if fault is not None:
            proxy = Process(['docker', 'exec', '-i', client, 'python3', '-B',
                '/lab/' + proxy_source.name, '--upstream-port', '5432',
                '--mode', fault['mode'], '--commit-number', str(fault['commit_number']),
                '--deadline-seconds', '30'], input_pipe=True)
            ready = proxy.event('ready', 5)
            wire.update(status='RUNNING', ready=ready, proxy_sha256=digest(proxy_source))
            lab.write_json(output / (mode + '-wire.json'), wire)
            assert ready['proxy_sha256'] == digest(proxy_source)
            assert ready['listen_host'] == '127.0.0.1' and ready['upstream_port'] == 5432
            assert ready['mode'] == fault['mode'] and ready['commit_number'] == fault['commit_number']
            fault['config'] = proxy_config(output, config, mode, ready)
            config = fault['config']['proxy_path']
            lab.write_json(output / (mode + '-fault.json'), fault)
        clock['before_us'] = server_clock(pg, database)
        lab.write_json(output / (mode + '-clock.json'), clock)
        lab.docker('exec', '-d', client, 'python3', '/lab/' + helper.name,
            config, '/lab/' + mode, timeout=5)
        wait_for(lambda: Path(str(prefix) + '.terminal.json').exists(), 'activated recovery joined', 50)
        terminal = json.loads(Path(str(prefix) + '.terminal.json').read_text())
        clock['after_us'] = server_clock(pg, database)
    except Exception as error:
        response['orchestration_error'] = type(error).__name__
        if wire is not None:
            wire.update(status='INCOMPLETE', error=type(error).__name__)
    finally:
        path = Path(str(prefix) + '.terminal.json')
        if terminal is None and path.exists():
            try:
                terminal = json.loads(path.read_text())
            except (ValueError, TypeError) as error:
                response['terminal_error'] = type(error).__name__
        for stream in ('stdout', 'stderr'):
            path = Path(str(prefix) + '.' + stream)
            if path.exists():
                with path.open('rb') as captured:
                    raw = captured.read(65537)
                response[stream] = raw[:65536].decode(errors='replace')
                if len(raw) > 65536:
                    response['output_limit_exceeded'] = True
        response.update(terminal=terminal, seconds=time.monotonic() - started)
        if isinstance(terminal, dict) and terminal.get('reaped') is True and terminal.get('forced') is False:
            response.update(returncode=terminal['exit_code'], outcome='PROCESS_EXITED')
            if 'after_us' not in clock:
                try:
                    clock['after_us'] = server_clock(pg, database)
                except Exception as error:
                    response['clock_error'] = type(error).__name__
        # Preserve the process observation before bounded proxy collection.
        lab.write_json(output / (mode + '-response.json'), response)
        lab.write_json(output / (mode + '-clock.json'), clock)
        if proxy is not None:
            finish_proxy(proxy, wire)
        if fault is not None:
            lab.write_json(output / (mode + '-wire.json'), wire)
            lab.write_json(output / (mode + '-fault.json'), fault)
    return response, clock


def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_recovery_client.py')
    proxy_source = Path(__file__).resolve().parents[1] / 'control-migrations/fault_proxy.py'
    for source in (helper, proxy_source):
        target = output / source.name
        if not target.exists():
            shutil.copy2(source, target)
        assert digest(target) == digest(source)
    proxy_source = output / proxy_source.name
    spec = recovery.SOURCES['off']
    basis = output.parent.parent / spec['run'] / 'bindings'
    store = basis / spec['prefix'] / 'store'
    frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
        spec['prefix'] + '-after-native.json': spec['native'],
        spec['prefix'] + '-verification.json': spec['verification']}
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    source = None
    cases = []

    def dump(label, directory):
        answer, _ = capture(client, output, label + '-dump', 'dump-state', '/lab/' + directory.name + '/store')
        assert answer.returncode == 0 and not answer.stderr
        value = json.loads(answer.stdout)
        lab.write_json(output / (label + '-native.json'), value)
        return value

    def save_sql(label, database):
        value = rows(database)
        lab.write_json(output / (label + '-sql.json'), value)
        return value

    def execute(mode, database, config, directory, seed, setup, fault, predecessor=None):
        observe_backend_absence(pg, database, output, mode + '-before')
        before = save_sql(mode + '-before', database)
        baseline = dump(mode + '-before', directory)
        originals = inputs_at(directory / 'store')
        current_setup = copy.deepcopy(setup)
        current_setup['catalog_before'] = recovery.catalog(pg, database)
        lab.write_json(output / (mode + '-before-inputs.json'), originals)
        response, clock = invoke(pg, client, output, database, config, mode, helper, proxy_source,
            fault if predecessor is None else None)
        if response['outcome'] != 'PROCESS_EXITED':
            lab.write_json(output / (mode + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                reason='Native custody not excluded; outer owner must join before reopening'))
            raise AssertionError('installed CLI custody unavailable')
        try:
            absent = observe_backend_absence(pg, database, output, mode)
        except Exception as error:
            unavailable = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN',
                reason='Backend exclusion unavailable after joined CLI', error=type(error).__name__)
            lab.write_json(output / (mode + '-external-unavailable.json'), unavailable)
            # These are diagnostic observations, not a final state proof. Keep
            # the normal after-state capture strictly after backend exclusion.
            try:
                save_sql(mode + '-uncertain', database)
                lab.write_json(output / (mode + '-uncertain-inputs.json'),
                    dict(inputs=inputs_at(directory / 'store'),
                        contents=recovery.contents_at(directory / 'store')))
            except Exception as capture_error:
                unavailable['capture_error'] = type(capture_error).__name__
                lab.write_json(output / (mode + '-external-unavailable.json'), unavailable)
            raise
        after = save_sql(mode + '-after', database)
        final_native = dump(mode + '-after', directory)
        final_inputs = inputs_at(directory / 'store')
        contents = recovery.contents_at(directory / 'store')
        current_setup['catalog_after'] = recovery.catalog(pg, database)
        lab.write_json(output / (mode + '-external.json'), dict(control=after, native=final_native,
            inputs=final_inputs, input_contents=contents, catalog=current_setup['catalog_after'], absence=absent))
        exact = exact_absence(pg, database, output, mode, after, before)
        context = dict(before=before, after=after, native_before=baseline, native_after=final_native,
            inputs_before=originals, inputs_after=final_inputs, input_contents=contents,
            response=response, mode=mode, seed=seed, setup=current_setup, server_window=clock,
            backend=dict(exact=exact, absence=absent), fault=copy.deepcopy(fault), predecessor=predecessor)
        lab.write_json(output / (mode + '-verification.json'), context)
        assert not any(name in response for name in ('orchestration_error', 'terminal_error',
            'clock_error', 'output_limit_exceeded'))
        evidence = verify(**context)
        perturbations = negative_controls(**context)
        return dict(name=mode, status='PASS', oracle=evidence, negative_controls=perturbations,
            verification_sha256=digest(output / (mode + '-verification.json'))), context

    try:
        assert all(digest(basis / name) == sha for name, sha in frozen.items())
        retained = json.loads((basis / 'result.json').read_text())
        assert retained['status'] == spec['status'] and retained['source'] == spec['source'] and retained['owner'] == spec['owner']
        assert not retained['cleanup_errors'] and not retained['unrelated_changed'] and not retained['missing_owned']
        old, = [c for c in containers if c['Id'] == retained['client']['id']]
        assert old['Config']['Labels'][retained['label']] == spec['owner']
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
        absent = observe_backend_absence(pg, spec['database'], output, 'activated-fault-source')
        original = rows(spec['database'])
        assert original == json.loads((basis / spec['sql_file']).read_text())
        seed = dict(name='off', control=original,
            native=json.loads((basis / (spec['prefix'] + '-after-native.json')).read_text()),
            inputs=inputs_at(store), contents=recovery.contents_at(store))
        known(seed, 'off')
        source = dict(seed=seed, files=census(store), frozen=frozen,
            catalog=recovery.previous_catalog(pg, spec['database']), backend_absence=absent,
            client_id=old['Id'], client_state=old['State'], label=retained['label'])
        lab.write_json(output / 'activated-fault-source.json', source)
        for mode in CASES:
            database = clone(mode, spec['database'])
            assert rows(database) == seed['control']
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert dump(mode + '-seed', directory) == seed['native']
            assert recovery.contents_at(directory / 'store') == seed['contents']
            config = configuration(pg, client, output, database, directory, seed['control'])
            migration = recovery.upgrade(pg, client, output, database, config, mode, rows)
            setup = dict(migration=migration, migration_sha256=digest(Path(__file__).resolve().parents[2] /
                'migrations/postgres/0033_activated_initial_recovery.sql'),
                correction_sha256=digest(Path(__file__).resolve().parents[2] /
                    'migrations/postgres/0034_activated_control_retirement.sql'),
                policy=recovery.advance_policy(pg, database, output, mode, rows), removed_copy=None)
            point = 'close' if '-close-' in mode else 'outcome'
            if point == 'outcome':
                row, = [r for r in rows(database)['producer_outcomes'] if r['producer_epoch'] == 2 and r['request_sequence'] == 2]
                removed = dict(row=row, statement=REMOVE_COPY)
                lab.write_json(output / (mode + '-removed-outcome.json'), removed)
                removed['response'] = recovery.statement(pg, database, output, mode + '-remove-copy', REMOVE_COPY)
                lab.write_json(output / (mode + '-removed-outcome.json'), removed)
                setup['removed_copy'] = removed
            fault = dict(point=point, mode='before-forward' if mode.endswith('-before') else 'after-complete',
                commit_number=2 if point == 'close' else 6, config=None, wire=dict(status='NOT_RUN'))
            failed, prior = execute(mode, database, config, directory, seed, setup, fault)
            recovered, _ = execute(mode + '-recover', database, config, directory, seed, setup, fault, prior)
            cases.append(dict(name=mode, status='PASS', attempts=[failed, recovered]))
            lab.write_json(output / 'activated-recovery-fault-cases.json', cases)
    finally:
        if source is not None:
            old, = json.loads(lab.docker('inspect', source['client_id']).stdout)
            preserved = dict(control=rows(spec['database']) == source['seed']['control'],
                files=census(store) == source['files'], inputs=inputs_at(store) == source['seed']['inputs'],
                contents=recovery.contents_at(store) == source['seed']['contents'],
                catalog=recovery.previous_catalog(pg, spec['database']) == source['catalog'],
                client=old['State'] == source['client_state'] and
                    old['Config']['Labels'][source['label']] == spec['owner'],
                artifacts=all(digest(basis / name) == sha for name, sha in frozen.items()))
            lab.write_json(output / 'activated-recovery-fault-source-preserved.json', preserved)
            assert all(preserved.values()), preserved
    assert len(cases) == 4 and sum(len(c['attempts']) for c in cases) == 8
    return dict(name='installed-activated-recovery-faults', status='PASS', cli_invocations=8,
        cases=cases, source_preserved=preserved, helper_sha256=digest(helper), proxy_sha256=digest(proxy_source),
        scope='actual installed closing/outcome COMMIT loss and separate config-only positive recovery',
        not_run=['driver-result or same-session poison telemetry', 'issued-positive source-zero',
            'corrupt/missing inputs and dependencies', 'source2 closure/retirement', 'full M1'])
