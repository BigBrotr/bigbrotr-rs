#!/usr/bin/env python3
"""Three installed successor lifetimes on exact retained terminal copies.

Only the actual Serve process owns native state. HTTP creates the job and Sync
claims it; the fixture never writes a claim, producer input or scientific row.
Unknown process custody preserves SQL/input evidence without reopening native.
"""
import hashlib
import json
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
import transition
from installed import messages, set_value, wait_for
from installed_empty import exact_absence, signal_child
from installed_live import stop_quiet
from live_replay import OLD_EVENT, NEW_EVENT

history, schema, lab = transition.history, transition.schema, transition.lab
read, normalized = transition.read, transition.normalized
ENDPOINT = 'ws://127.0.0.1:7447/installed-successor-fixture'
CASES = ('activated-live3', 'empty-live3', 'later-live4')
CLONES = ('activated', 'empty', 'later')
HELPERS = ('installed_family_client.py', 'installed_sync_client.py')

ORIGINS = ({'name': 'activated',
  'run': 'ingest-activated-retirement-engine-9bdc86a',
  'subdirectory': 'bindings',
  'database': 'bb_scope_bind_9bdc86a_installed_activated_retirement_off',
  'status': 'PASS_INSTALLED_ACTIVATED_RETIREMENT_FIRST_WAVE',
  'source': '9bdc86a2316e0aa0d1078ededb80a8d9c9502a2f',
  'owner': '1979cce97962b0239e79e3fa47b2e5ae',
  'label': 'org.bigbrotr.acquisition-binding',
  'client_id': 'fe56e36ac2dd47fa41842cef7d6bbc8564e2032352c5097f68448047bb2c89c0',
  'source_clone': 'installed-activated-retirement-off',
  'final_case': 'installed-activated-retirement-off-retry',
  'catalog_sha256': 'baa02a5fc187770cd8b5782ab47feab4dd11d54667aa2737d19411db546e9276',
  'result_sha256': '48506a152b393854f2f50cbedfedb71b66e53ed9578753d7e6b4df3bd76c8e09',
  'snapshot': 'installed-activated-retirement-off-retry-after-sql.json',
  'snapshot_sha256': '7ac690057de370a00144ffd415bb1f504b05ff4b5599d6458b728820a98acbad',
  'native_file': 'installed-activated-retirement-off-retry-after-native.json',
  'native_sha256': 'cd49f8bfc44c5c08ae307f845f1aaf1da7f66079bf14f8ee54f9c4c1a3819238',
  'inputs_file': 'installed-activated-retirement-off-retry-after-inputs.json',
  'inputs_sha256': 'f59224bde2f441efa1b74bbdb64c65ae5a30bdd2f283f9d31b5dd5c3adb82589',
  'store': 'installed-activated-retirement-off/store',
  'origin_format': 'legacy-installed',
  'verification_file': 'installed-activated-retirement-off-retry-verification.json',
  'verification_sha256': 'b1e0b4eaada03b9c169592750d70285e8234abfa7ebbebc61877255b3b046d82'},
 {'name': 'empty',
  'run': 'ingest-empty-installed-engine-788f971',
  'subdirectory': 'bindings',
  'database': 'bb_scope_bind_788f971_installed_empty_normal',
  'status': 'PASS_INSTALLED_EMPTY_RETIREMENT',
  'source': '788f9711e19776c4441042747ef68d5f2057605f',
  'owner': 'a736fb2609186b694cf08d128696083b',
  'label': 'org.bigbrotr.acquisition-binding',
  'client_id': '40f8a4e5f9c86ed6c5c0600b1d47c3d3ecf24ac0382252326e1d2effad84b8ff',
  'source_clone': 'installed-empty-normal',
  'final_case': 'installed-empty-normal-terminal',
  'catalog_sha256': 'f275e865518402db42799a42f6571dfae4e7299e576b784c21a9c5d6e7ce3b1e',
  'result_sha256': 'ff1d2e111dc42ff4026c7e42985a7814dbf058eb0bd1d7ba92c5e269e7642d34',
  'snapshot': 'installed-empty-normal-terminal-after.json',
  'snapshot_sha256': '5cc9803df1c4f2093f56694ca08857bbdf62066c51f260a35637ce2208aa7579',
  'native_file': 'installed-empty-normal-terminal-after-native.json',
  'native_sha256': '53ca6c812d7ed8de07a0843af25b438794e5aa7b4f925f296c1c0de80c14097b',
  'inputs_file': 'installed-empty-normal-terminal-after-inputs.json',
  'inputs_sha256': 'c757c9978a581ccef16c249f0fd1d8d15c64d9996b4c02b3bea1830a86cddba0',
  'store': 'installed-empty-normal/store',
  'origin_format': 'legacy-installed',
  'verification_file': 'installed-empty-normal-terminal-verification.json',
  'verification_sha256': '3802cfce18da75320ccb2b9f6b5c49f11c077a2f0d258168c2610f6593f65187'},
 {'name': 'later',
  'run': 'ingest-succession-retirement-fault-engine-58b79a4',
  'subdirectory': 'retirement-faults',
  'database': 'bb_successor_retirement_fault_58b79a4_before',
  'status': 'PASS_SUCCESSOR_RETIREMENT_FAULTS',
  'source': '58b79a49e59be77be554fff0be826dbe8ee20903',
  'owner': '3c545f54303dc8aafd23e00253f613e7',
  'label': 'org.bigbrotr.succession-retirement-fault',
  'client_id': 'e87ef2ecbe2387731f97add4dbd74998318c084d8fcb0916c98b530822ffd80c',
  'source_clone': 'before',
  'final_case': 'before-forward-fresh',
  'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
  'result_sha256': '4690448e2ac9b565dea2fd1c5f3207831a3d0a77ef3dca5d7896d1928abfc03f',
  'snapshot': 'before-forward-fresh-after.json',
  'snapshot_sha256': '43ec972783bf1d82bad94ab954dd605e32d50da7f6db3acf48c95778280415b3',
  'native_file': 'before-forward-fresh-after-native.json',
  'native_sha256': '269bf396c8cfc09588405495364359df1fab33737036d30990469530aa5722bc',
  'inputs_file': 'before-forward-fresh-after-inputs.json',
  'inputs_sha256': 'c757c9978a581ccef16c249f0fd1d8d15c64d9996b4c02b3bea1830a86cddba0',
  'store': 'before/store'})

SUITE = transition.Suite(directory='installed-family', label='org.bigbrotr.installed-successor',
    status='PASS_INSTALLED_SUCCESSOR_FAMILY', client_prefix='bigbrotr-installed-successor-',
    database_prefix='bb_installed_successor_', clones=CLONES, cases=CASES,
    scope='Three actual Serve lifetimes: terminal2 activated/off, terminal2 empty/on, terminal3/on; '
          'authenticated finite acquisition, relay admission/certification, renewal and joined SIGTERM',
    not_run=('Installed source/workflow/retirement cycle', 'Archive publication or coverage',
        'Writer adoption, unresolved input cleanup, COMMIT loss, capacity/RSS qualification'))


def configure(pg, client, output, config, *, ack=False):
    assert type(ack) is bool
    path = output / Path(config).name
    text = path.read_text()
    for section, key, value in [
        ('topology', 'local_roles', ['control', 'ingest', 'sync', 'archive']),
        ('administration.listen', 'port', 18081),
        ('administration', 'enabled', False), ('sync', 'enabled', False),
        ('ingest', 'producer_outstanding_requests', 2 if ack else 65),
        ('network.dns', 'resolver', '127.0.0.1:9'), ('network.tls', 'extra_roots', []),
        ('runtime', 'lease_ttl', '6s'), ('runtime', 'heartbeat_interval', '1s'),
        ('runtime', 'renewal_timeout', '1s'), ('runtime', 'authorization_ttl', '5s'),
        ('runtime', 'graceful_shutdown_timeout', '2s'),
    ]:
        text = set_value(text, section, key, value)
    rule = [dict(endpoint=ENDPOINT, cidr='127.0.0.1/32', port=7447)]
    text = set_value(text, 'network.private', 'rules', rule).replace(json.dumps(rule),
        '[{endpoint="' + ENDPOINT + '",cidr="127.0.0.1/32",port=7447}]')
    path.write_text(text)
    answer = lab.cli(dict(pg, client=client), 'config', 'validate', '--file', config)
    lab.write_json(output / (path.stem + '-installed-config-validation.json'), dict(
        returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr))
    assert answer.returncode == 0 and not answer.stderr


def process_closed(response, case):
    """The final report is emitted by run_owned only after native/driver joins."""
    assert response['outcome'] == 'PROCESS_EXITED' and response['returncode'] == 0
    assert not response['stderr'] and 'capture_error' not in response
    terminal = response['terminal']
    assert terminal['reaped'] is True and terminal['forced'] is False and terminal['exit_code'] == 0
    assert terminal['config'] == case['config'] and terminal['prefix'] == '/lab/' + case['name']
    assert terminal['command'] == ['/build/bigbrotr', 'serve', '--file', case['config'],
        '--activate-successor', '--roles', 'control,ingest,sync']
    activated, stopped = [json.loads(line) for line in response['stdout'].splitlines()]
    assert activated['format'] == stopped['format'] == 'bigbrotr-ingest-successor/1'
    assert activated['state'] == 'activated'
    assert stopped == {key: activated[key] for key in ('format', 'dataset_id', 'owner_resource_id',
        'producer_slot_id', 'producer_epoch')} | dict(state='stopped', native='closed',
        owner_release='committed', source_closure='not-performed', ready=False)
    return activated, stopped


def capture_progress_observation(directory, phase, observation, wire):
    """Retain a phase cut; this does not validate any checkpoint or native proof.

    The installed owner must supply its complete existing SQL/input snapshot.
    Only the final independent progress oracle can establish durable progress.
    Never open native state while Serve is alive to obtain this observation.
    """
    assert phase in ('mutation', 'proof-only')
    assert observation['stage'] == 'progress-' + phase
    assert set(observation) == {'stage', 'server_us', 'observed_by_us', 'control', 'inputs'}
    assert wire['progress_phase'] == phase and wire['completion'] == 'request-and-websocket-close'
    assert wire['closed_at_us'] <= observation['server_us'] <= observation['observed_by_us']
    assert isinstance(observation['control'], dict) and isinstance(observation['inputs'], dict)
    value = dict(format='bigbrotr-progress-transport-observation/1',
        phase=phase, observation=observation, wire=wire)
    path = directory / ('progress-' + phase + '-observation.json')
    raw = (json.dumps(value, indent=2) + '\n').encode()
    with path.open('xb') as stream:
        stream.write(raw)
    return dict(phase=phase, request_id=wire['request'][1],
        observation_sha256=hashlib.sha256(raw).hexdigest())


def release_progress_transport(directory, captured):
    """Release a later real REQ after a retained cut, without claiming its meaning.

    This transport preparation is deliberately not called by the qualified
    default runner. The new profile still needs the frozen typed checkpoint
    predicate before release and the complete independent final oracle.
    """
    assert set(captured) == {'phase', 'request_id', 'observation_sha256'}
    assert captured['phase'] == 'mutation'
    path = directory / 'progress-mutation-observation.json'
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == captured['observation_sha256']
    observation = json.loads(raw)
    assert observation['phase'] == 'mutation'
    assert observation['wire']['request'][1] == captured['request_id']
    gate = dict(format='bigbrotr-progress-transport-release/1',
        mutation_request=captured['request_id'],
        observation_sha256=captured['observation_sha256'])
    target = directory / 'progress-release.json'
    assert not target.exists() and not target.is_symlink()
    partial = directory / 'progress-release.json.partial'
    with partial.open('x') as stream:
        json.dump(gate, stream, indent=2)
        stream.write('\n')
    partial.rename(target)
    return gate


def execute(pg, client, output, case, directory, expected, catalog, *, progress=False, hole=False, fault=None, ack=False):
    from installed_family_replay import verify, negative_controls
    assert type(progress) is type(hole) is bool and (not hole or progress)
    assert fault is None or (progress and not hole)
    assert type(ack) is bool and (not ack or (progress and not hole and case['origin']=='activated'))
    if progress:
        import installed_progress_replay as progress_replay
        progress_replay.require_frozen_profile(ack=ack)
    name, database = case['name'], case['database']
    prefix = output / name
    before = schema.rows(pg, database)
    if ack:
        dataset, = before['datasets']
        for table, field in (('config_revisions','config_revision'),('policy_revisions','policy_revision')):
            current, = [r for r in before[table] if r['revision']==dataset[field]]
            assert current['document']['settings']['ingest.producer_outstanding_requests']==2
    native = history.dump(client, output, name + '-before', directory)
    inputs = history.inputs_at(directory / 'store')
    lab.write_json(output / (name + '-before.json'), before)
    lab.write_json(output / (name + '-inputs.json'), inputs)
    assert normalized(before) == normalized(expected[0]) and native == expected[1] and inputs == expected[2]
    dataset, = before['datasets']
    old_scope, = [r for r in before['allocation_domains'] if r['owner_epoch'] == 1 and r['dictionary_kind'] == 4]
    fixture_input = dict(config_revision=str(dataset['config_revision']), policy_revision=str(dataset['policy_revision']),
        visibility=dict(dataset_id=dataset['dataset_id'][2:], dictionary_kind=4,
            allocation_domain=old_scope['allocation_domain'][2:], ordinal='3'))
    lab.write_json(directory / 'fixture.json', fixture_input)
    lab.write_json(directory / 'events.json', [OLD_EVENT, NEW_EVENT])
    snapshots, window = [], {}
    fixture = dict(cleanup_errors=[])
    response = dict(returncode=None, stdout='', stderr='', outcome='OUTCOME_UNKNOWN', terminal=None)
    terminal, journey_started, failure = None, False, None

    def save_sql(stage):
        value = schema.rows(pg, database)
        lab.write_json(output / (name + '-' + stage + '.json'), value)
        return value

    def snapshot(stage):
        value = dict(stage=stage, server_us=history.server_clock(pg, database),
            control=save_sql(stage), inputs=history.inputs_at(directory / 'store'))
        value['observed_by_us'] = history.server_clock(pg, database)
        snapshots.append(value)
        lab.write_json(output / (name + '-observations.json'), snapshots)
        return value

    def advance(stage):
        (directory / (stage + '.continue')).write_text('observed\n')

    try:
        if fault is not None:
            fault.start()
        window['before_us'] = history.server_clock(pg, database)
        lab.write_json(output / (name + '-clock.json'), window)
        lab.docker('exec', '-d', client, 'python3', '/lab/installed_family_client.py', 'serve',
            case['config'], '/lab/' + name, timeout=5)
        wait_for(lambda: Path(str(prefix) + '.pid.json').exists(), 'successor Serve identity', 5)
        published = wait_for(lambda: messages(prefix) or Path(str(prefix) + '.terminal.json').exists(),
            'successor activation report', 15)
        assert isinstance(published, list) and len(published) == 1, 'Serve exited before activation'
        initial, = published
        initialized = snapshot('activated')
        epoch = int(initial['producer_epoch'])
        target, = [r for r in initialized['control']['owner_epochs'] if r['role'] == 'ingest' and r['epoch'] == epoch]
        if ack:
            assert epoch==3
            producer, = [r for r in initialized['control']['producer_epochs'] if r['producer_epoch']==epoch]
            assert producer['outstanding_request_limit']==2
        wrapper = '''import subprocess,json,sys
from pathlib import Path
p=Path(sys.argv[1])
with (p/'client.stdout').open('xb') as out,(p/'client.stderr').open('xb') as err:
 try:
  r=subprocess.run(['python3','/lab/installed_family_client.py','journey',str(p)],stdout=out,stderr=err,timeout=45)
  value={'exit_code':r.returncode,'reaped':True,'outcome':'PROCESS_EXITED'}
 except subprocess.TimeoutExpired:
  value={'exit_code':None,'reaped':False,'outcome':'UNKNOWN_TIMEOUT'}
(p/'client-exit.json').write_text(json.dumps(value))
'''
        if progress:
            wrapper = wrapper.replace("'journey'", 'sys.argv[2]')
            journey_mode = ('journey-progress-ack-loss' if ack and fault is not None else 'journey-progress-ack' if ack else 'journey-progress-loss' if fault is not None else
                'journey-progress-hole' if hole else 'journey-progress')
            lab.docker('exec', '-d', client, 'python3', '-c', wrapper, '/lab/' + directory.name, journey_mode)
        else:
            lab.docker('exec', '-d', client, 'python3', '-c', wrapper, '/lab/' + directory.name)
        journey_started = True
        for stage in ('prepared', 'submitted'):
            wait_for(lambda: (directory / (stage + '.json')).exists() or (directory / 'client-exit.json').exists(),
                'successor HTTP ' + stage, 15)
            assert (directory / (stage + '.json')).exists(), 'HTTP client exited before ' + stage
            if stage == 'submitted' and not (ack and fault is not None):
                expected_count = 4 if ack else 2 if case['origin'] == 'activated' else 3
                def durable():
                    actual = schema.rows(pg, database)
                    return actual if len([r for r in actual['producer_outcomes'] if r['producer_epoch'] == epoch]) == expected_count else None
                wait_for(durable, 'successor issued durable prefix', 10)
                if progress:
                    for phase in (('mutation',) if fault is not None else ('mutation', 'proof-only')):
                        def ready():
                            assert not Path(str(prefix) + '.terminal.json').exists(), 'Serve exited before progress'
                            wires = [read(path) for path in sorted(directory.glob('relay-closed-*.json'))]
                            found = [wire for wire in wires if wire.get('progress_phase') == phase]
                            assert len(found) <= 1, 'replayed progress phase'
                            if not found:
                                return None
                            state = schema.rows(pg, database)
                            if ack:
                                from installed_progress_context_oracle import ready as ack_ready
                                return found[0] if ack_ready(state,'successor',phase,found[0],ack=True) else None
                            return found[0] if progress_replay.phase_ready(
                                state, case['origin'], hole, phase, epoch, found[0]) else None
                        wire = wait_for(ready, 'durable progress ' + phase, 10)
                        observed = snapshot('progress-' + phase)
                        if ack:
                            from installed_progress_context_oracle import ready as ack_ready
                            assert ack_ready(observed['control'],'successor',phase,wire,ack=True)
                        else:
                            progress_replay.require_phase(observed['control'], case['origin'],
                                hole, phase, epoch, wire)
                        captured = capture_progress_observation(directory, phase, observed, wire)
                        if phase == 'mutation':
                            if fault is not None:
                                fault.gate(captured, observed)
                            release_progress_transport(directory, captured)
            if stage == 'submitted' and fault is not None:
                wait_for(Path(str(prefix) + '.terminal.json').exists, 'COMMIT-loss Serve natural join', 10)
                terminal = read(Path(str(prefix) + '.terminal.json'))
                if ack:
                    # Positive custody is checked below; this only binds the
                    # exact Control backend for the proxy's selected session.
                    fault.current = max((r for r in schema.rows(pg,database)['owner_epochs']
                        if r['role']=='control'),key=lambda r:r['epoch'])
            else:
                snapshot(stage)
            advance(stage)
        wait_for(lambda: (directory / 'commands-finished.json').exists() or (directory / 'client-exit.json').exists(),
            'successor command client completion', 15)
        assert (directory / 'commands-finished.json').exists(), 'HTTP client exited before completion'
        if fault is None:
            initial_deadline = int(initial['lease_expires_at_us'])
            minimum = max(initial_deadline, int(initial['prepared_at_us']) + 6_000_000)
            def renewed():
                now = history.server_clock(pg, database)
                assert not Path(str(prefix) + '.terminal.json').exists(), 'Serve exited before renewal observation'
                return now if now > minimum else None
            at = wait_for(renewed, 'successor lifetime beyond initial lease and six seconds', 15)
            current = snapshot('renewed')
            fixture['renewal'] = dict(initial_deadline_us=initial_deadline, observed_at_us=at,
                initial_target=target, current=current)
            snapshot('before-stop')
            identity = read(Path(str(prefix) + '.pid.json'))
            fixture['signal'] = stop_quiet(client, directory, identity, initial_deadline)
            fixture['quiet'] = fixture['signal']['opened']
            wait_for(Path(str(prefix) + '.terminal.json').exists, 'successor Serve join', 12)
            terminal = read(Path(str(prefix) + '.terminal.json'))
    except BaseException as error:
        failure = error
        response['capture_error'] = repr(error)
    finally:
        terminal_path = Path(str(prefix) + '.terminal.json')
        try:
            if terminal is None and terminal_path.exists():
                terminal = read(terminal_path)
            if terminal is None and Path(str(prefix) + '.pid.json').exists():
                signal_child(client, read(Path(str(prefix) + '.pid.json')))
                wait_for(terminal_path.exists, 'failed successor Serve join', 5)
                terminal = read(terminal_path)
        except BaseException as error:
            fixture['cleanup_errors'].append(dict(stage='serve-join', error=repr(error)))
        if journey_started:
            try:
                for stage in ('prepared', 'submitted', 'commands-finished'):
                    advance(stage)
                wait_for((directory / 'client-exit.json').exists, 'HTTP/relay join', 5)
                fixture['client_terminal'] = read(directory / 'client-exit.json')
            except BaseException as error:
                fixture['cleanup_errors'].append(dict(stage='client-join', error=repr(error)))
        if fault is not None:
            try:
                fault.finish()
            except BaseException as error:
                fixture['cleanup_errors'].append(dict(stage='proxy-join', error=repr(error)))
        try:
            window['after_us'] = history.server_clock(pg, database)
        except BaseException as error:
            fixture['cleanup_errors'].append(dict(stage='final-clock', error=repr(error)))
        for stream in ('stdout', 'stderr'):
            try:
                path = Path(str(prefix) + '.' + stream)
                if path.exists():
                    raw = path.read_bytes()
                    assert len(raw) <= 65536, 'Serve output limit'
                    response[stream] = raw.decode()
            except BaseException as error:
                fixture['cleanup_errors'].append(dict(stage=stream, error=repr(error)))
        response['terminal'] = terminal
        if terminal and terminal['reaped'] is True and terminal['forced'] is False:
            response.update(returncode=terminal['exit_code'], outcome='PROCESS_EXITED')
        lab.write_json(output / (name + '-response.json'), response)
        lab.write_json(output / (name + '-clock.json'), window)
        lab.write_json(output / (name + '-fixture.json'), fixture)
        after = after_inputs = after_native = after_catalog = None
        backend, errors = {}, []
        for stage, capture in [('sql', lambda: save_sql('provisional-sql' if fault is not None else 'after')),
                               ('inputs', lambda: history.inputs_at(directory / 'store'))]:
            try:
                value = capture()
                if stage == 'sql':
                    after = value
                else:
                    after_inputs = value
                    lab.write_json(output / (name + '-after-inputs.json'), value)
            except BaseException as error:
                errors.append(dict(stage=stage, error=repr(error)))
        closed = False
        try:
            if fault is None:
                process_closed(response, case)
            else:
                fault.process_closed(response, case)
                assert not fixture['cleanup_errors'], 'fault process/relay/proxy custody incomplete'
                assert fixture['client_terminal'] == dict(exit_code=0,reaped=True,outcome='PROCESS_EXITED')
            closed = True
        except BaseException as error:
            errors.append(dict(stage='native-custody', error=repr(error)))
        try:
            backend['absence'] = history.observe_backend_absence(pg, database, output, name + '-control')
            if fault is not None:
                after = save_sql('after')
            old_keys = {(r['resource_id'], r['epoch']) for r in before['owner_epochs']}
            assert after is not None
            tuples = [dict(pid=r['control_backend_pid'], backend_start=r['control_backend_start'],
                database=database, control_epoch=r['epoch']) for r in after['owner_epochs']
                if r['role'] == 'control' and (r['resource_id'], r['epoch']) not in old_keys]
            backend['exact'] = exact_absence(pg, database, tuples)
            backend['all'] = schema.absence(pg, database)
            assert backend['all'] == []
            lab.write_json(output / (name + '-backends.json'), backend)
        except BaseException as error:
            errors.append(dict(stage='backend-absence', error=repr(error)))
        if closed and not errors:
            try:
                after_native = history.dump(client, output, name + '-after', directory)
                after_catalog = history.catalog(pg, database)
            except BaseException as error:
                errors.append(dict(stage='external-native-catalog', error=repr(error)))
        else:
            lab.write_json(output / (name + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', sql_and_inputs='provisional', cleanup='outer owned client exclusion'))
        lab.write_json(output / (name + '-external.json'), dict(control=after, native=after_native,
            inputs=after_inputs, backend=backend, catalog=after_catalog))
        custody = dict(status='OBSERVED_CLOSED_AND_JOINED' if closed and not errors else 'OUTCOME_UNKNOWN',
            database=database, backend_absent=backend.get('all') == [], errors=errors)
        lab.write_json(output / (name + '-custody.json'), custody)
        case['custody'] = custody['status']
    if failure is not None:
        raise failure
    assert not fixture['cleanup_errors'] and not errors
    journey = dict(result=read(directory / 'client-result.json'), terminal=fixture['client_terminal'],
        stdout=(directory / 'client.stdout').read_text(), stderr=(directory / 'client.stderr').read_text(),
        opened=[read(p) for p in sorted(directory.glob('relay-open-*.json'))],
        closed=[read(p) for p in sorted(directory.glob('relay-closed-*.json'))])
    contents = {p.name: p.read_bytes().hex() for p in (directory / 'store/producer-inputs-v1').iterdir()}
    context = dict(case=dict(case), before=before, after=after, native_before=native, native_after=after_native,
        inputs_before=inputs, inputs_after=after_inputs, input_contents=contents, snapshots=snapshots,
        response=response, journey=journey, fixture=fixture, server_window=window, backend=backend,
        catalog_before=catalog, catalog_after=after_catalog, fixture_input=fixture_input)
    if progress:
        context['progress_profile'] = 'ack-commit-loss' if ack and fault is not None else 'ack' if ack else 'commit-loss' if fault is not None else 'hole' if hole else 'split'
        if ack:
            context['ack_event_recipe']=(directory/'progress-ack-events.json').read_text()
            context['progress_gates']=({} if fault is not None else {name:(directory/name).read_text() for name in (
                'progress-mutation-observation.json','progress-release.json','progress-proof-only-observation.json')})
    if fault is not None:
        context['fault'] = fault.evidence
    lab.write_json(output / (name + '-verification.json'), context)
    if fault is not None:
        from installed_family_capture import progress_transport_checked
        transport = progress_transport_checked(directory, journey, snapshots, window,
            lambda path: path.read_bytes(), lambda path: json.loads(path.read_bytes()), commit_loss=True,
            ack=ack,mutation_loss=ack)
        result, controls = fault.verify(context), fault.controls(context)
        case['progress_transport'] = transport
    elif progress:
        # Raw phase/gate binding precedes all scientific qualification.
        from installed_family_capture import progress_transport_checked
        transport = progress_transport_checked(directory, journey, snapshots, window,
            lambda path: path.read_bytes(), lambda path: json.loads(path.read_bytes()), hole=hole,ack=ack)
        result, controls = progress_replay.verify(context,ack=ack), progress_replay.controls(context,ack=ack)
        assert result['status'] == ('PASS_INSTALLED_PRODUCER_ACK' if ack else 'PASS_INSTALLED_PRODUCER_PROGRESS')
        assert controls['status'] == 'PASS' and controls['controls'] > 0
        case['progress_transport'] = transport
    else:
        result, controls = verify(context), negative_controls(context)
    case.update(status='PASS', returncode=response['returncode'], oracle=result, negative_controls=controls,
        server_window=window)
    return after, after_native, after_inputs


def run_sequence(pg, client, output, clone, directory, expected, report, *, progress=False, hole=False, ack=False):
    assert type(ack) is bool and (not ack or (progress and not hole and clone['origin']=='activated'))
    if ack:
        from installed_progress_replay import require_frozen_profile
        require_frozen_profile(ack=True)
    for helper in HELPERS:
        source = Path(__file__).with_name(helper) if helper == HELPERS[0] else (
            transition.SOURCE / 'experiments/ingest-bootstrap' / helper)
        target = output / helper
        if not target.exists():
            shutil.copy2(source, target)
        assert history.digest(target) == history.digest(source)
    if ack:
        from installed_progress_context import helpers
        helpers(output, directory, 'successor', ack=True)
    configure(pg, client, output, clone['config'], ack=ack)
    name = CASES[CLONES.index(clone['name'])]
    case = dict(name=name, clone=clone['name'], origin=clone['origin'], operation='installed-live',
        predecessor=None, database=clone['database'], config=clone['config'], status='RUNNING')
    report['cases'].append(case)
    lab.write_json(output / 'result.json', report)
    execute(pg, client, output, case, directory, expected, clone['catalog_before'],
        progress=progress, hole=hole, ack=ack)
    case['catalog_after'] = history.catalog(pg, clone['database'])
    assert case['catalog_after'] == clone['catalog_before']
    lab.write_json(output / 'result.json', report)
    return name


def run(root, build, *, progress=False, hole=False, ack=False):
    assert type(progress) is type(hole) is bool and (not hole or progress)
    assert type(ack) is bool and (not ack or (progress and not hole))
    if progress:
        from dataclasses import replace
        from installed_progress_replay import require_frozen_profile, PROGRESS_GRANTS
        require_frozen_profile(ack=ack)
        mode = 'ack' if ack else 'hole' if hole else 'split'
        suite = replace(SUITE, directory='installed-progress-' + mode,
            label='org.bigbrotr.installed-progress-' + mode,
            status='PASS_INSTALLED_PRODUCER_PROGRESS',
            client_prefix='bigbrotr-installed-progress-' + mode + '-',
            database_prefix='bb_installed_progress_' + mode + '_',
            scope='Separated real retained EVENT quanta with typed progress; ACK0 and all detail retained')
        if ack:
            suite = replace(suite,clones=('activated',),cases=('activated-live3',),
                status='PASS_INSTALLED_PRODUCER_ACK',
                scope='One actual epoch3 consumer of a retained window2 origin; three signed events then a proof-only REQ',
                not_run=(*suite.not_run,'Epoch4 and window32 origins; full-window proof-only and lost-completion recovery'))
        return transition.run(root, build, suite=suite,
            sequence=lambda *args: run_sequence(*args, progress=True, hole=hole, ack=ack),
            origin_specs=ORIGINS[:1] if ack else ORIGINS,
            origin_for_clone={'activated':'activated'} if ack else dict(zip(CLONES, CLONES)),
            extra_grants=(PROGRESS_GRANTS,), producer_progress=True, producer_ack=ack)
    return transition.run(root, build, suite=SUITE, sequence=run_sequence, origin_specs=ORIGINS,
        origin_for_clone=dict(zip(CLONES, CLONES)))


if __name__ == '__main__':
    assert len(sys.argv) in (3, 4)
    mode = sys.argv[3] if len(sys.argv) == 4 else None
    assert mode in (None, '--progress', '--progress-hole', '--producer-ack')
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True),
        progress=mode is not None, hole=mode == '--progress-hole', ack=mode == '--producer-ack')
