"""Actual installed live successor, authenticated finite job and real WebSocket.

Four private copies and five Serve invocations preserve the retired original.
The second invocation on the completed on-copy must refuse epoch2 unchanged.
No observed process failure is interpreted as native or SQL rollback.
"""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time

import lab
from fault_cases import Process
from installed import messages, set_value, wait_for
from installed_empty import exact_absence, native_ready, signal_child
from initial_gap import configuration
from initial_gap_policy import GUARDS, require_guards
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_live import catalog, upgrade
import initial_succession as preparing
from installed_live_oracle import verify, negative_controls

CASES = ('installed-live-off', 'installed-live-on', 'installed-live-epoch2',
    'installed-live-missing-original', 'installed-live-held')
HELPERS = ('installed_live_client.py', 'installed_sync_client.py')
ENDPOINT = 'ws://127.0.0.1:7447/installed-live-fixture'


def stop_quiet(client, directory, identity, initial_deadline):
    """Select and signal inside one owned container; retain the close race."""
    script = r'''import json,os,signal,sys,time
from pathlib import Path
r=json.loads(sys.argv[1]); directory=Path(sys.argv[2]); original=int(sys.argv[3])
p=Path('/proc')/str(r['pid']); deadline=time.monotonic()+4
while True:
 paths=sorted(directory.glob('relay-open-*.json'))
 if paths:
  opened=json.loads(paths[-1].read_text()); closed=directory/('relay-closed-%03d.json'%opened['number'])
  now=time.monotonic_ns()
  if opened['number']>1 and opened['request_at_us']>original and 0<=now-opened['request_monotonic_ns']<20_000_000 and not closed.exists():
   assert int(p.joinpath('stat').read_text().split()[21])==r['start_ticks']
   assert p.joinpath('cmdline').read_bytes().split(b'\0')==[v.encode() for v in r['command']]+[b'']
   observation=dict(kind='SIGTERM',identity=r,opened=opened,signal_at_us=time.time_ns()//1000,
    signal_monotonic_ns=time.monotonic_ns(),closed_before_signal=closed.exists())
   if not observation['closed_before_signal']:
    os.kill(r['pid'],signal.SIGTERM)
    observation.update(signal_returned_at_us=time.time_ns()//1000,signal_returned_monotonic_ns=time.monotonic_ns())
    (directory/'signal-observation.json').write_text(json.dumps(observation))
    break
 assert time.monotonic()<deadline,'no fresh outstanding quiet receive at signal'
 time.sleep(.001)
'''
    lab.docker('exec', client, 'python3', '-c', script, json.dumps(identity),
        '/lab/' + directory.name, str(initial_deadline), timeout=5)
    return json.loads((directory / 'signal-observation.json').read_text())


def policy(pg, database, output, label, rows, provenance):
    """Privileged clone fixture, all normal guards active; no activation claim."""
    before = rows(database)
    guard_before = json.loads(lab.sql(pg, database, 'SELECT ' + GUARDS))
    require_guards(guard_before)
    statements = []
    for table in ('config_revisions', 'policy_revisions'):
        old, = before[table]
        document = copy.deepcopy(old['document'])
        document['settings']['ingest.producer_outstanding_requests'] = 2
        document['settings']['sync.provenance'] = 'local_first_witness' if provenance else 'off'
        encoded = json.dumps(document, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        sha = hashlib.sha256(b'bigbrotr/product-policy/2\0' + encoded).hexdigest()
        common = (f"2,dataset_id,1," + ("schema_version,authority_mode," if table == 'config_revisions'
            else "2,schema_version,") + f"convert_from(decode('{encoded.hex()}','hex'),'UTF8')::jsonb,"
            f"decode('{sha}','hex')," + ("accepted_source_digest," if table == 'config_revisions' else "")
            + "(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,actor_id")
        statements.append(f'INSERT INTO control.{table} SELECT {common} FROM control.{table} WHERE revision=1;')
    statement = """BEGIN;
SET LOCAL search_path=pg_catalog;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='5s';
SET LOCAL idle_in_transaction_session_timeout='10s';
DO $fixture$ BEGIN
 IF current_setting('session_replication_role')<>'origin' OR EXISTS(
   SELECT FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
   JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='control' AND t.tgenabled NOT IN ('O','A')) THEN
   RAISE EXCEPTION 'installed live fixture requires ordinary guards';
 END IF;
 PERFORM 1 FROM control.datasets FOR UPDATE;
 IF (SELECT count(*) FROM control.datasets WHERE config_revision=1 AND policy_revision=1)<>1
 OR (SELECT count(*) FROM control.config_revisions)<>1
 OR (SELECT count(*) FROM control.policy_revisions)<>1 THEN
   RAISE EXCEPTION 'installed live fixture requires original policy heads';
 END IF;
END $fixture$;
""" + '\n'.join(statements) + """
UPDATE control.datasets SET config_revision=2,policy_revision=2 WHERE config_revision=1 AND policy_revision=1;
SELECT pg_current_xact_id()::text;
COMMIT;
"""
    (output / (label + '-policy.sql')).write_text(statement)
    context = dict(authority='privileged-isolated-fixture', product_policy_activation=False,
        before=before, guards_before=guard_before, sql=statement, provenance=provenance,
        server_window={'before_us': server_clock(pg, database)}, status='INCOMPLETE')
    try:
        answer = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
            '--no-password', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
            data=statement, timeout=30, check=False)
        context['response'] = dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr)
        context['server_window']['after_us'] = server_clock(pg, database)
        context['after'] = rows(database)
        context['guards_after'] = json.loads(lab.sql(pg, database, 'SELECT ' + GUARDS))
        assert answer.returncode == 0 and not answer.stderr
        assert context['guards_after'] == guard_before
        context['status'] = 'APPLIED'
        return context
    except subprocess.TimeoutExpired as error:
        def text(value):
            return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
        context.update(status='UNKNOWN_TIMEOUT', response=dict(returncode=None,
            stdout=text(error.stdout), stderr=text(error.stderr)))
        raise
    finally:
        lab.write_json(output / (label + '-policy.json'), context)


def configure(pg, client, output, database, directory, original, *, ack=False):
    assert type(ack) is bool
    config = configuration(pg, client, output, database, directory, original)
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
    checked = lab.cli(dict(pg, client=client), 'config', 'validate', '--file', config)
    lab.write_json(output / (directory.name + '-config-check.json'), dict(
        returncode=checked.returncode, stdout=checked.stdout, stderr=checked.stderr))
    assert checked.returncode == 0, checked.stderr
    return config


def capture_case(pg, client, output, rows, mode, database, config, directory, seed, setup,
        previous=None, *, positive, after_activated=None, progress_setup=None, ack=False):
    """Capture one owned Serve process; qualification belongs to its caller.

The optional fixture runs only between confirmed activation and starting the
HTTP client. Existing modes pass no fixture and retain the same observations.
    """
    assert type(ack) is bool and (not ack or (positive and progress_setup is not None))
    if ack:
        from installed_progress_replay import require_frozen_profile
        require_frozen_profile(ack=True)
    def dump(label, directory):
        response, _ = capture(client, output, label + '-dump', 'dump-state',
            '/lab/' + directory.name + '/store')
        assert response.returncode == 0 and not response.stderr
        value = json.loads(response.stdout)
        lab.write_json(output / (label + '-native.json'), value)
        return value

    def sql(label, database):
        result = rows(database)
        lab.write_json(output / (label + '-sql.json'), result)
        return result

    label = mode
    before = sql(label + '-before', database)
    before_native = dump(label + '-before', directory)
    before_inputs = inputs_at(directory / 'store')
    guards = catalog(pg, database)
    prefix, terminal = output / label, None
    fixture = dict(holder=None, cleanup_errors=[])
    snapshots, response = [], dict(returncode=None, outcome='OUTCOME_UNKNOWN')
    window = {}
    holder = None
    journey_started = False

    def snapshot(stage):
        item = dict(stage=stage, server_us=server_clock(pg, database),
            control=sql(label + '-' + stage, database), inputs=inputs_at(directory / 'store'))
        item['observed_by_us'] = server_clock(pg, database)
        snapshots.append(item)
        lab.write_json(output / (label + '-observations.json'), snapshots)
        return item

    def continue_client(stage):
        (directory / (stage + '.continue')).write_text('observed\n')

    try:
        if mode == 'installed-live-held':
            ds, = before['datasets']
            command = ['docker', 'exec', '-i', client, '/build/ingest_bootstrap_probe',
                'history-hold', database, '/run/secrets/application',
                '/lab/' + directory.name + '/store', ds['dataset_id'][2:], ds['instance_id'][2:]]
            holder = Process(command, input_pipe=True)
            fixture['holder'] = dict(command=command, ready=wait_for(
                lambda: native_ready(holder), 'installed live native holder', 5), joined=False, forced=False)
        window['before_us'] = server_clock(pg, database)
        lab.docker('exec', '-d', client, 'python3', '/lab/installed_live_client.py', 'serve',
            config, '/lab/' + label, timeout=5)
        wait_for(lambda: Path(str(prefix) + '.pid.json').exists(), 'installed successor pid', 5)
        if positive:
            first = wait_for(lambda: messages(prefix) or Path(str(prefix) + '.terminal.json').exists(),
                'installed successor publication', 15)
            if not isinstance(first, list):
                raise AssertionError('installed successor exited before publication')
            initialized = snapshot('activated')
            if after_activated is not None:
                fixture['after_activated'] = after_activated(initialized)
            target, = [r for r in initialized['control']['owner_epochs'] if r['role'] == 'ingest' and r['epoch'] == 2]
            if ack:
                producer, = [r for r in initialized['control']['producer_epochs'] if r['producer_epoch']==2]
                assert producer['outstanding_request_limit']==2
            old_domain, = [r for r in before['allocation_domains'] if r['dictionary_kind'] == 4]
            lab.write_json(directory / 'visibility.json', dict(dataset_id=old_domain['dataset_id'][2:],
                dictionary_kind=4, allocation_domain=old_domain['allocation_domain'][2:], ordinal='3'))
            wrapper = '''import subprocess,json,sys
from pathlib import Path
p=Path(sys.argv[1])
with (p/'client.stdout').open('xb') as out,(p/'client.stderr').open('xb') as err:
 try:
  r=subprocess.run(['python3','/lab/installed_live_client.py','journey',str(p)],stdout=out,stderr=err,timeout=45)
  value={'exit_code':r.returncode,'reaped':True,'outcome':'PROCESS_EXITED'}
 except subprocess.TimeoutExpired:
  value={'exit_code':None,'reaped':False,'outcome':'UNKNOWN_TIMEOUT'}
(p/'client-exit.json').write_text(json.dumps(value))
'''
            lab.docker('exec', '-d', client, 'python3', '-c', wrapper, '/lab/' + directory.name)
            journey_started = True
            for stage in ('prepared', 'submitted'):
                wait_for(lambda: (directory / (stage + '.json')).exists() or (directory / 'client-exit.json').exists(),
                    'installed successor ' + stage, 15)
                assert (directory / (stage + '.json')).exists(), (directory / 'client.stderr').read_text()
                if stage == 'submitted':
                    if progress_setup is not None:
                        from installed_progress_context import phases
                        phases(pg, database, directory, rows, snapshot, 'activated', prefix, ack=ack)
                    def durable():
                        observed = rows(database)
                        fresh = [r for r in observed['producer_outcomes'] if r['producer_epoch'] == 2]
                        return observed if len(fresh) == (4 if ack else 2) else None
                    wait_for(durable, 'two durable successor requests', 10)
                snapshot(stage)
                continue_client(stage)
            wait_for(lambda: (directory / 'commands-finished.json').exists() or (directory / 'client-exit.json').exists(),
                'installed successor logout', 15)
            assert (directory / 'commands-finished.json').exists(), (directory / 'client.stderr').read_text()
            initial_deadline = int(first[0]['initial_lease_expires_at_us'])

            def renewed():
                now = server_clock(pg, database)
                if now <= initial_deadline:
                    assert not Path(str(prefix) + '.terminal.json').exists(), 'successor stopped before renewal'
                    return None
                return now

            renewed_at = wait_for(renewed, 'live past original target deadline', 15)
            current = snapshot('renewed')
            fixture['renewal'] = dict(initial_deadline_us=initial_deadline, observed_at_us=renewed_at,
                initial_target=target, current=current)
            snapshot('before-stop')
            identity = json.loads(Path(str(prefix) + '.pid.json').read_text())
            fixture['signal'] = stop_quiet(client, directory, identity, initial_deadline)
            fixture['quiet'] = fixture['signal']['opened']
        path = Path(str(prefix) + '.terminal.json')
        wait_for(path.exists, 'actual successor Serve join', 12)
        terminal = json.loads(path.read_text())
        window['after_us'] = server_clock(pg, database)
        if holder:
            fixture['holder']['alive_during_cli'] = holder.process.poll() is None
    finally:
        if holder:
            try:
                holder.send('release')
                fixture['holder'].update(exit_code=holder.wait(5), stdout=bytes(holder.stdout).decode(),
                    stderr=bytes(holder.stderr).decode(), joined=True)
            except BaseException as error:
                fixture['holder']['forced'] = True
                fixture['cleanup_errors'].append(type(error).__name__)
            finally:
                holder.stop()
        # Preserve all known terminal output before any semantic oracle runs.
        path = Path(str(prefix) + '.terminal.json')
        if terminal is None and path.exists():
            terminal = json.loads(path.read_text())
        if terminal is None and Path(str(prefix) + '.pid.json').exists():
            try:
                signal_child(client, json.loads(Path(str(prefix) + '.pid.json').read_text()))
                wait_for(path.exists, 'failed journey Serve join', 5)
                terminal = json.loads(path.read_text())
            except BaseException as error:
                fixture['cleanup_errors'].append(type(error).__name__)
        if journey_started:
            continue_client('commands-finished')
            try:
                wait_for((directory / 'client-exit.json').exists, 'HTTP and relay client join', 5)
                fixture['client_terminal'] = json.loads((directory / 'client-exit.json').read_text())
            except BaseException as error:
                fixture['cleanup_errors'].append(type(error).__name__)
        for stream in ('stdout', 'stderr'):
            path = Path(str(prefix) + '.' + stream)
            if path.exists():
                raw = path.read_bytes()
                if len(raw) > 65536:
                    fixture['cleanup_errors'].append('OutputLimitExceeded')
                response[stream] = raw[:65536].decode(errors='replace')
        response['terminal'] = terminal
        if terminal and terminal['reaped'] and not terminal['forced'] and not fixture['cleanup_errors']:
            response.update(returncode=terminal['exit_code'], outcome='PROCESS_EXITED')
        if terminal and 'after_us' not in window:
            window['after_us'] = server_clock(pg, database)
        lab.write_json(output / (label + '-response.json'), response)
        lab.write_json(output / (label + '-clock.json'), window)
        lab.write_json(output / (label + '-fixture.json'), fixture)
        if response['outcome'] == 'PROCESS_EXITED':
            after = sql(label + '-after', database)
            after_inputs = inputs_at(directory / 'store')
            absence = observe_backend_absence(pg, database, output, label)
            old_keys = {(r['resource_id'], r['epoch']) for r in before['owner_epochs']}
            tuples = [dict(pid=r['control_backend_pid'], backend_start=r['control_backend_start'],
                database=database, control_epoch=r['epoch']) for r in after['owner_epochs']
                if r['role'] == 'control' and (r['resource_id'], r['epoch']) not in old_keys]
            excluded = exact_absence(pg, database, tuples)
            after_native = dump(label + '-after', directory)
            current_guards = catalog(pg, database)
            lab.write_json(output / (label + '-external.json'), dict(control=after, native=after_native,
                inputs=after_inputs, backend=dict(exact=excluded, absence=absence), catalog=current_guards))
        else:
            lab.write_json(output / (label + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                reason='native ownership not excluded; outer owner must join before reopening'))
    assert response['outcome'] == 'PROCESS_EXITED'
    journey = None
    if positive:
        journey = dict(result=json.loads((directory / 'client-result.json').read_text()),
            terminal=fixture['client_terminal'], stdout=(directory / 'client.stdout').read_text(),
            stderr=(directory / 'client.stderr').read_text(),
            opened=[json.loads(p.read_text()) for p in sorted(directory.glob('relay-open-*.json'))],
            closed=[json.loads(p.read_text()) for p in sorted(directory.glob('relay-closed-*.json'))])
    contents = {p.name: p.read_bytes().hex() for p in (directory / 'store/producer-inputs-v1').iterdir()}
    context = dict(mode=mode, original=before, after=after, native_before=before_native,
        native_after=after_native, inputs_before=before_inputs, inputs_after=after_inputs,
        input_contents=contents, snapshots=snapshots, response=response, journey=journey,
        fixture=fixture, server_window=window, backend=dict(exact=excluded, absence=absence),
        catalog_before=guards, catalog_after=current_guards, seed=seed, setup=setup, predecessor=previous)
    if progress_setup is not None:
        from installed_progress_context import catalog as complete_catalog
        context['progress_setup'] = progress_setup
        context['progress_catalog_after'] = complete_catalog(pg, database)
        lab.write_json(output/(label+'-progress-catalog.json'),context['progress_catalog_after'])
        context['progress_gates'] = {name:(directory / name).read_text() for name in (
            'progress-mutation-observation.json', 'progress-proof-only-observation.json', 'progress-release.json')}
        context['progress_input_files'] = {phase:json.loads((directory / ('progress-'+phase+'-input-files.json')).read_text())
            for phase in ('mutation','proof-only')}
        context['progress_input_files_after'] = preparing.census(directory / 'store/producer-inputs-v1')
    if ack:
        context['producer_ack_profile']='two-slot-three-events/1'
        context['ack_event_recipe']=(directory/'progress-ack-events.json').read_text()
    lab.write_json(output / (label + '-verification.json'), context)
    return context


def run_all(pg, client, output, rows, clone, *, progress=False, ack=False):
    assert type(progress) is bool
    assert type(ack) is bool and (not ack or progress)
    if ack:
        from installed_progress_replay import require_frozen_profile
        require_frozen_profile(ack=True)
    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    assert all(preparing.digest(basis / name) == sha for name, sha in preparing.FROZEN.items())
    source = json.loads((basis / 'result.json').read_text())
    assert source['status'] == 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE'
    assert source['source'] == preparing.SOURCE and source['owner'] == preparing.OWNER
    assert not source['cleanup_errors'] and not source['unrelated_changed']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [c for c in containers if c['Id'] == source['client']['id']]
    assert old['Config']['Labels'][source['label']] == preparing.OWNER
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    absent = observe_backend_absence(pg, preparing.DATABASE, output, 'installed-live-source')
    original = rows(preparing.DATABASE)
    assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
    baseline = json.loads((basis / 'retire-signed-retry-native.json').read_text())
    files, inputs = preparing.census(store), inputs_at(store)
    seed = dict(control=original, native=baseline, inputs=inputs, sequence=58)
    assert len(original) == 70 and len(baseline['rows']) == 49 and len(inputs) == 4
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    lab.write_json(output / 'installed-live-source.json', dict(**seed, files=files,
        frozen=preparing.FROZEN, backend_absence=absent, client_state=old['State']))
    root = Path(__file__).resolve().parents[2]
    for name in HELPERS:
        target = output / name
        if target.exists():
            assert preparing.digest(target) == preparing.digest(Path(__file__).with_name(name))
        else:
            shutil.copy2(Path(__file__).with_name(name), target)
    cases, contexts = [], {}
    on = None

    def execute(mode, database, config, directory, setup, previous=None, progress_setup=None):
        context = capture_case(pg, client, output, rows, mode, database, config,
            directory, seed, setup, previous, positive=mode in CASES[:2], progress_setup=progress_setup, ack=ack)
        if progress:
            from installed_progress_context_oracle import verify_live, controls as progress_controls
            checker=lambda value:verify_live(value,ack=ack)
            checked = checker(context)
            controls = progress_controls(context, checker)
        else:
            checked = verify(**context)
            controls = negative_controls(**context)
        contexts[mode] = context
        return dict(name=mode, status='PASS', actual_cli=True, attempts=1, oracle=checked,
            negative_controls=controls, backend_absence=context['backend']['absence'],
            terminal=context['response']['terminal'])

    try:
        for mode in (('installed-live-on',) if progress else CASES):
            if mode == 'installed-live-epoch2':
                database, config, directory, setup = on
                cases.append(execute(mode, database, config, directory, setup, contexts['installed-live-on']))
            else:
                database = clone(mode, preparing.DATABASE)
                assert rows(database) == original
                directory = output / mode
                directory.mkdir(mode=0o700)
                shutil.copytree(store, directory / 'store')
                shutil.copy2(basis / 'signed-seed/event.jsonl', directory / 'event.jsonl')
                shutil.copy2(root / 'tests/fixtures/acquisition/selector-events.json', directory / 'selector-events.json')
                observe_backend_absence(pg, database, output, mode + '-before')
                config = configure(pg, client, output, database, directory, original, ack=ack)
                upgrade(pg, client, output, database, config, mode, rows)
                setup = policy(pg, database, output, mode, rows, mode != 'installed-live-off')
                if ack:
                    assert all(next(r for r in setup['after'][table] if r['revision']==2)[
                        'document']['settings']['ingest.producer_outstanding_requests']==2
                        for table in ('config_revisions','policy_revisions'))
                progress_setup = None
                if progress:
                    from installed_progress_context import upgrade as progress_upgrade, helpers
                    reference_catalog = catalog(pg, database)
                    progress_setup = progress_upgrade(pg, client, output, database, config, mode, rows,
                        reference_catalog=reference_catalog, ack=ack)
                    helpers(output, directory, 'activated', ack=ack)
                if mode == 'installed-live-missing-original':
                    name, = [n for n in inputs if n.endswith('-0000000000000004.cbor')]
                    removed = directory / 'removed-original.cbor'
                    (directory / 'store/producer-inputs-v1' / name).rename(removed)
                    setup['removed'] = dict(name=name, identity=inputs[name],
                        bytes=removed.read_bytes().hex(), path='/lab/' + mode + '/removed-original.cbor')
                    lab.write_json(output / (mode + '-policy.json'), setup)
                cases.append(execute(mode, database, config, directory, setup, progress_setup=progress_setup))
                if mode == 'installed-live-on':
                    on = (database, config, directory, setup)
            lab.write_json(output / 'installed-live-cases.json', cases)
    finally:
        preserved = dict(control=rows(preparing.DATABASE) == original,
            files=preparing.census(store) == files, inputs=inputs_at(store) == inputs,
            artifacts=all(preparing.digest(basis / n) == h for n, h in preparing.FROZEN.items()))
        lab.write_json(output / 'installed-live-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='installed-live-initial-successor', status='PASS', cli_invocations=1 if progress else 5, cases=cases,
        source_lab=str(basis.parent), source_revision=preparing.SOURCE, source_owner=preparing.OWNER,
        source_preserved=preserved, source_artifacts=preparing.FROZEN,
        not_run=['writer replacement after activation', 'installed COMMIT loss',
            'GC/full-window proof-only/loss-recovery' if ack else 'ACK/GC',
            'Archive publication', 'finite-job completion/coverage', 'full M1'])
