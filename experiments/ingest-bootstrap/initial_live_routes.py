"""Earlier recovery routes on isolated copies of a committed empty activation.

Three fresh processes must refuse the frozen live-zero state. A fourth reads an
unactivated preparation after the same additive catalog upgrade. The outer runner
owns engines and timeout cleanup; no original native directory is ever opened.
"""
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil
import subprocess

import lab
import initial_history as history
from initial_gap import configuration
from initial_live import FUNCTIONS, catalog, exact_absence
from initial_live_routes_oracle import verify, negative_controls
from initial_succession import census, digest, physical

CASES = ('activated-history', 'activated-gap', 'activated-empty', 'prepared-history')
COMMANDS = dict(zip(CASES, ('successor-history', 'successor-gap-recover',
    'empty-retire', 'successor-history')))
ACTIVE_FROZEN = {
    'result.json': '31245bea8f3f369f1bb464552489671e63debfb8f89e76c6f926ce4661a2c9a2',
    'successor-live-zero-after.json': '454831f6e88365058af33e2cba13f9c5849f937ae1bec6e5e541d99ef07efac4',
    'successor-live-zero-after-native.json': 'b8230b58689c11f36c92e372469a068ce95428142d00e6cca51247f69ebc2b37',
    'successor-live-zero-inputs-after.json': 'c757c9978a581ccef16c249f0fd1d8d15c64d9996b4c02b3bea1830a86cddba0',
    'successor-live-zero-response.json': 'ea55a9e993734e33089d8905b2f65416b4cf178a44982be157a9058bb9cee0fe',
    'successor-live-zero-verification.json': '88a72a3a79c4121ae10798b19a3a427c7d7cb6ed9df1e5c0e3725e2306c74c02',
    'live-source-preserved.json': '4638429fcf37bb52601d2070d0939c3a08756bbb4559a872b5457f50cb96f2e1',
}
SOURCES = {
    'activated': dict(run='ingest-live-engine-2c142c6',
        database='bb_scope_bind_2c142c6_successor_live_zero',
        revision='2c142c611eaab0462b6f65df852cd75c098c6c77',
        owner='892de65cb9d38396125405dcb4046bf7',
        status='PASS_LIVE_INITIAL_ACTIVATION_FINITE', prefix='successor-live-zero',
        store='successor-live-zero/store', frozen=ACTIVE_FROZEN),
    'prepared': dict(run=history.SEED, database=history.DATABASE,
        revision=history.SOURCE, owner=history.OWNER,
        status='PASS_INITIAL_SUCCESSOR_PREPARATION', prefix='successor-normal',
        store='successor-normal/store', frozen=history.FROZEN),
}


def load_source(pg, output, rows, containers, kind):
    spec = SOURCES[kind]
    basis = output.parent.parent / spec['run'] / 'bindings'
    store = basis / spec['store']
    assert all(digest(basis / n) == h for n, h in spec['frozen'].items())
    retained = json.loads((basis / 'result.json').read_text())
    assert (retained['source'], retained['owner'], retained['status']) == (
        spec['revision'], spec['owner'], spec['status'])
    assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
    old, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert old['Config']['Labels'][retained['label']] == spec['owner']
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    absence = history.observe_backend_absence(pg, spec['database'], output, kind + '-source')
    original = rows(spec['database'])
    assert original == json.loads((basis / (spec['prefix'] + '-after.json')).read_text())
    native = json.loads((basis / (spec['prefix'] + '-after-native.json')).read_text())
    inputs, files = history.inputs_at(store), census(store)
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    assert len(original) == (72 if kind == 'activated' else 71)
    assert len(native['rows']) == 52 and len(inputs) == 4
    response = json.loads((basis / (spec['prefix'] + '-response.json')).read_text())
    assert response['returncode'] == 0 and not response['stderr']
    events = [json.loads(line) for line in response['stdout'].splitlines()]
    final, = [e for e in events if e['event'] == 'successor-state' and e['stage'] == 'final']
    assert {t: json.loads(v) for t, v in final['state']['control_tables']} == original
    assert final['rows'] == native['rows'] and dict(final['state']['originals']) == inputs
    assert final['state']['sequence'] == 62
    if kind == 'activated':
        assert native == dict(event='dump-state', rows=final['rows'], sequence=62)
        assert inputs == json.loads((basis / 'successor-live-zero-inputs-after.json').read_text())
        assert all(json.loads((basis / 'live-source-preserved.json').read_text()).values())
        prior = json.loads((basis / 'successor-live-zero-verification.json').read_text())
        assert prior['events'] == events and prior['mode'] == 'successor-live-zero'
    else:
        assert native['event'] == 'dump'
        assert inputs == json.loads((basis / 'successor-source.json').read_text())['inputs']
        assert all(json.loads((basis / 'successor-source-preserved.json').read_text()).values())
    assert all(r['state'] == 'released' for r in original['owner_epochs'] if r['role'] == 'control')
    seed = dict(control=original, native=dict(event='dump', rows=native['rows']),
        inputs=inputs, sequence=62)
    observed = dict(**seed, spec=spec, files=files, backend_absence=absence,
        previous_client=retained['client'], previous_client_state=old['State'])
    lab.write_json(output / (kind + '-source.json'), observed)
    return dict(seed=seed, store=store, basis=basis, files=files, spec=spec)


def migrate(pg, client, output, database, config, label, rows):
    try:
        response = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '32')
    except subprocess.TimeoutExpired as error:
        def partial(value):
            return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
        lab.write_json(output / (label + '.json'), dict(returncode=None,
            outcome='UNKNOWN_TIMEOUT', stdout=partial(error.stdout), stderr=partial(error.stderr),
            cleanup='outer owner-checked client stop; migration COMMIT outcome is not inferred'))
        raise
    lab.write_json(output / (label + '.json'), dict(returncode=response.returncode,
        stdout=response.stdout, stderr=response.stderr))
    current = rows(database)
    lab.write_json(output / (label + '-rows.json'), current)
    assert response.returncode == 0, response.stderr
    return current


def reapply_activated(pg, client, output, database, config, label, rows):
    before, files, guards = rows(database), physical(pg, database), catalog(pg, database)
    assert len(before) == 72 and len(before['initial_ingest_activations']) == 1
    assert {29, 30, 31, 32} <= {r['migration'] for r in before['schema_versions']}
    lab.write_json(output / (label + '-reapply-before.json'), dict(control=before,
        physical=files, catalog=guards))
    after = migrate(pg, client, output, database, config, label + '-reapply', rows)
    final_files, final_guards = physical(pg, database), catalog(pg, database)
    lab.write_json(output / (label + '-reapply-after.json'), dict(control=after,
        physical=final_files, catalog=final_guards))
    assert after == before and final_files == files and final_guards == guards


def upgrade_prepared(pg, client, output, database, config, rows):
    """Apply only31/32 to the populated schema30 clone, with exact reapply."""
    label = 'prepared-history'
    before, files = rows(database), physical(pg, database)
    versions = {r['migration'] for r in before['schema_versions']}
    assert len(before) == 71 and {29, 30} <= versions and versions.isdisjoint({31, 32})
    lab.write_json(output / (label + '-upgrade-before.json'), dict(control=before, physical=files))
    after = None
    for suffix in ('migration', 'reapply'):
        current = migrate(pg, client, output, database, config, label + '-' + suffix, rows)
        if after is not None:
            assert current == after
        after = current
    assert set(after) == set(before) | {'initial_ingest_activations'} and len(after) == 72
    assert not after['initial_ingest_activations']
    assert all(after[t] == before[t] for t in before if t != 'schema_versions')
    assert all(r in after['schema_versions'] for r in before['schema_versions'])
    added = [r for r in after['schema_versions'] if r not in before['schema_versions']]
    assert len(added) == 2 and {r['migration'] for r in added} == {31, 32}
    assert len({r['xmin'] for r in added}) == 1
    assert all(r['xmin'] != added[0]['xmin'] for entries in before.values() for r in entries)
    root = Path(__file__).resolve().parents[2]
    for number, name in ((31, '0031_initial_empty_retirement.sql'), (32, '0032_initial_activation.sql')):
        row, = [r for r in added if r['migration'] == number]
        assert row['name'] == name and row['checksum'] == '\\x' + digest(root / 'migrations/postgres' / name)
        assert row['reader_min'] == row['reader_max'] == row['writer_floor'] == 1
        assert row['engine'] == 'postgres-control' and row['applied_by'] == 'bigbrotr_migrator'
    final_files = physical(pg, database)
    assert all(final_files[t] == value for t, value in files.items())
    grants = ('GRANT SELECT,INSERT,UPDATE ON control.initial_ingest_successions TO bigbrotr_control;\n'
        'GRANT SELECT,INSERT ON control.initial_ingest_activations TO bigbrotr_control;\n'
        'GRANT EXECUTE ON FUNCTION ' + ','.join('control.' + f for f in FUNCTIONS) + ' TO bigbrotr_control;\n')
    (output / (label + '-grants.sql')).write_text(grants)
    lab.sql(pg, database, grants)
    assert rows(database) == after and physical(pg, database) == final_files
    lab.write_json(output / (label + '-upgrade.json'), dict(before=before, after=after,
        physical_before=files, physical_after=final_files, exact_reapply=True))


def activation_diagnostic(pg, backend, window, mode):
    """Read only the named error/context for this exact claimed backend window."""
    since, until = [datetime.fromtimestamp(window[k] / 1_000_000, timezone.utc).isoformat()
        for k in ('before_us', 'after_us')]
    response = lab.docker('logs', '--timestamps', '--since', since, '--until', until,
        pg['postgres'], check=False, timeout=5)
    output = response.stdout + response.stderr
    assert len(output.encode()) <= 256 * 1024, 'server error observation exceeds bound'
    message = 'activated initial writer is not an unactivated preparation'
    function = {'activated-history': 'require_prepared_initial_successor(bytea)',
        'activated-empty': 'lock_initial_empty_retirement(bytea,bytea)'}[mode]
    pattern = rf'\[{backend["pid"]}\] ERROR:  (?:(?P<sqlstate>[0-9A-Z]{{5}}): )?' + re.escape(message) + '$'
    errors = [line for line in output.splitlines() if re.search(pattern, line)]
    contexts = [line for line in output.splitlines() if f'[{backend["pid"]}] CONTEXT:  ' in line
        and re.search(r'function (?:control\.)?' + re.escape(function), line)]
    observed = re.search(pattern, errors[0]).group('sqlstate') if len(errors) == 1 else None
    # Default PostgreSQL logs omit SQLSTATE; null is the actual observation.
    return dict(backend=backend, since=since, until=until, returncode=response.returncode,
        observed_sqlstate=observed, errors=errors, contexts=contexts)


def run(pg, client, output, rows, clone):
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    sources, cases = {}, []

    def dump(label, directory):
        response, _ = history.capture(client, output, label + '-dump', 'dump-state',
            '/lab/' + directory.name + '/store')
        assert response.returncode == 0 and not response.stderr, response.stderr
        value = json.loads(response.stdout)
        lab.write_json(output / (label + '-native.json'), value)
        assert value['event'] == 'dump-state' and type(value['sequence']) is int
        return value

    try:
        for kind in SOURCES:
            sources[kind] = load_source(pg, output, rows, containers, kind)
        for mode in CASES:
            source = sources['prepared' if mode == 'prepared-history' else 'activated']
            seed = source['seed']
            database = clone(mode, source['spec']['database'])
            assert rows(database) == seed['control']
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(source['store'], directory / 'store')
            history.observe_backend_absence(pg, database, output, mode + '-before')
            native = dump(mode + '-before', directory)
            assert native == dict(event='dump-state', rows=seed['native']['rows'], sequence=62)
            assert history.inputs_at(directory / 'store') == seed['inputs']
            config = configuration(pg, client, output, database, directory, seed['control'])
            if mode == 'prepared-history':
                upgrade_prepared(pg, client, output, database, config, rows)
            else:
                reapply_activated(pg, client, output, database, config, mode, rows)
            before, guards = rows(database), catalog(pg, database)
            lab.write_json(output / (mode + '-before.json'), before)
            lab.write_json(output / (mode + '-catalog-before.json'), guards)
            lab.write_json(output / (mode + '-inputs-before.json'), seed['inputs'])
            window = dict(before_us=history.server_clock(pg, database))
            lab.write_json(output / (mode + '-clock.json'), window)
            try:
                response, seconds = history.capture(client, output, mode, COMMANDS[mode], database,
                    '/run/secrets/application', config)
            finally:
                receipt = output / (mode + '-response.json')
                terminal = receipt.exists() and json.loads(receipt.read_text())['returncode'] is not None
                if terminal:
                    window['after_us'] = history.server_clock(pg, database)
                    lab.write_json(output / (mode + '-clock.json'), window)
                    after = rows(database)
                    lab.write_json(output / (mode + '-after.json'), after)
                    after_inputs = history.inputs_at(directory / 'store')
                    lab.write_json(output / (mode + '-inputs-after.json'), after_inputs)
                    absence = history.observe_backend_absence(pg, database, output, mode)
                    # Known process exit and global absence permit safe native
                    # inspection before stricter row/event assertions can fail.
                    after_native = dump(mode + '-after', directory)
                    exact = exact_absence(pg, database, output, mode, after, before)
                else:
                    lab.write_json(output / (mode + '-external-unavailable.json'), dict(
                        status='NOT_RUN_UNKNOWN_PROCESS_OUTCOME',
                        reason='Native may still be owned; outer owner-checked cleanup must complete first'))
            diagnostic = None
            if mode in ('activated-history', 'activated-empty'):
                diagnostic = activation_diagnostic(pg, exact['expected'], window, mode)
            lab.write_json(output / (mode + '-diagnostic.json'), diagnostic)
            final_guards = catalog(pg, database)
            lab.write_json(output / (mode + '-catalog-after.json'), final_guards)
            assert response.returncode == 0 and not response.stderr, (mode, response.stderr)
            events = [json.loads(line) for line in response.stdout.splitlines()]
            context = dict(events=events, original=before, native=seed['native'], inputs=seed['inputs'],
                mode=mode, seed=seed, server_window=window,
                backend=dict(exact=exact, absence=absence), diagnostic=diagnostic)
            lab.write_json(output / (mode + '-verification.json'), context)
            evidence = verify(**context)
            controls = negative_controls(**context)
            stage = {'activated-gap': 'gap-final', 'activated-empty': 'empty-final'}.get(mode, 'history-final')
            final, = [e for e in events if e['event'] == 'successor-state' and e['stage'] == stage]
            assert after == {t: json.loads(v) for t, v in final['state']['control_tables']}
            assert after_native == dict(event='dump-state', rows=final['rows'], sequence=final['state']['sequence']) == native
            assert after_inputs == dict(final['state']['originals']) == seed['inputs']
            assert final_guards == guards
            cases.append(dict(name=mode, status='PASS', seconds=seconds, oracle=evidence,
                negative_controls=controls, backend=context['backend'], diagnostic=diagnostic,
                server_window=window, external_final_matches=True, catalog_unchanged=True))
            lab.write_json(output / 'live-routes-cases.json', cases)
    finally:
        preserved = {}
        for kind, source in sources.items():
            spec, seed = source['spec'], source['seed']
            current = dict(control=rows(spec['database']), files=census(source['store']),
                inputs=history.inputs_at(source['store']),
                artifacts={n: digest(source['basis'] / n) for n in spec['frozen']})
            lab.write_json(output / (kind + '-source-final.json'), current)
            preserved[kind] = dict(control=current['control'] == seed['control'],
                files=current['files'] == source['files'], inputs=current['inputs'] == seed['inputs'],
                artifacts=current['artifacts'] == spec['frozen'])
        lab.write_json(output / 'live-routes-source-preserved.json', preserved)
        assert all(all(v.values()) for v in preserved.values()), preserved
    return dict(name='live-initial-routes', status='PASS', cases=cases, sources=SOURCES,
        source_preserved=preserved,
        scope='activated-zero history/empty receipt veto, earlier gap epoch preflight, unactivated positive history',
        not_run=['history/gap runtime poison-reuse observation', 'writer replacement',
            'first-request manifest mismatch', 'refused/expired/lost renewal', 'installed successor', 'full M1'])
