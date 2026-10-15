"""Retire an activated empty source; refuse a genuine unbound rejected Event.

Both seeds already have native CLOSING and committed source2 closure. No probe,
native mutation, acquisition abandonment or synthesized workflow is introduced.
"""
import copy
import json
from pathlib import Path
import shutil

import lab
import activated_acquisition as acquisition
import activated_recovery as recovery
import activated_retirement as retirement
from activated_recovery_zero import invoke
from activated_retirement_zero_oracle import CASES, verify, negative_controls
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence
from initial_live import exact_absence
from initial_succession import census, digest

BASE = dict(run='ingest-activated-source-engine-c7b2b3c',
    source='c7b2b3cccbbc356609c2bb9cdffd59ec681b13ba', owner='893c3ddbddd3c7ca698c6bbc2cc7b8e1',
    status='PASS_INSTALLED_ACTIVATED_SOURCE_CLOSURE_FIRST_WAVE',
    result='01ebda022458719700afa4d0da2b1867551be4e9db2c7d290825a76c1a4d9ca9')
SOURCES = {
    'zero': dict(BASE, prefix='installed-source-zero',
        database='bb_scope_bind_c7b2b3c_installed_source_zero',
        final='installed-source-zero-legacy-closed',
        sql='3fb138e9ff960933b9ce33b51e0789d5727ce5d312f67d2e4a5524bb4d9585d9',
        native='553302fb342847140d388677c0bb1504af53e5bf25ec30150d65746b17f1ced7',
        verification='3b32a2d8e6788ef18db9bfd5602f05c8b013fc4853ade505b617f5dbe0b24ff5'),
    'rejected': dict(BASE, prefix='installed-source-rejected',
        database='bb_scope_bind_c7b2b3c_installed_source_rejected',
        final='installed-source-rejected-retry',
        sql='05c123810e00dc8b6af860a7e79856ac29c334ec5a02496af8209f37ed37a52a',
        native='ab3dc3498a8c4c056c75a621fc0b2bb0db253ba4132212919d1fdecc7c17aae2',
        verification='3f1cf5108ad1186836be99e530f6a6d01434ab9afe558f29a0bb21900e57908e'),
}


def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_acquisition_client.py')
    target = output / helper.name
    if not target.exists():
        shutil.copy2(helper, target)
    assert digest(target) == digest(helper)
    origins, cases = [], []

    def state(label, database, directory):
        value = dict(control=rows(database))
        lab.write_json(output / (label + '-sql.json'), value['control'])
        value.update(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store'))
        lab.write_json(output / (label + '-inputs.json'), dict(inputs=value['inputs'], contents=value['contents']))
        try:
            answer, _ = capture(client, output, label + '-dump', 'dump-state', '/lab/' + directory.name + '/store')
            assert answer.returncode == 0 and not answer.stderr
            value['native'] = json.loads(answer.stdout)
            lab.write_json(output / (label + '-native.json'), value['native'])
        except BaseException as error:
            lab.write_json(output / (label + '-state-incomplete.json'), dict(value,
                native='UNAVAILABLE', status='INCOMPLETE', error=type(error).__name__))
            raise
        lab.write_json(output / (label + '-state.json'), value)
        return value

    def uncertain(label, database, directory, reason):
        value = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', reason=reason,
            detail='Native custody unproved; available SQL and inputs are provisional')
        lab.write_json(output / (label + '-external-unavailable.json'), value)
        for name, read in [('sql', lambda: rows(database)), ('inputs', lambda:
                dict(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
            try:
                lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
            except BaseException as error:
                value[name + '_capture_error'] = type(error).__name__
        lab.write_json(output / (label + '-external-unavailable.json'), value)

    def exclude(label, database, directory):
        try:
            return observe_backend_absence(pg, database, output, label)
        except BaseException as error:
            uncertain(label, database, directory, 'Backend absence unavailable: ' + type(error).__name__)
            raise

    def execute(mode, database, config, directory, seed, setup, predecessor=None):
        exclude(mode + '-before', database, directory)
        before = state(mode + '-before', database, directory)
        local = dict(copy.deepcopy(setup), catalog_before=retirement.catalog(pg, database))
        response, clock = invoke(pg, client, output, database, config, mode, helper,
            action='--retire-activated-initial')
        if response['outcome'] != 'PROCESS_EXITED':
            uncertain(mode, database, directory, 'CLI custody unknown; outer owner must join')
            raise AssertionError('activated retirement CLI custody unavailable')
        absent = exclude(mode, database, directory)
        after = state(mode + '-after', database, directory)
        local['catalog_after'] = retirement.catalog(pg, database)
        lab.write_json(output / (mode + '-external.json'), dict(**after, catalog=local['catalog_after'], absence=absent))
        exact = exact_absence(pg, database, output, mode, after['control'], before['control'])
        context = dict(before=before['control'], after=after['control'], native_before=before['native'],
            native_after=after['native'], inputs_before=before['inputs'], inputs_after=after['inputs'],
            input_contents=after['contents'], response=response, mode=mode, seed=seed, setup=local,
            server_window=clock, backend=dict(exact=exact, absence=absent), predecessor=predecessor)
        lab.write_json(output / (mode + '-verification.json'), context)
        checked, controls = verify(**context), negative_controls(**context)
        return dict(name=mode, status='PASS', oracle=checked, negative_controls=controls,
            verification_sha256=digest(output / (mode + '-verification.json'))), context

    try:
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        for mode, name in zip(CASES, ('zero', 'rejected'), strict=True):
            spec = SOURCES[name]
            basis = output.parent.parent / spec['run'] / 'bindings'
            frozen = {'result.json': spec['result'], spec['final'] + '-after-sql.json': spec['sql'],
                spec['final'] + '-after-native.json': spec['native'],
                spec['final'] + '-verification.json': spec['verification']}
            assert all(digest(basis / n) == sha for n, sha in frozen.items())
            retained = json.loads((basis / 'result.json').read_text())
            assert all(retained[k] == spec[k] for k in ('source', 'owner', 'status'))
            assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
            old, = [c for c in containers if c['Id'] == retained['client']['id']]
            assert old['Config']['Labels'][retained['label']] == spec['owner']
            assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
            assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
            absent = observe_backend_absence(pg, spec['database'], output, mode + '-original')
            original = rows(spec['database'])
            assert original == json.loads((basis / (spec['final'] + '-after-sql.json')).read_text())
            store = basis / spec['prefix'] / 'store'
            seed = dict(name=name, verification=json.loads((basis / (spec['final'] + '-verification.json')).read_text()),
                control=original, native=json.loads((basis / (spec['final'] + '-after-native.json')).read_text()),
                inputs=inputs_at(store), contents=recovery.contents_at(store), catalog=retirement.catalog(pg, spec['database']))
            origin = dict(seed=seed, files=census(store), frozen=frozen, backend_absence=absent,
                client_id=old['Id'], client_state=old['State'], label=retained['label'])
            origins.append((origin, basis, store, spec))
            lab.write_json(output / (mode + '-original.json'), origin)
            database = clone(mode, spec['database'])
            assert rows(database) == original
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            exclude(mode + '-seed', database, directory)
            assert state(mode + '-seed', database, directory) == {k: seed[k] for k in ('control', 'native', 'inputs', 'contents')}
            config = configuration(pg, client, output, database, directory, original)
            path = output / Path(config).name
            setup = dict(configuration=dict(path=config, text=path.read_text(), sha256=digest(path)),
                acquisition_migration_sha256=digest(Path(__file__).resolve().parents[2] /
                    'migrations/postgres/0036_activated_acquisition_abandonment.sql'),
                acquisition_upgrade=acquisition.upgrade(pg, client, output, database, config, mode + '-36', rows),
                migration_sha256=digest(Path(__file__).resolve().parents[2] /
                    'migrations/postgres/0037_activated_initial_retirement.sql'),
                migration=retirement.upgrade(pg, client, output, database, config, mode + '-37', rows))
            lab.write_json(output / (mode + '-setup.json'), setup)
            first, prior = execute(mode, database, config, directory, seed, setup)
            attempts = [first]
            if name == 'zero':
                retry, _ = execute(mode + '-retry', database, config, directory, seed, setup, prior)
                attempts.append(retry)
            cases.append(dict(name=mode, status='PASS', attempts=attempts))
            lab.write_json(output / 'activated-retirement-zero-cases.json', cases)
    finally:
        preserved = {}
        for origin, basis, store, spec in origins:
            seed = origin['seed']
            old, = json.loads(lab.docker('inspect', origin['client_id']).stdout)
            preserved[seed['name']] = dict(control=rows(spec['database']) == seed['control'],
                files=census(store) == origin['files'], inputs=inputs_at(store) == seed['inputs'],
                contents=recovery.contents_at(store) == seed['contents'], catalog=retirement.catalog(pg, spec['database']) == seed['catalog'],
                client=old['State'] == origin['client_state'] and old['Config']['Labels'][origin['label']] == spec['owner'],
                artifacts=all(digest(basis / n) == sha for n, sha in origin['frozen'].items()))
        lab.write_json(output / 'activated-retirement-zero-sources-preserved.json', preserved)
        assert all(all(v.values()) for v in preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES) and sum(len(c['attempts']) for c in cases) == 3
    return dict(name='installed-activated-retirement-zero', status='PASS', cli_invocations=3,
        finite_seed_invocations=0, cases=cases, source_preserved=preserved, helper_sha256=digest(helper),
        scope='Activated zero completion and fresh retry; native-classified unbound Rejected Event refusal',
        not_run=['final Event-mask dispatch for the refused request (existing-copy check refuses first)',
            'unresolved workflow/import and missing/corrupt detail gates', 'blocked cancellation/UTC expiry',
            'repeated lifetimes, import closure, ACK/GC, epoch3, Archive restart and full M1'])
