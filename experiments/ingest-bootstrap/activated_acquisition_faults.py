"""Final abandonment reply loss on two copies of the qualified closed off seed.

Each failed installed process is followed by a fresh direct process. Neither
native data nor an outcome copy is altered to manufacture this transaction gap.
"""
import copy
import json
from pathlib import Path
import shutil

import lab
import activated_acquisition as acquisition
import activated_recovery as recovery
from activated_acquisition_faults_oracle import CASES, verify, negative_controls
from activated_recovery_faults import invoke
from activated_source_prefix import CLOSED
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence
from initial_live import exact_absence
from initial_succession import census, digest


def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_acquisition_client.py')
    proxy_source = Path(__file__).resolve().parents[1] / 'control-migrations/fault_proxy.py'
    for path in (helper, proxy_source):
        target = output / path.name
        if not target.exists():
            shutil.copy2(path, target)
        assert digest(target) == digest(path)
    proxy_source = output / proxy_source.name
    spec = CLOSED
    basis = output.parent.parent / spec['run'] / 'bindings'
    store = basis / spec['prefix'] / 'store'
    frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
        spec['native_file']: spec['native'], spec['verification_file']: spec['verification']}
    original_source, cases = None, []

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
        except Exception as error:
            lab.write_json(output / (label + '-state-incomplete.json'), dict(**value,
                native='UNAVAILABLE', status='INCOMPLETE', error=type(error).__name__))
            raise
        lab.write_json(output / (label + '-state.json'), value)
        return value

    def exclude(label, database, directory):
        try:
            return observe_backend_absence(pg, database, output, label)
        except BaseException as error:
            value = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', error=type(error).__name__,
                detail='Backend absence unproved; available SQL and inputs are provisional')
            lab.write_json(output / (label + '-external-unavailable.json'), value)
            for name, read in [('sql', lambda: rows(database)), ('inputs', lambda:
                    dict(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
                except Exception as problem:
                    value[name + '_capture_error'] = type(problem).__name__
            lab.write_json(output / (label + '-external-unavailable.json'), value)
            raise

    def execute(mode, database, config, directory, seed, setup, fault, predecessor=None):
        exclude(mode + '-before', database, directory)
        before = state(mode + '-before', database, directory)
        local = dict(copy.deepcopy(setup), catalog_before=acquisition.catalog(pg, database))
        response, clock = invoke(pg, client, output, database, config, mode, helper, proxy_source,
            fault if predecessor is None else None)
        if response['outcome'] != 'PROCESS_EXITED':
            lab.write_json(output / (mode + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', reason='CLI custody unknown; outer owner must join'))
            raise AssertionError('activated abandonment CLI custody unavailable')
        absent = exclude(mode, database, directory)
        after = state(mode + '-after', database, directory)
        local['catalog_after'] = acquisition.catalog(pg, database)
        lab.write_json(output / (mode + '-external.json'), dict(**after, catalog=local['catalog_after'], absence=absent))
        exact = exact_absence(pg, database, output, mode, after['control'], before['control'])
        context = dict(before=before['control'], after=after['control'], native_before=before['native'],
            native_after=after['native'], inputs_before=before['inputs'], inputs_after=after['inputs'],
            input_contents=after['contents'], response=response, mode=mode, seed=seed, setup=local,
            server_window=clock, backend=dict(exact=exact, absence=absent),
            fault=copy.deepcopy(fault), predecessor=predecessor)
        lab.write_json(output / (mode + '-verification.json'), context)
        evidence, controls = verify(**context), negative_controls(**context)
        return dict(name=mode, status='PASS', oracle=evidence, negative_controls=controls,
            verification_sha256=digest(output / (mode + '-verification.json'))), context

    try:
        assert all(digest(basis / name) == sha for name, sha in frozen.items())
        retained = json.loads((basis / 'result.json').read_text())
        assert all(retained[k] == spec[k] for k in ('source', 'owner', 'status'))
        assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        old, = [c for c in containers if c['Id'] == retained['client']['id']]
        assert old['Config']['Labels'][retained['label']] == spec['owner']
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
        absent = observe_backend_absence(pg, spec['database'], output, 'acquisition-fault-original')
        original = rows(spec['database'])
        assert original == json.loads((basis / spec['sql_file']).read_text())
        seed = dict(name='off', verification=json.loads((basis / spec['verification_file']).read_text()),
            control=original, native=json.loads((basis / spec['native_file']).read_text()),
            inputs=inputs_at(store), contents=recovery.contents_at(store), catalog=acquisition.catalog(pg, spec['database']))
        original_source = dict(seed=seed, files=census(store), frozen=frozen, backend_absence=absent,
            client_id=old['Id'], client_state=old['State'], label=retained['label'])
        lab.write_json(output / 'activated-acquisition-fault-original.json', original_source)
        for mode in CASES:
            database = clone(mode, spec['database'])
            assert rows(database) == seed['control']
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert state(mode + '-seed', database, directory) == {k:seed[k] for k in ('control','native','inputs','contents')}
            config = configuration(pg, client, output, database, directory, original)
            setup = dict(migration_sha256=digest(Path(__file__).resolve().parents[2] /
                'migrations/postgres/0036_activated_acquisition_abandonment.sql'),
                migration=acquisition.upgrade(pg, client, output, database, config, mode, rows))
            fault = dict(point='abandonment', mode='before-forward' if mode.endswith('-before') else 'after-complete',
                commit_number=10, config=None, wire=dict(status='NOT_RUN'))
            failed, prior = execute(mode, database, config, directory, seed, setup, fault)
            recovered, _ = execute(mode + '-recover', database, config, directory, seed, setup, fault, prior)
            cases.append(dict(name=mode, status='PASS', attempts=[failed, recovered]))
            lab.write_json(output / 'activated-acquisition-fault-cases.json', cases)
    finally:
        if original_source is not None:
            old, = json.loads(lab.docker('inspect', original_source['client_id']).stdout)
            preserved = dict(control=rows(spec['database']) == original_source['seed']['control'],
                files=census(store) == original_source['files'], inputs=inputs_at(store) == original_source['seed']['inputs'],
                contents=recovery.contents_at(store) == original_source['seed']['contents'],
                catalog=acquisition.catalog(pg, spec['database']) == original_source['seed']['catalog'],
                client=old['State'] == original_source['client_state'] and old['Config']['Labels'][original_source['label']] == spec['owner'],
                artifacts=all(digest(basis / name) == sha for name, sha in frozen.items()))
            lab.write_json(output / 'activated-acquisition-fault-source-preserved.json', preserved)
            assert all(preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES) and sum(len(c['attempts']) for c in cases) == 4
    return dict(name='installed-activated-acquisition-faults', status='PASS', cli_invocations=4,
        cases=cases, source_preserved=preserved, helper_sha256=digest(helper), proxy_sha256=digest(proxy_source),
        scope='installed final abandonment COMMIT10 reply loss and fresh direct recovery with exact retained attribution',
        not_run=['driver-result or same-session poison telemetry', 'power loss', 'producer retirement', 'Archive restart and full M1'])
