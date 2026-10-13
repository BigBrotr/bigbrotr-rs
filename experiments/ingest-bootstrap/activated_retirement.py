"""Retire two genuinely abandoned activated targets, then retry in fresh processes.

The source, gaps, jobs, sessions and all native/input bytes are immutable.
Only exact target retirement and fresh Control lifetimes may change SQL.
"""
import copy
import json
from pathlib import Path
import shutil
import subprocess

import lab
import activated_recovery as recovery
import activated_acquisition as acquisition
from activated_recovery_zero import invoke
from activated_retirement_oracle import CASES, CATALOG_FUNCTIONS, GRANTS, verify, negative_controls
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence
from initial_live import exact_absence
from initial_succession import census, digest, physical

BASE = dict(run='ingest-activated-acquisition-engine-1d64cd7',
    source='1d64cd7c5c6bc1fb72ac7eb0435d0e7164831a0a', owner='2c7d9912914b8958ea6ffe3f9f47723d',
    status='PASS_INSTALLED_ACTIVATED_ACQUISITION_ABANDONMENT_FIRST_WAVE',
    result='03545615b8c2e84fc1a545ccf47b33aeed8fb02e4e76e44d3f1ffd246315054c')
SOURCES = {
    'off': dict(BASE, prefix='installed-activated-acquisition-off',
        database='bb_scope_bind_1d64cd7_installed_activated_acquisition_off',
        sql_file='installed-activated-acquisition-off-retry-after-sql.json',
        sql='8e74f67a8dfff4b634bcdf3481733d3a72ec9007c13faaaba1c14b05fb86126b',
        native_file='installed-activated-acquisition-off-retry-after-native.json',
        native='cd49f8bfc44c5c08ae307f845f1aaf1da7f66079bf14f8ee54f9c4c1a3819238',
        verification_file='installed-activated-acquisition-off-retry-verification.json',
        verification='87209a110e38dbeac5588e069c5fe3f2d17ce2cc7c7f39093226c900b21b52eb'),
    'durable': dict(BASE, prefix='installed-activated-acquisition-durable',
        database='bb_scope_bind_1d64cd7_installed_activated_acquisition_durable',
        sql_file='installed-activated-acquisition-durable-retry-after-sql.json',
        sql='9a115edc8f23c35dcaf8213be6f81b42bd348333d5a6868e84b50bc8fbb4991b',
        native_file='installed-activated-acquisition-durable-retry-after-native.json',
        native='be37b4a515d7727bfb14f9ccfc6d14dc69483a0f02ac96d69b9506e117e4370a',
        verification_file='installed-activated-acquisition-durable-retry-verification.json',
        verification='73860b58190ddfd3a9dce9627f3f33ac941daa7b60dc46ae9531f582efdcd1cb'),
}


def catalog(pg, database):
    observed = acquisition.catalog(pg, database)
    names = ','.join("'" + name + "'" for name in CATALOG_FUNCTIONS)
    observed['retirement'] = json.loads(lab.sql(pg, database, "SET search_path=pg_catalog; "
        "SELECT json_build_object('functions',(SELECT coalesce(json_agg(json_build_object("
        "'name',p.oid::regprocedure::text,'definition',pg_get_functiondef(p.oid),"
        "'execute',has_function_privilege('bigbrotr_control',p.oid,'EXECUTE')) "
        "ORDER BY p.oid::regprocedure::text),'[]') FROM pg_proc p WHERE p.oid::regprocedure::text IN (" + names + ")),"
        "'privileges',(SELECT json_object_agg(name,grants) FROM (SELECT name,json_build_object("
        "'select',has_table_privilege('bigbrotr_control','control.'||name,'SELECT'),"
        "'update',has_table_privilege('bigbrotr_control','control.'||name,'UPDATE'),"
        "'delete',has_table_privilege('bigbrotr_control','control.'||name,'DELETE'),"
        "'truncate',has_table_privilege('bigbrotr_control','control.'||name,'TRUNCATE')) grants "
        "FROM unnest(ARRAY['producer_epochs','source_streams','allocation_domains','owner_epochs','role_instances']) name) t));"))
    return observed


def upgrade(pg, client, output, database, config, label, rows):
    value = dict(before=rows(database), physical_before=physical(pg, database),
        catalog_before=catalog(pg, database), responses=[], grant_statements=[])
    try:
        for name in ('after', 'reapplied'):
            answer = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
                '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '37')
            value['responses'].append(dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr))
            value[name] = rows(database)
            lab.write_json(output / (label + '-upgrade.json'), value)
            assert answer.returncode == 0 and not answer.stderr
        value['physical_after'] = physical(pg, database)
        value['catalog_ungranted'] = catalog(pg, database)
        value['grant_responses'] = []
        for statement in GRANTS:
            value['grant_statements'].append(statement)
            value['grant_responses'].append(lab.sql(pg, database, statement))
            lab.write_json(output / (label + '-upgrade.json'), value)
        value['catalog_after'] = catalog(pg, database)
        return value
    except subprocess.TimeoutExpired as error:
        def text(raw):
            return raw.decode(errors='replace') if isinstance(raw, bytes) else raw or ''
        value.update(outcome='UNKNOWN_TIMEOUT', incomplete_response=dict(returncode=None,
            stdout=text(error.stdout), stderr=text(error.stderr)))
        raise
    finally:
        lab.write_json(output / (label + '-upgrade.json'), value)


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
        except Exception as error:
            lab.write_json(output / (label + '-state-incomplete.json'), dict(**value,
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
            except Exception as problem:
                value[name + '_capture_error'] = type(problem).__name__
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
        local = dict(copy.deepcopy(setup), catalog_before=catalog(pg, database))
        response, clock = invoke(pg, client, output, database, config, mode, helper,
            action='--retire-activated-initial')
        if response['outcome'] != 'PROCESS_EXITED':
            uncertain(mode, database, directory, 'CLI custody unknown; outer owner must join')
            raise AssertionError('activated retirement CLI custody unavailable')
        absent = exclude(mode, database, directory)
        after = state(mode + '-after', database, directory)
        local['catalog_after'] = catalog(pg, database)
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
        for mode, name in zip(CASES, ('off', 'durable'), strict=True):
            spec = SOURCES[name]
            basis = output.parent.parent / spec['run'] / 'bindings'
            frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
                spec['native_file']: spec['native'], spec['verification_file']: spec['verification']}
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
            assert original == json.loads((basis / spec['sql_file']).read_text())
            store = basis / spec['prefix'] / 'store'
            seed = dict(name=name, verification=json.loads((basis / spec['verification_file']).read_text()),
                control=original, native=json.loads((basis / spec['native_file']).read_text()),
                inputs=inputs_at(store), contents=recovery.contents_at(store), catalog=catalog(pg, spec['database']))
            origin = dict(seed=seed, files=census(store), frozen=frozen, backend_absence=absent,
                client_id=old['Id'], client_state=old['State'], label=retained['label'])
            origins.append((origin, basis, store, spec))
            lab.write_json(output / (mode + '-original.json'), origin)
            database = clone(mode, spec['database'])
            assert rows(database) == original
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert state(mode + '-seed', database, directory) == {k:seed[k] for k in ('control','native','inputs','contents')}
            config = configuration(pg, client, output, database, directory, original)
            setup = dict(migration_sha256=digest(Path(__file__).resolve().parents[2] /
                'migrations/postgres/0037_activated_initial_retirement.sql'),
                migration=upgrade(pg, client, output, database, config, mode, rows))
            first, prior = execute(mode, database, config, directory, seed, setup)
            retry, _ = execute(mode + '-retry', database, config, directory, seed, setup, prior)
            cases.append(dict(name=mode, status='PASS', attempts=[first, retry]))
            lab.write_json(output / 'activated-retirement-cases.json', cases)
    finally:
        preserved = {}
        for origin, basis, store, spec in origins:
            seed = origin['seed']
            old, = json.loads(lab.docker('inspect', origin['client_id']).stdout)
            preserved[seed['name']] = dict(control=rows(spec['database']) == seed['control'],
                files=census(store) == origin['files'], inputs=inputs_at(store) == seed['inputs'],
                contents=recovery.contents_at(store) == seed['contents'], catalog=catalog(pg, spec['database']) == seed['catalog'],
                client=old['State'] == origin['client_state'] and old['Config']['Labels'][origin['label']] == spec['owner'],
                artifacts=all(digest(basis / n) == sha for n, sha in origin['frozen'].items()))
        lab.write_json(output / 'activated-retirement-sources-preserved.json', preserved)
        assert all(all(v.values()) for v in preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES) and sum(len(c['attempts']) for c in cases) == 4
    return dict(name='installed-activated-initial-retirement', status='PASS', cli_invocations=4,
        finite_seed_invocations=0, cases=cases, source_preserved=preserved, helper_sha256=digest(helper),
        scope='Two previously abandoned sources: exact twelve-row target retirement and fresh-process retry',
        not_run=['zero-request activated target', 'unbound Event and unresolved workflow refusals',
            'final retirement COMMIT loss', 'raw SQL lifecycle/role guards', 'blocked cancellation/UTC expiry',
            'repeated lifetimes, import closure, ACK/GC, epoch3, Archive restart and full M1'])
