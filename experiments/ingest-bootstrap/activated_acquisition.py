"""Two copied, positively closed activated sources; first abandonment and retry.

No job is fabricated or adopted. Every original laboratory remains immutable;
unknown process/backend custody prevents native inspection of its owned copy.
"""
import copy
import json
from pathlib import Path
import shutil
import subprocess

import lab
import activated_recovery as recovery
import activated_source_closure as source
from activated_recovery_zero import invoke
from activated_source_prefix import CLOSED
from activated_acquisition_oracle import CASES, verify, negative_controls
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence
from initial_live import exact_absence
from initial_succession import census, digest, physical

DURABLE = dict(run='ingest-activated-durable-engine-0e4917f', prefix='installed-source-already-durable',
    database='bb_scope_bind_0e4917f_installed_source_already_durable',
    source='0e4917fe01a43116f57d941a81ebbb7a108e0b19', owner='1cb10d96f103a42e884fb3dac5147c38',
    status='PASS_INSTALLED_ACTIVATED_SOURCE_ALREADY_DURABLE',
    result='f49a5eeba3148b5ab3abe5aacd54a54eee69be84feb5cea3e05a2bb9dfc0c233',
    sql_file='installed-source-already-durable-retry-after-sql.json',
    sql='d0acf04de6214f7ecc533b86488e43514d5372a3a6402e63b520dd637aa0bea0',
    native_file='installed-source-already-durable-retry-after-native.json',
    native='be37b4a515d7727bfb14f9ccfc6d14dc69483a0f02ac96d69b9506e117e4370a',
    verification_file='installed-source-already-durable-retry-verification.json',
    verification='879eb27d3116030b771ac5bd9685a00fbfd3b42584301aca37a93b5676399c96')


def catalog(pg, database):
    observed = source.catalog(pg, database)
    observed['acquisition'] = json.loads(lab.sql(pg, database, """
SET search_path=pg_catalog;
SELECT json_build_object(
 'functions',(SELECT json_agg(json_build_object('name',p.oid::regprocedure::text,
  'definition',pg_get_functiondef(p.oid),'execute',has_function_privilege('bigbrotr_control',p.oid,'EXECUTE'))
  ORDER BY p.oid::regprocedure::text) FROM pg_proc p WHERE p.oid IN (
  'control.guard_acquisition_gap()'::regprocedure,'control.guard_acquisition_claim_owner()'::regprocedure,
  'control.require_acquisition_gap_complete()'::regprocedure)),
 'checks',(SELECT json_agg(json_build_object('name',conname,'definition',pg_get_constraintdef(oid)) ORDER BY conname)
  FROM pg_constraint WHERE conrelid='control.acquisition_gaps'::regclass AND contype='c'),
 'indexes',(SELECT json_agg(json_build_object('table',tablename,'name',indexname,'definition',indexdef) ORDER BY tablename,indexname)
  FROM pg_indexes WHERE schemaname='control' AND indexname IN ('acquisition_gaps_pkey','acquisition_gaps_producer','acquisition_jobs_owner')),
 'privileges',json_build_object(
  'gap_select',has_table_privilege('bigbrotr_control','control.acquisition_gaps','SELECT'),
  'gap_insert',has_table_privilege('bigbrotr_control','control.acquisition_gaps','INSERT'),
  'gap_update',has_table_privilege('bigbrotr_control','control.acquisition_gaps','UPDATE'),
  'gap_delete',has_table_privilege('bigbrotr_control','control.acquisition_gaps','DELETE'),
  'gap_truncate',has_table_privilege('bigbrotr_control','control.acquisition_gaps','TRUNCATE'),
  'job_select',has_table_privilege('bigbrotr_control','control.jobs','SELECT'),
  'job_update',has_table_privilege('bigbrotr_control','control.jobs','UPDATE')));
"""))
    return observed


def upgrade(pg, client, output, database, config, label, rows):
    value = dict(before=rows(database), physical_before=physical(pg, database),
        catalog_before=catalog(pg, database), responses=[], grant_statements=[])
    try:
        for name in ('after', 'reapplied'):
            answer = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
                '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '36')
            value['responses'].append(dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr))
            value[name] = rows(database)
            lab.write_json(output / (label + '-upgrade.json'), value)
            assert answer.returncode == 0 and not answer.stderr
        value['physical_after'] = physical(pg, database)
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

    def execute(mode, database, config, directory, seed, setup, predecessor=None):
        exclude(mode + '-before', database, directory)
        before = state(mode + '-before', database, directory)
        local = dict(copy.deepcopy(setup), catalog_before=catalog(pg, database))
        response, clock = invoke(pg, client, output, database, config, mode, helper)
        if response['outcome'] != 'PROCESS_EXITED':
            lab.write_json(output / (mode + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', reason='CLI custody unknown; outer owner must join'))
            raise AssertionError('activated acquisition CLI custody unavailable')
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
        for mode, spec in zip(CASES, (CLOSED, DURABLE), strict=True):
            name = mode.removeprefix('installed-activated-acquisition-')
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
                'migrations/postgres/0036_activated_acquisition_abandonment.sql'),
                migration=upgrade(pg, client, output, database, config, mode, rows))
            first, prior = execute(mode, database, config, directory, seed, setup)
            retry, _ = execute(mode + '-retry', database, config, directory, seed, setup, prior)
            cases.append(dict(name=mode, status='PASS', attempts=[first, retry]))
            lab.write_json(output / 'activated-acquisition-cases.json', cases)
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
        lab.write_json(output / 'activated-acquisition-sources-preserved.json', preserved)
        assert all(all(v.values()) for v in preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES) and sum(len(c['attempts']) for c in cases) == 4
    return dict(name='installed-activated-acquisition-abandonment', status='PASS', cli_invocations=4,
        finite_seed_invocations=0, cases=cases, source_preserved=preserved, helper_sha256=digest(helper),
        scope='one actual activator-owned queued acquisition per closed source; first terminal gap and exact retry',
        not_run=['additional zero-request/unclaimed live intents', 'final abandonment COMMIT loss',
            'raw SQL negative guards', 'ordinary HTTP legacy grants regression', 'producer retirement', 'Archive restart and full M1'])
