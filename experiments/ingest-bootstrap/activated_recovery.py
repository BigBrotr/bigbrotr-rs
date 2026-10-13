"""Installed positive activated recovery in copies of qualified live states.

Three clones, eight genuine config-only processes. The original stores are never
opened natively. Every terminal process is followed by actual backend exclusion
before native inspection; unresolved custody leaves native capture NOT_RUN.
"""
import copy
import json
from pathlib import Path
import shutil
import subprocess
import time

import lab
from installed import wait_for
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_live import catalog as previous_catalog, exact_absence
from initial_succession import census, digest, physical
from activated_recovery_oracle import CASES, REMOVE_COPY, known, verify, negative_controls


SOURCES = {
    'off': dict(run='ingest-installed-live-engine-61a3de1', prefix='installed-live-off',
        database='bb_scope_bind_61a3de1_installed_live_off', source='61a3de10e458b891babbf4bd52212473ec52164a',
        owner='1020f240da197ca01f1a56ee61eb17aa', status='PASS_INSTALLED_LIVE_INITIAL_SUCCESSOR',
        result='30b44add9f3819c136496cb656d625998368cc8e593e3e39bdfb29472097ff5e',
        sql_file='installed-live-off-after-sql.json', sql='b613931fc2785ecd5b2a83ec398c38ca44be5d423c4ce1c6dee986ca243b35ab',
        native='81891df753cb0c8d98a1b92d73961c316c06b1acd3fd53e4259d58386fe78781',
        verification='f2b14a5416e7e39c28e2e5d0a613e41e5f4a0be94e4dc9509da38225d6870f40'),
    'on': dict(run='ingest-installed-live-engine-61a3de1', prefix='installed-live-on',
        database='bb_scope_bind_61a3de1_installed_live_on', source='61a3de10e458b891babbf4bd52212473ec52164a',
        owner='1020f240da197ca01f1a56ee61eb17aa', status='PASS_INSTALLED_LIVE_INITIAL_SUCCESSOR',
        result='30b44add9f3819c136496cb656d625998368cc8e593e3e39bdfb29472097ff5e',
        sql_file='installed-live-on-after-sql.json', sql='29b917dfcf5e0c93ccb3c7f3b188f1e32517dfd1fd63a9cdd5afb4ecaef7d72b',
        native='f5d9e99411a4a2d6655791a4c06292d6f7d9fc7d1afb9c9ac938266f78343679',
        verification='a4a221288de0620badab2419b77972af5abc15f7dc95e405e697fe96b6513245'),
    'zero': dict(run='ingest-live-engine-2c142c6', prefix='successor-live-zero',
        database='bb_scope_bind_2c142c6_successor_live_zero', source='2c142c611eaab0462b6f65df852cd75c098c6c77',
        owner='892de65cb9d38396125405dcb4046bf7', status='PASS_LIVE_INITIAL_ACTIVATION_FINITE',
        result='31245bea8f3f369f1bb464552489671e63debfb8f89e76c6f926ce4661a2c9a2',
        sql_file='successor-live-zero-after.json', sql='454831f6e88365058af33e2cba13f9c5849f937ae1bec6e5e541d99ef07efac4',
        native='b8230b58689c11f36c92e372469a068ce95428142d00e6cca51247f69ebc2b37',
        verification='88a72a3a79c4121ae10798b19a3a427c7d7cb6ed9df1e5c0e3725e2306c74c02'),
}


def contents_at(store):
    return {p.name: p.read_bytes().hex() for p in (store / 'producer-inputs-v1').iterdir()}


def statement(pg, database, output, label, sql):
    (output / (label + '.sql')).write_text(sql)
    result = dict(returncode=None, stdout='', stderr='', outcome='OUTCOME_UNKNOWN')
    try:
        answer = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
            '--no-password', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
            data=sql, check=False, timeout=30)
        result.update(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr,
            outcome='PROCESS_EXITED')
    except subprocess.TimeoutExpired as error:
        def text(value):
            return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
        result.update(stdout=text(error.stdout), stderr=text(error.stderr), outcome='UNKNOWN_TIMEOUT')
        raise
    finally:
        lab.write_json(output / (label + '-response.json'), result)
    return result


def catalog(pg, database):
    observed = previous_catalog(pg, database)
    extra = json.loads(lab.sql(pg, database,
        "SET search_path=pg_catalog; SELECT json_build_object('name',p.oid::regprocedure::text,"
        "'definition',pg_get_functiondef(p.oid),'execute',has_function_privilege("
        "'bigbrotr_control',p.oid,'EXECUTE')) FROM pg_proc p WHERE p.oid="
        "'control.require_activated_initial_recovery(bytea)'::regprocedure"))
    observed['functions'].append(extra)
    observed['functions'].sort(key=lambda value: value['name'])
    return observed


def upgrade(pg, client, output, database, config, mode, rows):
    context = dict(before=rows(database), physical_before=physical(pg, database),
        catalog_before=previous_catalog(pg, database), responses=[])
    try:
        for suffix, target in (('migration33', '33'), ('migration', '34'), ('reapply', '34')):
            response = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
                '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', target)
            answer = dict(returncode=response.returncode, stdout=response.stdout, stderr=response.stderr)
            context['responses'].append(answer)
            context[{'migration33': 'intermediate', 'migration': 'after', 'reapply': 'reapplied'}[suffix]] = rows(database)
            if suffix == 'migration33':
                context['catalog_intermediate'] = catalog(pg, database)
            lab.write_json(output / (mode + '-' + suffix + '.json'), answer)
            assert response.returncode == 0 and not response.stderr
        context['physical_after'] = physical(pg, database)
        grant = 'GRANT EXECUTE ON FUNCTION control.require_activated_initial_recovery(bytea) TO bigbrotr_control;'
        context['grant'] = dict(statement=grant,
            response=statement(pg, database, output, mode + '-grants', grant))
        context['catalog_after'] = catalog(pg, database)
        return context
    except subprocess.TimeoutExpired as error:
        def text(value):
            return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
        context.update(outcome='UNKNOWN_TIMEOUT', incomplete_response=dict(returncode=None,
            stdout=text(error.stdout), stderr=text(error.stderr)))
        raise
    finally:
        lab.write_json(output / (mode + '-upgrade.json'), context)


def policy_sql():
    """Fixed revision2→3 clone setup, same document/digest, every guard enabled."""
    return """BEGIN;
SET LOCAL search_path=pg_catalog;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='5s';
SET LOCAL idle_in_transaction_session_timeout='10s';
DO $fixture$ DECLARE changed bigint; BEGIN
 IF current_setting('session_replication_role')<>'origin' OR EXISTS(
   SELECT FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
   JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='control' AND t.tgenabled NOT IN ('O','A')) THEN
   RAISE EXCEPTION 'activated recovery fixture requires ordinary guards';
 END IF;
 PERFORM 1 FROM control.datasets FOR UPDATE;
 IF (SELECT count(*) FROM control.datasets WHERE config_revision=2 AND policy_revision=2)<>1
 OR (SELECT count(*) FROM control.config_revisions)<>2
 OR (SELECT count(*) FROM control.policy_revisions)<>2 THEN
   RAISE EXCEPTION 'activated recovery fixture requires retained policy2';
 END IF;
 INSERT INTO control.config_revisions
   SELECT 3,dataset_id,2,schema_version,authority_mode,document,document_digest,
     accepted_source_digest,(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,actor_id
   FROM control.config_revisions WHERE revision=2;
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed<>1 THEN RAISE EXCEPTION 'config fixture row count'; END IF;
 INSERT INTO control.policy_revisions
   SELECT 3,dataset_id,2,3,schema_version,document,document_digest,
     (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,actor_id
   FROM control.policy_revisions WHERE revision=2;
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed<>1 THEN RAISE EXCEPTION 'policy fixture row count'; END IF;
 UPDATE control.datasets SET config_revision=3,policy_revision=3 WHERE config_revision=2 AND policy_revision=2;
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed<>1 THEN RAISE EXCEPTION 'head fixture CAS'; END IF;
END $fixture$;
SELECT pg_current_xact_id()::text;
COMMIT;
"""


def advance_policy(pg, database, output, mode, rows):
    value = dict(authority='privileged-isolated-fixture', product_policy_activation=False,
        before=rows(database), catalog_before=catalog(pg, database), sql=policy_sql(),
        server_window={'before_us': server_clock(pg, database)}, status='INCOMPLETE')
    try:
        value['response'] = statement(pg, database, output, mode + '-policy', value['sql'])
        value['server_window']['after_us'] = server_clock(pg, database)
        value['after'] = rows(database)
        value['catalog_after'] = catalog(pg, database)
        assert value['response']['returncode'] == 0 and not value['response']['stderr']
        value['status'] = 'APPLIED'
        return value
    finally:
        lab.write_json(output / (mode + '-policy.json'), value)


def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_recovery_client.py')
    shutil.copy2(helper, output / helper.name)
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    sources, cases = {}, []

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

    def execute(mode, database, config, directory, seed, setup, predecessor=None):
        prefix = output / mode
        observe_backend_absence(pg, database, output, mode + '-before')
        before = save_sql(mode + '-before', database)
        baseline = dump(mode + '-before', directory)
        originals = inputs_at(directory / 'store')
        current_setup = copy.deepcopy(setup)
        current_setup['catalog_before'] = catalog(pg, database)
        lab.write_json(output / (mode + '-before-inputs.json'), originals)
        terminal = None
        response = dict(returncode=None, stdout='', stderr='', outcome='OUTCOME_UNKNOWN')
        clock = dict(before_us=server_clock(pg, database))
        started = time.monotonic()
        try:
            lab.docker('exec', '-d', client, 'python3', '/lab/' + helper.name,
                config, '/lab/' + mode, timeout=5)
            wait_for(lambda: Path(str(prefix) + '.terminal.json').exists(), 'activated recovery joined', 50)
            terminal = json.loads(Path(str(prefix) + '.terminal.json').read_text())
            clock['after_us'] = server_clock(pg, database)
        finally:
            path = Path(str(prefix) + '.terminal.json')
            if terminal is None and path.exists():
                terminal = json.loads(path.read_text())
            for stream in ('stdout', 'stderr'):
                path = Path(str(prefix) + '.' + stream)
                if path.exists():
                    with path.open('rb') as captured:
                        raw = captured.read(65537)
                    response[stream] = raw[:65536].decode(errors='replace')
                    if len(raw) > 65536:
                        response['output_limit_exceeded'] = True
            response.update(terminal=terminal, seconds=time.monotonic() - started)
            if terminal and terminal['reaped'] and not terminal['forced']:
                response.update(returncode=terminal['exit_code'], outcome='PROCESS_EXITED')
                if 'after_us' not in clock:
                    clock['after_us'] = server_clock(pg, database)
            lab.write_json(output / (mode + '-response.json'), response)
            lab.write_json(output / (mode + '-clock.json'), clock)
            if response['outcome'] != 'PROCESS_EXITED':
                lab.write_json(output / (mode + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                    reason='native custody not excluded; outer owner must join before reopening'))
            else:
                absent = observe_backend_absence(pg, database, output, mode)
                after = save_sql(mode + '-after', database)
                final_native = dump(mode + '-after', directory)
                final_inputs = inputs_at(directory / 'store')
                contents = contents_at(directory / 'store')
                current_setup['catalog_after'] = catalog(pg, database)
                # Preserve native/full SQL even if the expected Control claim
                # is absent or malformed and the exact tuple helper then refuses.
                lab.write_json(output / (mode + '-external.json'), dict(control=after, native=final_native,
                    inputs=final_inputs, input_contents=contents, catalog=current_setup['catalog_after'], absence=absent))
                exact = exact_absence(pg, database, output, mode, after, before)
                context = dict(before=before, after=after, native_before=baseline, native_after=final_native,
                    inputs_before=originals, inputs_after=final_inputs, input_contents=contents, response=response,
                    mode=mode, seed=seed, setup=current_setup, server_window=clock,
                    backend=dict(exact=exact, absence=absent), predecessor=predecessor)
                lab.write_json(output / (mode + '-verification.json'), context)
        assert response['outcome'] == 'PROCESS_EXITED'
        evidence = verify(**context)
        perturbations = negative_controls(**context)
        return dict(name=mode, status='PASS', oracle=evidence, negative_controls=perturbations,
            verification_sha256=digest(output / (mode + '-verification.json'))), context

    try:
        # Capture all originals before any mutation of an exclusively named copy.
        for name, spec in SOURCES.items():
            basis = output.parent.parent / spec['run'] / 'bindings'
            frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
                spec['prefix'] + '-after-native.json': spec['native'],
                spec['prefix'] + '-verification.json': spec['verification']}
            assert all(digest(basis / n) == value for n, value in frozen.items())
            result = json.loads((basis / 'result.json').read_text())
            assert result['status'] == spec['status'] and result['source'] == spec['source'] and result['owner'] == spec['owner']
            assert not result['cleanup_errors'] and not result['unrelated_changed'] and not result['missing_owned']
            old, = [c for c in containers if c['Id'] == result['client']['id']]
            assert old['Config']['Labels'][result['label']] == spec['owner']
            assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
            assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
            absent = observe_backend_absence(pg, spec['database'], output, 'activated-source-' + name)
            original = rows(spec['database'])
            assert original == json.loads((basis / spec['sql_file']).read_text())
            store = basis / spec['prefix'] / 'store'
            seed = dict(name=name, control=original,
                native=json.loads((basis / (spec['prefix'] + '-after-native.json')).read_text()),
                inputs=inputs_at(store), contents=contents_at(store))
            known(seed, name)
            source = dict(seed=seed, files=census(store), frozen=frozen, catalog=previous_catalog(pg, spec['database']),
                backend_absence=absent, client_id=old['Id'], client_state=old['State'], label=result['label'])
            sources[name] = (source, basis, store)
            lab.write_json(output / ('activated-source-' + name + '.json'), source)
        for mode in CASES:
            name = mode.removeprefix('installed-activated-')
            spec = SOURCES[name]
            source, basis, store = sources[name]
            seed = source['seed']
            database = clone(mode, spec['database'])
            assert rows(database) == seed['control']
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert dump(mode + '-seed', directory) == seed['native']
            assert contents_at(directory / 'store') == seed['contents']
            config = configuration(pg, client, output, database, directory, seed['control'])
            migration = upgrade(pg, client, output, database, config, mode, rows)
            schema = Path(__file__).resolve().parents[2] / 'migrations/postgres/0033_activated_initial_recovery.sql'
            correction = schema.with_name('0034_activated_control_retirement.sql')
            setup = dict(migration=migration, migration_sha256=digest(schema), correction_sha256=digest(correction),
                policy=None if name == 'zero' else advance_policy(pg, database, output, mode, rows), removed_copy=None)
            attempts = []
            checked, prior = execute(mode, database, config, directory, seed, setup)
            attempts.append(checked)
            checked, prior = execute(mode + '-retry', database, config, directory, seed, setup, prior)
            attempts.append(checked)
            if name != 'zero':
                row, = [r for r in prior['after']['producer_outcomes'] if r['producer_epoch'] == 2 and r['request_sequence'] == 2]
                removed = dict(row=row, statement=REMOVE_COPY)
                # Retain original complete attribution before the deliberate
                # operator deletion on this clone, even if the SQL call fails.
                lab.write_json(output / (mode + '-removed-outcome.json'), removed)
                removed['response'] = statement(pg, database, output, mode + '-remove-copy', REMOVE_COPY)
                lab.write_json(output / (mode + '-removed-outcome.json'), removed)
                setup = dict(setup, removed_copy=removed)
                checked, prior = execute(mode + '-repair', database, config, directory, seed, setup, prior)
                attempts.append(checked)
            cases.append(dict(name=mode, status='PASS', attempts=attempts))
            lab.write_json(output / 'activated-recovery-cases.json', cases)
    finally:
        preserved = {}
        for name, (source, basis, store) in sources.items():
            old, = json.loads(lab.docker('inspect', source['client_id']).stdout)
            preserved[name] = dict(control=rows(SOURCES[name]['database']) == source['seed']['control'],
                files=census(store) == source['files'], inputs=inputs_at(store) == source['seed']['inputs'],
                contents=contents_at(store) == source['seed']['contents'],
                catalog=previous_catalog(pg, SOURCES[name]['database']) == source['catalog'],
                client=old['State'] == source['client_state'] and
                    old['Config']['Labels'][source['label']] == SOURCES[name]['owner'],
                artifacts=all(digest(basis / n) == sha for n, sha in source['frozen'].items()))
        lab.write_json(output / 'activated-recovery-sources-preserved.json', preserved)
        assert all(all(v.values()) for v in preserved.values()), preserved
    assert len(cases) == 3 and sum(len(c['attempts']) for c in cases) == 8
    return dict(name='installed-activated-recovery', status='PASS', cli_invocations=8, cases=cases,
        source_preserved=preserved, helper_sha256=digest(helper),
        scope='installed activated positive recovery, retained original science, current policy and expired target lease',
        not_run=['issued-positive source-zero', 'corrupt/missing inputs and reference dependencies',
            'wrong activation/handoff/extra producers/foreign streams', 'native-held refusal',
            'closing/outcome COMMIT loss and config-only resolution', 'source2 closure/retirement', 'full M1'])
