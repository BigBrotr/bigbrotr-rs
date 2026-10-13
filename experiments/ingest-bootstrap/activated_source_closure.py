"""Installed source2 closure on five owned copies of qualified activated states.

No workflow retirement or new writer is inferred. Deliberate operator changes
retain complete preimages; uncertain process/backend custody forbids native open.
"""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil
import subprocess

import lab
import activated_recovery as recovery
from activated_recovery_refusals import corrupt_input
from activated_recovery_zero import invoke
from activated_source_closure_oracle import CASES, verify, negative_controls
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_live import exact_absence
from initial_succession import census, digest, physical


GRANT = 'GRANT EXECUTE ON FUNCTION control.require_activated_initial_source(bytea) TO bigbrotr_control;'
DRAIN = """BEGIN;
SET LOCAL session_replication_role=replica;
UPDATE control.owner_epochs SET state='draining' WHERE role='ingest' AND epoch=2 AND state='active'
RETURNING (to_jsonb(owner_epochs)||jsonb_build_object('xmin',xmin::text))::text;
COMMIT;
"""
MIXED = """BEGIN;
SET LOCAL session_replication_role=replica;
UPDATE control.producer_epochs SET state='open' WHERE producer_epoch=2 AND state='closing'
RETURNING (to_jsonb(producer_epochs)||jsonb_build_object('xmin',xmin::text))::text;
COMMIT;
"""
REJECTED = dict(run='ingest-activated-zero-engine-ced4c42', prefix='installed-activated-source-zero',
    database='bb_scope_bind_ced4c42_installed_activated_source_zero',
    source='ced4c4278bd3a2b7aeee4d388686c240780d45dc', owner='2af3dcd04cdcfd171269969c72e551ce',
    status='PASS_INSTALLED_ACTIVATED_RECOVERY_SOURCE_ZERO',
    result='08f408ddd7ff15185b3f56ccd7b6f70c589d08852bcaf848e79a36a11bcbfc6b',
    sql_file='installed-activated-source-zero-retry-after-sql.json',
    sql='ad98106fd0a049623737f891e1ec8ea419e3c16939d909859ed374dbd9f99afd',
    native_file='installed-activated-source-zero-retry-after-native.json',
    native='ab3dc3498a8c4c056c75a621fc0b2bb0db253ba4132212919d1fdecc7c17aae2',
    verification_file='installed-activated-source-zero-retry-verification.json',
    verification='70c88b13e045e1e9e6943a08a93def46967ee5352e9e2a811ed4275f8a69ce6c')


def catalog(pg, database):
    observed = recovery.catalog(pg, database)
    extra = json.loads(lab.sql(pg, database,
        "SET search_path=pg_catalog; SELECT json_build_object('name',p.oid::regprocedure::text,"
        "'definition',pg_get_functiondef(p.oid),'execute',has_function_privilege("
        "'bigbrotr_control',p.oid,'EXECUTE')) FROM pg_proc p WHERE p.oid="
        "'control.require_activated_initial_source(bytea)'::regprocedure"))
    observed['functions'].append(extra)
    observed['functions'].sort(key=lambda r: r['name'])
    return observed


def upgrade(pg, client, output, database, config, label, rows):
    value = dict(before=rows(database), physical_before=physical(pg, database),
        catalog_before=recovery.catalog(pg, database), responses=[])
    try:
        for name in ('after', 'reapplied'):
            answer = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
                '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '35')
            value['responses'].append(dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr))
            value[name] = rows(database)
            lab.write_json(output / (label + '-source-upgrade.json'), value)
            assert answer.returncode == 0 and not answer.stderr
        value['physical_after'] = physical(pg, database)
        value['catalog_ungranted'] = catalog(pg, database)
        return value
    except subprocess.TimeoutExpired as error:
        def text(raw):
            return raw.decode(errors='replace') if isinstance(raw, bytes) else raw or ''
        value.update(outcome='UNKNOWN_TIMEOUT', incomplete_response=dict(returncode=None,
            stdout=text(error.stdout), stderr=text(error.stderr)))
        raise
    finally:
        lab.write_json(output / (label + '-source-upgrade.json'), value)


def grant_source(pg, database, output, label):
    value = dict(statement=GRANT, before=catalog(pg, database))
    try:
        value['response'] = recovery.statement(pg, database, output, label + '-source-grant', GRANT)
        value['after'] = catalog(pg, database)
        return value
    finally:
        lab.write_json(output / (label + '-source-grant.json'), value)


def diagnostic(pg, expected, clock, label):
    function, message = (
        ('require_activated_initial_recovery', 'activated recovery target registry identity differs')
        if label.endswith('legacy-closed') else
        ('require_activated_initial_source', 'activated source closure requires its closing producer and complete outcome copies'))
    since, until = [datetime.fromtimestamp(clock[k] / 1_000_000, timezone.utc).isoformat()
        for k in ('before_us', 'after_us')]
    answer = lab.docker('logs', '--timestamps', '--since', since, '--until', until,
        pg['postgres'], check=False, timeout=5)
    raw = answer.stdout + answer.stderr
    assert len(raw.encode()) <= 256 * 1024
    pattern = rf'\[{expected["pid"]}\] ERROR:  (?:(?P<sqlstate>[0-9A-Z]{{5}}): )?' + re.escape(message) + '$'
    errors = [line for line in raw.splitlines() if re.search(pattern, line)]
    contexts = [line for line in raw.splitlines() if f'[{expected["pid"]}] CONTEXT:  ' in line
        and re.search(r'function (?:control\.)?' + function + r'\(bytea\)', line)]
    code = re.search(pattern, errors[0]).group('sqlstate') if len(errors) == 1 else None
    return dict(backend=expected, since=since, until=until, returncode=answer.returncode,
        observed_sqlstate=code, errors=errors, contexts=contexts)


def run_all(pg, client, output, rows, clone):
    helpers = {name: Path(__file__).with_name(filename) for name, filename in (
        ('source', 'activated_source_closure_client.py'), ('positive', 'activated_recovery_client.py'))}
    for helper in helpers.values():
        target = output / helper.name
        if not target.exists():
            shutil.copy2(helper, target)
        assert digest(target) == digest(helper)
    sources, cases = {}, []
    specs = dict(recovery.SOURCES, rejected=REJECTED)

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
        except Exception as error:
            value = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', error=type(error).__name__,
                detail='Backend absence unproved; available SQL and inputs are provisional')
            lab.write_json(output / (label + '-external-unavailable.json'), value)
            for name, read in [('sql', lambda: rows(database)), ('inputs', lambda:
                    dict(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
                except Exception as capture_error:
                    value[name + '_capture_error'] = type(capture_error).__name__
            lab.write_json(output / (label + '-external-unavailable.json'), value)
            raise

    def mutate(label, database, directory, kind):
        value = dict(preimage=state(label + '-operator-before', database, directory), operation=None)
        lab.write_json(output / (label + '-operator.json'), value)
        known_exit = True
        try:
            if kind == 'missing-input':
                value['operation'] = corrupt_input(directory, output, label, value['preimage'])
            else:
                statement = dict(drain=DRAIN, mixed=MIXED, missing_copy=recovery.REMOVE_COPY)[kind]
                value['operation'] = dict(kind=kind, statement=statement,
                    authority='privileged-isolated-fixture', product_transition=False)
                lab.write_json(output / (label + '-operator.json'), value)
                known_exit = False
                value['operation']['response'] = recovery.statement(pg, database, output, label + '-operator', statement)
                known_exit = True
        finally:
            if known_exit:
                value['after'] = state(label + '-operator-after', database, directory)
            else:
                value.update(outcome='OUTCOME_UNKNOWN', native='NOT_RUN')
            lab.write_json(output / (label + '-operator.json'), value)
        if kind != 'missing-input':
            answer = value['operation']['response']
            assert answer['returncode'] == 0 and not answer['stderr']
            value['operation']['changed'] = json.loads(answer['stdout'])
            lab.write_json(output / (label + '-operator.json'), value)
        return value

    def execute(label, database, config, directory, seed, setup, predecessor=None):
        exclude(label + '-before', database, directory)
        before = state(label + '-before', database, directory)
        local = dict(copy.deepcopy(setup), catalog_before=catalog(pg, database), diagnostic=None)
        helper = helpers['positive' if '-legacy-' in label else 'source']
        response, clock = invoke(pg, client, output, database, config, label, helper)
        if response['outcome'] != 'PROCESS_EXITED':
            lab.write_json(output / (label + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', reason='CLI custody unknown; outer owner must join'))
            raise AssertionError('source closure CLI custody unavailable')
        absent = exclude(label, database, directory)
        after = state(label + '-after', database, directory)
        local['catalog_after'] = catalog(pg, database)
        lab.write_json(output / (label + '-external.json'), dict(**after, catalog=local['catalog_after'], absence=absent))
        exact = exact_absence(pg, database, output, label, after['control'], before['control'])
        context = dict(before=before['control'], after=after['control'], native_before=before['native'],
            native_after=after['native'], inputs_before=before['inputs'], inputs_after=after['inputs'],
            input_contents=after['contents'], response=response, mode=label, seed=seed, setup=local,
            server_window=clock, backend=dict(exact=exact, absence=absent), predecessor=predecessor)
        lab.write_json(output / (label + '-verification.json'), context)
        if label.endswith(('closed-missing-copy', 'mixed', 'legacy-closed')):
            local['diagnostic'] = diagnostic(pg, exact['expected'], clock, label)
            lab.write_json(output / (label + '-diagnostic.json'), local['diagnostic'])
            lab.write_json(output / (label + '-verification.json'), context)
        checked, controls = verify(**context), negative_controls(**context)
        return dict(name=label, status='PASS', oracle=checked, negative_controls=controls,
            verification_sha256=digest(output / (label + '-verification.json'))), context

    try:
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        for name, spec in specs.items():
            basis = output.parent.parent / spec['run'] / 'bindings'
            frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
                spec.get('native_file', spec['prefix'] + '-after-native.json'): spec['native'],
                spec.get('verification_file', spec['prefix'] + '-verification.json'): spec['verification']}
            assert all(digest(basis / n) == sha for n, sha in frozen.items())
            retained = json.loads((basis / 'result.json').read_text())
            assert all(retained[k] == spec[k] for k in ('source', 'owner', 'status'))
            assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
            old, = [c for c in containers if c['Id'] == retained['client']['id']]
            assert old['Config']['Labels'][retained['label']] == spec['owner']
            assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
            assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
            absent = observe_backend_absence(pg, spec['database'], output, 'source-closure-original-' + name)
            original = rows(spec['database'])
            assert original == json.loads((basis / spec['sql_file']).read_text())
            store = basis / spec['prefix'] / 'store'
            seed = dict(name=name, control=original,
                native=json.loads((basis / spec.get('native_file', spec['prefix'] + '-after-native.json')).read_text()),
                inputs=inputs_at(store), contents=recovery.contents_at(store))
            source = dict(seed=seed, files=census(store), frozen=frozen, backend_absence=absent,
                catalog=(recovery.catalog if name == 'rejected' else recovery.previous_catalog)(pg, spec['database']),
                client_id=old['Id'], client_state=old['State'], label=retained['label'])
            sources[name] = source, basis, store
            lab.write_json(output / ('source-closure-original-' + name + '.json'), source)
        for mode in CASES:
            label = mode.removeprefix('installed-source-')
            name = 'off' if label == 'missing-input' else label
            spec = specs[name]
            source, _, store = sources[name]
            seed = source['seed']
            database = clone(mode, spec['database'])
            assert rows(database) == seed['control']
            directory = output / mode
            directory.mkdir(mode=0o700)
            shutil.copytree(store, directory / 'store')
            assert state(mode + '-seed', database, directory) == {k: seed[k] for k in ('control', 'native', 'inputs', 'contents')}
            config = configuration(pg, client, output, database, directory, seed['control'])
            root = Path(__file__).resolve().parents[2]
            recovery_upgrade = None if name == 'rejected' else recovery.upgrade(pg, client, output, database, config, mode, rows)
            policy = recovery.advance_policy(pg, database, output, mode, rows) if name in ('off', 'on') else None
            setup = dict(recovery_upgrade=recovery_upgrade, policy=policy,
                migration_sha256=digest(root / 'migrations/postgres/0033_activated_initial_recovery.sql'),
                correction_sha256=digest(root / 'migrations/postgres/0034_activated_control_retirement.sql'),
                source_migration_sha256=digest(root / 'migrations/postgres/0035_activated_source_closure.sql'),
                source_upgrade=upgrade(pg, client, output, database, config, mode, rows),
                grant=None, operator=None)
            attempts, prior = [], None
            if name == 'zero':
                checked, prior = execute(mode + '-legacy-open', database, config, directory, seed, setup)
                attempts.append(checked)
            setup['grant'] = grant_source(pg, database, output, mode)
            operation = {'off': 'missing_copy', 'on': 'drain', 'missing-input': 'missing-input'}.get(label)
            if operation:
                setup['operator'] = mutate(mode, database, directory, operation)
            checked, prior = execute(mode, database, config, directory, seed, setup, prior)
            attempts.append(checked)
            if label != 'missing-input':
                setup['operator'] = None
                checked, prior = execute(mode + '-retry', database, config, directory, seed, setup, prior)
                attempts.append(checked)
            tail = {'off': ('closed-missing-copy', 'missing_copy'), 'on': ('mixed', 'mixed'), 'zero': ('legacy-closed', None)}.get(label)
            if tail:
                suffix, operation = tail
                setup['operator'] = mutate(mode + '-' + suffix, database, directory, operation) if operation else None
                checked, prior = execute(mode + '-' + suffix, database, config, directory, seed, setup, prior)
                attempts.append(checked)
            cases.append(dict(name=mode, status='PASS', attempts=attempts))
            lab.write_json(output / 'activated-source-closure-cases.json', cases)
    finally:
        preserved = {}
        for name, (source, basis, store) in sources.items():
            spec = specs[name]
            old, = json.loads(lab.docker('inspect', source['client_id']).stdout)
            preserved[name] = dict(control=rows(spec['database']) == source['seed']['control'],
                files=census(store) == source['files'], inputs=inputs_at(store) == source['seed']['inputs'],
                contents=recovery.contents_at(store) == source['seed']['contents'],
                catalog=(recovery.catalog if name == 'rejected' else recovery.previous_catalog)(pg, spec['database']) == source['catalog'],
                client=old['State'] == source['client_state'] and old['Config']['Labels'][source['label']] == spec['owner'],
                artifacts=all(digest(basis / n) == sha for n, sha in source['frozen'].items()))
        lab.write_json(output / 'activated-source-closure-sources-preserved.json', preserved)
        assert all(all(v.values()) for v in preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES)
    assert sum(len(c['attempts']) for c in cases) == 13
    return dict(name='installed-activated-source-closure', status='PASS', cli_invocations=13,
        finite_seed_invocations=0, cases=cases, source_preserved=preserved,
        helper_sha256={name: digest(path) for name, path in helpers.items()},
        scope='source2-only closure and exact retry; open-copy repair, terminal-copy refusal, draining and legacy compatibility',
        not_run=['connected AlreadyDurable/source-zero', 'final source COMMIT loss', 'job/closure lock schedules',
            'omitted/duplicate/out-of-order request and unresolved-tail connected refusals',
            'workflow retirement', 'Archive restart and full M1'])
