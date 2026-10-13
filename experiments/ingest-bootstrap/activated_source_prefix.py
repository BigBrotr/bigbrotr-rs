"""Three fresh clone source-prefix refusals; originals and failed states retained.

Only the new reservation seed runs a finite probe. The other two mutations are
explicit privileged laboratory faults, not supported application transitions.
"""
import copy
import json
from pathlib import Path
import shutil

import lab
import activated_recovery as recovery
import activated_source_closure as source
import initial_live as live
import initial_succession as preparing
from activated_recovery_zero import invoke
from activated_source_prefix_oracle import (CASES, PROBE, CONFLICT, TRUNCATE,
    seed_state, verify, negative_controls)
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_succession import census, digest, physical

CLOSED = dict(run='ingest-activated-source-faults-engine-939cf13', prefix='installed-source-close-after',
    database='bb_scope_bind_939cf13_installed_source_close_after',
    source='939cf1321a72e515be8c901bc0a5316828a88336', owner='924cf5c3a409754cf88bc7556bb3e5ea',
    status='PASS_INSTALLED_ACTIVATED_SOURCE_CLOSURE_COMMIT_LOSS',
    result='7ff76055fb37142fadfd2f0ed1b7fbfd2d857c6751391ab72932eed6fc348011',
    sql_file='installed-source-close-after-recover-after-sql.json',
    sql='b0d97344275c8912f10492da8953f037447d2f6a3f66434fdfe275b412873b24',
    native_file='installed-source-close-after-recover-after-native.json',
    native='cd49f8bfc44c5c08ae307f845f1aaf1da7f66079bf14f8ee54f9c4c1a3819238',
    verification_file='installed-source-close-after-recover-verification.json',
    verification='a0df6dd3924e05f1a60b6895580abdf991a2896a1c02020bf25fd35314af9f1b')


def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_source_closure_client.py')
    target = output / helper.name
    if not target.exists(): shutil.copy2(helper, target)
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
                try: lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
                except Exception as problem: value[name + '_capture_error'] = type(problem).__name__
            lab.write_json(output / (label + '-external-unavailable.json'), value)
            raise

    def origin(name, spec, frozen):
        basis = output.parent.parent / spec['run'] / 'bindings'
        store = basis / spec['prefix'] / 'store'
        assert all(digest(basis / filename) == sha for filename, sha in frozen.items())
        retained = json.loads((basis / 'result.json').read_text())
        assert all(retained[k] == spec[k] for k in ('source', 'owner', 'status'))
        assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        old, = [c for c in containers if c['Id'] == retained['client']['id']]
        assert old['Config']['Labels'][retained['label']] == spec['owner']
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
        absent = observe_backend_absence(pg, spec['database'], output, name + '-original')
        control = rows(spec['database'])
        assert control == json.loads((basis / spec['sql_file']).read_text())
        value = dict(name=name, spec=spec, frozen=frozen, files=census(store), control=control,
            inputs=inputs_at(store), contents=recovery.contents_at(store), backend_absence=absent,
            client_id=old['Id'], client_state=old['State'], label=retained['label'])
        origins.append(value)
        lab.write_json(output / (name + '-original.json'), value)
        return basis, store, value

    def copied(mode, spec, store, original):
        database = clone(mode, spec['database'])
        assert rows(database) == original['control']
        directory = output / mode
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        assert inputs_at(directory / 'store') == original['inputs']
        assert recovery.contents_at(directory / 'store') == original['contents']
        config = configuration(pg, client, output, database, directory, original['control'])
        return database, directory, config

    def upgraded(mode, database, config, *, advance):
        root = Path(__file__).resolve().parents[2]
        migration = recovery.upgrade(pg, client, output, database, config, mode, rows)
        policy = recovery.advance_policy(pg, database, output, mode, rows) if advance else None
        return dict(migration=migration, recovery_upgrade=migration, policy=policy,
            migration_sha256=digest(root / 'migrations/postgres/0033_activated_initial_recovery.sql'),
            correction_sha256=digest(root / 'migrations/postgres/0034_activated_control_retirement.sql'),
            source_migration_sha256=digest(root / 'migrations/postgres/0035_activated_source_closure.sql'),
            source_upgrade=source.upgrade(pg, client, output, database, config, mode, rows),
            grant=source.grant_source(pg, database, output, mode))

    def reapply(mode, database, config):
        value = dict(before=rows(database), physical_before=physical(pg, database),
            catalog_before=source.catalog(pg, database))
        try:
            answer = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
                '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '35')
            value['response'] = dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr)
            value.update(after=rows(database), physical_after=physical(pg, database),
                catalog_after=source.catalog(pg, database))
        finally:
            lab.write_json(output / (mode + '-reapply.json'), value)
        assert answer.returncode == 0 and not answer.stderr
        return dict(reapply=value)

    def mutate(mode, database, directory):
        value = dict(preimage=state(mode + '-operator-before', database, directory),
            operation=dict(authority='privileged-isolated-fixture', product_transition=False,
                kind='retained-target-tail' if mode == CASES[1] else 'outcome-conflict',
                statement=TRUNCATE if mode == CASES[1] else CONFLICT))
        lab.write_json(output / (mode + '-operator.json'), value)
        if mode == CASES[1]:
            known = False
            try:
                answer, _ = capture(client, output, mode + '-corruption', 'activated-corrupt',
                    '/lab/' + directory.name + '/store', 'retained-target-tail')
                known = True
                value['operation']['native_response'] = dict(returncode=answer.returncode,
                    stdout=answer.stdout, stderr=answer.stderr)
                value['native_after'] = state(mode + '-native-operator-after', database, directory)
                value['operation']['native_events'] = [json.loads(line) for line in answer.stdout.splitlines()]
            finally:
                if not known: value.update(outcome='OUTCOME_UNKNOWN', native='NOT_RUN')
                lab.write_json(output / (mode + '-operator.json'), value)
            assert answer.returncode == 0 and not answer.stderr
        known = False
        try:
            answer = recovery.statement(pg, database, output, mode + '-operator', value['operation']['statement'])
            known = True
            value['operation']['response'] = answer
        finally:
            if known: value['after'] = state(mode + '-operator-after', database, directory)
            else: value.update(outcome='OUTCOME_UNKNOWN', native='NOT_RUN')
            lab.write_json(output / (mode + '-operator.json'), value)
        assert answer['returncode'] == 0 and not answer['stderr']
        return value

    def execute(mode, database, config, directory, seed, setup, mutation, seed_context=None):
        exclude(mode + '-before', database, directory)
        before = state(mode + '-before', database, directory)
        setup = dict(copy.deepcopy(setup), catalog_before=source.catalog(pg, database), diagnostic=None)
        response, clock = invoke(pg, client, output, database, config, mode, helper)
        if response['outcome'] != 'PROCESS_EXITED':
            lab.write_json(output / (mode + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', reason='CLI custody unknown; outer owner must join'))
            raise AssertionError('installed source-prefix custody unavailable')
        absent = exclude(mode, database, directory)
        after = state(mode + '-after', database, directory)
        setup['catalog_after'] = source.catalog(pg, database)
        lab.write_json(output / (mode + '-external.json'), dict(**after, catalog=setup['catalog_after'], absence=absent))
        exact = live.exact_absence(pg, database, output, mode, after['control'], before['control'])
        context = dict(before=before['control'], after=after['control'], native_before=before['native'],
            native_after=after['native'], inputs_before=before['inputs'], inputs_after=after['inputs'],
            input_contents=after['contents'], response=response, mode=mode, seed=seed, setup=setup,
            server_window=clock, backend=dict(exact=exact, absence=absent), mutation=mutation, seed_context=seed_context)
        lab.write_json(output / (mode + '-verification.json'), context)
        result = dict(name=mode, status='PASS', oracle=verify(**context), negative_controls=negative_controls(**context),
            verification_sha256=digest(output / (mode + '-verification.json')))
        cases.append(result)
        lab.write_json(output / 'activated-source-prefix-cases.json', cases)

    try:
        spec = dict(run=preparing.SEED, prefix='retire_signed', database=preparing.DATABASE,
            source=preparing.SOURCE, owner=preparing.OWNER, status='PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE',
            sql_file='retire-signed-retry-after.json')
        basis, store, original = origin('prefix-retired', spec, preparing.FROZEN)
        database, directory, config = copied(CASES[0], spec, store, original)
        frozen_native = json.loads((basis / 'retire-signed-retry-native.json').read_text())
        retired = dict(control=original['control'], native=frozen_native, inputs=original['inputs'], sequence=58)
        initial = state(CASES[0] + '-retired', database, directory)
        assert initial['native'] == dict(event='dump-state', rows=frozen_native['rows'], sequence=58)
        catalog_before = live.upgrade(pg, client, output, database, config, PROBE, rows)
        exclude(PROBE + '-before', database, directory)
        before = state(PROBE + '-before', database, directory)
        clock = dict(before_us=server_clock(pg, database))
        lab.write_json(output / (PROBE + '-clock.json'), clock)
        response = None
        try:
            answer, seconds, wire = preparing.invoke(client, output, database, config, PROBE,
                'after-complete', commit_number=14)
            response = dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr, seconds=seconds)
        except BaseException as error:
            path = output / (PROBE + '-response.json')
            if path.exists():
                observed = json.loads(path.read_text())
                if type(observed.get('returncode')) is int:
                    response = dict(observed, orchestration_error=type(error).__name__)
            if response is None:
                lab.write_json(output / (PROBE + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                    native='NOT_RUN', reason='Finite process custody unknown; outer owner must join'))
                raise
        try: clock['after_us'] = server_clock(pg, database)
        except Exception as error: clock['after_error'] = type(error).__name__
        lab.write_json(output / (PROBE + '-clock.json'), clock)
        absent = exclude(PROBE, database, directory)
        after = state(PROBE + '-after', database, directory)
        catalog_after = live.catalog(pg, database)
        lab.write_json(output / (PROBE + '-external.json'), dict(**after, catalog=catalog_after, absence=absent))
        exact = live.exact_absence(pg, database, output, PROBE, after['control'], before['control'])
        # A missing proxy receipt must not erase the known-exit external state.
        try:
            wire = json.loads((output / (PROBE + '-wire.json')).read_text())
        except (OSError, ValueError) as error:
            wire = dict(status='INCOMPLETE', receipt_error=type(error).__name__)
            lab.write_json(output / (PROBE + '-wire-unavailable.json'), wire)
        seed_context = dict(events=[json.loads(line) for line in response['stdout'].splitlines()],
            original=before['control'], native_before=frozen_native, inputs=original['inputs'], retired=retired,
            server_window=clock, response=response, wire=wire, backend=dict(exact=exact, absence=absent),
            after=after['control'], native_after=after['native'], inputs_after=after['inputs'],
            input_contents=after['contents'], catalog_before=catalog_before, catalog_after=catalog_after)
        lab.write_json(output / (PROBE + '-verification.json'), seed_context)
        seed_report = seed_state(**seed_context)
        lab.write_json(output / (PROBE + '-oracle.json'), seed_report)
        execute(CASES[0], database, config, directory, None,
            upgraded(CASES[0], database, config, advance=False), None, seed_context)

        spec = recovery.SOURCES['off']
        frozen = {'result.json':spec['result'], spec['sql_file']:spec['sql'],
            spec['prefix'] + '-after-native.json':spec['native'], spec['prefix'] + '-verification.json':spec['verification']}
        basis, store, original = origin('prefix-off', spec, frozen)
        seed = dict(name='off', control=original['control'], inputs=original['inputs'], contents=original['contents'],
            native=json.loads((basis / (spec['prefix'] + '-after-native.json')).read_text()))
        recovery.known(seed, 'off')
        database, directory, config = copied(CASES[1], spec, store, original)
        baseline = state(CASES[1] + '-seed', database, directory)
        assert baseline == {k:seed[k] for k in ('control', 'native', 'inputs', 'contents')}
        setup = upgraded(CASES[1], database, config, advance=True)
        mutation = mutate(CASES[1], database, directory)
        execute(CASES[1], database, config, directory, seed, setup, mutation)

        frozen = {'result.json':CLOSED['result'], CLOSED['sql_file']:CLOSED['sql'],
            CLOSED['native_file']:CLOSED['native'], CLOSED['verification_file']:CLOSED['verification']}
        basis, store, original = origin('prefix-closed', CLOSED, frozen)
        seed = json.loads((basis / CLOSED['verification_file']).read_text())
        database, directory, config = copied(CASES[2], CLOSED, store, original)
        initial = state(CASES[2] + '-seed', database, directory)
        assert initial == dict(control=seed['after'], native=seed['native_after'],
            inputs=seed['inputs_after'], contents=seed['input_contents'])
        setup = reapply(CASES[2], database, config)
        mutation = mutate(CASES[2], database, directory)
        execute(CASES[2], database, config, directory, seed, setup, mutation)
    finally:
        preserved = {}
        for original in origins:
            spec = original['spec']; basis = output.parent.parent / spec['run'] / 'bindings'
            store = basis / spec['prefix'] / 'store'
            old, = json.loads(lab.docker('inspect', original['client_id']).stdout)
            preserved[original['name']] = dict(control=rows(spec['database']) == original['control'],
                files=census(store) == original['files'], inputs=inputs_at(store) == original['inputs'],
                contents=recovery.contents_at(store) == original['contents'],
                client=old['State'] == original['client_state'] and old['Config']['Labels'][original['label']] == spec['owner'],
                artifacts=all(digest(basis / filename) == sha for filename, sha in original['frozen'].items()))
        lab.write_json(output / 'activated-source-prefix-originals-preserved.json', preserved)
        assert all(all(values.values()) for values in preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES)
    return dict(name='installed-activated-source-prefix-refusals', status='PASS', cases=cases,
        cli_invocations=3, finite_seed_invocations=1, seed=seed_report, source_preserved=preserved,
        helper_sha256=digest(helper), scope='reserved/unissued, retained source tail, terminal SQL copy conflict',
        not_run=['final-publication interposition', 'workflow retirement', 'Archive restart and full M1'])
