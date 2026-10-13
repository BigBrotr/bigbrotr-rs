"""Three actual authenticated intents, then selected acquisition abandonment and retry.

Original labs remain read-only. Unknown process/backend custody forbids native
inspection; known exits retain SQL and original inputs even if native capture fails.
"""
import copy
import json
from pathlib import Path
import shutil

import lab
import activated_recovery as recovery
import initial_live as live
import initial_succession as preparing
from activated_acquisition_claims_oracle import (CASES, MODE, PROBE, verify, negative_controls,
    verify_seed, seed_negative_controls)
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_succession import census, digest
from activated_source_durable import configure
import activated_acquisition as abandonment
from activated_recovery_zero import invoke
import activated_source_closure as closure



def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_acquisition_client.py')
    target = output / helper.name
    if not target.exists():
        shutil.copy2(helper, target)
    assert digest(target) == digest(helper)
    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    source = None
    cases = []

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
            observation = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', reason=type(error).__name__,
                detail='Backend absence not established; available SQL/inputs are provisional, not final')
            lab.write_json(output / (label + '-external-unavailable.json'), observation)
            for name, read in [('sql', lambda: rows(database)),
                    ('inputs', lambda: dict(inputs=inputs_at(directory / 'store'),
                        contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
                except Exception as capture_error:
                    observation[name + '_capture_error'] = type(capture_error).__name__
            lab.write_json(output / (label + '-external-unavailable.json'), observation)
            raise

    def execute(mode, database, config, directory, seed_context, setup, predecessor=None):
        exclude(mode + '-before', database, directory)
        before = state(mode + '-before', database, directory)
        current_setup = copy.deepcopy(setup)
        current_setup['catalog_before'] = abandonment.catalog(pg, database)
        response, clock = invoke(pg, client, output, database, config, mode, helper)
        if response['outcome'] != 'PROCESS_EXITED':
            lab.write_json(output / (mode + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', reason='CLI custody unknown; outer owner must join before reopening'))
            raise AssertionError('installed source-durable custody unavailable')
        absent = exclude(mode, database, directory)
        after = state(mode + '-after', database, directory)
        current_setup['catalog_after'] = abandonment.catalog(pg, database)
        lab.write_json(output / (mode + '-external.json'), dict(**after,
            catalog=current_setup['catalog_after'], absence=absent))
        exact = live.exact_absence(pg, database, output, mode, after['control'], before['control'])
        context = dict(before=before['control'], after=after['control'], native_before=before['native'],
            native_after=after['native'], inputs_before=before['inputs'], inputs_after=after['inputs'],
            input_contents=after['contents'], response=response, mode=mode, seed_context=seed_context,
            setup=current_setup, server_window=clock, backend=dict(exact=exact, absence=absent), predecessor=predecessor)
        lab.write_json(output / (mode + '-verification.json'), context)
        evidence = verify(**context)
        controls = negative_controls(**context)
        return dict(name=mode, status='PASS', oracle=evidence, negative_controls=controls,
            verification_sha256=digest(output / (mode + '-verification.json'))), context

    try:
        assert all(digest(basis / name) == sha for name, sha in preparing.FROZEN.items())
        retained = json.loads((basis / 'result.json').read_text())
        assert retained['status'] == 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE'
        assert retained['source'] == preparing.SOURCE and retained['owner'] == preparing.OWNER
        assert not retained['cleanup_errors'] and not retained['unrelated_changed'] and not retained['missing_owned']
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        old, = [c for c in containers if c['Id'] == retained['client']['id']]
        assert old['Config']['Labels'][retained['label']] == preparing.OWNER
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
        absent = observe_backend_absence(pg, preparing.DATABASE, output, 'acquisition-claims-original')
        original = rows(preparing.DATABASE)
        assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
        frozen_native = json.loads((basis / 'retire-signed-retry-native.json').read_text())
        retired = dict(control=original, native=frozen_native, inputs=inputs_at(store), sequence=58)
        source = dict(retired=retired, files=census(store), contents=recovery.contents_at(store),
            client_id=old['Id'], client_state=old['State'], label=retained['label'], backend_absence=absent)
        lab.write_json(output / 'acquisition-claims-original.json', source)
        database = clone(MODE, preparing.DATABASE)
        assert rows(database) == original
        directory = output / MODE
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        initial = state(MODE + '-retired', database, directory)
        assert initial['native'] == dict(event='dump-state', rows=frozen_native['rows'], sequence=58)
        assert initial['control'] == original
        assert recovery.contents_at(directory / 'store') == source['contents']
        config = configure(pg, client, output, database, directory, original)
        seed_label = MODE + '-seed'
        catalog_before = live.upgrade(pg, client, output, database, config, seed_label, rows)
        exclude(seed_label + '-before', database, directory)
        before = state(seed_label + '-before', database, directory)
        resolution = lab.docker('exec', client, 'python3', '-c',
            "import json,time; n=round(time.clock_getres(time.CLOCK_REALTIME)*1000000000); print(json.dumps(dict(clock='CLOCK_REALTIME',resolution_ns=n,resolution_us=(n+999)//1000)))", timeout=5)
        lab.write_json(output / (seed_label + '-receive-clock.json'), dict(returncode=resolution.returncode,
            stdout=resolution.stdout, stderr=resolution.stderr))
        assert resolution.returncode == 0 and not resolution.stderr
        receive_clock = json.loads(resolution.stdout)
        clock = dict(before_us=server_clock(pg, database))
        lab.write_json(output / (seed_label + '-clock.json'), clock)
        answer = None
        try:
            answer, _ = capture(client, output, seed_label, PROBE, database, '/run/secrets/application', config)
        finally:
            if answer is None:
                lab.write_json(output / (seed_label + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                    native='NOT_RUN', reason='Finite seed process custody unknown; outer owner must join'))
        try:
            clock['after_us'] = server_clock(pg, database)
        except Exception as error:
            # The finite child has exited. Preserve its external state even
            # when the clock observation fails; an incomplete window refuses.
            clock['after_error'] = type(error).__name__
        lab.write_json(output / (seed_label + '-clock.json'), clock)
        absent = exclude(seed_label, database, directory)
        after = state(seed_label + '-after', database, directory)
        catalog_after = live.catalog(pg, database)
        lab.write_json(output / (seed_label + '-external.json'), dict(**after, catalog=catalog_after, absence=absent))
        exact = live.exact_absence(pg, database, output, seed_label, after['control'], before['control'])
        # Preserve full external evidence before interpreting finite output.
        seed_context = dict(events=[json.loads(line) for line in answer.stdout.splitlines()],
            original=before['control'], native=frozen_native, inputs=retired['inputs'], retired=retired,
            server_window=clock, backend=dict(exact=exact, absence=absent), receive_clock=receive_clock,
            response=dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr),
            after=after['control'], native_after=after['native'], inputs_after=after['inputs'], input_contents=after['contents'],
            catalog_before=catalog_before, catalog_after=catalog_after)
        lab.write_json(output / (seed_label + '-verification.json'), seed_context)
        seed_report = dict(oracle=verify_seed(**seed_context), negative_controls=seed_negative_controls(**seed_context),
            verification_sha256=digest(output / (seed_label + '-verification.json')))
        lab.write_json(output / (seed_label + '-oracle.json'), seed_report)
        # Only positive seed verification authorizes reuse of this new clone.
        migration = recovery.upgrade(pg, client, output, database, config, MODE, rows)
        root = Path(__file__).resolve().parents[2]
        setup = dict(migration=migration,
            migration_sha256=digest(root / 'migrations/postgres/0033_activated_initial_recovery.sql'),
            correction_sha256=digest(root / 'migrations/postgres/0034_activated_control_retirement.sql'),
            source_migration_sha256=digest(root / 'migrations/postgres/0035_activated_source_closure.sql'),
            source_upgrade=closure.upgrade(pg, client, output, database, config, MODE, rows),
            grant=closure.grant_source(pg, database, output, MODE),
            abandonment_migration_sha256=digest(root / 'migrations/postgres/0036_activated_acquisition_abandonment.sql'),
            abandonment_upgrade=abandonment.upgrade(pg, client, output, database, config, MODE + '-abandonment', rows))
        first, prior = execute(MODE, database, config, directory, seed_context, setup)
        retry, _ = execute(MODE + '-retry', database, config, directory, seed_context, setup, prior)
        cases.append(dict(name=MODE, status='PASS', seed=seed_report, attempts=[first, retry]))
    finally:
        if source is not None:
            old, = json.loads(lab.docker('inspect', source['client_id']).stdout)
            preserved = dict(control=rows(preparing.DATABASE) == original,
                files=census(store) == source['files'], inputs=inputs_at(store) == retired['inputs'],
                contents=recovery.contents_at(store) == source['contents'],
                client=old['State'] == source['client_state'] and
                    old['Config']['Labels'][source['label']] == preparing.OWNER,
                artifacts=all(digest(basis / name) == sha for name, sha in preparing.FROZEN.items()))
            lab.write_json(output / 'acquisition-claims-original-preserved.json', preserved)
            assert all(preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES) and len(cases[0]['attempts']) == 2
    return dict(name='installed-activated-acquisition-claims', status='PASS', cli_invocations=2,
        finite_seed_invocations=1, cases=cases, source_preserved=preserved, helper_sha256=digest(helper),
        scope='actual same-Control claimed and unclaimed intents; select two claims including zero-request, preserve accepted intent',
        abandoned_jobs=2, unclaimed_jobs=1, session_revoked=False,
        not_run=['installed acquisition scheduling', 'ordinary worker job execution', 'final abandonment COMMIT loss',
            'job/closure lock schedules', 'workflow retirement', 'Archive restart and full M1'])
