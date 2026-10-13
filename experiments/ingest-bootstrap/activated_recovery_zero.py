"""One actual rejected live request, then two installed positive recovery processes.

The native source-zero seed is produced through real typed activation/admission,
never by installing a synthesized receipt. Original retired laboratories remain
read-only; unknown custody prevents native inspection of the owned clone.
"""
import copy
import json
from pathlib import Path
import shutil
import time

import lab
import activated_recovery as recovery
import initial_live as live
import initial_succession as preparing
from activated_recovery_zero_oracle import (CASES, MODE, PROBE, verify, negative_controls,
    verify_seed, seed_negative_controls)
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_succession import census, digest
from installed import wait_for


def invoke(pg, client, output, database, config, mode, helper):
    prefix = output / mode
    terminal = None
    response = dict(returncode=None, stdout='', stderr='', outcome='OUTCOME_UNKNOWN')
    clock = dict(before_us=server_clock(pg, database))
    started = time.monotonic()
    try:
        lab.docker('exec', '-d', client, 'python3', '/lab/' + helper.name,
            config, '/lab/' + mode, timeout=5)
        wait_for(lambda: Path(str(prefix) + '.terminal.json').exists(), 'source-zero recovery joined', 50)
        terminal = json.loads(Path(str(prefix) + '.terminal.json').read_text())
        clock['after_us'] = server_clock(pg, database)
    except Exception as error:
        response['orchestration_error'] = type(error).__name__
    finally:
        path = Path(str(prefix) + '.terminal.json')
        if terminal is None and path.exists():
            try:
                terminal = json.loads(path.read_text())
            except (ValueError, TypeError) as error:
                response['terminal_error'] = type(error).__name__
        for stream in ('stdout', 'stderr'):
            path = Path(str(prefix) + '.' + stream)
            if path.exists():
                with path.open('rb') as captured:
                    raw = captured.read(65537)
                response[stream] = raw[:65536].decode(errors='replace')
                if len(raw) > 65536:
                    response['output_limit_exceeded'] = True
        response.update(terminal=terminal, seconds=time.monotonic() - started)
        if isinstance(terminal, dict) and terminal.get('reaped') is True and terminal.get('forced') is False:
            response.update(returncode=terminal['exit_code'], outcome='PROCESS_EXITED')
            if 'after_us' not in clock:
                try:
                    clock['after_us'] = server_clock(pg, database)
                except Exception as error:
                    response['clock_error'] = type(error).__name__
        lab.write_json(output / (mode + '-response.json'), response)
        lab.write_json(output / (mode + '-clock.json'), clock)
    return response, clock


def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_recovery_client.py')
    target = output / helper.name
    if not target.exists():
        shutil.copy2(helper, target)
    assert digest(target) == digest(helper)
    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    source = None
    cases = []

    def sql(label, database):
        value = rows(database)
        lab.write_json(output / (label + '-sql.json'), value)
        return value

    def dump(label, directory):
        answer, _ = capture(client, output, label + '-dump', 'dump-state', '/lab/' + directory.name + '/store')
        assert answer.returncode == 0 and not answer.stderr
        value = json.loads(answer.stdout)
        lab.write_json(output / (label + '-native.json'), value)
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
        before = sql(mode + '-before', database)
        baseline = dump(mode + '-before', directory)
        originals = inputs_at(directory / 'store')
        current_setup = copy.deepcopy(setup)
        current_setup['catalog_before'] = recovery.catalog(pg, database)
        lab.write_json(output / (mode + '-before-inputs.json'), originals)
        response, clock = invoke(pg, client, output, database, config, mode, helper)
        if response['outcome'] != 'PROCESS_EXITED':
            lab.write_json(output / (mode + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', reason='CLI custody unknown; outer owner must join before reopening'))
            raise AssertionError('installed source-zero custody unavailable')
        absent = exclude(mode, database, directory)
        after = sql(mode + '-after', database)
        final_native = dump(mode + '-after', directory)
        final_inputs = inputs_at(directory / 'store')
        contents = recovery.contents_at(directory / 'store')
        current_setup['catalog_after'] = recovery.catalog(pg, database)
        lab.write_json(output / (mode + '-external.json'), dict(control=after, native=final_native,
            inputs=final_inputs, input_contents=contents, catalog=current_setup['catalog_after'], absence=absent))
        exact = live.exact_absence(pg, database, output, mode, after, before)
        context = dict(before=before, after=after, native_before=baseline, native_after=final_native,
            inputs_before=originals, inputs_after=final_inputs, input_contents=contents, response=response,
            mode=mode, seed_context=seed_context, setup=current_setup, server_window=clock,
            backend=dict(exact=exact, absence=absent), predecessor=predecessor)
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
        absent = observe_backend_absence(pg, preparing.DATABASE, output, 'source-zero-original')
        original = rows(preparing.DATABASE)
        assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
        frozen_native = json.loads((basis / 'retire-signed-retry-native.json').read_text())
        retired = dict(control=original, native=frozen_native, inputs=inputs_at(store), sequence=58)
        source = dict(retired=retired, files=census(store), contents=recovery.contents_at(store),
            client_id=old['Id'], client_state=old['State'], label=retained['label'], backend_absence=absent)
        lab.write_json(output / 'source-zero-original.json', source)
        database = clone(MODE, preparing.DATABASE)
        assert rows(database) == original
        directory = output / MODE
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        initial = dump(MODE + '-retired', directory)
        assert initial == dict(event='dump-state', rows=frozen_native['rows'], sequence=58)
        assert recovery.contents_at(directory / 'store') == source['contents']
        config = configuration(pg, client, output, database, directory, original)
        seed_label = MODE + '-seed'
        catalog_before = live.upgrade(pg, client, output, database, config, seed_label, rows)
        exclude(seed_label + '-before', database, directory)
        before = sql(seed_label + '-before', database)
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
        after = sql(seed_label + '-after', database)
        final_native = dump(seed_label + '-after', directory)
        final_inputs = inputs_at(directory / 'store')
        contents = recovery.contents_at(directory / 'store')
        catalog_after = live.catalog(pg, database)
        lab.write_json(output / (seed_label + '-external.json'), dict(control=after, native=final_native,
            inputs=final_inputs, input_contents=contents, catalog=catalog_after, absence=absent))
        exact = live.exact_absence(pg, database, output, seed_label, after, before)
        # Full external evidence is retained even if output JSON/semantic checks fail.
        seed_context = dict(events=[json.loads(line) for line in answer.stdout.splitlines()],
            original=before, native=frozen_native, inputs=retired['inputs'], retired=retired,
            server_window=clock, backend=dict(exact=exact, absence=absent),
            response=dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr),
            after=after, native_after=final_native, inputs_after=final_inputs, input_contents=contents,
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
            policy=None, removed_copy=None)
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
            lab.write_json(output / 'source-zero-original-preserved.json', preserved)
            assert all(preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES) and len(cases[0]['attempts']) == 2
    return dict(name='installed-activated-recovery-source-zero', status='PASS', cli_invocations=2,
        finite_seed_invocations=1, cases=cases, source_preserved=preserved, helper_sha256=digest(helper),
        scope='actual rejected request1 with source0, then installed config-only first/retry recovery',
        not_run=['installed import', 'network provenance', 'AlreadyDurable source-zero',
            'source2 closure/retirement', 'corruptions and COMMIT-loss', 'full M1'])
