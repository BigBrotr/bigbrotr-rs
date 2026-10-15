"""One live typed zero-request acquisition claim, source close and retirement veto.

No historical owner/job rewrite, workflow completion or native fabrication.
"""
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil
import stat

import lab
import activated_acquisition as acquisition
import activated_recovery as recovery
import activated_retirement as retirement
import activated_source_closure as source
import initial_live as live
import initial_succession as preparing
from activated_recovery_zero import invoke
from activated_source_durable import configure
from initial_empty_locks import guards
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_live import exact_absence
from activated_retirement_unresolved_oracle import CASES, PROBE, verify, verify_seed, negative_controls

FROZEN = dict(preparing.FROZEN)


def copy_manifest(files):
    """Compare copied contents; retain physical identity for original custody."""
    result = {}
    for name, value in files.items():
        mode = value['mode']
        assert stat.S_ISDIR(mode) or stat.S_ISREG(mode)
        result[name] = dict(mode=mode)
        if stat.S_ISREG(mode):
            result[name].update(bytes=value['bytes'], sha256=value['sha256'])
        else:
            assert value['sha256'] is None
    return result


def closed_and_joined(response):
    """A docker-exec result is not remote native custody evidence by itself."""
    if response.returncode != 0 or response.stderr != '':
        return False
    try:
        def unique(pairs):
            assert len(pairs) == len(dict(pairs))
            return dict(pairs)
        events = [json.loads(line, object_pairs_hook=unique) for line in response.stdout.splitlines()]
        assert all(isinstance(event, dict) for event in events)
        claimed, = [e for e in events if e.get('event') == 'successor-control'
            and e.get('stage') == 'initial' and e.get('phase') == 'claimed']
        present, absent = [e for e in events if e.get('event') == 'successor-backend']
        assert present['stage'] == 'initial' and present['present'] is True
        assert absent == dict(present, present=False)
        assert present['control_epoch'] == claimed['epoch'] == 8
        tail = events[-10:]
        assert len(tail) == 10
        for position, stage in ((0, 'final-before-control-end'),
                (6, 'final-after-control-end'), (7, 'final')):
            assert tail[position]['event'] == 'successor-state' and tail[position]['stage'] == stage
        assert tail[1] == dict(event='live-zero-claim-release', attempted=True, error=None)
        for position, stage in ((2, 'control'), (8, 'observer')):
            assert tail[position] == dict(event='live-zero-claim-driver', stage=stage,
                joined=True, failed=False, aborted=False, error=None)
        assert tail[3] == dict(event='successor-control', stage='final', phase='released-and-joined',
            resource=claimed['resource'], epoch=8)
        assert tail[4] == dict(event='live-zero-claim-backend-absence', error=None)
        assert tail[5] == absent
        assert tail[9] == dict(event='live-complete', mode=PROBE, native='closed',
            control='released-and-joined', writer=False, admission=False, passed=True)
        return True
    except (AssertionError, KeyError, ValueError, TypeError, IndexError):
        return False


def state(client, output, label, database, directory, rows):
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
    return value


def uncertain(output, label, database, directory, rows, reason):
    value = dict(status='OUTCOME_UNKNOWN', native='NOT_RUN', reason=reason)
    lab.write_json(output / (label + '-external-unavailable.json'), value)
    for name, read in [('sql', lambda: rows(database)), ('inputs', lambda:
            dict(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
        try:
            lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
        except BaseException as error:
            value[name + '_capture_error'] = type(error).__name__
    lab.write_json(output / (label + '-external-unavailable.json'), value)


def diagnostic(pg, expected, clock):
    since, until = [datetime.fromtimestamp(clock[k] / 1_000_000, timezone.utc).isoformat()
        for k in ('before_us', 'after_us')]
    answer = lab.docker('logs', '--timestamps', '--since', since, '--until', until,
        pg['postgres'], check=False, timeout=5)
    raw = answer.stdout + answer.stderr
    assert len(raw.encode()) <= 256 * 1024
    pattern = rf'\[{expected["pid"]}\] ERROR:  (?:(?P<sqlstate>[0-9A-Z]{{5}}): )?activated producer workflow claims remain unresolved$'
    errors = [line for line in raw.splitlines() if re.search(pattern, line)]
    contexts = [line for line in raw.splitlines() if f'[{expected["pid"]}] CONTEXT:  ' in line
        and re.search(r'function (?:control\.)?lock_activated_initial_retirement\(bytea\)', line)]
    return dict(backend=expected, since=since, until=until, returncode=answer.returncode,
        errors=errors, contexts=contexts, observed_sqlstate=(re.search(pattern, errors[0]).group('sqlstate')
            if len(errors) == 1 else None))


def exclude(pg, output, rows, database, directory, label):
    try:
        return observe_backend_absence(pg, database, output, label)
    except BaseException as error:
        uncertain(output, label, database, directory, rows, 'Backend absence unavailable: ' + type(error).__name__)
        raise


def run_all(pg, client, output, rows, clone):
    name, = CASES
    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    assert all(preparing.digest(basis / n) == sha for n, sha in FROZEN.items())
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE'
    assert retained['source'] == preparing.SOURCE and retained['owner'] == preparing.OWNER
    assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert old['Config']['Labels'][retained['label']] == preparing.OWNER
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
    absence = observe_backend_absence(pg, preparing.DATABASE, output, name + '-original')
    original = rows(preparing.DATABASE)
    assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
    raw = json.loads((basis / 'retire-signed-retry-native.json').read_text())
    retired = dict(control=original, native=raw, inputs=inputs_at(store), sequence=58)
    files, contents, catalog = preparing.census(store), recovery.contents_at(store), guards(pg, preparing.DATABASE)
    lab.write_json(output / (name + '-original.json'), dict(retired=retired, files=files,
        contents=contents, catalog=catalog, backend_absence=absence, client=old['State'], frozen=FROZEN))
    for filename in ('activated_source_closure_client.py', 'activated_acquisition_client.py'):
        helper, target = Path(__file__).with_name(filename), output / filename
        if not target.exists():
            shutil.copy2(helper, target)
        assert preparing.digest(target) == preparing.digest(helper)
    attempts = []
    try:
        database = clone(name, preparing.DATABASE)
        assert rows(database) == original
        directory = output / name
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        copied = preparing.census(directory / 'store')
        lab.write_json(output / (name + '-copied-files.json'), copied)
        assert copy_manifest(copied) == copy_manifest(files)
        config = configure(pg, client, output, database, directory, original)
        config_path = output / Path(config).name
        catalog_before = live.upgrade(pg, client, output, database, config, name + '-32', rows)
        exclude(pg, output, rows, database, directory, name + '-seed-before')
        before = state(client, output, name + '-seed-before', database, directory, rows)
        clock = dict(before_us=server_clock(pg, database))
        lab.write_json(output / (name + '-seed-clock.json'), clock)
        answer = None
        try:
            answer, _ = capture(client, output, name + '-seed', PROBE, database, '/run/secrets/application', config)
        finally:
            if answer is None:
                uncertain(output, name + '-seed', database, directory, rows,
                    'Finite process outcome unknown; native custody not excluded')
        try:
            clock['after_us'] = server_clock(pg, database)
        except BaseException as error:
            clock['after_error'] = type(error).__name__
        lab.write_json(output / (name + '-seed-clock.json'), clock)
        closed = closed_and_joined(answer)
        lab.write_json(output / (name + '-seed-custody.json'), dict(
            status='OBSERVED_CLOSED_AND_JOINED' if closed else 'OUTCOME_UNKNOWN',
            native='closed' if closed else 'NOT_RUN'))
        if not closed:
            uncertain(output, name + '-seed', database, directory, rows,
                'Finite native close and both joined drivers were not positively observed')
            raise AssertionError('finite seed native custody unavailable')
        absence = exclude(pg, output, rows, database, directory, name + '-seed')
        after = state(client, output, name + '-seed-after', database, directory, rows)
        catalog_after = live.catalog(pg, database)
        lab.write_json(output / (name + '-seed-external.json'), dict(after, absence=absence, catalog=catalog_after))
        exact = exact_absence(pg, database, output, name + '-seed', after['control'], before['control'])
        seed = dict(events=[json.loads(line) for line in answer.stdout.splitlines()], original=before['control'],
            native=raw, inputs=retired['inputs'], retired=retired, server_window=clock,
            backend=dict(exact=exact, absence=absence), response=dict(returncode=answer.returncode,
                stdout=answer.stdout, stderr=answer.stderr), after=after['control'], native_after=after['native'],
            inputs_after=after['inputs'], input_contents=after['contents'], catalog_before=catalog_before,
            catalog_after=catalog_after)
        lab.write_json(output / (name + '-seed-verification.json'), seed)
        # The seed is checked before any recovery consumer is invoked.
        lab.write_json(output / (name + '-seed-oracle.json'), verify_seed(**seed))
        setup = dict(configuration=dict(path=config, text=config_path.read_text(), sha256=preparing.digest(config_path)),
            recovery=recovery.upgrade(pg, client, output, database, config, name + '-34', rows))
        setup['source'] = source.upgrade(pg, client, output, database, config, name + '-35', rows)
        setup['source_grant'] = source.grant_source(pg, database, output, name)
        setup['acquisition'] = acquisition.upgrade(pg, client, output, database, config, name + '-36', rows)
        setup['retirement'] = retirement.upgrade(pg, client, output, database, config, name + '-37', rows)
        lab.write_json(output / (name + '-setup.json'), setup)
        for phase in ('source', 'retirement'):
            mode = name + '-' + phase
            exclude(pg, output, rows, database, directory, mode + '-before')
            before = state(client, output, mode + '-before', database, directory, rows)
            catalog_before = retirement.catalog(pg, database)
            helper = Path(__file__).with_name('activated_source_closure_client.py' if phase == 'source'
                else 'activated_acquisition_client.py')
            response, window = invoke(pg, client, output, database, config, mode, helper,
                action='--retire-activated-initial' if phase == 'retirement' else None)
            if response['outcome'] != 'PROCESS_EXITED':
                uncertain(output, mode, database, directory, rows, 'CLI outcome unknown; native custody not excluded')
                raise AssertionError('unknown installed process outcome')
            absence = exclude(pg, output, rows, database, directory, mode)
            after = state(client, output, mode + '-after', database, directory, rows)
            catalog_after = retirement.catalog(pg, database)
            lab.write_json(output / (mode + '-external.json'), dict(after, catalog=catalog_after, absence=absence))
            exact = exact_absence(pg, database, output, mode, after['control'], before['control'])
            item = dict(mode=mode, phase=phase, before=before, after=after, response=response,
                server_window=window, backend=dict(exact=exact, absence=absence), catalog_before=catalog_before,
                catalog_after=catalog_after, diagnostic=None)
            attempts.append(item)
            lab.write_json(output / (name + '-attempts.json'), attempts)
            if phase == 'retirement':
                item['diagnostic'] = diagnostic(pg, exact['expected'], window)
            lab.write_json(output / (name + '-attempts.json'), attempts)
        context = dict(seed=seed, setup=setup, attempts=attempts)
        lab.write_json(output / (name + '-verification.json'), context)
        checked, controls = verify(**context), negative_controls(**context)
    finally:
        current, = json.loads(lab.docker('inspect', old['Id']).stdout)
        preserved = dict(control=rows(preparing.DATABASE) == original, files=preparing.census(store) == files,
            inputs=inputs_at(store) == retired['inputs'], contents=recovery.contents_at(store) == contents,
            catalog=guards(pg, preparing.DATABASE) == catalog,
            artifacts=all(preparing.digest(basis / n) == sha for n, sha in FROZEN.items()),
            client=current['State'] == old['State'] and current['Config']['Labels'][retained['label']] == preparing.OWNER)
        lab.write_json(output / (name + '-source-preserved.json'), preserved)
        assert all(preserved.values()), preserved
    return dict(name='installed-activated-retirement-unresolved', status='PASS', finite_seed_invocations=1,
        helper_sha256={n: preparing.digest(Path(__file__).with_name(n)) for n in
            ('activated_source_closure_client.py', 'activated_acquisition_client.py')},
        cli_invocations=2, cases=[dict(name=name, status='PASS', oracle=checked, negative_controls=controls,
            verification_sha256=preparing.digest(output / (name + '-verification.json')))], source_preserved=preserved,
        scope='One genuine live activating-Control zero-request acquisition claim; source closure then named retirement veto',
        not_run=['socket execution, Event admission, coverage or abandonment', 'import closure, ACK/GC, epoch3, Archive and full M1'])
