"""A real live-activator import claim survives source/acquisition completion.

One frozen f12 copy, one installed Serve journey and three configuration-only
recovery processes. Ordinary guarded SQL queues the import while Control8 is
alive. No import execution, producer binding or historical claim is invented.
"""
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil

import lab
from concurrency_cases import Session
import activated_acquisition as acquisition
import activated_recovery as recovery
import activated_retirement as retirement
import activated_source_closure as source
import installed_live as live
import initial_succession as preparing
from activated_recovery_zero import invoke
from activated_retirement_import_oracle import CASES, verify, negative_controls
from initial_empty_preparer import statements
from initial_empty_locks import guards, session_identity
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_live import exact_absence

FROZEN = dict(preparing.FROZEN, **{'signed-seed/event.jsonl':
    'c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae'})


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


def claim_import(pg, output, rows, database, directory, initialized):
    """The registered callback runs before the HTTP client, under live Control8."""
    label = CASES[0] + '-claim'
    assert initialized['stage'] == 'activated'
    assert not any(r['producer_epoch'] == 2 for r in initialized['control']['producer_requests'])
    sessions = []
    value = dict(status='INCOMPLETE', transactions=[], cleanup_errors=[],
        before=rows(database), inputs_before=inputs_at(directory / 'store'), guards_before=guards(pg, database))
    control, = [r for r in value['before']['owner_epochs'] if r['role'] == 'control' and r['state'] == 'active']
    assert control['epoch'] == 8
    value['control'] = control
    pg_id, = json.loads(lab.docker('inspect', pg['postgres']).stdout)

    def owned():
        item, = json.loads(lab.docker('inspect', pg['postgres']).stdout)
        assert item['Id'] == pg_id['Id'] and item['State']['Running']
        assert item['Config']['Labels'][lab.LABEL] == pg['owner']

    def save():
        lab.write_json(output / (label + '.json'), value)

    def observe():
        return json.loads(lab.sql(pg, database, "SELECT json_build_object('pid',pid,"
            "'backend_start',backend_start::text,'database',datname,'user',usename) "
            f"FROM pg_stat_activity WHERE pid={control['control_backend_pid']} "
            f"AND backend_start='{control['control_backend_start']}'::timestamptz AND datname='{database}'"))

    save()
    try:
        value['server_window'] = dict(before_us=server_clock(pg, database))
        value['live_before'] = observe()
        actor = Session(pg, database, 'retirement_import_claim', sessions, role='bigbrotr_control')
        value['backend'] = actor.backend
        value['identity'] = session_identity(actor)
        value['created_at_us'] = server_clock(pg, database)
        value['statements'] = statements(control, value['created_at_us'])
        save()
        for statement in value['statements']:
            actor.start(statement)
            answer = actor.finish()
            value['transactions'].append(answer)
            save()
            Session.expect(answer)
        value['server_window']['after_us'] = server_clock(pg, database)
        value['after'] = rows(database)
        value['observed_by_us'] = server_clock(pg, database)
        value['live_after'] = observe()
        value['inputs_after'] = inputs_at(directory / 'store')
        value['guards_after'] = guards(pg, database)
        value['status'] = 'CAPTURED'
    except BaseException as error:
        value['failure'] = type(error).__name__
        raise
    finally:
        for session in reversed(sessions):
            try:
                session.close(owned)
                b = session.backend
                value['actor_absence'] = lab.sql(pg, database, 'SELECT count(*) FROM pg_stat_activity WHERE '
                    f"pid={b['pid']} AND backend_start='{b['backend_start']}'::timestamptz AND datname='{database}';")
                value['actor_exit_code'] = session.process.returncode
            except BaseException as error:
                value['cleanup_errors'].append(type(error).__name__)
        save()
    assert not value['cleanup_errors']
    return value


def diagnostic(pg, expected, clock):
    since, until = [datetime.fromtimestamp(clock[k] / 1_000_000, timezone.utc).isoformat()
        for k in ('before_us', 'after_us')]
    answer = lab.docker('logs', '--timestamps', '--since', since, '--until', until,
        pg['postgres'], check=False, timeout=5)
    raw = answer.stdout + answer.stderr
    assert len(raw.encode()) <= 256 * 1024
    pattern = rf'\[{expected["pid"]}\] ERROR:  (?:(?P<sqlstate>[0-9A-Z]{{5}}): )?activated producer import claims remain unresolved$'
    errors = [line for line in raw.splitlines() if re.search(pattern, line)]
    contexts = [line for line in raw.splitlines() if f'[{expected["pid"]}] CONTEXT:  ' in line
        and re.search(r'function (?:control\.)?lock_activated_initial_retirement\(bytea\)', line)]
    return dict(backend=expected, since=since, until=until, returncode=answer.returncode,
        errors=errors, contexts=contexts, observed_sqlstate=(re.search(pattern, errors[0]).group('sqlstate')
            if len(errors) == 1 else None))


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
    absent = observe_backend_absence(pg, preparing.DATABASE, output, name + '-original')
    original = rows(preparing.DATABASE)
    assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
    seed = dict(control=original, native=json.loads((basis / 'retire-signed-retry-native.json').read_text()),
        inputs=inputs_at(store), sequence=58)
    files = preparing.census(store)
    original_catalog = guards(pg, preparing.DATABASE)
    lab.write_json(output / (name + '-original.json'), dict(seed=seed, files=files,
        frozen=FROZEN, catalog=original_catalog, backend_absence=absent, client_state=old['State']))
    root = Path(__file__).resolve().parents[2]
    helpers = (*live.HELPERS, 'activated_source_closure_client.py', 'activated_acquisition_client.py')
    for filename in helpers:
        helper, target = Path(__file__).with_name(filename), output / filename
        if not target.exists():
            shutil.copy2(helper, target)
        assert preparing.digest(target) == preparing.digest(helper)
    attempts = []
    try:
        database = clone(name, preparing.DATABASE)
        assert rows(database) == original
        # The unchanged installed journey owns these exact path/command labels.
        directory = output / 'installed-live-off'
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        shutil.copy2(basis / 'signed-seed/event.jsonl', directory / 'event.jsonl')
        shutil.copy2(root / 'tests/fixtures/acquisition/selector-events.json', directory / 'selector-events.json')
        config = live.configure(pg, client, output, database, directory, original)
        live.upgrade(pg, client, output, database, config, name + '-32', rows)
        policy = live.policy(pg, database, output, name, rows, False)
        try:
            preparation = live.capture_case(pg, client, output, rows, 'installed-live-off', database,
                config, directory, seed, policy, positive=True,
                after_activated=lambda initialized: claim_import(pg, output, rows, database, directory, initialized))
        except BaseException:
            # The shared owner attempts exact shutdown. Preserve provisional
            # SQL/input evidence even if one of its final observations failed;
            # no competing native open is introduced by this failure path.
            uncertain(output, name + '-serve', database, directory, rows,
                'Serve capture incomplete; use retained shared custody receipt, native NOT_RUN here')
            raise
        # Recovery uses its own ordinary20s owner config; Serve's six-second
        # renewal envelope remains retained in the complete preparation context.
        serve_configuration = dict(path=config, text=(output / Path(config).name).read_text())
        config = configuration(pg, client, output, database, directory, original)
        setup = dict(serve_configuration=serve_configuration,
            configuration=dict(path=config, text=(output / Path(config).name).read_text()),
            recovery=recovery.upgrade(pg, client, output, database, config, name + '-34', rows))
        setup['source'] = source.upgrade(pg, client, output, database, config, name + '-35', rows)
        setup['source_grant'] = source.grant_source(pg, database, output, name)
        setup['acquisition'] = acquisition.upgrade(pg, client, output, database, config, name + '-36', rows)
        setup['retirement'] = retirement.upgrade(pg, client, output, database, config, name + '-37', rows)
        lab.write_json(output / (name + '-setup.json'), setup)
        for phase in ('source', 'acquisition', 'retirement'):
            mode = name + '-' + phase
            observe_backend_absence(pg, database, output, mode + '-before')
            before = state(client, output, mode + '-before', database, directory, rows)
            catalog_before = retirement.catalog(pg, database)
            helper = Path(__file__).with_name('activated_source_closure_client.py' if phase == 'source'
                else 'activated_acquisition_client.py')
            response, clock = invoke(pg, client, output, database, config, mode, helper,
                action='--retire-activated-initial' if phase == 'retirement' else None)
            if response['outcome'] != 'PROCESS_EXITED':
                uncertain(output, mode, database, directory, rows, 'CLI outcome unknown; native custody not excluded')
                raise AssertionError('unknown installed process outcome')
            try:
                absent = observe_backend_absence(pg, database, output, mode)
            except BaseException as error:
                uncertain(output, mode, database, directory, rows, 'Backend absence unavailable: ' + type(error).__name__)
                raise
            after = state(client, output, mode + '-after', database, directory, rows)
            catalog_after = retirement.catalog(pg, database)
            lab.write_json(output / (mode + '-external.json'), dict(**after, catalog=catalog_after, absence=absent))
            exact = exact_absence(pg, database, output, mode, after['control'], before['control'])
            item = dict(mode=mode, phase=phase, before=before, after=after, response=response,
                server_window=clock, backend=dict(exact=exact, absence=absent),
                catalog_before=catalog_before, catalog_after=catalog_after, diagnostic=None)
            attempts.append(item)
            lab.write_json(output / (name + '-attempts.json'), attempts)
            if phase == 'retirement':
                item['diagnostic'] = diagnostic(pg, exact['expected'], clock)
            lab.write_json(output / (name + '-attempts.json'), attempts)
        context = dict(preparation=preparation, setup=setup, attempts=attempts)
        lab.write_json(output / (name + '-verification.json'), context)
        checked, controls = verify(**context), negative_controls(**context)
    finally:
        current, = json.loads(lab.docker('inspect', old['Id']).stdout)
        preserved = dict(control=rows(preparing.DATABASE) == original, files=preparing.census(store) == files,
            inputs=inputs_at(store) == seed['inputs'], catalog=guards(pg, preparing.DATABASE) == original_catalog,
            artifacts=all(preparing.digest(basis / n) == sha for n, sha in FROZEN.items()),
            client=current['State'] == old['State'] and current['Config']['Labels'][retained['label']] == preparing.OWNER)
        lab.write_json(output / (name + '-source-preserved.json'), preserved)
        assert all(preserved.values()), preserved
    return dict(name='installed-activated-retirement-import', status='PASS', serve_invocations=1,
        cli_invocations=3, cases=[dict(name=name, status='PASS', oracle=checked, negative_controls=controls,
            verification_sha256=preparing.digest(output / (name + '-verification.json')))],
        source_preserved=preserved, source_revision=preparing.SOURCE, source_owner=preparing.OWNER,
        scope='Genuine live-Control import claim survives source closure and acquisition abandonment; retirement refuses',
        not_run=['import execution/closure', 'import input binding or checkpoint', 'epoch3, Archive and full M1'])
