"""A genuine retained preparer claim, followed by config-only retirement refusal.

One frozen retired-source clone and two actual processes. The application-role
claim is committed while its preparing Control is still alive; no history is
revived, rewritten or attributed to the later recovery process.
"""
import json
import shutil
import time

import lab
from concurrency_cases import Session
import initial_succession as preparing
from initial_empty import upgrade
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_empty_locks import Probe, claim_guard, guards, literal, session_identity
from initial_empty_preparer_oracle import verify, negative_controls

CASES = ('empty-preparer-claim',)
JOB, ACTOR, REQUEST, DIGEST = 'e6' * 16, 'e7' * 16, 'e8' * 16, 'e9' * 32


def statements(control, created):
    return ['BEGIN ISOLATION LEVEL READ COMMITTED;',
        "INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,"
        "kind,config_revision,policy_revision,created_at_us) SELECT "
        f"decode('{JOB}','hex'),dataset_id,decode('{ACTOR}','hex'),decode('{REQUEST}','hex'),"
        f"decode('{DIGEST}','hex'),'import',config_revision,policy_revision,{created} "
        "FROM control.datasets RETURNING to_jsonb(jobs)||jsonb_build_object('xmin',xmin::text);",
        f"UPDATE control.jobs j SET input_complete=true WHERE job_id=decode('{JOB}','hex') "
        "RETURNING to_jsonb(j)||jsonb_build_object('xmin',xmin::text);",
        "UPDATE control.jobs j SET state='queued',job_revision=2,"
        f"owner_resource_id={literal(control['resource_id'])},owner_epoch={control['epoch']},"
        f"owner_incarnation={literal(control['process_incarnation'])} "
        f"WHERE job_id=decode('{JOB}','hex') AND state='accepted' AND job_revision=1 "
        "RETURNING to_jsonb(j)||jsonb_build_object('xmin',xmin::text);",
        'COMMIT;']


def wait_gate(probe):
    deadline = time.monotonic() + 10
    while True:
        probe.observe()
        found = [e for e in probe.parsed if e.get('event') == 'successor-preparer-gate'
            and e.get('gate') == 'preparer-claim' and e.get('phase') == 'waiting']
        if found:
            assert len(found) == 1
            return
        assert probe.process.poll() is None and time.monotonic() < deadline, 'preparer gate unobserved'
        with probe.condition:
            probe.condition.wait(.02)


def run(pg, client, output, rows, clone):
    name, = CASES
    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    assert all(preparing.digest(basis / n) == h for n, h in preparing.FROZEN.items())
    retained = json.loads((basis / 'result.json').read_text())
    assert retained['status'] == 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE'
    assert retained['source'] == preparing.SOURCE and retained['owner'] == preparing.OWNER
    assert not retained['cleanup_errors'] and not retained['unrelated_changed'] and not retained['missing_owned']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [c for c in containers if c['Id'] == retained['client']['id']]
    assert old['Config']['Labels'][retained['label']] == preparing.OWNER
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '')
        for m in c['Mounts']) for c in containers)
    original = rows(preparing.DATABASE)
    assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
    native = json.loads((basis / 'retire-signed-retry-native.json').read_text())
    files, inputs = preparing.census(store), inputs_at(store)
    assert len(original) == 70 and len(native['rows']) == 49 and len(inputs) == 4
    assert sum(v['bytes'] for v in files.values()) < 1024**2
    seed = dict(control=original, native=native, inputs=inputs, sequence=58)
    absence = observe_backend_absence(pg, preparing.DATABASE, output, 'preparer-source')
    lab.write_json(output / 'preparer-source.json', dict(**seed, files=files,
        frozen=preparing.FROZEN, backend_absence=absence))
    pg_id = json.loads(lab.docker('inspect', pg['postgres']).stdout)[0]['Id']

    def owned():
        item, = json.loads(lab.docker('inspect', pg_id).stdout)
        assert item['State']['Running'] and item['Config']['Labels'][lab.LABEL] == pg['owner']

    def dump(label, directory):
        response, _ = capture(client, output, label + '-dump', 'dump', '/lab/' + directory.name + '/store')
        assert response.returncode == 0 and not response.stderr
        (output / (label + '-native.json')).write_text(response.stdout)
        return json.loads(response.stdout)

    def external(label, database, directory, window, code):
        if code is None:
            lab.write_json(output / (label + '-external-unavailable.json'), dict(
                status='NOT_RUN_UNKNOWN_PROCESS_OUTCOME',
                reason='Native directory may still be owned; outer owner-checked cleanup is required'))
            return None
        window['after_us'] = server_clock(pg, database)
        lab.write_json(output / (label + '-clock.json'), window)
        after = rows(database)
        lab.write_json(output / (label + '-after.json'), after)
        present = inputs_at(directory / 'store')
        lab.write_json(output / (label + '-after-inputs.json'), present)
        actual_guards = guards(pg, database)
        lab.write_json(output / (label + '-after-guards.json'), actual_guards)
        actual_native = dump(label + '-after', directory)
        return dict(control=after, native=actual_native, inputs=present, guards=actual_guards)

    try:
        database = clone(name, preparing.DATABASE)
        directory = output / name
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        assert dump(name + '-seed', directory) == native and inputs_at(directory / 'store') == inputs
        config = configuration(pg, client, output, database, directory, original)
        preparing.upgrade(pg, client, output, database, config, name + '-schema30', rows)
        upgrade(pg, client, output, database, config, name + '-schema31', rows)
        before = rows(database)
        label = name + '-prepare'
        lab.write_json(output / (label + '-before.json'), before)
        prepared, after, preparation_absence, seconds = prepare(pg, client, output, database,
            directory, config, label, before, native, inputs, rows, owned, external)
        # No proof/plan is transferred: the second executable receives only the
        # same configured durable stores after positive process/backend closure.
        recovery_label = name + '-recovery'
        recovered, after_recovery, window, recovery_absence, elapsed = recover(pg, client,
            output, database, directory, config, recovery_label, rows, external, prepared)
        verification = dict(events=recovered, original=after['control'], native=after['native'],
            inputs=after['inputs'], mode=name, seed=seed, server_window=window, preparation=prepared)
        lab.write_json(output / (name + '-verification.json'), verification)
        checked = verify(**verification)
        controls = negative_controls(**verification)
        final, = [e for e in recovered if e.get('event') == 'successor-state' and e.get('stage') == 'empty-final']
        assert after_recovery['control'] == {t: json.loads(v) for t, v in final['state']['control_tables']}
        assert after_recovery['native'] == dict(after['native'], rows=final['rows']) == after['native']
        assert after_recovery['inputs'] == after['inputs'] == inputs
        assert after_recovery['guards'] == after['guards'] == prepared['claim']['guards_before']
        case = dict(name=name, status='PASS', oracle=checked, negative_controls=controls,
            preparation_seconds=seconds, recovery_seconds=elapsed,
            preparation_backend_absence=preparation_absence, recovery_backend_absence=recovery_absence,
            verification_sha256=preparing.digest(output / (name + '-verification.json')))
        lab.write_json(output / 'preparer-cases.json', [case])
    finally:
        preserved = dict(control=rows(preparing.DATABASE) == original, files=preparing.census(store) == files,
            artifacts=all(preparing.digest(basis / n) == h for n, h in preparing.FROZEN.items()))
        lab.write_json(output / 'preparer-source-preserved.json', preserved)
        assert all(preserved.values()), preserved
    return dict(name='initial-empty-preparer', status='PASS', cases=[case],
        source_lab=str(basis.parent), source_revision=preparing.SOURCE, source_owner=preparing.OWNER,
        source_preserved=preserved, source_artifacts=preparing.FROZEN,
        scope='live preparing Control8 generic import claim retained through config-only Control9 refusal',
        excluded='installed import execution, writer activation, native syscall/power loss, installed restart/resend and full M1')


def prepare(pg, client, output, database, directory, config, label, before, native, inputs,
            rows, owned, external):
    sessions, probe = [], None
    started = time.monotonic()
    window = dict(before_us=server_clock(pg, database))
    prefix = output / (label + '-gate')
    assert not prefix.with_suffix('.go').exists()
    claim = dict(job_id='\\x' + JOB, principal_id='\\x' + ACTOR, request_key='\\x' + REQUEST,
        request_digest='\\x' + DIGEST, statement=[], transactions=[], gates=[], status='INCOMPLETE')

    def save():
        lab.write_json(output / (label + '-claim.json'), claim)
        lab.write_json(output / (label + '-clock.json'), window)

    try:
        session = Session(pg, database, label + '_claimant', sessions, role='bigbrotr_control')
        claim['backend'], claim['identity'] = session.backend, session_identity(session)
        save()
        probe = Probe(['docker', 'exec', client, '/build/ingest_bootstrap_probe',
            'successor-preparer-claim', database, '/run/secrets/application', config, '/lab/' + prefix.name])
        wait_gate(probe)
        claim['gates'].append(dict(gate='preparer-claim', phase='waiting', server_us=server_clock(pg, database)))
        backend, = [e for e in probe.parsed if e['event'] == 'successor-backend' and e['present']]
        state, = [e for e in probe.parsed if e['event'] == 'successor-state' and e['stage'] == 'same-control-retry']
        claim['control_backend'] = backend
        claim['before'] = rows(database)
        claim['guards_before'] = guards(pg, database)
        assert claim['before'] == {t: json.loads(v) for t, v in state['state']['control_tables']}
        control, = [r for r in claim['before']['owner_epochs'] if r['role'] == 'control' and r['state'] == 'active']
        claim['control'] = control
        assert control['epoch'] == backend['control_epoch'] == 8 and backend['database'] == database
        assert not any(r['job_id'] == '\\x' + JOB for r in claim['before']['jobs'])
        claim['server_window'] = dict(before_us=server_clock(pg, database))
        created = server_clock(pg, database)
        claim['created_at_us'] = created
        assert created < control['lease_expires_at_us']
        claim['statement'] = statements(control, created)
        save()
        for statement in claim['statement']:
            claim['transactions'].append(session.execute(statement))
            save()
        claim['server_window']['after_us'] = server_clock(pg, database)
        claim['after'] = rows(database)
        claim['guards_after'] = guards(pg, database)
        save()
        assert claim['guards_after'] == claim['guards_before']
        assert claim['server_window']['after_us'] < control['lease_expires_at_us']
        released = server_clock(pg, database)
        with prefix.with_suffix('.go').open('x') as stream:
            stream.write('continue\n')
        claim['gates'].append(dict(gate='preparer-claim', phase='released', server_us=released))
        claim['status'] = 'CAPTURED'
        save()
        code = probe.wait(12)
        probe.observe()
        assert code == 0 and not probe.stderr
    except BaseException as error:
        claim['failure'] = type(error).__name__
        raise
    finally:
        code = None if probe is None else probe.process.poll()
        if probe is not None and code is not None:
            for thread in probe.threads:
                thread.join(1)
        lab.write_json(output / (label + '-response.json'), dict(returncode=code,
            stdout='' if probe is None else bytes(probe.stdout).decode(errors='replace'),
            stderr='' if probe is None else bytes(probe.stderr).decode(errors='replace'),
            seconds=time.monotonic() - started,
            outcome='UNKNOWN_PROCESS_OUTCOME' if code is None else 'PROCESS_EXITED'))
        try:
            after = external(label, database, directory, window, code)
        finally:
            errors = []
            if probe:
                try:
                    probe.stop()
                except BaseException as error:
                    errors.append('probe-' + type(error).__name__)
            for session in reversed(sessions):
                try:
                    session.close(owned)
                except BaseException as error:
                    errors.append(type(error).__name__)
            claim['cleanup_errors'] = errors
            save()
            assert not errors, errors
    absence = observe_backend_absence(pg, database, output, label)
    events = probe.parsed
    backend, = [e for e in events if e['event'] == 'successor-backend' and e['present']]
    absent, = [e for e in events if e['event'] == 'successor-backend' and not e['present']]
    assert absent == dict(backend, present=False)
    completed, = [e for e in events if e['event'] == 'successor-complete']
    assert completed == dict(event='successor-complete', mode='successor-preparer-claim',
        native='closed', control='released-and-joined', admission=False)
    final, = [e for e in events if e['event'] == 'successor-state' and e['stage'] == 'final']
    assert after['control'] == {t: json.loads(v) for t, v in final['state']['control_tables']}
    assert after['native'] == dict(native, rows=final['rows']) and after['inputs'] == inputs
    assert final['state']['sequence'] == 62 and len(final['rows']) == 52
    preparation = dict(events=events, original=before, native=native, inputs=inputs,
        server_window=window, claim=claim)
    lab.write_json(output / (label + '-verification.json'), preparation)
    return preparation, after, absence, time.monotonic() - started


def recover(pg, client, output, database, directory, config, label, rows, external, preparation):
    before = rows(database)
    lab.write_json(output / (label + '-before.json'), before)
    window = dict(before_us=server_clock(pg, database))
    lab.write_json(output / (label + '-clock.json'), window)
    try:
        response, seconds = capture(client, output, label, 'empty-retire', database,
            '/run/secrets/application', config)
    finally:
        path = output / (label + '-response.json')
        code = json.loads(path.read_text())['returncode'] if path.exists() else None
        after = external(label, database, directory, window, code)
    assert response.returncode == 0 and not response.stderr
    events = [json.loads(line) for line in response.stdout.splitlines()]
    backend, = [e for e in events if e['event'] == 'successor-backend' and e['present']]
    preparation['claim_guard'] = claim_guard(pg, backend, window)
    lab.write_json(output / (label + '-claim-guard.json'), preparation['claim_guard'])
    absence = observe_backend_absence(pg, database, output, label)
    return events, after, window, absence, seconds
