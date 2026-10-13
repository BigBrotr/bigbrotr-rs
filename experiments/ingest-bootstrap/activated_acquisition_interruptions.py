"""Signal one installed owner during the actual final queued-job row wait.

Only an already qualified copied source is used. The holder locks one genuine
job without changing it; no fixture mutation supplies historical authority.
"""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shutil

import lab
import activated_acquisition as acquisition
import activated_recovery as recovery
from activated_acquisition_interruptions_oracle import CASES, MODE, LOCK, verify, negative_controls
from activated_recovery_zero import invoke
from activated_source_closure_locks import identity
from activated_source_prefix import CLOSED
from concurrency_cases import Session
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_live import exact_absence
from initial_succession import census, digest
from installed import wait_for
from installed_empty import signal_child


def blocked(pg, database, holder):
    """Same actual blocking relation as source_closure_locks, plus final phase anchors."""
    statement = f"""SELECT coalesce(json_agg(json_build_object(
        'pid',a.pid,'backend_start',a.backend_start,'database',a.datname,'user',a.usename,
        'query',a.query,'query_start',a.query_start,'wait_event_type',a.wait_event_type,
        'wait_event',a.wait_event,'blockers',pg_blocking_pids(a.pid),
        'observed_at_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
        'holder_pid',h.pid,'holder_start',h.backend_start,'holder_user',h.usename,
        'control',to_jsonb(o)||jsonb_build_object('xmin',o.xmin::text),
        'waiting_locks',(SELECT coalesce(json_agg(json_build_object('type',locktype,'mode',mode,
            'relation',relation::regclass::text,'transaction',transactionid::text)),'[]')
            FROM pg_locks WHERE pid=a.pid AND NOT granted))), '[]')
    FROM pg_stat_activity a JOIN pg_stat_activity h ON h.pid={holder['pid']}
        AND h.backend_start='{holder['backend_start']}'::timestamptz AND h.datname=current_database()
    JOIN control.owner_epochs o ON o.role='control' AND o.state='active'
        AND o.control_backend_pid=a.pid AND o.control_backend_start=a.backend_start
    WHERE a.datname=current_database() AND a.usename='bigbrotr_control'
        AND a.wait_event_type='Lock' AND h.pid=ANY(pg_blocking_pids(a.pid));"""
    result = json.loads(lab.sql(pg, database, statement))
    if result:
        value, = result
        return value
    return None


def interrupt(pg, client, output, database, config, helper):
    fixture = dict(kind='owned-job-row-signal', cleanup_errors=[], released=False,
        window=dict(before_us=server_clock(pg, database)),
        config=dict(path=config, text=(output / Path(config).name).read_text(),
            sha256=digest(output / Path(config).name)))
    save = lambda: lab.write_json(output / (MODE + '-fixture.json'), fixture)
    sessions, future = [], None
    response, clock = dict(outcome='OUTCOME_UNKNOWN'), {}
    save()

    def owned():
        observed, = json.loads(lab.docker('inspect', pg['postgres']).stdout)
        assert observed['State']['Running'] and observed['Config']['Labels'][lab.LABEL] == pg['owner']

    with ThreadPoolExecutor(max_workers=1) as executor:
        try:
            holder = Session(pg, database, MODE + '_holder', sessions, role='postgres')
            fixture.update(holder=holder.backend, identity=identity(holder))
            save()
            fixture['begin'] = holder.execute('BEGIN;')
            fixture['lock_statement'] = LOCK
            save()
            holder.start(LOCK)
            fixture['lock_result'] = holder.finish()
            save()
            holder.expect(fixture['lock_result'], rows=1)
            future = executor.submit(invoke, pg, client, output, database, config, MODE, helper)
            pid_file = output / (MODE + '.pid.json')
            wait_for(pid_file.exists, 'activated acquisition CLI identity', 5)
            process = json.loads(pid_file.read_text())
            fixture['blocked'] = wait_for(lambda: blocked(pg, database, holder.backend),
                'actual final acquisition job wait', 5)
            # Keep the held row while delivering SIGTERM to the actual CLI
            # incarnation. Do not spend the phase envelope on a full census.
            save()
            fixture['signal'] = dict(name='SIGTERM', identity=process, returned=False)
            save()
            signal_child(client, process)
            fixture['signal']['returned'] = True
            fixture['signal']['after_us'] = server_clock(pg, database)
            save()
            # The installed supervisor can finish while PostgreSQL still waits
            # on this row. Retain that distinction; never unlock to invent a
            # successful cancellation or infer server absence from child exit.
            response, clock = future.result(timeout=8)
            fixture['remaining_backends_before_unlock'] = json.loads(lab.sql(pg, database,
                "SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,"
                "'database',datname,'user',usename,'query',query) ORDER BY pid),'[]') "
                "FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';"))
            save()
        except Exception as error:
            fixture['schedule_error'] = type(error).__name__
            save()
        finally:
            for holder in reversed(sessions):
                try:
                    if holder.pending is not None:
                        raise AssertionError('holder command outcome unknown')
                    holder.start('ROLLBACK;')
                    fixture['rollback'] = holder.finish()
                    save()
                    holder.expect(fixture['rollback'])
                    fixture['released'] = True
                except Exception as error:
                    fixture['cleanup_errors'].append(type(error).__name__)
                finally:
                    ended = dict(backend=holder.backend, joined=False)
                    fixture['holder_end'] = ended
                    save()
                    try:
                        holder.close(owned)
                        b = holder.backend
                        absent = lab.sql(pg, 'postgres', f"SELECT count(*) FROM pg_stat_activity WHERE "
                            f"pid={b['pid']} AND backend_start='{b['backend_start']}'::timestamptz "
                            f"AND datname='{database}';")
                        ended.update(wrapper_exit=holder.process.returncode, absence=absent, joined=True)
                        assert absent == '0'
                    except Exception as error:
                        ended['error'] = type(error).__name__
                        fixture['cleanup_errors'].append(type(error).__name__)
                    save()
            if future is not None:
                try:
                    response, clock = future.result(timeout=55)
                except Exception as error:
                    fixture['invoke_error'] = type(error).__name__
                    save()
    try:
        fixture['window']['after_us'] = server_clock(pg, database)
    except Exception as error:
        fixture['clock_error'] = type(error).__name__
    save()
    return response, clock, fixture


def run_all(pg, client, output, rows, clone):
    helper = Path(__file__).with_name('activated_acquisition_client.py')
    target = output / helper.name
    if not target.exists():
        shutil.copy2(helper, target)
    assert digest(target) == digest(helper)
    spec = CLOSED
    basis = output.parent.parent / spec['run'] / 'bindings'
    store = basis / spec['prefix'] / 'store'
    frozen = {'result.json': spec['result'], spec['sql_file']: spec['sql'],
        spec['native_file']: spec['native'], spec['verification_file']: spec['verification']}
    original_source, cases = None, []

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
                detail='Backend absence unproved; available SQL/inputs remain provisional')
            for name, read in [('sql', lambda: rows(database)), ('inputs', lambda:
                    dict(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (label + '-uncertain-' + name + '.json'), read())
                except Exception as problem:
                    value[name + '_capture_error'] = type(problem).__name__
            lab.write_json(output / (label + '-external-unavailable.json'), value)
            raise

    try:
        assert all(digest(basis / name) == sha for name, sha in frozen.items())
        retained = json.loads((basis / 'result.json').read_text())
        assert all(retained[k] == spec[k] for k in ('source', 'owner', 'status'))
        assert not any(retained[k] for k in ('cleanup_errors', 'unrelated_changed', 'missing_owned'))
        containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
        old, = [c for c in containers if c['Id'] == retained['client']['id']]
        assert old['Config']['Labels'][retained['label']] == spec['owner']
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
        absent = observe_backend_absence(pg, spec['database'], output, MODE + '-original')
        original = rows(spec['database'])
        assert original == json.loads((basis / spec['sql_file']).read_text())
        seed = dict(name='off', verification=json.loads((basis / spec['verification_file']).read_text()),
            control=original, native=json.loads((basis / spec['native_file']).read_text()),
            inputs=inputs_at(store), contents=recovery.contents_at(store), catalog=acquisition.catalog(pg, spec['database']))
        original_source = dict(seed=seed, files=census(store), frozen=frozen, backend_absence=absent,
            client_id=old['Id'], client_state=old['State'], label=retained['label'])
        lab.write_json(output / (MODE + '-original.json'), original_source)
        # Keep PostgreSQL's generated database identifier below 64 bytes; the
        # full mode remains the unambiguous command/capture/storage prefix.
        database = clone(MODE.removeprefix('installed-'), spec['database'])
        assert rows(database) == seed['control']
        directory = output / MODE
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        assert state(MODE + '-seed', database, directory) == {k: seed[k] for k in ('control', 'native', 'inputs', 'contents')}
        config = configuration(pg, client, output, database, directory, original)
        setup = dict(migration_sha256=digest(Path(__file__).resolve().parents[2] /
            'migrations/postgres/0036_activated_acquisition_abandonment.sql'),
            migration=acquisition.upgrade(pg, client, output, database, config, MODE, rows))
        exclude(MODE + '-before', database, directory)
        before = state(MODE + '-before', database, directory)
        setup['catalog_before'] = acquisition.catalog(pg, database)
        response, clock, fixture = interrupt(pg, client, output, database, config, helper)
        ended = fixture.get('holder_end', {})
        if (response.get('outcome') != 'PROCESS_EXITED' or ended.get('joined') is not True
                or ended.get('absence') != '0'):
            lab.write_json(output / (MODE + '-external-unavailable.json'), dict(status='OUTCOME_UNKNOWN',
                native='NOT_RUN', reason='CLI or fixture holder custody unknown; outer owner must join'))
            # An unresolved process may still mutate SQL. Preserve what can be
            # observed without presenting it as a final native/SQL state.
            for name, read in [('sql', lambda: rows(database)), ('inputs', lambda:
                    dict(inputs=inputs_at(directory / 'store'), contents=recovery.contents_at(directory / 'store')))]:
                try:
                    lab.write_json(output / (MODE + '-uncertain-' + name + '.json'), read())
                except Exception as error:
                    lab.write_json(output / (MODE + '-uncertain-' + name + '-error.json'), dict(error=type(error).__name__))
            raise AssertionError('activated acquisition CLI custody unavailable')
        absent = exclude(MODE, database, directory)
        after = state(MODE + '-after', database, directory)
        setup['catalog_after'] = acquisition.catalog(pg, database)
        lab.write_json(output / (MODE + '-external.json'), dict(**after, catalog=setup['catalog_after'], absence=absent))
        exact = exact_absence(pg, database, output, MODE, after['control'], before['control'])
        context = dict(before=before['control'], after=after['control'], native_before=before['native'],
            native_after=after['native'], inputs_before=before['inputs'], inputs_after=after['inputs'],
            input_contents=after['contents'], response=response, mode=MODE, seed=seed, setup=setup,
            server_window=clock, backend=dict(exact=exact, absence=absent), fixture=fixture)
        lab.write_json(output / (MODE + '-verification.json'), context)
        evidence, controls = verify(**context), negative_controls(**context)
        cases.append(dict(name=MODE, status='PASS', oracle=evidence, negative_controls=controls,
            verification_sha256=digest(output / (MODE + '-verification.json'))))
        lab.write_json(output / 'activated-acquisition-interruption-cases.json', cases)
    finally:
        if original_source is not None:
            old, = json.loads(lab.docker('inspect', original_source['client_id']).stdout)
            preserved = dict(control=rows(spec['database']) == original_source['seed']['control'],
                files=census(store) == original_source['files'], inputs=inputs_at(store) == original_source['seed']['inputs'],
                contents=recovery.contents_at(store) == original_source['seed']['contents'],
                catalog=acquisition.catalog(pg, spec['database']) == original_source['seed']['catalog'],
                client=old['State'] == original_source['client_state'] and old['Config']['Labels'][original_source['label']] == spec['owner'],
                artifacts=all(digest(basis / name) == sha for name, sha in frozen.items()))
            lab.write_json(output / 'activated-acquisition-interruption-source-preserved.json', preserved)
            assert all(preserved.values()), preserved
    assert [c['name'] for c in cases] == list(CASES)
    return dict(name='installed-activated-acquisition-interruptions', status='PASS', cli_invocations=1,
        cases=cases, source_preserved=preserved, helper_sha256=digest(helper),
        scope='SIGTERM during exact final job FOR UPDATE wait, within observed one-second UTC envelope',
        not_run=['internal Rust timer telemetry', 'post-wait Control UTC expiry', 'blocked native syscall',
            'power loss', 'producer retirement', 'Archive restart and full M1'])
