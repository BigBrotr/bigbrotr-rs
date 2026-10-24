"""Own one real final-job lock schedule without changing job or lease rows.

The finite probe owns native continuously. This separate fixture backend holds
only the genuine cancelled gap job, after the probe's final positive proof.
"""
from datetime import datetime, timezone
import json
import os
import re
import time

import transition
from activated_retirement_expiry import block_observation, holder_excluded, lock_statement, wait_event
from concurrency_cases import Session
from initial_empty_locks import Probe, session_identity

history, schema, lab = transition.history, transition.schema, transition.lab


def closed_and_joined(response, schedule, *, compacted=False):
    """Auxiliary exclusion; transition.external separately checks native telemetry."""
    try:
        if compacted and (not isinstance(schedule,dict) or schedule.get('status')!='CAPTURED'
                or schedule.get('cleanup_errors')!=[] or 'schedule_error' in schedule):
            return False
        return (isinstance(schedule, dict) and (holder_excluded(schedule) or (compacted and schedule['mode']=='gate' and schedule['holder'] is None))
            and response.get('outcome') == 'PROCESS_EXITED' and response.get('returncode') == 0
            and response.get('readers_joined') is True and response.get('overflow') is False
            and response.get('stderr') == '')
    except (AttributeError, KeyError, TypeError):
        return False


def open_gate(path):
    # The probe accepts only empty regular nonsymlink files. No retry may reuse
    # an old gate or replace an operator-provided path.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)


def invoke(pg, client, output, case, mode, *, compacted=False, drift=None):
    assert type(compacted) is bool and (drift is None or (compacted and mode=='gate' and callable(drift)))
    assert mode in (('cancel','expiry','gate') if compacted else ('cancel','expiry')) and (compacted or case['proxy_port'] is None)
    name, database = case['name'], case['database']
    gate = output / (name + '-gate')
    assert not gate.with_suffix('.go').exists() and not gate.with_suffix('.go').is_symlink()
    assert not gate.with_suffix('.cancel').exists() and not gate.with_suffix('.cancel').is_symlink()
    response = dict(returncode=None, stdout='', stderr='', outcome='UNKNOWN')
    window = {}
    schedule = dict(mode=mode, status='INCOMPLETE', gates=[], blocks=[], confirmed_blocks=[],
        expiry_observations=[], cleanup_errors=[], holder=None)
    sessions, probe = [], None
    started = time.monotonic()

    def save():
        lab.write_json(output / (name + '-schedule.json'), schedule)
        lab.write_json(output / (name + '-clock.json'), window)

    def owned():
        observed, = json.loads(lab.docker('inspect', pg['postgres']).stdout)
        assert observed['State']['Running'] and observed['Config']['Labels'][lab.LABEL] == pg['owner']

    try:
        window['before_us'] = history.server_clock(pg, database)
        save()
        arguments=([case['operation'],database,'/run/secrets/application',case['config'],'/lab/'+gate.name] if compacted else
            ['successor-lineage-retire-wait',database,'/run/secrets/application',case['config'],str(case['metadata_bytes']),mode,'/lab/'+gate.name])
        response['command']=['docker','exec',client,'/build/ingest_bootstrap_probe',*arguments]
        probe = Probe(response['command'])
        gate_event='retirement-expiry-gate' if compacted else 'lineage-retirement-wait-gate'
        clock_event='retirement-compacted-clock' if compacted else 'lineage-retirement-wait-clock'
        ready = wait_event(probe,gate_event,phase='waiting' if compacted else 'ready')
        refreshed = wait_event(probe,clock_event,stage='proof' if compacted else 'refreshed')
        backend, = [e for e in probe.parsed if e.get('event') == ('successor-backend' if compacted else 'lineage-history-backend') and e['present']]
        captured, = [e for e in probe.parsed if e.get('event') == ('successor-state' if compacted else 'lineage-history-state')
            and e['stage'] == ('retirement-expiry-final-refreshed' if compacted else 'retirement-finish-returned')]
        expected = {table: json.loads(value) for table, value in captured['state']['control_tables']}
        control, = [r for r in expected['owner_epochs'] if r['role'] == 'control' and r['state'] == 'active']
        schedule.update(control_backend=backend, control=control, refreshed=refreshed,
            before_holder=schema.rows(pg, database))
        schedule['gates'].append(dict(event=ready, observed_us=history.server_clock(pg, database)))
        save()
        assert transition.normalized(schedule['before_holder']) == transition.normalized(expected)
        assert ready['prefix'] == '/lab/' + gate.name and ready['mode'] == mode
        assert refreshed['error'] is None and refreshed['lease_expires_at_us' if compacted else 'control_deadline_us'] == control['lease_expires_at_us']
        if mode=='gate':
            assert drift is not None
            schedule['mutation']=drift(expected)
            schedule['held']=schema.rows(pg,database); save()
        else:
            gap, = [g for g in expected['acquisition_gaps'] if g['producer_epoch'] == (2 if compacted else 3)]
            job, = [j for j in expected['jobs'] if j['job_id'] == gap['job_id']]
            assert job['request_key'] == '\\x' + 'a6' * 16 and job['state'] == 'cancelled'
            assert job['job_revision'] == 4 and job['checkpoint_revision'] == 0
            holder = Session(pg, database, name.replace('-', '_') + '_holder', sessions, role='bigbrotr_control')
            schedule['holder'] = dict(backend=holder.backend, identity=session_identity(holder))
            schedule['holder']['settings'] = holder.execute(
                "SELECT json_build_object('statement_timeout',current_setting('statement_timeout'),"
                "'lock_timeout',current_setting('lock_timeout'),'idle_timeout',current_setting('idle_in_transaction_session_timeout'));")
            schedule['holder']['begin'] = holder.execute('BEGIN ISOLATION LEVEL READ COMMITTED;')
            statement = lock_statement(job['job_id'])
            schedule['holder']['statement'] = statement
            schedule['holder']['lock'] = holder.execute(statement, rows=1)
            schedule['held'] = schema.rows(pg, database)
            save()
            assert transition.normalized(schedule['held']) == transition.normalized(expected)
        schedule['go_before_us'] = history.server_clock(pg, database)
        open_gate(gate.with_suffix('.go'))
        schedule['gates'].append(dict(event=wait_event(probe,gate_event,phase='released'),
            observed_us=history.server_clock(pg, database)))
        schedule['dispatch'] = wait_event(probe,clock_event,stage='dispatch',timeout=24)
        if mode!='gate':
            schedule['blocks'] = block_observation(pg, database, backend, holder.backend, timeout=2)
            save()
            if mode == 'cancel':
                schedule['cancel_before_us'] = history.server_clock(pg, database)
                open_gate(gate.with_suffix('.cancel'))
                schedule['cancel'] = wait_event(probe,'retirement-compacted-cancel' if compacted else 'lineage-retirement-wait-cancel',phase='applied',timeout=1)
                schedule['cancel_after_us'] = history.server_clock(pg, database)
            else:
                deadline = time.monotonic() + 3
                while True:
                    now = history.server_clock(pg, database)
                    schedule['expiry_observations'].append(dict(server_us=now,
                        remaining_us=control['lease_expires_at_us'] - now))
                    save()
                    if now >= control['lease_expires_at_us'] + 50_000:
                        break
                    assert time.monotonic() < deadline, 'real UTC expiry not observed inside lock margin'
                    time.sleep(.02)
            # The second sample establishes that the same typed call remained
            # blocked after cancellation/expiry, before releasing the real holder.
            schedule['confirmed_blocks'] = block_observation(pg, database, backend, holder.backend, timeout=1)
            schedule['release_before_us'] = history.server_clock(pg, database)
            save()
            schedule['holder']['rollback'] = holder.execute('ROLLBACK;')
            schedule['release_after_us'] = history.server_clock(pg, database)
            save()
        code = probe.wait(12)
        probe.observe()
        schedule['status'] = 'CAPTURED'
        assert code == 0 and not probe.stderr
    except BaseException as error:
        schedule['schedule_error'] = repr(error)
    finally:
        for session in reversed(sessions):
            try:
                session.close(owned)
                physical = session.backend
                query = ("SET max_parallel_workers_per_gather=0; SELECT count(*) FROM pg_stat_activity WHERE pid=" + str(physical['pid'])
                    + " AND backend_start='" + physical['backend_start'] + "'::timestamptz"
                    + " AND datname='" + database + "';")
                absent = lab.sql(pg, 'postgres', query)
                if schedule['holder'] is not None:
                    schedule['holder']['closed'] = dict(wrapper_exit=session.process.returncode,
                        backend=physical, statement=query, observation=absent)
                assert absent == '0'
            except BaseException as error:
                schedule['cleanup_errors'].append(dict(stage='holder', error=repr(error)))
        if probe is not None:
            # Capture terminal status before stopping a wrapper. A forced local
            # stop never changes UNKNOWN into positive remote native custody.
            code = probe.process.poll()
            if code is not None:
                for thread in probe.threads:
                    thread.join(1)
                response.update(outcome='PROCESS_EXITED', returncode=code,
                    readers_joined=not any(t.is_alive() for t in probe.threads), overflow=probe.overflow)
            response.update(stdout=bytes(probe.stdout).decode(errors='replace'),
                stderr=bytes(probe.stderr).decode(errors='replace'))
            lab.write_json(output / (name + '-response.json'), response)
            try:
                probe.stop()
            except BaseException as error:
                schedule['cleanup_errors'].append(dict(stage='probe-stop', error=repr(error)))
        response['seconds'] = time.monotonic() - started
        try:
            window['after_us'] = history.server_clock(pg, database)
        except BaseException as error:
            response['clock_error'] = repr(error)
        lab.write_json(output / (name + '-response.json'), response)
        save()
    return response, window, schedule


def diagnostic(pg, output, name, schedule, window, mode, *, compacted=False):
    result = None
    try:
        if mode == 'expiry':
            backend = schedule['control_backend']
            since, until = [datetime.fromtimestamp(window[k] / 1_000_000, timezone.utc).isoformat()
                for k in ('before_us', 'after_us')]
            answer = lab.docker('logs', '--timestamps', '--since', since, '--until', until,
                pg['postgres'], check=False, timeout=5)
            raw = answer.stdout + answer.stderr
            result = dict(backend=backend, since=since, until=until, returncode=answer.returncode,
                stdout=answer.stdout, stderr=answer.stderr)
            assert len(raw.encode()) <= 256 * 1024, 'server diagnostic exceeds bound'
            kind='activated' if compacted else 'lineage'
            pattern = rf'\[{backend["pid"]}\] ERROR:  (?:(?P<code>[0-9A-Z]{{5}}): )?{kind} retirement Control authority expired$'
            errors = [line for line in raw.splitlines() if re.search(pattern, line)]
            contexts = [line for line in raw.splitlines() if f'[{backend["pid"]}] CONTEXT:  ' in line
                and ('function control.lock_activated_initial_retirement(bytea)' if compacted else 'function control.require_ingest_successor(bytea,numeric,text)') in line]
            result.update(errors=errors, contexts=contexts,
                observed_sqlstate=re.search(pattern, errors[0]).group('code') if len(errors) == 1 else None)
        return result
    except BaseException as error:
        if result is None:
            result = {}
        result['capture_error'] = repr(error)
        raise
    finally:
        lab.write_json(output / (name + '-diagnostic.json'), result)
