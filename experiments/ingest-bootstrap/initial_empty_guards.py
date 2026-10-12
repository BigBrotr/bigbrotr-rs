"""Finite relational counterexamples for empty initial-attempt retirement.

The caller supplies an exclusively owned, migrated clone and application grants.
Every transaction creates a new, actual-backend Control epoch with ordinary
guards enabled, then rolls back or fails. No historical owner is resurrected.
These raw SQL checks neither open native storage nor attest native exclusion.
"""
import hashlib
import json
import re
import subprocess
import time

import lab
from initial_gap_policy import GUARDS, require_guards


REQUIRED = {
    ('source_streams', 'initial_empty_source_complete'),
    ('producer_epochs', 'initial_empty_producer_complete'),
    ('allocation_domains', 'initial_empty_domain_complete'),
    ('owner_epochs', 'initial_empty_owner_complete'),
    ('role_instances', 'initial_empty_role_complete'),
    ('producer_epochs', 'producer_epoch_guard'),
    ('producer_requests', 'producer_request_activation_guard'),
    ('producer_input_bindings', 'input_binding_activation_guard'),
}
PARTIAL = ('empty retirement requires exactly prepared or complete terminal metadata',
    'control.require_empty_initial_retirement(bytea,bytea)')
CLAIM = ('empty attempt Control workflow claims remain unresolved',
    'control.require_initial_empty_claims(control.initial_ingest_successions)')


def _bytes(value, size=None):
    assert isinstance(value, str) and value.startswith('\\x')
    answer = bytes.fromhex(value[2:])
    assert size is None or len(answer) == size
    return answer


def _literal(value):
    return "decode('" + _bytes(value).hex() + "','hex')"


def _id(label, kind):
    return hashlib.sha256(('bigbrotr/empty-sql-fixture/1\0' + label + '\0' + kind).encode()).digest()[:16].hex()


def expected_digest(intent):
    """Literal selected recipe, independent of every SQL/native digest helper."""
    return hashlib.sha256(b'bigbrotr/initial-empty-attempt-closure/1\0'
        + hashlib.sha256(_bytes(intent['handoff'])).digest()
        + hashlib.sha256(_bytes(intent['target_manifest'])).digest()
        + _bytes(intent['producer_slot_id'], 16)
        + (2).to_bytes(8, 'big') + bytes(16)).hexdigest()


def _guards(pg, database):
    result = json.loads(lab.sql(pg, database, 'SELECT ' + GUARDS))
    require_guards(result)
    assert REQUIRED <= {(t['table'], t['name']) for t in result['triggers']}
    return result


def _absence(pg, database, role, backend=None):
    """Observe exact retained backend tuples and the application login; no kill."""
    if backend is not None:
        assert type(backend['pid']) is int and backend['pid'] > 0
        assert re.fullmatch('[0-9T:.+ Z-]+', backend['backend_start'])
    exact = '' if backend is None else (f" OR (a.pid={backend['pid']} AND "
        f"a.backend_start='{backend['backend_start']}'::timestamptz)")
    statement = f"""SELECT coalesce(json_agg(json_build_object(
        'pid',a.pid,'backend_start',a.backend_start,'database',a.datname,'user',a.usename)), '[]')
        FROM pg_stat_activity a WHERE a.datname=current_database() AND
        (a.usename='{role}' OR EXISTS (SELECT 1 FROM control.owner_epochs o
            WHERE o.role='control' AND o.control_backend_pid=a.pid
                AND o.control_backend_start=a.backend_start){exact})"""
    started, samples = time.monotonic(), []
    while True:
        remaining = 5 - (time.monotonic() - started)
        if remaining <= 0:
            raise TimeoutError('raw guard backend absence unobserved')
        response = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
            '--no-password', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
            data=statement, timeout=remaining)
        present = json.loads(response.stdout)
        elapsed = time.monotonic() - started
        samples.append(dict(seconds=elapsed, present=present))
        if not present and elapsed <= 5:
            return dict(status='OBSERVED_ABSENT', database=database, role=role,
                samples=samples, exact_backend=backend, retained_control_tuples_checked=True)
        time.sleep(min(.05, max(0, 5 - elapsed)))


def _setup(before, intent, name, role):
    previous = max((r for r in before['owner_epochs'] if r['role'] == 'control'), key=lambda r: r['epoch'])
    assert all(r['state'] == 'released' for r in before['owner_epochs'] if r['role'] == 'control')
    assert previous['resource_id'] == intent['prepared_by_resource']
    resource, dataset = _literal(previous['resource_id']), _literal(intent['dataset_id'])
    role_id, incarnation = _id(name, 'role'), _id(name, 'incarnation')
    assert '\\x' + role_id not in {r['role_instance_id'] for r in before['role_instances']}
    assert '\\x' + incarnation not in {r['process_incarnation'] for r in before['owner_epochs']}
    epoch = previous['epoch'] + 1
    statement = f"""BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL search_path=pg_catalog;
SET LOCAL synchronous_commit=on;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='5s';
SET LOCAL idle_in_transaction_session_timeout='5s';
SELECT pg_advisory_xact_lock(1111651442,1129599564);
SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint AS fixture_time \\gset
INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us)
    VALUES(decode('{role_id}','hex'),{dataset},'control',:fixture_time);
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
    process_incarnation,acquired_at_us,lease_expires_at_us,state,control_fence_version,
    control_backend_pid,control_backend_start)
SELECT {resource},{epoch},{dataset},'control',decode('{role_id}','hex'),
    decode('{incarnation}','hex'),:fixture_time,:fixture_time+20000000,'active',1,
    pg_backend_pid(),backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid();
SELECT control.require_control_owner({dataset},{resource},{epoch},decode('{incarnation}','hex'));
SELECT json_build_object('event','backend','pid',pid,'backend_start',backend_start,
    'database',datname,'user',session_user,'role',current_user,
    'superuser',(SELECT rolsuper FROM pg_roles WHERE rolname=session_user),
    'isolation',current_setting('transaction_isolation'),
    'control_epoch',{epoch},'incarnation','{incarnation}','role_instance','{role_id}',
    'replication_role',current_setting('session_replication_role'),
    'fixture_time',:fixture_time) FROM pg_stat_activity WHERE pid=pg_backend_pid();
"""
    return statement, dict(control_epoch=epoch, incarnation=incarnation, role_instance=role_id, user=role, role=role)


def _classify(resource, digest, phase):
    return (f"SELECT json_build_object('event','classified','phase','{phase}','terminal_at',"
        f"control.require_empty_initial_retirement({resource},decode('{digest}','hex')));\n")


def _claim(before, intent, name, kind):
    assert kind in ('import', 'acquisition')
    job = _id(name, 'job')
    assert '\\x' + job not in {r['job_id'] for r in before['jobs']}
    return f"""
INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,
    kind,config_revision,policy_revision,created_at_us)
SELECT decode('{job}','hex'),dataset_id,decode('{_id(name, 'actor')}','hex'),
    decode('{_id(name, 'request')}','hex'),decode(repeat('47',32),'hex'),'{kind}',
    config_revision,policy_revision,:fixture_time FROM control.datasets;
UPDATE control.jobs SET input_complete=true WHERE job_id=decode('{job}','hex');
UPDATE control.jobs j SET state='queued',job_revision=2,
    owner_resource_id=o.resource_id,owner_epoch=o.epoch,owner_incarnation=o.process_incarnation
FROM control.owner_epochs o WHERE j.job_id=decode('{job}','hex')
    AND o.dataset_id=j.dataset_id AND o.role='control' AND o.state='active';
SELECT json_build_object('event','claim','job',to_jsonb(j),'admission',to_jsonb(a),
    'requests',(SELECT count(*) FROM control.producer_requests WHERE acquisition_job_id=j.job_id),
    'bindings',(SELECT count(*) FROM control.producer_input_bindings WHERE job_id=j.job_id),
    'bounds',(SELECT count(*) FROM control.job_input_bounds WHERE job_id=j.job_id))
FROM control.jobs j JOIN control.job_admissions a USING(job_id) WHERE j.job_id=decode('{job}','hex');
""", job


def _mutations(intent, digest):
    resource, slot, dataset = map(_literal, (intent['owner_resource_id'], intent['producer_slot_id'], intent['dataset_id']))
    role = _literal(intent['target_role_instance_id'])
    source = f'dataset_id={dataset} AND shard_id={intent["shard_id"]} AND owner_epoch=2'
    producer = f'producer_slot_id={slot} AND producer_epoch=2'
    domains = f'dataset_id={dataset} AND owner_resource_id={resource} AND owner_epoch=2'
    close_source = f"UPDATE control.source_streams SET state='closing' WHERE {source};\n"
    finish_source = (f"UPDATE control.source_streams SET state='closed',closed_at_us=:fixture_time,"
        f"last_complete_sequence=0,closure_digest=decode('{digest}','hex') WHERE {source};\n")
    close_producer = f"UPDATE control.producer_epochs SET state='closing' WHERE {producer};\n"
    finish_producer = (f"UPDATE control.producer_epochs SET state='retired',retired_at_us=:fixture_time,"
        f"closure_digest=decode('{digest}','hex') WHERE {producer};\n")
    close_domains = (f"UPDATE control.allocation_domains SET state='closed',closed_at_us=:fixture_time,"
        f"closure_digest=decode('{digest}','hex') WHERE {domains}")
    release = (f"UPDATE control.owner_epochs SET state='released',released_at_us=:fixture_time,"
        f"exclusion_method='storage_fenced',exclusion_digest=decode('{digest}','hex') "
        f"WHERE resource_id={resource} AND epoch=2;\n")
    retire_role = f'UPDATE control.role_instances SET retired_at_us=:fixture_time WHERE role_instance_id={role};\n'
    complete = close_source + finish_source + close_producer + finish_producer + close_domains + ';\n' + release + retire_role
    return dict(source=close_source, producer=close_producer,
        domain=close_domains + f' AND allocation_domain=(SELECT allocation_domain FROM control.allocation_domains '
            f'WHERE {domains} ORDER BY dictionary_kind,allocation_domain LIMIT 1);\n',
        owner=release, role=retire_role, complete=complete,
        wrong_cut=complete.replace('last_complete_sequence=0', 'last_complete_sequence=1'),
        wrong_time=complete.replace('SET retired_at_us=:fixture_time WHERE role_instance_id=',
            'SET retired_at_us=:fixture_time+1 WHERE role_instance_id='))


def _run(pg, database, output, rows, label, role, terminal):
    assert re.fullmatch('[a-z][a-z0-9_]+', database)
    assert re.fullmatch('[a-z][a-z0-9_-]+', role) and re.fullmatch('[a-z][a-z0-9-]+', label)
    assert output.is_dir() and not output.is_symlink()
    info, = json.loads(lab.docker('inspect', pg['postgres']).stdout)
    assert info['State']['Running'] and info['Config']['Labels'][lab.LABEL] == pg['owner']
    baseline = rows(database)
    assert len(baseline) == 71
    assert any(r['migration'] == 31 for r in baseline['schema_versions'])
    intent, = baseline['initial_ingest_successions']
    target, = [r for r in baseline['producer_epochs'] if r['producer_epoch'] == 2]
    assert target['state'] == ('retired' if terminal else 'open')
    digest = expected_digest(intent)
    assert digest != '00' * 32
    expected_time = target['retired_at_us'] if terminal else None
    assert not terminal or target['closure_digest'] == '\\x' + digest
    resource = _literal(intent['owner_resource_id'])
    guards = _guards(pg, database)
    report_path = output / (label + '-guards.json')
    assert not report_path.exists() and not report_path.is_symlink()
    report = dict(format='bigbrotr-initial-empty-sql-guards/1', status='INCOMPLETE',
        database=database, role=role, terminal=terminal, empty_digest=digest,
        qualification='raw relational guards; actual temporary Control backend; all attempts rolled back',
        native_authority=False, backend_absence_before=_absence(pg, database, role),
        baseline=baseline, guards=guards, cases=[], excluded=[
            'preparer-Control claim requires a distinct live-preparer fixture; released history is not rewritten',
            'typed acquisition execution, native exclusion, product Control acquisition API and password authentication'])
    mutations = _mutations(intent, digest)
    schedules = [('baseline', '', None, False, False)]
    if not terminal:
        schedules += [('wrong-digest', _classify(resource, '00' * 32, 'wrong-digest'),
            ('empty retirement digest differs from retained preparation', 'control.lock_initial_empty_retirement(bytea,bytea)'), False, False)]
        for name in ('source', 'producer', 'domain', 'role'):
            if name == 'domain':
                assert any(r['owner_epoch'] == 2 for r in baseline['allocation_domains'])
            schedules.append((name + '-only', mutations[name], PARTIAL, True, False))
        schedules += [
            ('owner-only', mutations['owner'], ('ingest owner release requires closed domains, streams and producer epochs',
                'guard_owner_ingest_dependents()'), False, False),
            ('wrong-cut', mutations['wrong_cut'], ('empty retirement requires its exact atomic transition staging',
                'control.require_initial_empty_transition(bytea,bytea,bigint)'), False, False),
            ('wrong-common-time', mutations['wrong_time'], PARTIAL, True, False),
            ('complete-rollback', mutations['complete'] + 'SET CONSTRAINTS ALL IMMEDIATE;\n'
                + _classify(resource, digest, 'complete'), None, False, True),
        ]
    for kind in ('import', 'acquisition') if not terminal else ('import',):
        schedules.append((kind + '-claim', '', None if terminal else CLAIM, False, False))
    try:
        for mode, mutation, refusal, at_commit, completes in schedules:
            name = label + '-' + mode
            paths = {suffix: output / (name + '-' + suffix) for suffix in ('statement.sql', 'response.json', 'after.json')}
            assert all(not p.exists() and not p.is_symlink() for p in paths.values())
            setup, expected_owner = _setup(baseline, intent, name, role)
            claim_id = None
            if mode.endswith('-claim'):
                mutation, claim_id = _claim(baseline, intent, name, mode[:-6])
                mutation += _classify(resource, digest, 'after-claim')
            ending = ("SELECT json_build_object('event','before-commit');\nCOMMIT;\n" if at_commit
                else ('COMMIT;\n' if refusal else 'ROLLBACK;\n'))
            statement = setup + _classify(resource, digest, 'baseline') + mutation + ending
            paths['statement.sql'].write_text(statement)
            result = dict(name=name, status='INCOMPLETE', expected_refusal=refusal, expected_at_commit=at_commit)
            report['cases'].append(result)
            lab.write_json(report_path, report)
            started = time.monotonic()
            try:
                response = lab.docker('exec', '-i', '--env', 'PGCONNECT_TIMEOUT=5', pg['postgres'],
                    'psql', '-X', '-A', '-t', '-q', '--no-password', '-v', 'ON_ERROR_STOP=1',
                    '-v', 'VERBOSITY=verbose', '-U', role, '-d', database,
                    data=statement, check=False, timeout=30)
            except subprocess.TimeoutExpired as error:
                def partial(value):
                    return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
                result['status'] = 'UNKNOWN_TIMEOUT'
                lab.write_json(paths['response.json'], dict(returncode=None,
                    stdout=partial(error.stdout), stderr=partial(error.stderr), outcome='UNKNOWN_TIMEOUT'))
                raise
            lab.write_json(paths['response.json'], dict(returncode=response.returncode,
                stdout=response.stdout, stderr=response.stderr, seconds=time.monotonic() - started))
            events = [json.loads(line) for line in response.stdout.splitlines() if line.startswith('{')]
            backends = [e for e in events if e['event'] == 'backend']
            # Preserve actual terminal observations before asserting expectations.
            # An unexpectedly successful negative remains a retained failed clone.
            result['backend_absence'] = _absence(pg, database, role, backends[0] if len(backends) == 1 else None)
            after = rows(database)
            lab.write_json(paths['after.json'], after)
            result['observed_guards'] = _guards(pg, database)
            result['all_control_rows_xmin_unchanged'] = after == baseline
            result['events'] = events
            lab.write_json(report_path, report)
            backend, = backends
            assert backend['database'] == database and backend['replication_role'] == 'origin'
            assert backend['superuser'] is False and backend['isolation'] == 'read committed'
            assert type(backend['pid']) is int and backend['pid'] > 0 and backend['backend_start']
            assert all(backend[k] == v for k, v in expected_owner.items())
            classified = [e for e in events if e['event'] == 'classified']
            assert classified[0] == dict(event='classified', phase='baseline', terminal_at=expected_time)
            assert (any(e['event'] == 'before-commit' for e in events)) == at_commit
            if claim_id:
                observed, = [e for e in events if e['event'] == 'claim']
                job = observed['job']
                assert job['job_id'] == '\\x' + claim_id and job['kind'] == mode[:-6]
                assert job['state'] == 'queued' and job['input_complete'] and job['job_revision'] == 2
                assert job['checkpoint_revision'] == 0 and job['input_topology_generation'] is None
                assert job['owner_resource_id'] == intent['prepared_by_resource']
                assert job['owner_epoch'] == backend['control_epoch']
                assert job['owner_incarnation'] == '\\x' + backend['incarnation']
                assert job['acquisition_job_protocol'] is None and job['export_job_protocol'] is None
                assert observed['requests'] == observed['bindings'] == observed['bounds'] == 0
                assert observed['admission']['job_id'] == job['job_id']
            if refusal:
                assert response.returncode == 3
                assert re.search(r'ERROR:\s+23514: ' + re.escape(refusal[0]) + r'\s*\n', response.stderr), response.stderr
                assert 'function ' + refusal[1] + ' line ' in response.stderr, response.stderr
                if at_commit:
                    assert 'function control.require_complete_initial_empty_retirement() line ' in response.stderr
                result.update(sqlstate='23514', message=refusal[0], function=refusal[1])
            else:
                assert response.returncode == 0 and not response.stderr, response.stderr
                if completes:
                    assert classified[-1] == dict(event='classified', phase='complete', terminal_at=backend['fixture_time'])
                elif claim_id:
                    assert classified[-1] == dict(event='classified', phase='after-claim', terminal_at=expected_time)
                result['sqlstate'] = '00000'
            assert after == baseline, 'raw SQL attempt changed durable rows/xmin'
            assert result['observed_guards'] == guards
            result.update(status='PASS', backend=backend, all_control_rows_xmin_unchanged=True,
                guards_unchanged=True, events=events)
            lab.write_json(report_path, report)
        report['status'] = 'PASS_RELATIONAL_EMPTY_TERMINAL' if terminal else 'PASS_RELATIONAL_EMPTY_PREPARED'
        return report
    except BaseException as error:
        report['error'] = type(error).__name__
        raise
    finally:
        lab.write_json(report_path, report)


def run_prepared(pg, database, output, rows, label, role='bigbrotr_control'):
    return _run(pg, database, output, rows, label, role, terminal=False)


def run_terminal(pg, database, output, rows, label, role='bigbrotr_control'):
    return _run(pg, database, output, rows, label, role, terminal=True)
