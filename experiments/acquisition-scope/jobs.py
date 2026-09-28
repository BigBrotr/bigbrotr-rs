#!/usr/bin/env python3
"""Actual finite job acceptance; immutable originals and new PostgreSQL clones."""
import datetime
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
from fault_cases import Process, safe_event
from concurrency_cases import Session
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection

arguments = sys.argv[1:]
acceptance_first_only = bool(arguments and arguments[-1] == '--acceptance-first')
if acceptance_first_only: arguments.pop()
faults = bool(arguments and arguments[-1] == '--faults')
if faults: arguments.pop()
assert not acceptance_first_only or faults
assert len(arguments) in (1, 2)
root = Path(arguments[0]).resolve(strict=True)
assert SOURCE == root / 'source'
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
build = Path(arguments[1]).resolve(strict=True) if len(arguments) == 2 else root / 'out/artifacts'
built = json.loads((build / 'build-result.json').read_text())
assert built['probe_name'] == 'acquisition_job_probe'
if built['source'] != revision:
    changes = lab.command(['git', '-C', str(SOURCE), 'diff', '--name-only', built['source'], revision]).stdout.splitlines()
    assert changes and all(name.startswith('docs/') or name in (
        'experiments/acquisition-scope/README.md', 'experiments/acquisition-scope/jobs.py',
        'experiments/acquisition-scope/job_guards.sql') for name in changes)
    assert len(arguments) == 2

sha = lambda value: hashlib.sha256(value).hexdigest()
for name, key in [('bigbrotr', 'binary_sha256'), (built['probe_name'], 'probe_sha256')]:
    assert sha((build / name).read_bytes()) == built[key]
pg_root = root.parent / 'relay-source-recheck-e06f580/pg'
pg = lab.load(pg_root)
assert pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608' and pg['state'] == 'stopped-retained'
basis_root = root.parent / ('acquisition-jobs-engine-28a7db9/jobs' if faults else 'acquisition-scope-54d1a18/engine')
basis = 'bb_acq_job_28a7db9_no_grant' if faults else 'bb_scope_54d1a18_basic'
basis_report = (basis_root / 'result.json').read_bytes()
assert json.loads(basis_report)['status'] == ('PASS_FINITE_ACQUISITION_ACCEPTANCE' if faults else 'PASS_SCOPED_ACQUISITION_REGISTRATION')
output = root / ('job-faults' if faults else 'jobs')
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.acquisition-jobs'
processes, sessions = [], []
expected_status = 'PASS_FINITE_ACQUISITION_FAULTS' if faults else 'PASS_FINITE_ACQUISITION_ACCEPTANCE'
if acceptance_first_only: expected_status = 'PASS_ACQUISITION_ACCEPTANCE_FIRST_LOCKS'
client = 'bigbrotr-acquisition-jobs-' + revision[:7]
report = dict(format='bigbrotr-acquisition-jobs/1', source=revision, build=built,
    status='RUNNING', mode='faults' if faults else 'acceptance',
    selection='acceptance-first locks only' if acceptance_first_only else 'complete wave',
    owner=owner, label=label, client=None, cases=[], databases=[],
    postgres=pg['postgres'], postgres_owner=pg['owner'],
    not_run=['concurrent locks, cancellation and lost/delayed COMMIT',
             'remaining raw SQL relational negatives and historical grant/policy retry',
             'claim/dispatch, installed Sync, socket admission and M1',
             'populated prior-job migration, aged/mixed load, arm64/release'])
if faults:
    report['not_run'] = ['session expiry/revocation and credential-reset races',
        'raw SQL relational negatives beyond the qualified timestamp pair',
        'claim/dispatch, installed Sync, socket admission and M1',
        'populated prior-job migration, aged/mixed load, arm64/release']


def require(value, reason):
    if not value: raise RuntimeError(reason)


def save(): lab.write_json(output / 'result.json', report)


def record(name, value):
    report['cases'].append(dict(name=name, status='PASS' if value else 'FAIL'))
    save()
    require(value, name)


def containers():
    return json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)


def rows(database):
    names = json.loads(lab.sql(pg, database,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    require(len(names) in (66, 67) and all(re.fullmatch('[a-z_]+', n) for n in names), 'table-set')
    parts = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
             f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg, database,
        'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t'))


def prepare(name):
    database = 'bb_acq_job_' + revision[:7] + '_' + name.replace('-', '_')
    require(re.fullmatch('[a-z0-9_]{1,63}', database), 'database-name')
    lab.sql(pg, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE {basis}')
    report['databases'].append(database); save()
    require(rows(database) == basic, 'exact-clone')
    if faults:
        require(not basic['jobs'] and not basic['acquisition_jobs'] and
            any(r['migration'] == 22 for r in basic['schema_versions']), 'qualified-empty-job-basis')
        lab.sql(pg, database, "INSERT INTO control.principal_grants SELECT dataset_id,principal_id,"
            "'roles.control','dataset',dataset_id FROM control.local_accounts WHERE username='alice';")
        return database, rows(database)
    config = lab.config(output, dict(pg, client=client), database, endpoint='127.0.0.1:5432', boundary='loopback')
    def migrate(target):
        result = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', str(target))
        (output / f'{name}-migration-{target}.stdout').write_text(result.stdout)
        (output / f'{name}-migration-{target}.stderr').write_text(result.stderr)
        require(result.returncode == 0, 'explicit-migration')
    migrate(21)
    before = rows(database)
    if name != 'old-schema':
        migrate(22)
        after = rows(database)
        added = [r for r in after['schema_versions'] if r not in before['schema_versions']]
        record(name + '-upgrade-preserves-prior-state',
            set(after) - set(before) == {'acquisition_jobs'} and not after['acquisition_jobs'] and
            all(after[t] == before[t] for t in before if t != 'schema_versions') and
            all(r in after['schema_versions'] for r in before['schema_versions']) and len(added) == 1 and
            added[0]['migration'] == 22 and added[0]['checksum'] == '\\x' + sha((SOURCE / 'migrations/postgres/0022_acquisition_jobs.sql').read_bytes()))
        # Keep the first migration transcript; reapply has its own capture.
        repeated = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '22')
        (output / (name + '-reapply.stdout')).write_text(repeated.stdout)
        (output / (name + '-reapply.stderr')).write_text(repeated.stderr)
        record(name + '-identical-reapply', repeated.returncode == 0 and rows(database) == after)
        lab.sql(pg, database, 'GRANT SELECT,INSERT ON control.acquisition_jobs TO bigbrotr_control; '
            'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
    # Independent fixture setup, not a claim of management grant activation.
    if name != 'no-grant':
        lab.sql(pg, database, "INSERT INTO control.principal_grants SELECT dataset_id,principal_id,"
            "'roles.control','dataset',dataset_id FROM control.local_accounts WHERE username='alice';")
    if name == 'disabled':
        lab.sql(pg, database, 'BEGIN; SET LOCAL session_replication_role=replica; '
            'UPDATE control.relay_enrollments SET enabled=false; COMMIT;')
    if name == 'rollback':
        lab.sql(pg, database, """
            CREATE FUNCTION control.fixture_reject_acquisition() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture commit refusal' USING ERRCODE='23514'; END $$;
            CREATE CONSTRAINT TRIGGER zzz_fixture_refusal AFTER INSERT ON control.acquisition_jobs
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.fixture_reject_acquisition();
        """)
    return database, rows(database)


def run_case(name):
    database, before = prepare(name)
    lab.write_json(output / (name + '-before.json'), before)
    result = lab.docker('exec', client, '/build/acquisition_job_probe', database,
        '/run/secrets/application', '/lab/policy.json', name, check=False, timeout=45)
    (output / (name + '.stdout')).write_text(result.stdout)
    (output / (name + '.stderr')).write_text(result.stderr)
    record(name + '-actual-adapter', result.returncode == 0 and not result.stderr)
    events = [json.loads(line) for line in result.stdout.splitlines()]
    require(events[-1] == dict(event='terminal', mode=name, status='PASS', actual_driver_joined=True), 'terminal')
    after = rows(database)
    lab.write_json(output / (name + '-after.json'), after)
    mutable = {'owner_epochs', 'role_instances', 'browser_sessions', 'login_buckets', 'management_audit'}
    if name == 'pass':
        mutable |= {'management_commands', 'jobs', 'job_admissions', 'acquisition_jobs'}
        receipt = events[0]['value']
        require(events[0]['event'] == 'receipt' and receipt['created'], 'created-receipt')
        job, detail, admission = [after[t][0] for t in ('jobs', 'acquisition_jobs', 'job_admissions')]
        commands = [c for c in after['management_commands'] if c not in before['management_commands']]
        require(len(commands) == 1 and all(len(after[t]) == 1 for t in ('jobs','acquisition_jobs','job_admissions')), 'single-admission')
        command = commands[0]
        dataset, actor = before['datasets'][0]['dataset_id'], '\\x' + '10' * 16
        scope = min(s['scope_id'] for s in before['acquisition_scopes'])
        preimage = (b'bigbrotr/acquisition-job/1\0' + bytes.fromhex(dataset[2:]) + bytes.fromhex(actor[2:]) +
                    bytes([77]) * 16 + bytes.fromhex(scope[2:]) +
                    (120_000_000).to_bytes(8, 'big') + (1).to_bytes(8, 'big') * 2)
        digest = '\\x' + sha(preimage)
        expected_key, expected_job = '\\x' + '4d' * 16, '\\x' + receipt['job_id']
        require(receipt['command_key'] == '4d' * 16 and
            job['job_id'] == detail['job_id'] == admission['job_id'] == expected_job and
            job['request_key'] == detail['request_key'] == command['request_key'] == expected_key and
            job['request_digest'] == command['request_digest'] == digest and
            job['principal_id'] == detail['actor_id'] == command['actor_id'] == actor and
            job['dataset_id'] == detail['dataset_id'] == command['dataset_id'] == dataset and
            job['created_at_us'] == command['accepted_at_us'] == int(receipt['accepted_at_us']) and
            job['kind'] == 'acquisition' and job['state'] == 'accepted' and job['input_complete'] and
            job['job_revision'] == 1 and job['checkpoint_revision'] == 0 and
            job['input_topology_generation'] is None and job['owner_resource_id'] is None and
            job['acquisition_job_protocol'] == 1 and job['export_job_protocol'] is None and
            job['config_revision'] == job['policy_revision'] == command['expected_config_revision'] == command['policy_revision'] == 1 and
            detail['scope_id'] == scope and detail['duration_us'] == 120_000_000 and
            detail['deadline_at_us'] == job['created_at_us'] + 120_000_000 and admission['admission_ordinal'] == 1,
            'independent-exact-intent')
        audits = [a for a in after['management_audit'] if a['action'] == 'job.acquisition.accept']
        require(len(audits) == 1 and audits[0]['audit_id'] == command['audit_id'] and
            audits[0]['subject_id'] == expected_key and audits[0]['actor_id'] == actor and
            audits[0]['created_at_us'] == command['accepted_at_us'], 'exact-acceptance-audit')
        sessions = [s for s in after['browser_sessions'] if s['session_id'] == detail['session_id']]
        require(len(sessions) == 1 and sessions[0]['principal_id'] == actor and
            all(detail[k] == 1 for k in ('actor_credential_revision','actor_authentication_revision','actor_grant_revision')), 'authenticated-attribution')
    record(name + '-other-tables-unchanged', all(after[t] == before[t] for t in before if t not in mutable))
    # Retain historical audit/owner rows. Login alone retires expired/revoked
    # sessions; compare that exact set rather than exempting the entire table.
    for table in mutable - {'login_buckets', 'browser_sessions'}:
        require(all(row in after[table] for row in before[table]), 'prior-rows-' + table)
    new_sessions = [r for r in after['browser_sessions']
                    if r['session_id'] not in {b['session_id'] for b in before['browser_sessions']}]
    require(len(new_sessions) == 1 and new_sessions[0]['principal_id'] == '\\x' + '10' * 16,
            'single-authenticated-session')
    issued = new_sessions[0]['issued_at_us']
    retained_sessions = [r for r in before['browser_sessions']
        if r['principal_id'] != new_sessions[0]['principal_id'] or
        (r['revoked_at_us'] is None and min(r['expires_at_us'], r['idle_expires_at_us']) > issued)]
    require(sorted(after['browser_sessions'], key=lambda r: r['session_id']) ==
        sorted(retained_sessions + new_sessions, key=lambda r: r['session_id']), 'exact-login-session-cleanup')
    require(not [a for a in after['management_audit'] if a['action'] == 'job.acquisition.accept'] or name == 'pass', 'refusal-audit-absent')
    record(name + '-actual-backends-joined', lab.sql(pg, database,
        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0')


def raw_timestamp_guards():
    fixture = SOURCE / 'experiments/acquisition-scope/job_guards.sql'
    for negative in (False, True):
        name = 'timestamp_negative' if negative else 'timestamp_guards'
        database = 'bb_acq_job_' + revision[:7] + '_' + name
        lab.sql(pg, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE bb_acq_job_{revision[:7]}_pass')
        report['databases'].append(database); save()
        before = rows(database)
        lab.write_json(output / (name + '-before.json'), before)
        if negative:
            # Clone-only removal of the single future-time guard. The same
            # independent fixture must then fail, with all data rolled back.
            lab.sql(pg, database, """
                DO $$ DECLARE definition text; BEGIN
                    SELECT pg_get_functiondef('control.require_acquisition_acceptance()'::regprocedure) INTO definition;
                    IF position('command.accepted_at_us>now_us OR ' in definition)=0 THEN
                        RAISE EXCEPTION 'negative-control predicate absent';
                    END IF;
                    EXECUTE replace(definition,'command.accepted_at_us>now_us OR ','');
                END $$;
            """)
        result = lab.docker('exec', '-i', '--env', 'PGOPTIONS=-c statement_timeout=30000 -c lock_timeout=5000',
            target['Id'], 'psql', '-X', '-A', '-t', '-q', '--no-password', '-U', 'bigbrotr_control',
            '-d', database, '-v', 'ON_ERROR_STOP=1', data=fixture.read_text(), check=False, timeout=45)
        (output / (name + '.stdout')).write_text(result.stdout)
        (output / (name + '.stderr')).write_text(result.stderr)
        if negative:
            record('future-time-negative-discriminates', result.returncode != 0 and
                'expected BB001, observed BZ001' in result.stderr)
        else:
            events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
            record('raw-current-and-future-timestamps', result.returncode == 0 and not result.stderr and
                events == [dict(status='PASS', valid_current_time=True, future_time_refused=True)])
        after = rows(database)
        lab.write_json(output / (name + '-after.json'), after)
        record(name + '-complete-rollback', before == after)

def probe_event(process, wanted, seconds=10):
    deadline = time.monotonic() + seconds
    with process.condition:
        while True:
            require(not process.overflow, 'probe-output-overflow')
            end = process.stdout.find(b'\n', process.offset)
            if end >= 0:
                value = json.loads(bytes(process.stdout[process.offset:end])); process.offset = end + 1
                require(value.get('event') == wanted, 'probe-event-' + wanted)
                return value
            require(time.monotonic() < deadline and
                (process.process.poll() is None or process.threads[0].is_alive()), 'missing-probe-event-' + wanted)
            process.condition.wait(.02)


def app(database, mode, nonce, port=5432):
    token = database + '-' + str(nonce)
    process = Process(['docker', 'exec', client, '/build/acquisition_job_probe', database,
        '/run/secrets/application', '/lab/policy.json', 'fault-' + mode, str(port), str(nonce), '/lab/' + token])
    processes.append((token, process))
    return process, token


def go(token, suffix='go'):
    (output / (token + '.' + suffix)).touch(exist_ok=False)


def finish(process):
    value = probe_event(process, 'done')
    require(process.wait(10) == 0 and not process.stderr and value['actual_driver_joined'], 'actual-probe-joined')
    return value['outcome']


def proxy_for(database, mode):
    process = Process(['docker', 'exec', '-i', client, 'python3', '-B',
        '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432',
        '--mode', mode, '--commit-number', '4', '--deadline-seconds', '25'], input_pipe=True)
    processes.append((database + '-proxy', process))
    ready = process.event('ready', 5)
    require(ready['proxy_sha256'] == sha((SOURCE / 'experiments/control-migrations/fault_proxy.py').read_bytes()), 'actual-proxy-source')
    return process, ready['listen_port']


def proxy_finished(proxy, expected):
    wire = proxy.event('receipt', 5)
    require(proxy.wait(5) == 0 and not proxy.stderr, 'actual-proxy-joined')
    for line in proxy.stdout.splitlines(): safe_event(json.loads(line))
    require([wire[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']] == expected,
            'exact-fourth-COMMIT-cut')
    return wire


def snapshot(database, tag):
    value = rows(database)
    lab.write_json(output / (tag + '.json'), value)
    return value


def observe(database, ready):
    pid = ready['pid']
    value = json.loads(lab.sql(pg, database, f"""
        SELECT json_build_object('pid',a.pid,'start',a.backend_start::text,
            'now_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
            'expired',o.lease_expires_at_us<=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
            'fenced',EXISTS(SELECT 1 FROM pg_locks WHERE pid=a.pid AND locktype='advisory'
                AND classid=1111651442::oid AND objid=1129599564::oid AND objsubid=2
                AND mode='ExclusiveLock' AND granted))
        FROM pg_stat_activity a JOIN control.owner_epochs o ON o.control_backend_pid=a.pid
            AND o.control_backend_start=a.backend_start WHERE a.pid={pid} AND a.datname='{database}' AND o.state='active'
    """))
    require(value and value['pid'] == pid and value['fenced'], 'observed-actual-Control-custody')
    return value


def blocked(database, waiter, holder):
    deadline = time.monotonic() + 4
    while True:
        value = json.loads(lab.sql(pg, database, f"""SELECT json_build_object('pid',pid,'start',backend_start::text,
            'wait',wait_event_type,'blockers',pg_blocking_pids(pid)) FROM pg_stat_activity
            WHERE datname='{database}' AND pid={waiter}"""))
        if value and value['wait'] == 'Lock' and holder in value['blockers']: return value
        require(time.monotonic() < deadline, 'required-lock-barrier-not-observed')
        time.sleep(.02)


WORK_TABLES = {'jobs', 'acquisition_jobs', 'job_admissions', 'management_commands', 'management_audit'}


def compare_work(before, after, created, writer_tables=()):
    require(before.keys() == after.keys(), 'same-complete-table-set')
    for table in before:
        if table not in set(writer_tables) | (WORK_TABLES if created else set()):
            require(before[table] == after[table], 'unexpected-effect-' + table)
    if not created: return
    delta = {t:[row for row in after[t] if row not in before[t]] for t in WORK_TABLES}
    require(all(len(delta[t]) == 1 and len(after[t]) == len(before[t])+1 and
                all(row in after[t] for row in before[t]) for t in WORK_TABLES), 'one-atomic-job')
    job, detail, command, audit, admission = [delta[t][0] for t in
        ['jobs','acquisition_jobs','management_commands','management_audit','job_admissions']]
    payload = b'bigbrotr/acquisition-job/1\0'
    for value in [detail['dataset_id'], '\\x'+'10'*16, '\\x'+'4d'*16, min(s['scope_id'] for s in before['acquisition_scopes'])]:
        payload += bytes.fromhex(value[2:])
    payload += (120000000).to_bytes(8,'big') + (1).to_bytes(8,'big')*2
    require(len({r['xmin'] for r in [job,detail,command,audit,admission]}) == 1 and
        job['job_id'] == detail['job_id'] == admission['job_id'] and admission['admission_ordinal'] == 1 and
        job['request_key'] == detail['request_key'] == command['request_key'] == audit['subject_id'] == '\\x'+'4d'*16 and
        job['request_digest'] == command['request_digest'] == '\\x'+sha(payload) and
        job['created_at_us'] == command['accepted_at_us'] == audit['created_at_us'] and
        command['audit_id'] == audit['audit_id'] and command['action'] == audit['action'] == 'job.acquisition.accept' and
        detail['deadline_at_us'] == command['accepted_at_us']+120000000 and detail['duration_us'] == 120000000 and
        job['state'] == 'accepted' and job['input_complete'] and job['input_topology_generation'] is None,
        'independent-complete-acceptance')


def receipt_matches(outcome, state, created):
    require(len(state['jobs']) == 1, 'one-durable-job')
    job = state['jobs'][0]
    require(outcome == dict(receipt=dict(command_key='4d'*16, job_id=job['job_id'][2:],
        accepted_at_us=job['created_at_us'], created=created), commit_started=created), 'exact-recovered-receipt')


def backends_absent(database):
    deadline = time.monotonic() + 5
    while lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity "
                 "WHERE datname=current_database() AND usename='bigbrotr_control'") != '0':
        require(time.monotonic() < deadline, 'application-backend-still-present')
        time.sleep(.02)


def compare_writer(before, after, kind, changed):
    # Independent expected rows, including exact unchanged history and a single
    # new transaction for modified rows. Exempting writer tables would hide an
    # aborted injection followed by psql's successful ROLLBACK at COMMIT.
    expected = copy.deepcopy({t: before[t] for t in changed})
    modified = []
    if kind == 'policy':
        require(len(expected['datasets']) == 1, 'single-dataset')
        head = expected['datasets'][0]
        require(head['config_revision'] == head['policy_revision'] == 1, 'initial-policy-head')
        head.update(config_revision=2, policy_revision=2); modified.append(head)
        for table in ['config_revisions', 'policy_revisions']:
            require(len(expected[table]) == 1 and expected[table][0]['revision'] == 1, 'initial-policy-history')
            row = copy.deepcopy(expected[table][0])
            row.update(revision=2, previous_revision=1, accepted_at_us=row['accepted_at_us']+1)
            if table == 'policy_revisions': row['config_revision'] = 2
            expected[table].append(row); modified.append(row)
    elif kind == 'permission':
        grants = expected['principal_grants']
        require(sum(r['action'] == 'roles.control' for r in grants) == 1, 'one-request-grant')
        expected['principal_grants'] = [r for r in grants if r['action'] != 'roles.control']
        accounts = [r for r in expected['local_accounts'] if r['username'] == 'alice']
        require(len(accounts) == 1, 'single-actor')
        accounts[0]['grant_revision'] += 1; modified.append(accounts[0])
    else:
        require(kind == 'disabled' and expected['relay_enrollments'] and
                all(r['enabled'] for r in expected['relay_enrollments']), 'enabled-enrollments')
        for row in expected['relay_enrollments']:
            row['enabled'] = False; modified.append(row)
    new_xmins = {r['xmin'] for t in changed for r in after[t] if r not in before[t]}
    require(len(new_xmins) == 1, 'single-writer-transaction')
    xmin = new_xmins.pop()
    for row in modified:
        require(row['xmin'] != xmin, 'writer-made-a-durable-change')
        row['xmin'] = xmin
    ordered = lambda values: sorted(values, key=lambda r: json.dumps(r, sort_keys=True))
    require(all(ordered(expected[t]) == ordered(after[t]) for t in changed), 'exact-writer-effects')


def writer(kind):
    # Clone-only privileged fault injection, not management activation. Every
    # writer first locks dataset, then account/source; never reverse acquisition.
    lock = ['SELECT dataset_id FROM control.datasets FOR UPDATE;']
    if kind == 'policy':
        mutation = ["""INSERT INTO control.config_revisions
            SELECT revision+1,dataset_id,revision,schema_version,authority_mode,document,document_digest,
                accepted_source_digest,accepted_at_us+1,actor_id FROM control.config_revisions WHERE revision=1;""",
            """INSERT INTO control.policy_revisions SELECT revision+1,dataset_id,revision,2,schema_version,
                document,document_digest,accepted_at_us+1,actor_id FROM control.policy_revisions WHERE revision=1;""",
            'UPDATE control.datasets SET config_revision=2,policy_revision=2;']
        changed = {'datasets','config_revisions','policy_revisions'}
    elif kind == 'permission':
        lock.append("SELECT principal_id FROM control.local_accounts WHERE username='alice' FOR UPDATE;")
        mutation = ["DELETE FROM control.principal_grants WHERE action='roles.control';",
                    "UPDATE control.local_accounts SET grant_revision=grant_revision+1 WHERE username='alice';"]
        changed = {'principal_grants','local_accounts'}
    else:
        assert kind == 'disabled'
        mutation = ['UPDATE control.relay_enrollments SET enabled=false;']
        changed = {'relay_enrollments'}
    return lock, ['SET LOCAL session_replication_role=replica;'] + mutation, changed


def run_faults():
    commits = [
        ('pass','pass','accept',[4,4,4,4,0],True),
        ('before','before-forward','unknown',[4,3,3,3,1],False),
        ('after','after-complete','unknown',[4,4,4,3,1],True),
        ('cancel_close','hold','cancel',[4,3,3,3,1],False),
        ('cancel_release','hold','cancel',[4,4,4,4,1],True),
        ('delayed','hold','accept',[4,4,4,4,1],True),
        ('owner_expiry','hold','owner-expiry',[4,4,3,3,1],False),
    ]
    for name, wire_mode, mode, counts, committed in ([] if acceptance_first_only else commits):
        database, _ = prepare('commit_' + name)
        proxy, port = proxy_for(database, wire_mode)
        process, token = app(database, mode, 50, port)
        ready = probe_event(process, 'ready'); observed = [observe(database, ready)]
        require(not observed[0]['expired'], 'initial-live-owner')
        before = snapshot(database, name + '-before')
        go(token)
        if wire_mode == 'hold':
            proxy.event('held', 5)
            observed.append(observe(database, ready))
            require(rows(database) == before, 'uncommitted-acceptance-invisible')
            if mode == 'cancel':
                require(probe_event(process, 'cancelled', 4)['commit_started'], 'cancelled-during-COMMIT')
            elif mode == 'owner-expiry':
                deadline = time.monotonic()+5
                while not observed[-1]['expired']:
                    require(time.monotonic()<deadline, 'server-expiry-not-observed')
                    time.sleep(.05); observed.append(observe(database, ready))
            else:
                time.sleep(.4)
            contender, _ = app(database, 'busy', 51)
            require(probe_event(contender, 'busy')['actual_driver_joined'] and
                contender.wait(5) == 0 and not contender.stderr, 'competing-owner-refused')
            require(rows(database) == before, 'held-custody-durable-state')
            proxy.send('close' if name == 'cancel_close' else 'release')
            if mode == 'cancel': go(token, 'finish')
        outcome = finish(process); wire = proxy_finished(proxy, counts)
        backends_absent(database)
        after = snapshot(database, name + '-after'); compare_work(before, after, committed)
        if committed and wire_mode == 'hold':
            require(after['jobs'][0]['created_at_us'] <= observed[1]['now_us'], 'waiting-consumes-original-duration')
        if mode == 'accept': receipt_matches(outcome, after, True)
        recovery, retry_token = app(database, 'replay' if committed else 'accept', 52)
        retry_ready = probe_event(recovery, 'ready')
        require(retry_ready['epoch'] == ready['epoch']+1, 'fresh-recovery-owner')
        retry_before = snapshot(database, name + '-retry-before'); go(retry_token)
        recovered = finish(recovery); retry_after = snapshot(database, name + '-retry-after')
        backends_absent(database)
        compare_work(retry_before, retry_after, not committed); receipt_matches(recovered, retry_after, not committed)
        if committed:
            require(all(retry_after[t] == after[t] for t in WORK_TABLES - {'management_audit'}), 'historical-retry-preserves-job')
        report['cases'].append(dict(name='commit-'+name,status='PASS',outcome=outcome,wire=wire,observations=observed,recovery=recovered))
        save()
    for first in (['acceptance'] if acceptance_first_only else ['writer','acceptance']):
        for kind in ['policy','permission','disabled']:
            name = first + '_' + kind
            database, _ = prepare(name)
            proxy, port = proxy_for(database, 'hold') if first == 'acceptance' else (None,5432)
            process, token = app(database, 'accept' if proxy else kind, 60, port)
            ready = probe_event(process, 'ready'); observe(database, ready)
            initial = snapshot(database, name + '-before')
            lock, mutation, changed = writer(kind)
            session = Session(pg, database, 'acquisition-' + name, sessions, role='postgres')
            session.execute('BEGIN;')
            if first == 'writer':
                for statement in lock + mutation: session.execute(statement)
                go(token)
                barrier = blocked(database, ready['pid'], session.backend['pid'])
            else:
                go(token); proxy.event('held',5)
                session.start(lock[0])
                barrier = blocked(database, session.backend['pid'], ready['pid'])
            require(rows(database) == initial, 'waiting-writer-is-not-visible')
            if first == 'writer': session.execute('COMMIT;')
            else: proxy.send('release')
            outcome = finish(process)
            if first == 'acceptance':
                session.expect(session.finish())
                for statement in lock[1:] + mutation: session.execute(statement)
                session.execute('COMMIT;')
                proxy_finished(proxy,[4,4,4,4,1])
            session.close(lambda:lab.load(pg_root));sessions.remove(session)
            backends_absent(database)
            after = snapshot(database, name + '-after'); compare_work(initial,after,first=='acceptance',changed)
            compare_writer(initial, after, kind, changed)
            if first == 'acceptance':
                receipt_matches(outcome,after,True)
                recovery_mode = 'replay-' + kind if kind in ('policy','permission') else 'replay'
                recovery, retry_token = app(database,recovery_mode,64)
                probe_event(recovery,'ready'); retry_before=snapshot(database,name+'-retry-before');go(retry_token)
                recovered=finish(recovery);retry_after=snapshot(database,name+'-retry-after')
                backends_absent(database)
                compare_work(retry_before,retry_after,False);receipt_matches(recovered,retry_after,False)
            report['cases'].append(dict(name=name,status='PASS',barrier=barrier,outcome=outcome))
            save()


prior = containers()
target = next(c for c in prior if c['Name'] == '/' + pg['postgres'])
assert target['Config']['Labels'][lab.LABEL] == pg['owner'] and not target['State']['Running']
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
assert lab.docker('inspect', client, check=False).returncode != 0
report.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              free_bytes_before=shutil.disk_usage(root).free, postgres_id=target['Id'])
assert report['free_bytes_before'] > 2 * 1024**3
lab.write_json(output / 'preflight.json', dict(containers=[projection(c) for c in prior],
    processes=lab.command(['ps','-eo','pid,ppid,stat,comm']).stdout,
    ports=lab.command(['ss','-lntup']).stdout, volumes=lab.docker('volume','ls').stdout,
    directories=sorted(p.name for p in root.parent.iterdir())))
save()
try:
    lab.docker('start', target['Id'])
    deadline = time.monotonic() + 30
    while lab.docker('exec', target['Id'], 'pg_isready', '-U', 'postgres', check=False).returncode:
        require(time.monotonic() < deadline, 'postgres-readiness'); time.sleep(.1)
    basic = rows(basis)
    require(basic == json.loads((basis_root / ('no-grant-after.json' if faults else 'basic-after.json')).read_text()), 'qualified-basis-unchanged')
    require(not basic['jobs'], 'empty-job-ledger-basis')
    if faults:
        # Historical Ingest and Control identities share the role-instance key.
        # Never pick fixture identities solely by looking at Control rows.
        prior_ids = {r['role_instance_id'] for r in basic['role_instances']}
        prior_ids |= {r['process_incarnation'] for r in basic['owner_epochs']}
        require(all('\\x' + bytes([n]).hex()*16 not in prior_ids for n in (50,51,52,60,64)),
                'fixture-owner-identity-collision')
    policy = basic['config_revisions'][0]['document']
    require(policy == basic['policy_revisions'][0]['document'] and policy['format'] == 'bigbrotr-product-policy/2', 'fixture-policy')
    (output / 'policy.json').write_text(json.dumps(policy, sort_keys=True, separators=(',', ':')))
    ident = lab.docker('create', '--init', '--name', client, '--label', label + '=' + owner,
        '--network', 'container:' + pg['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
        '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128', '--read-only',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
        '--mount', f'type=bind,src={output},dst=/lab',
        '--mount', f'type=bind,src={pg_root}/secrets,dst=/run/secrets,readonly',
        '--mount', f'type=bind,src={build},dst=/build,readonly',
        '--mount', f'type=bind,src={SOURCE},dst=/source,readonly', pg['client_image'],
        'python3', '-c', 'import time; time.sleep(600)').stdout.strip()
    report['client'] = dict(id=ident, name=client); save(); lab.docker('start', ident)
    if faults:
        run_faults()
    else:
        for name in ('pass', 'no-grant', 'stale-policy', 'disabled', 'expired', 'rollback', 'old-schema'):
            run_case(name)
        raw_timestamp_guards()
    record('retained-basis-unchanged', rows(basis) == basic and (basis_root / 'result.json').read_bytes() == basis_report)
    report['status'] = expected_status
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error)); raise
finally:
    errors = []
    for token, process in processes:
        try:
            if process.process.poll() is None: errors.append('unfinished-process:' + token)
            process.stop()
        except BaseException as error: errors.append('process:' + type(error).__name__)
        for suffix, data in [('.stdout', process.stdout), ('.stderr', process.stderr)]:
            try: (output / (token + suffix)).write_bytes(data)
            except BaseException as error: errors.append('transcript:' + type(error).__name__)
    for session in sessions:
        try: session.close(lambda: lab.load(pg_root))
        except BaseException as error: errors.append('session:' + type(error).__name__)
    owned = [target['Id']] + ([report['client']['id']] if report['client'] else [])
    for ident in reversed(owned):
        try:
            item = json.loads(lab.docker('inspect', ident).stdout)[0]
            key, wanted = (lab.LABEL, pg['owner']) if ident == target['Id'] else (label, owner)
            require(item['Config']['Labels'][key] == wanted, 'cleanup-owner')
            if item['State']['Running']: lab.docker('stop', '--time', '10', ident)
        except BaseException as error: errors.append(type(error).__name__)
    try:
        logs = lab.docker('logs', '--since', report['started_at'], target['Id'], check=False)
        (output / 'postgres.stdout').write_text(logs.stdout)
        (output / 'postgres.stderr').write_text(logs.stderr)
    except BaseException as error: errors.append('logs:' + type(error).__name__)
    try:
        current = {c['Id']: projection(c) for c in containers()}
        report.update(missing_owned=[i for i in owned if i not in current],
            terminal=[current[i] for i in owned if i in current],
            unrelated_changed=[c['Id'] for c in prior if c['Id'] != target['Id'] and current.get(c['Id']) != projection(c)])
        if report['missing_owned'] or report['unrelated_changed'] or any(
            c['State']['Running'] or c['State']['Pid'] or c['State']['OOMKilled'] for c in report['terminal']):
            errors.append('incomplete-terminal-audit')
    except BaseException as error: errors.append('audit:' + type(error).__name__)
    report.update(cleanup_errors=errors, finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  free_bytes_after=shutil.disk_usage(root).free)
    if errors: report['status'] = 'INCOMPLETE_LAB_AUDIT'
    save()
require(report['status'] == expected_status, 'incomplete-jobs')
print(json.dumps(dict(status=report['status'], checks=len(report['cases']))))
