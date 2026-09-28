#!/usr/bin/env python3
"""Actual finite job acceptance; immutable originals and new PostgreSQL clones."""
import datetime
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
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection

root = Path(sys.argv[1]).resolve(strict=True)
assert len(sys.argv) in (2, 3) and SOURCE == root / 'source'
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
build = Path(sys.argv[2]).resolve(strict=True) if len(sys.argv) == 3 else root / 'out/artifacts'
built = json.loads((build / 'build-result.json').read_text())
assert built['probe_name'] == 'acquisition_job_probe'
if built['source'] != revision:
    changes = lab.command(['git', '-C', str(SOURCE), 'diff', '--name-only', built['source'], revision]).stdout.splitlines()
    assert changes and all(name.startswith('docs/') or name in (
        'experiments/acquisition-scope/README.md', 'experiments/acquisition-scope/jobs.py',
        'experiments/acquisition-scope/job_guards.sql') for name in changes)
    assert len(sys.argv) == 3

sha = lambda value: hashlib.sha256(value).hexdigest()
for name, key in [('bigbrotr', 'binary_sha256'), (built['probe_name'], 'probe_sha256')]:
    assert sha((build / name).read_bytes()) == built[key]
pg_root = root.parent / 'relay-source-recheck-e06f580/pg'
pg = lab.load(pg_root)
assert pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608' and pg['state'] == 'stopped-retained'
basis_root = root.parent / 'acquisition-scope-54d1a18/engine'
basis = 'bb_scope_54d1a18_basic'
basis_report = (basis_root / 'result.json').read_bytes()
assert json.loads(basis_report)['status'] == 'PASS_SCOPED_ACQUISITION_REGISTRATION'
output = root / 'jobs'
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.acquisition-jobs'
client = 'bigbrotr-acquisition-jobs-' + revision[:7]
report = dict(format='bigbrotr-acquisition-jobs/1', source=revision, build=built,
    status='RUNNING', owner=owner, label=label, client=None, cases=[], databases=[],
    postgres=pg['postgres'], postgres_owner=pg['owner'],
    not_run=['concurrent locks, cancellation and lost/delayed COMMIT',
             'remaining raw SQL relational negatives and historical grant/policy retry',
             'claim/dispatch, installed Sync, socket admission and M1',
             'populated prior-job migration, aged/mixed load, arm64/release'])


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
    # Existing audit/owner/session rows remain immutable; only this probe's
    # lifecycle and real login may add rows. A refusal adds no acceptance audit.
    for table in mutable - {'login_buckets'}:
        require(all(row in after[table] for row in before[table]), 'prior-rows-' + table)
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
    require(basic == json.loads((basis_root / 'basic-after.json').read_text()), 'qualified-basis-unchanged')
    require(not basic['jobs'], 'empty-job-ledger-basis')
    policy = basic['config_revisions'][0]['document']
    require(policy == basic['policy_revisions'][0]['document'] and policy['format'] == 'bigbrotr-product-policy/2', 'fixture-policy')
    (output / 'policy.json').write_text(json.dumps(policy, sort_keys=True, separators=(',', ':')))
    ident = lab.docker('create', '--init', '--name', client, '--label', label + '=' + owner,
        '--network', 'container:' + pg['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
        '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128', '--read-only',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
        '--mount', f'type=bind,src={output},dst=/lab',
        '--mount', f'type=bind,src={pg_root}/secrets,dst=/run/secrets,readonly',
        '--mount', f'type=bind,src={build},dst=/build,readonly', pg['client_image'],
        'python3', '-c', 'import time; time.sleep(600)').stdout.strip()
    report['client'] = dict(id=ident, name=client); save(); lab.docker('start', ident)
    for name in ('pass', 'no-grant', 'stale-policy', 'disabled', 'expired', 'rollback', 'old-schema'):
        run_case(name)
    raw_timestamp_guards()
    record('retained-basis-unchanged', rows(basis) == basic and (basis_root / 'result.json').read_bytes() == basis_report)
    report['status'] = 'PASS_FINITE_ACQUISITION_ACCEPTANCE'
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error)); raise
finally:
    errors = []
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
require(report['status'] == 'PASS_FINITE_ACQUISITION_ACCEPTANCE', 'incomplete-jobs')
print(json.dumps(dict(status=report['status'], checks=len(report['cases']))))
