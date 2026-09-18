#!/usr/bin/env python3
"""Actual CLI/PG first-owner transitions; generated tokens never enter the report."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
from fault_cases import Process


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    root = parser.parse_args().root.resolve(strict=True)
    state = lab.load(root)
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    require(not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty-source')
    require(revision == state['build']['build_info']['source']['revision'] and Path(state['source']) == SOURCE, 'source-identity')
    require(json.loads(lab.cli(state, 'build-info').stdout) == state['build']['build_info'], 'mounted-build-identity')
    require(lab.docker('exec', state['client'], 'sha256sum', '/build/bigbrotr').stdout.split()[0] == state['binary_sha256'], 'mounted-binary-bytes')
    mounts = json.loads(lab.docker('inspect', state['client']).stdout)[0]['Mounts']
    build = Path(next(m['Source'] for m in mounts if m['Destination'] == '/build' and not m['RW']))
    output = root / 'owner-enrollment.json'
    require(not output.exists(), 'existing-report')
    report = {'format': 'bigbrotr-owner-enrollment-native/1', 'source': revision, 'binary_sha256': state['binary_sha256'], 'status': 'RUNNING', 'cases': [], 'fault_containers': [], 'observations': {}, 'server': state['server']}
    databases = []
    credential_observations = {}
    password = 'Independent owner password 2026'
    password_file = root / 'secrets' / 'owner-password'
    password_file.write_text(password); password_file.chmod(0o600)
    wrong_file = root / 'secrets' / 'wrong-token'
    wrong_file.write_text('00' * 32); wrong_file.chmod(0o600)
    request = '31' * 16

    def save():
        lab.write_json(output, report)

    def prepare(name, target=805):
        database = 'bb_owner_' + name
        lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
        databases.append(database)
        path = lab.config(root, state, database)
        local = root / Path(path).name
        local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application'))
        migrated = lab.migrate(state, 'apply', path, target=target)
        require(migrated.returncode == 0 and json.loads(migrated.stdout)['current_version'] == str(target), 'migration-' + name)
        lab.sql(state, database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT ON control.datasets, control.config_revisions, control.principals, control.management_audit TO bigbrotr_control; GRANT UPDATE(config_revision) ON control.datasets TO bigbrotr_control;')
        require(lab.cli(state, 'config', 'initialize', '--file', path).returncode == 0, 'initialize-' + name)
        if target == 805:
            lab.sql(state, database, 'GRANT INSERT ON control.local_accounts, control.local_credentials, control.principal_grants, control.owner_challenges TO bigbrotr_control; GRANT UPDATE(state, failed_attempts, consumed_principal, consumed_at_us) ON control.owner_challenges TO bigbrotr_control;')
        return database, path

    def arguments(action, path, challenge=request, token=None, username='alice'):
        if action == 'enroll':
            return ['access', 'enroll-owner', '--file', path, '--challenge-id', challenge, '--token-ref', token, '--password-ref', 'file:/lab/secrets/owner-password']
        args = ['access', 'owner-token', action, '--file', path]
        if action == 'issue': args += ['--request-id', challenge, '--username', username]
        if action == 'revoke': args += ['--challenge-id', challenge]
        return args

    def result(name, process, code, status=None):
        value = json.loads(process.stdout) if process.stdout else None
        secret = value.pop('token', None) if value else None
        passed = process.returncode == code and (status is None or value['status'] == status)
        if value:
            passed = passed and value['runtime_authorization'] == 'not-granted' and value['session'] == 'not-issued'
        report['cases'].append({'name': name, 'status': 'PASS' if passed else 'FAIL', 'exit': process.returncode, 'output': value, 'stderr': process.stderr.decode() if isinstance(process.stderr, bytes) else str(process.stderr), 'secret_returned': secret is not None})
        save(); require(passed, 'case-' + name)
        return value, secret

    def call(name, database, args, code=0, status=None, unchanged=False):
        before = lab.schema_digest(state, database)
        value, secret = result(name, lab.cli(state, *args), code, status)
        if unchanged: require(before == lab.schema_digest(state, database), 'unexpected-write-' + name)
        return value, secret

    def issue(name, database, path, challenge=request):
        value, secret = call(name, database, arguments('issue', path, challenge), status='issued')
        require(secret is not None and len(secret) == 64 and set(secret) <= set('0123456789abcdef'), 'token-shape')
        token_file = root / 'secrets' / name
        token_file.write_text(secret); token_file.chmod(0o600)
        stored = lab.sql(state, database, f"SELECT encode(token_digest,'hex') FROM control.owner_challenges WHERE challenge_id=decode('{challenge}','hex');")
        require(stored == sha(b'bigbrotr/first-owner-token/1\0' + bytes.fromhex(secret)), 'hash-only-token')
        require(int(value['expires_at_us']) > 0, 'positive-expiry')
        return 'file:/lab/secrets/' + name, value

    def observe(database, expected_owner):
        names = json.loads(lab.sql(state, database, "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control';"))
        require(len(names) == 29, 'table-inventory')
        tables = {name: json.loads(lab.sql(state, database, f"SELECT coalesce(json_agg(t),'[]') FROM control.{name} t;")) for name in names}
        allowed = {'schema_versions', 'datasets', 'config_revisions', 'principals', 'management_audit', 'owner_challenges', 'local_accounts', 'local_credentials', 'principal_grants'}
        require(all(not rows for name, rows in tables.items() if name not in allowed), 'unexpected-runtime-effects')
        require(len(tables['datasets']) == len(tables['config_revisions']) == 1, 'bootstrap-preserved')
        require(tables['datasets'][0]['policy_revision'] is None and tables['datasets'][0]['topology_generation'] is None, 'no-active-authority')
        require(len(tables['local_accounts']) == len(tables['local_credentials']) == expected_owner, 'owner-cardinality')
        require(len(tables['principals']) == 1 + expected_owner, 'principal-cardinality')
        require(len(tables['principal_grants']) == 7 * expected_owner, 'grant-cardinality')
        if expected_owner:
            account, credential = tables['local_accounts'][0], tables['local_credentials'][0]
            require(set(account) == {'principal_id', 'dataset_id', 'username', 'initial_owner', 'active', 'credential_revision', 'grant_revision'}, 'account-columns')
            require(set(credential) == {'principal_id', 'dataset_id', 'credential_revision', 'format', 'argon_version', 'memory_kib', 'passes', 'lanes', 'salt', 'password_hash'}, 'credential-columns')
            principal = account['principal_id']
            require(account['username'] == 'alice' and account['active'] and account['initial_owner'] and account['credential_revision'] == account['grant_revision'] == 1, 'owner-account')
            require(credential['principal_id'] == principal and credential['dataset_id'] == account['dataset_id'] and credential['credential_revision'] == 1, 'credential-binding')
            require(credential['format'] == 'argon2id-v1' and [credential[k] for k in ['argon_version', 'memory_kib', 'passes', 'lanes']] == [19, 65536, 3, 1], 'password-profile')
            require(len(bytes.fromhex(credential['salt'][2:])) == 16 and len(bytes.fromhex(credential['password_hash'][2:])) == 32, 'password-bytes')
            credential_observations[database] = {'principal_id': principal, 'salt': credential['salt'][2:], 'password_hash': credential['password_hash'][2:]}
            credential_path = root / 'credential-observations.json'
            with os.fdopen(os.open(credential_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'w') as file:
                json.dump(credential_observations, file, indent=2)
            require({r['action'] for r in tables['principal_grants']} == {'data.read', 'status.read', 'roles.control', 'budgets.change', 'policy.change', 'principals.manage', 'audit.read'}, 'explicit-admin-actions')
            require(all(r['principal_id'] == principal and r['dataset_id'] == r['resource_id'] == account['dataset_id'] and r['resource_kind'] == 'dataset' for r in tables['principal_grants']), 'grant-scope')
            consumed = [r for r in tables['owner_challenges'] if r['state'] == 'consumed']
            require(len(consumed) == 1 and consumed[0]['consumed_principal'] == consumed[0]['intended_principal'] == principal, 'single-consume')
            audit = [r for r in tables['management_audit'] if r['action'] == 'owner.enroll' and r['outcome'] == 'accepted']
            require(len(audit) == 1 and audit[0]['actor_id'] == principal and audit[0]['subject_id'] == consumed[0]['challenge_id'], 'accepted-audit')
        # The complete table digest is evidence of observations, not an expected
        # oracle. Password/token hashes and generated secrets stay out of reports.
        report['observations'][database] = {name: {'rows': len(rows), 'sha256': sha(json.dumps(rows, sort_keys=True).encode())} for name, rows in tables.items()}
        save()

    try:
        expired_db, expired_path = prepare('expired')
        expired_token, expired_receipt = issue('expiry-token', expired_db, expired_path)
        database, path = prepare('basic')
        token, first = issue('initial-token', database, path)
        repeat, secret = call('same-issuance', database, arguments('issue', path), status='secret-unavailable', unchanged=True)
        require(secret is None and repeat['challenge_id'] == first['challenge_id'], 'retry-secret-disclosure')
        call('changed-name', database, arguments('issue', path, username='mallory'), 3, unchanged=True)
        call('pending-replacement', database, arguments('issue', path, '32' * 16), 3, unchanged=True)
        call('revoke', database, arguments('revoke', path), status='revoked')
        call('revoke-retry', database, arguments('revoke', path), status='already-revoked', unchanged=True)
        call('revoked-token', database, arguments('enroll', path, token=token), 3, 'terminal')
        token, second = issue('replacement-token', database, path, '32' * 16)
        call('wrong-token', database, arguments('enroll', path, '32' * 16, 'file:/lab/secrets/wrong-token'), 3, 'invalid-token')
        enrolled, _ = call('enroll', database, arguments('enroll', path, '32' * 16, token), status='enrolled')
        observe(database, 1)
        before = lab.schema_digest(state, database)
        lab.sql(state, database, (SOURCE / 'experiments/local-enrollment/schema_cases.sql').read_text())
        require(before == lab.schema_digest(state, database), 'negative-sql-mutated-state')
        report['cases'].append({'name': 'schema-negatives', 'status': 'PASS', 'cases': 15, 'sqlstate': '23514', 'unchanged_dump': before}); save()
        call('replay', database, arguments('enroll', path, '32' * 16, token), 3, unchanged=True)
        call('replacement-after-owner', database, arguments('issue', path, '33' * 16), 3, unchanged=True)
        status, _ = call('status', database, arguments('status', path), status='enrolled', unchanged=True)
        require(status['principal_id'] == enrolled['principal_id'] and status['audit_id'] == enrolled['audit_id'], 'receipt-resolution')
        dump = lab.schema_digest(state, database)
        lab.docker('restart', state['postgres'])
        deadline = time.monotonic() + 15
        while lab.docker('exec', state['postgres'], 'pg_isready', '-U', 'postgres', check=False).returncode:
            require(time.monotonic() < deadline, 'restart-timeout'); time.sleep(.1)
        call('restart', database, arguments('status', path), status='enrolled', unchanged=True)
        require(lab.schema_digest(state, database) == dump, 'restart-durable-state')
        database, path = prepare('denied')
        token, _ = issue('denied-token', database, path)
        lab.sql(state, database, 'REVOKE INSERT ON control.principal_grants FROM bigbrotr_control;')
        call('grant-insert-rollback', database, arguments('enroll', path, token=token), 1, unchanged=True)
        observe(database, 0)
        lab.sql(state, database, 'GRANT INSERT ON control.principal_grants TO bigbrotr_control;')
        call('grant-insert-recovery', database, arguments('enroll', path, token=token), status='enrolled'); observe(database, 1)
        database, path = prepare('old', 801)
        call('old-schema', database, arguments('issue', path), 3, unchanged=True)
        database, path = prepare('exhausted')
        issue('exhaust-token', database, path)
        for attempt in range(32):
            value, _ = call('guess-' + str(attempt + 1), database, arguments('enroll', path, token='file:/lab/secrets/wrong-token'), 3, 'invalid-token')
            require(value['failed_attempts'] == attempt + 1, 'attempt-counter')
        call('exhausted', database, arguments('enroll', path, token='file:/lab/secrets/wrong-token'), 3, 'exhausted'); observe(database, 0)

        for scenario, mode in [('before', 'before-forward'), ('after', 'after-complete'), ('race', 'hold'), ('killed', 'hold')]:
            database, path = prepare(scenario)
            token, _ = issue(scenario + '-token', database, path)
            container = state['client'] + '-' + scenario
            require(lab.docker('container', 'inspect', container, check=False).returncode != 0, 'existing-fault-container')
            record = {'name': container, 'owner': state['owner'], 'state': 'creating'}
            report['fault_containers'].append(record); save()
            lab.docker('run', '-d', '--init', '--name', container, '--label', 'org.bigbrotr.enrollment-fault=' + state['owner'], '--network', 'container:' + state['postgres'], '--user', f'{os.getuid()}:{os.getgid()}', '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--mount', f'type=bind,src={root},dst=/lab', '--mount', f'type=bind,src={build},dst=/build,readonly', '--mount', f'type=bind,src={SOURCE},dst=/source,readonly', state['client_image'], 'python3', '-c', 'import time; time.sleep(180)')
            record['id'] = json.loads(lab.docker('inspect', container).stdout)[0]['Id']; record['state'] = 'running'; save()
            processes = []
            try:
                proxy = Process(['docker', 'exec', '-i', container, 'python3', '-B', '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432', '--mode', mode, '--deadline-seconds', '60'], input_pipe=True); processes.append(proxy)
                ready = proxy.event('ready', 10)
                require(ready['proxy_sha256'] == sha((SOURCE / 'experiments/control-migrations/fault_proxy.py').read_bytes()), 'proxy-source')
                proxyfile = root / (database + '_proxy.toml')
                original = (root / Path(path).name).read_text()
                proxyfile.write_text(lab.change(lab.change(original, 'postgres.control', 'endpoint', f'127.0.0.1:{ready["listen_port"]}'), 'postgres.control.tls', 'local_boundary', 'loopback'))
                app = Process(['docker', 'exec', container, '/build/bigbrotr', *arguments('enroll', '/lab/' + proxyfile.name, token=token)]); processes.append(app)
                if mode == 'hold':
                    proxy.event('held', 15); observe(database, 0)
                    if scenario == 'race':
                        other = Process(['docker', 'exec', state['client'], '/build/bigbrotr', *arguments('enroll', path, token=token)]); processes.append(other)
                        deadline = time.monotonic() + 5
                        while True:
                            locks = json.loads(lab.sql(state, database, "SELECT coalesce(json_agg(json_build_object('mode',mode,'granted',granted)),'[]') FROM pg_locks WHERE locktype='advisory' AND classid=1111651442 AND objid=1331121746;"))
                            if any(r['granted'] for r in locks) and any(not r['granted'] for r in locks): break
                            require(time.monotonic() < deadline, 'concurrent-owner-lock'); time.sleep(.05)
                        report['race_locks'] = locks; proxy.send('release')
                        require(other.wait(15) == 3, 'concurrent-owner-refusal')
                    else: lab.docker('kill', container)
                code = app.wait(15)
                if scenario != 'killed':
                    value = json.loads(app.stdout)
                    require(code == (0 if scenario == 'race' else 4) and value['status'] == ('enrolled' if scenario == 'race' else 'outcome-unknown') and not app.stderr, 'fault-outcome')
                    receipt = proxy.event('receipt', 10)
                    require([receipt[k] for k in ['commits_seen', 'commits_forwarded', 'server_commit_complete', 'client_commit_complete', 'faults']] == {'before': [1, 0, 0, 0, 1], 'after': [1, 1, 1, 0, 1], 'race': [1, 1, 1, 1, 1]}[scenario], 'commit-boundary')
                    require(proxy.wait(5) == 0 and not proxy.stderr, 'proxy-terminal')
                else:
                    require(code == 137, 'actual-kill'); receipt = None
                report['cases'].append({'name': 'commit-' + scenario, 'status': 'PASS', 'exit': code, 'proxy': receipt}); save()
                present = scenario in ['after', 'race']
                call('resolve-' + scenario, database, arguments('status', path), status='enrolled' if present else 'pending')
                if not present: call('retry-' + scenario, database, arguments('enroll', path, token=token), status='enrolled')
                observe(database, 1)
            finally:
                info = json.loads(lab.docker('inspect', container).stdout)[0]
                require(info['Id'] == record['id'] and info['Config']['Labels']['org.bigbrotr.enrollment-fault'] == state['owner'], 'fault-owner')
                if info['State']['Running']: lab.docker('stop', '--time', '3', container)
                for process in processes: process.stop()
                record['state'] = 'stopped-retained'; record['terminal'] = json.loads(lab.docker('inspect', container).stdout)[0]['State']; save()
        # Exercise the real ten-minute lifetime, not altered timestamps or a
        # cheaper product test mode. Other cases run while this clock elapses.
        while time.time_ns() // 1000 <= int(expired_receipt['expires_at_us']): time.sleep(1)
        call('expired-token', expired_db, arguments('enroll', expired_path, token=expired_token), 3, 'expired'); observe(expired_db, 0)
        for database in databases:
            require(int(lab.sql(state, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename IN ('bigbrotr_control','bigbrotr_migrator');")) == 0, 'remaining-backend')
        report['status'] = 'PASS'
    except Exception as error:
        report['status'] = 'FAIL'; report['failure'] = str(error); raise
    finally: save()
    print(json.dumps({'status': report['status'], 'cases': len(report['cases']), 'report': str(output)}))


if __name__ == '__main__':
    main()
