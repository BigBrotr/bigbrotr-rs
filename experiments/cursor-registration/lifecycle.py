#!/usr/bin/env python3
"""Actual maintenance CLI, SQL closure and Control readiness; no cursor issuance."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
from fault_cases import Process


class Fixture:
    def __init__(self, root):
        self.root = root
        self.state = lab.load(root)
        self.output = root / 'cursor-lifecycle.json'
        assert not self.output.exists()
        revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
        assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
        info = json.loads(lab.cli(self.state, 'build-info').stdout)
        assert info == self.state['build']['build_info']
        if info['source']['revision'] != revision:
            origin = json.loads((root.parent / 'artifact-origin.json').read_text())
            assert origin['source'] == info['source']['revision'] and origin['fixture_revision'] == revision
            manifest_path = Path(origin['root']) / 'out/build/source-manifest.json'
            assert hashlib.sha256(manifest_path.read_bytes()).hexdigest() == info['source']['manifest_sha256']
            manifest = json.loads(manifest_path.read_text())
            changed = {'experiments/cursor-registration/README.md', 'experiments/cursor-registration/retry.py',
                       'experiments/cursor-registration/lifecycle.py', 'experiments/cursor-registration/lifecycle_faults.py'}
            def relevant(name):
                return (not name.startswith('docs/') or name.startswith('docs/reference/')) and name not in changed
            current = lab.command(['git', '-C', str(SOURCE), 'ls-files']).stdout.splitlines()
            names = {name for name in current if relevant(name)}
            assert names == {name for name in manifest['files_sha256'] if relevant(name)}
            assert all(hashlib.sha256((SOURCE / name).read_bytes()).hexdigest() == manifest['files_sha256'][name] for name in names)
        assert lab.docker('exec', self.state['client'], 'sha256sum', '/build/bigbrotr').stdout.split()[0] == self.state['binary_sha256']
        self.vectors = json.loads((SOURCE / 'tests/fixtures/read/keyring-vectors.json').read_text())
        self.wire = bytes.fromhex(self.vectors['cases'][0]['hex'])
        self.a, self.b = self.wire[42:58].hex(), self.wire[90:106].hex()
        self.processes = []
        self.report = dict(source=revision, binary_source=info['source']['revision'], binary_sha256=self.state['binary_sha256'], server=self.state['server'],
                           status='RUNNING', cases=[], databases=[])
        self.fault = self.state['client'] + '-cursor-fault'
        self.save()

    def save(self):
        temporary = self.output.with_suffix('.partial')
        temporary.write_text(json.dumps(self.report, indent=2) + '\n')
        temporary.replace(self.output)

    def record(self, name, passed, **detail):
        assert not any(c['name'] == name for c in self.report['cases'])
        self.report['cases'].append(dict(name=name, status='PASS' if passed else 'FAIL', **detail))
        self.save()
        if not passed:
            raise RuntimeError('case failed: ' + name)

    def setup(self):
        password = (self.root / 'secrets/application').read_text()
        lab.sql(self.state, 'postgres', f"CREATE ROLE bigbrotr_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}';")
        del password
        mounts = json.loads(lab.docker('inspect', self.state['client']).stdout)[0]['Mounts']
        build = next(m['Source'] for m in mounts if m['Destination'] == '/build' and not m['RW'])
        assert lab.docker('inspect', self.fault, check=False).returncode != 0
        cid = lab.docker('run', '-d', '--init', '--name', self.fault,
                        '--label', 'org.bigbrotr.cursor-registration=' + self.state['owner'],
                        '--network', 'container:' + self.state['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
                        '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128',
                        '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                        '--mount', f'type=bind,src={self.root},dst=/lab',
                        '--mount', f'type=bind,src={build},dst=/build,readonly',
                        '--mount', f'type=bind,src={SOURCE},dst=/source,readonly', self.state['client_image'],
                        'python3', '-c', 'import time; time.sleep(1800)').stdout.strip()
        self.report['fault_container'] = dict(id=cid, name=self.fault, owner=self.state['owner'])
        self.save()
        self.client = {**self.state, 'client': self.fault}

    def prepare(self, name, target=604):
        database = 'bb_lifecycle_' + name
        lab.sql(self.state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
        self.report['databases'].append(database)
        self.save()
        path = lab.config(self.root, self.state, database, endpoint='127.0.0.1:5432', boundary='loopback')
        local = self.root / Path(path).name
        text, count = re.subn(r'(?ms)^local_roles = \[.*?\]', 'local_roles = ["control"]', local.read_text())
        assert count == 1
        text = lab.change(text, 'topology', 'dataset_id', self.vectors['dataset_id'])
        text = lab.change(text, 'administration', 'enabled', False)
        text = lab.change(text, 'postgres.control', 'password_ref', 'file:/lab/secrets/application')
        text += '\n[runtime]\nlease_ttl = "2500ms"\nheartbeat_interval = "100ms"\nrenewal_timeout = "2s"\nrenewal_safety_margin = "100ms"\n'
        local.write_text(text)
        result = lab.migrate(self.client, 'apply', path, target=target)
        self.record('migrate-' + name, result.returncode == 0, stderr=result.stderr)
        lab.sql(self.state, database, '''GRANT USAGE ON SCHEMA control TO bigbrotr_control,bigbrotr_runtime;
            GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA control TO bigbrotr_control;
            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control,bigbrotr_runtime;
            GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_runtime;
            GRANT INSERT,UPDATE ON control.role_instances,control.owner_epochs TO bigbrotr_runtime;
            GRANT UPDATE(config_revision) ON control.datasets TO bigbrotr_runtime;''')
        for args in [('config', 'initialize'), ('config', 'activate-initial', '--request-id', '71'*16, '--expected-revision', '1')]:
            result = lab.cli(self.client, *args, '--file', path)
            self.record(name + '-' + args[1], result.returncode == 0, stderr=result.stderr)
        directory = self.root / ('keys-' + name)
        directory.mkdir(mode=0o700)
        file = directory / 'ring'
        file.write_bytes(self.wire)
        file.chmod(0o600)
        local.write_text(text + f'\n[read.cursor]\nkeyring_ref = "file:/lab/{directory.name}/ring"\n')
        result = lab.cli(self.client, 'access', 'cursor-keyring', 'register', '--file', path)
        self.record('register-' + name, result.returncode == 0, stderr=result.stderr)
        runtime = self.root / (name + '-runtime.toml')
        runtime.write_text(lab.change(local.read_text(), 'postgres.control', 'user', 'bigbrotr_runtime'))
        return database, path, file, '/lab/' + runtime.name

    def snapshot(self, database):
        tables = json.loads(lab.sql(self.state, database, """SELECT json_agg(tablename ORDER BY tablename)
            FROM pg_tables WHERE schemaname='control' AND tablename IN
            ('cursor_keyrings','cursor_keys','cursor_key_materials','cursor_key_transitions',
             'cursor_key_lifecycle','management_audit','datasets','config_revisions','policy_revisions');"""))
        return {table: json.loads(lab.sql(self.state, database, f"SELECT coalesce(json_agg(t ORDER BY row_to_json(t)::text),'[]') FROM (SELECT *,xmin::text AS row_version FROM control.{table}) t;")) for table in tables}

    def change(self, name, database, path, action, key, request, expected, code=0, created=True):
        before = self.snapshot(database)
        result = lab.cli(self.client, 'access', 'cursor-keyring', action + '-key', '--file', path,
                         '--key-id', key, '--request-id', request, '--expected-revision', str(expected))
        value = json.loads(result.stdout) if result.stdout else None
        after = self.snapshot(database)
        self.record(name, result.returncode == code and (code != 0 or value is not None),
                    exit=result.returncode, output=value, stderr=result.stderr)
        if code:
            self.record(name + '-unchanged', not result.stdout and before == after)
            return None
        receipt = next(row for row in after['cursor_key_transitions'] if row['request_id'] == '\\x' + request)
        actor = receipt['actor_id'][2:]
        digest = self.digest(actor, action, key, request, expected)
        expected_output = dict(format='bigbrotr-cursor-key-lifecycle/1',
            state='accepted' if created else 'already-accepted', dataset_id=self.vectors['dataset_id'],
            keyring_id=self.vectors['keyring_id'], request_id=request, action=action, key_id=key,
            expected_revision=str(expected), revision=str(expected+1),
            active_key_id=receipt['resulting_active_key_id'][2:] if receipt['resulting_active_key_id'] else None,
            actor_id=actor, audit_id=receipt['audit_id'][2:], accepted_at_us=str(receipt['accepted_at_us']),
            request_digest_sha256=digest, cursor_authority='not-granted')
        audit = [r for r in after['management_audit'] if r['audit_id'] == receipt['audit_id']]
        expected_audit = dict(audit_id=receipt['audit_id'], dataset_id=receipt['dataset_id'], actor_id=receipt['actor_id'],
            action='cursor-key.'+action, outcome='accepted', created_at_us=receipt['accepted_at_us'],
            config_revision=None, policy_revision=None, subject_id='\\x'+key, reason=None,
            requester_database_role='bigbrotr_control', requester_os_uid=os.getuid(), row_version=receipt['row_version'])
        self.record(name + '-receipt', value == expected_output and audit == [expected_audit]
                    and receipt['request_digest'] == '\\x'+digest and receipt['expected_revision'] == expected,
                    observed=after)
        if not created:
            self.record(name + '-no-write', before == after)
        return value

    def digest(self, actor, action, key, request, expected):
        wire = b'bigbrotr/cursor-key-lifecycle/1\0'
        wire += bytes.fromhex(self.vectors['dataset_id'] + self.vectors['keyring_id'] + actor + request)
        wire += bytes([1 if action == 'activate' else 2]) + bytes.fromhex(key) + expected.to_bytes(8, 'big')
        return hashlib.sha256(wire).hexdigest()

    def process(self, args, **options):
        process = Process(args, **options)
        self.processes.append(process)
        return process

    def start(self, path, name, container=None):
        return self.process(['docker', 'exec', container or self.fault, 'python3', '-B',
            '/source/experiments/cursor-registration/runtime.py', '--child', '--config', path,
            '--record', '/lab/' + name + '.process.json'])

    def idle(self, process, name):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if b'\n' in process.stdout:
                observed = json.loads(bytes(process.stdout).splitlines()[0])
                self.record(name, observed['state'] == 'idle' and not process.stderr, observed=observed)
                return observed
            assert process.process.poll() is None, bytes(process.stderr).decode()
            time.sleep(.02)
        raise TimeoutError('no runtime idle: ' + name)

    def stop(self, process, name, signal='TERM', container=None):
        lab.docker('exec', container or self.fault, 'python3', '-B', '/source/experiments/cursor-registration/runtime.py',
                   '--signal', signal, '--record', '/lab/' + name + '.process.json')
        self.record('stop-' + name, process.wait(10) == (137 if signal == 'KILL' else 0), stderr=bytes(process.stderr).decode())

    def quiescent(self, database):
        deadline = time.monotonic() + 8
        while lab.sql(self.state, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename IN ('bigbrotr_control','bigbrotr_runtime');") != '0':
            assert time.monotonic() < deadline
            time.sleep(.02)

    def close(self):
        errors = []
        for index, process in enumerate(self.processes):
            try:
                process.stop()
                (self.root / f'lifecycle-process-{index}.stdout').write_bytes(process.stdout)
                (self.root / f'lifecycle-process-{index}.stderr').write_bytes(process.stderr)
                self.report.setdefault('processes', []).append(dict(args=process.process.args, exit=process.process.poll(), overflow=process.overflow))
            except BaseException as error:
                errors.append(dict(process=index, error=type(error).__name__))
        self.report['process_cleanup_errors'] = errors
        self.report['status'] = 'PASS' if not errors and self.report['status'] == 'CHECKS_PASSED' else 'INCOMPLETE'
        self.save()
        if errors:
            raise RuntimeError('owned process cleanup incomplete; enclosing owner must stop containers')


def journey(f):
    db, path, file, runtime = f.prepare('journey', 815)
    before = f.snapshot(db)
    f.change('old-schema-refuses', db, path, 'activate', f.a, '10'*16, 0, code=3)
    result = lab.migrate(f.client, 'apply', path, target=604)
    f.record('explicit-upgrade', result.returncode == 0, stderr=result.stderr)
    lab.sql(f.state, db, 'GRANT SELECT,INSERT,UPDATE,DELETE ON control.cursor_key_transitions,control.cursor_key_lifecycle TO bigbrotr_control; GRANT SELECT ON control.cursor_key_transitions,control.cursor_key_lifecycle TO bigbrotr_runtime; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control,bigbrotr_runtime;')
    after = f.snapshot(db)
    f.record('upgrade-preserves-original-rows', all(after[k] == v for k, v in before.items()), before=before, after=after)
    first = f.change('first', db, path, 'activate', f.a, '11'*16, 0)
    f.record('first-active', first['active_key_id'] == f.a)
    f.change('retry', db, path, 'activate', f.a, '11'*16, 0, created=False)
    for name, action, key, request, expected in [
        ('changed-request', 'activate', f.b, '11'*16, 0),
        ('same-active-new-request', 'activate', f.a, '12'*16, 1),
        ('stale-revision', 'activate', f.b, '12'*16, 0),
        ('unenrolled', 'activate', '55'*16, '12'*16, 1),
    ]:
        f.change(name, db, path, action, key, request, expected, code=3)
    live = f.start(runtime, 'live')
    f.idle(live, 'live-ready')
    f.change('retry-while-live', db, path, 'activate', f.a, '11'*16, 0, created=False)
    f.change('new-while-live', db, path, 'activate', f.b, '12'*16, 1, code=3)
    f.stop(live, 'live')
    second = f.change('rotate-b', db, path, 'activate', f.b, '12'*16, 1)
    f.record('second-active', second['active_key_id'] == f.b)
    historical = f.change('old-after-rotation', db, path, 'activate', f.a, '11'*16, 0, created=False)
    f.record('historical-result-keeps-original-active', historical['active_key_id'] == f.a)
    # Valid enrolled subset, but the active B is absent. This discriminates602.
    body = f.wire[:40] + b'\0\1' + f.wire[42:90]
    file.write_bytes(body + hashlib.sha256(body).digest())
    refused = f.start(runtime, 'missing-active')
    f.record('missing-active-refuses', refused.wait(10) == 3 and not refused.stdout
             and b'IncompatibleDeployment' in refused.stderr, stderr=bytes(refused.stderr).decode())
    file.write_bytes(f.wire)
    recovered = f.start(runtime, 'recovered')
    f.idle(recovered, 'restored-snapshot-recovers')
    f.stop(recovered, 'recovered')
    f.change('rotate-back-a', db, path, 'activate', f.a, '13'*16, 2)
    rows = f.snapshot(db)['cursor_key_lifecycle']
    f.record('first-activation-preserved', next(r for r in rows if r['key_id'] == '\\x'+f.a)['first_activation_revision'] == 1)
    revoked = f.change('revoke-active-a', db, path, 'revoke', f.a, '14'*16, 3)
    f.record('revoked-active-has-no-issuer', revoked['active_key_id'] is None)
    f.change('irreversible', db, path, 'activate', f.a, '15'*16, 4, code=3)
    # Enroll C, then remove it from the prepared snapshot before revocation.
    for operation in ['prepare-key', 'enroll-key']:
        result = lab.cli(f.client, 'access', 'cursor-keyring', operation, '--file', path, '--key-id', '55'*16)
        f.record('unused-key-' + operation, result.returncode == 0, stderr=result.stderr)
    file.write_bytes(f.wire)
    f.change('revoke-unloaded-never-active', db, path, 'revoke', '55'*16, '15'*16, 4)
    f.change('revoke-unenrolled', db, path, 'revoke', '56'*16, '16'*16, 5, code=3)
    f.change('activate-b-again', db, path, 'activate', f.b, '16'*16, 5)
    crash = f.start(runtime, 'crash')
    f.idle(crash, 'crash-ready')
    f.stop(crash, 'crash', 'KILL')
    f.quiescent(db)
    deadline = time.monotonic() + 5
    while True:
        owner = json.loads(lab.sql(f.state, db, "SELECT json_build_object('state',state,'expires',lease_expires_at_us,'now',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint) FROM control.owner_epochs WHERE state<>'released';"))
        if owner['now'] >= owner['expires']:
            break
        assert time.monotonic() < deadline
        time.sleep(.05)
    f.record('expired-unreleased-owner', owner['state'] == 'active', observed=owner)
    f.change('expired-is-not-quiescence', db, path, 'revoke', f.b, '17'*16, 6, code=3)
    f.change('historical-after-crash', db, path, 'activate', f.a, '11'*16, 0, created=False)
    recovery = f.start(runtime, 'crash-recovery')
    f.idle(recovery, 'crash-recovery-ready')
    f.stop(recovery, 'crash-recovery')
    f.change('released-permits-revoke', db, path, 'revoke', f.b, '17'*16, 6)
    f.report['journey'] = dict(database=db, config=path, runtime=runtime)


def main(root, faults_only=False):
    f = Fixture(root)
    try:
        f.setup()
        if not faults_only:
            journey(f)
            from lifecycle_sql import cases as sql_cases
            sql_cases(f)
        from lifecycle_faults import cases as fault_cases
        fault_cases(f)
        for database in f.report['databases']:
            f.quiescent(database)
            f.record('no-backends-' + database, lab.sql(f.state, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename IN ('bigbrotr_control','bigbrotr_runtime','bigbrotr_migrator');") == '0')
        f.report['not_run'] = ['cursor issuance/retention writer', 'online multiworker rollout', 'browser pagination', 'host power loss', 'capacity and release qualification']
        if faults_only:
            f.report['not_run'].append('unchanged journey and SQL cases retained in original a11bcac result')
        f.report['status'] = 'CHECKS_PASSED'
    except BaseException as error:
        f.report.update(status='INCOMPLETE', failure=str(error))
        raise
    finally:
        f.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--faults-only', action='store_true')
    args = parser.parse_args()
    main(args.root.resolve(strict=True), args.faults_only)
