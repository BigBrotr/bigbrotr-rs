#!/usr/bin/env python3
"""Actual finite catalog CLI against fresh native and malformed PostgreSQL state.

Only an exact-source owned isolated lab is accepted. Credentials and PKI remain
inside that lab; logical dumps retain schema and sorted COPY rows, not secrets.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
import tls_cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    root = parser.parse_args().root.resolve(strict=True)
    state = json.loads((root / 'resources.json').read_text())
    assert state['root'] == str(root) and state['state'] == 'running'
    assert lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip() == state['source']
    assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain']).stdout
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source']
    assert hashlib.sha256((root / 'out/native-tests/bigbrotr').read_bytes()).hexdigest() == ready['control']
    for key in ('container_id', 'postgres_id'):
        info = json.loads(lab.docker('inspect', state[key]).stdout)[0]
        assert info['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
        assert info['State']['Running'] and not info['HostConfig']['PortBindings']
    network = json.loads(lab.docker('network', 'inspect', state['network_id']).stdout)[0]
    assert network['Internal'] and network['Labels']['org.bigbrotr.publication'] == state['owner']
    work = root / 'out/catalog-cli'
    work.mkdir(mode=0o700)
    report = dict(source=state['source'], binary_sha256=ready['control'], status='RUNNING', cases=[], containers=[])
    output = root / 'catalog-cli.json'
    assert not output.exists()

    def save(): lab.write_json(output, report)
    def sql(database, text): return lab.sql(state, database, text)
    def command(*args): return lab.docker('exec', state['container_id'], *args, check=False)
    def cli(*args): return command('/out/native-tests/bigbrotr', *args)
    def snapshot(database):
        dump = lab.docker('exec', state['postgres_id'], 'pg_dump', '-U', 'postgres', '-d', database).stdout
        stable, rows = [], None
        for line in dump.splitlines():
            if line.startswith(('\\restrict ', '\\unrestrict ')): continue
            if line.startswith('COPY '):
                assert rows is None
                stable.append(line); rows = []
            elif rows is not None and line == '\\.':
                stable.extend(sorted(rows)); stable.append(line); rows = None
            elif rows is not None: rows.append(line)
            else: stable.append(line)
        assert rows is None
        return '\n'.join(stable) + '\n'
    def digest(text): return hashlib.sha256(text.encode()).hexdigest()
    def record(name, result, expected, *, child_exit=None, **details):
        observed_exit = result.returncode if child_exit is None else child_exit
        assert observed_exit == expected, (name, observed_exit, result.stderr)
        if expected: assert not result.stdout
        else:
            observed = json.loads(result.stdout)
            assert observed == dict(format='bigbrotr-catalog-inspection/1', status='verified',
                dataset_id='01'*16, catalog_id='5a'*16, data_root='/out/schema_bbcat_cli/',
                scientific_schema_version=1, scope='catalog identity, commit guard and logical schema metadata',
                file_contents='not-checked', runtime_authorization='not-granted', ready=False)
            assert not result.stderr
        report['cases'].append(dict(name=name, status='PASS', exit=observed_exit,
                                    stdout=result.stdout, stderr=result.stderr, **details)); save()
    def candidate(name, database='bbcat_cli', changes=()):
        text = (SOURCE / 'config/recipes/archive-only.toml').read_text()
        for section, key, value in [('administration', 'enabled', False),
                ('topology', 'dataset_id', '01'*16), ('archive', 'catalog_id', '5a'*16),
                ('storage', 'archive_root', f'/out/schema_{database}'),
                ('postgres.catalog', 'database', database), ('postgres.catalog', 'user', 'postgres'),
                ('postgres.catalog', 'password_ref', 'file:/out/catalog-cli/postgres'),
                # This path must stay unopened even though Control is selected.
                ('postgres.control', 'password_ref', 'file:/does/not/exist/control'), *changes]:
            text = lab.change(text, section, key, value)
        (work / (name + '.toml')).write_text(text)
        return '/out/catalog-cli/' + name + '.toml'
    def inspect_case(name, path, expected, database='bbcat_cli'):
        before = snapshot(database)
        began = time.monotonic()
        result = cli('archive', 'inspect', '--file', path)
        seconds = time.monotonic() - began
        assert snapshot(database) == before
        assert sql(database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND pid<>pg_backend_pid();") == '0'
        record(name, result, expected, logical_sha256=digest(before), seconds=seconds, remaining_clients=0)
    def create(database, mode=None, guard=None):
        assert re.fullmatch('bbcat_[a-z_]+', database)
        sql('postgres', f'CREATE DATABASE {database};')
        sql(database, 'REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT USAGE,CREATE ON SCHEMA public TO "BootstrapMixed";')
        (root / 'out' / ('schema_' + database)).mkdir(mode=0o700)
        if mode:
            result = command('/out/native-tests/catalog-schema-probe', database, mode, 'success')
            assert result.returncode == 0 and json.loads(result.stdout)['success'], result.stderr
        if guard:
            result = command('/out/native-tests/scientific-guard-probe', guard, database, 'installed')
            assert result.returncode == 0 and json.loads(result.stdout)['outcome'] == 'installed', result.stderr
    save()
    blocker = None
    original = None
    try:
        for role in ('postgres', 'bigbrotr_control'):
            tls_cases.private_write(work / role, (root / 'secrets' / role).read_bytes())
        create('bbcat_cli', 'initialize', 'install')
        initial = snapshot('bbcat_cli'); (work / 'before.sql').write_text(initial)
        plain = candidate('plain')
        inspect_case('scientific-catalog-control-secret-unavailable', plain, 0)
        for name, changes in [
            ('wrong-dataset', [('topology', 'dataset_id', '02'*16)]),
            ('wrong-catalog', [('archive', 'catalog_id', '5b'*16)]),
            ('wrong-root', [('storage', 'archive_root', '/out/wrong')]),
            ('wrong-password', [('postgres.catalog', 'password_ref', 'file:/out/catalog-cli/bigbrotr_control')]),
            ('denied-manager', [('postgres.catalog', 'user', 'bigbrotr_control'), ('postgres.catalog', 'password_ref', 'file:/out/catalog-cli/bigbrotr_control')]),
            ('unavailable', [('postgres.catalog', 'endpoint', '127.0.0.1:5433')]),
        ]:
            inspect_case(name, candidate(name, changes=changes), 4)
        inspect_case('terminal-slash', candidate('slash', changes=[('storage', 'archive_root', '/out/schema_bbcat_cli/')]), 0)
        for database, mode, guard in [('bbcat_empty', None, None), ('bbcat_unbound', 'initialize', None), ('bbcat_native', 'bootstrap', 'native-install')]:
            create(database, mode, guard)
            inspect_case(database, candidate(database, database), 4, database)

        # A fixed response is required BEFORE trusting the owner table's shape.
        # The malformed ordinal is larger than this actual CLI's hard cgroup cap.
        create('bbcat_malformed')
        sql('bbcat_malformed', "CREATE SCHEMA bigbrotr_archive; CREATE TABLE bigbrotr_archive.owner(singleton boolean, ordinal text); INSERT INTO bigbrotr_archive.owner VALUES(true,repeat('x',268435456));")
        malformed_before = sql('bbcat_malformed', 'SELECT octet_length(ordinal),md5(ordinal) FROM bigbrotr_archive.owner;')
        expected_md5 = hashlib.md5()
        for _ in range(256): expected_md5.update(b'x' * 1024**2)
        assert malformed_before == '268435456|' + expected_md5.hexdigest()
        path = candidate('malformed', 'bbcat_malformed')
        record_container = dict(name=state['container'] + '-malformed', state='creating')
        report['containers'].append(record_container); save()
        assert lab.docker('inspect', record_container['name'], check=False).returncode != 0
        record_container['id'] = lab.docker('create', '--name', record_container['name'], '--init',
            '--label', 'org.bigbrotr.catalog-cli=' + state['owner'], '--network', 'container:' + state['postgres_id'],
            '--user', f'{os.getuid()}:{os.getgid()}', '--cpus', '1', '--memory', '192m', '--memory-swap', '192m',
            '--pids-limit', '64', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--mount', f'type=bind,src={root / "out"},dst=/out,readonly', state['image'],
            '/out/native-tests/bigbrotr', 'archive', 'inspect', '--file', path).stdout.strip(); save()
        result = lab.docker('start', '--attach', record_container['id'], check=False, timeout=30)
        info = json.loads(lab.docker('inspect', record_container['id']).stdout)[0]
        assert not info['State']['Running'] and not info['State']['OOMKilled'] and info['State']['ExitCode'] == 4
        assert result.stderr == 'error: archive catalog guard refused: IncompatibleLayout\n'
        assert sql('bbcat_malformed', 'SELECT octet_length(ordinal),md5(ordinal) FROM bigbrotr_archive.owner;') == malformed_before
        record_container.update(state='stopped-retained', terminal=info['State']); save()
        # docker start --attach does not consistently propagate the child exit.
        record('bounded-malformed-owner', result, 4, child_exit=info['State']['ExitCode'],
               docker_attach_exit=result.returncode, ordinal_bytes=268435456, cgroup_bytes=192*1024**2,
               unchanged_ordinal_md5=expected_md5.hexdigest())
        sql('bbcat_malformed', "TRUNCATE bigbrotr_archive.owner; INSERT INTO bigbrotr_archive.owner VALUES(true,'1'),(true,'2');")
        inspect_case('multiple-singletons', path, 4, 'bbcat_malformed')

        # Actual row contention must terminate, join the driver and permit retry.
        blocker = subprocess.Popen(['docker', 'exec', '-i', state['postgres_id'], 'psql', '-Xq', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'bbcat_cli'],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        blocker.stdin.write("BEGIN; SET LOCAL application_name='catalog-cli-blocker'; SELECT 1 FROM bigbrotr_archive.owner FOR UPDATE;\n"); blocker.stdin.flush()
        until = time.monotonic() + 5
        while sql('bbcat_cli', "SELECT count(*) FROM pg_stat_activity WHERE application_name='catalog-cli-blocker' AND state='idle in transaction';") != '1':
            assert time.monotonic() < until; time.sleep(.02)
        began = time.monotonic(); result = cli('archive', 'inspect', '--file', plain); elapsed = time.monotonic()-began
        assert 4.5 <= elapsed < 12
        assert snapshot('bbcat_cli') == initial
        assert sql('bbcat_cli', "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND pid<>pg_backend_pid() AND application_name<>'catalog-cli-blocker';") == '0'
        record('owner-lock-deadline', result, 4, seconds=elapsed, remaining_inspect_clients=0)
        blocker.stdin.write('ROLLBACK;\n'); blocker.stdin.close(); assert blocker.wait(timeout=5) == 0; blocker = None
        inspect_case('retry-after-lock', plain, 0)

        report['pki'] = tls_cases.generate_pki(work / 'pki'); save()
        server_dir = '/var/lib/postgresql/data/catalog-cli-pki'
        assert lab.docker('exec', state['postgres_id'], 'test', '-e', server_dir, check=False).returncode == 1
        lab.docker('cp', str(work / 'pki'), state['postgres_id'] + ':' + server_dir)
        lab.docker('exec', state['postgres_id'], 'chown', '-R', 'postgres:postgres', server_dir)
        auto = '/var/lib/postgresql/data/postgresql.auto.conf'
        original = lab.docker('exec', state['postgres_id'], 'cat', auto).stdout
        settings = tls_cases.settings(state)
        assert settings['ssl'] == 'off'
        for key, value in {'ssl': 'on', 'ssl_cert_file': server_dir+'/server.crt', 'ssl_key_file': server_dir+'/server.key', 'ssl_ca_file': server_dir+'/ca.crt'}.items():
            sql('postgres', f"ALTER SYSTEM SET {key}='{value}';")
        assert sql('postgres', 'SELECT pg_reload_conf();') == 't'
        until = time.monotonic()+5
        while tls_cases.settings(state)['ssl'] != 'on':
            assert time.monotonic()<until; time.sleep(.05)
        for name, ca, server, expected in [('tls', 'ca', tls_cases.SERVER_NAME, 0), ('wrong-ca', 'wrong_ca', tls_cases.SERVER_NAME, 4), ('wrong-name', 'ca', 'wrong.bigbrotr.invalid', 4)]:
            text = (work / 'plain.toml').read_text()
            block = '[postgres.catalog.tls]\nmode="verify-full"\n'+f'ca_file="/out/catalog-cli/pki/{ca}.crt"\nserver_name="{server}"\n\n'
            text, count = re.subn(r'(?ms)^\[postgres\.catalog\.tls\]\n.*?(?=^\[|\Z)', lambda _: block, text)
            assert count == 1
            (work / (name+'.toml')).write_text(text)
            inspect_case(name, '/out/catalog-cli/'+name+'.toml', expected)
        assert snapshot('bbcat_cli') == initial
        (work / 'after.sql').write_text(snapshot('bbcat_cli'))
        report['status'] = 'COLLECTED'
    except BaseException as error:
        report.update(status='FAIL', failure_class=type(error).__name__); raise
    finally:
        cleanup_errors = []
        try:
            if blocker is not None:
                try:
                    sql('bbcat_cli', "SELECT pg_terminate_backend(pid,5000) FROM pg_stat_activity WHERE application_name='catalog-cli-blocker';")
                    blocker.stdin.close(); blocker.wait(timeout=5)
                except BaseException as error:
                    cleanup_errors.append(dict(resource='blocker', error=type(error).__name__))
            for record_container in report['containers']:
                try:
                    # An ambiguous create is resolved only by reserved name and
                    # owner label; failed inspection does not prove absence.
                    info = json.loads(lab.docker('inspect', record_container.get('id', record_container['name'])).stdout)[0]
                    assert info['Config']['Labels']['org.bigbrotr.catalog-cli'] == state['owner']
                    record_container['id'] = info['Id']
                    if info['State']['Running']: lab.docker('stop', '--time', '3', info['Id'])
                    terminal = json.loads(lab.docker('inspect', info['Id']).stdout)[0]['State']
                    assert not terminal['Running']
                    record_container.update(state='stopped-retained', terminal=terminal)
                except BaseException as error:
                    cleanup_errors.append(dict(resource=record_container['name'], error=type(error).__name__))
            if original is not None:
                try:
                    pg = json.loads(lab.docker('inspect', state['postgres_id']).stdout)[0]
                    assert pg['Config']['Labels']['org.bigbrotr.publication'] == state['owner'] and pg['State']['Running']
                    lab.docker('exec', '-i', state['postgres_id'], 'sh', '-c', 'cat > /var/lib/postgresql/data/postgresql.auto.conf', data=original)
                    assert lab.docker('exec', state['postgres_id'], 'cat', auto).stdout == original
                    assert sql('postgres', 'SELECT pg_reload_conf();') == 't'
                    until = time.monotonic()+5
                    while tls_cases.settings(state) != settings:
                        assert time.monotonic()<until; time.sleep(.05)
                    report['restoration'] = 'PASS'
                except BaseException as error:
                    report['restoration'] = 'FAIL'
                    cleanup_errors.append(dict(resource='postgres-settings', error=type(error).__name__))
        finally:
            report['cleanup_errors'] = cleanup_errors
            if cleanup_errors: report['status'] = 'INCOMPLETE'
            elif report['status'] == 'COLLECTED' and report.get('restoration') == 'PASS': report['status'] = 'PASS'
            save()
        if cleanup_errors:
            raise RuntimeError('cleanup incomplete; inspect retained ownership report')
    print(json.dumps(dict(status=report['status'], cases=len(report['cases']))))


if __name__ == '__main__': main()
