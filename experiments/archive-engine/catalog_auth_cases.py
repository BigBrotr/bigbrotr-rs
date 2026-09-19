#!/usr/bin/env python3
"""Actual native reader authentication in an exact-source isolated PostgreSQL lab.

The baseline is a separately rebuilt published parent, with the same native
worker/engines. Trust and an omitted SCRAM server-final are discriminating controls.
An empty, actually initialized scientific catalog is intentional: successful
Absent requires metadata validation and a complete native query, not just login.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
import tls_cases
from fault_cases import Process


def main(root):
    state = json.loads((root / 'resources.json').read_text())
    assert SOURCE == root / 'source' and state['root'] == str(root) and state['state'] == 'running'
    assert lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip() == state['source']
    assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    assert ready['source'] == state['source']
    for key, path in [('receipt_reader', 'native-tests/archive-receipt-probe'),
                      ('baseline_reader', 'native-tests/baseline-receipt-probe'),
                      ('worker', 'archive-worker/bigbrotr-archive-worker')]:
        assert hashlib.sha256((root / 'out' / path).read_bytes()).hexdigest() == ready[key]
    for key in ('container_id', 'postgres_id'):
        info = json.loads(lab.docker('inspect', state[key]).stdout)[0]
        assert info['Config']['Labels']['org.bigbrotr.publication'] == state['owner']
        assert info['State']['Running'] and not info['HostConfig']['PortBindings']
    network = json.loads(lab.docker('network', 'inspect', state['network_id']).stdout)[0]
    assert network['Internal'] and network['Labels']['org.bigbrotr.publication'] == state['owner']
    work = root / 'out/catalog-auth'
    work.mkdir(mode=0o700)
    report = dict(source=state['source'], artifacts=ready, status='INCOMPLETE', cases=[], auxiliary=[])
    output = root / 'catalog-auth.json'
    assert not output.exists()
    database, role = 'bbcat_auth', 'bigbrotr_receipt_reader'
    pg = state['postgres_id']

    def save(): lab.write_json(output, report)
    def sql(text, db=database): return lab.sql(state, db, text)
    def command(*args): return lab.docker('exec', state['container_id'], *args)
    def snapshot():
        dump = lab.docker('exec', pg, 'pg_dump', '-U', 'postgres', '-d', database).stdout
        return '\n'.join(line for line in dump.splitlines() if not line.startswith(('\\restrict ', '\\unrestrict ')))
    def reload():
        before = sql('SELECT pg_conf_load_time();', 'postgres')
        assert sql('SELECT pg_reload_conf();', 'postgres') == 't'
        until = time.monotonic() + 5
        while sql('SELECT pg_conf_load_time();', 'postgres') == before:
            assert time.monotonic() < until
            time.sleep(.02)
        assert sql('SELECT count(*) FROM pg_hba_file_rules WHERE error IS NOT NULL;', 'postgres') == '0'
    def replace(path, text):
        actual = json.loads(lab.docker('inspect', pg).stdout)[0]
        assert actual['Config']['Labels']['org.bigbrotr.publication'] == state['owner'] and actual['State']['Running']
        assert path in ('/var/lib/postgresql/data/pg_hba.conf', '/var/lib/postgresql/data/postgresql.auto.conf')
        lab.docker('exec', '-i', pg, 'sh', '-c', 'cat > "$1"', 'sh', path, data=text)
        assert lab.docker('exec', pg, 'cat', path).stdout == text
    save()
    original_hba = original_auto = original_settings = None
    proxy = None

    def finish_proxy():
        nonlocal proxy
        current, proxy = proxy, None
        errors = []
        capture = dict(status='INCOMPLETE')
        report.setdefault('proxies', []).append(capture)
        try:
            current.process.stdin.write(b'stop\n'); current.process.stdin.flush()
            assert current.wait(5) == 0
        except BaseException as error:
            errors.append(dict(stage='control-or-wait', error=type(error).__name__))
        finally:
            # Local wrapper cleanup and evidence capture must still run after a
            # broken control pipe/timeout. This is not remote process exclusion.
            try: current.stop()
            except BaseException as error: errors.append(dict(stage='wrapper-stop', error=type(error).__name__))
            try:
                capture.update(events=[json.loads(line) for line in current.stdout.splitlines()],
                               stderr_empty=not current.stderr, overflow=current.overflow)
                assert not current.stderr and not current.overflow
                terminal = capture['events'][-1]
                assert terminal['event'] == 'terminal' and terminal['unexpected'] == 0
            except BaseException as error:
                errors.append(dict(stage='terminal-capture', error=type(error).__name__))
            capture['errors'] = errors
            capture['status'] = 'INCOMPLETE' if errors else 'PASS'
            if errors: report['unresolved_proxy_requires_runner_stop'] = True
            save()
        if errors:
            raise RuntimeError('proxy termination incomplete; retain owned runner for terminal audit')
        return terminal

    try:
        sql(f'CREATE DATABASE {database};', 'postgres')
        sql('REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT USAGE,CREATE ON SCHEMA public TO "BootstrapMixed";')
        (root / 'out' / ('schema_' + database)).mkdir(mode=0o700)
        initialized = command('/out/native-tests/catalog-schema-probe', database, 'initialize', 'success')
        assert json.loads(initialized.stdout) == dict(success=True, active_permits=0, outcome='Ok(())')
        installed = command('/out/native-tests/scientific-guard-probe', 'install', database, 'installed')
        assert json.loads(installed.stdout)['outcome'] == 'installed'
        sql(f'REVOKE TEMP ON DATABASE {database} FROM PUBLIC; GRANT CONNECT ON DATABASE {database} TO {role}; GRANT USAGE ON SCHEMA public,bigbrotr_archive TO {role}; GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}; GRANT SELECT ON bigbrotr_archive.installation,bigbrotr_archive.scientific_schema TO {role};')
        before = snapshot()
        (work / 'before.sql').write_text(before)
        base = dict(expected=dict(dataset_id='01'*16, schema_version=1, topology_generation='0',
            batch_digest='71'*32, record_count=1,
            input_ranges=[dict(shard_id=0, owner_epoch=1, first_sequence=1, last_sequence=1)],
            attempt_id=[72]*16, publication_ordinal=1), catalog_id='5a'*16,
            data_root=f'/out/schema_{database}/', database=database, port=5432,
            role=role, max_receipts=1, reserve_bytes=384 << 20, deadline_ms=15000, cancel_when_blocked=False)

        def run(name, wanted, *, baseline=False, **changes):
            directory = work / name
            directory.mkdir(mode=0o700)
            (directory / 'job').mkdir(mode=0o700)
            fixture = dict(base, **changes)
            (directory / 'input.json').write_text(json.dumps(fixture) + '\n')
            case = dict(name=name, status='INCOMPLETE', parent='baseline' if baseline else 'candidate', input=fixture)
            report['cases'].append(case); save()
            began = time.monotonic()
            if baseline:
                # The unchanged old probe uses fixed fixture/job paths. Keep
                # its actual executable and input provenance separately visible.
                auxiliary = dict(name=state['container']+'-'+name, state='creating')
                report['auxiliary'].append(auxiliary); save()
                assert lab.docker('inspect', auxiliary['name'], check=False).returncode != 0
                auxiliary['id'] = lab.docker('create', '--name', auxiliary['name'],
                    '--label', 'org.bigbrotr.catalog-auth='+state['owner'],
                    '--user', f'{os.getuid()}:{os.getgid()}', '--cpus', '1', '--memory', '768m', '--memory-swap', '768m',
                    '--pids-limit', '256', '--network', 'container:'+pg, '--read-only', '--cap-drop', 'ALL',
                    '--security-opt', 'no-new-privileges',
                    '--mount', f'type=bind,src={root / "out"},dst=/out,readonly',
                    '--mount', f'type=bind,src={root / "secrets"},dst=/secrets,readonly',
                    '--mount', f'type=bind,src={directory},dst=/fixture,readonly',
                    '--mount', f'type=bind,src={directory / "job"},dst=/job',
                    state['image'], '/out/native-tests/baseline-receipt-probe').stdout.strip(); save()
                lab.docker('start', auxiliary['id'])
                assert lab.docker('wait', auxiliary['id'], timeout=25).stdout.strip() == '0'
                result = lab.docker('logs', auxiliary['id'])
                terminal = json.loads(lab.docker('inspect', auxiliary['id']).stdout)[0]['State']
                assert not terminal['Running'] and not terminal['OOMKilled']
                auxiliary.update(state='stopped-retained', terminal=terminal)
            else:
                result = command('/out/native-tests/archive-receipt-probe',
                                 f'/out/catalog-auth/{name}/input.json', f'/out/catalog-auth/{name}/job')
            assert not result.stderr
            value = json.loads(result.stdout)
            case.update(observed=value, seconds=time.monotonic()-began); save()
            assert value['outcome'] == wanted and value['active_permits'] == 0 and value['cancelled'] is False
            assert snapshot() == before
            until = time.monotonic() + 5
            while sql(f"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='{role}';") != '0':
                assert time.monotonic() < until
                time.sleep(.02)
            case.update(status='PASS', catalog_unchanged=True, remaining_clients=0); save()

        run('scram', 'Ok(Absent)')
        run('wrong-password', 'Err(ObservationUnknown)', wrong_password=True)
        run('scram-retry', 'Ok(Absent)')
        hba = '/var/lib/postgresql/data/pg_hba.conf'
        auto = '/var/lib/postgresql/data/postgresql.auto.conf'
        original_hba = lab.docker('exec', pg, 'cat', hba).stdout
        original_auto = lab.docker('exec', pg, 'cat', auto).stdout
        original_settings = tls_cases.settings(state)

        def auth(method):
            replace(hba, f'host {database} {role} 127.0.0.1/32 {method}\n'+original_hba)
            reload()
            assert sql('SELECT auth_method FROM pg_hba_file_rules WHERE line_number=1;', 'postgres') == method
        auth('trust')
        run('baseline-trust', 'Ok(Absent)', baseline=True)
        run('trust', 'Err(ObservationUnknown)')
        run('trust-wrong-password', 'Err(ObservationUnknown)', wrong_password=True)
        auth('password')
        run('cleartext-password-challenge', 'Err(ObservationUnknown)')
        auth('scram-sha-256')

        def proxy_run(name, baseline):
            nonlocal proxy
            proxy = Process(['docker', 'exec', '-i', state['container_id'], 'python3', '-B',
                             '/source/experiments/archive-engine/native_commit_proxy.py', '--mode', 'omit-auth-final'], input_pipe=True)
            until = time.monotonic() + 5
            with proxy.condition:
                while b'\n' not in proxy.stdout:
                    assert not proxy.overflow and time.monotonic() < until
                    proxy.condition.wait(.02)
                ready_proxy = json.loads(bytes(proxy.stdout).splitlines()[0])
            assert ready_proxy['event'] == 'ready'
            try:
                run(name, 'Ok(Absent)' if baseline else 'Err(ObservationUnknown)', baseline=baseline, port=ready_proxy['port'])
            finally:
                terminal = finish_proxy()
            report['cases'][-1]['proxy'] = terminal; save()
            assert terminal['auth_finals_omitted'] > 0
            assert (terminal['frontend_queries'] > 0) == baseline
        proxy_run('baseline-incomplete-scram', True)
        proxy_run('incomplete-scram', False)
        run('scram-after-proxy', 'Ok(Absent)')

        report['pki'] = tls_cases.generate_pki(work / 'pki'); save()
        server_dir = '/var/lib/postgresql/data/catalog-auth-pki'
        assert lab.docker('exec', pg, 'test', '-e', server_dir, check=False).returncode == 1
        lab.docker('cp', str(work / 'pki'), pg+':'+server_dir)
        lab.docker('exec', pg, 'chown', '-R', 'postgres:postgres', server_dir)
        for key, value in {'ssl':'on', 'ssl_cert_file':server_dir+'/server.crt',
                           'ssl_key_file':server_dir+'/server.key', 'ssl_ca_file':server_dir+'/ca.crt',
                           'log_connections':'on'}.items():
            sql(f"ALTER SYSTEM SET {key}='{value}';", 'postgres')
        reload()
        assert tls_cases.settings(state)['ssl'] == 'on'
        tls = dict(tls_server_name=tls_cases.SERVER_NAME, tls_ca_file='/out/catalog-auth/pki/ca.crt')
        run('tls-scram', 'Ok(Absent)', **tls)
        run('tls-wrong-password', 'Err(ObservationUnknown)', wrong_password=True, **tls)
        run('tls-wrong-ca', 'Err(ObservationUnknown)', **dict(tls, tls_ca_file='/out/catalog-auth/pki/wrong_ca.crt'))
        run('tls-wrong-name', 'Err(ObservationUnknown)', **dict(tls, tls_server_name='wrong.bigbrotr.invalid'))
        auth('trust')
        run('tls-trust', 'Err(ObservationUnknown)', **tls)
        auth('scram-sha-256')
        run('tls-restored-scram', 'Ok(Absent)', **tls)
        (work / 'after.sql').write_text(snapshot())
        assert snapshot() == before
        report['status'] = 'COLLECTED'
    except BaseException as error:
        report.update(status='FAIL', failure_class=type(error).__name__)
        raise
    finally:
        errors = []
        if proxy is not None:
            try: finish_proxy()
            except BaseException as error: errors.append(dict(resource='proxy', error=type(error).__name__))
        for auxiliary in report['auxiliary']:
            try:
                info = json.loads(lab.docker('inspect', auxiliary.get('id', auxiliary['name'])).stdout)[0]
                assert info['Config']['Labels']['org.bigbrotr.catalog-auth'] == state['owner']
                auxiliary['id'] = info['Id']
                if info['State']['Running']: lab.docker('stop', '--time', '3', info['Id'])
                info = json.loads(lab.docker('inspect', info['Id']).stdout)[0]
                assert not info['State']['Running']
                auxiliary.update(state='stopped-retained', terminal=info['State'])
            except BaseException as error: errors.append(dict(resource=auxiliary['name'], error=type(error).__name__))
        for path, original in [('/var/lib/postgresql/data/pg_hba.conf', original_hba),
                               ('/var/lib/postgresql/data/postgresql.auto.conf', original_auto)]:
            if original is not None:
                try: replace(path, original)
                except BaseException as error: errors.append(dict(resource=path, error=type(error).__name__))
        if original_settings is not None:
            try:
                reload()
                assert tls_cases.settings(state) == original_settings
                report['restoration'] = 'PASS'
            except BaseException as error: errors.append(dict(resource='reload', error=type(error).__name__))
        report['cleanup_errors'] = errors
        if errors or report.get('unresolved_proxy_requires_runner_stop'): report['status'] = 'INCOMPLETE'
        elif report['status'] == 'COLLECTED': report['status'] = 'PASS'
        save()
    assert report['status'] == 'PASS'
    print(json.dumps(dict(status='PASS', cases=len(report['cases']))), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    main(parser.parse_args().root.resolve(strict=True))
