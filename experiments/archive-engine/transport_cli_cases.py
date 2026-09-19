#!/usr/bin/env python3
"""Scoped real Control CLI regression in a fresh owned publication laboratory.

Runs sequentially after its native build. No source edits or external network;
only this lab's PostgreSQL TLS settings are changed, backed up and restored.
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    root = parser.parse_args().root.resolve(strict=True)
    state = json.loads((root / 'resources.json').read_text())
    assert state['root'] == str(root) and state['state'] == 'running'
    assert lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip() == state['source']
    assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain']).stdout
    ready = json.loads((root / 'out/native-tests/ready.json').read_text())
    binary = root / 'out/native-tests/bigbrotr'
    assert ready['source'] == state['source'] and hashlib.sha256(binary.read_bytes()).hexdigest() == ready['control']
    for key in ('container_id', 'postgres_id'):
        info = json.loads(lab.docker('inspect', state[key]).stdout)[0]
        assert info['Config']['Labels']['org.bigbrotr.publication'] == state['owner'] and info['State']['Running']
        assert not info['HostConfig']['PortBindings']
    network = json.loads(lab.docker('network', 'inspect', state['network_id']).stdout)[0]
    assert network['Internal'] and network['Labels']['org.bigbrotr.publication'] == state['owner']
    work = root / 'out/transport-cli'
    work.mkdir(mode=0o700)
    output = root / 'transport-cli.json'
    assert not output.exists()
    report = dict(source=state['source'], binary_sha256=ready['control'], status='RUNNING', cases=[], containers=[], restoration='NOT_RUN')
    def save(): lab.write_json(output, report)
    def check(name, **detail):
        report['cases'].append(dict(name=name, status='PASS', **detail)); save()
    def sql(query): return lab.sql(state, 'bb_transport', query)
    def cli(*args): return lab.docker('exec', state['container_id'], '/out/native-tests/bigbrotr', *args, check=False)
    def inspect(record):
        info = json.loads(lab.docker('inspect', record['id']).stdout)[0]
        assert info['Config']['Labels']['org.bigbrotr.transport-cli'] == state['owner']
        return info
    def start(path, label):
        name = state['container'] + '-cli-' + label
        assert lab.docker('inspect', name, check=False).returncode != 0
        record = dict(name=name, state='creating'); report['containers'].append(record); save()
        record['id'] = lab.docker('run', '-d', '--init', '--name', name,
            '--label', 'org.bigbrotr.transport-cli=' + state['owner'], '--network', 'container:' + state['postgres_id'],
            '--user', f'{os.getuid()}:{os.getgid()}', '--cpus', '2', '--memory', '4g', '--memory-swap', '4g',
            '--pids-limit', '128', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '--mount', f'type=bind,src={root / "out"},dst=/out,readonly', state['image'],
            '/out/native-tests/bigbrotr', 'serve', '--file', path, '--roles', 'control').stdout.strip()
        record['state'] = 'running'; save(); return record
    def logs(record):
        r = lab.docker('logs', record['id']); return r.stdout, r.stderr
    def idle(record):
        until = time.monotonic() + 12
        while time.monotonic() < until:
            out, err = logs(record)
            if out:
                result = json.loads(out.splitlines()[0]); assert result['state'] == 'idle' and not err
                return result
            assert inspect(record)['State']['Running'], (record['name'], err)
            time.sleep(.05)
        raise RuntimeError('idle deadline')
    def terminal(record, code, seconds=6):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            current = inspect(record)['State']
            if not current['Running']: break
            time.sleep(.05)
        assert not current['Running'] and current['ExitCode'] == code and not current['OOMKilled'], current
        out, err = logs(record)
        record.update(state='stopped-retained', terminal=current, stdout=out, stderr=err); save()
        return out, err
    def release(record, signal):
        began = time.monotonic(); lab.docker('kill', '--signal', signal, record['id'])
        out, err = terminal(record, 0)
        assert not err and [json.loads(line)['state'] for line in out.splitlines()] == ['idle', 'stopped']
        assert json.loads(out.splitlines()[1])['release'] == 'committed'
        assert sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';") == '0'
        return time.monotonic() - began
    def owners():
        return json.loads(sql("SELECT coalesce(json_agg(t ORDER BY epoch),'[]') FROM control.owner_epochs t;"))
    save()
    original = None
    settings = None
    blocker = None
    try:
        # Protected inputs are owned by the same UID as the actual application.
        for role in ('postgres', 'bigbrotr_control'):
            tls_cases.private_write(work / role, (root / 'secrets' / role).read_bytes())
        lab.sql(state, 'postgres', 'CREATE DATABASE bb_transport;')
        text = (SOURCE / 'config/recipes/archive-only.toml').read_text()
        for section, key, value in [('administration', 'enabled', False),
                ('postgres.control', 'database', 'bb_transport'),
                ('postgres.control', 'password_ref', 'file:/out/transport-cli/bigbrotr_control')]:
            text = lab.change(text, section, key, value)
        plain = '/out/transport-cli/plain.toml'; (work / 'plain.toml').write_text(text)
        result = cli('migrate', 'apply', '--file', plain, '--user', 'postgres', '--password-ref', 'file:/out/transport-cli/postgres')
        assert result.returncode == 0, (result.returncode, result.stderr)
        sql('GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        for args in [('config', 'initialize', '--file', plain),
                     ('config', 'activate-initial', '--file', plain, '--request-id', '51'*16, '--expected-revision', '1')]:
            r = cli(*args); assert r.returncode == 0, (r.returncode, r.stderr)
        record = start(plain, 'plain'); first = idle(record)
        assert first['owner_epoch'] == '1'
        assert sql("SELECT ssl FROM pg_stat_ssl JOIN pg_stat_activity USING(pid) WHERE datname=current_database() AND usename='bigbrotr_control';") == 'f'
        check('loopback-sigterm', seconds=release(record, 'TERM'), identity=first)

        report['pki'] = tls_cases.generate_pki(work / 'pki'); save()
        server_dir = '/var/lib/postgresql/data/shared-transport-pki'
        assert lab.docker('exec', state['postgres_id'], 'test', '-e', server_dir, check=False).returncode == 1
        lab.docker('cp', str(work / 'pki'), state['postgres_id'] + ':' + server_dir)
        lab.docker('exec', state['postgres_id'], 'chown', '-R', 'postgres:postgres', server_dir)
        auto = '/var/lib/postgresql/data/postgresql.auto.conf'
        original = lab.docker('exec', state['postgres_id'], 'cat', auto).stdout
        tls_cases.private_write(work / 'original-auto.conf', original.encode())
        settings = tls_cases.settings(state)
        assert settings['ssl'] == 'off'
        for key, value in {'ssl': 'on', 'ssl_cert_file': server_dir + '/server.crt',
                'ssl_key_file': server_dir + '/server.key', 'ssl_ca_file': server_dir + '/ca.crt',
                'log_connections': 'on', 'log_line_prefix': '%m [%p] ', 'lc_messages': 'C'}.items():
            lab.sql(state, 'postgres', f"ALTER SYSTEM SET {key} = '{value}';")
        assert lab.sql(state, 'postgres', 'SELECT pg_reload_conf();') == 't'
        until = time.monotonic() + 5
        while tls_cases.settings(state)['ssl'] != 'on':
            assert time.monotonic() < until; time.sleep(.05)
        paths = {}
        for name, ca, server in [('tls', 'ca', tls_cases.SERVER_NAME), ('wrong_ca', 'wrong_ca', tls_cases.SERVER_NAME), ('wrong_name', 'ca', 'wrong.bigbrotr.invalid')]:
            block = '[postgres.control.tls]\nmode = "verify-full"\n' + f'ca_file = "/out/transport-cli/pki/{ca}.crt"\nserver_name = "{server}"\n'
            import re
            candidate, count = re.subn(r'(?ms)^\[postgres\.control\.tls\]\n.*?(?=^\[|\Z)', lambda _: block+'\n', text)
            assert count == 1
            (work / (name+'.toml')).write_text(candidate); paths[name] = '/out/transport-cli/'+name+'.toml'
            assert cli('config', 'validate', '--file', paths[name]).returncode == 0
        record = start(paths['tls'], 'tls'); second = idle(record)
        tls = json.loads(sql("SELECT row_to_json(t) FROM (SELECT ssl,version,cipher,bits FROM pg_stat_ssl JOIN pg_stat_activity USING(pid) WHERE datname=current_database() AND usename='bigbrotr_control') t;"))
        assert second['owner_epoch'] == '2' and second['process_incarnation'] != first['process_incarnation']
        assert tls['ssl'] and tls['version'] in ('TLSv1.2', 'TLSv1.3') and tls['bits'] >= 128
        check('verify-full-sigint-restart', seconds=release(record, 'INT'), identity=second, tls=tls)
        before = owners()
        for name, alert in [('wrong_ca', 'unknown ca'), ('wrong_name', 'bad certificate')]:
            previous = tls_cases.log_snapshot(state)
            result = cli('serve', '--file', paths[name], '--roles', 'control')
            assert result.returncode == 1 and not result.stdout and result.stderr == 'error: PostgreSQL connection unavailable\n'
            until = time.monotonic()+3
            while True:
                after = tls_cases.log_snapshot(state); assert after.startswith(previous)
                delta = after[len(previous):].lower()
                assert 'connection authorized: user=bigbrotr_control database=bb_transport' not in delta
                if alert in delta: break
                assert time.monotonic()<until, 'server TLS alert deadline'
                time.sleep(.05)
            assert owners() == before
            assert sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';") == '0'
            check(name, exit=result.returncode, server_alert=alert, unchanged_history=True)

        record = start(paths['tls'], 'cancel'); third = idle(record)
        blocker = subprocess.Popen(['docker', 'exec', '-i', state['postgres_id'], 'psql', '-Xq', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'bb_transport'], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        blocker.stdin.write("BEGIN; SET LOCAL application_name='bigbrotr-transport-blocker'; SELECT 1 FROM control.datasets FOR UPDATE; SELECT pg_sleep(60); ROLLBACK;\n"); blocker.stdin.close()
        until = time.monotonic() + 8
        while time.monotonic() < until:
            waits = json.loads(sql("SELECT coalesce(json_agg(t),'[]') FROM (SELECT pid,backend_start,pg_blocking_pids(pid) blockers FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock') t;"))
            if waits: break
            time.sleep(.02)
        assert len(waits) == 1 and len(waits[0]['blockers']) == 1
        before = owners(); began = time.monotonic()
        lab.docker('kill', '--signal', 'TERM', record['id']); terminal(record, 4)
        elapsed = time.monotonic() - began
        # Cancel server blocker explicitly so its queued peer can reach EOF.
        sql("SELECT pg_terminate_backend(pid,5000) FROM pg_stat_activity WHERE datname=current_database() AND application_name='bigbrotr-transport-blocker';")
        blocker.wait(timeout=8); blocker = None
        until = time.monotonic()+5
        while sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';") != '0':
            assert time.monotonic()<until; time.sleep(.05)
        assert owners() == before
        check('cancel-blocked-policy', seconds=elapsed, identity=third, waits=waits, unchanged_history=True)
        # The old claim remains fenced until normal replacement; no fake release.
        record = start(paths['tls'], 'restart'); fourth = idle(record)
        assert fourth['owner_epoch'] == '4'
        check('restart-after-cancel', seconds=release(record, 'TERM'), identity=fourth)
        rows = owners()
        assert [r['epoch'] for r in rows] == [1,2,3,4] and not any(r['state']=='active' for r in rows)
        report['owner_history'] = rows
        report['status'] = 'COLLECTED'
    except BaseException as error:
        report.update(status='FAIL', failure_class=type(error).__name__); raise
    finally:
        cleanup_errors = []
        try:
            if blocker is not None:
                try:
                    sql("SELECT pg_terminate_backend(pid,5000) FROM pg_stat_activity WHERE datname=current_database() AND application_name='bigbrotr-transport-blocker';")
                    blocker.wait(timeout=8)
                except BaseException as error:
                    cleanup_errors.append(dict(resource='blocker', error=type(error).__name__))
            for record in report['containers']:
                try:
                    if 'id' not in record:
                        # A failed/lost docker run reply can still have created
                        # the reserved name. Its label must prove ownership.
                        found = lab.docker('inspect', record['name'], check=False)
                        if found.returncode:
                            # Failed inspection does not prove nonexistence.
                            raise RuntimeError('reserved container identity unresolved')
                        info = json.loads(found.stdout)[0]
                        assert info['Config']['Labels'].get('org.bigbrotr.transport-cli') == state['owner']
                        record['id'] = info['Id']
                    current = inspect(record)
                    if current['State']['Running']: lab.docker('stop', '--time', '3', record['id'])
                    record.update(state='stopped-retained', terminal=inspect(record)['State'])
                    assert not record['terminal']['Running']
                except BaseException as error:
                    cleanup_errors.append(dict(resource=record['name'], error=type(error).__name__))
            if original is not None:
                try:
                    # Recheck the exact server and label before restoration.
                    pg = json.loads(lab.docker('inspect', state['postgres_id']).stdout)[0]
                    assert pg['Config']['Labels']['org.bigbrotr.publication'] == state['owner'] and pg['State']['Running']
                    lab.docker('exec', '-i', state['postgres_id'], 'sh', '-c', 'cat > /var/lib/postgresql/data/postgresql.auto.conf', data=original)
                    assert lab.docker('exec', state['postgres_id'], 'cat', '/var/lib/postgresql/data/postgresql.auto.conf').stdout == original
                    assert lab.sql(state, 'postgres', 'SELECT pg_reload_conf();') == 't'
                    until = time.monotonic()+5
                    while tls_cases.settings(state) != settings:
                        assert time.monotonic()<until; time.sleep(.05)
                    report['restoration'] = 'PASS'
                except BaseException as error:
                    report['restoration'] = 'FAIL'
                    cleanup_errors.append(dict(resource='postgres-settings', error=type(error).__name__))
        finally:
            report['cleanup_errors'] = cleanup_errors
            if cleanup_errors:
                report['status'] = 'INCOMPLETE'
            elif report['status'] == 'COLLECTED' and report['restoration'] == 'PASS':
                report['status'] = 'PASS'
            save()
        if cleanup_errors:
            raise RuntimeError('cleanup incomplete; inspect retained ownership report')
    print(json.dumps(dict(status=report['status'], cases=len(report['cases']), restoration=report['restoration'])))


if __name__ == '__main__': main()
