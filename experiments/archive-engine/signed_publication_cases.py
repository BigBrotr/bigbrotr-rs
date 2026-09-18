#!/usr/bin/env python3
"""Trusted exact-Git Tux lab: actual signed publication and native COMMIT loss.

Requires the individually inventoried publication runner; no host ports or secret
output. Results are retained before assertions. Failed/unknown processes require
the existing owner-verified terminal lab audit; killing docker-exec is not exclusion.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
from fault_cases import Process
from signed_publication_oracle import check


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command(*args, input=None, timeout=30):
    r = subprocess.run(args, input=input, capture_output=True, text=True, timeout=timeout)
    assert r.returncode == 0, (args[0], r.returncode, r.stderr)
    return r.stdout.strip()


def execute(args, timeout):
    process = Process(args, limit=256 * 1024)
    try:
        code = process.wait(timeout)
        return dict(exit=code, stdout=process.stdout.decode(), stderr=process.stderr.decode())
    finally:
        process.stop()


def main(root):
    resources = json.loads((root / 'resources.json').read_text())
    revision = resources['source']
    assert SOURCE == root / 'source'
    assert command('git', '-C', str(SOURCE), 'rev-parse', 'HEAD') == revision
    assert not command('git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all')
    runner, pg = resources['container_id'], resources['postgres_id']
    for identity in [runner, pg]:
        c = json.loads(command('docker', 'inspect', identity))[0]
        assert c['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
        assert c['State']['Running'] and not c['State']['OOMKilled']
    out = root / 'out/native-tests'
    ready = json.loads((out / 'ready.json').read_text())
    assert ready['source'] == revision
    for label, path in [('worker', root / 'out/archive-worker/bigbrotr-archive-worker'),
                        ('publisher', out / 'archive-publication-probe'), ('reader', out / 'archive-read-probe')]:
        assert sha(path) == ready[label]
    result = dict(source=revision, tree=resources['tree'], status='INCOMPLETE', artifacts=ready,
                  oracle_sha256=sha(SOURCE / 'experiments/archive-engine/signed_publication_oracle.py'),
                  harness_sha256=sha(Path(__file__)), cases=[])
    evidence = root / 'signed-cases.json'

    def save():
        evidence.write_text(json.dumps(result, indent=2) + '\n')

    def sql(db, query):
        return command('docker', 'exec', '-i', pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
                       '-U', 'postgres', '-d', db, input=query)

    binary = ['docker', 'exec', '--env', 'LD_LIBRARY_PATH=/out/archive-worker/lib', runner]
    save()
    for mode in ['normal', 'before', 'after']:
        case = dict(mode=mode, status='INCOMPLETE')
        result['cases'].append(case)
        save()
        db = 'bbpub_' + mode
        sql('postgres', f'CREATE DATABASE {db}; CREATE DATABASE bbpub_control_{mode};')
        sql(db, 'GRANT USAGE,CREATE ON SCHEMA public TO "BootstrapMixed";')
        proxy = None
        try:
            port = 5432
            if mode != 'normal':
                proxy = Process(['docker', 'exec', '-i', runner, 'python3', '-B',
                    '/source/experiments/archive-engine/native_commit_proxy.py', '--mode',
                    'before-forward' if mode == 'before' else 'after-complete'], input_pipe=True)
                end = time.monotonic() + 10
                with proxy.condition:
                    while b'\n' not in proxy.stdout:
                        assert not proxy.overflow and proxy.process.poll() is None and time.monotonic() < end
                        proxy.condition.wait(.1)
                    hello = json.loads(bytes(proxy.stdout).splitlines()[0])
                assert hello['event'] == 'ready'
                assert hello['source_sha256'] == sha(SOURCE / 'experiments/archive-engine/native_commit_proxy.py')
                port = hello['port']
            case['parent'] = execute(binary + ['/out/native-tests/archive-publication-probe', mode, str(port)], 180)
            save()
            if proxy:
                proxy.process.stdin.write(b'stop\n')
                proxy.process.stdin.flush()
                case['proxy_exit'] = proxy.wait(10)
                case['proxy'] = [json.loads(line) for line in proxy.stdout.splitlines()]
                case['proxy_stderr'] = proxy.stderr.decode()
                save()
                assert case['proxy_exit'] == 0 and not case['proxy_stderr']
                terminal = case['proxy'][-1]
                assert terminal['event'] == 'terminal' and terminal['unexpected'] == 0
                assert terminal['data_queries'] == terminal['data_commits'] == terminal['faults'] == 1
                assert terminal['client_complete'] == 0 and terminal['server_complete'] == int(mode == 'after')
            assert case['parent']['exit'] == 0 and not case['parent']['stderr'], 'actual parent failed'
            case['reader'] = execute(binary + ['/out/native-tests/archive-read-probe', mode], 45)
            save()
            assert case['reader']['exit'] == 0 and not case['reader']['stderr'], 'actual native readback failed'
            check(mode, [json.loads(line) for line in case['parent']['stdout'].splitlines()],
                  json.loads(case['reader']['stdout']))
            case['catalog_files'] = json.loads(sql(db, "SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT d.data_file_id,d.path,d.path_is_relative,d.file_size_bytes,d.record_count,t.table_name,t.path AS table_path,s.path AS schema_path FROM ducklake_data_file d JOIN ducklake_table t ON t.table_id=d.table_id JOIN ducklake_schema s ON s.schema_id=t.schema_id ORDER BY t.table_name,d.data_file_id)t;"))
            data = root / f'out/signed_{mode}/kind=7'
            case['physical_files'] = [dict(path=str(p.relative_to(data)), bytes=p.stat().st_size, sha256=sha(p))
                                      for p in sorted(data.rglob('*.parquet'))]
            case['active_writers'] = int(sql(db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND left(usename,11)='bb_archive_';"))
            save()
            assert case['active_writers'] == 0
            assert len(case['physical_files']) == 8
            physical = {row['path']: row for row in case['physical_files']}
            assert len(physical) == 8
            assert len(case['catalog_files']) == (2 if mode == 'before' else 8)
            for file in case['catalog_files']:
                assert file['path_is_relative'] and file['schema_path'] == 'main/'
                assert file['table_path'] == file['table_name'] + '/'
                path = file['schema_path'] + file['table_path'] + file['path']
                assert path in physical and physical[path]['bytes'] == file['file_size_bytes']
                bootstrap_attempt = 'prepared-' + f'{182 + 8 * ["normal", "before", "after"].index(mode):02x}' * 16
                bootstrap_dictionary = file['table_name'] == 'dictionaries' and file['path'].split('/')[0] == bootstrap_attempt
                assert file['record_count'] == (5 if bootstrap_dictionary else 1)
            # Missing dictionaries and duplicate receipt checks must precede COPY;
            # exactly two prepared attempts exist, including the retained rollback.
            attempts = {Path(row['path']).parts[-2] for row in case['physical_files']}
            n = 180 + 8 * ['normal', 'before', 'after'].index(mode)
            assert attempts == {'prepared-' + f'{a:02x}' * 16 for a in (n + 2, n + 3)}
            case['status'] = 'PASS'
            save()
            print(json.dumps(dict(mode=mode, status='PASS', catalog_files=len(case['catalog_files']),
                                  physical_files=len(physical))), flush=True)
        finally:
            if proxy:
                proxy.stop()
    result['status'] = 'PASS'
    save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    main(parser.parse_args().root.resolve())
