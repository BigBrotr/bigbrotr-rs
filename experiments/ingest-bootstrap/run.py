#!/usr/bin/env python3
"""Real Control/native initial batches, process kills and independent full byte comparison."""
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
from oracle import expected
from event_oracle import expected_event


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--suite', choices=['bootstrap', 'event'], default='bootstrap')
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    state = lab.load(root)
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    require(not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty-source')
    require(revision == state['build']['build_info']['source']['revision'], 'source-identity')
    mounts = json.loads(lab.docker('inspect', state['client']).stdout)[0]['Mounts']
    build = Path(next(m['Source'] for m in mounts if m['Destination'] == '/build' and not m['RW']))
    probe = json.loads((build / 'ingest-bootstrap-probe-build.json').read_text())
    require(probe['source_revision'] == revision and lab.docker('exec', state['client'], 'sha256sum', '/build/ingest_bootstrap_probe').stdout.split()[0] == probe['binary_sha256'], 'probe-identity')
    event_suite = args.suite == 'event'
    output = root / ('ingest-event.json' if event_suite else 'ingest-bootstrap.json')
    require(not output.exists(), 'existing-report')
    report = {'format': 'bigbrotr-ingest-event-native/1' if event_suite else 'bigbrotr-ingest-bootstrap-native/1', 'source': revision, 'probe': probe,
        'status': 'RUNNING', 'cases': [], 'containers': [], 'server': state['server']}
    def save(): lab.write_json(output, report)
    def rows(database):
        names = json.loads(lab.sql(state, database, "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control';"))
        require(len(names) == 30, 'table-inventory')
        return {n: json.loads(lab.sql(state, database, f"SELECT coalesce(json_agg(t),'[]') FROM control.{n} t;")) for n in names}
    save()
    try:
        modes = ['write', 'cancelled', 'expired', 'retired', 'wrong-root', 'before', 'after'] if event_suite else ['write', 'cancelled', 'expired', 'retired', 'wrong-manifest', 'wrong-root', 'before', 'after']
        for case in modes:
            mode = 'event-' + case if event_suite else case
            db = 'bb_bootstrap_' + mode.replace('-', '_')
            lab.sql(state, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator;')
            path = lab.config(root, state, db)
            local = root / Path(path).name
            local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application'))
            require(lab.migrate(state, 'apply', path, target=7).returncode == 0, 'migration')
            lab.sql(state, db, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
            require(lab.cli(state, 'config', 'initialize', '--file', path).returncode == 0, 'initialize')
            require(lab.cli(state, 'config', 'activate-initial', '--file', path, '--request-id', '71' * 16, '--expected-revision', '1').returncode == 0, 'activate')
            before = rows(db)
            dataset = bytes.fromhex(before['datasets'][0]['dataset_id'][2:])
            command = ['/build/ingest_bootstrap_probe', mode, db, '/lab/secrets/application', '/lab/store_' + mode]
            if case in ['before', 'after']:
                name = state['client'] + '-' + mode
                require(lab.docker('inspect', name, check=False).returncode != 0, 'existing-container')
                record = {'name': name, 'state': 'creating'}
                report['containers'].append(record); save()
                record['id'] = lab.docker('run', '-d', '--init', '--name', name, '--label', 'org.bigbrotr.bootstrap=' + state['owner'],
                    '--network', state['network'], '--user', f'{os.getuid()}:{os.getgid()}', '--cpus', '2', '--memory', '512m', '--memory-swap', '512m', '--pids-limit', '128',
                    '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                    '--mount', f'type=bind,src={root},dst=/lab', '--mount', f'type=bind,src={build},dst=/build,readonly', state['client_image'], *command).stdout.strip()
                record['state'] = 'running'; save()
                marker = 'before-native' if case == 'before' else 'after-native-before-receipt'
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    result = lab.docker('logs', name)
                    if any(json.loads(line).get('event') == marker for line in result.stdout.splitlines()): break
                    info = json.loads(lab.docker('inspect', name).stdout)[0]
                    require(info['State']['Running'], 'early-probe-exit')
                    time.sleep(.05)
                else: raise RuntimeError('marker-deadline')
                info = json.loads(lab.docker('inspect', name).stdout)[0]
                require(info['Id'] == record['id'] and info['Config']['Labels']['org.bigbrotr.bootstrap'] == state['owner'], 'kill-ownership')
                lab.docker('kill', '--signal', 'KILL', name)
                lab.docker('wait', name)
                terminal = json.loads(lab.docker('inspect', name).stdout)[0]['State']
                require(terminal['ExitCode'] == 137 and not terminal['OOMKilled'] and not terminal['Running'], 'kill-terminal')
                record.update(state='stopped-retained', terminal=terminal); save()
                result = lab.docker('logs', name)
            else:
                result = lab.docker('exec', state['client'], *command, check=False)
            (root / (mode + '.stdout')).write_text(result.stdout)
            (root / (mode + '.stderr')).write_text(result.stderr)
            require(result.returncode == 0, 'probe-' + mode)
            require(not result.stderr, 'probe-stderr')
            messages = [json.loads(line) for line in result.stdout.splitlines()]
            require(messages and messages[0]['event'] == 'initialized', 'empty-workload')
            dumped = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe', 'dump', '/lab/store_' + mode)
            (root / (mode + '.rocks.json')).write_text(dumped.stdout)
            actual = json.loads(dumped.stdout)
            wanted, receipt, facts = expected_event(dataset, case in ['write', 'after']) if event_suite else expected(dataset, case in ['write', 'after'], policy=2 if case == 'wrong-manifest' else 1)
            tuples = [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in actual['rows']]
            require(actual['event'] == 'dump' and tuples == wanted, 'independent-complete-native-state-' + mode)
            if case == 'write':
                require(messages[-1] == {'event':'receipt', 'bytes':receipt.hex(), 'facts':[f.hex() for f in facts], 'replay_exact':True}, 'external-receipt-and-facts')
            elif case not in ['before', 'after']:
                require(messages[-1]['event'] == 'refused', 'missing-refusal')
            else:
                require(all(m['event'] != 'receipt' for m in messages), 'crash-delivered-receipt')
            after = rows(db)
            changed = {n for n in before if before[n] != after[n]}
            require(changed == {'datasets','owner_epochs','role_instances','topology_generations','topology_streams','source_streams','producer_slots','producer_epochs','allocation_domains'}, 'unexpected-pg-effects')
            require(len(after['owner_epochs']) == 2 and len(after['producer_epochs']) == 1 and len(after['allocation_domains']) == (3 if event_suite else 1), 'pg-cardinality')
            (root / (mode + '.control.json')).write_text(json.dumps(after, indent=2) + '\n')
            report['cases'].append({'name':mode, 'result':'PASS', 'native_rows':len(tuples), 'native_sha256':hashlib.sha256(dumped.stdout.encode()).hexdigest(), 'control_tables':len(after)})
            save()
        report['status'] = 'PASS'
        report['not_run'] = ['power-loss','ENOSPC','write-error-ambiguity','mid-FFI-kill','producer-closing-concurrent-with-FFI','maximum-batch-load','complete-event-admission-and-retirement','installed-runtime','successor-recovery-authority','arm64']
    except BaseException as error:
        report['status'] = 'FAIL'; report['failure'] = str(error); raise
    finally: save()
    print(json.dumps({'status':report['status'], 'cases':len(report['cases'])}))


if __name__ == '__main__': main()
