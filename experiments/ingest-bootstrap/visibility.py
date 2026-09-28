#!/usr/bin/env python3
"""Actual PG/RocksDB visibility reader; independent raw dictionary oracle."""
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
from oracle import expected

root, build = map(Path, sys.argv[1:])
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
receipt = json.loads((build / 'build-result.json').read_text())
assert receipt['source'] == revision
assert hashlib.sha256((build / 'ingest_bootstrap_probe').read_bytes()).hexdigest() == receipt['probe_sha256']
report = dict(format='bigbrotr-ingest-visibility-native/1', source=revision, build=receipt,
              status='RUNNING', cases=[], not_run=['current acquisition source grant', 'socket admission',
              'policy epoch transition', 'read response loss', 'mid-FFI cancellation',
              'load and aged state', 'arm64', 'release'])

def save():
    lab.write_json(root / 'visibility-result.json', report)

def record(name, condition, **details):
    report['cases'].append(dict(name=name, status='PASS' if condition else 'FAIL', **details))
    save()
    if not condition:
        raise RuntimeError(name)

try:
    lab.setup(root, build, SOURCE, 'visibility_' + revision[:7], subnet='172.16.240.32/28')
    state = lab.load(root)
    report['server'] = state['server']
    save()
    modes = ['visibility', 'visibility-expired', 'visibility-cancelled', 'visibility-policy', 'visibility-closing']
    for mode in modes:
        database = 'bb_' + mode.replace('-', '_')
        lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator')
        config = lab.config(root, state, database)
        migration = lab.migrate(state, 'apply', config, target=19)
        record(mode + '-migrations', migration.returncode == 0, stdout=migration.stdout, stderr=migration.stderr)
        lab.sql(state, database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        initialized = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe',
            'initialize-fixture', database, '/lab/secrets/application', '/lab/unused', check=False)
        record(mode + '-initialized', initialized.returncode == 0, stdout=initialized.stdout, stderr=initialized.stderr)
        result = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe',
            mode, database, '/lab/secrets/application', '/lab/store_' + mode, check=False, timeout=45)
        (root / (mode + '.stdout')).write_text(result.stdout)
        (root / (mode + '.stderr')).write_text(result.stderr)
        messages = [json.loads(line) for line in result.stdout.splitlines()]
        record(mode + '-actual-adapter', result.returncode == 0 and not result.stderr
               and messages[-1]['event'] == 'visibility-after' and messages[-1]['status'] == 'PASS',
               exit_code=result.returncode, stderr=result.stderr)
        before, after = messages[1]['state'], messages[-1]['state']
        assert messages[1]['event'] == 'visibility-before'
        record(mode + '-all-state-unchanged', before == after and len(before['control_tables']) == 64,
               native_rows=before['native_rows'], sequence=before['sequence'],
               native_sha256=before['native_sha256'], control_sha256=before['control_sha256'],
               control_tables=len(before['control_tables']), originals=before['originals'])
        if mode == 'visibility':
            reads = [m for m in messages if m['event'] == 'visibility-read']
            record('literal-visible-classes', reads == [dict(event='visibility-read', ordinal=n, **{'class':c})
                   for n, c in [(1, 'public'), (2, 'private'), (3, None)]])
        dumped = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe', 'dump', '/lab/store_' + mode)
        (root / (mode + '.rocks.json')).write_text(dumped.stdout)
        rows = [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
        # The old independent corpus owns descriptor/fact/index bytes. Current
        # receipt persistence has its own codec: do not relabel it as that format.
        wanted, _, _ = expected(bytes([1]) * 16, True)
        record(mode + '-independent-dictionary-bytes',
               [r for r in rows if r[0] != 'receipts'] == [r for r in wanted if r[0] != 'receipts'])
        active = lab.sql(state, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='bigbrotr-ingest-bootstrap-probe'")
        record(mode + '-drivers-joined', active == '0')
    report['status'] = 'PASS_SCOPED_INGEST_VISIBILITY'
except BaseException as error:
    report.update(status='FAIL', failure=str(error))
    raise
finally:
    if root.exists():
        save()
        if (root / 'resources.json').exists():
            state = json.loads((root / 'resources.json').read_text())
            # The outer exact-Git owner also handles partial setup. Stop only
            # containers that exist and match this run's recorded ownership.
            for name in ('client', 'postgres'):
                result = lab.docker('inspect', state[name], check=False)
                if result.returncode:
                    continue
                item = json.loads(result.stdout)[0]
                assert item['Config']['Labels'][lab.LABEL] == state['owner']
                if item['State']['Running']:
                    lab.docker('stop', '--time', '10', item['Id'])
                logs = lab.docker('logs', item['Id'])
                (root / (name + '.stdout')).write_text(logs.stdout)
                (root / (name + '.stderr')).write_text(logs.stderr)
            state['state'] = 'stopped-retained'
            lab.write_json(root / 'resources.json', state)
print(json.dumps(dict(status=report['status'], cases=len(report['cases']))))
