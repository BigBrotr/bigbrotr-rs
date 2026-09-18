#!/usr/bin/env python3
"""Independent populated-801 upgrade check for the exact enrollment candidate."""
import hashlib
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
runner_sha256 = sys.argv[2]
state = json.loads((root / 'resources.json').read_text())
source = Path(state['source'])
sys.path.insert(0, str(source / 'experiments/control-migrations'))
import lab


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def contents(database):
    names = json.loads(lab.sql(state, database, "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control';"))
    return {name: json.loads(lab.sql(state, database, f"SELECT coalesce(json_agg(t),'[]') FROM control.{name} t;")) for name in names}


require(state['owner'] == '8ff3fd9ddc7c624683f942cbb8b86acd', 'wrong-lab')
require(not lab.command(['git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty-product-source')
build = json.loads(lab.cli(state, 'build-info').stdout)
require(build == state['build']['build_info'] and build['source']['revision'] == '992f0dca6ca66af1eab5be27ff5e0f03391c2d12', 'artifact-source')
require(lab.docker('exec', state['client'], 'sha256sum', '/build/bigbrotr').stdout.split()[0] == state['binary_sha256'], 'artifact-bytes')
database = 'bb_owner_upgrade'
output = root / 'populated-upgrade.json'
require(not output.exists(), 'existing-report')
lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
report = {'status': 'RUNNING', 'source': build['source']['revision'], 'runner_sha256': runner_sha256, 'binary_sha256': state['binary_sha256'], 'database': database}
try:
    path = lab.config(root, state, database)
    local = root / Path(path).name
    local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application'))
    require(lab.migrate(state, 'apply', path, target=801).returncode == 0, 'initial-801')
    lab.sql(state, database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT ON control.datasets, control.config_revisions, control.principals, control.management_audit TO bigbrotr_control; GRANT UPDATE(config_revision) ON control.datasets TO bigbrotr_control;')
    initialized = lab.cli(state, 'config', 'initialize', '--file', path)
    require(initialized.returncode == 0, 'initialize-801')
    receipt = json.loads(initialized.stdout)
    before = contents(database)
    require(len(before) == 25 and len(before['principals']) == len(before['management_audit']) == 1, 'populated-start')
    migrated = lab.migrate(state, 'apply', path, target=805)
    require(migrated.returncode == 0 and json.loads(migrated.stdout)['current_version'] == '805', 'upgrade-805')
    after = contents(database)
    require(set(after) - set(before) == {'local_accounts', 'local_credentials', 'principal_grants', 'owner_challenges'}, 'new-relations')
    for name, rows in before.items():
        if name == 'schema_versions':
            require([r for r in after[name] if r['migration'] != 805] == rows, 'old-migration-receipts')
        elif name == 'management_audit':
            expected = [{**row, 'subject_id': None, 'reason': None, 'requester_database_role': None, 'requester_os_uid': None} for row in rows]
            require(after[name] == expected, 'old-audit-preservation')
        else:
            require(after[name] == rows, 'old-content-' + name)
    require(all(not after[name] for name in set(after) - set(before)), 'no-synthetic-accounts')
    replay = lab.cli(state, 'config', 'initialize', '--file', path)
    require(replay.returncode == 0, 'initialize-replay')
    observed = json.loads(replay.stdout)
    require(observed['status'] == 'already-initialized' and observed['installation_actor'] == receipt['installation_actor'] and observed['audit_id'] == receipt['audit_id'], 'stable-bootstrap-receipt')
    require(contents(database) == after, 'replay-mutated-state')
    refused = lab.migrate(state, 'apply', path, target=801)
    require(refused.returncode == 1 and 'downgrade' in refused.stderr and contents(database) == after, 'downgrade-refusal')
    report.update(status='PASS', old_relations=25, new_relations=4, old_payload_rows_preserved=True, old_audit_columns_preserved=True, no_synthetic_accounts=True, bootstrap_receipt_unchanged=True, downgrade_refused=True,
                  before_sha256=hashlib.sha256(json.dumps(before, sort_keys=True).encode()).hexdigest(), after_sha256=hashlib.sha256(json.dumps(after, sort_keys=True).encode()).hexdigest())
except Exception as error:
    report.update(status='FAIL', failure=str(error))
    raise
finally:
    output.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
