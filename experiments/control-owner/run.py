#!/usr/bin/env python3
"""Real Control adapter/backend exclusion, isolated native PostgreSQL 16.15."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab


def require(value, reason):
    if not value: raise RuntimeError(reason)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    root = parser.parse_args().root.resolve(strict=True)
    state = lab.load(root)
    require(not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty-source')
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    require(revision == state['build']['build_info']['source']['revision'], 'source-identity')
    require(json.loads(lab.cli(state, 'build-info').stdout) == state['build']['build_info'], 'actual-cli-identity')
    mounts = json.loads(lab.docker('inspect', state['client']).stdout)[0]['Mounts']
    build = Path(next(m['Source'] for m in mounts if m['Destination'] == '/build' and not m['RW']))
    receipt = json.loads((build / 'control-owner-probe-build.json').read_text())
    require(receipt['source_revision'] == revision, 'probe-source')
    require(lab.docker('exec', state['client'], 'sha256sum', '/build/control_owner_probe').stdout.split()[0] == receipt['binary_sha256'], 'probe-bytes')
    output = root / 'control-owner.json'
    require(not output.exists(), 'existing-report')
    report = {'format':'bigbrotr-control-owner-native/1', 'source':revision, 'probe':receipt, 'status':'RUNNING', 'cases':[], 'observations':{}, 'server':state['server']}
    def save(): lab.write_json(output, report)
    def prepare(name, active=True, target=7):
        database = 'bb_owner_' + name
        lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
        path = lab.config(root, state, database)
        local = root / Path(path).name
        local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application'))
        require(lab.migrate(state, 'apply', path, target=802).returncode == 0, 'migration-802')
        lab.sql(state, database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        require(lab.cli(state, 'config', 'initialize', '--file', path).returncode == 0, 'initialize')
        if active: require(lab.cli(state, 'config', 'activate-initial', '--file', path, '--request-id', '41'*16, '--expected-revision', '1').returncode == 0, 'activate')
        if target == 7:
            before = rows(database)
            require(lab.migrate(state, 'apply', path, target=7).returncode == 0, 'migration-7')
            after = rows(database)
            require(all(before[n] == after[n] for n in before if n != 'schema_versions'), 'upgrade-data-preservation')
            lab.sql(state, database, 'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
        return database, path
    def rows(database):
        names = json.loads(lab.sql(state, database, "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control';"))
        require(len(names) == 30, 'nonempty-table-inventory')
        return {n:json.loads(lab.sql(state, database, f"SELECT coalesce(json_agg(t),'[]') FROM control.{n} t;")) for n in names}
    save()
    try:
        database, path = prepare('legacy', target=802)
        lab.sql(state, database, "INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us) SELECT decode(repeat('31',16),'hex'),dataset_id,'control',1 FROM control.datasets; INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state) SELECT decode(repeat('32',16),'hex'),1,dataset_id,'control',decode(repeat('31',16),'hex'),decode(repeat('33',16),'hex'),1,2,'active' FROM control.datasets;")
        before = lab.schema_digest(state, database)
        result = lab.migrate(state, 'apply', path, target=7)
        require(result.returncode != 0 and before == lab.schema_digest(state, database), 'legacy-upgrade-atomic-refusal')
        report['cases'].append({'name':'unqualified-legacy-history','result':'PASS','exit_code':result.returncode})
        for mode in ['no-policy', 'wrong-dataset', 'wrong-instance', 'wrong-mode', 'invalid-policy', 'lifecycle']:
            database, _ = prepare(mode.replace('-', '_'), active=mode != 'no-policy')
            if mode == 'invalid-policy':
                lab.sql(state, database, "ALTER TABLE control.policy_revisions DISABLE TRIGGER immutable_policy_revisions; UPDATE control.policy_revisions SET document_digest = decode(repeat('ff',32),'hex'); ALTER TABLE control.policy_revisions ENABLE TRIGGER immutable_policy_revisions;")
            before = rows(database)
            digest = lab.schema_digest(state, database)
            result = lab.docker('exec', state['client'], '/build/control_owner_probe', mode, database, '/lab/secrets/application', check=False)
            (root / (mode + '.stdout')).write_text(result.stdout)
            (root / (mode + '.stderr')).write_text(result.stderr)
            require(result.returncode == 0, 'actual-probe-' + mode)
            value = json.loads(result.stdout)
            after = rows(database)
            if mode != 'lifecycle':
                require(digest == lab.schema_digest(state, database) and before == after, 'refusal-mutation-' + mode)
            else:
                controls = sorted((r for r in after['owner_epochs'] if r['role'] == 'control'), key=lambda r:r['epoch'])
                require(len(controls) == 3 and [r['epoch'] for r in controls] == [1,2,3], 'epoch-history')
                require(len({r['resource_id'] for r in controls}) == 1, 'stable-resource')
                for i,r in enumerate(controls):
                    require(r['state'] == 'released' and r['exclusion_method'] == 'storage_fenced' and r['control_fence_version'] == 1 and r['control_backend_pid'] > 0 and r['control_backend_start'] is not None, 'whole-release-attribution')
                    successor = controls[1]['process_incarnation'] if i == 0 else r['process_incarnation']
                    expected = hashlib.sha256(b'bigbrotr/control-exclusion/1\0' + bytes.fromhex(r['dataset_id'][2:] + r['resource_id'][2:]) + r['epoch'].to_bytes(8,'big') + bytes.fromhex(r['process_incarnation'][2:] + successor[2:])).hexdigest()
                    require(r['exclusion_digest'] == '\\x' + expected, 'independent-exclusion-digest')
                require(len(after['owner_epochs']) == 4 and len(after['role_instances']) == 4, 'owner-role-cardinality')
                require(sum(r['retired_at_us'] is not None for r in after['role_instances']) == 3, 'retired-control-roles')
                require(len(after['source_streams']) == 1 and len(after['producer_slots']) == 1 and after['datasets'][0]['topology_generation'] == 0, 'registered-ingest')
                changed = {n for n in before if before[n] != after[n]}
                require(changed == {'owner_epochs','role_instances','datasets','topology_generations','topology_streams','source_streams','producer_slots','producer_epochs','allocation_domains'}, 'complete-mutation-set')
            report['observations'][database] = {n:{'rows':len(v),'sha256':hashlib.sha256(json.dumps(v,sort_keys=True).encode()).hexdigest()} for n,v in after.items()}
            report['cases'].append({'name':mode, 'result':'PASS', 'output':value})
            save()
        report['status']='PASS'
        report['not_run']=['transaction-lock-after-session-unlock','SQL-attribution-negative-matrix','COMMIT-response-faults','PG-crash-restart','paused-client-beyond-TTL','native-writer-exclusion','production-runtime-root-validator','load','arm64']
    except BaseException as error:
        report['status']='FAIL'; report['failure']=str(error); raise
    finally: save()
    print(json.dumps({'status':report['status'],'cases':len(report['cases'])}))

if __name__ == '__main__': main()
