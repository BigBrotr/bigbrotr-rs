#!/usr/bin/env python3
"""Exercise the real read-only configuration CLI against an explicitly owned PG lab."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def digest(value):
    return sha(b'bigbrotr/product-policy/3\0' + encoded(value).encode())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    state = lab.load(root)
    require(not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty-source')
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    require(revision == state['build']['build_info']['source']['revision'] and Path(state['source']) == SOURCE, 'source-mismatch')
    actual = lab.cli(state, 'build-info')
    require(actual.returncode == 0 and json.loads(actual.stdout) == state['build']['build_info'], 'mounted-artifact-identity')
    output = root / 'configuration-inspection.json'
    require(not output.exists(), 'existing-report')
    policy = json.loads((SOURCE / 'tests/fixtures/configuration/policy-v3.json').read_text())
    require(digest(policy) == '77324b5f6aea113a4e4b81e11d946c0c4e113b051851d398c240de49a7a6322b', 'frozen-policy-identity')
    report = {'format': 'bigbrotr-configuration-inspection/1', 'status': 'RUNNING', 'source': revision,
              'binary_sha256': state['binary_sha256'], 'server': state['server'], 'cases': [],
              'fixture_sha256': sha((SOURCE / 'tests/fixtures/configuration/policy-v3.json').read_bytes()),
              'scope': 'Actual CLI and PG read boundaries; independently inserted fixture rows, not product bootstrap or activation.'}

    def config(database):
        path = lab.config(root, state, database)
        local = root / Path(path).name
        text = lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application')
        local.write_text(text)
        return path, local

    def run(name, database, path, code, predicate):
        before = lab.schema_digest(state, database)
        result = lab.cli(state, 'config', 'inspect', '--file', path)
        value = json.loads(result.stdout) if result.stdout else None
        after = lab.schema_digest(state, database)
        passed = result.returncode == code and before == after and predicate(value, result.stderr)
        report['cases'].append({'name': name, 'status': 'PASS' if passed else 'FAIL', 'exit': result.returncode,
                                'stdout': value, 'stderr': result.stderr, 'before_dump_sha256': before, 'after_dump_sha256': after})
        lab.write_json(output, report)
        require(passed, 'case-failed-' + name)

    def accepted(value, error):
        return (not error and value['status'] == 'accepted-configuration-observed'
                and value['desired_revision'] == '1' and value['security_revision'] is None
                and value['applied_revision'] is None and value['activation'] == 'not-performed'
                and value['authorization'] == 'not-granted'
                and value['settings']['ingest.max_event_bytes']['value'] == '262144'
                and value['settings']['ingest.max_event_bytes']['source'] == 'accepted-policy')

    def write_policy(database, value, mode='managed', override_digest=None):
        # Mutations are controlled oracle setup under the isolated PG superuser.
        # They do not exercise, authorize or claim a product configuration write.
        text = encoded(value).replace("'", "''")
        fingerprint = override_digest or digest(value)
        lab.sql(state, database, "ALTER TABLE control.config_revisions DISABLE TRIGGER immutable_config_revisions; "
                f"UPDATE control.config_revisions SET document='{text}'::jsonb, document_digest=decode('{fingerprint}','hex'), authority_mode='{mode}'; "
                "ALTER TABLE control.config_revisions ENABLE TRIGGER immutable_config_revisions;")

    try:
        database = 'bb_configuration_read'
        lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
        path, local = config(database)
        migration = lab.migrate(state, 'apply', path, target=2)
        require(migration.returncode == 0 and json.loads(migration.stdout)['applied_count'] == '2', 'migration-prerequisite')
        lab.sql(state, database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control;')
        run('empty-dataset', database, path, 4, lambda v,e: not e and v['status'] == 'dataset-not-initialized')
        # Read only public root identities from the independently decoded CLI.
        shown = lab.cli(state, 'config', 'show', '--file', path)
        require(shown.returncode == 0, 'static-fixture-invalid')
        fields = json.loads(shown.stdout)['settings']
        dataset = fields['topology.dataset_id']['value']; instance = fields['topology.instance_id']['value']
        require(len(dataset) == len(instance) == 32 and all(c in '0123456789abcdef' for c in dataset+instance), 'root-identity-shape')
        lab.sql(state, database, f"INSERT INTO control.datasets(dataset_id,instance_id,created_at_us) VALUES(decode('{dataset}','hex'),decode('{instance}','hex'),123);")
        run('empty-configuration', database, path, 4, lambda v,e: not e and v['status'] == 'configuration-not-initialized')
        text = encoded(policy).replace("'", "''")
        source_digest = sha(local.read_bytes())
        lab.sql(state, database, f"BEGIN; INSERT INTO control.config_revisions(revision,dataset_id,previous_revision,schema_version,authority_mode,document,document_digest,accepted_source_digest,accepted_at_us,actor_id) VALUES(1,decode('{dataset}','hex'),NULL,3,'managed','{text}'::jsonb,decode('{digest(policy)}','hex'),decode('{source_digest}','hex'),123,decode(repeat('03',16),'hex')); UPDATE control.datasets SET config_revision=1; COMMIT;")
        run('managed-accepted', database, path, 0, lambda v,e: accepted(v,e) and not v['source_file_differs'] and not v['candidate_policy_differs'])
        original = local.read_text()
        local.write_text(original + '\n# staged source change\n')
        run('comments-only', database, path, 0, lambda v,e: accepted(v,e) and v['source_file_differs'] and not v['candidate_policy_differs'])
        local.write_text(original + '\n[ingest]\nmax_event_bytes="512KiB"\n')
        run('managed-seed-drift', database, path, 0, lambda v,e: accepted(v,e) and v['candidate_policy_differs'] and v['file_policy_use'] == 'unused-initialization-input')
        local.write_text(lab.change(original, 'config', 'authority_mode', 'declarative'))
        run('mode-mismatch', database, path, 3, lambda v,e: v is None and 'mode disagrees' in e)
        write_policy(database, policy, mode='declarative')
        run('declarative-staged', database, path, 0, lambda v,e: accepted(v,e) and v['source_file_differs'] and v['file_policy_use'] == 'staged-until-explicit-activation')
        local.write_text(original)
        maximum = copy.deepcopy(policy); maximum['settings']['sync.history_since'] = 18446744073709551615
        write_policy(database, maximum)
        run('full-u64-jsonb', database, path, 0, lambda v,e: accepted(v,e) and v['settings']['sync.history_since']['value'] == '18446744073709551615')
        fraction = copy.deepcopy(policy); fraction['settings']['monitor.schedule_jitter'] = 2.0030397744267762e-253
        write_policy(database, fraction)
        run('exact-fraction-jsonb', database, path, 0, lambda v,e: accepted(v,e) and v['settings']['monitor.schedule_jitter']['value'] == 2.0030397744267762e-253)
        private = copy.deepcopy(policy); private['settings']['sync.endpoints.mode'] = 'fixed'; private['settings']['sync.endpoints.urls'] = ['wss://fixture.example/PRIVATE_POLICY_CANARY?secret=fixture']
        write_policy(database, private)
        run('private-redaction', database, path, 0, lambda v,e: accepted(v,e) and 'PRIVATE_POLICY_CANARY' not in json.dumps(v)+e and v['settings']['sync.endpoints.urls']['value'] == '[redacted]')
        variants = {}
        value = copy.deepcopy(policy); value['settings'].pop('ranker.enabled'); variants['missing-key'] = value
        value = copy.deepcopy(policy); value['settings']['topology.dataset_id'] = dataset; variants['root-injection'] = value
        value = copy.deepcopy(policy); value['settings']['ingest.max_event_bytes'] = None; variants['required-null'] = value
        value = copy.deepcopy(policy); value['settings']['pipeline.prepare_slot_bytes'] = 4 * 1024**3; variants['root-budget-overflow'] = value
        value = copy.deepcopy(policy); value['format'] = 'bigbrotr-product-policy/2'; variants['superseded-format'] = value
        for name, value in variants.items():
            write_policy(database, value)
            run(name, database, path, 3, lambda v,e: v is None and e.startswith('error:'))
        write_policy(database, policy, override_digest='00'*32)
        run('digest-corruption', database, path, 3, lambda v,e: v is None and 'digest mismatch' in e)
        write_policy(database, policy)
        for key in ['dataset_id', 'instance_id']:
            local.write_text(lab.change(original, 'topology', key, 'ff'*16))
            run('wrong-'+key, database, path, 1, lambda v,e: v is None and 'WrongDeployment' in e)
        local.write_text(original)
        lab.sql(state, database, 'REVOKE SELECT ON control.config_revisions FROM bigbrotr_control;')
        run('denied-read', database, path, 1, lambda v,e: v is None and 'Backend' in e)
        lab.sql(state, database, 'GRANT SELECT ON control.config_revisions TO bigbrotr_control;')
        run('recovered-read', database, path, 0, accepted)
        require(int(lab.sql(state, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';")) == 0, 'client-backends-remain')
        require(len(report['cases']) == 20, 'incomplete-scenario-set')
        report['status'] = 'PASS'
    except Exception as error:
        report['status'] = 'FAIL'; report['failure'] = str(error)
        raise
    finally:
        lab.write_json(output, report)
    print(json.dumps({'status': report['status'], 'cases': len(report['cases']), 'report': str(output)}))


if __name__ == '__main__':
    main()
