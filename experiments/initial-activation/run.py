#!/usr/bin/env python3
"""Actual first policy activation, durable receipts and PostgreSQL commit faults."""
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
from fault_cases import Process


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    root = parser.parse_args().root.resolve(strict=True)
    state = lab.load(root)
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    require(not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty-source')
    require(revision == state['build']['build_info']['source']['revision'] and Path(state['source']) == SOURCE, 'source-identity')
    require(json.loads(lab.cli(state, 'build-info').stdout) == state['build']['build_info'], 'mounted-build-identity')
    require(lab.docker('exec', state['client'], 'sha256sum', '/build/bigbrotr').stdout.split()[0] == state['binary_sha256'], 'mounted-binary-bytes')
    mounts = json.loads(lab.docker('inspect', state['client']).stdout)[0]['Mounts']
    build = Path(next(m['Source'] for m in mounts if m['Destination'] == '/build' and not m['RW']))
    output = root / 'initial-activation.json'
    require(not output.exists(), 'existing-report')
    report = {'format': 'bigbrotr-initial-activation-native/1', 'source': revision, 'binary_sha256': state['binary_sha256'], 'status': 'RUNNING', 'cases': [], 'fault_containers': [], 'observations': {}, 'server': state['server']}
    frozen = json.loads((SOURCE / 'experiments/configuration-inspection/policy.json').read_text())
    policy_digest = sha(b'bigbrotr/product-policy/1\0' + json.dumps(frozen, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode())
    require(policy_digest == 'da36c9bdae45e20a4a99f61e82e3ce1f46f6352ba0bddbf23b2ecf2879d94ea9', 'policy-fixture')
    request = '41' * 16
    databases = []

    def save(): lab.write_json(output, report)

    def prepare(name, mode='managed', target=802, initialize=True):
        database = 'bb_activation_' + name
        lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
        databases.append(database)
        path = lab.config(root, state, database)
        local = root / Path(path).name
        local.write_text(lab.change(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application'), 'config', 'authority_mode', mode))
        migration = lab.migrate(state, 'apply', path, target=target)
        require(migration.returncode == 0, 'migration-' + name)
        lab.sql(state, database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT ON control.datasets, control.config_revisions, control.principals, control.management_audit, control.policy_revisions TO bigbrotr_control; GRANT UPDATE(config_revision,policy_revision) ON control.datasets TO bigbrotr_control;')
        if target == 802: lab.sql(state, database, 'GRANT INSERT ON control.management_commands TO bigbrotr_control;')
        if initialize: require(lab.cli(state, 'config', 'initialize', '--file', path).returncode == 0, 'initialize-' + name)
        return database, path, local

    def args(path, request_id=request, expected='1'):
        return ['config', 'activate-initial', '--file', path, '--request-id', request_id, '--expected-revision', expected]

    def observe(database, active, policy=frozen, config_count=1, role_count=0):
        names = json.loads(lab.sql(state, database, "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control';"))
        require(len(names) == 30, 'table-inventory')
        tables = {name: json.loads(lab.sql(state, database, f"SELECT coalesce(json_agg(t),'[]') FROM control.{name} t;")) for name in names}
        expected = {'schema_versions': 5, 'datasets': 1, 'config_revisions': config_count, 'principals': 1, 'management_audit': 1 + int(active), 'policy_revisions': int(active), 'management_commands': int(active), 'role_instances': role_count}
        require(all(len(rows) == expected.get(name, 0) for name, rows in tables.items()), 'complete-row-cardinalities-' + database)
        d = tables['datasets'][0]
        require(d['config_revision'] == config_count and d['policy_revision'] == (1 if active else None) and d['topology_generation'] is None, 'dataset-heads')
        require(tables['config_revisions'][0]['document'] == policy, 'complete-frozen-policy')
        if active:
            p, m = tables['policy_revisions'][0], tables['management_commands'][0]
            a = next(row for row in tables['management_audit'] if row['action'] == 'policy.activate-initial')
            c = tables['config_revisions'][0]
            require(p == {'revision': 1, 'dataset_id': d['dataset_id'], 'previous_revision': None, 'config_revision': 1, 'schema_version': 1, 'document': policy, 'document_digest': c['document_digest'], 'accepted_at_us': m['accepted_at_us'], 'actor_id': tables['principals'][0]['principal_id']}, 'whole-policy-snapshot')
            require(m['actor_id'] == a['actor_id'] == p['actor_id'] and m['audit_id'] == a['audit_id'] and m['request_key'] == a['subject_id'] and m['document_digest'] == c['document_digest'] and m['expected_config_revision'] == a['config_revision'] == 1 and m['policy_revision'] == 1 and m['authority_mode'] == c['authority_mode'] and m['action'] == a['action'] == 'policy.activate-initial' and a['outcome'] == 'accepted' and a['reason'] is None and a['requester_database_role'] == 'bigbrotr_control' and a['requester_os_uid'] == os.getuid() and a['created_at_us'] == p['accepted_at_us'], 'receipt-audit-binding')
            expected_digest = sha(b'bigbrotr/activate-initial/1\0' + bytes.fromhex(d['dataset_id'][2:] + d['instance_id'][2:]) + (1).to_bytes(8, 'big') + bytes([c['authority_mode'] == 'declarative']) + bytes.fromhex(c['document_digest'][2:]))
            require(m['request_digest'] == '\\x' + expected_digest, 'independent-command-digest')
        report['observations'][database] = {name: {'rows': len(rows), 'sha256': sha(json.dumps(rows, sort_keys=True).encode())} for name, rows in tables.items()}
        save()
        return tables

    def call(name, database, path, code=0, status='activated', reason=None, unchanged=False, request_id=request, expected='1'):
        before = lab.schema_digest(state, database)
        result = lab.cli(state, *args(path, request_id, expected))
        value = json.loads(result.stdout) if result.stdout else None
        require(result.returncode == code, name + '-exit-' + str(result.returncode))
        if code == 0:
            table = json.loads(lab.sql(state, database, "SELECT row_to_json(m) FROM control.management_commands m;"))
            identity = json.loads(lab.sql(state, database, "SELECT row_to_json(d) FROM control.datasets d;"))
            expected_value = {'format': 'bigbrotr-initial-activation/1', 'status': status, 'dataset_id': identity['dataset_id'][2:], 'instance_id': identity['instance_id'][2:], 'request_id': request_id, 'config_revision': expected, 'security_revision': '1', 'installation_actor': table['actor_id'][2:], 'audit_id': table['audit_id'][2:], 'policy_sha256': table['document_digest'][2:], 'request_sha256': table['request_digest'][2:], 'initial_barrier': 'no-prior-runtime', 'runtime_authorization': 'not-granted'}
            require(value == expected_value and not result.stderr, name + '-complete-response')
        else:
            require(value is None and result.stderr == 'error: initial policy activation refused: ' + reason + '\n', name + '-redacted-error')
        after = lab.schema_digest(state, database)
        if unchanged: require(before == after, name + '-unexpected-write')
        report['cases'].append({'name': name, 'status': 'PASS', 'exit': code, 'output': value, 'unchanged_dump': before if unchanged else None}); save()
        return value

    try:
        db, path, local = prepare('empty', initialize=False)
        call('uninitialized', db, path, 3, reason='Prerequisite', unchanged=True)
        db, path, local = prepare('old', target=805)
        call('missing-migration', db, path, 3, reason='Prerequisite', unchanged=True)
        # Upgrade real initialized state: existing rows survive; no implicit activation.
        before = json.loads(lab.sql(state, db, "SELECT json_build_object('config',(SELECT json_agg(c) FROM control.config_revisions c),'audit',(SELECT json_agg(a) FROM control.management_audit a));"))
        require(lab.migrate(state, 'apply', path, target=802).returncode == 0, 'upgrade')
        require(before == json.loads(lab.sql(state, db, "SELECT json_build_object('config',(SELECT json_agg(c) FROM control.config_revisions c),'audit',(SELECT json_agg(a) FROM control.management_audit a));")), 'upgrade-preservation')
        lab.sql(state, db, 'GRANT SELECT,INSERT ON control.management_commands TO bigbrotr_control;')
        observe(db, False); call('upgraded-activation', db, path); observe(db, True)
        db, path, local = prepare('managed')
        call('wrong-revision', db, path, 3, reason='RevisionConflict', expected='2', unchanged=True)
        original = local.read_text()
        local.write_text(original + '\n[ingest]\nmax_event_bytes="512KiB"\n')
        call('managed-unused-seed', db, path); observe(db, True)
        call('same-request', db, path, status='already-activated', unchanged=True)
        call('new-request-after-activation', db, path, 3, reason='ExistingActivity', request_id='42' * 16, unchanged=True)
        for key in ['dataset_id', 'instance_id']:
            local.write_text(lab.change(original, 'topology', key, '99' * 16))
            call('wrong-' + key, db, path, 3, reason='WrongDeployment', unchanged=True)
        local.write_text(lab.change(original, 'config', 'authority_mode', 'declarative'))
        call('wrong-mode', db, path, 3, reason='PolicyInvalid', unchanged=True)
        local.write_text(original)
        lab.sql(state, db, "BEGIN; INSERT INTO control.config_revisions SELECT 2,dataset_id,1,schema_version,authority_mode,document,document_digest,accepted_source_digest,accepted_at_us+1,actor_id FROM control.config_revisions WHERE revision=1; UPDATE control.datasets SET config_revision=2; COMMIT;")
        call('changed-content-same-key', db, path, 3, reason='RequestConflict', expected='2', unchanged=True)
        call('historical-receipt-after-config-advance', db, path, status='already-activated', unchanged=True); observe(db, True, config_count=2)
        db, path, local = prepare('declarative', mode='declarative')
        original = local.read_text(); local.write_text(original + '\n[ingest]\nmax_event_bytes="512KiB"\n')
        call('declarative-drift', db, path, 3, reason='PolicyInvalid', unchanged=True)
        local.write_text(original + '\n# equivalent policy\n')
        call('declarative-equivalent', db, path); observe(db, True)
        db, path, local = prepare('activity')
        lab.sql(state, db, "INSERT INTO control.role_instances SELECT decode(repeat('22',16),'hex'),dataset_id,'control',1,2 FROM control.datasets;")
        call('retired-role-refuses-first-activation', db, path, 3, reason='ExistingActivity', unchanged=True); observe(db, False, role_count=1)
        db, path, local = prepare('denied')
        lab.sql(state, db, 'REVOKE INSERT ON control.management_commands FROM bigbrotr_control;')
        call('receipt-insert-rollback', db, path, 1, reason='Backend', unchanged=True); observe(db, False)
        lab.sql(state, db, 'GRANT INSERT ON control.management_commands TO bigbrotr_control;')
        call('privilege-recovery', db, path); observe(db, True)
        db, path, local = prepare('invalid')
        lab.sql(state, db, "ALTER TABLE control.config_revisions DISABLE TRIGGER immutable_config_revisions; UPDATE control.config_revisions SET document_digest=decode(repeat('00',32),'hex'); ALTER TABLE control.config_revisions ENABLE TRIGGER immutable_config_revisions;")
        call('digest-corruption', db, path, 3, reason='PolicyInvalid', unchanged=True)
        lab.sql(state, db, "ALTER TABLE control.config_revisions DISABLE TRIGGER immutable_config_revisions; UPDATE control.config_revisions SET document_digest=decode('" + policy_digest + "','hex'); ALTER TABLE control.config_revisions ENABLE TRIGGER immutable_config_revisions;")
        call('restored-policy', db, path); observe(db, True)
        for scenario, mode in [('before', 'before-forward'), ('after', 'after-complete'), ('same', 'hold'), ('different', 'hold'), ('killed', 'hold')]:
            db, path, local = prepare(scenario)
            container = state['client'] + '-' + scenario
            require(lab.docker('container', 'inspect', container, check=False).returncode != 0, 'existing-fault-container')
            record = {'name': container, 'owner': state['owner'], 'state': 'creating'}
            report['fault_containers'].append(record); save()
            lab.docker('run', '-d', '--init', '--name', container, '--label', 'org.bigbrotr.activation-fault=' + state['owner'], '--network', 'container:' + state['postgres'], '--user', f'{os.getuid()}:{os.getgid()}', '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--mount', f'type=bind,src={root},dst=/lab', '--mount', f'type=bind,src={build},dst=/build,readonly', '--mount', f'type=bind,src={SOURCE},dst=/source,readonly', state['client_image'], 'python3', '-c', 'import time; time.sleep(180)')
            record['id'] = json.loads(lab.docker('inspect', container).stdout)[0]['Id']; record['state'] = 'running'; save()
            processes = []
            try:
                proxy = Process(['docker', 'exec', '-i', container, 'python3', '-B', '/source/experiments/control-migrations/fault_proxy.py', '--upstream-port', '5432', '--mode', mode, '--deadline-seconds', '60'], input_pipe=True); processes.append(proxy)
                ready = proxy.event('ready', 10)
                require(ready['proxy_sha256'] == sha((SOURCE / 'experiments/control-migrations/fault_proxy.py').read_bytes()), 'proxy-source')
                proxyfile = root / (db + '_proxy.toml')
                proxyfile.write_text(lab.change(lab.change(local.read_text(), 'postgres.control', 'endpoint', f'127.0.0.1:{ready["listen_port"]}'), 'postgres.control.tls', 'local_boundary', 'loopback'))
                app = Process(['docker', 'exec', container, '/build/bigbrotr', *args('/lab/' + proxyfile.name)]); processes.append(app)
                if mode == 'hold':
                    proxy.event('held', 15); observe(db, False)
                    if scenario in ['same', 'different']:
                        other = Process(['docker', 'exec', state['client'], '/build/bigbrotr', *args(path, request if scenario == 'same' else '42' * 16)]); processes.append(other)
                        deadline = time.monotonic() + 4
                        while lab.sql(state, db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock';") != '1':
                            require(time.monotonic() < deadline, 'blocked-concurrent-activation'); time.sleep(.05)
                        proxy.send('release')
                        require(other.wait(15) == (0 if scenario == 'same' else 3), 'concurrent-result')
                        if scenario == 'same': require(json.loads(other.stdout)['status'] == 'already-activated', 'same-receipt')
                    else: lab.docker('kill', container)
                code = app.wait(15)
                if scenario != 'killed':
                    value = json.loads(app.stdout)
                    require(code == (0 if mode == 'hold' else 4) and value['status'] == ('activated' if mode == 'hold' else 'outcome-unknown') and not app.stderr, 'fault-outcome')
                    receipt = proxy.event('receipt', 10)
                    require([receipt[k] for k in ['commits_seen', 'commits_forwarded', 'server_commit_complete', 'client_commit_complete', 'faults']] == {'before': [1, 0, 0, 0, 1], 'after': [1, 1, 1, 0, 1], 'same': [1, 1, 1, 1, 1], 'different': [1, 1, 1, 1, 1]}[scenario], 'commit-boundary')
                    require(proxy.wait(5) == 0 and not proxy.stderr, 'proxy-terminal')
                else:
                    require(code == 137, 'actual-kill'); receipt = None
                report['cases'].append({'name': 'commit-' + scenario, 'status': 'PASS', 'exit': code, 'proxy': receipt}); save()
                present = scenario in ['after', 'same', 'different']
                observe(db, present)
                call('resolve-' + scenario, db, path, status='already-activated' if present else 'activated', unchanged=present); observe(db, True)
            finally:
                info = json.loads(lab.docker('inspect', container).stdout)[0]
                require(info['Id'] == record['id'] and info['Config']['Labels']['org.bigbrotr.activation-fault'] == state['owner'], 'fault-owner')
                if info['State']['Running']: lab.docker('stop', '--time', '3', container)
                for process in processes: process.stop()
                record['state'] = 'stopped-retained'; record['terminal'] = json.loads(lab.docker('inspect', container).stdout)[0]['State']; save()
        # Actual engine crash recovery preserves complete snapshots and receipts.
        before = {db: lab.schema_digest(state, db) for db in databases}
        lab.docker('kill', '--signal', 'KILL', state['postgres']); lab.docker('start', state['postgres'])
        deadline = time.monotonic() + 20
        while lab.docker('exec', state['postgres'], 'pg_isready', '-U', 'postgres', check=False).returncode:
            require(time.monotonic() < deadline, 'crash-recovery-timeout'); time.sleep(.1)
        require(before == {db: lab.schema_digest(state, db) for db in databases}, 'crash-durable-state')
        require(int(lab.sql(state, 'postgres', "SELECT count(*) FROM pg_stat_activity WHERE usename IN ('bigbrotr_control','bigbrotr_migrator');")) == 0, 'remaining-backends')
        report['cases'].append({'name': 'postgres-sigkill-recovery', 'status': 'PASS', 'database_dump_hashes': before})
        report.update(status='PASS', remaining_backends=0)
    except Exception as error:
        report.update(status='FAIL', failure=str(error)); raise
    finally: save()
    print(json.dumps({'status': report['status'], 'cases': len(report['cases']), 'report': str(output)}))


if __name__ == '__main__': main()
