#!/usr/bin/env python3
"""Real CLI/PG registration; all generated keys stay in the owned native lab."""
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    root = parser.parse_args().root.resolve(strict=True)
    state = lab.load(root)
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
    assert Path(state['source']) == SOURCE
    info = json.loads(lab.cli(state, 'build-info').stdout)
    assert info == state['build']['build_info'] and info['source']['revision'] == revision
    assert lab.docker('exec', state['client'], 'sha256sum', '/build/bigbrotr').stdout.split()[0] == state['binary_sha256']
    output = root / 'cursor-registration.json'
    assert not output.exists()
    report = dict(format='bigbrotr-cursor-registration-native/1', source=revision,
                  binary_sha256=state['binary_sha256'], server=state['server'],
                  status='RUNNING', cases=[], databases=[], fault_containers=[])
    fixtures = json.loads((SOURCE / 'tests/fixtures/read/keyring-vectors.json').read_text())
    bindings = json.loads((SOURCE / 'tests/fixtures/read/key-binding-vectors.json').read_text())
    wire = bytes.fromhex(fixtures['cases'][0]['hex'])

    def save():
        staging = output.with_suffix('.partial')
        staging.write_text(json.dumps(report, indent=2) + '\n')
        staging.replace(output)
    def record(name, passed, **observed):
        report['cases'].append(dict(name=name, status='PASS' if passed else 'FAIL', **observed))
        save()
        if not passed: raise RuntimeError('case failed: ' + name)
    def call(name, operation, path, code=0, status=None):
        result = lab.cli(state, 'access', 'cursor-keyring', operation, '--file', path)
        value = json.loads(result.stdout) if result.stdout else None
        record(name, result.returncode == code and (status is None or value and value['state'] == status),
               exit=result.returncode, output=value, stderr=result.stderr)
        if value: assert value['cursor_authority'] == 'not-granted'
        return value
    def prepare(name, target=601):
        database = 'bb_cursor_' + name
        lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
        report['databases'].append(database); save()
        path = lab.config(root, state, database)
        local = root / Path(path).name
        text = lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application')
        text = lab.change(text, 'topology', 'dataset_id', fixtures['dataset_id'])
        keys = root / ('keys-' + name); keys.mkdir(mode=0o700)
        file = keys / 'keyring'; file.write_bytes(wire); file.chmod(0o600)
        local.write_text(text + f'\n[read.cursor]\nkeyring_ref = "file:/lab/{keys.name}/keyring"\n')
        migrated = lab.migrate(state, 'apply', path, target=target)
        record('migrate-' + name, migrated.returncode == 0, exit=migrated.returncode, stderr=migrated.stderr)
        lab.sql(state, database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT ON control.datasets,control.config_revisions,control.principals,control.management_audit TO bigbrotr_control; GRANT UPDATE(config_revision) ON control.datasets TO bigbrotr_control;')
        initialized = lab.cli(state, 'config', 'initialize', '--file', path)
        record('initialize-' + name, initialized.returncode == 0, exit=initialized.returncode, stderr=initialized.stderr)
        if target == 601:
            lab.sql(state, database, 'GRANT INSERT ON control.cursor_keyrings,control.cursor_keys TO bigbrotr_control; GRANT EXECUTE ON FUNCTION control.guard_cursor_registration(),control.require_cursor_registration() TO bigbrotr_control;')
        return database, path, file
    def snapshot(database):
        return json.loads(lab.sql(state, database, "SELECT json_build_object('rings',(SELECT coalesce(json_agg(t),'[]') FROM (SELECT *,xmin::text AS row_version FROM control.cursor_keyrings ORDER BY dataset_id) t),'keys',(SELECT coalesce(json_agg(t),'[]') FROM (SELECT *,xmin::text AS row_version FROM control.cursor_keys ORDER BY dataset_id,registration_position) t),'audit',(SELECT coalesce(json_agg(t),'[]') FROM (SELECT *,xmin::text AS row_version FROM control.management_audit ORDER BY audit_id) t));"))
    def validate_receipt(name, database, receipt, observed, expected_state):
        assert len(observed['rings']) == 1
        ring = observed['rings'][0]
        audit = [row for row in observed['audit'] if row['action'] == 'cursor-keyring.register']
        principal = lab.sql(state, database, "SELECT encode(principal_id,'hex') FROM control.principals WHERE kind='installation' AND database_role='bigbrotr_control';")
        expected = dict(format='bigbrotr-cursor-registration/1', state=expected_state,
                        dataset_id=fixtures['dataset_id'], keyring_id=fixtures['keyring_id'],
                        key_ids=[b['key_id'] for b in bindings],
                        initial_file_fingerprint_sha256=fixtures['golden_fingerprint'],
                        cursor_authority='not-granted', actor_id=principal,
                        audit_id=ring['audit_id'][2:], registered_at_us=str(ring['registered_at_us']))
        expected_audit = dict(dataset_id='\\x'+fixtures['dataset_id'], actor_id='\\x'+principal,
                              audit_id=ring['audit_id'], action='cursor-keyring.register', outcome='accepted',
                              subject_id='\\x'+fixtures['keyring_id'], created_at_us=ring['registered_at_us'],
                              config_revision=None, policy_revision=None, reason=None,
                              requester_database_role='bigbrotr_control', requester_os_uid=os.getuid())
        record(name, receipt == expected and len(audit) == 1
               and all(audit[0][k] == v for k,v in expected_audit.items())
               and ring['actor_id'] == expected_audit['actor_id']
               and ring['dataset_id'] == expected_audit['dataset_id']
               and ring['keyring_id'] == expected_audit['subject_id']
               and ring['file_format'] == 1 and ring['key_count'] == len(bindings)
               and ring['initial_file_digest'] == '\\x'+fixtures['golden_fingerprint']
               and [(r['dataset_id'],r['keyring_id'],r['key_id'][2:],r['material_digest'][2:],r['registration_position']) for r in observed['keys']]
                   == [('\\x'+fixtures['dataset_id'],'\\x'+fixtures['keyring_id'],b['key_id'],b['digest'],i+1) for i,b in enumerate(bindings)],
               receipt=receipt, expected=expected, observed=observed)
    def recover(name, database, path, committed):
        before = snapshot(database)
        expected_state = 'already-registered' if committed else 'registered'
        receipt = call(name, 'register', path, status=expected_state)
        after = snapshot(database)
        validate_receipt(name+'-sql-receipt', database, receipt, after, expected_state)
        if committed: record(name+'-no-sql-write', after == before)
        return receipt
    try:
        database, path, file = prepare('initial')
        first = call('register', 'register', path, status='registered')
        before = snapshot(database); before_dump = lab.schema_digest(state, database)
        validate_receipt('initial-sql-receipt', database, first, before, 'registered')
        expected = [(r['key_id'], r['digest'], i + 1) for i, r in enumerate(bindings)]
        actual = [(r['key_id'][2:], r['material_digest'][2:], r['registration_position']) for r in before['keys']]
        ring = before['rings'][0]
        record('exact-independent-binding', actual == expected and len(before['rings']) == 1
               and ring['initial_file_digest'] == '\\x' + fixtures['golden_fingerprint']
               and ring['keyring_id'] == '\\x' + fixtures['keyring_id'] and ring['key_count'] == 2
               and file.read_bytes() == wire, observed=before)
        retry = call('retry', 'register', path, status='already-registered')
        validate_receipt('retry-sql-receipt', database, retry, snapshot(database), 'already-registered')
        record('retry-original-receipt-and-no-sql-write',
               {k:v for k,v in retry.items() if k != 'state'} == {k:v for k,v in first.items() if k != 'state'}
               and snapshot(database) == before and lab.schema_digest(state, database) == before_dump)
        changed = bytearray(wire); changed[58] ^= 1
        changed[-32:] = hashlib.sha256(changed[:-32]).digest(); file.write_bytes(changed)
        call('changed-material-refused', 'register', path, code=3)
        record('conflict-preserves-control', snapshot(database) == before and lab.schema_digest(state, database) == before_dump)
        file.write_bytes(wire)
        retained = root / 'retained-original-keyfile'
        file.rename(retained)
        call('missing-registered-key-refused', 'register', path, code=4)
        record('missing-never-regenerates', not file.exists() and snapshot(database) == before)
        retained.rename(file)
        recover('recovery-same-registration', database, path, True)
        old_database, old_path, _ = prepare('old', target=814)
        old_before = lab.schema_digest(state, old_database)
        call('explicit-migration-required', 'register', old_path, code=3)
        record('old-schema-unchanged', lab.schema_digest(state, old_database) == old_before)
        denied_database, denied_path, _ = prepare('denied')
        lab.sql(state, denied_database, 'REVOKE INSERT ON control.cursor_keys FROM bigbrotr_control;')
        denied_before = lab.schema_digest(state, denied_database)
        call('late-key-insert-permission', 'register', denied_path, code=1)
        record('audit-and-registration-rollback', lab.schema_digest(state, denied_database) == denied_before)
        lab.sql(state, denied_database, 'GRANT INSERT ON control.cursor_keys TO bigbrotr_control;')
        recover('permission-recovery', denied_database, denied_path, False)
        schema_db, _, _ = prepare('schema')
        dataset = "decode('" + fixtures['dataset_id'] + "','hex')"
        ring_id = "decode('" + fixtures['keyring_id'] + "','hex')"
        audit_id = "decode(repeat('61',16),'hex')"
        actor = f"(SELECT principal_id FROM control.principals WHERE dataset_id={dataset} AND kind='installation')"
        audit_sql = f"INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,subject_id,requester_database_role,requester_os_uid) VALUES({audit_id},{dataset},{actor},'cursor-keyring.register','accepted',123,{ring_id},SESSION_USER,1000);"
        header_sql = f"INSERT INTO control.cursor_keyrings VALUES({dataset},{ring_id},1,decode('{fixtures['golden_fingerprint']}','hex'),2,123,{actor},{audit_id});"
        members = [f"INSERT INTO control.cursor_keys VALUES({dataset},{ring_id},decode('{b['key_id']}','hex'),{i+1},decode('{b['digest']}','hex'));" for i,b in enumerate(bindings)]
        def sql_case(name, statement, expected, role='bigbrotr_control', unchanged=True):
            before = lab.schema_digest(state, schema_db)
            result = lab.docker('exec', '-i', state['postgres'], 'psql', '-X', '-A', '-t', '-q',
                                '--no-password', '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=sqlstate',
                                '-U', role, '-d', schema_db, data=statement, check=False)
            (root / (name + '.stdout')).write_text(result.stdout)
            (root / (name + '.stderr')).write_text(result.stderr)
            passed = (result.returncode == 0 and expected is None) or (result.returncode != 0 and expected is not None and 'ERROR:  ' + expected in result.stderr)
            if unchanged: passed = passed and lab.schema_digest(state, schema_db) == before
            record(name, passed, exit=result.returncode, expected_sqlstate=expected, stderr=result.stderr)
        for name, body, expected in [
            ('missing-member', audit_sql + header_sql + members[0], '23514'),
            ('audit-alone', audit_sql, 'P0002'),
            ('missing-audit', header_sql + ''.join(members), '23503'),
            ('missing-registration', members[0], '23503'),
            ('duplicate-position', audit_sql + header_sql + members[0] + members[1].replace(',2,decode(', ',1,decode('), '23505'),
            ('early-constraint-flush', audit_sql + header_sql + 'SET CONSTRAINTS ALL IMMEDIATE;' + ''.join(members), '23514'),
            ('mismatched-audit-subject', audit_sql.replace(',123,' + ring_id, ",123,decode(repeat('55',16),'hex')") + header_sql + ''.join(members), '23514'),
            ('extra-member', audit_sql + header_sql + ''.join(members) + members[0].replace(bindings[0]['key_id'], '55'*16).replace(',1,decode(', ',3,decode('), '23514'),
        ]:
            sql_case('sql-' + name, 'BEGIN;' + body + 'COMMIT;', expected)
        sql_case('sql-complete-commit', 'BEGIN;' + audit_sql + header_sql + ''.join(members) + 'COMMIT;', None, unchanged=False)
        record('sql-independent-complete-set', len(snapshot(schema_db)['keys']) == 2 and len(snapshot(schema_db)['rings']) == 1)
        for relation in ['cursor_keyrings', 'cursor_keys']:
            for operation in [f'UPDATE control.{relation} SET dataset_id=dataset_id;', f'DELETE FROM control.{relation};', f'TRUNCATE control.{relation} CASCADE;']:
                verb = operation.split()[0].lower()
                sql_case('sql-immutable-' + relation + '-' + verb, operation, '23514', role='bigbrotr_migrator')
                sql_case('sql-denied-' + relation + '-' + verb, operation, '42501')
        mounts = json.loads(lab.docker('inspect', state['client']).stdout)[0]['Mounts']
        build = next(m['Source'] for m in mounts if m['Destination'] == '/build' and not m['RW'])
        fault = state['client'] + '-cursor-fault'
        assert lab.docker('container', 'inspect', fault, check=False).returncode != 0
        entry = dict(name=fault, state='creating', owner=state['owner'])
        report['fault_containers'].append(entry); save()
        lab.docker('run', '-d', '--init', '--name', fault,
                   '--label', 'org.bigbrotr.cursor-registration=' + state['owner'],
                   '--network', 'container:' + state['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
                   '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128',
                   '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                   '--cap-add', 'SYS_PTRACE', '--security-opt', 'seccomp=unconfined',
                   '--mount', f'type=bind,src={root},dst=/lab', '--mount', f'type=bind,src={build},dst=/build,readonly',
                   '--mount', 'type=bind,src=/home/tux/bigbrotr-implementation-20260917/runs/strace-exit-9b30c35/out,dst=/instrument,readonly',
                   '--mount', f'type=bind,src={SOURCE},dst=/source,readonly', state['client_image'],
                   'python3', '-c', 'import time; time.sleep(600)')
        entry.update(id=json.loads(lab.docker('inspect', fault).stdout)[0]['Id'], state='running'); save()
        processes = []
        try:
            for scenario in ['blocked-output', 'sync-error']:
                scenario_db, scenario_path, keyfile = prepare(scenario.replace('-', '_'))
                baseline = snapshot(scenario_db)
                result_path = root / (scenario + '.json')
                result = lab.docker('exec', fault, 'python3', '-B', '/source/experiments/cursor-registration/syscall_cases.py',
                                    scenario, scenario_path, '/lab/' + result_path.name, check=False)
                observation = json.loads(result_path.read_text()) if result_path.exists() else None
                record('syscall-' + scenario, result.returncode == 0 and observation and observation['status'] == 'PASS',
                       exit=result.returncode, stdout=result.stdout, stderr=result.stderr, observation=observation)
                after = snapshot(scenario_db)
                if scenario == 'sync-error':
                    staging = list(keyfile.parent.glob('.cursor-keyring-*.partial'))
                    record('sync-error-no-control-effects', after == baseline and keyfile.read_bytes() == wire and len(staging) == 1,
                           retained_files=[dict(name=p.name, bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in keyfile.parent.iterdir()])
                else:
                    record('blocked-output-committed-registration', len(after['rings']) == 1 and len(after['keys']) == 2,
                           observed=after)
                    recover('blocked-output-retry', scenario_db, scenario_path, True)
                    record('blocked-output-retry-no-sql-write', snapshot(scenario_db) == after and keyfile.read_bytes() == wire)
            for scenario, mode in [('before', 'before-forward'), ('after', 'after-complete'),
                                   ('same', 'hold'), ('different', 'hold'), ('killed', 'hold')]:
                fault_db, fault_path, fault_file = prepare(scenario)
                baseline = snapshot(fault_db)
                proxy = Process(['docker', 'exec', '-i', fault, 'python3', '-B',
                                 '/source/experiments/control-migrations/fault_proxy.py',
                                 '--upstream-port', '5432', '--mode', mode, '--deadline-seconds', '45'], input_pipe=True)
                processes.append(proxy)
                ready = proxy.event('ready', 10)
                assert ready['proxy_sha256'] == hashlib.sha256((SOURCE / 'experiments/control-migrations/fault_proxy.py').read_bytes()).hexdigest()
                original = (root / Path(fault_path).name).read_text()
                proxyfile = root / (scenario + '-proxy.toml')
                proxyfile.write_text(lab.change(lab.change(original, 'postgres.control', 'endpoint',
                                    f'127.0.0.1:{ready["listen_port"]}'), 'postgres.control.tls', 'local_boundary', 'loopback'))
                app = Process(['docker', 'exec', fault, '/build/bigbrotr', 'access', 'cursor-keyring', 'register', '--file', '/lab/' + proxyfile.name])
                processes.append(app)
                other = None
                if mode == 'hold':
                    held = proxy.event('held', 15)
                    record('uncommitted-' + scenario, snapshot(fault_db) == baseline
                           and held['held'] and held['mode'] == 'hold' and held['commit_number'] == 1
                           and [held[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete']] == [1,0,0,0], proxy=held)
                    if scenario == 'killed':
                        lab.docker('kill', '--signal', 'KILL', fault)
                    else:
                        second = root / (scenario + '-second.toml')
                        directory = root / (scenario + '-second-keys'); directory.mkdir(mode=0o700)
                        second_wire = wire
                        if scenario == 'different':
                            material = bytearray(wire); material[58] ^= 1
                            material[-32:] = hashlib.sha256(material[:-32]).digest(); second_wire = bytes(material)
                        second_keys = directory / 'keyring'; second_keys.write_bytes(second_wire); second_keys.chmod(0o600)
                        second.write_text(lab.change(original, 'read.cursor', 'keyring_ref', 'file:/lab/' + directory.name + '/keyring'))
                        other = Process(['docker', 'exec', state['client'], '/build/bigbrotr', 'access', 'cursor-keyring', 'register', '--file', '/lab/' + second.name])
                        processes.append(other)
                        deadline = time.monotonic() + 4
                        while True:
                            blocked = json.loads(lab.sql(state, fault_db, "SELECT coalesce(json_agg(json_build_object('pid',pid,'wait_event_type',wait_event_type,'blockers',pg_blocking_pids(pid))),'[]') FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control' AND wait_event_type='Lock';"))
                            if any(r['blockers'] for r in blocked): break
                            if time.monotonic() >= deadline: raise RuntimeError('registration did not block on original transaction')
                            time.sleep(.02)
                        record('actual-registration-lock-wait-' + scenario, other.process.poll() is None, blocked=blocked)
                        proxy.send('release')
                code = app.wait(15)
                if scenario == 'killed':
                    record('killed-before-commit', code == 137, exit=code)
                else:
                    expected_code = 0 if mode == 'hold' else 4
                    record('commit-response-' + scenario, code == expected_code
                           and (b'COMMIT outcome unknown' in app.stderr if expected_code == 4 else not app.stderr),
                           exit=code, stdout=bytes(app.stdout).decode(), stderr=bytes(app.stderr).decode())
                    receipt = proxy.event('receipt', 10)
                    expected = {'before':[1,0,0,0,1], 'after':[1,1,1,0,1], 'same':[1,1,1,1,1], 'different':[1,1,1,1,1]}[scenario]
                    record('observed-commit-boundary-' + scenario,
                           [receipt[k] for k in ['commits_seen','commits_forwarded','server_commit_complete','client_commit_complete','faults']] == expected
                           and proxy.wait(5) == 0 and not proxy.stderr, proxy=receipt)
                if other:
                    code = other.wait(15)
                    record('concurrent-resolution-' + scenario,
                           code == (0 if scenario == 'same' else 3)
                           and (json.loads(other.stdout)['state'] == 'already-registered' if scenario == 'same' else not other.stdout),
                           exit=code, stdout=bytes(other.stdout).decode(), stderr=bytes(other.stderr).decode())
                # Observe actual backend disappearance before interpreting absence.
                deadline = time.monotonic() + 5
                while lab.sql(state, fault_db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';") != '0':
                    if time.monotonic() >= deadline: raise RuntimeError('registration backend remains live')
                    time.sleep(.02)
                committed = scenario in ['after', 'same', 'different']
                observed = snapshot(fault_db)
                record('durable-outcome-' + scenario,
                       (len(observed['rings']) == 1 and len(observed['keys']) == 2 if committed else observed == baseline), observed=observed)
                if mode == 'hold' and scenario != 'killed':
                    validate_receipt('commit-sql-receipt-'+scenario, fault_db, json.loads(app.stdout), observed, 'registered')
                if scenario == 'same':
                    validate_receipt('concurrent-sql-receipt', fault_db, json.loads(other.stdout), observed, 'already-registered')
                recover('same-input-recovery-' + scenario, fault_db, fault_path, committed)
                record('protected-keys-unchanged-' + scenario, fault_file.read_bytes() == wire)
        finally:
            metadata = json.loads(lab.docker('inspect', fault).stdout)[0]
            assert metadata['Id'] == entry['id'] and metadata['Config']['Labels']['org.bigbrotr.cursor-registration'] == state['owner']
            if metadata['State']['Running']: lab.docker('stop', '--time', '3', fault)
            for process in processes: process.stop()
            entry.update(state='stopped-retained', terminal=json.loads(lab.docker('inspect', fault).stdout)[0]['State']); save()
        before_restart = snapshot(database)
        lab.docker('kill', '--signal', 'KILL', state['postgres'])
        lab.docker('start', state['postgres'])
        deadline = time.monotonic() + 30
        while lab.docker('exec', state['postgres'], 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres', check=False).returncode:
            if time.monotonic() >= deadline: raise RuntimeError('engine did not restart')
            time.sleep(.1)
        recover('engine-restart-retry', database, path, True)
        record('engine-restart-preserves-receipt', snapshot(database) == before_restart)
        for checked in report['databases']:
            record('no-client-backends-' + checked,
                   lab.sql(state, checked, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename IN ('bigbrotr_control','bigbrotr_migrator');") == '0')
        report['status'] = 'PASS'
        report['not_run'] = ['worker activation/rotation/distribution', 'host power loss', 'Compose release journeys']
    except Exception:
        report['status'] = 'FAIL'
        raise
    finally:
        save()


if __name__ == '__main__': main()
