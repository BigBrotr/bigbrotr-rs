#!/usr/bin/env python3
"""Native local credential self-inspection, with an independent stored C vector."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


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
    output = root / 'self-inspection.json'
    require(not output.exists(), 'existing-report')
    report = {'format': 'bigbrotr-self-inspection-native/1', 'source': revision, 'binary_sha256': state['binary_sha256'], 'status': 'RUNNING', 'cases': [], 'server': state['server']}
    password = 'Independent owner password 2026'
    stored_hash = 'a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff'
    for name, content in [('owner-password', password), ('wrong-password', 'Wrong independent password 2026'), ('space-password', password + ' '), ('empty-password', ''), ('large-password', 'x' * 1025)]:
        file = root / 'secrets' / name
        file.write_text(content); file.chmod(0o600)
    database = 'bb_auth_self'
    lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator;')
    path = lab.config(root, state, database)
    local = root / Path(path).name
    local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application'))
    require(lab.migrate(state, 'apply', path, target=805).returncode == 0, 'migration')
    lab.sql(state, database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; GRANT INSERT ON control.datasets, control.config_revisions, control.principals, control.management_audit, control.local_accounts, control.local_credentials, control.principal_grants, control.owner_challenges TO bigbrotr_control; GRANT UPDATE(config_revision) ON control.datasets TO bigbrotr_control; GRANT UPDATE(state, failed_attempts, consumed_principal, consumed_at_us) ON control.owner_challenges TO bigbrotr_control;')
    require(lab.cli(state, 'config', 'initialize', '--file', path).returncode == 0, 'initialize')
    issued = lab.cli(state, 'access', 'owner-token', 'issue', '--file', path, '--username', 'alice', '--request-id', '31' * 16)
    require(issued.returncode == 0, 'issue')
    token = json.loads(issued.stdout)['token']
    token_file = root / 'secrets' / 'owner-token'; token_file.write_text(token); token_file.chmod(0o600)
    enrolled = lab.cli(state, 'access', 'enroll-owner', '--file', path, '--challenge-id', '31' * 16, '--token-ref', 'file:/lab/secrets/owner-token', '--password-ref', 'file:/lab/secrets/owner-password')
    require(enrolled.returncode == 0, 'enrollment')
    principal = json.loads(enrolled.stdout)['principal_id']
    dataset = lab.sql(state, database, "SELECT encode(dataset_id,'hex') FROM control.datasets;")
    expected = {'format': 'bigbrotr-local-account/1', 'dataset_id': dataset, 'principal_id': principal, 'username': 'alice', 'credential_revision': '1', 'grant_revision': '1', 'security_revision': None, 'stored_actions': ['audit.read', 'budgets.change', 'data.read', 'policy.change', 'principals.manage', 'roles.control', 'status.read'], 'resource_kind': 'dataset', 'resource_id': dataset, 'runtime_authorization': 'not-granted', 'session': 'not-issued'}

    def save():
        lab.write_json(output, report)

    def call(name, code=0, reason=None, secret='owner-password', username='alice', config=path, expected_value=None):
        before = lab.schema_digest(state, database)
        result = lab.cli(state, 'access', 'me', '--file', config, '--username', username, '--password-ref', 'file:/lab/secrets/' + secret)
        after = lab.schema_digest(state, database)
        require(result.returncode == code and before == after, name + '-outcome')
        require(all(value not in result.stdout + result.stderr for value in [password, stored_hash, token, 'file:/lab/secrets/']), name + '-secret-disclosure')
        if code == 0:
            require(not result.stderr and json.loads(result.stdout) == (expected if expected_value is None else expected_value), name + '-complete-observation')
        else:
            require(not result.stdout and result.stderr == 'error: ' + reason + '\n', name + '-redacted-refusal')
        report['cases'].append({'name': name, 'status': 'PASS', 'exit': code, 'unchanged_schema_sha256': before, 'output_sha256': hashlib.sha256(result.stdout.encode()).hexdigest()})
        save()

    try:
        call('actual-enrolled-password')
        # Freeze credential bytes from the separate PHC C oracle. Acceptance is
        # not merely enrollment's hasher agreeing with its own verifier.
        lab.sql(state, database, f"UPDATE control.local_credentials SET salt=decode(repeat('02',16),'hex'),password_hash=decode('{stored_hash}','hex');")
        call('independent-c-credential')
        denied = 'local authentication refused: Denied'
        call('wrong-password', 3, denied, secret='wrong-password')
        call('exact-whitespace', 3, denied, secret='space-password')
        call('unknown-account', 3, denied, username='unknown')
        call('empty-password', 3, denied, secret='empty-password')
        call('oversized-password', 3, 'cannot read protected account password', secret='large-password')
        lab.sql(state, database, 'UPDATE control.local_accounts SET active=false;')
        call('disabled-account', 3, denied)
        lab.sql(state, database, 'UPDATE control.local_accounts SET active=true,credential_revision=2;')
        call('credential-revision-mismatch', 3, 'local authentication refused: InvalidMetadata')
        lab.sql(state, database, 'UPDATE control.local_accounts SET credential_revision=1;')
        lab.sql(state, database, "DELETE FROM control.principal_grants WHERE action='policy.change'; UPDATE control.local_accounts SET grant_revision=2;")
        smaller = {**expected, 'grant_revision': '2', 'stored_actions': [a for a in expected['stored_actions'] if a != 'policy.change']}
        call('current-stored-grants', expected_value=smaller)
        lab.sql(state, database, 'DELETE FROM control.principal_grants; UPDATE control.local_accounts SET grant_revision=3;')
        empty = {**expected, 'grant_revision': '3', 'stored_actions': []}
        call('self-inspection-does-not-invent-rights', expected_value=empty)
        lab.sql(state, database, 'REVOKE SELECT ON control.local_credentials FROM bigbrotr_control;')
        call('database-privilege-refusal', 1, 'local authentication refused: Backend')
        lab.sql(state, database, 'GRANT SELECT ON control.local_credentials TO bigbrotr_control;')
        for key in ['dataset_id', 'instance_id']:
            changed = root / (key + '.toml')
            changed.write_text(lab.change(local.read_text(), 'topology', key, '99' * 16))
            call('wrong-' + key, 3, 'local authentication refused: WrongDeployment', config='/lab/' + changed.name)
        protected = root / 'secrets' / 'owner-password'
        protected.chmod(0o644)
        call('password-file-permissions', 3, 'cannot read protected account password')
        protected.chmod(0o600)
        before = lab.schema_digest(state, database)
        lab.docker('restart', state['postgres'])
        deadline = time.monotonic() + 15
        while lab.docker('exec', state['postgres'], 'pg_isready', '-U', 'postgres', check=False).returncode:
            require(time.monotonic() < deadline, 'restart-timeout'); time.sleep(.1)
        require(lab.schema_digest(state, database) == before, 'restart-preservation')
        call('restart', expected_value=empty)
        require(int(lab.sql(state, database, "SELECT count(*) FROM pg_stat_activity WHERE usename IN ('bigbrotr_control','bigbrotr_migrator');")) == 0, 'remaining-backends')
        report.update(status='PASS', database=database, final_schema_sha256=lab.schema_digest(state, database), remaining_backends=0, independent_credential='PHC C fixed salt02 vector; source and parameters in local-enrollment/reference_vector.c')
    except Exception as error:
        report.update(status='FAIL', failure=str(error)); raise
    finally:
        save()
    print(json.dumps({'status': report['status'], 'cases': len(report['cases']), 'report': str(output)}))


if __name__ == '__main__':
    main()
