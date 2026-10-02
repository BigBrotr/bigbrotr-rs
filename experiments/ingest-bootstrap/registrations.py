"""Installed authenticated URL→enrollment journey; original engine states retained."""
import hashlib
import json
from pathlib import Path
import shutil
import lab
from installed import messages, native_oracle, policy_setup, set_value, start, stop, terminal, wait_for
from sessions import configuration

CASES = ['registration-upgrade', 'registration-journey', 'registration-permission',
         'registration-prerequisite', 'registration-candidate', 'registration-digest', 'registration-schema']


def upgrade(pg, client, output, database, name, rows):
    before = rows(database)
    physical = lambda: json.loads(lab.sql(pg, database, """
        SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid))
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='control' AND c.relkind='r'
    """))
    files = physical()
    state = dict(pg, client=client)
    config = lab.config(output, state, database, endpoint='127.0.0.1:5432', boundary='loopback')
    def apply():
        return lab.cli(state, 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '25')
    applied = apply()
    (output / (name + '-migration.stdout')).write_text(applied.stdout)
    (output / (name + '-migration.stderr')).write_text(applied.stderr)
    assert applied.returncode == 0, applied.stderr
    after = rows(database)
    stripped = json.loads(json.dumps(after))
    for row in stripped['producer_requests']:
        assert row.pop('acquisition_scope_id') is None
        assert row.pop('acquisition_job_id') is None
    for row in stripped['jobs']:
        assert row.pop('acquisition_job_protocol') is None
    assert all(stripped[t] == before[t] for t in before if t != 'schema_versions')
    assert set(after) - set(before) == {
        'acquisition_selectors', 'acquisition_scopes', 'acquisition_jobs', 'relay_registration_commands'}
    assert all(not after[t] for t in set(after) - set(before))
    expected = {4: '0004_acquisition.sql', 21: '0021_acquisition_bindings.sql',
        22: '0022_acquisition_jobs.sql', 23: '0023_acquisition_claims.sql',
        24: '0024_acquisition_execution.sql', 25: '0025_relay_registration.sql'}
    added = [r for r in after['schema_versions'] if r not in before['schema_versions']]
    assert {r['migration'] for r in added} == set(expected) and len(added) == len(expected)
    assert all(r in after['schema_versions'] for r in before['schema_versions'])
    source = Path(__file__).resolve().parents[2]
    for row in added:
        assert row['checksum'] == '\\x' + hashlib.sha256(
            (source / 'migrations/postgres' / expected[row['migration']]).read_bytes()).hexdigest()
    current_files = physical()
    assert all(current_files[t] == node for t, node in files.items())
    reapplied = apply()
    (output / (name + '-reapply.stdout')).write_text(reapplied.stdout)
    (output / (name + '-reapply.stderr')).write_text(reapplied.stderr)
    assert reapplied.returncode == 0 and rows(database) == after and physical() == current_files
    lab.write_json(output / (name + '-upgrade.json'), dict(before=before, after=after,
        physical_before=files, physical_after=current_files))
    lab.sql(pg, database, '''
        GRANT SELECT ON control.acquisition_selectors,control.acquisition_scopes,control.acquisition_jobs TO bigbrotr_control;
        GRANT SELECT,INSERT ON control.relay_registration_commands TO bigbrotr_control;
        GRANT EXECUTE ON FUNCTION control.relay_registration_digest(
            control.relay_registration_commands,control.management_commands) TO bigbrotr_control;
    ''')
    return before, after


def run(pg, client, output, database, name, rows):
    assert name in CASES
    if name != 'registration-schema':
        previous, upgraded = upgrade(pg, client, output, database, name, rows)
    if name == 'registration-upgrade':
        assert previous['relay_endpoints'] and previous['relay_enrollment_changes'] and previous['producer_requests']
        return dict(name=name, status='PASS', actual_migration=True, original_rows_xmin_filenodes=True,
                    control_tables=len(upgraded))
    functions = {
        'registration-candidate': 'control.relay_endpoint_candidate(bytea)',
        'registration-digest': 'control.relay_registration_digest(control.relay_registration_commands,control.management_commands)',
    }
    prerequisite = name not in ('registration-journey', 'registration-permission')
    for filename in ('installed_client.py', 'session_client.py', 'registration_client.py'):
        if not (output / filename).exists():
            shutil.copy2(Path(__file__).with_name(filename), output / filename)
    policy_setup(pg, database, name, rows, output, administration=True)
    if name == 'registration-permission':
        lab.sql(pg, database, "UPDATE control.principal_grants SET action='principals.manage'")
    if name == 'registration-prerequisite':
        lab.sql(pg, database, 'REVOKE INSERT ON control.relay_registration_commands FROM bigbrotr_control')
    if name in functions:
        signature = functions[name]
        lab.sql(pg, database, f'REVOKE EXECUTE ON FUNCTION {signature} FROM bigbrotr_control')
        assert lab.sql(pg, database, f"SELECT has_function_privilege('bigbrotr_control','{signature}','EXECUTE')") == 'f'
        lab.write_json(output / (name + '-revoked.json'), dict(function=signature, app_execute=False))
    before = rows(database)
    assert not before['relay_endpoints'] and not before['owner_epochs']
    lab.write_json(output / (name + '-before.json'), before)
    state = dict(pg, client=client)
    config = lab.config(output, state, database, endpoint='127.0.0.1:5432', boundary='loopback')
    path = output / Path(config).name
    text = configuration(path.read_text(), before, name)
    rule = [dict(endpoint='ws://127.0.0.1:7447/installed-fixture', cidr='127.0.0.1/32', port=7447)]
    text = set_value(text, 'network.private', 'rules', rule)
    assert text.count(json.dumps(rule)) == 1
    text = text.replace(json.dumps(rule), '[{endpoint="ws://127.0.0.1:7447/installed-fixture",cidr="127.0.0.1/32",port=7447}]')
    path.write_text(text)
    validated = lab.cli(state, 'config', 'validate', '--file', config)
    (output / (name + '-validate.stdout')).write_text(validated.stdout)
    (output / (name + '-validate.stderr')).write_text(validated.stderr)
    assert validated.returncode == 0, validated.stderr
    directory = output / name
    directory.mkdir(mode=0o700)
    prefix = start(client, config, output, name)
    first = wait_for(lambda: messages(prefix) or Path(str(prefix)+'.terminal.json').exists(), 'registration startup')
    initialized = None
    if prerequisite:
        if messages(prefix):
            stop(client, prefix)
            terminal(prefix)
            lab.write_json(output / (name + '-unexpected-after.json'), rows(database))
            raise RuntimeError('missing registration prerequisite bound the listener: ' + name)
        ended = terminal(prefix)
        assert ended['exit_code'] == 4
        assert 'initial relay registration prerequisites unavailable: Prerequisite' in Path(str(prefix)+'.stderr').read_text()
        lab.docker('exec', client, 'python3', '-c',
            'import socket; s=socket.socket(); assert s.connect_ex(("127.0.0.1",18081))!=0; s.close()')
    else:
        assert isinstance(first, list) and len(first) == 1 and first[0]['state'] == 'initialized', first
        initialized = first[0]
        assert initialized['listeners'][0]['profile'] == 'initial-source-commands'
        assert not initialized['ready'] and initialized['acquisition'] == 'not-started'
        lab.write_json(output / (name + '-initialized.json'), rows(database))
        visibility = dict(initialized['public_scope'], ordinal='2')
        enrollment = dict(visibility=visibility, route={'class':'private','cidr':'127.0.0.1/32'},
            enabled=True, expected_generation=None, expected_config_revision='1', expected_policy_revision='1')
        lab.write_json(directory / 'command-input.json', dict(enrollment=enrollment, permitted=name=='registration-journey'))
        wrapper = '''import subprocess,sys,json
from pathlib import Path
p=Path(sys.argv[1])
with (p/'client.stdout').open('xb') as out,(p/'client.stderr').open('xb') as err:
 r=subprocess.run(['python3','/lab/session_client.py',str(p),'registration'],stdout=out,stderr=err,timeout=20)
(p/'client-exit.json').write_text(json.dumps({'exit_code':r.returncode}))
'''
        lab.docker('exec', '-d', client, 'python3', '-c', wrapper, '/lab/' + name)
        if name == 'registration-journey':
            snapshots = []
            for stage in ('before-replay', 'after-replay'):
                wait_for(lambda: (directory/(stage+'.json')).exists() or (directory/'client-exit.json').exists(), stage, 10)
                assert (directory/(stage+'.json')).exists(), (directory/'client.stderr').read_text()
                observed = rows(database)
                lab.write_json(output / (name + '-' + stage + '-sql.json'), observed)
                snapshots.append(observed)
                (directory/(stage+'.continue')).write_text('observed\n')
            assert all(snapshots[0][t] == snapshots[1][t] for t in snapshots[0] if t != 'owner_epochs')
            stable = lambda r: {k:v for k,v in r.items() if k not in ('lease_expires_at_us','xmin')}
            assert [stable(r) for r in snapshots[0]['owner_epochs']] == [stable(r) for r in snapshots[1]['owner_epochs']]
        wait_for((directory/'client-exit.json').exists, 'registration HTTP client terminal', 20)
        assert json.loads((directory/'client-exit.json').read_text())['exit_code'] == 0, (directory/'client.stderr').read_text()
        stop(client, prefix)
        ended = terminal(prefix)
        assert ended['exit_code'] == 0 and not Path(str(prefix)+'.stderr').read_text()
        assert messages(prefix)[-1]['owner_release'] == 'committed'
    after = rows(database)
    lab.write_json(output / (name + '-after.json'), after)
    assert len(after['owner_epochs']) == len(after['role_instances']) == 2
    if initialized:
        native_oracle(pg, client, output, database, name, after, initialized)
        assert len(after['browser_sessions']) == 1 and after['browser_sessions'][0]['revoked_at_us'] is not None
    else:
        from commits import native_oracle as retained_oracle
        retained_oracle(client, output, name, after, number=6, healthy=True, last_commit=6)
        assert not after['browser_sessions']
    audits = [r for r in after['management_audit'] if r not in before['management_audit']]
    expected = [] if not initialized else ['browser.session.issue', 'browser.session.revoke']
    if name == 'registration-journey':
        expected += ['relay.endpoint.register'] * 3 + ['relay.enrollment.change'] * 2
        assert len(after['relay_endpoints']) == 2 and len(after['relay_registration_commands']) == 3
        assert len(after['relay_enrollments']) == len(after['network_routes']) == len(after['relay_access_contexts']) == 1
        assert not after['relay_enrollments'][0]['enabled'] and after['relay_enrollments'][0]['generation'] == 2
        assert len(after['relay_enrollment_changes']) == 2
        assert len(after['management_commands']) == len(before['management_commands']) + 5
        registration_oracle(after)
    else:
        for table in ('relay_endpoints','relay_enrollments','network_routes','relay_access_contexts',
                      'relay_enrollment_changes','management_commands'):
            assert after[table] == before[table]
        if 'relay_registration_commands' in before:
            assert after['relay_registration_commands'] == before['relay_registration_commands']
    assert sorted(r['action'] for r in audits) == sorted(expected)
    assert all(r in after['management_audit'] for r in before['management_audit'])
    allowed = {'datasets','owner_epochs','role_instances','topology_generations','topology_streams',
        'source_streams','allocation_domains','producer_slots','producer_epochs','producer_requests',
        'producer_outcomes','browser_sessions','login_buckets','management_audit','management_commands',
        'relay_endpoints','relay_registration_commands','relay_enrollments','network_routes',
        'relay_access_contexts','relay_enrollment_changes'}
    assert {t for t in before if before[t] != after[t]} <= allowed
    assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
    return dict(name=name, status='PASS', actual_cli=True, actual_http=initialized is not None,
                startup_refusal=prerequisite, control_tables=len(after), terminal=ended)


def registration_oracle(state):
    """Literal expected URLs and separately encoded command digests/attribution."""
    dataset = state['datasets'][0]['dataset_id']
    actor = state['local_accounts'][0]
    session = state['browser_sessions'][0]
    owner = next(r for r in state['owner_epochs'] if r['role'] == 'control')
    canonical = 'ws://127.0.0.1:7447/installed-fixture'
    prefix = 'wss://example.test/?'
    maximum = prefix + '"' * (8192 - len(prefix))
    endpoint = {bytes.fromhex(r['url_bytes'][2:]).decode(): r for r in state['relay_endpoints']}
    assert set(endpoint) == {canonical, maximum}
    assert len(state['relay_registration_commands']) == 3
    for nonce, url in [('91', canonical), ('92', canonical), ('93', maximum)]:
        key = bytes.fromhex(nonce * 16)
        stored_key = '\\x' + key.hex()
        detail = next(r for r in state['relay_registration_commands'] if r['request_key'] == stored_key)
        command = next(r for r in state['management_commands'] if r['request_key'] == stored_key)
        audit = next(r for r in state['management_audit'] if r['audit_id'] == command['audit_id'])
        original = endpoint[url]
        assert detail['endpoint_id'] == original['endpoint_id']
        assert detail['dataset_id'] == command['dataset_id'] == audit['dataset_id'] == dataset
        assert detail['actor_id'] == command['actor_id'] == audit['actor_id'] == actor['principal_id']
        assert command['action'] == audit['action'] == 'relay.endpoint.register'
        assert command['expected_config_revision'] == command['policy_revision'] == 1
        assert audit['config_revision'] == audit['policy_revision'] == 1 and audit['outcome'] == 'accepted'
        assert audit['subject_id'] == stored_key and audit['created_at_us'] == command['accepted_at_us']
        assert detail['session_id'] == session['session_id'] and detail['owner_role'] == 'control'
        for field in ('credential_revision', 'authentication_revision', 'grant_revision'):
            assert detail['actor_' + field] == actor[field]
        for field, current in [('owner_resource_id','resource_id'), ('owner_epoch','epoch'), ('owner_incarnation','process_incarnation')]:
            assert detail[field] == original[field] == owner[current]
        raw = url.encode()
        digest_input = (b'bigbrotr/relay-registration/1\0' + bytes.fromhex(dataset[2:])
            + bytes.fromhex(actor['principal_id'][2:]) + key + (1).to_bytes(8,'big') * 2
            + b'\0\1' + len(raw).to_bytes(4,'big') + raw)
        assert command['request_digest'] == '\\x' + hashlib.sha256(digest_input).hexdigest()
        assert original['normalization_version'] == original['config_revision'] == original['policy_revision'] == 1
        assert original['registered_at_us'] <= command['accepted_at_us']
