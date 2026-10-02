"""Actual initial-owner HTTP enrollment; isolated endpoint setup is explicit."""
import hashlib
import json
from pathlib import Path
import shutil
import lab
from installed import messages, native_oracle, policy_setup, set_value, start, stop, terminal, wait_for
from sessions import configuration


def run(pg, client, output, database, name, rows):
    functions = {
        'enrollment-function-route': 'control.relay_route_valid(bytea)',
        'enrollment-function-digest': 'control.relay_enrollment_digest(control.relay_enrollment_changes,control.management_commands)',
        'enrollment-function-u64': 'control.command_u64_bytes(control.u64)',
    }
    assert name in ('enrollment-journey', 'enrollment-permission', 'enrollment-prerequisite') or name in functions
    prerequisite = name == 'enrollment-prerequisite' or name in functions
    for filename in ('installed_client.py', 'session_client.py'):
        if not (output / filename).exists():
            shutil.copy2(Path(__file__).with_name(filename), output / filename)
    # Endpoint preparation uses the actual internal registry, before the installed
    # owner starts. It is not evidence for installed authenticated URL registration.
    prepared = lab.docker('exec', client, '/build/ingest_bootstrap_probe',
        'installed-relay-setup', database, '/run/secrets/application', '/lab/unused',
        check=False, timeout=15)
    (output / (name + '-setup.stdout')).write_text(prepared.stdout)
    (output / (name + '-setup.stderr')).write_text(prepared.stderr)
    assert prepared.returncode == 0 and not prepared.stderr
    endpoint = json.loads(prepared.stdout)
    assert endpoint['event'] == 'installed-relay-fixture'
    setup = rows(database)
    assert len(setup['relay_endpoints']) == 1 and len(setup['owner_epochs']) == 1
    assert setup['owner_epochs'][0]['state'] == 'released'
    assert setup['relay_endpoints'][0]['endpoint_id'] == '\\x' + endpoint['endpoint_id']
    assert setup['relay_endpoints'][0]['url_bytes'] == '\\x' + endpoint['url'].encode().hex()
    lab.write_json(output / (name + '-endpoint-setup.json'), setup)
    policy_setup(pg, database, name, rows, output, administration=True)
    # The root seed and first accepted document permit public routing; only the
    # second accepted document refuses it. This discriminates their intersection.
    policy = rows(database)['policy_revisions'][0]['document']
    policy['settings']['network.public.enabled'] = False
    encoded = json.dumps(policy, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
    digest = hashlib.sha256(b'bigbrotr/product-policy/2\0' + encoded).hexdigest()
    lab.sql(pg, database, f"""BEGIN; SET LOCAL session_replication_role=replica;
        UPDATE control.policy_revisions SET document=convert_from(decode('{encoded.hex()}','hex'),'UTF8')::jsonb,
            document_digest=decode('{digest}','hex'); COMMIT;""")
    if name == 'enrollment-permission':
        lab.sql(pg, database, "UPDATE control.principal_grants SET action='principals.manage'")
    if name == 'enrollment-prerequisite':
        lab.sql(pg, database, 'REVOKE INSERT ON control.relay_enrollment_changes FROM bigbrotr_control')
    if name in functions:
        signature = functions[name]
        lab.sql(pg, database, f'REVOKE EXECUTE ON FUNCTION {signature} FROM bigbrotr_control')
        assert lab.sql(pg, database, f"SELECT has_function_privilege('bigbrotr_control','{signature}','EXECUTE')") == 'f'
        lab.write_json(output / (name + '-revoked.json'), dict(function=signature, app_execute=False))
    before = rows(database)
    lab.write_json(output / (name + '-before.json'), before)
    state = dict(pg, client=client)
    config = lab.config(output, state, database, endpoint='127.0.0.1:5432', boundary='loopback')
    path = output / Path(config).name
    text = configuration(path.read_text(), before, name)
    text = set_value(text, 'network.private', 'rules', [
        dict(endpoint=endpoint['url'], cidr='127.0.0.1/32', port=7447)])
    # TOML inline tables differ from JSON objects; render this bounded literal
    # through the existing configuration document, with no dynamic secret data.
    text = text.replace(json.dumps([dict(endpoint=endpoint['url'], cidr='127.0.0.1/32', port=7447)]),
        '[{endpoint="ws://127.0.0.1:7447/installed-fixture",cidr="127.0.0.1/32",port=7447}]')
    path.write_text(text)
    validated = lab.cli(state, 'config', 'validate', '--file', config)
    (output / (name + '-validate.stdout')).write_text(validated.stdout)
    (output / (name + '-validate.stderr')).write_text(validated.stderr)
    assert validated.returncode == 0
    directory = output / name
    directory.mkdir(mode=0o700)
    prefix = start(client, config, output, name)
    if prerequisite:
        wait_for(lambda: messages(prefix) or Path(str(prefix)+'.terminal.json').exists(), 'prerequisite outcome')
        if messages(prefix):
            # Preserve the counterexample and stop the exact CLI normally. The
            # missing privilege is known; binding itself violates the contract.
            stop(client, prefix)
            terminal(prefix)
            lab.write_json(output / (name + '-unexpected-after.json'), rows(database))
            raise RuntimeError('missing enrollment prerequisite still bound the listener: ' + name)
        ended = terminal(prefix)
        assert ended['exit_code'] == 4 and not messages(prefix)
        assert 'initial relay enrollment prerequisites unavailable: Prerequisite' in Path(str(prefix)+'.stderr').read_text()
        lab.docker('exec', client, 'python3', '-c',
            'import socket; s=socket.socket(); assert s.connect_ex(("127.0.0.1",18081))!=0; s.close()')
        initialized = None
    else:
        first = wait_for(lambda: messages(prefix) or Path(str(prefix)+'.terminal.json').exists(), 'initial owner')
        assert isinstance(first, list) and len(first) == 1 and first[0]['state'] == 'initialized', first
        initialized = first[0]
        assert initialized['listeners'][0]['profile'] == 'initial-source-commands'
        assert not initialized['ready'] and initialized['acquisition'] == 'not-started'
        initial = rows(database)
        lab.write_json(output / (name + '-initialized.json'), initial)
        visibility = dict(initialized['public_scope'], ordinal='2')
        command = dict(endpoint_id=endpoint['endpoint_id'], visibility=visibility,
            route=dict(**{'class': 'private'}, cidr='127.0.0.1/32'), enabled=True,
            expected_generation=None, expected_config_revision='1', expected_policy_revision='1')
        lab.write_json(directory / 'command-input.json', dict(command=command, permitted=name=='enrollment-journey'))
        # Keep stdout/stderr and the exact finite client outcome, even on failure.
        wrapper = '''import subprocess,sys,json
from pathlib import Path
p=Path(sys.argv[1])
with (p/'client.stdout').open('xb') as out,(p/'client.stderr').open('xb') as err:
 r=subprocess.run(['python3','/lab/session_client.py',str(p),'enrollment'],stdout=out,stderr=err,timeout=20)
(p/'client-exit.json').write_text(json.dumps({'exit_code':r.returncode}))
'''
        lab.docker('exec', '-d', client, 'python3', '-c', wrapper, '/lab/' + name)
        if name == 'enrollment-journey':
            snapshots = []
            for stage in ('before-replay', 'after-replay'):
                wait_for(lambda: (directory/(stage+'.json')).exists() or (directory/'client-exit.json').exists(), stage, 10)
                assert (directory/(stage+'.json')).exists(), (directory/'client.stderr').read_text()
                observed = rows(database)
                lab.write_json(output / (name + '-' + stage + '-sql.json'), observed)
                snapshots.append(observed)
                (directory/(stage+'.continue')).write_text('observed\n')
            # Authority heartbeats alone may update deadlines/xmin. Every user
            # command, enrollment, audit, session and other retained row is exact.
            assert all(snapshots[0][t] == snapshots[1][t] for t in snapshots[0] if t != 'owner_epochs')
        wait_for((directory/'client-exit.json').exists, 'HTTP client terminal', 20)
        assert json.loads((directory/'client-exit.json').read_text())['exit_code'] == 0, (directory/'client.stderr').read_text()
        stop(client, prefix)
        ended = terminal(prefix)
        assert ended['exit_code'] == 0 and not Path(str(prefix)+'.stderr').read_text()
        assert messages(prefix)[-1]['owner_release'] == 'committed'
    after = rows(database)
    lab.write_json(output / (name + '-after.json'), after)
    assert len(after['owner_epochs']) == len(after['role_instances']) == 3
    assert all(r in after['owner_epochs'] for r in before['owner_epochs'])
    assert after['relay_endpoints'] == before['relay_endpoints']
    assert after['principal_grants'] == before['principal_grants']
    assert after['config_revisions'] == before['config_revisions'] and after['policy_revisions'] == before['policy_revisions']
    if initialized:
        native_oracle(pg, client, output, database, name, after, initialized)
        assert len(after['browser_sessions']) == 1 and after['browser_sessions'][0]['revoked_at_us'] is not None
    else:
        from commits import native_oracle as retained_oracle
        retained_oracle(client, output, name, after, number=6, healthy=True, last_commit=6)
        assert not after['browser_sessions']
    audits = [r for r in after['management_audit'] if r not in before['management_audit']]
    expected = [] if not initialized else ['browser.session.issue', 'browser.session.revoke']
    if name == 'enrollment-journey':
        expected += ['relay.enrollment.change'] * 2
        assert len(after['relay_enrollments']) == len(after['network_routes']) == len(after['relay_access_contexts']) == 1
        assert not after['relay_enrollments'][0]['enabled'] and after['relay_enrollments'][0]['generation'] == 2
        assert len(after['relay_enrollment_changes']) == 2
        assert len(after['management_commands']) == len(before['management_commands']) + 2
    else:
        for table in ('relay_enrollments', 'network_routes', 'relay_access_contexts', 'relay_enrollment_changes', 'management_commands'):
            assert after[table] == before[table]
    assert sorted(r['action'] for r in audits) == sorted(expected)
    assert all(r in after['management_audit'] for r in before['management_audit'])
    allowed = {'datasets','owner_epochs','role_instances','topology_generations','topology_streams',
        'source_streams','allocation_domains','producer_slots','producer_epochs','producer_requests',
        'producer_outcomes','browser_sessions','login_buckets','management_audit','management_commands',
        'relay_enrollments','network_routes','relay_access_contexts','relay_enrollment_changes'}
    assert {t for t in before if before[t] != after[t]} <= allowed
    assert lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0'
    return dict(name=name, status='PASS', actual_cli=True, actual_http=True, control_tables=len(after), terminal=ended)
