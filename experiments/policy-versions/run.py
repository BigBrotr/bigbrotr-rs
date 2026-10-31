#!/usr/bin/env python3
"""Actual current policy initialization, replay and superseded-format refusals."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tomllib

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
sys.path.insert(0, str(SOURCE / 'experiments/policy-versions'))
import lab
from oracle import expected
from schema_inventory import declared_inventory, require_tables

root, build = map(Path, sys.argv[1:])
# Refuse before entering cleanup: an existing root belongs to an earlier run.
assert root.is_absolute() and not root.exists() and not root.is_symlink()
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
receipt = json.loads((build / 'build-result.json').read_text())
assert receipt['source'] == revision


def sha(data):
    return hashlib.sha256(data).hexdigest()


def compact(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()


def fingerprint(document, version):
    return sha(f'bigbrotr/product-policy/{version}\0'.encode() + compact(document))


assert sha((build / 'ingest_bootstrap_probe').read_bytes()) == receipt['probe_sha256'], \
    'current ingest probe differs from its exact-source build receipt'
current = json.loads((SOURCE / 'tests/fixtures/configuration/policy-v3.json').read_text())
assert fingerprint(current, 3) == '77324b5f6aea113a4e4b81e11d946c0c4e113b051851d398c240de49a7a6322b'
inventory = declared_inventory(SOURCE)
report = dict(format='bigbrotr-policy-versions-native/1', source=revision,
              build=receipt, schema=inventory, status='RUNNING', cases=[],
              not_run=['populated policy upgrade', 'source issuance and restrictive drain',
                       'COMMIT transport faults', 'load and aged state', 'arm64', 'release'])


def save():
    lab.write_json(root / 'policy-versions-result.json', report)


def record(name, passed, **details):
    report['cases'].append(dict(name=name, status='PASS' if passed else 'FAIL', **details))
    save()
    if not passed:
        raise RuntimeError(name)


def contents(database):
    names = json.loads(lab.sql(state, database,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    require_tables(names, inventory)
    branches = [f"SELECT '{name}' AS name, coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text), '[]') AS rows "
                f"FROM (SELECT *, xmin::text AS lab_xmin FROM control.{name}) t" for name in names]
    return json.loads(lab.sql(state, database,
        'SELECT jsonb_object_agg(name, rows) FROM (' + ' UNION ALL '.join(branches) + ') all_tables'))


def call(name, database, action, path, code=0, *, unchanged=False, extra=(), reason=None):
    before = contents(database)
    result = lab.docker('exec', state['client'], '/build/bigbrotr',
                       'config', action, '--file', path, *extra, check=False)
    after = contents(database)
    value = json.loads(result.stdout) if result.stdout else None
    lab.write_json(root / (name + '-before.json'), before)
    lab.write_json(root / (name + '-after.json'), after)
    record(name, result.returncode == code and (code != 0 or not result.stderr)
           and (code == 0 or value is None) and (reason is None or reason in result.stderr)
           and (not unchanged or before == after), exit_code=result.returncode,
           output=value, stderr=result.stderr, before_sha256=sha(compact(before)),
           after_sha256=sha(compact(after)), unchanged_required=unchanged, expected_reason=reason)
    return value, after


def prepare(name, mode):
    database = 'bb_policy_' + name
    lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator')
    path = lab.config(root, state, database)
    local = root / Path(path).name
    text = lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application')
    text = lab.change(text, 'config', 'authority_mode', mode)
    local.write_text(text)
    migration = lab.migrate(state, 'apply', path, target=inventory['target'])
    record(name + '-migrations', migration.returncode == 0, stdout=migration.stdout, stderr=migration.stderr)
    lab.sql(state, database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; '
            'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; '
            'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
    verify_application_role(name, database)
    return database, path, local


def verify_application_role(name, database):
    # This is the existing broad lab role, not the separate deployment-grant
    # proposal. Preserve its non-owner fence and record the actual grant surface.
    role = json.loads(lab.sql(state, database, """
        SELECT jsonb_build_object(
            'role',r.rolname,'login',r.rolcanlogin,'superuser',r.rolsuper,
            'create_database',r.rolcreatedb,'create_role',r.rolcreaterole,
            'replication',r.rolreplication,'bypass_rls',r.rolbypassrls,
            'memberships',(SELECT count(*) FROM pg_auth_members WHERE member=r.oid),
            'database_owner',(SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()),
            'schema_owner',pg_get_userbyid(n.nspowner),
            'schema_usage',has_schema_privilege(r.oid,n.oid,'USAGE'),
            'schema_create',has_schema_privilege(r.oid,n.oid,'CREATE'),
            'tables',(SELECT jsonb_object_agg(c.relname,jsonb_build_object(
                'owner',pg_get_userbyid(c.relowner),
                'select',has_table_privilege(r.oid,c.oid,'SELECT'),
                'insert',has_table_privilege(r.oid,c.oid,'INSERT'),
                'update',has_table_privilege(r.oid,c.oid,'UPDATE'),
                'delete',has_table_privilege(r.oid,c.oid,'DELETE'),
                'truncate',has_table_privilege(r.oid,c.oid,'TRUNCATE'),
                'references',has_table_privilege(r.oid,c.oid,'REFERENCES'),
                'trigger',has_table_privilege(r.oid,c.oid,'TRIGGER')))
                FROM pg_class c WHERE c.relnamespace=n.oid AND c.relkind IN ('r','p')),
            'functions',(SELECT jsonb_object_agg(p.oid::regprocedure::text,jsonb_build_object(
                'owner',pg_get_userbyid(p.proowner),'security_definer',p.prosecdef,
                'execute',has_function_privilege(r.oid,p.oid,'EXECUTE')))
                FROM pg_proc p WHERE p.pronamespace=n.oid))
        FROM pg_roles r CROSS JOIN pg_namespace n
        WHERE r.rolname='bigbrotr_control' AND n.nspname='control'
    """))
    require_tables(sorted(role['tables']), inventory)
    wanted = dict(owner='bigbrotr_migrator', select=True, insert=True, update=True,
                  delete=False, truncate=False, references=False, trigger=False)
    record(name + '-application-role',
           role['role'] == 'bigbrotr_control' and role['login']
           and not any(role[key] for key in ['superuser','create_database','create_role',
                                            'replication','bypass_rls','schema_create','memberships'])
           and role['database_owner'] == role['schema_owner'] == 'bigbrotr_migrator'
           and role['schema_usage'] and all(value == wanted for value in role['tables'].values())
           and bool(role['functions']) and all(value['owner'] == 'bigbrotr_migrator' and value['execute']
                                              for value in role['functions'].values()), profile=role)


def bans(text, rules):
    assert text.count('[relay_policy]\n') == 1
    return text.replace('[relay_policy]\n', f'[relay_policy]\nbanned={rules}\n')


def immutable_rows(rows):
    return {n: rows[n] for n in ['config_revisions', 'policy_revisions', 'management_commands',
                                'management_audit', 'principals', 'schema_versions']}


try:
    lab.setup(root, build, SOURCE, 'policy_' + revision[:7], subnet='172.16.240.48/28')
    state = lab.load(root)
    report['server'] = state['server']
    save()
    prepared = {}
    activation = ('--request-id', '04' * 16, '--expected-revision', '1')
    for mode in ['managed', 'declarative']:
        name = 'v3_' + mode
        database, path, local = prepare(name, mode)
        original = local.read_text()
        digest = fingerprint(current, 3)
        initialized, rows = call(name + '-initialize', database, 'initialize', path)
        config = rows['config_revisions'][0]
        record(name + '-independent-policy', config['document'] == current and config['schema_version'] == 3
               and config['document_digest'] == '\\x' + digest and initialized['initial_policy_sha256'] == digest)
        historical = copy.deepcopy(rows['config_revisions'])
        retry, _ = call(name + '-initialize-retry', database, 'initialize', path, unchanged=True)
        record(name + '-original-receipt', retry['status'] == 'already-initialized'
               and retry['installation_actor'] == initialized['installation_actor']
               and retry['audit_id'] == initialized['audit_id'] and not retry['candidate_policy_differs'])
        observed, _ = call(name + '-inspect', database, 'inspect', path, unchanged=True)
        record(name + '-accepted-retention', observed['accepted_policy_format'] == current['format']
               and observed['settings']['ingest.request_receipt_retention']['value'] == '64'
               and not observed['candidate_policy_differs'])
        local.write_text(original + '\n[ingest]\nrequest_receipt_retention=62\n')
        staged, _ = call(name + '-staged-retention', database, 'inspect', path, unchanged=True)
        record(name + '-captured-value', staged['candidate_policy_differs']
               and staged['settings']['ingest.request_receipt_retention']['value'] == '64')
        if mode == 'declarative':
            call(name + '-refuse-staged-activation', database, 'activate-initial', path, 3,
                 unchanged=True, extra=activation, reason='PolicyInvalid')
        local.write_text(original)
        accepted, rows = call(name + '-activate', database, 'activate-initial', path, extra=activation)
        record(name + '-activation-identity', accepted['policy_sha256'] == digest
               and rows['config_revisions'] == historical and len(rows['policy_revisions']) == 1
               and rows['policy_revisions'][0]['document'] == current
               and rows['policy_revisions'][0]['schema_version'] == 3)
        replayed, _ = call(name + '-activation-replay', database, 'activate-initial', path, unchanged=True, extra=activation)
        record(name + '-identical-activation-receipt', replayed == dict(accepted, status='already-activated'))
        prepared[mode] = database, path, local

    # Separate invalid clones preserve the healthy current-format original.
    # No old executable, private binary/cache or legacy-format write path is used.
    for version in [1, 2, 4]:
        origin, _, _ = prepared['managed']
        database = f'bb_policy_incompatible_{version}'
        lab.sql(state, 'postgres', f'CREATE DATABASE {database} TEMPLATE {origin} OWNER bigbrotr_migrator')
        invalid = copy.deepcopy(current)
        invalid['format'] = f'bigbrotr-product-policy/{version}'
        document = compact(invalid).decode().replace("'", "''")
        for table in ['config_revisions', 'policy_revisions']:
            lab.sql(state, database, f"ALTER TABLE control.{table} DISABLE TRIGGER USER; "
                    f"UPDATE control.{table} SET schema_version={version},document='{document}'::jsonb,"
                    f"document_digest=decode('{fingerprint(invalid,version)}','hex'); "
                    f"ALTER TABLE control.{table} ENABLE TRIGGER USER")
        before = contents(database)
        store = '/lab/store_incompatible_' + str(version)
        result = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe',
            'write', database, '/lab/secrets/application', store, check=False)
        after = contents(database)
        record(database + '-owner-refuses', result.returncode != 0 and 'InvalidPolicy' in result.stderr
               and before == after, stdout=result.stdout, stderr=result.stderr,
               before_sha256=sha(compact(before)), after_sha256=sha(compact(after)))
        path = lab.config(root, state, database)
        local = root / Path(path).name
        local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application'))
        call(database + '-inspect-refuses', database, 'inspect', path, 1, unchanged=True)
        call(database + '-activation-refuses', database, 'activate-initial', path, 3,
             unchanged=True, extra=activation, reason='InvalidMetadata')

    database, path, local = prepare('v3_rules', 'managed')
    local.write_text(bans(local.read_text(), "[{host='BÜCHER.example.'},{endpoint='WSS://RELAY.example.:443/x/../PRIVATE_CANARY?a=1&a=2'},{host='[::ffff:127.0.0.1]'},{host='127.0.0.1'}]"))
    wanted_rules = copy.deepcopy(current)
    wanted_rules['settings']['relay_policy.banned'] = [
        {'endpoint': 'wss://relay.example/PRIVATE_CANARY?a=1&a=2'},
        {'host': '127.0.0.1'}, {'host': 'xn--bcher-kva.example'}]
    _, rows = call('v3-rules-initialize', database, 'initialize', path)
    record('v3-rules-independent-jsonb', rows['config_revisions'][0]['document'] == wanted_rules
           and rows['config_revisions'][0]['document_digest'] == '\\x' + fingerprint(wanted_rules, 3))
    observed, _ = call('v3-rules-inspect', database, 'inspect', path, unchanged=True)
    record('v3-rules-redaction', observed['settings']['relay_policy.banned']['value'] == '[redacted]'
           and 'PRIVATE_CANARY' not in json.dumps(observed))
    call('v3-rules-activate', database, 'activate-initial', path, extra=activation)

    # Initialization retry must validate the retained body too. A recomputed
    # digest cannot repair a missing setting; a stale digest cannot certify an
    # altered complete body. Inspect/activation exercise their own boundaries.
    for name in ['missing-bans', 'missing-retention', 'non-normalized-bans',
                 'missing-retention-stale-digest', 'changed-body-stale-digest']:
        bad = copy.deepcopy(current)
        if name == 'missing-bans':
            del bad['settings']['relay_policy.banned']
        elif name in ('missing-retention', 'missing-retention-stale-digest'):
            del bad['settings']['ingest.request_receipt_retention']
        elif name == 'changed-body-stale-digest':
            bad['settings']['ingest.request_receipt_retention'] = 62
        else:
            bad['settings']['relay_policy.banned'] = [{'host': 'RELAY.example'}]
        database = 'bb_policy_' + name.replace('-', '_')
        origin, _, _ = prepared['managed']
        lab.sql(state, 'postgres', f'CREATE DATABASE {database} TEMPLATE {origin} OWNER bigbrotr_migrator')
        document = compact(bad).decode().replace("'", "''")
        retained_digest = fingerprint(current if name.endswith('stale-digest') else bad, 3)
        lab.sql(state, database, "ALTER TABLE control.config_revisions DISABLE TRIGGER USER; "
                f"UPDATE control.config_revisions SET document='{document}'::jsonb, "
                f"document_digest=decode('{retained_digest}','hex'); "
                "ALTER TABLE control.config_revisions ENABLE TRIGGER USER")
        path = lab.config(root, state, database)
        local = root / Path(path).name
        local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application'))
        call(name + '-initialize-refuses', database, 'initialize', path, 1, unchanged=True)
        call(name + '-inspect-refuses', database, 'inspect', path, 3, unchanged=True)
        call(name + '-activate-refuses', database, 'activate-initial', path, 3,
             unchanged=True, extra=activation, reason='PolicyInvalid')

    for version in [3]:
        database, path, local = prepared['managed']
        before = contents(database)
        store = '/lab/store_v' + str(version)
        result = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe',
            'write', database, '/lab/secrets/application', store, check=False)
        messages = [json.loads(line) for line in result.stdout.splitlines()]
        record(f'v{version}-actual-control-ingest', result.returncode == 0 and not result.stderr
               and messages[-1]['event'] == 'receipt' and messages[-1]['replay_exact'],
               stdout=messages, stderr=result.stderr)
        after = contents(database)
        record(f'v{version}-historical-rows-unchanged', immutable_rows(before) == immutable_rows(after))
        lab.write_json(root / f'v{version}-ingest-before.json', before)
        lab.write_json(root / f'v{version}-ingest-after.json', after)
        dumped = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe', 'dump', store)
        (root / f'v{version}-rocks.json').write_text(dumped.stdout)
        actual = [(r['cf'], bytes.fromhex(r['key']), bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
        dataset = bytes.fromhex(tomllib.loads(local.read_text())['topology']['dataset_id'])
        wanted, _, _ = expected(dataset, True)
        record(f'v{version}-independent-durable-facts',
               [r for r in actual if r[0] != 'receipts'] == [r for r in wanted if r[0] != 'receipts'])
        call(f'v{version}-post-native-inspect', database, 'inspect', path, unchanged=True)
    active = lab.sql(state, 'postgres', "SELECT count(*) FROM pg_stat_activity WHERE usename='bigbrotr_control'")
    record('all-application-drivers-joined', active == '0')
    report['status'] = 'PASS_SCOPED_POLICY_VERSIONS'
except BaseException as error:
    report.update(status='FAIL', failure=str(error))
    raise
finally:
    if root.exists():
        save()
        if (root / 'resources.json').exists():
            state = json.loads((root / 'resources.json').read_text())
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
