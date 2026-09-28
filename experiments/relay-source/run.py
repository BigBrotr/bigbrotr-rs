#!/usr/bin/env python3
"""Actual enrollment + original signed request with operational source bindings.

This wave checks populated upgrade, retry identity and the two admission phases.
It does not qualify socket attribution, revocation drain or ambiguous recovery.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys
import tomllib

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
spec = importlib.util.spec_from_file_location('bootstrap_oracle', SOURCE / 'experiments/ingest-bootstrap/oracle.py')
bootstrap_oracle = importlib.util.module_from_spec(spec)
sys.modules['bootstrap_oracle'] = bootstrap_oracle
spec.loader.exec_module(bootstrap_oracle)
from oracle import expected_source

assert len(sys.argv) in (3, 4)
root, build = map(Path, sys.argv[1:3])
subnet = sys.argv[3] if len(sys.argv) == 4 else '172.16.240.64/28'
assert root.is_absolute() and not root.exists() and not root.is_symlink()
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
receipt = json.loads((build / 'build-result.json').read_text())
if receipt['source'] != revision:
    changed = lab.command(['git','-C',str(SOURCE),'diff','--name-only',receipt['source'],revision]).stdout.splitlines()
    # A fixture/documentation correction can reuse the exact executable; no
    # product, migration, lockfile, toolchain or other build input may differ.
    assert changed and all(n.startswith(('docs/','experiments/')) for n in changed), changed
sha = lambda data: hashlib.sha256(data).hexdigest()
assert sha((build / 'ingest_bootstrap_probe').read_bytes()) == receipt['probe_sha256']
report = dict(format='bigbrotr-relay-source-native/1', source=revision, build=receipt,
    application_source=receipt['source'],
    status='RUNNING', cases=[], not_run=['source grant and actual socket attribution',
    'private source visibility policy and bans', 'restrictive drain', 'concurrent lock schedules',
    'lost COMMIT and positive recovery after revocation', 'relay/import double binding',
    'load and aged state', 'arm64', 'release'])


def save(): lab.write_json(root / 'source-result.json', report)


def record(name, condition, **details):
    report['cases'].append(dict(name=name, status='PASS' if condition else 'FAIL', **details))
    save()
    if not condition: raise RuntimeError(name)


def rows(database):
    names = json.loads(lab.sql(state, database,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    assert len(names) == 64 and all(re.fullmatch('[a-z_]+', n) for n in names)
    queries = [f"SELECT '{n}' AS name, coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
               f"FROM (SELECT *,xmin::text AS lab_xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(state, database, 'SELECT jsonb_object_agg(name,rows) FROM (' +
        ' UNION ALL '.join(queries) + ') all_tables'))


def prepare(database):
    lab.sql(state, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator')
    config = lab.config(root, state, database)
    local = root / Path(config).name
    local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/lab/secrets/application'))
    migrated = lab.migrate(state, 'apply', config, target=19)
    record(database + '-initial-schema', migrated.returncode == 0, stdout=migrated.stdout, stderr=migrated.stderr)
    lab.sql(state, database, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; '
        'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; '
        'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control; '
        'GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control;')
    for args in [('config','initialize'), ('config','activate-initial','--request-id','77'*16,'--expected-revision','1')]:
        result = lab.cli(state, *args, '--file', config)
        record(database + '-' + args[1], result.returncode == 0, stdout=result.stdout, stderr=result.stderr)
    return config, local


def upgrade(database, config):
    def physical_tables():
        return json.loads(lab.sql(state, database, """
            SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid))
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='control' AND c.relkind='r'
        """))

    before = rows(database)
    physical_before = physical_tables()
    lab.write_json(root / (database + '-upgrade-before.json'), before)
    result = lab.migrate(state, 'apply', config, target=20)
    after = rows(database)
    physical_after = physical_tables()
    lab.write_json(root / (database + '-upgrade-after.json'), after)
    lab.write_json(root / (database + '-upgrade-physical.json'),
        dict(before=physical_before,after=physical_after))
    record(database + '-domain-column-rewrite', set(physical_before) == set(physical_after) and
        {n for n in physical_before if physical_before[n] != physical_after[n]} == {'producer_requests'})
    stripped = json.loads(json.dumps(after))
    for row in stripped['producer_requests']:
        for field in ['relay_endpoint_id','relay_access_scope_id','relay_enrollment_generation','relay_route_id']:
            assert row.pop(field) is None
    # PostgreSQL16 ATExecAddColumn rewrites constrained-domain columns even
    # with NULL defaults. Only this table's physical xmin may change; every
    # original application field and every other relation must be identical.
    original_requests = [{k:v for k,v in r.items() if k != 'lab_xmin'} for r in before['producer_requests']]
    retained_requests = [{k:v for k,v in r.items() if k != 'lab_xmin'} for r in stripped['producer_requests']]
    new_versions = [r for r in after['schema_versions'] if r not in before['schema_versions']]
    record(database + '-additive-upgrade', result.returncode == 0 and
        all(stripped[n] == before[n] for n in before if n not in ('schema_versions','producer_requests')) and
        sorted(original_requests,key=str) == sorted(retained_requests,key=str) and
        all(r in after['schema_versions'] for r in before['schema_versions']) and
        len(new_versions) == 1 and new_versions[0]['migration'] == 20 and
        new_versions[0]['checksum'] == '\\x357c7eff977e4cdedda73fb1b2b803fbd3ad0703b46f334f1c01b6484cca0ad2' and
        all(r['lab_xmin'] == new_versions[0]['lab_xmin'] for r in after['producer_requests']),
        stdout=result.stdout, stderr=result.stderr)
    result = lab.migrate(state, 'apply', config, target=20)
    record(database + '-reapply-unchanged', result.returncode == 0 and rows(database) == after and
        physical_tables() == physical_after)
    lab.sql(state, database, 'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control')


try:
    lab.setup(root, build, SOURCE, 'source_' + revision[:7], subnet=subnet)
    state = lab.load(root); report['server'] = state['server']; save()
    config, _ = prepare('bb_source_upgrade')
    result = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe', 'write-v2',
        'bb_source_upgrade', '/lab/secrets/application', '/lab/upgrade-store', check=False)
    record('legacy-unbound-real-bootstrap', result.returncode == 0 and '"replay_exact":true' in result.stdout,
        stdout=result.stdout, stderr=result.stderr)
    upgrade('bb_source_upgrade', config)
    config, local = prepare('bb_source_template')
    upgrade('bb_source_template', config)
    dataset = bytes.fromhex(tomllib.loads(local.read_text())['topology']['dataset_id'])
    lab.sql(state, 'bb_source_template', """
        INSERT INTO control.principals(principal_id,dataset_id,kind,created_at_us,database_role,local_os_uid)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'local',0,'fixture',0 FROM control.datasets;
        INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'alice',true,true,1,1 FROM control.datasets;
        INSERT INTO control.local_credentials(principal_id,dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash)
            SELECT principal_id,dataset_id,1,'argon2id-v1',19,65536,3,1,decode(repeat('02',16),'hex'),
                decode('a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff','hex') FROM control.local_accounts;
        INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'policy.change','dataset',dataset_id FROM control.local_accounts;
    """)
    modes = ['source-pass','source-omitted','source-changed','source-legacy-adopt','source-visibility',
             'source-withdrawn','source-stale-generation','source-dispatch-refused']
    for mode in modes:
        database = 'bb_' + mode.replace('-', '_')
        lab.sql(state, 'postgres', f'CREATE DATABASE {database} TEMPLATE bb_source_template OWNER bigbrotr_migrator')
        if mode == 'source-dispatch-refused':
            # Independent fault point keyed to durable reservation existence,
            # not a count of production calls. The real pre-reservation checks
            # still run; only the later dispatch observation becomes unavailable.
            lab.sql(state, database, """
                ALTER FUNCTION control.require_relay_source(bytea,bytea,bytea,numeric,bytea)
                    RENAME TO require_relay_source_original;
                CREATE FUNCTION control.require_relay_source(d bytea,e bytea,a bytea,g numeric,r bytea)
                RETURNS void LANGUAGE plpgsql AS $$ BEGIN
                    IF EXISTS(SELECT 1 FROM control.producer_requests WHERE relay_endpoint_id=e) THEN
                        RAISE EXCEPTION 'relay source enrollment is not current and enabled' USING ERRCODE='23514';
                    END IF;
                    PERFORM control.require_relay_source_original(d,e,a,g,r);
                END $$;
                REVOKE ALL ON FUNCTION control.require_relay_source(bytea,bytea,bytea,numeric,bytea) FROM PUBLIC;
                GRANT EXECUTE ON FUNCTION control.require_relay_source(bytea,bytea,bytea,numeric,bytea) TO bigbrotr_control;
            """)
        before = rows(database)
        result = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe', mode,
            database, '/lab/secrets/application', '/lab/' + mode, check=False, timeout=60)
        (root / (mode + '.stdout')).write_text(result.stdout)
        (root / (mode + '.stderr')).write_text(result.stderr)
        messages = [json.loads(line) for line in result.stdout.splitlines()]
        record(mode + '-actual-adapter', result.returncode == 0 and not result.stderr and
            messages[-1]['event'] == 'source-after' and messages[-1]['status'] == 'PASS',
            exit_code=result.returncode, stderr=result.stderr)
        phase_before = next(m['state'] for m in messages if m['event'] == 'source-before')
        phase_after = messages[-1]['state']
        phase_tables = [{name:json.loads(value) for name,value in phase['control_tables']}
                        for phase in [phase_before,phase_after]]
        if mode == 'source-dispatch-refused':
            old, new = phase_tables
            assert {n for n in old if old[n] != new[n]} == {'producer_requests','producer_epochs'}
            assert len(old['producer_requests']) == 1 and len(new['producer_requests']) == 2
            assert old['producer_requests'][0] in new['producer_requests']
            assert len(old['producer_epochs']) == len(new['producer_epochs']) == 1
            a,b = [dict(v['producer_epochs'][0]) for v in phase_tables]
            assert a.pop('reserved_through_request') == 1 and b.pop('reserved_through_request') == 2
            a.pop('xmin'); b.pop('xmin'); assert a == b
            assert all(v in phase_after['originals'] for v in phase_before['originals'])
        else:
            assert phase_before == phase_after
        record(mode + '-complete-phase-state', True)
        after = rows(database)
        lab.write_json(root / (mode + '-before.json'), before)
        lab.write_json(root / (mode + '-after.json'), after)
        committed = mode in modes[:4]
        expected, inputs = expected_source(dataset, committed)
        dumped = lab.docker('exec', state['client'], '/build/ingest_bootstrap_probe', 'dump', '/lab/' + mode)
        (root / (mode + '.rocks.json')).write_text(dumped.stdout)
        actual = [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
        record(mode + '-independent-complete-native-facts',
            [r for r in actual if r[0] != 'receipts'] == [r for r in expected if r[0] != 'receipts'])
        if mode == 'source-dispatch-refused':
            _, inputs = expected_source(dataset, True)
        originals = sorted((root / mode / 'producer-inputs-v1').glob('*.cbor'))
        record(mode + '-independent-original-inputs', len(originals) == len(inputs) and
            all(p.read_bytes() == inputs[int(p.stem[-16:],16)] for p in originals))
        reservations = after['producer_requests']
        record(mode + '-exact-reservation-count', len(reservations) == len(inputs) and
            len(after['producer_outcomes']) == (2 if committed else 1))
        for row in reservations:
            sequence = int(row['request_sequence'])
            assert row['request_digest'] == '\\x' + sha(inputs[sequence]) and row['input_bytes'] == len(inputs[sequence])
            if sequence == 1 or mode == 'source-legacy-adopt':
                assert all(row[f] is None for f in ['relay_endpoint_id','relay_access_scope_id','relay_enrollment_generation','relay_route_id'])
            else:
                change = next(c for c in after['relay_enrollment_changes'] if c['request_key'] == '\\x' + '01'*16)
                assert [row[f] for f in ['relay_endpoint_id','relay_access_scope_id','relay_enrollment_generation','relay_route_id']] == [
                    change['endpoint_id'],change['access_scope_id'],1,change['route_id']]
        record(mode + '-independent-source-metadata', True)
        active = lab.sql(state, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='bigbrotr-ingest-bootstrap-probe'")
        record(mode + '-drivers-joined', active == '0')
    report['status'] = 'PASS_SCOPED_RELAY_SOURCE_FIRST_WAVE'
except BaseException as error:
    report.update(status='FAIL', failure=str(error)); raise
finally:
    if root.exists():
        save()
        if (root / 'resources.json').exists():
            state = json.loads((root / 'resources.json').read_text())
            for name in ('client','postgres'):
                result = lab.docker('inspect', state[name], check=False)
                if result.returncode: continue
                item = json.loads(result.stdout)[0]
                assert item['Config']['Labels'][lab.LABEL] == state['owner']
                if item['State']['Running']: lab.docker('stop','--time','10',item['Id'])
                logs = lab.docker('logs',item['Id'])
                (root / (name + '.stdout')).write_text(logs.stdout)
                (root / (name + '.stderr')).write_text(logs.stderr)
            state['state'] = 'stopped-retained'; lab.write_json(root / 'resources.json',state)
print(json.dumps(dict(status=report['status'],cases=len(report['cases']))))
