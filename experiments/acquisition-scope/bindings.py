#!/usr/bin/env python3
"""Scope-bound actual admission/certification; new PG clones and native stores only."""
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
sys.path.insert(0, str(SOURCE / 'experiments/relay-source'))
spec = importlib.util.spec_from_file_location('bootstrap_oracle', SOURCE / 'experiments/ingest-bootstrap/oracle.py')
oracle = importlib.util.module_from_spec(spec)
sys.modules['bootstrap_oracle'] = oracle
spec.loader.exec_module(oracle)
from oracle import complete_source
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection

root = Path(sys.argv[1]).resolve(strict=True)
assert len(sys.argv) == 2 and SOURCE == root / 'source'
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
build = root / 'out/artifacts'
built = json.loads((build / 'build-result.json').read_text())
assert built['source'] == revision and built['probe_name'] == 'ingest_bootstrap_probe'
sha = lambda value: hashlib.sha256(value).hexdigest()
for name, key in [('bigbrotr', 'binary_sha256'), (built['probe_name'], 'probe_sha256')]:
    assert sha((build / name).read_bytes()) == built[key]
pg_root = root.parent / 'relay-source-recheck-e06f580/pg'
pg = lab.load(pg_root)
assert pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608' and pg['state'] == 'stopped-retained'
basis = root.parent / 'acquisition-scope-54d1a18/engine'
basis_report = (basis / 'result.json').read_bytes()
assert json.loads(basis_report)['status'] == 'PASS_SCOPED_ACQUISITION_REGISTRATION'
output = root / 'bindings'
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.acquisition-binding'
client = 'bigbrotr-acquisition-binding-' + revision[:7]
report = dict(format='bigbrotr-acquisition-binding/1', source=revision, build=built,
    status='RUNNING', owner=owner, label=label, client=None, cases=[], databases=[],
    postgres=pg['postgres'], postgres_owner=pg['owner'],
    not_run=['concurrent scope-policy locks and lost COMMIT recovery', 'installed Sync/progress/M1',
             'measured memory capacity/load/aged-state', 'arm64/release'])


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def save():
    lab.write_json(output / 'result.json', report)


def containers():
    return json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)


def rows(db):
    names = json.loads(lab.sql(pg, db,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    require(len(names) in (64, 66) and all(re.fullmatch('[a-z_]+', n) for n in names), 'table-set')
    parts = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
             f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg, db, 'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t'))


def physical(db):
    return json.loads(lab.sql(pg, db, """
        SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid))
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='control' AND c.relkind='r'
    """))


def clone(name, template):
    db = 'bb_scope_bind_' + revision[:7] + '_' + name.replace('-', '_')
    require(re.fullmatch('[a-z0-9_]{1,63}', db), 'database-name')
    lab.sql(pg, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator TEMPLATE {template}')
    report['databases'].append(db)
    save()
    require(rows(db) == rows(template), 'exact-retained-clone')
    return db


def upgrade(db, name, target=21):
    before, files = rows(db), physical(db)
    config = lab.config(output, dict(pg, client=client), db, endpoint='127.0.0.1:5432', boundary='loopback')
    def apply():
        return lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', str(target))
    result = apply()
    (output / (name + '-migration.stdout')).write_text(result.stdout)
    (output / (name + '-migration.stderr')).write_text(result.stderr)
    require(result.returncode == 0, 'actual-upgrade-' + name)
    after = rows(db)
    stripped = json.loads(json.dumps(after))
    if target == 21:
        for row in stripped['producer_requests']:
            require(row.pop('acquisition_scope_id') is None, 'historical-scope-adoption')
    require(all(stripped[t] == before[t] for t in before if t != 'schema_versions'), 'upgrade-rewrote-old-rows')
    new_versions = [r for r in after['schema_versions'] if r not in before['schema_versions']]
    wanted = ([4] if 'acquisition_scopes' not in before else []) + ([21] if target == 21 else [])
    require(sorted(r['migration'] for r in new_versions) == sorted(wanted), 'upgrade-ledger')
    for row in new_versions:
        filename = {4:'0004_acquisition.sql',21:'0021_acquisition_bindings.sql'}[row['migration']]
        require(row['checksum'] == '\\x' + sha((SOURCE / 'migrations/postgres' / filename).read_bytes()), 'upgrade-checksum')
    require(all(row in after['schema_versions'] for row in before['schema_versions']), 'old-ledger-changed')
    current_files = physical(db)
    require(all(current_files[t] == value for t, value in files.items()), 'nullable-column-heap-rewrite')
    require(apply().returncode == 0 and rows(db) == after and physical(db) == current_files, 'reapply-mutated-data')
    lab.write_json(output / (name + '-upgrade.json'), dict(before=before, after=after, physical_before=files, physical_after=current_files))
    lab.sql(pg, db, 'GRANT SELECT,INSERT ON control.acquisition_selectors,control.acquisition_scopes TO bigbrotr_control; '
        'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
    return before, after


def old_source_schema():
    db = 'bb_scope_bind_' + revision[:7] + '_old_source_schema'
    lab.sql(pg, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator')
    report['databases'].append(db)
    save()
    config = lab.config(output, dict(pg, client=client), db, endpoint='127.0.0.1:5432', boundary='loopback')
    result = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
        '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '19')
    require(result.returncode == 0, 'old-source-schema-setup')
    local = output / Path(config).name
    local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/run/secrets/application'))
    lab.sql(pg, db, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; '
        'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; '
        'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control; '
        'GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control;')
    for args in [('config','initialize'), ('config','activate-initial','--request-id','77'*16,'--expected-revision','1')]:
        result = lab.cli(dict(pg, client=client), *args, '--file', config)
        require(result.returncode == 0, 'old-source-' + args[1])
    lab.sql(pg, db, """
        INSERT INTO control.principals(principal_id,dataset_id,kind,created_at_us,database_role,local_os_uid)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'local',0,'fixture',0 FROM control.datasets;
        INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'alice',true,true,1,1 FROM control.datasets;
        INSERT INTO control.local_credentials(principal_id,dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash)
            SELECT principal_id,dataset_id,1,'argon2id-v1',19,65536,3,1,decode(repeat('02',16),'hex'),
                decode('a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff','hex') FROM control.local_accounts;
        INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'policy.change','dataset',dataset_id FROM control.local_accounts;
    """)
    return db


def run_case(name):
    db = old_source_schema() if name == 'old-source-schema' else clone(name, 'bb_source_template')
    if name != 'old-source-schema':
        upgrade(db, name, target=4 if name == 'old-binding-schema' else 21)
    before = rows(db)
    require(not before['producer_requests'] and not before['owner_epochs'], 'fresh-consumer-prerequisite')
    lab.write_json(output / (name + '-before.json'), before)
    mode = 'source-scope-' + name
    result = lab.docker('exec', client, '/build/ingest_bootstrap_probe', mode, db,
        '/run/secrets/application', '/lab/' + name, check=False, timeout=90)
    (output / (name + '.stdout')).write_text(result.stdout)
    (output / (name + '.stderr')).write_text(result.stderr)
    require(result.returncode == 0 and not result.stderr, 'actual-scope-adapter-' + name)
    messages = [json.loads(line) for line in result.stdout.splitlines()]
    require(messages[-1]['event'] == 'scope-finished' and messages[-1]['mode'] == mode
        and messages[-1]['status'] == 'PASS' and messages[-1]['zero_writes'], 'phase-state-comparison')
    after = rows(db)
    lab.write_json(output / (name + '-after.json'), after)
    committed = name in ('pass','changed','omitted','legacy-adopt','certify-withdrawn')
    dataset = bytes.fromhex(after['datasets'][0]['dataset_id'][2:])
    expected, inputs, receipts = complete_source(dataset, committed)
    dumped = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/' + name)
    (output / (name + '-native.json')).write_text(dumped.stdout)
    actual = [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
    require(actual == expected, 'independent-complete-native-bytes-' + name)
    originals = sorted((output / name / 'producer-inputs-v1').glob('*.cbor'))
    require(len(originals) == len(inputs) and all(p.read_bytes() == inputs[int(p.stem[-16:],16)] for p in originals), 'exact-original-inputs')
    require(len(after['producer_requests']) == len(after['producer_outcomes']) == len(inputs), 'exact-operational-counts')
    for row in after['producer_requests']:
        sequence = int(row['request_sequence'])
        require(row['request_digest'] == '\\x' + sha(inputs[sequence]) and row['input_bytes'] == len(inputs[sequence]), 'original-request-binding')
        if 'acquisition_scope_id' in row:
            if sequence == 1 or name == 'legacy-adopt':
                require(row['acquisition_scope_id'] is None, 'legacy-row-adopted')
            else:
                identities = next(m for m in messages if m['event'] == 'scope-identities')
                require(row['acquisition_scope_id'] == '\\x' + identities['original'], 'changed-reservation-scope')
                scope = next(s for s in after['acquisition_scopes'] if s['scope_id'] == row['acquisition_scope_id'])
                require([row[k] for k in ['dataset_id','relay_endpoint_id','relay_access_scope_id']] ==
                    [scope[k] for k in ['dataset_id','endpoint_id','access_scope_id']], 'complete-source-fk')
    if name != 'old-source-schema':
        documents = {r['document'] for r in after['acquisition_selectors']}
        kinds = [2] if name == 'outside-collection' else [1]
        def canonical(values):
            return '\\x' + json.dumps(dict(version=1,filters=[dict(kinds=values)],history_since=0,ephemeral='retain'),separators=(',',':')).encode().hex()
        require(documents == {canonical(kinds), canonical([1,2])} and len(after['acquisition_scopes']) == 2, 'literal-retained-selectors')
    if name == 'pass':
        require(any(m.get('event') == 'scope-needs-admission' and m['zero_writes'] for m in messages), 'read-before-admission')
    if name == 'certify-withdrawn':
        require(sum(not r['enabled'] and r['generation'] == 2 for r in after['relay_enrollments']) == 1, 'actual-withdrawal')
    require(lab.sql(pg, db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0', 'unjoined-driver')
    report['cases'].append(dict(name=name, status='PASS', actual_pg_rocks=True, complete_native_rows=len(actual), original_inputs=len(inputs)))
    save()


prior = containers()
target = next(c for c in prior if c['Name'] == '/' + pg['postgres'])
assert target['Config']['Labels'][lab.LABEL] == pg['owner'] and not target['State']['Running']
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
assert lab.docker('inspect', client, check=False).returncode != 0
report.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    postgres_id=target['Id'], free_bytes_before=shutil.disk_usage(root).free)
assert report['free_bytes_before'] > 2 * 1024**3
lab.write_json(output / 'preflight.json', dict(containers=[projection(c) for c in prior],
    processes=lab.command(['ps', '-eo', 'pid,ppid,stat,comm']).stdout,
    ports=lab.command(['ss', '-lntup']).stdout, volumes=lab.docker('volume', 'ls').stdout,
    directories=sorted(p.name for p in root.parent.iterdir())))
save()
try:
    lab.docker('start', target['Id'])
    deadline = time.monotonic() + 30
    while lab.docker('exec', target['Id'], 'pg_isready', '-U', 'postgres', check=False).returncode:
        require(time.monotonic() < deadline, 'postgres-readiness')
        time.sleep(.1)
    basic = rows('bb_scope_54d1a18_basic')
    template = rows('bb_source_template')
    require(basic == json.loads((basis / 'basic-after.json').read_text()), 'original-scope-basis')
    require(not template['producer_requests'] and not template['owner_epochs'], 'original-template-preconditions')
    ident = lab.docker('create', '--init', '--name', client, '--label', label + '=' + owner,
        '--network', 'container:' + pg['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
        '--cpus', '2', '--memory', '1g', '--memory-swap', '1g', '--pids-limit', '128', '--read-only',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
        '--mount', f'type=bind,src={output},dst=/lab',
        '--mount', f'type=bind,src={pg_root}/secrets,dst=/run/secrets,readonly',
        '--mount', f'type=bind,src={build},dst=/build,readonly', pg['client_image'],
        'python3', '-c', 'import time; time.sleep(600)').stdout.strip()
    report['client'] = dict(id=ident, name=client)
    save()
    lab.docker('start', ident)
    db = clone('populated', 'bb_scope_54d1a18_basic')
    before, after = upgrade(db, 'populated')
    require(before['producer_requests'], 'nonempty-upgrade-workload')
    report['cases'].append(dict(name='populated-upgrade', status='PASS', prior_rows_xmin_filenodes_unchanged=True))
    save()
    for name in ['pass','changed','omitted','legacy-adopt','outside-collection','outside-request',
                 'no-source','certify-withdrawn','old-source-schema','old-binding-schema']:
        run_case(name)
    require(rows('bb_scope_54d1a18_basic') == basic and rows('bb_source_template') == template
        and (basis / 'result.json').read_bytes() == basis_report, 'retained-original-changed')
    report['status'] = 'PASS_SCOPED_ACQUISITION_BINDING'
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
    raise
finally:
    errors = []
    owned = [target['Id']] + ([report['client']['id']] if report['client'] else [])
    for ident in reversed(owned):
        try:
            item = json.loads(lab.docker('inspect', ident).stdout)[0]
            key, wanted = (lab.LABEL, pg['owner']) if ident == target['Id'] else (label, owner)
            require(item['Config']['Labels'][key] == wanted, 'cleanup-owner')
            if item['State']['Running']:
                lab.docker('stop', '--time', '10', ident)
        except BaseException as error:
            errors.append('stop-' + type(error).__name__)
    try:
        current = {c['Id']: projection(c) for c in containers()}
    except BaseException as error:
        current = {}
        errors.append('inventory-' + type(error).__name__)
    report.update(cleanup_errors=errors, missing_owned=[i for i in owned if i not in current],
        terminal=[current[i] for i in owned if i in current],
        unrelated_changed=[c['Id'] for c in prior if c['Id'] != target['Id'] and current.get(c['Id']) != projection(c)],
        finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), free_bytes_after=shutil.disk_usage(root).free)
    if errors or report['missing_owned'] or report['unrelated_changed'] or any(
        c['State']['Running'] or c['State']['Pid'] or c['State']['OOMKilled'] for c in report['terminal']):
        report['status'] = 'INCOMPLETE_LAB_AUDIT'
    save()
require(report['status'] == 'PASS_SCOPED_ACQUISITION_BINDING', 'incomplete-binding')
print(json.dumps(dict(status=report['status'], cases=len(report['cases']))))
