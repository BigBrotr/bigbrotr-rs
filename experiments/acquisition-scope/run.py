#!/usr/bin/env python3
"""Scoped real-PG registration checks; borrow retained PG, mutate new clones only."""
import datetime
import hashlib
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
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection

root = Path(sys.argv[1]).resolve(strict=True)
assert SOURCE == root / 'source' and len(sys.argv) == 2
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
build = root / 'out/artifacts'
built = json.loads((build / 'build-result.json').read_text())
assert built['source'] == revision and built['probe_name'] == 'acquisition_scope_probe'
sha = lambda value: hashlib.sha256(value).hexdigest()
for name, key in [('bigbrotr', 'binary_sha256'), ('acquisition_scope_probe', 'probe_sha256')]:
    assert sha((build / name).read_bytes()) == built[key]
original = root.parent / 'relay-source-recheck-e06f580/pg'
pg = lab.load(original)
assert pg['state'] == 'stopped-retained' and pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608'
basis_root = root.parent / 'source-transport-8c4d12c/transport'
basis = 'bb_transport_8c4d12c_pass'
original_report = (basis_root / 'result.json').read_bytes()
assert json.loads(original_report)['status'] == 'PASS_SCOPED_SOURCE_TRANSPORT'
output = root / 'engine'
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.acquisition-scope'
client = 'bigbrotr-acquisition-scope-' + revision[:7]
report = dict(format='bigbrotr-acquisition-scope-native/1', source=revision, build=built,
    owner=owner, label=label, status='RUNNING', client=None, postgres=pg['postgres'],
    postgres_owner=pg['owner'], cases=[], databases=[], basis=basis,
    not_run=['lost COMMIT and cancellation schedules', 'concurrent policy/registration locks',
             'corrupt overfull candidate bucket', 'raw relational negative matrix',
             'installed Sync, durable socket admission, coverage/M1', 'load/aged state', 'arm64/release'])


def save(): lab.write_json(output / 'result.json', report)


def require(value, reason):
    if not value: raise RuntimeError(reason)


def record(name, condition, **details):
    report['cases'].append(dict(name=name, status='PASS' if condition else 'FAIL', **details))
    save()
    require(condition, name)


def containers():
    return json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)


def rows(database):
    names = json.loads(lab.sql(pg, database,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    require(len(names) in (64, 66) and all(re.fullmatch('[a-z_]+', n) for n in names), 'table-set')
    parts = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
             f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg, database,
        'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t'))


def physical(database):
    return json.loads(lab.sql(pg, database, """
        SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid))
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='control' AND c.relkind='r'
    """))


def prepare(name, template=basis):
    database = 'bb_scope_' + revision[:7] + '_' + name.replace('-', '_')
    require(re.fullmatch('[a-z0-9_]{1,63}', database), 'database-name')
    lab.sql(pg, 'postgres', f'CREATE DATABASE {database} OWNER bigbrotr_migrator TEMPLATE {template}')
    report['databases'].append(database)
    before = rows(database)
    expected = basis_before if template == basis else rows(template)
    require(before == expected, 'exact-clone')
    config = lab.config(output, dict(pg, client=client), database, endpoint='127.0.0.1:5432', boundary='loopback')
    if name == 'old-schema' or template != basis:
        return database, before
    files = physical(database)
    def migrate():
        return lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '4')
    migrated = migrate()
    (output / (name + '-migration.stdout')).write_text(migrated.stdout)
    (output / (name + '-migration.stderr')).write_text(migrated.stderr)
    after = rows(database)
    added = [r for r in after['schema_versions'] if r not in before['schema_versions']]
    after_files = physical(database)
    record(name + '-populated-upgrade', migrated.returncode == 0 and
        set(after)-set(before) == {'acquisition_selectors', 'acquisition_scopes'} and
        all(after[t] == before[t] for t in before if t != 'schema_versions') and
        all(r in after['schema_versions'] for r in before['schema_versions']) and
        len(added) == 1 and added[0]['migration'] == 4 and
        added[0]['checksum'] == '\\x' + sha((SOURCE / 'migrations/postgres/0004_acquisition.sql').read_bytes()) and
        all(after_files[t] == node for t, node in files.items()))
    record(name + '-repeat-upgrade', migrate().returncode == 0 and rows(database) == after)
    lab.sql(pg, database, 'GRANT SELECT,INSERT ON control.acquisition_selectors,control.acquisition_scopes TO bigbrotr_control; '
        'GRANT EXECUTE ON FUNCTION control.acquisition_selector_candidate(bytea),control.guard_acquisition_registration() TO bigbrotr_control;')
    return database, after


def canonical(kinds):
    # Independent literal serializer recipe for these simple fixture documents.
    return json.dumps(dict(version=1, filters=[dict(kinds=kinds)], history_since=0, ephemeral='retain'),
                      separators=(',', ':')).encode()


def run_case(name, template=basis):
    database, before = prepare(name, template)
    if name == 'collision':
        lab.sql(pg, database, """
            CREATE OR REPLACE FUNCTION control.acquisition_selector_candidate(document bytea) RETURNS bytea
            LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SET search_path=pg_catalog
            AS $$ SELECT decode(repeat('42',32),'hex') $$;
        """)
    if name == 'historical':
        # Clone-only revision advancement, no product activation claim. Preserve
        # both exact accepted documents/digests while changing their revision.
        lab.sql(pg, database, """
            BEGIN; SET LOCAL session_replication_role=replica;
            INSERT INTO control.config_revisions
                SELECT 2,dataset_id,1,2,authority_mode,document,document_digest,accepted_source_digest,
                    accepted_at_us+1,actor_id FROM control.config_revisions WHERE revision=1;
            INSERT INTO control.policy_revisions
                SELECT 2,dataset_id,1,2,2,document,document_digest,accepted_at_us+1,actor_id
                FROM control.policy_revisions WHERE revision=1;
            UPDATE control.datasets SET config_revision=2,policy_revision=2;
            COMMIT;
        """)
        before = rows(database)
    lab.write_json(output / (name + '-before.json'), before)
    result = lab.docker('exec', client, '/build/acquisition_scope_probe', database,
        '/run/secrets/application', '/lab/policy.json', name, check=False, timeout=100)
    (output / (name + '.stdout')).write_text(result.stdout)
    (output / (name + '.stderr')).write_text(result.stderr)
    record(name + '-actual-adapter', result.returncode == 0 and not result.stderr,
           exit_code=result.returncode)
    value = json.loads(result.stdout)
    require(value['status'] == 'PASS' and value['mode'] == name, 'probe-result')
    after = rows(database)
    lab.write_json(output / (name + '-after.json'), after)
    allowed = {'owner_epochs', 'role_instances', 'acquisition_selectors', 'acquisition_scopes'}
    record(name + '-retained-other-tables', all(after[t] == before[t] for t in before if t not in allowed))
    if name != 'old-schema':
        selectors, scopes = after['acquisition_selectors'], after['acquisition_scopes']
        if name == 'basic':
            require(len(selectors) == 2 and len(scopes) == 3, 'basic-row-counts')
            wanted = {canonical([1,2]).hex():value['selector'], canonical([1,2,3]).hex():value['wider_selector']}
            require(all(wanted.get(r['document'][2:]) == r['selector_id'][2:] and
                r['candidate_digest'] == '\\x'+sha(bytes.fromhex(r['document'][2:])) for r in selectors), 'exact-selector-documents')
            expected = {(value['original'],value['selector'],1), (value['other'],value['selector'],2),
                        (value['wider'],value['wider_selector'],1)}
            require({(r['scope_id'][2:],r['selector_id'][2:],r['visibility_ordinal']) for r in scopes} == expected,
                    'exact-scope-relationships')
        elif name == 'collision':
            require(len(selectors) == len(scopes) == 64, 'collision-row-counts')
            require({r['document'][2:] for r in selectors} == {canonical([n]).hex() for n in range(64)} and
                all(r['candidate_digest'] == '\\x'+'42'*32 for r in selectors), 'collision-full-equality')
        elif name == 'historical':
            require(selectors == before['acquisition_selectors'] and
                all(r in scopes for r in before['acquisition_scopes']), 'historical-identity-preserved')
            added = [r for r in scopes if r not in before['acquisition_scopes']]
            require(len(added) == 1 and added[0]['policy_revision'] == 2 and
                added[0]['scope_id'] == '\\x'+value['fresh'] and
                any(r['scope_id'] == '\\x'+value['original'] and r['policy_revision'] == 1 for r in before['acquisition_scopes']),
                'fresh-epoch-separate')
        else:
            require(selectors == scopes == [], 'failed-registration-left-data')
        known_access = {row['ordinal']:row['access_scope_id'] for row in basis_before['relay_access_contexts']}
        require(all(row['dataset_id'] == basis_before['datasets'][0]['dataset_id'] and
            row['endpoint_id'] == basis_before['relay_endpoints'][0]['endpoint_id'] and
            row['visibility_kind'] == 4 and row['visibility_domain'] == '\\x'+'40'*16 and
            row['access_scope_id'] == known_access[row['visibility_ordinal']] and
            row['policy_revision'] == (2 if name == 'historical' and row['scope_id'] == '\\x'+value['fresh'] else 1)
            for row in scopes), 'complete-scope-dimensions')
        record(name + '-independent-durable-effects', True, selectors=len(selectors), scopes=len(scopes))
    active = lab.sql(pg, database, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'")
    record(name + '-drivers-joined', active == '0')
    return database


prior = containers()
target = next(c for c in prior if c['Name'] == '/' + pg['postgres'])
assert target['Config']['Labels'][lab.LABEL] == pg['owner'] and not target['State']['Running']
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
assert lab.docker('inspect', client, check=False).returncode != 0
report.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              free_bytes_before=shutil.disk_usage(root).free, postgres_id=target['Id'])
assert report['free_bytes_before'] > 2 * 1024**3
lab.write_json(output / 'preflight.json', dict(containers=[projection(c) for c in prior],
    processes=lab.command(['ps','-eo','pid,ppid,stat,comm']).stdout,
    ports=lab.command(['ss','-lntup']).stdout, volumes=lab.docker('volume','ls').stdout,
    directories=sorted(p.name for p in root.parent.iterdir())))
save()
try:
    lab.docker('start', target['Id'])
    deadline = time.monotonic()+30
    while lab.docker('exec',target['Id'],'pg_isready','-U','postgres',check=False).returncode:
        require(time.monotonic()<deadline,'postgres-readiness'); time.sleep(.1)
    basis_before = rows(basis)
    require(basis_before == json.loads((basis_root/'pass-final.json').read_text()), 'qualified-basis-unchanged')
    policy = basis_before['config_revisions'][0]['document']
    require(policy == basis_before['policy_revisions'][0]['document'] and
        policy['format'] == 'bigbrotr-product-policy/3', 'exact-v2-fixture-policy')
    (output/'policy.json').write_text(json.dumps(policy,sort_keys=True,separators=(',',':')))
    ident = lab.docker('create','--init','--name',client,'--label',label+'='+owner,
        '--network','container:'+pg['postgres'],'--user',f'{os.getuid()}:{os.getgid()}',
        '--cpus','2','--memory','1g','--memory-swap','1g','--pids-limit','128','--read-only',
        '--cap-drop','ALL','--security-opt','no-new-privileges',
        '--mount',f'type=bind,src={output},dst=/lab',
        '--mount',f'type=bind,src={original}/secrets,dst=/run/secrets,readonly',
        '--mount',f'type=bind,src={build},dst=/build,readonly',
        '--mount',f'type=bind,src={SOURCE},dst=/source,readonly',pg['client_image'],
        'python3','-c','import time; time.sleep(600)').stdout.strip()
    report['client'] = dict(id=ident,name=client); save(); lab.docker('start',ident)
    basic = run_case('basic')
    for name in ['collision','rollback','expired','old-schema']: run_case(name)
    run_case('historical',basic)
    record('retained-basis-unchanged', rows(basis)==basis_before and
        (basis_root/'result.json').read_bytes()==original_report)
    report['status']='PASS_SCOPED_ACQUISITION_REGISTRATION'
except BaseException as error:
    report.update(status='FAIL',failure=type(error).__name__+': '+str(error)); raise
finally:
    errors=[]
    owned=[target['Id']]+([report['client']['id']] if report['client'] else [])
    for ident in reversed(owned):
        try:
            item=json.loads(lab.docker('inspect',ident).stdout)[0]
            key,wanted=(lab.LABEL,pg['owner']) if ident==target['Id'] else (label,owner)
            require(item['Config']['Labels'][key]==wanted,'cleanup-owner')
            if item['State']['Running']: lab.docker('stop','--time','10',ident)
        except BaseException as error: errors.append(type(error).__name__)
    logs=lab.docker('logs','--since',report['started_at'],target['Id'],check=False)
    (output/'postgres.stdout').write_text(logs.stdout); (output/'postgres.stderr').write_text(logs.stderr)
    current={c['Id']:projection(c) for c in containers()}
    report.update(cleanup_errors=errors,missing_owned=[i for i in owned if i not in current],
        terminal=[current[i] for i in owned if i in current],
        unrelated_changed=[c['Id'] for c in prior if c['Id']!=target['Id'] and current.get(c['Id'])!=projection(c)],
        finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_bytes_after=shutil.disk_usage(root).free)
    if errors or report['missing_owned'] or report['unrelated_changed'] or any(
        c['State']['Running'] or c['State']['Pid'] or c['State']['OOMKilled'] for c in report['terminal']):
        report['status']='INCOMPLETE_LAB_AUDIT'
    save()
require(report['status']=='PASS_SCOPED_ACQUISITION_REGISTRATION','incomplete-registration')
print(json.dumps(dict(status=report['status'],cases=len(report['cases']))))
