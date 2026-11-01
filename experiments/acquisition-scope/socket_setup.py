"""Fresh current-schema setup for the existing three socket probe modes only."""
import copy
import hashlib
import json
import os
import re
import secrets

from current_lab import prepare_database
from schema_inventory import declared_inventory, require_tables

# Exact noncompiled changes reviewed for this fixture increment. An unrestricted
# experiments/ prefix would also admit changed inputs used by compiled examples.
HARNESS_PATHS = {
    'experiments/acquisition-scope/bindings.py',
    'experiments/acquisition-scope/socket_setup.py',
    'experiments/acquisition-scope/README.md',
    'experiments/policy-versions/current_lab.py',
    'experiments/policy-versions/run.py',
    'tests/acceptance/test_socket_setup.py',
}


def require_build(lab, source, build, receipt, revision):
    assert re.fullmatch('[0-9a-f]{40}', receipt['source']), 'invalid built product revision'
    assert receipt['probe_name'] == 'ingest_bootstrap_probe', 'wrong socket probe'
    changes = lab.command(['git', '-C', str(source), 'diff', '--name-only', '--no-renames',
                           receipt['source'], revision, '--']).stdout.splitlines()
    assert set(changes) <= HARNESS_PATHS, 'built product changed beyond socket harness'
    for name, field in [('bigbrotr', 'binary_sha256'), ('ingest_bootstrap_probe', 'probe_sha256')]:
        assert hashlib.sha256((build / name).read_bytes()).hexdigest() == receipt[field], \
            name + ' differs from its exact-source build receipt'
    return changes


def rows(lab, state, database, inventory):
    names = json.loads(lab.sql(state, database,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    require_tables(names, inventory)
    queries = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
               f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(state, database, 'SET max_parallel_workers_per_gather=0; '
        'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(queries) + ') t'))


def seed_alice(lab, state, database, inventory, record):
    """The existing fixed credential fixture, inserted only in the new template."""
    before = rows(lab, state, database, inventory)
    dataset, = before['datasets']
    dataset = dataset['dataset_id']
    principal = '\\x' + '10' * 16
    assert not before['local_accounts'] and not before['local_credentials'] and not before['principal_grants']
    assert not any(row['principal_id'] == principal for row in before['principals'])
    password_hash = 'a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff'
    lab.sql(state, database, """
        BEGIN;
        INSERT INTO control.principals(principal_id,dataset_id,kind,created_at_us,database_role,local_os_uid)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'local',0,'fixture',0 FROM control.datasets;
        INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision,authentication_revision)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'alice',true,true,1,1,1 FROM control.datasets;
        INSERT INTO control.local_credentials(principal_id,dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash)
            SELECT principal_id,dataset_id,1,'argon2id-v1',19,65536,3,1,decode(repeat('02',16),'hex'),
                decode('a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff','hex') FROM control.local_accounts;
        INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'policy.change','dataset',dataset_id FROM control.local_accounts;
        COMMIT;
    """)
    expected = copy.deepcopy(before)
    expected['principals'].append(dict(principal_id=principal,dataset_id=dataset,kind='local',created_at_us=0,
                                      database_role='fixture',local_os_uid=0))
    # Migration 0806 starts a fresh account's authentication generation at one.
    expected['local_accounts'] = [dict(principal_id=principal,dataset_id=dataset,username='alice',
        initial_owner=True,active=True,credential_revision=1,grant_revision=1,authentication_revision=1)]
    expected['local_credentials'] = [dict(principal_id=principal,dataset_id=dataset,credential_revision=1,
        format='argon2id-v1',argon_version=19,memory_kib=65536,passes=3,lanes=1,
        salt='\\x'+'02'*16,password_hash='\\x'+password_hash)]
    expected['principal_grants'] = [dict(dataset_id=dataset,principal_id=principal,action='policy.change',
                                       resource_kind='dataset',resource_id=dataset)]
    after = rows(lab, state, database, inventory)
    comparable = copy.deepcopy(after)
    for table in ('principals','local_accounts','local_credentials','principal_grants'):
        added, = [row for row in comparable[table] if row['principal_id'] == principal]
        assert int(added.pop('xmin')) > 0
    ordered = lambda value: {name: sorted(values,key=lambda row: json.dumps(row,sort_keys=True))
                             for name,values in value.items()}
    record('alice-only-template-seed', ordered(comparable) == ordered(expected), before=before, after=after)


def stop_setup(lab, pg_root, report):
    """Join only resources named by this new lab's ownership receipt."""
    path = pg_root / 'resources.json'
    if not path.exists():
        return
    state = json.loads(path.read_text())
    assert state['root'] == str(pg_root) and state['format'] == 'bigbrotr-pg-lab/1'
    terminal, errors = [], []
    for kind in ('client','postgres'):
        try:
            result = lab.docker('inspect', state[kind], check=False)
            if result.returncode:
                assert report['status'] != 'PASS_SOCKET_SETUP', 'missing created setup resource'
                continue
            item, = json.loads(result.stdout)
            assert item['Config']['Labels'][lab.LABEL] == state['owner'], 'setup cleanup owner mismatch'
            if item['State']['Running']:
                lab.docker('stop','--time','10',item['Id'])
            item, = json.loads(lab.docker('inspect',item['Id']).stdout)
            assert not item['State']['Running'] and not item['State']['Pid'] and not item['State']['OOMKilled']
            terminal.append(dict(kind=kind,id=item['Id'],state=item['State']))
        except BaseException as error:
            errors.append(kind+'-'+type(error).__name__)
    state['state'] = 'stopped-retained' if not errors else 'cleanup-incomplete'
    lab.write_json(path,state)
    report.update(terminal=terminal,cleanup_errors=errors)
    if errors:
        report['status'] = 'INCOMPLETE_LAB_AUDIT'
        raise RuntimeError('socket setup cleanup incomplete')


def prepare(lab, root, build, source, revision):
    pg_root = root / 'pg'
    assert not pg_root.exists() and not pg_root.is_symlink(), 'socket PG root already exists'
    assert not (root/'bindings').exists() and not (root/'bindings').is_symlink(), 'socket output already exists'
    inventory = declared_inventory(source)
    report = dict(format='bigbrotr-socket-setup/1',status='RUNNING',harness_revision=revision,
                  schema=inventory,cases=[],not_run=['actual three-mode socket/native qualification'])
    def save():
        if pg_root.exists():
            lab.write_json(pg_root/'socket-setup-result.json',report)
    def record(name, condition, **details):
        report['cases'].append(dict(name=name,status='PASS' if condition else 'FAIL',**details))
        save()
        if not condition:
            raise RuntimeError(name)
    try:
        # Fresh attempts keep stopped resources. A random suffix prevents names
        # colliding when the same exact revision is qualified more than once.
        lab.setup(pg_root,build,source,'socket_'+revision[:7]+'_'+secrets.token_hex(6),
                  subnet=os.environ.get('BIGBROTR_SOCKET_SUBNET'))
        state = lab.load(pg_root)
        _, path, _ = prepare_database(lab,state,pg_root,'socket-template','bb_source_template',
                                       'managed',inventory,record)
        for action, extra in [('initialize',()),('activate-initial',
                ('--request-id','77'*16,'--expected-revision','1'))]:
            result = lab.cli(state,'config',action,'--file',path,*extra)
            record('socket-template-'+action,result.returncode == 0 and not result.stderr,
                   stdout=result.stdout,stderr=result.stderr)
        current = rows(lab,state,'bb_source_template',inventory)
        document = json.loads((source/'tests/fixtures/configuration/policy-v3.json').read_text())
        encoded = json.dumps(document,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
        digest = '\\x'+hashlib.sha256(b'bigbrotr/product-policy/3\0'+encoded).hexdigest()
        config, = current['config_revisions']
        policy, = current['policy_revisions']
        record('socket-template-current-accepted-policy',all(row['document'] == document
            and row['schema_version'] == 3 and row['document_digest'] == digest for row in (config,policy)))
        record('socket-template-no-intake',not current['owner_epochs'] and not current['producer_requests']
               and not current['producer_outcomes'])
        seed_alice(lab,state,'bb_source_template',inventory,record)
        report['status'] = 'PASS_SOCKET_SETUP'
    except BaseException as error:
        report.update(status='FAIL',failure=type(error).__name__+': '+str(error))
        raise
    finally:
        try:
            stop_setup(lab,pg_root,report)
        finally:
            save()
    return pg_root, lab.load(pg_root), inventory
