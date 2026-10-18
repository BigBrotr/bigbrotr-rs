"""Installed Archive bootstrap and selected-role RED; no publication simulation.

Called only by the inventoried bindings runner. Every database, credential and
local directory is fresh and retained. Expected scientific bytes stay in the
independent Sync oracle and installed_archive_oracle, not CLI result templates.
"""
import hashlib
import json
from pathlib import Path
import re
import secrets
import shutil
import stat
import sys
import time

import lab
from installed import policy_setup, set_value, terminal, wait_for
from sessions import configuration
from registrations import upgrade as upgrade_registry
from scopes import upgrade as upgrade_scopes

SOURCE = Path(__file__).resolve().parents[2]
CASES = ['archive-setup-refusal']
TARGET = 'x86_64-unknown-linux-gnu'
REFUSAL = 'initial Ingest requires control,ingest or control,ingest,sync roles'


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(65536), b''):
            value.update(block)
    return value.hexdigest()


def verify_file(path, expected, executable=False):
    metadata = path.lstat()
    assert stat.S_ISREG(metadata.st_mode) and not metadata.st_mode & 0o022
    assert metadata.st_size == expected['bytes'] and digest(path) == expected['sha256']
    if executable:
        assert metadata.st_mode & 0o100


def artifacts(build, built):
    """Independently bind every installed file to exact build or pinned contract."""
    bundle = build / 'archive-worker'
    assert bundle.is_dir() and not bundle.is_symlink()
    worker = built['archive_worker']
    assert set(worker) == {'bytes', 'sha256'} and worker['bytes'] > 0
    assert re.fullmatch('[0-9a-f]{64}', worker['sha256'])
    verify_file(bundle / 'bigbrotr-archive-worker', worker, executable=True)
    registry = SOURCE / 'contracts/release/archive-engine.json'
    pinned = json.loads(registry.read_text())['platforms'][TARGET]
    expected = {'lib/' + pinned[k]['name']: pinned[k] for k in ('library', 'header')}
    expected.update({'extensions/' + a['name'] + '.duckdb_extension': a for a in pinned['extensions']})
    for name, entry in expected.items():
        verify_file(bundle / name, entry)
    actual = {str(p.relative_to(bundle)) for p in bundle.rglob('*') if p.is_file()}
    assert actual == set(expected) | {'bigbrotr-archive-worker'}, 'unexpected-bundle-file'
    assert all(not p.is_symlink() for p in bundle.rglob('*'))
    return dict(worker=worker, registry_sha256=digest(registry), target=TARGET,
        assets={name: dict(bytes=v['bytes'], sha256=v['sha256']) for name, v in expected.items()})


def invoke(client, output, label, *args, timeout=160):
    started = time.monotonic()
    result = lab.docker('exec', client, '/build/bigbrotr', *args, check=False, timeout=timeout)
    (output / (label + '.stdout')).write_text(result.stdout)
    (output / (label + '.stderr')).write_text(result.stderr)
    lab.write_json(output / (label + '.command.json'), dict(args=args, exit_code=result.returncode,
        seconds=time.monotonic()-started))
    assert result.returncode == 0 and not result.stderr, 'installed-archive-command-' + label
    return json.loads(result.stdout)


def catalog_rows(pg, database):
    tables = json.loads(lab.sql(pg, database, """
        SELECT coalesce(json_agg(schemaname||'.'||tablename ORDER BY schemaname,tablename),'[]')
        FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')
    """))
    assert all(re.fullmatch(r'[a-z_]+\.[a-z_]+', t) for t in tables)
    if not tables:
        return {}
    parts = [f"SELECT '{t}' AS name,coalesce(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]') AS rows "
        f"FROM (SELECT *,xmin::text AS xmin FROM {t}) r" for t in tables]
    return json.loads(lab.sql(pg, database,
        'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t'))


def files(home):
    """Full setup-root census including inode/mode; never reads credential bytes."""
    result = {}
    for p in sorted(home.rglob('*')):
        if p == home / 'manager.secret':
            continue
        metadata = p.lstat()
        assert not stat.S_ISLNK(metadata.st_mode)
        assert stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode)
        result[str(p.relative_to(home))] = dict(mode=stat.S_IMODE(metadata.st_mode),
            inode=metadata.st_ino, device=metadata.st_dev, links=metadata.st_nlink,
            sha256=digest(p) if p.is_file() else None)
    return result


def empty_catalog(pg, database, manager, dataset, catalog, data_root):
    actual = catalog_rows(pg, database)
    native = json.loads((SOURCE / 'contracts/archive/catalog-native-v1.json').read_text())['tables']
    assert len(native) == 28
    assert set(actual) == {'public.' + n for n in native} | {
        'bigbrotr_archive.' + n for n in ('installation', 'owner', 'attempts', 'scientific_schema')}
    assert actual['bigbrotr_archive.attempts'] == []
    owner, = actual['bigbrotr_archive.owner']
    assert owner['ordinal'] == 0 and owner['enabled'] is False
    installation, = actual['bigbrotr_archive.installation']
    assert installation['dataset_id'] == '\\x' + dataset
    assert installation['catalog_id'] == '\\x' + catalog and installation['data_root'] == data_root + '/'
    assert installation['protocol'] == 1
    scientific, = actual['bigbrotr_archive.scientific_schema']
    assert scientific['initialization_snapshot'] == scientific['format_version'] == 1
    assert sorted(r['snapshot_id'] for r in actual['public.ducklake_snapshot']) == [0, 1]
    assert actual['public.ducklake_data_file'] == actual['public.ducklake_delete_file'] == []
    sys.path.insert(0, str(SOURCE / 'experiments/archive-engine'))
    from scientific_layout_oracle import expected as layout_expected, observed as layout_observed
    layout_observed({key.removeprefix('public.'): value for key, value in actual.items()}, layout_expected())
    for column, path in [('declaration_checksum', 'contracts/archive/v1/schema.json'),
            ('native_layout_checksum', 'contracts/archive/v1/native-layout.json'),
            ('sql_checksum', 'migrations/catalog/0002_scientific_schema.sql')]:
        assert scientific[column] == '\\x' + digest(SOURCE / path)
    owners = json.loads(lab.sql(pg, database, """
        SELECT json_agg(json_build_object('schema',n.nspname,'table',c.relname,'owner',r.rolname)
            ORDER BY n.nspname,c.relname)
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles r ON r.oid=c.relowner
        WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relkind='r'
    """))
    assert len(owners) == 32 and all(r['owner'] == manager for r in owners)
    database_oid = lab.sql(pg, database, 'SELECT oid FROM pg_database WHERE datname=current_database()')
    reservation = lab.sql(pg, database,
        "SELECT shobj_description(oid,'pg_authid') FROM pg_roles WHERE rolname='bb_init_database_" + database_oid + "'")
    prefix, nonce, bootstrap_oid, manager_oid = reservation.split(':')
    assert prefix == 'bigbrotr-catalog-initialization/1' and re.fullmatch('[0-9a-f]{32}', nonce)
    assert bootstrap_oid.isdigit() and manager_oid.isdigit()
    assert lab.sql(pg, database, f"SELECT oid={manager_oid} FROM pg_roles WHERE rolname='{manager}'") == 't'
    assert lab.sql(pg, database, f"SELECT NOT rolcanlogin AND rolpassword IS NULL AND rolname='bb_init_{nonce}' FROM pg_authid WHERE oid={bootstrap_oid}") == 't'
    assert lab.sql(pg, database, f'SELECT count(*) FROM pg_stat_activity WHERE usesysid={bootstrap_oid}') == '0'
    return actual, dict(manager=manager, manager_oid=int(manager_oid), bootstrap='bb_init_' + nonce,
        bootstrap_oid=int(bootstrap_oid), reservation='bb_init_database_' + database_oid, bootstrap_retired=True)


def global_roles(pg):
    # Retain only credential presence plus xmin, never password/verifier bytes.
    return json.loads(lab.sql(pg, 'postgres', """
        SELECT json_build_object(
            'roles',(SELECT jsonb_agg((to_jsonb(r)-'rolpassword')||jsonb_build_object(
                'credential_present',rolpassword IS NOT NULL,'xmin',xmin::text) ORDER BY oid) FROM pg_authid r),
            'members',(SELECT coalesce(jsonb_agg(to_jsonb(m)||jsonb_build_object('xmin',xmin::text)
                ORDER BY roleid,member,grantor),'[]') FROM pg_auth_members m))
    """))


def prepare(pg, client, output, database, name, rows, resources, save, build, built,
            setup_upgrade=None):
    verified = artifacts(build, built)
    info = invoke(client, output, name + '-build-info', 'build-info')
    assert info['archive_worker'] == verified['worker']
    assert info['source']['revision'] == built['source'] and info['source']['dirty'] is False
    verification = invoke(client, output, name + '-worker', 'archive', 'verify-worker', '--root', '/build/archive-worker')
    assert verification['format'] == 'bigbrotr-worker-verification/1'
    assert verification['result'] == 'verified' and verification['executable'] == verified['worker']
    assert verification['target'] == TARGET
    lab.write_json(output / (name + '-artifacts.json'), verified)
    upgrade_registry(pg, client, output, database, name, rows)
    upgrade_scopes(pg, client, output, database, name, rows)
    if setup_upgrade is not None:
        setup_upgrade(pg, client, output, database, name, rows)
    policy_setup(pg, database, name, rows, output, administration=True)
    before = rows(database)
    assert not before['owner_epochs'] and not before['producer_requests']
    assert not before['archive_coordinators'] and not before['archive_persistence'] and not before['archive_attempts']
    assert {10, 15, 23, 24, 26} <= {r['migration'] for r in before['schema_versions']}
    home = output / name
    home.mkdir(mode=0o700)
    for directory in ('data', 'state', 'scratch'):
        (home / directory).mkdir(mode=0o700)
    catalog_db = database + '_cat'
    assert re.fullmatch('[a-z0-9_]{1,63}', catalog_db)
    manager = 'bb_archive_' + secrets.token_hex(12)
    catalog = secrets.token_hex(16)
    resource = dict(control_database=database, catalog_database=catalog_db,
        manager=manager, home=str(home), catalog_id=catalog, state='creating')
    resources.setdefault('archive_resources', []).append(resource)
    resources['databases'].append(catalog_db)
    save()
    original_roles = global_roles(pg)
    secret = home / 'manager.secret'
    with secret.open('x') as stream:
        secret.chmod(0o600)
        stream.write(secrets.token_hex(32))
    lab.sql(pg, 'postgres', f"CREATE ROLE {manager} LOGIN NOSUPERUSER CREATEROLE NOCREATEDB NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '{secret.read_text()}';")
    lab.sql(pg, 'postgres', f'CREATE DATABASE {catalog_db}')
    lab.sql(pg, catalog_db, f'REVOKE ALL ON DATABASE {catalog_db} FROM PUBLIC; REVOKE ALL ON SCHEMA public FROM PUBLIC;')
    assert catalog_rows(pg, catalog_db) == {}
    config = lab.config(output, dict(pg, client=client), database, endpoint='127.0.0.1:5432', boundary='loopback')
    path = output / Path(config).name
    text = configuration(path.read_text(), before, name)
    for section, key, value in [
        ('topology', 'local_roles', ['control', 'ingest', 'sync', 'archive']),
        ('archive', 'catalog_id', catalog), ('archive', 'worker_memory_bytes', '512MiB'),
        ('postgres.catalog', 'database', catalog_db), ('postgres.catalog', 'endpoint', '127.0.0.1:5432'),
        ('postgres.catalog', 'user', manager), ('postgres.catalog', 'password_ref', 'file:/lab/' + name + '/manager.secret'),
        ('postgres.catalog.tls', 'mode', 'local-plaintext'), ('postgres.catalog.tls', 'local_boundary', 'loopback'),
        ('storage', 'archive_root', '/lab/' + name + '/data'),
        ('storage', 'archive_state_root', '/lab/' + name + '/state'),
        ('storage', 'scratch_root', '/lab/' + name + '/scratch'),
        ('network.dns', 'resolver', '127.0.0.1:9'), ('network.tls', 'extra_roots', []),
    ]:
        text = set_value(text, section, key, value)
    path.write_text(text)
    invoke(client, output, name + '-validate', 'config', 'validate', '--file', config)
    dataset = before['datasets'][0]['dataset_id'][2:]
    initialized = invoke(client, output, name + '-initialize', 'archive', 'initialize', '--file', config,
        '--initializer-user', 'postgres', '--initializer-password-ref', 'file:/run/secrets/bootstrap',
        '--worker-root', '/build/archive-worker')
    assert initialized == dict(format='bigbrotr-catalog-initialization/1', status='installed',
        new_installation=True, dataset_id=dataset, catalog_id=catalog, bootstrap='retired',
        manager_inspection='verified', runtime_authorization='not-granted', ready=False)
    empty, roles = empty_catalog(pg, catalog_db, manager, dataset, catalog, '/lab/' + name + '/data')
    final_roles = global_roles(pg)
    assert all(r in final_roles['roles'] for r in original_roles['roles'])
    assert all(r in final_roles['members'] for r in original_roles['members'])
    added_roles = [r for r in final_roles['roles'] if r not in original_roles['roles']]
    assert {r['rolname'] for r in added_roles} == {manager, roles['bootstrap'], roles['reservation']}
    assert len(added_roles) == 3
    added_members = [r for r in final_roles['members'] if r not in original_roles['members']]
    assert all(r['member'] == roles['manager_oid'] and r['roleid'] == roles['bootstrap_oid']
        and r['admin_option'] and not r['inherit_option'] and not r['set_option'] for r in added_members)
    manager_row, = [r for r in added_roles if r['rolname'] == manager]
    assert manager_row['rolcanlogin'] and manager_row['rolcreaterole']
    assert not any(manager_row[k] for k in ('rolsuper','rolcreatedb','rolinherit','rolreplication','rolbypassrls'))
    lab.write_json(output / (name + '-setup-roles.json'), dict(before=original_roles, after=final_roles))
    resource.update(roles=roles, state='catalog-initialized'); save()
    assert rows(database) == before, 'catalog-initialization-mutated-control'
    bound = invoke(client, output, name + '-state', 'archive', 'state-initialize', '--file', config)
    assert bound == dict(format='bigbrotr-coordinator-state/1', state='bound', dataset_id=dataset,
        catalog_id=catalog, catalog_inspection='not-performed', attempt_adoption='not-performed',
        publication_authority='not-granted', ready=False)
    binding = (home / 'state/coordinator.json').read_bytes()
    assert binding == (home / 'data/.bigbrotr-initialization/coordinator.json').read_bytes()
    binding_hash = hashlib.sha256(binding).hexdigest()
    local = files(home)
    for retry in (False, True):
        result = invoke(client, output, name + ('-coordinator-retry' if retry else '-coordinator'),
            'archive', 'coordinator-initialize', '--file', config)
        assert result == dict(format='bigbrotr-coordinator-initialization/1', state='bound',
            created=not retry, dataset_id=dataset, catalog_id=catalog, binding_sha256=binding_hash,
            owner_release='committed', catalog_inspection='verified', persistence_protocol=1,
            persistence_created=not retry, publication_authority='not-granted', ready=False)
        current = rows(database)
        for table in before.keys() - {'owner_epochs', 'role_instances', 'archive_coordinators', 'archive_persistence'}:
            assert current[table] == before[table], table
        assert len(current['owner_epochs']) == len(current['role_instances']) == (2 if retry else 1)
        assert all(r['role'] == 'control' and r['state'] == 'released' and r['released_at_us'] is not None for r in current['owner_epochs'])
        assert all(r['retired_at_us'] is not None for r in current['role_instances'])
        coordinator, = current['archive_coordinators']; persistence, = current['archive_persistence']
        assert coordinator['binding_sha256'] == persistence['coordinator_binding_sha256'] == '\\x' + binding_hash
        assert coordinator['dataset_id'] == persistence['dataset_id'] == '\\x' + dataset
        assert coordinator['catalog_id'] == persistence['catalog_id'] == '\\x' + catalog
        assert coordinator['protocol_version'] == persistence['protocol_version'] == persistence['baseline_snapshot'] == 1
        assert persistence['protection_class'] == 'local-synced'
        issuer, = [r for r in current['owner_epochs'] if r['resource_id'] == coordinator['issuer_resource']
            and r['epoch'] == coordinator['issuer_epoch']]
        assert coordinator['issuer_role'] == persistence['issuer_role'] == 'control'
        for field in ('issuer_resource', 'issuer_epoch', 'issuer_incarnation'):
            assert persistence[field] == coordinator[field]
        assert coordinator['issuer_incarnation'] == issuer['process_incarnation']
        assert issuer['acquired_at_us'] <= coordinator['initialized_at_us'] <= persistence['initialized_at_us'] <= issuer['released_at_us']
        if retry:
            for table in ('archive_coordinators', 'archive_persistence'):
                assert current[table] == first[table], 'idempotent-binding-rewritten'
            assert all(row in current['owner_epochs'] for row in first['owner_epochs'])
            assert all(row in current['role_instances'] for row in first['role_instances'])
        else:
            first = current
        assert catalog_rows(pg, catalog_db) == empty and files(home) == local
        assert global_roles(pg) == final_roles
    resource.update(state='setup-retained', config=config, binding_sha256=binding_hash); save()
    lab.write_json(output / (name + '-setup-control.json'), current)
    lab.write_json(output / (name + '-setup-catalog.json'), empty)
    lab.write_json(output / (name + '-setup-files.json'), local)
    return dict(config=config, home=home, resource=resource, control=current, catalog=empty, files=local, roles=final_roles)


def run(pg, client, output, database, name, rows, **context):
    assert name in CASES
    setup = prepare(pg, client, output, database, name, rows, **context)
    shutil.copy2(Path(__file__).with_name('installed_client.py'), output / 'installed_client.py')
    prefix = output / (name + '-selected')
    lab.docker('exec', '-d', client, 'python3', '/lab/installed_client.py', setup['config'],
        '/lab/' + prefix.name, 'archive')
    wait_for(lambda: Path(str(prefix) + '.pid.json').exists(), 'archive-cli-start')
    ended = terminal(prefix)
    assert ended['exit_code'] == 3 and not Path(str(prefix) + '.stdout').read_bytes()
    assert Path(str(prefix) + '.stderr').read_text().strip() == 'error: ' + REFUSAL
    assert rows(database) == setup['control'], 'refused-selection-mutated-control'
    assert catalog_rows(pg, setup['resource']['catalog_database']) == setup['catalog']
    assert global_roles(pg) == setup['roles']
    assert files(setup['home']) == setup['files'] and not (setup['home'] / 'store').exists()
    for db in (database, setup['resource']['catalog_database']):
        assert lab.sql(pg, db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()") == '0'
    return dict(name=name, status='PASS', scope='installed empty catalog, coordinator, persistence and baseline role refusal only',
        actual_cli=True, actual_catalog=True, selected_archive='EXPECTED_FAIL', refusal=REFUSAL,
        publication='NOT_RUN', original_native_49_row_oracle='preserved; not exercised',
        terminal=ended, retained_setup=setup['resource'])
