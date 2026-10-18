"""Actual initial signed acquisition through complete installed Archive publication."""
import json
from pathlib import Path
import secrets
import shutil
import time

import lab
from installed import messages, set_value, terminal, wait_for
from initial_jobs import unchanged, logged_out, released
from installed_sync import BOOTSTRAP, COMMANDS, validate_source, validate_queued, stop_during_quiet
from installed_sync_oracle import ENDPOINT, SIGNED_SHA256
from installed_archive import prepare, invoke, catalog_rows, global_roles, digest, files
from installed_archive_frontier import ARCHIVE_TABLES, current_owner_view, validate, retained_inputs
from installed_archive_native import native

SOURCE = Path(__file__).resolve().parents[2]
CASES = ['archive-wrong-reader', 'archive-missing-reader', 'archive-reader-grant', 'archive-signed']


def provision_reader(pg, output, name, setup, save):
    """Operator setup only, before Serve; no catalog DDL/role mutation at runtime."""
    database = setup['resource']['catalog_database']
    reader = 'bb_archive_read_' + secrets.token_hex(10)
    secret = setup['home'] / 'reader.secret'
    setup['resource']['reader'] = reader; save()
    with secret.open('x') as stream:
        secret.chmod(0o600); stream.write(secrets.token_hex(32))
    lab.sql(pg, 'postgres', f"CREATE ROLE {reader} LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '{secret.read_text()}'")
    lab.sql(pg, database, f'GRANT CONNECT ON DATABASE {database} TO {reader}; '
        f'GRANT USAGE ON SCHEMA public,bigbrotr_archive TO {reader}; '
        f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO {reader}; '
        f'GRANT SELECT ON bigbrotr_archive.installation,bigbrotr_archive.scientific_schema TO {reader}')
    observed = json.loads(lab.sql(pg, database, f"""
        SELECT json_build_object('role',rolname,'login',rolcanlogin,
            'privileged',rolsuper OR rolcreatedb OR rolcreaterole OR rolinherit OR rolreplication OR rolbypassrls,
            'memberships',(SELECT count(*) FROM pg_auth_members WHERE member=r.oid OR roleid=r.oid),
            'connect',has_database_privilege(r.oid,current_database(),'CONNECT'),
            'db_extra',has_database_privilege(r.oid,current_database(),'CREATE,TEMP,CONNECT WITH GRANT OPTION'),
            'schema_extra',has_schema_privilege(r.oid,'public','CREATE,USAGE WITH GRANT OPTION') OR has_schema_privilege(r.oid,'bigbrotr_archive','CREATE,USAGE WITH GRANT OPTION'),
            'tables',(SELECT jsonb_object_agg(n.nspname||'.'||c.relname,jsonb_build_object(
                'select',has_table_privilege(r.oid,c.oid,'SELECT'),
                'extra',has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,SELECT WITH GRANT OPTION')
                    OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES,SELECT WITH GRANT OPTION')))
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('public','bigbrotr_archive') AND c.relkind='r'),
            'function_execute',(SELECT coalesce(bool_or(has_function_privilege(r.oid,p.oid,'EXECUTE')),false)
                FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname IN ('public','bigbrotr_archive')))
        FROM pg_roles r WHERE rolname='{reader}'
    """))
    assert observed['role'] == reader and observed['login'] and observed['connect']
    assert observed['memberships'] == 0
    assert not any(observed[k] for k in ('privileged','db_extra','schema_extra','function_execute'))
    assert set(observed['tables']) == set(setup['catalog'])
    for table, privileges in observed['tables'].items():
        assert privileges == dict(select=table.startswith('public.') or table in (
            'bigbrotr_archive.installation','bigbrotr_archive.scientific_schema'), extra=False)
    assert catalog_rows(pg, database) == setup['catalog']
    lab.write_json(output / (name + '-reader-privileges.json'), observed)
    return reader


def run(pg, client, output, database, name, rows, *, continuation=None, **context):
    assert name in CASES or name == 'archive-export'
    setup = prepare(pg, client, output, database, name, rows, **context)
    directory = setup['home']; prefix = output / name
    reader = provision_reader(pg, output, name, setup, context['save'])
    lab.sql(pg, database, '''
        GRANT SELECT,INSERT ON control.acquisition_jobs TO bigbrotr_control;
        GRANT EXECUTE ON FUNCTION control.acquisition_command_digest(control.acquisition_jobs,control.management_commands),
            control.require_acquisition_scope(bytea,bytea,bytea,bytea),
            control.require_acquisition_job_authority(bytea,bytea),
            control.require_acquisition_execution(bytea,bytea,bytea,bytea,bytea) TO bigbrotr_control;
        INSERT INTO control.principal_grants SELECT dataset_id,principal_id,a,'dataset',dataset_id
            FROM control.local_accounts CROSS JOIN (VALUES ('roles.control'),('status.read')) v(a);
    ''')
    before = rows(database)
    assert all(before[t] == setup['control'][t] for t in before if t != 'principal_grants')
    assert len(before['principal_grants']) == len(setup['control']['principal_grants']) + 2
    assert all(r in before['principal_grants'] for r in setup['control']['principal_grants'])
    if name == 'archive-export':
        lab.sql(pg, database, """
            INSERT INTO control.principal_grants SELECT dataset_id,principal_id,a,'dataset',dataset_id
            FROM control.local_accounts CROSS JOIN (VALUES ('data.read'),('principals.manage')) v(a);
        """)
        before = rows(database)
        assert len(before['principal_grants']) == len(setup['control']['principal_grants']) + 4
    path = output / Path(setup['config']).name
    text = path.read_text()
    text = set_value(text, 'postgres.catalog', 'read_user', reader)
    text = set_value(text, 'postgres.catalog', 'read_password_ref', 'file:/lab/' + name + '/reader.secret')
    rules = [dict(endpoint=ENDPOINT, cidr='127.0.0.1/32', port=7447)]
    text = set_value(text, 'network.private', 'rules', rules)
    text = text.replace(json.dumps(rules), '[{endpoint="' + ENDPOINT + '",cidr="127.0.0.1/32",port=7447}]')
    if name in ('archive-signed', 'archive-export'):
        # This managed file is a staged seed, not the accepted policy. Actual
        # startup must restore accepted publisher=true/window32 before effects.
        text = set_value(text, 'archive.publisher', 'enabled', False)
        text = set_value(text, 'ingest', 'producer_outstanding_requests', 129)
    elif name == 'archive-wrong-reader':
        wrong = directory / 'wrong-reader.secret'
        with wrong.open('x') as stream:
            wrong.chmod(0o600); stream.write(secrets.token_hex(32))
        text = set_value(text, 'postgres.catalog', 'read_password_ref', 'file:/lab/' + name + '/wrong-reader.secret')
    elif name == 'archive-missing-reader':
        # Drop both optional static fields while HTTP archive reads are disabled.
        text = '\n'.join(line for line in text.splitlines()
            if not line.startswith(('read_user = ', 'read_password_ref = '))) + '\n'
    elif name == 'archive-reader-grant':
        lab.sql(pg, setup['resource']['catalog_database'],
            f'REVOKE SELECT ON public.ducklake_snapshot FROM {reader}')
    path.write_text(text)
    invoke(client, output, name + '-journey-validate', 'config', 'validate', '--file', setup['config'])
    before_roles = global_roles(pg)
    lab.write_json(output / (name + '-before.json'), before)
    for filename in ('installed_client.py', 'installed_sync_client.py', 'installed_archive_client.py', 'installed_archive_read.py'):
        shutil.copy2(Path(__file__).with_name(filename), output / filename)
    shutil.copy2(SOURCE / 'tests/fixtures/ingest/import-event.jsonl', directory / 'event.jsonl')
    assert digest(directory / 'event.jsonl') == SIGNED_SHA256
    lab.docker('exec', '-d', client, 'python3', '/lab/installed_client.py', setup['config'], '/lab/' + name, 'archive')
    wait_for(lambda: Path(str(prefix) + '.pid.json').exists(), 'Archive CLI start')
    first = wait_for(lambda: messages(prefix) or Path(str(prefix) + '.terminal.json').exists(), 'Archive initialized')
    if name not in ('archive-signed', 'archive-export'):
        observed = rows(database)
        catalog = catalog_rows(pg, setup['resource']['catalog_database'])
        lab.write_json(output / (name + '-refusal-control.json'), observed)
        lab.write_json(output / (name + '-refusal-catalog.json'), catalog)
        assert not messages(prefix), 'reader prerequisite was deferred beyond fresh Ingest effects'
        ended = terminal(prefix)
        assert ended['exit_code'] in (3, 4) and Path(str(prefix) + '.stderr').read_text()
        assert rows(database) == observed == before, 'reader refusal changed Control'
        assert catalog_rows(pg, setup['resource']['catalog_database']) == catalog == setup['catalog']
        assert not (directory / 'store').exists()
        assert not list((directory / 'state').glob('inputs/**/*'))
        for db in (database, setup['resource']['catalog_database']):
            assert lab.sql(pg, db, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()') == '0'
        return dict(name=name, status='PASS', actual_cli=True, actual_pg_catalog=True,
            no_ingest_effects=True, complete_control_catalog_unchanged=True, terminal=ended)

    assert isinstance(first, list) and first[0]['state'] == 'initialized', 'Archive selected owner refused'
    initialized = first[0]
    assert initialized['ready'] is False and initialized['acquisition'] == 'finite-initial'
    initial = rows(database); lab.write_json(output / (name + '-initialized.json'), initial)
    assert {t for t in before if before[t] != initial[t]} <= BOOTSTRAP
    view = current_owner_view(initial, setup)
    original_deadline = max(r['lease_expires_at_us'] for r in view['owner_epochs'])
    lab.write_json(directory / 'visibility.json', initialized['private_scope'])
    wrapper = '''import subprocess,sys
from pathlib import Path
p=Path(sys.argv[1])
with (p/'client.stdout').open('xb') as out,(p/'client.stderr').open('xb') as err:
 r=subprocess.run(['python3','-B','/lab/installed_archive_client.py',str(p)],stdout=out,stderr=err,timeout=80)
 assert r.returncode==0,'independent Archive client failed'
'''
    lab.docker('exec', '-d', client, 'python3', '-c', wrapper, '/lab/' + name)
    def stage(label, seconds=20):
        wait_for(lambda: (directory / (label + '.json')).exists() or (directory / 'client-exit.json').exists()
            or (directory / 'relay-error.json').exists() or Path(str(prefix) + '.terminal.json').exists(), label, seconds)
        assert (directory / (label + '.json')).exists(), 'client or installed owner ended before ' + label
        return json.loads((directory / (label + '.json')).read_text())
    observations = stage('prepared')
    prepared = rows(database); lab.write_json(output / (name + '-prepared.json'), prepared)
    unchanged(initial, prepared, COMMANDS)
    assert not prepared['jobs'] and len(prepared['producer_requests']) == 2
    validate_source(current_owner_view(prepared, setup), initialized, observations)
    (directory / 'prepared.continue').write_text('source checked\n')
    observations = stage('submitted')
    def published():
        assert not (directory / 'relay-error.json').exists(), 'independent relay failed'
        assert not Path(str(prefix) + '.terminal.json').exists(), 'installed owner ended before full frontier'
        assert not (directory / 'client-exit.json').exists(), 'independent client ended before full frontier'
        state = rows(database)
        assert len(state['producer_requests']) <= 4 and len(state['producer_outcomes']) <= 4
        ready = any(r['last_complete_sequence'] == 12 for r in state['consumer_frontiers'])
        if not ready:
            time.sleep(.2)
            return None
        catalog = catalog_rows(pg, setup['resource']['catalog_database'])
        assert max(r['snapshot_id'] for r in catalog['public.ducklake_snapshot']) == 5
        assert len(state['producer_requests']) == len(state['producer_outcomes']) == 4
        assert len(list(directory.glob('relay-closed-*.json'))) >= 2
        now = int(lab.sql(pg, database, 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        active = current_owner_view(state, setup)
        assert now > original_deadline and min(r['lease_expires_at_us'] for r in active['owner_epochs']) > now
        return state, catalog, now
    current, catalog, now = wait_for(published, 'complete original native prefix published', 60)
    lab.write_json(output / (name + '-published.json'), current)
    lab.write_json(output / (name + '-published-catalog.json'), catalog)
    lab.write_json(output / (name + '-renewed.json'), dict(original_deadline=original_deadline,
        observed_at_us=now, owners=current_owner_view(current, setup)['owner_epochs']))
    effects = ARCHIVE_TABLES | {'producer_epochs', 'producer_requests', 'producer_outcomes',
        'jobs', 'acquisition_jobs', 'job_admissions', 'management_commands', 'management_audit'}
    unchanged(current_owner_view(prepared, setup), current_owner_view(current, setup), effects)
    for table in ('management_commands', 'management_audit'):
        assert all(r in current[table] for r in prepared[table])
        assert len(current[table]) == len(prepared[table]) + 1
    old_producer, = prepared['producer_epochs']; new_producer, = current['producer_epochs']
    assert old_producer['reserved_through_request'] == 2 and new_producer['reserved_through_request'] == 4
    strip = lambda r: {k:v for k,v in r.items() if k not in ('xmin', 'reserved_through_request')}
    assert strip(old_producer) == strip(new_producer)
    validate_source(current_owner_view(current, setup), initialized, observations)
    validate_queued(current_owner_view(current, setup), observations)
    for table in ('archive_coordinators', 'archive_persistence'):
        assert current[table] == before[table]
    for table in ('producer_requests', 'producer_outcomes'):
        assert all(r in current[table] for r in prepared[table])
    (directory / 'submitted.continue').write_text('actual SQL frontier observed\n')
    stage('commands-finished')
    before_stop = rows(database); lab.write_json(output / (name + '-before-stop.json'), before_stop)
    logged_out(current, before_stop)
    signal = stop_during_quiet(client, prefix, True)
    ended = terminal(prefix)
    signal_seconds = (time.monotonic_ns() - signal['signal_monotonic_ns']) / 1_000_000_000
    assert signal_seconds < 5 and ended['exit_code'] == 0 and not Path(str(prefix) + '.stderr').read_text()
    assert messages(prefix)[-1]['native'] == 'closed' and messages(prefix)[-1]['owner_release'] == 'committed'
    (directory / 'commands-finished.continue').write_text('owner joined\n')
    wait_for((directory / 'client-exit.json').exists, 'independent Archive client joined', 6)
    assert json.loads((directory / 'client-exit.json').read_text())['exit_code'] == 0
    after = rows(database); lab.write_json(output / (name + '-after.json'), after)
    # Exact known historical owners are checked before the two-owner projection.
    released(current_owner_view(before_stop, setup), current_owner_view(after, setup))
    old_archive, = [r for r in before_stop['owner_epochs'] if r['role'] == 'archive']
    assert old_archive in after['owner_epochs'] and old_archive['state'] == 'active'
    archive_role, = [r for r in before_stop['role_instances'] if r['role'] == 'archive']
    assert archive_role in after['role_instances'] and archive_role['retired_at_us'] is None
    assert {t for t in before if before[t] != after[t]} <= ARCHIVE_TABLES | BOOTSTRAP | COMMANDS
    transcript = [json.loads(p.read_text()) for p in sorted(directory.glob('relay-closed-*.json'))]
    assert len(transcript) == len(list(directory.glob('relay-open-*.json'))) and not (directory / 'relay-error.json').exists()
    assert all(t['completion'] == 'request-and-websocket-close' and t['frames'] == ['signed','forged-signature','duplicate','EOSE'] for t in transcript[:2])
    selected, = [r for r in transcript if r['number'] == signal['round']]
    assert selected['completion'] == 'quiet-eof' and not selected['frames'] and selected['closed_at_us'] >= signal['signal_at_us']
    native_evidence, wanted = native(client, output, name, current_owner_view(after, setup), initialized,
        transcript, time.time_ns() // 1000)
    final_catalog = catalog_rows(pg, setup['resource']['catalog_database'])
    assert final_catalog == catalog, 'catalog changed after observed final frontier'
    receipts = validate(after, final_catalog, setup, wanted)
    retained = retained_inputs(directory, after, wanted)
    archive_files = {name: files(directory / name) for name in ('data', 'state')}
    lab.write_json(output / (name + '-archive-files.json'), archive_files)
    readback = lab.docker('exec', client, 'python3', '-B', '/lab/installed_archive_read.py', setup['config'],
        '/lab/' + name + '/readback-scratch', check=False, timeout=30)
    (output / (name + '-readback.stdout')).write_text(readback.stdout)
    (output / (name + '-readback.stderr')).write_text(readback.stderr)
    assert readback.returncode == 0 and not readback.stderr, 'independent complete scientific census failed'
    actual = json.loads(readback.stdout)
    assert actual == dict(format='bigbrotr-independent-installed-archive-read/1',
        tables={**wanted['tables'], 'archive_commits': receipts})
    assert catalog_rows(pg, setup['resource']['catalog_database']) == final_catalog and rows(database) == after
    assert {name: files(directory / name) for name in ('data', 'state')} == archive_files
    def naturally_expired():
        observed = int(lab.sql(pg, database, 'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint'))
        return observed if observed >= old_archive['lease_expires_at_us'] else None
    expired_at = wait_for(naturally_expired, 'retained Archive lease naturally expires', 8)
    assert rows(database) == after
    lab.write_json(output / (name + '-retained-authority.json'), dict(archive_owner=old_archive,
        observed_at_us=expired_at, archive_lease_expired=True, original_control_released=True))
    roles = global_roles(pg)
    lab.write_json(output / (name + '-final-roles.json'), roles)
    lab.write_json(output / (name + '-final-catalog.json'), final_catalog)
    assert all(r in roles['roles'] for r in before_roles['roles'])
    assert all(r in roles['members'] for r in before_roles['members'])
    added = [r for r in roles['roles'] if r not in before_roles['roles']]
    assert {r['rolname'] for r in added} == {r['login_name'] for r in final_catalog['bigbrotr_archive.attempts']} and len(added) == 4
    assert all(not any(r[k] for k in ('rolcanlogin', 'credential_present', 'rolsuper', 'rolcreatedb',
        'rolcreaterole', 'rolinherit', 'rolreplication', 'rolbypassrls')) for r in added)
    assert all(r['rolconnlimit'] == 8 and r['rolvaliduntil'] is None for r in added)
    assert {(r['rolname'], r['oid']) for r in added} == {
        (r['login_name'], r['login_oid']) for r in final_catalog['bigbrotr_archive.attempts']}
    new_members = [r for r in roles['members'] if r not in before_roles['members']]
    assert len(new_members) == 4
    assert {r['roleid'] for r in new_members} == {r['oid'] for r in added}
    assert all(int(r['member']) == setup['resource']['roles']['manager_oid'] and r['admin_option']
        and not r['inherit_option'] and not r['set_option'] for r in new_members)
    statuses = json.loads((directory / 'status-observations.json').read_text())
    assert len(statuses) >= 2 and int(statuses[-1]['observed_at_us']) >= now
    assert int(statuses[0]['observed_at_us']) < original_deadline < int(statuses[-1]['observed_at_us'])
    for db in (database, setup['resource']['catalog_database']):
        assert lab.sql(pg, db, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()') == '0'
    export = None
    if name == 'archive-export' and continuation is None:
        from installed_archive_export import run as run_export
        export = run_export(pg, client, output, database, name, rows, setup, initialized, after)
    result = dict(name=name, status='PASS', actual_cli=True, actual_http=True, actual_pg_rocks_archive=True,
        complete_scientific_rows=12, complete_archive_batches=4, retained_inputs=retained,
        frontier_snapshot=5, same_owner_renewed=True, archive_owner_retained_and_expired=True,
        http_status_observations=len(statuses),
        signal_seconds=signal_seconds, selected_quiet_signal=signal, websocket_rounds=len(transcript),
        terminal=ended, export=export, **native_evidence)
    if continuation is not None:
        return continuation(setup, initialized, after, wanted, result)
    return result
