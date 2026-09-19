"""Independent retained-row/collision controls on disposable catalog clones.

Each clone is created here from the completed empty dead-driver fixture and is
dropped here after recording its exact before/after state. No history trigger is
disabled and the original catalog/journal is never changed.
"""
import json
import secrets

from signed_publication_oracle import corpus


def run(sql, observe, records, save):
    digest = corpus()[0][0]['archive_digest']
    attempt, worker = 'f4'*16, 'f5'*16
    target, auxiliary = 'bb_archive_'+attempt, 'bbmiss_auxiliary'
    cases = (
        ('ordinal_collision', 'conflict'),
        ('attempt_collision', 'conflict'),
        ('worker_collision', 'conflict'),
        ('login_collision', 'conflict'),
        ('orphan_disabled_role', 'conflict'),
        ('orphan_login_role', 'conflict'),
        ('missing_imported_role', 'conflict'),
        ('recreated_imported_role', 'conflict'),
        ('owner_without_attempt', 'incompatible-layout'),
        ('unrelated_same_batch', 'not-imported'),
    )
    for name, expected in cases:
        db = 'bbmiss_'+name
        item = dict(name=name, database=db, status='INCOMPLETE', expected=expected)
        records.append(item)
        save()
        # CREATE refuses a pre-existing database; cleanup only follows this
        # successful creation, never adopting a same-named external resource.
        sql('postgres', f'CREATE DATABASE {db} TEMPLATE bbpub_custody_prelock_dead;')
        created_roles = []
        try:
            role = target if name in ('login_collision', 'orphan_disabled_role', 'orphan_login_role',
                                      'missing_imported_role', 'recreated_imported_role') else auxiliary
            login = 'LOGIN' if name in ('orphan_login_role', 'unrelated_same_batch') else 'NOLOGIN'
            # Only the preservation control needs usable authentication. Keep
            # its generated fixture secret off argv, events and captured state.
            password = f" PASSWORD '{secrets.token_hex(32)}'" if name == 'unrelated_same_batch' else ''
            sql('postgres', f'CREATE ROLE {role} {login} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS{password};')
            created_roles.append(role)
            if name == 'owner_without_attempt':
                sql(db, 'UPDATE bigbrotr_archive.owner SET ordinal=99 WHERE singleton;')
            elif not name.startswith('orphan_'):
                ordinal = 1 if name in ('ordinal_collision', 'missing_imported_role', 'recreated_imported_role') else 2
                aid = attempt if name in ('attempt_collision', 'missing_imported_role', 'recreated_imported_role') else '12'*16
                wid = worker if name in ('worker_collision', 'missing_imported_role', 'recreated_imported_role') else '13'*16
                sql(db, f"INSERT INTO bigbrotr_archive.attempts SELECT {ordinal},decode('{aid}','hex'),decode('{'50'*16}','hex'),decode('{wid}','hex'),0,decode('{digest}','hex'),'{role}',oid FROM pg_roles WHERE rolname='{role}';")
                if name == 'login_collision':
                    # Retain the conflicting historical login name, but remove
                    # the actual target-name role so orphan detection cannot
                    # accidentally satisfy this independent discriminator.
                    sql('postgres', f'ALTER ROLE {role} RENAME TO {auxiliary};')
                    created_roles.remove(role)
                    created_roles.append(auxiliary)
                if name in ('missing_imported_role', 'recreated_imported_role'):
                    item['original_oid'] = int(sql('postgres', f"SELECT oid FROM pg_roles WHERE rolname='{role}';"))
                    sql('postgres', f'DROP ROLE {role};')
                    created_roles.remove(role)
                    if name == 'recreated_imported_role':
                        sql('postgres', f'CREATE ROLE {role} NOLOGIN;')
                        created_roles.append(role)
                        item['replacement_oid'] = int(sql('postgres', f"SELECT oid FROM pg_roles WHERE rolname='{role}';"))
                        assert item['replacement_oid'] != item['original_oid']
                if name == 'unrelated_same_batch':
                    sql(db, 'UPDATE bigbrotr_archive.owner SET ordinal=2,enabled=true WHERE singleton;')

            def snapshot():
                return json.loads(sql(db, f"SELECT json_build_object('owner',(SELECT row_to_json(o) FROM bigbrotr_archive.owner o),'owner_xmin',(SELECT xmin::text FROM bigbrotr_archive.owner),'attempts',(SELECT coalesce(json_agg(row_to_json(a)),'[]'::json) FROM bigbrotr_archive.attempts a),'roles',(SELECT coalesce(json_agg(json_build_object('name',rolname,'oid',oid,'xmin',xmin::text,'login',rolcanlogin,'password_null',rolpassword IS NULL) ORDER BY rolname),'[]'::json) FROM pg_authid WHERE rolname IN ('{target}','{auxiliary}')),'files',(SELECT count(*) FROM public.ducklake_data_file),'snapshots',(SELECT count(*) FROM public.ducklake_snapshot));"))

            item['before'] = snapshot()
            if name == 'login_collision':
                assert item['before']['attempts'][0]['login_name'] == target
                assert [r['name'] for r in item['before']['roles']] == [auxiliary]
            if name == 'unrelated_same_batch':
                assert item['before']['roles'][0]['login'] and not item['before']['roles'][0]['password_null']
            save()
            item['observed'] = observe('resolve', expected, db)
            assert item['observed']['state'] == expected and not item['observed']['commit_started']
            item['after'] = snapshot()
            assert item['before'] == item['after']
            assert item['after']['files'] == 0 and item['after']['snapshots'] == 2
            item['status'] = 'PASS'
        finally:
            try:
                # Ordinary DROP refuses any still-live client. Do not force away
                # an unknown connection or weaken immutable history for cleanup.
                sql('postgres', f'DROP DATABASE {db};')
                for role in created_roles:
                    sql('postgres', f'DROP ROLE {role};')
                item['cleanup'] = 'owned clone and fixture roles dropped'
            finally:
                save()
