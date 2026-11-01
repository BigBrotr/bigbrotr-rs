"""Shared current-schema database preparation and broad laboratory role checks."""
import json
from pathlib import Path
from schema_inventory import require_tables


def prepare_database(lab, state, root, name, database, mode, inventory, record):
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
    verify_application_role(lab, state, name, database, inventory, record)
    return database, path, local


def verify_application_role(lab, state, name, database, inventory, record, *, delete_tables=()):
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
    assert set(delete_tables) <= {'browser_sessions'}, 'unsupported laboratory DELETE exception'
    assert set(delete_tables) <= set(inventory['tables']), 'unknown laboratory DELETE table'
    wanted = dict(owner='bigbrotr_migrator', select=True, insert=True, update=True,
                  delete=False, truncate=False, references=False, trigger=False)
    record(name + '-application-role',
           role['role'] == 'bigbrotr_control' and role['login']
           and not any(role[key] for key in ['superuser','create_database','create_role',
                                            'replication','bypass_rls','schema_create','memberships'])
           and role['database_owner'] == role['schema_owner'] == 'bigbrotr_migrator'
           and role['schema_usage'] and all(value == dict(wanted,delete=table in delete_tables)
                                          for table,value in role['tables'].items())
           and bool(role['functions']) and all(value['owner'] == 'bigbrotr_migrator' and value['execute']
                                              for value in role['functions'].values()), profile=role)
