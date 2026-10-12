"""Guarded revision advancement on an exclusively owned qualification clone.

This is privileged fixture setup, not a product policy activation command.
The caller owns the cloned database, resource checks and native/input captures;
the independent oracle owns the expected complete row delta.
"""
import json
import re
import subprocess

import lab


GUARDS = """json_build_object(
    'session_replication_role',current_setting('session_replication_role'),
    'triggers',(SELECT coalesce(json_agg(json_build_object(
        'table',c.relname,'name',t.tgname,'enabled',t.tgenabled,
        'internal',t.tgisinternal,'function',t.tgfoid::regprocedure::text)
        ORDER BY c.relname,t.tgname),'[]'::json)
        FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='control'),
    'constraints',(SELECT coalesce(json_agg(json_build_object(
        'table',c.relname,'name',k.conname,'type',k.contype,
        'validated',k.convalidated,'deferrable',k.condeferrable,'deferred',k.condeferred)
        ORDER BY c.relname,k.conname),'[]'::json)
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='control'))"""

MUTATION = """
DO $fixture$
DECLARE changed bigint;
BEGIN
    IF current_setting('session_replication_role') <> 'origin' OR EXISTS (
        SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='control' AND t.tgenabled NOT IN ('O','A')) THEN
        RAISE EXCEPTION 'policy fixture requires all ordinary Control guards';
    END IF;
    PERFORM 1 FROM control.datasets FOR UPDATE;
    IF (SELECT count(*) FROM control.datasets) <> 1 OR NOT EXISTS (
        SELECT 1 FROM control.datasets WHERE config_revision=1 AND policy_revision=1)
        OR (SELECT count(*) FROM control.config_revisions) <> 1
        OR (SELECT count(*) FROM control.policy_revisions) <> 1 THEN
        RAISE EXCEPTION 'policy fixture requires exactly the retained first revisions';
    END IF;
    INSERT INTO control.config_revisions
        (revision,dataset_id,previous_revision,schema_version,authority_mode,
         document,document_digest,accepted_source_digest,accepted_at_us,actor_id)
        SELECT 2,dataset_id,1,schema_version,authority_mode,
            document,document_digest,accepted_source_digest,accepted_at_us+1,actor_id
        FROM control.config_revisions WHERE revision=1;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN RAISE EXCEPTION 'policy fixture config insertion differs'; END IF;
    INSERT INTO control.policy_revisions
        (revision,dataset_id,previous_revision,config_revision,schema_version,
         document,document_digest,accepted_at_us,actor_id)
        SELECT 2,dataset_id,1,2,schema_version,
            document,document_digest,accepted_at_us+1,actor_id
        FROM control.policy_revisions WHERE revision=1;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN RAISE EXCEPTION 'policy fixture snapshot insertion differs'; END IF;
    UPDATE control.datasets SET config_revision=2,policy_revision=2
        WHERE config_revision=1 AND policy_revision=1;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN RAISE EXCEPTION 'policy fixture head CAS differs'; END IF;
END $fixture$;
"""


def require_guards(observed):
    assert observed['session_replication_role'] == 'origin'
    assert observed['triggers'] and all(t['enabled'] in ('O', 'A') for t in observed['triggers'])
    assert observed['constraints'] and all(k['validated'] is True for k in observed['constraints'])
    selected = {(t['table'], t['name']): t for t in observed['triggers']}
    for table, name in [('datasets', 'dataset_head_guard'),
                        ('config_revisions', 'immutable_config_revisions'),
                        ('policy_revisions', 'immutable_policy_revisions')]:
        assert (table, name) in selected and not selected[table, name]['internal']


def advance(pg, database, rows, output, label):
    """Append exact copied revision2 rows with all normal SQL guards enabled.

    No attempt is made to resolve a timed-out COMMIT or reuse this setup. Its
    caller preserves unknown state and joins/excludes resources before recovery.
    Acceptance timestamps are each predecessor's timestamp plus one microsecond;
    they are fixture coordinates, not a claim of current management authority.
    """
    assert re.fullmatch('[a-z][a-z0-9_]+', database)
    assert re.fullmatch('[a-z][a-z0-9-]+', label)
    assert output.is_dir() and not output.is_symlink()
    paths = {suffix: output / (label + '-policy-' + suffix)
             for suffix in ('setup.json', 'before.json', 'after.json', 'setup.sql', 'response.json')}
    assert all(not p.exists() and not p.is_symlink() for p in paths.values())
    context = dict(format='bigbrotr-initial-gap-policy-setup/1', database=database,
        authority='privileged-isolated-fixture', status='INCOMPLETE',
        timestamps='copied predecessor accepted_at_us plus one microsecond',
        product_policy_activation=False)
    statement = """BEGIN;
SET LOCAL search_path=pg_catalog;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='5s';
SET LOCAL idle_in_transaction_session_timeout='10s';
SELECT json_build_object('phase','before','guards',""" + GUARDS + ");\n" + MUTATION + """
SELECT json_build_object('phase','after','guards',""" + GUARDS + """,
    'transaction_id',pg_current_xact_id()::text);
COMMIT;
"""
    paths['setup.sql'].write_text(statement)
    try:
        context['before'] = rows(database)
        lab.write_json(paths['before.json'], context['before'])
        context['guards_before'] = json.loads(lab.sql(pg, database,
            'SET search_path=pg_catalog; SELECT ' + GUARDS))
        require_guards(context['guards_before'])
        dataset, = context['before']['datasets']
        assert dataset['config_revision'] == dataset['policy_revision'] == 1
        for table in ('config_revisions', 'policy_revisions'):
            original, = context['before'][table]
            assert original['revision'] == 1 and original['previous_revision'] is None
            assert original['dataset_id'] == dataset['dataset_id']
        lab.write_json(paths['setup.json'], context)
        try:
            response = lab.docker('exec', '-i', pg['postgres'], 'psql', '-X', '-A', '-t', '-q',
                '--no-password', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
                data=statement, check=False, timeout=30)
        except subprocess.TimeoutExpired as error:
            def partial(value):
                return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
            context['status'] = 'UNKNOWN_TIMEOUT'
            lab.write_json(paths['response.json'], dict(returncode=None, outcome='UNKNOWN_TIMEOUT',
                stdout=partial(error.stdout), stderr=partial(error.stderr)))
            raise
        lab.write_json(paths['response.json'], dict(returncode=response.returncode,
            stdout=response.stdout, stderr=response.stderr))
        assert response.returncode == 0 and not response.stderr, 'guarded fixture SQL did not complete'
        before, after = [json.loads(line) for line in response.stdout.splitlines()]
        assert before['phase'] == 'before' and after['phase'] == 'after'
        context['guards_transaction_before'] = before['guards']
        context['guards_transaction_after'] = after['guards']
        context['transaction_id'] = after['transaction_id']
        assert re.fullmatch('[1-9][0-9]*', context['transaction_id'])
        context['after'] = rows(database)
        lab.write_json(paths['after.json'], context['after'])
        context['guards_after'] = json.loads(lab.sql(pg, database,
            'SET search_path=pg_catalog; SELECT ' + GUARDS))
        for key in ('guards_transaction_before', 'guards_transaction_after', 'guards_after'):
            require_guards(context[key])
            assert context[key] == context['guards_before'], 'fixture changed SQL guard metadata'
        context['status'] = 'APPLIED'
        return context
    except BaseException as error:
        context['error'] = type(error).__name__
        raise
    finally:
        lab.write_json(paths['setup.json'], context)
