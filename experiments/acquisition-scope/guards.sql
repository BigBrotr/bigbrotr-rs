-- Isolated raw SQL fixtures. Connect as the application login, never SET ROLE.
-- Each execution rolls back ALL rows, including its actual backend attribution.
-- Mutation privileges are granted only on fresh clones to reach the guards.
\if :repeatable
BEGIN ISOLATION LEVEL REPEATABLE READ;
\else
BEGIN;
\endif
CREATE TEMP TABLE fixture_counts (executed integer NOT NULL);
INSERT INTO fixture_counts VALUES (0);
CREATE TEMP TABLE fixture_owner AS
    SELECT resource_id,epoch+1 AS epoch,dataset_id FROM control.owner_epochs
    WHERE role='control' ORDER BY epoch DESC LIMIT 1;
CREATE FUNCTION pg_temp.id(pair text) RETURNS bytea LANGUAGE sql IMMUTABLE
AS $$ SELECT decode(repeat(pair,16),'hex') $$;
CREATE FUNCTION pg_temp.must_fail(statement text, expected text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE observed text;
BEGIN
    BEGIN
        EXECUTE statement;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS observed = RETURNED_SQLSTATE;
    END;
    IF observed IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'expected SQLSTATE %, observed % for %', expected, observed, statement;
    END IF;
    UPDATE fixture_counts SET executed=executed+1;
END $$;

-- Focused fixture builders keep every inserted column visible while allowing
-- one independent relational counterexample at a time. No product encoder used.
CREATE FUNCTION pg_temp.selector(changes jsonb DEFAULT '{}'::jsonb) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE value control.acquisition_selectors%ROWTYPE;
BEGIN
    SELECT (jsonb_populate_record(NULL::control.acquisition_selectors,
        to_jsonb(s)||jsonb_build_object('selector_id',pg_temp.id('d1'),
            'document',convert_to('{"version":1,"filters":[{"kinds":[11]}],"history_since":0,"ephemeral":"retain"}','UTF8'),
            'owner_resource_id',o.resource_id,'owner_epoch',o.epoch,'owner_incarnation',pg_temp.id('c2'))||changes)).*
    INTO STRICT value FROM control.acquisition_selectors s CROSS JOIN fixture_owner o LIMIT 1;
    INSERT INTO control.acquisition_selectors(selector_id,dataset_id,profile_version,document,
        registered_at_us,config_revision,policy_revision,owner_resource_id,owner_epoch,owner_incarnation)
    VALUES(value.selector_id,value.dataset_id,value.profile_version,value.document,
        value.registered_at_us,value.config_revision,value.policy_revision,
        value.owner_resource_id,value.owner_epoch,value.owner_incarnation);
END $$;
CREATE FUNCTION pg_temp.scope(changes jsonb DEFAULT '{}'::jsonb) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE value control.acquisition_scopes%ROWTYPE;
BEGIN
    SELECT (jsonb_populate_record(NULL::control.acquisition_scopes,
        to_jsonb(s)||jsonb_build_object('scope_id',pg_temp.id('d2'),
            'owner_resource_id',o.resource_id,'owner_epoch',o.epoch,'owner_incarnation',pg_temp.id('c2'))||changes)).*
    INTO STRICT value FROM control.acquisition_scopes s CROSS JOIN fixture_owner o
    ORDER BY s.visibility_ordinal,s.selector_id LIMIT 1;
    INSERT INTO control.acquisition_scopes(scope_id,dataset_id,endpoint_id,selector_id,
        access_scope_id,visibility_kind,visibility_domain,visibility_ordinal,
        policy_revision,config_revision,registered_at_us,owner_resource_id,owner_epoch,owner_incarnation)
    VALUES(value.scope_id,value.dataset_id,value.endpoint_id,value.selector_id,
        value.access_scope_id,value.visibility_kind,value.visibility_domain,value.visibility_ordinal,
        value.policy_revision,value.config_revision,value.registered_at_us,
        value.owner_resource_id,value.owner_epoch,value.owner_incarnation);
END $$;

\if :repeatable
-- Even a fully attributed owner below must use READ COMMITTED for registration.
\else
SELECT pg_temp.must_fail('CREATE TABLE control.forbidden_scope_table(x integer)','42501');
SELECT pg_temp.must_fail('SELECT pg_temp.selector()','23514');
SELECT pg_temp.must_fail('SELECT pg_temp.scope()','23514');
\endif
SELECT pg_advisory_lock(1111651442,1129599564);
\if :repeatable
\else
SELECT pg_temp.must_fail('SELECT pg_temp.selector()','23514');
SELECT pg_temp.must_fail('SELECT pg_temp.scope()','23514');
\endif
-- The retained collision owner has no live backend. Exclusion is real (the
-- advisory lock above); this fixture's explicit attribution never commits.
UPDATE control.owner_epochs SET state='released',
    released_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
    exclusion_method='storage_fenced',exclusion_digest=sha256(convert_to('rolled-back-scope-fixture','UTF8'))
WHERE role='control' AND state<>'released';
INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us)
    SELECT pg_temp.id('c1'),dataset_id,'control',1 FROM fixture_owner;
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
    process_incarnation,acquired_at_us,lease_expires_at_us,state,control_fence_version,
    control_backend_pid,control_backend_start)
SELECT resource_id,epoch,dataset_id,'control',pg_temp.id('c1'),pg_temp.id('c2'),1,
    (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint+30000000,
    'active',1,pg_backend_pid(),(SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid())
FROM fixture_owner;

\if :repeatable
SELECT pg_temp.must_fail('SELECT pg_temp.selector()','25001');
SELECT pg_temp.must_fail('SELECT pg_temp.scope()','25001');
\else
SELECT pg_temp.must_fail('UPDATE control.acquisition_selectors SET registered_at_us=registered_at_us+1','23514');
SELECT pg_temp.must_fail('DELETE FROM control.acquisition_selectors','23514');
SELECT pg_temp.must_fail('TRUNCATE control.acquisition_selectors CASCADE','23514');
SELECT pg_temp.must_fail('UPDATE control.acquisition_scopes SET registered_at_us=registered_at_us+1','23514');
SELECT pg_temp.must_fail('DELETE FROM control.acquisition_scopes','23514');
SELECT pg_temp.must_fail('TRUNCATE control.acquisition_scopes','23514');
SELECT pg_temp.must_fail('SELECT pg_temp.scope()','23505');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('scope_id',pg_temp.id('00')))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('dataset_id',pg_temp.id('ef')))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('owner_incarnation',pg_temp.id('ef')))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('policy_revision',2))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('config_revision',2))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('visibility_kind',3))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('visibility_ordinal',0))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('visibility_domain',pg_temp.id('ef')))$q$,'23503');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('visibility_ordinal',2))$q$,'23503');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('access_scope_id',pg_temp.id('ef')))$q$,'23503');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('selector_id',pg_temp.id('ef')))$q$,'23503');
SELECT pg_temp.must_fail($q$SELECT pg_temp.scope(jsonb_build_object('endpoint_id',pg_temp.id('ef')))$q$,'23503');
\if :collision
SELECT pg_temp.must_fail($q$SELECT pg_temp.selector(jsonb_build_object('document',
    convert_to('{"version":1,"filters":[{"kinds":[777]}],"history_since":0,"ephemeral":"retain"}','UTF8')))$q$,'54000');
SELECT pg_temp.must_fail($q$SELECT pg_temp.selector(jsonb_build_object('document',
    (SELECT document FROM control.acquisition_selectors LIMIT 1)))$q$,'54000');
-- A full selector bucket still permits a new valid scope using an existing row.
SELECT pg_temp.scope(jsonb_build_object('access_scope_id',
    (SELECT access_scope_id FROM control.relay_access_contexts WHERE ordinal=2),'visibility_ordinal',2));
\else
SELECT pg_temp.must_fail($q$SELECT pg_temp.selector(jsonb_build_object('document',
    (SELECT document FROM control.acquisition_selectors LIMIT 1)))$q$,'23505');
SELECT pg_temp.must_fail($q$SELECT pg_temp.selector(jsonb_build_object('profile_version',2))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.selector(jsonb_build_object('selector_id',pg_temp.id('00')))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.selector(jsonb_build_object('policy_revision',2))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.selector(jsonb_build_object('document',decode('','hex')))$q$,'23514');
SELECT pg_temp.must_fail($q$SELECT pg_temp.selector(jsonb_build_object('document',convert_to(repeat('a',2097153),'UTF8')))$q$,'23514');
-- Exactly2MiB normalized JSON, with a decoded tag shorter than1MiB. Generated
-- independently from the frozen literal profile, never the product serializer.
SELECT pg_temp.selector(jsonb_build_object('document',convert_to(
    '{"version":1,"filters":[{"#t":["'||repeat(chr(92)||'n',1048538)||''||
    '"]}],"history_since":0,"ephemeral":"retain"}','UTF8')));
SELECT pg_temp.scope(jsonb_build_object('selector_id',pg_temp.id('d1')));
\endif
\endif
SELECT json_build_object('status','PASS','refusals',executed,
    'selectors',(SELECT count(*) FROM control.acquisition_selectors),
    'scopes',(SELECT count(*) FROM control.acquisition_scopes),
    'new_document_length',(SELECT octet_length(document) FROM control.acquisition_selectors WHERE selector_id=pg_temp.id('d1')),
    'new_document_sha256',(SELECT encode(sha256(document),'hex') FROM control.acquisition_selectors WHERE selector_id=pg_temp.id('d1')))
FROM fixture_counts;
ROLLBACK;
