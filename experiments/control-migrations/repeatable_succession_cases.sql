-- Independent SQL-shape fixture for0038, NEVER a native positive-proof fixture.
-- Run only on a disposable copy of a qualified fully retired legacy epoch2.
-- The caller installs0038 and provisions the SQL test role explicitly. It passes
-- resource_hex and expected_genesis_kind (1=activated,2=unactivated empty).
-- Both kinds require separate clones/invocations. No trigger is disabled, no
-- replica role is selected and no historical Control claim is manufactured.
-- The fresh Control below belongs to this actual backend. All new metadata is
-- SQL-only: no native handoff, input/effect proof, process adoption or writer is
-- claimed, even when the guarded registry shape succeeds.
\set ON_ERROR_STOP on
\pset pager off
\if :{?resource_hex}
\else
\echo 'required: resource_hex and expected_genesis_kind'
\quit 2
\endif
\if :{?expected_genesis_kind}
\else
\quit 2
\endif
SET statement_timeout='10s';
SET lock_timeout='2s';
SET idle_in_transaction_session_timeout='30s';
SET max_parallel_workers_per_gather=0;
CREATE TEMP TABLE fixture_context(resource bytea,genesis_kind integer);
INSERT INTO fixture_context VALUES(decode(:'resource_hex','hex'),:'expected_genesis_kind'::integer);
CREATE TEMP TABLE fixture_results(name text PRIMARY KEY,sqlstate text,message text,context text);
CREATE TEMP TABLE fixture_snapshots(name text PRIMARY KEY,rows jsonb);
CREATE TEMP TABLE fixture_terminals(epoch numeric PRIMARY KEY,at_us bigint,digest bytea,transaction_id xid8);

CREATE FUNCTION pg_temp.assert_true(label text,answer boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF answer IS DISTINCT FROM true THEN RAISE EXCEPTION 'fixture assertion failed: %',label; END IF;
    INSERT INTO fixture_results(name) VALUES(label);
END $$;
CREATE FUNCTION pg_temp.snapshot() RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE names text[]; relation text; rows jsonb; answer jsonb:='{}';
BEGIN
    SET LOCAL max_parallel_workers_per_gather=0;
    SELECT array_agg(tablename ORDER BY tablename) INTO names FROM pg_catalog.pg_tables WHERE schemaname='control';
    -- Full control schema, including all three new relations; no unchecked table
    -- projection. The owning legacy seed has72 relations before this migration.
    IF cardinality(names)<>75 OR NOT names@>ARRAY['ingest_successions','ingest_activations','ingest_terminal_transactions'] THEN
        RAISE EXCEPTION 'fixture control relation census differs';
    END IF;
    FOREACH relation IN ARRAY names LOOP
        EXECUTE format('SELECT coalesce(jsonb_agg(v ORDER BY v::text COLLATE "C"),''[]'') FROM '
            '(SELECT to_jsonb(t)||jsonb_build_object(''xmin'',xmin::text) v FROM control.%I t) q',relation) INTO rows;
        answer:=answer||jsonb_build_object(relation,rows);
    END LOOP;
    RETURN answer;
END $$;
CREATE FUNCTION pg_temp.expect_error(label text,expected text,expected_message text,command text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE before_rows jsonb:=pg_temp.snapshot(); state text; message text; context text;
BEGIN
    BEGIN
        EXECUTE command;
        SET CONSTRAINTS ALL IMMEDIATE;
        RAISE EXCEPTION 'unexpected fixture success' USING ERRCODE='ZX999';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS state=RETURNED_SQLSTATE,message=MESSAGE_TEXT,context=PG_EXCEPTION_CONTEXT;
    END;
    IF state IS DISTINCT FROM expected OR message IS DISTINCT FROM expected_message THEN
        RAISE EXCEPTION 'case % expected %/%, received %/%',label,expected,expected_message,state,message
            USING DETAIL=context;
    END IF;
    IF pg_temp.snapshot() IS DISTINCT FROM before_rows THEN RAISE EXCEPTION 'case % changed retained rows',label; END IF;
    INSERT INTO fixture_results VALUES(label,state,message,context);
END $$;
CREATE FUNCTION pg_temp.now_us() RETURNS bigint LANGUAGE sql VOLATILE AS $$
    SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint
$$;
CREATE FUNCTION pg_temp.u64(value numeric) RETURNS bytea LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE answer text:=''; n numeric:=value; i integer;
BEGIN
    FOR i IN 1..8 LOOP answer:=lpad(to_hex(mod(n,256)::integer),2,'0')||answer; n:=div(n,256); END LOOP;
    IF n<>0 OR value<0 OR trunc(value)<>value THEN RAISE EXCEPTION 'fixture integer overflow'; END IF;
    RETURN decode(answer,'hex');
END $$;

CREATE FUNCTION pg_temp.claim_control() RETURNS void LANGUAGE plpgsql AS $$
DECLARE old control.owner_epochs%ROWTYPE; dataset bytea; role_id bytea:=uuid_send(gen_random_uuid());
        incarnation bytea:=uuid_send(gen_random_uuid()); at_us bigint:=pg_temp.now_us();
BEGIN
    PERFORM control.retain_control_lock();
    SELECT dataset_id INTO STRICT dataset FROM control.initial_ingest_successions WHERE owner_resource_id=(SELECT resource FROM fixture_context);
    SELECT * INTO STRICT old FROM control.owner_epochs WHERE dataset_id=dataset AND role='control' ORDER BY epoch DESC LIMIT 1 FOR UPDATE;
    IF old.state<>'released' OR EXISTS(SELECT 1 FROM control.owner_epochs WHERE dataset_id=dataset AND role='control' AND state='active') THEN
        RAISE EXCEPTION 'fixture requires released prior actual Control';
    END IF;
    INSERT INTO control.role_instances VALUES(role_id,dataset,'control',at_us,NULL);
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
        acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
    VALUES(old.resource_id,old.epoch+1,dataset,'control',role_id,incarnation,at_us,at_us+60000000,'active',1,pg_backend_pid(),
        (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
END $$;
CREATE FUNCTION pg_temp.release_control() RETURNS void LANGUAGE plpgsql AS $$
DECLARE c control.owner_epochs%ROWTYPE; at_us bigint:=pg_temp.now_us();
BEGIN
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE role='control' AND state='active';
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    UPDATE control.owner_epochs SET state='released',released_at_us=at_us,exclusion_method='storage_fenced',
        exclusion_digest=sha256(c.process_incarnation) WHERE resource_id=c.resource_id AND epoch=c.epoch;
    UPDATE control.role_instances SET retired_at_us=at_us WHERE role_instance_id=c.role_instance_id;
END $$;

-- Independent byte construction uses literal offsets and the specified domains,
-- not production codec/closure helpers. The expected recipe is cross-checked
-- against the retained source digest by the production boundary, not copied from it.
CREATE FUNCTION pg_temp.prepare_next() RETURNS void LANGUAGE plpgsql AS $$
DECLARE resource bytea:=(SELECT context.resource FROM fixture_context context); previous bytea; target bytea; parent bytea; receipt bytea;
        legacy control.initial_ingest_successions%ROWTYPE; old control.producer_epochs%ROWTYPE;
        source control.source_streams%ROWTYPE; c control.owner_epochs%ROWTYPE; d control.datasets%ROWTYPE;
        incarnation bytea:=uuid_send(gen_random_uuid()); role_id bytea:=uuid_send(gen_random_uuid()); domain_id bytea;
        at_us bigint:=pg_temp.now_us(); e numeric; kind integer; topology numeric; k integer; cr bytea; pr bytea;
BEGIN
    SET CONSTRAINTS ALL DEFERRED;
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE role='control' AND state='active' FOR UPDATE;
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    SELECT * INTO STRICT d FROM control.datasets WHERE dataset_id=c.dataset_id FOR UPDATE;
    SELECT * INTO STRICT legacy FROM control.initial_ingest_successions WHERE owner_resource_id=resource;
    SELECT * INTO STRICT old FROM control.producer_epochs WHERE producer_slot_id=legacy.producer_slot_id ORDER BY producer_epoch DESC LIMIT 1;
    SELECT * INTO STRICT source FROM control.source_streams WHERE owner_resource_id=resource AND owner_epoch=old.producer_epoch;
    e:=old.producer_epoch+1; topology:=d.topology_generation+1;
    IF e=3 THEN previous:=legacy.target_manifest; parent:=legacy.handoff;
        kind:=CASE WHEN EXISTS(SELECT 1 FROM control.initial_ingest_activations WHERE owner_resource_id=resource) THEN 1 ELSE 2 END;
    ELSE SELECT target_manifest,handoff INTO STRICT previous,parent FROM control.ingest_successions WHERE owner_resource_id=resource AND target_epoch=e-1;
        kind:=CASE WHEN EXISTS(SELECT 1 FROM control.ingest_activations WHERE owner_resource_id=resource AND target_epoch=e-1) THEN 1 ELSE 2 END;
    END IF;
    target:=decode('01010003000100010001','hex')||substring(previous FROM 11 FOR 16)||pg_temp.u64(topology)
        ||substring(previous FROM 35 FOR 20)||pg_temp.u64(e)||incarnation||substring(previous FROM 79 FOR 16)
        ||pg_temp.u64(d.policy_revision)||decode('0109','hex');
    INSERT INTO control.role_instances VALUES(role_id,d.dataset_id,'ingest',at_us,NULL);
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state)
        VALUES(resource,e,d.dataset_id,'ingest',role_id,incarnation,at_us,at_us+60000000,'active');
    INSERT INTO control.topology_generations(dataset_id,generation,previous_generation,created_at_us)
        VALUES(d.dataset_id,topology,d.topology_generation,at_us);
    INSERT INTO control.source_streams(dataset_id,shard_id,owner_epoch,owner_resource_id,owner_incarnation,opened_at_us)
        VALUES(d.dataset_id,legacy.shard_id,e,resource,incarnation,at_us);
    FOR k IN 1..9 LOOP
        domain_id:=uuid_send(gen_random_uuid()); target:=target||decode(lpad(to_hex(k),4,'0'),'hex')||domain_id;
        INSERT INTO control.allocation_domains(dataset_id,dictionary_kind,allocation_domain,owner_resource_id,owner_epoch,owner_incarnation,created_at_us)
            VALUES(d.dataset_id,k,domain_id,resource,e,incarnation,at_us);
    END LOOP;
    INSERT INTO control.producer_epochs(producer_slot_id,producer_epoch,dataset_id,producer_role_instance_id,producer_role,producer_incarnation,
        owner_resource_id,owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us)
        VALUES(legacy.producer_slot_id,e,d.dataset_id,role_id,'ingest',incarnation,resource,e,incarnation,2,at_us);
    INSERT INTO control.topology_streams SELECT d.dataset_id,topology,legacy.shard_id,owner_epoch,
        CASE WHEN owner_epoch=e THEN NULL ELSE last_complete_sequence END FROM control.source_streams WHERE owner_resource_id=resource ORDER BY owner_epoch;
    UPDATE control.topology_generations SET sealed=true WHERE dataset_id=d.dataset_id AND generation=topology;
    UPDATE control.datasets SET topology_generation=topology WHERE dataset_id=d.dataset_id;
    receipt:=decode('020a','hex')||decode(lpad(to_hex(kind),2,'0'),'hex')||legacy.producer_slot_id||pg_temp.u64(e-1)
        ||pg_temp.u64(old.reserved_through_request)||pg_temp.u64(source.last_complete_sequence)||source.closure_digest||sha256(parent)
        ||decode('010a','hex')||previous||decode('010a','hex')||target;
    SELECT document_digest INTO STRICT cr FROM control.config_revisions WHERE dataset_id=d.dataset_id AND revision=d.config_revision;
    SELECT document_digest INTO STRICT pr FROM control.policy_revisions WHERE dataset_id=d.dataset_id AND revision=d.policy_revision;
    INSERT INTO control.ingest_successions(dataset_id,owner_resource_id,target_epoch,previous_epoch,terminal_kind,target_incarnation,target_role_instance_id,
        producer_slot_id,producer_epoch,shard_id,previous_topology,target_topology,issued_through,last_complete_sequence,closure_digest,
        previous_manifest,target_manifest,previous_manifest_sha256,target_manifest_sha256,handoff,handoff_sha256,parent_handoff_sha256,
        config_revision,policy_revision,config_digest,policy_digest,outstanding_request_limit,created_at_us,lease_expires_at_us,
        prepared_by_resource,prepared_by_epoch,prepared_by_incarnation)
    VALUES(d.dataset_id,resource,e,e-1,kind,incarnation,role_id,legacy.producer_slot_id,e,legacy.shard_id,d.topology_generation,topology,
        old.reserved_through_request,source.last_complete_sequence,source.closure_digest,previous,target,sha256(previous),sha256(target),receipt,sha256(receipt),sha256(parent),
        d.config_revision,d.policy_revision,cr,pr,2,at_us,at_us+60000000,c.resource_id,c.epoch,c.process_incarnation);
END $$;
CREATE FUNCTION pg_temp.activate_head() RETURNS void LANGUAGE plpgsql AS $$
DECLARE s control.ingest_successions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT s FROM control.ingest_successions ORDER BY target_epoch DESC LIMIT 1;
    INSERT INTO control.ingest_activations(dataset_id,owner_resource_id,target_epoch,target_incarnation,target_role_instance_id,producer_slot_id,
        producer_epoch,target_manifest_sha256,handoff_sha256,activated_by_resource,activated_by_epoch,activated_by_incarnation,activated_at_us)
    VALUES(s.dataset_id,s.owner_resource_id,s.target_epoch,s.target_incarnation,s.target_role_instance_id,s.producer_slot_id,s.target_epoch,
        sha256(s.target_manifest),sha256(s.handoff),s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation,0);
END $$;
CREATE FUNCTION pg_temp.zero_digest(empty_attempt boolean) RETURNS bytea LANGUAGE plpgsql AS $$
DECLARE s control.ingest_successions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT s FROM control.ingest_successions ORDER BY target_epoch DESC LIMIT 1;
    RETURN sha256(convert_to(CASE WHEN empty_attempt THEN 'bigbrotr/ingest-successor-empty-closure/1'
        ELSE 'bigbrotr/ingest-successor-source-closure/1' END,'UTF8')||decode('00','hex')||sha256(s.handoff)||sha256(s.target_manifest)
        ||s.producer_slot_id||pg_temp.u64(s.target_epoch)||decode(repeat('00',16),'hex'));
END $$;
CREATE FUNCTION pg_temp.close_zero_source(empty_attempt boolean) RETURNS void LANGUAGE plpgsql AS $$
DECLARE s control.ingest_successions%ROWTYPE; at_us bigint:=pg_temp.now_us(); digest bytea:=pg_temp.zero_digest(empty_attempt);
BEGIN
    SELECT * INTO STRICT s FROM control.ingest_successions ORDER BY target_epoch DESC LIMIT 1;
    UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=s.target_epoch;
    UPDATE control.source_streams SET state='closing' WHERE owner_resource_id=s.owner_resource_id AND owner_epoch=s.target_epoch;
    UPDATE control.source_streams SET state='closed',closed_at_us=at_us,last_complete_sequence=0,closure_digest=digest
        WHERE owner_resource_id=s.owner_resource_id AND owner_epoch=s.target_epoch;
END $$;
CREATE FUNCTION pg_temp.retire_head(empty_attempt boolean,change_empty_lease boolean DEFAULT false) RETURNS void LANGUAGE plpgsql AS $$
DECLARE s control.ingest_successions%ROWTYPE; at_us bigint; digest bytea:=pg_temp.zero_digest(empty_attempt);
BEGIN
    SELECT * INTO STRICT s FROM control.ingest_successions ORDER BY target_epoch DESC LIMIT 1;
    SELECT CASE WHEN empty_attempt THEN closed_at_us ELSE pg_temp.now_us() END INTO STRICT at_us
        FROM control.source_streams WHERE owner_resource_id=s.owner_resource_id AND owner_epoch=s.target_epoch;
    INSERT INTO fixture_terminals VALUES(s.target_epoch,at_us,digest,pg_current_xact_id());
    UPDATE control.producer_epochs SET state='retired',retired_at_us=at_us,closure_digest=digest
        WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=s.target_epoch;
    UPDATE control.allocation_domains SET state='closed',closed_at_us=at_us,closure_digest=digest
        WHERE owner_resource_id=s.owner_resource_id AND owner_epoch=s.target_epoch;
    UPDATE control.owner_epochs SET state='released',released_at_us=at_us,exclusion_method='storage_fenced',exclusion_digest=digest,
        lease_expires_at_us=lease_expires_at_us+CASE WHEN change_empty_lease THEN 1 ELSE 0 END
        WHERE resource_id=s.owner_resource_id AND epoch=s.target_epoch;
    UPDATE control.role_instances SET retired_at_us=at_us WHERE role_instance_id=s.target_role_instance_id;
END $$;

-- Independent full-row terminal delta. Only the exact12 activated or13 empty
-- target rows plus the one transaction-coordinate receipt may change. All75
-- tables, old attribution, JSON fields and retained xmin values remain compared.
CREATE FUNCTION pg_temp.assert_terminal(label text,before_name text,after_name text,e numeric,empty_attempt boolean)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE before_rows jsonb; after_rows jsonb; expected jsonb; before_table jsonb; changed_table jsonb;
        relation text; item jsonb; next_item jsonb; matches boolean; matched integer; wanted integer;
        spec jsonb; c jsonb; producer jsonb; fresh_xmin text; receipt jsonb; terminal fixture_terminals%ROWTYPE;
BEGIN
    SELECT rows INTO STRICT before_rows FROM fixture_snapshots WHERE name=before_name;
    SELECT rows INTO STRICT after_rows FROM fixture_snapshots WHERE name=after_name;
    SELECT * INTO STRICT terminal FROM fixture_terminals WHERE epoch=e;
    SELECT value INTO STRICT spec FROM jsonb_array_elements(before_rows->'ingest_successions') WHERE (value->>'target_epoch')::numeric=e;
    SELECT value INTO STRICT c FROM jsonb_array_elements(before_rows->'owner_epochs') WHERE value->>'role'='control' AND value->>'state'='active';
    SELECT value INTO STRICT producer FROM jsonb_array_elements(after_rows->'producer_epochs') WHERE (value->>'producer_epoch')::numeric=e;
    fresh_xmin:=producer->>'xmin'; expected:=before_rows;
    IF fresh_xmin IS NULL OR fresh_xmin!~'^[1-9][0-9]*$' OR EXISTS(
        SELECT 1 FROM jsonb_each(before_rows) t CROSS JOIN LATERAL jsonb_array_elements(t.value) r(value)
        WHERE r.value->>'xmin'=fresh_xmin) THEN RAISE EXCEPTION 'terminal delta did not use a fresh transaction'; END IF;
    FOREACH relation IN ARRAY ARRAY['producer_epochs','allocation_domains','owner_epochs','role_instances','source_streams'] LOOP
        before_table:=before_rows->relation; changed_table:='[]'; matched:=0;
        FOR item IN SELECT value FROM jsonb_array_elements(before_table) LOOP
            matches:=CASE relation
                WHEN 'producer_epochs' THEN item->'producer_slot_id'=spec->'producer_slot_id' AND (item->>'producer_epoch')::numeric=e
                WHEN 'owner_epochs' THEN item->'resource_id'=spec->'owner_resource_id' AND (item->>'epoch')::numeric=e
                WHEN 'role_instances' THEN item->'role_instance_id'=spec->'target_role_instance_id'
                ELSE item->'owner_resource_id'=spec->'owner_resource_id' AND (item->>'owner_epoch')::numeric=e END;
            IF relation='source_streams' AND NOT empty_attempt THEN matches:=false; END IF;
            next_item:=item;
            IF matches THEN
                matched:=matched+1;
                next_item:=item||jsonb_build_object('xmin',fresh_xmin);
                CASE relation
                    WHEN 'producer_epochs' THEN next_item:=next_item||jsonb_build_object('state','retired','retired_at_us',terminal.at_us,'closure_digest',terminal.digest);
                    WHEN 'allocation_domains' THEN next_item:=next_item||jsonb_build_object('state','closed','closed_at_us',terminal.at_us,'closure_digest',terminal.digest);
                    WHEN 'owner_epochs' THEN next_item:=next_item||jsonb_build_object('state','released','released_at_us',terminal.at_us,
                        'exclusion_method','storage_fenced','exclusion_digest',terminal.digest);
                    WHEN 'role_instances' THEN next_item:=next_item||jsonb_build_object('retired_at_us',terminal.at_us);
                    WHEN 'source_streams' THEN next_item:=next_item||jsonb_build_object('state','closed','closed_at_us',terminal.at_us,
                        'last_complete_sequence',0,'closure_digest',terminal.digest);
                END CASE;
            END IF;
            changed_table:=changed_table||jsonb_build_array(next_item);
        END LOOP;
        wanted:=CASE WHEN relation='allocation_domains' THEN 9 WHEN relation='source_streams' AND NOT empty_attempt THEN 0 ELSE 1 END;
        IF matched<>wanted THEN RAISE EXCEPTION 'terminal delta has wrong target census for %',relation; END IF;
        SELECT jsonb_agg(value ORDER BY value::text COLLATE "C") INTO changed_table FROM jsonb_array_elements(changed_table);
        expected:=jsonb_set(expected,ARRAY[relation],changed_table);
    END LOOP;
    receipt:=jsonb_build_object('owner_resource_id',spec->'owner_resource_id','target_epoch',e,
        'terminal_transaction',terminal.transaction_id,'control_resource',c->'resource_id','control_epoch',c->'epoch',
        'control_incarnation',c->'process_incarnation','xmin',fresh_xmin);
    SELECT jsonb_agg(value ORDER BY value::text COLLATE "C") INTO changed_table
        FROM jsonb_array_elements((before_rows->'ingest_terminal_transactions')||jsonb_build_array(receipt));
    expected:=jsonb_set(expected,ARRAY['ingest_terminal_transactions'],changed_table);
    PERFORM pg_temp.assert_true(label,expected=after_rows);
END $$;
CREATE FUNCTION pg_temp.queue_import(label text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE job bytea:=uuid_send(gen_random_uuid()); c control.owner_epochs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE role='control' AND state='active' FOR UPDATE;
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=c.dataset_id FOR UPDATE;
    INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,input_complete)
    SELECT job,dataset_id,uuid_send(gen_random_uuid()),convert_to(label,'UTF8'),sha256(decode('01','hex')),
        'import',config_revision,policy_revision,pg_temp.now_us(),true FROM control.datasets WHERE dataset_id=c.dataset_id;
    UPDATE control.jobs SET state='queued',job_revision=2,owner_resource_id=c.resource_id,owner_epoch=c.epoch,owner_incarnation=c.process_incarnation
        WHERE job_id=job;
END $$;
CREATE FUNCTION pg_temp.bad_genesis_creation() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE control.role_instances SET created_at_us=created_at_us+1 WHERE role_instance_id=
        (SELECT role_instance_id FROM control.owner_epochs WHERE role='ingest' AND epoch=1);
    PERFORM pg_temp.prepare_next();
END $$;
CREATE FUNCTION pg_temp.bad_empty_lease() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_temp.close_zero_source(true);
    PERFORM pg_temp.retire_head(true,true);
END $$;

SELECT pg_temp.assert_true('guards_origin',current_setting('session_replication_role')='origin'
    AND NOT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='control' AND t.tgenabled NOT IN('O','A'))
    AND NOT EXISTS(SELECT 1 FROM pg_constraint q JOIN pg_namespace n ON n.oid=q.connamespace WHERE n.nspname='control' AND NOT q.convalidated));
SELECT pg_temp.assert_true('selected_genesis',(SELECT genesis_kind IN(1,2) AND genesis_kind=
    CASE WHEN EXISTS(SELECT 1 FROM control.initial_ingest_activations WHERE owner_resource_id=resource) THEN 1 ELSE 2 END FROM fixture_context));
INSERT INTO fixture_snapshots VALUES('original',pg_temp.snapshot());
SELECT pg_temp.assert_true('actual_exclusive_control_session',pg_try_advisory_lock(1111651442,1129599564));
BEGIN;
SELECT pg_temp.claim_control();
COMMIT;
SELECT pg_temp.assert_true('terminal_genesis_flat',(SELECT (control.lock_ingest_lineage(resource,2)).epoch>0 FROM fixture_context));
SELECT pg_temp.assert_true('unsigned_full_width',control.ingest_lineage_u64(18446744073709551615)=decode('ffffffffffffffff','hex')
    AND control.ingest_lineage_u64(18446744073709551360)=decode('ffffffffffffff00','hex')
    AND control.ingest_lineage_u64(9223372036854775808)=decode('8000000000000000','hex')
    AND control.ingest_lineage_u64(0)=decode('0000000000000000','hex')
    AND pg_temp.u64(18446744073709551615)=decode('ffffffffffffffff','hex')
    AND control.ingest_lineage_uint(decode('8000000000000000','hex'),0,8)=9223372036854775808);
SELECT pg_temp.assert_true('manifest_coordinates_are_not_epoch_count',
    (SELECT m.topology=9223372036854775808 AND m.recovery=decode(repeat('00',16),'hex')
        FROM control.initial_ingest_successions s CROSS JOIN LATERAL control.ingest_lineage_manifest(
            overlay(overlay(s.target_manifest PLACING decode('8000000000000000','hex') FROM 27 FOR 8)
                PLACING decode(repeat('00',16),'hex') FROM 79 FOR 16)) m));
SELECT pg_temp.expect_error('unsigned_overflow','23514','invalid lineage u64','SELECT control.ingest_lineage_u64(18446744073709551616)');
SELECT pg_temp.expect_error('epoch256_before_history','23514','lineage metadata or membership profile exhausted',
    format('SELECT control.lock_ingest_lineage(%L::bytea,256)',(SELECT resource FROM fixture_context)));
SELECT pg_temp.expect_error('genesis_creation_before_anchor','23514','lineage prepared identities or creation times differ',
    'SELECT pg_temp.bad_genesis_creation()');
BEGIN;
SAVEPOINT preparation_subtransaction;
SELECT pg_temp.prepare_next();
SELECT pg_temp.assert_true('preparation_top_level_xid',(SELECT preparation_transaction=pg_current_xact_id() FROM control.ingest_successions WHERE target_epoch=3));
RELEASE SAVEPOINT preparation_subtransaction;
SELECT pg_temp.expect_error('prepare_activation_distinct_commit','23514','lineage activation requires previously committed preparation','SELECT pg_temp.activate_head()');
SET CONSTRAINTS ALL IMMEDIATE;
COMMIT;
INSERT INTO fixture_snapshots VALUES('prepared3',pg_temp.snapshot());
SELECT pg_temp.expect_error('terminal_receipt_requires_retirement','23514','lineage terminal transaction requires its retired producer',
    'INSERT INTO control.ingest_terminal_transactions SELECT owner_resource_id,target_epoch,pg_current_xact_id(),prepared_by_resource,prepared_by_epoch,prepared_by_incarnation FROM control.ingest_successions WHERE target_epoch=3');
SELECT pg_temp.expect_error('claim_before_activation','23514','lineage workflow claims require previously committed activation',
    'SELECT pg_temp.queue_import(''before-activation'')');
BEGIN;
SET CONSTRAINTS ALL IMMEDIATE;
SAVEPOINT activation_subtransaction;
SELECT pg_temp.activate_head();
SELECT pg_temp.assert_true('activation_top_level_xid',(SELECT activation_transaction=pg_current_xact_id() FROM control.ingest_activations WHERE target_epoch=3));
RELEASE SAVEPOINT activation_subtransaction;
SET CONSTRAINTS ALL IMMEDIATE;
SELECT pg_temp.expect_error('activation_control_fence','23514','lineage preparation/activation authority and target must remain unchanged through commit',
    'UPDATE control.owner_epochs SET lease_expires_at_us=lease_expires_at_us+1 WHERE role=''control'' AND state=''active''');
SELECT pg_temp.expect_error('activation_target_fence','23514','lineage preparation/activation authority and target must remain unchanged through commit',
    'UPDATE control.owner_epochs SET lease_expires_at_us=lease_expires_at_us+1 WHERE role=''ingest'' AND epoch=3');
SELECT pg_temp.expect_error('activation_admission_subtransaction','23514','lineage admission requires previously committed activation',
    'SELECT control.require_ingest_admission(producer_slot_id,target_epoch,dataset_id,target_manifest_sha256,activated_by_resource,activated_by_epoch,activated_by_incarnation) FROM control.ingest_activations WHERE target_epoch=3');
SELECT pg_temp.expect_error('claim_after_activation_savepoint_release','23514','lineage workflow claims require previously committed activation',
    'SELECT pg_temp.queue_import(''after-activation'')');
COMMIT;
INSERT INTO fixture_snapshots VALUES('active3',pg_temp.snapshot());
BEGIN;
SELECT pg_temp.release_control();
SELECT pg_temp.claim_control();
COMMIT;
BEGIN;
SELECT pg_temp.close_zero_source(false);
COMMIT;
INSERT INTO fixture_snapshots VALUES('closed3',pg_temp.snapshot());
BEGIN;
SELECT pg_temp.expect_error('partial_terminal','23514','lineage retirement requires exactly ready or complete terminal metadata',
    'UPDATE control.producer_epochs SET state=''retired'',retired_at_us=pg_temp.now_us(),closure_digest=pg_temp.zero_digest(false) WHERE producer_epoch=3');
SAVEPOINT retirement_subtransaction;
SELECT pg_temp.retire_head(false);
SELECT pg_temp.assert_true('retirement_top_level_xid',(SELECT terminal_transaction=pg_current_xact_id() FROM control.ingest_terminal_transactions WHERE target_epoch=3));
RELEASE SAVEPOINT retirement_subtransaction;
SET CONSTRAINTS ALL IMMEDIATE;
SELECT pg_temp.expect_error('terminal_role_early_constraints','23514','anchored lineage role permits only exact terminal retirement',
    'UPDATE control.role_instances SET created_at_us=created_at_us+1 WHERE role_instance_id=(SELECT target_role_instance_id FROM control.ingest_successions WHERE target_epoch=3)');
COMMIT;
INSERT INTO fixture_snapshots VALUES('retired3',pg_temp.snapshot());
SELECT pg_temp.assert_terminal('exact_activated_terminal_delta','closed3','retired3',3,false);
BEGIN;
SELECT pg_temp.prepare_next();
COMMIT;
INSERT INTO fixture_snapshots VALUES('prepared4',pg_temp.snapshot());
SELECT pg_temp.expect_error('empty_terminal_lease_rewrite','23514','empty lineage scheduling metadata differs from preparation',
    'SELECT pg_temp.bad_empty_lease()');
BEGIN;
SELECT pg_temp.close_zero_source(true);
SELECT pg_temp.retire_head(true);
SET CONSTRAINTS ALL IMMEDIATE;
COMMIT;
INSERT INTO fixture_snapshots VALUES('empty_terminal4',pg_temp.snapshot());
SELECT pg_temp.assert_terminal('exact_empty_terminal_delta','prepared4','empty_terminal4',4,true);
BEGIN;
SELECT pg_temp.prepare_next();
COMMIT;
BEGIN;
SELECT pg_temp.activate_head();
COMMIT;
INSERT INTO fixture_snapshots VALUES('active5',pg_temp.snapshot());

-- A genuine current raw relational import claim is sufficient for this SQL
-- discriminator; it is not authenticated import execution or logical closure.
BEGIN;
SELECT pg_temp.queue_import('lineage-history-fixture');
-- Unchanged ownership is ordinary progress, not acquisition of a new claim.
UPDATE control.jobs SET job_revision=job_revision+1 WHERE request_key=convert_to('lineage-history-fixture','UTF8');
COMMIT;
SELECT pg_temp.assert_true('historical_empty_after_future_claim',
    (SELECT control.require_ingest_successor(resource,5,'active')>0 FROM fixture_context));
SELECT pg_temp.expect_error('anchored_genesis_role1','23514','anchored lineage role permits only exact terminal retirement',
    'UPDATE control.role_instances SET created_at_us=created_at_us+1 WHERE role_instance_id=(SELECT role_instance_id FROM control.owner_epochs WHERE role=''ingest'' AND epoch=1)');
-- Activated legacy2 has its earlier0037 guard; empty legacy2 uses the new anchor.
SELECT pg_temp.expect_error('anchored_genesis_role2','23514',
    CASE WHEN (SELECT genesis_kind FROM fixture_context)=1 THEN 'activated target role permits only exact retirement'
        ELSE 'anchored lineage role permits only exact terminal retirement' END,
    'UPDATE control.role_instances SET created_at_us=created_at_us+1 WHERE role_instance_id=(SELECT role_instance_id FROM control.owner_epochs WHERE role=''ingest'' AND epoch=2)');
SELECT pg_temp.expect_error('terminal_role_delete','23514','anchored lineage role history cannot be deleted',
    'DELETE FROM control.role_instances WHERE role_instance_id=(SELECT target_role_instance_id FROM control.ingest_successions WHERE target_epoch=4)');
SELECT pg_temp.expect_error('domain_truncate','23514','anchored lineage registry history cannot be truncated','TRUNCATE control.allocation_domains');
SELECT pg_temp.expect_error('receipt_cascade_truncate','23514','lineage preparations are retained and immutable','TRUNCATE control.ingest_successions CASCADE');
INSERT INTO fixture_snapshots VALUES('final',pg_temp.snapshot());
BEGIN;
SELECT pg_temp.release_control();
COMMIT;
SELECT pg_temp.assert_true('actual_exclusive_control_released',pg_advisory_unlock(1111651442,1129599564));
INSERT INTO fixture_snapshots VALUES('released',pg_temp.snapshot());
-- No assertions identify native effects or claim an installed journey. Return
-- every complete snapshot and actual diagnostic for the external independent
-- replay. This clone intentionally retains the final raw import obligation.
SELECT jsonb_build_object('kind','SQL_ONLY_NO_NATIVE_PROOF','cases',(SELECT jsonb_agg(to_jsonb(r) ORDER BY name) FROM fixture_results r),
    'snapshots',(SELECT jsonb_object_agg(name,rows) FROM fixture_snapshots));
