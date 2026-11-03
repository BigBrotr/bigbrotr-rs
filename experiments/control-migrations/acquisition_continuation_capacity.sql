-- SQL typing/UNION accounting regression on a NEW empty current-schema database.
-- Synthetic metadata uses the same transaction-local replica seeding as the
-- linear_quantum_capacity.sql probe. It is NOT native or authority evidence.
-- The actual guard runs in origin mode, in one backend, with no DDL, plan reset
-- or replication-role change between epoch1, epoch2 and later-epoch calls.
-- The caller records the actual ledger/source hashes and any explicit function
-- override used before a rebuild. This fixture never rewrites a ledger receipt.
\set ON_ERROR_STOP on
\set VERBOSITY verbose
BEGIN;
CREATE TEMP TABLE workflow_capacity_ledger(expected bytea CHECK(octet_length(expected)=32));
INSERT INTO workflow_capacity_ledger VALUES(decode(:'expected_migration_45_sha256','hex'));
DO $guard$ BEGIN
    IF current_setting('server_version_num')<>'160015'
        OR current_setting('session_replication_role')<>'origin'
        OR (SELECT count(*) FROM control.schema_versions)<>61
        OR NOT EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=45
            AND checksum=(SELECT expected FROM workflow_capacity_ledger))
        OR EXISTS(SELECT 1 FROM control.datasets)
        OR EXISTS(SELECT 1 FROM control.producer_slots)
        OR EXISTS(SELECT 1 FROM control.producer_epochs)
        OR EXISTS(SELECT 1 FROM control.initial_ingest_activations)
        OR EXISTS(SELECT 1 FROM control.ingest_activations)
        OR EXISTS(SELECT 1 FROM control.jobs)
        OR EXISTS(SELECT 1 FROM control.acquisition_gaps)
        OR EXISTS(SELECT 1 FROM control.acquisition_continuations)
    THEN RAISE EXCEPTION 'fresh empty schema45 PostgreSQL16.15 required'; END IF;
END $guard$;

-- Literal memberships and expected outcomes are independent of the guard.
-- Empty ranges use0..-1. Each union65 case adds one new identity to exactly one
-- branch while every branch remains below65. The isolated65 cases also exercise
-- each branch's own LIMIT65. Finalized seals retain their old-source obligation
-- after the corresponding jobs have moved to another synthetic Control tuple.
CREATE TEMP TABLE workflow_capacity_cases(
    profile integer PRIMARY KEY,case_name text UNIQUE NOT NULL,refused boolean NOT NULL,
    job_count integer NOT NULL,claims integer NOT NULL,gap_first integer NOT NULL,gap_last integer NOT NULL,
    seal_first integer NOT NULL,seal_last integer NOT NULL,extra_branch text,finalized boolean NOT NULL);
INSERT INTO workflow_capacity_cases VALUES
    (0,'claims64',false,64,64,0,-1,0,-1,NULL,false),
    (1,'claims65',true,65,65,0,-1,0,-1,NULL,false),
    (2,'overlapping_union64',false,64,32,17,48,33,64,NULL,false),
    (3,'claim_adds_unique65',true,65,32,17,48,33,64,'claim',false),
    (4,'gap_adds_unique65',true,65,32,17,48,33,64,'gap',false),
    (5,'pending_seal_adds_unique65',true,65,32,17,48,33,64,'seal',false),
    (6,'three_full_branches_union64',false,64,64,1,64,1,64,NULL,false),
    (7,'gaps65',true,65,0,1,65,0,-1,NULL,false),
    (8,'pending_seals65',true,65,0,0,-1,1,65,NULL,false),
    (9,'finalized_seals64_after_owner_move',false,64,0,0,-1,1,64,NULL,true),
    (10,'finalized_seals65_after_owner_move',true,65,0,0,-1,1,65,NULL,true),
    (11,'predicate_exclusions_union64',false,74,32,17,48,33,64,'predicates',false);

CREATE TEMP TABLE workflow_capacity_fixture AS
SELECT c.*,decode(repeat('11',16),'hex') AS dataset,
    decode(lpad(to_hex(1000+profile),32,'0'),'hex') AS slot,
    decode(lpad(to_hex(2000+profile),32,'0'),'hex') AS ingest,
    decode(lpad(to_hex(3000+profile),32,'0'),'hex') AS control_resource,
    decode(repeat('ab',32),'hex') AS digest
FROM workflow_capacity_cases c;
CREATE TEMP TABLE workflow_capacity_results(case_name text,epoch integer,refused boolean);
CREATE TEMP TABLE workflow_capacity_members AS
SELECT f.*,e,n,decode(lpad(to_hex(e*100000+profile*1000+n),32,'0'),'hex') AS job
FROM workflow_capacity_fixture f CROSS JOIN generate_series(1,3) e
    CROSS JOIN LATERAL generate_series(1,job_count) n;

SET LOCAL session_replication_role=replica;
INSERT INTO control.producer_slots(producer_slot_id,dataset_id,registered_by_resource_id,
    registered_by_epoch,registered_by_incarnation,created_at_us)
SELECT slot,dataset,control_resource,1,
    decode(lpad(to_hex(4000+profile*10+1),32,'0'),'hex'),1 FROM workflow_capacity_fixture;
INSERT INTO control.producer_epochs(producer_slot_id,producer_epoch,dataset_id,
    producer_role_instance_id,producer_role,producer_incarnation,owner_resource_id,
    owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us)
SELECT slot,e,dataset,decode(lpad(to_hex(6000+profile*10+e),32,'0'),'hex'),'ingest',
    decode(lpad(to_hex(5000+profile*10+e),32,'0'),'hex'),ingest,e,
    decode(lpad(to_hex(5000+profile*10+e),32,'0'),'hex'),32,1
FROM workflow_capacity_fixture CROSS JOIN generate_series(1,3) e;
INSERT INTO control.initial_ingest_activations(dataset_id,owner_resource_id,target_epoch,
    target_incarnation,target_role_instance_id,producer_slot_id,producer_epoch,
    previous_manifest_sha256,target_manifest_sha256,handoff_sha256,prepared_by_resource,
    prepared_by_epoch,prepared_by_incarnation,activated_by_resource,activated_by_epoch,
    activated_by_incarnation,config_revision,policy_revision,config_digest,policy_digest,activated_at_us)
SELECT dataset,ingest,2,decode(lpad(to_hex(5000+profile*10+2),32,'0'),'hex'),
    decode(lpad(to_hex(6000+profile*10+2),32,'0'),'hex'),slot,2,digest,digest,digest,
    control_resource,2,decode(lpad(to_hex(4000+profile*10+2),32,'0'),'hex'),
    control_resource,2,decode(lpad(to_hex(4000+profile*10+2),32,'0'),'hex'),1,1,digest,digest,1
FROM workflow_capacity_fixture;
INSERT INTO control.ingest_activations(dataset_id,owner_resource_id,target_epoch,
    target_incarnation,target_role_instance_id,producer_slot_id,producer_epoch,
    target_manifest_sha256,handoff_sha256,activated_by_resource,activated_by_epoch,
    activated_by_incarnation,activated_at_us)
SELECT dataset,ingest,3,decode(lpad(to_hex(5000+profile*10+3),32,'0'),'hex'),
    decode(lpad(to_hex(6000+profile*10+3),32,'0'),'hex'),slot,3,digest,digest,
    control_resource,3,decode(lpad(to_hex(4000+profile*10+3),32,'0'),'hex'),1
FROM workflow_capacity_fixture;
INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,
    kind,config_revision,policy_revision,created_at_us,state,job_revision,
    owner_resource_id,owner_epoch,owner_incarnation,input_topology_generation,input_complete,acquisition_job_protocol)
SELECT job,CASE WHEN extra_branch='predicates' AND n=65 THEN decode(repeat('22',16),'hex') ELSE dataset END,
    decode(repeat('77',16),'hex'),job,digest,
    CASE WHEN extra_branch='predicates' AND n=66 THEN 'import' ELSE 'acquisition' END,1,1,1,'queued',
    CASE WHEN finalized THEN 3 ELSE 2 END,
    CASE WHEN extra_branch='predicates' AND n=70 THEN NULL
        WHEN n<=claims OR (extra_branch='claim' AND n=65) OR (extra_branch='predicates' AND n IN (65,66,68,69))
        THEN control_resource ELSE decode(repeat('99',16),'hex') END,
    CASE WHEN extra_branch='predicates' AND n=70 THEN NULL
        WHEN extra_branch='predicates' AND n=68 THEN e+50 ELSE e END,
    CASE WHEN extra_branch='predicates' AND n=70 THEN NULL
        WHEN extra_branch='predicates' AND n=69 THEN decode(repeat('98',16),'hex')
        ELSE decode(lpad(to_hex(4000+profile*10+e),32,'0'),'hex') END,0,true,
    CASE WHEN extra_branch='predicates' AND n=66 THEN NULL ELSE 1 END
FROM workflow_capacity_members;

INSERT INTO control.acquisition_gaps(job_id,dataset_id,producer_slot_id,producer_epoch,
    shard_id,source_owner_epoch,last_complete_sequence,source_closure_digest,previous_job_revision,
    previous_owner_resource,previous_owner_epoch,previous_owner_incarnation,
    issuer_resource,issuer_epoch,issuer_incarnation,abandoned_at_us,reason)
SELECT job,dataset,CASE WHEN extra_branch='predicates' AND n=71 THEN decode(repeat('97',16),'hex') ELSE slot END,
    CASE WHEN extra_branch='predicates' AND n=72 THEN 4 ELSE e END,0,
    CASE WHEN extra_branch='predicates' AND n=72 THEN 4 ELSE e END,0,digest,2,
    control_resource,e,decode(lpad(to_hex(4000+profile*10+e),32,'0'),'hex'),
    control_resource,e+10,decode(repeat('96',16),'hex'),1,
    CASE WHEN extra_branch='predicates' AND n=72 THEN 'ingest_successor_abandoned'
        WHEN e=1 THEN 'initial_producer_abandoned' WHEN e=2 THEN 'activated_initial_producer_abandoned'
        ELSE 'ingest_successor_abandoned' END
FROM workflow_capacity_members
WHERE n BETWEEN gap_first AND gap_last OR (extra_branch='gap' AND n=65)
    OR (extra_branch='predicates' AND n IN (71,72));

INSERT INTO control.acquisition_continuations(continuation_id,dataset_id,job_id,previous_job_revision,
    previous_owner_resource,previous_owner_epoch,previous_owner_incarnation,
    scope_id,enrollment_generation,route_id,producer_slot_id,producer_epoch,manifest_sha256,shard_id,
    source_sequence,source_digest,checkpoint_revision,complete_through_request,last_quantum_number,
    sealing_resource,sealing_epoch,sealing_incarnation,sealed_at_us,
    target_epoch,target_manifest_sha256,activation_sha256,adopting_resource,adopting_epoch,
    adopting_incarnation,adopted_job_revision,adopted_at_us,adoption_transaction)
SELECT job,dataset,job,2,control_resource,e,decode(lpad(to_hex(4000+profile*10+e),32,'0'),'hex'),
    decode(repeat('95',16),'hex'),1,decode(repeat('94',16),'hex'),
    CASE WHEN extra_branch='predicates' AND n=73 THEN decode(repeat('97',16),'hex') ELSE slot END,
    CASE WHEN extra_branch='predicates' AND n=74 THEN 4 ELSE e END,digest,0,0,digest,0,0,0,
    control_resource,e+10,decode(repeat('96',16),'hex'),1,
    CASE WHEN finalized THEN e+1 END,CASE WHEN finalized THEN digest END,CASE WHEN finalized THEN digest END,
    CASE WHEN finalized THEN decode(repeat('99',16),'hex') END,CASE WHEN finalized THEN e END,
    CASE WHEN finalized THEN decode(lpad(to_hex(4000+profile*10+e),32,'0'),'hex') END,
    CASE WHEN finalized THEN 3 END,CASE WHEN finalized THEN 2 END,CASE WHEN finalized THEN pg_current_xact_id() END
FROM workflow_capacity_members
WHERE n BETWEEN seal_first AND seal_last OR (extra_branch='seal' AND n=65)
    OR (extra_branch='predicates' AND n IN (73,74));
SET LOCAL session_replication_role=origin;

DO $exercise$
DECLARE e integer; c record; message text;
BEGIN
    FOR c IN SELECT * FROM workflow_capacity_fixture ORDER BY profile LOOP
        FOREACH e IN ARRAY ARRAY[1,2,3,1] LOOP
            IF current_setting('session_replication_role')<>'origin' THEN
                RAISE EXCEPTION 'actual capacity guard requires origin mode';
            END IF;
            IF c.refused THEN
                BEGIN
                    PERFORM control.require_acquisition_workflow_capacity(c.slot,e);
                    RAISE EXCEPTION 'workflow65 unexpectedly accepted: % epoch%',c.case_name,e;
                EXCEPTION WHEN check_violation THEN
                    GET STACKED DIAGNOSTICS message=MESSAGE_TEXT;
                    IF message<>'closed producer acquisition workflow identities exceed64' THEN RAISE; END IF;
                END;
            ELSE
                PERFORM control.require_acquisition_workflow_capacity(c.slot,e);
            END IF;
            INSERT INTO workflow_capacity_results VALUES(c.case_name,e,c.refused);
        END LOOP;
    END LOOP;
END $exercise$;
SELECT json_build_object('scope','isolated_sql_typing_and_accounting_not_native_authority',
    'positives',(SELECT count(*) FROM workflow_capacity_results WHERE NOT refused),
    'refusals',(SELECT count(*) FROM workflow_capacity_results WHERE refused),
    'case_counts',(SELECT json_object_agg(case_name,json_build_object('accepted',accepted,'refused',refused) ORDER BY case_name)
        FROM (SELECT case_name,count(*) FILTER(WHERE NOT refused) AS accepted,count(*) FILTER(WHERE refused) AS refused
            FROM workflow_capacity_results GROUP BY case_name) counts),
    'epoch_order',ARRAY[1,2,3,1],'backend_pid',pg_backend_pid(),
    'schema45_ledger_sha256',(SELECT encode(checksum,'hex') FROM control.schema_versions WHERE migration=45));
ROLLBACK;
