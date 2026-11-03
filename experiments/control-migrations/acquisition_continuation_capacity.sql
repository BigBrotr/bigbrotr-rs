-- SQL typing/accounting regression on a NEW empty current-schema database.
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

CREATE TEMP TABLE workflow_capacity_fixture AS
SELECT profile,decode(repeat('11',16),'hex') AS dataset,
    decode(lpad(to_hex(1000+profile),32,'0'),'hex') AS slot,
    decode(lpad(to_hex(2000+profile),32,'0'),'hex') AS ingest,
    decode(lpad(to_hex(3000+profile),32,'0'),'hex') AS control_resource,
    decode(repeat('ab',32),'hex') AS digest
FROM generate_series(0,1) profile;
CREATE TEMP TABLE workflow_capacity_results(epoch integer,refused boolean);

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
SELECT decode(lpad(to_hex(e*10000+profile*1000+n),32,'0'),'hex'),dataset,
    decode(repeat('77',16),'hex'),decode(lpad(to_hex(e*10000+profile*1000+n),32,'0'),'hex'),digest,
    'acquisition',1,1,1,'queued',2,control_resource,e,
    decode(lpad(to_hex(4000+profile*10+e),32,'0'),'hex'),0,true,1
FROM workflow_capacity_fixture CROSS JOIN generate_series(1,3) e
    CROSS JOIN LATERAL generate_series(1,64+profile) n;
SET LOCAL session_replication_role=origin;

DO $exercise$
DECLARE e integer; wanted bytea; message text;
BEGIN
    SELECT slot INTO STRICT wanted FROM workflow_capacity_fixture WHERE profile=0;
    FOREACH e IN ARRAY ARRAY[1,2,3,1] LOOP
        PERFORM control.require_acquisition_workflow_capacity(wanted,e);
        INSERT INTO workflow_capacity_results VALUES(e,false);
    END LOOP;
    SELECT slot INTO STRICT wanted FROM workflow_capacity_fixture WHERE profile=1;
    FOREACH e IN ARRAY ARRAY[1,2,3,1] LOOP
        BEGIN
            PERFORM control.require_acquisition_workflow_capacity(wanted,e);
            RAISE EXCEPTION 'workflow65 unexpectedly accepted';
        EXCEPTION WHEN check_violation THEN
            GET STACKED DIAGNOSTICS message=MESSAGE_TEXT;
            IF message<>'closed producer acquisition workflow identities exceed64' THEN RAISE; END IF;
        END;
        INSERT INTO workflow_capacity_results VALUES(e,true);
    END LOOP;
END $exercise$;
SELECT json_build_object('scope','isolated_sql_typing_and_accounting_not_native_authority',
    'positives',(SELECT count(*) FROM workflow_capacity_results WHERE NOT refused),
    'refusals',(SELECT count(*) FROM workflow_capacity_results WHERE refused),
    'epoch_order',ARRAY[1,2,3,1],'backend_pid',pg_backend_pid(),
    'schema45_ledger_sha256',(SELECT encode(checksum,'hex') FROM control.schema_versions WHERE migration=45));
ROLLBACK;
