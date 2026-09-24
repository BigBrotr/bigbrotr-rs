-- Same isolated database and connection: COMMIT the reservation before testing
-- execution. Literal file metadata is a schema oracle, not a claim of output I/O.
SELECT pg_advisory_lock(1111651442,1129599564);
BEGIN;
DO $$
DECLARE o control.owner_epochs%ROWTYPE; j control.jobs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT o FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1;
    IF o.state<>'released' THEN RAISE EXCEPTION 'fixture needs released Control'; END IF;
    INSERT INTO control.role_instances VALUES(decode(repeat('b2',16),'hex'),o.dataset_id,'control',1,NULL);
    INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,
        acquired_at_us,lease_expires_at_us,state,control_fence_version,control_backend_pid,control_backend_start)
        VALUES(o.resource_id,o.epoch+1,o.dataset_id,'control',decode(repeat('b2',16),'hex'),decode(repeat('b3',16),'hex'),
            (extract(epoch FROM clock_timestamp())*1000000)::bigint,
            (extract(epoch FROM clock_timestamp())*1000000)::bigint+120000000,'active',1,pg_backend_pid(),
            (SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()));
    SELECT * INTO STRICT j FROM control.jobs WHERE export_job_protocol=1;
    PERFORM control.require_export_execution_authority(j.job_id);
    UPDATE control.jobs SET state='queued',job_revision=job_revision+1,owner_resource_id=o.resource_id,
        owner_epoch=o.epoch+1,owner_incarnation=decode(repeat('b3',16),'hex') WHERE job_id=j.job_id;
    INSERT INTO control.export_execution_units(job_id,artifact_id,max_output_bytes,max_temporary_bytes)
        VALUES(j.job_id,decode(repeat('b1',16),'hex'),1048576,2097152);
END $$;
COMMIT;
BEGIN;
CREATE TEMP TABLE completion_trial_results(name text PRIMARY KEY,phase text,code text,message text);
DO $$
DECLARE o control.owner_epochs%ROWTYPE; j control.jobs%ROWTYPE; e control.export_jobs%ROWTYPE;
    u control.export_execution_units%ROWTYPE; mode text; phase text; code text; message text;
    pin bytea:=decode(repeat('b4',16),'hex'); time_us bigint; before_job jsonb;
BEGIN
    SELECT * INTO STRICT o FROM control.owner_epochs WHERE role='control' AND state='active';
    SELECT * INTO STRICT j FROM control.jobs WHERE export_job_protocol=1;
    SELECT * INTO STRICT e FROM control.export_jobs WHERE job_id=j.job_id;
    SELECT * INTO STRICT u FROM control.export_execution_units WHERE job_id=j.job_id;
    IF u.reservation_transaction=pg_current_xact_id() THEN RAISE EXCEPTION 'fixture failed to commit reservation'; END IF;
    before_job:=to_jsonb(j);
    FOREACH mode IN ARRAY ARRAY['complete','empty','artifact_missing','file_missing','checkpoint_missing',
        'checkpoint_without_artifact','success_without_artifact','manifest_mismatch','wrong_destination',
        'wrong_file','wrong_total','wrong_empty_digest','old_export_pin'] LOOP
        BEGIN
            PERFORM control.require_export_execution_authority(j.job_id);
            time_us:=(extract(epoch FROM clock_timestamp())*1000000)::bigint;
            phase:='reader';
            INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,topology_generation,
                holder_kind,holder_role_instance_id,holder_role,holder_incarnation,acquired_at_us,updated_at_us,expires_at_us,archive_pin_protocol)
                VALUES(pin,j.dataset_id,e.catalog_id,e.frontier_revision,j.input_topology_generation,'role_instance',
                    o.role_instance_id,'control',o.process_incarnation,time_us,time_us,time_us+60000000,1);
            INSERT INTO control.archive_snapshot_pins(lease_id,dataset_id,consumer_id,frontier_revision,issuer_resource,
                issuer_epoch,issuer_incarnation,purpose,requested_ttl_us)
                VALUES(pin,j.dataset_id,e.catalog_id,e.frontier_revision,o.resource_id,o.epoch,o.process_incarnation,'reader',60000000);
            INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,topology_generation,shard_id,owner_epoch,
                first_sequence,last_sequence,purpose,created_at_us)
                SELECT decode(md5('export completion fixture '||shard_id||':'||owner_epoch),'hex'),pin,j.dataset_id,
                    j.input_topology_generation,shard_id,owner_epoch,1,last_complete_sequence,'reader',time_us
                    FROM control.job_input_bounds WHERE job_id=j.job_id AND last_complete_sequence>0;
            phase:='running'; UPDATE control.jobs SET state='running',job_revision=job_revision+1 WHERE job_id=j.job_id;
            IF mode NOT IN ('artifact_missing','checkpoint_without_artifact','success_without_artifact') THEN
                phase:='artifact';
                INSERT INTO control.artifacts(artifact_id,job_id,dataset_id,principal_id,destination_id,
                    destination_binding_sha256,manifest_format,manifest_bytes,manifest_sha256,file_count,total_bytes,verification_pin_id,completed_at_us)
                    VALUES(u.artifact_id,j.job_id,j.dataset_id,j.principal_id,
                        CASE WHEN mode='wrong_destination' THEN decode(repeat('c1',16),'hex') ELSE e.destination_id END,
                        e.destination_binding_sha256,'bigbrotr-signed-events-export/1',1321,decode(repeat('cd',32),'hex'),1,
                        CASE WHEN mode IN ('empty','wrong_empty_digest') THEN 1321 WHEN mode='wrong_total' THEN 1656 ELSE 1655 END,
                        CASE WHEN mode='old_export_pin' THEN e.pin_lease_id ELSE pin END,time_us);
            END IF;
            IF mode NOT IN ('file_missing','artifact_missing','checkpoint_without_artifact','success_without_artifact') THEN
                phase:='file';
                INSERT INTO control.artifact_files VALUES(u.artifact_id,'events',
                    CASE WHEN mode='wrong_file' THEN 'different.jsonl' ELSE 'events.jsonl' END,'nostr-jsonl/1',
                    CASE WHEN mode IN ('empty','wrong_empty_digest') THEN 0 ELSE 334 END,
                    CASE WHEN mode IN ('empty','wrong_empty_digest') THEN 0 ELSE 1 END,
                    CASE WHEN mode='empty' THEN decode('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855','hex')
                        ELSE decode('2006c3bc3473e92f0bcf8e5b8905bc0784ce18e13b00b31ab095bfb469f0f06f','hex') END);
            END IF;
            IF mode='checkpoint_without_artifact' THEN
                phase:='job'; UPDATE control.jobs SET checkpoint_revision=1,job_revision=job_revision+1 WHERE job_id=j.job_id;
            ELSIF mode<>'checkpoint_missing' THEN
                phase:='job'; UPDATE control.jobs SET state='succeeded',checkpoint_revision=1,job_revision=job_revision+1,
                    result_manifest_digest=decode(repeat(CASE WHEN mode='manifest_mismatch' THEN 'ce' ELSE 'cd' END,32),'hex'),
                    completed_at_us=time_us WHERE job_id=j.job_id;
            END IF;
            phase:='constraints'; SET CONSTRAINTS ALL IMMEDIATE;
            phase:='positive-rollback'; RAISE EXCEPTION 'complete artifact rollback' USING ERRCODE='BB099';
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
        END;
        IF mode IN ('complete','empty') THEN
            IF (phase,code,message) IS DISTINCT FROM ('positive-rollback','BB099','complete artifact rollback') THEN
                RAISE EXCEPTION 'positive % failed: % % %',mode,phase,code,message;
            END IF;
        ELSIF phase<>'constraints' OR code<>CASE WHEN mode='old_export_pin' THEN 'BB001' ELSE '23514' END THEN
            RAISE EXCEPTION 'negative % failed elsewhere: % % %',mode,phase,code,message;
        END IF;
        IF (SELECT to_jsonb(t) FROM control.jobs t WHERE job_id=j.job_id) IS DISTINCT FROM before_job
            OR EXISTS(SELECT 1 FROM control.artifacts) OR EXISTS(SELECT 1 FROM control.artifact_files)
            OR EXISTS(SELECT 1 FROM control.snapshot_leases WHERE lease_id=pin) THEN RAISE EXCEPTION 'completion changed rows after rollback'; END IF;
        INSERT INTO completion_trial_results VALUES(mode,phase,code,message);
    END LOOP;
END $$;
SELECT json_agg(t ORDER BY name) FROM completion_trial_results t;
-- Only the reservation remains committed. Retain its original attribution and
-- export pin; no terminal recovery or reader release is inferred from rollback.
UPDATE control.owner_epochs SET state='released',released_at_us=(extract(epoch FROM clock_timestamp())*1000000)::bigint,
    exclusion_method='storage_fenced',exclusion_digest=decode(repeat('ef',32),'hex') WHERE role='control' AND state='active';
COMMIT;
SELECT pg_advisory_unlock(1111651442,1129599564);
