-- Independent synthetic constraints, after the actual bootstrap/retry fixture.
-- Run after event-bindings, with the real Control/native client already stopped.
\set ON_ERROR_STOP on
\pset pager off
SET statement_timeout='10s';
SET lock_timeout='2s';
CREATE FUNCTION pg_temp.id(n integer) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$
 SELECT decode(lpad(to_hex(n),32,'0'),'hex')
$$;
CREATE FUNCTION pg_temp.state() RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE result jsonb := '{}'::jsonb; name text; rows jsonb;
BEGIN
 FOR name IN SELECT tablename FROM pg_tables WHERE schemaname='control' ORDER BY tablename LOOP
  EXECUTE format('SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text COLLATE "C"),''[]''::jsonb) FROM control.%I t',name) INTO rows;
  result := result || jsonb_build_object(name,rows);
 END LOOP;
 RETURN result;
END $$;
CREATE TEMP TABLE results(label text PRIMARY KEY, sqlstate text);
CREATE FUNCTION pg_temp.check(label text, value boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 IF value IS DISTINCT FROM true THEN RAISE EXCEPTION 'failed: %',label; END IF;
 INSERT INTO results VALUES(label,NULL);
END $$;
CREATE FUNCTION pg_temp.refuse(label text, wanted text, command text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE before_rows jsonb:=pg_temp.state(); actual text;
BEGIN
 BEGIN EXECUTE command; SET CONSTRAINTS ALL IMMEDIATE;
 EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS actual=RETURNED_SQLSTATE; END;
 IF actual IS DISTINCT FROM wanted OR before_rows IS DISTINCT FROM pg_temp.state() THEN
  RAISE EXCEPTION 'failed: %, expected %, actual %, retained equal %',label,wanted,actual,before_rows=pg_temp.state();
 END IF;
 INSERT INTO results VALUES(label,actual);
END $$;
SELECT pg_temp.check('nonempty_native_fixture',
 (SELECT count(*)=3 FROM control.producer_requests) AND
 (SELECT count(*)=2 FROM control.producer_input_bindings) AND
 (SELECT reserved_through_request=3 FROM control.producer_epochs));
-- A new real PostgreSQL owner backend, not a forged old backend identity.
SELECT pg_advisory_lock(1111651442,1129599564);
BEGIN;
SELECT control.retain_control_lock();
INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us)
 SELECT pg_temp.id(225),dataset_id,'control',0 FROM control.datasets;
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,
 process_incarnation,acquired_at_us,lease_expires_at_us,state,control_fence_version,
 control_backend_pid,control_backend_start)
 SELECT resource_id,epoch+1,dataset_id,'control',pg_temp.id(225),pg_temp.id(226),
 (extract(epoch FROM clock_timestamp())*1000000)::bigint,
 (extract(epoch FROM clock_timestamp())*1000000)::bigint+60000000,'active',1,
 pg_backend_pid(),(SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid())
 FROM control.owner_epochs WHERE role='control' ORDER BY epoch DESC LIMIT 1;
COMMIT;
CREATE FUNCTION pg_temp.producer(n integer, ceiling integer) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO control.producer_slots(producer_slot_id,dataset_id,registered_by_resource_id,
 registered_by_epoch,registered_by_incarnation,created_at_us)
 SELECT pg_temp.id(n),dataset_id,resource_id,epoch,process_incarnation,acquired_at_us
 FROM control.owner_epochs WHERE role='control' AND state='active';
 INSERT INTO control.producer_epochs(producer_slot_id,producer_epoch,dataset_id,
 producer_role_instance_id,producer_role,producer_incarnation,owner_resource_id,
 owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us)
 SELECT pg_temp.id(n),1,dataset_id,producer_role_instance_id,producer_role,
 producer_incarnation,owner_resource_id,owner_epoch,owner_incarnation,ceiling,opened_at_us
 FROM control.producer_epochs WHERE producer_slot_id=decode(repeat('42',16),'hex');
END $$;
CREATE FUNCTION pg_temp.reserve(n integer, seq integer, bytes integer DEFAULT 300,
 context bytea DEFAULT decode(repeat('01',32),'hex')) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO control.producer_requests(producer_slot_id,producer_epoch,request_sequence,
 dataset_id,request_digest,input_bytes,input_profile,manifest_sha256,issuer_resource,
 issuer_epoch,issuer_incarnation,reserved_at_us)
 SELECT pg_temp.id(n),1,seq,dataset_id,decode(repeat('02',32),'hex'),bytes,1,context,
 resource_id,epoch,process_incarnation,acquired_at_us FROM control.owner_epochs
 WHERE role='control' AND state='active';
END $$;
CREATE FUNCTION pg_temp.job(n integer) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 PERFORM pg_temp.producer(n,64);
 INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,
 config_revision,policy_revision,created_at_us,input_complete)
 SELECT pg_temp.id(n),dataset_id,principal_id,pg_temp.id(n),decode(repeat('04',32),'hex'),
 'import',1,1,0,true FROM control.principals WHERE kind='installation';
 UPDATE control.jobs j SET state='queued',job_revision=2,owner_resource_id=o.resource_id,
 owner_epoch=o.epoch,owner_incarnation=o.process_incarnation FROM control.owner_epochs o
 WHERE j.job_id=pg_temp.id(n) AND o.role='control' AND o.state='active';
 UPDATE control.jobs SET state='running',job_revision=3 WHERE job_id=pg_temp.id(n);
END $$;
CREATE FUNCTION pg_temp.bind(n integer, seq integer, first_line integer, total integer,
 ordinal integer, start_byte integer, end_byte integer, slot bytea DEFAULT NULL,
 source bytea DEFAULT decode(repeat('01',32),'hex'), profile smallint DEFAULT 1,
 namespace bytea DEFAULT decode(repeat('03',32),'hex'), kind smallint DEFAULT 1)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO control.producer_input_bindings(job_id,dataset_id,source_namespace,shard_id,
 source_bytes,source_sha256,parser_profile,batch_first_record,batch_records,obligation_index,
 obligation_kind,logical_record,byte_start,byte_end,record_sha256,producer_slot_id,producer_epoch,
 request_sequence,request_digest,input_bytes,input_profile,manifest_sha256,
 issuer_resource,issuer_epoch,issuer_incarnation)
 SELECT pg_temp.id(n),dataset_id,namespace,0,1000,source,profile,first_line,total,ordinal,kind,
 first_line+ordinal,start_byte,end_byte,decode(repeat('04',32),'hex'),coalesce(slot,pg_temp.id(n)),
 1,seq,decode(repeat('02',32),'hex'),300,1,decode(repeat('01',32),'hex'),resource_id,epoch,
 process_incarnation FROM control.owner_epochs WHERE role='control' AND state='active';
END $$;
CREATE FUNCTION pg_temp.single(n integer, seq integer, line integer, start_byte integer, end_byte integer)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 PERFORM pg_temp.bind(n,seq,line,1,0,start_byte,end_byte);
 PERFORM pg_temp.reserve(n,seq);
END $$;
SELECT pg_temp.job(1001);
SELECT pg_temp.refuse('missing_batch_member','23514','SELECT pg_temp.bind(1001,1,1,2,0,0,353); SELECT pg_temp.reserve(1001,1)');
SELECT pg_temp.refuse('missing_original_reservation','23503','SELECT pg_temp.bind(1001,1,1,1,0,0,353)');
SELECT pg_temp.refuse('different_original_input_bytes','23514','SELECT pg_temp.bind(1001,1,1,1,0,0,353); SELECT pg_temp.reserve(1001,1,301)');
SELECT pg_temp.refuse('parser_profile_unknown','23514','SELECT pg_temp.bind(1001,1,1,1,0,0,353,profile=>2::smallint)');
SELECT pg_temp.refuse('unsupported_claim_is_not_event_only','23514','SELECT pg_temp.bind(1001,1,1,1,0,0,353,kind=>2::smallint)');
SELECT pg_temp.refuse('impossible_prior_delimiters','23514','SELECT pg_temp.bind(1001,1,2,1,0,0,353)');
SELECT pg_temp.refuse('late_bootstrap_binding_with_native_outcome','23514',$q$SELECT pg_temp.bind(1001,1,1,1,0,0,353,slot=>decode(repeat('42',16),'hex'))$q$);
SELECT pg_temp.reserve(1001,1);
SELECT pg_temp.refuse('late_binding_without_pg_outcome','23514','SELECT pg_temp.bind(1001,1,1,1,0,0,353)');
SELECT pg_temp.job(1002);
BEGIN;
SELECT pg_temp.bind(1002,1,1,2,0,0,353);
SELECT pg_temp.bind(1002,2,1,2,1,353,706);
SELECT pg_temp.reserve(1002,1);
SELECT pg_temp.reserve(1002,2);
COMMIT;
SELECT pg_temp.check('complete_two_obligation_batch',
 (SELECT count(*)=2 AND min(logical_record)=1 AND max(logical_record)=2
 FROM control.producer_input_bindings WHERE job_id=pg_temp.id(1002)));
SELECT pg_temp.refuse('update_immutable','23514','UPDATE control.producer_input_bindings SET byte_start=byte_start');
SELECT pg_temp.refuse('delete_immutable','23514','DELETE FROM control.producer_input_bindings');
SELECT pg_temp.refuse('truncate_refused','23514','TRUNCATE control.producer_input_bindings');
SELECT pg_temp.job(1003);
SELECT pg_temp.single(1003,1,1,0,353);
SELECT pg_temp.refuse('adjacent_gap_forward','23514','SELECT pg_temp.single(1003,2,2,354,706)');
SELECT pg_temp.refuse('adjacent_overlap_forward','23514','SELECT pg_temp.single(1003,2,2,352,706)');
SELECT pg_temp.refuse('changed_source','23514',$q$SELECT pg_temp.bind(1003,2,2,1,0,353,706,source=>decode(repeat('02',32),'hex'))$q$);
SELECT pg_temp.refuse('changed_namespace','23514',$q$SELECT pg_temp.bind(1003,2,2,1,0,353,706,namespace=>decode(repeat('02',32),'hex'))$q$);
SELECT pg_temp.single(1003,2,2,353,706);
SELECT pg_temp.check('adjacent_exact_forward', (SELECT count(*)=2 FROM control.producer_input_bindings WHERE job_id=pg_temp.id(1003)));
SELECT pg_temp.job(1004);
SELECT pg_temp.single(1004,1,2,354,706);
SELECT pg_temp.refuse('adjacent_gap_reverse','23514','SELECT pg_temp.single(1004,2,1,0,353)');
SELECT pg_temp.single(1004,2,1,0,354);
SELECT pg_temp.check('adjacent_exact_reverse', (SELECT count(*)=2 FROM control.producer_input_bindings WHERE job_id=pg_temp.id(1004)));
SELECT pg_temp.job(1005);
SELECT pg_temp.single(1005,1,1,0,100);
SELECT pg_temp.single(1005,2,3,200,300);
SELECT pg_temp.check('genuine_record_hole_is_unresolved',
 (SELECT count(*)=2 FROM control.producer_input_bindings WHERE job_id=pg_temp.id(1005))
 AND NOT EXISTS (SELECT 1 FROM control.producer_input_bindings WHERE job_id=pg_temp.id(1005) AND logical_record=2)
 AND NOT EXISTS (SELECT 1 FROM control.producer_checkpoints));
SELECT pg_temp.refuse('hole_fill_must_join_both_sides','23514','SELECT pg_temp.single(1005,3,2,100,199)');
SELECT pg_temp.single(1005,3,2,100,200);
SELECT pg_temp.job(1006);
SELECT pg_temp.refuse('whole_batch_rollback_after_first_reservation','23514',$q$
 SELECT pg_temp.bind(1006,1,1,2,0,0,353);
 SELECT pg_temp.bind(1006,2,1,2,1,353,706);
 SELECT pg_temp.reserve(1006,1); SELECT pg_temp.reserve(1006,2,301)$q$);
SELECT pg_temp.refuse('inconsistent_declared_count','23514',$q$
 SELECT pg_temp.bind(1006,1,1,2,0,0,353);
 SELECT pg_temp.bind(1006,2,1,3,1,353,706);
 SELECT pg_temp.reserve(1006,1); SELECT pg_temp.reserve(1006,2)$q$);
BEGIN;
DO $$ BEGIN
 FOR i IN 0..63 LOOP PERFORM pg_temp.bind(1006,i+1,1,64,i,i,i+1); END LOOP;
 FOR i IN 1..64 LOOP PERFORM pg_temp.reserve(1006,i); END LOOP;
END $$;
COMMIT;
SELECT pg_temp.check('exact_retained_ledger_limit',(SELECT count(*)=64 FROM control.producer_input_bindings WHERE job_id=pg_temp.id(1006)));
SELECT pg_temp.producer(1007,64);
SELECT pg_temp.refuse('another_producer_does_not_multiply_job_limit','23514','SELECT pg_temp.bind(1006,1,65,1,0,64,65,slot=>pg_temp.id(1007))');
UPDATE control.jobs SET state='paused',job_revision=job_revision+1,reason_code='operator_requested' WHERE job_id=pg_temp.id(1005);
SELECT pg_temp.refuse('paused_job_refuses_new_input','23514','SELECT pg_temp.single(1005,4,4,300,400)');
SELECT pg_temp.check('binding_never_advances_any_checkpoint',
 NOT EXISTS (SELECT 1 FROM control.producer_checkpoints)
 AND NOT EXISTS (SELECT 1 FROM control.jobs WHERE checkpoint_revision<>0));
SELECT jsonb_build_object('status','PASS','cases',(SELECT jsonb_agg(to_jsonb(t) ORDER BY label) FROM results t));
UPDATE control.owner_epochs SET state='released',released_at_us=(extract(epoch FROM clock_timestamp())*1000000)::bigint,
 exclusion_method='storage_fenced',exclusion_digest=decode(repeat('09',32),'hex') WHERE role='control' AND state='active';
SELECT pg_advisory_unlock_all();
