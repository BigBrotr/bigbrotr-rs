-- Independent synthetic constraints, after the actual bootstrap/retry fixture.
-- Run on its isolated bbproducer_write database, after both Control drivers end.
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
 (SELECT count(*)=1 FROM control.producer_requests) AND
 (SELECT reserved_through_request=1 FROM control.producer_epochs));
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
SELECT pg_temp.producer(1001,2);
SELECT pg_temp.refuse('gap_before_first','23514','SELECT pg_temp.reserve(1001,2)');
SELECT pg_temp.refuse('counter_without_row','23514','UPDATE control.producer_epochs SET reserved_through_request=1 WHERE producer_slot_id=pg_temp.id(1001)');
SELECT pg_temp.refuse('whole_transaction_rollback','23514','SELECT pg_temp.reserve(1001,1); SELECT pg_temp.reserve(1001,2); SELECT pg_temp.reserve(1001,3)');
BEGIN;
SELECT pg_temp.reserve(1001,1);
SELECT pg_temp.reserve(1001,2);
COMMIT;
SELECT pg_temp.check('two_statements_advance_exact_counter',(SELECT reserved_through_request=2 FROM control.producer_epochs WHERE producer_slot_id=pg_temp.id(1001)));
SELECT pg_temp.refuse('window_full','23514','SELECT pg_temp.reserve(1001,3)');
SELECT pg_temp.refuse('duplicate_sql_not_a_retry','23514','SELECT pg_temp.reserve(1001,2)');
SELECT pg_temp.refuse('counter_cannot_rewind','23514','UPDATE control.producer_epochs SET reserved_through_request=1 WHERE producer_slot_id=pg_temp.id(1001)');
SELECT pg_temp.refuse('row_update_immutable','23514','UPDATE control.producer_requests SET input_bytes=input_bytes');
SELECT pg_temp.refuse('row_delete_refused','23514','DELETE FROM control.producer_requests');
-- Include the referencing outcome table so PostgreSQL reaches the retained-row
-- guards instead of rejecting the command earlier for its foreign key.
SELECT pg_temp.refuse('truncate_foreign_key','0A000','TRUNCATE control.producer_requests');
SELECT pg_temp.refuse('truncate_refused','23514','TRUNCATE control.producer_requests, control.producer_outcomes');
SELECT pg_temp.producer(1002,64);
SELECT pg_temp.refuse('same_epoch_bulk_atomic_refusal','23514',$q$
 INSERT INTO control.producer_requests(producer_slot_id,producer_epoch,request_sequence,dataset_id,
 request_digest,input_bytes,input_profile,manifest_sha256,issuer_resource,issuer_epoch,issuer_incarnation,reserved_at_us)
 SELECT pg_temp.id(1002),1,n,dataset_id,decode(repeat('02',32),'hex'),300,1,decode(repeat('01',32),'hex'),resource_id,epoch,process_incarnation,acquired_at_us
 FROM control.owner_epochs CROSS JOIN generate_series(1,2) n WHERE role='control' AND state='active' ORDER BY n$q$);
SELECT pg_temp.refuse('oversize_input','23514','SELECT pg_temp.reserve(1002,1,16777217)');
SELECT pg_temp.reserve(1002,1,16777216);
SELECT pg_temp.refuse('manifest_context_change','23514',$q$SELECT pg_temp.reserve(1002,2,1,decode(repeat('03',32),'hex'))$q$);
-- 0012 reserves 64KiB per outcome before either native dispatch.
SELECT pg_temp.reserve(1002,2,16646144);
SELECT pg_temp.refuse('retained_byte_limit','23514','SELECT pg_temp.reserve(1002,3,1)');
UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=pg_temp.id(1002);
SELECT pg_temp.refuse('unresolved_reservations_refuse_retirement','55000',$q$
 UPDATE control.producer_epochs SET state='retired',retired_at_us=opened_at_us,
 closure_digest=decode(repeat('04',32),'hex') WHERE producer_slot_id=pg_temp.id(1002)$q$);
SELECT pg_temp.producer(1003,64);
UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=pg_temp.id(1003);
-- Empty and below every quota: only the closed state can reject this request.
SELECT pg_temp.refuse('closing_epoch_refuses_reservation','23514','SELECT pg_temp.reserve(1003,1,1)');
UPDATE control.producer_epochs SET state='retired',retired_at_us=opened_at_us,
 closure_digest=decode(repeat('04',32),'hex') WHERE producer_slot_id=pg_temp.id(1003);
SELECT pg_temp.check('empty_epoch_can_retire',(SELECT state='retired' AND reserved_through_request=0 FROM control.producer_epochs WHERE producer_slot_id=pg_temp.id(1003)));
SELECT pg_temp.check('all_cases_executed',(SELECT count(*)=19 FROM results));
SELECT jsonb_build_object('status','PASS','cases',(SELECT jsonb_agg(to_jsonb(t) ORDER BY label) FROM results t));
-- End the actual session owner; this fixture grants no native successor.
UPDATE control.owner_epochs SET state='released',released_at_us=(extract(epoch FROM clock_timestamp())*1000000)::bigint,
 exclusion_method='storage_fenced',exclusion_digest=decode(repeat('09',32),'hex') WHERE role='control' AND state='active';
SELECT pg_advisory_unlock_all();
