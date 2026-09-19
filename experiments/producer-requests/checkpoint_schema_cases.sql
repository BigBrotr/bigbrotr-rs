-- Harness first executes only the reusable setup/functions prefix of
-- binding_schema_cases.sql, through the definition of pg_temp.single().
-- Outcomes below are explicitly synthetic SQL consistency inputs, not receipts
-- attested by RocksDB; the separate connected signed cases own that guarantee.
CREATE FUNCTION pg_temp.outcome(n integer, seq integer, kind smallint DEFAULT 1)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO control.producer_outcomes(producer_slot_id,producer_epoch,request_sequence,
 dataset_id,request_digest,manifest_sha256,outcome_kind,policy_epoch,outcome_bytes,
 outcome_sha256,recorder_resource,recorder_epoch,recorder_incarnation,recorded_at_us)
 SELECT r.producer_slot_id,r.producer_epoch,r.request_sequence,r.dataset_id,r.request_digest,
 r.manifest_sha256,kind,1,decode('010203','hex'),sha256(decode('010203','hex')),
 o.resource_id,o.epoch,o.process_incarnation,o.acquired_at_us FROM control.producer_requests r
 JOIN control.owner_epochs o ON o.dataset_id=r.dataset_id AND o.role='control' AND o.state='active'
 WHERE r.producer_slot_id=pg_temp.id(n) AND r.request_sequence=seq;
END $$;
CREATE FUNCTION pg_temp.close_record(n integer, seq integer, wanted_hash bytea DEFAULT NULL)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO control.import_record_closures(job_id,dataset_id,shard_id,logical_record,
 producer_slot_id,producer_epoch,request_sequence,outcome_sha256,recorder_resource,
 recorder_epoch,recorder_incarnation,recorded_at_us)
 SELECT b.job_id,b.dataset_id,b.shard_id,b.logical_record,b.producer_slot_id,b.producer_epoch,
 b.request_sequence,coalesce(wanted_hash,sha256(decode('010203','hex'))),o.resource_id,
 o.epoch,o.process_incarnation,o.acquired_at_us FROM control.producer_input_bindings b
 JOIN control.owner_epochs o ON o.dataset_id=b.dataset_id AND o.role='control' AND o.state='active'
 WHERE b.producer_slot_id=pg_temp.id(n) AND b.request_sequence=seq
 ON CONFLICT (job_id,shard_id,logical_record) DO NOTHING;
END $$;
CREATE FUNCTION pg_temp.shard_record(n integer, seq integer, shard integer, line integer)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO control.producer_input_bindings(job_id,dataset_id,source_namespace,shard_id,
 source_bytes,source_sha256,parser_profile,batch_first_record,batch_records,obligation_index,
 obligation_kind,logical_record,byte_start,byte_end,record_sha256,producer_slot_id,producer_epoch,
 request_sequence,request_digest,input_bytes,input_profile,manifest_sha256,
 issuer_resource,issuer_epoch,issuer_incarnation)
 SELECT pg_temp.id(n),dataset_id,decode(repeat('03',32),'hex'),shard,1000,
 decode(repeat('01',32),'hex'),1,line,1,0,1,line,(line-1)*100,line*100,
 decode(repeat('04',32),'hex'),pg_temp.id(n),1,seq,decode(repeat('02',32),'hex'),300,1,
 decode(repeat('01',32),'hex'),resource_id,epoch,process_incarnation FROM control.owner_epochs
 WHERE role='control' AND state='active';
 PERFORM pg_temp.reserve(n,seq);
 PERFORM pg_temp.outcome(n,seq);
END $$;
SELECT pg_temp.job(2001);
SELECT pg_temp.single(2001,1,2,353,706);
SELECT pg_temp.refuse('closure_without_outcome','23514','SELECT pg_temp.close_record(2001,1)');
SELECT pg_temp.outcome(2001,1);
SELECT pg_temp.refuse('wrong_outcome_digest','23514',$q$SELECT pg_temp.close_record(2001,1,decode(repeat('ab',32),'hex'))$q$);
SELECT pg_temp.close_record(2001,1);
SELECT pg_temp.check('hole_preserves_zero',
 (SELECT count(*)=1 FROM control.import_record_closures WHERE job_id=pg_temp.id(2001))
 AND NOT EXISTS(SELECT 1 FROM control.import_checkpoints WHERE job_id=pg_temp.id(2001))
 AND (SELECT job_revision=3 AND checkpoint_revision=0 FROM control.jobs WHERE job_id=pg_temp.id(2001)));
SELECT pg_temp.single(2001,2,1,0,353);
SELECT pg_temp.outcome(2001,2,2::smallint);
SELECT pg_temp.close_record(2001,2);
SELECT pg_temp.check('gap_fill_one_revision',
 (SELECT revision=1 AND complete_through_record=2 AND byte_offset=706 FROM control.import_checkpoints WHERE job_id=pg_temp.id(2001))
 AND (SELECT job_revision=4 AND checkpoint_revision=1 FROM control.jobs WHERE job_id=pg_temp.id(2001)));
CREATE TEMP TABLE prior AS SELECT pg_temp.state() AS value;
SELECT pg_temp.close_record(2001,1);
SELECT pg_temp.close_record(2001,2);
SELECT pg_temp.check('identical_insert_no_rewrite',(SELECT value=pg_temp.state() FROM prior));
SELECT pg_temp.refuse('closure_immutable_update','23514','UPDATE control.import_record_closures SET recorded_at_us=recorded_at_us+1');
SELECT pg_temp.refuse('closure_immutable_delete','23514','DELETE FROM control.import_record_closures');
SELECT pg_temp.refuse('closure_truncate_fk','0A000','TRUNCATE control.import_record_closures');
SELECT pg_temp.refuse('closure_truncate_guard','23514','TRUNCATE control.import_record_closures,control.import_checkpoints');
SELECT pg_temp.refuse('retained_outcome_delete','23514','DELETE FROM control.producer_outcomes');
SELECT pg_temp.refuse('retained_outcome_truncate','23514','TRUNCATE control.producer_outcomes,control.import_record_closures,control.import_checkpoints');
SELECT pg_temp.refuse('head_delete','23514','DELETE FROM control.import_checkpoints');
SELECT pg_temp.refuse('head_noop_revision','23514','UPDATE control.import_checkpoints SET revision=revision+1');
SELECT pg_temp.refuse('forged_global_revision','23514','UPDATE control.jobs SET job_revision=job_revision+1,checkpoint_revision=checkpoint_revision+1 WHERE job_id=pg_temp.id(2001)');
SELECT pg_temp.refuse('head_without_contiguous_members','23514','UPDATE control.import_checkpoints SET revision=revision+1,complete_through_record=3,byte_offset=800 WHERE job_id=pg_temp.id(2001)');
SELECT pg_temp.refuse('head_wrong_byte_boundary','23514','INSERT INTO control.import_checkpoints SELECT job_id,shard_id,1,complete_through_record,byte_offset+1 FROM control.import_checkpoints WHERE job_id=pg_temp.id(2001)');
SELECT pg_temp.job(2002);
SELECT pg_temp.shard_record(2002,1,0,1);
SELECT pg_temp.shard_record(2002,2,1,1);
SELECT pg_temp.shard_record(2002,3,0,2);
SELECT pg_temp.close_record(2002,1);
SELECT pg_temp.check('first_shard_revision',
 (SELECT checkpoint_revision=1 AND job_revision=4 FROM control.jobs WHERE job_id=pg_temp.id(2002)));
SELECT pg_temp.close_record(2002,2);
SELECT pg_temp.check('second_shard_global_revision',
 (SELECT count(*)=2 AND sum(revision)=2 FROM control.import_checkpoints WHERE job_id=pg_temp.id(2002))
 AND (SELECT checkpoint_revision=2 AND job_revision=5 FROM control.jobs WHERE job_id=pg_temp.id(2002)));
SELECT pg_temp.close_record(2002,3);
SELECT pg_temp.check('local_revisions_remain_independent',
 (SELECT revision=2 AND complete_through_record=2 FROM control.import_checkpoints WHERE job_id=pg_temp.id(2002) AND shard_id=0)
 AND (SELECT revision=1 AND complete_through_record=1 FROM control.import_checkpoints WHERE job_id=pg_temp.id(2002) AND shard_id=1)
 AND (SELECT checkpoint_revision=3 AND job_revision=6 FROM control.jobs WHERE job_id=pg_temp.id(2002)));
SELECT pg_temp.job(2003);
SELECT pg_temp.single(2003,1,1,0,100);
SELECT pg_temp.outcome(2003,1,3::smallint);
SELECT pg_temp.refuse('rejected_is_not_event_closure','23514','SELECT pg_temp.close_record(2003,1)');
SELECT pg_temp.job(2004);
SELECT pg_temp.shard_record(2004,1,0,1);
SELECT pg_temp.shard_record(2004,2,0,2);
SELECT pg_temp.shard_record(2004,3,1,1);
-- One statement, all three memberships visible to AFTER ROW trigger queries.
INSERT INTO control.import_record_closures(job_id,dataset_id,shard_id,logical_record,
 producer_slot_id,producer_epoch,request_sequence,outcome_sha256,recorder_resource,
 recorder_epoch,recorder_incarnation,recorded_at_us)
 SELECT b.job_id,b.dataset_id,b.shard_id,b.logical_record,b.producer_slot_id,b.producer_epoch,
 b.request_sequence,sha256(decode('010203','hex')),o.resource_id,o.epoch,o.process_incarnation,
 o.acquired_at_us FROM control.producer_input_bindings b JOIN control.owner_epochs o
 ON o.dataset_id=b.dataset_id AND o.role='control' AND o.state='active'
 WHERE b.job_id=pg_temp.id(2004) ORDER BY b.request_sequence;
SELECT pg_temp.check('multirow_after_visibility',
 (SELECT revision=1 AND complete_through_record=2 FROM control.import_checkpoints WHERE job_id=pg_temp.id(2004) AND shard_id=0)
 AND (SELECT revision=1 AND complete_through_record=1 FROM control.import_checkpoints WHERE job_id=pg_temp.id(2004) AND shard_id=1)
 AND (SELECT checkpoint_revision=2 AND job_revision=5 FROM control.jobs WHERE job_id=pg_temp.id(2004)));
SELECT pg_temp.job(2005);
SELECT pg_temp.shard_record(2005,1,0,1);
UPDATE prior SET value=pg_temp.state();
BEGIN;
SELECT pg_temp.close_record(2005,1);
ROLLBACK;
SELECT pg_temp.check('closure_head_job_rollback',(SELECT value=pg_temp.state() FROM prior));
UPDATE control.jobs SET state='paused',reason_code='operator_requested',job_revision=job_revision+1 WHERE job_id=pg_temp.id(2005);
SELECT pg_temp.refuse('paused_job_closure_refused','23514','SELECT pg_temp.close_record(2005,1)');
SELECT pg_temp.check('producer_watermarks_unchanged',NOT EXISTS(SELECT 1 FROM control.producer_checkpoints));
SELECT jsonb_build_object('status','PASS','scope','synthetic SQL consistency only','cases',(SELECT jsonb_agg(to_jsonb(t) ORDER BY label) FROM results t));
UPDATE control.owner_epochs SET state='released',released_at_us=(extract(epoch FROM clock_timestamp())*1000000)::bigint,
 exclusion_method='storage_fenced',exclusion_digest=decode(repeat('09',32),'hex') WHERE role='control' AND state='active';
SELECT pg_advisory_unlock_all();
