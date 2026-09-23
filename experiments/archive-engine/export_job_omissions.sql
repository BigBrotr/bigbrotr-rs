-- Isolated native acceptance fixture, never a product function. Runs before the
-- first real export audit on its actual fenced Control connection. Every trial
-- is rolled back; the complete trial must reach its explicit BB099 marker.
CREATE TABLE public.export_fixture_results(case_name text PRIMARY KEY, phase text NOT NULL, sqlstate text NOT NULL, message text NOT NULL);
GRANT SELECT,INSERT ON public.export_fixture_results TO bigbrotr_control;
CREATE FUNCTION public.export_fixture_state() RETURNS jsonb LANGUAGE sql AS $$
    SELECT jsonb_build_object(
        'commands',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY actor_id,request_key) FROM control.management_commands t),
        'audit',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY audit_id) FROM control.management_audit t),
        'jobs',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY job_id) FROM control.jobs t),
        'details',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY job_id) FROM control.export_jobs t),
        'bounds',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY job_id,shard_id,owner_epoch) FROM control.job_input_bounds t),
        'leases',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY lease_id) FROM control.snapshot_leases t),
        'pins',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY lease_id) FROM control.archive_snapshot_pins t),
        'claims',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY claim_id) FROM control.retention_claims t))
$$;
CREATE FUNCTION public.export_fixture_omissions() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE omitted text; phase text; code text; message text; baseline jsonb;
    actor bytea:=decode(repeat('10',16),'hex'); dataset bytea:=decode(repeat('01',16),'hex');
    wanted_key bytea:=decode(repeat('e3',16),'hex'); job bytea:=decode(repeat('e4',16),'hex');
    audit bytea:=decode(repeat('e5',16),'hex'); pin bytea:=decode(repeat('e6',16),'hex');
    catalog bytea:=decode(repeat('5a',16),'hex');
    -- Independently encoded with Python struct.pack/hashlib, not the SQL/Rust encoder.
    digest bytea:=decode('f77a1a027a868aaedc1b59b5a05eb2697eb7badd2e8a723f9f16f798c64799ef','hex');
    owner control.owner_epochs%ROWTYPE; account control.local_accounts%ROWTYPE;
    session control.browser_sessions%ROWTYPE; time_us bigint;
BEGIN
    IF pg_trigger_depth()>1 THEN RETURN NEW; END IF;
    IF NEW.action<>'job.export.accept' OR NEW.actor_id<>actor OR NEW.dataset_id<>dataset
        OR NEW.subject_id<>decode(repeat('e2',16),'hex') THEN RAISE EXCEPTION 'unexpected export omission command'; END IF;
    SELECT * INTO STRICT owner FROM control.owner_epochs WHERE role='control' AND state='active';
    SELECT * INTO STRICT account FROM control.local_accounts WHERE principal_id=actor AND dataset_id=dataset;
    SELECT * INTO STRICT session FROM control.browser_sessions WHERE principal_id=actor AND dataset_id=dataset AND revoked_at_us IS NULL;
    baseline:=public.export_fixture_state();
    FOREACH omitted IN ARRAY ARRAY['none','audit','command','job','bounds','lease','pin','claim','detail','seal','wrong_cut','wrong_digest','extra_pin'] LOOP
        BEGIN
            time_us:=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
            phase:='audit';
            IF omitted<>'audit' THEN
                INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision)
                    VALUES(audit,dataset,actor,'job.export.accept','accepted',time_us,1,wanted_key,1);
            END IF;
            phase:='command';
            IF omitted<>'command' THEN
                INSERT INTO control.management_commands(actor_id,request_key,dataset_id,action,request_digest,expected_config_revision,policy_revision,audit_id,accepted_at_us)
                    VALUES(actor,wanted_key,dataset,'job.export.accept',CASE WHEN omitted='wrong_digest' THEN decode(repeat('bb',32),'hex') ELSE digest END,1,1,audit,time_us);
            END IF;
            phase:='job';
            IF omitted<>'job' THEN
                INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,input_topology_generation,export_job_protocol)
                    VALUES(job,dataset,actor,wanted_key,digest,'export',1,1,time_us,0,1);
            END IF;
            phase:='bounds';
            IF omitted<>'bounds' THEN
                INSERT INTO control.job_input_bounds VALUES(job,dataset,0,0,1,CASE WHEN omitted='wrong_cut' THEN 9 ELSE 10 END);
            END IF;
            phase:='lease';
            IF omitted<>'lease' THEN
                INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,topology_generation,holder_kind,holder_role_instance_id,holder_role,holder_incarnation,acquired_at_us,updated_at_us,expires_at_us,archive_pin_protocol)
                    VALUES(pin,dataset,catalog,1,0,'role_instance',owner.role_instance_id,'control',owner.process_incarnation,time_us,time_us,least(time_us+10000000,owner.lease_expires_at_us),1);
            END IF;
            phase:='pin';
            IF omitted<>'pin' THEN
                INSERT INTO control.archive_snapshot_pins(lease_id,dataset_id,consumer_id,frontier_revision,issuer_resource,issuer_epoch,issuer_incarnation,purpose,requested_ttl_us,export_job_id)
                    VALUES(pin,dataset,catalog,1,owner.resource_id,owner.epoch,owner.process_incarnation,'export',10000000,job);
            END IF;
            phase:='claim';
            IF omitted<>'claim' THEN
                INSERT INTO control.retention_claims(claim_id,lease_id,dataset_id,topology_generation,shard_id,owner_epoch,first_sequence,last_sequence,purpose,created_at_us)
                    VALUES(decode(repeat('e7',16),'hex'),pin,dataset,0,0,1,1,10,'export',time_us);
            END IF;
            phase:='detail';
            IF omitted<>'detail' THEN
                INSERT INTO control.export_jobs(job_id,dataset_id,actor_id,request_key,format,visibility,dictionary_kind,allocation_domain,ordinal,event_id,page_size,catalog_id,frontier_revision,destination_id,destination_binding_sha256,pin_lease_id,allocation_pin_id,session_id,actor_credential_revision,actor_authentication_revision,actor_grant_revision)
                    VALUES(job,dataset,actor,wanted_key,'signed-events-jsonl','private',4,decode(repeat('04',16),'hex'),1,decode(repeat('ab',32),'hex'),100,catalog,1,decode(repeat('e1',16),'hex'),decode(repeat('ee',32),'hex'),pin,decode(repeat('f7',16),'hex'),session.session_id,account.credential_revision,account.authentication_revision,account.grant_revision);
            END IF;
            phase:='extra_pin';
            IF omitted='extra_pin' THEN
                INSERT INTO control.snapshot_leases(lease_id,dataset_id,consumer_id,frontier_revision,topology_generation,holder_kind,holder_role_instance_id,holder_role,holder_incarnation,acquired_at_us,updated_at_us,expires_at_us,archive_pin_protocol)
                    VALUES(decode(repeat('e8',16),'hex'),dataset,catalog,1,0,'role_instance',owner.role_instance_id,'control',owner.process_incarnation,time_us,time_us,least(time_us+10000000,owner.lease_expires_at_us),1);
                INSERT INTO control.archive_snapshot_pins(lease_id,dataset_id,consumer_id,frontier_revision,issuer_resource,issuer_epoch,issuer_incarnation,purpose,requested_ttl_us,export_job_id)
                    VALUES(decode(repeat('e8',16),'hex'),dataset,catalog,1,owner.resource_id,owner.epoch,owner.process_incarnation,'export',10000000,job);
            END IF;
            phase:='seal';
            IF omitted<>'seal' THEN UPDATE control.jobs SET input_complete=true WHERE job_id=job; END IF;
            phase:='constraints';
            SET CONSTRAINTS ALL IMMEDIATE;
            phase:='positive-rollback';
            RAISE EXCEPTION 'fixture complete export rollback' USING ERRCODE='BB099';
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
        END;
        IF omitted='none' THEN
            IF (phase,code,message) IS DISTINCT FROM ('positive-rollback','BB099','fixture complete export rollback') THEN
                RAISE EXCEPTION 'positive export fixture failed: % % %',phase,code,message;
            END IF;
        ELSIF NOT ((omitted='audit' AND phase='command' AND code='23503')
            OR (omitted='command' AND phase='detail' AND code='23503')
            OR (omitted='job' AND phase='bounds' AND code='P0002')
            OR (omitted='lease' AND phase='pin' AND code='P0002')
            OR (omitted='pin' AND phase='claim' AND code='P0002')
            OR (omitted='extra_pin' AND phase='extra_pin' AND code='23505')
            OR (omitted IN ('bounds','claim','detail','seal','wrong_cut','wrong_digest') AND phase='constraints' AND code='23514')) THEN
            RAISE EXCEPTION 'export omission % failed elsewhere: % % %',omitted,phase,code,message;
        END IF;
        IF public.export_fixture_state() IS DISTINCT FROM baseline THEN
            RAISE EXCEPTION 'export omission % changed rows or xmin after rollback',omitted;
        END IF;
        INSERT INTO public.export_fixture_results VALUES(omitted,phase,code,message);
    END LOOP;
    RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.export_fixture_state(),public.export_fixture_omissions() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.export_fixture_state(),public.export_fixture_omissions() TO bigbrotr_control;
CREATE TRIGGER a_export_omissions BEFORE INSERT ON control.management_audit
    FOR EACH ROW WHEN (NEW.action='job.export.accept') EXECUTE FUNCTION public.export_fixture_omissions();
