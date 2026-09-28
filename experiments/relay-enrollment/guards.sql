-- Actual Control login. These missing-detail/immutability cases deliberately do
-- not pretend to own the dead probe's Control fence. Live-owner omissions/races
-- require their separate adapter qualification.
BEGIN;
DO $$
DECLARE d control.id16; p control.id16; config control.u64; policy control.u64;
    audit control.id16:=decode(repeat('91',16),'hex'); key control.id16:=decode(repeat('92',16),'hex');
    refusals integer:=0; name text;
BEGIN
    SELECT dataset_id,config_revision,policy_revision INTO STRICT d,config,policy FROM control.datasets;
    SELECT principal_id INTO STRICT p FROM control.local_accounts WHERE username='alice';
    BEGIN
        INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision)
            VALUES(audit,d,p,'relay.enrollment.change','accepted',0,config,key,policy);
        SET CONSTRAINTS ALL IMMEDIATE;
        RAISE EXCEPTION 'orphan audit was accepted';
    EXCEPTION WHEN check_violation THEN
        IF SQLERRM<>'enrollment requires atomic detail' THEN RAISE; END IF;
        refusals:=refusals+1;
    END;
    BEGIN
        INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision)
            VALUES(audit,d,p,'relay.enrollment.change','accepted',0,config,key,policy);
        INSERT INTO control.management_commands(actor_id,request_key,dataset_id,action,request_digest,
            expected_config_revision,policy_revision,audit_id,accepted_at_us)
            VALUES(p,key,d,'relay.enrollment.change',decode(repeat('00',32),'hex'),config,policy,audit,0);
        SET CONSTRAINTS ALL IMMEDIATE;
        RAISE EXCEPTION 'orphan command was accepted';
    EXCEPTION WHEN check_violation THEN
        IF SQLERRM<>'enrollment requires atomic detail' THEN RAISE; END IF;
        refusals:=refusals+1;
    END;
    FOREACH name IN ARRAY ARRAY['relay_access_contexts','network_routes','relay_enrollment_changes','relay_enrollments'] LOOP
        BEGIN
            EXECUTE format('DELETE FROM control.%I',name);
            RAISE EXCEPTION 'identity/history deletion was accepted';
        EXCEPTION WHEN check_violation THEN
            IF SQLERRM<>'immutable control history cannot be changed' THEN RAISE; END IF;
            refusals:=refusals+1;
        END;
        BEGIN
            EXECUTE format('TRUNCATE control.%I',name);
            RAISE EXCEPTION 'identity/history truncation was accepted';
        EXCEPTION WHEN check_violation OR feature_not_supported THEN
            -- Referenced tables may refuse TRUNCATE at the FK boundary first.
            refusals:=refusals+1;
        END;
    END LOOP;
    RAISE NOTICE 'expected enrollment refusals: %',refusals;
    IF refusals<>10 THEN RAISE EXCEPTION 'incomplete refusal execution'; END IF;
END $$;
ROLLBACK;
