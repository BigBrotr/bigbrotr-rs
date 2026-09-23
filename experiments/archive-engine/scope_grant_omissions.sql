-- Isolated acceptance fixture, never a product migration or runtime function.
-- Runs on the actual fenced Control backend before its first real grant audit.
CREATE TABLE public.scope_fixture_results (
    case_name text PRIMARY KEY,
    phase text NOT NULL,
    sqlstate text NOT NULL,
    message text NOT NULL
);
GRANT SELECT,INSERT ON public.scope_fixture_results TO bigbrotr_control;

CREATE FUNCTION public.scope_fixture_state() RETURNS jsonb LANGUAGE sql AS $$
    SELECT jsonb_build_object(
        'commands',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY actor_id,request_key) FROM control.management_commands t),
        'details',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY actor_id,request_key) FROM control.scope_grant_changes t),
        'grants',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY ordinal) FROM control.scope_grants t),
        'audit',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY audit_id) FROM control.management_audit t),
        'proofs',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY proof_id) FROM control.reauthentication_proofs t),
        'accounts',(SELECT jsonb_agg(jsonb_build_array(to_jsonb(t),xmin::text) ORDER BY principal_id) FROM control.local_accounts t))
$$;
CREATE FUNCTION public.scope_fixture_omissions() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE omitted text; phase text; code text; message text; baseline jsonb;
    proof control.reauthentication_proofs%ROWTYPE;
    actor bytea := decode(repeat('10',16),'hex'); dataset bytea := decode(repeat('01',16),'hex');
    wanted_key bytea := decode(repeat('c1',16),'hex'); domain bytea := decode(repeat('04',16),'hex');
BEGIN
    IF pg_trigger_depth() > 1 THEN RETURN NEW; END IF;
    IF NEW.action <> 'scope-grant.change' OR NEW.actor_id <> actor OR NEW.dataset_id <> dataset
        OR NEW.subject_id <> wanted_key THEN RAISE EXCEPTION 'unexpected omission fixture command'; END IF;
    SELECT * INTO STRICT proof FROM control.reauthentication_proofs p
        WHERE p.actor_id=actor AND p.request_key=wanted_key;
    baseline := public.scope_fixture_state();
    FOREACH omitted IN ARRAY ARRAY['none','audit','command','detail','account','grant','proof'] LOOP
        BEGIN
            phase := 'audit';
            IF omitted <> 'audit' THEN
                INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision)
                    VALUES(NEW.audit_id,dataset,actor,'scope-grant.change','accepted',NEW.created_at_us,1,wanted_key,1);
            END IF;
            phase := 'command';
            IF omitted <> 'command' THEN
                INSERT INTO control.management_commands(actor_id,request_key,dataset_id,action,request_digest,expected_config_revision,policy_revision,audit_id,accepted_at_us)
                    VALUES(actor,wanted_key,dataset,'scope-grant.change',proof.request_digest,1,1,NEW.audit_id,NEW.created_at_us);
            END IF;
            phase := 'detail';
            IF omitted <> 'detail' THEN
                INSERT INTO control.scope_grant_changes(actor_id,request_key,dataset_id,principal_id,action,dictionary_kind,allocation_domain,ordinal,enabled,scope_generation,
                    expected_grant_revision,resulting_grant_revision,proof_id,session_id,actor_credential_revision,actor_authentication_revision,actor_grant_revision,
                    proof_expires_at_us,catalog_id,frontier_revision,pin_lease_id)
                    VALUES(actor,wanted_key,dataset,actor,'history.read',4,domain,1,true,1,1,2,proof.proof_id,proof.session_id,1,1,1,proof.expires_at_us,
                        decode(repeat('5a',16),'hex'),1,decode(repeat('f7',16),'hex'));
                IF NOT FOUND THEN RAISE EXCEPTION 'fixture detail was suppressed'; END IF;
            END IF;
            phase := 'account';
            IF omitted <> 'account' THEN
                UPDATE control.local_accounts SET grant_revision=2 WHERE principal_id=actor AND dataset_id=dataset;
                IF NOT FOUND THEN RAISE EXCEPTION 'fixture account was absent'; END IF;
            END IF;
            phase := 'grant';
            IF omitted <> 'grant' THEN
                INSERT INTO control.scope_grants(dataset_id,principal_id,action,dictionary_kind,allocation_domain,ordinal,enabled,generation,actor_id,request_key)
                    VALUES(dataset,actor,'history.read',4,domain,1,true,1,actor,wanted_key);
            END IF;
            phase := 'proof';
            IF omitted <> 'proof' THEN
                DELETE FROM control.reauthentication_proofs WHERE proof_id=proof.proof_id;
                IF NOT FOUND THEN RAISE EXCEPTION 'fixture proof was absent'; END IF;
            END IF;
            phase := 'constraints';
            SET CONSTRAINTS control.scope_command_complete, control.scope_audit_complete,
                control.scope_detail_complete, control.scope_current_complete IMMEDIATE;
            phase := 'positive-rollback';
            RAISE EXCEPTION 'fixture complete transition rollback' USING ERRCODE='BB099';
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS code=RETURNED_SQLSTATE, message=MESSAGE_TEXT;
        END;
        -- A successful complete control is deliberately undone. A failed
        -- omission must fail at the expected boundary, not permission/expiry.
        IF omitted='none' THEN
            IF (phase,code,message) IS DISTINCT FROM ('positive-rollback','BB099','fixture complete transition rollback') THEN
                RAISE EXCEPTION 'positive fixture failed: % % %',phase,code,message;
            END IF;
        ELSIF omitted='audit' THEN
            IF phase<>'command' OR code<>'23503' OR message NOT LIKE '%violates foreign key constraint%' THEN
                RAISE EXCEPTION 'audit omission failed elsewhere: % % %',phase,code,message;
            END IF;
        ELSIF omitted='command' THEN
            IF (phase,code,message) IS DISTINCT FROM ('detail','P0002','query returned no rows') THEN
                RAISE EXCEPTION 'command omission failed elsewhere: % % %',phase,code,message;
            END IF;
        ELSIF omitted='detail' THEN
            IF (phase,code,message) IS DISTINCT FROM ('grant','23514','scope grant requires its exact new accepted command') THEN
                RAISE EXCEPTION 'detail omission failed elsewhere: % % %',phase,code,message;
            END IF;
        ELSE
            IF (phase,code,message) IS DISTINCT FROM ('constraints','23514','scope grant requires atomic command, exact audit, revision, grant and proof consumption') THEN
                RAISE EXCEPTION 'deferred omission % failed elsewhere: % % %',omitted,phase,code,message;
            END IF;
        END IF;
        IF public.scope_fixture_state() IS DISTINCT FROM baseline THEN
            RAISE EXCEPTION 'omission fixture % rollback changed durable fields or xmin',omitted;
        END IF;
        INSERT INTO public.scope_fixture_results VALUES(omitted,phase,code,message);
    END LOOP;
    RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.scope_fixture_state(),public.scope_fixture_omissions() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.scope_fixture_state(),public.scope_fixture_omissions() TO bigbrotr_control;
CREATE TRIGGER a_scope_omissions BEFORE INSERT ON control.management_audit
    FOR EACH ROW WHEN (NEW.action='scope-grant.change') EXECUTE FUNCTION public.scope_fixture_omissions();
