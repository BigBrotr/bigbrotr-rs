DO $$
BEGIN
    BEGIN
        UPDATE control.management_commands SET request_digest=decode(repeat('00',32),'hex');
        RAISE EXCEPTION 'mutable command receipt' USING ERRCODE='P0001';
    EXCEPTION WHEN check_violation THEN NULL; END;
    BEGIN
        DELETE FROM control.management_commands;
        RAISE EXCEPTION 'deleted command receipt' USING ERRCODE='P0001';
    EXCEPTION WHEN check_violation THEN NULL; END;
    BEGIN
        INSERT INTO control.management_commands SELECT * FROM control.management_commands;
        RAISE EXCEPTION 'duplicate command accepted' USING ERRCODE='P0001';
    EXCEPTION WHEN unique_violation THEN NULL; END;
    BEGIN
        UPDATE control.policy_revisions SET document='{}';
        RAISE EXCEPTION 'mutable policy snapshot' USING ERRCODE='P0001';
    EXCEPTION WHEN check_violation THEN NULL; END;
    BEGIN
        UPDATE control.management_audit SET outcome='rejected' WHERE action='policy.activate-initial';
        RAISE EXCEPTION 'mutable activation audit' USING ERRCODE='P0001';
    EXCEPTION WHEN check_violation THEN NULL; END;
    BEGIN
        UPDATE control.datasets SET policy_revision=NULL;
        RAISE EXCEPTION 'cleared security head' USING ERRCODE='P0001';
    EXCEPTION WHEN check_violation THEN NULL; END;
END $$;
