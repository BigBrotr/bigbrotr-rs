-- Actual engine negatives; each attempted mutation must raise exactly 23514.
-- Run after one successful basic enrollment, and compare the full dump outside.
DO $case$
DECLARE statement text; rejected integer := 0;
BEGIN
    FOREACH statement IN ARRAY ARRAY[
        'UPDATE control.local_credentials SET memory_kib=32768',
        'UPDATE control.local_credentials SET passes=1',
        'UPDATE control.local_credentials SET lanes=4',
        'UPDATE control.local_credentials SET argon_version=16',
        'UPDATE control.local_credentials SET salt=decode(''aa'',''hex'')',
        'UPDATE control.local_credentials SET password_hash=decode(''aa'',''hex'')',
        'UPDATE control.owner_challenges SET token_digest=decode(repeat(''aa'',32),''hex'')',
        'UPDATE control.owner_challenges SET expires_at_us=expires_at_us+1',
        'UPDATE control.owner_challenges SET state=''pending'' WHERE state=''consumed''',
        'DELETE FROM control.owner_challenges',
        'UPDATE control.principals SET kind=''installation'' WHERE kind=''local''',
        'UPDATE control.management_audit SET outcome=''rejected''',
        'UPDATE control.local_accounts SET principal_id=(SELECT principal_id FROM control.principals WHERE kind=''installation'')',
        'INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,subject_id,reason,requester_database_role,requester_os_uid) SELECT decode(repeat(''fe'',16),''hex''),dataset_id,principal_id,''owner.enroll'',''rejected'',1,decode(repeat(''fe'',16),''hex''),NULL,SESSION_USER,1000 FROM control.principals WHERE kind=''installation''',
        'UPDATE control.principal_grants SET action=''export.private'''
    ] LOOP
        BEGIN
            EXECUTE statement;
            RAISE EXCEPTION 'negative control unexpectedly succeeded';
        EXCEPTION WHEN check_violation THEN
            rejected := rejected + 1;
        END;
    END LOOP;
    IF rejected <> 15 THEN RAISE EXCEPTION 'incomplete negative controls'; END IF;
END $case$;
