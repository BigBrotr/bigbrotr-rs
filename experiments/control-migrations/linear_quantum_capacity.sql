-- Isolated SQL accounting probe on the stopped, owned current fixture. This is
-- NOT native/authority evidence: the owner seeds accounting scalars with history
-- triggers disabled, then invokes the actual reservation-capacity trigger via a
-- TEMP three-field row. Every persistent change rolls back, including its xmin.
BEGIN;
SET LOCAL session_replication_role=replica;
CREATE TEMP TABLE quantum_capacity_probe(producer_slot_id bytea,producer_epoch numeric,input_bytes bigint);
CREATE TRIGGER actual_reservation_capacity BEFORE INSERT ON quantum_capacity_probe
    FOR EACH ROW EXECUTE FUNCTION control.reserve_producer_outcome_capacity();
ALTER TABLE quantum_capacity_probe ENABLE ALWAYS TRIGGER actual_reservation_capacity;
CREATE TEMP TABLE quantum_capacity_results(label text PRIMARY KEY,boundary_input bigint);
DO $$
DECLARE slot bytea; head control.producer_checkpoints%ROWTYPE; wanted bytea; count_q bigint;
BEGIN
    IF (SELECT count(*) FROM control.producer_epochs WHERE producer_epoch=1)<>1 THEN
        RAISE EXCEPTION 'capacity probe requires the one frozen initial producer';
    END IF;
    SELECT producer_slot_id INTO STRICT slot FROM control.producer_epochs WHERE producer_epoch=1;
    SELECT * INTO STRICT head FROM control.producer_checkpoints WHERE producer_slot_id=slot AND producer_epoch=1;
    SELECT quantum_id INTO STRICT wanted FROM control.acquisition_quanta
        WHERE producer_slot_id=slot AND producer_epoch=1 ORDER BY quantum_number LIMIT 1;
    DELETE FROM control.producer_requests WHERE producer_slot_id=slot AND producer_epoch=1 AND request_sequence<>1;
    UPDATE control.producer_requests SET input_bytes=16777216 WHERE producer_slot_id=slot AND producer_epoch=1;
    UPDATE control.producer_checkpoints SET progress_jobs=2,dependency_count=12,prefix_profile=true
        WHERE producer_slot_id=slot AND producer_epoch=1;
    UPDATE control.acquisition_quanta SET charged_bytes=1024 WHERE producer_slot_id=slot AND producer_epoch=1;
    SELECT count(*) INTO count_q FROM control.acquisition_quanta WHERE producer_slot_id=slot AND producer_epoch=1;
    UPDATE control.acquisition_quanta SET charged_bytes=2097152-1024*(count_q-1) WHERE quantum_id=wanted;

    -- 16MiB retained input+64KiB completion,2MiB quanta,38912 head/prefix
    -- metadata, and the NEW input+64KiB completion reach exactly32MiB.
    INSERT INTO quantum_capacity_probe VALUES(slot,1,14510080);
    BEGIN
        INSERT INTO quantum_capacity_probe VALUES(slot,1,14510081);
        RAISE EXCEPTION 'reservation omitted retained quantum charge';
    EXCEPTION WHEN check_violation THEN NULL; END;
    INSERT INTO quantum_capacity_results VALUES('typed_head_and_quanta',14510080);

    DELETE FROM control.producer_checkpoints WHERE producer_slot_id=slot AND producer_epoch=1;
    INSERT INTO quantum_capacity_probe VALUES(slot,1,14515200);
    BEGIN
        INSERT INTO quantum_capacity_probe VALUES(slot,1,14515201);
        RAISE EXCEPTION 'no-head reservation omitted quantum/head allowance';
    EXCEPTION WHEN check_violation THEN NULL; END;
    INSERT INTO quantum_capacity_results VALUES('no_head_and_quanta',14515200);

    DELETE FROM control.acquisition_quanta WHERE producer_slot_id=slot AND producer_epoch=1;
    INSERT INTO quantum_capacity_probe VALUES(slot,1,16612352);
    BEGIN
        INSERT INTO quantum_capacity_probe VALUES(slot,1,16612353);
        RAISE EXCEPTION 'no-head reservation omitted1024-byte head allowance';
    EXCEPTION WHEN check_violation THEN NULL; END;
    INSERT INTO quantum_capacity_results VALUES('no_head_zero_quanta',16612352);
END $$;
SELECT jsonb_build_object('scope','isolated_sql_accounting_not_native_authority','positives',3,'refusals',3,
    'boundaries',(SELECT jsonb_object_agg(label,boundary_input) FROM quantum_capacity_results));
ROLLBACK;
