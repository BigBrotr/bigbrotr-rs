-- Repeatable single-lineage Ingest succession. This is coordination metadata,
-- not proof of native contents, exclusive custody, sidecars or candidate identity.
-- The typed consumer must check those independently before dispatch/publication.
-- Applied 0030..0037 identities and their epoch1/2 branches remain unchanged.

CREATE FUNCTION control.ingest_lineage_uint(value bytea, at_byte integer, width integer)
RETURNS numeric LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
DECLARE answer numeric:=0; i integer;
BEGIN
    IF width NOT IN (2,4,8) OR at_byte<0 OR octet_length(value)<at_byte+width THEN
        RAISE EXCEPTION 'invalid lineage unsigned integer' USING ERRCODE='23514';
    END IF;
    FOR i IN at_byte..at_byte+width-1 LOOP answer:=answer*256+get_byte(value,i); END LOOP;
    RETURN answer;
END $$;

CREATE FUNCTION control.ingest_lineage_u64(value numeric)
RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
DECLARE answer bytea:=decode('0000000000000000','hex'); remaining numeric:=value; i integer;
BEGIN
    IF value<0 OR value>18446744073709551615 OR trunc(value)<>value THEN
        RAISE EXCEPTION 'invalid lineage u64' USING ERRCODE='23514';
    END IF;
    FOR i IN REVERSE 7..0 LOOP
        answer:=set_byte(answer,i,mod(remaining,256)::integer); remaining:=trunc(remaining/256);
    END LOOP;
    RETURN answer;
END $$;

CREATE FUNCTION control.ingest_lineage_manifest(value bytea)
RETURNS TABLE(key_schema integer,dataset_id bytea,topology numeric,resource bytea,
    shard numeric,epoch numeric,incarnation bytea,recovery bytea,policy numeric)
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
DECLARE i integer; j integer; domain_id bytea;
BEGIN
    -- The selected SQL profile has every one of the nine allocation kinds.
    -- The codec's smaller manifest shapes are not registry authority here.
    IF octet_length(value)<>266 OR substring(value FROM 1 FOR 2)<>decode('0101','hex')
        OR substring(value FROM 5 FOR 6)<>decode('000100010001','hex')
        OR get_byte(value,102)<>1 OR get_byte(value,103)<>9 THEN
        RAISE EXCEPTION 'invalid lineage manifest profile' USING ERRCODE='23514';
    END IF;
    key_schema:=control.ingest_lineage_uint(value,2,2)::integer;
    dataset_id:=substring(value FROM 11 FOR 16); topology:=control.ingest_lineage_uint(value,26,8);
    resource:=substring(value FROM 35 FOR 16); shard:=control.ingest_lineage_uint(value,50,4);
    epoch:=control.ingest_lineage_uint(value,54,8); incarnation:=substring(value FROM 63 FOR 16);
    recovery:=substring(value FROM 79 FOR 16); policy:=control.ingest_lineage_uint(value,94,8);
    IF epoch NOT BETWEEN 1 AND 255 OR policy=0
        OR key_schema<>CASE WHEN epoch=1 THEN 1 WHEN epoch=2 THEN 2 ELSE 3 END
        OR dataset_id=decode(repeat('00',16),'hex') OR resource=decode(repeat('00',16),'hex')
        OR incarnation=decode(repeat('00',16),'hex') THEN
        RAISE EXCEPTION 'invalid lineage manifest identity' USING ERRCODE='23514';
    END IF;
    FOR i IN 1..9 LOOP
        domain_id:=substring(value FROM 107+(i-1)*18 FOR 16);
        IF control.ingest_lineage_uint(value,104+(i-1)*18,2)<>i
            OR domain_id=decode(repeat('00',16),'hex') THEN
            RAISE EXCEPTION 'invalid lineage allocation profile' USING ERRCODE='23514';
        END IF;
        FOR j IN 1..i-1 LOOP
            IF domain_id=substring(value FROM 107+(j-1)*18 FOR 16) THEN
                RAISE EXCEPTION 'duplicate lineage allocation identity' USING ERRCODE='23514';
            END IF;
        END LOOP;
    END LOOP;
    RETURN NEXT;
END $$;

CREATE TABLE control.ingest_successions (
    dataset_id control.id16 NOT NULL,
    owner_resource_id control.id16 NOT NULL,
    target_epoch control.u64 NOT NULL CHECK(target_epoch BETWEEN 3 AND 255),
    previous_epoch control.u64 NOT NULL CHECK(previous_epoch=target_epoch-1),
    terminal_kind smallint NOT NULL CHECK(terminal_kind IN(1,2)),
    owner_role text NOT NULL DEFAULT 'ingest' CHECK(owner_role='ingest'),
    target_incarnation control.id16 NOT NULL,
    target_role_instance_id control.id16 NOT NULL UNIQUE,
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK(producer_epoch=target_epoch),
    shard_id control.u32 NOT NULL,
    previous_topology control.u64 NOT NULL,
    target_topology control.u64 NOT NULL CHECK(target_topology=previous_topology+1),
    issued_through control.u64 NOT NULL CHECK(issued_through<=64),
    last_complete_sequence control.u64 NOT NULL,
    closure_digest control.digest32 NOT NULL,
    previous_manifest bytea NOT NULL CHECK(octet_length(previous_manifest)=266),
    target_manifest bytea NOT NULL CHECK(octet_length(target_manifest)=266),
    previous_manifest_sha256 control.digest32 NOT NULL CHECK(previous_manifest_sha256=sha256(previous_manifest)),
    target_manifest_sha256 control.digest32 NOT NULL CHECK(target_manifest_sha256=sha256(target_manifest)),
    handoff bytea NOT NULL CHECK(octet_length(handoff) BETWEEN 319 AND 643),
    handoff_sha256 control.digest32 NOT NULL CHECK(handoff_sha256=sha256(handoff)),
    parent_handoff_sha256 control.digest32 NOT NULL,
    config_revision control.u64 NOT NULL,
    policy_revision control.u64 NOT NULL,
    config_digest control.digest32 NOT NULL,
    policy_digest control.digest32 NOT NULL,
    outstanding_request_limit control.u32 NOT NULL CHECK(outstanding_request_limit BETWEEN 1 AND 64),
    created_at_us bigint NOT NULL,
    lease_expires_at_us bigint NOT NULL CHECK(lease_expires_at_us>created_at_us),
    prepared_by_resource control.id16 NOT NULL,
    prepared_by_epoch control.u64 NOT NULL,
    prepared_by_role text NOT NULL DEFAULT 'control' CHECK(prepared_by_role='control'),
    prepared_by_incarnation control.id16 NOT NULL,
    preparation_transaction xid8 NOT NULL DEFAULT pg_current_xact_id(),
    PRIMARY KEY(owner_resource_id,target_epoch),
    UNIQUE(producer_slot_id,producer_epoch),
    CHECK(terminal_kind<>2 OR (issued_through=0 AND last_complete_sequence=0)),
    CHECK(issued_through<>0 OR last_complete_sequence=0),
    FOREIGN KEY(dataset_id,owner_resource_id,target_epoch,owner_role,target_incarnation)
        REFERENCES control.owner_epochs(dataset_id,resource_id,epoch,role,process_incarnation),
    FOREIGN KEY(target_role_instance_id,dataset_id,owner_role)
        REFERENCES control.role_instances(role_instance_id,dataset_id,role),
    FOREIGN KEY(producer_slot_id,producer_epoch,dataset_id)
        REFERENCES control.producer_epochs(producer_slot_id,producer_epoch,dataset_id),
    FOREIGN KEY(dataset_id,shard_id,target_epoch) REFERENCES control.source_streams(dataset_id,shard_id,owner_epoch),
    FOREIGN KEY(dataset_id,target_topology) REFERENCES control.topology_generations(dataset_id,generation),
    FOREIGN KEY(dataset_id,previous_topology) REFERENCES control.topology_generations(dataset_id,generation),
    FOREIGN KEY(dataset_id,config_revision) REFERENCES control.config_revisions(dataset_id,revision),
    FOREIGN KEY(dataset_id,policy_revision) REFERENCES control.policy_revisions(dataset_id,revision),
    FOREIGN KEY(dataset_id,prepared_by_resource,prepared_by_epoch,prepared_by_role,prepared_by_incarnation)
        REFERENCES control.owner_epochs(dataset_id,resource_id,epoch,role,process_incarnation)
);
CREATE TABLE control.ingest_activations (
    dataset_id control.id16 NOT NULL,
    owner_resource_id control.id16 NOT NULL,
    target_epoch control.u64 NOT NULL CHECK(target_epoch BETWEEN 3 AND 255),
    target_incarnation control.id16 NOT NULL,
    target_role_instance_id control.id16 NOT NULL,
    producer_slot_id control.id16 NOT NULL,
    producer_epoch control.u64 NOT NULL CHECK(producer_epoch=target_epoch),
    target_manifest_sha256 control.digest32 NOT NULL,
    handoff_sha256 control.digest32 NOT NULL,
    activated_by_resource control.id16 NOT NULL,
    activated_by_epoch control.u64 NOT NULL,
    activated_by_incarnation control.id16 NOT NULL,
    activated_at_us bigint NOT NULL,
    activation_transaction xid8 NOT NULL DEFAULT pg_current_xact_id(),
    PRIMARY KEY(owner_resource_id,target_epoch),
    UNIQUE(producer_slot_id,producer_epoch),
    FOREIGN KEY(owner_resource_id,target_epoch) REFERENCES control.ingest_successions(owner_resource_id,target_epoch)
);

CREATE FUNCTION control.ingest_lineage_manifest_at(wanted_resource bytea,wanted_epoch numeric)
RETURNS bytea LANGUAGE plpgsql STRICT SET search_path=pg_catalog AS $$
DECLARE answer bytea;
BEGIN
    IF wanted_epoch=1 THEN
        SELECT previous_manifest INTO STRICT answer FROM control.initial_ingest_successions WHERE owner_resource_id=wanted_resource;
    ELSIF wanted_epoch=2 THEN
        SELECT target_manifest INTO STRICT answer FROM control.initial_ingest_successions WHERE owner_resource_id=wanted_resource;
    ELSE
        SELECT target_manifest INTO STRICT answer FROM control.ingest_successions
            WHERE owner_resource_id=wanted_resource AND target_epoch=wanted_epoch;
    END IF;
    RETURN answer;
END $$;

CREATE FUNCTION control.ingest_lineage_handoff_at(wanted_resource bytea,wanted_epoch numeric)
RETURNS bytea LANGUAGE plpgsql STRICT SET search_path=pg_catalog AS $$
DECLARE answer bytea;
BEGIN
    IF wanted_epoch=2 THEN
        SELECT handoff INTO STRICT answer FROM control.initial_ingest_successions WHERE owner_resource_id=wanted_resource;
    ELSE
        SELECT handoff INTO STRICT answer FROM control.ingest_successions
            WHERE owner_resource_id=wanted_resource AND target_epoch=wanted_epoch;
    END IF;
    RETURN answer;
END $$;

CREATE FUNCTION control.ingest_lineage_activation(wanted_resource bytea,wanted_epoch numeric)
RETURNS TABLE(resource bytea,epoch numeric,incarnation bytea)
LANGUAGE plpgsql STRICT SET search_path=pg_catalog AS $$
BEGIN
    IF wanted_epoch=1 THEN
        RETURN QUERY SELECT p.registered_by_resource_id::bytea,p.registered_by_epoch::numeric,p.registered_by_incarnation::bytea
            FROM control.producer_slots p JOIN control.initial_ingest_successions s USING(producer_slot_id)
            WHERE s.owner_resource_id=wanted_resource;
    ELSIF wanted_epoch=2 THEN
        RETURN QUERY SELECT a.activated_by_resource::bytea,a.activated_by_epoch::numeric,a.activated_by_incarnation::bytea
            FROM control.initial_ingest_activations a WHERE a.owner_resource_id=wanted_resource;
    ELSE
        RETURN QUERY SELECT a.activated_by_resource::bytea,a.activated_by_epoch::numeric,a.activated_by_incarnation::bytea
            FROM control.ingest_activations a WHERE a.owner_resource_id=wanted_resource AND a.target_epoch=wanted_epoch;
    END IF;
END $$;

-- One bounded ordered prefix, no receipt vector. This recomputes declared SQL
-- byte commitments; only the native owner can prove those bytes describe effects.
CREATE FUNCTION control.ingest_lineage_closure_digest(wanted_resource bytea,wanted_epoch numeric,empty_attempt boolean)
RETURNS bytea LANGUAGE plpgsql STRICT SET search_path=pg_catalog AS $$
DECLARE p control.producer_epochs%ROWTYPE; stream control.source_streams%ROWTYPE;
        manifest bytea; incoming bytea; buffer bytea; row record; sequence numeric:=0;
BEGIN
    manifest:=control.ingest_lineage_manifest_at(wanted_resource,wanted_epoch);
    SELECT * INTO STRICT p FROM control.producer_epochs WHERE owner_resource_id=wanted_resource AND owner_epoch=wanted_epoch;
    SELECT * INTO STRICT stream FROM control.source_streams WHERE owner_resource_id=wanted_resource AND owner_epoch=wanted_epoch;
    IF p.reserved_through_request>64 THEN
        RAISE EXCEPTION 'lineage request prefix exceeds its bound' USING ERRCODE='23514';
    END IF;
    IF wanted_epoch>1 THEN incoming:=control.ingest_lineage_handoff_at(wanted_resource,wanted_epoch); END IF;
    IF empty_attempt THEN
        IF wanted_epoch<2 OR p.reserved_through_request<>0 THEN
            RAISE EXCEPTION 'empty lineage closure has reserved requests' USING ERRCODE='23514';
        END IF;
        RETURN sha256(convert_to(CASE WHEN wanted_epoch=2 THEN 'bigbrotr/initial-empty-attempt-closure/1'
            ELSE 'bigbrotr/ingest-successor-empty-closure/1' END,'UTF8')||decode('00','hex')
            ||sha256(incoming)||sha256(manifest)||p.producer_slot_id||control.ingest_lineage_u64(wanted_epoch)
            ||control.ingest_lineage_u64(0)||control.ingest_lineage_u64(0));
    END IF;
    buffer:=convert_to(CASE WHEN wanted_epoch=1 THEN 'bigbrotr/initial-source-closure/1'
        WHEN wanted_epoch=2 THEN 'bigbrotr/activated-initial-source-closure/1'
        ELSE 'bigbrotr/ingest-successor-source-closure/1' END,'UTF8')||decode('00','hex');
    IF wanted_epoch>1 THEN buffer:=buffer||sha256(incoming); END IF;
    buffer:=buffer||sha256(manifest)||p.producer_slot_id||control.ingest_lineage_u64(wanted_epoch)
        ||control.ingest_lineage_u64(p.reserved_through_request);
    FOR row IN SELECT r.request_sequence,r.request_digest,r.dataset_id,r.manifest_sha256,
            o.request_digest AS copied_digest,o.dataset_id AS copied_dataset,o.manifest_sha256 AS copied_manifest,
            o.outcome_sha256,o.outcome_bytes
        FROM control.producer_requests r LEFT JOIN control.producer_outcomes o
            USING(producer_slot_id,producer_epoch,request_sequence)
        WHERE r.producer_slot_id=p.producer_slot_id AND r.producer_epoch=wanted_epoch
        ORDER BY r.request_sequence LIMIT 65 LOOP
        sequence:=sequence+1;
        IF sequence>64 OR row.request_sequence<>sequence OR row.dataset_id<>p.dataset_id
            OR row.manifest_sha256<>sha256(manifest)
            OR ROW(row.copied_digest,row.copied_dataset,row.copied_manifest)
                IS DISTINCT FROM ROW(row.request_digest,row.dataset_id,row.manifest_sha256)
            OR row.outcome_sha256 IS NULL OR row.outcome_sha256<>sha256(row.outcome_bytes) THEN
            RAISE EXCEPTION 'lineage closure requires exact retained outcome copies' USING ERRCODE='23514';
        END IF;
        buffer:=buffer||control.ingest_lineage_u64(sequence)||row.request_digest||row.outcome_sha256;
    END LOOP;
    IF sequence<>p.reserved_through_request OR stream.last_complete_sequence IS NULL THEN
        RAISE EXCEPTION 'lineage closure requires its complete declared source cut' USING ERRCODE='23514';
    END IF;
    RETURN sha256(buffer||control.ingest_lineage_u64(stream.last_complete_sequence));
END $$;

-- Only transaction identity and the actual changing Control are additional
-- evidence. Terminal meaning stays in producer/source/domain/owner/role rows.
-- pg_current_xact_id() is a top-level xid8, including inside subtransactions
-- (PostgreSQL16 xid8funcs.c); tuple xmin would not fence that boundary.
CREATE TABLE control.ingest_terminal_transactions (
    owner_resource_id control.id16 NOT NULL,
    target_epoch control.u64 NOT NULL,
    terminal_transaction xid8 NOT NULL,
    control_resource control.id16 NOT NULL,
    control_epoch control.u64 NOT NULL,
    control_incarnation control.id16 NOT NULL,
    PRIMARY KEY(owner_resource_id,target_epoch),
    FOREIGN KEY(owner_resource_id,target_epoch) REFERENCES control.ingest_successions(owner_resource_id,target_epoch)
);

-- Historical workflow evidence is immutable attribution, not live authority of
-- its old gap recorder. The caller holds actual current Control/dataset custody.
CREATE FUNCTION control.require_ingest_lineage_workflows(wanted_resource bytea,wanted_epoch numeric,current_epoch numeric)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE p control.producer_epochs%ROWTYPE; stream control.source_streams%ROWTYPE;
        original record; wanted_reason text; gap_count bigint;
BEGIN
    SELECT * INTO STRICT p FROM control.producer_epochs WHERE owner_resource_id=wanted_resource AND owner_epoch=wanted_epoch;
    SELECT * INTO STRICT stream FROM control.source_streams WHERE owner_resource_id=wanted_resource AND owner_epoch=wanted_epoch;
    SELECT * INTO STRICT original FROM control.ingest_lineage_activation(wanted_resource,wanted_epoch);
    wanted_reason:=CASE WHEN wanted_epoch=1 THEN 'initial_producer_abandoned'
        WHEN wanted_epoch=2 THEN 'activated_initial_producer_abandoned' ELSE 'ingest_successor_abandoned' END;
    IF EXISTS(SELECT 1 FROM control.producer_checkpoints WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=wanted_epoch)
        OR EXISTS(SELECT 1 FROM control.producer_input_bindings WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=wanted_epoch)
        OR EXISTS(SELECT 1 FROM control.producer_requests r
            WHERE r.producer_slot_id=p.producer_slot_id AND r.producer_epoch=wanted_epoch
                AND ((r.relay_endpoint_id IS NOT NULL AND r.acquisition_job_id IS NULL)
                    OR (r.acquisition_job_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM control.acquisition_gaps g
                        WHERE g.job_id=r.acquisition_job_id AND g.producer_slot_id=p.producer_slot_id AND g.producer_epoch=wanted_epoch)))) THEN
        RAISE EXCEPTION 'lineage retirement has unsupported workflow obligations' USING ERRCODE='23514';
    END IF;
    PERFORM job_id FROM control.jobs WHERE dataset_id=p.dataset_id AND kind='acquisition'
        AND owner_resource_id=original.resource AND owner_epoch=original.epoch AND owner_incarnation=original.incarnation
        ORDER BY job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN RAISE EXCEPTION 'lineage acquisition claims remain unresolved' USING ERRCODE='23514'; END IF;
    PERFORM job_id FROM control.jobs WHERE dataset_id=p.dataset_id AND kind='import'
        AND owner_resource_id=original.resource AND owner_epoch=original.epoch AND owner_incarnation=original.incarnation
        ORDER BY job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN RAISE EXCEPTION 'lineage import claims remain unresolved' USING ERRCODE='23514'; END IF;
    SELECT count(*) INTO gap_count FROM (SELECT 1 FROM control.acquisition_gaps
        WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=wanted_epoch LIMIT 65) bounded;
    IF gap_count>64 THEN RAISE EXCEPTION 'lineage gap bound exceeded' USING ERRCODE='23514'; END IF;
    PERFORM j.job_id FROM control.acquisition_gaps g JOIN control.jobs j USING(job_id)
        WHERE g.producer_slot_id=p.producer_slot_id AND g.producer_epoch=wanted_epoch
        ORDER BY j.job_id LIMIT 65 FOR UPDATE OF j;
    IF EXISTS(SELECT 1 FROM control.acquisition_gaps g LEFT JOIN control.jobs j USING(job_id)
        WHERE g.producer_slot_id=p.producer_slot_id AND g.producer_epoch=wanted_epoch
            AND NOT coalesce(g.dataset_id=p.dataset_id AND g.shard_id=stream.shard_id AND g.source_owner_epoch=wanted_epoch
                AND g.reason=wanted_reason AND g.owner_role='control' AND g.last_complete_sequence=stream.last_complete_sequence
                AND g.source_closure_digest=stream.closure_digest
                AND ROW(g.previous_owner_resource,g.previous_owner_epoch,g.previous_owner_incarnation)
                    IS NOT DISTINCT FROM ROW(original.resource,original.epoch,original.incarnation)
                AND g.issuer_resource=original.resource AND g.issuer_epoch>original.epoch AND g.issuer_epoch<=current_epoch
                AND g.abandoned_at_us>=stream.closed_at_us AND g.abandoned_at_us>=j.created_at_us
                AND g.abandoned_at_us<=(extract(epoch FROM clock_timestamp())*1000000)::bigint
                AND j.dataset_id=g.dataset_id AND j.kind='acquisition' AND j.acquisition_job_protocol=1 AND j.input_complete
                AND j.state='cancelled' AND j.job_revision=g.previous_job_revision+2 AND j.checkpoint_revision=0
                AND j.result_manifest_digest IS NULL AND j.reason_code=g.reason AND j.completed_at_us=g.abandoned_at_us
                AND ROW(j.owner_resource_id,j.owner_epoch,j.owner_incarnation)
                    IS NOT DISTINCT FROM ROW(g.issuer_resource,g.issuer_epoch,g.issuer_incarnation),false)) THEN
        RAISE EXCEPTION 'lineage retirement requires exact retained gaps' USING ERRCODE='23514';
    END IF;
END $$;

CREATE FUNCTION control.lock_ingest_lineage(wanted_resource bytea,wanted_epoch numeric)
RETURNS control.owner_epochs LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE legacy control.initial_ingest_successions%ROWTYPE; s control.ingest_successions%ROWTYPE;
        c control.owner_epochs%ROWTYPE; o control.owner_epochs%ROWTYPE; p control.producer_epochs%ROWTYPE;
        stream control.source_streams%ROWTYPE; role_row control.role_instances%ROWTYPE;
        old_p control.producer_epochs%ROWTYPE; old_stream control.source_streams%ROWTYPE;
        original record; m record; first_manifest record; previous_manifest record;
        a control.ingest_activations%ROWTYPE; old_a control.initial_ingest_activations%ROWTYPE;
        manifest bytea; previous bytea; incoming bytea; expected bytea; digest bytea;
        activated boolean; e integer; k integer; offset_generation numeric; empty_prefix integer; created bigint;
        expected_role bytea; prepared_resource bytea; prepared_epoch numeric; prepared_incarnation bytea;
        request_limit numeric; request_count bigint; maximum numeric;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'lineage requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    IF wanted_epoch NOT BETWEEN 2 AND 255 OR trunc(wanted_epoch)<>wanted_epoch
        OR wanted_epoch*(wanted_epoch+1)/2>32768 THEN
        RAISE EXCEPTION 'lineage metadata or membership profile exhausted' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT legacy FROM control.initial_ingest_successions WHERE owner_resource_id=wanted_resource;
    SELECT * INTO STRICT c FROM control.owner_epochs
        WHERE dataset_id=legacy.dataset_id AND role='control' AND state='active' FOR UPDATE;
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=legacy.dataset_id FOR UPDATE;
    IF wanted_epoch=2 THEN
        -- The same flat checker can inspect terminal legacy genesis before any
        -- new preparation. These are coordinates from its retained0030 anchor,
        -- not a fabricated new receipt or a reconstructed writer capability.
        s.dataset_id:=legacy.dataset_id; s.producer_slot_id:=legacy.producer_slot_id;
        s.shard_id:=legacy.shard_id; s.config_revision:=legacy.config_revision; s.policy_revision:=legacy.policy_revision;
    ELSE
        SELECT * INTO STRICT s FROM control.ingest_successions WHERE owner_resource_id=wanted_resource AND target_epoch=wanted_epoch;
    END IF;
    SELECT * INTO STRICT first_manifest FROM control.ingest_lineage_manifest(legacy.previous_manifest);
    offset_generation:=first_manifest.topology;
    empty_prefix:=CASE WHEN offset_generation=0 THEN 0 ELSE 1 END;
    IF wanted_epoch-1+offset_generation>18446744073709551615
        OR first_manifest.epoch<>1 OR first_manifest.dataset_id<>legacy.dataset_id
        OR first_manifest.resource<>wanted_resource OR first_manifest.shard<>legacy.shard_id
        OR legacy.previous_topology<>first_manifest.topology OR legacy.target_topology<>first_manifest.topology+1
        OR s.dataset_id<>legacy.dataset_id OR s.producer_slot_id<>legacy.producer_slot_id OR s.shard_id<>legacy.shard_id
        OR NOT EXISTS(SELECT 1 FROM control.datasets d JOIN control.config_revisions cr
            ON cr.dataset_id=d.dataset_id AND cr.revision=d.config_revision
            JOIN control.policy_revisions pr ON pr.dataset_id=d.dataset_id AND pr.revision=d.policy_revision AND pr.config_revision=cr.revision
            WHERE d.dataset_id=s.dataset_id AND d.topology_generation=wanted_epoch-1+offset_generation
                AND d.config_revision>=s.config_revision AND d.policy_revision>=s.policy_revision) THEN
        RAISE EXCEPTION 'lineage identity or current policy/head differs' USING ERRCODE='23514';
    END IF;
    -- Sentinel queries bound unsupported profiles before ordered history work.
    IF (SELECT count(*) FROM(SELECT 1 FROM control.ingest_successions WHERE owner_resource_id=wanted_resource LIMIT 254)b)<>wanted_epoch-2
        OR (SELECT count(*) FROM(SELECT 1 FROM control.owner_epochs WHERE dataset_id=s.dataset_id AND role='ingest' LIMIT 256)b)<>wanted_epoch
        OR (SELECT count(*) FROM(SELECT 1 FROM control.producer_epochs WHERE dataset_id=s.dataset_id LIMIT 256)b)<>wanted_epoch
        OR (SELECT count(*) FROM(SELECT 1 FROM control.producer_slots WHERE dataset_id=s.dataset_id LIMIT 2)b)<>1
        OR (SELECT count(*) FROM(SELECT 1 FROM control.source_streams WHERE dataset_id=s.dataset_id LIMIT 256)b)<>wanted_epoch
        OR (SELECT count(*) FROM(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id LIMIT 2296)b)<>wanted_epoch*9
        OR (SELECT count(DISTINCT allocation_domain) FROM control.allocation_domains WHERE dataset_id=s.dataset_id)<>wanted_epoch*9
        OR (SELECT count(*) FROM(SELECT 1 FROM control.topology_generations WHERE dataset_id=s.dataset_id LIMIT 257)b)<>wanted_epoch+empty_prefix
        OR (SELECT count(*) FROM(SELECT 1 FROM control.topology_streams WHERE dataset_id=s.dataset_id LIMIT 32769)b)<>wanted_epoch*(wanted_epoch+1)/2
        OR (SELECT count(DISTINCT process_incarnation) FROM control.owner_epochs
            WHERE dataset_id=s.dataset_id AND role='ingest')<>wanted_epoch
        OR (SELECT count(DISTINCT role_instance_id) FROM control.owner_epochs
            WHERE dataset_id=s.dataset_id AND role='ingest')<>wanted_epoch THEN
        RAISE EXCEPTION 'lineage has missing, foreign or excessive retained parents' USING ERRCODE='23514';
    END IF;
    -- The optional empty predecessor may have any u64 generation. Its presence,
    -- not its numerical value, consumes one inspected generation and no members.
    IF empty_prefix=1 AND (NOT EXISTS(SELECT 1 FROM control.topology_generations
            WHERE dataset_id=s.dataset_id AND generation=offset_generation-1 AND previous_generation IS NULL AND sealed)
        OR EXISTS(SELECT 1 FROM control.topology_streams WHERE dataset_id=s.dataset_id AND topology_generation=offset_generation-1)) THEN
        RAISE EXCEPTION 'lineage empty genesis prefix differs' USING ERRCODE='23514';
    END IF;
    -- One flat walk. It never calls a historical head validator recursively.
    FOR e IN 1..wanted_epoch::integer LOOP
        manifest:=control.ingest_lineage_manifest_at(wanted_resource,e);
        SELECT * INTO STRICT m FROM control.ingest_lineage_manifest(manifest);
        SELECT * INTO STRICT o FROM control.owner_epochs WHERE resource_id=wanted_resource AND epoch=e FOR UPDATE;
        SELECT * INTO STRICT role_row FROM control.role_instances WHERE role_instance_id=o.role_instance_id FOR UPDATE;
        PERFORM 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource AND owner_epoch=e
            ORDER BY dictionary_kind,allocation_domain LIMIT 10 FOR UPDATE;
        SELECT * INTO STRICT stream FROM control.source_streams WHERE dataset_id=s.dataset_id AND shard_id=s.shard_id AND owner_epoch=e FOR UPDATE;
        SELECT * INTO STRICT p FROM control.producer_epochs WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=e FOR UPDATE;
        IF ROW(m.dataset_id,m.resource,m.shard,m.epoch,m.topology,m.recovery)
            IS DISTINCT FROM ROW(s.dataset_id::bytea,wanted_resource,s.shard_id::numeric,e::numeric,(e-1+offset_generation)::numeric,first_manifest.recovery)
            OR ROW(o.dataset_id,o.role,o.process_incarnation,role_row.dataset_id,role_row.role)
                IS DISTINCT FROM ROW(s.dataset_id,'ingest'::text,m.incarnation,s.dataset_id,'ingest'::text)
            OR ROW(p.dataset_id,p.producer_role_instance_id,p.producer_role,p.producer_incarnation,p.owner_resource_id,p.owner_epoch,p.owner_role,p.owner_incarnation)
                IS DISTINCT FROM ROW(s.dataset_id,o.role_instance_id,'ingest'::text,m.incarnation,wanted_resource,e::numeric,'ingest'::text,m.incarnation)
            OR ROW(stream.owner_resource_id,stream.owner_role,stream.owner_incarnation)
                IS DISTINCT FROM ROW(wanted_resource,'ingest'::text,m.incarnation)
            OR p.reserved_through_request>p.outstanding_request_limit OR p.outstanding_request_limit>64
            OR (SELECT count(*) FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource AND owner_epoch=e)<>9 THEN
            RAISE EXCEPTION 'lineage registry/manifest identity differs' USING ERRCODE='23514';
        END IF;
        IF e>=3 AND EXISTS(SELECT 1 FROM control.owner_epochs WHERE dataset_id=s.dataset_id
            AND process_incarnation=m.incarnation AND (resource_id<>wanted_resource OR epoch<>e)) THEN
            RAISE EXCEPTION 'lineage target incarnation was already registered' USING ERRCODE='23514';
        END IF;
        FOR k IN 1..9 LOOP
            IF NOT EXISTS(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND dictionary_kind=k
                AND allocation_domain=substring(manifest FROM 107+(k-1)*18 FOR 16)
                AND owner_resource_id=wanted_resource AND owner_epoch=e AND owner_role='ingest' AND owner_incarnation=m.incarnation) THEN
                RAISE EXCEPTION 'lineage manifest allocation namespace differs' USING ERRCODE='23514';
            END IF;
        END LOOP;
        IF NOT EXISTS(SELECT 1 FROM control.policy_revisions WHERE dataset_id=s.dataset_id AND revision=m.policy) THEN
            RAISE EXCEPTION 'lineage manifest policy is absent' USING ERRCODE='23514';
        END IF;
        IF e=1 THEN
            created:=o.acquired_at_us; expected_role:=o.role_instance_id; request_limit:=p.outstanding_request_limit;
        ELSE
            incoming:=control.ingest_lineage_handoff_at(wanted_resource,e);
            IF e=2 THEN
                created:=legacy.created_at_us; expected_role:=legacy.target_role_instance_id; request_limit:=legacy.outstanding_request_limit;
                prepared_resource:=legacy.prepared_by_resource; prepared_epoch:=legacy.prepared_by_epoch; prepared_incarnation:=legacy.prepared_by_incarnation;
                expected:=decode('010a','hex')||s.producer_slot_id||control.ingest_lineage_u64(1)
                    ||control.ingest_lineage_u64(legacy.issued_through)||control.ingest_lineage_u64(legacy.last_complete_sequence)
                    ||legacy.closure_digest||decode('010a','hex')||legacy.previous_manifest||decode('010a','hex')||legacy.target_manifest;
                IF incoming<>expected OR legacy.previous_manifest<>previous OR legacy.target_manifest<>manifest
                    OR legacy.issued_through<>old_p.reserved_through_request OR legacy.last_complete_sequence<>old_stream.last_complete_sequence
                    OR legacy.closure_digest<>old_stream.closure_digest OR legacy.target_incarnation<>m.incarnation
                    OR sha256(incoming)<>legacy.handoff_sha256 OR sha256(previous)<>legacy.previous_manifest_sha256
                    OR sha256(manifest)<>legacy.target_manifest_sha256 OR legacy.policy_revision<>m.policy THEN
                    RAISE EXCEPTION 'lineage legacy anchor differs' USING ERRCODE='23514';
                END IF;
            ELSE
                SELECT * INTO STRICT s FROM control.ingest_successions WHERE owner_resource_id=wanted_resource AND target_epoch=e;
                created:=s.created_at_us; expected_role:=s.target_role_instance_id; request_limit:=s.outstanding_request_limit;
                prepared_resource:=s.prepared_by_resource; prepared_epoch:=s.prepared_by_epoch; prepared_incarnation:=s.prepared_by_incarnation;
                expected:=decode('020a','hex')||decode(lpad(to_hex(s.terminal_kind::integer),2,'0'),'hex')||s.producer_slot_id
                    ||control.ingest_lineage_u64(e-1)||control.ingest_lineage_u64(s.issued_through)
                    ||control.ingest_lineage_u64(s.last_complete_sequence)||s.closure_digest||s.parent_handoff_sha256
                    ||decode('010a','hex')||s.previous_manifest||decode('010a','hex')||s.target_manifest;
                IF incoming<>expected OR s.previous_manifest<>previous OR s.target_manifest<>manifest
                    OR s.dataset_id<>legacy.dataset_id OR s.producer_slot_id<>legacy.producer_slot_id OR s.shard_id<>legacy.shard_id
                    OR s.previous_epoch<>e-1 OR s.previous_topology<>e-2+offset_generation OR s.target_topology<>m.topology
                    OR s.target_incarnation<>m.incarnation OR s.policy_revision<>m.policy
                    OR s.parent_handoff_sha256<>sha256(control.ingest_lineage_handoff_at(wanted_resource,e-1))
                    OR s.issued_through<>old_p.reserved_through_request OR s.last_complete_sequence<>old_stream.last_complete_sequence
                    OR s.closure_digest<>old_stream.closure_digest
                    OR s.terminal_kind<>CASE WHEN EXISTS(SELECT 1 FROM control.ingest_lineage_activation(wanted_resource,e-1)) THEN 1 ELSE 2 END THEN
                    RAISE EXCEPTION 'lineage successor receipt or parent hash differs' USING ERRCODE='23514';
                END IF;
            END IF;
            IF created<old_p.retired_at_us THEN
                RAISE EXCEPTION 'lineage prepared identities or creation times differ' USING ERRCODE='23514';
            END IF;
            IF NOT EXISTS(SELECT 1 FROM control.config_revisions cr JOIN control.policy_revisions pr
                    ON pr.dataset_id=cr.dataset_id AND pr.config_revision=cr.revision
                    WHERE cr.dataset_id=s.dataset_id AND cr.revision=CASE WHEN e=2 THEN legacy.config_revision ELSE s.config_revision END
                        AND cr.document_digest=CASE WHEN e=2 THEN legacy.config_digest ELSE s.config_digest END
                        AND pr.revision=m.policy AND pr.document_digest=CASE WHEN e=2 THEN legacy.policy_digest ELSE s.policy_digest END) THEN
                RAISE EXCEPTION 'lineage immutable preparation policy differs' USING ERRCODE='23514';
            END IF;
            IF prepared_resource<>c.resource_id OR prepared_epoch>c.epoch
                OR NOT EXISTS(SELECT 1 FROM control.owner_epochs old JOIN control.role_instances r USING(role_instance_id)
                    WHERE old.dataset_id=s.dataset_id AND old.resource_id=prepared_resource AND old.epoch=prepared_epoch
                        AND old.process_incarnation=prepared_incarnation AND old.role='control' AND old.control_fence_version=1
                        AND r.dataset_id=s.dataset_id AND r.role='control'
                        AND ((old.epoch=c.epoch AND old.state='active' AND old.process_incarnation=c.process_incarnation AND r.retired_at_us IS NULL)
                            OR (old.epoch<c.epoch AND old.state='released' AND old.exclusion_method='storage_fenced'
                                AND old.exclusion_digest IS NOT NULL AND r.retired_at_us IS NOT NULL))) THEN
                RAISE EXCEPTION 'lineage historical preparing Control differs' USING ERRCODE='23514';
            END IF;
        END IF;
        IF o.role_instance_id<>expected_role OR o.acquired_at_us<>created
            OR role_row.created_at_us<>created OR p.opened_at_us<>created OR stream.opened_at_us<>created
            OR p.outstanding_request_limit<>request_limit
            OR EXISTS(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource
                AND owner_epoch=e AND created_at_us<>created)
            OR (e=1 AND NOT EXISTS(SELECT 1 FROM control.producer_slots slot
                WHERE slot.producer_slot_id=s.producer_slot_id AND slot.dataset_id=s.dataset_id AND slot.created_at_us=created
                    AND slot.registered_by_role='control' AND slot.registered_by_resource_id=c.resource_id)) THEN
            RAISE EXCEPTION 'lineage prepared identities or creation times differ' USING ERRCODE='23514';
        END IF;
        SELECT * INTO original FROM control.ingest_lineage_activation(wanted_resource,e);
        activated:=FOUND;
        IF NOT activated AND o.lease_expires_at_us IS DISTINCT FROM
            CASE WHEN e=2 THEN legacy.lease_expires_at_us ELSE s.lease_expires_at_us END THEN
            RAISE EXCEPTION 'empty lineage scheduling metadata differs from preparation' USING ERRCODE='23514';
        END IF;
        IF e=2 AND activated THEN
            SELECT * INTO STRICT old_a FROM control.initial_ingest_activations WHERE owner_resource_id=wanted_resource;
            IF ROW(old_a.dataset_id,old_a.target_epoch,old_a.target_incarnation,old_a.target_role_instance_id,
                    old_a.producer_slot_id,old_a.producer_epoch,old_a.previous_manifest_sha256,old_a.target_manifest_sha256,old_a.handoff_sha256,
                    old_a.prepared_by_resource,old_a.prepared_by_epoch,old_a.prepared_by_incarnation,
                    old_a.activated_by_resource,old_a.activated_by_epoch,old_a.activated_by_incarnation,
                    old_a.config_revision,old_a.policy_revision,old_a.config_digest,old_a.policy_digest)
                IS DISTINCT FROM ROW(legacy.dataset_id,legacy.target_epoch,legacy.target_incarnation,legacy.target_role_instance_id,
                    legacy.producer_slot_id,legacy.producer_epoch,legacy.previous_manifest_sha256,legacy.target_manifest_sha256,legacy.handoff_sha256,
                    legacy.prepared_by_resource,legacy.prepared_by_epoch,legacy.prepared_by_incarnation,
                    legacy.prepared_by_resource,legacy.prepared_by_epoch,legacy.prepared_by_incarnation,
                    legacy.config_revision,legacy.policy_revision,legacy.config_digest,legacy.policy_digest)
                OR old_a.activated_at_us<created OR old_a.activation_transaction=pg_current_xact_id() THEN
                RAISE EXCEPTION 'lineage legacy activation differs' USING ERRCODE='23514';
            END IF;
        ELSIF e>=3 AND activated THEN
            SELECT * INTO STRICT a FROM control.ingest_activations WHERE owner_resource_id=wanted_resource AND target_epoch=e;
            IF ROW(a.dataset_id,a.target_incarnation,a.target_role_instance_id,a.producer_slot_id,a.producer_epoch,
                    a.target_manifest_sha256,a.handoff_sha256,a.activated_by_resource,a.activated_by_epoch,a.activated_by_incarnation)
                IS DISTINCT FROM ROW(s.dataset_id,s.target_incarnation,s.target_role_instance_id,s.producer_slot_id,s.producer_epoch,
                    s.target_manifest_sha256,s.handoff_sha256,s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation)
                OR a.activated_at_us<created OR (e<wanted_epoch AND a.activation_transaction=pg_current_xact_id()) THEN
                RAISE EXCEPTION 'lineage activation differs' USING ERRCODE='23514';
            END IF;
        END IF;
        SELECT count(*),coalesce(max(request_sequence),0) INTO request_count,maximum FROM
            (SELECT request_sequence FROM control.producer_requests WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=e LIMIT 65)b;
        IF request_count<>p.reserved_through_request OR maximum<>p.reserved_through_request
            OR (NOT activated AND request_count<>0)
            OR EXISTS(SELECT 1 FROM control.producer_requests r WHERE r.producer_slot_id=p.producer_slot_id AND r.producer_epoch=e
                AND (r.dataset_id<>p.dataset_id OR r.manifest_sha256<>sha256(manifest)
                    OR ROW(r.issuer_resource,r.issuer_epoch,r.issuer_incarnation)
                        IS DISTINCT FROM ROW(original.resource,original.epoch,original.incarnation))) THEN
            RAISE EXCEPTION 'lineage reservation prefix or original issuer differs' USING ERRCODE='23514';
        END IF;
        IF NOT activated AND (EXISTS(SELECT 1 FROM control.producer_outcomes WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=e)
            OR EXISTS(SELECT 1 FROM control.producer_input_bindings WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=e)
            OR EXISTS(SELECT 1 FROM control.producer_checkpoints WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=e)
            OR EXISTS(SELECT 1 FROM control.acquisition_gaps WHERE producer_slot_id=p.producer_slot_id AND producer_epoch=e)) THEN
            RAISE EXCEPTION 'unactivated lineage has target detail' USING ERRCODE='23514';
        END IF;
        IF stream.state='closed' THEN
            digest:=control.ingest_lineage_closure_digest(wanted_resource,e,NOT activated);
            IF stream.closure_digest<>digest OR (p.reserved_through_request=0 AND stream.last_complete_sequence<>0) THEN
                RAISE EXCEPTION 'lineage closed-source commitment differs' USING ERRCODE='23514';
            END IF;
        END IF;
        IF e<wanted_epoch OR wanted_epoch=2 THEN
            IF p.state<>'retired' OR stream.state<>'closed' OR o.state<>'released'
                OR o.exclusion_method<>'storage_fenced' OR o.exclusion_digest IS DISTINCT FROM digest
                OR p.closure_digest IS DISTINCT FROM digest OR p.retired_at_us<stream.closed_at_us
                OR role_row.retired_at_us IS DISTINCT FROM p.retired_at_us OR o.released_at_us IS DISTINCT FROM p.retired_at_us
                OR EXISTS(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource
                    AND owner_epoch=e AND (state<>'closed' OR closure_digest IS DISTINCT FROM digest OR closed_at_us IS DISTINCT FROM p.retired_at_us))
                OR (NOT activated AND (stream.closed_at_us IS DISTINCT FROM p.retired_at_us OR stream.last_complete_sequence<>0))
                OR (e>=3 AND NOT EXISTS(SELECT 1 FROM control.ingest_terminal_transactions WHERE owner_resource_id=wanted_resource AND target_epoch=e)) THEN
                RAISE EXCEPTION 'lineage ancestor is not exactly terminal' USING ERRCODE='23514';
            END IF;
            IF activated THEN
                PERFORM control.require_ingest_lineage_workflows(wanted_resource,e,c.epoch);
                IF NOT EXISTS(SELECT 1 FROM control.owner_epochs old JOIN control.role_instances r USING(role_instance_id)
                    WHERE old.dataset_id=s.dataset_id AND old.resource_id=original.resource AND old.epoch=original.epoch
                        AND old.process_incarnation=original.incarnation AND old.role='control' AND old.control_fence_version=1
                        AND old.state='released' AND old.exclusion_method='storage_fenced' AND old.exclusion_digest IS NOT NULL
                        AND r.dataset_id=s.dataset_id AND r.role='control' AND r.retired_at_us IS NOT NULL AND old.epoch<c.epoch) THEN
                    RAISE EXCEPTION 'lineage original Control is not positively excluded' USING ERRCODE='23514';
                END IF;
            END IF;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM control.topology_generations WHERE dataset_id=s.dataset_id AND generation=m.topology AND sealed
            AND previous_generation IS NOT DISTINCT FROM CASE WHEN m.topology=0 THEN NULL::numeric ELSE m.topology-1 END
            AND created_at_us=created)
            OR (SELECT count(*) FROM control.topology_streams WHERE dataset_id=s.dataset_id AND topology_generation=m.topology)<>e
            OR EXISTS(SELECT 1 FROM control.topology_streams member LEFT JOIN control.source_streams source
                ON source.dataset_id=member.dataset_id AND source.shard_id=member.shard_id AND source.owner_epoch=member.owner_epoch
                WHERE member.dataset_id=s.dataset_id AND member.topology_generation=m.topology
                    AND (member.shard_id<>s.shard_id OR member.owner_epoch>e OR source.owner_resource_id IS DISTINCT FROM wanted_resource
                        OR member.handoff_last_complete_sequence IS DISTINCT FROM
                            CASE WHEN member.owner_epoch=e THEN NULL::numeric ELSE source.last_complete_sequence END)) THEN
            RAISE EXCEPTION 'lineage sealed topology membership differs' USING ERRCODE='23514';
        END IF;
        previous:=manifest; old_p:=p; old_stream:=stream;
    END LOOP;
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    IF c.lease_expires_at_us<=(extract(epoch FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'lineage current Control authority expired' USING ERRCODE='23514';
    END IF;
    RETURN c;
END $$;

CREATE FUNCTION control.require_ingest_successor(wanted_resource bytea,wanted_epoch numeric,purpose text)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s control.ingest_successions%ROWTYPE; c control.owner_epochs%ROWTYPE; o control.owner_epochs%ROWTYPE;
        p control.producer_epochs%ROWTYPE; stream control.source_streams%ROWTYPE; retired bigint;
        activated boolean; ready boolean; terminal boolean; now_us bigint; digest bytea;
BEGIN
    IF purpose IS NULL OR purpose NOT IN('prepared','active','positive','source','empty','retirement','empty-transition') THEN
        RAISE EXCEPTION 'unknown lineage purpose' USING ERRCODE='23514';
    END IF;
    c:=control.lock_ingest_lineage(wanted_resource,wanted_epoch);
    SELECT * INTO STRICT s FROM control.ingest_successions WHERE owner_resource_id=wanted_resource AND target_epoch=wanted_epoch;
    SELECT * INTO STRICT o FROM control.owner_epochs WHERE resource_id=wanted_resource AND epoch=wanted_epoch;
    SELECT * INTO STRICT p FROM control.producer_epochs WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=wanted_epoch;
    SELECT * INTO STRICT stream FROM control.source_streams WHERE dataset_id=s.dataset_id AND shard_id=s.shard_id AND owner_epoch=wanted_epoch;
    SELECT retired_at_us INTO STRICT retired FROM control.role_instances WHERE role_instance_id=s.target_role_instance_id;
    activated:=EXISTS(SELECT 1 FROM control.ingest_activations WHERE owner_resource_id=wanted_resource AND target_epoch=wanted_epoch);
    IF c.resource_id<>s.prepared_by_resource OR c.epoch<s.prepared_by_epoch
        OR NOT EXISTS(SELECT 1 FROM control.owner_epochs old JOIN control.role_instances r USING(role_instance_id)
            WHERE old.dataset_id=s.dataset_id AND old.resource_id=s.prepared_by_resource AND old.epoch=s.prepared_by_epoch
                AND old.process_incarnation=s.prepared_by_incarnation AND old.role='control' AND old.control_fence_version=1
                AND r.dataset_id=s.dataset_id AND r.role='control'
                AND ((old.epoch=c.epoch AND old.process_incarnation=c.process_incarnation AND old.state='active' AND r.retired_at_us IS NULL)
                    OR (old.epoch<c.epoch AND old.state='released' AND old.exclusion_method='storage_fenced'
                        AND old.exclusion_digest IS NOT NULL AND r.retired_at_us IS NOT NULL))) THEN
        RAISE EXCEPTION 'lineage recovery requires actual current Control and excluded preparation' USING ERRCODE='23514';
    END IF;
    IF purpose IN('active','positive','source','retirement') AND NOT activated THEN
        RAISE EXCEPTION 'lineage purpose requires activation' USING ERRCODE='23514';
    END IF;
    IF purpose IN('prepared','empty','empty-transition') AND activated THEN
        RAISE EXCEPTION 'activated lineage is not an empty preparation' USING ERRCODE='23514';
    END IF;
    IF purpose IN('positive','source','retirement') AND c.epoch<=s.prepared_by_epoch THEN
        RAISE EXCEPTION 'activated lineage recovery requires fresh excluded Control' USING ERRCODE='23514';
    END IF;
    IF purpose IN('prepared','active','positive','source') THEN
        IF retired IS NOT NULL OR o.state NOT IN('active','draining')
            OR EXISTS(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource
                AND owner_epoch=wanted_epoch AND state<>'active')
            OR p.state NOT IN('open','closing') THEN
            RAISE EXCEPTION 'lineage live/recovery registry shape differs' USING ERRCODE='23514';
        END IF;
        IF purpose IN('prepared','active','positive') AND (o.state<>'active' OR stream.state<>'open') THEN
            RAISE EXCEPTION 'lineage purpose requires active owner and open source' USING ERRCODE='23514';
        END IF;
        IF purpose IN('prepared','active') AND p.state<>'open' THEN
            RAISE EXCEPTION 'lineage purpose requires open producer' USING ERRCODE='23514';
        END IF;
        IF purpose='prepared' AND (p.reserved_through_request<>0 OR o.lease_expires_at_us<>s.lease_expires_at_us) THEN
            RAISE EXCEPTION 'lineage preparation is not exactly empty' USING ERRCODE='23514';
        END IF;
        IF purpose='source' AND (stream.state NOT IN('open','closed') OR (stream.state='closed' AND p.state<>'closing')) THEN
            RAISE EXCEPTION 'lineage source requires open or complete closed source' USING ERRCODE='23514';
        END IF;
        IF purpose='active' THEN
            IF ROW(c.resource_id,c.epoch,c.process_incarnation)
                    IS DISTINCT FROM ROW(s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation)
                OR NOT EXISTS(SELECT 1 FROM control.datasets WHERE dataset_id=s.dataset_id
                    AND config_revision=s.config_revision AND policy_revision=s.policy_revision) THEN
                RAISE EXCEPTION 'lineage writer requires its actual activating Control and accepted policy' USING ERRCODE='23514';
            END IF;
            now_us:=(extract(epoch FROM clock_timestamp())*1000000)::bigint;
            IF o.lease_expires_at_us<=now_us OR c.lease_expires_at_us<=now_us THEN
                RAISE EXCEPTION 'lineage writer scheduling authority expired' USING ERRCODE='23514';
            END IF;
            RETURN least(o.lease_expires_at_us,c.lease_expires_at_us)-now_us;
        END IF;
        RETURN NULL;
    END IF;
    IF purpose='retirement' THEN
        IF stream.state<>'closed' THEN
            RAISE EXCEPTION 'lineage retirement requires prior source closure' USING ERRCODE='23514';
        END IF;
        PERFORM control.require_ingest_lineage_workflows(wanted_resource,wanted_epoch,c.epoch);
        digest:=stream.closure_digest;
    ELSE
        digest:=control.ingest_lineage_closure_digest(wanted_resource,wanted_epoch,true);
    END IF;
    ready:=retired IS NULL AND (CASE WHEN activated THEN o.state IN('active','draining') ELSE o.state='active' END)
        AND NOT EXISTS(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource
            AND owner_epoch=wanted_epoch AND state<>'active');
    IF purpose='retirement' THEN ready:=ready AND p.state='closing';
    ELSIF purpose='empty-transition' THEN
        ready:=ready AND p.state='closing' AND stream.state='closed' AND stream.last_complete_sequence=0
            AND stream.closure_digest=digest;
    ELSE ready:=ready AND p.state='open' AND stream.state='open' AND o.lease_expires_at_us=s.lease_expires_at_us;
    END IF;
    terminal:=p.state='retired' AND stream.state='closed' AND p.closure_digest=digest
        AND stream.closure_digest=digest AND p.retired_at_us>=stream.closed_at_us AND o.state='released'
        AND o.exclusion_method='storage_fenced' AND o.exclusion_digest=digest
        AND o.released_at_us=p.retired_at_us AND retired=p.retired_at_us
        AND NOT EXISTS(SELECT 1 FROM control.allocation_domains WHERE dataset_id=s.dataset_id AND owner_resource_id=wanted_resource
            AND owner_epoch=wanted_epoch AND (state<>'closed' OR closure_digest IS DISTINCT FROM digest OR closed_at_us IS DISTINCT FROM p.retired_at_us))
        AND EXISTS(SELECT 1 FROM control.ingest_terminal_transactions WHERE owner_resource_id=wanted_resource AND target_epoch=wanted_epoch);
    IF NOT activated THEN terminal:=terminal AND stream.closed_at_us=p.retired_at_us AND stream.last_complete_sequence=0; END IF;
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    IF c.lease_expires_at_us<=(extract(epoch FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'lineage retirement Control authority expired' USING ERRCODE='23514';
    END IF;
    IF ready THEN RETURN NULL; END IF;
    IF terminal AND purpose<>'empty-transition' THEN RETURN p.retired_at_us; END IF;
    RAISE EXCEPTION 'lineage retirement requires exactly ready or complete terminal metadata' USING ERRCODE='23514';
END $$;

CREATE FUNCTION control.require_empty_ingest_claims(wanted_resource bytea,wanted_epoch numeric)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s control.ingest_successions%ROWTYPE; c control.owner_epochs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT s FROM control.ingest_successions WHERE owner_resource_id=wanted_resource AND target_epoch=wanted_epoch;
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE dataset_id=s.dataset_id AND role='control' AND state='active' FOR UPDATE;
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    PERFORM j.job_id FROM control.jobs j WHERE j.dataset_id=s.dataset_id AND j.kind='acquisition'
        AND j.owner_resource_id=s.prepared_by_resource AND j.owner_epoch BETWEEN s.prepared_by_epoch AND c.epoch
        ORDER BY j.job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN RAISE EXCEPTION 'empty lineage attempt has acquisition claims' USING ERRCODE='23514'; END IF;
    PERFORM j.job_id FROM control.jobs j WHERE j.dataset_id=s.dataset_id AND j.kind='import'
        AND j.owner_resource_id=s.prepared_by_resource AND j.owner_epoch BETWEEN s.prepared_by_epoch AND c.epoch
        ORDER BY j.job_id LIMIT 1 FOR UPDATE;
    IF FOUND THEN RAISE EXCEPTION 'empty lineage attempt has import claims' USING ERRCODE='23514'; END IF;
    IF c.lease_expires_at_us<=(extract(epoch FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'empty lineage retirement Control authority expired' USING ERRCODE='23514';
    END IF;
END $$;

CREATE FUNCTION control.require_ingest_retirement_transition(wanted_slot bytea,wanted_epoch numeric,wanted_digest bytea,wanted_at bigint)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s control.ingest_successions%ROWTYPE; activated boolean; expected bytea;
BEGIN
    SELECT * INTO STRICT s FROM control.ingest_successions WHERE producer_slot_id=wanted_slot AND target_epoch=wanted_epoch;
    activated:=EXISTS(SELECT 1 FROM control.ingest_activations WHERE owner_resource_id=s.owner_resource_id AND target_epoch=wanted_epoch);
    IF control.require_ingest_successor(s.owner_resource_id,wanted_epoch,CASE WHEN activated THEN 'retirement' ELSE 'empty-transition' END) IS NOT NULL THEN
        RAISE EXCEPTION 'lineage retirement is already terminal' USING ERRCODE='23514';
    END IF;
    expected:=control.ingest_lineage_closure_digest(s.owner_resource_id,wanted_epoch,NOT activated);
    IF wanted_digest IS DISTINCT FROM expected OR wanted_at IS NULL OR wanted_at>(extract(epoch FROM clock_timestamp())*1000000)::bigint
        OR NOT EXISTS(SELECT 1 FROM control.source_streams WHERE dataset_id=s.dataset_id AND shard_id=s.shard_id AND owner_epoch=wanted_epoch
            AND state='closed' AND closed_at_us<=wanted_at AND (activated OR closed_at_us=wanted_at)) THEN
        RAISE EXCEPTION 'lineage retirement staging digest or time differs' USING ERRCODE='23514';
    END IF;
    IF NOT activated THEN PERFORM control.require_empty_ingest_claims(s.owner_resource_id,wanted_epoch); END IF;
END $$;

CREATE FUNCTION control.guard_ingest_succession() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE c control.owner_epochs%ROWTYPE;
BEGIN
    IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'lineage preparations are retained and immutable' USING ERRCODE='23514'; END IF;
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE dataset_id=NEW.dataset_id AND resource_id=NEW.prepared_by_resource
        AND epoch=NEW.prepared_by_epoch AND process_incarnation=NEW.prepared_by_incarnation AND role='control' FOR UPDATE;
    PERFORM control.require_control_owner(NEW.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    NEW.preparation_transaction:=pg_current_xact_id();
    IF NOT EXISTS(SELECT 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id
            AND config_revision=NEW.config_revision AND policy_revision=NEW.policy_revision)
        OR NEW.created_at_us>(extract(epoch FROM clock_timestamp())*1000000)::bigint
        OR NEW.created_at_us<c.acquired_at_us OR NEW.lease_expires_at_us<=(extract(epoch FROM clock_timestamp())*1000000)::bigint
        OR c.lease_expires_at_us<=(extract(epoch FROM clock_timestamp())*1000000)::bigint THEN
        RAISE EXCEPTION 'lineage preparation requires current accepted policy and live scheduling' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER ingest_succession_guard BEFORE INSERT OR UPDATE OR DELETE ON control.ingest_successions
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_succession();
CREATE TRIGGER ingest_succession_truncate_guard BEFORE TRUNCATE ON control.ingest_successions
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_ingest_succession();

CREATE FUNCTION control.guard_ingest_activation() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s control.ingest_successions%ROWTYPE; c control.owner_epochs%ROWTYPE; deadline bigint;
BEGIN
    IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'lineage activations are retained and immutable' USING ERRCODE='23514'; END IF;
    SELECT * INTO STRICT s FROM control.ingest_successions WHERE owner_resource_id=NEW.owner_resource_id AND target_epoch=NEW.target_epoch;
    IF s.preparation_transaction=pg_current_xact_id() THEN
        RAISE EXCEPTION 'lineage activation requires previously committed preparation' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_ingest_successor(s.owner_resource_id,s.target_epoch,'prepared');
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE resource_id=s.prepared_by_resource AND epoch=s.prepared_by_epoch FOR UPDATE;
    IF ROW(NEW.dataset_id,NEW.target_incarnation,NEW.target_role_instance_id,NEW.producer_slot_id,NEW.producer_epoch,
            NEW.target_manifest_sha256,NEW.handoff_sha256,NEW.activated_by_resource,NEW.activated_by_epoch,NEW.activated_by_incarnation)
        IS DISTINCT FROM ROW(s.dataset_id,s.target_incarnation,s.target_role_instance_id,s.producer_slot_id,s.producer_epoch,
            s.target_manifest_sha256,s.handoff_sha256,s.prepared_by_resource,s.prepared_by_epoch,s.prepared_by_incarnation)
        OR NOT EXISTS(SELECT 1 FROM control.datasets WHERE dataset_id=s.dataset_id
            AND config_revision=s.config_revision AND policy_revision=s.policy_revision) THEN
        RAISE EXCEPTION 'lineage activation requires exact preparation and its current policy' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    SELECT lease_expires_at_us INTO STRICT deadline FROM control.owner_epochs WHERE resource_id=s.owner_resource_id AND epoch=s.target_epoch;
    NEW.activation_transaction:=pg_current_xact_id(); NEW.activated_at_us:=(extract(epoch FROM clock_timestamp())*1000000)::bigint;
    IF NEW.activated_at_us<s.created_at_us OR deadline<=NEW.activated_at_us OR c.lease_expires_at_us<=NEW.activated_at_us THEN
        RAISE EXCEPTION 'lineage activation cannot revive expired authority' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER ingest_activation_guard BEFORE INSERT OR UPDATE OR DELETE ON control.ingest_activations
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_activation();
CREATE TRIGGER ingest_activation_truncate_guard BEFORE TRUNCATE ON control.ingest_activations
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_ingest_activation();

CREATE FUNCTION control.require_ingest_activation_commit() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    PERFORM control.require_ingest_successor(NEW.owner_resource_id,NEW.target_epoch,'active');
    IF EXISTS(SELECT 1 FROM control.producer_requests WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.target_epoch)
        OR EXISTS(SELECT 1 FROM control.producer_outcomes WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.target_epoch)
        OR EXISTS(SELECT 1 FROM control.producer_input_bindings WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.target_epoch)
        OR EXISTS(SELECT 1 FROM control.producer_checkpoints WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.target_epoch)
        OR EXISTS(SELECT 1 FROM control.acquisition_gaps WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.target_epoch) THEN
        RAISE EXCEPTION 'lineage activation must commit before all target obligations' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER ingest_activation_complete AFTER INSERT ON control.ingest_activations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_ingest_activation_commit();

-- Narrow SECURITY DEFINER trigger observers preserve ordinary Control/epoch1
-- privileges. Every relation is qualified, search_path excludes user schemas,
-- there is no dynamic SQL and PUBLIC cannot invoke the functions directly.
-- They confer no INSERT/UPDATE grant and cannot manufacture actual backend custody.
CREATE FUNCTION control.guard_ingest_lineage_role() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF TG_OP='TRUNCATE' THEN
        IF EXISTS(SELECT 1 FROM control.ingest_successions) THEN
            RAISE EXCEPTION 'anchored lineage role history cannot be truncated' USING ERRCODE='23514';
        END IF;
        RETURN NULL;
    END IF;
    IF OLD.role<>'ingest' OR NOT EXISTS(SELECT 1 FROM control.owner_epochs o JOIN control.ingest_successions s
        ON s.owner_resource_id=o.resource_id WHERE o.role_instance_id=OLD.role_instance_id AND o.role='ingest') THEN
        IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END IF;
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'anchored lineage role history cannot be deleted' USING ERRCODE='23514'; END IF;
    IF OLD.retired_at_us IS NOT NULL OR NEW.retired_at_us IS NULL
        OR (to_jsonb(NEW)-'retired_at_us') IS DISTINCT FROM (to_jsonb(OLD)-'retired_at_us') THEN
        RAISE EXCEPTION 'anchored lineage role permits only exact terminal retirement' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER ingest_lineage_role_guard BEFORE UPDATE OR DELETE ON control.role_instances
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_lineage_role();
CREATE TRIGGER ingest_lineage_role_truncate_guard BEFORE TRUNCATE ON control.role_instances
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_ingest_lineage_role();

CREATE FUNCTION control.guard_ingest_lineage_transaction() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE s control.ingest_successions%ROWTYPE; old_row jsonb:=to_jsonb(OLD); new_row jsonb:=to_jsonb(NEW);
        protected boolean; terminal control.ingest_terminal_transactions%ROWTYPE;
BEGIN
    -- A mutation fence, not an assertion about elapsed time after a caller has
    -- forced deferred constraints. Typed publication must recheck its deadline.
    FOR s IN SELECT * FROM control.ingest_successions
        WHERE preparation_transaction=pg_current_xact_id()
            OR EXISTS(SELECT 1 FROM control.ingest_activations a WHERE a.owner_resource_id=ingest_successions.owner_resource_id
                AND a.target_epoch=ingest_successions.target_epoch AND a.activation_transaction=pg_current_xact_id()) LOOP
        IF TG_TABLE_NAME='datasets' THEN
            protected:=OLD.dataset_id=s.dataset_id AND ROW(OLD.config_revision,OLD.policy_revision,OLD.topology_generation)
                IS DISTINCT FROM ROW(NEW.config_revision,NEW.policy_revision,NEW.topology_generation);
        ELSIF TG_TABLE_NAME='role_instances' THEN
            protected:=OLD.role_instance_id=s.target_role_instance_id OR EXISTS(SELECT 1 FROM control.owner_epochs
                WHERE resource_id=s.prepared_by_resource AND epoch=s.prepared_by_epoch AND role_instance_id=OLD.role_instance_id);
        ELSIF TG_TABLE_NAME='owner_epochs' THEN
            protected:=(OLD.resource_id=s.owner_resource_id AND OLD.epoch=s.target_epoch)
                OR (OLD.resource_id=s.prepared_by_resource AND OLD.epoch=s.prepared_by_epoch);
        ELSE
            protected:=old_row->>'owner_resource_id'=to_jsonb(s)->>'owner_resource_id'
                AND (old_row->>'owner_epoch')::numeric=s.target_epoch;
        END IF;
        IF protected THEN
            RAISE EXCEPTION 'lineage preparation/activation authority and target must remain unchanged through commit' USING ERRCODE='23514';
        END IF;
    END LOOP;
    -- Complete retirement itself still changes its target rows after the
    -- producer transition. Freeze only its current authority/policy envelope.
    FOR terminal IN SELECT * FROM control.ingest_terminal_transactions WHERE terminal_transaction=pg_current_xact_id() LOOP
        SELECT * INTO STRICT s FROM control.ingest_successions WHERE owner_resource_id=terminal.owner_resource_id AND target_epoch=terminal.target_epoch;
        protected:=false;
        IF TG_TABLE_NAME='datasets' THEN
            protected:=OLD.dataset_id=s.dataset_id AND ROW(OLD.config_revision,OLD.policy_revision,OLD.topology_generation)
                IS DISTINCT FROM ROW(NEW.config_revision,NEW.policy_revision,NEW.topology_generation);
        ELSIF TG_TABLE_NAME='owner_epochs' THEN
            protected:=OLD.resource_id=terminal.control_resource AND OLD.epoch=terminal.control_epoch;
        ELSIF TG_TABLE_NAME='role_instances' THEN
            protected:=EXISTS(SELECT 1 FROM control.owner_epochs WHERE resource_id=terminal.control_resource
                AND epoch=terminal.control_epoch AND role_instance_id=OLD.role_instance_id);
        END IF;
        IF protected THEN RAISE EXCEPTION 'lineage retirement authority must remain unchanged through commit' USING ERRCODE='23514'; END IF;
    END LOOP;
    RETURN NEW;
END $$;
CREATE TRIGGER ingest_lineage_transaction_guard BEFORE UPDATE ON control.datasets
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_lineage_transaction();
CREATE TRIGGER ingest_lineage_transaction_guard BEFORE UPDATE ON control.owner_epochs
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_lineage_transaction();
CREATE TRIGGER ingest_lineage_transaction_guard BEFORE UPDATE ON control.role_instances
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_lineage_transaction();
CREATE TRIGGER ingest_lineage_transaction_guard BEFORE UPDATE ON control.allocation_domains
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_lineage_transaction();
CREATE TRIGGER ingest_lineage_transaction_guard BEFORE UPDATE ON control.source_streams
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_lineage_transaction();
CREATE TRIGGER ingest_lineage_transaction_guard BEFORE UPDATE ON control.producer_epochs
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_lineage_transaction();

CREATE FUNCTION control.guard_ingest_obligation() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s control.ingest_successions%ROWTYPE; a control.ingest_activations%ROWTYPE;
BEGIN
    IF NEW.producer_epoch<3 THEN RETURN NEW; END IF;
    SELECT * INTO STRICT s FROM control.ingest_successions WHERE producer_slot_id=NEW.producer_slot_id AND target_epoch=NEW.producer_epoch;
    SELECT * INTO STRICT a FROM control.ingest_activations WHERE owner_resource_id=s.owner_resource_id AND target_epoch=s.target_epoch;
    IF s.preparation_transaction=pg_current_xact_id() OR a.activation_transaction=pg_current_xact_id() THEN
        RAISE EXCEPTION 'lineage obligations require a previously committed activation' USING ERRCODE='23514';
    END IF;
    IF EXISTS(SELECT 1 FROM control.source_streams WHERE dataset_id=s.dataset_id AND shard_id=s.shard_id
        AND owner_epoch=s.target_epoch AND state='closed') THEN
        RAISE EXCEPTION 'closed lineage source cannot acquire new obligations or outcome copies' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER ingest_obligation_guard BEFORE INSERT OR UPDATE ON control.producer_checkpoints
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_obligation();
CREATE TRIGGER ingest_obligation_guard BEFORE INSERT ON control.producer_outcomes
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_obligation();

CREATE FUNCTION control.record_ingest_terminal_transaction() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c control.owner_epochs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE dataset_id=NEW.dataset_id AND role='control' AND state='active';
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    INSERT INTO control.ingest_terminal_transactions VALUES(NEW.owner_resource_id,NEW.owner_epoch,pg_current_xact_id(),
        c.resource_id,c.epoch,c.process_incarnation);
    RETURN NULL;
END $$;
CREATE TRIGGER ingest_terminal_transaction AFTER UPDATE ON control.producer_epochs FOR EACH ROW
    WHEN(NEW.owner_epoch>=3 AND NEW.state='retired') EXECUTE FUNCTION control.record_ingest_terminal_transaction();
CREATE TRIGGER ingest_terminal_transaction_immutable BEFORE UPDATE OR DELETE ON control.ingest_terminal_transactions
    FOR EACH ROW EXECUTE FUNCTION control.reject_history_mutation();
CREATE TRIGGER ingest_terminal_transaction_truncate BEFORE TRUNCATE ON control.ingest_terminal_transactions
    FOR EACH STATEMENT EXECUTE FUNCTION control.reject_history_mutation();

CREATE FUNCTION control.guard_ingest_terminal_claim() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF NEW.kind NOT IN('acquisition','import') OR NEW.owner_resource_id IS NULL THEN RETURN NEW; END IF;
    -- A retained claim's ordinary progress/recovery is not a new workflow.
    IF TG_OP='UPDATE' AND ROW(NEW.dataset_id,NEW.kind,NEW.owner_resource_id,NEW.owner_epoch,NEW.owner_incarnation)
        IS NOT DISTINCT FROM ROW(OLD.dataset_id,OLD.kind,OLD.owner_resource_id,OLD.owner_epoch,OLD.owner_incarnation) THEN
        RETURN NEW;
    END IF;
    -- Serialize the observation with preparation/activation's Control UPDATE
    -- lock before reading receipts. The old generic import guard otherwise
    -- takes its SHARE lock only later in alphabetical trigger order. Preserve
    -- that legacy tuple authority (no new actual-backend/grant requirement).
    PERFORM 1 FROM control.owner_epochs WHERE dataset_id=NEW.dataset_id AND resource_id=NEW.owner_resource_id
        AND epoch=NEW.owner_epoch AND role='control' AND process_incarnation=NEW.owner_incarnation FOR SHARE;
    -- Preparation is already committed before activation. Checking that pending
    -- head also closes claim-before-activation in one transaction, including
    -- subtransactions. Historical jobs are never retroactively reclassified.
    IF EXISTS(SELECT 1 FROM control.ingest_successions s JOIN control.producer_epochs p
        ON p.producer_slot_id=s.producer_slot_id AND p.producer_epoch=s.target_epoch
        WHERE s.dataset_id=NEW.dataset_id AND s.prepared_by_resource=NEW.owner_resource_id
            AND s.prepared_by_epoch=NEW.owner_epoch AND s.prepared_by_incarnation=NEW.owner_incarnation
            AND p.state<>'retired' AND NOT EXISTS(SELECT 1 FROM control.ingest_successions later
                WHERE later.owner_resource_id=s.owner_resource_id AND later.target_epoch>s.target_epoch)
            AND NOT EXISTS(SELECT 1 FROM control.ingest_activations a WHERE a.owner_resource_id=s.owner_resource_id
                AND a.target_epoch=s.target_epoch AND a.activation_transaction<>pg_current_xact_id())) THEN
        RAISE EXCEPTION 'lineage workflow claims require previously committed activation' USING ERRCODE='23514';
    END IF;
    IF EXISTS(SELECT 1 FROM control.ingest_terminal_transactions t JOIN control.ingest_successions s
        ON s.owner_resource_id=t.owner_resource_id AND s.target_epoch=t.target_epoch
        WHERE t.terminal_transaction=pg_current_xact_id() AND s.dataset_id=NEW.dataset_id
            AND NEW.owner_resource_id=s.prepared_by_resource AND NEW.owner_epoch BETWEEN s.prepared_by_epoch AND t.control_epoch
            AND NOT EXISTS(SELECT 1 FROM control.ingest_activations a WHERE a.owner_resource_id=s.owner_resource_id AND a.target_epoch=s.target_epoch)) THEN
        RAISE EXCEPTION 'empty lineage terminal transaction cannot acquire workflow claims' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER ingest_terminal_claim_guard BEFORE INSERT OR UPDATE ON control.jobs
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_terminal_claim();

CREATE FUNCTION control.require_complete_ingest_lineage() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE data jsonb:=to_jsonb(NEW); dataset bytea; resource bytea; head numeric;
        s control.ingest_successions%ROWTYPE; p control.producer_epochs%ROWTYPE;
        stream control.source_streams%ROWTYPE; activated boolean; completed bigint;
BEGIN
    dataset:=NEW.dataset_id;
    IF TG_TABLE_NAME='role_instances' THEN
        SELECT o.resource_id INTO resource FROM control.owner_epochs o WHERE o.role_instance_id=NEW.role_instance_id AND o.role='ingest' AND o.epoch>=3;
        IF NOT FOUND THEN RETURN NULL; END IF;
    ELSIF TG_TABLE_NAME='owner_epochs' THEN
        IF NEW.role<>'ingest' THEN RETURN NULL; END IF;
        resource:=NEW.resource_id;
    END IF;
    IF (SELECT count(*) FROM(SELECT DISTINCT owner_resource_id FROM control.ingest_successions WHERE dataset_id=dataset LIMIT 2)b)>1 THEN
        RAISE EXCEPTION 'dataset has multiple lineage resources' USING ERRCODE='23514';
    END IF;
    SELECT owner_resource_id,max(target_epoch) INTO resource,head FROM control.ingest_successions
        WHERE dataset_id=dataset GROUP BY owner_resource_id;
    IF NOT FOUND THEN
        IF coalesce((data->>'owner_epoch')::numeric,(data->>'epoch')::numeric,(data->>'target_epoch')::numeric,0)>=3 THEN
            RAISE EXCEPTION 'new lineage parents require their complete preparation' USING ERRCODE='23514';
        END IF;
        RETURN NULL;
    END IF;
    SELECT * INTO STRICT s FROM control.ingest_successions WHERE owner_resource_id=resource AND target_epoch=head;
    SELECT * INTO STRICT p FROM control.producer_epochs WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=head;
    SELECT * INTO STRICT stream FROM control.source_streams WHERE dataset_id=s.dataset_id AND shard_id=s.shard_id AND owner_epoch=head;
    activated:=EXISTS(SELECT 1 FROM control.ingest_activations WHERE owner_resource_id=resource AND target_epoch=head);
    IF NOT activated THEN
        completed:=control.require_ingest_successor(resource,head,'empty');
        IF ((TG_OP='UPDATE' AND TG_TABLE_NAME IN('owner_epochs','producer_epochs','source_streams','allocation_domains','role_instances'))
            OR p.state<>'open' OR stream.state<>'open') AND completed IS NULL THEN
            RAISE EXCEPTION 'empty lineage lifecycle changes must commit complete retirement' USING ERRCODE='23514';
        END IF;
        IF EXISTS(SELECT 1 FROM control.ingest_terminal_transactions WHERE owner_resource_id=resource AND target_epoch=head
            AND terminal_transaction=pg_current_xact_id()) THEN PERFORM control.require_empty_ingest_claims(resource,head); END IF;
    ELSIF p.state='retired' OR EXISTS(SELECT 1 FROM control.owner_epochs WHERE resource_id=resource AND epoch=head AND state='released')
        OR EXISTS(SELECT 1 FROM control.allocation_domains WHERE owner_resource_id=resource AND owner_epoch=head AND state='closed')
        OR EXISTS(SELECT 1 FROM control.role_instances WHERE role_instance_id=s.target_role_instance_id AND retired_at_us IS NOT NULL) THEN
        IF control.require_ingest_successor(resource,head,'retirement') IS NULL THEN
            RAISE EXCEPTION 'lineage terminal writes must commit the complete terminal shape' USING ERRCODE='23514';
        END IF;
    ELSIF p.state='open' AND stream.state='open' AND EXISTS(SELECT 1 FROM control.owner_epochs
        WHERE resource_id=resource AND epoch=head AND state='active') THEN
        PERFORM control.require_ingest_successor(resource,head,'active');
    ELSE
        PERFORM control.require_ingest_successor(resource,head,'source');
        IF TG_TABLE_NAME='source_streams' AND stream.state<>'closed' THEN
            RAISE EXCEPTION 'lineage source changes must commit complete closure' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER ingest_preparation_complete AFTER INSERT ON control.ingest_successions
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_complete_ingest_lineage();
CREATE CONSTRAINT TRIGGER ingest_owner_complete AFTER INSERT OR UPDATE ON control.owner_epochs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN(NEW.role='ingest') EXECUTE FUNCTION control.require_complete_ingest_lineage();
CREATE CONSTRAINT TRIGGER ingest_producer_complete AFTER INSERT OR UPDATE ON control.producer_epochs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_complete_ingest_lineage();
CREATE CONSTRAINT TRIGGER ingest_source_complete AFTER INSERT OR UPDATE ON control.source_streams
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_complete_ingest_lineage();
CREATE CONSTRAINT TRIGGER ingest_domain_complete AFTER INSERT OR UPDATE ON control.allocation_domains
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_complete_ingest_lineage();
CREATE CONSTRAINT TRIGGER ingest_role_complete AFTER UPDATE ON control.role_instances
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN(NEW.role='ingest') EXECUTE FUNCTION control.require_complete_ingest_lineage();
CREATE CONSTRAINT TRIGGER ingest_topology_complete AFTER INSERT OR UPDATE ON control.topology_generations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_complete_ingest_lineage();
CREATE CONSTRAINT TRIGGER ingest_membership_complete AFTER INSERT ON control.topology_streams
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION control.require_complete_ingest_lineage();
CREATE CONSTRAINT TRIGGER ingest_dataset_head_complete AFTER UPDATE ON control.datasets
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN(OLD.topology_generation IS DISTINCT FROM NEW.topology_generation)
    EXECUTE FUNCTION control.require_complete_ingest_lineage();

CREATE FUNCTION control.guard_ingest_terminal_receipt() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s control.ingest_successions%ROWTYPE; c control.owner_epochs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT s FROM control.ingest_successions WHERE owner_resource_id=NEW.owner_resource_id AND target_epoch=NEW.target_epoch;
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE dataset_id=s.dataset_id AND role='control' AND state='active' FOR UPDATE;
    PERFORM control.require_control_owner(c.dataset_id,c.resource_id,c.epoch,c.process_incarnation);
    IF NOT EXISTS(SELECT 1 FROM control.producer_epochs WHERE producer_slot_id=s.producer_slot_id AND producer_epoch=s.target_epoch AND state='retired') THEN
        RAISE EXCEPTION 'lineage terminal transaction requires its retired producer' USING ERRCODE='23514';
    END IF;
    NEW.terminal_transaction:=pg_current_xact_id();
    NEW.control_resource:=c.resource_id; NEW.control_epoch:=c.epoch; NEW.control_incarnation:=c.process_incarnation;
    RETURN NEW;
END $$;
CREATE TRIGGER ingest_terminal_receipt_guard BEFORE INSERT ON control.ingest_terminal_transactions
    FOR EACH ROW EXECUTE FUNCTION control.guard_ingest_terminal_receipt();

CREATE FUNCTION control.guard_ingest_lineage_truncate() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM control.ingest_successions) THEN
        RAISE EXCEPTION 'anchored lineage registry history cannot be truncated' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER ingest_lineage_truncate BEFORE TRUNCATE ON control.owner_epochs
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_ingest_lineage_truncate();
CREATE TRIGGER ingest_lineage_truncate BEFORE TRUNCATE ON control.producer_slots
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_ingest_lineage_truncate();
CREATE TRIGGER ingest_lineage_truncate BEFORE TRUNCATE ON control.producer_epochs
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_ingest_lineage_truncate();
CREATE TRIGGER ingest_lineage_truncate BEFORE TRUNCATE ON control.source_streams
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_ingest_lineage_truncate();
CREATE TRIGGER ingest_lineage_truncate BEFORE TRUNCATE ON control.allocation_domains
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_ingest_lineage_truncate();
CREATE TRIGGER ingest_lineage_truncate BEFORE TRUNCATE ON control.topology_generations
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_ingest_lineage_truncate();
CREATE TRIGGER ingest_lineage_truncate BEFORE TRUNCATE ON control.topology_streams
    FOR EACH STATEMENT EXECUTE FUNCTION control.guard_ingest_lineage_truncate();

-- Request/input binding admission intentionally does not take the producer lock
-- ahead of the existing import job guard. Full typed writer validation is a
-- separate operation; immutable preparation/activation establish this binding.
CREATE FUNCTION control.require_ingest_admission(wanted_slot bytea,wanted_epoch numeric,wanted_dataset bytea,
    wanted_manifest bytea,issuer_resource bytea,issuer_epoch numeric,issuer_incarnation bytea)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE s control.ingest_successions%ROWTYPE; a control.ingest_activations%ROWTYPE;
        c control.owner_epochs%ROWTYPE; o control.owner_epochs%ROWTYPE; p control.producer_epochs%ROWTYPE; now_us bigint;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'lineage admission requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    SELECT * INTO STRICT s FROM control.ingest_successions WHERE producer_slot_id=wanted_slot AND target_epoch=wanted_epoch;
    SELECT * INTO STRICT a FROM control.ingest_activations WHERE owner_resource_id=s.owner_resource_id AND target_epoch=wanted_epoch;
    IF a.activation_transaction=pg_current_xact_id() OR s.preparation_transaction=pg_current_xact_id() THEN
        RAISE EXCEPTION 'lineage admission requires previously committed activation' USING ERRCODE='23514';
    END IF;
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE resource_id=issuer_resource AND epoch=issuer_epoch FOR UPDATE;
    PERFORM control.require_control_owner(wanted_dataset,issuer_resource,issuer_epoch,issuer_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=wanted_dataset FOR UPDATE;
    SELECT * INTO STRICT o FROM control.owner_epochs WHERE resource_id=s.owner_resource_id AND epoch=wanted_epoch FOR SHARE;
    SELECT * INTO STRICT p FROM control.producer_epochs WHERE producer_slot_id=wanted_slot AND producer_epoch=wanted_epoch;
    IF ROW(wanted_dataset,wanted_manifest,issuer_resource,issuer_epoch,issuer_incarnation)
        IS DISTINCT FROM ROW(s.dataset_id::bytea,s.target_manifest_sha256::bytea,a.activated_by_resource::bytea,a.activated_by_epoch::numeric,a.activated_by_incarnation::bytea)
        OR p.dataset_id<>s.dataset_id OR p.owner_resource_id<>s.owner_resource_id OR p.owner_epoch<>wanted_epoch
        OR p.owner_incarnation<>s.target_incarnation OR p.producer_incarnation<>s.target_incarnation
        OR p.producer_role_instance_id<>s.target_role_instance_id OR p.state<>'open'
        OR o.state<>'active' OR o.process_incarnation<>s.target_incarnation OR o.role_instance_id<>s.target_role_instance_id
        OR NOT EXISTS(SELECT 1 FROM control.datasets WHERE dataset_id=s.dataset_id AND topology_generation=s.target_topology
            AND config_revision=s.config_revision AND policy_revision=s.policy_revision) THEN
        RAISE EXCEPTION 'lineage request requires its exact activated manifest and actual Control' USING ERRCODE='23514';
    END IF;
    now_us:=(extract(epoch FROM clock_timestamp())*1000000)::bigint;
    IF c.lease_expires_at_us<=now_us OR o.lease_expires_at_us<=now_us THEN
        RAISE EXCEPTION 'lineage admission scheduling authority expired' USING ERRCODE='23514';
    END IF;
END $$;

-- Shared legacy dispatch: unchanged existing bodies below, with one numerical
-- family branch. OR REPLACE preserves the established function ACLs/OIDs.
ALTER TABLE control.acquisition_gaps DROP CONSTRAINT acquisition_gap_epoch_reason,
    ADD CONSTRAINT acquisition_gap_epoch_reason CHECK (
        (producer_epoch=1 AND source_owner_epoch=1 AND reason='initial_producer_abandoned')
        OR (producer_epoch=2 AND source_owner_epoch=2 AND reason='activated_initial_producer_abandoned')
        OR (producer_epoch BETWEEN 3 AND 255 AND source_owner_epoch=producer_epoch AND reason='ingest_successor_abandoned'));

CREATE OR REPLACE FUNCTION control.veto_unactivated_producer() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog AS $$
DECLARE a control.initial_ingest_activations%ROWTYPE;
        p control.producer_epochs%ROWTYPE;
        c control.owner_epochs%ROWTYPE;
        i control.owner_epochs%ROWTYPE;
        now_us bigint;
BEGIN
    SELECT * INTO STRICT p FROM control.producer_epochs
        WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch;
    IF NEW.producer_epoch=1 AND p.owner_epoch=1 THEN RETURN NEW; END IF;
    IF NEW.producer_epoch>=3 AND p.owner_epoch=NEW.producer_epoch THEN
        PERFORM control.require_ingest_admission(NEW.producer_slot_id,NEW.producer_epoch,NEW.dataset_id,
            NEW.manifest_sha256,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation);
        RETURN NEW;
    END IF;
    IF NEW.producer_epoch<>2 OR p.owner_epoch<>2 THEN
        RAISE EXCEPTION 'producer epoch has no selected activation protocol' USING ERRCODE='23514';
    END IF;
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'activated requests require READ COMMITTED' USING ERRCODE='25001';
    END IF;
    SELECT * INTO STRICT a FROM control.initial_ingest_activations WHERE producer_slot_id=NEW.producer_slot_id;
    IF a.activation_transaction=pg_current_xact_id() THEN
        RAISE EXCEPTION 'target admission requires a previously committed activation' USING ERRCODE='23514';
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation);
    PERFORM 1 FROM control.datasets WHERE dataset_id=NEW.dataset_id FOR UPDATE;
    SELECT * INTO STRICT c FROM control.owner_epochs WHERE resource_id=NEW.issuer_resource AND epoch=NEW.issuer_epoch FOR SHARE;
    SELECT * INTO STRICT i FROM control.owner_epochs WHERE resource_id=p.owner_resource_id AND epoch=2 FOR SHARE;
    IF ROW(NEW.dataset_id,NEW.manifest_sha256,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation)
        IS DISTINCT FROM ROW(a.dataset_id,a.target_manifest_sha256,a.activated_by_resource,a.activated_by_epoch,a.activated_by_incarnation)
        OR p.dataset_id<>a.dataset_id OR p.owner_resource_id<>a.owner_resource_id
        OR p.owner_incarnation<>a.target_incarnation OR p.producer_incarnation<>a.target_incarnation
        OR p.producer_role_instance_id<>a.target_role_instance_id OR p.state<>'open'
        OR i.state<>'active' OR i.process_incarnation<>a.target_incarnation OR i.role_instance_id<>a.target_role_instance_id
        OR NOT EXISTS (SELECT 1 FROM control.datasets WHERE dataset_id=a.dataset_id
            AND config_revision=a.config_revision AND policy_revision=a.policy_revision) THEN
        RAISE EXCEPTION 'request requires its activated target manifest and actual activating Control' USING ERRCODE='23514';
    END IF;
    now_us := (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;
    IF c.lease_expires_at_us<=now_us OR i.lease_expires_at_us<=now_us THEN
        RAISE EXCEPTION 'activated request scheduling authority expired' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION control.guard_producer_epoch() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, control AS $$
DECLARE prior control.producer_epochs%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'producer-epoch history is retained' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'INSERT' AND NEW.reserved_through_request <> 0 THEN
        RAISE EXCEPTION 'new producer epoch starts with zero reserved requests' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.reserved_through_request <> OLD.reserved_through_request THEN
            -- Only the AFTER INSERT reservation trigger has a new retained row.
            -- Compare the complete row except this one monotone counter.
            IF (to_jsonb(NEW) - 'reserved_through_request') IS DISTINCT FROM
                   (to_jsonb(OLD) - 'reserved_through_request')
                OR OLD.state <> 'open'
                OR NEW.reserved_through_request <> OLD.reserved_through_request + 1
                OR NOT EXISTS (SELECT 1 FROM control.producer_requests
                    WHERE producer_slot_id = NEW.producer_slot_id
                        AND producer_epoch = NEW.producer_epoch
                        AND request_sequence = NEW.reserved_through_request) THEN
                RAISE EXCEPTION 'reservation counter requires its exact new retained input row'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        -- Preserve epoch1 and unactivated empty staging. Only a retained
        -- activation selects the separate epoch2 retirement protocol.
        IF NEW.state = 'retired' THEN
            IF NEW.producer_epoch>=3 AND NEW.owner_epoch=NEW.producer_epoch THEN
                PERFORM control.require_ingest_retirement_transition(
                    NEW.producer_slot_id,NEW.producer_epoch,NEW.closure_digest,NEW.retired_at_us);
            ELSIF NEW.producer_epoch = 2 AND NEW.owner_epoch = 2 THEN
                IF EXISTS (SELECT 1 FROM control.initial_ingest_activations WHERE producer_slot_id=NEW.producer_slot_id) THEN
                    PERFORM control.require_activated_initial_retirement_transition(
                        NEW.producer_slot_id, NEW.closure_digest, NEW.retired_at_us);
                ELSE
                    PERFORM control.require_initial_empty_transition(
                        NEW.producer_slot_id, NEW.closure_digest, NEW.retired_at_us);
                END IF;
            ELSE
                PERFORM control.require_initial_retirement(
                    NEW.producer_slot_id, NEW.producer_epoch, NEW.closure_digest);
            END IF;
        END IF;
    END IF;
    IF TG_OP = 'INSERT' THEN
        PERFORM 1 FROM control.producer_slots WHERE producer_slot_id = NEW.producer_slot_id FOR UPDATE;
        PERFORM 1 FROM control.role_instances
            WHERE role_instance_id = NEW.producer_role_instance_id
                AND dataset_id = NEW.dataset_id AND role = NEW.producer_role
                AND retired_at_us IS NULL FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'producer epoch requires a live registered producer role'
                USING ERRCODE = '23514';
        END IF;
        SELECT * INTO prior FROM control.producer_epochs
            WHERE producer_slot_id = NEW.producer_slot_id
            ORDER BY producer_epoch DESC LIMIT 1;
        IF NEW.state <> 'open'
            OR (NOT FOUND AND NEW.producer_epoch <> 1)
            OR (FOUND AND (prior.state <> 'retired'
                OR NEW.producer_epoch <> prior.producer_epoch + 1
                OR NEW.dataset_id <> prior.dataset_id)) THEN
            RAISE EXCEPTION 'producer epoch requires the next ordinal and retired predecessor'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        IF ROW(OLD.producer_slot_id, OLD.producer_epoch, OLD.dataset_id,
                OLD.producer_role_instance_id, OLD.producer_role, OLD.producer_incarnation,
                OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_role,
                OLD.owner_incarnation, OLD.outstanding_request_limit, OLD.opened_at_us)
            IS DISTINCT FROM
               ROW(NEW.producer_slot_id, NEW.producer_epoch, NEW.dataset_id,
                NEW.producer_role_instance_id, NEW.producer_role, NEW.producer_incarnation,
                NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_role,
                NEW.owner_incarnation, NEW.outstanding_request_limit, NEW.opened_at_us)
            OR OLD.state = 'retired'
            OR NOT ((OLD.state = 'open' AND NEW.state = 'closing')
                OR (OLD.state = 'closing' AND NEW.state = 'retired')) THEN
            RAISE EXCEPTION 'invalid producer-epoch transition' USING ERRCODE = '23514';
        END IF;
    END IF;
    PERFORM 1 FROM control.owner_epochs
        WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.owner_resource_id
            AND epoch = NEW.owner_epoch AND role = NEW.owner_role
            AND process_incarnation = NEW.owner_incarnation
            AND state IN ('active', 'draining') FOR SHARE;
    IF NOT FOUND OR (TG_OP = 'INSERT' AND NOT EXISTS (
        SELECT 1 FROM control.owner_epochs
        WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.owner_resource_id
            AND epoch = NEW.owner_epoch AND role = NEW.owner_role
            AND process_incarnation = NEW.owner_incarnation AND state = 'active')) THEN
        RAISE EXCEPTION 'producer-epoch mutation requires its exact live ingest owner'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION control.guard_acquisition_gap() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE job control.jobs%ROWTYPE; activation control.initial_ingest_activations%ROWTYPE; successor control.ingest_activations%ROWTYPE;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'acquisition abandonment requires READ COMMITTED' USING ERRCODE = '25001';
    END IF;
    IF NEW.producer_epoch>=3 THEN
        PERFORM 1 FROM control.owner_epochs WHERE dataset_id=NEW.dataset_id
            AND resource_id=NEW.issuer_resource AND epoch=NEW.issuer_epoch FOR UPDATE;
    END IF;
    PERFORM control.require_control_owner(NEW.dataset_id, NEW.issuer_resource,
        NEW.issuer_epoch, NEW.issuer_incarnation);
    -- The connected adapter takes this before registry/source/producer locks.
    PERFORM 1 FROM control.datasets WHERE dataset_id = NEW.dataset_id FOR UPDATE;
    IF NEW.producer_epoch>=3 THEN
        SELECT * INTO STRICT successor FROM control.ingest_activations
            WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch;
        PERFORM control.require_ingest_successor(successor.owner_resource_id,NEW.producer_epoch,'source');
        IF ROW(NEW.dataset_id,NEW.source_owner_epoch,NEW.reason,
            NEW.previous_owner_resource,NEW.previous_owner_epoch,NEW.previous_owner_incarnation)
            IS DISTINCT FROM ROW(successor.dataset_id,successor.target_epoch,'ingest_successor_abandoned'::text,
                successor.activated_by_resource,successor.activated_by_epoch,successor.activated_by_incarnation) THEN
            RAISE EXCEPTION 'lineage acquisition gap requires its activating Control' USING ERRCODE='23514';
        END IF;
    END IF;
    IF NEW.producer_epoch = 2 THEN
        SELECT * INTO STRICT activation FROM control.initial_ingest_activations
            WHERE producer_slot_id = NEW.producer_slot_id;
        PERFORM control.require_activated_initial_source(activation.owner_resource_id);
        IF (NEW.dataset_id, NEW.source_owner_epoch, NEW.reason,
            NEW.previous_owner_resource, NEW.previous_owner_epoch, NEW.previous_owner_incarnation)
            IS DISTINCT FROM (activation.dataset_id, 2::numeric, 'activated_initial_producer_abandoned'::text,
                activation.activated_by_resource, activation.activated_by_epoch, activation.activated_by_incarnation) THEN
            RAISE EXCEPTION 'activated acquisition gap requires its activating Control'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    PERFORM 1 FROM control.owner_epochs
        WHERE dataset_id = NEW.dataset_id AND resource_id = NEW.previous_owner_resource
            AND epoch = NEW.previous_owner_epoch AND role = 'control'
            AND process_incarnation = NEW.previous_owner_incarnation
            AND state = 'released' AND exclusion_method = 'storage_fenced' FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'old acquisition Control must be positively fenced' USING ERRCODE = '23514';
    END IF;
    PERFORM 1 FROM control.source_streams s
        JOIN control.producer_epochs p ON p.dataset_id = s.dataset_id
            AND p.owner_resource_id = s.owner_resource_id AND p.owner_epoch = s.owner_epoch
            AND p.owner_incarnation = s.owner_incarnation
        JOIN control.producer_slots slot ON slot.producer_slot_id = p.producer_slot_id
        WHERE s.dataset_id = NEW.dataset_id AND s.shard_id = NEW.shard_id
            AND s.owner_epoch = NEW.source_owner_epoch AND s.state = 'closed'
            AND s.last_complete_sequence = NEW.last_complete_sequence
            AND s.closure_digest = NEW.source_closure_digest
            AND s.closed_at_us <= NEW.abandoned_at_us
            AND p.producer_slot_id = NEW.producer_slot_id AND p.producer_epoch = NEW.producer_epoch
            AND p.state = 'closing' AND p.reserved_through_request <= 64
            AND (NEW.producer_epoch >= 2 OR
                (slot.registered_by_resource_id, slot.registered_by_epoch, slot.registered_by_incarnation)
                    = (NEW.previous_owner_resource, NEW.previous_owner_epoch, NEW.previous_owner_incarnation))
            AND NOT EXISTS (
                SELECT 1 FROM control.producer_requests r
                LEFT JOIN control.producer_outcomes o USING (producer_slot_id, producer_epoch, request_sequence)
                WHERE r.producer_slot_id = p.producer_slot_id AND r.producer_epoch = p.producer_epoch
                    AND o.request_sequence IS NULL)
        FOR SHARE OF s, p;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'acquisition gap requires its closed resolved initial source' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT job FROM control.jobs WHERE job_id = NEW.job_id FOR UPDATE;
    IF job.dataset_id <> NEW.dataset_id OR job.acquisition_job_protocol IS DISTINCT FROM 1
        OR job.state <> 'queued' OR job.checkpoint_revision <> 0 OR NOT job.input_complete
        OR job.job_revision <> NEW.previous_job_revision
        OR (job.owner_resource_id, job.owner_epoch, job.owner_incarnation)
            IS DISTINCT FROM (NEW.previous_owner_resource, NEW.previous_owner_epoch, NEW.previous_owner_incarnation)
        OR NEW.abandoned_at_us < job.created_at_us
        OR NEW.abandoned_at_us > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint THEN
        RAISE EXCEPTION 'acquisition gap must describe its original queued claim' USING ERRCODE = '23514';
    END IF;
    IF NEW.producer_epoch>=3 THEN
        IF (SELECT count(*) FROM(
            SELECT job_id FROM control.acquisition_gaps WHERE producer_slot_id=NEW.producer_slot_id AND producer_epoch=NEW.producer_epoch
            UNION SELECT job_id FROM control.jobs WHERE dataset_id=NEW.dataset_id AND kind='acquisition'
                AND owner_resource_id=successor.activated_by_resource AND owner_epoch=successor.activated_by_epoch
                AND owner_incarnation=successor.activated_by_incarnation LIMIT 65)b)>64 THEN
            RAISE EXCEPTION 'lineage acquisition abandonment bound exceeded' USING ERRCODE='23514';
        END IF;
        PERFORM control.require_control_owner(NEW.dataset_id,NEW.issuer_resource,NEW.issuer_epoch,NEW.issuer_incarnation);
        IF NOT EXISTS(SELECT 1 FROM control.owner_epochs WHERE resource_id=NEW.issuer_resource AND epoch=NEW.issuer_epoch
            AND lease_expires_at_us>(extract(epoch FROM clock_timestamp())*1000000)::bigint) THEN
            RAISE EXCEPTION 'lineage acquisition authority expired after job wait' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION control.guard_acquisition_claim_owner() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF (NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation)
        IS DISTINCT FROM (OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_incarnation) THEN
        IF OLD.state <> 'queued' OR NEW.state <> 'cancel_requested'
            OR OLD.checkpoint_revision <> 0 OR NEW.checkpoint_revision <> 0
            OR NEW.job_revision <> OLD.job_revision + 1
            OR NEW.reason_code NOT IN ('initial_producer_abandoned', 'activated_initial_producer_abandoned', 'ingest_successor_abandoned')
            OR NEW.reason_code IS NULL
            OR NOT EXISTS (SELECT 1 FROM control.acquisition_gaps g
                WHERE g.job_id = OLD.job_id AND g.dataset_id = OLD.dataset_id
                    AND g.reason = NEW.reason_code
                    AND ((g.producer_epoch = 1 AND g.source_owner_epoch = 1 AND g.reason = 'initial_producer_abandoned')
                        OR (g.producer_epoch = 2 AND g.source_owner_epoch = 2 AND g.reason = 'activated_initial_producer_abandoned')
                        OR (g.producer_epoch BETWEEN 3 AND 255 AND g.source_owner_epoch=g.producer_epoch AND g.reason='ingest_successor_abandoned'))
                    AND g.previous_job_revision = OLD.job_revision
                    AND (g.previous_owner_resource, g.previous_owner_epoch, g.previous_owner_incarnation)
                        = (OLD.owner_resource_id, OLD.owner_epoch, OLD.owner_incarnation)
                    AND (g.issuer_resource, g.issuer_epoch, g.issuer_incarnation)
                        = (NEW.owner_resource_id, NEW.owner_epoch, NEW.owner_incarnation)) THEN
            RAISE EXCEPTION 'acquisition owner replacement requires explicit custody recovery'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END $$;

-- All new entry points and relations are explicitly provisioned, never PUBLIC.
REVOKE ALL ON control.ingest_successions,control.ingest_activations,control.ingest_terminal_transactions FROM PUBLIC;
REVOKE ALL ON FUNCTION
    control.ingest_lineage_uint(bytea,integer,integer),
    control.ingest_lineage_u64(numeric),
    control.ingest_lineage_manifest(bytea),
    control.ingest_lineage_manifest_at(bytea,numeric),
    control.ingest_lineage_handoff_at(bytea,numeric),
    control.ingest_lineage_activation(bytea,numeric),
    control.ingest_lineage_closure_digest(bytea,numeric,boolean),
    control.require_ingest_lineage_workflows(bytea,numeric,numeric),
    control.lock_ingest_lineage(bytea,numeric),
    control.require_ingest_successor(bytea,numeric,text),
    control.require_empty_ingest_claims(bytea,numeric),
    control.require_ingest_retirement_transition(bytea,numeric,bytea,bigint),
    control.guard_ingest_succession(),
    control.guard_ingest_activation(),
    control.require_ingest_activation_commit(),
    control.guard_ingest_lineage_role(),
    control.guard_ingest_lineage_transaction(),
    control.guard_ingest_obligation(),
    control.record_ingest_terminal_transaction(),
    control.guard_ingest_terminal_claim(),
    control.require_complete_ingest_lineage(),
    control.guard_ingest_terminal_receipt(),
    control.guard_ingest_lineage_truncate(),
    control.require_ingest_admission(bytea,numeric,bytea,bytea,bytea,numeric,bytea) FROM PUBLIC;
