use super::query::lookup_attached;
use bigbrotr_protocol::archive_event::{
    EventMatch, EventQueryObservation, EventSelection, QUERY_FORMAT,
};

fn present(observation: EventQueryObservation) -> EventMatch {
    assert_eq!(observation.format, QUERY_FORMAT);
    match observation.result {
        EventSelection::Present { matched } => {
            assert_eq!(matched.snapshot_id, observation.snapshot_id);
            *matched
        }
        EventSelection::Absent {} => panic!("expected a signed event"),
    }
}
use super::*;
use crate::{
    engine::{Engine, EngineLimits},
    rows::{RowLimits, Rows},
    schema,
};
use bigbrotr_model::*;

#[test]
fn event_id_lookup_obeys_both_source_prefixes_and_exact_scope() {
    let expected = signed("unsigned-maximum");
    let home = tempfile::tempdir().unwrap();
    let references = seed(home.path(), expected.clone());
    let query = bigbrotr_protocol::archive_event::EventQuery {
        dataset: references.dataset,
        event_id: expected.id,
        scope: references.scope,
        topology_generation: TopologyGeneration::new(0),
        bounds: vec![bigbrotr_protocol::archive_cut::CutBound {
            shard_id: u32::MAX,
            owner_epoch: u64::MAX,
            complete_through: 2,
        }],
        snapshot_id: 2,
    };
    for variant in [
        "complete",
        "zero",
        "before-eligibility",
        "missing-stream",
        "foreign-epoch",
        "scope",
        "event",
    ] {
        let mut request = query.clone();
        match variant {
            "zero" => request.bounds[0].complete_through = 0,
            "before-eligibility" => request.bounds[0].complete_through = 1,
            "missing-stream" => request.bounds.clear(),
            "foreign-epoch" => request.bounds[0].owner_epoch = 1,
            "scope" => request.scope.ordinal = Ordinal::new(1).unwrap(),
            "event" => request.event_id = EventId::from_bytes([0; 32]),
            _ => (),
        }
        let temp = tempfile::tempdir().unwrap();
        let reader = engine_at(temp.path(), home.path(), true, Some(2)).unwrap();
        let actual = lookup_attached(&reader.connection, &request, 4 * 1024 * 1024);
        if variant == "complete" {
            let actual = present(actual.unwrap());
            assert_eq!(actual.snapshot_id, 2);
            assert_eq!(actual.event, expected);
        } else {
            let actual = actual.unwrap();
            assert_eq!(actual.snapshot_id, 2);
            assert_eq!(actual.request_digest, request.digest());
            assert!(
                matches!(actual.result, EventSelection::Absent {}),
                "{variant}: {actual:?}"
            );
        }
    }
}

#[test]
fn private_request_is_closed_bounded_and_checks_cross_fact_identity_before_io() {
    let (_, _, read) = facts(signed("timestamp-zero"));
    let input = serde_json::json!({"catalog":{"dsn":"hostaddr=127.0.0.1","data_root":"/not-opened/"},"catalog_id":"05050505050505050505050505050505","expected":read});
    let bytes = serde_json::to_vec(&input).unwrap();
    assert!(EventRequest::read(&bytes[..]).is_ok());
    for snapshot in [0, 2, i64::MAX as u64] {
        let mut selected = input.clone();
        selected["expected"]["snapshot_id"] = serde_json::json!(snapshot);
        assert!(EventRequest::read(&serde_json::to_vec(&selected).unwrap()[..]).is_ok());
    }
    for snapshot in [
        serde_json::json!(-1),
        serde_json::json!(u64::MAX),
        serde_json::json!(1.5),
        serde_json::json!("2"),
    ] {
        let mut selected = input.clone();
        selected["expected"]["snapshot_id"] = snapshot;
        assert!(matches!(
            EventRequest::read(&serde_json::to_vec(&selected).unwrap()[..]),
            Err(EventReadError::InvalidInput)
        ));
    }
    let mut legacy = input.clone();
    legacy["expected"]
        .as_object_mut()
        .unwrap()
        .remove("snapshot_id");
    assert_eq!(
        EventRequest::read(&serde_json::to_vec(&legacy).unwrap()[..])
            .unwrap()
            .expected
            .snapshot_id,
        None
    );
    for field in ["catalog", "catalog_id", "expected"] {
        let mut missing = input.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(matches!(
            EventRequest::read(&serde_json::to_vec(&missing).unwrap()[..]),
            Err(EventReadError::InvalidInput)
        ));
    }
    let mut unknown = input.clone();
    unknown["policy_grant"] = serde_json::json!(true);
    assert!(EventRequest::read(&serde_json::to_vec(&unknown).unwrap()[..]).is_err());
    let duplicate = format!(
        "{{\"catalog_id\":\"05050505050505050505050505050505\",{}",
        std::str::from_utf8(&bytes)
            .unwrap()
            .strip_prefix('{')
            .unwrap()
    );
    assert!(EventRequest::read(duplicate.as_bytes()).is_err());
    assert!(EventRequest::read(&[bytes.as_slice(), b"{}"].concat()[..]).is_err());
    assert!(matches!(
        EventRequest::read(&vec![b' '; 32769][..]),
        Err(EventReadError::ResourceLimit)
    ));
    for invalid in [0, bigbrotr_protocol::archive_event::MAX_EVENT_ROW_BYTES + 1] {
        assert_eq!(read.validate(invalid), Err(EventReadError::ResourceLimit));
    }
    for variant in ["same-source", "scope-kind", "dataset", "scope-dataset"] {
        let mut bad = read.clone();
        match variant {
            "same-source" => bad.eligibility.source_record_id = bad.payload.source_record_id,
            "scope-kind" => bad.scope.dictionary_kind = DictionaryKind::Event,
            "dataset" => bad.eligibility.source_record_id.dataset_id = [2; 16].try_into().unwrap(),
            _ => bad.scope.dataset_id = [2; 16].try_into().unwrap(),
        }
        assert_eq!(
            bad.validate(4096),
            Err(EventReadError::InvalidInput),
            "{variant}"
        );
    }
}

fn signed(name: &str) -> SignedEvent {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/protocol/signed-extremes.json"
    ))
    .unwrap();
    serde_json::from_value(v["events"][name]["event"].clone()).unwrap()
}
fn facts(event: SignedEvent) -> (Fact, Fact, EventRead) {
    let dataset = [1; 16].try_into().unwrap();
    let reference = |kind, ordinal| QualifiedRef {
        dataset_id: dataset,
        dictionary_kind: kind,
        allocation_domain: [2; 16].try_into().unwrap(),
        ordinal: Ordinal::new(ordinal).unwrap(),
    };
    let scope = reference(DictionaryKind::VisibilityScope, 2);
    let source = SourceRecordId {
        dataset_id: dataset,
        shard_id: u32::MAX,
        owner_epoch: OwnerEpoch::new(u64::MAX).unwrap(),
        sequence: Sequence::new(1).unwrap(),
    };
    let eligibility = Fact::new(
        SourceRecordId {
            sequence: Sequence::new(2).unwrap(),
            ..source
        },
        scope,
        ObservedTime::new(i64::MAX),
        FactPayload::EventEligibility(EventEligibility {
            event_id: event.id,
            body_digest: event.body_digest(),
            sig: event.sig,
            first_eligible_at: ObservedTime::new(-1),
            origin: EventOrigin::Network,
        }),
    )
    .unwrap();
    // The shared payload's storage scope is deliberately DIFFERENT. Its metadata
    // must not choose the requested representative or become public attribution.
    let payload = Fact::new(
        source,
        reference(DictionaryKind::VisibilityScope, 1),
        ObservedTime::new(i64::MIN),
        FactPayload::EventPayload(EventPayload {
            body: event.clone().into(),
            event_ordinal: reference(DictionaryKind::Event, u64::MAX),
        }),
    )
    .unwrap();
    let read = EventRead {
        dataset,
        event_id: event.id,
        scope,
        payload: payload.fact_ref(),
        eligibility: eligibility.fact_ref(),
        snapshot_id: None,
    };
    (payload, eligibility, read)
}
fn engine(temp: &std::path::Path, home: &std::path::Path, readonly: bool) -> Engine {
    engine_at(temp, home, readonly, None).unwrap()
}
fn engine_at(
    temp: &std::path::Path,
    home: &std::path::Path,
    readonly: bool,
    snapshot: Option<u64>,
) -> Result<Engine, crate::receipt::ReceiptError> {
    let e = Engine::open(
        &temp.canonicalize().unwrap(),
        EngineLimits {
            memory_bytes: 134217728,
            temp_bytes: 67108864,
            threads: 1,
        },
    )
    .unwrap();
    e.load_bundle(std::path::Path::new(
        &std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").expect("pinned native bundle"),
    ))
    .unwrap();
    let options = if readonly {
        crate::receipt::readonly_archive_options(home.join("objects").to_str().unwrap(), snapshot)?
    } else {
        format!(
            "DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false",
            home.join("objects").display()
        )
    };
    e.connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:{}' AS archive ({options})",
            home.join("catalog.ducklake").display()
        ))
        .map_err(|_| crate::receipt::ReceiptError::Native)?;
    Ok(e)
}

#[test]
fn exact_snapshot_reads_original_pair_after_later_committed_deletion() {
    let home = tempfile::tempdir().unwrap();
    let expected = signed("timestamp-zero");
    let mut read = seed(home.path(), expected.clone());
    let temp = tempfile::tempdir().unwrap();
    let writer = engine(temp.path(), home.path(), false);
    writer
        .connection
        .execute_batch("DELETE FROM archive.event_eligibility_facts")
        .unwrap();
    drop(writer);
    read.snapshot_id = Some(2);
    let temp = tempfile::tempdir().unwrap();
    let reader = engine_at(temp.path(), home.path(), true, read.snapshot_id).unwrap();
    let actual = read_attached(&reader.connection, &read, 4 * 1024 * 1024).unwrap();
    assert_eq!(actual.snapshot_id, 2);
    assert_eq!(actual.event, expected);
    drop(reader);
    read.snapshot_id = None;
    let temp = tempfile::tempdir().unwrap();
    let reader = engine(temp.path(), home.path(), true);
    assert!(matches!(
        read_attached(&reader.connection, &read, 4 * 1024 * 1024),
        Err(EventReadError::Missing)
    ));
}

#[test]
fn expired_future_and_wrong_actual_snapshots_never_fall_back_to_current() {
    let home = tempfile::tempdir().unwrap();
    let expected = signed("timestamp-zero");
    let mut read = seed(home.path(), expected.clone());
    let temp = tempfile::tempdir().unwrap();
    let writer = engine(temp.path(), home.path(), false);
    writer
        .connection
        .execute_batch("COMMENT ON TABLE archive.events IS 'snapshot fixture'")
        .unwrap();
    writer
        .connection
        .execute_batch("CALL ducklake_expire_snapshots('archive', versions => [2])")
        .unwrap();
    assert_eq!(
        writer
            .connection
            .query_row(
                "SELECT count(*) FROM ducklake_snapshots('archive') WHERE snapshot_id=2",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
        0
    );
    drop(writer);
    for selected in [2, 999, i64::MAX as u64] {
        read.snapshot_id = Some(selected);
        let temp = tempfile::tempdir().unwrap();
        match engine_at(temp.path(), home.path(), true, read.snapshot_id) {
            Ok(reader) => assert!(matches!(
                read_attached(&reader.connection, &read, 4 * 1024 * 1024),
                Err(EventReadError::Native)
            )),
            Err(error) => assert_eq!(error, crate::receipt::ReceiptError::Native),
        }
    }
    // Independently current data is still valid. A hidden latest fallback would
    // return the event rather than refuse the expired/missing snapshot above.
    read.snapshot_id = None;
    let temp = tempfile::tempdir().unwrap();
    let reader = engine(temp.path(), home.path(), true);
    assert_eq!(
        read_attached(&reader.connection, &read, 4 * 1024 * 1024)
            .unwrap()
            .event,
        expected
    );
    drop(reader);
    read.snapshot_id = Some(2);
    let temp = tempfile::tempdir().unwrap();
    let wrong = engine(temp.path(), home.path(), true);
    assert!(matches!(
        read_attached(&wrong.connection, &read, 4 * 1024 * 1024),
        Err(EventReadError::Mismatch)
    ));
}
fn seed(home: &std::path::Path, event: SignedEvent) -> EventRead {
    let temp = tempfile::tempdir().unwrap();
    let e = engine(temp.path(), home, false);
    schema::initialize(&e.connection).unwrap();
    let (payload, eligibility, read) = facts(event);
    let limits = RowLimits {
        max_rows: 1,
        max_text_bytes: 4 * 1024 * 1024,
        max_tag_nodes: 100_000,
    };
    schema::create_stage(&e.connection).unwrap();
    Rows::events(&[&payload], limits)
        .unwrap()
        .append(&mut e.connection.appender_to_db("events", "stage").unwrap())
        .unwrap();
    Rows::eligibility(&[&eligibility], limits)
        .unwrap()
        .append(
            &mut e
                .connection
                .appender_to_db("event_eligibility_facts", "stage")
                .unwrap(),
        )
        .unwrap();
    e.connection.execute_batch("BEGIN TRANSACTION; INSERT INTO archive.events SELECT * FROM stage.events; INSERT INTO archive.event_eligibility_facts SELECT * FROM stage.event_eligibility_facts; COMMIT").unwrap();
    read
}
#[test]
fn exact_archived_signed_extremes_survive_fresh_readonly_hydration() {
    for name in ["timestamp-zero", "unsigned-maximum"] {
        let expected = signed(name);
        let home = tempfile::tempdir().unwrap();
        let read = seed(home.path(), expected.clone());
        let temp = tempfile::tempdir().unwrap();
        let e = engine(temp.path(), home.path(), true);
        let event = read_attached(&e.connection, &read, 4 * 1024 * 1024).unwrap();
        assert_eq!(event.snapshot_id, 2);
        assert_eq!(event.event, expected);
        bigbrotr_protocol::verify_owned(event.event, 4 * 1024 * 1024).unwrap();
        assert!(
            e.connection
                .execute_batch("DELETE FROM archive.events")
                .is_err()
        );
    }
}

#[test]
fn complete_envelope_conflicts_missing_rows_and_resource_refusals_never_yield_an_event() {
    for (mutation, expected) in [
        ("DELETE FROM archive.events", EventReadError::Missing),
        (
            "DELETE FROM archive.event_eligibility_facts",
            EventReadError::Missing,
        ),
        (
            "UPDATE archive.events SET recorded_at=0",
            EventReadError::Mismatch,
        ),
        (
            "UPDATE archive.events SET record_schema_version=2",
            EventReadError::Mismatch,
        ),
        (
            "UPDATE archive.events SET record_kind='event-eligibility'",
            EventReadError::Mismatch,
        ),
        (
            "UPDATE archive.events SET content=content||'altered'",
            EventReadError::Mismatch,
        ),
        (
            "UPDATE archive.event_eligibility_facts SET first_eligible_at=0",
            EventReadError::Mismatch,
        ),
        (
            "UPDATE archive.event_eligibility_facts SET origin='import'",
            EventReadError::Mismatch,
        ),
        (
            "UPDATE archive.event_eligibility_facts SET sig=from_hex(repeat('00',64))",
            EventReadError::Mismatch,
        ),
        (
            "INSERT INTO archive.events SELECT * REPLACE(from_hex(repeat('00',32)) AS payload_digest) FROM archive.events",
            EventReadError::Mismatch,
        ),
        (
            "INSERT INTO archive.event_eligibility_facts SELECT * FROM archive.event_eligibility_facts",
            EventReadError::Mismatch,
        ),
        (
            "UPDATE archive.events SET tags=[['x'],NULL]",
            EventReadError::Mismatch,
        ),
    ] {
        let home = tempfile::tempdir().unwrap();
        let read = seed(home.path(), signed("timestamp-zero"));
        let temp = tempfile::tempdir().unwrap();
        let writer = engine(temp.path(), home.path(), false);
        writer.connection.execute_batch(mutation).unwrap();
        drop(writer);
        let temp = tempfile::tempdir().unwrap();
        let reader = engine(temp.path(), home.path(), true);
        assert!(
            matches!(read_attached(&reader.connection,&read,4*1024*1024),Err(e) if e==expected),
            "{mutation}"
        );
    }
    let home = tempfile::tempdir().unwrap();
    let read = seed(home.path(), signed("timestamp-zero"));
    for variant in ["scope", "event", "dataset", "digest", "limit"] {
        let mut wanted = read.clone();
        match variant {
            "scope" => wanted.scope.ordinal = Ordinal::new(1).unwrap(),
            "event" => wanted.event_id = EventId::from_bytes([0; 32]),
            "dataset" => wanted.dataset = [3; 16].try_into().unwrap(),
            "digest" => wanted.payload.payload_digest = Digest256::from_bytes([0; 32]),
            _ => (),
        }
        let temp = tempfile::tempdir().unwrap();
        let reader = engine(temp.path(), home.path(), true);
        assert!(
            read_attached(
                &reader.connection,
                &wanted,
                if variant == "limit" {
                    32
                } else {
                    4 * 1024 * 1024
                }
            )
            .is_err(),
            "{variant}"
        );
    }
    // A reduced limit cannot poison identical retry with adequate capacity.
    let temp = tempfile::tempdir().unwrap();
    let reader = engine(temp.path(), home.path(), true);
    assert_eq!(
        read_attached(&reader.connection, &read, 4 * 1024 * 1024)
            .unwrap()
            .event,
        signed("timestamp-zero")
    );
}

#[test]
fn exact_fact_digest_cannot_replace_cryptographic_verification() {
    let event = signed("timestamp-zero");
    let home = tempfile::tempdir().unwrap();
    let mut read = seed(home.path(), event.clone());
    let (_, eligibility, _) = facts(event);
    let FactPayload::EventEligibility(mut bad) = eligibility.payload().clone() else {
        panic!()
    };
    bad.sig = Signature::from_bytes([0; 64]);
    let altered = Fact::new(
        eligibility.source_record_id(),
        eligibility.visibility_scope_id(),
        eligibility.recorded_at(),
        FactPayload::EventEligibility(bad),
    )
    .unwrap();
    read.eligibility = altered.fact_ref();
    let temp = tempfile::tempdir().unwrap();
    let writer = engine(temp.path(), home.path(), false);
    writer.connection.execute_batch(&format!("UPDATE archive.event_eligibility_facts SET sig=from_hex(repeat('00',64)), payload_digest=from_hex('{}')",read.eligibility.payload_digest)).unwrap();
    drop(writer);
    let temp = tempfile::tempdir().unwrap();
    let reader = engine(temp.path(), home.path(), true);
    assert!(matches!(
        read_attached(&reader.connection, &read, 4 * 1024 * 1024),
        Err(EventReadError::Mismatch)
    ));
}

#[test]
fn scope_representatives_and_native_snapshot_remain_distinct() {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../experiments/identity-contract/signed-fixture.json"
    ))
    .unwrap();
    let public: SignedEvent = serde_json::from_value(v["events"]["E_public"].clone()).unwrap();
    let private: SignedEvent = serde_json::from_value(v["events"]["E_private"].clone()).unwrap();
    assert_eq!(public.id, private.id);
    assert_ne!(public.sig, private.sig);
    let home = tempfile::tempdir().unwrap();
    let read = seed(home.path(), public.clone());
    let temp = tempfile::tempdir().unwrap();
    let reader = engine(temp.path(), home.path(), false);
    // Exercise the SAME transaction/snapshot/query primitives under actual
    // interleaving. This local catalog test deliberately uses writable attachments
    // in both connections because embedded metadata cannot mix open flags.
    reader
        .connection
        .execute_batch("BEGIN TRANSACTION")
        .unwrap();
    let snapshot: u64 = reader
        .connection
        .query_row(
            "SELECT id FROM ducklake_current_snapshot('archive')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let payload = read_payload(&reader.connection, &read, 4 * 1024 * 1024).unwrap();
    let temp2 = tempfile::tempdir().unwrap();
    let writer = engine(temp2.path(), home.path(), false);
    let (_, original, _) = facts(public.clone());
    let FactPayload::EventEligibility(mut alternate) = original.payload().clone() else {
        panic!()
    };
    alternate.sig = private.sig;
    let alternate = Fact::new(
        original.source_record_id(),
        original.visibility_scope_id(),
        original.recorded_at(),
        FactPayload::EventEligibility(alternate),
    )
    .unwrap();
    writer.connection.execute_batch(&format!("UPDATE archive.event_eligibility_facts SET sig=from_hex('{}'),payload_digest=from_hex('{}')",private.sig,alternate.payload_digest())).unwrap();
    let now: u64 = writer
        .connection
        .query_row(
            "SELECT id FROM ducklake_current_snapshot('archive')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(now > snapshot);
    let eligibility = read_eligibility(&reader.connection, &read, 4 * 1024 * 1024).unwrap();
    assert_eq!(
        join(&payload, &eligibility, &read, 4 * 1024 * 1024).unwrap(),
        public
    );
    reader.connection.execute_batch("COMMIT").unwrap();
    // The embedded metadata attachment can retain its older view across COMMIT.
    // Product reads consume the engine; resolve changed references with a fresh
    // attachment rather than assuming reuse discovers the newest metadata.
    drop(reader);
    drop(writer);
    let fresh_temp = tempfile::tempdir().unwrap();
    let fresh = engine(fresh_temp.path(), home.path(), true);
    let actual = read_attached(&fresh.connection, &read, 4 * 1024 * 1024);
    assert!(
        matches!(actual, Err(EventReadError::Mismatch)),
        "actual subsequent transaction: {actual:?}"
    );
}

#[test]
fn full_large_signed_body_exceeds_diagnostic_pipe_size_without_projection() {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/ingest/large-replay.json"
    ))
    .unwrap();
    let mut value = v["event"].clone();
    value["content"] = serde_json::json!(
        v["content_repeat"]
            .as_str()
            .unwrap()
            .repeat(v["content_count"].as_u64().unwrap() as usize)
    );
    let expected: SignedEvent = serde_json::from_value(value).unwrap();
    let home = tempfile::tempdir().unwrap();
    let read = seed(home.path(), expected.clone());
    let temp = tempfile::tempdir().unwrap();
    let e = engine(temp.path(), home.path(), true);
    let actual = read_attached(&e.connection, &read, 4 * 1024 * 1024).unwrap();
    assert_eq!(actual.event, expected);
    assert!(serde_json::to_vec(&actual).unwrap().len() > 2 * 1024 * 1024);
}

#[test]
fn two_scopes_return_their_own_valid_signature_not_a_global_first_representative() {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../experiments/identity-contract/signed-fixture.json"
    ))
    .unwrap();
    let public: SignedEvent = serde_json::from_value(v["events"]["E_public"].clone()).unwrap();
    let private: SignedEvent = serde_json::from_value(v["events"]["E_private"].clone()).unwrap();
    let home = tempfile::tempdir().unwrap();
    let public_read = seed(home.path(), public.clone());
    let (_, eligibility, _) = facts(private.clone());
    let private_fact = Fact::new(
        SourceRecordId {
            sequence: Sequence::new(3).unwrap(),
            ..eligibility.source_record_id()
        },
        QualifiedRef {
            ordinal: Ordinal::new(1).unwrap(),
            ..eligibility.visibility_scope_id()
        },
        eligibility.recorded_at(),
        eligibility.payload().clone(),
    )
    .unwrap();
    let private_read = EventRead {
        scope: private_fact.visibility_scope_id(),
        eligibility: private_fact.fact_ref(),
        ..public_read.clone()
    };
    let other_domain = Fact::new(
        SourceRecordId {
            sequence: Sequence::new(4).unwrap(),
            ..eligibility.source_record_id()
        },
        QualifiedRef {
            allocation_domain: [3; 16].try_into().unwrap(),
            ..eligibility.visibility_scope_id()
        },
        eligibility.recorded_at(),
        eligibility.payload().clone(),
    )
    .unwrap();
    let domain_read = EventRead {
        scope: other_domain.visibility_scope_id(),
        eligibility: other_domain.fact_ref(),
        ..public_read.clone()
    };
    let temp = tempfile::tempdir().unwrap();
    let writer = engine(temp.path(), home.path(), false);
    writer.connection.execute_batch(&format!("INSERT INTO archive.event_eligibility_facts SELECT * REPLACE(struct_update(source_record_id,sequence:=3::UBIGINT) AS source_record_id,struct_update(visibility_scope_id,ordinal:=1::UBIGINT) AS visibility_scope_id,from_hex('{}') AS sig,from_hex('{}') AS payload_digest) FROM archive.event_eligibility_facts",private.sig,private_fact.payload_digest())).unwrap();
    writer.connection.execute_batch(&format!("INSERT INTO archive.event_eligibility_facts SELECT * REPLACE(struct_update(source_record_id,sequence:=4::UBIGINT) AS source_record_id,struct_update(visibility_scope_id,allocation_domain:=from_hex('{}')) AS visibility_scope_id,from_hex('{}') AS sig,from_hex('{}') AS payload_digest) FROM archive.event_eligibility_facts WHERE source_record_id.sequence=2",other_domain.visibility_scope_id().allocation_domain,private.sig,other_domain.payload_digest())).unwrap();
    drop(writer);
    for (request, expected) in [
        (&public_read, &public),
        (&private_read, &private),
        (&domain_read, &private),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let reader = engine(temp.path(), home.path(), true);
        let actual = read_attached(&reader.connection, request, 4 * 1024 * 1024).unwrap();
        assert_eq!(&actual.event, expected);
        drop(reader);
        let query = bigbrotr_protocol::archive_event::EventQuery {
            dataset: request.dataset,
            event_id: request.event_id,
            scope: request.scope,
            topology_generation: TopologyGeneration::new(0),
            bounds: vec![bigbrotr_protocol::archive_cut::CutBound {
                shard_id: u32::MAX,
                owner_epoch: u64::MAX,
                complete_through: 4,
            }],
            snapshot_id: 4,
        };
        let temp = tempfile::tempdir().unwrap();
        let reader = engine_at(temp.path(), home.path(), true, Some(4)).unwrap();
        let selected =
            present(lookup_attached(&reader.connection, &query, 4 * 1024 * 1024).unwrap());
        assert_eq!(&selected.event, expected);
        assert_eq!(selected.payload, request.payload);
        assert_eq!(selected.eligibility, request.eligibility);
    }
}

#[test]
fn lookup_cannot_cross_a_body_prefix_and_rejects_conflicting_logical_duplicates() {
    for variant in [
        "body-after-cut",
        "body-other-stream",
        "body-duplicate",
        "eligibility-duplicate",
        "body-corrupt",
    ] {
        let home = tempfile::tempdir().unwrap();
        let expected = signed("timestamp-zero");
        let original = seed(home.path(), expected.clone());
        let (body, eligibility, _) = facts(expected.clone());
        let mut alternate_read = None;
        let temp = tempfile::tempdir().unwrap();
        let writer = engine(temp.path(), home.path(), false);
        let mutation = match variant {
            "body-after-cut" | "body-other-stream" => {
                let changed_source = if variant == "body-after-cut" {
                    SourceRecordId {
                        sequence: Sequence::new(3).unwrap(),
                        ..body.source_record_id()
                    }
                } else {
                    SourceRecordId {
                        owner_epoch: OwnerEpoch::new(1).unwrap(),
                        ..body.source_record_id()
                    }
                };
                let changed = Fact::new(
                    changed_source,
                    body.visibility_scope_id(),
                    body.recorded_at(),
                    body.payload().clone(),
                )
                .unwrap();
                format!(
                    "UPDATE archive.events SET source_record_id=struct_update(source_record_id,owner_epoch:={}::UBIGINT,sequence:={}::UBIGINT),payload_digest=from_hex('{}')",
                    changed_source.owner_epoch.get(),
                    changed_source.sequence.get(),
                    changed.payload_digest()
                )
            }
            // Different source IDs catch an implementation that first hydrates
            // one arbitrary representative and only checks source duplicates.
            "body-duplicate" | "eligibility-duplicate" => {
                let (fact, table) = if variant == "body-duplicate" {
                    (&body, "events")
                } else {
                    (&eligibility, "event_eligibility_facts")
                };
                let changed = Fact::new(
                    SourceRecordId {
                        sequence: Sequence::new(3).unwrap(),
                        ..fact.source_record_id()
                    },
                    fact.visibility_scope_id(),
                    fact.recorded_at(),
                    fact.payload().clone(),
                )
                .unwrap();
                let mut read = original.clone();
                if variant == "body-duplicate" {
                    read.payload = changed.fact_ref();
                } else {
                    read.eligibility = changed.fact_ref();
                }
                alternate_read = Some(read);
                format!(
                    "INSERT INTO archive.{table} SELECT * REPLACE(struct_update(source_record_id,sequence:=3::UBIGINT) AS source_record_id,from_hex('{}') AS payload_digest) FROM archive.{table}",
                    changed.payload_digest()
                )
            }
            _ => "UPDATE archive.events SET content=content||'altered'".into(),
        };
        writer.connection.execute_batch(&mutation).unwrap();
        drop(writer);
        if let Some(alternate) = alternate_read {
            // Both alternatives are independently valid. Refusal must come from
            // ambiguity, not the signature or fact-integrity checks.
            for read in [&original, &alternate] {
                let temp = tempfile::tempdir().unwrap();
                let reader = engine_at(temp.path(), home.path(), true, Some(3)).unwrap();
                assert_eq!(
                    read_attached(&reader.connection, read, 4 * 1024 * 1024)
                        .unwrap()
                        .event,
                    expected
                );
            }
        }
        let mut query = bigbrotr_protocol::archive_event::EventQuery {
            dataset: original.dataset,
            event_id: original.event_id,
            scope: original.scope,
            topology_generation: TopologyGeneration::new(0),
            bounds: vec![bigbrotr_protocol::archive_cut::CutBound {
                shard_id: u32::MAX,
                owner_epoch: u64::MAX,
                complete_through: if variant.ends_with("duplicate") { 3 } else { 2 },
            }],
            snapshot_id: 3,
        };
        let temp = tempfile::tempdir().unwrap();
        let reader = engine_at(temp.path(), home.path(), true, Some(3)).unwrap();
        let error = lookup_attached(&reader.connection, &query, 4 * 1024 * 1024).unwrap_err();
        assert_eq!(error, EventReadError::Mismatch, "{variant}");
        drop(reader);
        if variant == "body-corrupt" {
            let mut absent = query.clone();
            absent.scope.ordinal = Ordinal::new(3).unwrap();
            let temp = tempfile::tempdir().unwrap();
            let reader = engine_at(temp.path(), home.path(), true, Some(3)).unwrap();
            assert!(
                matches!(
                    lookup_attached(&reader.connection, &absent, 4 * 1024 * 1024)
                        .unwrap()
                        .result,
                    EventSelection::Absent {}
                ),
                "absent scope must not hydrate a corrupt body"
            );
        }
        // Expanding the explicit cut makes the complete pair available. An
        // unrelated zero stream neither admits records nor changes the result.
        match variant {
            "body-after-cut" => query.bounds[0].complete_through = 3,
            "body-other-stream" => query.bounds.insert(
                0,
                bigbrotr_protocol::archive_cut::CutBound {
                    shard_id: u32::MAX,
                    owner_epoch: 1,
                    complete_through: 1,
                },
            ),
            _ => continue,
        }
        query.bounds.insert(
            0,
            bigbrotr_protocol::archive_cut::CutBound {
                shard_id: 0,
                owner_epoch: 1,
                complete_through: 0,
            },
        );
        let temp = tempfile::tempdir().unwrap();
        let reader = engine_at(temp.path(), home.path(), true, Some(3)).unwrap();
        assert_eq!(
            present(lookup_attached(&reader.connection, &query, 4 * 1024 * 1024).unwrap()).event,
            expected
        );
    }
}
