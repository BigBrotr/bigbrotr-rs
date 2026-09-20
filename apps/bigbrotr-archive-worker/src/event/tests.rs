use super::*;
use crate::{
    engine::{Engine, EngineLimits},
    rows::{RowLimits, Rows},
    schema,
};
use bigbrotr_model::*;

#[test]
fn private_request_is_closed_bounded_and_checks_cross_fact_identity_before_io() {
    let (_, _, read) = facts(signed("timestamp-zero"));
    let input = serde_json::json!({"catalog":{"dsn":"hostaddr=127.0.0.1","data_root":"/not-opened/"},"catalog_id":"05050505050505050505050505050505","expected":read});
    let bytes = serde_json::to_vec(&input).unwrap();
    assert!(EventRequest::read(&bytes[..]).is_ok());
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
    };
    (payload, eligibility, read)
}
fn engine(temp: &std::path::Path, home: &std::path::Path, readonly: bool) -> Engine {
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
    e.connection.execute_batch(&format!("ATTACH 'ducklake:{}' AS archive (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false{})",home.join("catalog.ducklake").display(),home.join("objects").display(),if readonly{", CREATE_IF_NOT_EXISTS false, READ_ONLY"}else{""})).unwrap();
    e
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
    let temp = tempfile::tempdir().unwrap();
    let writer = engine(temp.path(), home.path(), false);
    writer.connection.execute_batch(&format!("INSERT INTO archive.event_eligibility_facts SELECT * REPLACE(struct_update(source_record_id,sequence:=3::UBIGINT) AS source_record_id,struct_update(visibility_scope_id,ordinal:=1::UBIGINT) AS visibility_scope_id,from_hex('{}') AS sig,from_hex('{}') AS payload_digest) FROM archive.event_eligibility_facts",private.sig,private_fact.payload_digest())).unwrap();
    drop(writer);
    for (request, expected) in [(&public_read, &public), (&private_read, &private)] {
        let temp = tempfile::tempdir().unwrap();
        let reader = engine(temp.path(), home.path(), true);
        let actual = read_attached(&reader.connection, request, 4 * 1024 * 1024).unwrap();
        assert_eq!(&actual.event, expected);
    }
}
