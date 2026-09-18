use super::*;
use crate::engine::{Engine, EngineLimits};
use bigbrotr_model::*;
use bigbrotr_protocol::verify_owned;

// Independent table definition and literal expected fixture values. No schema
// inferred from the production Arrow batch can hide a wrong column or type.
const TABLE: &str = "CREATE TABLE events (
 source_record_id STRUCT(dataset_id BLOB, shard_id UINTEGER, owner_epoch UBIGINT, sequence UBIGINT) NOT NULL,
 record_schema_version USMALLINT NOT NULL, record_kind VARCHAR NOT NULL,
 visibility_scope_id STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT) NOT NULL,
 payload_digest BLOB NOT NULL, recorded_at BIGINT NOT NULL,
 event_id BLOB NOT NULL, pubkey BLOB NOT NULL, created_at UBIGINT NOT NULL,
 kind USMALLINT NOT NULL, tags VARCHAR[][] NOT NULL, content VARCHAR NOT NULL,
 event_ordinal STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT) NOT NULL
)";

fn limits() -> RowLimits {
    RowLimits {
        max_rows: 1024,
        max_text_bytes: 4 * 1024 * 1024,
        max_tag_nodes: 100_000,
    }
}
fn engine(path: &std::path::Path) -> Engine {
    Engine::open(
        &path.canonicalize().unwrap(),
        EngineLimits {
            memory_bytes: 128 * 1024 * 1024,
            temp_bytes: 64 * 1024 * 1024,
            threads: 1,
        },
    )
    .unwrap()
}
fn fixture_events() -> Vec<SignedEvent> {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/protocol/signed-extremes.json"
    ))
    .unwrap();
    ["unsigned-maximum", "timestamp-zero"]
        .iter()
        .map(|name| {
            let event = serde_json::from_value(fixture["events"][name]["event"].clone()).unwrap();
            verify_owned(event, 4 * 1024 * 1024).unwrap().into_event()
        })
        .collect()
}
fn fact(event: SignedEvent, sequence: u64) -> Fact {
    let dataset = DatasetId::try_from([1; 16]).unwrap();
    let reference = |kind| QualifiedRef {
        dataset_id: dataset,
        dictionary_kind: kind,
        allocation_domain: AllocationDomainId::try_from([2; 16]).unwrap(),
        ordinal: Ordinal::new(u64::MAX).unwrap(),
    };
    Fact::new(
        SourceRecordId {
            dataset_id: dataset,
            shard_id: u32::MAX,
            owner_epoch: OwnerEpoch::new(u64::MAX).unwrap(),
            sequence: Sequence::new(sequence).unwrap(),
        },
        reference(DictionaryKind::VisibilityScope),
        ObservedTime::new(i64::MIN),
        FactPayload::EventPayload(EventPayload {
            body: event.into(),
            event_ordinal: reference(DictionaryKind::Event),
        }),
    )
    .unwrap()
}

#[test]
fn signed_extremes_survive_native_append_parquet_and_independent_readback() {
    let events = fixture_events();
    let facts: Vec<_> = events
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, e)| fact(e, i as u64 + 1))
        .collect();
    let rows = EventRows::encode(&facts.iter().collect::<Vec<_>>(), limits()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let writer = engine(temp.path());
    writer.connection.execute_batch(TABLE).unwrap();
    rows.append(&mut writer.connection.appender("events").unwrap())
        .unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let file = artifact.path().join("events.parquet");
    writer
        .connection
        .execute(
            "COPY events TO ? (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)",
            [&file.to_str().unwrap()],
        )
        .unwrap();
    drop(writer);
    let read_temp = tempfile::tempdir().unwrap();
    let reader = engine(read_temp.path());
    for (index, expected) in events.iter().enumerate() {
        let sql = "SELECT hex(event_id), hex(pubkey), created_at, kind, to_json(tags), content,
            hex(source_record_id.dataset_id), source_record_id.shard_id, source_record_id.owner_epoch,
            source_record_id.sequence, recorded_at, record_schema_version, record_kind,
            hex(visibility_scope_id.dataset_id), visibility_scope_id.dictionary_kind,
            hex(visibility_scope_id.allocation_domain), visibility_scope_id.ordinal,
            hex(payload_digest), hex(event_ordinal.dataset_id), event_ordinal.dictionary_kind,
            hex(event_ordinal.allocation_domain), event_ordinal.ordinal
            FROM read_parquet(?) WHERE source_record_id.sequence = ?";
        let actual = reader
            .connection
            .query_row(
                sql,
                duckdb::params![file.to_str().unwrap(), index as u64 + 1],
                |row| {
                    assert_eq!(row.get::<_, String>(6)?, "01".repeat(16));
                    assert_eq!(row.get::<_, u32>(7)?, u32::MAX);
                    assert_eq!(row.get::<_, u64>(8)?, u64::MAX);
                    assert_eq!(row.get::<_, u64>(9)?, index as u64 + 1);
                    assert_eq!(row.get::<_, i64>(10)?, i64::MIN);
                    assert_eq!(row.get::<_, u16>(11)?, 1);
                    assert_eq!(row.get::<_, String>(12)?, "event-payload");
                    for offset in [13, 18] {
                        assert_eq!(row.get::<_, String>(offset)?, "01".repeat(16));
                        assert_eq!(
                            row.get::<_, u16>(offset + 1)?,
                            if offset == 13 { 4 } else { 8 }
                        );
                        assert_eq!(row.get::<_, String>(offset + 2)?, "02".repeat(16));
                        assert_eq!(row.get::<_, u64>(offset + 3)?, u64::MAX);
                    }
                    assert_eq!(
                        row.get::<_, String>(17)?.to_lowercase(),
                        facts[index].payload_digest().to_string()
                    );
                    Ok(SignedEvent {
                        id: row.get::<_, String>(0)?.to_lowercase().parse().unwrap(),
                        pubkey: row.get::<_, String>(1)?.to_lowercase().parse().unwrap(),
                        created_at: NostrTime(row.get(2)?),
                        kind: row.get(3)?,
                        tags: serde_json::from_str(&row.get::<_, String>(4)?).unwrap(),
                        content: row.get(5)?,
                        sig: expected.sig,
                    })
                },
            )
            .unwrap();
        assert_eq!(&actual, expected);
        verify_owned(actual, 4 * 1024 * 1024).unwrap();
    }
    assert_eq!(
        reader
            .connection
            .query_row(
                "SELECT count(*) FROM read_parquet(?)",
                [file.to_str().unwrap()],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
        2
    );
}

#[test]
fn refusal_happens_before_native_mutation() {
    let value = fact(fixture_events().remove(0), 1);
    let facts = [&value];
    for (limit, expected) in [
        (
            RowLimits {
                max_rows: 0,
                ..limits()
            },
            RowError::InvalidLimits,
        ),
        (
            RowLimits {
                max_rows: 1025,
                ..limits()
            },
            RowError::InvalidLimits,
        ),
        (
            RowLimits {
                max_text_bytes: 0,
                ..limits()
            },
            RowError::TextLimit,
        ),
        (
            RowLimits {
                max_tag_nodes: 0,
                ..limits()
            },
            RowError::TagLimit,
        ),
        (
            RowLimits {
                max_text_bytes: usize::MAX,
                ..limits()
            },
            RowError::InvalidLimits,
        ),
    ] {
        assert!(matches!(EventRows::encode(&facts, limit), Err(e) if e == expected));
    }
    assert!(matches!(
        EventRows::encode(&[], limits()),
        Err(RowError::RowLimit)
    ));
    assert!(matches!(
        EventRows::encode(
            &[&value, &value],
            RowLimits {
                max_rows: 1,
                ..limits()
            }
        ),
        Err(RowError::RowLimit)
    ));
}

#[test]
fn flush_failure_is_not_silently_discarded() {
    let value = fact(fixture_events().remove(0), 1);
    let rows = EventRows::encode(&[&value], limits()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let writer = engine(temp.path());
    writer
        .connection
        .execute_batch(&TABLE.replace(
            "created_at UBIGINT NOT NULL",
            "created_at UBIGINT NOT NULL CHECK(created_at = 0)",
        ))
        .unwrap();
    assert_eq!(
        rows.append(&mut writer.connection.appender("events").unwrap()),
        Err(RowError::NativeAppend)
    );
    assert_eq!(
        writer
            .connection
            .query_row("SELECT count(*) FROM events", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn fresh_batches_preserve_large_nested_children_and_bound_empty_nodes() {
    let mut event = fixture_events().remove(1);
    // Conversion fixture only: these modified fields are not claimed signed.
    event.tags = vec![vec!["x".into(), "".into(), "é".into()]; 8];
    let value = fact(event, 1);
    let facts = vec![&value; 1024];
    let exact = RowLimits {
        max_rows: 1024,
        max_text_bytes: 24 * 1024,
        max_tag_nodes: 32 * 1024,
    };
    let rows = EventRows::encode(&facts, exact).unwrap();
    assert!(rows.array_memory_bytes() > 24 * 1024);
    assert!(matches!(
        EventRows::encode(
            &facts,
            RowLimits {
                max_text_bytes: exact.max_text_bytes - 1,
                ..exact
            }
        ),
        Err(RowError::TextLimit)
    ));
    assert!(matches!(
        EventRows::encode(
            &facts,
            RowLimits {
                max_tag_nodes: exact.max_tag_nodes - 1,
                ..exact
            }
        ),
        Err(RowError::TagLimit)
    ));
    let temp = tempfile::tempdir().unwrap();
    let writer = engine(temp.path());
    writer.connection.execute_batch(TABLE).unwrap();
    rows.append(&mut writer.connection.appender("events").unwrap())
        .unwrap();
    assert_eq!(writer.connection.query_row("SELECT count(*) FROM events WHERE len(tags)=8 AND list_sum(list_transform(tags, x -> len(x)))=24 AND tags[1][1]='x' AND tags[8][2]='' AND tags[8][3]='é'", [], |r| r.get::<_, u64>(0)).unwrap(), 1024);

    let mut wide = fixture_events().remove(1);
    wide.tags = vec![vec!["".into()]; 10_000];
    wide.content = "z".repeat(1024 * 1024);
    let wide = fact(wide, 1);
    let wide_limits = RowLimits {
        max_rows: 1,
        max_text_bytes: 1024 * 1024,
        max_tag_nodes: 20_000,
    };
    let rows = EventRows::encode(&[&wide], wide_limits).unwrap();
    writer
        .connection
        .execute_batch("DELETE FROM events")
        .unwrap();
    rows.append(&mut writer.connection.appender("events").unwrap())
        .unwrap();
    assert_eq!(
        writer
            .connection
            .query_row(
                "SELECT len(content), len(tags), tags[10000][1] FROM events",
                [],
                |r| Ok((
                    r.get::<_, u64>(0)?,
                    r.get::<_, u64>(1)?,
                    r.get::<_, String>(2)?
                ))
            )
            .unwrap(),
        (1024 * 1024, 10_000, String::new())
    );
}

#[test]
fn other_families_and_mixed_rows_cannot_enter_the_event_relation() {
    let value = fact(fixture_events().remove(0), 1);
    let eligibility = Fact::new(
        value.source_record_id(),
        value.visibility_scope_id(),
        value.recorded_at(),
        FactPayload::EventEligibility(EventEligibility {
            event_id: fixture_events()[0].id,
            body_digest: fixture_events()[0].body_digest(),
            sig: fixture_events()[0].sig,
            first_eligible_at: ObservedTime::new(-1),
            origin: EventOrigin::Network,
        }),
    )
    .unwrap();
    for facts in [vec![&eligibility], vec![&value, &eligibility]] {
        assert!(matches!(
            EventRows::encode(&facts, limits()),
            Err(RowError::WrongFamily)
        ));
    }
}
