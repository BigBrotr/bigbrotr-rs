use super::{
    tests::{engine, limits},
    *,
};
use bigbrotr_model::{codec::FactDecodeLimits, *};

const COMMON: &str = "source_record_id STRUCT(dataset_id BLOB, shard_id UINTEGER, owner_epoch UBIGINT, sequence UBIGINT) NOT NULL,
 record_schema_version USMALLINT NOT NULL, record_kind VARCHAR NOT NULL,
 visibility_scope_id STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT) NOT NULL,
 payload_digest BLOB NOT NULL, recorded_at BIGINT NOT NULL";
const REFERENCE: &str =
    "STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT)";
const CLOCK: &str = "STRUCT(clock_id STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT), resolution_us UBIGINT, uncertainty_us UBIGINT, anomaly_flags UINTEGER)";
const CORRECTS: &str =
    "STRUCT(dataset_id BLOB, shard_id UINTEGER, owner_epoch UBIGINT, sequence UBIGINT)";

pub(super) fn frozen(name: &str) -> Fact {
    for file in [
        include_str!("../../../../tests/fixtures/model/core-fingerprints.json"),
        include_str!("../../../../experiments/identity-contract/golden-vectors.json"),
    ] {
        let values: serde_json::Value = serde_json::from_str(file).unwrap();
        if let Some(row) = values["vectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["name"] == name)
        {
            let bytes: Vec<_> = row["preimage_hex"]
                .as_str()
                .unwrap()
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
                .collect();
            return Fact::decode_preimage(
                &bytes,
                row["sha256"].as_str().unwrap().parse().unwrap(),
                FactDecodeLimits::default(),
            )
            .unwrap();
        }
    }
    panic!("missing frozen fixture")
}
pub(super) fn reopen(
    rows: Rows,
    fields: &str,
) -> (tempfile::TempDir, crate::engine::Engine, String) {
    let temp = tempfile::tempdir().unwrap();
    let writer = engine(temp.path());
    writer
        .connection
        .execute_batch(&format!("CREATE TABLE metadata ({COMMON}, {fields})"))
        .unwrap();
    rows.append(&mut writer.connection.appender("metadata").unwrap())
        .unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let file = artifact
        .path()
        .join("metadata.parquet")
        .to_str()
        .unwrap()
        .to_owned();
    writer
        .connection
        .execute("COPY metadata TO ? (FORMAT PARQUET)", [&file])
        .unwrap();
    drop(writer);
    let temp = artifact.path().join("reader");
    std::fs::create_dir(&temp).unwrap();
    let reader = engine(&temp);
    reader
        .connection
        // DuckDB refuses prepared parameters in CREATE VIEW. This owned
        // generated fixture path is a quoted literal, not request SQL.
        .execute_batch(&format!(
            "CREATE VIEW metadata AS SELECT * FROM read_parquet('{}')",
            file.replace('\'', "''")
        ))
        .unwrap();
    (artifact, reader, file)
}

#[test]
fn eligibility_keeps_scope_representative_and_nullable_import_lineage() {
    let imported = frozen("eligibility-import");
    let network = frozen("fact-eligibility");
    let rows = Rows::eligibility(&[&network, &imported], limits()).unwrap();
    let fields = format!(
        "event_id BLOB NOT NULL, body_digest BLOB NOT NULL, sig BLOB NOT NULL, first_eligible_at BIGINT NOT NULL, origin VARCHAR NOT NULL, import_lineage {REFERENCE}"
    );
    let (_artifact, reader, _) = reopen(rows, &fields);
    let mut query = reader.connection.prepare("SELECT hex(event_id), hex(body_digest), hex(sig), first_eligible_at, origin, import_lineage IS NULL, hex(import_lineage.dataset_id), import_lineage.dictionary_kind, hex(import_lineage.allocation_domain), import_lineage.ordinal, hex(payload_digest) FROM metadata ORDER BY origin").unwrap();
    let mut output = query.query([]).unwrap();
    for imported in [true, false] {
        let row = output.next().unwrap().unwrap();
        assert_eq!(row.get::<_, String>(0).unwrap(), "AA".repeat(32));
        assert_eq!(
            row.get::<_, String>(1).unwrap().to_lowercase(),
            "0e6fa8588956533ae2408caf6fa304437b7a0b8fd216a0e11a10cf276704f760"
        );
        assert_eq!(row.get::<_, String>(2).unwrap(), "CC".repeat(64));
        assert_eq!(row.get::<_, i64>(3).unwrap(), -1);
        assert_eq!(
            row.get::<_, String>(4).unwrap(),
            if imported { "import" } else { "network" }
        );
        assert_eq!(row.get::<_, bool>(5).unwrap(), !imported);
        if imported {
            assert_eq!(row.get::<_, String>(6).unwrap(), "01".repeat(16));
            assert_eq!(row.get::<_, u16>(7).unwrap(), 9);
            assert_eq!(row.get::<_, String>(8).unwrap(), "02".repeat(16));
            assert_eq!(row.get::<_, u64>(9).unwrap(), 1);
        } else {
            assert_eq!(row.get::<_, Option<u64>>(9).unwrap(), None);
        }
        assert_eq!(
            row.get::<_, String>(10).unwrap().to_lowercase(),
            if imported {
                "44d9fac62b1b586950d25a3b0fc7ee610bcb598b0745fde7bafac245fd2484ad"
            } else {
                "578671867996ee16e2fc7a109cf0e9a11298f767d083ddc16ddc68ceeed6938e"
            }
        );
    }
    assert!(output.next().unwrap().is_none());
}

#[test]
fn receive_distinguishes_unknown_zero_extremes_and_correction_identity() {
    let original = frozen("event-receive-correction");
    let FactPayload::EventReceive(mut value) = original.payload().clone() else {
        panic!()
    };
    value.received_at = ObservedTime::new(i64::MAX);
    value.clock_quality = ClockQuality::new(
        value.clock_quality.clock_id(),
        DurationUs::new(u64::MAX),
        Some(DurationUs::new(0)),
        7,
    )
    .unwrap();
    value.corrects = None;
    let changed = Fact::new(
        original.source_record_id(),
        original.visibility_scope_id(),
        original.recorded_at(),
        FactPayload::EventReceive(value),
    )
    .unwrap();
    let rows = Rows::receive(&[&changed, &original], limits()).unwrap();
    let fields = format!(
        "event_id BLOB NOT NULL, received_at BIGINT NOT NULL, observer_id {REFERENCE} NOT NULL, clock_quality {CLOCK} NOT NULL, corrects {CORRECTS}"
    );
    let (_artifact, reader, _) = reopen(rows, &fields);
    let mut query = reader.connection.prepare("SELECT hex(event_id), received_at, observer_id.dictionary_kind, clock_quality.clock_id.dictionary_kind, clock_quality.resolution_us, clock_quality.uncertainty_us, clock_quality.anomaly_flags, corrects IS NULL, hex(corrects.dataset_id), corrects.shard_id, corrects.owner_epoch, corrects.sequence FROM metadata ORDER BY received_at").unwrap();
    let mut output = query.query([]).unwrap();
    for first in [true, false] {
        let row = output.next().unwrap().unwrap();
        assert_eq!(row.get::<_, String>(0).unwrap(), "AA".repeat(32));
        assert_eq!(
            row.get::<_, i64>(1).unwrap(),
            if first { -1 } else { i64::MAX }
        );
        assert_eq!(row.get::<_, u16>(2).unwrap(), 2);
        assert_eq!(row.get::<_, u16>(3).unwrap(), 2);
        assert_eq!(
            row.get::<_, u64>(4).unwrap(),
            if first { 1 } else { u64::MAX }
        );
        assert_eq!(
            row.get::<_, Option<u64>>(5).unwrap(),
            if first { None } else { Some(0) }
        );
        assert_eq!(row.get::<_, u32>(6).unwrap(), if first { 0 } else { 7 });
        assert_eq!(row.get::<_, bool>(7).unwrap(), !first);
        if first {
            assert_eq!(row.get::<_, String>(8).unwrap(), "01".repeat(16));
            assert_eq!(row.get::<_, u32>(9).unwrap(), 0);
            assert_eq!(row.get::<_, u64>(10).unwrap(), 1);
            assert_eq!(row.get::<_, u64>(11).unwrap(), 1);
        } else {
            assert_eq!(row.get::<_, Option<u64>>(11).unwrap(), None);
        }
    }
    assert!(output.next().unwrap().is_none());
}

#[test]
fn provenance_keeps_each_distinct_reference_position() {
    let value = frozen("provenance-correction");
    let rows = Rows::provenance(&[&value], limits()).unwrap();
    let fields = format!(
        "event_id BLOB NOT NULL, endpoint_id {REFERENCE} NOT NULL, observer_id {REFERENCE} NOT NULL, access_scope_id {REFERENCE} NOT NULL, received_at BIGINT NOT NULL, clock_quality {CLOCK} NOT NULL, verification_method_id {REFERENCE} NOT NULL, corrects {CORRECTS}"
    );
    let (_artifact, reader, _) = reopen(rows, &fields);
    let observed = reader.connection.query_row("SELECT hex(event_id), endpoint_id.dictionary_kind, observer_id.dictionary_kind, access_scope_id.dictionary_kind, received_at, verification_method_id.dictionary_kind, corrects.sequence, hex(payload_digest) FROM metadata", [], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u16>(1)?, r.get::<_, u16>(2)?, r.get::<_, u16>(3)?, r.get::<_, i64>(4)?, r.get::<_, u16>(5)?, r.get::<_, u64>(6)?, r.get::<_, String>(7)?))).unwrap();
    assert_eq!(
        observed,
        (
            "AA".repeat(32),
            1,
            2,
            3,
            -1,
            5,
            1,
            "78d23684a0870a3631510a59a27602173449d507731ff637143830ae4aa07e52".to_uppercase()
        )
    );
}
