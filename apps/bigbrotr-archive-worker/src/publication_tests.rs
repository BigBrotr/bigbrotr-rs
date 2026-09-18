use super::*;
use crate::engine::{Engine, EngineLimits};
use bigbrotr_model::{AdmissionBatch, Fact, Sequence, codec::FactDecodeLimits};
use bigbrotr_protocol::archive_batch::{self, Limits};
use std::{path::Path, sync::atomic::AtomicBool};

fn decode(value: &serde_json::Value, digest: &str) -> Fact {
    let bytes: Vec<u8> = value["preimage_hex"]
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect();
    Fact::decode_preimage(
        &bytes,
        value[digest].as_str().unwrap().parse().unwrap(),
        FactDecodeLimits::default(),
    )
    .unwrap()
}
fn facts() -> Vec<Fact> {
    let dictionaries: serde_json::Value = serde_json::from_str(include_str!(
        "../../../crates/bigbrotr-model/tests/fixtures/dictionary-v1.json"
    ))
    .unwrap();
    let event: serde_json::Value = serde_json::from_str(include_str!(
        "../../../crates/bigbrotr-protocol/tests/fixtures/archive/single.json"
    ))
    .unwrap();
    [
        decode(&dictionaries["vectors"][1], "sha256"),
        decode(&dictionaries["vectors"][3], "sha256"),
        decode(&event, "fact_digest"),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, f)| {
        let mut source = f.source_record_id();
        source.sequence = Sequence::new(i as u64 + 1).unwrap();
        Fact::new(
            source,
            f.visibility_scope_id(),
            f.recorded_at(),
            f.payload().clone(),
        )
        .unwrap()
    })
    .collect()
}
fn open(temp: &Path, catalog: &Path, data: &Path) -> Engine {
    let engine = Engine::open(
        &temp.canonicalize().unwrap(),
        EngineLimits {
            memory_bytes: 134217728,
            temp_bytes: 67108864,
            threads: 1,
        },
    )
    .unwrap();
    engine
        .load_bundle(Path::new(
            &std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").unwrap(),
        ))
        .unwrap();
    engine
        .connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:{}' AS archive (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0)",
            catalog.display(),
            data.display()
        ))
        .unwrap();
    engine
}
fn prepare(engine: Engine, facts: &[Fact]) -> (PreparedBatch, PublicationRequest) {
    let batch = AdmissionBatch::new(facts.iter().map(Fact::fact_ref).collect()).unwrap();
    let records: Vec<_> = facts
        .iter()
        .map(|f| f.encode_preimage(8192).unwrap())
        .collect();
    let limits = Limits {
        max_records: 16,
        max_encoded_bytes: 1048576,
        fact: FactDecodeLimits::default(),
    };
    let cancel = AtomicBool::new(false);
    let mut wire = Vec::new();
    archive_batch::write_batch(&batch, &records, &mut wire, limits, &cancel).unwrap();
    let digest = ArchiveBatch::new(
        1,
        TopologyGeneration::new(u64::MAX),
        std::slice::from_ref(&batch),
    )
    .unwrap()
    .digest();
    let request=serde_json::from_value(serde_json::json!({"catalog":{"dsn":"unused","data_root":"/unused/"},"topology_generation":u64::MAX.to_string(),"batch_digest":digest,"attempt_id":([9;16]),"publication_ordinal":u64::MAX})).unwrap();
    (
        PreparedBatch::prepare(engine, wire.as_slice(), limits, &cancel).unwrap(),
        request,
    )
}

#[test]
fn complete_rows_and_receipt_survive_fresh_native_read_and_duplicate_refuses() {
    let home = tempfile::tempdir().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let catalog = home.path().join("catalog.ducklake");
    let data = home.path().join("objects");
    let engine = open(temp.path(), &catalog, &data);
    schema::initialize(&engine.connection).unwrap();
    let (prepared, request) = prepare(engine, &facts());
    prepared.publish_attached(&request).unwrap();
    drop(prepared);
    let temp = tempfile::tempdir().unwrap();
    let engine = open(temp.path(), &catalog, &data);
    let row:(u64,u16,String,String,u64)=engine.connection.query_row("SELECT created_at,kind,tags::VARCHAR,content,source_record_id.sequence FROM archive.events",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap();
    assert_eq!(
        row,
        (
            u64::MAX,
            1,
            "[[x], [x, ''], [x, '']]".into(),
            "é\n".into(),
            3
        )
    );
    let receipt:(u64,u64,u64,String)=engine.connection.query_row("SELECT topology_generation,publication_ordinal,record_count,input_ranges::VARCHAR FROM archive.archive_commits",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(
        receipt,
        (
            u64::MAX,
            u64::MAX,
            3,
            "[{'shard_id': 0, 'owner_epoch': 1, 'first_sequence': 1, 'last_sequence': 3}]".into()
        )
    );
    let counts:(u64,u64,u64)=engine.connection.query_row("SELECT (SELECT count(*) FROM archive.dictionaries),(SELECT count(*) FROM archive.archive_commits),(SELECT count(*) FROM ducklake_snapshots('archive'))",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(counts, (2, 1, 3));
    let (prepared, request) = prepare(engine, &facts());
    assert_eq!(
        prepared.publish_attached(&request),
        Err(PublicationError::ExistingData)
    );
}

#[test]
fn missing_dictionary_and_late_receipt_failure_leave_no_committed_rows() {
    for missing in [true, false] {
        let home = tempfile::tempdir().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let catalog = home.path().join("catalog.ducklake");
        let data = home.path().join("objects");
        let engine = open(temp.path(), &catalog, &data);
        schema::initialize(&engine.connection).unwrap();
        let all = facts();
        let selected = if missing { &all[2..] } else { &all[..] };
        let (prepared, request) = prepare(engine, selected);
        if !missing {
            // An intentionally incompatible native receipt layout makes the final
            // INSERT fail after scientific INSERTs. This bypasses the PG guard
            // only in this embedded rollback fixture.
            prepared
                .engine
                .connection
                .execute_batch("ALTER TABLE archive.archive_commits ADD COLUMN canary BLOB")
                .unwrap();
        }
        assert_eq!(
            prepared.publish_attached(&request),
            Err(if missing {
                PublicationError::MissingDictionary
            } else {
                PublicationError::Native
            })
        );
        drop(prepared);
        let temp = tempfile::tempdir().unwrap();
        let engine = open(temp.path(), &catalog, &data);
        for table in schema::TABLES.into_iter().chain(["archive_commits"]) {
            assert_eq!(
                engine
                    .connection
                    .query_row(&format!("SELECT count(*) FROM archive.{table}"), [], |r| {
                        r.get::<_, u64>(0)
                    })
                    .unwrap(),
                0
            );
        }
    }
}

#[test]
fn incorrect_semantic_identity_refuses_before_catalog_attachment() {
    let home = tempfile::tempdir().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let engine = open(
        temp.path(),
        &home.path().join("catalog.ducklake"),
        &home.path().join("objects"),
    );
    let (prepared, mut request) = prepare(engine, &facts());
    request.batch_digest = Digest256::from_bytes([0; 32]);
    assert_eq!(prepared.publish(request), Err(PublicationError::Identity));
}
