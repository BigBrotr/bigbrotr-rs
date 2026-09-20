//! Quiescent laboratory-only mutation, bypassing publication deliberately. The
//! fixture tests lookup semantics, never certifies a cut, pin or source admission.
use bigbrotr_archive_worker::rows::{RowLimits, Rows};
use bigbrotr_model::{DictionaryDescriptor, Fact, FactPayload, codec::FactDecodeLimits};
use duckdb::Connection;
use std::{fs::File, io::Read};

pub fn append(connection: &Connection, mode: &str) {
    let (sequence, class) = match mode {
        "visibility-repeat" => (11, "public"),
        "visibility-conflict" => (12, "private"),
        _ => panic!("closed fixture mutation"),
    };
    let mut bytes = Vec::new();
    File::open(format!("/out/visibility-inputs/{mode}.json"))
        .unwrap()
        .take(4097)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() <= 4096);
    let input: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(input.as_object().unwrap().len(), 2);
    let hex = input["preimage_hex"].as_str().unwrap();
    assert!(hex.len() <= 1024 && hex.len().is_multiple_of(2));
    let preimage: Vec<u8> = hex
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect();
    let fact = Fact::decode_preimage(
        &preimage,
        input["digest"].as_str().unwrap().parse().unwrap(),
        FactDecodeLimits::default(),
    )
    .unwrap();
    assert_eq!(
        fact.source_record_id().dataset_id.to_string(),
        "01".repeat(16)
    );
    assert_eq!(fact.source_record_id().shard_id, 0);
    assert_eq!(fact.source_record_id().owner_epoch.get(), 1);
    assert_eq!(fact.source_record_id().sequence.get(), sequence);
    let FactPayload::Dictionary(entry) = fact.payload() else {
        panic!()
    };
    assert_eq!(
        entry.reference().allocation_domain.to_string(),
        "04".repeat(16)
    );
    assert_eq!(entry.reference().ordinal.get(), 1);
    assert!(
        matches!(entry.descriptor(),DictionaryDescriptor::Visibility {class: actual} if actual.as_str()==class)
    );
    connection
        .execute_batch("CREATE SCHEMA stage; CREATE TABLE stage.dictionaries AS SELECT * FROM archive.dictionaries LIMIT 0")
        .unwrap();
    Rows::dictionaries(
        &[&fact],
        RowLimits {
            max_rows: 1,
            max_text_bytes: 4096,
            max_tag_nodes: 0,
        },
    )
    .unwrap()
    .append(&mut connection.appender_to_db("dictionaries", "stage").unwrap())
    .unwrap();
    connection
        .execute_batch("INSERT INTO archive.dictionaries SELECT * FROM stage.dictionaries")
        .unwrap();
}
