use super::*;
use arrow::array::{BinaryArray, FixedSizeBinaryArray, StringArray};
use bigbrotr_model::codec::FactDecodeLimits;
use serde_json::{Value, json};
use std::collections::BTreeMap;

const DESCRIPTOR_FIELDS: &str = "event_id BLOB, class VARCHAR, url VARCHAR, clock VARCHAR, vantage VARCHAR,
 software VARCHAR, method_id STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT),
 public_key BLOB, route_class VARCHAR, software_version VARCHAR, label VARCHAR,
 visibility_scope_id STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT),
 input VARCHAR, algorithm VARCHAR, capability VARCHAR, output_unit VARCHAR, failure_taxonomy VARCHAR,
 algorithm_version USMALLINT, protocol_revision VARCHAR, format VARCHAR, format_version USMALLINT,
 source_dataset_id BLOB, source_artifact_digest BLOB";

fn fixtures() -> Vec<(String, String, Fact)> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../crates/bigbrotr-model/tests/fixtures/dictionary-v1.json"
    ))
    .unwrap();
    fixture["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["family"] == "fact")
        .map(|v| {
            let bytes: Vec<_> = v["preimage_hex"]
                .as_str()
                .unwrap()
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
                .collect();
            let digest = v["sha256"].as_str().unwrap();
            (
                v["name"].as_str().unwrap().into(),
                digest.into(),
                Fact::decode_preimage(&bytes, digest.parse().unwrap(), FactDecodeLimits::default())
                    .unwrap(),
            )
        })
        .collect()
}
fn reference(kind: u16) -> Value {
    json!({"dataset_id":"01".repeat(16), "dictionary_kind":kind, "allocation_domain":"02".repeat(16), "ordinal":1})
}
fn expected(name: &str) -> (u16, &'static str, Value) {
    // Literal oracle from the independent fixture authoring input, not decoded
    // product descriptors or a production row-to-JSON adapter.
    match name {
        "visibility-public" => (4, "visibility", json!({"class":"public"})),
        "event" => (8, "event", json!({"event_id":"aa".repeat(32)})),
        "endpoint" => (
            1,
            "endpoint",
            json!({"url":"wss://relay.example/path?b=2&a=1"}),
        ),
        "observer-known" => (
            2,
            "observer",
            json!({"clock":"system-utc","vantage":"lab-a","software":"BigBrotr","method_id":reference(5),"public_key":"bb".repeat(32),"route_class":"public","software_version":"0.1.0-dev"}),
        ),
        "observer-unknown" => (2, "observer", json!({})),
        "access-public" => (
            3,
            "access-scope",
            json!({"class":"public","label":null,"visibility_scope_id":reference(4)}),
        ),
        "access-protected" => (
            3,
            "access-scope",
            json!({"class":"protected","label":"research-a","visibility_scope_id":reference(4)}),
        ),
        "event-verification" => (
            5,
            "event-verification",
            json!({"input":"nip01-event/1","algorithm":"sha256-bip340","capability":"event-id-and-signature","output_unit":"verification-result","failure_taxonomy":"bigbrotr-nip01-verifier/1","algorithm_version":1,"protocol_revision":"a2494f4f81d46684e5814a9bf35e2b1df978f955"}),
        ),
        "lineage-native" => (
            9,
            "dataset-lineage",
            json!({"format":"bigbrotr-native","format_version":1,"source_dataset_id":"03".repeat(16),"source_artifact_digest":"cc".repeat(32)}),
        ),
        "lineage-jsonl" => (
            9,
            "dataset-lineage",
            json!({"format":"nostr-jsonl","format_version":1,"source_dataset_id":null,"source_artifact_digest":"dd".repeat(32)}),
        ),
        _ => panic!("unknown fixture"),
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
fn scalar(array: &dyn Array, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    macro_rules! primitive {
        ($ty:ty) => {
            json!(array.as_any().downcast_ref::<$ty>().unwrap().value(row))
        };
    }
    match array.data_type() {
        DataType::Utf8 => primitive!(StringArray),
        DataType::UInt16 => primitive!(UInt16Array),
        DataType::UInt32 => primitive!(UInt32Array),
        DataType::UInt64 => primitive!(UInt64Array),
        DataType::Int64 => primitive!(Int64Array),
        DataType::Binary => json!(hex(array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(row))),
        DataType::FixedSizeBinary(_) => json!(hex(array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(row))),
        DataType::Struct(fields) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            Value::Object(
                fields
                    .iter()
                    .zip(array.columns())
                    .map(|(field, child)| (field.name().clone(), scalar(child.as_ref(), row)))
                    .collect(),
            )
        }
        other => panic!("unexpected physical type {other}"),
    }
}

#[test]
fn all_closed_descriptors_survive_native_parquet_with_nulls_and_foreign_lineage() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 10);
    let rows = Rows::dictionaries(
        &fixtures.iter().map(|(_, _, f)| f).collect::<Vec<_>>(),
        tests::limits(),
    )
    .unwrap();
    let fields = format!(
        "reference STRUCT(dataset_id BLOB, dictionary_kind USMALLINT, allocation_domain BLOB, ordinal UBIGINT) NOT NULL, normalization_version USMALLINT NOT NULL, descriptor_schema VARCHAR NOT NULL, descriptor_version USMALLINT NOT NULL, descriptor_value STRUCT({DESCRIPTOR_FIELDS}) NOT NULL"
    );
    let (_artifact, reader, _) = metadata_tests::reopen(rows, &fields);
    let mut query = reader.connection.prepare("SELECT * FROM metadata").unwrap();
    let mut actual = BTreeMap::new();
    for batch in query.query_arrow([]).unwrap() {
        for row in 0..batch.num_rows() {
            let value: Value = Value::Object(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .map(|(f, a)| (f.name().clone(), scalar(a.as_ref(), row)))
                    .collect(),
            );
            assert!(
                actual
                    .insert(value["payload_digest"].as_str().unwrap().to_owned(), value)
                    .is_none()
            );
        }
    }
    assert_eq!(actual.len(), 10);
    let names = [
        "event_id",
        "class",
        "url",
        "clock",
        "vantage",
        "software",
        "method_id",
        "public_key",
        "route_class",
        "software_version",
        "label",
        "visibility_scope_id",
        "input",
        "algorithm",
        "capability",
        "output_unit",
        "failure_taxonomy",
        "algorithm_version",
        "protocol_revision",
        "format",
        "format_version",
        "source_dataset_id",
        "source_artifact_digest",
    ];
    for (name, digest, _) in fixtures {
        let row = &actual[&digest];
        let (kind, schema, fields) = expected(&name);
        assert_eq!(
            row["source_record_id"],
            json!({"dataset_id":"01".repeat(16),"shard_id":0,"owner_epoch":1,"sequence":1})
        );
        assert_eq!(row["record_schema_version"], 1);
        assert_eq!(row["record_kind"], "dictionary");
        assert_eq!(row["visibility_scope_id"], reference(4));
        assert_eq!(row["recorded_at"], -1);
        assert_eq!(row["reference"], reference(kind));
        assert_eq!(row["normalization_version"], 1);
        assert_eq!(row["descriptor_schema"], schema);
        assert_eq!(row["descriptor_version"], 1);
        let mut descriptor: serde_json::Map<String, Value> =
            names.iter().map(|n| (n.to_string(), Value::Null)).collect();
        descriptor.extend(fields.as_object().unwrap().clone());
        assert_eq!(row["descriptor_value"], Value::Object(descriptor), "{name}");
    }
}

#[test]
fn descriptor_text_bound_is_exact_and_rows_must_be_dictionary_facts() {
    let fixtures = fixtures();
    let value = &fixtures.iter().find(|(n, _, _)| n == "endpoint").unwrap().2;
    let exact = RowLimits {
        max_text_bytes: "wss://relay.example/path?b=2&a=1".len(),
        max_tag_nodes: 0,
        ..tests::limits()
    };
    assert!(Rows::dictionaries(&[value], exact).is_ok());
    assert!(matches!(
        Rows::dictionaries(
            &[value],
            RowLimits {
                max_text_bytes: exact.max_text_bytes - 1,
                ..exact
            }
        ),
        Err(RowError::TextLimit)
    ));
    assert!(matches!(
        Rows::dictionaries(&[], exact),
        Err(RowError::RowLimit)
    ));
    assert!(matches!(
        Rows::dictionaries(
            &[value, value],
            RowLimits {
                max_rows: 1,
                ..exact
            }
        ),
        Err(RowError::RowLimit)
    ));
    let other = metadata_tests::frozen("provenance-correction");
    assert!(matches!(
        Rows::dictionaries(&[&other], exact),
        Err(RowError::WrongFamily)
    ));
    assert!(matches!(
        Rows::dictionaries(&[value, &other], tests::limits()),
        Err(RowError::WrongFamily)
    ));
}

#[test]
fn raw_binding_negative_control_shifts_unmasked_struct_children_in_parquet() {
    // Valid Arrow can retain non-null child placeholders under a null struct.
    // This bypasses our constructors to reproduce the pinned native boundary.
    // The positive product oracle above must retain complete descriptor values.
    for mask_children in [false, true] {
        let child = UInt64Array::from_iter((0..10).map(|i| {
            if i == 3 {
                Some(73)
            } else if mask_children {
                None
            } else {
                Some(0)
            }
        }));
        let validity: NullBuffer = (0..10).map(|i| i == 3).collect();
        let inner =
            nullable_structure(vec![("ordinal", Arc::new(child))], &[], Some(validity)).unwrap();
        let outer = nullable_structure(vec![("nested", inner)], &["nested"], None).unwrap();
        let rows = Rows::finish(vec![("descriptor", outer)], &[]).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let engine = tests::engine(temp.path());
        engine
            .connection
            .execute_batch(
                "CREATE TABLE control(descriptor STRUCT(nested STRUCT(ordinal UBIGINT)))",
            )
            .unwrap();
        rows.append(&mut engine.connection.appender("control").unwrap())
            .unwrap();
        assert_eq!(
            engine
                .connection
                .query_row(
                    "SELECT sum(descriptor.nested.ordinal)::UBIGINT FROM control",
                    [],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            73
        );
        let artifact = tempfile::tempdir().unwrap();
        let file = artifact.path().join("control.parquet");
        engine
            .connection
            .execute(
                "COPY control TO ? (FORMAT PARQUET)",
                [file.to_str().unwrap()],
            )
            .unwrap();
        let values = engine.connection.query_row("SELECT count(descriptor.nested), sum(descriptor.nested.ordinal)::UBIGINT FROM read_parquet(?)", [file.to_str().unwrap()], |r| Ok((r.get::<_, u64>(0)?,r.get::<_, u64>(1)?))).unwrap();
        assert_eq!(values, (1, if mask_children { 73 } else { 0 }));
    }
}

#[test]
fn native_sql_nullable_struct_parquet_control() {
    let temp = tempfile::tempdir().unwrap();
    let engine = tests::engine(temp.path());
    engine.connection.execute_batch("CREATE TABLE control AS SELECT {'nested': CASE WHEN i=3 THEN {'ordinal': 73::UBIGINT} ELSE NULL END} descriptor FROM range(10) t(i)").unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let file = artifact.path().join("control.parquet");
    engine
        .connection
        .execute(
            "COPY control TO ? (FORMAT PARQUET)",
            [file.to_str().unwrap()],
        )
        .unwrap();
    let values = engine.connection.query_row("SELECT count(descriptor.nested), sum(descriptor.nested.ordinal)::UBIGINT FROM read_parquet(?)", [file.to_str().unwrap()], |r| Ok((r.get::<_, u64>(0)?,r.get::<_, u64>(1)?))).unwrap();
    assert_eq!(values, (1, 73));
}
