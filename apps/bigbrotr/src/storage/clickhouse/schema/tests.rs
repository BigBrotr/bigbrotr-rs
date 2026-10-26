use super::*;
use crate::storage::clickhouse::rows::*;
use clickhouse::Row;

fn tables() -> IndexTables {
    IndexTables::new(
        "serving_fixture".into(),
        std::array::from_fn(|i| [i as u8 + 1; 16]),
    )
    .unwrap()
}

#[test]
fn closed_table_columns_match_the_typed_serialization_order() {
    let expected: [&[&str]; 10] = [
        DictionaryRow::COLUMN_NAMES,
        EventRow::COLUMN_NAMES,
        EligibilityRow::COLUMN_NAMES,
        ReceiveRow::COLUMN_NAMES,
        EventIndexRow::COLUMN_NAMES,
        EventIndexRow::COLUMN_NAMES,
        EventIndexRow::COLUMN_NAMES,
        TagRow::COLUMN_NAMES,
        ProvenanceRow::COLUMN_NAMES,
        ProvenanceRow::COLUMN_NAMES,
    ];
    for (target, expected) in IndexTarget::ALL.into_iter().zip(expected) {
        assert_eq!(
            columns(target).map(|c| c.name).collect::<Vec<_>>(),
            expected
        );
    }
    // Fixed independent field inventory. A typo in both schema dispatch and its
    // caller cannot silently turn an explicit empty family into an omitted one.
    assert_eq!(
        IndexTarget::ALL.map(|target| columns(target).count()),
        [35, 15, 13, 15, 12, 12, 12, 10, 18, 18]
    );
}

#[test]
fn fresh_manifest_refuses_identifier_injection_duplicate_and_nil_uuids() {
    let ids = std::array::from_fn(|i| [i as u8 + 1; 16]);
    for database in [
        "",
        "system",
        "INFORMATION_SCHEMA",
        "a.b",
        "a`b",
        "a;DROP",
        "a b",
        "1first",
    ] {
        assert_eq!(
            IndexTables::new(database.into(), ids).unwrap_err(),
            SchemaError::InvalidDatabase
        );
    }
    assert_eq!(
        IndexTables::new("a".repeat(64), ids).unwrap_err(),
        SchemaError::InvalidDatabase
    );
    let mut changed = ids;
    changed[4] = changed[1];
    assert_eq!(
        IndexTables::new("owned".into(), changed).unwrap_err(),
        SchemaError::InvalidUuid
    );
    changed[4] = [0; 16];
    assert_eq!(
        IndexTables::new("owned".into(), changed).unwrap_err(),
        SchemaError::InvalidUuid
    );
}

#[test]
fn every_family_is_append_only_with_explicit_persistence_and_no_calendar_cast() {
    let tables = tables();
    for target in IndexTarget::ALL {
        let ddl = tables.create_table(target);
        assert!(ddl.starts_with(&format!(
            "CREATE TABLE `serving_fixture`.`{}` UUID '",
            target.as_str()
        )));
        assert!(ddl.contains("ENGINE = MergeTree ORDER BY"));
        assert!(!ddl.contains("PARTITION BY"));
        assert!(ddl.ends_with("SETTINGS fsync_after_insert = 1, fsync_part_directory = 1, non_replicated_deduplication_window = 0"));
        for forbidden in [
            "IF NOT EXISTS",
            "Replacing",
            "TTL",
            "PROJECTION",
            "DateTime",
            "toDate",
            "DROP",
            "ALTER",
        ] {
            assert!(
                !ddl.contains(forbidden),
                "unselected physical behavior: {forbidden}"
            );
        }
    }
    assert_eq!(
        columns(IndexTarget::Events)
            .find(|c| c.name == "created_at")
            .unwrap()
            .ty,
        "UInt64"
    );
    assert_eq!(
        columns(IndexTarget::EventEligibilityFacts)
            .find(|c| c.name == "sig")
            .unwrap()
            .ty,
        "FixedString(64)"
    );
    assert_eq!(
        columns(IndexTarget::Events)
            .find(|c| c.name == "tags")
            .unwrap()
            .ty,
        "Array(Array(String))"
    );
}

#[test]
fn manifest_identity_distinguishes_database_and_every_table_uuid() {
    let original = tables();
    // Python/hashlib over the literal schema-1 field/key inventory, the database
    // serving_fixture, UUID bytes 01..0a repeated16 and no PARTITION BY. No Rust
    // encoder or observed engine metadata computed this independent vector.
    assert_eq!(
        original.identity_digest().to_string(),
        "a696c1e480ddeafdcab16d13eb8aac8b1eaddf5282b8596eb4d480b4bb43c8c9"
    );
    let ids = std::array::from_fn(|i| [i as u8 + 1; 16]);
    assert_ne!(
        original.identity_digest(),
        IndexTables::new("another".into(), ids)
            .unwrap()
            .identity_digest()
    );
    for i in 0..10 {
        let mut changed = ids;
        changed[i][15] = 100;
        assert_ne!(
            original.identity_digest(),
            IndexTables::new("serving_fixture".into(), changed)
                .unwrap()
                .identity_digest()
        );
    }
    assert_eq!(
        uuid_text([0x12; 16]),
        "12121212-1212-1212-1212-121212121212"
    );
}

#[test]
fn retained_manifest_roundtrips_and_refuses_changed_format_unknown_fields_and_aliases() {
    let expected = tables();
    let bytes = expected.encode_manifest().unwrap();
    assert!(bytes.len() < MAX_TABLE_MANIFEST_BYTES);
    assert_eq!(IndexTables::decode_manifest(&bytes).unwrap(), expected);
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for (field, bad) in [
        ("format", "unknown/2"),
        ("server_version", "26.5.1.882"),
        ("database", "a.b"),
    ] {
        let mut changed = value.clone();
        changed[field] = bad.into();
        assert!(IndexTables::decode_manifest(&serde_json::to_vec(&changed).unwrap()).is_err());
    }
    let mut changed = value.clone();
    changed["unrecognized"] = true.into();
    assert!(IndexTables::decode_manifest(&serde_json::to_vec(&changed).unwrap()).is_err());
    let mut changed = value;
    changed["table_uuids"][0] = "01010101-0101-0101-0101-01010101010A".into();
    assert!(IndexTables::decode_manifest(&serde_json::to_vec(&changed).unwrap()).is_err());
    assert!(IndexTables::decode_manifest(&vec![b' '; MAX_TABLE_MANIFEST_BYTES + 1]).is_err());
}

#[test]
fn closed_schemas_fit_the_selected_bounded_client_header_profile() {
    use clickhouse::_priv::{RBWNAT_MAX_NAME_BYTES, RBWNAT_MAX_TYPE_BYTES, RBWNAT_MAX_TYPE_DEPTH};
    let mut maxima = (0usize, 0usize, 0usize);
    for target in IndexTarget::ALL {
        for column in columns(target) {
            maxima.0 = maxima.0.max(column.name.len());
            maxima.1 = maxima.1.max(column.ty.len());
            assert!(!column.name.is_empty() && column.name.len() <= RBWNAT_MAX_NAME_BYTES);
            assert!(!column.ty.is_empty() && column.ty.len() <= RBWNAT_MAX_TYPE_BYTES);
            let mut depth = 0usize;
            for byte in column.ty.bytes() {
                match byte {
                    b'(' => {
                        depth += 1;
                        maxima.2 = maxima.2.max(depth);
                        assert!(depth <= RBWNAT_MAX_TYPE_DEPTH);
                    }
                    b')' => {
                        assert!(depth > 0);
                        depth -= 1;
                    }
                    _ => {}
                }
            }
            assert_eq!(depth, 0);
        }
    }
    println!(
        "closed schema maxima: name_bytes={} type_bytes={} type_depth={}",
        maxima.0, maxima.1, maxima.2
    );
}
