use super::*;
use crate::{engine::EngineLimits, schema};
use std::path::Path;

fn common() -> ReceiptExpectation {
    serde_json::from_value(
        serde_json::json!({"dataset_id":"01".repeat(16),"schema_version":1,
        "topology_generation":"0","batch_digest":"05".repeat(32),"record_count":3,
        "input_ranges":[{"shard_id":0,"owner_epoch":1,"first_sequence":1,"last_sequence":3}],
        "attempt_id":([8;16]),"publication_ordinal":3}),
    )
    .unwrap()
}
fn members() -> [Member; 3] {
    [
        Member {
            attempt_id: [8; 16],
            publication_ordinal: 3,
        },
        Member {
            attempt_id: [9; 16],
            publication_ordinal: 4,
        },
        Member {
            attempt_id: [10; 16],
            publication_ordinal: 5,
        },
    ]
}
fn open(home: &Path, scratch: &Path, readonly: bool) -> Engine {
    let engine = Engine::open(
        &scratch.canonicalize().unwrap(),
        EngineLimits {
            memory_bytes: 128 << 20,
            temp_bytes: 64 << 20,
            threads: 1,
        },
    )
    .unwrap();
    engine
        .load_bundle(Path::new(
            &std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").expect("pinned bundle"),
        ))
        .unwrap();
    engine.connection.execute_batch(&format!("ATTACH 'ducklake:{}' AS archive (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false{})",
        home.join("catalog.ducklake").display(), home.join("objects").display(),
        if readonly { ", CREATE_IF_NOT_EXISTS false, READ_ONLY" } else { "" })).unwrap();
    engine
}
fn insert(c: &Connection, id: &str, digest: &str, ordinal: u64) {
    c.execute_batch(&format!("INSERT INTO archive.archive_commits VALUES(from_hex('{}'),1,0,from_hex('{}'),3,[struct_pack(shard_id:=0::UINTEGER,owner_epoch:=1::UBIGINT,first_sequence:=1::UBIGINT,last_sequence:=3::UBIGINT)],from_hex('{}'),{ordinal})",
        "01".repeat(16),digest.repeat(32),id.repeat(16))).unwrap();
}

#[test]
fn grouped_receipts_full_eof_selects_none_or_each_exact_member_and_keeps_strict_guard() {
    let common = common();
    let members = members();
    let read = GroupRead {
        common: &common,
        members: &members,
    };
    for selected in [None, Some(0), Some(1), Some(2)] {
        let home = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let engine = open(home.path(), scratch.path(), false);
        schema::initialize(&engine.connection).unwrap();
        insert(&engine.connection, "20", "06", 1);
        if let Some(index) = selected {
            insert(
                &engine.connection,
                ["08", "09", "0a"][index],
                "05",
                index as u64 + 3,
            );
        }
        insert(&engine.connection, "21", "07", 6);
        drop(engine);
        let scratch = tempfile::tempdir().unwrap();
        let engine = open(home.path(), scratch.path(), true);
        assert_eq!(
            inspect_attached(&engine.connection, read, 128),
            Ok(selected)
        );
        // Both original A and B retain their unchanged OR-collision guard on C.
        if selected == Some(2) {
            for index in [0, 1] {
                let mut strict = common.clone();
                strict.attempt_id = members[index].attempt_id;
                strict.publication_ordinal = members[index].publication_ordinal;
                assert_eq!(
                    crate::receipt::inspect_attached(&engine.connection, &strict, 128),
                    Err(ReceiptError::Mismatch)
                );
                engine.connection.execute_batch("ROLLBACK").unwrap();
            }
        }
        assert!(
            engine
                .connection
                .execute_batch("DELETE FROM archive.archive_commits")
                .is_err()
        );
    }
}

#[test]
fn grouped_receipts_reject_second_duplicate_crossed_semantic_and_late_unrelated_rows() {
    let common = common();
    let members = members();
    let read = GroupRead {
        common: &common,
        members: &members,
    };
    for mutation in [
        "second-a",
        "second-b",
        "duplicate",
        "unknown-digest",
        "crossed-id",
        "crossed-ordinal",
        "schema_version=2",
        "topology_generation=1",
        "record_count=4",
        "input_ranges=[struct_pack(shard_id:=0::UINTEGER,owner_epoch:=1::UBIGINT,first_sequence:=2::UBIGINT,last_sequence:=4::UBIGINT)]",
        "malformed-unrelated",
        "foreign-unrelated",
    ] {
        let home = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let engine = open(home.path(), scratch.path(), false);
        schema::initialize(&engine.connection).unwrap();
        insert(&engine.connection, "0a", "05", 5);
        match mutation {
            "second-a" => insert(&engine.connection, "08", "05", 3),
            "second-b" => insert(&engine.connection, "09", "05", 4),
            "duplicate" => insert(&engine.connection, "0a", "05", 5),
            "unknown-digest" => insert(&engine.connection, "0b", "05", 6),
            "crossed-id" => insert(&engine.connection, "09", "06", 5),
            "crossed-ordinal" => insert(&engine.connection, "0a", "06", 4),
            "malformed-unrelated" | "foreign-unrelated" => {
                insert(&engine.connection, "20", "06", 6);
                engine
                    .connection
                    .execute_batch(&format!(
                        "UPDATE archive.archive_commits SET {} WHERE publication_ordinal=6",
                        if mutation == "malformed-unrelated" {
                            "record_count=0"
                        } else {
                            "dataset_id=from_hex(repeat('02',16))"
                        }
                    ))
                    .unwrap();
            }
            assignment => engine
                .connection
                .execute_batch(&format!("UPDATE archive.archive_commits SET {assignment}"))
                .unwrap(),
        }
        drop(engine);
        let scratch = tempfile::tempdir().unwrap();
        let engine = open(home.path(), scratch.path(), true);
        assert_eq!(
            inspect_attached(&engine.connection, read, 128),
            Err(ReceiptError::Mismatch),
            "{mutation}"
        );
    }
}

#[test]
fn grouped_receipts_scan_max_plus_one_and_persisted_range_limits_refuse() {
    let common = common();
    let members = members();
    let read = GroupRead {
        common: &common,
        members: &members,
    };
    for ranges in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let engine = open(home.path(), scratch.path(), false);
        schema::initialize(&engine.connection).unwrap();
        insert(&engine.connection, "0a", "05", 5);
        if ranges {
            engine.connection.execute_batch("UPDATE archive.archive_commits SET input_ranges=list_transform(range(1025), i->struct_pack(shard_id:=0::UINTEGER,owner_epoch:=1::UBIGINT,first_sequence:=(i+1)::UBIGINT,last_sequence:=(i+1)::UBIGINT))").unwrap();
        } else {
            for ordinal in 6..=133 {
                insert(&engine.connection, "20", "06", ordinal);
            }
        }
        drop(engine);
        let scratch = tempfile::tempdir().unwrap();
        let engine = open(home.path(), scratch.path(), true);
        assert_eq!(
            inspect_attached(&engine.connection, read, 128),
            Err(ReceiptError::ResourceLimit)
        );
    }
    let connection = Connection::open_in_memory().unwrap();
    for max in [0, 2, 129, usize::MAX] {
        assert_eq!(
            inspect_attached(&connection, read, max),
            Err(ReceiptError::ResourceLimit)
        );
        connection.execute_batch("BEGIN; ROLLBACK").unwrap();
    }
}

#[test]
fn grouped_receipts_missing_or_corrupt_later_native_object_never_delivers_a_choice() {
    let common = common();
    let members = members();
    let read = GroupRead {
        common: &common,
        members: &members,
    };
    for missing in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let engine = open(home.path(), scratch.path(), false);
        schema::initialize(&engine.connection).unwrap();
        insert(&engine.connection, "0a", "05", 5);
        insert(&engine.connection, "20", "06", 6);
        drop(engine);
        let meta = Connection::open(home.path().join("catalog.ducklake")).unwrap();
        let relative: String = meta.query_row("SELECT s.path||t.path||d.path FROM ducklake_data_file d JOIN ducklake_table t USING(table_id) JOIN ducklake_schema s USING(schema_id) ORDER BY d.data_file_id DESC LIMIT 1",[],|r|r.get(0)).unwrap();
        drop(meta);
        let file = home.path().join("objects").join(relative);
        if missing {
            std::fs::remove_file(file).unwrap();
        } else {
            std::fs::write(file, b"corrupt late native receipt fixture").unwrap();
        }
        let scratch = tempfile::tempdir().unwrap();
        let engine = open(home.path(), scratch.path(), true);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            inspect_attached(&engine.connection, read, 128)
        }));
        assert!(
            matches!(result, Err(_) | Ok(Err(ReceiptError::Native))),
            "{result:?}"
        );
    }
}

#[test]
fn grouped_request_parser_is_closed_and_enforces_exact_wire_and_member_bounds() {
    let request = serde_json::json!({"catalog":{"dsn":"host=127.0.0.1 user=fixture","data_root":"/archive/"},
        "catalog_id":"02".repeat(16),"expected":{"common":common(),"members":members()}});
    let bytes = serde_json::to_vec(&request).unwrap();
    assert!(GroupRequest::read(bytes.as_slice()).is_ok());
    let mut padded = bytes.clone();
    padded.resize(MAX_INPUT_BYTES, b' ');
    assert!(GroupRequest::read(padded.as_slice()).is_ok());
    padded.push(b' ');
    assert!(matches!(
        GroupRequest::read(padded.as_slice()),
        Err(ReceiptError::ResourceLimit)
    ));
    for field in [
        "root",
        "expected",
        "common",
        "member",
        "empty",
        "duplicate",
        "order",
        "maximum",
    ] {
        let mut altered = request.clone();
        match field {
            "root" => altered["unknown"] = true.into(),
            "expected" => altered["expected"]["unknown"] = true.into(),
            "common" => altered["expected"]["common"]["unknown"] = true.into(),
            "member" => altered["expected"]["members"][2]["unknown"] = true.into(),
            "empty" => altered["expected"]["members"] = serde_json::json!([]),
            "duplicate" => {
                altered["expected"]["members"][2] = altered["expected"]["members"][1].clone()
            }
            "order" => altered["expected"]["members"]
                .as_array_mut()
                .unwrap()
                .swap(1, 2),
            "maximum" => {
                altered["expected"]["members"] =
                    serde_json::to_value(vec![members()[0]; 129]).unwrap()
            }
            _ => unreachable!(),
        }
        assert!(
            GroupRequest::read(serde_json::to_vec(&altered).unwrap().as_slice()).is_err(),
            "{field}"
        );
    }
    for invalid in [
        format!("{}{{}}", String::from_utf8(bytes.clone()).unwrap()),
        format!(
            "{},\"catalog_id\":\"{}\"}}",
            String::from_utf8(bytes[..bytes.len() - 1].to_vec()).unwrap(),
            "02".repeat(16)
        ),
    ] {
        assert!(GroupRequest::read(invalid.as_bytes()).is_err());
    }
}
