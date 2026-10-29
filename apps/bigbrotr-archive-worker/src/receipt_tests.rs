use super::*;
use crate::{engine::EngineLimits, schema};
use std::path::Path;

fn expected() -> ReceiptExpectation {
    serde_json::from_value(serde_json::json!({"dataset_id":"01".repeat(16),"schema_version":1,
        "topology_generation":"18446744073709551615","batch_digest":"05".repeat(32),
        "record_count":3,"input_ranges":[{"shard_id":0,"owner_epoch":1,"first_sequence":1,"last_sequence":3}],
        "attempt_id":([9;16]),"publication_ordinal":u64::MAX})).unwrap()
}
#[test]
fn invalid_scan_does_not_open_a_transaction() {
    let connection = Connection::open_in_memory().unwrap();
    for maximum in [0, usize::MAX] {
        assert_eq!(
            inspect_attached(&connection, &expected(), maximum),
            Err(ReceiptError::InvalidInput)
        );
        connection.execute_batch("BEGIN; ROLLBACK").unwrap();
    }
    let mut invalid = expected();
    invalid.record_count = 0;
    assert_eq!(
        inspect_attached(&connection, &invalid, 1),
        Err(ReceiptError::InvalidInput)
    );
    connection.execute_batch("BEGIN; ROLLBACK").unwrap();
}
fn open(home: &Path, temp: &Path, readonly: bool) -> Engine {
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
            &std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").expect("pinned bundle"),
        ))
        .unwrap();
    engine.connection.execute_batch(&format!("ATTACH 'ducklake:{}' AS archive (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false{})",
        home.join("catalog.ducklake").display(), home.join("objects").display(),
        if readonly { ", CREATE_IF_NOT_EXISTS false, READ_ONLY" } else { "" })).unwrap();
    engine
}
fn insert(c: &Connection) {
    // Independent literal schema1 physical receipt, including unsigned maxima.
    c.execute_batch(&format!("INSERT INTO archive.archive_commits VALUES (from_hex('{}'),1,18446744073709551615,from_hex('{}'),3,[struct_pack(shard_id:=0::UINTEGER,owner_epoch:=1::UBIGINT,first_sequence:=1::UBIGINT,last_sequence:=3::UBIGINT)],from_hex('{}'),18446744073709551615)", "01".repeat(16),"05".repeat(32),"09".repeat(16))).unwrap();
}
#[test]
fn complete_receipt_and_absence_survive_a_fresh_readonly_native_connection() {
    let home = tempfile::tempdir().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let e = open(home.path(), temp.path(), false);
    schema::initialize(&e.connection).unwrap();
    insert(&e.connection);
    drop(e);
    for present in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let e = open(home.path(), temp.path(), true);
        let mut wanted = expected();
        if !present {
            wanted.attempt_id = [10; 16];
            wanted.batch_digest = bigbrotr_model::Digest256::from_bytes([6; 32]);
            wanted.publication_ordinal = 2;
        }
        assert_eq!(
            inspect_attached(&e.connection, &wanted, 1),
            Ok(if present {
                ReceiptObservation::Present
            } else {
                ReceiptObservation::Absent
            })
        );
        assert!(
            e.connection
                .execute_batch("DELETE FROM archive.archive_commits")
                .is_err()
        );
    }
}

#[test]
fn retained_replacement_uses_the_strict_receipt_guard_through_complete_eof() {
    for case in [
        "replacement",
        "absent",
        "old-and-replacement",
        "duplicate",
        "third-digest",
        "id-collision",
        "ordinal-collision",
    ] {
        let home = tempfile::tempdir().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let e = open(home.path(), temp.path(), false);
        schema::initialize(&e.connection).unwrap();
        if case != "absent" {
            insert(&e.connection);
            e.connection
                .execute_batch("UPDATE archive.archive_commits SET publication_ordinal=4")
                .unwrap();
        }
        let extra = match case {
            "old-and-replacement" => Some(("08", "05", 3)),
            "duplicate" => Some(("09", "05", 4)),
            "third-digest" => Some(("0a", "05", 5)),
            "id-collision" => Some(("09", "06", 5)),
            "ordinal-collision" => Some(("0a", "06", 4)),
            _ => None,
        };
        if let Some((id, digest, ordinal)) = extra {
            e.connection.execute_batch(&format!(
                "INSERT INTO archive.archive_commits SELECT dataset_id,schema_version,topology_generation,from_hex('{}'),record_count,input_ranges,from_hex('{}'),{ordinal} FROM archive.archive_commits WHERE publication_ordinal=4 LIMIT 1",
                digest.repeat(32), id.repeat(16))).unwrap();
        }
        drop(e);
        let temp = tempfile::tempdir().unwrap();
        let e = open(home.path(), temp.path(), true);
        let mut replacement = expected();
        replacement.publication_ordinal = 4;
        let mut original = replacement.clone();
        original.attempt_id = [8; 16];
        original.publication_ordinal = 3;
        if case == "replacement" {
            // The old observer is the concrete retry counterexample. Targeting
            // the one already-current replacement keeps the generic guard.
            assert_eq!(
                inspect_attached(&e.connection, &original, 16),
                Err(ReceiptError::Mismatch)
            );
            assert_eq!(
                inspect_attached(&e.connection, &replacement, 16),
                Ok(ReceiptObservation::Present)
            );
        } else if case == "absent" {
            assert_eq!(
                inspect_attached(&e.connection, &replacement, 16),
                Ok(ReceiptObservation::Absent)
            );
        } else {
            // Includes rows after the target: no early Present before EOF.
            assert_eq!(
                inspect_attached(&e.connection, &replacement, 16),
                Err(ReceiptError::Mismatch),
                "{case}"
            );
        }
    }
}
#[test]
fn every_identity_field_duplicate_and_resource_truncation_refuse() {
    for mutation in [
        "dataset_id=from_hex(repeat('02',16))",
        "schema_version=2",
        "topology_generation=0",
        "batch_digest=from_hex(repeat('06',32))",
        "record_count=4",
        "input_ranges=[struct_pack(shard_id:=0::UINTEGER,owner_epoch:=1::UBIGINT,first_sequence:=2::UBIGINT,last_sequence:=4::UBIGINT)]",
        "attempt_id=from_hex(repeat('0a',16))",
        "publication_ordinal=2",
        "duplicate",
        "limit",
    ] {
        let home = tempfile::tempdir().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let e = open(home.path(), temp.path(), false);
        schema::initialize(&e.connection).unwrap();
        insert(&e.connection);
        if matches!(mutation, "duplicate" | "limit") {
            insert(&e.connection);
        } else {
            e.connection
                .execute_batch(&format!("UPDATE archive.archive_commits SET {mutation}"))
                .unwrap();
        }
        drop(e);
        let temp = tempfile::tempdir().unwrap();
        let e = open(home.path(), temp.path(), true);
        assert_eq!(
            inspect_attached(
                &e.connection,
                &expected(),
                if mutation == "limit" { 1 } else { 16 }
            ),
            Err(if mutation == "limit" {
                ReceiptError::ResourceLimit
            } else {
                ReceiptError::Mismatch
            }),
            "{mutation}"
        );
    }
}
#[test]
fn missing_receipt_object_cannot_become_absence_even_for_an_unrelated_target() {
    for missing in [true, false] {
        let home = tempfile::tempdir().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let e = open(home.path(), temp.path(), false);
        schema::initialize(&e.connection).unwrap();
        insert(&e.connection);
        drop(e);
        let meta = Connection::open(home.path().join("catalog.ducklake")).unwrap();
        let relative:String=meta.query_row("SELECT s.path||t.path||d.path FROM ducklake_data_file d JOIN ducklake_table t USING(table_id) JOIN ducklake_schema s USING(schema_id)",[],|r|r.get(0)).unwrap();
        drop(meta);
        let file = home.path().join("objects").join(relative);
        assert!(file.is_file());
        if missing {
            std::fs::remove_file(file).unwrap();
        } else {
            std::fs::write(file, b"corrupt receipt fixture").unwrap();
        }
        let temp = tempfile::tempdir().unwrap();
        let e = open(home.path(), temp.path(), true);
        let mut wanted = expected();
        wanted.batch_digest = bigbrotr_model::Digest256::from_bytes([6; 32]);
        wanted.attempt_id = [10; 16];
        wanted.publication_ordinal = 2;
        // Either execute or lazy fetch can fail. The CLI suppresses native panic
        // diagnostics and only reports observation after successful stream EOF.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            inspect_attached(&e.connection, &wanted, 16)
        }));
        assert!(
            matches!(outcome, Err(_) | Ok(Err(ReceiptError::Native))),
            "{outcome:?}"
        );
    }
}

#[test]
fn oversized_persisted_ranges_are_a_resource_refusal() {
    let home = tempfile::tempdir().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let e = open(home.path(), temp.path(), false);
    schema::initialize(&e.connection).unwrap();
    insert(&e.connection);
    e.connection.execute_batch("UPDATE archive.archive_commits SET input_ranges=list_transform(range(1025), i->struct_pack(shard_id:=0::UINTEGER,owner_epoch:=1::UBIGINT,first_sequence:=(i+1)::UBIGINT,last_sequence:=(i+1)::UBIGINT))").unwrap();
    drop(e);
    let temp = tempfile::tempdir().unwrap();
    let e = open(home.path(), temp.path(), true);
    assert_eq!(
        inspect_attached(&e.connection, &expected(), 16),
        Err(ReceiptError::ResourceLimit)
    );
}

#[test]
fn malformed_unrelated_receipt_cannot_become_absence() {
    for mutation in [
        "schema_version=2",
        "record_count=0",
        "publication_ordinal=0",
        "dataset_id=from_hex(repeat('02',16))",
        "input_ranges=[]",
    ] {
        let home = tempfile::tempdir().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let e = open(home.path(), temp.path(), false);
        schema::initialize(&e.connection).unwrap();
        insert(&e.connection);
        e.connection
            .execute_batch(&format!("UPDATE archive.archive_commits SET {mutation}"))
            .unwrap();
        drop(e);
        let temp = tempfile::tempdir().unwrap();
        let e = open(home.path(), temp.path(), true);
        let mut wanted = expected();
        wanted.batch_digest = bigbrotr_model::Digest256::from_bytes([6; 32]);
        wanted.attempt_id = [10; 16];
        wanted.publication_ordinal = 2;
        assert_eq!(
            inspect_attached(&e.connection, &wanted, 16),
            Err(ReceiptError::Mismatch),
            "{mutation}"
        );
    }
}
