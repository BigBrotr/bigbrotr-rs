//! Privileged synthetic post-GC reader fixture on an excluded, retired clone.
//! This raw test helper is not a product reclamation writer or eligibility check.
use crate::{emit, hex, raw_state};
use sha2::{Digest, Sha256};
use std::path::Path;

const SLOT: &str = "36a0253e6d94c18099f6c4dae9b7e040";
const DATASET: &str = "309d9507c5b5c39e9947e1406482d519";
const MANIFEST: &str = "4d6754fa296d44b3b140da79b6829cf1b7f777ff109c30170db473ce124bb042";
const HEAD: &str = "a3948e3f2727a98d9b07bb0cd6815ca1004391143604c4a18e44683f57f00894";

fn bytes(value: &str) -> Vec<u8> {
    assert!(value.len().is_multiple_of(2));
    assert!(
        value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
    (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
        .collect()
}

pub fn mutate(path: &Path, mode: &str, literal_floor: &str) {
    assert!(matches!(mode, "install" | "missing-pending"));
    // Restrict this intentionally privileged helper to this fixture's isolated
    // /lab clone roots. PostgreSQL eligibility/exclusion is checked by its owner.
    assert_eq!(path.file_name().unwrap(), "store");
    assert_eq!(path.parent().unwrap().parent().unwrap(), Path::new("/lab"));
    let clone = path
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    assert!(matches!(
        clone,
        "positive" | "incompatible-head" | "floor-digest" | "missing-pending"
    ));
    if mode == "missing-pending" {
        assert_eq!(clone, "missing-pending");
    }
    let floor = bytes(literal_floor);
    assert_eq!(floor.len(), 139);
    assert_eq!(&floor[..2], &[3, 0x31]);
    for start in [2, 10, 18] {
        assert_eq!(&floor[start..start + 8], &7_u64.to_be_bytes());
    }
    assert_eq!(floor[26], 1);
    assert_eq!(hex(&floor[27..59]), MANIFEST);
    assert_eq!(&floor[59..67], &27_u64.to_be_bytes());
    assert!(floor[67..99].iter().any(|b| *b != 0));
    assert_eq!(&floor[99..107], &8_u64.to_be_bytes());
    assert_eq!(hex(&floor[107..]), HEAD);
    let (before, sequence) = raw_state(path);
    assert_eq!(
        (before.len(), sequence),
        if mode == "install" {
            (88, 113)
        } else {
            (81, 121)
        }
    );
    let db = rocksdb::DB::open_cf(
        &rocksdb::Options::default(),
        path,
        ["default", "facts", "receipts"],
    )
    .unwrap();
    let manifest = db
        .get_cf(db.cf_handle("default").unwrap(), [1, 1])
        .unwrap()
        .unwrap();
    assert_eq!(hex(&Sha256::digest(&manifest)), MANIFEST);
    let state_key = bytes(&format!("0131{SLOT}0000000000000001"));
    let receipts = db.cf_handle("receipts").unwrap();
    let state = db.get_cf(receipts, &state_key).unwrap().unwrap();
    let mut batch = rocksdb::WriteBatch::default();
    let mut removed = Vec::new();
    if mode == "install" {
        assert_eq!(
            hex(&state),
            "023100000000000000070000000000000007000000000000000001"
        );
        batch.put_cf(receipts, &state_key, &floor);
        for sequence in 1..=7_u64 {
            let key = bytes(&format!("0130{SLOT}0000000000000001{sequence:016x}"));
            let value = db.get_cf(receipts, &key).unwrap().unwrap();
            assert!(value.starts_with(b"bigbrotr/receipt/1\0"));
            removed.push(serde_json::json!({"cf":"receipts","key":hex(&key),"value":hex(&value)}));
            batch.delete_cf(receipts, key);
        }
    } else {
        assert_eq!(state, floor);
        // First pending batch covers source1..2. Its facts and every index stay.
        let key = bytes(&format!(
            "0121{DATASET}0000000000000000000000010000000000000001"
        ));
        let facts = db.cf_handle("facts").unwrap();
        let value = db.get_cf(facts, &key).unwrap().unwrap();
        assert_eq!(value.len(), 42);
        assert_eq!(&value[..10], &[1, 0x21, 0, 0, 0, 0, 0, 0, 0, 2]);
        removed.push(serde_json::json!({"cf":"facts","key":hex(&key),"value":hex(&value)}));
        batch.delete_cf(facts, key);
    }
    emit(
        serde_json::json!({"event":"synthetic-post-gc-reader-fixture","phase":"before",
        "mode":mode,"rows":before,"sequence":sequence,"floor":literal_floor,"removed":removed}),
    );
    let mut options = rocksdb::WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options).unwrap();
    drop(db);
    let (rows, sequence) = raw_state(path);
    emit(
        serde_json::json!({"event":"synthetic-post-gc-reader-fixture","phase":"after",
        "mode":mode,"rows":rows,"sequence":sequence,"native":"closed"}),
    );
}

/// Separate bounded later-history fixture. The caller supplies the independently
/// derived complete before-state; accepting a product dump as that expectation
/// is forbidden by the Python owner. No arbitrary key/value mutation is exposed.
pub fn mutate_history(path: &Path, mode: &str, literal_floor: &str, expected_file: &Path) {
    assert!(matches!(mode, "install" | "missing-later-pending"));
    assert_eq!(path.file_name().unwrap(), "store");
    assert_eq!(path.parent().unwrap().parent().unwrap(), Path::new("/lab"));
    let clone = path
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    assert!(matches!(
        clone,
        "history-terminal"
            | "history-positive"
            | "history-strict"
            | "history-missing-required"
            | "history-incompatible-head"
            | "history-corrupt-residual"
            | "history-missing-pending"
    ));
    assert_eq!(expected_file.parent().unwrap(), Path::new("/lab"));
    assert_eq!(
        expected_file.file_name().unwrap().to_str().unwrap(),
        format!("{clone}-{mode}-expected-native.json")
    );
    let metadata = std::fs::symlink_metadata(expected_file).unwrap();
    assert!(metadata.is_file() && metadata.len() <= 1024 * 1024);
    let expected: serde_json::Value =
        serde_json::from_slice(&std::fs::read(expected_file).unwrap()).unwrap();
    assert_eq!(expected.as_object().unwrap().len(), 3);
    assert_eq!(expected["event"], "dump-state");
    let floor = bytes(literal_floor);
    assert_eq!(floor.len(), 139);
    assert_eq!(
        hex(&floor),
        format!(
            "0331{0:016x}{0:016x}{0:016x}01{MANIFEST}{1:016x}c0414bfb32040948e843e06a05dd0dcab9fc136713bc302e047db1317c107954{2:016x}{HEAD}",
            7, 27, 8
        )
    );
    let (before, sequence) = raw_state(path);
    assert_eq!(
        serde_json::json!({"event":"dump-state","rows":before,"sequence":sequence}),
        expected
    );
    let terminal = clone == "history-terminal";
    assert_eq!(
        (before.len(), sequence),
        if mode == "install" {
            if terminal { (97, 126) } else { (100, 130) }
        } else {
            (93, 138)
        }
    );
    let manifest = before
        .iter()
        .find(|r| r["cf"] == "default" && r["key"] == "0101")
        .unwrap();
    let manifest = bytes(manifest["value"].as_str().unwrap());
    assert_eq!(
        &manifest[..10],
        &bytes(if terminal {
            "01010002000100010001"
        } else {
            "01010003000100010001"
        })
    );
    assert_eq!(&manifest[10..26], bytes(DATASET));
    assert_eq!(
        &manifest[54..62],
        &(if terminal { 2_u64 } else { 3_u64 }).to_be_bytes()
    );
    let db = rocksdb::DB::open_cf(
        &rocksdb::Options::default(),
        path,
        ["default", "facts", "receipts"],
    )
    .unwrap();
    assert_eq!(db.latest_sequence_number(), sequence);
    let receipts = db.cf_handle("receipts").unwrap();
    let state_key = bytes(&format!("0131{SLOT}0000000000000001"));
    let state = db.get_cf(receipts, &state_key).unwrap().unwrap();
    let mut batch = rocksdb::WriteBatch::default();
    let mut removed = Vec::new();
    if mode == "install" {
        assert_eq!(
            hex(&state),
            "023100000000000000070000000000000007000000000000000001"
        );
        batch.put_cf(receipts, &state_key, &floor);
        for sequence in 1..=7_u64 {
            let key = bytes(&format!("0130{SLOT}0000000000000001{sequence:016x}"));
            let value = db.get_cf(receipts, &key).unwrap().unwrap();
            assert!(value.starts_with(b"bigbrotr/receipt/1\0"));
            removed.push(serde_json::json!({"cf":"receipts","key":hex(&key),"value":hex(&value)}));
            batch.delete_cf(receipts, key);
        }
    } else {
        assert_eq!(clone, "history-missing-pending");
        assert_eq!(state, floor);
        let key = bytes(&format!(
            "0121{DATASET}0000000000000000000000020000000000000001"
        ));
        let facts = db.cf_handle("facts").unwrap();
        let value = db.get_cf(facts, &key).unwrap().unwrap();
        assert_eq!(value.len(), 42);
        assert_eq!(&value[..10], &[1, 0x21, 0, 0, 0, 0, 0, 0, 0, 1]);
        removed.push(serde_json::json!({"cf":"facts","key":hex(&key),"value":hex(&value)}));
        batch.delete_cf(facts, key);
    }
    emit(
        serde_json::json!({"event":"synthetic-post-gc-history-fixture","phase":"before",
        "mode":mode,"rows":before,"sequence":sequence,"floor":literal_floor,"removed":removed}),
    );
    let mut options = rocksdb::WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options).unwrap();
    drop(db);
    let (rows, sequence) = raw_state(path);
    emit(
        serde_json::json!({"event":"synthetic-post-gc-history-fixture","phase":"after",
        "mode":mode,"rows":rows,"sequence":sequence,"native":"closed"}),
    );
}
