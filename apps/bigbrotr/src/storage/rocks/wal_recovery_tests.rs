//! Detected WAL damage refuses ordinary open; an intact lost boundary is distinct.
use super::bootstrap_tests::{budget, grant, intent, manifest, native, request};
use super::*;
use bigbrotr_model::ObservedTime;
use std::{
    io::{BufRead, Write},
    process::{Command, Stdio},
    sync::mpsc,
    time::Duration,
};

const MIB: u64 = 1024 * 1024;
const TEST: &str =
    "storage::rocks::store::wal_recovery_tests::wal_recovery_refuses_loss_of_a_synced_bootstrap";
const CHILD: &str = "BIGBROTR_WAL_TEST_CHILD";

fn seed(path: &Path) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture"])
        .env(CHILD, path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines().take(16) {
            if line.unwrap() == "SYNCED-BOOTSTRAP" {
                let _ = send.send(());
                return;
            }
        }
    });
    let reached = receive.recv_timeout(Duration::from_secs(15));
    let killed = child.kill();
    let status = child.wait().unwrap();
    reader.join().unwrap();
    assert!(reached.is_ok(), "child did not synchronize bootstrap");
    killed.unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(status.signal(), Some(9));
}

fn damage_last_record(path: &Path, mode: &str) -> (PathBuf, Vec<u8>) {
    let logs: Vec<_> = std::fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect();
    assert_eq!(logs.len(), 1, "fixture must have one unflushed live WAL");
    let log = &logs[0];
    let original = std::fs::read(log).unwrap();
    assert!(original.len() < 32768, "fixture is one physical log block");
    // The pinned engine's ordinary (nonrecycled, uncompressed) physical record:
    // CRC32C[4], little-endian payload length[2], type[1], payload. Derive no
    // expected state from replay: select the last FULL record with our receipt.
    let mut offset = 0;
    let mut last = None;
    while offset + 7 <= original.len() {
        let length =
            u16::from_le_bytes(original[offset + 4..offset + 6].try_into().unwrap()) as usize;
        let kind = original[offset + 6];
        assert_eq!(kind, 1, "unexpected WAL record type");
        let end = offset + 7 + length;
        assert!(end <= original.len());
        last = Some((offset, end));
        offset = end;
    }
    assert_eq!(offset, original.len());
    let (start, end) = last.unwrap();
    assert!(start > 0, "manifest must precede bootstrap");
    let receipt_key = [
        b"\x01\x30".as_slice(),
        &[5; 16],
        &1u64.to_be_bytes(),
        &1u64.to_be_bytes(),
    ]
    .concat();
    assert!(
        original[start + 7..end]
            .windows(receipt_key.len())
            .any(|w| w == receipt_key)
    );
    let mut damaged = original.clone();
    match mode {
        "truncated" => damaged.truncate(end - 1),
        "boundary" => damaged.truncate(start),
        "checksum" => damaged[start] ^= 1,
        _ => panic!("unknown fixture damage"),
    }
    std::fs::write(log, &damaged).unwrap();
    std::fs::File::open(log).unwrap().sync_all().unwrap();
    use sha2::{Digest, Sha256};
    println!(
        "{}",
        serde_json::json!({"event":"damaged-wal", "mode":mode,
        "file":log.file_name().unwrap().to_str().unwrap(), "record_offset":start,
        "original_bytes":original.len(), "damaged_bytes":damaged.len(),
        "original_sha256":hex(&Sha256::digest(&original)),
        "damaged_sha256":hex(&Sha256::digest(&damaged))})
    );
    (log.clone(), damaged)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}

fn rows(db: &DB) -> serde_json::Value {
    let mut rows = Vec::new();
    for family in ["default", "facts", "receipts"] {
        for row in db.iterator_cf(db.cf_handle(family).unwrap(), rocksdb::IteratorMode::Start) {
            let (key, value) = row.unwrap();
            rows.push(serde_json::json!([family, hex(&key), hex(&value)]));
        }
    }
    serde_json::Value::Array(rows)
}

#[test]
fn wal_recovery_refuses_loss_of_a_synced_bootstrap() {
    if let Some(path) = std::env::var_os(CHILD) {
        let b = budget();
        let mut store = IngestStore::initialize(
            Path::new(&path),
            manifest(),
            native(),
            grant(&b, 0, 128 * MIB),
        )
        .unwrap();
        drop(
            store
                .bootstrap_dictionaries(
                    request(),
                    &intent("public"),
                    ObservedTime::new(123),
                    BootstrapLimits {
                        max_mappings: 8,
                        max_batch_bytes: MIB as usize,
                    },
                    grant(&b, 8 * MIB, 4 * MIB),
                    || Ok(()),
                )
                .unwrap(),
        );
        println!("SYNCED-BOOTSTRAP");
        std::io::stdout().flush().unwrap();
        std::thread::sleep(Duration::from_secs(30));
        panic!("parent failed to kill native owner");
    }
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/staged-bootstrap-crash.json"
    ))
    .unwrap();
    for mode in ["intact", "checksum", "truncated"] {
        if mode != "intact" {
            let reference = root.path().join(format!("{mode}-permissive"));
            seed(&reference);
            damage_last_record(&reference, mode);
            let mut profile = RocksNativeProfile::build(native());
            profile
                .db_options
                .set_wal_recovery_mode(rocksdb::DBRecoveryMode::PointInTime);
            let db =
                DB::open_cf_descriptors(&profile.db_options, &reference, profile.column_families)
                    .unwrap();
            assert_eq!(
                rows(&db),
                fixture["before"],
                "negative control must lose the whole synchronized bootstrap"
            );
            println!("{mode}: PointInTime reopened and lost all bootstrap effects");
        }
        let path = root.path().join(mode);
        seed(&path);
        let damaged = (mode != "intact").then(|| damage_last_record(&path, mode));
        let result = IngestRecovery::open(&path, manifest(), native(), grant(&b, 0, 128 * MIB));
        if mode != "intact" {
            let observation = match &result {
                Err(error) => format!("{error:?}"),
                Ok(recovery) => format!(
                    "opened with sequence {}",
                    recovery.store.db.latest_sequence_number()
                ),
            };
            println!("{mode}: {observation}");
            assert!(
                matches!(result, Err(StoreError::EngineOpen)),
                "{mode}: synchronized effects disappeared behind successful recovery: {observation}"
            );
            let (log, bytes) = damaged.unwrap();
            assert_eq!(std::fs::read(&log).unwrap(), bytes);
            assert!(matches!(
                IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)),
                Err(StoreError::EngineOpen)
            ));
            assert_eq!(
                std::fs::read(&log).unwrap(),
                bytes,
                "failed retry must preserve suspect WAL"
            );
        } else {
            let store = result.unwrap().store;
            assert_eq!(rows(&store.db), fixture["after"]);
        }
    }
}

#[test]
fn strict_wal_open_is_not_a_proof_against_complete_record_loss() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("missing-last-record");
    seed(&path);
    damage_last_record(&path, "boundary");
    let b = budget();
    let recovered =
        IngestRecovery::open(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/staged-bootstrap-crash.json"
    ))
    .unwrap();
    assert_eq!(rows(&recovered.store.db), fixture["before"]);
    // AbsoluteConsistency sees a complete shorter WAL, not a damaged record.
    // This executed counterexample prevents using open success as noncommit.
    println!(
        "record-boundary truncation: strict open succeeds with only the manifest; no noncommit inference is valid"
    );
}
