//! Application handling of simulated sticky observations. These tests do not
//! invoke the native listener or qualify POSIX faults, FFI races, or recovery.
use super::bootstrap_tests::{budget, grant, intent, manifest, native, request};
use super::*;
use bigbrotr_model::ObservedTime;

const MIB: u64 = 1024 * 1024;

fn limits() -> BootstrapLimits {
    BootstrapLimits {
        max_mappings: 8,
        max_batch_bytes: MIB as usize,
    }
}

fn rows(store: &IngestStore) -> Vec<(&'static str, Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    for name in INGEST_COLUMN_FAMILIES {
        for row in store.db.iterator_cf(
            store.db.cf_handle(name).unwrap(),
            rocksdb::IteratorMode::Start,
        ) {
            let (key, value) = row.unwrap();
            rows.push((name, key.into_vec(), value.into_vec()));
        }
    }
    rows
}

#[test]
fn simulated_background_latch_and_foreground_unknown_independently_refuse_bootstrap() {
    for background in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let b = budget();
        let mut store =
            IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
        let before = rows(&store);
        let sequence = store.db.latest_sequence_number();
        let accounting = store.accounting;
        assert!(!store.native_unavailable());
        if background {
            store.test_latch_background_failure();
            assert!(!store.write_uncertain);
        } else {
            store.write_uncertain = true; // Explicit foreground unknown premise.
        }
        assert!(store.native_unavailable());
        assert!(matches!(
            store.stage_initial_dictionaries(
                request(),
                &intent("public"),
                limits(),
                &grant(&b, 8 * MIB, 4 * MIB),
            ),
            Err(BootstrapError::CommitUnknown)
        ));
        assert!(matches!(
            store.bootstrap_dictionaries(
                request(),
                &intent("public"),
                ObservedTime::new(123),
                limits(),
                grant(&b, 8 * MIB, 4 * MIB),
                || Ok(()),
            ),
            Err(BootstrapError::CommitUnknown)
        ));
        assert!(!path.join("producer-inputs-v1").exists());
        assert_eq!(store.db.latest_sequence_number(), sequence);
        assert_eq!(store.accounting, accounting);
        assert_eq!(rows(&store), before);
        assert_eq!(b.snapshot().used().data_bytes(), 0);
    }
}

#[test]
fn simulated_latch_at_final_dispatch_check_retains_original_without_native_commit() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let before = rows(&store);
    let sequence = store.db.latest_sequence_number();
    let accounting = store.accounting;
    let input = intent("public");
    let original = input
        .encode_request_preimage(request(), limits().max_batch_bytes)
        .unwrap();
    let simulated = store.test_background_failure();
    let mut checks = 0;
    assert!(matches!(
        store.bootstrap_dictionaries(
            request(),
            &input,
            ObservedTime::new(123),
            limits(),
            grant(&b, 8 * MIB, 4 * MIB),
            || {
                checks += 1;
                if checks == 2 {
                    simulated.latch();
                }
                Ok(())
            },
        ),
        Err(BootstrapError::CommitUnknown)
    ));
    assert_eq!(checks, 2);
    assert!(store.native_unavailable());
    assert!(!store.write_uncertain, "no native dispatch occurred");
    assert_eq!(store.db.latest_sequence_number(), sequence);
    assert_eq!(store.accounting, accounting);
    assert_eq!(rows(&store), before);
    let files: Vec<_> = std::fs::read_dir(path.join("producer-inputs-v1"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(files.len(), 1);
    assert_eq!(std::fs::read(&files[0]).unwrap(), original);
    assert_eq!(b.snapshot().used().data_bytes(), 0);
}

#[test]
fn simulated_latch_during_exact_replay_refuses_success_and_retains_native_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let input = intent("public");
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &input,
                ObservedTime::new(123),
                limits(),
                grant(&b, 8 * MIB, 4 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let before = rows(&store);
    let sequence = store.db.latest_sequence_number();
    let accounting = store.accounting;
    let simulated = store.test_background_failure();
    let mut checks = 0;
    assert!(matches!(
        store.bootstrap_dictionaries(
            request(),
            &input,
            ObservedTime::new(456),
            limits(),
            grant(&b, 8 * MIB, 4 * MIB),
            || {
                checks += 1;
                if checks == 2 {
                    simulated.latch();
                }
                Ok(())
            },
        ),
        Err(BootstrapError::CommitUnknown)
    ));
    assert_eq!(checks, 2);
    assert_eq!(store.db.latest_sequence_number(), sequence);
    assert_eq!(store.accounting, accounting);
    assert_eq!(rows(&store), before);
    assert_eq!(b.snapshot().used().data_bytes(), 0);
}

#[test]
fn simulated_latch_retains_native_reservation_and_path_until_explicit_close() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    store.test_latch_background_failure();
    let other = budget();
    assert!(matches!(
        IngestStore::reopen(&path, manifest(), native(), grant(&other, 0, 128 * MIB)),
        Err(StoreError::AlreadyOpen)
    ));
    assert_eq!(b.snapshot().used().native_bytes(), 128 * MIB);
    assert!(path.join("CURRENT").exists());
    assert!(store.native_unavailable());
    drop(store);
    assert_eq!(b.snapshot().used().native_bytes(), 0);
    assert_eq!(other.snapshot().used().native_bytes(), 0);
    assert!(path.join("CURRENT").exists());
}
