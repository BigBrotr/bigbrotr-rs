//! Real shared-spool scans under an actual exclusive native owner. Literal
//! payloads exercise census accounting/stamps, not canonical request replay or GC.
use super::{MIB, budget, grant, manifest, native};
use crate::storage::rocks::store::{
    BootstrapError, IngestStore, RequestInputError,
    request_inputs::{LineageInputSet, SuccessorStagedInput},
};
use bigbrotr_model::{
    Digest256, ProducerEpoch, ProducerRequestId, ProducerSlotId, RequestSequence,
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

const OLD1: &str = "05050505050505050505050505050505-0000000000000001-0000000000000001.cbor";
const OLD2: &str = "05050505050505050505050505050505-0000000000000001-0000000000000002.cbor";
const OLD3: &str = "05050505050505050505050505050505-0000000000000001-0000000000000003.cbor";
const NEW1: &str = "05050505050505050505050505050505-0000000000000002-0000000000000001.cbor";
const NEW2: &str = "05050505050505050505050505050505-0000000000000002-0000000000000002.cbor";
const TAIL: &str = "05050505050505050505050505050505-0000000000000002-0000000000000003.cbor";
const FULL: &[(&str, &[u8])] = &[
    (OLD1, b"old-a"),
    (OLD2, b"old-b"),
    (OLD3, b"old-c"),
    (NEW1, b"next-a"),
    (NEW2, b"next-b"),
];

fn fixture() -> (tempfile::TempDir, IngestStore) {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    (root, store)
}

fn slot() -> ProducerSlotId {
    [5; 16].try_into().unwrap()
}
fn request(epoch: u64, sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: slot(),
        producer_epoch: ProducerEpoch::new(epoch).unwrap(),
        request_sequence: RequestSequence::new(sequence).unwrap(),
    }
}
fn mixed(required: u64) -> LineageInputSet {
    LineageInputSet::with_compacted_initial(required, slot(), 3).unwrap()
}
fn required(id: ProducerRequestId) -> bool {
    [request(2, 1), request(2, 2)].contains(&id)
}
fn path(root: &Path, name: &str) -> PathBuf {
    root.join("db/producer-inputs-v1").join(name)
}
fn spool(root: &Path) {
    let directory = root.join("db/producer-inputs-v1");
    fs::create_dir_all(&directory).unwrap();
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
}
fn put(root: &Path, name: &str, bytes: &[u8]) {
    spool(root);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path(root, name))
        .unwrap();
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .unwrap();
    file.write_all(bytes).unwrap();
}
fn quarantine(root: &Path, name: &str) {
    let directory = root.join("quarantined");
    fs::create_dir_all(&directory).unwrap();
    fs::rename(path(root, name), directory.join(name)).unwrap();
}

// Explicit ordered membership and literal content come from each fixture, never
// a directory enumeration, scanner classifier or its name/identity helpers.
// Only host-assigned inode/time fields are observed to construct the exact stamp.
fn expected_stamp(root: &Path, files: &[(&str, &[u8])]) -> [u8; 32] {
    assert!(files.windows(2).all(|pair| pair[0].0 < pair[1].0));
    let mut expected = Sha256::new();
    expected.update(b"bigbrotr/local-lineage-input-census/1\0");
    expected.update((files.len() as u64).to_be_bytes());
    expected.update(
        files
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>()
            .to_be_bytes(),
    );
    for (name, bytes) in files {
        assert_eq!(name.len(), 71);
        let metadata = fs::metadata(path(root, name)).unwrap();
        assert_eq!(metadata.len(), bytes.len() as u64);
        let mut entry = Sha256::new();
        entry.update(Sha256::digest(bytes));
        for field in [
            metadata.dev(),
            metadata.ino(),
            bytes.len() as u64,
            metadata.mtime() as u64,
            metadata.mtime_nsec() as u64,
            metadata.ctime() as u64,
            metadata.ctime_nsec() as u64,
        ] {
            entry.update(field.to_be_bytes());
        }
        expected.update(name.as_bytes());
        expected.update(entry.finalize());
    }
    expected.finalize().into()
}

#[test]
fn compacted_input_set_validates_floor_and_required_bounds_separately() {
    assert!(matches!(
        LineageInputSet::with_compacted_initial(0, slot(), 0),
        Err(BootstrapError::InvalidIntent)
    ));
    for (required, floor) in [(0, 65), (65, 1), (u64::MAX, 1), (1, u64::MAX)] {
        assert!(matches!(
            LineageInputSet::with_compacted_initial(required, slot(), floor),
            Err(BootstrapError::Limit)
        ));
    }
    assert!(LineageInputSet::with_compacted_initial(64, slot(), 64).is_ok());
}

#[test]
fn mixed_inputs_accept_full_partial_and_no_residuals_with_exact_global_stamps() {
    let (root, store) = fixture();
    let root = root.path();
    let cancelled = AtomicBool::new(false);
    let sequence = store.db.latest_sequence_number();
    for (name, bytes) in FULL {
        put(root, name, bytes);
    }
    let full = store
        .lineage_input_census(mixed(2), required, &cancelled)
        .unwrap();
    assert_eq!(full, expected_stamp(root, FULL)); // Five files, 27 literal bytes.
    assert_eq!(
        store
            .lineage_input_census(
                5_u64,
                |id| required(id)
                    || (id.producer_slot_id == slot()
                        && id.producer_epoch.get() == 1
                        && id.request_sequence.get() <= 3),
                &cancelled
            )
            .unwrap(),
        full
    );
    assert!(matches!(
        store.lineage_input_census(2_u64, required, &cancelled),
        Err(BootstrapError::RequestInput(RequestInputError::Identity))
    ));
    quarantine(root, OLD1);
    quarantine(root, OLD3);
    let partial = store
        .lineage_input_census(mixed(2), required, &cancelled)
        .unwrap();
    assert_eq!(
        partial,
        expected_stamp(
            root,
            &[(OLD2, b"old-b"), (NEW1, b"next-a"), (NEW2, b"next-b")]
        )
    );
    assert_ne!(partial, full); // Three files, 17 bytes; residual changes are visible.
    quarantine(root, OLD2);
    let none = store
        .lineage_input_census(mixed(2), required, &cancelled)
        .unwrap();
    assert_eq!(
        none,
        expected_stamp(root, &[(NEW1, b"next-a"), (NEW2, b"next-b")])
    );
    assert_ne!(none, partial); // Two required files, 12 bytes.
    assert!(matches!(
        store.lineage_input_census(5_u64, |_| true, &cancelled),
        Err(BootstrapError::RequestInput(RequestInputError::Missing))
    ));
    assert_eq!(store.db.latest_sequence_number(), sequence);
}

#[test]
fn one_valid_residual_cannot_mask_one_missing_required_original() {
    let (root, store) = fixture();
    put(root.path(), OLD1, b"old-a");
    put(root.path(), NEW1, b"next-a");
    // Exactly two physical files, but NEW2 is missing. Even a predicate admitting
    // both epochs cannot turn OLD1 into the second required original.
    assert!(matches!(
        store.lineage_input_census(
            mixed(2),
            |id| required(id) || id == request(1, 1),
            &AtomicBool::new(false)
        ),
        Err(BootstrapError::RequestInput(RequestInputError::Missing))
    ));
    assert_eq!(fs::read(path(root.path(), OLD1)).unwrap(), b"old-a");
    assert_eq!(fs::read(path(root.path(), NEW1)).unwrap(), b"next-a");
}

#[test]
fn compacted_wrappers_check_present_residuals_and_preserve_missing_directory_refusal() {
    let (root, store) = fixture();
    let root = root.path();
    let cancelled = AtomicBool::new(false);
    let empty = expected_stamp(root, &[]);
    assert_eq!(
        store
            .lineage_input_census(0_u64, |_| false, &cancelled)
            .unwrap(),
        empty
    );
    assert!(matches!(
        store.compacted_input_census(slot(), 3, &cancelled),
        Err(BootstrapError::RequestInput(RequestInputError::Missing))
    ));
    assert!(!path(root, OLD1).parent().unwrap().exists());
    spool(root);
    assert_eq!(
        store.compacted_input_census(slot(), 3, &cancelled).unwrap(),
        empty
    );
    let digest: [u8; 32] = Sha256::digest(b"old-a").into();
    assert_eq!(
        store
            .require_compacted_input(3, request(1, 1), digest, 5, &cancelled)
            .unwrap(),
        empty
    );
    for (name, bytes) in &FULL[..3] {
        put(root, name, bytes);
    }
    let full = expected_stamp(root, &FULL[..3]);
    assert_eq!(
        store.compacted_input_census(slot(), 3, &cancelled).unwrap(),
        full
    );
    assert_eq!(
        store
            .require_compacted_input(3, request(1, 1), digest, 5, &cancelled)
            .unwrap(),
        full
    );
    for (digest, length) in [([0; 32], 5), (digest, 6)] {
        assert!(matches!(
            store.require_compacted_input(3, request(1, 1), digest, length, &cancelled),
            Err(BootstrapError::RequestInput(RequestInputError::Conflict))
        ));
    }
    for id in [request(2, 1), request(1, 4)] {
        assert!(matches!(
            store.require_compacted_input(3, id, digest, 5, &cancelled),
            Err(BootstrapError::InvalidIntent)
        ));
    }
    quarantine(root, OLD1);
    quarantine(root, OLD3);
    assert_eq!(
        store.compacted_input_census(slot(), 3, &cancelled).unwrap(),
        expected_stamp(root, &[(OLD2, b"old-b")])
    );
    assert_eq!(
        store
            .require_compacted_input(3, request(1, 1), digest, 5, &cancelled)
            .unwrap(),
        expected_stamp(root, &[(OLD2, b"old-b")])
    );
}

#[test]
fn mixed_inputs_refuse_wrong_slot_epoch_sequence_and_noncanonical_names() {
    let (root, store) = fixture();
    let root = root.path();
    put(root, NEW1, b"next-a");
    put(root, NEW2, b"next-b");
    for name in [
        "06060606060606060606060606060606-0000000000000001-0000000000000001.cbor",
        "05050505050505050505050505050505-0000000000000003-0000000000000001.cbor",
        "05050505050505050505050505050505-0000000000000001-0000000000000004.cbor",
        TAIL,
        "05050505050505050505050505050505-0000000000000000-0000000000000001.cbor",
        "05050505050505050505050505050505-0000000000000001-0000000000000000.cbor",
        "05050505050505050505050505050505-0000000000000001-000000000000000A.cbor",
        "05050505050505050505050505050505-0000000000000001-0000000000000001.cbor.tmp",
    ] {
        put(root, name, b"unexpected");
        assert!(
            matches!(
                store.lineage_input_census(mixed(2), required, &AtomicBool::new(false)),
                Err(BootstrapError::RequestInput(RequestInputError::Identity))
            ),
            "accepted {name}"
        );
        quarantine(root, name);
    }
}

#[test]
fn mixed_residual_check_binds_content_and_the_complete_required_spool() {
    let (root, store) = fixture();
    let root = root.path();
    let cancelled = AtomicBool::new(false);
    put(root, OLD1, b"old-a");
    put(root, NEW1, b"next-a");
    put(root, NEW2, b"next-b");
    let checked = (request(1, 1), Sha256::digest(b"old-a").into(), 5);
    let expected = expected_stamp(
        root,
        &[(OLD1, b"old-a"), (NEW1, b"next-a"), (NEW2, b"next-b")],
    );
    assert_eq!(
        store
            .lineage_input_census(mixed(2), required, &cancelled)
            .unwrap(),
        expected
    );
    assert_eq!(
        store
            .require_compacted_lineage_input(mixed(2), required, checked, &cancelled)
            .unwrap(),
        expected
    );
    for invalid in [(checked.0, [0; 32], 5), (checked.0, checked.1, 6)] {
        assert!(matches!(
            store.require_compacted_lineage_input(mixed(2), required, invalid, &cancelled),
            Err(BootstrapError::RequestInput(RequestInputError::Conflict))
        ));
    }
    quarantine(root, OLD1);
    let absent = store
        .require_compacted_lineage_input(mixed(2), required, checked, &cancelled)
        .unwrap();
    assert_eq!(
        absent,
        expected_stamp(root, &[(NEW1, b"next-a"), (NEW2, b"next-b")])
    );
    assert_ne!(absent, expected);
    assert_eq!(
        absent,
        store
            .lineage_input_census(mixed(2), required, &cancelled)
            .unwrap()
    );
    quarantine(root, NEW2);
    assert!(matches!(
        store.require_compacted_lineage_input(mixed(2), required, checked, &cancelled),
        Err(BootstrapError::RequestInput(RequestInputError::Missing))
    ));
}

#[test]
fn staged_addition_excludes_only_exact_new_required_file() {
    let (root, store) = fixture();
    let root = root.path();
    let cancelled = AtomicBool::new(false);
    put(root, OLD2, b"old-b");
    put(root, NEW1, b"next-a");
    let before = expected_stamp(root, &[(OLD2, b"old-b"), (NEW1, b"next-a")]);
    assert_eq!(
        store
            .lineage_input_census(mixed(1), |id| id == request(2, 1), &cancelled)
            .unwrap(),
        before
    );
    let added = (request(2, 2), Sha256::digest(b"next-b").into(), 6);
    assert!(matches!(
        store.lineage_input_delta(mixed(1), required, Some(added), &cancelled),
        Err(BootstrapError::RequestInput(RequestInputError::Missing))
    ));
    put(root, NEW2, b"next-b");
    let (all, previous) = store
        .lineage_input_delta(mixed(2), required, Some(added), &cancelled)
        .unwrap();
    assert_eq!(previous, before);
    assert_eq!(
        all,
        expected_stamp(
            root,
            &[(OLD2, b"old-b"), (NEW1, b"next-a"), (NEW2, b"next-b")]
        )
    );
    for addition in [(request(2, 2), [0; 32], 6), (request(2, 2), added.1, 7)] {
        assert!(matches!(
            store.lineage_input_delta(mixed(2), required, Some(addition), &cancelled),
            Err(BootstrapError::RequestInput(RequestInputError::Conflict))
        ));
    }
    for id in [request(1, 2), request(2, 3)] {
        assert!(matches!(
            store.lineage_input_delta(mixed(2), required, Some((id, added.1, 6)), &cancelled),
            Err(BootstrapError::InvalidIntent)
        ));
    }
    fs::write(path(root, OLD2), b"OLD-b").unwrap();
    let (changed, changed_previous) = store
        .lineage_input_delta(mixed(2), required, Some(added), &cancelled)
        .unwrap();
    assert_ne!(changed, all);
    assert_ne!(changed_previous, before);
    assert_eq!(
        changed_previous,
        expected_stamp(root, &[(OLD2, b"OLD-b"), (NEW1, b"next-a")])
    );
}

#[test]
fn recovery_tail_is_stamped_separately_and_never_supplies_required_credit() {
    let (root, store) = fixture();
    let root = root.path();
    let cancelled = AtomicBool::new(false);
    put(root, OLD2, b"old-b");
    put(root, NEW1, b"next-a");
    put(root, NEW2, b"next-b");
    let tail = request(2, 3);
    let (without, observed) = store
        .successor_recovery_inputs(mixed(2), required, tail, &cancelled)
        .unwrap();
    assert_eq!(observed, None);
    assert_eq!(
        without,
        expected_stamp(
            root,
            &[(OLD2, b"old-b"), (NEW1, b"next-a"), (NEW2, b"next-b")]
        )
    );
    put(root, TAIL, b"staged");
    let (with, observed) = store
        .successor_recovery_inputs(mixed(2), required, tail, &cancelled)
        .unwrap();
    assert_eq!(
        observed,
        Some(SuccessorStagedInput {
            request: tail,
            digest: Digest256::from_bytes(Sha256::digest(b"staged").into()),
            input_bytes: 6
        })
    );
    assert_eq!(
        with,
        expected_stamp(
            root,
            &[
                (OLD2, b"old-b"),
                (NEW1, b"next-a"),
                (NEW2, b"next-b"),
                (TAIL, b"staged")
            ]
        )
    );
    assert_ne!(with, without);
    for overlap in [request(1, 1), request(2, 1)] {
        assert!(matches!(
            store.successor_recovery_inputs(mixed(2), required, overlap, &cancelled),
            Err(BootstrapError::InvalidIntent)
        ));
    }
    quarantine(root, NEW2);
    assert!(matches!(
        store.successor_recovery_inputs(mixed(2), required, tail, &cancelled),
        Err(BootstrapError::RequestInput(RequestInputError::Missing))
    ));
}

#[test]
fn all_present_classes_share_the_physical_file_limit() {
    let (root, store) = fixture();
    let root = root.path();
    let cancelled = AtomicBool::new(false);
    let names: Vec<_> = (1..=64)
        .map(|n| format!("05050505050505050505050505050505-0000000000000002-{n:016x}.cbor"))
        .collect();
    for name in &names {
        put(root, name, b"q");
    }
    let inputs = LineageInputSet::with_compacted_initial(64, slot(), 7).unwrap();
    let accepts = |id: ProducerRequestId| {
        id.producer_slot_id == slot()
            && id.producer_epoch.get() == 2
            && id.request_sequence.get() <= 64
    };
    let all = store
        .lineage_input_census(inputs, accepts, &cancelled)
        .unwrap();
    let expected: Vec<_> = names
        .iter()
        .map(|name| (name.as_str(), b"q".as_slice()))
        .collect();
    assert_eq!(all, expected_stamp(root, &expected)); // 64 actual files/bytes, despite floor7.
    put(root, OLD1, b"r");
    assert!(matches!(
        store.lineage_input_census(inputs, accepts, &cancelled),
        Err(BootstrapError::Limit)
    )); // 65 actual files.
    quarantine(root, &names[63]);
    let inputs = LineageInputSet::with_compacted_initial(63, slot(), 7).unwrap();
    assert!(
        store
            .lineage_input_census(inputs, accepts, &cancelled)
            .is_ok()
    ); // 63 required + one residual.
    let tail_name = "05050505050505050505050505050505-0000000000000002-0000000000000041.cbor";
    put(root, tail_name, b"t");
    assert!(matches!(
        store.successor_recovery_inputs(inputs, accepts, request(2, 65), &cancelled),
        Err(BootstrapError::Limit)
    ));
}

#[test]
fn required_residual_and_tail_bytes_share_the_existing_logical_byte_limit() {
    let (root, store) = fixture();
    let root = root.path();
    let cancelled = AtomicBool::new(false);
    // Two sparse zero files: exactly 16MiB each, 32MiB total. No input payload
    // allocation proportional to their size is needed by this fixture or scanner.
    for name in [OLD1, NEW1] {
        put(root, name, b"\0");
        OpenOptions::new()
            .write(true)
            .open(path(root, name))
            .unwrap()
            .set_len(16 * MIB)
            .unwrap();
    }
    let accepts = |id| id == request(2, 1);
    assert!(
        store
            .lineage_input_census(mixed(1), accepts, &cancelled)
            .is_ok()
    );
    put(root, TAIL, b"t");
    assert!(matches!(
        store.successor_recovery_inputs(mixed(1), accepts, request(2, 3), &cancelled),
        Err(BootstrapError::Limit)
    )); // 32MiB + one byte.
    quarantine(root, TAIL);
    put(root, NEW2, b"q");
    assert!(matches!(
        store.lineage_input_census(mixed(2), required, &cancelled),
        Err(BootstrapError::Limit)
    ));
    quarantine(root, NEW2);
    OpenOptions::new()
        .write(true)
        .open(path(root, OLD1))
        .unwrap()
        .set_len(16 * MIB + 1)
        .unwrap();
    assert!(matches!(
        store.lineage_input_census(mixed(1), accepts, &cancelled),
        Err(BootstrapError::RequestInput(RequestInputError::Limit))
    ));
}

#[test]
fn residual_files_keep_private_link_and_complete_content_checks() {
    let (root, store) = fixture();
    let root = root.path();
    let cancelled = AtomicBool::new(false);
    put(root, OLD1, b"old-a");
    put(root, NEW1, b"next-a");
    let scan = || store.lineage_input_census(mixed(1), |id| id == request(2, 1), &cancelled);
    let old = path(root, OLD1);
    fs::set_permissions(&old, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(matches!(
        scan(),
        Err(BootstrapError::RequestInput(RequestInputError::Identity))
    ));
    fs::set_permissions(&old, fs::Permissions::from_mode(0o600)).unwrap();
    let link = root.join("residual-link");
    fs::hard_link(&old, &link).unwrap();
    assert!(matches!(
        scan(),
        Err(BootstrapError::RequestInput(RequestInputError::Identity))
    ));
    fs::remove_file(link).unwrap();
    let original = scan().unwrap();
    quarantine(root, OLD1);
    std::os::unix::fs::symlink(root.join("quarantined").join(OLD1), &old).unwrap();
    assert!(matches!(
        scan(),
        Err(BootstrapError::RequestInput(RequestInputError::Filesystem))
    ));
    fs::remove_file(&old).unwrap();
    put(root, OLD1, b"old-a");
    let replaced = scan().unwrap();
    assert_ne!(replaced, original); // Identical bytes in another inode are not a stable stamp.
    assert_eq!(
        replaced,
        expected_stamp(root, &[(OLD1, b"old-a"), (NEW1, b"next-a")])
    );
    fs::write(&old, b"").unwrap();
    assert!(matches!(
        scan(),
        Err(BootstrapError::RequestInput(RequestInputError::Incomplete))
    ));
}

#[test]
fn mixed_input_scan_observes_cancellation_before_and_during_enumeration() {
    let (root, store) = fixture();
    let cancelled = AtomicBool::new(true);
    assert!(matches!(
        store.lineage_input_census(mixed(1), required, &cancelled),
        Err(BootstrapError::AuthorityUnavailable)
    ));
    put(root.path(), OLD1, b"old-a");
    put(root.path(), NEW1, b"next-a");
    cancelled.store(false, Ordering::Relaxed);
    assert!(matches!(
        store.lineage_input_census(
            mixed(1),
            |id| {
                cancelled.store(true, Ordering::Relaxed);
                id == request(2, 1)
            },
            &cancelled
        ),
        Err(BootstrapError::AuthorityUnavailable)
    ));
    assert_eq!(fs::read(path(root.path(), OLD1)).unwrap(), b"old-a");
    assert_eq!(fs::read(path(root.path(), NEW1)).unwrap(), b"next-a");
}
