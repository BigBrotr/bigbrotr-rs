use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::rocks::{
        NATIVE_OPTION_PROFILE_VERSION, RocksNativeLimits, RocksNativeSettings,
        manifest::{AllocationNamespaces, IngestManifest, ManifestError, ManifestIdentity},
        store::{BootstrapError, IngestRecovery, IngestStore, InitialWriterCandidate, StoreError},
    },
};
use bigbrotr_model::{
    AllocationDomainId, DictionaryKind, OwnerEpoch, PolicyEpoch, RecoveryGenerationId,
    TopologyGeneration,
};
use rocksdb::{ColumnFamilyDescriptor, DB, Options, WriteOptions};
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const MIB: u64 = 1024 * 1024;
const ALLOWANCE: u64 = 128 * MIB;

fn limits() -> RocksNativeLimits {
    RocksNativeLimits::new(RocksNativeSettings {
        block_cache_bytes: 32 * MIB,
        write_buffer_manager_bytes: 16 * MIB,
        per_cf_write_buffer_bytes: 2 * MIB,
        max_total_wal_bytes: 16 * MIB,
        max_open_files: 64,
        max_background_jobs: 2,
        native_allowance_bytes: ALLOWANCE,
    })
    .unwrap()
}
fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            4 * ALLOWANCE + 1,
            0,
            ResourceLimits::new(0, 1, 4 * ALLOWANCE, 1),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    )
}
fn permit(budget: &ResourceBudget) -> OwnedPermit {
    budget
        .try_acquire(ResourceRequest::data(0, 0, ALLOWANCE).unwrap())
        .unwrap()
}
fn manifest() -> IngestManifest {
    IngestManifest::new(
        ManifestIdentity {
            dataset_id: [1; 16].try_into().unwrap(),
            topology_generation: TopologyGeneration::new(u64::MAX),
            owner_resource_id: [2; 16].try_into().unwrap(),
            shard_id: u32::MAX,
            owner_epoch: OwnerEpoch::new(u64::MAX).unwrap(),
            process_incarnation: [3; 16].try_into().unwrap(),
            recovery_generation: RecoveryGenerationId::from_bytes([0; 16]),
            policy_epoch: PolicyEpoch::new(0),
        },
        AllocationNamespaces::from_entries(&[(DictionaryKind::Event, [4; 16].try_into().unwrap())])
            .unwrap(),
    )
}
fn raw(path: &Path, names: &[&str], create: bool) -> DB {
    // Independent test fixture, deliberately outside the product's private handles.
    let mut options = Options::default();
    options.create_if_missing(create);
    options.create_missing_column_families(create);
    options.set_max_open_files(32);
    options.set_max_background_jobs(2);
    options.set_max_total_wal_size(8 * MIB);
    options.set_write_buffer_size(MIB as usize);
    options.set_max_write_buffer_number(2);
    let descriptors = names
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, options.clone()));
    DB::open_cf_descriptors(&options, path, descriptors).unwrap()
}
fn replace_manifest(path: &Path, bytes: Option<&[u8]>) {
    let db = raw(path, &["default", "facts", "receipts"], false);
    let mut write = WriteOptions::default();
    write.set_sync(true);
    write.disable_wal(false);
    if let Some(bytes) = bytes {
        db.put_opt([1, 1], bytes, &write).unwrap();
    } else {
        db.delete_opt([1, 1], &write).unwrap();
    }
}
fn assert_error(result: Result<IngestStore, StoreError>, expected: StoreError) {
    match result {
        Err(error) => assert_eq!(error, expected),
        Ok(_) => panic!("store unexpectedly opened"),
    }
}

#[test]
fn explicit_initialization_persists_only_the_exact_manifest_and_reopens() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = budget();
    let store = IngestStore::initialize(&path, manifest(), limits(), permit(&budget)).unwrap();
    assert!(path.join("CURRENT").is_file());
    assert_eq!(store.manifest(), &manifest());
    assert_eq!(store.native_limits(), limits());
    assert_eq!(
        &store.manifest().encode().as_bytes()[8..10],
        &NATIVE_OPTION_PROFILE_VERSION.to_be_bytes()
    );
    assert!(store.native_usage().memtable_bytes > 0);
    assert_eq!(budget.snapshot().used().native_bytes(), ALLOWANCE);
    assert_error(
        IngestStore::initialize(&path, manifest(), limits(), permit(&budget)),
        StoreError::PathExists,
    );
    drop(store);
    assert_eq!(budget.snapshot().active_permits(), 0);
    {
        let db = raw(&path, &["default", "facts", "receipts"], false);
        assert_eq!(
            db.get([1, 1]).unwrap().unwrap(),
            manifest().encode().as_bytes()
        );
        for cf in ["default", "facts", "receipts"] {
            let rows: Vec<_> = db
                .iterator_cf(db.cf_handle(cf).unwrap(), rocksdb::IteratorMode::Start)
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(rows.len(), usize::from(cf == "default"));
            if cf == "default" {
                assert_eq!(rows[0].0.as_ref(), &[1, 1]);
            }
        }
    }
    let store = IngestStore::reopen(&path, manifest(), limits(), permit(&budget)).unwrap();
    assert_eq!(store.manifest(), &manifest());
    drop(store);
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn recovery_inspection_retains_historical_identity_and_exclusive_custody() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = budget();
    let original = manifest();
    let dataset = original.identity().dataset_id;
    let store = IngestStore::initialize(&path, original, limits(), permit(&budget)).unwrap();
    assert!(matches!(
        IngestRecovery::inspect(&path, dataset, limits(), permit(&budget)),
        Err(StoreError::AlreadyOpen)
    ));
    drop(store);
    let recovery = IngestRecovery::inspect(&path, dataset, limits(), permit(&budget)).unwrap();
    // No original incarnation, recovery generation or allocation domain was
    // supplied by the new process. Inspection must retain the actual full tuple.
    assert_eq!(recovery.manifest(), &original);
    assert_eq!(budget.snapshot().used().native_bytes(), ALLOWANCE);
    assert_error(
        IngestStore::reopen(&path, original, limits(), permit(&budget)),
        StoreError::AlreadyOpen,
    );
    child_check(&path, root.path(), "locked");
    drop(recovery);
    assert_eq!(budget.snapshot().active_permits(), 0);
    child_check(&path, root.path(), "released");
    let db = raw(&path, &["default", "facts", "receipts"], false);
    for cf in ["default", "facts", "receipts"] {
        let rows: Vec<_> = db
            .iterator_cf(db.cf_handle(cf).unwrap(), rocksdb::IteratorMode::Start)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(rows.len(), usize::from(cf == "default"));
        if cf == "default" {
            assert_eq!(rows[0].0.as_ref(), &[1, 1]);
            assert_eq!(rows[0].1.as_ref(), original.encode().as_bytes());
        }
    }
}

#[test]
fn schema_three_is_inspectable_but_cannot_initialize_or_recreate_initial_candidate() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("schema-three");
    let budget = budget();
    let original = manifest();
    let mut identity = *original.identity();
    identity.owner_epoch = OwnerEpoch::new(3).unwrap();
    identity.topology_generation = TopologyGeneration::new(2);
    let successor =
        IngestManifest::for_repeatable_successor(identity, *original.allocation_namespaces());
    assert_error(
        IngestStore::initialize(&path, successor, limits(), permit(&budget)),
        StoreError::InvalidManifest(ManifestError::UnsupportedFormat),
    );
    assert!(!path.exists());
    assert_eq!(budget.snapshot().active_permits(), 0);
    drop(IngestStore::initialize(&path, original, limits(), permit(&budget)).unwrap());
    // Privileged fixture metadata is deliberately not a valid prepared lineage.
    // The format can be inspected; that alone cannot supply a writer candidate.
    replace_manifest(&path, Some(successor.encode().as_bytes()));
    let sequence = raw(&path, &["default", "facts", "receipts"], false).latest_sequence_number();
    let store = IngestStore::reopen(&path, successor, limits(), permit(&budget)).unwrap();
    assert_eq!(store.manifest(), &successor);
    drop(store);
    let recovery =
        IngestRecovery::inspect(&path, identity.dataset_id, limits(), permit(&budget)).unwrap();
    assert_eq!(recovery.manifest(), &successor);
    assert_error(
        IngestStore::reopen(&path, successor, limits(), permit(&budget)),
        StoreError::AlreadyOpen,
    );
    assert!(matches!(
        InitialWriterCandidate::new(recovery),
        Err(BootstrapError::InvalidIntent)
    ));
    assert_eq!(budget.snapshot().active_permits(), 0);
    let db = raw(&path, &["default", "facts", "receipts"], false);
    assert_eq!(db.latest_sequence_number(), sequence);
    for cf in ["default", "facts", "receipts"] {
        let rows: Vec<_> = db
            .iterator_cf(db.cf_handle(cf).unwrap(), rocksdb::IteratorMode::Start)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(rows.len(), usize::from(cf == "default"));
        if cf == "default" {
            assert_eq!(rows[0].0.as_ref(), &[1, 1]);
            assert_eq!(rows[0].1.as_ref(), successor.encode().as_bytes());
        }
    }
}

#[test]
fn recovery_inspection_refuses_foreign_missing_and_incompatible_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = budget();
    let dataset = manifest().identity().dataset_id;
    assert!(matches!(
        IngestRecovery::inspect(&path, dataset, limits(), permit(&budget)),
        Err(StoreError::MissingDatabase)
    ));
    assert!(!path.exists());
    drop(IngestStore::initialize(&path, manifest(), limits(), permit(&budget)).unwrap());
    let insufficient = budget
        .try_acquire(ResourceRequest::data(0, 0, ALLOWANCE - 1).unwrap())
        .unwrap();
    assert!(matches!(
        IngestRecovery::inspect(&path, dataset, limits(), insufficient),
        Err(StoreError::InsufficientNativeReservation)
    ));
    assert!(matches!(
        IngestRecovery::inspect(
            &path,
            [9; 16].try_into().unwrap(),
            limits(),
            permit(&budget)
        ),
        Err(StoreError::ManifestMismatch)
    ));
    drop(IngestStore::reopen(&path, manifest(), limits(), permit(&budget)).unwrap());
    replace_manifest(&path, None);
    assert!(matches!(
        IngestRecovery::inspect(&path, dataset, limits(), permit(&budget)),
        Err(StoreError::MissingManifest)
    ));
    let mut future = manifest().encode().as_bytes().to_vec();
    future[9] = 2;
    for bytes in [vec![1; 267], future] {
        replace_manifest(&path, Some(&bytes));
        assert!(matches!(
            IngestRecovery::inspect(&path, dataset, limits(), permit(&budget)),
            Err(StoreError::InvalidManifest(_))
        ));
        let db = raw(&path, &["default", "facts", "receipts"], false);
        assert_eq!(db.get([1, 1]).unwrap().unwrap(), bytes);
    }
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn missing_incomplete_or_existing_paths_never_bootstrap_implicitly() {
    let root = tempfile::tempdir().unwrap();
    let budget = budget();
    let absent = root.path().join("absent");
    assert_error(
        IngestStore::reopen(&absent, manifest(), limits(), permit(&budget)),
        StoreError::MissingDatabase,
    );
    assert!(!absent.exists());
    let parent_missing = root.path().join("missing-parent").join("child");
    assert_error(
        IngestStore::initialize(&parent_missing, manifest(), limits(), permit(&budget)),
        StoreError::InvalidPath,
    );
    assert!(!parent_missing.parent().unwrap().exists());
    let empty = root.path().join("empty");
    fs::create_dir(&empty).unwrap();
    assert_error(
        IngestStore::initialize(&empty, manifest(), limits(), permit(&budget)),
        StoreError::PathExists,
    );
    assert_error(
        IngestStore::reopen(&empty, manifest(), limits(), permit(&budget)),
        StoreError::MissingDatabase,
    );
    assert_eq!(fs::read_dir(&empty).unwrap().count(), 0);
    let file = root.path().join("file");
    fs::write(&file, b"independent sentinel").unwrap();
    assert_error(
        IngestStore::initialize(&file, manifest(), limits(), permit(&budget)),
        StoreError::PathExists,
    );
    assert_error(
        IngestStore::reopen(&file, manifest(), limits(), permit(&budget)),
        StoreError::InvalidPath,
    );
    assert_eq!(fs::read(file).unwrap(), b"independent sentinel");
    assert_error(
        IngestStore::initialize(Path::new("relative"), manifest(), limits(), permit(&budget)),
        StoreError::InvalidPath,
    );
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn missing_or_malformed_current_is_refused_before_native_open() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = budget();
    drop(IngestStore::initialize(&path, manifest(), limits(), permit(&budget)).unwrap());
    let current = fs::read(path.join("CURRENT")).unwrap();
    fs::remove_file(path.join("CURRENT")).unwrap();
    assert_error(
        IngestStore::reopen(&path, manifest(), limits(), permit(&budget)),
        StoreError::MissingDatabase,
    );
    assert!(!path.join("CURRENT").exists());
    for bytes in [
        &b""[..],
        &b"../elsewhere\n"[..],
        &[b'x'; 129][..],
        &b"MANIFEST-\n"[..],
        &b"MANIFEST-+1\n"[..],
        &b"MANIFEST-18446744073709551616\n"[..],
        &b"MANIFEST-1"[..],
        &b"MANIFEST-1\nignored"[..],
        &b"MANIFEST-../../x\n"[..],
    ] {
        fs::write(path.join("CURRENT"), bytes).unwrap();
        assert_error(
            IngestStore::reopen(&path, manifest(), limits(), permit(&budget)),
            StoreError::InvalidCurrent,
        );
        assert_eq!(fs::read(path.join("CURRENT")).unwrap(), bytes);
    }
    fs::write(path.join("CURRENT"), current).unwrap();
    drop(IngestStore::reopen(&path, manifest(), limits(), permit(&budget)).unwrap());
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn native_permit_is_checked_before_any_creation_and_released_on_refusal() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = budget();
    for request in [
        ResourceRequest::data(0, 0, ALLOWANCE - 1).unwrap(),
        ResourceRequest::control(1).unwrap(),
    ] {
        let permit = budget.try_acquire(request).unwrap();
        assert_error(
            IngestStore::initialize(&path, manifest(), limits(), permit),
            StoreError::InsufficientNativeReservation,
        );
        assert!(!path.exists());
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
    let store = IngestStore::initialize(&path, manifest(), limits(), permit(&budget)).unwrap();
    budget.close();
    assert!(budget.drain_until(Instant::now()).is_err());
    assert_eq!(budget.snapshot().used().native_bytes(), ALLOWANCE);
    drop(store);
    budget
        .drain_until(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

#[test]
fn every_full_identity_coordinate_and_namespace_mismatch_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = budget();
    drop(IngestStore::initialize(&path, manifest(), limits(), permit(&budget)).unwrap());
    let original = manifest();
    let base = *original.identity();
    let changed = [
        ManifestIdentity {
            dataset_id: [11; 16].try_into().unwrap(),
            ..base
        },
        ManifestIdentity {
            topology_generation: TopologyGeneration::new(0),
            ..base
        },
        ManifestIdentity {
            owner_resource_id: [12; 16].try_into().unwrap(),
            ..base
        },
        ManifestIdentity {
            shard_id: 0,
            ..base
        },
        ManifestIdentity {
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            ..base
        },
        ManifestIdentity {
            process_incarnation: [13; 16].try_into().unwrap(),
            ..base
        },
        ManifestIdentity {
            recovery_generation: RecoveryGenerationId::from_bytes([14; 16]),
            ..base
        },
        ManifestIdentity {
            policy_epoch: PolicyEpoch::new(u64::MAX),
            ..base
        },
    ];
    for identity in changed {
        let expected = IngestManifest::new(identity, *original.allocation_namespaces());
        assert_error(
            IngestStore::reopen(&path, expected, limits(), permit(&budget)),
            StoreError::ManifestMismatch,
        );
    }
    for entries in [
        vec![],
        vec![(
            DictionaryKind::Observer,
            AllocationDomainId::try_from([4; 16]).unwrap(),
        )],
        vec![(
            DictionaryKind::Event,
            AllocationDomainId::try_from([5; 16]).unwrap(),
        )],
    ] {
        let expected =
            IngestManifest::new(base, AllocationNamespaces::from_entries(&entries).unwrap());
        assert_error(
            IngestStore::reopen(&path, expected, limits(), permit(&budget)),
            StoreError::ManifestMismatch,
        );
    }
    assert_eq!(budget.snapshot().active_permits(), 0);
    let db = raw(&path, &["default", "facts", "receipts"], false);
    assert_eq!(
        db.get([1, 1]).unwrap().unwrap(),
        original.encode().as_bytes()
    );
}

#[test]
fn missing_corrupt_or_future_manifest_is_preserved_and_never_replaced() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = budget();
    drop(IngestStore::initialize(&path, manifest(), limits(), permit(&budget)).unwrap());
    replace_manifest(&path, None);
    assert_error(
        IngestStore::reopen(&path, manifest(), limits(), permit(&budget)),
        StoreError::MissingManifest,
    );
    assert_eq!(
        raw(&path, &["default", "facts", "receipts"], false)
            .get([1, 1])
            .unwrap(),
        None
    );
    let mut future = manifest().encode().as_bytes().to_vec();
    future[9] = 2;
    for bytes in [
        vec![],
        vec![0; 103],
        vec![1; 267],
        future,
        vec![0; 1024 * 1024],
    ] {
        replace_manifest(&path, Some(&bytes));
        match IngestStore::reopen(&path, manifest(), limits(), permit(&budget)) {
            Err(StoreError::InvalidManifest(_)) => {}
            _ => panic!("invalid manifest accepted"),
        }
        assert_eq!(
            raw(&path, &["default", "facts", "receipts"], false)
                .get([1, 1])
                .unwrap()
                .unwrap(),
            bytes
        );
    }
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn missing_or_extra_column_families_fail_closed_without_creating_families() {
    let root = tempfile::tempdir().unwrap();
    let budget = budget();
    for (index, names) in [
        &["default", "facts"][..],
        &["default", "receipts"][..],
        &["default", "facts", "receipts", "foreign"][..],
    ]
    .into_iter()
    .enumerate()
    {
        let path = root.path().join(format!("db{index}"));
        drop(raw(&path, names, true));
        let before = DB::list_cf(&Options::default(), &path).unwrap();
        assert_error(
            IngestStore::reopen(&path, manifest(), limits(), permit(&budget)),
            StoreError::ColumnFamiliesMismatch,
        );
        assert_eq!(DB::list_cf(&Options::default(), &path).unwrap(), before);
    }
    assert_eq!(budget.snapshot().active_permits(), 0);
}

fn child_check(path: &Path, root: &Path, expected: &str) {
    let output_path = root.join(format!("child-{expected}.log"));
    let output = fs::File::create(&output_path).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "canonical_alias_reservation_preserves_native_second_process_lock",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("BIGBROTR_ROCKS_STORE_TEST_CHILD", path)
        .env("BIGBROTR_ROCKS_STORE_TEST_EXPECT", expected)
        .stdout(Stdio::from(output.try_clone().unwrap()))
        .stderr(Stdio::from(output))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("owned child exceeded bounded lock test deadline");
        }
        thread::sleep(Duration::from_millis(10));
    };
    let output = fs::read_to_string(output_path).unwrap();
    assert!(status.success(), "child failed: {output}");
    assert!(
        output.contains("1 passed"),
        "child scenario did not execute: {output}"
    );
}

#[test]
fn canonical_alias_reservation_preserves_native_second_process_lock() {
    if let Some(path) = std::env::var_os("BIGBROTR_ROCKS_STORE_TEST_CHILD") {
        let budget = budget();
        let result = IngestStore::reopen(Path::new(&path), manifest(), limits(), permit(&budget));
        if std::env::var("BIGBROTR_ROCKS_STORE_TEST_EXPECT").unwrap() == "locked" {
            assert_error(result, StoreError::EngineOpen);
        } else {
            drop(result.unwrap());
        }
        assert_eq!(budget.snapshot().active_permits(), 0);
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = budget();
    let store = IngestStore::initialize(&path, manifest(), limits(), permit(&budget)).unwrap();
    assert_error(
        IngestStore::reopen(&path, manifest(), limits(), permit(&budget)),
        StoreError::AlreadyOpen,
    );
    #[cfg(unix)]
    {
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert_error(
            IngestStore::reopen(&alias, manifest(), limits(), permit(&budget)),
            StoreError::AlreadyOpen,
        );
    }
    child_check(&path, root.path(), "locked");
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(store);
    child_check(&path, root.path(), "released");
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[cfg(target_os = "macos")]
#[test]
fn case_alias_cannot_open_the_same_native_directory_twice() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("MixedCase");
    let alias = root.path().join("mixedcase");
    let budget = budget();
    let store = IngestStore::initialize(&path, manifest(), limits(), permit(&budget)).unwrap();
    // This concrete Mac filesystem scenario must execute; a case-sensitive
    // filesystem is unavailable evidence for this particular alias case.
    assert!(
        alias.exists(),
        "case-insensitive fixture unavailable on this filesystem"
    );
    assert_eq!(
        fs::canonicalize(&path).unwrap(),
        fs::canonicalize(&alias).unwrap()
    );
    assert_error(
        IngestStore::reopen(&alias, manifest(), limits(), permit(&budget)),
        StoreError::AlreadyOpen,
    );
    child_check(&path, root.path(), "locked");
    drop(store);
    child_check(&path, root.path(), "released");
}

#[cfg(unix)]
#[test]
fn special_current_entries_and_dangling_initialization_targets_are_refused() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let budget = budget();
    let missing = root.path().join("missing-target");
    let dangling = root.path().join("dangling");
    symlink(&missing, &dangling).unwrap();
    assert_error(
        IngestStore::initialize(&dangling, manifest(), limits(), permit(&budget)),
        StoreError::PathExists,
    );
    assert!(!missing.exists());
    assert!(fs::symlink_metadata(&dangling).unwrap().is_symlink());
    drop(IngestStore::initialize(&path, manifest(), limits(), permit(&budget)).unwrap());
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let current = path.join("CURRENT");
    let original = fs::read(&current).unwrap();
    fs::remove_file(&current).unwrap();
    let target = root.path().join("outside-current");
    fs::write(&target, &original).unwrap();
    symlink(&target, &current).unwrap();
    assert_error(
        IngestStore::reopen(&path, manifest(), limits(), permit(&budget)),
        StoreError::InvalidCurrent,
    );
    assert_eq!(fs::read(&target).unwrap(), original);
    fs::remove_file(&current).unwrap();
    assert!(
        Command::new("mkfifo")
            .arg(&current)
            .status()
            .unwrap()
            .success()
    );
    assert_error(
        IngestStore::reopen(&path, manifest(), limits(), permit(&budget)),
        StoreError::InvalidCurrent,
    );
    fs::remove_file(&current).unwrap();
    fs::create_dir(&current).unwrap();
    assert_error(
        IngestStore::reopen(&path, manifest(), limits(), permit(&budget)),
        StoreError::InvalidCurrent,
    );
    fs::remove_dir(&current).unwrap();
    fs::write(&current, original).unwrap();
    drop(IngestStore::reopen(&path, manifest(), limits(), permit(&budget)).unwrap());
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[cfg(unix)]
#[test]
fn racing_initializers_through_a_parent_alias_create_one_owned_store() {
    use std::sync::{Arc, Barrier};
    let root = tempfile::tempdir().unwrap();
    let budget = budget();
    let alias = root.path().join("parent-alias");
    std::os::unix::fs::symlink(root.path(), &alias).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let children: Vec<_> = [root.path().join("raced"), alias.join("raced")]
        .into_iter()
        .map(|path| {
            let barrier = Arc::clone(&barrier);
            let budget = budget.clone();
            thread::spawn(move || {
                let permit = permit(&budget);
                barrier.wait();
                IngestStore::initialize(&path, manifest(), limits(), permit)
            })
        })
        .collect();
    let results: Vec<_> = children
        .into_iter()
        .map(|child| child.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(
                result,
                Err(StoreError::AlreadyOpen | StoreError::PathExists)
            ))
            .count(),
        1
    );
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(results);
    assert_eq!(budget.snapshot().active_permits(), 0);
    let db = raw(
        &root.path().join("raced"),
        &["default", "facts", "receipts"],
        false,
    );
    assert_eq!(
        db.get([1, 1]).unwrap().unwrap(),
        manifest().encode().as_bytes()
    );
}
