use super::*;
use bigbrotr_model::{
    AdmissionBatch, ArchiveBatch, DatasetId, Digest256, FactRef, OwnerEpoch, Sequence,
    SourceRecordId, TopologyGeneration,
};
use std::{
    fs,
    io::{BufRead, BufReader},
    os::unix::fs::{PermissionsExt, symlink},
    process::Stdio,
};

const BOOT: &str = "01234567-89ab-cdef-0123-456789abcdef";
fn fixture() -> (tempfile::TempDir, ArchiveAttemptRequest, CatalogIdentity) {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let input = [AdmissionBatch::new(vec![FactRef {
        source_record_id: SourceRecordId {
            dataset_id: DatasetId::try_from([1; 16]).unwrap(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(1).unwrap(),
        },
        payload_digest: Digest256::from_bytes([2; 32]),
    }])
    .unwrap()];
    let batch = ArchiveBatch::new(1, TopologyGeneration::new(0), &input).unwrap();
    let request = ArchiveAttemptRequest::new(
        [6; 16],
        [3; 16].try_into().unwrap(),
        [4; 16].try_into().unwrap(),
        [5; 16].try_into().unwrap(),
        0,
        &batch,
    )
    .unwrap();
    let identity =
        CatalogIdentity::local(batch.dataset_id(), request.catalog(), "/archive/").unwrap();
    (root, request, identity)
}

// Another test's concurrent fork can briefly retain CLOEXEC descriptors before
// exec. Retry actual nonblocking acquisition; elapsed time never grants custody.
fn recover_released(
    root: &Path,
    request: &ArchiveAttemptRequest,
    identity: &CatalogIdentity,
) -> LocalAttempt {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match LocalAttempt::recover_for_boot(root, request, identity, BOOT) {
            Ok(value) => return value,
            Err(LifetimeError::Occupied) if std::time::Instant::now() < until => {
                std::thread::sleep(std::time::Duration::from_millis(2))
            }
            Err(error) => panic!("released fixture remains unavailable: {error:?}"),
        }
    }
}

#[test]
fn retained_identity_is_locked_before_recovery_and_never_recreated() {
    let (root, request, identity) = fixture();
    let root = root.path().canonicalize().unwrap();
    let owner = LocalAttempt::create_for_boot(&root, &request, &identity, BOOT).unwrap();
    assert!(matches!(
        LocalAttempt::recover_for_boot(&root, &request, &identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    assert!(matches!(
        LocalAttempt::create_for_boot(&root, &request, &identity, BOOT),
        Err(LifetimeError::Exists)
    ));
    drop(owner);
    let recovered = recover_released(&root, &request, &identity);
    let mut command = Command::new("/bin/cat");
    assert_eq!(
        recovered.inherit(&mut command),
        Err(LifetimeError::Identity),
        "recovery must never grant another launch"
    );
    drop(recovered);
    let changed =
        CatalogIdentity::local(request.dataset(), request.catalog(), "/different/").unwrap();
    assert!(matches!(
        LocalAttempt::recover_for_boot(&root, &request, &changed, BOOT),
        Err(LifetimeError::Identity)
    ));
    assert!(matches!(
        LocalAttempt::recover_for_boot(
            &root,
            &request,
            &identity,
            "fedcba98-7654-3210-fedc-ba9876543210"
        ),
        Err(LifetimeError::Identity)
    ));
}

#[test]
fn recovery_authority_requires_reacquisition_not_a_consumed_launch() {
    let (root, request, identity) = fixture();
    let root = root.path().canonicalize().unwrap();
    let owner = LocalAttempt::create_for_boot(&root, &request, &identity, BOOT).unwrap();
    assert!(!owner.recovered_for(&request, &identity));
    let mut command = Command::new("/nonexistent-bigbrotr-lifetime-worker");
    owner.inherit(&mut command).unwrap();
    assert!(!owner.recovered_for(&request, &identity));
    drop(command);
    drop(owner);
    let recovered = recover_released(&root, &request, &identity);
    assert!(recovered.recovered_for(&request, &identity));
    let changed = CatalogIdentity::local(request.dataset(), request.catalog(), "/other/").unwrap();
    assert!(!recovered.recovered_for(&request, &changed));
    let batches = [AdmissionBatch::new(vec![FactRef {
        source_record_id: SourceRecordId {
            dataset_id: request.dataset(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(1).unwrap(),
        },
        payload_digest: Digest256::from_bytes([2; 32]),
    }])
    .unwrap()];
    let batch = ArchiveBatch::new(1, TopologyGeneration::new(1), &batches).unwrap();
    let changed = ArchiveAttemptRequest::new(
        *request.attempt_id(),
        request.catalog(),
        request.coordinator(),
        request.worker(),
        0,
        &batch,
    )
    .unwrap();
    assert!(!recovered.recovered_for(&changed, &identity));
}

#[test]
fn child_retains_lock_after_parent_descriptors_close_and_failed_spawn_does_not_leak() {
    let (root, request, identity) = fixture();
    let root = root.path().canonicalize().unwrap();
    let owner = LocalAttempt::create_for_boot(&root, &request, &identity, BOOT).unwrap();
    let mut command = Command::new("/bin/sh");
    command
        .args(["-c", "printf 'ready\\n'; read ignored"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    owner.inherit(&mut command).unwrap();
    assert_eq!(
        owner.inherit(&mut Command::new("/bin/cat")),
        Err(LifetimeError::Identity),
        "one launch only, including failures"
    );
    let mut child = command.spawn().unwrap();
    let mut ready = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    assert_eq!(ready, "ready\n");
    drop(command);
    drop(owner);
    assert!(matches!(
        LocalAttempt::recover_for_boot(&root, &request, &identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    child.kill().unwrap();
    child.wait().unwrap();
    drop(recover_released(&root, &request, &identity));

    let (second, request, identity) = fixture();
    let second = second.path().canonicalize().unwrap();
    let owner = LocalAttempt::create_for_boot(&second, &request, &identity, BOOT).unwrap();
    let mut command = Command::new("/nonexistent-bigbrotr-lifetime-worker");
    owner.inherit(&mut command).unwrap();
    assert!(command.spawn().is_err());
    drop(command);
    drop(owner);
    drop(recover_released(&second, &request, &identity));
}

#[test]
fn catalog_transport_retains_custody_even_before_its_future_is_polled() {
    let (root, request, identity) = fixture();
    let root = root.path().canonicalize().unwrap();
    let owner = LocalAttempt::create_for_boot(&root, &request, &identity, BOOT).unwrap();
    let connection = owner
        .retain_catalog_connection(std::future::pending::<()>())
        .unwrap();
    drop(owner);
    assert!(matches!(
        LocalAttempt::recover_for_boot(&root, &request, &identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    drop(connection);
    drop(recover_released(&root, &request, &identity));
}

#[test]
fn cancelled_transport_is_destroyed_before_its_custody_descriptor() {
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll, Waker},
    };
    struct Transport {
        root: std::path::PathBuf,
        request: ArchiveAttemptRequest,
        identity: CatalogIdentity,
        locked_on_drop: Arc<AtomicBool>,
    }
    impl Future for Transport {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for Transport {
        fn drop(&mut self) {
            self.locked_on_drop.store(
                matches!(
                    LocalAttempt::recover_for_boot(&self.root, &self.request, &self.identity, BOOT),
                    Err(LifetimeError::Occupied)
                ),
                Ordering::SeqCst,
            );
        }
    }
    let mut outcomes = Vec::new();
    for polled in [false, true] {
        let (root, request, identity) = fixture();
        let root = root.path().canonicalize().unwrap();
        let owner = LocalAttempt::create_for_boot(&root, &request, &identity, BOOT).unwrap();
        let observed = Arc::new(AtomicBool::new(false));
        let transport = Transport {
            root: root.clone(),
            request: request.clone(),
            identity: identity.clone(),
            locked_on_drop: observed.clone(),
        };
        let mut wrapped = Box::pin(owner.retain_catalog_connection(transport).unwrap());
        drop(owner);
        if polled {
            assert!(
                wrapped
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        drop(wrapped);
        outcomes.push(observed.load(Ordering::SeqCst));
        drop(recover_released(&root, &request, &identity));
    }
    assert_eq!(outcomes, vec![true, true]);
}

#[test]
fn mount_profile_uses_the_descriptors_actual_mount_not_shared_ext_magic() {
    let fdinfo = "pos:\t0\nflags:\t02100000\nmnt_id:\t45\nino:\t55\n";
    for (fstype, accepted) in [
        ("ext4", true),
        ("ext3", false),
        ("ext2", false),
        ("nfs", false),
        ("overlay", false),
    ] {
        let mounts = format!(
            "44 1 8:1 / /other rw - ext4 /dev/other rw\n45 1 8:2 / /journal rw - {fstype} /dev/journal rw\n"
        );
        assert_eq!(ext4_mount(fdinfo, &mounts), Ok(accepted), "{fstype}");
    }
    assert!(ext4_mount("mnt_id:\t45\nmnt_id:\t46\n", "").is_err());
    assert!(ext4_mount(fdinfo, "44 1 8:1 / / rw - ext4 /dev/a rw\n").is_err());
}

#[test]
fn competing_creators_keep_one_immutable_journal_and_one_lock_owner() {
    let (root, request, identity) = fixture();
    let root = root.path().canonicalize().unwrap();
    let barrier = std::sync::Barrier::new(2);
    let outcomes = std::thread::scope(|scope| {
        let run = || {
            barrier.wait();
            LocalAttempt::create_for_boot(&root, &request, &identity, BOOT)
        };
        let first = scope.spawn(run);
        let second = scope.spawn(run);
        [first.join().unwrap(), second.join().unwrap()]
    });
    assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, Err(LifetimeError::Exists)))
            .count(),
        1
    );
    assert!(matches!(
        LocalAttempt::recover_for_boot(&root, &request, &identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    drop(outcomes);
    drop(recover_released(&root, &request, &identity));
}

#[test]
fn altered_partial_oversized_and_linked_journals_never_become_recovery_custody() {
    let (root, request, identity) = fixture();
    let root = root.path().canonicalize().unwrap();
    let owner = LocalAttempt::create_for_boot(&root, &request, &identity, BOOT).unwrap();
    let manifest = root.join("06".repeat(16)).join(MANIFEST);
    let original = fs::read(&manifest).unwrap();
    let mut readonly = &owner.lock;
    assert!(readonly.write_all(b"must not modify custody").is_err());
    assert_eq!(owner.lock.metadata().unwrap().len(), 0);
    drop(owner);
    drop(recover_released(&root, &request, &identity));
    for (bytes, error) in [
        (Vec::new(), LifetimeError::Incomplete),
        (
            original[..original.len() - 1].to_vec(),
            LifetimeError::Identity,
        ),
        (
            format!(
                "{},\"extra\":true}}",
                std::str::from_utf8(&original[..original.len() - 1]).unwrap()
            )
            .into_bytes(),
            LifetimeError::Identity,
        ),
        (vec![b'x'; MAX_MANIFEST + 1], LifetimeError::ResourceLimit),
    ] {
        fs::write(&manifest, bytes).unwrap();
        assert_eq!(
            LocalAttempt::recover_for_boot(&root, &request, &identity, BOOT).err(),
            Some(error)
        );
    }
    fs::write(&manifest, &original).unwrap();
    let link = manifest.with_extension("second-link");
    fs::hard_link(&manifest, &link).unwrap();
    assert!(matches!(
        LocalAttempt::recover_for_boot(&root, &request, &identity, BOOT),
        Err(LifetimeError::Incomplete)
    ));
    fs::remove_file(link).unwrap();
    drop(recover_released(&root, &request, &identity));
}

#[test]
fn unsafe_roots_partial_history_and_inode_replacement_fail_closed() {
    let (root, request, identity) = fixture();
    let canonical = root.path().canonicalize().unwrap();
    fs::set_permissions(&canonical, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(
        LocalAttempt::create_for_boot(&canonical, &request, &identity, BOOT),
        Err(LifetimeError::InvalidRoot)
    ));
    fs::set_permissions(&canonical, fs::Permissions::from_mode(0o700)).unwrap();
    symlink(&canonical, canonical.join("alias")).unwrap();
    assert!(matches!(
        LocalAttempt::create_for_boot(&canonical.join("alias"), &request, &identity, BOOT),
        Err(LifetimeError::InvalidRoot)
    ));
    assert!(matches!(
        LocalAttempt::recover_for_boot(&canonical, &request, &identity, BOOT),
        Err(LifetimeError::Incomplete)
    ));
    let attempt = canonical.join("06".repeat(16));
    fs::create_dir(&attempt).unwrap();
    fs::set_permissions(&attempt, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(matches!(
        LocalAttempt::recover_for_boot(&canonical, &request, &identity, BOOT),
        Err(LifetimeError::Incomplete)
    ));
    fs::remove_dir(&attempt).unwrap(); // Test-only empty pre-provisioning fixture.
    let owner = LocalAttempt::create_for_boot(&canonical, &request, &identity, BOOT).unwrap();
    fs::rename(
        attempt.join("custody.lock"),
        attempt.join("retained-original.lock"),
    )
    .unwrap();
    let replacement = fs::File::create(attempt.join("custody.lock")).unwrap();
    replacement
        .set_permissions(fs::Permissions::from_mode(0o600))
        .unwrap();
    assert!(matches!(
        LocalAttempt::recover_for_boot(&canonical, &request, &identity, BOOT),
        Err(LifetimeError::Identity)
    ));
    drop(owner);
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
#[ignore = "Requires the exact native worker bundle; the native harness explicitly executes this case"]
fn native_worker_alone_retains_custody_after_coordinator_descriptors_close() {
    use crate::{runtime::*, storage::archive::*};
    use sha2::{Digest, Sha256};
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};
    const MIB: u64 = 1024 * 1024;
    let bundle = std::path::PathBuf::from(
        std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE").expect("explicit native bundle required"),
    );
    let bytes = fs::read(bundle.join("bigbrotr-archive-worker")).unwrap();
    let installation = WorkerInstallation {
        root: bundle,
        executable: ArtifactIdentity {
            bytes: bytes.len() as u64,
            sha256: Digest256::from_bytes(Sha256::digest(&bytes).into()),
        },
    };
    let (root, request, identity) = fixture();
    let root = root.path().canonicalize().unwrap();
    let owner = LocalAttempt::create_for_boot(&root, &request, &identity, BOOT).unwrap();
    let temp = tempfile::tempdir().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let temp = temp.path().canonicalize().unwrap();
    let cancelled = AtomicBool::new(false);
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            513 * MIB,
            0,
            ResourceLimits::new(0, 1, 512 * MIB, MIB),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let preparation = Preparation {
        installation: &installation,
        temp_root: &temp,
        limits: PreparationLimits {
            memory_bytes: 128 * MIB,
            temp_bytes: 64 * MIB,
            threads: 1,
            max_records: 16,
            max_encoded_bytes: MIB as usize,
            native_reserve_bytes: 384 * MIB,
        },
        deadline: Instant::now() + Duration::from_secs(15),
        cancelled: &cancelled,
    };
    let process = preparation
        .start_with_custody(
            "inspect-batch",
            &["16".into(), MIB.to_string()],
            &budget,
            Some(&owner),
        )
        .unwrap();
    // start_with_custody has returned: its Command/clone are already destroyed.
    drop(owner);
    let children = fs::read_to_string("/proc/thread-self/children").unwrap();
    let pids: Vec<u32> = children
        .split_whitespace()
        .map(|v| v.parse().unwrap())
        .collect();
    assert_eq!(pids.len(), 1);
    let pid = pids[0];
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let executable = fs::read_link(format!("/proc/{pid}/exe")).unwrap();
        let syscall = fs::read_to_string(format!("/proc/{pid}/syscall")).unwrap();
        // Native x86_64 read(0,...) occurs only after engine/bundle startup.
        if executable == installation.root.join("bigbrotr-archive-worker")
            && syscall.starts_with("0 0x0 ")
        {
            break;
        }
        assert!(
            Instant::now() < until,
            "actual native stdin boundary missing"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(matches!(
        LocalAttempt::recover_for_boot(&root, &request, &identity, BOOT),
        Err(LifetimeError::Occupied)
    ));
    assert!(budget.snapshot().active_permits() > 0);
    drop(process); // actual supervisor terminates/reaps before releasing grants
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    assert_eq!(budget.snapshot().active_permits(), 0);
    drop(recover_released(&root, &request, &identity));
}
