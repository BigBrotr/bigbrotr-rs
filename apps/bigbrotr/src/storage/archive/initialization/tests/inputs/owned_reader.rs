//! Real file/lock controls for the owned read-only handoff. No catalog, native
//! child, source proof or SQL authority is fabricated by these fixtures.
use super::*;
use rustix::fs::{FlockOperation, flock};
use std::{
    fs::File,
    os::unix::fs::MetadataExt,
    sync::mpsc::sync_channel,
    time::{Duration, Instant},
};

fn input_budget() -> ResourceBudget {
    let bytes = RetainedInputs::required_data_bytes();
    ResourceBudget::new(
        ProcessEnvelope::new(
            bytes,
            0,
            ResourceLimits::new(bytes, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn granted(budget: &ResourceBudget, bytes: u64, items: u64) -> OwnedPermit {
    budget
        .try_acquire(ResourceRequest::data(bytes, items, 0).unwrap())
        .unwrap()
}
fn full_grant(budget: &ResourceBudget) -> OwnedPermit {
    granted(budget, RetainedInputs::required_data_bytes(), 1)
}
fn competitor(identity: &CatalogIdentity) -> File {
    // A genuinely separate open, not a dup of any product-held descriptor.
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(Path::new(identity.root()).join(".bigbrotr-initialization/custody.lock"))
        .unwrap()
}
fn denied(file: &File) -> bool {
    flock(file, FlockOperation::NonBlockingLockExclusive) == Err(rustix::io::Errno::WOULDBLOCK)
}
fn acquire_released(file: &File) {
    // Other test threads may fork and briefly inherit CLOEXEC descriptors. Only
    // a successful independent flock establishes release; time alone never does.
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        match flock(file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => return,
            Err(rustix::io::Errno::WOULDBLOCK) if Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(2));
            }
            result => panic!("independent lock acquisition after final drop: {result:?}"),
        }
    }
}

#[test]
fn owned_input_reader_and_joined_inputs_exclude_independent_open_until_last_drop() {
    for reader_first in [false, true] {
        let (_data, identity) = fixture();
        let (state, _) = fixture();
        let root = state.path().canonicalize().unwrap();
        drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
        let owner = coordinator(&identity, &root, true);
        let batches = batches();
        let batch = ArchiveBatch::new(1, TopologyGeneration::new(0), &batches).unwrap();
        let request = request(&batch);
        let seeded = owner
            .retain_inputs(&request, &batch, permit(), &AtomicBool::new(false))
            .unwrap();
        let expected_headers = seeded.headers().to_vec();
        let expected_receipt = seeded.expectation().clone();
        let expected_binding = seeded.coordinator_binding();
        drop(seeded);
        let input_path = root.join("06060606060606060606060606060606.inputs");
        let original_bytes = fs::read(&input_path).unwrap();
        let contender = competitor(&identity);
        let lock_identity = contender.metadata().unwrap();
        assert!(denied(&contender));
        let reader = owner.inputs_reader().unwrap();
        drop(owner);
        assert!(denied(&contender), "only the owned input reader remains");

        let budget = input_budget();
        let worker_budget = budget.clone();
        let worker_request = request.clone();
        let (ready_tx, ready_rx) = sync_channel(1);
        let (finish_tx, finish_rx) = sync_channel(1);
        // An unscoped thread checks the real Send handoff. No coordinator
        // reference or product lock acquisition path enters this worker.
        let worker = std::thread::spawn(move || {
            let inputs = reader
                .read_inputs(
                    &worker_request,
                    full_grant(&worker_budget),
                    &AtomicBool::new(false),
                )
                .unwrap()
                .unwrap();
            ready_tx.send(()).unwrap();
            finish_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            (reader, inputs)
        });
        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let locked_during_work = denied(&contender);
        let charged_during_work = budget.snapshot().used().data_bytes();
        finish_tx.send(()).unwrap();
        let (reader, inputs) = worker.join().unwrap();
        assert!(locked_during_work);
        assert_eq!(charged_during_work, RetainedInputs::required_data_bytes());
        assert!(
            denied(&contender),
            "thread join cannot drop returned custody"
        );
        assert_eq!(inputs.request(), &request);
        assert_eq!(inputs.headers(), expected_headers);
        assert_eq!(inputs.expectation(), &expected_receipt);
        assert_eq!(inputs.coordinator_binding(), expected_binding);
        assert_eq!(inputs.verify_replay(&batch), Ok(()));
        assert_eq!(fs::read(&input_path).unwrap(), original_bytes);
        if reader_first {
            drop(reader);
            assert!(denied(&contender), "returned inputs retain the same lock");
            assert_eq!(budget.snapshot().active_permits(), 1);
            drop(inputs);
        } else {
            drop(inputs);
            assert_eq!(budget.snapshot().active_permits(), 0);
            assert!(
                denied(&contender),
                "the reader keeps custody without inputs"
            );
            drop(reader);
        }
        assert_eq!(budget.snapshot().active_permits(), 0);
        assert_eq!(budget.snapshot().used().data_bytes(), 0);
        acquire_released(&contender);
        let after =
            fs::metadata(Path::new(identity.root()).join(".bigbrotr-initialization/custody.lock"))
                .unwrap();
        assert_eq!(
            (after.dev(), after.ino()),
            (lock_identity.dev(), lock_identity.ino())
        );
        assert_eq!(after.len(), 0);
        assert_eq!(fs::read(&input_path).unwrap(), original_bytes);
    }
}

#[test]
fn owned_input_reader_missing_and_corrupt_files_never_repair_or_retain_grants() {
    let (_data, identity) = fixture();
    let (state, _) = fixture();
    let root = state.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let owner = coordinator(&identity, &root, true);
    let batches = batches();
    let batch = ArchiveBatch::new(1, TopologyGeneration::new(0), &batches).unwrap();
    let request = request(&batch);
    let reader = owner.inputs_reader().unwrap();
    let budget = input_budget();
    let path = root.join("06060606060606060606060606060606.inputs");
    assert!(
        reader
            .read_inputs(&request, full_grant(&budget), &AtomicBool::new(false))
            .unwrap()
            .is_none()
    );
    assert!(!path.exists());
    assert_eq!(budget.snapshot().active_permits(), 0);
    drop(
        owner
            .retain_inputs(&request, &batch, permit(), &AtomicBool::new(false))
            .unwrap(),
    );
    let original = fs::read(&path).unwrap();
    drop(owner);
    let contender = competitor(&identity);
    assert!(denied(&contender));
    let mut checksum_corruption = original.clone();
    *checksum_corruption.last_mut().unwrap() ^= 1;
    for corrupt in [b"partial".to_vec(), checksum_corruption] {
        fs::write(&path, &corrupt).unwrap();
        assert!(matches!(
            reader.read_inputs(&request, full_grant(&budget), &AtomicBool::new(false)),
            Err(InputError::Identity)
        ));
        assert_eq!(budget.snapshot().active_permits(), 0);
        assert_eq!(budget.snapshot().used().data_bytes(), 0);
        assert_eq!(fs::read(&path).unwrap(), corrupt);
        assert!(denied(&contender));
    }
    fs::remove_file(&path).unwrap();
    assert!(
        reader
            .read_inputs(&request, full_grant(&budget), &AtomicBool::new(false))
            .unwrap()
            .is_none()
    );
    assert!(!path.exists());
    assert_eq!(budget.snapshot().active_permits(), 0);
    drop(reader);
    acquire_released(&contender);
}

#[test]
fn owned_input_reader_cancellation_foreign_identity_and_short_grants_preserve_bytes() {
    let (_data, identity) = fixture();
    let (state, _) = fixture();
    let root = state.path().canonicalize().unwrap();
    drop(LocalInitialization::create_for_boot(&identity, [3; 16], BOOT).unwrap());
    let owner = coordinator(&identity, &root, true);
    let batches = batches();
    let batch = ArchiveBatch::new(1, TopologyGeneration::new(0), &batches).unwrap();
    let request = request(&batch);
    drop(
        owner
            .retain_inputs(&request, &batch, permit(), &AtomicBool::new(false))
            .unwrap(),
    );
    let path = root.join("06060606060606060606060606060606.inputs");
    let original = fs::read(&path).unwrap();
    let reader = owner.inputs_reader().unwrap();
    drop(owner);
    let contender = competitor(&identity);
    let budget = input_budget();
    assert!(matches!(
        reader.read_inputs(&request, full_grant(&budget), &AtomicBool::new(true)),
        Err(InputError::Cancelled)
    ));
    assert_eq!(budget.snapshot().active_permits(), 0);
    let foreign = ArchiveAttemptRequest::new(
        [6; 16],
        [8; 16].try_into().unwrap(),
        request.coordinator(),
        request.worker(),
        7,
        &batch,
    )
    .unwrap();
    assert!(matches!(
        reader.read_inputs(&foreign, full_grant(&budget), &AtomicBool::new(false)),
        Err(InputError::Identity)
    ));
    assert_eq!(budget.snapshot().active_permits(), 0);
    let bytes = RetainedInputs::required_data_bytes();
    for (bytes, items) in [(bytes - 1, 1), (bytes, 0)] {
        assert!(matches!(
            reader.read_inputs(
                &request,
                granted(&budget, bytes, items),
                &AtomicBool::new(false)
            ),
            Err(InputError::ResourceLimit)
        ));
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
    assert_eq!(budget.snapshot().used().data_bytes(), 0);
    assert_eq!(fs::read(&path).unwrap(), original);
    assert!(denied(&contender));
    drop(reader);
    acquire_released(&contender);
}
