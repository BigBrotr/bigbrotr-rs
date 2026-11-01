//! Opt-in application-owner test on a dedicated disposable Linux tmpfs. It
//! never changes the production Env/options or calls the simulated Rust latch.
use super::admission_tests::{inputs, kv};
use super::bootstrap_tests::{budget, grant, intent, manifest, native, request};
use super::producer_ack::AckTarget;
use super::producer_checkpoint::CheckpointStamp;
use super::*;
use crate::storage::rocks::store::bootstrap_tests::initialize as fixture_initialize;
use bigbrotr_model::{ObservedTime, ProducerRequestId, RequestSequence};
use rocksdb::{FlushOptions, WaitForCompactOptions};
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    os::unix::fs::MetadataExt,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    time::{Duration, Instant},
};

const MIB: u64 = 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(30);

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn emit(evidence: &Path, event: serde_json::Value) {
    let mut file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(evidence.join("events.jsonl"))
        .unwrap();
    writeln!(file, "{event}").unwrap();
    file.sync_all().unwrap();
}

fn available(root: &Path) -> u64 {
    let stats = rustix::fs::statvfs(root).unwrap();
    stats.f_bavail.checked_mul(stats.f_frsize).unwrap()
}

fn no_space(error: &rocksdb::Error) -> bool {
    // Native Status::ToString on this fixed Linux/C-locale profile. A generic
    // I/O error, latch, or filler ENOSPC alone cannot satisfy this oracle.
    error.to_string().contains("No space left on device")
}

fn exercise(root: &Path, evidence: &Path) {
    let started = Instant::now();
    let b = budget();
    let path = root.join("db");
    let mut store =
        fixture_initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let limits = BootstrapLimits {
        max_mappings: 8,
        max_batch_bytes: MIB as usize,
    };
    let input = intent("public");
    let receipt = store
        .bootstrap_checkpoint_dictionaries(
            request(),
            &input,
            ObservedTime::new(123),
            limits,
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(()),
        )
        .unwrap();
    drop(receipt);
    assert!(!store.resources.background_failed());
    let stamp = CheckpointStamp::capture(&store).unwrap();
    let target = AckTarget::new(
        stamp,
        request().producer_slot_id,
        request().producer_epoch,
        1,
    );
    let next = ProducerRequestId {
        request_sequence: RequestSequence::new(2).unwrap(),
        ..request()
    };
    let append_limits = AdmissionLimits {
        max_batch_bytes: 256 * 1024,
        ..AdmissionLimits::default()
    };
    // A genuine next operation, with its original synchronized but no native
    // outcome or SQL reservation. Existing dictionary facts make its intended
    // result AlreadyDurable; a new receipt/producer state still needs a write.
    store
        .stage_dictionary_append(
            next,
            &input,
            ObservedTime::new(125),
            append_limits,
            &grant(&b, 4 * MIB, MIB),
        )
        .unwrap();
    let before = kv(&store);
    let originals = inputs(&path);
    assert_eq!(originals.len(), 2);
    for id in [request(), next] {
        let bytes = input
            .encode_request_preimage(id, limits.max_batch_bytes)
            .unwrap();
        assert_eq!(
            originals
                .iter()
                .filter(|(_, value)| value == &bytes)
                .count(),
            1
        );
    }
    let sequence = store.db.latest_sequence_number();
    let accounting = store.accounting;
    let credit = store.producer_credit(request().producer_slot_id).unwrap();
    assert_eq!(
        (credit.issued_through(), credit.acknowledged_through()),
        (1, 0)
    );
    emit(
        evidence,
        serde_json::json!({"event":"healthy", "sequence":sequence,
        "rows":before.len(), "rows_sha256":sha256(&serde_json::to_vec(&before).unwrap()),
        "originals_sha256":originals.iter().map(|(_, value)| sha256(value)).collect::<Vec<_>>(),
        "issued":1, "acknowledged":0, "staged_request":2, "available_bytes":available(root)}),
    );

    // Only this new file consumes the isolated tmpfs. Drop unlinks it even if
    // an assertion fails, restoring room before the store's native destructor.
    let mut filler = tempfile::NamedTempFile::new_in(root).unwrap();
    let chunk = [0xa5; 64 * 1024];
    let mut filled = 0_u64;
    loop {
        assert!(
            started.elapsed() < DEADLINE,
            "filler deadline; no fault conclusion"
        );
        // NamedTempFile::write wraps errors with path context and loses
        // raw_os_error(). Use the underlying File for the exact errno oracle.
        match filler.as_file_mut().write(&chunk) {
            Ok(0) => panic!("filler made no progress without ENOSPC"),
            Ok(count) => filled += count as u64,
            Err(error) => {
                assert_eq!(
                    error.raw_os_error(),
                    Some(rustix::io::Errno::NOSPC.raw_os_error()),
                    "filler must return raw ENOSPC: {error}"
                );
                break;
            }
        }
    }
    assert!(filled > 0);
    filler.as_file().sync_all().unwrap();
    assert_eq!(available(root), 0);
    emit(
        evidence,
        serde_json::json!({"event":"filler_enospc", "bytes":filled,
        "available_bytes":available(root)}),
    );

    let mut flush = FlushOptions::default();
    flush.set_wait(false);
    // A failed switch/foreground call is preserved as FAIL, not relabeled as
    // a background flush. Healthy product writes already returned before this.
    let scheduled = store.db.flush_opt(&flush);
    emit(
        evidence,
        serde_json::json!({"event":"flush_scheduled", "result":scheduled.as_ref().err().map(ToString::to_string)}),
    );
    scheduled.unwrap();
    while !store.resources.background_failed() {
        assert!(
            started.elapsed() < DEADLINE,
            "native latch deadline; no failure inference"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut wait = WaitForCompactOptions::default();
    wait.set_abort_on_pause(true);
    wait.set_wait_for_purge(true);
    wait.set_timeout(1_000_000);
    let failure = store.db.wait_for_compact(&wait).unwrap_err();
    assert!(
        no_space(&failure),
        "native failure was not the required POSIX NoSpace"
    );
    let background_errors = store
        .db
        .property_int_value("rocksdb.background-errors")
        .unwrap()
        .unwrap();
    assert!(background_errors > 0);
    assert!(
        !store.write_uncertain,
        "no foreground application write failed"
    );
    emit(
        evidence,
        serde_json::json!({"event":"native_nospace", "status":failure.to_string(),
        "background_errors":background_errors, "guard_failed":store.resources.background_failed()}),
    );
    filler.close().unwrap();
    assert!(available(root) > MIB);

    // Restored headroom cannot clear this owner's sticky observation. These
    // are existing app entrypoints, including replay and a pre-fault ACK stamp.
    assert!(store.resources.background_failed());
    assert_eq!(
        CheckpointStamp::capture(&store),
        Err(AdmissionError::CommitUnknown)
    );
    assert_eq!(stamp.require(&store), Err(AdmissionError::CommitUnknown));
    assert_eq!(
        store.producer_credit(request().producer_slot_id),
        Err(AdmissionError::CommitUnknown)
    );
    assert_eq!(
        store.test_acknowledge_target(target, &grant(&b, 4096, 4096), || Ok(())),
        Err(AdmissionError::CommitUnknown)
    );
    assert!(matches!(
        store.stage_dictionary_append(
            next,
            &input,
            ObservedTime::new(125),
            append_limits,
            &grant(&b, 4 * MIB, MIB)
        ),
        Err(AdmissionError::CommitUnknown)
    ));
    assert!(matches!(
        store.append_dictionaries(
            next,
            &input,
            ObservedTime::new(125),
            append_limits,
            grant(&b, 4 * MIB, MIB),
            || Ok(())
        ),
        Err(AdmissionError::CommitUnknown)
    ));
    assert!(matches!(
        store.stage_initial_dictionaries(
            request(),
            &input,
            ObservedTime::new(123),
            limits,
            &grant(&b, 8 * MIB, 4 * MIB)
        ),
        Err(BootstrapError::CommitUnknown)
    ));
    assert!(matches!(
        store.bootstrap_checkpoint_dictionaries(
            request(),
            &input,
            ObservedTime::new(124),
            limits,
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(())
        ),
        Err(BootstrapError::CommitUnknown)
    ));
    assert!(matches!(
        store.bootstrap_dictionaries(
            request(),
            &input,
            ObservedTime::new(124),
            limits,
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(())
        ),
        Err(BootstrapError::CommitUnknown)
    ));
    assert_eq!(store.db.latest_sequence_number(), sequence);
    assert_eq!(store.accounting, accounting);
    assert_eq!(kv(&store), before);
    assert_eq!(inputs(&path), originals);
    assert_eq!(
        b.snapshot().used().data_bytes(),
        crate::storage::rocks::store::PENDING_QUOTA_OWNER_DATA_BYTES
    );
    assert_eq!(b.snapshot().used().native_bytes(), 128 * MIB);
    let retained = store.db.wait_for_compact(&wait).unwrap_err();
    assert!(no_space(&retained));
    emit(
        evidence,
        serde_json::json!({"event":"sticky_refusal", "sequence":sequence,
        "available_bytes":available(root), "guard_failed":store.resources.background_failed(),
        "rows_preserved":true, "originals_preserved":true, "native_status":retained.to_string()}),
    );
    drop(store); // DB/native jobs finish before resources and owner permit.
    assert_eq!(b.snapshot().used().native_bytes(), 0);
    assert_eq!(inputs(&path), originals);
    assert!(
        started.elapsed() < DEADLINE,
        "close deadline; no close qualification"
    );
    emit(
        evidence,
        serde_json::json!({"event":"close_return", "owner_native_bytes":0,
        "elapsed_seconds":started.elapsed().as_secs_f64()}),
    );
}

fn preserve(root: &Path, evidence: &Path) {
    fn copy(source: &Path, target: &Path, total: &mut u64, records: &mut Vec<serde_json::Value>) {
        fs::create_dir(target).unwrap();
        let mut entries: Vec<_> = fs::read_dir(source).unwrap().map(Result::unwrap).collect();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let info = fs::symlink_metadata(entry.path()).unwrap();
            assert!(!info.file_type().is_symlink());
            let destination = target.join(entry.file_name());
            if info.is_dir() {
                copy(&entry.path(), &destination, total, records);
            } else {
                assert!(info.is_file() && info.nlink() == 1);
                *total = total.checked_add(info.len()).unwrap();
                assert!(*total <= 64 * MIB && records.len() < 128);
                fs::copy(entry.path(), &destination).unwrap();
                let bytes = fs::read(entry.path()).unwrap();
                assert_eq!(fs::read(&destination).unwrap(), bytes);
                records.push(serde_json::json!({"path":destination, "bytes":info.len(),
                    "allocated_bytes":info.blocks() * 512, "sha256":sha256(&bytes)}));
            }
        }
    }
    let db = root.join("db");
    let mut records = Vec::new();
    if db.exists() {
        copy(&db, &evidence.join("retained-db"), &mut 0, &mut records);
    }
    fs::write(
        evidence.join("retained-files.json"),
        serde_json::to_vec_pretty(&records).unwrap(),
    )
    .unwrap();
}

#[test]
#[ignore = "requires dedicated empty 64MiB Linux tmpfs and separate persistent evidence mount"]
fn real_posix_background_nospace_latches_application_owner() {
    let root = PathBuf::from(std::env::var_os("BIGBROTR_NATIVE_POSIX_ROOT").unwrap());
    let evidence = PathBuf::from(std::env::var_os("BIGBROTR_NATIVE_POSIX_EVIDENCE").unwrap());
    assert!(root.is_absolute() && evidence.is_absolute());
    assert_eq!(root.canonicalize().unwrap(), root);
    assert_eq!(evidence.canonicalize().unwrap(), evidence);
    assert!(root.read_dir().unwrap().next().is_none());
    assert!(evidence.read_dir().unwrap().next().is_none());
    let uid = rustix::process::geteuid().as_raw();
    assert_eq!(root.metadata().unwrap().uid(), uid);
    assert_eq!(evidence.metadata().unwrap().uid(), uid);
    assert_ne!(
        root.metadata().unwrap().dev(),
        evidence.metadata().unwrap().dev()
    );
    let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap();
    let matches: Vec<_> = mounts
        .lines()
        .filter(|line| {
            let (before, after) = line.split_once(" - ").unwrap();
            before.split_whitespace().nth(4) == root.to_str() && after.starts_with("tmpfs ")
        })
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "require the exact dedicated tmpfs mountpoint"
    );
    let stats = rustix::fs::statvfs(&root).unwrap();
    assert_eq!(stats.f_blocks * stats.f_frsize, 64 * MIB);
    assert!(available(&root) >= 63 * MIB);
    emit(
        &evidence,
        serde_json::json!({"event":"identity", "pid":std::process::id(),
        "root_device":root.metadata().unwrap().dev(), "evidence_device":evidence.metadata().unwrap().dev(),
        "mount":matches[0], "capacity_bytes":64 * MIB}),
    );
    let result = catch_unwind(AssertUnwindSafe(|| exercise(&root, &evidence)));
    // Preserve native files and original inputs after the owner has closed,
    // including failures, before the container's disposable tmpfs is unmounted.
    preserve(&root, &evidence);
    emit(
        &evidence,
        serde_json::json!({"event":"terminal", "status":if result.is_ok() {"PASS"} else {"FAIL"}}),
    );
    if let Err(error) = result {
        resume_unwind(error);
    }
}
