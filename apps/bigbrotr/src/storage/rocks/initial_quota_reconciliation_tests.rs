//! Native OPEN proof mechanics under explicit synthetic current-PG premises.
//! Reuse the independently signed inputs and exact prior immutable native bytes;
//! no closing resolver or fixture quota bypass supplies admission authority.
use super::*;
use crate::storage::rocks::{
    keys::{IngestKey, KeyDomain},
    store::{
        INITIAL_QUOTA_RECONCILIATION_DATA_BYTES, InitialQuotaReconciliation, PendingQuotaPolicy,
        ProducerRecoveryError,
    },
};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

static RUNNING: AtomicBool = AtomicBool::new(false);
struct Original {
    request: ProducerRequestId,
    digest: Digest256,
    input_bytes: u32,
    outcome: Vec<u8>,
}
struct InitialFixture {
    root: tempfile::TempDir,
    store: IngestStore,
    budget: ResourceBudget,
    originals: Vec<Original>,
}
fn path_input(root: &Path, sequence: u64) -> std::path::PathBuf {
    root.join("db/producer-inputs-v1").join(format!(
        "{}-0000000000000001-{sequence:016x}.cbor",
        request(7, 1).producer_slot_id
    ))
}
fn fixture() -> InitialFixture {
    let (root, mut store, budget, manifest, native) = setup(false);
    let evidence = EvidenceIntent::new(
        1,
        reference(DictionaryKind::VisibilityScope, 1),
        vec![entries(false).remove(0)],
        EvidenceIntentLimits {
            max_entries: 16,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap();
    let result = store
        .append_dictionaries(
            request(7, 2),
            &evidence,
            ObservedTime::new(20),
            AdmissionLimits::default(),
            grant(&budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
        .unwrap();
    assert!(result.receipt().batch().is_none());
    drop(result);
    let before_event = store.pending_accounting().unwrap().pending_bytes();
    for n in [3, 4, 5] {
        let intent = if n == 5 {
            let corpus: serde_json::Value = serde_json::from_str(include_str!(
                "../../../../../tests/fixtures/ingest/signed-event-admission.json"
            ))
            .unwrap();
            let mut input = corpus["scenarios"][0]["operations"][4]["input"].clone();
            input["observation"]["witness"] = serde_json::Value::Null;
            fixture_intent(&input)
        } else {
            intent("E_private", 2, 10_000_000)
        };
        let input =
            PreparedEvent::new(request(7, n), intent, 256 * 1024, grant(&budget, MIB, 0)).unwrap();
        let result = store
            .admit_event(
                input,
                ObservedTime::new(10_000_000),
                false,
                AdmissionLimits::default(),
                grant(&budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap();
        match result.outcome() {
            AdmissionOutcome::Durable(receipt) => assert_eq!(receipt.batch().is_some(), n == 3),
            AdmissionOutcome::Rejected(reason) => {
                assert_eq!(n, 5);
                assert_eq!(*reason, RejectionReason::Signature);
            }
        }
        drop(result);
    }
    assert_eq!(
        store.pending_accounting().unwrap().pending_bytes() - before_event,
        1265
    );
    let originals = (1..=5)
        .map(|sequence| {
            let request = request(7, sequence);
            let bytes = std::fs::read(path_input(root.path(), sequence)).unwrap();
            let key = IngestKey::Request {
                request_id: request,
            }
            .encode()
            .unwrap();
            let outcome = store
                .db
                .get_cf(store.db.cf_handle("receipts").unwrap(), key.as_bytes())
                .unwrap()
                .unwrap();
            Original {
                request,
                digest: Digest256::from_bytes(Sha256::digest(&bytes).into()),
                input_bytes: u32::try_from(bytes.len()).unwrap(),
                outcome,
            }
        })
        .collect();
    drop(store);
    let store = IngestStore::reopen(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    assert_eq!(
        store.pending_quota_status(),
        PendingQuotaStatus::RecoveryBlocked
    );
    InitialFixture {
        root,
        store,
        budget,
        originals,
    }
}
fn policy() -> PendingQuotaPolicy {
    PendingQuotaPolicy::new(
        1024 * MIB,
        512 * MIB,
        16 * MIB,
        2,
        Digest256::from_bytes([31; 32]),
        2,
        Digest256::from_bytes([32; 32]),
        Duration::from_secs(30),
    )
    .unwrap()
}
fn begin(f: &InitialFixture) -> InitialQuotaReconciliation {
    f.store
        .begin_initial_quota_reconciliation(
            request(7, 1).producer_slot_id,
            grant(&f.budget, INITIAL_QUOTA_RECONCILIATION_DATA_BYTES, 0),
            &RUNNING,
        )
        .unwrap()
}
fn record_all(f: &InitialFixture, head: &mut InitialQuotaReconciliation) {
    let mut input = grant(&f.budget, 3 * MIB, 0);
    let mut outcome = grant(&f.budget, 40 * MIB, 0);
    let retained = f.budget.snapshot().used();
    for (index, original) in f.originals.iter().enumerate() {
        let resolved = f
            .store
            .read_initial_quota_original(
                head,
                original.request,
                original.digest,
                producer_recovery_tests::recovery_limits(),
                input,
                outcome,
                &RUNNING,
            )
            .unwrap();
        assert_eq!(resolved.input_bytes(), original.input_bytes);
        assert_eq!(resolved.native_outcome().bytes(), original.outcome);
        assert_eq!(
            resolved.native_outcome().kind(),
            [
                NativeOutcomeKind::NewFacts,
                NativeOutcomeKind::AlreadyDurable,
                NativeOutcomeKind::NewFacts,
                NativeOutcomeKind::AlreadyDurable,
                NativeOutcomeKind::Rejected
            ][index]
        );
        (input, outcome) = head.record(resolved).unwrap();
        assert_eq!(
            f.budget.snapshot().used(),
            retained,
            "same two grants reused after PG equality premise"
        );
    }
}
#[test]
fn complete_open_initial_reconciliation_replays_all_known_kinds_without_native_writes() {
    let mut f = fixture();
    let before = kv(&f.store);
    let originals = inputs(&f.root.path().join("db"));
    let sequence = f.store.db.latest_sequence_number();
    let accounting = f.store.pending_accounting().unwrap();
    let mut head = begin(&f);
    assert_eq!(head.issued_through(), 5);
    record_all(&f, &mut head);
    f.store
        .finish_initial_quota_reconciliation(head, policy(), &RUNNING)
        .unwrap();
    assert_eq!(f.store.pending_quota_status(), PendingQuotaStatus::Pressure);
    assert_eq!(
        f.store.observe_pending_quota().unwrap(),
        PendingQuotaStatus::Pressure
    );
    assert_eq!(f.store.pending_reserved_bytes(), Ok(0));
    assert_eq!(f.store.pending_accounting().unwrap(), accounting);
    assert_eq!(f.store.db.latest_sequence_number(), sequence);
    assert_eq!(kv(&f.store), before);
    assert_eq!(inputs(&f.root.path().join("db")), originals);
}

#[test]
fn open_reconciliation_refuses_missing_extra_corrupt_and_foreign_native_or_original_sets() {
    for fault in 0..7 {
        let f = fixture();
        let cf = f.store.db.cf_handle("receipts").unwrap();
        match fault {
            0 => std::fs::remove_file(path_input(f.root.path(), 2)).unwrap(),
            1 => {
                std::fs::copy(path_input(f.root.path(), 1), path_input(f.root.path(), 6)).unwrap();
            }
            2 => f
                .store
                .db
                .delete_cf(
                    cf,
                    IngestKey::Request {
                        request_id: request(7, 3),
                    }
                    .encode()
                    .unwrap()
                    .as_bytes(),
                )
                .unwrap(),
            3 => {
                let marker = [
                    vec![2, 0x31],
                    5u64.to_be_bytes().to_vec(),
                    vec![0; 16],
                    vec![1],
                ]
                .concat();
                f.store
                    .db
                    .put_cf(
                        cf,
                        IngestKey::ProducerState {
                            producer_slot_id: request(7, 1).producer_slot_id,
                            producer_epoch: ProducerEpoch::new(1).unwrap(),
                        }
                        .encode()
                        .unwrap()
                        .as_bytes(),
                        marker,
                    )
                    .unwrap();
            }
            4 => {
                let marker = [
                    vec![1, 0x31],
                    5u64.to_be_bytes().to_vec(),
                    1u64.to_be_bytes().to_vec(),
                    1u64.to_be_bytes().to_vec(),
                ]
                .concat();
                f.store
                    .db
                    .put_cf(
                        cf,
                        IngestKey::ProducerState {
                            producer_slot_id: request(7, 1).producer_slot_id,
                            producer_epoch: ProducerEpoch::new(1).unwrap(),
                        }
                        .encode()
                        .unwrap()
                        .as_bytes(),
                        marker,
                    )
                    .unwrap();
            }
            5 => f
                .store
                .db
                .put_cf(
                    cf,
                    IngestKey::Request {
                        request_id: request(9, 1),
                    }
                    .encode()
                    .unwrap()
                    .as_bytes(),
                    &f.originals[0].outcome,
                )
                .unwrap(),
            6 => f
                .store
                .db
                .put_cf(
                    cf,
                    IngestKey::ProducerState {
                        producer_slot_id: request(9, 1).producer_slot_id,
                        producer_epoch: ProducerEpoch::new(1).unwrap(),
                    }
                    .encode()
                    .unwrap()
                    .as_bytes(),
                    [vec![1, 0x31], vec![0; 24]].concat(),
                )
                .unwrap(),
            _ => unreachable!(),
        }
        let before = kv(&f.store);
        let originals = inputs(&f.root.path().join("db"));
        let sequence = f.store.db.latest_sequence_number();
        assert!(
            f.store
                .begin_initial_quota_reconciliation(
                    request(7, 1).producer_slot_id,
                    grant(&f.budget, INITIAL_QUOTA_RECONCILIATION_DATA_BYTES, 0),
                    &RUNNING
                )
                .is_err(),
            "fault{fault}"
        );
        assert_eq!(f.store.db.latest_sequence_number(), sequence);
        assert_eq!(kv(&f.store), before);
        assert_eq!(inputs(&f.root.path().join("db")), originals);
        assert_eq!(
            f.store.pending_quota_status(),
            PendingQuotaStatus::RecoveryBlocked
        );
    }
    let f = fixture();
    std::fs::write(path_input(f.root.path(), 2), b"partial-original").unwrap();
    let mut head = begin(&f);
    let first = f
        .store
        .read_initial_quota_original(
            &head,
            f.originals[0].request,
            f.originals[0].digest,
            producer_recovery_tests::recovery_limits(),
            grant(&f.budget, 3 * MIB, 0),
            grant(&f.budget, 40 * MIB, 0),
            &RUNNING,
        )
        .unwrap();
    let (input, outcome) = head.record(first).unwrap();
    assert!(
        f.store
            .read_initial_quota_original(
                &head,
                f.originals[1].request,
                f.originals[1].digest,
                producer_recovery_tests::recovery_limits(),
                input,
                outcome,
                &RUNNING
            )
            .is_err()
    );
}

#[test]
fn reconciliation_finish_refuses_incomplete_or_changed_native_custody_and_input_stamp() {
    for fault in 0..5 {
        let mut f = fixture();
        let mut head = begin(&f);
        if fault != 0 {
            record_all(&f, &mut head);
        }
        match fault {
            0 => {}
            1 => {
                let path = path_input(f.root.path(), 5);
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[0] ^= 1;
                std::fs::write(path, bytes).unwrap();
            }
            2 => f
                .store
                .db
                .put(
                    KeyDomain::Manifest.prefix(),
                    f.store.manifest().encode().as_bytes(),
                )
                .unwrap(),
            3 => f.store.write_uncertain = true,
            4 => {}
            _ => unreachable!(),
        }
        let before = kv(&f.store);
        let sequence = f.store.db.latest_sequence_number();
        let originals = inputs(&f.root.path().join("db"));
        let cancelled = AtomicBool::new(fault == 4);
        assert!(
            f.store
                .finish_initial_quota_reconciliation(head, policy(), &cancelled)
                .is_err(),
            "fault{fault}"
        );
        assert_eq!(kv(&f.store), before);
        assert_eq!(f.store.db.latest_sequence_number(), sequence);
        assert_eq!(inputs(&f.root.path().join("db")), originals);
        assert_eq!(
            f.store.pending_quota_status(),
            PendingQuotaStatus::RecoveryBlocked
        );
    }
    let first = fixture();
    let mut second = fixture();
    let head = begin(&first);
    assert!(
        second
            .store
            .finish_initial_quota_reconciliation(head, policy(), &RUNNING)
            .is_err()
    );
}

#[test]
fn reconciliation_requires_all_retained_source_batches_to_belong_to_the_exact_request_prefix() {
    let mut f = fixture();
    let receipts = f.store.db.cf_handle("receipts").unwrap();
    let quarantine = f.root.path().join("mixed-copy-originals");
    std::fs::create_dir(&quarantine).unwrap();
    for sequence in 3..=5 {
        f.store
            .db
            .delete_cf(
                receipts,
                IngestKey::Request {
                    request_id: request(7, sequence),
                }
                .encode()
                .unwrap()
                .as_bytes(),
            )
            .unwrap();
        std::fs::rename(
            path_input(f.root.path(), sequence),
            quarantine.join(format!("{sequence}.cbor")),
        )
        .unwrap();
    }
    f.store
        .db
        .put_cf(
            receipts,
            IngestKey::ProducerState {
                producer_slot_id: request(7, 1).producer_slot_id,
                producer_epoch: ProducerEpoch::new(1).unwrap(),
            }
            .encode()
            .unwrap()
            .as_bytes(),
            [vec![1, 0x31], 2u64.to_be_bytes().to_vec(), vec![0; 16]].concat(),
        )
        .unwrap();
    f.originals.truncate(2);
    let before = kv(&f.store);
    let sequence = f.store.db.latest_sequence_number();
    let mut head = begin(&f); // Native P census and both exact receipt/input sets are valid.
    assert_eq!(head.issued_through(), 2);
    record_all(&f, &mut head);
    assert!(matches!(
        f.store
            .finish_initial_quota_reconciliation(head, policy(), &RUNNING),
        Err(ProducerRecoveryError::Native(AdmissionError::StateMismatch))
    ));
    assert_eq!(kv(&f.store), before);
    assert_eq!(f.store.db.latest_sequence_number(), sequence);
    assert_eq!(
        f.store.pending_quota_status(),
        PendingQuotaStatus::RecoveryBlocked
    );
}

#[test]
fn known_empty_three_row_reconciliation_does_not_adopt_an_original_tail_or_open_marker() {
    use crate::storage::rocks::store::bootstrap_tests as small;
    for fault in 0..3 {
        let root = tempfile::tempdir().unwrap();
        let budget = small::budget();
        let path = root.path().join("db");
        drop(
            IngestStore::initialize(
                &path,
                small::manifest(),
                small::native(),
                small::grant(&budget, 0, 128 * MIB),
            )
            .unwrap(),
        );
        let mut store = IngestStore::reopen(
            &path,
            small::manifest(),
            small::native(),
            small::grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        if fault == 1 {
            let original = small::intent("public")
                .encode_request_preimage(small::request(), 8192)
                .unwrap();
            // Explicit incompatible retained original; no quota/staging bypass.
            let spool = path.join("producer-inputs-v1");
            std::fs::create_dir(&spool).unwrap();
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o700)).unwrap();
            let name = format!(
                "{}-0000000000000001-0000000000000001.cbor",
                small::request().producer_slot_id
            );
            use std::io::Write;
            std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(spool.join(name))
                .unwrap()
                .write_all(&original)
                .unwrap();
        } else if fault == 2 {
            store
                .db
                .put_cf(
                    store.db.cf_handle("receipts").unwrap(),
                    IngestKey::ProducerState {
                        producer_slot_id: small::request().producer_slot_id,
                        producer_epoch: ProducerEpoch::new(1).unwrap(),
                    }
                    .encode()
                    .unwrap()
                    .as_bytes(),
                    [vec![1, 0x31], vec![0; 24]].concat(),
                )
                .unwrap();
        }
        let sequence = store.db.latest_sequence_number();
        let result = store.begin_initial_quota_reconciliation(
            small::request().producer_slot_id,
            small::grant(&budget, INITIAL_QUOTA_RECONCILIATION_DATA_BYTES, 0),
            &RUNNING,
        );
        if fault == 0 {
            let head = result.unwrap();
            assert_eq!(head.issued_through(), 0);
            store
                .finish_initial_quota_reconciliation(head, policy(), &RUNNING)
                .unwrap();
            assert_eq!(store.pending_quota_status(), PendingQuotaStatus::Pressure);
        } else {
            assert!(result.is_err());
            assert_eq!(
                store.pending_quota_status(),
                PendingQuotaStatus::RecoveryBlocked
            );
        }
        assert_eq!(store.db.latest_sequence_number(), sequence);
        assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 0);
    }
}

#[test]
fn open_quota_proof_keeps_closing_resolver_strict_and_refuses_cancelled_or_unfunded_proof() {
    let f = fixture();
    let bad = f.store.begin_initial_quota_reconciliation(
        request(7, 1).producer_slot_id,
        grant(&f.budget, 1, 0),
        &RUNNING,
    );
    assert!(matches!(
        bad,
        Err(ProducerRecoveryError::Native(
            AdmissionError::InsufficientReservation
        ))
    ));
    let cancel = AtomicBool::new(true);
    assert!(
        f.store
            .begin_initial_quota_reconciliation(
                request(7, 1).producer_slot_id,
                grant(&f.budget, INITIAL_QUOTA_RECONCILIATION_DATA_BYTES, 0),
                &cancel
            )
            .is_err()
    );
    cancel.store(false, Ordering::Release);
    let head = begin(&f);
    assert!(
        f.store
            .read_initial_quota_original(
                &head,
                f.originals[1].request,
                f.originals[1].digest,
                producer_recovery_tests::recovery_limits(),
                grant(&f.budget, 3 * MIB, 0),
                grant(&f.budget, 40 * MIB, 0),
                &RUNNING,
            )
            .is_err()
    );
    assert!(
        f.store
            .read_initial_quota_original(
                &head,
                f.originals[0].request,
                Digest256::from_bytes([99; 32]),
                producer_recovery_tests::recovery_limits(),
                grant(&f.budget, 3 * MIB, 0),
                grant(&f.budget, 40 * MIB, 0),
                &RUNNING,
            )
            .is_err()
    );
    drop(head);
    let original = &f.originals[0];
    let recovery = IngestRecovery { store: f.store };
    assert!(matches!(
        recovery.resolve_original_request(
            original.request,
            original.digest,
            producer_recovery_tests::recovery_limits(),
            grant(&f.budget, 3 * MIB, 0),
            grant(&f.budget, 40 * MIB, 0),
            || Ok(())
        ),
        Err(ProducerRecoveryError::ProducerOpen)
    ));
}
