//! Finite logical admission controls. These exercise the actual native owner;
//! fixed revision/digests are explicit fixture policy, not SQL authority.
use super::super::{
    AdmissionError, AdmissionLimits, BootstrapError, BootstrapLimits, IngestStore,
    bootstrap::Prepared,
    bootstrap_tests::{budget, grant, intent, manifest, native, request, scope},
};
use super::*;
use crate::storage::rocks::keys::IngestKey;
use bigbrotr_model::*;

const MIB: u64 = 1024 * 1024;
fn policy(high: u64, low: u64, unit: u64) -> PendingQuotaPolicy {
    PendingQuotaPolicy::new(
        high,
        low,
        unit,
        1,
        Digest256::from_bytes([7; 32]),
        1,
        Digest256::from_bytes([8; 32]),
        Duration::from_secs(30),
    )
    .unwrap()
}
fn at(sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        request_sequence: RequestSequence::new(sequence).unwrap(),
        ..request()
    }
}
fn terms(sequence: u64, allowance: u64) -> PendingReservationRequest {
    PendingReservationRequest {
        request: at(sequence),
        digest: Digest256::from_bytes([sequence as u8; 32]),
        allowance_bytes: allowance,
    }
}
fn limits() -> BootstrapLimits {
    BootstrapLimits {
        max_mappings: 8,
        max_batch_bytes: 256 * 1024,
    }
}
fn bootstrap(
    store: &mut IngestStore,
    b: &crate::runtime::ResourceBudget,
) -> Result<(), BootstrapError> {
    store
        .bootstrap_dictionaries(
            request(),
            &intent("public"),
            ObservedTime::new(123),
            limits(),
            grant(b, 4 * MIB, MIB),
            || Ok(()),
        )
        .map(drop)
}
fn dictionary(ordinal: u64, class: &str) -> EvidenceIntent {
    let reference = QualifiedRef {
        ordinal: Ordinal::new(ordinal).unwrap(),
        ..scope()
    };
    EvidenceIntent::new(
        1,
        reference,
        vec![
            DictionaryEntry::new(
                reference,
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new(class.into()).unwrap(),
                },
            )
            .unwrap(),
        ],
        EvidenceIntentLimits {
            max_entries: 8,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap()
}
fn fixture() -> (
    tempfile::TempDir,
    IngestStore,
    crate::runtime::ResourceBudget,
) {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    (root, store, b)
}

#[test]
fn raw_owner_is_unfunded_and_refuses_every_original_and_new_native_admission() {
    let (root, mut store, b) = fixture();
    assert_eq!(store.db.latest_sequence_number(), 3);
    assert_eq!(store.pending_quota_status(), PendingQuotaStatus::Unfunded);
    let bytes = intent("public")
        .encode_request_preimage(request(), 8192)
        .unwrap();
    assert_eq!(
        store.retain_request_input(request(), &bytes),
        Err(super::super::RequestInputError::PendingQuota(
            PendingQuotaError::Unfunded
        ))
    );
    assert!(matches!(
        store.stage_initial_dictionaries(
            request(),
            &intent("public"),
            ObservedTime::new(123),
            limits(),
            &grant(&b, 4 * MIB, MIB)
        ),
        Err(BootstrapError::PendingQuota(PendingQuotaError::Unfunded))
    ));
    assert_eq!(
        bootstrap(&mut store, &b),
        Err(BootstrapError::PendingQuota(PendingQuotaError::Unfunded))
    );
    assert_eq!(store.db.latest_sequence_number(), 3);
    assert!(!root.path().join("db/producer-inputs-v1").exists());
    store
        .bind_pending_quota(policy(1024 * 1024, 512 * 1024, 256 * 1024))
        .unwrap();
    bootstrap(&mut store, &b).unwrap();
    // Frozen one-visibility fixture: full framed fact plus its complete marker.
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 306);
    assert_eq!(store.pending_reserved_bytes(), Ok(0));
}

#[test]
fn aggregate_reservations_equal_high_and_refusal_creates_no_partial_credit_or_original() {
    let (root, mut store, _b) = fixture();
    let accepted = policy(100, 50, 99);
    store.bind_pending_quota(accepted).unwrap();
    store
        .reserve_pending_requests(&[terms(1, 60), terms(2, 40)])
        .unwrap();
    assert_eq!(store.pending_reserved_bytes(), Ok(100));
    assert_eq!(
        store.reserve_pending_requests(&[terms(3, 1)]),
        Err(PendingQuotaError::Pressure)
    );
    store.bind_pending_quota(accepted).unwrap();
    assert_eq!(store.pending_quota_status(), PendingQuotaStatus::Pressure);
    assert_eq!(store.pending_reserved_bytes(), Ok(100));
    store.reserve_pending_requests(&[terms(1, 60)]).unwrap(); // held continuation is not charged twice
    assert_eq!(
        store.reserve_pending_requests(&[terms(1, 61)]),
        Err(PendingQuotaError::InsufficientAllowance)
    );
    let mut changed = terms(1, 60);
    changed.digest = Digest256::from_bytes([55; 32]);
    assert_eq!(
        store.reserve_pending_requests(&[changed]),
        Err(PendingQuotaError::Conflict)
    );
    assert_eq!(
        store.bind_pending_quota(policy(101, 50, 99)),
        Err(PendingQuotaError::PolicyMismatch)
    );
    assert!(!root.path().join("db/producer-inputs-v1").exists());
    assert_eq!(store.db.latest_sequence_number(), 3);

    let (_root, mut store, _b) = fixture();
    store.bind_pending_quota(accepted).unwrap();
    assert_eq!(
        store.reserve_pending_requests(&[terms(1, 60), terms(2, 60)]),
        Err(PendingQuotaError::Pressure)
    );
    assert_eq!(store.pending_reserved_bytes(), Ok(0));
    store.reserve_pending_requests(&[terms(1, 0)]).unwrap();
    assert_eq!(store.pending_reserved_bytes(), Ok(0));
    assert_eq!(
        store.reserve_pending_requests(&[terms(65, 0)]),
        Err(PendingQuotaError::Limit)
    );
}

#[test]
fn literal_bootstrap_dictionary_charges_reach_high_then_no_effect_survives_pressure() {
    let (root, mut store, b) = fixture();
    store
        .bind_pending_quota(policy(1024 * 1024, 512 * 1024, 256 * 1024))
        .unwrap();
    bootstrap(&mut store, &b).unwrap();
    let admission = AdmissionLimits {
        max_batch_bytes: 256 * 1024,
        ..AdmissionLimits::default()
    };
    drop(
        store
            .append_dictionaries(
                at(2),
                &dictionary(2, "private"),
                ObservedTime::new(20),
                admission,
                grant(&b, 4 * MIB, MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    // Extra class byte and one fewer recorded_at byte cancel exactly.
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 612);
    assert_eq!(store.pending_reserved_bytes(), Ok(0));
    // Both genuine writes were funded by their conservative operation maxima.
    // With no unresolved requests, the native fixture can now prove the smaller
    // literal policy. Exercise 612+1>612 as ledger arithmetic, rather than claim
    // that a 256KiB operation is supported by a 306-byte maximum unit.
    store
        .bind_reconciled_pending_quota(policy(612, 100, 306))
        .unwrap();
    let before = store.db.latest_sequence_number();
    assert_eq!(
        store.reserve_pending_requests(&[terms(3, 1)]),
        Err(PendingQuotaError::Pressure)
    );
    assert_eq!(store.db.latest_sequence_number(), before);
    assert_eq!(
        std::fs::read_dir(root.path().join("db/producer-inputs-v1"))
            .unwrap()
            .count(),
        2
    );
    // A new AlreadyDurable request still reserves zero and writes its own result.
    drop(
        store
            .append_dictionaries(
                at(3),
                &dictionary(2, "private"),
                ObservedTime::new(999),
                admission,
                grant(&b, 4 * MIB, MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 612);
    assert_eq!(store.pending_quota_status(), PendingQuotaStatus::Pressure);
    assert_eq!(store.pending_reserved_bytes(), Ok(0));
    bootstrap(&mut store, &b).unwrap(); // read-only replay never needs positive credit
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 612);
}

#[test]
fn rebound_high_below_retained_p_permits_funded_replay_and_new_zero_p_outcome() {
    use super::super::admission_tests::kv;
    let (root, mut store, b) = fixture();
    store
        .bind_pending_quota(policy(1024 * 1024, 512 * 1024, 256 * 1024))
        .unwrap();
    bootstrap(&mut store, &b).unwrap();
    let admission = AdmissionLimits {
        max_batch_bytes: 256 * 1024,
        ..AdmissionLimits::default()
    };
    let private = dictionary(2, "private");
    let original = store
        .append_dictionaries(
            at(2),
            &private,
            ObservedTime::new(20),
            admission,
            grant(&b, 4 * MIB, MIB),
            || Ok(()),
        )
        .unwrap();
    let dependencies = original.receipt().batch().unwrap().records().to_vec();
    drop(original);
    // The two independent fixed visibility charges are306+306, as above.
    let accounting = store.pending_accounting().unwrap();
    assert_eq!(accounting.pending_bytes(), 612);
    assert_eq!(store.pending_reserved_bytes(), Ok(0));
    // The complete native fixture has no unresolved request. This private
    // reconciliation supplies the lowered-policy premise, never SQL authority.
    let rebound = PendingQuotaPolicy::new(
        400,
        200,
        399,
        2,
        Digest256::from_bytes([9; 32]),
        2,
        Digest256::from_bytes([10; 32]),
        Duration::from_secs(30),
    )
    .unwrap();
    store.bind_reconciled_pending_quota(rebound).unwrap();
    assert_eq!(store.pending_quota_status(), PendingQuotaStatus::Pressure);

    let before_replay = kv(&store);
    let replay_sequence = store.db.latest_sequence_number();
    let permit = grant(&b, 4 * MIB, MIB);
    let replay = store
        .bootstrap_pending_reservation(
            request(),
            &intent("public"),
            ObservedTime::new(123),
            limits(),
            &permit,
        )
        .unwrap();
    assert_eq!(replay.allowance_bytes, 0);
    store.reserve_pending_requests(&[replay]).unwrap();
    store
        .stage_initial_dictionaries(
            request(),
            &intent("public"),
            ObservedTime::new(123),
            limits(),
            &permit,
        )
        .unwrap();
    drop(permit);
    bootstrap(&mut store, &b).unwrap();
    assert_eq!(store.db.latest_sequence_number(), replay_sequence);
    assert_eq!(kv(&store), before_replay);
    assert_eq!(store.pending_accounting().unwrap(), accounting);
    assert_eq!(store.pending_reserved_bytes(), Ok(0));

    let before_zero = kv(&store);
    let permit = grant(&b, 4 * MIB, MIB);
    let zero = store
        .dictionary_pending_reservation(at(3), &private, ObservedTime::new(999), admission, &permit)
        .unwrap();
    assert_eq!(zero.allowance_bytes, 0);
    store.reserve_pending_requests(&[zero]).unwrap();
    store
        .stage_dictionary_append(at(3), &private, ObservedTime::new(999), admission, &permit)
        .unwrap();
    let outcome = store
        .append_dictionaries(
            at(3),
            &private,
            ObservedTime::new(999),
            admission,
            permit,
            || Ok(()),
        )
        .unwrap();
    assert!(outcome.receipt().batch().is_none());
    assert_eq!(outcome.receipt().request_id(), at(3));
    assert_eq!(
        outcome.receipt().request_digest(),
        private.request_digest(at(3))
    );
    assert_eq!(outcome.receipt().dependencies().records(), dependencies);
    drop(outcome);
    assert_eq!(store.db.latest_sequence_number(), replay_sequence + 2);
    assert_eq!(store.pending_accounting().unwrap(), accounting);
    assert_eq!(store.pending_reserved_bytes(), Ok(0));
    assert_eq!(store.pending_quota_status(), PendingQuotaStatus::Pressure);

    // Only the new receipt and issued watermark may change; every charged
    // fact/marker and all previous immutable metadata remain byte-exact.
    let request_key = IngestKey::Request { request_id: at(3) }.encode().unwrap();
    let producer_key = IngestKey::ProducerState {
        producer_slot_id: request().producer_slot_id,
        producer_epoch: request().producer_epoch,
    }
    .encode()
    .unwrap();
    let after_zero = kv(&store);
    let mut preserved = after_zero.clone();
    preserved.retain(|(cf, key, _)| !(cf == "receipts" && key == request_key.as_bytes()));
    let mut expected = before_zero;
    expected
        .iter_mut()
        .find(|(cf, key, _)| cf == "receipts" && key == producer_key.as_bytes())
        .unwrap()
        .2 = [vec![1, 0x31], 3u64.to_be_bytes().to_vec(), vec![0; 16]].concat();
    assert_eq!(preserved, expected);
    assert_eq!(after_zero.len(), preserved.len() + 1);

    // A 306-byte logical unit is supported by the smaller 399-byte maximum, but
    // 612+306>400 still refuses before any input or native change. The existing
    // 256KiB operation profile is not misrepresented as a 306-byte reservation.
    assert_eq!(
        store.reserve_pending_requests(&[terms(4, 306)]),
        Err(PendingQuotaError::Pressure)
    );
    assert_eq!(kv(&store), after_zero);
    assert_eq!(store.db.latest_sequence_number(), replay_sequence + 2);
    assert_eq!(
        std::fs::read_dir(root.path().join("db/producer-inputs-v1"))
            .unwrap()
            .count(),
        3
    );
}

#[test]
fn undersized_allowance_refuses_before_original_and_cancellation_keeps_staged_debt() {
    let (root, mut store, b) = fixture();
    store
        .bind_pending_quota(policy(1024 * 1024, 512 * 1024, 256 * 1024))
        .unwrap();
    let digest = intent("public").request_digest(request());
    store
        .reserve_pending_requests(&[PendingReservationRequest {
            request: request(),
            digest,
            allowance_bytes: 0,
        }])
        .unwrap();
    assert_eq!(
        bootstrap(&mut store, &b),
        Err(BootstrapError::PendingQuota(
            PendingQuotaError::InsufficientAllowance
        ))
    );
    assert!(!root.path().join("db/producer-inputs-v1").exists());
    drop(store);

    let (_root, mut store, b) = fixture();
    let accepted = policy(1024 * 1024, 512 * 1024, 256 * 1024);
    store.bind_pending_quota(accepted).unwrap();
    let cancelled = store.bootstrap_dictionaries(
        request(),
        &intent("public"),
        ObservedTime::new(123),
        limits(),
        grant(&b, 4 * MIB, MIB),
        || Err(BootstrapError::AuthorityUnavailable),
    );
    assert!(matches!(
        cancelled,
        Err(BootstrapError::AuthorityUnavailable)
    ));
    assert_eq!(store.pending_reserved_bytes(), Ok(0));
    {
        let _stage = store
            .stage_initial_dictionaries(
                request(),
                &intent("public"),
                ObservedTime::new(123),
                limits(),
                &grant(&b, 4 * MIB, MIB),
            )
            .unwrap();
        // Ending this witness scope never returns the owner's retained credit.
    }
    assert_eq!(store.pending_reserved_bytes(), Ok(256 * 1024));
    let calls = std::cell::Cell::new(0);
    let result = store.bootstrap_dictionaries(
        request(),
        &intent("public"),
        ObservedTime::new(123),
        limits(),
        grant(&b, 4 * MIB, MIB),
        || {
            calls.set(calls.get() + 1);
            if calls.get() == 2 {
                Err(BootstrapError::AuthorityUnavailable)
            } else {
                Ok(())
            }
        },
    );
    assert!(matches!(result, Err(BootstrapError::AuthorityUnavailable)));
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 0);
    assert_eq!(store.pending_reserved_bytes(), Ok(256 * 1024));
    store.bind_pending_quota(accepted).unwrap();
    assert_eq!(store.pending_reserved_bytes(), Ok(256 * 1024));
    bootstrap(&mut store, &b).unwrap();
    assert_eq!(store.pending_reserved_bytes(), Ok(0));
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 306);
}

#[test]
fn staged_underfunded_continuation_refuses_without_enlarging_or_releasing_debt() {
    let (root, mut store, b) = fixture();
    store
        .bind_pending_quota(policy(1024 * 1024, 512 * 1024, 256 * 1024))
        .unwrap();
    let input = intent("public");
    let bytes = input.encode_request_preimage(request(), 8192).unwrap();
    // Deliberately insufficient held terms exercise refusal, not an alternative
    // native admission profile. The real prepared delta is the frozen 306 bytes.
    store
        .reserve_pending_requests(&[PendingReservationRequest {
            request: request(),
            digest: input.request_digest(request()),
            allowance_bytes: 305,
        }])
        .unwrap();
    store.retain_request_input(request(), &bytes).unwrap();
    assert_eq!(
        bootstrap(&mut store, &b),
        Err(BootstrapError::PendingQuota(
            PendingQuotaError::InsufficientAllowance
        ))
    );
    assert_eq!(store.pending_reserved_bytes(), Ok(305));
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 0);
    assert_eq!(store.db.latest_sequence_number(), 3);
    let path = root.path().join("db/producer-inputs-v1").join(format!(
        "{}-0000000000000001-0000000000000001.cbor",
        request().producer_slot_id
    ));
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[test]
fn held_bootstrap_keeps_original_credit_across_larger_bounds_and_changed_time() {
    let (root, mut store, b) = fixture();
    store
        .bind_pending_quota(policy(1024 * 1024, 512 * 1024, 512 * 1024))
        .unwrap();
    let input = intent("public");
    let first = grant(&b, 4 * MIB, MIB);
    store
        .stage_initial_dictionaries(request(), &input, ObservedTime::new(123), limits(), &first)
        .unwrap();
    drop(first);
    let path = root.path().join("db/producer-inputs-v1").join(format!(
        "{}-0000000000000001-0000000000000001.cbor",
        request().producer_slot_id
    ));
    let original = std::fs::read(&path).unwrap();
    assert_eq!(store.pending_reserved_bytes(), Ok(256 * 1024));
    let larger = BootstrapLimits {
        max_batch_bytes: 512 * 1024,
        ..limits()
    };
    let permit = grant(&b, 4 * MIB, 2 * MIB);
    let terms = store
        .bootstrap_pending_reservation(
            request(),
            &input,
            ObservedTime::new(i64::MAX),
            larger,
            &permit,
        )
        .unwrap();
    assert_eq!(terms.allowance_bytes, 256 * 1024);
    store
        .stage_initial_dictionaries(
            request(),
            &input,
            ObservedTime::new(i64::MAX),
            larger,
            &permit,
        )
        .unwrap();
    assert_eq!(store.pending_reserved_bytes(), Ok(256 * 1024));
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 0);
    assert_eq!(store.db.latest_sequence_number(), 3);
    assert_eq!(std::fs::read(&path).unwrap(), original);
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &input,
                ObservedTime::new(i64::MAX),
                larger,
                permit,
                || Ok(()),
            )
            .unwrap(),
    );
    assert_eq!(store.pending_reserved_bytes(), Ok(0));
    // The original123 has a 2-byte CBOR integer; MAX has 9. Only this one
    // dictionary fact changes width, so the frozen 306-byte charge grows by 7.
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 313);
    assert_eq!(std::fs::read(&path).unwrap(), original);
}

fn adversarial_prepared(store: &IngestStore) -> Prepared {
    let mut writes = Prepared::new(limits()).unwrap();
    let id = store.manifest.identity();
    let first = SourceRecordId {
        dataset_id: id.dataset_id,
        shard_id: id.shard_id,
        owner_epoch: id.owner_epoch,
        sequence: Sequence::new(1).unwrap(),
    };
    writes
        .put(
            IngestKey::Fact {
                source_record_id: first,
            },
            vec![0; 10],
        )
        .unwrap();
    writes
        .put(IngestKey::PendingBatch { first }, vec![0; 42])
        .unwrap();
    writes
        .put(
            IngestKey::SourceStream {
                dataset_id: id.dataset_id,
                shard_id: id.shard_id,
                owner_epoch: id.owner_epoch,
            },
            vec![1, 2, 0, 0, 0, 0, 0, 0, 0, 1],
        )
        .unwrap();
    writes
        .put(
            IngestKey::Request {
                request_id: request(),
            },
            Vec::new(),
        )
        .unwrap();
    store.seal_pending_accounting(&mut writes).unwrap();
    // The module under test mints the actual private funding, never a test bypass.
    writes
        .fund(store, request(), intent("public").request_digest(request()))
        .unwrap();
    writes
}

#[test]
fn exact_commit_recheck_rejects_foreign_and_undersized_grants_without_writes() {
    let (_a, mut first, _b) = fixture();
    let (_c, mut second, _d) = fixture();
    let accepted = policy(1000, 500, 500);
    let bytes = intent("public")
        .encode_request_preimage(request(), 8192)
        .unwrap();
    let digest = intent("public").request_digest(request());
    for store in [&mut first, &mut second] {
        store.bind_pending_quota(accepted).unwrap();
        store
            .reserve_pending_requests(&[PendingReservationRequest {
                request: request(),
                digest,
                allowance_bytes: 127,
            }])
            .unwrap();
        store.retain_request_input(request(), &bytes).unwrap();
    }
    let mut zero = Prepared::new(limits()).unwrap();
    zero.put(
        IngestKey::Request {
            request_id: request(),
        },
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        first.commit_prepared(zero, || Ok(())),
        Err(BootstrapError::PendingQuota(PendingQuotaError::Unfunded))
    );
    assert!(matches!(
        first.stage_producer_request(request(), Digest256::from_bytes([99; 32]), &bytes),
        Err(super::super::RequestInputError::Conflict)
    ));
    let writes = adversarial_prepared(&first);
    assert_eq!(
        second.commit_prepared(writes, || Ok(())),
        Err(BootstrapError::PendingQuota(PendingQuotaError::Conflict))
    );
    let writes = adversarial_prepared(&first);
    assert_eq!(
        first.commit_prepared(writes, || Ok(())),
        Err(BootstrapError::PendingQuota(
            PendingQuotaError::InsufficientAllowance
        ))
    );
    assert_eq!(first.db.latest_sequence_number(), 3);
    assert_eq!(second.db.latest_sequence_number(), 3);
    first.write_uncertain = true;
    assert_eq!(
        first.pending_reserved_bytes(),
        Err(PendingQuotaError::WriteUncertain)
    );
    assert!(first.reserve_pending_requests(&[terms(2, 1)]).is_err());
    let PendingQuota::Bound(quota) = &first.pending_quota else {
        panic!()
    };
    assert_eq!(quota.entries.len(), 1);
    assert_eq!(quota.entries[0].terms.allowance_bytes, 127); // unknown completion retained the original obligation
}

#[test]
fn reopened_original_without_receipt_stays_recovery_blocked_and_metadata_is_zero_growth() {
    let (root, mut store, b) = fixture();
    let accepted = policy(1024 * 1024, 512 * 1024, 256 * 1024);
    store.bind_pending_quota(accepted).unwrap();
    store
        .stage_initial_dictionaries(
            request(),
            &intent("public"),
            ObservedTime::new(123),
            limits(),
            &grant(&b, 4 * MIB, MIB),
        )
        .unwrap();
    drop(store);
    let mut store = IngestStore::reopen(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 0);
    assert_eq!(
        store.pending_quota_status(),
        PendingQuotaStatus::RecoveryBlocked
    );
    assert_eq!(
        store.bind_pending_quota(accepted),
        Err(PendingQuotaError::RecoveryBlocked)
    );
    assert_eq!(
        bootstrap(&mut store, &b),
        Err(BootstrapError::PendingQuota(
            PendingQuotaError::RecoveryBlocked
        ))
    );
    assert_eq!(
        std::fs::read_dir(root.path().join("db/producer-inputs-v1"))
            .unwrap()
            .count(),
        1
    );
    // Authorized closing metadata remains available, preserving the exact P.
    let mut recovery = super::super::IngestRecovery { store };
    recovery
        .close_producer(
            request().producer_slot_id,
            request().producer_epoch,
            grant(&b, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        recovery.store.pending_accounting().unwrap().pending_bytes(),
        0
    );
    assert_eq!(
        recovery.store.pending_quota_status(),
        PendingQuotaStatus::RecoveryBlocked
    );
}

#[test]
fn logical_pressure_needs_strict_low_and_a_complete_fresh_stable_interval() {
    let (_root, mut store, _b) = fixture();
    store.bind_pending_quota(policy(100, 50, 99)).unwrap();
    let PendingQuota::Bound(quota) = &mut store.pending_quota else {
        panic!()
    };
    quota.pressure = true;
    store.reserve_pending_requests(&[terms(1, 0)]).unwrap();
    let start = Instant::now();
    let PendingQuota::Bound(quota) = &mut store.pending_quota else {
        panic!()
    };
    quota.below_low_since = None;
    assert_eq!(
        store.observe_pending_quota_at(start),
        Ok(PendingQuotaStatus::Pressure)
    );
    assert_eq!(
        store.observe_pending_quota_at(start + Duration::from_secs(29)),
        Ok(PendingQuotaStatus::Pressure)
    );
    assert_eq!(
        store.observe_pending_quota_at(start + Duration::from_secs(30)),
        Ok(PendingQuotaStatus::Available)
    );
    let PendingQuota::Bound(quota) = &mut store.pending_quota else {
        panic!()
    };
    quota.pressure = true;
    quota.below_low_since = None;
    quota.entries[0].terms.allowance_bytes = 50;
    assert_eq!(
        store.observe_pending_quota_at(start + Duration::from_secs(100)),
        Ok(PendingQuotaStatus::Pressure)
    );
    let PendingQuota::Bound(quota) = &mut store.pending_quota else {
        panic!()
    };
    assert_eq!(quota.below_low_since, None); // equality never starts stability
    quota.entries[0].terms.allowance_bytes = 49;
    assert_eq!(
        store.observe_pending_quota_at(start + Duration::from_secs(101)),
        Ok(PendingQuotaStatus::Pressure)
    );
    store.bind_pending_quota(policy(100, 50, 99)).unwrap();
    assert_eq!(
        store.observe_pending_quota_at(start + Duration::from_secs(130)),
        Ok(PendingQuotaStatus::Pressure)
    );
    assert_eq!(
        store.observe_pending_quota_at(start + Duration::from_secs(131)),
        Ok(PendingQuotaStatus::Available)
    );
}

#[test]
fn invalid_policy_never_rounds_or_defaults_into_an_available_owner() {
    for (high, low, unit, revision, stable) in [
        (100, 0, 1, 1, 30),
        (100, 100, 1, 1, 30),
        (100, 50, 100, 1, 30),
        (100, 50, 0, 1, 30),
        (100, 50, 1, 0, 30),
        (100, 50, 1, 1, 0),
    ] {
        assert_eq!(
            PendingQuotaPolicy::new(
                high,
                low,
                unit,
                revision,
                Digest256::from_bytes([1; 32]),
                1,
                Digest256::from_bytes([2; 32]),
                Duration::from_secs(stable)
            ),
            Err(PendingQuotaError::InvalidPolicy)
        );
    }
}

#[test]
fn binding_requires_retained_owner_data_while_raw_reads_remain_available() {
    use crate::runtime::ResourceRequest;
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let permit = b
        .try_acquire(ResourceRequest::data(0, 1, 128 * MIB).unwrap())
        .unwrap();
    let mut store =
        IngestStore::initialize(&root.path().join("db"), manifest(), native(), permit).unwrap();
    assert_eq!(store.pending_accounting().unwrap().pending_bytes(), 0);
    assert_eq!(
        store.bind_pending_quota(policy(100, 50, 99)),
        Err(PendingQuotaError::InsufficientReservation)
    );
    assert_eq!(store.pending_quota_status(), PendingQuotaStatus::Unfunded);
    assert_eq!(store.db.latest_sequence_number(), 3);
}
