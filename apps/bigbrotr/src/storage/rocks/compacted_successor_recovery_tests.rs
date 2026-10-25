//! Native components after actual same-process activation and handle closure.
//! SQL activation, H/typed-C confirmation and committed copies remain explicit
//! component premises; installed qualification must establish them separately.
use super::*;
use crate::storage::rocks::store::{
    CompactedRecoveryPredecessor, NativeSuccessorRetirement, RecoveryAcknowledgement,
    ResolvedSuccessorRecovery, SuccessorRecoveryResolution,
};

fn fresh(residuals: usize, issued: bool, acknowledged: bool) -> (Fixture, IngestRecovery) {
    let (f, mut writer) = active(residuals);
    if issued {
        dictionaries(&f, &mut writer);
    }
    if acknowledged {
        assert!(issued);
        let target = AckTarget::new(
            CheckpointStamp::capture(writer.store()).unwrap(),
            at(4, 1).producer_slot_id,
            ProducerEpoch::new(4).unwrap(),
            1,
        );
        writer
            .test_acknowledge_target(target, &grant(&f.budget, 4096, 4096), || Ok(()))
            .unwrap();
    }
    let manifest = *writer.manifest();
    drop(writer);
    let recovery = IngestRecovery::open(
        &f.path(),
        manifest,
        f.native,
        grant(&f.budget, 0, 128 * MIB),
    )
    .unwrap();
    (f, recovery)
}

fn inspect(f: &Fixture, recovery: &IngestRecovery) -> NativeSuccessorRecovery {
    NativeSuccessorRecovery::inspect_with_compacted_initial(
        recovery,
        grant(&f.budget, 256 * 1024, 0),
        &NOT_CANCELLED,
    )
    .unwrap()
}

fn fresh_acknowledged_prefix() -> (Fixture, IngestRecovery) {
    let (f, mut writer) = active(0);
    dictionaries(&f, &mut writer);
    let target = AckTarget::new(
        CheckpointStamp::capture(writer.store()).unwrap(),
        at(4, 1).producer_slot_id,
        ProducerEpoch::new(4).unwrap(),
        1,
    );
    writer
        .test_acknowledge_target(target, &grant(&f.budget, 4096, 4096), || Ok(()))
        .unwrap();
    for (sequence, name) in [(2, "E_private"), (3, "B_note")] {
        let event = PreparedEvent::new(
            at(4, sequence),
            intent(name, 2, 10_000_000),
            256 * 1024,
            grant(&f.budget, MIB, 0),
        )
        .unwrap();
        writer
            .admit_checkpoint_event(
                event,
                ObservedTime::new(77),
                false,
                AdmissionLimits::default(),
                grant(&f.budget, 40 * MIB, 10 * MIB),
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
    }
    let manifest = *writer.manifest();
    drop(writer);
    let recovery = IngestRecovery::open(
        &f.path(),
        manifest,
        f.native,
        grant(&f.budget, 0, 128 * MIB),
    )
    .unwrap();
    (f, recovery)
}

fn canonical<'a>(
    f: &Fixture,
    view: &CompactedRecoveryPredecessor<'a>,
) -> VerifiedCompactedPrefix<'a> {
    let effect = grant(&f.budget, 40 * MIB, 0);
    let mut cursor = view.resolution(&NOT_CANCELLED).unwrap();
    for n in 0..4 {
        cursor
            .record(
                row(f, n),
                AdmissionLimits::default(),
                &effect,
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
    }
    cursor.finish(&NOT_CANCELLED).unwrap()
}

fn complete_originals(f: &Fixture, cursor: &mut SuccessorRecoveryResolution<'_>) {
    while cursor.current_epoch().is_some() {
        while let Some(id) = cursor.next_request() {
            let bytes = std::fs::read(f.path().join("producer-inputs-v1").join(input_name(
                id.producer_epoch.get(),
                id.request_sequence.get(),
            )))
            .unwrap();
            let pending = cursor
                .preview_next(
                    Digest256::from_bytes(Sha256::digest(bytes).into()),
                    recovery_limits(),
                    grant(&f.budget, 3 * MIB, 0),
                    grant(&f.budget, 40 * MIB, 0),
                    || Ok(()),
                )
                .unwrap();
            // Connected code accepts only after its known SQL result.
            pending.accept(|| Ok(())).unwrap();
        }
        cursor.finish_epoch().unwrap();
    }
}

fn complete<'a>(
    f: &Fixture,
    recovery: &'a IngestRecovery,
    owner: &'a NativeSuccessorRecovery,
) -> ResolvedSuccessorRecovery<'a> {
    let effects = grant(&f.budget, 40 * MIB, 0);
    let view = owner
        .compacted_predecessor(
            recovery,
            AdmissionLimits::default(),
            &effects,
            &NOT_CANCELLED,
        )
        .unwrap();
    let proof = canonical(f, &view);
    let mut cursor = owner.resolution(recovery, &NOT_CANCELLED).unwrap();
    assert!(cursor.needs_compacted_initial());
    assert_eq!(
        cursor
            .accept_compacted_initial(&proof, || Ok(()))
            .unwrap()
            .compacted_requests,
        4
    );
    complete_originals(f, &mut cursor);
    cursor
        .finish(AdmissionLimits::default(), grant(&f.budget, 40 * MIB, 0))
        .unwrap()
}

#[test]
fn compacted_successor_recovery_closes_once_and_retries_without_replaying_floor_originals() {
    for (residuals, issued, acknowledged) in [(4, true, true), (1, true, false), (0, false, false)]
    {
        let (f, mut recovery) = fresh(residuals, issued, acknowledged);
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        let files = inputs(&f.path());
        let mut expected = before.clone();
        let mut closing = marker(true, u64::from(issued));
        closing[10..18].copy_from_slice(&u64::from(acknowledged).to_be_bytes());
        replace(&mut expected, producer_key(4), &closing);
        for retry in [false, true] {
            let mut owner = inspect(&f, &recovery);
            assert_eq!(owner.is_closing(), retry);
            let pointer = owner.entry(1).unwrap() as *const _;
            owner
                .close_producer(
                    &mut recovery,
                    grant(&f.budget, 4096, 4096),
                    &NOT_CANCELLED,
                    || Ok(()),
                )
                .unwrap();
            assert_eq!(owner.entry(1).unwrap() as *const _, pointer);
            let proof = complete(&f, &recovery, &owner);
            assert_eq!(proof.verified_through(), 4);
            assert_eq!(proof.last_epoch().resolved_requests, u64::from(issued));
            let ack = proof.recovery_acknowledgement(&NOT_CANCELLED).unwrap();
            assert_eq!(ack.compacted_requests(), 4);
            assert_eq!(ack.acknowledged(), u64::from(acknowledged));
            drop(owner);
            assert_eq!(kv(&recovery.store), expected);
            assert_eq!(recovery.store.db.latest_sequence_number(), sequence + 1);
            assert_eq!(inputs(&f.path()), files);
            if !retry {
                let manifest = *recovery.manifest();
                drop(recovery);
                recovery = IngestRecovery::open(
                    &f.path(),
                    manifest,
                    f.native,
                    grant(&f.budget, 0, 128 * MIB),
                )
                .unwrap();
            }
        }
    }
}

#[test]
fn compacted_successor_recovery_requires_each_later_original_even_with_all_residuals() {
    for missing in [(3, 1), (4, 1)] {
        let (f, recovery) = fresh(4, true, false);
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        std::fs::remove_file(
            f.path()
                .join("producer-inputs-v1")
                .join(input_name(missing.0, missing.1)),
        )
        .unwrap();
        assert!(matches!(
            NativeSuccessorRecovery::inspect_with_compacted_initial(
                &recovery,
                grant(&f.budget, 256 * 1024, 0),
                &NOT_CANCELLED
            ),
            Err(BootstrapError::RequestInput(RequestInputError::Missing))
        ));
        assert_eq!(kv(&recovery.store), before);
        assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    }
}

#[test]
fn compacted_successor_recovery_proof_binds_owner_cancel_grant_and_refuses_other_capabilities() {
    let (f, mut recovery) = fresh(0, true, false);
    assert!(
        NativeSuccessorRecovery::inspect(
            &recovery,
            grant(&f.budget, 256 * 1024, 0),
            &NOT_CANCELLED
        )
        .is_err()
    );
    let mut owner = inspect(&f, &recovery);
    owner
        .close_producer(
            &mut recovery,
            grant(&f.budget, 4096, 4096),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    assert!(
        NativeSuccessorRetirement::inspect(
            &recovery,
            grant(&f.budget, 256 * 1024, 0),
            &NOT_CANCELLED
        )
        .is_err()
    );
    assert!(
        owner
            .compacted_predecessor(
                &recovery,
                AdmissionLimits::default(),
                &grant(&f.budget, 4096, 0),
                &NOT_CANCELLED
            )
            .is_err()
    );
    let effects = grant(&f.budget, 40 * MIB, 0);
    let retained = AtomicBool::new(false);
    let view = owner
        .compacted_predecessor(&recovery, AdmissionLimits::default(), &effects, &retained)
        .unwrap();
    let verified = canonical(&f, &view);
    assert!(!verified.matches_lineage(owner.lineage()));
    assert!(verified.handoff_context(&NOT_CANCELLED).is_err());
    assert!(
        owner
            .lineage()
            .resolution(
                &recovery,
                4,
                Some(PredecessorTerminalKind::Activated),
                &NOT_CANCELLED
            )
            .is_err()
    );
    let mut other = inspect(&f, &recovery);
    other
        .observe_closed(
            &recovery,
            grant(&f.budget, 4096, 4096),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    let mut cursor = other.resolution(&recovery, &NOT_CANCELLED).unwrap();
    assert!(
        cursor
            .accept_compacted_initial(&verified, || Ok(()))
            .is_err()
    );
    assert!(
        cursor
            .accept_compacted_initial(&verified, || Ok(()))
            .is_err()
    );
    retained.store(true, Ordering::Relaxed);
    assert!(view.validate(&recovery, &AtomicBool::new(false)).is_err());
    assert!(view.resolution(&AtomicBool::new(false)).is_err());
    let retained_completion = AtomicBool::new(false);
    let view = owner
        .compacted_predecessor(
            &recovery,
            AdmissionLimits::default(),
            &effects,
            &retained_completion,
        )
        .unwrap();
    let verified = canonical(&f, &view);
    let mut cursor = owner.resolution(&recovery, &retained_completion).unwrap();
    cursor
        .accept_compacted_initial(&verified, || Ok(()))
        .unwrap();
    complete_originals(&f, &mut cursor);
    let complete = cursor
        .finish(AdmissionLimits::default(), grant(&f.budget, 40 * MIB, 0))
        .unwrap();
    retained_completion.store(true, Ordering::Relaxed);
    let replacement = AtomicBool::new(false);
    assert!(complete.validate(&replacement).is_err());
    assert!(complete.recovery_acknowledgement(&replacement).is_err());
}

#[test]
fn compacted_successor_recovery_authenticates_residuals_and_rejects_changed_canonical_digest() {
    for residuals in [4, 0] {
        let (f, recovery) = fresh(residuals, true, false);
        let owner = inspect(&f, &recovery);
        let effects = grant(&f.budget, 40 * MIB, 0);
        let view = owner
            .compacted_predecessor(
                &recovery,
                AdmissionLimits::default(),
                &effects,
                &NOT_CANCELLED,
            )
            .unwrap();
        let mut cursor = view.resolution(&NOT_CANCELLED).unwrap();
        let mut wrong = row(&f, 0);
        wrong.input_bytes += 1;
        let rejected = cursor.record(
            wrong,
            AdmissionLimits::default(),
            &effects,
            &NOT_CANCELLED,
            || Ok(()),
        );
        assert_eq!(rejected.is_err(), residuals != 0);
        if residuals == 0 {
            // Length itself is not a D field. Change canonical receipt identity
            // with no residual available: native receipt validation still refuses.
            let mut wrong = row(&f, 1);
            wrong.digest = Digest256::from_bytes([0xee; 32]);
            assert!(
                cursor
                    .record(
                        wrong,
                        AdmissionLimits::default(),
                        &effects,
                        &NOT_CANCELLED,
                        || Ok(())
                    )
                    .is_err()
            );
        }
        assert!(cursor.finish(&NOT_CANCELLED).is_err());
    }
    // Pure D: with optional originals absent, change both request digest and
    // its canonical receipt consistently, retaining every scientific Fact.
    let (f, recovery) = fresh(0, true, false);
    let owner = inspect(&f, &recovery);
    let effects = grant(&f.budget, 40 * MIB, 0);
    let view = owner
        .compacted_predecessor(
            &recovery,
            AdmissionLimits::default(),
            &effects,
            &NOT_CANCELLED,
        )
        .unwrap();
    let original = Receipt::decode_persistent(
        &f.originals[0].outcome,
        bigbrotr_model::receipt_codec::ReceiptDecodeLimits::default(),
    )
    .unwrap();
    let digest = Digest256::from_bytes([0xed; 32]);
    let changed = Receipt::new_facts(
        original.request_id(),
        digest,
        *original.protection_context(),
        original.batch().unwrap().clone(),
        original.dependencies().clone(),
    )
    .unwrap()
    .encode_persistent(65536)
    .unwrap();
    let mut cursor = view.resolution(&NOT_CANCELLED).unwrap();
    for n in 0..4 {
        let mut candidate = row(&f, n);
        if n == 0 {
            candidate.digest = digest;
            candidate.bytes = &changed;
        }
        cursor
            .record(
                candidate,
                AdmissionLimits::default(),
                &effects,
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
    }
    assert!(matches!(
        cursor.finish(&NOT_CANCELLED),
        Err(AdmissionError::StateMismatch)
    ));
}

#[test]
fn compacted_successor_recovery_budget_selector_keeps_ordinary_and_malformed_distinct() {
    let f = prepared_fixture();
    let ordinary = valid_empty_third(&f);
    let before = kv(&ordinary.store);
    let sequence = ordinary.store.db.latest_sequence_number();
    assert!(
        !ordinary
            .has_compacted_successor_ancestor(&NOT_CANCELLED)
            .unwrap()
    );
    assert_eq!(kv(&ordinary.store), before);
    assert_eq!(ordinary.store.db.latest_sequence_number(), sequence);
    drop(ordinary);
    let (f, recovery) = fresh(0, false, false);
    assert!(
        recovery
            .has_compacted_successor_ancestor(&NOT_CANCELLED)
            .unwrap()
    );
    assert!(
        recovery
            .has_compacted_successor_ancestor(&AtomicBool::new(true))
            .is_err()
    );
    let mut floor = floor_bytes(&f);
    floor[25] = 3; // fixed retired4->3 invariant failure; no ordinary fallback
    put(&recovery.store, producer_key(1), &floor);
    assert!(
        recovery
            .has_compacted_successor_ancestor(&NOT_CANCELLED)
            .is_err()
    );
}

#[test]
fn compacted_successor_recovery_stale_sequence_and_failed_final_close_poison_owner() {
    for stale in [true, false] {
        let (f, mut recovery) = fresh(0, false, false);
        let mut owner = inspect(&f, &recovery);
        let sequence = recovery.store.db.latest_sequence_number();
        let before = kv(&recovery.store);
        if stale {
            put(
                &recovery.store,
                source_key(4),
                &value(&recovery.store, source_key(4)),
            );
        }
        let mut calls = 0;
        let result = owner.close_producer(
            &mut recovery,
            grant(&f.budget, 4096, 4096),
            &NOT_CANCELLED,
            || {
                calls += 1;
                if !stale && calls == 3 {
                    Err(BootstrapError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err());
        assert_eq!(
            owner.validate(&recovery, &NOT_CANCELLED),
            Err(BootstrapError::AuthorityUnavailable)
        );
        assert!(owner.resolution(&recovery, &NOT_CANCELLED).is_err());
        let mut expected = before;
        if !stale {
            replace(&mut expected, producer_key(4), &marker(true, 0));
        }
        assert_eq!(kv(&recovery.store), expected);
        assert_eq!(recovery.store.db.latest_sequence_number(), sequence + 1);
    }
}

#[test]
fn compacted_successor_recovery_tail_is_stamped_but_cannot_complete_or_acknowledge() {
    let (f, mut recovery) = fresh(4, false, false);
    let scope = target_scope(&recovery);
    let input = EvidenceIntent::new(
        1,
        scope,
        vec![
            DictionaryEntry::new(
                scope,
                DictionaryDescriptor::Visibility {
                    class: label("staged-only"),
                },
            )
            .unwrap(),
        ],
        EvidenceIntentLimits {
            max_entries: 8,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap();
    stored_input(
        &f,
        4,
        1,
        &input.encode_request_preimage(at(4, 1), 8192).unwrap(),
    );
    let mut owner = inspect(&f, &recovery);
    assert_eq!(owner.staged_input().unwrap().request, at(4, 1));
    owner
        .validate_tail(
            &recovery,
            recovery_limits().input,
            grant(&f.budget, 3 * MIB, 0),
            &NOT_CANCELLED,
        )
        .unwrap();
    owner
        .close_producer(
            &mut recovery,
            grant(&f.budget, 4096, 4096),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    let effects = grant(&f.budget, 40 * MIB, 0);
    let view = owner
        .compacted_predecessor(
            &recovery,
            AdmissionLimits::default(),
            &effects,
            &NOT_CANCELLED,
        )
        .unwrap();
    let proof = canonical(&f, &view);
    let mut cursor = owner.resolution(&recovery, &NOT_CANCELLED).unwrap();
    cursor.accept_compacted_initial(&proof, || Ok(())).unwrap();
    complete_originals(&f, &mut cursor);
    assert!(matches!(
        cursor.finish(AdmissionLimits::default(), grant(&f.budget, 40 * MIB, 0)),
        Err(ProducerRecoveryError::Unresolved)
    ));
}

#[test]
fn compacted_successor_recovery_equal_ack_rechecks_inputs_grant_and_final_authority() {
    for fault in 0..5 {
        let (f, mut recovery) = fresh_acknowledged_prefix();
        let mut owner = inspect(&f, &recovery);
        owner
            .close_producer(
                &mut recovery,
                grant(&f.budget, 4096, 4096),
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
        let proof = complete(&f, &recovery, &owner);
        let ack = proof.recovery_acknowledgement(&NOT_CANCELLED).unwrap();
        assert_eq!((ack.issued(), ack.acknowledged()), (3, 1));
        // Typed C1 is an explicit component premise, not inferred from issued3.
        let target = ack.compacted_target(1).unwrap();
        drop(owner);
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        if fault == 1 {
            std::fs::remove_file(f.path().join("producer-inputs-v1").join(input_name(3, 1)))
                .unwrap();
        }
        let cancelled = AtomicBool::new(false);
        let mut calls = 0;
        let result = recovery.store.test_acknowledge_compacted_target(
            target,
            &grant(
                &f.budget,
                if fault == 2 {
                    4096
                } else {
                    RecoveryAcknowledgement::COMPACTED_REQUIRED_DATA_BYTES
                },
                4096,
            ),
            &cancelled,
            || {
                calls += 1;
                if fault == 3 && calls == 2 {
                    cancelled.store(true, Ordering::Relaxed);
                }
                if fault == 4 && calls == 3 {
                    Err(AdmissionError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result.is_ok(), fault == 0);
        assert_eq!(kv(&recovery.store), before);
        assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    }
}
