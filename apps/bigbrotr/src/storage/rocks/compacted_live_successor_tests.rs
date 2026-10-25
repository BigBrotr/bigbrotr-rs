//! Native mechanics with an explicit synthetic confirmed activation premise.
//! SQL canonical H/COMMIT, real workflows and delivery remain connected gates.
use super::*;
use crate::storage::rocks::store::{
    ActiveSuccessorWriter, AdmissionOutcome, CertificationLimits, CertificationOutcome,
    CompactedLiveSuccessor, PreparedCertification, RetainedLineageEntry, producer_ack::AckTarget,
    producer_checkpoint::CheckpointStamp,
};

fn prepared(
    residuals: usize,
) -> (
    Fixture,
    NativeSuccessorActivation,
    *const RetainedLineageEntry,
) {
    let (f, r) = fixture();
    for n in residuals + 1..=4 {
        std::fs::remove_file(
            f.path()
                .join("producer-inputs-v1")
                .join(input_name(1, n as u64)),
        )
        .unwrap();
    }
    let mut candidate = SuccessorCandidate::new(r).unwrap();
    let lineage = inspect_compacted(&f, candidate.recovery()).unwrap();
    let pointer = lineage.entries().as_ptr();
    let proof = complete(
        &f,
        candidate.recovery(),
        &lineage,
        3,
        Some(PredecessorTerminalKind::Activated),
    );
    let handoff = candidate
        .handoff_compacted(&proof, PolicyEpoch::new(7), &NOT_CANCELLED)
        .unwrap();
    drop(proof);
    candidate
        .apply_compacted_handoff(
            &lineage,
            &handoff,
            grant(&f.budget, 8192, 8192),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    (
        f,
        NativeSuccessorActivation::new(candidate, lineage, handoff),
        pointer,
    )
}
fn active(residuals: usize) -> (Fixture, ActiveSuccessorWriter) {
    let (f, mut owner, pointer) = prepared(residuals);
    assert!(owner.require_prepared(&NOT_CANCELLED).is_err());
    owner.require_prepared_compacted(&NOT_CANCELLED).unwrap();
    let rows = kv(&owner.recovery().unwrap().store);
    let sequence = owner.recovery().unwrap().store.db.latest_sequence_number();
    let used = f.budget.snapshot().used();
    let writer = owner.activate_compacted(&NOT_CANCELLED, || Ok(())).unwrap();
    assert!(owner.recovery().is_none());
    assert!(owner.activate_compacted(&NOT_CANCELLED, || Ok(())).is_err());
    assert_eq!(f.budget.snapshot().used(), used);
    assert_eq!(
        writer
            .store()
            .live_successor
            .as_ref()
            .unwrap()
            .entries_identity()
            .0,
        pointer
    );
    assert_eq!(kv(writer.store()), rows);
    assert_eq!(writer.store().db.latest_sequence_number(), sequence);
    (f, writer)
}
fn canonical(f: &Fixture, view: &CompactedLiveSuccessor<'_>, cancel: &AtomicBool) {
    let permit = grant(&f.budget, 40 * MIB, 0);
    let mut cursor = view.resolution(cancel).unwrap();
    for n in 0..4 {
        cursor
            .record(
                row(f, n),
                AdmissionLimits::default(),
                &permit,
                cancel,
                || Ok(()),
            )
            .unwrap();
    }
    let verified = cursor.finish(cancel).unwrap();
    verified.validate_live(cancel).unwrap();
    assert_eq!(verified.floor().fields().closure_digest, f.closure);
    assert!(InitialHandoff::from_compacted(&verified, f.next, cancel).is_err());
}
fn dictionaries(f: &Fixture, writer: &mut ActiveSuccessorWriter) {
    let scope = QualifiedRef {
        allocation_domain: writer
            .manifest()
            .allocation_namespaces()
            .domain(DictionaryKind::VisibilityScope)
            .unwrap(),
        ..reference(DictionaryKind::VisibilityScope, 1)
    };
    let input = EvidenceIntent::new(
        1,
        scope,
        vec![
            DictionaryEntry::new(
                scope,
                DictionaryDescriptor::Visibility {
                    class: label("live-compacted"),
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
    let limits = AdmissionLimits::default();
    let permit = grant(
        &f.budget,
        limits.required_dictionary_data_bytes().unwrap(),
        limits.required_native_bytes().unwrap(),
    );
    let rows = kv(writer.store());
    let sequence = writer.store().db.latest_sequence_number();
    let files = inputs(&f.path());
    writer
        .stage_dictionary_append(
            at(4, 1),
            &input,
            ObservedTime::new(77),
            limits,
            &permit,
            &NOT_CANCELLED,
        )
        .unwrap();
    assert_eq!(kv(writer.store()), rows);
    assert_eq!(writer.store().db.latest_sequence_number(), sequence);
    assert_eq!(inputs(&f.path()).len(), files.len() + 1);
    let result = writer
        .append_checkpoint_dictionaries(
            at(4, 1),
            &input,
            ObservedTime::new(77),
            limits,
            permit,
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    let receipt = result.receipt();
    let batch = receipt.batch().unwrap();
    assert_eq!(
        (
            batch.first().owner_epoch.get(),
            batch.first().sequence.get(),
            batch.last().sequence.get()
        ),
        (4, 1, 1)
    );
    let recorded = receipt.encode_persistent(8192).unwrap();
    drop(result);
    // One dictionary: fact/pending/index/reverse/allocation/source/request/state.
    assert_eq!(writer.store().db.latest_sequence_number(), sequence + 8);
    for row in rows.iter().filter(|row| {
        row.1 != source_key(4).encode().unwrap().as_bytes()
            && row.1 != producer_key(4).encode().unwrap().as_bytes()
    }) {
        assert!(kv(writer.store()).contains(row));
    }
    assert_eq!(value(writer.store(), producer_key(4)), marker(false, 1));
    assert_eq!(
        value(writer.store(), source_key(4)),
        [vec![1, 2], 1_u64.to_be_bytes().to_vec()].concat()
    );
    let before = kv(writer.store());
    let replay = writer
        .append_checkpoint_dictionaries(
            at(4, 1),
            &input,
            ObservedTime::new(999),
            limits,
            grant(
                &f.budget,
                limits.required_dictionary_data_bytes().unwrap(),
                limits.required_native_bytes().unwrap(),
            ),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    assert_eq!(replay.receipt().encode_persistent(8192).unwrap(), recorded);
    drop(replay);
    assert_eq!(kv(writer.store()), before);
    assert_eq!(writer.store().db.latest_sequence_number(), sequence + 8);
}

#[test]
fn compacted_live_promotes_one_allocation_with_full_partial_and_absent_residuals() {
    for residuals in [4, 1, 0] {
        let (f, mut writer) = active(residuals);
        assert_eq!(inputs(&f.path()).len(), residuals + 2);
        let effects = grant(&f.budget, 40 * MIB, 0);
        canonical(
            &f,
            &writer
                .compacted_view(AdmissionLimits::default(), &effects, &NOT_CANCELLED)
                .unwrap(),
            &NOT_CANCELLED,
        );
        dictionaries(&f, &mut writer);
        canonical(
            &f,
            &writer
                .compacted_view(AdmissionLimits::default(), &effects, &NOT_CANCELLED)
                .unwrap(),
            &NOT_CANCELLED,
        );
        assert_eq!(inputs(&f.path()).len(), residuals + 3);
    }
}

#[test]
fn compacted_live_required_original_cannot_be_masked_by_residual_and_failure_poison_persists() {
    for fault in 0..3 {
        let (f, writer) = active(1);
        let rows = kv(writer.store());
        let sequence = writer.store().db.latest_sequence_number();
        let spool = f.path().join("producer-inputs-v1");
        let path = spool.join(input_name(if fault == 1 { 1 } else { 3 }, 1));
        let original = std::fs::read(&path).unwrap();
        match fault {
            0 => std::fs::remove_file(&path).unwrap(),
            1 => {
                let mut changed = original.clone();
                changed[0] ^= 1;
                std::fs::write(&path, changed).unwrap();
            }
            _ => stored_input(&f, 4, 1, b"unowned staged target"),
        }
        assert!(writer.validate(&NOT_CANCELLED).is_err());
        if fault < 2 {
            std::fs::write(&path, original).unwrap();
        }
        assert_eq!(
            writer.validate(&NOT_CANCELLED),
            Err(AdmissionError::AuthorityUnavailable)
        );
        assert_eq!(kv(writer.store()), rows);
        assert_eq!(writer.store().db.latest_sequence_number(), sequence);
    }
}

#[test]
fn compacted_live_view_binds_grant_original_cancel_and_canonical_digest() {
    for fault in 0..3 {
        let (f, writer) = active(0);
        let rows = kv(writer.store());
        let effects = grant(&f.budget, if fault == 0 { 4096 } else { 40 * MIB }, 0);
        let retained = AtomicBool::new(false);
        if fault == 0 {
            assert!(
                writer
                    .compacted_view(AdmissionLimits::default(), &effects, &retained)
                    .is_err()
            );
        } else {
            let view = writer
                .compacted_view(AdmissionLimits::default(), &effects, &retained)
                .unwrap();
            if fault == 1 {
                retained.store(true, Ordering::Relaxed);
                assert!(view.validate(&AtomicBool::new(false)).is_err());
                assert!(view.resolution(&AtomicBool::new(false)).is_err());
            } else {
                let mut cursor = view.resolution(&NOT_CANCELLED).unwrap();
                let mut wrong = row(&f, 0);
                wrong.digest = Digest256::from_bytes([0xfe; 32]);
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
                // Refused row cannot advance or certify the original digest.
                assert!(cursor.finish(&NOT_CANCELLED).is_err());
            }
        }
        assert_eq!(kv(writer.store()), rows);
    }
}

#[test]
fn compacted_live_checkpoint_ack_preserves_inputs_and_poisons_failed_final_authority() {
    for failure in [false, true] {
        let (f, mut writer) = active(0);
        dictionaries(&f, &mut writer);
        let before = kv(writer.store());
        let sequence = writer.store().db.latest_sequence_number();
        let files = inputs(&f.path());
        let target = AckTarget::new(
            CheckpointStamp::capture(writer.store()).unwrap(),
            at(4, 1).producer_slot_id,
            ProducerEpoch::new(4).unwrap(),
            1,
        );
        let cancelled = AtomicBool::new(false);
        let mut calls = 0;
        let result = writer.test_acknowledge_target_cancelled(
            target,
            &grant(&f.budget, 4096, 4096),
            &cancelled,
            || {
                calls += 1;
                // Outer precheck, kernel precheck, one commit check, kernel
                // postcommit, then outer post-census authority.
                if failure && calls == 5 {
                    Err(AdmissionError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(calls, 5);
        assert_eq!(result.is_err(), failure);
        assert_eq!(writer.store().db.latest_sequence_number(), sequence + 1);
        let mut expected = marker(false, 1);
        expected[10..18].copy_from_slice(&1_u64.to_be_bytes());
        assert_eq!(value(writer.store(), producer_key(4)), expected);
        for row in before
            .iter()
            .filter(|row| row.1 != producer_key(4).encode().unwrap().as_bytes())
        {
            assert!(kv(writer.store()).contains(row));
        }
        assert_eq!(inputs(&f.path()), files);
        if failure {
            assert_eq!(
                writer.validate(&NOT_CANCELLED),
                Err(AdmissionError::AuthorityUnavailable)
            );
        } else {
            writer.validate(&NOT_CANCELLED).unwrap();
            assert_eq!(writer.producer_credit().unwrap().outstanding_requests(), 0);
            let target = AckTarget::new(
                CheckpointStamp::capture(writer.store()).unwrap(),
                at(4, 1).producer_slot_id,
                ProducerEpoch::new(4).unwrap(),
                1,
            );
            writer
                .test_acknowledge_target(target, &grant(&f.budget, 4096, 4096), || Ok(()))
                .unwrap();
            assert_eq!(writer.store().db.latest_sequence_number(), sequence + 1);
        }
    }
}

#[test]
fn compacted_live_reads_and_event_replay_keep_complete_science_but_not_fresh_adoption() {
    let (f, mut writer) = active(0);
    let sequence = writer.store().db.latest_sequence_number();
    let prepared = PreparedEvent::new(
        at(4, 1),
        intent("B_note", 2, 10_000_000),
        256 * 1024,
        grant(&f.budget, MIB, 0),
    )
    .unwrap();
    let result = writer
        .admit_checkpoint_event(
            prepared,
            ObservedTime::new(77),
            false,
            AdmissionLimits::default(),
            grant(&f.budget, 40 * MIB, 10 * MIB),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    assert!(matches!(result.outcome(), AdmissionOutcome::Durable(_)));
    drop(result);
    assert!(writer.store().db.latest_sequence_number() > sequence);
    let rows = kv(writer.store());
    let sequence = writer.store().db.latest_sequence_number();
    let effects = grant(&f.budget, 40 * MIB, 0);
    canonical(
        &f,
        &writer
            .compacted_view(AdmissionLimits::default(), &effects, &NOT_CANCELLED)
            .unwrap(),
        &NOT_CANCELLED,
    );
    let limits = CertificationLimits::default();
    for name in ["E_private", "B_note"] {
        let event = PreparedCertification::new(
            intent(name, 2, 10_000_000),
            256 * 1024,
            grant(&f.budget, MIB, 0),
        )
        .unwrap();
        let result = writer
            .certify_checkpoint_event(
                event,
                false,
                limits,
                grant(&f.budget, limits.required_data_bytes().unwrap(), 0),
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
        assert!(matches!(
            result.outcome(),
            CertificationOutcome::Existing(_)
        ));
    }
    let visibility = writer
        .read_visibility(
            reference(DictionaryKind::VisibilityScope, 1),
            limits,
            grant(&f.budget, limits.required_data_bytes().unwrap(), 0),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        visibility.entry().reference(),
        reference(DictionaryKind::VisibilityScope, 1)
    );
    drop(visibility);
    assert_eq!(kv(writer.store()), rows);
    assert_eq!(writer.store().db.latest_sequence_number(), sequence);
    let manifest = *writer.manifest();
    drop(writer);
    let recovery = IngestRecovery::open(
        &f.path(),
        manifest,
        f.native,
        grant(&f.budget, 0, 128 * MIB),
    )
    .unwrap();
    assert!(
        RetainedLineage::inspect(
            &recovery,
            None,
            grant(&f.budget, 256 * 1024, 0),
            &NOT_CANCELLED
        )
        .is_err()
    );
    assert_eq!(kv(&recovery.store), rows);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
}

#[test]
fn compacted_live_ack_refuses_sidecar_drift_without_using_checkpoint_stamp_as_census() {
    let (f, mut writer) = active(0);
    dictionaries(&f, &mut writer);
    let target = AckTarget::new(
        CheckpointStamp::capture(writer.store()).unwrap(),
        at(4, 1).producer_slot_id,
        ProducerEpoch::new(4).unwrap(),
        1,
    );
    let sequence = writer.store().db.latest_sequence_number();
    let rows = kv(writer.store());
    std::fs::remove_file(f.path().join("producer-inputs-v1").join(input_name(3, 1))).unwrap();
    assert!(
        writer
            .test_acknowledge_target(target, &grant(&f.budget, 4096, 4096), || Ok(()))
            .is_err()
    );
    assert_eq!(kv(writer.store()), rows);
    assert_eq!(writer.store().db.latest_sequence_number(), sequence);
    assert_eq!(
        writer.validate(&NOT_CANCELLED),
        Err(AdmissionError::AuthorityUnavailable)
    );
}
