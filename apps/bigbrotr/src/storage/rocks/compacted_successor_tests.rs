//! Synthetic floor over retained real science. These components do not claim
//! SQL preparation, delivered COMMIT, activation or a reclaimed-input writer.
use super::*;
use crate::storage::rocks::store::CompactedSuccessorPredecessor;

fn plan(f: &Fixture, r: &IngestRecovery) -> (RetainedLineage, SuccessorHandoff) {
    let lineage = RetainedLineage::inspect_compacted_initial(
        r,
        Some(4),
        grant(&f.budget, 256 * 1024, 0),
        &NOT_CANCELLED,
    )
    .unwrap();
    let proof = complete(f, r, &lineage, 3, Some(PredecessorTerminalKind::Activated));
    let record = SuccessionRecord::new(SuccessionFields {
        terminal_kind: PredecessorTerminalKind::Activated,
        producer_slot_id: lineage.producer_slot_id(),
        predecessor_epoch: ProducerEpoch::new(3).unwrap(),
        issued_through: 2,
        last_complete_sequence: 1,
        closure_digest: proof.last_epoch().digest,
        incoming_handoff_sha256: lineage.native_head().incoming_handoff_sha256(),
        previous_manifest: *r.manifest(),
        next_manifest: later_manifest(*r.manifest(), 4),
    })
    .unwrap();
    assert!(SuccessorHandoff::reconstruct_gap(&proof, record, &NOT_CANCELLED).is_err());
    let plan = SuccessorHandoff::reconstruct_compacted_gap(&proof, record, &NOT_CANCELLED).unwrap();
    drop(proof);
    (lineage, plan)
}

fn replace(rows: &mut Rows, key: IngestKey, value: &[u8]) {
    let key = key.encode().unwrap();
    rows.retain(|(cf, bytes, _)| cf != key.column_family().name() || bytes != key.as_bytes());
    rows.push((
        key.column_family().name().into(),
        key.as_bytes().to_vec(),
        value.to_vec(),
    ));
    rows.sort();
}

fn checked(f: &Fixture, view: &CompactedSuccessorPredecessor<'_>) {
    let permit = grant(&f.budget, 40 * MIB, 0);
    let mut cursor = view.resolution(&NOT_CANCELLED).unwrap();
    for index in 0..4 {
        cursor
            .record(
                row(f, index),
                AdmissionLimits::default(),
                &permit,
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
    }
    let verified = cursor.finish(&NOT_CANCELLED).unwrap();
    assert_eq!(verified.floor().fields().closure_digest, f.closure);
    assert!(InitialHandoff::from_compacted(&verified, f.next, &NOT_CANCELLED).is_err());
}

#[test]
fn compacted_successor_gap_four_puts_then_empty_one_put_and_fresh_zero_retry() {
    let (f, mut r) = fixture();
    let (lineage, handoff) = plan(&f, &r);
    let sequence = r.store.db.latest_sequence_number();
    let originals = inputs(&f.path());
    let before = kv(&r.store);
    let effect = grant(&f.budget, 40 * MIB, 0);
    checked(
        &f,
        &CompactedSuccessorPredecessor::handoff(
            &lineage,
            &r,
            &handoff,
            AdmissionLimits::default(),
            &effect,
            &NOT_CANCELLED,
        )
        .unwrap(),
    );
    assert!(
        r.apply_successor_handoff(
            &lineage,
            &handoff,
            grant(&f.budget, 8192, 8192),
            &NOT_CANCELLED,
            || Ok(())
        )
        .is_err()
    );
    r.apply_compacted_successor_handoff(
        &lineage,
        &handoff,
        grant(&f.budget, 8192, 8192),
        &NOT_CANCELLED,
        || Ok(()),
    )
    .unwrap();
    let mut wanted = before;
    replace(
        &mut wanted,
        IngestKey::Manifest,
        handoff.record().fields().next_manifest.encode().as_bytes(),
    );
    replace(
        &mut wanted,
        handoff.record().key(),
        handoff.encode().as_bytes(),
    );
    replace(&mut wanted, source_key(4), &[1, 2, 0, 0, 0, 0, 0, 0, 0, 0]);
    replace(&mut wanted, producer_key(4), &marker(false, 0));
    assert_eq!(kv(&r.store), wanted);
    assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
    assert_eq!(inputs(&f.path()), originals);
    assert!(lineage.validate(&r, &NOT_CANCELLED).is_err());
    checked(
        &f,
        &CompactedSuccessorPredecessor::handoff(
            &lineage,
            &r,
            &handoff,
            AdmissionLimits::default(),
            &effect,
            &NOT_CANCELLED,
        )
        .unwrap(),
    );
    r.apply_compacted_successor_handoff(
        &lineage,
        &handoff,
        grant(&f.budget, 8192, 8192),
        &NOT_CANCELLED,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
    drop(lineage);
    for retry in [false, true] {
        let lineage = inspect_compacted(&f, &r).unwrap();
        let proof = complete(&f, &r, &lineage, 3, None);
        assert_eq!(proof.compacted_initial_requests(), 4);
        assert_eq!(proof.last_epoch().resolved_requests, 2);
        assert!(EmptySuccessorRetirement::from_lineage(&proof, &NOT_CANCELLED).is_err());
        let empty =
            EmptySuccessorRetirement::from_compacted_lineage(&proof, &NOT_CANCELLED).unwrap();
        assert_eq!(empty.initially_closing(), retry);
        assert_eq!(empty.closure_digest(), epoch_digest(&f, &r, 4, 0, 0, true));
        drop(proof);
        checked(
            &f,
            &CompactedSuccessorPredecessor::empty(
                &lineage,
                &r,
                &empty,
                AdmissionLimits::default(),
                &effect,
                &NOT_CANCELLED,
            )
            .unwrap(),
        );
        assert!(
            r.close_empty_successor(
                &lineage,
                &empty,
                grant(&f.budget, 4096, 4096),
                &NOT_CANCELLED,
                || Ok(())
            )
            .is_err()
        );
        r.close_compacted_empty_successor(
            &lineage,
            &empty,
            grant(&f.budget, 4096, 4096),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
        replace(&mut wanted, producer_key(4), &marker(true, 0));
        assert_eq!(kv(&r.store), wanted);
        assert_eq!(r.store.db.latest_sequence_number(), sequence + 5);
        assert_eq!(inputs(&f.path()), originals);
        checked(
            &f,
            &CompactedSuccessorPredecessor::empty(
                &lineage,
                &r,
                &empty,
                AdmissionLimits::default(),
                &effect,
                &NOT_CANCELLED,
            )
            .unwrap(),
        );
        drop(lineage);
        drop(r);
        r = f.reopen();
    }
}

#[test]
fn compacted_successor_view_binds_root_sequence_cancellation_and_effect_grant() {
    let (f, r) = fixture();
    let (lineage, handoff) = plan(&f, &r);
    let before = kv(&r.store);
    let sequence = r.store.db.latest_sequence_number();
    assert!(matches!(
        CompactedSuccessorPredecessor::handoff(
            &lineage,
            &r,
            &handoff,
            AdmissionLimits::default(),
            &grant(&f.budget, 4096, 0),
            &NOT_CANCELLED
        ),
        Err(AdmissionError::InsufficientReservation)
    ));
    let cancelled = AtomicBool::new(false);
    let effect = grant(&f.budget, 40 * MIB, 0);
    let view = CompactedSuccessorPredecessor::handoff(
        &lineage,
        &r,
        &handoff,
        AdmissionLimits::default(),
        &effect,
        &cancelled,
    )
    .unwrap();
    let (_foreign_fixture, foreign) = fixture();
    assert_eq!(kv(&foreign.store), before);
    assert_eq!(foreign.store.db.latest_sequence_number(), sequence);
    assert!(view.validate(&foreign, &NOT_CANCELLED).is_err());
    cancelled.store(true, Ordering::Relaxed);
    assert_eq!(
        view.validate(&r, &NOT_CANCELLED),
        Err(AdmissionError::AuthorityUnavailable)
    );
    assert!(view.resolution(&NOT_CANCELLED).is_err());
    cancelled.store(false, Ordering::Relaxed);
    put(&r.store, producer_key(3), &marker(true, 2)); // identical bytes, stale sequence
    assert_eq!(kv(&r.store), before);
    assert!(view.validate(&r, &NOT_CANCELLED).is_err());
}

#[test]
fn compacted_successor_residual_inputs_never_replace_required_later_originals() {
    for remaining in [4, 1, 0] {
        let (f, r) = fixture();
        for sequence in remaining + 1..=4 {
            std::fs::remove_file(
                f.path()
                    .join("producer-inputs-v1")
                    .join(input_name(1, sequence)),
            )
            .unwrap();
        }
        let (lineage, handoff) = plan(&f, &r);
        let effect = grant(&f.budget, 40 * MIB, 0);
        let view = CompactedSuccessorPredecessor::handoff(
            &lineage,
            &r,
            &handoff,
            AdmissionLimits::default(),
            &effect,
            &NOT_CANCELLED,
        )
        .unwrap();
        checked(&f, &view);
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        std::fs::remove_file(f.path().join("producer-inputs-v1").join(input_name(3, 1))).unwrap();
        // remaining=1 discriminates one valid residual masking one missing required input.
        assert!(view.validate(&r, &NOT_CANCELLED).is_err());
        assert!(
            RetainedLineage::inspect_with_compacted_initial(
                &r,
                Some(4),
                grant(&f.budget, 256 * 1024, 0),
                &NOT_CANCELLED
            )
            .is_err()
        );
        assert_eq!(kv(&r.store), before);
        assert_eq!(r.store.db.latest_sequence_number(), sequence);
    }
}

#[test]
fn compacted_successor_candidate_preserves_original_and_activation_refusals() {
    let (f, r) = fixture();
    let mut candidate = SuccessorCandidate::new(r).unwrap();
    let lineage = inspect_compacted(&f, candidate.recovery()).unwrap();
    let proof = complete(
        &f,
        candidate.recovery(),
        &lineage,
        3,
        Some(PredecessorTerminalKind::Activated),
    );
    assert!(
        candidate
            .handoff(&proof, PolicyEpoch::new(7), &NOT_CANCELLED)
            .is_err()
    );
    let handoff = candidate
        .handoff_compacted(&proof, PolicyEpoch::new(7), &NOT_CANCELLED)
        .unwrap();
    drop(proof);
    let sequence = candidate.recovery().store.db.latest_sequence_number();
    candidate
        .apply_compacted_handoff(
            &lineage,
            &handoff,
            grant(&f.budget, 8192, 8192),
            &NOT_CANCELLED,
            || Ok(()),
        )
        .unwrap();
    assert_eq!(
        candidate.recovery().store.db.latest_sequence_number(),
        sequence + 4
    );
    assert!(
        candidate
            .require_prepared_handoff(&lineage, &handoff, &NOT_CANCELLED)
            .is_err()
    );
    let activated = NativeSuccessorActivation::new(candidate, lineage, handoff);
    assert!(activated.require_prepared(&NOT_CANCELLED).is_err());
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    let lineage = RetainedLineage::inspect_with_compacted_initial(
        &r,
        None,
        grant(&f.budget, 256 * 1024, 0),
        &NOT_CANCELLED,
    )
    .unwrap();
    assert!(!lineage.has_compacted_initial());
    let proof = replay(&f, &r, &lineage, 2, None).unwrap();
    EmptySuccessorRetirement::from_lineage(&proof, &NOT_CANCELLED).unwrap();
    assert!(EmptySuccessorRetirement::from_compacted_lineage(&proof, &NOT_CANCELLED).is_err());
}

#[test]
fn compacted_successor_dispatch_failure_keeps_exact_completed_retry() {
    for fail_at in [1, 2, 3] {
        let (f, mut r) = fixture();
        let (lineage, handoff) = plan(&f, &r);
        let before = kv(&r.store);
        let sequence = r.store.db.latest_sequence_number();
        let mut calls = 0;
        assert!(
            r.apply_compacted_successor_handoff(
                &lineage,
                &handoff,
                grant(&f.budget, 8192, 8192),
                &NOT_CANCELLED,
                || {
                    calls += 1;
                    if calls == fail_at {
                        Err(BootstrapError::AuthorityUnavailable)
                    } else {
                        Ok(())
                    }
                }
            )
            .is_err()
        );
        if fail_at < 3 {
            assert_eq!(kv(&r.store), before);
            assert_eq!(r.store.db.latest_sequence_number(), sequence);
        } else {
            assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
            let completed = kv(&r.store);
            r.apply_compacted_successor_handoff(
                &lineage,
                &handoff,
                grant(&f.budget, 8192, 8192),
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
            assert_eq!(kv(&r.store), completed);
            assert_eq!(r.store.db.latest_sequence_number(), sequence + 4);
        }
    }
}
