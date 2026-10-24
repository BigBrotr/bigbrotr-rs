//! Native cursor components only. SQL rollback/typed-head revalidation and
//! connected delivery are independently required by the historical adapter.
use super::*;
use crate::storage::rocks::store::producer_recovery::RecoveryContext;
use crate::storage::rocks::store::{
    CompactedInitialSummary, EmptySuccessorRetirement, LineagePendingError,
    NativeSuccessorActivation, PendingReadLimits, SuccessorCandidate, SuccessorHandoff,
};
use bigbrotr_model::codec::FactDecodeLimits;
use std::sync::atomic::Ordering;

fn pending(cursor: &LineageResolution<'_>) {
    assert_eq!(cursor.current_epoch(), Some(1));
    assert!(cursor.needs_compacted_initial());
    assert_eq!(cursor.next_request(), None);
    assert_eq!(cursor.completed_epoch(), None);
}

fn complete<'a>(
    f: &Fixture,
    r: &'a IngestRecovery,
    lineage: &'a RetainedLineage,
    through: u64,
    kind: Option<PredecessorTerminalKind>,
) -> ResolvedLineage<'a> {
    let verified = resolve(f, &ancestor(f, r, lineage).unwrap()).unwrap();
    let mut cursor = lineage
        .compacted_resolution(r, through, kind, &NOT_CANCELLED)
        .unwrap();
    let summary = cursor
        .accept_compacted_initial(&verified, || Ok(()))
        .unwrap();
    assert_eq!(
        summary,
        CompactedInitialSummary {
            compacted_requests: 4,
            last_complete_sequence: 9,
            digest: f.closure
        }
    );
    while cursor.current_epoch().is_some() {
        while cursor.next_request().is_some() {
            cursor.record(&resolve_one(f, &cursor)).unwrap();
        }
        cursor.finish_epoch().unwrap();
    }
    cursor
        .finish(AdmissionLimits::default(), grant(&f.budget, 40 * MIB, 0))
        .unwrap()
}

#[test]
fn compacted_history_replays_later_originals_and_binds_initial_recorded_digest() {
    let (f, r) = fixture();
    for sequence in 1..=4 {
        std::fs::remove_file(
            f.path()
                .join("producer-inputs-v1")
                .join(input_name(1, sequence)),
        )
        .unwrap();
    }
    let before = kv(&r.store);
    let originals = inputs(&f.path());
    let sequence = r.store.db.latest_sequence_number();
    let lineage = inspect_compacted(&f, &r).unwrap();
    let verified = resolve(&f, &ancestor(&f, &r, &lineage).unwrap()).unwrap();
    let mut cursor = lineage
        .compacted_resolution(
            &r,
            3,
            Some(PredecessorTerminalKind::Activated),
            &NOT_CANCELLED,
        )
        .unwrap();
    pending(&cursor);
    let summary = cursor
        .accept_compacted_initial(&verified, || Ok(()))
        .unwrap();
    assert_eq!(summary.compacted_requests, 4);
    assert_eq!(summary.last_complete_sequence, 9);
    assert_eq!(summary.digest, f.closure);
    assert_eq!(cursor.current_epoch(), Some(2));
    assert!(!cursor.needs_compacted_initial());
    assert_eq!(cursor.finish_epoch().unwrap().resolved_requests, 0);
    for sequence in [1, 2] {
        assert_eq!(cursor.next_request(), Some(at(3, sequence)));
        cursor.record(&resolve_one(&f, &cursor)).unwrap();
    }
    let later = cursor.finish_epoch().unwrap();
    assert_eq!(later.resolved_requests, 2);
    assert_eq!(later.event_request_mask, 0);
    let proof = cursor
        .finish(AdmissionLimits::default(), grant(&f.budget, 40 * MIB, 0))
        .unwrap();
    assert_eq!(proof.compacted_initial_requests(), 4);
    assert_eq!(proof.verified_through(), 3);
    assert_eq!(proof.last_epoch(), later);
    let mut recorded: Vec<(u64, [u8; 32], [u8; 32])> = f
        .originals
        .iter()
        .enumerate()
        .map(|(index, original)| {
            (
                index as u64 + 1,
                Sha256::digest(&original.input).into(),
                Sha256::digest(&original.outcome).into(),
            )
        })
        .collect();
    assert!(proof.matches_recorded(1, recorded.iter().copied()));
    assert!(!proof.matches_recorded(1, []));
    recorded[0].1[0] ^= 1;
    assert!(!proof.matches_recorded(1, recorded));
    let limits = PendingReadLimits {
        max_records: 64,
        max_encoded_bytes: MIB as usize,
        fact: FactDecodeLimits::default(),
    };
    assert_eq!(
        proof
            .read_complete_pending_batch(
                fact_id(1, 1),
                limits,
                grant(&f.budget, limits.required_data_bytes().unwrap(), 0),
                &NOT_CANCELLED
            )
            .err(),
        Some(LineagePendingError::Proof(BootstrapError::StateMismatch))
    );
    assert_eq!(kv(&r.store), before);
    assert_eq!(inputs(&f.path()), originals);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
}

#[test]
fn compacted_history_pending_requires_its_exact_owner_and_final_authority() {
    let (f, r) = fixture();
    let lineage = inspect_compacted(&f, &r).unwrap();
    let same_state_other_owner = inspect_compacted(&f, &r).unwrap();
    let wrong = resolve(&f, &ancestor(&f, &r, &same_state_other_owner).unwrap()).unwrap();
    let verified = resolve(&f, &ancestor(&f, &r, &lineage).unwrap()).unwrap();
    let cancelled = AtomicBool::new(false);
    let mut cursor = lineage
        .compacted_resolution(&r, 3, Some(PredecessorTerminalKind::Activated), &cancelled)
        .unwrap();
    let before = kv(&r.store);
    let originals = inputs(&f.path());
    let sequence = r.store.db.latest_sequence_number();
    pending(&cursor);
    assert!(cursor.finish_epoch().is_err());
    assert!(
        cursor
            .resolve_next(
                input_digest(&f.originals[0]),
                recovery_limits(),
                grant(&f.budget, 3 * MIB, 0),
                grant(&f.budget, 40 * MIB, 0),
                || Ok(())
            )
            .is_err()
    );
    let later_input =
        std::fs::read(f.path().join("producer-inputs-v1").join(input_name(3, 1))).unwrap();
    let later = r
        .resolve_original_request_in(
            at(3, 1),
            Digest256::from_bytes(Sha256::digest(later_input).into()),
            recovery_limits(),
            grant(&f.budget, 3 * MIB, 0),
            grant(&f.budget, 40 * MIB, 0),
            || Ok(()),
            RecoveryContext::Lineage {
                lineage: &lineage,
                epoch: 3,
                cancelled: &cancelled,
            },
        )
        .unwrap();
    assert!(cursor.record(&later).is_err());
    drop(later);
    pending(&cursor);
    assert_eq!(
        cursor.accept_compacted_initial(&wrong, || Ok(())),
        Err(AdmissionError::StateMismatch)
    );
    pending(&cursor);
    assert_eq!(
        cursor.accept_compacted_initial(&verified, || Err(AdmissionError::AuthorityUnavailable)),
        Err(AdmissionError::AuthorityUnavailable)
    );
    pending(&cursor);
    cancelled.store(true, Ordering::Relaxed);
    assert_eq!(
        cursor.accept_compacted_initial(&verified, || Ok(())),
        Err(AdmissionError::AuthorityUnavailable)
    );
    pending(&cursor);
    cancelled.store(false, Ordering::Relaxed);
    assert_eq!(
        cursor.accept_compacted_initial(&verified, || {
            cancelled.store(true, Ordering::Relaxed);
            Ok(())
        }),
        Err(AdmissionError::AuthorityUnavailable)
    );
    pending(&cursor);
    cancelled.store(false, Ordering::Relaxed);
    let actual = cursor
        .accept_compacted_initial(&verified, || Ok(()))
        .unwrap();
    assert_eq!(actual.compacted_requests, 4);
    assert_eq!(cursor.current_epoch(), Some(2));
    assert!(
        cursor
            .accept_compacted_initial(&verified, || Ok(()))
            .is_err()
    );
    assert_eq!(cursor.current_epoch(), Some(2));
    assert_eq!(kv(&r.store), before);
    assert_eq!(inputs(&f.path()), originals);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
}

#[test]
fn compacted_history_refuses_stale_completion_and_original_only_construction() {
    for sidecar in [false, true] {
        let (f, r) = fixture();
        let lineage = inspect_compacted(&f, &r).unwrap();
        let verified = resolve(&f, &ancestor(&f, &r, &lineage).unwrap()).unwrap();
        let mut cursor = lineage
            .compacted_resolution(
                &r,
                3,
                Some(PredecessorTerminalKind::Activated),
                &NOT_CANCELLED,
            )
            .unwrap();
        if sidecar {
            let path = f.path().join("producer-inputs-v1").join(input_name(3, 1));
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[0] ^= 1;
            std::fs::write(path, bytes).unwrap();
        } else {
            put(&r.store, producer_key(3), &marker(true, 2));
        }
        let before = kv(&r.store);
        let originals = inputs(&f.path());
        assert!(
            cursor
                .accept_compacted_initial(&verified, || Ok(()))
                .is_err()
        );
        pending(&cursor);
        assert_eq!(kv(&r.store), before);
        assert_eq!(inputs(&f.path()), originals);
    }
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    let lineage = inspect(&f, &r, None).unwrap();
    assert!(
        lineage
            .compacted_resolution(&r, 2, None, &NOT_CANCELLED)
            .is_err()
    );
    let proof = replay(&f, &r, &lineage, 2, None).unwrap();
    assert_eq!(proof.compacted_initial_requests(), 0);
}

#[test]
fn compacted_history_cannot_mint_handoff_gap_empty_or_activation_capabilities() {
    let f = prepared_fixture();
    let r = f.reopen();
    put(&r.store, producer_key(2), &marker(true, 0));
    let candidate = SuccessorCandidate::new(r).unwrap();
    let old = inspect(&f, candidate.recovery(), Some(3)).unwrap();
    // A genuine original-only plan supplies portable target bytes for the gap
    // refusal. It cannot acquire a new compacted native witness by being reused.
    let proof = replay(
        &f,
        candidate.recovery(),
        &old,
        2,
        Some(PredecessorTerminalKind::UnactivatedEmpty),
    )
    .unwrap();
    let record = SuccessionRecord::new(SuccessionFields {
        terminal_kind: PredecessorTerminalKind::UnactivatedEmpty,
        producer_slot_id: old.producer_slot_id(),
        predecessor_epoch: ProducerEpoch::new(2).unwrap(),
        issued_through: 0,
        last_complete_sequence: 0,
        closure_digest: proof.last_epoch().digest,
        incoming_handoff_sha256: old.native_head().incoming_handoff_sha256(),
        previous_manifest: *candidate.recovery().manifest(),
        next_manifest: later_manifest(*candidate.recovery().manifest(), 3),
    })
    .unwrap();
    let old_plan = SuccessorHandoff::reconstruct_gap(&proof, record, &NOT_CANCELLED).unwrap();
    drop(proof);
    drop(old);
    install_floor(&f, candidate.recovery());
    let lineage = RetainedLineage::inspect_compacted_initial(
        candidate.recovery(),
        Some(3),
        grant(&f.budget, 256 * 1024, 0),
        &NOT_CANCELLED,
    )
    .unwrap();
    let positive = complete(
        &f,
        candidate.recovery(),
        &lineage,
        2,
        Some(PredecessorTerminalKind::UnactivatedEmpty),
    );
    assert!(SuccessorHandoff::reconstruct_gap(&positive, record, &NOT_CANCELLED).is_err());
    drop(positive);
    let local = inspect_compacted(&f, candidate.recovery()).unwrap();
    let positive = complete(
        &f,
        candidate.recovery(),
        &local,
        2,
        Some(PredecessorTerminalKind::UnactivatedEmpty),
    );
    assert!(
        candidate
            .handoff(&positive, PolicyEpoch::new(7), &NOT_CANCELLED)
            .is_err()
    );
    drop(positive);
    drop(local);
    let activation = NativeSuccessorActivation::new(candidate, lineage, old_plan);
    assert!(activation.require_prepared(&NOT_CANCELLED).is_err());

    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    install_floor(&f, &r);
    let before = kv(&r.store);
    let lineage = inspect_compacted(&f, &r).unwrap();
    let proof = complete(&f, &r, &lineage, 2, None);
    assert!(EmptySuccessorRetirement::from_lineage(&proof, &NOT_CANCELLED).is_err());
    assert_eq!(kv(&r.store), before);
}

#[test]
fn compacted_history_reuses_one_permit_for_full_census_and_canonical_rows() {
    let (f, r) = fixture();
    let lineage = inspect_compacted(&f, &r).unwrap();
    let needed =
        crate::storage::rocks::store::compacted_outcome_data_bytes(AdmissionLimits::default())
            .unwrap();
    let permit = grant(&f.budget, needed, 0);
    let used = f.budget.snapshot().used().data_bytes();
    let count = f.budget.snapshot().active_permits();
    let ancestor = lineage
        .compacted_initial_ancestor_with_permit(
            &r,
            AdmissionLimits::default(),
            &permit,
            &NOT_CANCELLED,
        )
        .unwrap()
        .unwrap();
    let mut cursor = ancestor.resolution(&NOT_CANCELLED).unwrap();
    for index in 0..4 {
        cursor
            .record(
                row(&f, index),
                AdmissionLimits::default(),
                &permit,
                &NOT_CANCELLED,
                || Ok(()),
            )
            .unwrap();
    }
    cursor.finish(&NOT_CANCELLED).unwrap();
    assert_eq!(f.budget.snapshot().used().data_bytes(), used);
    assert_eq!(f.budget.snapshot().active_permits(), count);
}

fn correction_chain(disconnected: bool) -> (Fixture, IngestRecovery) {
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    let old = Receipt::decode_persistent(
        &f.originals[3].outcome,
        bigbrotr_model::receipt_codec::ReceiptDecodeLimits {
            max_encoded_bytes: 8192,
            max_allocation_bytes: 8192,
            max_batch_records: 64,
            max_dependencies: 64,
        },
    )
    .unwrap();
    let mut previous = *old
        .dependencies()
        .records()
        .iter()
        .find(|reference| reference.source_record_id == fact_id(1, 9))
        .unwrap();
    for sequence in 1..=64 {
        let time = 10_000_000 - sequence as i64;
        let input = intent("E_private", 2, time);
        let fact = Fact::new(
            fact_id(3, sequence),
            reference(DictionaryKind::VisibilityScope, 2),
            ObservedTime::new(77),
            FactPayload::EventReceive(EventReceive {
                event_id: fixture_event("E_private").id,
                received_at: ObservedTime::new(time),
                observer_id: reference(DictionaryKind::Observer, 1),
                clock_quality: ClockQuality::new(
                    reference(DictionaryKind::Observer, 1),
                    DurationUs::new(1),
                    None,
                    0,
                )
                .unwrap(),
                corrects: Some(if disconnected && sequence == 32 {
                    fact_id(1, 9)
                } else {
                    previous.source_record_id
                }),
            }),
        )
        .unwrap();
        let mut bytes = vec![1, 0x20];
        bytes.extend(fact.payload_digest().as_bytes());
        bytes.extend(fact.encode_preimage(8192).unwrap());
        put(
            &r.store,
            IngestKey::Fact {
                source_record_id: fact.source_record_id(),
            },
            &bytes,
        );
        let mut pointer = vec![1, 8];
        pointer.extend(
            &IngestKey::Fact {
                source_record_id: fact.source_record_id(),
            }
            .encode()
            .unwrap()
            .as_bytes()[2..],
        );
        pointer.extend(fact.payload_digest().as_bytes());
        put(
            &r.store,
            IngestKey::EventReceive {
                dataset_id: f.previous.identity().dataset_id,
                event_id: fixture_event("E_private").id,
                visibility_scope: reference(DictionaryKind::VisibilityScope, 2),
            },
            &pointer,
        );
        let batch = AdmissionBatch::new(vec![fact.fact_ref()]).unwrap();
        let mut dependencies = old.dependencies().records().to_vec();
        if sequence > 1 {
            dependencies.push(previous);
        }
        dependencies.push(fact.fact_ref());
        let id = at(3, sequence);
        let receipt = Receipt::new_facts(
            id,
            input.request_digest(id),
            context(*r.manifest()),
            batch.clone(),
            SourceDependencies::new(f.previous.identity().dataset_id, dependencies).unwrap(),
        )
        .unwrap();
        put(
            &r.store,
            IngestKey::Request { request_id: id },
            &receipt.encode_persistent(8192).unwrap(),
        );
        put(
            &r.store,
            IngestKey::PendingBatch {
                first: fact.source_record_id(),
            },
            &PendingBatchHeader::from_batch(&batch).encode(),
        );
        stored_input(
            &f,
            3,
            sequence,
            &input.encode_request_preimage(id, 8192).unwrap(),
        );
        previous = fact.fact_ref();
    }
    let mut source = vec![1, 2];
    source.extend(64_u64.to_be_bytes());
    put(&r.store, source_key(3), &source);
    put(&r.store, producer_key(3), &marker(true, 64));
    install_floor(&f, &r);
    for sequence in 1..=4 {
        std::fs::remove_file(
            f.path()
                .join("producer-inputs-v1")
                .join(input_name(1, sequence)),
        )
        .unwrap();
    }
    (f, r)
}

#[test]
fn compacted_history_correction_census_counts_reclaimed_requests_beyond_spool_cap() {
    for disconnected in [false, true] {
        let (f, r) = correction_chain(disconnected);
        let before = kv(&r.store);
        let originals = inputs(&f.path());
        assert_eq!(originals.len(), 64);
        let sequence = r.store.db.latest_sequence_number();
        let lineage = inspect_compacted(&f, &r).unwrap();
        assert_eq!(
            lineage
                .entries()
                .iter()
                .map(|entry| entry.issued_through())
                .sum::<u64>(),
            68
        );
        // 65 retained receive facts form one 64-edge chain. The old 64-iteration
        // loop could not check its final ancestor. Even the unchanged minimum
        // 8-dependency ReadBudget (160 reads) suffices for these bounded facts.
        let limits = AdmissionLimits {
            max_dependencies: 8,
            ..AdmissionLimits::default()
        };
        let result = lineage.compacted_initial_ancestor(
            &r,
            limits,
            grant(&f.budget, 40 * MIB, 0),
            &NOT_CANCELLED,
        );
        if disconnected {
            assert!(result.is_err());
        } else {
            resolve(&f, &result.unwrap().unwrap()).unwrap();
        }
        assert_eq!(kv(&r.store), before);
        assert_eq!(inputs(&f.path()), originals);
        assert_eq!(r.store.db.latest_sequence_number(), sequence);
    }
}

#[path = "compacted_successor_tests.rs"]
mod compacted_successor_tests;
