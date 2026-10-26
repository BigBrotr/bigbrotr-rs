use super::*;
use crate::storage::postgres::{
    IndexChunk, IndexPlan, IndexProvenance, IndexPublicationReceipt, IndexTarget,
};
use bigbrotr_model::{
    AdmissionBatch, CompleteThrough, DatasetId, FactRef, OwnerEpoch, OwnerResourceId,
    ProcessIncarnation, ProjectionGenerationId, Sequence, SourceRecordId, StreamId,
    TopologyGeneration,
};

fn binding() -> IndexBinding {
    IndexBinding::new(
        DatasetId::try_from([1; 16]).unwrap(),
        OwnerResourceId::try_from([2; 16]).unwrap(),
        ProjectionGenerationId::try_from([3; 16]).unwrap(),
        ProcessIncarnation::try_from([4; 16]).unwrap(),
        Digest256::from_bytes([5; 32]),
        Digest256::from_bytes([6; 32]),
        IndexProvenance::LocalFirstWitness,
    )
    .unwrap()
}
fn owner() -> IndexOwnerIdentity {
    IndexOwnerIdentity {
        resource: binding().resource(),
        epoch: OwnerEpoch::new(2).unwrap(),
        incarnation: ProcessIncarnation::try_from([8; 16]).unwrap(),
    }
}
fn cut(last: u64) -> SourceCut {
    SourceCut::new(
        binding().dataset(),
        TopologyGeneration::new(1),
        vec![StreamBound {
            stream: StreamId {
                shard_id: 0,
                owner_epoch: OwnerEpoch::new(1).unwrap(),
            },
            last_complete_sequence: CompleteThrough::new(last),
        }],
    )
    .unwrap()
}
fn plan(revision: u64, first: u64) -> IndexPlan {
    let batch = AdmissionBatch::new(vec![FactRef {
        source_record_id: SourceRecordId {
            dataset_id: binding().dataset(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(first).unwrap(),
        },
        payload_digest: Digest256::from_bytes([9; 32]),
    }])
    .unwrap();
    let chunks = std::array::from_fn(|i| {
        IndexChunk::new(
            IndexTarget::ALL[i],
            1,
            "all".into(),
            Digest256::from_bytes([20 + i as u8; 32]),
            u64::from(i == 0),
            Digest256::from_bytes([40 + i as u8; 32]),
        )
        .unwrap()
    });
    IndexPlan::new(
        binding(),
        revision,
        cut(first - 1),
        cut(first),
        batch,
        chunks,
    )
    .unwrap()
}
fn retained(revision: u64, first: u64, published: bool) -> RetainedIndexPlan {
    let plan = plan(revision, first);
    let publication = published.then(|| IndexPublicationReceipt {
        binding: binding(),
        plan_digest: plan.digest(),
        revision: revision + 1,
        cut: plan.after().clone(),
        family_counts: [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        publisher: owner(),
        engine_receipt_digest: Digest256::from_bytes([11; 32]),
        committed_at_us: 100,
        created: false,
    });
    RetainedIndexPlan { plan, publication }
}
fn state(last: &RetainedIndexPlan, pending: Option<&RetainedIndexPlan>) -> IndexRecoveryState {
    IndexRecoveryState {
        published_revision: last.publication.as_ref().unwrap().revision,
        published_plan: Some(last.plan.digest()),
        pending_plan: pending.map(|v| v.plan.digest()),
    }
}

#[test]
fn complete_prefix_and_pending_must_be_contiguous_and_exact() {
    // Synthetic metadata exercises only capture sequencing. No source, engine
    // observation, custody or private physical proof is manufactured here.
    let first = retained(0, 1, true);
    let second = retained(1, 2, true);
    let pending = retained(2, 3, false);
    let captured = state(&second, Some(&pending));
    for (completed, prior, next) in [
        (0, None, &first),
        (1, Some(first.plan.after()), &second),
        (2, Some(second.plan.after()), &pending),
    ] {
        assert_eq!(
            validate_step(&binding(), owner(), captured, completed, false, prior, next),
            Ok(())
        );
    }
    assert!(validate_step(&binding(), owner(), captured, 0, false, None, &second).is_err());
    assert!(
        validate_step(
            &binding(),
            owner(),
            captured,
            1,
            false,
            Some(&cut(0)),
            &second
        )
        .is_err()
    );
    assert!(
        validate_step(
            &binding(),
            owner(),
            captured,
            2,
            true,
            Some(second.plan.after()),
            &pending
        )
        .is_err()
    );
    let alternate = retained(2, 4, false);
    assert!(
        validate_step(
            &binding(),
            owner(),
            captured,
            2,
            false,
            Some(second.plan.after()),
            &alternate
        )
        .is_err()
    );
    let published_pending = retained(2, 3, true);
    assert!(
        validate_step(
            &binding(),
            owner(),
            captured,
            2,
            false,
            Some(second.plan.after()),
            &published_pending
        )
        .is_err()
    );
}

#[test]
fn receipt_mutations_and_unrelated_captured_head_refuse_before_dispatch() {
    let first = retained(0, 1, true);
    let captured = state(&first, None);
    for mutation in 0..9 {
        let mut changed = first.clone();
        let receipt = changed.publication.as_mut().unwrap();
        match mutation {
            0 => receipt.revision = 2,
            1 => receipt.plan_digest = Digest256::from_bytes([12; 32]),
            2 => receipt.cut = cut(0),
            3 => receipt.family_counts[9] = 1,
            4 => receipt.publisher.resource = OwnerResourceId::try_from([12; 16]).unwrap(),
            5 => receipt.publisher.epoch = OwnerEpoch::new(3).unwrap(),
            6 => receipt.publisher.incarnation = ProcessIncarnation::try_from([12; 16]).unwrap(),
            7 => changed.publication = None,
            8 => {
                receipt.binding = IndexBinding::new(
                    binding().dataset(),
                    binding().resource(),
                    ProjectionGenerationId::try_from([12; 16]).unwrap(),
                    binding().storage_incarnation(),
                    binding().table_identity_digest(),
                    binding().custody_binding_digest(),
                    binding().provenance(),
                )
                .unwrap()
            }
            _ => unreachable!(),
        }
        assert!(
            validate_step(&binding(), owner(), captured, 0, true, None, &changed).is_err(),
            "mutation {mutation}"
        );
    }
    let mut other = captured;
    other.published_plan = Some(Digest256::from_bytes([12; 32]));
    assert!(validate_step(&binding(), owner(), other, 0, true, None, &first).is_err());
    // An earlier immutable publisher is valid after a positively fenced rebind.
    let mut older = first;
    older.publication.as_mut().unwrap().publisher.epoch = OwnerEpoch::new(1).unwrap();
    assert_eq!(
        validate_step(&binding(), owner(), captured, 0, true, None, &older),
        Ok(())
    );
}

#[test]
fn empty_generation_has_only_the_exact_optional_first_pending_plan() {
    let pending = retained(0, 1, false);
    let captured = IndexRecoveryState {
        published_revision: 0,
        published_plan: None,
        pending_plan: Some(pending.plan.digest()),
    };
    assert_eq!(
        validate_step(&binding(), owner(), captured, 0, false, None, &pending),
        Ok(())
    );
    assert!(validate_step(&binding(), owner(), captured, 0, true, None, &pending).is_err());
    let absent = IndexRecoveryState {
        pending_plan: None,
        ..captured
    };
    assert!(validate_step(&binding(), owner(), absent, 0, false, None, &pending).is_err());
}

#[test]
fn recovery_observation_has_literal_nullable_field_encoding() {
    // Independently encoded Python/hashlib literals; these are only operational
    // fingerprints, never fabricated engine or ownership observations.
    let mut state = IndexRecoveryState {
        published_revision: 7,
        published_plan: Some(Digest256::from_bytes([70; 32])),
        pending_plan: Some(Digest256::from_bytes([71; 32])),
    };
    assert_eq!(
        Digest256::from_bytes(history_start(binding(), owner(), state).finalize().into())
            .to_string(),
        "e48190b4e70d0bcba26f99edf012f6ca1139836c3e713d64a9578d64dcb7a6d0"
    );
    state.pending_plan = None;
    assert_eq!(
        Digest256::from_bytes(history_start(binding(), owner(), state).finalize().into())
            .to_string(),
        "e03e6cc7ac907db7c11542087d1720e0490a9aa413c8bea9d05f4d7e59bdee46"
    );
}
