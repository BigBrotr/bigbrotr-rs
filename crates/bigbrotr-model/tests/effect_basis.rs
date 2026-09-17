use bigbrotr_model::{
    AdmissionBatch, DatasetId, Digest256, EventEffectBasis, ExistingProof, FactRef, OwnerEpoch,
    PolicyEpoch, ProducerEpoch, ProducerRequestId, ProducerSlotId, ProtectionClass,
    ProtectionContext, Receipt, RecoveryGenerationId, RequestSequence, Sequence,
    SourceDependencies, SourceRecordId, TopologyGeneration, effect::EffectError,
};

fn dataset() -> DatasetId {
    DatasetId::try_from([1; 16]).unwrap()
}
fn context() -> ProtectionContext {
    ProtectionContext {
        dataset_id: dataset(),
        topology_generation: TopologyGeneration::new(1),
        protection_class: ProtectionClass::LocalSynced,
        recovery_generation: RecoveryGenerationId::from_bytes([3; 16]),
        policy_epoch: PolicyEpoch::new(1),
    }
}
fn record(sequence: u64) -> FactRef {
    FactRef {
        source_record_id: SourceRecordId {
            dataset_id: dataset(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(sequence).unwrap(),
        },
        payload_digest: Digest256::from_bytes([sequence as u8; 32]),
    }
}
fn deps(records: Vec<FactRef>) -> SourceDependencies {
    SourceDependencies::new(dataset(), records).unwrap()
}
fn request() -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: ProducerSlotId::try_from([4; 16]).unwrap(),
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(1).unwrap(),
    }
}

#[test]
fn closure_rejects_empty_foreign_unordered_and_duplicate_id_even_with_changed_digest() {
    for records in [
        vec![],
        vec![record(2), record(1)],
        vec![record(1), record(1)],
    ] {
        assert!(SourceDependencies::new(dataset(), records).is_err());
    }
    let mut changed = record(1);
    changed.payload_digest = Digest256::from_bytes([9; 32]);
    assert_eq!(
        SourceDependencies::new(dataset(), vec![record(1), changed]),
        Err(EffectError::UnorderedDependencies)
    );
    changed.source_record_id.dataset_id = DatasetId::try_from([2; 16]).unwrap();
    assert_eq!(
        SourceDependencies::new(dataset(), vec![changed]),
        Err(EffectError::ForeignDataset)
    );
    let mut other_shard = record(1);
    other_shard.source_record_id.shard_id = u32::MAX;
    assert!(SourceDependencies::new(dataset(), vec![record(u64::MAX), other_shard]).is_ok());
    assert!(SourceDependencies::new(dataset(), vec![other_shard, record(u64::MAX)]).is_err());
}

#[test]
fn new_fact_receipt_requires_exact_batch_dependency_closure() {
    let make = |records| {
        Receipt::new_facts(
            request(),
            Digest256::from_bytes([8; 32]),
            context(),
            AdmissionBatch::new(vec![record(2), record(3)]).unwrap(),
            deps(records),
        )
    };
    assert!(make(vec![record(1), record(2), record(3)]).is_ok());
    assert_eq!(
        make(vec![record(1), record(2), record(3), record(4)]),
        Err(EffectError::FutureDependency)
    );
    let mut other_stream = record(u64::MAX);
    other_stream.source_record_id.shard_id = 1;
    assert!(make(vec![record(1), record(2), record(3), other_stream]).is_ok());
    assert_eq!(
        make(vec![record(1), record(3)]),
        Err(EffectError::MissingBatchDependency)
    );
    let mut changed = record(2);
    changed.payload_digest = Digest256::from_bytes([99; 32]);
    assert_eq!(
        make(vec![changed, record(3)]),
        Err(EffectError::ConflictingDependency)
    );
    let mut foreign = context();
    foreign.dataset_id = DatasetId::try_from([2; 16]).unwrap();
    assert_eq!(
        Receipt::already_durable(
            request(),
            Digest256::from_bytes([8; 32]),
            foreign,
            deps(vec![record(1)])
        ),
        Err(EffectError::ForeignDataset)
    );
}

#[test]
fn proof_uses_frozen_profile_and_never_allocates_a_mutation_request() {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../experiments/identity-contract/golden-vectors.json"
    ))
    .unwrap();
    let vector = |name| {
        vectors["vectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["name"] == name)
            .unwrap()
    };
    let mut fact = record(1);
    fact.payload_digest = vector("fact-eligibility")["sha256"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let intent = vector("certification-intent")["sha256"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let proof = ExistingProof::new(context(), intent, deps(vec![fact])).unwrap();
    assert_eq!(
        proof.digest().to_string(),
        vector("existing-proof")["sha256"]
    );
    let basis = EventEffectBasis::ExistingProof(proof);
    assert_eq!(basis.request_id(), None);
    assert_eq!(basis.dependencies().records(), &[fact]);
    assert_eq!(basis.protection_context(), &context());
    let receipt = Receipt::already_durable(request(), intent, context(), deps(vec![fact])).unwrap();
    assert!(receipt.batch().is_none());
    let basis = EventEffectBasis::Receipt(receipt);
    assert_eq!(basis.request_id(), Some(request()));
    assert_eq!(basis.dependencies().records(), &[fact]);
}

#[test]
fn proof_context_and_exact_dependency_digest_are_bound_and_lossless() {
    let intent = Digest256::from_bytes([9; 32]);
    let make = |ctx, intent, dep| {
        ExistingProof::new(ctx, intent, deps(vec![dep]))
            .unwrap()
            .digest()
    };
    let original = make(context(), intent, record(1));
    for changed in [
        ProtectionContext {
            topology_generation: TopologyGeneration::new(u64::MAX),
            ..context()
        },
        ProtectionContext {
            policy_epoch: PolicyEpoch::new(u64::MAX),
            ..context()
        },
        ProtectionContext {
            recovery_generation: RecoveryGenerationId::from_bytes([0; 16]),
            ..context()
        },
    ] {
        assert_ne!(make(changed, intent, record(1)), original);
    }
    assert_ne!(
        make(context(), Digest256::from_bytes([8; 32]), record(1)),
        original
    );
    let mut changed = record(1);
    changed.payload_digest = Digest256::from_bytes([8; 32]);
    assert_ne!(make(context(), intent, changed), original);
    assert_eq!(
        serde_json::to_string(&PolicyEpoch::new(u64::MAX)).unwrap(),
        "\"18446744073709551615\""
    );
    assert!(PolicyEpoch::new(u64::MAX).checked_add(1).is_err());
    assert!(serde_json::from_str::<PolicyEpoch>("18446744073709551615").is_err());
    assert!(serde_json::from_str::<ProtectionClass>("\"remote-replicated\"").is_err());
    let foreign = ProtectionContext {
        dataset_id: DatasetId::try_from([2; 16]).unwrap(),
        ..context()
    };
    assert_eq!(
        ExistingProof::new(foreign, intent, deps(vec![record(1)])),
        Err(EffectError::ForeignDataset)
    );
}
