use bigbrotr_model::{
    AllocationDomainId, ClockQuality, DatasetId, DictionaryKind, DurationUs, EventBody,
    EventEligibility, EventId, EventIntent, EventOrigin, EventPayload, EventReceive, Fact,
    FactPayload, LocalObservation, NostrTime, ObservedTime, Ordinal, OwnerEpoch, ProducerEpoch,
    ProducerRequestId, ProducerSlotId, Provenance, PublicKey, QualifiedRef, RequestSequence,
    Sequence, Signature, SignedEvent, SourceRecordId, WitnessContext,
};

fn reference(kind: DictionaryKind) -> QualifiedRef {
    QualifiedRef {
        dataset_id: DatasetId::try_from([1; 16]).unwrap(),
        dictionary_kind: kind,
        allocation_domain: AllocationDomainId::try_from([2; 16]).unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    }
}
fn source(sequence: u64) -> SourceRecordId {
    SourceRecordId {
        dataset_id: reference(DictionaryKind::VisibilityScope).dataset_id,
        shard_id: 0,
        owner_epoch: OwnerEpoch::new(1).unwrap(),
        sequence: Sequence::new(sequence).unwrap(),
    }
}
fn event() -> SignedEvent {
    SignedEvent {
        id: EventId::from_bytes([0xaa; 32]),
        pubkey: PublicKey::from_bytes([0xbb; 32]),
        created_at: NostrTime(u64::MAX),
        kind: 1,
        tags: vec![
            vec!["x".into()],
            vec!["x".into(), "".into()],
            vec!["x".into(), "".into()],
        ],
        content: "é\n".into(),
        sig: Signature::from_bytes([0xcc; 64]),
    }
}
fn observation() -> LocalObservation {
    LocalObservation {
        received_at: ObservedTime::new(-1),
        observer_id: reference(DictionaryKind::Observer),
        clock_quality: ClockQuality::new(
            reference(DictionaryKind::Observer),
            DurationUs::new(1),
            None,
            0,
        )
        .unwrap(),
        witness: None,
    }
}
fn witness() -> WitnessContext {
    WitnessContext {
        endpoint_id: reference(DictionaryKind::Endpoint),
        access_scope_id: reference(DictionaryKind::AccessScope),
        verification_method_id: reference(DictionaryKind::Method),
    }
}
fn request() -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: ProducerSlotId::try_from([3; 16]).unwrap(),
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(1).unwrap(),
    }
}
fn fact(payload: FactPayload) -> Result<Fact, bigbrotr_model::fact::FactError> {
    Fact::new(
        source(1),
        reference(DictionaryKind::VisibilityScope),
        ObservedTime::new(-1),
        payload,
    )
}

#[test]
fn eligibility_and_request_match_independently_frozen_digests() {
    // Synthetic bytes test encoding only, never signature validity or admission.
    let signed = event();
    let eligibility = fact(FactPayload::EventEligibility(EventEligibility {
        event_id: signed.id,
        body_digest: signed.body_digest(),
        sig: signed.sig,
        first_eligible_at: ObservedTime::new(-1),
        origin: EventOrigin::Network,
    }))
    .unwrap();
    assert_eq!(
        eligibility.payload_digest().to_string(),
        "578671867996ee16e2fc7a109cf0e9a11298f767d083ddc16ddc68ceeed6938e"
    );
    let intent = EventIntent::network(
        1,
        reference(DictionaryKind::VisibilityScope),
        signed,
        observation(),
    )
    .unwrap();
    assert_eq!(
        intent.request_digest(request()).to_string(),
        "c82039cd222506c1f1d1caa001dd529d786af1e324fce9b93b2a0a27abc7e1e1"
    );
    assert_ne!(
        intent.certification_digest(),
        intent.request_digest(request())
    );
    assert!(intent.validate_provenance(false).is_ok());
    assert!(intent.validate_provenance(true).is_err());
}

#[test]
fn request_identity_binds_supplied_signature_scope_policy_and_observation() {
    let make =
        |policy, scope, signed, local| EventIntent::network(policy, scope, signed, local).unwrap();
    let original = make(
        1,
        reference(DictionaryKind::VisibilityScope),
        event(),
        observation(),
    );
    let digest = original.request_digest(request());
    let mut variant = event();
    variant.sig = Signature::from_bytes([0xdd; 64]);
    let mut scope = reference(DictionaryKind::VisibilityScope);
    scope.ordinal = Ordinal::new(2).unwrap();
    let mut local = observation();
    local.received_at = ObservedTime::new(0);
    for changed in [
        make(
            1,
            reference(DictionaryKind::VisibilityScope),
            variant,
            observation(),
        ),
        make(
            2,
            reference(DictionaryKind::VisibilityScope),
            event(),
            observation(),
        ),
        make(1, scope, event(), observation()),
        make(
            1,
            reference(DictionaryKind::VisibilityScope),
            event(),
            local,
        ),
    ] {
        assert_ne!(digest, changed.request_digest(request()));
    }
    let mut other_request = request();
    other_request.request_sequence = RequestSequence::new(2).unwrap();
    assert_ne!(digest, original.request_digest(other_request));
    let imported = EventIntent::import(
        1,
        reference(DictionaryKind::VisibilityScope),
        event(),
        reference(DictionaryKind::DatasetLineage),
    )
    .unwrap();
    assert_ne!(digest, imported.request_digest(request()));
    assert!(imported.validate_provenance(true).is_ok());
}

#[test]
fn facts_and_intents_reject_foreign_or_wrong_kind_references() {
    let eligibility = |origin| {
        FactPayload::EventEligibility(EventEligibility {
            event_id: event().id,
            body_digest: event().body_digest(),
            sig: event().sig,
            first_eligible_at: ObservedTime::new(0),
            origin,
        })
    };
    assert!(
        fact(eligibility(EventOrigin::Import(reference(
            DictionaryKind::Endpoint
        ))))
        .is_err()
    );
    let mut foreign = reference(DictionaryKind::DatasetLineage);
    foreign.dataset_id = DatasetId::try_from([9; 16]).unwrap();
    assert!(fact(eligibility(EventOrigin::Import(foreign))).is_err());
    assert!(
        EventIntent::import(
            1,
            reference(DictionaryKind::VisibilityScope),
            event(),
            foreign
        )
        .is_err()
    );
    assert!(
        EventIntent::network(
            1,
            reference(DictionaryKind::AccessScope),
            event(),
            observation()
        )
        .is_err()
    );
    let mut local = observation();
    local.observer_id = reference(DictionaryKind::Endpoint);
    assert!(
        EventIntent::network(
            1,
            reference(DictionaryKind::VisibilityScope),
            event(),
            local
        )
        .is_err()
    );
    let mut local = observation();
    let mut clock = reference(DictionaryKind::Observer);
    clock.dataset_id = foreign.dataset_id;
    local.clock_quality = ClockQuality::new(clock, DurationUs::new(0), None, 0).unwrap();
    assert!(
        EventIntent::network(
            1,
            reference(DictionaryKind::VisibilityScope),
            event(),
            local
        )
        .is_err()
    );
    let mut local = observation();
    local.witness = Some(witness());
    let intent = EventIntent::network(
        1,
        reference(DictionaryKind::VisibilityScope),
        event(),
        local,
    )
    .unwrap();
    assert!(intent.validate_provenance(true).is_ok());
    assert!(intent.validate_provenance(false).is_err());
}

#[test]
fn body_fact_preserves_payload_and_correction_dependencies_are_explicit() {
    let body = EventBody::from(event());
    assert_eq!(body.clone().with_signature(event().sig), event());
    assert_eq!(body.body_digest(), event().body_digest());
    let payload = fact(FactPayload::EventPayload(EventPayload {
        body,
        event_ordinal: reference(DictionaryKind::Event),
    }))
    .unwrap();
    assert_eq!(payload.record_kind(), "event-payload");
    assert_eq!(payload.record_schema_version(), 1);
    assert_eq!(payload.fact_ref().source_record_id, source(1));
    let local = observation();
    let receive = EventReceive {
        event_id: event().id,
        received_at: local.received_at,
        observer_id: local.observer_id,
        clock_quality: local.clock_quality,
        corrects: Some(source(1)),
    };
    assert!(fact(FactPayload::EventReceive(receive.clone())).is_err());
    let mut future = receive.clone();
    future.corrects = Some(source(2));
    assert!(fact(FactPayload::EventReceive(future)).is_err());
    let correction = Fact::new(
        source(2),
        reference(DictionaryKind::VisibilityScope),
        ObservedTime::new(1),
        FactPayload::EventReceive(receive),
    )
    .unwrap();
    assert_eq!(correction.corrects(), Some(source(1)));
    let edge = witness();
    let provenance = fact(FactPayload::Provenance(Provenance {
        event_id: event().id,
        endpoint_id: edge.endpoint_id,
        observer_id: local.observer_id,
        access_scope_id: edge.access_scope_id,
        received_at: local.received_at,
        clock_quality: local.clock_quality,
        verification_method_id: edge.verification_method_id,
        corrects: None,
    }))
    .unwrap();
    assert_eq!(provenance.dictionary_refs().count(), 6);
    assert_ne!(payload.payload_digest(), provenance.payload_digest());
}
