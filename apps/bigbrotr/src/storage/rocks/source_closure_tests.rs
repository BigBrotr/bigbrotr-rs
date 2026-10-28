//! Actual native results must cover the whole initial source before closure.
use super::producer_recovery_tests::recovery_limits;
use super::*;
use crate::storage::rocks::keys::IngestKey;
use std::sync::atomic::AtomicBool;

#[test]
fn initial_historical_batch_reader_requires_complete_proof_and_preserves_signed_bytes() {
    let (root, mut store, budget, manifest, native) =
        setup_entries_at(entries(false), TopologyGeneration::new(0));
    let prepared = PreparedEvent::new(
        request(7, 2),
        intent("E_private", 2, 10_000_000),
        256 * 1024,
        grant(&budget, MIB, 0),
    )
    .unwrap();
    drop(
        store
            .admit_event(
                prepared,
                ObservedTime::new(10_000_000),
                false,
                AdmissionLimits::default(),
                grant(&budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let closing = recovery
        .close_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    let mut resolution = closing.source_resolution(&recovery, 2).unwrap();
    for sequence in 1..=2 {
        let resolved = recovery
            .resolve_original_request(
                request(7, sequence),
                original_digest(sequence),
                recovery_limits(),
                grant(&budget, 3 * MIB, 0),
                grant(&budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        resolution.record(&resolved).unwrap();
    }
    let source = resolution.finish().unwrap();
    let closure_digest = source.digest();
    let acknowledgement = source.into_recovery_acknowledgement().unwrap();
    let before_ack = kv(&recovery.store);
    // Native mechanics only: PG confirmation is explicitly synthetic here.
    // The real close-source command may ACK its recovered checkpoint. This
    // only advances the producer watermark; pending facts/originals remain.
    recovery
        .store
        .test_acknowledge_target(
            acknowledgement.target(2).unwrap(),
            &grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    let producer_key = IngestKey::ProducerState {
        producer_slot_id: request(7, 1).producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
    }
    .encode()
    .unwrap();
    let after_ack = kv(&recovery.store);
    for row in before_ack
        .iter()
        .filter(|row| row.1 != producer_key.as_bytes())
    {
        assert!(after_ack.contains(row));
    }
    let closing = recovery
        .observe_closed_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
        )
        .unwrap();
    assert_eq!(closing.acknowledged_through(), 2);
    let mut resolution = closing.source_resolution(&recovery, 2).unwrap();
    for sequence in 1..=2 {
        let resolved = recovery
            .resolve_original_request(
                request(7, sequence),
                original_digest(sequence),
                recovery_limits(),
                grant(&budget, 3 * MIB, 0),
                grant(&budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        resolution.record(&resolved).unwrap();
    }
    let proof = resolution.finish().unwrap();
    assert_eq!(proof.digest(), closure_digest);
    assert_eq!(proof.acknowledged_through(), 2);
    let before = kv(&recovery.store);
    let stamp = recovery.store.db.latest_sequence_number();
    let limits = PendingReadLimits {
        max_records: 64,
        max_encoded_bytes: MIB as usize,
        fact: bigbrotr_model::codec::FactDecodeLimits::default(),
    };
    let first = SourceRecordId {
        dataset_id: manifest.identity().dataset_id,
        shard_id: manifest.identity().shard_id,
        owner_epoch: OwnerEpoch::new(1).unwrap(),
        sequence: Sequence::new(1).unwrap(),
    };
    let cancel = AtomicBool::new(false);
    let read = |source, limits: PendingReadLimits| {
        proof.read_complete_pending_batch(
            source,
            limits,
            grant(&budget, limits.required_data_bytes().unwrap(), 0),
            &cancel,
        )
    };
    let dictionaries = read(first, limits).unwrap();
    let next = SourceRecordId {
        sequence: Sequence::new(dictionaries.batch().last().sequence.get() + 1).unwrap(),
        ..first
    };
    drop(dictionaries);
    let signed = read(next, limits).unwrap();
    assert_eq!(
        signed.batch().last().sequence.get(),
        proof.last_complete_sequence()
    );
    assert!(
        signed.batch().records().len() > 1,
        "the signed source is a whole multi-fact admission"
    );
    let expected = fixture_event("E_private");
    let mut payloads = 0;
    let mut signatures = 0;
    for (reference, bytes) in signed.batch().records().iter().zip(signed.records()) {
        let fact = Fact::decode_preimage(bytes, reference.payload_digest, limits.fact).unwrap();
        assert_eq!(fact.source_record_id(), reference.source_record_id);
        assert_eq!(fact.source_record_id().owner_epoch.get(), 1);
        assert_eq!(fact.recorded_at().get(), 10_000_000);
        match fact.payload() {
            FactPayload::EventPayload(value) => {
                assert_eq!(value.body, EventBody::from(expected.clone()));
                payloads += 1;
            }
            FactPayload::EventEligibility(value) => {
                assert_eq!(value.event_id, expected.id);
                assert_eq!(value.sig, expected.sig);
                signatures += 1;
            }
            _ => {}
        }
    }
    assert_eq!((payloads, signatures), (1, 1));
    drop(signed);
    for outside in [
        SourceRecordId {
            dataset_id: [99; 16].try_into().unwrap(),
            ..first
        },
        SourceRecordId {
            shard_id: 1,
            ..first
        },
        SourceRecordId {
            owner_epoch: OwnerEpoch::new(2).unwrap(),
            ..first
        },
        SourceRecordId {
            sequence: Sequence::new(proof.last_complete_sequence() + 1).unwrap(),
            ..first
        },
    ] {
        assert!(matches!(
            read(outside, limits),
            Err(LineagePendingError::OutsideVerifiedCut)
        ));
    }
    assert!(matches!(
        read(
            SourceRecordId {
                sequence: Sequence::new(2).unwrap(),
                ..first
            },
            limits
        ),
        Err(LineagePendingError::RequiredBatchMissing)
    ));
    assert!(matches!(
        read(
            next,
            PendingReadLimits {
                max_records: 1,
                ..limits
            }
        ),
        Err(LineagePendingError::Read(PendingError::Limit))
    ));
    assert!(matches!(
        proof.read_complete_pending_batch(first, limits, grant(&budget, 1, 0), &cancel),
        Err(LineagePendingError::Read(
            PendingError::InsufficientReservation
        ))
    ));
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), stamp);
    cancel.store(true, std::sync::atomic::Ordering::Release);
    assert!(read(first, limits).is_err());
    cancel.store(false, std::sync::atomic::Ordering::Release);
    // A changed native sequence invalidates the complete proof even when the
    // retained fact bytes and the requested batch themselves are unchanged.
    recovery
        .store
        .db
        .put(b"test-unrelated-sequence", b"changed")
        .unwrap();
    assert!(matches!(
        read(first, limits),
        Err(LineagePendingError::Proof(BootstrapError::StateMismatch))
    ));
}

fn original_digest(sequence: u64) -> Digest256 {
    if sequence == 1 {
        EvidenceIntent::new(
            1,
            reference(DictionaryKind::VisibilityScope, 1),
            entries(false),
            EvidenceIntentLimits {
                max_entries: 16,
                max_encoded_intent_bytes: 8192,
                max_validation_scratch_bytes: 4096,
            },
        )
        .unwrap()
        .request_digest(request(7, sequence))
    } else {
        intent("E_private", 2, 10_000_000).request_digest(request(7, sequence))
    }
}

#[test]
fn source_closure_requires_every_original_request_and_preserves_all_native_bytes() {
    let (root, mut store, budget, manifest, native) = setup(false);
    for sequence in [2, 3] {
        let input = PreparedEvent::new(
            request(7, sequence),
            intent("E_private", 2, 10_000_000),
            256 * 1024,
            grant(&budget, MIB, 0),
        )
        .unwrap();
        drop(
            store
                .admit_event(
                    input,
                    ObservedTime::new(10_000_000),
                    false,
                    AdmissionLimits::default(),
                    grant(&budget, 40 * MIB, 10 * MIB),
                    || Ok(()),
                )
                .unwrap(),
        );
    }
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let closed = recovery
        .close_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    let before = kv(&recovery.store);
    let native_sequence = recovery.store.db.latest_sequence_number();
    let mut prefix = closed.source_resolution(&recovery, 3).unwrap();
    for sequence in 1..=3 {
        let resolved = recovery
            .resolve_original_request(
                request(7, sequence),
                original_digest(sequence),
                recovery_limits(),
                grant(&budget, 3 * MIB, 0),
                grant(&budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        prefix.record(&resolved).unwrap();
        assert!(
            prefix.record(&resolved).is_err(),
            "duplicate cannot fill the next request"
        );
    }
    let verified = prefix.finish().unwrap();
    assert!(matches!(
        verified.validate_initial(&AtomicBool::new(false)),
        Err(BootstrapError::StateMismatch)
    ));
    assert_eq!(verified.last_complete_sequence(), 8);
    assert_eq!(verified.resolved_requests(), 3);
    assert_eq!(verified.event_request_mask(), 0b110);
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), native_sequence);
    let digest = verified.digest();
    drop(verified);
    let closed = recovery
        .close_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    let mut missing = closed.source_resolution(&recovery, 3).unwrap();
    let resolved = recovery
        .resolve_original_request(
            request(7, 1),
            original_digest(1),
            recovery_limits(),
            grant(&budget, 3 * MIB, 0),
            grant(&budget, 40 * MIB, 0),
            || Ok(()),
        )
        .unwrap();
    missing.record(&resolved).unwrap();
    assert!(
        missing.finish().is_err(),
        "retained later outcomes do not fill a skipped request"
    );
    assert_ne!(digest, [0; 32]);
    assert_eq!(kv(&recovery.store), before);
}

#[test]
fn source_closure_preserves_zero_and_refuses_unissued_reservations() {
    let (root, store, budget, manifest, native) = setup(false);
    drop(store);
    let path = root.path().join("empty");
    drop(IngestStore::initialize(&path, manifest, native, grant(&budget, 0, 128 * MIB)).unwrap());
    let mut recovery =
        IngestRecovery::open(&path, manifest, native, grant(&budget, 0, 128 * MIB)).unwrap();
    let closed = recovery
        .close_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    assert!(closed.source_resolution(&recovery, 1).is_err());
    let closed = recovery
        .close_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(&budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    let before = kv(&recovery.store);
    let verified = closed
        .source_resolution(&recovery, 0)
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(verified.last_complete_sequence(), 0);
    assert_eq!(verified.resolved_requests(), 0);
    assert_eq!(verified.event_request_mask(), 0);
    assert_eq!(kv(&recovery.store), before);
}

#[test]
fn source_closure_refuses_native_counter_disagreement_and_another_producer() {
    for extra_producer in [false, true] {
        let (root, store, budget, manifest, native) = setup(false);
        if !extra_producer {
            let id = manifest.identity();
            let key = IngestKey::SourceStream {
                dataset_id: id.dataset_id,
                shard_id: id.shard_id,
                owner_epoch: id.owner_epoch,
            }
            .encode()
            .unwrap();
            let mut bytes = vec![1, 2];
            bytes.extend_from_slice(&5_u64.to_be_bytes());
            store.db.put(key.as_bytes(), bytes).unwrap();
        }
        drop(store);
        let mut recovery = IngestRecovery::open(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        if extra_producer {
            drop(
                recovery
                    .close_producer(
                        request(8, 1).producer_slot_id,
                        ProducerEpoch::new(1).unwrap(),
                        grant(&budget, 4096, 4096),
                        || Ok(()),
                    )
                    .unwrap(),
            );
        }
        let closed = recovery
            .close_producer(
                request(7, 1).producer_slot_id,
                ProducerEpoch::new(1).unwrap(),
                grant(&budget, 4096, 4096),
                || Ok(()),
            )
            .unwrap();
        let before = kv(&recovery.store);
        let mut prefix = closed.source_resolution(&recovery, 1).unwrap();
        let resolved = recovery
            .resolve_original_request(
                request(7, 1),
                original_digest(1),
                recovery_limits(),
                grant(&budget, 3 * MIB, 0),
                grant(&budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        prefix.record(&resolved).unwrap();
        assert!(prefix.finish().is_err());
        assert_eq!(kv(&recovery.store), before);
    }
}
