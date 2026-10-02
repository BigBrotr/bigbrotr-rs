//! Actual native results must cover the whole initial source before closure.
use super::producer_recovery_tests::recovery_limits;
use super::*;
use crate::storage::rocks::keys::IngestKey;

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
    assert_eq!(verified.last_complete_sequence(), 8);
    assert_eq!(verified.resolved_requests(), 3);
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
