//! Native storage tests only; the connected Control factory has separate tests.
use super::*;
use crate::runtime::{
    ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
};
use crate::storage::rocks::{
    RocksNativeSettings,
    manifest::{AllocationNamespaces, ManifestIdentity},
};
use bigbrotr_model::{
    DescriptorLabel, DictionaryDescriptor, DictionaryEntry, DictionaryKind, EvidenceIntent,
    EvidenceIntentLimits, ObservedTime, Ordinal, OwnerEpoch, PolicyEpoch, ProducerEpoch,
    ProducerRequestId, QualifiedRef, RecoveryGenerationId, RequestSequence, TopologyGeneration,
};
use std::sync::atomic::AtomicBool;

const MIB: u64 = 1024 * 1024;
pub(super) fn scope() -> QualifiedRef {
    QualifiedRef {
        dataset_id: [1; 16].try_into().unwrap(),
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: [2; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    }
}
pub(super) fn intent(class: &str) -> EvidenceIntent {
    EvidenceIntent::new(
        1,
        scope(),
        vec![
            DictionaryEntry::new(
                scope(),
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new(class.into()).unwrap(),
                },
            )
            .unwrap(),
        ],
        EvidenceIntentLimits {
            max_entries: 1,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap()
}
pub(super) fn request() -> ProducerRequestId {
    ProducerRequestId {
        producer_slot_id: [5; 16].try_into().unwrap(),
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(1).unwrap(),
    }
}
pub(super) fn manifest() -> IngestManifest {
    IngestManifest::new(
        ManifestIdentity {
            dataset_id: scope().dataset_id,
            topology_generation: TopologyGeneration::new(0),
            owner_resource_id: [3; 16].try_into().unwrap(),
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            process_incarnation: [4; 16].try_into().unwrap(),
            recovery_generation: RecoveryGenerationId::from_bytes([6; 16]),
            policy_epoch: PolicyEpoch::new(1),
        },
        AllocationNamespaces::from_entries(&[(
            DictionaryKind::VisibilityScope,
            scope().allocation_domain,
        )])
        .unwrap(),
    )
}
pub(super) fn native() -> RocksNativeLimits {
    RocksNativeLimits::new(RocksNativeSettings {
        block_cache_bytes: 32 * MIB,
        write_buffer_manager_bytes: 16 * MIB,
        per_cf_write_buffer_bytes: 2 * MIB,
        max_total_wal_bytes: 16 * MIB,
        max_open_files: 32,
        max_background_jobs: 2,
        native_allowance_bytes: 128 * MIB,
    })
    .unwrap()
}
pub(super) fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            161 * MIB,
            0,
            ResourceLimits::new(16 * MIB, 8, 144 * MIB, MIB),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    )
}
pub(super) fn grant(b: &ResourceBudget, data: u64, native: u64) -> OwnedPermit {
    b.try_acquire(ResourceRequest::data(data, 1, native).unwrap())
        .unwrap()
}

#[test]
fn later_schema_with_initial_identity_cannot_stage_or_bootstrap() {
    for schema in [2, 3] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let b = budget();
        let original = manifest();
        let store =
            IngestStore::initialize(&path, original, native(), grant(&b, 0, 128 * MIB)).unwrap();
        // Model privileged singleton corruption, then use an actual reopened
        // handle. Identity and namespaces remain exactly the valid epoch1 set.
        let altered = if schema == 2 {
            IngestManifest::for_successor(*original.identity(), *original.allocation_namespaces())
        } else {
            IngestManifest::for_repeatable_successor(
                *original.identity(),
                *original.allocation_namespaces(),
            )
        };
        store.db.put([1, 1], altered.encode().as_bytes()).unwrap();
        drop(store);
        let mut store =
            IngestStore::reopen(&path, altered, native(), grant(&b, 0, 128 * MIB)).unwrap();
        let sequence = store.db.latest_sequence_number();
        let limits = BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: MIB as usize,
        };
        let input = intent("public");
        assert!(matches!(
            store.stage_initial_dictionaries(
                request(),
                &input,
                limits,
                &grant(&b, 8 * MIB, 4 * MIB)
            ),
            Err(BootstrapError::InvalidIntent)
        ));
        assert!(matches!(
            store.bootstrap_dictionaries(
                request(),
                &input,
                ObservedTime::new(123),
                limits,
                grant(&b, 8 * MIB, 4 * MIB),
                || Ok(())
            ),
            Err(BootstrapError::InvalidIntent)
        ));
        for id in [None, Some(request())] {
            assert_eq!(
                store.require_admission_owner(id),
                Err(AdmissionError::InvalidInput)
            );
        }
        assert!(!path.join("producer-inputs-v1").exists());
        assert_eq!(store.db.latest_sequence_number(), sequence);
        for cf in ["default", "facts", "receipts"] {
            let rows = store
                .db
                .iterator_cf(
                    store.db.cf_handle(cf).unwrap(),
                    rocksdb::IteratorMode::Start,
                )
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), usize::from(cf == "default"));
            if cf == "default" {
                assert_eq!(rows[0].0.as_ref(), &[1, 1]);
                assert_eq!(rows[0].1.as_ref(), altered.encode().as_bytes());
            }
        }
        // Changing only the corrupted schema back permits the same request.
        store.db.put([1, 1], original.encode().as_bytes()).unwrap();
        drop(store);
        let mut store =
            IngestStore::reopen(&path, original, native(), grant(&b, 0, 128 * MIB)).unwrap();
        drop(
            store
                .bootstrap_dictionaries(
                    request(),
                    &input,
                    ObservedTime::new(123),
                    limits,
                    grant(&b, 8 * MIB, 4 * MIB),
                    || Ok(()),
                )
                .unwrap(),
        );
    }
}

#[test]
fn synchronized_bootstrap_reopens_as_one_complete_pending_batch_and_exact_receipt() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let original = intent("public");
    let result = store
        .bootstrap_dictionaries(
            request(),
            &original,
            ObservedTime::new(123),
            BootstrapLimits {
                max_mappings: 8,
                max_batch_bytes: 1024 * 1024,
            },
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(()),
        )
        .unwrap();
    let bytes = result.receipt().encode_persistent(8192).unwrap();
    assert_eq!(result.receipt().batch().unwrap().records().len(), 1);
    let first = result.receipt().batch().unwrap().first();
    assert_eq!(first.sequence.get(), 1);
    assert_eq!(first.owner_epoch.get(), 1);
    assert_eq!(
        result.receipt().request_digest(),
        original.request_digest(request())
    );
    drop(result);
    drop(store);
    let mut store =
        IngestStore::reopen(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let pending = store
        .read_pending_batch(
            first,
            PendingReadLimits {
                max_records: 8,
                max_encoded_bytes: 8192,
                fact: bigbrotr_model::codec::FactDecodeLimits {
                    max_encoded_bytes: 8192,
                    max_allocation_bytes: 65536,
                    max_tag_nodes: 128,
                    max_string_bytes: 1024,
                },
            },
            grant(&b, MIB, 0),
            &AtomicBool::new(false),
        )
        .unwrap()
        .unwrap();
    let record = bigbrotr_model::Fact::decode_preimage(
        &pending.records()[0],
        pending.batch().records()[0].payload_digest,
        bigbrotr_model::codec::FactDecodeLimits::default(),
    )
    .unwrap();
    assert_eq!(record.recorded_at().get(), 123);
    assert_eq!(
        record.payload(),
        &bigbrotr_model::FactPayload::Dictionary(original.dictionaries()[0].clone())
    );
    let sequence_before_transfer = store.db.latest_sequence_number();
    let mut wire = Vec::new();
    let transfer_limits = bigbrotr_protocol::archive_batch::Limits {
        max_records: 8,
        max_encoded_bytes: 8192,
        fact: bigbrotr_model::codec::FactDecodeLimits::default(),
    };
    pending
        .write_archive_batch(&mut wire, transfer_limits, &AtomicBool::new(false))
        .unwrap();
    let mut transferred = bigbrotr_protocol::archive_batch::BatchReader::new(
        wire.as_slice(),
        transfer_limits,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(
        transferred
            .next_fact(&AtomicBool::new(false))
            .unwrap()
            .unwrap(),
        record
    );
    assert!(
        transferred
            .next_fact(&AtomicBool::new(false))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        &transferred.finish(&AtomicBool::new(false)).unwrap(),
        pending.batch()
    );
    assert_eq!(store.db.latest_sequence_number(), sequence_before_transfer);
    drop(pending);
    let sequence_before = store.db.latest_sequence_number();
    let reduced = store.bootstrap_dictionaries(
        request(),
        &original,
        ObservedTime::new(456),
        BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: 128,
        },
        grant(&b, 8 * MIB, 4 * MIB),
        || Ok(()),
    );
    assert_eq!(reduced.err(), Some(BootstrapError::Limit));
    assert_eq!(store.db.latest_sequence_number(), sequence_before);
    let replay = store
        .bootstrap_dictionaries(
            request(),
            &original,
            ObservedTime::new(456),
            BootstrapLimits {
                max_mappings: 8,
                max_batch_bytes: 1024 * 1024,
            },
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(replay.receipt().encode_persistent(8192).unwrap(), bytes);
    drop(replay);
    let conflict = store.bootstrap_dictionaries(
        request(),
        &intent("private"),
        ObservedTime::new(789),
        BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: 1024 * 1024,
        },
        grant(&b, 8 * MIB, 4 * MIB),
        || Ok(()),
    );
    assert!(matches!(conflict, Err(BootstrapError::RequestConflict)));
}

#[test]
fn authority_is_checked_again_before_any_native_write() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let b = budget();
    let mut store =
        IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
    let before = store.db.latest_sequence_number();
    let mut checks = 0;
    let result = store.bootstrap_dictionaries(
        request(),
        &intent("public"),
        ObservedTime::new(123),
        BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: 1024 * 1024,
        },
        grant(&b, 8 * MIB, 4 * MIB),
        || {
            checks += 1;
            if checks == 1 {
                Ok(())
            } else {
                Err(BootstrapError::AuthorityUnavailable)
            }
        },
    );
    assert!(matches!(result, Err(BootstrapError::AuthorityUnavailable)));
    assert!(checks >= 2);
    assert_eq!(store.db.latest_sequence_number(), before);
    assert_eq!(b.snapshot().used().data_bytes(), 0);
}

#[test]
fn every_missing_or_changed_batch_member_prevents_replay_and_missing_receipt_is_not_rollback() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    let limits = BootstrapLimits {
        max_mappings: 8,
        max_batch_bytes: MIB as usize,
    };
    let original = intent("public");
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &original,
                ObservedTime::new(123),
                limits,
                grant(&b, 8 * MIB, 4 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
    let mut members = Vec::new();
    for cf in ["default", "facts", "receipts"] {
        for entry in store.db.iterator_cf(
            store.db.cf_handle(cf).unwrap(),
            rocksdb::IteratorMode::Start,
        ) {
            let (key, value) = entry.unwrap();
            members.push((cf, key, value));
        }
    }
    assert_eq!(members.len(), 9);
    for (cf, key, value) in members {
        let handle = store.db.cf_handle(cf).unwrap();
        store.db.delete_cf(handle, &key).unwrap();
        let before = store.db.latest_sequence_number();
        let result = store.bootstrap_dictionaries(
            request(),
            &original,
            ObservedTime::new(456),
            limits,
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(()),
        );
        assert!(
            matches!(result, Err(BootstrapError::StateMismatch)),
            "missing {cf}:{key:?}"
        );
        assert_eq!(store.db.latest_sequence_number(), before);
        let handle = store.db.cf_handle(cf).unwrap();
        let mut corrupt = value.to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        store.db.put_cf(handle, &key, corrupt).unwrap();
        let before = store.db.latest_sequence_number();
        let result = store.bootstrap_dictionaries(
            request(),
            &original,
            ObservedTime::new(456),
            limits,
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(()),
        );
        assert!(
            matches!(result, Err(BootstrapError::StateMismatch)),
            "changed {cf}:{key:?}"
        );
        assert_eq!(store.db.latest_sequence_number(), before);
        store
            .db
            .put_cf(store.db.cf_handle(cf).unwrap(), key, value)
            .unwrap();
    }
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &original,
                ObservedTime::new(456),
                limits,
                grant(&b, 8 * MIB, 4 * MIB),
                || Ok(()),
            )
            .unwrap(),
    );
}

#[test]
fn bounds_wrong_epoch_and_unallocated_ordinals_leave_the_initial_directory_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    let limits = BootstrapLimits {
        max_mappings: 8,
        max_batch_bytes: MIB as usize,
    };
    let before = store.db.latest_sequence_number();
    let insufficient = store.bootstrap_dictionaries(
        request(),
        &intent("public"),
        ObservedTime::new(1),
        limits,
        grant(&b, 1, 1),
        || panic!("authority must follow reservation"),
    );
    assert!(matches!(
        insufficient,
        Err(BootstrapError::InsufficientReservation)
    ));
    let small = store.bootstrap_dictionaries(
        request(),
        &intent("public"),
        ObservedTime::new(1),
        BootstrapLimits {
            max_batch_bytes: 100,
            ..limits
        },
        grant(&b, 8 * MIB, 4 * MIB),
        || Ok(()),
    );
    assert!(matches!(small, Err(BootstrapError::Limit)));
    let mut wrong = request();
    wrong.producer_epoch = ProducerEpoch::new(2).unwrap();
    assert!(matches!(
        store.bootstrap_dictionaries(
            wrong,
            &intent("public"),
            ObservedTime::new(1),
            limits,
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(())
        ),
        Err(BootstrapError::InvalidIntent)
    ));
    let mut reference = scope();
    reference.ordinal = Ordinal::new(2).unwrap();
    let hole = EvidenceIntent::new(
        1,
        reference,
        vec![
            DictionaryEntry::new(
                reference,
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new("public".into()).unwrap(),
                },
            )
            .unwrap(),
        ],
        EvidenceIntentLimits {
            max_entries: 1,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap();
    assert!(matches!(
        store.bootstrap_dictionaries(
            request(),
            &hole,
            ObservedTime::new(1),
            limits,
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(())
        ),
        Err(BootstrapError::InvalidIntent)
    ));
    assert_eq!(store.db.latest_sequence_number(), before);
    assert_eq!(b.snapshot().used().data_bytes(), 0);
}

#[test]
fn noncanonical_endpoint_is_not_a_durable_dictionary_identity() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let namespaces = AllocationNamespaces::from_entries(&[
        (DictionaryKind::Endpoint, [7; 16].try_into().unwrap()),
        (DictionaryKind::VisibilityScope, scope().allocation_domain),
    ])
    .unwrap();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        IngestManifest::new(*manifest().identity(), namespaces),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    let reference = QualifiedRef {
        dictionary_kind: DictionaryKind::Endpoint,
        allocation_domain: [7; 16].try_into().unwrap(),
        ..scope()
    };
    for (url, accepted) in [
        ("wss://RELAY.example:443/", false),
        ("wss://relay.example/", true),
    ] {
        let original = intent("public");
        let (_, scope, dictionaries) = original.into_parts();
        let mut dictionaries = dictionaries;
        dictionaries.push(
            DictionaryEntry::new(
                reference,
                DictionaryDescriptor::Endpoint(
                    bigbrotr_model::EndpointDescriptor::new(url.into()).unwrap(),
                ),
            )
            .unwrap(),
        );
        let intent = EvidenceIntent::new(
            1,
            scope,
            dictionaries,
            EvidenceIntentLimits {
                max_entries: 2,
                max_encoded_intent_bytes: 8192,
                max_validation_scratch_bytes: 1024,
            },
        )
        .unwrap();
        let before = store.db.latest_sequence_number();
        let result = store.bootstrap_dictionaries(
            request(),
            &intent,
            ObservedTime::new(123),
            BootstrapLimits {
                max_mappings: 8,
                max_batch_bytes: MIB as usize,
            },
            grant(&b, 8 * MIB, 4 * MIB),
            || Ok(()),
        );
        if accepted {
            assert!(result.is_ok());
        } else {
            assert!(matches!(result, Err(BootstrapError::InvalidIntent)));
            assert_eq!(store.db.latest_sequence_number(), before);
        }
    }
}
