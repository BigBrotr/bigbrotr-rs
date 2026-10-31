//! Actual RocksDB reads; raw mutations model corrupt metadata, not authority.
use super::bootstrap_tests::{budget, grant, intent, manifest, native, request, scope};
use super::*;
use crate::storage::rocks::{
    keys::{EncodedKey, IngestKey},
    manifest::ManifestIdentity,
};
use bigbrotr_model::*;
use std::sync::atomic::AtomicBool;

const MIB: u64 = 1024 * 1024;

fn source_key(manifest: &IngestManifest) -> EncodedKey {
    let identity = manifest.identity();
    IngestKey::SourceStream {
        dataset_id: identity.dataset_id,
        shard_id: identity.shard_id,
        owner_epoch: identity.owner_epoch,
    }
    .encode()
    .unwrap()
}
fn head(store: &IngestStore) -> Result<SourceHead, SourceHeadError> {
    store.observe_source_head(store.manifest(), &AtomicBool::new(false))
}

#[test]
fn source_head_fresh_explicit_zero_and_missing_source_never_write_or_repair() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    let before = store.db.latest_sequence_number();
    let observed = head(&store).unwrap();
    assert_eq!(observed.manifest(), &manifest());
    assert_eq!(observed.identity(), manifest().identity());
    assert_eq!(
        observed.bound(),
        StreamBound {
            stream: StreamId {
                shard_id: 0,
                owner_epoch: OwnerEpoch::new(1).unwrap()
            },
            last_complete_sequence: CompleteThrough::new(0),
        }
    );
    assert_eq!(store.db.latest_sequence_number(), before);
    store
        .db
        .delete(source_key(store.manifest()).as_bytes())
        .unwrap();
    let sequence = store.db.latest_sequence_number();
    assert_eq!(head(&store), Err(SourceHeadError::MissingSource));
    assert_eq!(store.db.latest_sequence_number(), sequence);
}

#[test]
fn source_head_tracks_dictionary_commits_and_not_no_output_retries() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    let bootstrap = BootstrapLimits {
        max_mappings: 8,
        max_batch_bytes: MIB as usize,
    };
    drop(
        store
            .bootstrap_dictionaries(
                request(),
                &intent("public"),
                ObservedTime::new(10),
                bootstrap,
                grant(
                    &b,
                    bootstrap.required_data_bytes().unwrap(),
                    bootstrap.required_native_bytes().unwrap(),
                ),
                || Ok(()),
            )
            .unwrap(),
    );
    assert_eq!(head(&store).unwrap().through().get(), 1);
    let scope = QualifiedRef {
        ordinal: Ordinal::new(2).unwrap(),
        ..scope()
    };
    let input = EvidenceIntent::new(
        1,
        scope,
        vec![
            DictionaryEntry::new(
                scope,
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new("private".into()).unwrap(),
                },
            )
            .unwrap(),
        ],
        EvidenceIntentLimits {
            max_entries: 8,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap();
    let admission = AdmissionLimits {
        max_batch_bytes: 256 * 1024,
        ..AdmissionLimits::default()
    };
    for sequence in [2, 3] {
        drop(
            store
                .append_dictionaries(
                    ProducerRequestId {
                        request_sequence: RequestSequence::new(sequence).unwrap(),
                        ..request()
                    },
                    &input,
                    ObservedTime::new(20),
                    admission,
                    grant(
                        &b,
                        admission.required_dictionary_data_bytes().unwrap(),
                        admission.required_native_bytes().unwrap(),
                    ),
                    || Ok(()),
                )
                .unwrap(),
        );
        assert_eq!(head(&store).unwrap().through().get(), 2);
    }
    let sequence = store.db.latest_sequence_number();
    store
        .db
        .delete(source_key(store.manifest()).as_bytes())
        .unwrap();
    assert_eq!(head(&store), Err(SourceHeadError::MissingSource));
    assert_eq!(store.db.latest_sequence_number(), sequence + 1);
}

#[test]
fn source_head_refuses_foreign_identity_persisted_manifest_and_uncertain_write() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let mut store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    let original = manifest();
    for field in 0..8 {
        let mut identity = *original.identity();
        match field {
            0 => identity.dataset_id = [91; 16].try_into().unwrap(),
            1 => identity.topology_generation = TopologyGeneration::new(91),
            2 => identity.owner_resource_id = [91; 16].try_into().unwrap(),
            3 => identity.shard_id = 91,
            4 => identity.owner_epoch = OwnerEpoch::new(91).unwrap(),
            5 => identity.process_incarnation = [91; 16].try_into().unwrap(),
            6 => identity.recovery_generation = RecoveryGenerationId::from_bytes([91; 16]),
            7 => identity.policy_epoch = PolicyEpoch::new(91),
            _ => unreachable!(),
        }
        let foreign = IngestManifest::new(identity, *original.allocation_namespaces());
        assert_eq!(
            store.observe_source_head(&foreign, &AtomicBool::new(false)),
            Err(SourceHeadError::ManifestMismatch)
        );
        store.db.put([1, 1], foreign.encode().as_bytes()).unwrap();
        assert_eq!(head(&store), Err(SourceHeadError::ManifestMismatch));
    }
    store.db.put([1, 1], b"bad-manifest").unwrap();
    assert_eq!(head(&store), Err(SourceHeadError::MalformedManifest));
    store.db.delete([1, 1]).unwrap();
    assert_eq!(head(&store), Err(SourceHeadError::MissingManifest));
    store.db.put([1, 1], original.encode().as_bytes()).unwrap();
    store.write_uncertain = true;
    assert_eq!(head(&store), Err(SourceHeadError::WriteUncertain));
}

#[test]
fn source_head_refuses_bad_framing_and_an_unsigned_head_unbound_to_accounting() {
    let root = tempfile::tempdir().unwrap();
    let b = budget();
    let store = IngestStore::initialize(
        &root.path().join("db"),
        manifest(),
        native(),
        grant(&b, 0, 128 * MIB),
    )
    .unwrap();
    let key = source_key(store.manifest());
    // Includes an integer wider than u64. Explicit initial zero is current;
    // malformed bytes may not be truncated or replaced by a default.
    for bytes in [vec![], vec![1, 2], vec![1; 9], vec![1; 10], vec![1; 18]] {
        store.db.put(key.as_bytes(), bytes).unwrap();
        let sequence = store.db.latest_sequence_number();
        assert_eq!(head(&store), Err(SourceHeadError::MalformedSource));
        assert_eq!(store.db.latest_sequence_number(), sequence);
    }
    store
        .db
        .put(
            key.as_bytes(),
            [1, 2, 255, 255, 255, 255, 255, 255, 255, 255],
        )
        .unwrap();
    assert_eq!(
        head(&store),
        Err(SourceHeadError::Accounting(
            PendingAccountingError::SourceMismatch
        ))
    );
    // Preserve the unsigned bytes but do not certify a watermark that differs
    // from mandatory accounting or invent an empty result from that failure.
    assert_eq!(
        &store.db.get(key.as_bytes()).unwrap().unwrap()[2..],
        &u64::MAX.to_be_bytes()
    );
    let mut facts = store
        .db
        .raw_iterator_cf(store.db.cf_handle("facts").unwrap());
    facts.seek_to_first();
    facts.status().unwrap();
    assert!(!facts.valid());
    drop(facts);
    let sequence = store.db.latest_sequence_number();
    assert_eq!(
        store.observe_source_head(store.manifest(), &AtomicBool::new(true)),
        Err(SourceHeadError::Cancelled)
    );
    assert_eq!(store.db.latest_sequence_number(), sequence);
}

#[test]
fn source_head_zero_uses_current_epoch_and_requires_empty_current_stream() {
    for epoch in [1, 2, 3] {
        let root = tempfile::tempdir().unwrap();
        let b = budget();
        let path = root.path().join("db");
        let mut store =
            IngestStore::initialize(&path, manifest(), native(), grant(&b, 0, 128 * MIB)).unwrap();
        if epoch > 1 {
            let limits = BootstrapLimits {
                max_mappings: 8,
                max_batch_bytes: MIB as usize,
            };
            drop(
                store
                    .bootstrap_dictionaries(
                        request(),
                        &intent("public"),
                        ObservedTime::new(123),
                        limits,
                        grant(
                            &b,
                            limits.required_data_bytes().unwrap(),
                            limits.required_native_bytes().unwrap(),
                        ),
                        || Ok(()),
                    )
                    .unwrap(),
            );
        }
        let identity = ManifestIdentity {
            owner_epoch: OwnerEpoch::new(epoch).unwrap(),
            topology_generation: TopologyGeneration::new(19),
            ..*manifest().identity()
        };
        let current = if epoch == 1 {
            IngestManifest::new(identity, *manifest().allocation_namespaces())
        } else if epoch == 2 {
            IngestManifest::for_successor(identity, *manifest().allocation_namespaces())
        } else {
            IngestManifest::for_repeatable_successor(identity, *manifest().allocation_namespaces())
        };
        // Metadata fixture only, not a handoff/activation proof. For successor
        // heads retain real predecessor science and its charge; current S is0.
        let pending = store.accounting.pending_bytes();
        store.db.put([1, 1], current.encode().as_bytes()).unwrap();
        store
            .db
            .put(
                source_key(&current).as_bytes(),
                [1, 2, 0, 0, 0, 0, 0, 0, 0, 0],
            )
            .unwrap();
        store
            .db
            .put(
                [1, 0x0b],
                PendingAccounting::new(&current, pending, 0).encode(),
            )
            .unwrap();
        drop(store);
        let store = IngestStore::reopen(&path, current, native(), grant(&b, 0, 128 * MIB)).unwrap();
        let observed = head(&store).unwrap();
        assert_eq!(observed.identity(), &identity);
        assert_eq!(observed.bound().stream.owner_epoch.get(), epoch);
        assert_eq!(observed.through().get(), 0);
        let first = SourceRecordId {
            dataset_id: identity.dataset_id,
            shard_id: identity.shard_id,
            owner_epoch: identity.owner_epoch,
            sequence: Sequence::new(1).unwrap(),
        };
        // Both current-prefix kinds contradict zero; neither is inferred from
        // a prior epoch's metadata or used as a complete-prefix certificate.
        for key in [
            IngestKey::Fact {
                source_record_id: first,
            },
            IngestKey::PendingBatch { first },
        ] {
            let key = key.encode().unwrap();
            let cf = store.db.cf_handle("facts").unwrap();
            store
                .db
                .put_cf(cf, key.as_bytes(), b"contradiction")
                .unwrap();
            assert_eq!(head(&store), Err(SourceHeadError::NonemptyZeroSource));
            store.db.delete_cf(cf, key.as_bytes()).unwrap();
        }
    }
}
