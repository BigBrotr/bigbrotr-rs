//! Real engine transition; expected delta is written independently of the writer.
use super::producer_recovery_tests::recovery_limits;
use super::*;
use crate::storage::rocks::keys::{ColumnFamily, IngestKey, decode_key};

fn target(old: IngestManifest) -> IngestManifest {
    let mut identity = *old.identity();
    identity.owner_epoch = OwnerEpoch::new(2).unwrap();
    identity.topology_generation = TopologyGeneration::new(2);
    identity.process_incarnation = [44; 16].try_into().unwrap();
    let domains: Vec<_> = (1..=9)
        .filter_map(|n| {
            let kind = DictionaryKind::try_from(n).unwrap();
            old.allocation_namespaces()
                .domain(kind)
                .map(|_| (kind, [n as u8 + 32; 16].try_into().unwrap()))
        })
        .collect();
    IngestManifest::for_successor(
        identity,
        AllocationNamespaces::from_entries(&domains).unwrap(),
    )
}

fn plan(recovery: &mut IngestRecovery, budget: &ResourceBudget, issued: u64) -> InitialHandoff {
    let next = target(*recovery.manifest());
    InitialHandoff::from_source(&proof(recovery, budget, issued), next).unwrap()
}
fn proof<'a>(
    recovery: &'a mut IngestRecovery,
    budget: &ResourceBudget,
    issued: u64,
) -> ResolvedSource<'a> {
    let closed = recovery
        .close_producer(
            request(7, 1).producer_slot_id,
            ProducerEpoch::new(1).unwrap(),
            grant(budget, 4096, 4096),
            || Ok(()),
        )
        .unwrap();
    let mut source = closed.source_resolution(recovery, issued).unwrap();
    if issued == 1 {
        let digest = EvidenceIntent::new(
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
        .request_digest(request(7, 1));
        let resolved = recovery
            .resolve_original_request(
                request(7, 1),
                digest,
                recovery_limits(),
                grant(budget, 3 * MIB, 0),
                grant(budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        source.record(&resolved).unwrap();
    }
    source.finish().unwrap()
}

fn keys() -> [Vec<u8>; 3] {
    let mut handoff = vec![1, 10];
    handoff.extend_from_slice(&[1; 16]);
    handoff.extend_from_slice(&[3; 16]);
    handoff.extend_from_slice(&1_u64.to_be_bytes());
    let mut source = vec![1, 2];
    source.extend_from_slice(&[1; 16]);
    source.extend_from_slice(&[0; 4]);
    source.extend_from_slice(&2_u64.to_be_bytes());
    let mut producer = vec![1, 49];
    producer.extend_from_slice(&[7; 16]);
    producer.extend_from_slice(&2_u64.to_be_bytes());
    [handoff, source, producer]
}

fn expected(
    plan: &InitialHandoff,
    before: &[(String, Vec<u8>, Vec<u8>)],
) -> Vec<(String, Vec<u8>, Vec<u8>)> {
    let original = before
        .iter()
        .find(|(cf, key, _)| cf == "default" && key == &[1, 1])
        .unwrap()
        .2
        .clone();
    let mut successor = original.clone();
    successor[3] = 2;
    successor[26..34].copy_from_slice(&2_u64.to_be_bytes());
    successor[54..62].copy_from_slice(&2_u64.to_be_bytes());
    successor[62..78].fill(44);
    for entry in successor[104..].as_chunks_mut::<18>().0.iter_mut() {
        let domain = entry[1] + 32;
        entry[2..].fill(domain);
    }
    let mut receipt = vec![1, 10];
    receipt.extend_from_slice(&[7; 16]);
    receipt.extend_from_slice(&1_u64.to_be_bytes());
    receipt.extend_from_slice(&plan.issued_through().to_be_bytes());
    receipt.extend_from_slice(&plan.last_complete_sequence().to_be_bytes());
    receipt.extend_from_slice(&plan.closure_digest());
    for manifest in [&original, &successor] {
        receipt.extend_from_slice(&(manifest.len() as u16).to_be_bytes());
        receipt.extend_from_slice(manifest);
    }
    assert_eq!(plan.encode().as_bytes(), receipt);
    let [handoff, source, producer] = keys();
    let mut result = before.to_vec();
    result
        .iter_mut()
        .find(|(cf, key, _)| cf == "default" && key == &[1, 1])
        .unwrap()
        .2 = successor;
    let mut zero = vec![1, 2];
    zero.extend_from_slice(&[0; 8]);
    let mut open = vec![1, 49];
    open.extend_from_slice(&[0; 24]);
    result.extend([
        (String::from("default"), handoff, receipt),
        (String::from("default"), source, zero),
        (String::from("receipts"), producer, open),
    ]);
    result.sort();
    result
}

#[test]
fn initial_handoff_has_exact_four_key_delta_and_read_only_reopened_replay() {
    for issued in [0, 1] {
        let (root, store, budget, manifest, native) = setup(false);
        drop(store);
        let path = root.path().join(if issued == 0 { "empty" } else { "db" });
        if issued == 0 {
            drop(
                IngestStore::initialize(&path, manifest, native, grant(&budget, 0, 128 * MIB))
                    .unwrap(),
            );
        }
        let mut recovery =
            IngestRecovery::open(&path, manifest, native, grant(&budget, 0, 128 * MIB)).unwrap();
        let plan = plan(&mut recovery, &budget, issued);
        let before = kv(&recovery.store);
        let expected = expected(&plan, &before);
        let observed = recovery
            .apply_initial_handoff(&plan, grant(&budget, 8192, 8192), || Ok(()))
            .unwrap();
        assert_eq!(
            observed.handoff().encode().as_bytes(),
            plan.encode().as_bytes()
        );
        drop(observed);
        assert_eq!(kv(&recovery.store), expected);
        let sequence = recovery.store.db.latest_sequence_number();
        drop(recovery);
        assert!(matches!(
            IngestStore::reopen(&path, manifest, native, grant(&budget, 0, 128 * MIB)),
            Err(StoreError::ManifestMismatch)
        ));
        let mut recovery = IngestRecovery::open(
            &path,
            *plan.next_manifest(),
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        drop(
            recovery
                .apply_initial_handoff(&plan, grant(&budget, 8192, 8192), || Ok(()))
                .unwrap(),
        );
        assert_eq!(kv(&recovery.store), expected);
        assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
        assert!(
            IngestStore::initialize(
                &root.path().join("fresh-successor"),
                *plan.next_manifest(),
                native,
                grant(&budget, 0, 128 * MIB)
            )
            .is_err()
        );
        assert!(!root.path().join("fresh-successor").exists());
    }
}

#[test]
fn initial_handoff_refuses_conflicts_insufficient_budget_and_stale_proof() {
    for fault in [
        "permit",
        "authority",
        "dispatch-authority",
        "intervening",
        "new-source",
        "new-producer",
        "receipt",
        "partial-replay",
        "changed-plan",
    ] {
        let (root, store, budget, manifest, native) = setup(false);
        drop(store);
        let mut recovery = IngestRecovery::open(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        let mut handoff = plan(&mut recovery, &budget, 1);
        let [receipt, source, producer] = keys();
        match fault {
            "intervening" => {
                let original = recovery.store.db.get([1, 1]).unwrap().unwrap();
                recovery.store.db.put([1, 1], original).unwrap();
            }
            "new-source" => recovery.store.db.put(source, [1, 2]).unwrap(),
            "new-producer" => recovery
                .store
                .db
                .put_cf(
                    recovery.store.db.cf_handle("receipts").unwrap(),
                    producer,
                    [1, 49],
                )
                .unwrap(),
            "receipt" => recovery.store.db.put(receipt, [1, 10]).unwrap(),
            "partial-replay" => {
                drop(
                    recovery
                        .apply_initial_handoff(&handoff, grant(&budget, 8192, 8192), || Ok(()))
                        .unwrap(),
                );
                recovery.store.db.delete(source).unwrap();
            }
            "changed-plan" => {
                // A second genuine witness targets another incarnation, before either application.
                let closed = recovery
                    .observe_closed_producer(
                        request(7, 1).producer_slot_id,
                        ProducerEpoch::new(1).unwrap(),
                        grant(&budget, 4096, 4096),
                    )
                    .unwrap();
                let digest = EvidenceIntent::new(
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
                .request_digest(request(7, 1));
                let mut source = closed.source_resolution(&recovery, 1).unwrap();
                let resolved = recovery
                    .resolve_original_request(
                        request(7, 1),
                        digest,
                        recovery_limits(),
                        grant(&budget, 3 * MIB, 0),
                        grant(&budget, 40 * MIB, 0),
                        || Ok(()),
                    )
                    .unwrap();
                source.record(&resolved).unwrap();
                let proof = source.finish().unwrap();
                let mut identity = *handoff.next_manifest().identity();
                identity.process_incarnation = [45; 16].try_into().unwrap();
                let different = InitialHandoff::from_source(
                    &proof,
                    IngestManifest::for_successor(
                        identity,
                        *handoff.next_manifest().allocation_namespaces(),
                    ),
                )
                .unwrap();
                drop(proof);
                drop(resolved);
                drop(
                    recovery
                        .apply_initial_handoff(&handoff, grant(&budget, 8192, 8192), || Ok(()))
                        .unwrap(),
                );
                handoff = different;
            }
            _ => {}
        }
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        let bytes = if fault == "permit" { 8191 } else { 8192 };
        let mut checks = 0;
        assert!(
            recovery
                .apply_initial_handoff(&handoff, grant(&budget, bytes, 8192), || {
                    checks += 1;
                    if fault == "authority" || (fault == "dispatch-authority" && checks == 2) {
                        Err(BootstrapError::AuthorityUnavailable)
                    } else {
                        Ok(())
                    }
                })
                .is_err(),
            "{fault}"
        );
        assert_eq!(kv(&recovery.store), before, "{fault}");
        assert_eq!(
            recovery.store.db.latest_sequence_number(),
            sequence,
            "{fault}"
        );
    }
}

#[test]
fn initial_handoff_authority_loss_after_commit_does_not_imply_rollback() {
    let (root, store, budget, manifest, native) = setup(false);
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let handoff = plan(&mut recovery, &budget, 1);
    let expected = expected(&handoff, &kv(&recovery.store));
    let mut checks = 0;
    let result = recovery.apply_initial_handoff(&handoff, grant(&budget, 8192, 8192), || {
        checks += 1;
        if checks == 3 {
            Err(BootstrapError::AuthorityUnavailable)
        } else {
            Ok(())
        }
    });
    assert!(matches!(result, Err(BootstrapError::AuthorityUnavailable)));
    assert_eq!(checks, 3);
    assert_eq!(kv(&recovery.store), expected);
    let sequence = recovery.store.db.latest_sequence_number();
    drop(
        recovery
            .apply_initial_handoff(&handoff, grant(&budget, 8192, 8192), || Ok(()))
            .unwrap(),
    );
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
}

#[test]
fn initial_handoff_key_has_literal_bytes_and_closed_decoding() {
    let bytes = keys()[0].clone();
    let key = IngestKey::OwnerHandoff {
        dataset_id: [1; 16].try_into().unwrap(),
        owner_resource_id: [3; 16].try_into().unwrap(),
        predecessor_epoch: OwnerEpoch::new(1).unwrap(),
    };
    assert_eq!(key.encode().unwrap().as_bytes(), bytes);
    assert_eq!(decode_key(ColumnFamily::Default, &bytes).unwrap(), key);
    for end in 0..bytes.len() {
        assert!(decode_key(ColumnFamily::Default, &bytes[..end]).is_err());
    }
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(decode_key(ColumnFamily::Default, &extra).is_err());
    assert!(decode_key(ColumnFamily::Facts, &bytes).is_err());
    for range in [2..18, 18..34, 34..42] {
        let mut bad = bytes.clone();
        bad[range].fill(0);
        assert!(decode_key(ColumnFamily::Default, &bad).is_err());
    }
}

#[test]
fn initial_handoff_proof_cannot_move_to_another_equal_sequence_store() {
    let (root_a, store_a, budget_a, manifest, native) = setup(false);
    let (root_b, store_b, budget_b, _, _) = setup(false);
    drop(store_a);
    drop(store_b);
    let mut a = IngestRecovery::open(
        &root_a.path().join("db"),
        manifest,
        native,
        grant(&budget_a, 0, 128 * MIB),
    )
    .unwrap();
    let mut b = IngestRecovery::open(
        &root_b.path().join("db"),
        manifest,
        native,
        grant(&budget_b, 0, 128 * MIB),
    )
    .unwrap();
    let handoff = plan(&mut a, &budget_a, 1);
    {
        let _handoff = plan(&mut b, &budget_b, 1);
    }
    assert_eq!(
        a.store.db.latest_sequence_number(),
        b.store.db.latest_sequence_number()
    );
    let before = kv(&b.store);
    assert!(matches!(
        b.apply_initial_handoff(&handoff, grant(&budget_b, 8192, 8192), || Ok(())),
        Err(BootstrapError::StateMismatch)
    ));
    assert_eq!(kv(&b.store), before);
}

#[test]
fn initial_handoff_refuses_target_keys_that_predate_the_positive_source_proof() {
    for fault in ["source", "fact", "dictionary"] {
        let (root, store, budget, manifest, native) = setup(false);
        let key = match fault {
            "source" => IngestKey::SourceStream {
                dataset_id: manifest.identity().dataset_id,
                shard_id: 0,
                owner_epoch: OwnerEpoch::new(2).unwrap(),
            },
            "fact" => IngestKey::Fact {
                source_record_id: SourceRecordId {
                    dataset_id: manifest.identity().dataset_id,
                    shard_id: 0,
                    owner_epoch: OwnerEpoch::new(2).unwrap(),
                    sequence: Sequence::new(1).unwrap(),
                },
            },
            "dictionary" => IngestKey::Dictionary {
                reference: QualifiedRef {
                    dataset_id: manifest.identity().dataset_id,
                    dictionary_kind: DictionaryKind::Observer,
                    allocation_domain: [34; 16].try_into().unwrap(),
                    ordinal: Ordinal::new(1).unwrap(),
                },
            },
            _ => unreachable!(),
        }
        .encode()
        .unwrap();
        store
            .db
            .put_cf(
                store.db.cf_handle(key.column_family().name()).unwrap(),
                key.as_bytes(),
                b"conflicting retained state",
            )
            .unwrap();
        drop(store);
        let mut recovery = IngestRecovery::open(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        let handoff = plan(&mut recovery, &budget, 1);
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        assert!(
            matches!(
                recovery.apply_initial_handoff(&handoff, grant(&budget, 8192, 8192), || Ok(())),
                Err(BootstrapError::StateMismatch)
            ),
            "{fault}"
        );
        assert_eq!(kv(&recovery.store), before);
        assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    }
}

#[test]
fn initial_handoff_requires_explicit_successor_identity_and_fresh_namespaces() {
    let (root, store, budget, manifest, native) = setup(false);
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    let proof = proof(&mut recovery, &budget, 1);
    let valid = target(manifest);
    for fault in [
        "schema",
        "epoch",
        "topology",
        "incarnation",
        "dataset",
        "resource",
        "shard",
        "recovery",
        "old-domain",
        "duplicate-domain",
        "missing-kind",
    ] {
        let mut identity = *valid.identity();
        let mut namespaces = *valid.allocation_namespaces();
        match fault {
            "epoch" => identity.owner_epoch = OwnerEpoch::new(3).unwrap(),
            "topology" => identity.topology_generation = TopologyGeneration::new(3),
            "incarnation" => identity.process_incarnation = manifest.identity().process_incarnation,
            "dataset" => identity.dataset_id = [21; 16].try_into().unwrap(),
            "resource" => identity.owner_resource_id = [22; 16].try_into().unwrap(),
            "shard" => identity.shard_id = 1,
            "recovery" => identity.recovery_generation = RecoveryGenerationId::from_bytes([23; 16]),
            "old-domain" => namespaces = *manifest.allocation_namespaces(),
            "duplicate-domain" => {
                let domains: Vec<_> = (1..=9)
                    .filter_map(|n| {
                        let kind = DictionaryKind::try_from(n).unwrap();
                        namespaces
                            .domain(kind)
                            .map(|_| (kind, [55; 16].try_into().unwrap()))
                    })
                    .collect();
                namespaces = AllocationNamespaces::from_entries(&domains).unwrap();
            }
            "missing-kind" => namespaces = AllocationNamespaces::from_entries(&[]).unwrap(),
            _ => {}
        }
        let next = if fault == "schema" {
            IngestManifest::new(identity, namespaces)
        } else {
            IngestManifest::for_successor(identity, namespaces)
        };
        assert!(
            matches!(
                InitialHandoff::from_source(&proof, next),
                Err(BootstrapError::InvalidIntent)
            ),
            "{fault}"
        );
    }
}
