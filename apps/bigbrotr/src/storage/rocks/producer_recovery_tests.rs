//! Resolution observes actual closed stores without manufacturing a new request.
use super::*;
use bigbrotr_model::codec::{FactDecodeLimits, request::RequestDecodeLimits};

pub(super) fn recovery_limits() -> ProducerRecoveryLimits {
    ProducerRecoveryLimits {
        input: RequestDecodeLimits {
            values: FactDecodeLimits {
                max_encoded_bytes: MIB as usize,
                max_allocation_bytes: MIB as usize,
                max_tag_nodes: 1024,
                max_string_bytes: MIB as usize,
            },
            max_dictionaries: 16,
        },
        bootstrap: BootstrapLimits {
            max_mappings: 16,
            max_batch_bytes: MIB as usize,
        },
        event: AdmissionLimits::default(),
    }
}

#[test]
fn recovery_resolves_signed_originals_after_closing_without_native_mutation() {
    let (root, mut store, budget, manifest, native) = setup(false);
    drop(admit(&mut store, &budget, 1).unwrap());
    drop(admit(&mut store, &budget, 2).unwrap());
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    drop(
        recovery
            .close_producer(
                request(5, 1).producer_slot_id,
                ProducerEpoch::new(1).unwrap(),
                grant(&budget, 4096, 4096),
                || Ok(()),
            )
            .unwrap(),
    );
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    for n in [1, 2, 1] {
        let id = request(5, n);
        let digest = intent("E_private", 2, 10_000_000).request_digest(id);
        let resolved = recovery
            .resolve_original_request(
                id,
                digest,
                recovery_limits(),
                grant(&budget, 3 * MIB, 0),
                grant(&budget, 40 * MIB, 0),
                || Ok(()),
            )
            .unwrap();
        let AdmissionOutcome::Durable(receipt) = resolved.outcome() else {
            panic!("signed original")
        };
        assert_eq!(receipt.request_id(), id);
        assert_eq!(receipt.request_digest(), digest);
        assert_eq!(receipt.batch().is_some(), n == 1);
        assert_eq!(receipt.protection_context().policy_epoch.get(), 1);
        // Public visibility is also needed by the private scope/observer
        // dictionary envelopes; lineage record4 is unrelated to network input.
        assert_eq!(
            receipt
                .dependencies()
                .records()
                .iter()
                .map(|r| r.source_record_id.sequence.get())
                .collect::<Vec<_>>(),
            [1, 2, 3, 5, 6, 7, 8]
        );
        if let Some(batch) = receipt.batch() {
            assert_eq!(batch.first().sequence.get(), 5);
            assert_eq!(batch.last().sequence.get(), 8);
        }
        assert_eq!(resolved.native_outcome().request(), id);
        assert_eq!(kv(&recovery.store), before);
        assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    }
}

#[test]
fn recovery_refuses_an_existing_receipt_that_omits_original_event_effects() {
    let (root, mut store, budget, manifest, native) = setup(false);
    drop(admit(&mut store, &budget, 1).unwrap());
    let result = admit(&mut store, &budget, 2).unwrap();
    let AdmissionOutcome::Durable(receipt) = result.outcome() else {
        panic!()
    };
    let id = request(5, 2);
    // A structurally valid receipt whose only dependency exists but proves no
    // event payload, eligibility or receive effect for this signed request.
    let damaged = Receipt::already_durable(
        id,
        receipt.request_digest(),
        *receipt.protection_context(),
        SourceDependencies::new(
            manifest.identity().dataset_id,
            vec![facts(&store)[0].fact_ref()],
        )
        .unwrap(),
    )
    .unwrap();
    drop(result);
    let key = crate::storage::rocks::keys::IngestKey::Request { request_id: id }
        .encode()
        .unwrap();
    store
        .db
        .put_cf(
            store.db.cf_handle("receipts").unwrap(),
            key.as_bytes(),
            damaged.encode_persistent(8192).unwrap(),
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
    drop(
        recovery
            .close_producer(
                id.producer_slot_id,
                id.producer_epoch,
                grant(&budget, 4096, 4096),
                || Ok(()),
            )
            .unwrap(),
    );
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    assert!(matches!(
        recovery.resolve_original_request(
            id,
            damaged.request_digest(),
            recovery_limits(),
            grant(&budget, 3 * MIB, 0),
            grant(&budget, 40 * MIB, 0),
            || Ok(())
        ),
        Err(ProducerRecoveryError::Native(AdmissionError::StateMismatch))
    ));
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
}

fn close_for_test(recovery: &mut IngestRecovery, budget: &ResourceBudget, slot: u8) {
    drop(
        recovery
            .close_producer(
                request(slot, 1).producer_slot_id,
                ProducerEpoch::new(1).unwrap(),
                grant(budget, 4096, 4096),
                || Ok(()),
            )
            .unwrap(),
    );
}
fn recover(
    recovery: &IngestRecovery,
    budget: &ResourceBudget,
    id: ProducerRequestId,
    digest: Digest256,
) -> Result<ResolvedProducerRequest, ProducerRecoveryError> {
    recovery.resolve_original_request(
        id,
        digest,
        recovery_limits(),
        grant(budget, 3 * MIB, 0),
        grant(budget, 40 * MIB, 0),
        || Ok(()),
    )
}
fn input_files(root: &std::path::Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut files: Vec<_> = std::fs::read_dir(root.join("db/producer-inputs-v1"))
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (e.file_name(), std::fs::read(e.path()).unwrap())
        })
        .collect();
    files.sort();
    files
}

#[test]
fn recovery_preserves_bootstrap_and_rejection_bytes_after_reopen() {
    let (root, mut store, budget, manifest, native) = setup(false);
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/signed-event-admission.json"
    ))
    .unwrap();
    let mut input = fixture["scenarios"][0]["operations"][4]["input"].clone();
    input["observation"]["witness"] = serde_json::Value::Null;
    let invalid = fixture_intent(&input);
    let id = request(5, 1);
    let digest = invalid.request_digest(id);
    let prepared = PreparedEvent::new(id, invalid, 256 * 1024, grant(&budget, MIB, 0)).unwrap();
    drop(
        store
            .admit_event(
                prepared,
                ObservedTime::new(55),
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
    close_for_test(&mut recovery, &budget, 5);
    close_for_test(&mut recovery, &budget, 7);
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let inputs = input_files(root.path());
    let result = recover(&recovery, &budget, id, digest).unwrap();
    assert!(matches!(
        result.outcome(),
        AdmissionOutcome::Rejected(RejectionReason::Signature)
    ));
    assert_eq!(result.native_outcome().kind(), NativeOutcomeKind::Rejected);
    let golden: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/rejected-request.json"
    ))
    .unwrap();
    assert_eq!(
        result
            .native_outcome()
            .bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        golden["stored_value_hex"].as_str().unwrap()
    );
    assert!(
        inputs
            .iter()
            .any(|(_, b)| b.len() == result.input_bytes() as usize)
    );
    drop(result);
    let bootstrap = EvidenceIntent::new(
        1,
        reference(DictionaryKind::VisibilityScope, 1),
        entries(false),
        EvidenceIntentLimits {
            max_entries: 16,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap();
    let result = recover(
        &recovery,
        &budget,
        request(7, 1),
        bootstrap.request_digest(request(7, 1)),
    )
    .unwrap();
    let AdmissionOutcome::Durable(receipt) = result.outcome() else {
        panic!()
    };
    assert_eq!(receipt.batch().unwrap().first().sequence.get(), 1);
    assert_eq!(receipt.batch().unwrap().last().sequence.get(), 4);
    assert_eq!(receipt.dependencies().records().len(), 4);
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
    assert_eq!(input_files(root.path()), inputs);
}

#[test]
fn recovery_uses_historical_dependencies_after_a_newer_receive_correction() {
    let (root, mut store, budget, manifest, native) = setup(false);
    let original = admit(&mut store, &budget, 1)
        .unwrap()
        .native_outcome()
        .bytes()
        .to_vec();
    let earlier = PreparedEvent::new(
        request(5, 2),
        intent("E_private", 2, 9_000_000),
        256 * 1024,
        grant(&budget, MIB, 0),
    )
    .unwrap();
    drop(
        store
            .admit_event(
                earlier,
                ObservedTime::new(20_000_000),
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
    close_for_test(&mut recovery, &budget, 5);
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let result = recover(
        &recovery,
        &budget,
        request(5, 1),
        intent("E_private", 2, 10_000_000).request_digest(request(5, 1)),
    )
    .unwrap();
    assert_eq!(result.native_outcome().bytes(), original);
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
}

#[test]
fn recovery_refuses_foreign_context_and_incomplete_dependency_closure() {
    use crate::storage::rocks::keys::IngestKey;
    for fault in [
        "topology",
        "recovery-generation",
        "scope-dictionary",
        "pending",
        "fact",
    ] {
        let (root, mut store, budget, manifest, native) = setup(false);
        let result = admit(&mut store, &budget, 1).unwrap();
        let AdmissionOutcome::Durable(receipt) = result.outcome() else {
            panic!()
        };
        let digest = receipt.request_digest();
        let mut context = *receipt.protection_context();
        match fault {
            "topology" => context.topology_generation = TopologyGeneration::new(99),
            "recovery-generation" => {
                context.recovery_generation = RecoveryGenerationId::from_bytes([99; 16])
            }
            _ => {}
        }
        let dependencies = SourceDependencies::new(
            manifest.identity().dataset_id,
            receipt
                .dependencies()
                .records()
                .iter()
                .copied()
                .filter(|r| fault != "scope-dictionary" || r.source_record_id.sequence.get() != 1)
                .collect(),
        )
        .unwrap();
        let batch = receipt.batch().unwrap().clone();
        let first = batch.first();
        let damaged =
            Receipt::new_facts(request(5, 1), digest, context, batch, dependencies).unwrap();
        drop(result);
        let key = IngestKey::Request {
            request_id: request(5, 1),
        }
        .encode()
        .unwrap();
        store
            .db
            .put_cf(
                store.db.cf_handle("receipts").unwrap(),
                key.as_bytes(),
                damaged.encode_persistent(8192).unwrap(),
            )
            .unwrap();
        if fault == "pending" || fault == "fact" {
            let key = if fault == "pending" {
                IngestKey::PendingBatch { first }
            } else {
                IngestKey::Fact {
                    source_record_id: first,
                }
            }
            .encode()
            .unwrap();
            store
                .db
                .delete_cf(
                    store.db.cf_handle(key.column_family().name()).unwrap(),
                    key.as_bytes(),
                )
                .unwrap();
        }
        drop(store);
        let mut recovery = IngestRecovery::open(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        close_for_test(&mut recovery, &budget, 5);
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        let error = recover(&recovery, &budget, request(5, 1), digest)
            .err()
            .unwrap();
        assert_eq!(
            error,
            if matches!(fault, "topology" | "recovery-generation") {
                ProducerRecoveryError::UnsupportedContext
            } else {
                ProducerRecoveryError::Native(AdmissionError::StateMismatch)
            },
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
fn recovery_missing_input_or_detail_never_implies_a_negative_outcome() {
    use crate::storage::rocks::keys::IngestKey;
    for fault in [
        "open",
        "future",
        "detail",
        "input",
        "partial-input",
        "digest",
    ] {
        let (root, mut store, budget, manifest, native) = setup(false);
        drop(admit(&mut store, &budget, 1).unwrap());
        drop(store);
        let mut recovery = IngestRecovery::open(
            &root.path().join("db"),
            manifest,
            native,
            grant(&budget, 0, 128 * MIB),
        )
        .unwrap();
        if fault != "open" {
            close_for_test(&mut recovery, &budget, 5);
        }
        let id = request(5, if fault == "future" { 2 } else { 1 });
        let digest = if fault == "digest" {
            Digest256::from_bytes([99; 32])
        } else {
            intent("E_private", 2, 10_000_000).request_digest(id)
        };
        if fault == "detail" {
            let key = IngestKey::Request { request_id: id }.encode().unwrap();
            recovery
                .store
                .db
                .delete_cf(
                    recovery.store.db.cf_handle("receipts").unwrap(),
                    key.as_bytes(),
                )
                .unwrap();
        }
        if fault == "input" || fault == "partial-input" {
            let file = root.path().join("db/producer-inputs-v1").join(format!(
                "{}-0000000000000001-0000000000000001.cbor",
                id.producer_slot_id
            ));
            if fault == "input" {
                std::fs::remove_file(file).unwrap();
            } else {
                std::fs::write(file, []).unwrap();
            }
        }
        let before = kv(&recovery.store);
        let sequence = recovery.store.db.latest_sequence_number();
        let inputs = input_files(root.path());
        let result = recover(&recovery, &budget, id, digest);
        assert!(
            match fault {
                "open" => matches!(result, Err(ProducerRecoveryError::ProducerOpen)),
                "future" | "detail" => matches!(result, Err(ProducerRecoveryError::Unresolved)),
                "input" => matches!(
                    result,
                    Err(ProducerRecoveryError::Input(RequestInputError::Missing))
                ),
                "partial-input" => matches!(
                    result,
                    Err(ProducerRecoveryError::Input(RequestInputError::Incomplete))
                ),
                "digest" => matches!(
                    result,
                    Err(ProducerRecoveryError::Input(RequestInputError::Codec(
                        bigbrotr_model::codec::CodecError::DigestMismatch
                    )))
                ),
                _ => unreachable!(),
            },
            "{fault}"
        );
        assert_eq!(kv(&recovery.store), before, "{fault}");
        assert_eq!(
            recovery.store.db.latest_sequence_number(),
            sequence,
            "{fault}"
        );
        assert_eq!(input_files(root.path()), inputs, "{fault}");
    }
}

#[test]
fn recovery_resource_and_authority_refusals_preserve_identical_retry() {
    let (root, mut store, budget, manifest, native) = setup(false);
    drop(admit(&mut store, &budget, 1).unwrap());
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    close_for_test(&mut recovery, &budget, 5);
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let id = request(5, 1);
    let digest = intent("E_private", 2, 10_000_000).request_digest(id);
    for fault in [
        "input-limit",
        "receipt-limit",
        "grant",
        "authority1",
        "authority2",
        "authority3",
    ] {
        let mut limits = recovery_limits();
        if fault == "input-limit" {
            limits.input.values.max_encoded_bytes = 1;
        }
        if fault == "receipt-limit" {
            limits.event.max_batch_bytes = 100;
        }
        let mut calls = 0;
        let result = recovery.resolve_original_request(
            id,
            digest,
            limits,
            grant(&budget, 3 * MIB, 0),
            grant(&budget, if fault == "grant" { 1 } else { 40 * MIB }, 0),
            || {
                calls += 1;
                if fault == format!("authority{calls}") {
                    Err(AdmissionError::AuthorityUnavailable)
                } else {
                    Ok(())
                }
            },
        );
        assert!(
            match fault {
                "input-limit" => matches!(
                    result,
                    Err(ProducerRecoveryError::Input(RequestInputError::Codec(
                        bigbrotr_model::codec::CodecError::InputLimit
                    )))
                ),
                "receipt-limit" => matches!(
                    result,
                    Err(ProducerRecoveryError::Native(AdmissionError::Limit))
                ),
                "grant" =>
                    calls == 0
                        && matches!(
                            result,
                            Err(ProducerRecoveryError::Native(
                                AdmissionError::InsufficientReservation
                            ))
                        ),
                _ => matches!(
                    result,
                    Err(ProducerRecoveryError::Native(
                        AdmissionError::AuthorityUnavailable
                    ))
                ),
            },
            "{fault}"
        );
        drop(recover(&recovery, &budget, id, digest).unwrap());
        assert_eq!(kv(&recovery.store), before, "{fault}");
        assert_eq!(
            recovery.store.db.latest_sequence_number(),
            sequence,
            "{fault}"
        );
    }
}

#[test]
fn original_success_survives_a_smaller_retry_verification_limit_without_input_sidecar() {
    let (root, mut store, budget, _manifest, _native) = setup(false);
    let original = admit(&mut store, &budget, 1)
        .unwrap()
        .native_outcome()
        .bytes()
        .to_vec();
    // Historical open replay never required recreating a missing producer input.
    std::fs::remove_file(root.path().join("db/producer-inputs-v1").join(format!(
        "{}-0000000000000001-0000000000000001.cbor",
        request(5, 1).producer_slot_id
    )))
    .unwrap();
    let before = kv(&store);
    let sequence = store.db.latest_sequence_number();
    let input = PreparedEvent::new(
        request(5, 1),
        intent("E_private", 2, 10_000_000),
        1,
        grant(&budget, MIB, 0),
    )
    .unwrap();
    assert!(input.verified_event().is_none());
    let outcome = store
        .admit_event(
            input,
            ObservedTime::new(999),
            false,
            AdmissionLimits::default(),
            grant(&budget, 40 * MIB, 10 * MIB),
            || Ok(()),
        )
        .unwrap();
    assert_eq!(outcome.native_outcome().bytes(), original);
    assert_eq!(kv(&store), before);
    assert_eq!(store.db.latest_sequence_number(), sequence);
    assert_eq!(input_files(root.path()).len(), 1);
}

#[test]
fn recovered_receipt_preserves_its_original_full_width_policy_epoch() {
    let (root, mut store, budget, manifest, native) = setup(false);
    drop(admit(&mut store, &budget, 1).unwrap());
    let original = admit(&mut store, &budget, 2).unwrap();
    let AdmissionOutcome::Durable(receipt) = original.outcome() else {
        panic!()
    };
    let mut context = *receipt.protection_context();
    context.policy_epoch = PolicyEpoch::new(u64::MAX);
    let historic = Receipt::already_durable(
        receipt.request_id(),
        receipt.request_digest(),
        context,
        receipt.dependencies().clone(),
    )
    .unwrap();
    let bytes = historic.encode_persistent(8192).unwrap();
    let key = crate::storage::rocks::keys::IngestKey::Request {
        request_id: historic.request_id(),
    }
    .encode()
    .unwrap();
    store
        .db
        .put_cf(
            store.db.cf_handle("receipts").unwrap(),
            key.as_bytes(),
            &bytes,
        )
        .unwrap();
    drop(original);
    drop(store);
    let mut recovery = IngestRecovery::open(
        &root.path().join("db"),
        manifest,
        native,
        grant(&budget, 0, 128 * MIB),
    )
    .unwrap();
    close_for_test(&mut recovery, &budget, 5);
    let before = kv(&recovery.store);
    let sequence = recovery.store.db.latest_sequence_number();
    let recovered = recover(
        &recovery,
        &budget,
        historic.request_id(),
        historic.request_digest(),
    )
    .unwrap();
    assert_eq!(recovered.native_outcome().policy_epoch().get(), u64::MAX);
    assert_eq!(recovered.native_outcome().bytes(), bytes);
    assert_eq!(kv(&recovery.store), before);
    assert_eq!(recovery.store.db.latest_sequence_number(), sequence);
}

#[test]
fn size_limited_replay_charges_reconstruction_to_the_retained_input_grant() {
    let (_root, mut store, budget, _manifest, _native) = setup(false);
    let original = admit(&mut store, &budget, 1).unwrap();
    let id = request(5, 1);
    let intent = intent("E_private", 2, 10_000_000);
    let digest = intent.request_digest(id);
    let bytes = intent.encode_request_preimage(id, 8192).unwrap();
    let before = kv(&store);
    let sequence = store.db.latest_sequence_number();
    for (allowance, success) in [(0, false), (MIB as usize, true)] {
        let limits = AdmissionLimits::default();
        let result = store.replay_event(
            id,
            digest,
            None,
            Some((&bytes, allowance)),
            limits,
            &mut super::super::admission::ReadBudget::new(limits),
        );
        if success {
            assert_eq!(
                result.unwrap().unwrap().1.bytes(),
                original.native_outcome().bytes()
            );
        } else {
            assert!(matches!(result, Err(AdmissionError::Limit)));
        }
        assert_eq!(kv(&store), before);
        assert_eq!(store.db.latest_sequence_number(), sequence);
    }
}

#[test]
fn size_limited_retry_accepts_original_request_larger_than_its_fact_limit() {
    let (_root, mut store, budget, _manifest, _native) = setup(false);
    let recipe: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/large-replay.json"
    ))
    .unwrap();
    let input = || {
        let mut event = recipe["event"].clone();
        event["content"] = serde_json::Value::String(
            recipe["content_repeat"]
                .as_str()
                .unwrap()
                .repeat(recipe["content_count"].as_u64().unwrap() as usize),
        );
        EventIntent::import(
            1,
            reference(DictionaryKind::VisibilityScope, 1),
            serde_json::from_value(event).unwrap(),
            reference(DictionaryKind::DatasetLineage, 1),
        )
        .unwrap()
    };
    let preimage = input()
        .encode_request_preimage(request(5, 1), 4 * MIB as usize)
        .unwrap();
    assert_eq!(
        preimage.len(),
        recipe["request_preimage_bytes"].as_u64().unwrap() as usize
    );
    assert_eq!(
        input().request_digest(request(5, 1)).to_string(),
        recipe["request_preimage_sha256"].as_str().unwrap()
    );
    let mut original = None;
    let mut original_state = None;
    for verification_limit in [4 * MIB as usize, 1] {
        let prepared = PreparedEvent::new(
            request(5, 1),
            input(),
            verification_limit,
            grant(&budget, 8 * MIB, 0),
        )
        .unwrap();
        let result = store
            .admit_event(
                prepared,
                ObservedTime::new(1),
                false,
                AdmissionLimits::default(),
                grant(&budget, 40 * MIB, 10 * MIB),
                || Ok(()),
            )
            .unwrap();
        let AdmissionOutcome::Durable(_) = result.outcome() else {
            panic!()
        };
        if let Some(original) = &original {
            assert_eq!(result.native_outcome().bytes(), original);
            assert_eq!(
                Some((kv(&store), store.db.latest_sequence_number())),
                original_state
            );
        } else {
            use sha2::{Digest, Sha256};
            let key = crate::storage::rocks::keys::IngestKey::Fact {
                source_record_id: SourceRecordId {
                    dataset_id: reference(DictionaryKind::VisibilityScope, 1).dataset_id,
                    shard_id: 0,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                    sequence: Sequence::new(6).unwrap(),
                },
            }
            .encode()
            .unwrap();
            let payload = store
                .db
                .get_cf(store.db.cf_handle("facts").unwrap(), key.as_bytes())
                .unwrap()
                .unwrap();
            assert_eq!(&payload[..2], &[1, 32]);
            assert_eq!(
                payload.len() - 34,
                recipe["payload_fact_bytes"].as_u64().unwrap() as usize
            );
            assert_eq!(
                Digest256::from_bytes(Sha256::digest(&payload[34..]).into()).to_string(),
                recipe["payload_fact_sha256"].as_str().unwrap()
            );
            original = Some(result.native_outcome().bytes().to_vec());
            original_state = Some((kv(&store), store.db.latest_sequence_number()));
        }
    }
}
