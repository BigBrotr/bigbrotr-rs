//! Future epoch3 bytes are explicit test-only retained-state fixtures. They do
//! not exercise preparation, SQL authority or a future native writer API.
use super::super::super::super::{admission::ReadBudget, receipt_effects::ReplayEventIntent};
use super::*;
use crate::storage::rocks::store::{LineageResolution, ResolvedLineage};
use std::os::unix::fs::PermissionsExt;
static NOT_CANCELLED: AtomicBool = AtomicBool::new(false);

#[path = "compacted_ancestor_tests.rs"]
mod compacted_ancestor_tests;

#[path = "successor_handoff_tests.rs"]
mod successor_handoff_tests;

#[path = "lineage_pending_tests.rs"]
mod lineage_pending_tests;

fn stored_input(f: &Fixture, epoch: u64, sequence: u64, bytes: &[u8]) {
    let path = f
        .path()
        .join("producer-inputs-v1")
        .join(input_name(epoch, sequence));
    std::fs::write(&path, bytes).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}
fn at(epoch: u64, sequence: u64) -> ProducerRequestId {
    ProducerRequestId {
        producer_epoch: ProducerEpoch::new(epoch).unwrap(),
        ..request(7, sequence)
    }
}
fn context(manifest: IngestManifest) -> ProtectionContext {
    let id = manifest.identity();
    ProtectionContext {
        dataset_id: id.dataset_id,
        topology_generation: id.topology_generation,
        recovery_generation: id.recovery_generation,
        policy_epoch: id.policy_epoch,
        protection_class: ProtectionClass::LocalSynced,
    }
}
fn epoch_digest(
    f: &Fixture,
    r: &IngestRecovery,
    epoch: u64,
    count: u64,
    cut: u64,
    empty: bool,
) -> [u8; 32] {
    let domain: &[u8] = match (epoch, empty) {
        (2, true) => b"bigbrotr/initial-empty-attempt-closure/1\0",
        (2, false) => b"bigbrotr/activated-initial-source-closure/1\0",
        (_, true) => b"bigbrotr/ingest-successor-empty-closure/1\0",
        (_, false) => b"bigbrotr/ingest-successor-source-closure/1\0",
    };
    let key = IngestKey::OwnerHandoff {
        dataset_id: f.previous.identity().dataset_id,
        owner_resource_id: f.previous.identity().owner_resource_id,
        predecessor_epoch: OwnerEpoch::new(epoch - 1).unwrap(),
    };
    let incoming = value(&r.store, key);
    let manifest = if epoch == 2 {
        *InitialHandoffRecord::decode(&incoming)
            .unwrap()
            .next_manifest()
    } else {
        SuccessionRecord::decode(&incoming)
            .unwrap()
            .fields()
            .next_manifest
    };
    let mut bytes = domain.to_vec();
    bytes.extend(Sha256::digest(incoming));
    bytes.extend(Sha256::digest(manifest.encode().as_bytes()));
    bytes.extend([7; 16]);
    bytes.extend(epoch.to_be_bytes());
    bytes.extend(count.to_be_bytes());
    for sequence in 1..=count {
        bytes.extend(sequence.to_be_bytes());
        bytes.extend(Sha256::digest(
            std::fs::read(
                f.path()
                    .join("producer-inputs-v1")
                    .join(input_name(epoch, sequence)),
            )
            .unwrap(),
        ));
        bytes.extend(Sha256::digest(value(
            &r.store,
            IngestKey::Request {
                request_id: at(epoch, sequence),
            },
        )));
    }
    bytes.extend(cut.to_be_bytes());
    Sha256::digest(bytes).into()
}
fn valid_empty_third(f: &Fixture) -> IngestRecovery {
    let r = f.reopen();
    let digest = epoch_digest(f, &r, 2, 0, 0, true);
    drop(r);
    let r = extend_to(f, 3);
    let key = IngestKey::OwnerHandoff {
        dataset_id: f.previous.identity().dataset_id,
        owner_resource_id: f.previous.identity().owner_resource_id,
        predecessor_epoch: OwnerEpoch::new(2).unwrap(),
    };
    let mut fields = *SuccessionRecord::decode(&value(&r.store, key))
        .unwrap()
        .fields();
    fields.closure_digest = digest;
    put(
        &r.store,
        key,
        SuccessionRecord::new(fields).unwrap().encode().as_bytes(),
    );
    r
}
fn try_resolve_one(
    f: &Fixture,
    p: &LineageResolution<'_>,
) -> Result<ResolvedProducerRequest, ProducerRecoveryError> {
    let id = p.next_request().unwrap();
    let input = std::fs::read(f.path().join("producer-inputs-v1").join(input_name(
        id.producer_epoch.get(),
        id.request_sequence.get(),
    )))
    .unwrap();
    p.resolve_next(
        Digest256::from_bytes(Sha256::digest(input).into()),
        recovery_limits(),
        grant(&f.budget, 3 * MIB, 0),
        grant(&f.budget, 40 * MIB, 0),
        || Ok(()),
    )
}
fn resolve_one(f: &Fixture, p: &LineageResolution<'_>) -> ResolvedProducerRequest {
    try_resolve_one(f, p).unwrap()
}
fn replay<'a>(
    f: &Fixture,
    r: &'a IngestRecovery,
    l: &'a RetainedLineage,
    through: u64,
    kind: Option<PredecessorTerminalKind>,
) -> Result<ResolvedLineage<'a>, String> {
    let mut p = l
        .resolution(r, through, kind, &NOT_CANCELLED)
        .map_err(|e| format!("start: {e:?}"))?;
    while p.current_epoch().is_some() {
        while p.next_request().is_some() {
            let outcome = try_resolve_one(f, &p).map_err(|e| format!("resolve: {e:?}"))?;
            p.record(&outcome).map_err(|e| format!("record: {e:?}"))?;
        }
        p.finish_epoch().map_err(|e| format!("epoch: {e:?}"))?;
    }
    p.finish(AdmissionLimits::default(), grant(&f.budget, 40 * MIB, 0))
        .map_err(|e| format!("census: {e:?}"))
}

fn dictionary_bytes(
    r: &IngestRecovery,
    entry: DictionaryEntry,
    scope: QualifiedRef,
    sequence: u64,
) -> FactRef {
    let reference = entry.reference();
    let descriptor = entry.encode_descriptor(8192).unwrap();
    let fact = Fact::new(
        fact_id(3, sequence),
        scope,
        ObservedTime::new(77),
        FactPayload::Dictionary(entry),
    )
    .unwrap();
    let mut value = vec![1, 0x20];
    value.extend(fact.payload_digest().as_bytes());
    value.extend(fact.encode_preimage(8192).unwrap());
    put(
        &r.store,
        IngestKey::Fact {
            source_record_id: fact.source_record_id(),
        },
        &value,
    );
    let mut pointer = vec![1, 4];
    pointer.extend(
        &IngestKey::Fact {
            source_record_id: fact.source_record_id(),
        }
        .encode()
        .unwrap()
        .as_bytes()[2..],
    );
    pointer.extend(fact.payload_digest().as_bytes());
    put(&r.store, IngestKey::Dictionary { reference }, &pointer);
    let key = IngestKey::DictionaryReverseCandidate {
        dataset_id: reference.dataset_id,
        dictionary_kind: reference.dictionary_kind,
        descriptor_digest: Digest256::from_bytes(Sha256::digest(&descriptor).into()),
        reference,
    };
    let mut reverse = vec![1, 5];
    reverse.extend(descriptor);
    put(&r.store, key, &reverse);
    let mut high = vec![1, 3];
    high.extend(reference.ordinal.get().to_be_bytes());
    put(
        &r.store,
        IngestKey::AllocationStream {
            dataset_id: reference.dataset_id,
            dictionary_kind: reference.dictionary_kind,
            allocation_domain: reference.allocation_domain,
        },
        &high,
    );
    fact.fact_ref()
}

fn target_scope(r: &IngestRecovery) -> QualifiedRef {
    QualifiedRef {
        allocation_domain: r
            .manifest()
            .allocation_namespaces()
            .domain(DictionaryKind::VisibilityScope)
            .unwrap(),
        ..reference(DictionaryKind::VisibilityScope, 1)
    }
}

#[test]
fn lineage_evidence_request_one_uses_append_and_retains_equal_old_descriptors() {
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    let scope = target_scope(&r);
    // Public exists in epoch1 too. The namespace-qualified new mapping is valid;
    // reverse candidates are unique per reference, not globally by descriptor.
    let entry = DictionaryEntry::new(
        scope,
        DictionaryDescriptor::Visibility {
            class: label("public"),
        },
    )
    .unwrap();
    let input = EvidenceIntent::new(
        1,
        scope,
        vec![entry.clone()],
        EvidenceIntentLimits {
            max_entries: 8,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap();
    let fact = dictionary_bytes(&r, entry, scope, 1);
    let batch = AdmissionBatch::new(vec![fact]).unwrap();
    let dependencies = SourceDependencies::new(scope.dataset_id, vec![fact]).unwrap();
    for sequence in [1, 2] {
        let id = at(3, sequence);
        let receipt = if sequence == 1 {
            Receipt::new_facts(
                id,
                input.request_digest(id),
                context(*r.manifest()),
                batch.clone(),
                dependencies.clone(),
            )
        } else {
            Receipt::already_durable(
                id,
                input.request_digest(id),
                context(*r.manifest()),
                dependencies.clone(),
            )
        }
        .unwrap();
        put(
            &r.store,
            IngestKey::Request { request_id: id },
            &receipt.encode_persistent(8192).unwrap(),
        );
        stored_input(
            &f,
            3,
            sequence,
            &input.encode_request_preimage(id, 8192).unwrap(),
        );
    }
    put(
        &r.store,
        IngestKey::PendingBatch {
            first: fact_id(3, 1),
        },
        &PendingBatchHeader::from_batch(&batch).encode(),
    );
    put(&r.store, source_key(3), &[1, 2, 0, 0, 0, 0, 0, 0, 0, 1]);
    put(&r.store, producer_key(3), &marker(true, 2));
    let before = kv(&r.store);
    let sequence = r.store.db.latest_sequence_number();
    let originals = inputs(&f.path());
    let l = inspect(&f, &r, None).unwrap();
    let p = replay(&f, &r, &l, 3, Some(PredecessorTerminalKind::Activated)).unwrap();
    assert_eq!(
        (
            p.last_epoch().resolved_requests,
            p.last_epoch().last_complete_sequence,
            p.last_epoch().event_request_mask
        ),
        (2, 1, 0)
    );
    assert_eq!(p.last_epoch().digest, epoch_digest(&f, &r, 3, 2, 1, false));
    assert_eq!(kv(&r.store), before);
    assert_eq!(inputs(&f.path()), originals);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
}

#[test]
fn lineage_event_batch_refuses_a_valid_fact_unrelated_to_its_original_input() {
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    correction(&f, &r, false);
    let before_receipt = value(
        &r.store,
        IngestKey::Request {
            request_id: at(3, 1),
        },
    );
    let receipt = Receipt::decode_persistent(
        &before_receipt,
        bigbrotr_model::receipt_codec::ReceiptDecodeLimits {
            max_encoded_bytes: 8192,
            max_allocation_bytes: 8192,
            max_batch_records: 64,
            max_dependencies: 64,
        },
    )
    .unwrap();
    let extra = dictionary_bytes(
        &r,
        DictionaryEntry::new(
            target_scope(&r),
            DictionaryDescriptor::Visibility {
                class: label("unrelated"),
            },
        )
        .unwrap(),
        reference(DictionaryKind::VisibilityScope, 2),
        2,
    );
    let mut members = receipt.batch().unwrap().records().to_vec();
    members.push(extra);
    let batch = AdmissionBatch::new(members).unwrap();
    let mut dependencies = receipt.dependencies().records().to_vec();
    dependencies.push(extra);
    let altered = Receipt::new_facts(
        at(3, 1),
        receipt.request_digest(),
        *receipt.protection_context(),
        batch.clone(),
        SourceDependencies::new(f.previous.identity().dataset_id, dependencies).unwrap(),
    )
    .unwrap();
    put(
        &r.store,
        IngestKey::Request {
            request_id: at(3, 1),
        },
        &altered.encode_persistent(8192).unwrap(),
    );
    put(
        &r.store,
        IngestKey::PendingBatch {
            first: fact_id(3, 1),
        },
        &PendingBatchHeader::from_batch(&batch).encode(),
    );
    put(&r.store, source_key(3), &[1, 2, 0, 0, 0, 0, 0, 0, 0, 2]);
    let l = inspect(&f, &r, None).unwrap();
    let before = kv(&r.store);
    let original = inputs(&f.path());
    // Pin the old boundary: all original requested effects are present. The
    // new lineage path must additionally reject the unrelated NewFacts member.
    let event = fixture_event("E_private");
    let input = intent("E_private", 2, 5_000_000);
    r.store
        .require_receipt_effects(
            &altered,
            ReplayEventIntent {
                event: &event,
                scope: input.visibility_scope_id(),
                origin: input.origin(),
                observation: input.observation().copied(),
            },
            AdmissionLimits::default(),
            &mut ReadBudget::new(AdmissionLimits::default()),
        )
        .unwrap();
    let error = match replay(&f, &r, &l, 3, Some(PredecessorTerminalKind::Activated)) {
        Ok(_) => panic!("unrelated member"),
        Err(e) => e,
    };
    assert!(error.starts_with("resolve:"), "{error}");
    assert_eq!(kv(&r.store), before);
    assert_eq!(inputs(&f.path()), original);
}

#[test]
fn lineage_replays_zero_source_rejections_without_inventing_empty_history() {
    let f = zero_fixture(1);
    let r = f.reopen();
    let l = inspect(&f, &r, None).unwrap();
    let prefix = replay(&f, &r, &l, 1, None).unwrap();
    assert_eq!(
        (
            prefix.last_epoch().resolved_requests,
            prefix.last_epoch().last_complete_sequence,
            prefix.last_epoch().event_request_mask
        ),
        (1, 0, 1)
    );
    assert_eq!(prefix.last_epoch().digest, f.closure);
    drop(prefix);
    drop(l);
    drop(r);
    let r = valid_empty_third(&f);
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/ingest/signed-event-admission.json"
    ))
    .unwrap();
    let mut raw = corpus["scenarios"][0]["operations"][4]["input"].clone();
    raw["observation"]["witness"] = serde_json::Value::Null;
    let input = fixture_intent(&raw);
    let id = at(3, 1);
    let bytes = input.encode_request_preimage(id, 8192).unwrap();
    let mut rejection = b"bigbrotr/rejection/1\0".to_vec();
    rejection.extend([7; 16]);
    rejection.extend(3_u64.to_be_bytes());
    rejection.extend(1_u64.to_be_bytes());
    rejection.extend(Sha256::digest(&bytes));
    rejection.extend(f.previous.identity().dataset_id.as_bytes());
    rejection.extend(1_u64.to_be_bytes());
    rejection.push(4);
    stored_input(&f, 3, 1, &bytes);
    put(&r.store, IngestKey::Request { request_id: id }, &rejection);
    put(&r.store, producer_key(3), &marker(true, 1));
    let before = kv(&r.store);
    let originals = inputs(&f.path());
    let l = inspect(&f, &r, None).unwrap();
    assert!(
        l.resolution(
            &r,
            3,
            Some(PredecessorTerminalKind::UnactivatedEmpty),
            &NOT_CANCELLED
        )
        .is_err()
    );
    let proof = replay(&f, &r, &l, 3, Some(PredecessorTerminalKind::Activated)).unwrap();
    assert_eq!(
        (
            proof.last_epoch().resolved_requests,
            proof.last_epoch().last_complete_sequence,
            proof.last_epoch().event_request_mask
        ),
        (1, 0, 1)
    );
    assert_eq!(
        proof.last_epoch().digest,
        epoch_digest(&f, &r, 3, 1, 0, false)
    );
    assert_eq!(kv(&r.store), before);
    assert_eq!(inputs(&f.path()), originals);
}

#[test]
fn lineage_record_requires_its_checked_profile_and_unchanged_input_custody() {
    let f = prepared_fixture();
    let r = f.reopen();
    let l = inspect(&f, &r, None).unwrap();
    let mut p = l.resolution(&r, 1, None, &NOT_CANCELLED).unwrap();
    let historical = r
        .historical_initial(&value(&r.store, handoff_key()))
        .unwrap();
    let legacy = historical
        .resolve_original_request(
            request(7, 1),
            input_digest(&f.originals[0]),
            recovery_limits(),
            grant(&f.budget, 3 * MIB, 0),
            grant(&f.budget, 40 * MIB, 0),
            || Ok(()),
        )
        .unwrap();
    assert!(
        p.record(&legacy).is_err(),
        "legacy closure alone did not run the new Event member checks"
    );
    assert_eq!(p.next_request(), Some(at(1, 1)));
    drop(legacy);
    drop(historical);
    let resolved = resolve_one(&f, &p);
    let old_sequence = r.store.db.latest_sequence_number();
    // Equal bytes with a different physical file must not transfer an old read
    // into a freshly inspected census. No Rocks sequence changes here.
    let replacement = f.root.path().join("replacement-input");
    std::fs::write(&replacement, &f.originals[0].input).unwrap();
    std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::rename(
        replacement,
        f.path().join("producer-inputs-v1").join(input_name(1, 1)),
    )
    .unwrap();
    assert!(p.record(&resolved).is_err());
    drop(p);
    drop(l);
    let fresh = inspect(&f, &r, None).unwrap();
    let mut p = fresh.resolution(&r, 1, None, &NOT_CANCELLED).unwrap();
    assert!(
        p.record(&resolved).is_err(),
        "an old input stamp is not the fresh input census"
    );
    assert_eq!(p.next_request(), Some(at(1, 1)));
    drop(resolved);
    let before = kv(&r.store);
    let originals = inputs(&f.path());
    let first = resolve_one(&f, &p);
    p.record(&first).unwrap();
    assert!(p.record(&first).is_err());
    assert_eq!(p.next_request(), Some(at(1, 2)));
    drop(first);
    assert!(p.finish_epoch().is_err());
    while p.next_request().is_some() {
        p.record(&resolve_one(&f, &p)).unwrap();
    }
    p.finish_epoch().unwrap();
    let limits = AdmissionLimits::default();
    let required = limits.required_dictionary_replay_data_bytes().unwrap();
    assert!(matches!(
        p.finish(limits, grant(&f.budget, required - 1, 0)),
        Err(AdmissionError::InsufficientReservation)
    ));
    let cancelled = AtomicBool::new(false);
    let mut p = fresh.resolution(&r, 1, None, &cancelled).unwrap();
    let resolved = resolve_one(&f, &p);
    cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        p.record(&resolved),
        Err(BootstrapError::AuthorityUnavailable)
    );
    assert_eq!(p.next_request(), Some(at(1, 1)));
    drop(resolved);
    drop(p);
    assert!(replay(&f, &r, &fresh, 1, None).is_ok());
    assert_eq!(kv(&r.store), before);
    assert_eq!(inputs(&f.path()), originals);
    assert_eq!(r.store.db.latest_sequence_number(), old_sequence);
}

/// One actual old receipt supplies its original old-stream closure. The fixture
/// installs a new received-at minimum at epoch3 and independent request framing.
fn correction(f: &Fixture, r: &IngestRecovery, future: bool) {
    correction_with_link(f, r, future, true);
}
fn correction_with_link(f: &Fixture, r: &IngestRecovery, future: bool, linked: bool) {
    let input = intent("E_private", 2, 5_000_000);
    let fact = Fact::new(
        fact_id(3, 1),
        reference(DictionaryKind::VisibilityScope, 2),
        ObservedTime::new(77),
        FactPayload::EventReceive(EventReceive {
            event_id: fixture_event("E_private").id,
            received_at: ObservedTime::new(5_000_000),
            observer_id: reference(DictionaryKind::Observer, 1),
            clock_quality: ClockQuality::new(
                reference(DictionaryKind::Observer, 1),
                DurationUs::new(1),
                None,
                0,
            )
            .unwrap(),
            corrects: linked.then_some(fact_id(1, 9)),
        }),
    )
    .unwrap();
    let bytes = fact.encode_preimage(8192).unwrap();
    let mut encoded = vec![1, 0x20];
    encoded.extend(fact.payload_digest().as_bytes());
    encoded.extend(bytes);
    put(
        &r.store,
        IngestKey::Fact {
            source_record_id: fact_id(3, 1),
        },
        &encoded,
    );
    let mut pointer = vec![1, 8];
    pointer.extend(
        &IngestKey::Fact {
            source_record_id: fact_id(3, 1),
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
    let mut deps = old.dependencies().records().to_vec();
    deps.push(fact.fact_ref());
    let dependencies = SourceDependencies::new(f.previous.identity().dataset_id, deps).unwrap();
    let batch = AdmissionBatch::new(vec![fact.fact_ref()]).unwrap();
    let count = if future { 2 } else { 1 };
    for sequence in 1..=count {
        let request = at(3, sequence);
        let receipt = if (future && sequence == 2) || !future {
            Receipt::new_facts(
                request,
                input.request_digest(request),
                context(*r.manifest()),
                batch.clone(),
                dependencies.clone(),
            )
        } else {
            Receipt::already_durable(
                request,
                input.request_digest(request),
                context(*r.manifest()),
                dependencies.clone(),
            )
        }
        .unwrap();
        put(
            &r.store,
            IngestKey::Request {
                request_id: request,
            },
            &receipt.encode_persistent(8192).unwrap(),
        );
        stored_input(
            f,
            3,
            sequence,
            &input.encode_request_preimage(request, 8192).unwrap(),
        );
    }
    put(
        &r.store,
        IngestKey::PendingBatch {
            first: fact_id(3, 1),
        },
        &PendingBatchHeader::from_batch(&batch).encode(),
    );
    put(&r.store, source_key(3), &[1, 2, 0, 0, 0, 0, 0, 0, 0, 1]);
    put(&r.store, producer_key(3), &marker(true, count));
}

#[test]
fn lineage_positive_frontier_uses_dependency_older_than_predecessor() {
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    correction(&f, &r, false);
    let before = kv(&r.store);
    let sequence = r.store.db.latest_sequence_number();
    let original = inputs(&f.path());
    let l = inspect(&f, &r, None).unwrap();
    let p = replay(&f, &r, &l, 3, Some(PredecessorTerminalKind::Activated)).unwrap();
    assert_eq!(p.verified_through(), 3);
    assert_eq!(p.last_epoch().resolved_requests, 1);
    assert_eq!(p.last_epoch().last_complete_sequence, 1);
    assert_eq!(p.last_epoch().event_request_mask, 1);
    assert_eq!(p.last_epoch().digest, epoch_digest(&f, &r, 3, 1, 1, false));
    assert!(p.matches_lineage(&l));
    assert_eq!(p.validate(&NOT_CANCELLED), Ok(()));
    let records = f.originals.iter().enumerate().map(|(i, o)| {
        (
            i as u64 + 1,
            Sha256::digest(&o.input).into(),
            Sha256::digest(&o.outcome).into(),
        )
    });
    assert!(p.matches_recorded(1, records));
    assert!(!p.matches_recorded(1, []));
    assert_eq!(kv(&r.store), before);
    assert_eq!(inputs(&f.path()), original);
    assert_eq!(r.store.db.latest_sequence_number(), sequence);
    assert!(
        l.resolution(&r, 2, None, &NOT_CANCELLED).is_err(),
        "nonempty head cannot supply unverified minima to ancestor-only proof"
    );
}

#[test]
fn lineage_frontier_refuses_future_dependencies_without_advancing() {
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    correction(&f, &r, true);
    let l = inspect(&f, &r, None).unwrap();
    let mut p = l
        .resolution(
            &r,
            3,
            Some(PredecessorTerminalKind::Activated),
            &NOT_CANCELLED,
        )
        .unwrap();
    for _ in 1..=2 {
        while p.next_request().is_some() {
            p.record(&resolve_one(&f, &p)).unwrap();
        }
        p.finish_epoch().unwrap();
    }
    let resolved = resolve_one(&f, &p);
    let next = p.next_request();
    assert!(p.record(&resolved).is_err());
    assert_eq!(p.next_request(), next);
    assert_eq!(p.current_epoch(), Some(3));
    assert!(p.finish_epoch().is_err());
}

#[test]
fn lineage_positive_finish_rejects_alias_indices_and_stale_minima() {
    for variant in 0..4 {
        let f = prepared_fixture();
        let r = valid_empty_third(&f);
        correction_with_link(&f, &r, false, variant != 3);
        if variant == 0 {
            let old = IngestKey::EventPayload {
                dataset_id: f.previous.identity().dataset_id,
                event_id: fixture_event("E_private").id,
            };
            let other = IngestKey::EventPayload {
                dataset_id: f.previous.identity().dataset_id,
                event_id: EventId::from_bytes([0xfe; 32]),
            };
            put(&r.store, other, &value(&r.store, old));
        } else if variant == 1 {
            let key = IngestKey::EventReceive {
                dataset_id: f.previous.identity().dataset_id,
                event_id: fixture_event("E_private").id,
                visibility_scope: reference(DictionaryKind::VisibilityScope, 2),
            };
            let fact = value(
                &r.store,
                IngestKey::Fact {
                    source_record_id: fact_id(1, 9),
                },
            );
            let mut pointer = vec![1, 8];
            pointer.extend(
                &IngestKey::Fact {
                    source_record_id: fact_id(1, 9),
                }
                .encode()
                .unwrap()
                .as_bytes()[2..],
            );
            pointer.extend(&fact[2..34]);
            put(&r.store, key, &pointer);
        } else if variant == 2 {
            // All structural counters and the replacement candidate's own hash
            // remain exact. Its descriptor is not the pointed mapping's value.
            let reference = reference(DictionaryKind::Event, 1);
            for (id, remove) in [
                (fixture_event("E_private").id, true),
                (EventId::from_bytes([0xfe; 32]), false),
            ] {
                let descriptor = DictionaryEntry::new(reference, DictionaryDescriptor::Event(id))
                    .unwrap()
                    .encode_descriptor(8192)
                    .unwrap();
                let key = IngestKey::DictionaryReverseCandidate {
                    dataset_id: reference.dataset_id,
                    dictionary_kind: DictionaryKind::Event,
                    descriptor_digest: Digest256::from_bytes(Sha256::digest(&descriptor).into()),
                    reference,
                };
                if remove {
                    delete(&r.store, key);
                } else {
                    let mut bytes = vec![1, 5];
                    bytes.extend(descriptor);
                    put(&r.store, key, &bytes);
                }
            }
        }
        let l = inspect(&f, &r, None).unwrap();
        let before = kv(&r.store);
        let error = match replay(&f, &r, &l, 3, Some(PredecessorTerminalKind::Activated)) {
            Ok(_) => panic!("bad semantic index"),
            Err(e) => e,
        };
        assert!(error.starts_with("census:"), "{error}");
        assert_eq!(kv(&r.store), before);
    }
}

#[test]
fn lineage_positive_closure_checks_ancestor_digest_and_head_kind_separately() {
    let f = prepared_fixture();
    let r = valid_empty_third(&f);
    let l = inspect(&f, &r, None).unwrap();
    let prefix = replay(&f, &r, &l, 2, None).unwrap();
    assert_eq!(
        prefix.last_epoch().digest,
        epoch_digest(&f, &r, 2, 0, 0, true)
    );
    assert!(
        l.resolution(
            &r,
            3,
            Some(PredecessorTerminalKind::Activated),
            &NOT_CANCELLED
        )
        .is_err(),
        "OPEN is not CLOSING"
    );
    drop(prefix);
    drop(l);
    put(&r.store, producer_key(3), &marker(true, 0));
    let l = inspect(&f, &r, None).unwrap();
    let active = replay(&f, &r, &l, 3, Some(PredecessorTerminalKind::Activated)).unwrap();
    let empty = replay(
        &f,
        &r,
        &l,
        3,
        Some(PredecessorTerminalKind::UnactivatedEmpty),
    )
    .unwrap();
    assert_ne!(active.last_epoch().digest, empty.last_epoch().digest);
    assert_eq!(
        active.last_epoch().digest,
        epoch_digest(&f, &r, 3, 0, 0, false)
    );
    assert_eq!(
        empty.last_epoch().digest,
        epoch_digest(&f, &r, 3, 0, 0, true)
    );
    drop(active);
    drop(empty);
    drop(l);
    let key = IngestKey::OwnerHandoff {
        dataset_id: f.previous.identity().dataset_id,
        owner_resource_id: f.previous.identity().owner_resource_id,
        predecessor_epoch: OwnerEpoch::new(2).unwrap(),
    };
    let mut fields = *SuccessionRecord::decode(&value(&r.store, key))
        .unwrap()
        .fields();
    fields.closure_digest[0] ^= 1;
    put(
        &r.store,
        key,
        SuccessionRecord::new(fields).unwrap().encode().as_bytes(),
    );
    let l = inspect(&f, &r, None).unwrap();
    assert!(replay(&f, &r, &l, 2, None).is_err());
}
