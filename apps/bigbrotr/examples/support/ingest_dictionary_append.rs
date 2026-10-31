//! Actual PG reservation/outcome copy and native append, before a signed socket.
//! Python supplies independent complete native/input/receipt expectations.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::InitialDictionaryAppendRequest, rocks::store::AdmissionLimits,
};
use bigbrotr_model::{ClockSource, ObserverDescriptor, RequestSequence};

pub(super) fn private_intent(bootstrap: &EvidenceIntent) -> EvidenceIntent {
    let visibility = QualifiedRef {
        ordinal: Ordinal::new(3).unwrap(),
        ..bootstrap.visibility_scope_id()
    };
    let mut entries = vec![
        DictionaryEntry::new(
            visibility,
            DictionaryDescriptor::Visibility {
                class: DescriptorLabel::new("private".into()).unwrap(),
            },
        )
        .unwrap(),
        DictionaryEntry::new(
            QualifiedRef {
                dictionary_kind: DictionaryKind::Observer,
                allocation_domain: [65; 16].try_into().unwrap(),
                ordinal: Ordinal::new(1).unwrap(),
                ..visibility
            },
            DictionaryDescriptor::Observer(ObserverDescriptor {
                clock: Some(ClockSource::SystemUtc),
                vantage: Some(DescriptorLabel::new("source-fixture".into()).unwrap()),
                software: None,
                method_id: None,
                public_key: None,
                route_class: None,
                software_version: None,
            }),
        )
        .unwrap(),
    ];
    ingest_observation::dictionaries(
        &mut entries,
        visibility,
        true,
        false,
        "ws://127.0.0.1:7447/private",
    );
    EvidenceIntent::new(
        1,
        visibility,
        entries,
        EvidenceIntentLimits {
            max_entries: 5,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "fixture retains the original owner and store"
)]
pub async fn run(
    owner: &mut ControlSession,
    observer: &tokio_postgres::Client,
    mut store: IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    bootstrap: &EvidenceIntent,
    interned: bool,
) -> (IngestStore, EvidenceIntent) {
    let fixed = private_intent(bootstrap);
    let cancelled = AtomicBool::new(false);
    let admission = AdmissionLimits {
        max_batch_bytes: MIB as usize,
        max_dependencies: 16,
        max_outstanding_requests: u64::from(spec.outstanding_request_limit),
    };
    let permit = || {
        grant(
            budget,
            admission.required_dictionary_data_bytes().unwrap(),
            admission.required_native_bytes().unwrap(),
        )
    };
    let validate = |d: &str, h| validate(d, h);
    emit(
        serde_json::json!({"event":"dictionary-before","state":ingest_snapshot::state(observer,path).await}),
    );
    // Visibility is an explicit operator identity, committed before interning
    // attribution values. The original private2 mapping is not substituted.
    let visibility_only = EvidenceIntent::new(
        1,
        fixed.visibility_scope_id(),
        vec![fixed.dictionaries()[0].clone()],
        EvidenceIntentLimits {
            max_entries: 1,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .unwrap();
    let visibility_input = || InitialDictionaryAppendRequest {
        ingest_root: path,
        registration: spec,
        request_sequence: RequestSequence::new(2).unwrap(),
        intent: &visibility_only,
        recorded_at: ObservedTime::new(234),
        limits: admission,
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    let mut visibility_bytes = None;
    let prepared = if interned {
        let result = owner
            .append_initial_dictionaries(&mut store, visibility_input(), permit(), validate)
            .await
            .unwrap();
        assert_eq!(result.receipt().batch().unwrap().records().len(), 1);
        visibility_bytes = Some(result.receipt().encode_persistent(8192).unwrap());
        drop(result);
        let before = ingest_snapshot::state(observer, path).await;
        let mut preparation = store
            .prepare_dictionaries(
                fixed.visibility_scope_id(),
                admission,
                grant(
                    budget,
                    admission
                        .required_dictionary_preparation_data_bytes()
                        .unwrap(),
                    0,
                ),
                &cancelled,
            )
            .unwrap();
        for entry in &fixed.dictionaries()[1..] {
            assert_eq!(
                preparation.intern(entry.descriptor().clone()).unwrap(),
                entry.reference()
            );
        }
        // Clock and observer intentionally share one complete descriptor.
        assert_eq!(
            preparation
                .intern(fixed.dictionaries()[1].descriptor().clone())
                .unwrap(),
            fixed.dictionaries()[1].reference()
        );
        let prepared = preparation.finish().unwrap();
        assert_eq!(prepared.intent().dictionaries(), &fixed.dictionaries()[1..]);
        assert_eq!(ingest_snapshot::state(observer, path).await, before);
        emit(
            serde_json::json!({"event":"dictionary-prepared", "visibility_receipt":
            hex(visibility_bytes.as_ref().unwrap()), "state":before}),
        );
        Some(prepared)
    } else {
        None
    };
    let intent = prepared.as_ref().map_or(&fixed, |p| p.intent());
    let append_sequence = if interned { 3 } else { 2 };
    let reuse_sequence = append_sequence + 1;
    let input = |sequence, recorded_at| InitialDictionaryAppendRequest {
        ingest_root: path,
        registration: spec,
        request_sequence: RequestSequence::new(sequence).unwrap(),
        intent,
        recorded_at: ObservedTime::new(recorded_at),
        limits: admission,
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    let first = owner
        .append_initial_dictionaries(&mut store, input(append_sequence, 234), permit(), validate)
        .await
        .unwrap();
    assert_eq!(
        first.receipt().batch().unwrap().records().len(),
        if interned { 4 } else { 5 }
    );
    let first_bytes = first.receipt().encode_persistent(8192).unwrap();
    drop(first);
    let after_append = ingest_snapshot::state(observer, path).await;
    let retry = owner
        .append_initial_dictionaries(&mut store, input(append_sequence, 999), permit(), validate)
        .await
        .unwrap();
    assert_eq!(
        retry.receipt().encode_persistent(8192).unwrap(),
        first_bytes
    );
    drop(retry);
    assert_eq!(ingest_snapshot::state(observer, path).await, after_append);
    emit(
        serde_json::json!({"event":"dictionary-appended","receipt":hex(&first_bytes),"state":after_append}),
    );

    let reused = owner
        .append_initial_dictionaries(&mut store, input(reuse_sequence, 345), permit(), validate)
        .await
        .unwrap();
    assert!(reused.receipt().batch().is_none());
    let reused_bytes = reused.receipt().encode_persistent(8192).unwrap();
    drop(reused);
    let before_reopen = ingest_snapshot::state(observer, path).await;
    let manifest = *store.manifest();
    drop(store);
    store = IngestStore::reopen(path, manifest, limits(), grant(budget, 0, 128 * MIB)).unwrap();
    for (sequence, expected) in [
        (append_sequence, &first_bytes),
        (reuse_sequence, &reused_bytes),
    ] {
        let retry = owner
            .append_initial_dictionaries(&mut store, input(sequence, 999), permit(), validate)
            .await
            .unwrap();
        assert_eq!(retry.receipt().encode_persistent(8192).unwrap(), *expected);
    }
    if let Some(expected) = visibility_bytes {
        let retry = owner
            .append_initial_dictionaries(&mut store, visibility_input(), permit(), validate)
            .await
            .unwrap();
        assert_eq!(retry.receipt().encode_persistent(8192).unwrap(), expected);
        drop(retry);
        let mut reused = store
            .prepare_dictionaries(
                fixed.visibility_scope_id(),
                admission,
                grant(
                    budget,
                    admission
                        .required_dictionary_preparation_data_bytes()
                        .unwrap(),
                    0,
                ),
                &cancelled,
            )
            .unwrap();
        for entry in &fixed.dictionaries()[1..] {
            assert_eq!(
                reused.intern(entry.descriptor().clone()).unwrap(),
                entry.reference()
            );
        }
        assert_eq!(reused.finish().unwrap().intent(), intent);
    }
    // Reopening the native handle under its retained original Control owner is
    // not successor adoption or a restarted installed service.
    let replay = owner
        .bootstrap_initial_dictionaries(
            &mut store,
            InitialBatchRequest {
                ingest_root: path,
                registration: spec,
                intent: bootstrap,
                recorded_at: ObservedTime::new(999),
                limits: BootstrapLimits {
                    max_mappings: 8,
                    max_batch_bytes: MIB as usize,
                },
                policy_ttl: Duration::from_secs(2),
                cancelled: &cancelled,
            },
            grant(budget, 8 * MIB, 4 * MIB),
            validate,
        )
        .await
        .unwrap();
    let bootstrap_bytes = replay.receipt().encode_persistent(8192).unwrap();
    drop(replay);
    assert_eq!(ingest_snapshot::state(observer, path).await, before_reopen);
    emit(
        serde_json::json!({"event":"dictionary-reopened","bootstrap_receipt":hex(&bootstrap_bytes),"reuse_receipt":hex(&reused_bytes),"state":before_reopen}),
    );
    // The fixture retains the fixed full descriptor set for socket attribution.
    (store, fixed)
}
