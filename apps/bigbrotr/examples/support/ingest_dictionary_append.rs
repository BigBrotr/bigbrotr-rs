//! Actual PG reservation/outcome copy and native append, before a signed socket.
//! Python supplies independent complete native/input/receipt expectations.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::InitialDictionaryAppendRequest, rocks::store::AdmissionLimits,
};
use bigbrotr_model::{ClockSource, ObserverDescriptor, RequestSequence};

fn private_intent(bootstrap: &EvidenceIntent) -> EvidenceIntent {
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
) -> (IngestStore, EvidenceIntent) {
    let intent = private_intent(bootstrap);
    let cancelled = AtomicBool::new(false);
    let admission = AdmissionLimits {
        max_batch_bytes: MIB as usize,
        max_dependencies: 16,
        max_outstanding_requests: 4,
    };
    let input = |sequence, recorded_at| InitialDictionaryAppendRequest {
        ingest_root: path,
        registration: spec,
        request_sequence: RequestSequence::new(sequence).unwrap(),
        intent: &intent,
        recorded_at: ObservedTime::new(recorded_at),
        limits: admission,
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    let permit = || {
        grant(
            budget,
            admission.required_dictionary_data_bytes().unwrap(),
            admission.required_native_bytes().unwrap(),
        )
    };
    let validate = |d: &str, h| validate_version(d, h, true);
    emit(
        serde_json::json!({"event":"dictionary-before","state":ingest_snapshot::state(observer,path).await}),
    );
    let first = owner
        .append_initial_dictionaries(&mut store, input(2, 234), permit(), validate)
        .await
        .unwrap();
    assert_eq!(first.receipt().batch().unwrap().records().len(), 5);
    let first_bytes = first.receipt().encode_persistent(8192).unwrap();
    drop(first);
    let after_append = ingest_snapshot::state(observer, path).await;
    let retry = owner
        .append_initial_dictionaries(&mut store, input(2, 999), permit(), validate)
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
        .append_initial_dictionaries(&mut store, input(3, 345), permit(), validate)
        .await
        .unwrap();
    assert!(reused.receipt().batch().is_none());
    let reused_bytes = reused.receipt().encode_persistent(8192).unwrap();
    drop(reused);
    let before_reopen = ingest_snapshot::state(observer, path).await;
    let manifest = *store.manifest();
    drop(store);
    store = IngestStore::reopen(path, manifest, limits(), grant(budget, 0, 128 * MIB)).unwrap();
    for (sequence, expected) in [(2, &first_bytes), (3, &reused_bytes)] {
        let retry = owner
            .append_initial_dictionaries(&mut store, input(sequence, 999), permit(), validate)
            .await
            .unwrap();
        assert_eq!(retry.receipt().encode_persistent(8192).unwrap(), *expected);
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
    (store, intent)
}
