//! Current-format, real connected history qualification. No portable bytes mint
//! reclamation, preparation, activation or acknowledgement capabilities here.
use super::*;
use bigbrotr::storage::{
    postgres::{
        control_owner::{
            ActivatedInitialResolveRequest, InitialDictionaryAppendRequest, InitialHistoryRequest,
            InitialSuccessionRequest, LiveInitialDictionaryAppendRequest, ProducerCloseRequest,
            ProducerResolveRequest, ProgressGrant,
        },
        ingest_registry::{CapturedIntakePolicy, RegistryError},
    },
    rocks::{
        store::{
            AdmissionLimits, COMPACTED_PREFIX_METADATA_BYTES, InitialWriterCandidate,
            ProducerRecoveryLimits, RecoveryAcknowledgement, RetainedCompactedPrefix,
            RetainedRequestInput, compacted_outcome_data_bytes,
        },
        succession::SuccessionProfile,
    },
};
use bigbrotr_model::{ProducerEpoch, ProducerRequestId, RequestSequence};
use std::time::Instant;

static CANCELLED: AtomicBool = AtomicBool::new(false);
const TTL: Duration = Duration::from_secs(5);

fn owner_budget(root: &crate::config::Resolved) -> ResourceBudget {
    let n = |key: &str| root.number(&format!("runtime.processes.application.{key}"));
    ResourceBudget::new(
        ProcessEnvelope::new(
            n("memory_max_bytes"),
            n("baseline_bytes"),
            ResourceLimits::new(
                n("payload_bytes"),
                16,
                n("native_bytes"),
                n("completion_reserve_bytes"),
            ),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn validate(root: &crate::config::Resolved, doc: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
    let result = crate::config::validate_fixture_policy(doc, digest, root);
    emit(
        serde_json::json!({"event":"history-integrity-policy","digest":hex(&digest),
        "valid":result.is_ok()}),
    );
    result
}
fn scope(dataset: bigbrotr_model::DatasetId, domain: [u8; 16], ordinal: u64) -> QualifiedRef {
    QualifiedRef {
        dataset_id: dataset,
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: domain.try_into().unwrap(),
        ordinal: Ordinal::new(ordinal).unwrap(),
    }
}
fn evidence(scope: QualifiedRef, entries: &[(QualifiedRef, &str)]) -> EvidenceIntent {
    EvidenceIntent::new(
        1,
        scope,
        entries
            .iter()
            .map(|(reference, label)| {
                DictionaryEntry::new(
                    *reference,
                    DictionaryDescriptor::Visibility {
                        class: DescriptorLabel::new((*label).into()).unwrap(),
                    },
                )
                .unwrap()
            })
            .collect(),
        EvidenceIntentLimits {
            max_entries: 8,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 4096,
        },
    )
    .unwrap()
}
fn recovery_limits() -> ProducerRecoveryLimits {
    ProducerRecoveryLimits {
        input: bigbrotr_model::codec::request::RequestDecodeLimits {
            values: bigbrotr_model::codec::FactDecodeLimits {
                max_encoded_bytes: MIB as usize,
                max_allocation_bytes: 4 * MIB as usize,
                max_tag_nodes: 4096,
                max_string_bytes: MIB as usize,
            },
            max_dictionaries: 8,
        },
        bootstrap: BootstrapLimits {
            max_mappings: 8,
            max_batch_bytes: MIB as usize,
        },
        event: AdmissionLimits {
            max_outstanding_requests: 2,
            ..AdmissionLimits::default()
        },
    }
}
fn progress(budget: &ResourceBudget) -> ProgressGrant {
    ProgressGrant::new(grant(
        budget,
        ProgressGrant::REQUIRED_DATA_BYTES,
        ProgressGrant::REQUIRED_NATIVE_BYTES,
    ))
    .unwrap()
}
fn effects(budget: &ResourceBudget) -> OwnedPermit {
    grant(
        budget,
        compacted_outcome_data_bytes(recovery_limits().event).unwrap(),
        0,
    )
}
fn original_grants(budget: &ResourceBudget) -> (OwnedPermit, OwnedPermit) {
    let limits = recovery_limits();
    (
        grant(
            budget,
            RetainedRequestInput::required_data_bytes(limits.input).unwrap(),
            0,
        ),
        grant(budget, limits.required_data_bytes().unwrap(), 0),
    )
}
fn closing<'a>(path: &'a Path, registration: &'a InitialRegistration) -> ProducerCloseRequest<'a> {
    ProducerCloseRequest {
        ingest_root: path,
        registration,
        policy_ttl: TTL,
        cancelled: &CANCELLED,
    }
}
fn history(path: &Path) -> InitialHistoryRequest<'_> {
    InitialHistoryRequest {
        ingest_root: path,
        policy_ttl: TTL,
        cancelled: &CANCELLED,
    }
}
async fn snapshot(
    observer: &tokio_postgres::Client,
    path: &Path,
    stage: &str,
) -> serde_json::Value {
    let state = crate::ingest_snapshot::state(observer, path).await;
    let (rows, sequence) = raw_state(path);
    assert_eq!(state["sequence"].as_u64(), Some(sequence));
    assert_eq!(state["control_tables"].as_array().unwrap().len(), 82);
    emit(
        serde_json::json!({"event":"history-integrity-state","stage":stage,"state":state,"rows":rows}),
    );
    state
}
async fn gate(path: &Path, phase: &str) {
    let ready = path.with_extension("integrity-ready.json");
    let go = path.with_extension("integrity-go");
    assert!(!ready.exists() && !go.exists());
    let record = serde_json::json!({"event":"history-integrity-ready","phase":phase,
        "pid":std::process::id(),"go":go});
    let temporary = ready.with_extension("tmp");
    assert!(!temporary.exists());
    std::fs::write(&temporary, serde_json::to_vec(&record).unwrap()).unwrap();
    std::fs::rename(temporary, &ready).unwrap();
    emit(record);
    let deadline = Instant::now() + Duration::from_secs(20);
    while !go.exists() {
        assert!(
            Instant::now() < deadline,
            "history integrity fixture gate expired"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(std::fs::read(go).unwrap(), b"go\n");
}
async fn connect(
    config: &Config,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let (client, driver) = config.connect(NoTls).await.unwrap();
    (client, tokio::spawn(driver))
}
async fn released_control_joined(
    observer: &tokio_postgres::Client,
    identity: ingest_registry::ControlOwner,
    driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    stage: &str,
) {
    crate::ingest_closing::join(driver, false).await;
    let absent: bool = observer
        .query_one(
            "SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity a JOIN control.owner_epochs o
          ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start
          WHERE o.resource_id=$1::bytea AND o.epoch=$2::text::numeric)",
            &[
                &&identity.resource_id.as_bytes()[..],
                &identity.epoch.get().to_string(),
            ],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        absent,
        "released original Control backend must actually exit"
    );
    emit(
        serde_json::json!({"event":"history-integrity-control","stage":stage,
        "resource":hex(identity.resource_id.as_bytes()),"epoch":identity.epoch.get(),
        "role":hex(&identity.role_instance_id),"incarnation":hex(identity.process_incarnation.as_bytes()),
        "released":true,"driver_joined":true,"backend_absent":absent}),
    );
}

/// Two actual dictionary originals, typed progress/ACK, source retirement and
/// closed reclamation create the reusable current compacted predecessor.
pub async fn seed(
    config: Config,
    mode: &str,
    path: &Path,
    dataset: bigbrotr_model::DatasetId,
    instance: [u8; 16],
) {
    let root = crate::config::fixture_root(dataset, instance);
    let budget = owner_budget(&root);
    let _documents = grant(&budget, 512 * MIB, 0);
    let (observer, observer_driver) = connect(&config).await;
    let (client, driver) = connect(&config).await;
    let mut capture = None;
    let acquired = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance: instance.try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: [31; 16],
            incarnation: [41; 16].try_into().unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        |doc, digest| {
            let restored = crate::config::policy::PolicySeed::restore(doc, digest, root.clone())
                .map_err(|_| OwnerError::InvalidPolicy)?;
            if restored.number("ingest.producer_outstanding_requests") != 2 {
                return Err(OwnerError::InvalidPolicy);
            }
            let next = CapturedIntakePolicy::capture(&restored.values)
                .map_err(|_| OwnerError::InvalidPolicy)?;
            if capture.as_ref().is_some_and(|old| old != &next) {
                return Err(OwnerError::InvalidPolicy);
            }
            capture = Some(next);
            Ok(())
        },
        &Cell::new(false),
    )
    .await;
    if mode == "history-integrity-seed-refuse" {
        assert!(matches!(acquired, Err(OwnerError::InvalidPolicy)));
        crate::ingest_closing::join(driver, false).await;
        assert!(!path.exists());
        emit(
            serde_json::json!({"event":"history-integrity-result","phase":"startup","success":false,
            "error":"InvalidPolicy"}),
        );
        drop(observer);
        crate::ingest_closing::join(observer_driver, false).await;
        return;
    }
    let (mut owner, policy) = acquired.unwrap();
    let basis = owner
        .initial_ingest_basis(|d, h| validate(&root, d, h))
        .await
        .unwrap();
    assert_eq!(basis.policy, policy);
    let spec = InitialRegistration {
        dataset_id: dataset,
        instance_id: instance,
        control: owner.identity(),
        ingest_resource_id: [61; 16].try_into().unwrap(),
        ingest_role_instance_id: [62; 16],
        process_incarnation: [63; 16].try_into().unwrap(),
        shard_id: 0,
        config_revision: policy.config_revision,
        policy_revision: policy.policy_revision,
        config_digest: Digest256::from_bytes(policy.config_digest),
        policy_digest: Digest256::from_bytes(policy.policy_digest),
        previous_topology: basis.previous_topology,
        allocation_domains: (1..=9)
            .map(|number| {
                (
                    DictionaryKind::try_from(number).unwrap(),
                    [60 + number as u8; 16].try_into().unwrap(),
                )
            })
            .collect(),
        producer_slot_id: [66; 16].try_into().unwrap(),
        outstanding_request_limit: 2,
        intake_policy: capture.unwrap(),
        created_at_us: basis.created_at_us,
        lease_expires_at_us: basis.lease_expires_at_us,
    };
    owner
        .register_ingest(&mut RegistrationAttempt::new(spec.clone()).unwrap())
        .await
        .unwrap();
    let manifest = IngestManifest::new(
        ManifestIdentity {
            dataset_id: dataset,
            topology_generation: spec.target_topology().unwrap(),
            owner_resource_id: spec.ingest_resource_id,
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            process_incarnation: spec.process_incarnation,
            recovery_generation: RecoveryGenerationId::from_bytes([67; 16]),
            policy_epoch: PolicyEpoch::new(policy.policy_revision),
        },
        AllocationNamespaces::from_entries(&spec.allocation_domains).unwrap(),
    );
    let mut store = IngestStore::initialize(
        path,
        manifest,
        limits(),
        crate::ingest_owner::grant(&budget, 128 * MIB),
    )
    .unwrap();
    let public = scope(dataset, [64; 16], 1);
    let input = evidence(
        public,
        &[(public, "public"), (scope(dataset, [64; 16], 2), "private")],
    );
    let pg = progress(&budget);
    drop(
        owner
            .bootstrap_initial_dictionaries_with_progress(
                &mut store,
                InitialBatchRequest {
                    ingest_root: path,
                    registration: &spec,
                    intent: &input,
                    recorded_at: ObservedTime::new(123),
                    limits: recovery_limits().bootstrap,
                    policy_ttl: TTL,
                    cancelled: &CANCELLED,
                },
                grant(&budget, 8 * MIB, 4 * MIB),
                &pg,
                |d, h| validate(&root, d, h),
            )
            .await
            .unwrap(),
    );
    let input = evidence(public, &[(scope(dataset, [64; 16], 3), "history-third")]);
    let admission = recovery_limits().event;
    drop(
        owner
            .append_initial_dictionaries_with_progress(
                &mut store,
                InitialDictionaryAppendRequest {
                    ingest_root: path,
                    registration: &spec,
                    request_sequence: RequestSequence::new(2).unwrap(),
                    intent: &input,
                    recorded_at: ObservedTime::new(456),
                    limits: admission,
                    policy_ttl: TTL,
                    cancelled: &CANCELLED,
                },
                grant(
                    &budget,
                    admission.required_dictionary_data_bytes().unwrap(),
                    admission.required_native_bytes().unwrap(),
                ),
                &pg,
                |d, h| validate(&root, d, h),
            )
            .await
            .unwrap(),
    );
    snapshot(&observer, path, "seed-originals").await;
    drop(store);
    let mut recovery =
        IngestRecovery::open(path, manifest, limits(), grant(&budget, 0, 128 * MIB)).unwrap();
    let closed = owner
        .close_initial_producer(
            &mut recovery,
            closing(path, &spec),
            history_grant(&budget, 4096, 4096),
            |d, h| validate(&root, d, h),
        )
        .await
        .unwrap();
    assert_eq!(
        (closed.issued_through(), closed.acknowledged_through()),
        (2, 2)
    );
    let mut prefix = closed.source_resolution(&recovery).unwrap();
    for sequence in 1..=2 {
        let (input, outcome) = original_grants(&budget);
        let resolved = owner
            .inspect_initial_retirement_request(
                &recovery,
                ProducerResolveRequest {
                    ingest_root: path,
                    registration: &spec,
                    request: ProducerRequestId {
                        producer_slot_id: spec.producer_slot_id,
                        producer_epoch: ProducerEpoch::new(1).unwrap(),
                        request_sequence: RequestSequence::new(sequence).unwrap(),
                    },
                    limits: recovery_limits(),
                    policy_ttl: TTL,
                    cancelled: &CANCELLED,
                },
                input,
                outcome,
                |d, h| validate(&root, d, h),
            )
            .await
            .unwrap();
        prefix.record(&resolved).unwrap();
    }
    let source = prefix.finish().unwrap();
    assert_eq!(source.last_complete_sequence(), 3);
    owner
        .close_initial_source(
            &source,
            closing(path, &spec),
            grant(&budget, 64 * 1024, 0),
            |d, h| validate(&root, d, h),
        )
        .await
        .unwrap();
    snapshot(&observer, path, "seed-before-control-release").await;
    let original = owner.identity();
    assert_eq!(original, spec.control);
    owner.release().await.unwrap();
    released_control_joined(&observer, original, driver, "seed").await;
    snapshot(&observer, path, "seed-after-control-release").await;
    let (client, driver) = connect(&config).await;
    let (mut owner, current_policy) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance: instance.try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: [33; 16],
            incarnation: [43; 16].try_into().unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        |d, h| validate(&root, d, h),
        &Cell::new(false),
    )
    .await
    .unwrap();
    assert_eq!(current_policy, policy);
    assert_eq!(owner.identity().resource_id, original.resource_id);
    assert_eq!(owner.identity().epoch.get(), original.epoch.get() + 1);
    snapshot(&observer, path, "seed-after-control-acquire").await;
    assert_eq!(
        owner
            .retire_initial_producer(
                &source,
                closing(path, &spec),
                history_grant(&budget, 128 * 1024, 0),
                |d, h| validate(&root, d, h)
            )
            .await
            .unwrap(),
        0
    );
    let confirmed = owner.confirm_initial_producer_reclamation(&source, closing(path, &spec), admission,
        history_grant(&budget, bigbrotr::storage::postgres::control_owner::ConfirmedClosedReclamation::REQUIRED_DATA_BYTES,
            bigbrotr::storage::postgres::control_owner::ConfirmedClosedReclamation::REQUIRED_NATIVE_BYTES),
        effects(&budget), |d, h| validate(&root, d, h)).await.unwrap();
    drop(source);
    owner
        .reclaim_closed_producer(&mut recovery, confirmed, &CANCELLED)
        .unwrap();
    snapshot(&observer, path, "seed-compacted").await;
    drop(recovery);
    owner.release().await.unwrap();
    crate::ingest_closing::join(driver, false).await;
    snapshot(&observer, path, "seed-final").await;
    emit(
        serde_json::json!({"event":"history-integrity-result","phase":"seed","success":true,
        "original_requests":2,"source_through":3,"config_revision":policy.config_revision,
        "policy_revision":policy.policy_revision,"retention":spec.intake_policy.request_receipt_retention(),
        "pending_limit":spec.intake_policy.pending_limit()}),
    );
    drop(observer);
    crate::ingest_closing::join(observer_driver, false).await;
}

/// Reach the actual private original-body restore after producing all opaque
/// capabilities through real current Control, native and progress operations.
pub async fn private(
    config: Config,
    mode: &str,
    path: &Path,
    dataset: bigbrotr_model::DatasetId,
    instance: [u8; 16],
) {
    let root = crate::config::fixture_root(dataset, instance);
    let budget = owner_budget(&root);
    let _documents = grant(&budget, 512 * MIB, 0);
    let (mut observer, observer_driver) = connect(&config).await;
    let recovery = IngestRecovery::inspect(
        path,
        dataset,
        limits(),
        crate::ingest_owner::grant(&budget, 128 * MIB),
    )
    .unwrap();
    let spec = ingest_registry::load_historical(
        &mut observer,
        instance,
        &recovery,
        ingest_registry::MAX_HISTORICAL_DOCUMENT_BYTES,
        |d, h| validate(&root, d, h),
    )
    .await
    .unwrap();
    let mut candidate = InitialWriterCandidate::new(recovery).unwrap();
    let (client, driver) = connect(&config).await;
    let (mut owner, policy) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance: instance.try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: [211; 16],
            incarnation: [221; 16].try_into().unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        |d, h| validate(&root, d, h),
        &Cell::new(false),
    )
    .await
    .unwrap();
    let basis = owner
        .initial_successor_basis(&spec, |d, h| validate(&root, d, h))
        .await
        .unwrap();
    assert_eq!(basis.policy, policy);
    let prefix = RetainedCompactedPrefix::inspect_initial(
        candidate.recovery(),
        spec.producer_slot_id,
        recovery_limits().event,
        grant(&budget, COMPACTED_PREFIX_METADATA_BYTES, 0),
        effects(&budget),
        &CANCELLED,
    )
    .unwrap();
    let source = owner
        .inspect_initial_compacted_retirement(
            candidate.recovery(),
            &prefix,
            closing(path, &spec),
            recovery_limits().event,
            history_grant(&budget, 128 * 1024, 0),
            effects(&budget),
            |d, h| validate(&root, d, h),
        )
        .await
        .unwrap();
    let handoff = source
        .handoff_candidate(
            &candidate,
            PolicyEpoch::new(policy.policy_revision),
            &CANCELLED,
        )
        .unwrap();
    drop(source);
    drop(prefix);
    let role = candidate.role_instance_id();
    drop(
        owner
            .prepare_live_compacted_initial_successor(
                &mut candidate,
                InitialSuccessionRequest {
                    ingest_root: path,
                    previous_registration: &spec,
                    handoff: &handoff,
                    role_instance_id: role,
                    created_at_us: basis.created_at_us,
                    lease_expires_at_us: basis.lease_expires_at_us,
                    outstanding_request_limit: 2,
                    policy_ttl: TTL,
                    cancelled: &CANCELLED,
                },
                recovery_limits().event,
                history_grant(&budget, 128 * 1024, 8192),
                effects(&budget),
                |d, h| validate(&root, d, h),
            )
            .await
            .unwrap(),
    );
    let activation = mode.contains("activation");
    let refused = mode.ends_with("-refuse");
    if activation {
        snapshot(&observer, path, "before-corruption").await;
        gate(path, "activation").await;
    }
    let before = if activation {
        Some(snapshot(&observer, path, "before-action").await)
    } else {
        None
    };
    let activated = owner
        .activate_compacted_initial_writer(
            candidate,
            handoff,
            history(path),
            recovery_limits().event,
            history_grant(&budget, 128 * 1024, 8192),
            effects(&budget),
            |d, h| validate(&root, d, h),
        )
        .await;
    if activation {
        if refused {
            assert!(matches!(
                activated,
                Err(
                    bigbrotr::storage::postgres::control_owner::InitialSuccessionError::Registry(
                        RegistryError::InvalidMetadata
                    )
                )
            ));
        } else {
            drop(activated.unwrap());
        }
        let after = snapshot(&observer, path, "after-action").await;
        if refused {
            assert_eq!(before.unwrap(), after);
        }
        emit(
            serde_json::json!({"event":"history-integrity-result","phase":"activation",
            "success":!refused,"error":if refused {Some("Registry(InvalidMetadata)")}else{None}}),
        );
        if refused {
            drop(owner);
        } else {
            owner.release().await.unwrap();
        }
        crate::ingest_closing::join(driver, false).await;
        drop(observer);
        crate::ingest_closing::join(observer_driver, false).await;
        return;
    }
    let mut writer = activated.unwrap();
    let next = *writer.manifest();
    let reference = QualifiedRef {
        dataset_id: dataset,
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: next
            .allocation_namespaces()
            .domain(DictionaryKind::VisibilityScope)
            .unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    };
    emit(
        serde_json::json!({"event":"history-integrity-target-input","reference":reference,
        "scope":scope(dataset,[64;16],1),"class":"history-successor","recorded_at":789}),
    );
    let input = evidence(
        scope(dataset, [64; 16], 1),
        &[(reference, "history-successor")],
    );
    let pg = progress(&budget);
    let admission = recovery_limits().event;
    drop(
        owner
            .append_live_initial_dictionaries_with_progress(
                &mut writer,
                LiveInitialDictionaryAppendRequest {
                    ingest_root: path,
                    request_sequence: RequestSequence::new(1).unwrap(),
                    intent: &input,
                    recorded_at: ObservedTime::new(789),
                    limits: admission,
                    policy_ttl: TTL,
                    cancelled: &CANCELLED,
                },
                grant(
                    &budget,
                    admission.required_dictionary_data_bytes().unwrap(),
                    admission.required_native_bytes().unwrap(),
                ),
                &pg,
                |d, h| validate(&root, d, h),
            )
            .await
            .unwrap(),
    );
    assert_eq!(writer.producer_credit().unwrap().acknowledged_through(), 1);
    drop(writer);
    let mut recovery =
        IngestRecovery::open(path, next, limits(), grant(&budget, 0, 128 * MIB)).unwrap();
    // The compacted two-entry reader owns scratch plus both entry allowances.
    // Admit its exact existing profile before claiming the recovery Control.
    let metadata = history_grant(
        &budget,
        SuccessionProfile::for_recovery(recovery.manifest().identity().owner_epoch.get(), None)
            .unwrap()
            .required_metadata_bytes(),
        0,
    );
    snapshot(&observer, path, "ack-before-control-release").await;
    let original = owner.identity();
    owner.release().await.unwrap();
    released_control_joined(&observer, original, driver, "ack").await;
    snapshot(&observer, path, "ack-after-control-release").await;
    let (client, driver) = connect(&config).await;
    let (mut owner, current_policy) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance: instance.try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: [212; 16],
            incarnation: [222; 16].try_into().unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        |d, h| validate(&root, d, h),
        &Cell::new(false),
    )
    .await
    .unwrap();
    assert_eq!(current_policy, policy);
    assert_eq!(owner.identity().resource_id, original.resource_id);
    assert_eq!(owner.identity().epoch.get(), original.epoch.get() + 1);
    snapshot(&observer, path, "ack-after-control-acquire").await;
    let observed = owner
        .inspect_activated_initial_producer_with_compacted(
            &recovery,
            history(path),
            metadata,
            admission,
            effects(&budget),
            |d, h| validate(&root, d, h),
        )
        .await
        .unwrap();
    let closed = owner
        .close_activated_initial_producer(
            &mut recovery,
            observed,
            history(path),
            grant(&budget, 4096, 4096),
            |d, h| validate(&root, d, h),
        )
        .await
        .unwrap();
    let mut prefix = closed
        .compacted_acknowledgement_resolution(&recovery, &CANCELLED)
        .unwrap();
    assert_eq!(
        owner
            .resolve_activated_compacted_predecessor(
                &recovery,
                &closed,
                history(path),
                admission,
                effects(&budget),
                Some(&mut prefix),
                |d, h| validate(&root, d, h)
            )
            .await
            .unwrap(),
        2
    );
    let (input, outcome) = original_grants(&budget);
    let original = owner
        .resolve_activated_initial_request(
            &recovery,
            &closed,
            ActivatedInitialResolveRequest {
                ingest_root: path,
                request: ProducerRequestId {
                    producer_slot_id: spec.producer_slot_id,
                    producer_epoch: ProducerEpoch::new(2).unwrap(),
                    request_sequence: RequestSequence::new(1).unwrap(),
                },
                limits: recovery_limits(),
                policy_ttl: TTL,
                cancelled: &CANCELLED,
            },
            input,
            outcome,
            |d, h| validate(&root, d, h),
        )
        .await
        .unwrap();
    prefix.record(&original).unwrap();
    drop(original);
    let replay = prefix.finish().unwrap();
    assert_eq!(replay.compacted_requests(), 2);
    drop(closed);
    snapshot(&observer, path, "before-corruption").await;
    gate(path, "compacted-ack").await;
    let before = snapshot(&observer, path, "before-action").await;
    let acknowledged = owner
        .acknowledge_recovered_compacted_producer(
            &mut recovery,
            replay,
            history(path),
            history_grant(
                &budget,
                RecoveryAcknowledgement::COMPACTED_REQUIRED_DATA_BYTES,
                RecoveryAcknowledgement::REQUIRED_NATIVE_BYTES,
            ),
            admission,
            effects(&budget),
            |d, h| validate(&root, d, h),
        )
        .await;
    if refused {
        assert!(matches!(
            acknowledged,
            Err(
                bigbrotr::storage::postgres::control_owner::ProducerAcknowledgementError::Registry(
                    RegistryError::InvalidMetadata
                )
            )
        ));
    } else {
        let acknowledged = acknowledged.unwrap();
        assert_eq!(
            (
                acknowledged.issued_through(),
                acknowledged.acknowledged_through()
            ),
            (1, 1)
        );
    }
    let after = snapshot(&observer, path, "after-action").await;
    assert_eq!(
        before, after,
        "idempotent ACK/refusal preserves the complete baseline"
    );
    emit(
        serde_json::json!({"event":"history-integrity-result","phase":"compacted-ack",
        "success":!refused,"issued":1,"acknowledged":1,"idempotent":true,
        "error":if refused {Some("Registry(InvalidMetadata)")}else{None}}),
    );
    drop(recovery);
    if refused {
        drop(owner);
    } else {
        owner.release().await.unwrap();
    }
    crate::ingest_closing::join(driver, false).await;
    drop(observer);
    crate::ingest_closing::join(observer_driver, false).await;
}
