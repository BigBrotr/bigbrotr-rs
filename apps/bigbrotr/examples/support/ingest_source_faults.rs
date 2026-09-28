//! Finite source-bound admission followed by real revocation and original recovery.
//! The external laboratory owns wire faults and lock schedules; no product hook.
use super::*;
use bigbrotr::{
    acquisition::{relay_source::RelaySourceBinding, scope::AcquisitionEventContext},
    management::{
        credentials::HASH_RESERVATION, grants::Revision, relay_enrollment::EnrollmentCommand,
        sessions::SessionLifetime,
    },
    storage::{
        postgres::{
            control_owner::{
                EnrollmentRoutes, InitialEventError, InitialEventRequest, ProducerCloseRequest,
                ProducerResolveError, ProducerResolveRequest, ScopeIntent,
            },
            producer_outcomes::OutcomeError,
            producer_requests::ReservationError,
            producer_sources::SourceBindingError,
        },
        rocks::store::{
            AdmissionLimits, AdmissionOutcome, IngestRecovery, PreparedEvent,
            ProducerRecoveryError, ProducerRecoveryLimits,
        },
    },
};
use bigbrotr_model::{
    ClockQuality, DurationUs, EventIntent, LocalObservation, ProducerEpoch, ProducerRequestId,
    RequestSequence,
    codec::{FactDecodeLimits, request::RequestDecodeLimits},
};

#[allow(
    clippy::too_many_arguments,
    reason = "isolated worker keeps actual custody and historical coordinates explicit"
)]
pub async fn run(
    config: Config,
    mut owner: ControlSession,
    driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    observer: &tokio_postgres::Client,
    mut store: IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
    source: RelaySourceBinding,
    mut enrollment: EnrollmentCommand,
) {
    let (scoped, mode) = match mode.strip_prefix("source-fault-scope-") {
        Some(phase) => (true, phase),
        None => (false, mode.strip_prefix("source-fault-").unwrap()),
    };
    assert!(matches!(
        mode,
        "pass" | "admission" | "reservation-lost" | "outcome-lost" | "source-refused"
    ));
    let retained_scope = if scoped {
        let selector = bigbrotr_protocol::selector::decode_selector(
            br#"{"version":1,"filters":[{"kinds":[1]}],"history_since":0,"ephemeral":"retain"}"#,
        )
        .unwrap();
        let registered = owner
            .register_acquisition_scope(
                ScopeIntent {
                    selector: &selector,
                    endpoint: source.endpoint_id,
                    access: source.access_scope_id,
                    visibility: enrollment.visibility,
                    policy_epoch: bigbrotr_model::PolicyEpoch::new(1),
                },
                &Cell::new(false),
            )
            .await
            .unwrap();
        Some(
            owner
                .read_acquisition_scope(registered.identity, grant(budget, 8 * MIB, 0))
                .await
                .unwrap()
                .unwrap(),
        )
    } else {
        None
    };
    let validate = |d: &str, h| validate_version(d, h, true);
    let backend = observer
        .query_one(
            "SELECT control_backend_pid,control_backend_start::text FROM control.owner_epochs
        WHERE role='control' AND epoch=1",
            &[],
        )
        .await
        .unwrap();
    emit(
        serde_json::json!({"event":"source-ready", "pid":backend.get::<_,i32>(0),
        "backend_start":backend.get::<_,String>(1), "binding":{
            "endpoint":hex(source.endpoint_id.as_bytes()),
            "access":hex(source.access_scope_id.as_bytes()),
            "generation":source.enrollment_generation.get(),
            "route":hex(source.route_id.as_bytes()),
            "scope":retained_scope.as_ref().map(|scope|scope.identity())},
        "state":ingest_snapshot::state(observer,path).await}),
    );
    // This gate precedes the operation's fresh policy lease. It never pauses a
    // native FFI call or pretends that dropping a future cancels one.
    let gate = path.with_extension("go");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !gate.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "source fixture gate expired"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let scope = enrollment.visibility;
    let observer_ref = QualifiedRef {
        dictionary_kind: DictionaryKind::Observer,
        allocation_domain: [65; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
        ..scope
    };
    let observation = LocalObservation {
        received_at: ObservedTime::new(456),
        observer_id: observer_ref,
        clock_quality: ClockQuality::new(observer_ref, DurationUs::new(1), None, 0).unwrap(),
        witness: None,
    };
    let event: bigbrotr_model::SignedEvent = serde_json::from_slice(include_bytes!(
        "../../../../tests/fixtures/ingest/import-event.jsonl"
    ))
    .unwrap();
    let request = ProducerRequestId {
        producer_slot_id: spec.producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(2).unwrap(),
    };
    let prepared = PreparedEvent::new(
        request,
        EventIntent::network(1, scope, event.clone(), observation).unwrap(),
        256 * 1024,
        grant(budget, MIB, 0),
    )
    .unwrap();
    let cancelled = AtomicBool::new(false);
    let admission = AdmissionLimits {
        max_outstanding_requests: 2,
        ..AdmissionLimits::default()
    };
    let result = owner
        .admit_initial_event(
            &mut store,
            InitialEventRequest {
                ingest_root: path,
                registration: spec,
                event: prepared,
                relay_source: Some(source),
                acquisition: retained_scope.as_ref().map(|scope| {
                    AcquisitionEventContext::new(scope, scope.selector().filters()).unwrap()
                }),
                provenance_enabled: false,
                recorded_at: ObservedTime::new(1234),
                limits: admission,
                policy_ttl: Duration::from_secs(2),
                cancelled: &cancelled,
            },
            grant(
                budget,
                admission.required_data_bytes().unwrap(),
                admission.required_native_bytes().unwrap(),
            ),
            |document, digest, candidate| {
                validate(document, digest)?;
                if candidate
                    .verified_event()
                    .is_none_or(|e| e.event() != &event)
                    || candidate.observation() != Some(&observation)
                {
                    return Err(OwnerError::InvalidPolicy);
                }
                Ok(())
            },
        )
        .await;
    let expected = match mode {
        "pass" | "admission" => None,
        "reservation-lost" => Some(InitialEventError::Reservation(
            ReservationError::CommitUnknown,
        )),
        "outcome-lost" => Some(InitialEventError::Outcome(OutcomeError::CommitUnknown)),
        // This initial factory is bound to the original accepted revisions.
        // Its post-wait snapshot refuses an advanced head before scope checks.
        "source-refused" if scoped => Some(InitialEventError::Control(OwnerError::InvalidPolicy)),
        "source-refused" => Some(InitialEventError::Source(SourceBindingError::Unavailable)),
        _ => unreachable!(),
    };
    let admitted_receipt = match result {
        Ok(value) => {
            assert!(expected.is_none());
            let AdmissionOutcome::Durable(receipt) = value.outcome() else {
                panic!("signed event rejected");
            };
            Some(receipt.encode_persistent(65536).unwrap())
        }
        Err(error) => {
            assert_eq!(Some(error), expected);
            None
        }
    };
    emit(
        serde_json::json!({"event":"source-admission-finished", "error":expected.map(|e|format!("{e:?}")),
        "receipt":admitted_receipt.as_ref().map(|b|hex(b)),
        "state":ingest_snapshot::state(observer,path).await}),
    );
    drop(owner);
    crate::ingest_closing::join(driver, mode.ends_with("-lost")).await;
    if matches!(mode, "source-refused" | "admission") {
        drop(store);
        emit(serde_json::json!({"event":"source-fault-done","driver_joined":true}));
        return;
    }
    if scoped {
        // The released Control driver cannot retain locks while the external
        // laboratory advances policy. Native exclusion stays with this worker.
        emit(serde_json::json!({"event":"scope-recovery-ready", "driver_joined":true}));
        let gate = path.with_extension("recover");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while !gate.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "scope recovery gate expired"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let row = observer
            .query_one(
                "SELECT config_revision::text,policy_revision::text FROM control.datasets",
                &[],
            )
            .await
            .unwrap();
        assert_eq!((row.get::<_, &str>(0), row.get::<_, &str>(1)), ("2", "2"));
        enrollment.expected_config_revision = Revision::try_from("2".to_owned()).unwrap();
        enrollment.expected_policy_revision = Revision::try_from("2".to_owned()).unwrap();
    }

    // Positive historical resolution must survive actual source revocation.
    // This fresh owner/session, unlike the injected concurrency writer, performs
    // the real authenticated management operation before closing the old target.
    let (client, driver) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let (mut current, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: spec.dataset_id,
            instance: spec.instance_id.try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: [33; 16],
            incarnation: [43; 16].try_into().unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    assert_eq!(current.identity().epoch.get(), 2);
    let hash_budget = ResourceBudget::new(
        ProcessEnvelope::new(
            HASH_RESERVATION,
            0,
            ResourceLimits::new(HASH_RESERVATION, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let verified = current
        .begin_browser_login("alice", "127.0.0.1".parse().unwrap())
        .await
        .unwrap()
        .verify(
            b"Independent owner password 2026",
            &mut grant(&hash_budget, HASH_RESERVATION, 0),
        )
        .unwrap();
    let session = current
        .finish_browser_login(verified, SessionLifetime::default())
        .await
        .unwrap();
    enrollment.enabled = false;
    enrollment.expected_generation = Some(Revision::try_from("1".to_owned()).unwrap());
    let receipt = current
        .accept_relay_enrollment(
            &session.session,
            &session.csrf,
            [3; 16].try_into().unwrap(),
            &enrollment,
            EnrollmentRoutes {
                public_enabled: true,
                private_ceiling: None,
            },
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert!(!receipt.enabled && receipt.generation.get() == 2);
    emit(
        serde_json::json!({"event":"source-revoked","state":ingest_snapshot::state(observer,path).await}),
    );
    let manifest = *store.manifest();
    drop(store);
    let mut recovery =
        IngestRecovery::open(path, manifest, limits(), grant(budget, 0, 128 * MIB)).unwrap();
    let closed = current
        .close_initial_producer(
            &mut recovery,
            ProducerCloseRequest {
                ingest_root: path,
                registration: spec,
                policy_ttl: Duration::from_secs(2),
                cancelled: &cancelled,
            },
            grant(budget, 4096, 4096),
            validate,
        )
        .await
        .unwrap();
    let unresolved = mode == "reservation-lost";
    assert_eq!(closed.issued_through(), if unresolved { 1 } else { 2 });
    let baseline = ingest_snapshot::state(observer, path).await;
    emit(
        serde_json::json!({"event":"source-closed","issued":closed.issued_through(),"state":baseline}),
    );
    let mut previous = None;
    for attempt in 0..if unresolved { 1 } else { 2 } {
        let result = current
            .resolve_initial_producer_request(
                &recovery,
                ProducerResolveRequest {
                    ingest_root: path,
                    registration: spec,
                    request,
                    policy_ttl: Duration::from_secs(2),
                    cancelled: &cancelled,
                    limits: ProducerRecoveryLimits {
                        input: RequestDecodeLimits {
                            values: FactDecodeLimits {
                                max_encoded_bytes: MIB as usize,
                                max_allocation_bytes: MIB as usize,
                                max_tag_nodes: 1024,
                                max_string_bytes: MIB as usize,
                            },
                            max_dictionaries: 8,
                        },
                        bootstrap: BootstrapLimits {
                            max_mappings: 8,
                            max_batch_bytes: MIB as usize,
                        },
                        event: admission,
                    },
                },
                grant(budget, 3 * MIB, 0),
                grant(budget, 40 * MIB, 0),
                validate,
            )
            .await;
        let encoded = if unresolved {
            assert!(matches!(
                result,
                Err(ProducerResolveError::Native(
                    ProducerRecoveryError::Unresolved
                ))
            ));
            None
        } else {
            let value = result.unwrap();
            let AdmissionOutcome::Durable(receipt) = value.outcome() else {
                panic!("original outcome changed");
            };
            let bytes = receipt.encode_persistent(65536).unwrap();
            if let Some(original) = &admitted_receipt {
                assert_eq!(&bytes, original);
            }
            Some(bytes)
        };
        let after = ingest_snapshot::state(observer, path).await;
        for field in ["native_sha256", "native_rows", "sequence", "originals"] {
            assert_eq!(after[field], baseline[field]);
        }
        if unresolved {
            assert_eq!(after, baseline);
        }
        if let Some(previous) = &previous {
            assert_eq!(&after, previous);
        }
        emit(
            serde_json::json!({"event":"source-resolved","attempt":attempt,"unresolved":unresolved,
            "receipt":encoded.as_ref().map(|b|hex(b)),"state":after}),
        );
        previous = Some(after);
    }
    drop(recovery);
    if unresolved {
        drop(current);
    } else {
        current.release().await.unwrap();
    }
    crate::ingest_closing::join(driver, false).await;
    emit(serde_json::json!({"event":"source-fault-done","driver_joined":true}));
}
