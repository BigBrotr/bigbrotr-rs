//! Connected append COMMIT ambiguity; the external peer controls actual PG wire.
use super::*;
use bigbrotr::storage::{
    postgres::{
        control_owner::{
            InitialDictionaryAppendRequest, InitialEventError, ProducerCloseRequest,
            ProducerResolveError, ProducerResolveRequest,
        },
        producer_outcomes::OutcomeError,
        producer_requests::ReservationError,
    },
    rocks::store::{
        AdmissionLimits, AdmissionOutcome, IngestRecovery, ProducerRecoveryError,
        ProducerRecoveryLimits,
    },
};
use bigbrotr_model::{
    ProducerEpoch, ProducerRequestId, RequestSequence,
    codec::{FactDecodeLimits, request::RequestDecodeLimits},
};

async fn gate(path: &Path, extension: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !path.with_extension(extension).exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "dictionary fixture gate expired"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "fixture retains actual native and Control custody"
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
    bootstrap: &EvidenceIntent,
) {
    let mode = mode.strip_prefix("dictionary-fault-").unwrap();
    assert!(matches!(
        mode,
        "pass"
            | "reservation-before"
            | "reservation-after"
            | "outcome-before"
            | "outcome-after"
            | "cancel-reservation"
            | "cancel-outcome"
            | "reuse-before"
            | "reuse-after"
    ));
    let reuse = mode.starts_with("reuse-");
    let sequence = if reuse { 3 } else { 2 };
    let intent = ingest_dictionary_append::private_intent(bootstrap);
    let cancelled = AtomicBool::new(false);
    let admission = AdmissionLimits {
        max_batch_bytes: MIB as usize,
        max_dependencies: 16,
        max_outstanding_requests: 4,
    };
    let input = |sequence| InitialDictionaryAppendRequest {
        ingest_root: path,
        registration: spec,
        request_sequence: RequestSequence::new(sequence).unwrap(),
        intent: &intent,
        recorded_at: ObservedTime::new(234),
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
    if reuse {
        drop(
            owner
                .append_initial_dictionaries(&mut store, input(2), permit(), validate)
                .await
                .unwrap(),
        );
    }
    let backend = observer.query_one(
        "SELECT control_backend_pid,control_backend_start::text FROM control.owner_epochs WHERE role='control' AND epoch=1", &[]).await.unwrap();
    emit(
        serde_json::json!({"event":"dictionary-ready", "pid":backend.get::<_,i32>(0),
        "backend_start":backend.get::<_,String>(1),"request":sequence,
        "state":ingest_snapshot::state(observer,path).await}),
    );
    gate(path, "go").await;
    let interrupted = mode.starts_with("cancel-");
    let result = {
        let operation =
            owner.append_initial_dictionaries(&mut store, input(sequence), permit(), validate);
        if interrupted {
            tokio::pin!(operation);
            tokio::select! {
                _ = &mut operation => panic!("append completed before observed COMMIT cancellation"),
                () = gate(path, "cancel") => None,
            }
        } else {
            Some(operation.await)
        }
    };
    if interrupted {
        assert_eq!(
            owner.poll_policy(validate).await,
            Err(OwnerError::Unavailable)
        );
        emit(serde_json::json!({"event":"dictionary-cancelled","unavailable":true}));
        // The driver and original owner stay alive until the external observer
        // verifies custody and explicitly releases the held SQL COMMIT.
        gate(path, "finish").await;
    }
    let expected = match mode {
        "pass" | "cancel-reservation" | "cancel-outcome" => None,
        "reservation-before" | "reservation-after" => Some(InitialEventError::Reservation(
            ReservationError::CommitUnknown,
        )),
        _ => Some(InitialEventError::Outcome(OutcomeError::CommitUnknown)),
    };
    let delivered = match result {
        Some(Ok(value)) => {
            assert!(expected.is_none() && !interrupted);
            Some(value.receipt().encode_persistent(8192).unwrap())
        }
        Some(Err(error)) => {
            assert_eq!(Some(error), expected);
            None
        }
        None => {
            assert!(interrupted);
            None
        }
    };
    emit(serde_json::json!({"event":"dictionary-operation-finished",
        "error":expected.map(|e|format!("{e:?}")),"cancelled":interrupted,
        "receipt":delivered.as_ref().map(|b|hex(b)),"state":ingest_snapshot::state(observer,path).await}));
    let manifest = *store.manifest();
    drop(store);
    drop(owner);
    ingest_closing::join(driver, !interrupted && mode != "pass").await;
    emit(serde_json::json!({"event":"dictionary-recovery-ready","driver_joined":true}));
    gate(path, "recover").await;

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
    let unresolved = mode.starts_with("reservation-") || mode == "cancel-reservation";
    assert_eq!(
        closed.issued_through(),
        if unresolved { 1 } else { sequence }
    );
    drop(closed);
    let baseline = ingest_snapshot::state(observer, path).await;
    emit(serde_json::json!({"event":"dictionary-closed","state":baseline}));
    let mut previous = None;
    for attempt in 0..if unresolved { 1 } else { 2 } {
        let result = current
            .resolve_initial_producer_request(
                &recovery,
                ProducerResolveRequest {
                    ingest_root: path,
                    registration: spec,
                    request: ProducerRequestId {
                        producer_slot_id: spec.producer_slot_id,
                        producer_epoch: ProducerEpoch::new(1).unwrap(),
                        request_sequence: RequestSequence::new(sequence).unwrap(),
                    },
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
                    policy_ttl: Duration::from_secs(2),
                    cancelled: &cancelled,
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
                panic!("dictionary outcome changed")
            };
            let bytes = receipt.encode_persistent(8192).unwrap();
            if let Some(original) = &delivered {
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
            serde_json::json!({"event":"dictionary-resolved","attempt":attempt,"unresolved":unresolved,
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
    ingest_closing::join(driver, false).await;
    emit(serde_json::json!({"event":"dictionary-fault-done","driver_joined":true}));
}
