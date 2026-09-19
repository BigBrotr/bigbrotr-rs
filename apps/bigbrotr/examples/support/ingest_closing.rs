//! Actual current-Control / historical-native closing consumer for isolated runs.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{ProducerCloseError, ProducerCloseRequest},
    rocks::store::{AdmissionLimits, IngestRecovery, PreparedEvent},
};
use bigbrotr_model::{EventIntent, ProducerEpoch, ProducerRequestId, RequestSequence};

#[allow(
    clippy::too_many_arguments,
    reason = "isolated acceptance client owns lab handles"
)]
pub async fn run(
    mut config: Config,
    mut owner: ControlSession,
    old_driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    observer: &tokio_postgres::Client,
    mut store: IngestStore,
    spec: &InitialRegistration,
    intent: &EvidenceIntent,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
    scope: QualifiedRef,
) {
    let cancelled = AtomicBool::new(false);
    let empty = mode == "event-closing-empty";
    if !empty {
        drop(
            owner
                .bootstrap_initial_dictionaries(
                    &mut store,
                    InitialBatchRequest {
                        ingest_root: path,
                        registration: spec,
                        intent,
                        recorded_at: ObservedTime::new(123),
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
                .unwrap(),
        );
        let event = serde_json::from_slice(include_bytes!(
            "../../../../tests/fixtures/ingest/import-event.jsonl"
        ))
        .unwrap();
        let prepared = PreparedEvent::new(
            ProducerRequestId {
                producer_slot_id: spec.producer_slot_id,
                producer_epoch: ProducerEpoch::new(1).unwrap(),
                request_sequence: RequestSequence::new(2).unwrap(),
            },
            EventIntent::import(
                1,
                scope,
                event,
                QualifiedRef {
                    dictionary_kind: DictionaryKind::DatasetLineage,
                    allocation_domain: [69; 16].try_into().unwrap(),
                    ..scope
                },
            )
            .unwrap(),
            256 * 1024,
            grant(budget, MIB, 0),
        )
        .unwrap();
        drop(
            owner
                .admit_initial_event(
                    &mut store,
                    bigbrotr::storage::postgres::control_owner::InitialEventRequest {
                        ingest_root: path,
                        registration: spec,
                        event: prepared,
                        provenance_enabled: true,
                        recorded_at: ObservedTime::new(456),
                        limits: AdmissionLimits {
                            max_outstanding_requests: 2,
                            ..AdmissionLimits::default()
                        },
                        policy_ttl: Duration::from_secs(2),
                        cancelled: &cancelled,
                    },
                    grant(budget, 40 * MIB, 10 * MIB),
                    |d, h, _| validate(d, h),
                )
                .await
                .unwrap(),
        );
    }
    let manifest = *store.manifest();
    drop(store);
    // Closing is a fresh actor's operation. Join the old actual driver before
    // attempting its replacement; a scheduling sleep is not backend exclusion.
    owner.release().await.unwrap();
    join(old_driver, false).await;
    if mode == "event-closing-commit-lost" {
        config.hostaddr("127.0.0.1".parse().unwrap()).port(6543);
    }
    config.application_name("bigbrotr-closing-fault");
    let (mut current, mut driver) = acquire(&config, spec, 33).await;
    assert_ne!(current.identity(), spec.control);
    if mode == "event-closing-pg-first" {
        observer.execute("UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=$1::bytea AND producer_epoch=1", &[&&spec.producer_slot_id.as_bytes()[..]]).await.unwrap();
    }
    if mode == "event-closing-old-lease" {
        observer.execute("UPDATE control.owner_epochs SET lease_expires_at_us=acquired_at_us+1, state='draining' WHERE resource_id=$1::bytea AND epoch=1", &[&&spec.ingest_resource_id.as_bytes()[..]]).await.unwrap();
    }
    let mut supplied = spec.clone();
    if mode == "event-closing-metadata" {
        supplied.created_at_us -= 1;
    }
    let before = crate::ingest_snapshot::state(observer, path).await;
    emit(serde_json::json!({"event":"closing-before", "state":before}));
    let mut recovery =
        IngestRecovery::open(path, manifest, limits(), grant(budget, 0, 128 * MIB)).unwrap();
    let request = || ProducerCloseRequest {
        ingest_root: if mode == "event-closing-root" {
            path.parent().unwrap()
        } else {
            path
        },
        registration: &supplied,
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    let failure = match mode {
        "event-closing-root" => Some(ProducerCloseError::Control(OwnerError::InvalidInput)),
        "event-closing-metadata" => Some(ProducerCloseError::Registry(
            bigbrotr::storage::postgres::ingest_registry::RegistryError::InvalidMetadata,
        )),
        "event-closing-policy" => Some(ProducerCloseError::Control(OwnerError::InvalidPolicy)),
        "event-closing-expired" => Some(ProducerCloseError::Control(OwnerError::Unavailable)),
        "event-closing-late-sql" => Some(ProducerCloseError::DeliveryUnknown),
        "event-closing-rollback" => Some(ProducerCloseError::Control(OwnerError::Backend)),
        "event-closing-commit-lost" => Some(ProducerCloseError::CommitUnknown),
        _ => None,
    };
    let interrupted = matches!(
        mode,
        "event-closing-rollback" | "event-closing-late-sql" | "event-closing-commit-lost"
    );
    let mut previous = None;
    for attempt in 0..2 {
        let mut validations = 0;
        let result = current
            .close_initial_producer(
                &mut recovery,
                request(),
                grant(budget, 4096, 4096),
                |d, h| {
                    validate(d, h)?;
                    validations += 1;
                    if mode == "event-closing-policy" {
                        return Err(OwnerError::InvalidPolicy);
                    }
                    if mode == "event-closing-expired" && validations == 1 {
                        std::thread::sleep(Duration::from_secs(3));
                    }
                    Ok(())
                },
            )
            .await;
        if attempt == 0
            && let Some(expected) = failure
        {
            let error = match result {
                Err(e) => e,
                Ok(_) => panic!("negative closing accepted"),
            };
            assert_eq!(error, expected);
            emit(serde_json::json!({"event":"closing-refused", "error":format!("{error:?}")}));
            drop(current);
            join(driver, mode == "event-closing-commit-lost").await;
            drop(recovery);
            let observed = crate::ingest_snapshot::state(observer, path).await;
            emit(serde_json::json!({"event":"closing-interrupted", "state":observed}));
            if !interrupted {
                assert_eq!(observed, before);
                return;
            }
            config
                .port(5432)
                .application_name("bigbrotr-closing-recovery");
            (current, driver) = acquire(&config, spec, 34).await;
            recovery = IngestRecovery::open(path, manifest, limits(), grant(budget, 0, 128 * MIB))
                .unwrap();
        } else {
            let closed = result.unwrap_or_else(|e| panic!("unexpected closing error: {e:?}"));
            assert_eq!(closed.issued_through(), if empty { 0 } else { 2 });
            assert_eq!(closed.manifest(), &manifest);
            assert_eq!(closed.producer_slot_id(), spec.producer_slot_id);
            emit(
                serde_json::json!({"event":"producer-closed", "attempt":attempt,
                "issued":closed.issued_through(), "current_control_epoch":current.identity().epoch.get(),
                "historical_control_epoch":spec.control.epoch.get()}),
            );
            let observed = crate::ingest_snapshot::state(observer, path).await;
            if let Some(previous) = &previous {
                assert_eq!(&observed, previous);
            }
            previous = Some(observed);
        }
    }
    drop(recovery);
    let after = crate::ingest_snapshot::state(observer, path).await;
    assert_eq!(
        after["sequence"].as_u64().unwrap(),
        before["sequence"].as_u64().unwrap() + 1
    );
    emit(serde_json::json!({"event":"closing-after", "state":after}));
    current.release().await.unwrap();
    join(driver, false).await;
}
async fn join(driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>, failed: bool) {
    let result = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.is_err(), failed);
}
async fn acquire(
    config: &Config,
    spec: &InitialRegistration,
    id: u8,
) -> (
    ControlSession,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let (client, driver) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let (current, _) = ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: spec.dataset_id,
            instance: spec.instance_id.try_into().unwrap(),
            authority_mode: "managed".into(),
            role_instance_id: [id; 16],
            incarnation: [id + 10; 16].try_into().unwrap(),
            lease_ttl: Duration::from_secs(60),
        },
        validate,
        &Cell::new(false),
    )
    .await
    .unwrap();
    (current, driver)
}
