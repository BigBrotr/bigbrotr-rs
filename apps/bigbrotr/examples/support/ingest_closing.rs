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
    let recovering = mode.starts_with("event-recovery-");
    let issued = if empty {
        0
    } else if mode == "event-recovery-already" {
        3
    } else {
        2
    };
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
        let mut event: bigbrotr_model::SignedEvent = serde_json::from_slice(include_bytes!(
            "../../../../tests/fixtures/ingest/import-event.jsonl"
        ))
        .unwrap();
        if mode == "event-recovery-rejected" {
            event.sig = bigbrotr_model::Signature::from_bytes([0; 64]);
        }
        for sequence in 2..=issued {
            let prepared = PreparedEvent::new(
                ProducerRequestId {
                    producer_slot_id: spec.producer_slot_id,
                    producer_epoch: ProducerEpoch::new(1).unwrap(),
                    request_sequence: RequestSequence::new(sequence).unwrap(),
                },
                EventIntent::import(
                    1,
                    scope,
                    event.clone(),
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
            let admitted = owner
                .admit_initial_event(
                    &mut store,
                    bigbrotr::storage::postgres::control_owner::InitialEventRequest {
                        ingest_root: path,
                        registration: spec,
                        event: prepared,
                        relay_source: None,
                        acquisition: None,
                        provenance_enabled: true,
                        recorded_at: ObservedTime::new(if sequence == 2 { 456 } else { 1000 }),
                        limits: AdmissionLimits {
                            max_outstanding_requests: u64::from(spec.outstanding_request_limit),
                            ..AdmissionLimits::default()
                        },
                        policy_ttl: Duration::from_secs(2),
                        cancelled: &cancelled,
                    },
                    grant(budget, 40 * MIB, 10 * MIB),
                    |d, h, _| validate(d, h),
                )
                .await;
            if recovering && sequence == issued && mode != "event-recovery-copy-conflict" {
                assert!(matches!(
                    admitted,
                    Err(
                        bigbrotr::storage::postgres::control_owner::InitialEventError::Outcome(
                            bigbrotr::storage::postgres::producer_outcomes::OutcomeError::Backend
                        )
                    )
                ));
                emit(serde_json::json!({"event":"recovery-seed-copy-rolled-back"}));
            } else {
                drop(admitted.unwrap());
                if mode == "event-recovery-copy-conflict" {
                    emit(serde_json::json!({"event":"recovery-seed-copy-corrupted"}));
                }
            }
        }
    }
    if mode == "event-recovery-hold-old" {
        emit(
            serde_json::json!({"event":"old-native-held", "state":crate::ingest_snapshot::state(observer, path).await}),
        );
        // The external finite supervisor observes the real POSIX LOCK, kills
        // only this Control backend, then kills and reaps this entire process.
        tokio::time::sleep(Duration::from_secs(120)).await;
        panic!("held-owner supervisor failed to finish");
    }
    let manifest = *store.manifest();
    drop(store);
    // Closing is a fresh actor's operation. Join the old actual driver before
    // attempting its replacement; a scheduling sleep is not backend exclusion.
    if recovering {
        drop(owner);
    } else {
        owner.release().await.unwrap();
    }
    join(old_driver, false).await;
    let direct_config = config.clone();
    if matches!(
        mode,
        "event-closing-commit-lost" | "event-recovery-commit-lost"
    ) {
        config.hostaddr("127.0.0.1".parse().unwrap()).port(6543);
    }
    config.application_name("bigbrotr-closing-fault");
    let (mut current, mut driver) = acquire(&config, spec, 33).await;
    assert_ne!(current.identity(), spec.control);
    if matches!(mode, "event-closing-pg-first" | "event-recovery-pg-only") {
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
    for attempt in 0..if mode == "event-recovery-pg-only" {
        0
    } else {
        2
    } {
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
            // Config::port appends: retain one port per host after proxy loss.
            config = direct_config.clone();
            config.application_name("bigbrotr-closing-recovery");
            (current, driver) = acquire(&config, spec, 34).await;
            recovery = IngestRecovery::open(path, manifest, limits(), grant(budget, 0, 128 * MIB))
                .unwrap();
        } else {
            let closed = result.unwrap_or_else(|e| panic!("unexpected closing error: {e:?}"));
            assert_eq!(closed.issued_through(), issued);
            assert_eq!(closed.native().manifest(), &manifest);
            assert_eq!(closed.native().producer_slot_id(), spec.producer_slot_id);
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
    if mode == "event-recovery-missing-detail" {
        // Deliberate lab corruption after the complete closing barrier, retaining
        // the original input, issued counter and every scientific record.
        drop(recovery);
        emit(
            serde_json::json!({"event":"recovery-corruption-before", "state":crate::ingest_snapshot::state(observer, path).await}),
        );
        let db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            path,
            ["default", "facts", "receipts"],
        )
        .unwrap();
        let key = [
            b"\x01\x30".as_slice(),
            spec.producer_slot_id.as_bytes(),
            &1u64.to_be_bytes(),
            &2u64.to_be_bytes(),
        ]
        .concat();
        let cf = db.cf_handle("receipts").unwrap();
        assert!(db.get_cf(cf, &key).unwrap().is_some());
        let mut options = rocksdb::WriteOptions::default();
        options.set_sync(true);
        db.delete_cf_opt(cf, &key, &options).unwrap();
        drop(db);
        emit(
            serde_json::json!({"event":"recovery-corruption-after", "state":crate::ingest_snapshot::state(observer, path).await}),
        );
        recovery =
            IngestRecovery::open(path, manifest, limits(), grant(budget, 0, 128 * MIB)).unwrap();
    }
    if recovering {
        crate::ingest_recovery::run(
            direct_config,
            current,
            driver,
            recovery,
            observer,
            spec,
            path,
            budget,
            mode,
        )
        .await;
        return;
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
pub(super) async fn join(
    driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    failed: bool,
) {
    let result = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.is_err(), failed);
}
pub(super) async fn acquire(
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
