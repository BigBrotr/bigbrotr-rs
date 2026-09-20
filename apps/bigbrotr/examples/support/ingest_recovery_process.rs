//! Separate-process consumer of the original fixed signed fixture registration.
use super::*;
use bigbrotr::storage::{
    postgres::{control_owner::ProducerCloseRequest, ingest_registry::ControlOwner},
    rocks::store::{IngestRecovery, StoreError},
};

pub async fn run(config: Config, mode: &str, path: &Path) {
    assert!(matches!(
        mode,
        "recovery-process-held" | "recovery-process-resume"
    ));
    emit(serde_json::json!({"event":"recovery-process", "pid":std::process::id()}));
    let (observer, observer_driver) = config.connect(NoTls).await.unwrap();
    let observer_driver = tokio::spawn(observer_driver);
    // These are fixture coordinates read from retained registration history, not
    // a production factory or an assumption that PG metadata grants native access.
    // Every field is revalidated by the actual connected recovery operation.
    let row = observer.query_one("SELECT c.resource_id, i.acquired_at_us, i.lease_expires_at_us, cfg.document_digest, p.document_digest FROM control.owner_epochs c CROSS JOIN control.owner_epochs i CROSS JOIN control.config_revisions cfg CROSS JOIN control.policy_revisions p WHERE c.role='control' AND c.epoch=1 AND i.role='ingest' AND i.epoch=1 AND cfg.revision=1 AND p.revision=1", &[]).await.unwrap();
    let spec = InitialRegistration {
        dataset_id: [1; 16].try_into().unwrap(),
        instance_id: [2; 16],
        control: ControlOwner {
            resource_id: <[u8; 16]>::try_from(row.get::<_, &[u8]>(0))
                .unwrap()
                .try_into()
                .unwrap(),
            epoch: OwnerEpoch::new(1).unwrap(),
            role_instance_id: [31; 16],
            process_incarnation: [41; 16].try_into().unwrap(),
        },
        ingest_resource_id: [61; 16].try_into().unwrap(),
        ingest_role_instance_id: [62; 16],
        process_incarnation: [63; 16].try_into().unwrap(),
        shard_id: 0,
        config_revision: 1,
        policy_revision: 1,
        config_digest: Digest256::from_bytes(row.get::<_, &[u8]>(3).try_into().unwrap()),
        policy_digest: Digest256::from_bytes(row.get::<_, &[u8]>(4).try_into().unwrap()),
        previous_topology: None,
        allocation_domains: vec![
            (
                DictionaryKind::VisibilityScope,
                [64; 16].try_into().unwrap(),
            ),
            (DictionaryKind::Event, [68; 16].try_into().unwrap()),
            (DictionaryKind::DatasetLineage, [69; 16].try_into().unwrap()),
        ],
        producer_slot_id: [66; 16].try_into().unwrap(),
        outstanding_request_limit: 2,
        created_at_us: row.get(1),
        lease_expires_at_us: row.get(2),
    };
    let manifest = IngestManifest::new(
        ManifestIdentity {
            dataset_id: spec.dataset_id,
            topology_generation: TopologyGeneration::new(0),
            owner_resource_id: spec.ingest_resource_id,
            shard_id: 0,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            process_incarnation: spec.process_incarnation,
            recovery_generation: RecoveryGenerationId::from_bytes([67; 16]),
            policy_epoch: PolicyEpoch::new(1),
        },
        AllocationNamespaces::from_entries(&spec.allocation_domains).unwrap(),
    );
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            241 * MIB,
            0,
            ResourceLimits::new(96 * MIB, 8, 144 * MIB, MIB),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    );
    let _inputs = grant(&budget, 24 * MIB, 0);
    let before = crate::ingest_snapshot::state(&observer, path).await;
    emit(serde_json::json!({"event":"process-before", "state":before}));
    let opened = IngestRecovery::open(path, manifest, limits(), grant(&budget, 0, 128 * MIB));
    if mode == "recovery-process-held" {
        assert!(matches!(opened, Err(StoreError::EngineOpen)));
        let after = crate::ingest_snapshot::state(&observer, path).await;
        assert_eq!(before, after);
        emit(serde_json::json!({"event":"held-open-refused", "error":"EngineOpen", "state":after}));
    } else {
        let mut recovery = opened.unwrap();
        let (mut current, driver) = crate::ingest_closing::acquire(&config, &spec, 33).await;
        assert_eq!(current.identity().epoch.get(), 2);
        emit(
            serde_json::json!({"event":"closing-before", "state":crate::ingest_snapshot::state(&observer, path).await}),
        );
        let cancelled = AtomicBool::new(false);
        for attempt in 0..2 {
            let closed = current
                .close_initial_producer(
                    &mut recovery,
                    ProducerCloseRequest {
                        ingest_root: path,
                        registration: &spec,
                        policy_ttl: Duration::from_secs(2),
                        cancelled: &cancelled,
                    },
                    grant(&budget, 4096, 4096),
                    validate,
                )
                .await
                .unwrap();
            assert_eq!(closed.issued_through(), 2);
            emit(
                serde_json::json!({"event":"producer-closed", "attempt":attempt,
                "issued":closed.issued_through(), "current_control_epoch":current.identity().epoch.get(),
                "historical_control_epoch":spec.control.epoch.get()}),
            );
        }
        crate::ingest_recovery::run(
            config,
            current,
            driver,
            recovery,
            &observer,
            &spec,
            path,
            &budget,
            "event-recovery-normal",
        )
        .await;
    }
    drop(observer);
    crate::ingest_closing::join(observer_driver, false).await;
}
