//! Qualification-only retained-attempt holder and valid unprovisioned issuance.
//! No catalog writer, receipt, seal or frontier is manufactured by this helper.
use super::*;
use bigbrotr::storage::{
    archive::{LifetimeError, LocalAttempt, LocalCoordinator, RetainedInputs},
    postgres::{
        archive_attempt::ArchiveAttemptRequest,
        catalog_guard::CatalogIdentity,
        control_owner::{RetainedArchiveFrontier, RetainedArchiveRequest, SuccessorHistoryState},
    },
    rocks::store::PendingBatchHeader,
};
use bigbrotr_model::{ArchiveBatch, Sequence, SourceRecordId};

struct Custody {
    native: Option<IngestRecovery>,
    history: Option<SuccessorHistory>,
    owner: Option<ControlSession>,
    coordinator: Option<LocalCoordinator>,
}
struct ClaimExit<'a>(Option<&'a mut Option<IngestRecovery>>);
impl Drop for ClaimExit<'_> {
    fn drop(&mut self) {
        if let Some(native) = self.0.take() {
            drop(native.take());
        }
    }
}

pub async fn run(config: Config, file: &Path, metadata: u64, gate: Option<&Path>) {
    assert_eq!(metadata, MAX_LINEAGE_METADATA_BYTES);
    let root = crate::config::resolve::parse(&std::fs::read_to_string(file).unwrap()).unwrap();
    assert_eq!(config.get_dbname(), root.text("postgres.control.database"));
    let path = PathBuf::from(root.text("storage.ingest_root").unwrap());
    let dataset: DatasetId = root.text("topology.dataset_id").unwrap().parse().unwrap();
    let catalog = root.text("archive.catalog_id").unwrap().parse().unwrap();
    let identity = CatalogIdentity::local(
        dataset,
        catalog,
        &format!(
            "{}/",
            root.text("storage.archive_root")
                .unwrap()
                .trim_end_matches('/')
        ),
    )
    .unwrap();
    let state = PathBuf::from(root.text("storage.archive_state_root").unwrap());
    let n = |field: &str| root.number(&format!("runtime.processes.application.{field}"));
    let budget = ResourceBudget::new(
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
    );
    let _observations = grant(&budget, 32 * MIB, 0);
    let _transport = grant(&budget, 4 * MIB, 0);
    let metadata = grant(&budget, metadata, 0);
    let mut custody = Custody {
        native: Some(
            IngestRecovery::inspect(&path, dataset, limits(), grant(&budget, 0, 128 * MIB))
                .unwrap(),
        ),
        history: None,
        owner: None,
        coordinator: Some(LocalCoordinator::open(&identity, &state).unwrap()),
    };
    let pending_limits = PendingReadLimits {
        max_records: 64,
        max_encoded_bytes: MIB as usize,
        fact: FactDecodeLimits::default(),
    };
    // Serial issuance retains one pending payload and one new input, not 122 payloads.
    let preflight_pending = grant(&budget, pending_limits.required_data_bytes().unwrap(), 0);
    let preflight_inputs = grant(&budget, 5 * RetainedInputs::required_data_bytes(), 0);
    drop(replay_grants(&budget));
    drop(effect_grant(&budget));
    drop(preflight_inputs);
    drop(preflight_pending);
    emit(
        serde_json::json!({"event":"archive-attempt-resources","mode":if gate.is_some(){"holder"}else{"cap"},
        "metadata_bytes":MAX_LINEAGE_METADATA_BYTES,"native_bytes":128*MIB,"observation_bytes":32*MIB,
        "transport_bytes":4*MIB,"input_bytes":RetainedInputs::required_data_bytes(),
        "pending_bytes":pending_limits.required_data_bytes().unwrap(),"items":16,"operation_timeout_ms":10000,
        "holder_timeout_ms":180000,"new_attempts":if gate.is_some(){0}else{122}}),
    );
    let (observer, connection) =
        tokio::time::timeout(Duration::from_secs(10), config.connect(NoTls))
            .await
            .unwrap()
            .unwrap();
    let observer_driver = tokio::spawn(connection);
    let probe = Probe {
        root,
        path,
        budget,
        observer,
        database: config.get_dbname().unwrap().into(),
        cancelled: AtomicBool::new(false),
    };
    probe.bounded("observer-settings",probe.observer.batch_execute(
        "SET max_parallel_workers_per_gather=0; SET statement_timeout='8s'; SET lock_timeout='5s'")).await.unwrap();
    probe.snapshot("attempt-before-claim").await;
    let (client, connection) = probe
        .bounded("connect", config.connect(NoTls))
        .await
        .unwrap();
    let driver = tokio::spawn(
        custody
            .coordinator
            .as_ref()
            .unwrap()
            .retain_connection(connection)
            .unwrap(),
    );
    let fresh = || {
        let mut v = [0; 16];
        getrandom::fill(&mut v).unwrap();
        assert_ne!(v, [0; 16]);
        v
    };
    let role = fresh();
    let incarnation = fresh();
    let instance = crate::ingest_history::configured_instance(
        probe.root.text("topology.instance_id").unwrap(),
    );
    let policy = {
        let committing = Cell::new(false);
        let acquire = ControlSession::acquire(
            client,
            ClaimRequest {
                dataset,
                instance: instance.try_into().unwrap(),
                authority_mode: probe.root.text("config.authority_mode").unwrap().into(),
                role_instance_id: role,
                incarnation: incarnation.try_into().unwrap(),
                lease_ttl: CONTROL_TTL,
            },
            |d, h| probe.validate(d, h),
            &committing,
        );
        tokio::pin!(acquire);
        // On caller timeout/unwind native closes before this pinned acquisition drops.
        let mut exit = ClaimExit(Some(&mut custody.native));
        let (owner, policy) = probe.bounded("claim", acquire.as_mut()).await.unwrap();
        custody.owner = Some(owner);
        exit.0.take();
        drop(exit);
        policy
    };
    let owner = custody.owner.as_mut().unwrap();
    probe.policy(owner, "acquired", None, None, policy);
    let backend = probe.backend(owner).await;
    probe.snapshot("attempt-after-claim").await;
    probe
        .refresh(owner, None, None)
        .await
        .map_err(|e| e.error)
        .unwrap();
    probe
        .bounded(
            "coordinator-binding",
            owner.require_archive_coordinator(custody.coordinator.as_ref().unwrap()),
        )
        .await
        .unwrap();
    let mut scan = probe
        .bounded("scan", owner.begin_archive_attempt_scan(catalog))
        .await
        .unwrap();
    assert_eq!(scan.high_water(), 4);
    let originals = probe
        .bounded("page", owner.read_archive_attempt_page(&mut scan))
        .await
        .unwrap();
    assert!(scan.is_complete());
    assert_eq!(originals.len(), 4);
    let mut held = None;
    if gate.is_some() {
        held = Some(LocalAttempt::recover(&state, &originals[0], &identity).unwrap());
        assert!(matches!(
            LocalAttempt::recover(&state, &originals[0], &identity),
            Err(LifetimeError::Occupied)
        ));
        emit(
            serde_json::json!({"event":"archive-attempt-held","attempt_id":hex(originals[0].request().attempt_id()),
            "ordinal":1,"state_root":state,"catalog_root":identity.root(),"second_recover":"Occupied"}),
        );
    } else {
        custody.history = Some(
            probe
                .load(owner, custody.native.as_ref().unwrap(), metadata)
                .await
                .map_err(|e| e.error)
                .unwrap(),
        );
        let history = custody.history.as_ref().unwrap();
        assert_eq!(history.state(), SuccessorHistoryState::Terminal);
        assert_eq!(history.native_epoch(), 3);
        assert_eq!(history.registered_epoch(), 3);
        let proof = probe
            .replay(owner, custody.native.as_ref().unwrap(), history)
            .await
            .map_err(|e| e.error)
            .unwrap();
        assert_eq!(proof.verified_through(), 3);
        let coordinator = custody.coordinator.as_ref().unwrap();
        let inputs: Vec<_> = originals
            .iter()
            .map(|receipt| {
                coordinator
                    .read_inputs(
                        receipt.request(),
                        grant(&probe.budget, RetainedInputs::required_data_bytes(), 0),
                        &probe.cancelled,
                    )
                    .unwrap()
                    .unwrap()
            })
            .collect();
        probe
            .refresh(owner, None, None)
            .await
            .map_err(|e| e.error)
            .unwrap();
        let frontier = probe
            .bounded(
                "current-frontier",
                owner.inspect_retained_archive_frontier(
                    &proof,
                    custody.native.as_ref().unwrap(),
                    RetainedArchiveRequest {
                        coordinator,
                        attempts: &scan,
                    },
                    probe.request(),
                    grant(
                        &probe.budget,
                        RetainedArchiveFrontier::required_data_bytes(),
                        0,
                    ),
                    |d, h| probe.validate(d, h),
                ),
            )
            .await
            .unwrap();
        let frontier = frontier
            .bind_current_inputs(&inputs.iter().collect::<Vec<_>>())
            .unwrap();
        assert_eq!(frontier.issued_attempts(), 4);
        assert!(frontier.current_receipt().is_some());
        assert_eq!(frontier.current_cut().unwrap().receipts.len(), 4);
        drop(frontier);
        drop(inputs);
        let id = history.lineage().native_head().manifest().identity();
        let first = SourceRecordId {
            dataset_id: dataset,
            shard_id: id.shard_id,
            owner_epoch: id.owner_epoch,
            sequence: Sequence::new(1).unwrap(),
        };
        let pending = proof
            .resolved_lineage()
            .read_complete_pending_batch(
                first,
                pending_limits,
                grant(
                    &probe.budget,
                    pending_limits.required_data_bytes().unwrap(),
                    0,
                ),
                &probe.cancelled,
            )
            .unwrap();
        let header = PendingBatchHeader::from_batch(pending.batch());
        let batch = ArchiveBatch::new(
            1,
            id.topology_generation,
            std::slice::from_ref(pending.batch()),
        )
        .unwrap();
        emit(
            serde_json::json!({"event":"archive-attempt-input","first":first,"header":hex(&header.encode()),
            "archive_sha256":hex(batch.digest().as_bytes()),"topology":id.topology_generation.get()}),
        );
        for ordinal in 5..=126 {
            let policy = probe
                .bounded(
                    "attempt-refresh",
                    owner.refresh(|d, h| probe.validate(d, h)),
                )
                .await
                .unwrap();
            probe.policy(owner, "attempt-refreshed", None, None, policy);
            proof.validate(&probe.cancelled).unwrap();
            let request = ArchiveAttemptRequest::new(
                fresh(),
                catalog,
                owner.identity().process_incarnation,
                fresh().try_into().unwrap(),
                ordinal - 1,
                &batch,
            )
            .unwrap();
            let retained = coordinator
                .retain_inputs(
                    &request,
                    &batch,
                    grant(&probe.budget, RetainedInputs::required_data_bytes(), 0),
                    &probe.cancelled,
                )
                .unwrap();
            let committing = Cell::new(false);
            let receipt = probe
                .bounded(
                    "register",
                    owner.register_retained_archive_attempt(&retained, &committing),
                )
                .await
                .unwrap();
            assert!(receipt.created && receipt.latest_issued && committing.get());
            assert_eq!(receipt.ordinal, ordinal);
            // No manager connection, writer/login provisioning, or worker exists.
            let local = LocalAttempt::create(&state, &receipt, &identity).unwrap();
            proof.validate(&probe.cancelled).unwrap();
            emit(
                serde_json::json!({"event":"archive-attempt-issued","ordinal":ordinal,"attempt_id":hex(request.attempt_id()),
                "worker_incarnation":hex(request.worker().as_bytes()),"coordinator_incarnation":hex(request.coordinator().as_bytes()),
                "registered":true,"local_created":true,"provisioned":false,"published":false}),
            );
            drop(local);
            drop(retained);
        }
        probe.snapshot("attempt-issued126").await;
    }
    drop(custody.history.take());
    drop(custody.native.take());
    emit(serde_json::json!({"event":"archive-attempt-native","state":"closed"}));
    probe.snapshot("attempt-before-release").await;
    probe
        .bounded("release", custody.owner.take().unwrap().release())
        .await
        .unwrap();
    crate::ingest_closing::join(driver, false).await;
    probe.absent(&backend).await;
    emit(serde_json::json!({"event":"archive-attempt-driver","driver":"control","joined":true}));
    probe.snapshot("attempt-released").await;
    drop(probe);
    crate::ingest_closing::join(observer_driver, false).await;
    emit(serde_json::json!({"event":"archive-attempt-driver","driver":"observer","joined":true}));
    drop(custody.coordinator.take());
    if let Some(gate) = gate {
        assert!(gate.is_absolute() && gate.parent() == Some(Path::new("/lab")) && !gate.exists());
        // A live attempt holder must NOT retain the global initialization lock.
        drop(LocalCoordinator::open(&identity, &state).unwrap());
        emit(
            serde_json::json!({"event":"archive-attempt-ready","native":"closed","control":"released-and-joined",
            "observer":"joined","coordinator":"released-and-reacquired","gate":gate,"ordinal":1}),
        );
        tokio::time::timeout(Duration::from_secs(180), async {
            while !gate.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(std::fs::read(gate).unwrap(), b"release\n");
        assert!(matches!(
            LocalAttempt::recover(&state, &originals[0], &identity),
            Err(LifetimeError::Occupied)
        ));
        drop(held.take());
        drop(LocalAttempt::recover(&state, &originals[0], &identity).unwrap());
        emit(
            serde_json::json!({"event":"archive-attempt-holder","state":"released-and-recovered"}),
        );
    }
    emit(
        serde_json::json!({"event":"archive-attempt-complete","mode":if gate.is_some(){"holder"}else{"cap"},
        "native":"closed","control":"released-and-joined","observer":"joined","coordinator":"closed",
        "new_attempts":if gate.is_some(){0}else{122},"provisioned":0,"published":0}),
    );
}
