//! Finite read-only successor history under one actual native/Control lifetime.
//! This isolated blocking owner is not installed supervision or writer adoption.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        InitialHistoryRequest, PolicySnapshot, SuccessorHistory, SuccessorHistoryResolution,
        SuccessorReplayRequest, VerifiedSuccessorHistory,
    },
    rocks::{
        store::{
            AdmissionLimits, IngestRecovery, LineageEpochSummary, ProducerRecoveryLimits,
            RetainedRequestInput, RetainedTerminalKind, compacted_outcome_data_bytes,
        },
        succession::{LINEAGE_ENTRY_BYTES, LINEAGE_SCRATCH_BYTES, MAX_LINEAGE_METADATA_BYTES},
    },
};
use bigbrotr_model::{
    DatasetId, ProducerRequestId,
    codec::{FactDecodeLimits, request::RequestDecodeLimits},
};
use std::{fmt::Debug, future::Future, path::PathBuf, sync::atomic::Ordering};

#[path = "ingest_archive_attempts.rs"]
pub(super) mod archive_attempts;
#[path = "ingest_successor_transition.rs"]
pub(super) mod transition;

const CONTROL_TTL: Duration = Duration::from_secs(20);
const FRAME_BYTES: usize = 16 * MIB as usize;

// Preserve native-first teardown on unexpected errors/unwinding too. A timeout
// emits no completed custody receipt; the laboratory must reap the whole worker.
struct Custody {
    native: Option<IngestRecovery>,
    owner: Option<ControlSession>,
}
struct Probe {
    root: config::Resolved,
    path: PathBuf,
    budget: ResourceBudget,
    observer: tokio_postgres::Client,
    database: String,
    cancelled: AtomicBool,
}
struct Failure {
    stage: &'static str,
    epoch: Option<u64>,
    sequence: Option<u64>,
    error: String,
}
impl Failure {
    fn at(
        stage: &'static str,
        epoch: Option<u64>,
        sequence: Option<u64>,
        error: impl Debug,
    ) -> Self {
        Self {
            stage,
            epoch,
            sequence,
            error: format!("{error:?}"),
        }
    }
}
struct Backend {
    pid: i32,
    start: String,
    database: String,
    control_epoch: u64,
}
impl Backend {
    fn emit(&self, present: bool) {
        emit(
            serde_json::json!({"event":"lineage-history-backend","pid":self.pid,
            "backend_start":self.start,"database":self.database,"control_epoch":self.control_epoch,
            "present":present}),
        );
    }
}

// Finite test seams only. The external fixture owns proxy loss/head mutation;
// these gates observe real product boundaries and never manufacture a result.
struct HistoryFault {
    port: u16,
    mode: &'static str,
    prefix: PathBuf,
    cancel: PathBuf,
}
impl HistoryFault {
    const STAGES: [&'static str; 5] = [
        "after-factory",
        "before-compacted",
        "compacted-returned",
        "before-final",
        "final-returned",
    ];
    const POLL: Duration = Duration::from_millis(5);
    const GATE_TIMEOUT: Duration = Duration::from_secs(10);

    fn new(port: u16, mode: &str, prefix: &Path) -> Self {
        let mode = match mode {
            "baseline" => "baseline",
            "wire" => "wire",
            "head-after-factory" => "head-after-factory",
            "head-before-final" => "head-before-final",
            "cancel" => "cancel",
            "expiry" => "expiry",
            _ => panic!("unknown compacted history fault mode"),
        };
        assert_ne!(port, 0);
        let text = prefix.to_str().expect("UTF-8 gate prefix");
        assert!(prefix.is_absolute() && text.len() <= 512 && prefix.file_name().is_some());
        assert!(prefix.components().all(|part| matches!(
            part,
            std::path::Component::RootDir | std::path::Component::Normal(_)
        )));
        assert!(prefix.parent().unwrap().is_dir());
        let mut result = Self {
            port,
            mode,
            prefix: prefix.into(),
            cancel: PathBuf::new(),
        };
        result.cancel = result.signal("cancel");
        // Refuse stale/unsafe signals before native inspection or Control claim.
        for suffix in ["go", "cancel"] {
            assert!(!fault_signal(&result.signal(suffix)).expect("initially absent signal"));
        }
        for stage in Self::STAGES {
            assert!(
                !fault_signal(&result.signal(&format!("{stage}.go")))
                    .expect("initially absent baseline signal")
            );
        }
        result
    }
    fn signal(&self, suffix: &str) -> PathBuf {
        let mut name = self.prefix.as_os_str().to_owned();
        name.push(".");
        name.push(suffix);
        PathBuf::from(name)
    }
    async fn checkpoint(&self, stage: &'static str, error: Option<String>) -> Result<(), Failure> {
        assert!(Self::STAGES.contains(&stage));
        emit(serde_json::json!({"event":"lineage-history-fault-stage",
            "mode":self.mode,"stage":stage,"error":error}));
        let suffix = if self.mode == "baseline" {
            Some(format!("{stage}.go"))
        } else if (self.mode == "head-after-factory" && stage == "after-factory")
            || (self.mode == "head-before-final" && stage == "before-final")
        {
            Some("go".into())
        } else {
            None
        };
        if let Some(suffix) = suffix {
            self.gate(stage, self.signal(&suffix)).await?;
        }
        Ok(())
    }
    async fn gate(&self, stage: &'static str, path: PathBuf) -> Result<(), Failure> {
        let emit_gate = |phase| {
            emit(serde_json::json!({"event":"lineage-history-fault-gate",
            "mode":self.mode,"stage":stage,"phase":phase,"prefix":self.prefix,"path":path,
            "gate_timeout_ms":Self::GATE_TIMEOUT.as_millis(),"poll_ms":Self::POLL.as_millis()}))
        };
        emit_gate("ready");
        tokio::time::timeout(Self::GATE_TIMEOUT, async {
            loop {
                if fault_signal(&self.cancel)? {
                    return Err(std::io::Error::other("cancel preceded compacted dispatch"));
                }
                if fault_signal(&path)? {
                    return Ok(());
                }
                tokio::time::sleep(Self::POLL).await;
            }
        })
        .await
        .map_err(|_| Failure::at("fault-gate", None, None, "GateTimeout"))?
        .map_err(|e| Failure::at("fault-gate", None, None, e))?;
        emit_gate("released");
        Ok(())
    }
    fn cursor(&self, stage: &str, cursor: &SuccessorHistoryResolution<'_>) {
        emit(
            serde_json::json!({"event":"lineage-history-fault-cursor","mode":self.mode,
            "stage":stage,"current_epoch":cursor.current_epoch(),
            "needs_compacted_initial":cursor.needs_compacted_initial(),
            "next_request":cursor.next_request().map(|r|serde_json::json!({
                "producer_slot":hex(r.producer_slot_id.as_bytes()),"epoch":r.producer_epoch.get(),
                "sequence":r.request_sequence.get()}))}),
        );
    }
    async fn before_compacted(
        &self,
        probe: &Probe,
        owner: &ControlSession,
    ) -> Result<Option<i64>, Failure> {
        self.checkpoint("before-compacted", None).await?;
        if !matches!(self.mode, "cancel" | "expiry") {
            return Ok(None);
        }
        let refreshed = fault_clock(probe, owner).await;
        self.emit_clock("refreshed", &refreshed);
        let (_, deadline) = refreshed?;
        self.gate("before-compacted", self.signal("go")).await?;
        // No product transaction or refresh runs during preage. Actual PG UTC
        // and the same retained deadline, not elapsed sleep, establish liveness.
        let dispatch = tokio::time::timeout(Duration::from_secs(22), async {
            loop {
                let time = fault_clock(probe, owner).await?;
                if time.1 != deadline {
                    return Err(Failure::at("fault-clock", None, None, "LeaseChanged"));
                }
                if self.mode == "cancel" || time.1 - time.0 <= 2_000_000 {
                    return Ok(time);
                }
                let sleep_us = u64::try_from((time.1 - time.0 - 2_000_000).min(1_000_000))
                    .expect("positive remaining preage interval");
                tokio::time::sleep(Duration::from_micros(sleep_us)).await;
            }
        })
        .await
        .unwrap_or_else(|_| Err(Failure::at("fault-clock", None, None, "PreageTimeout")));
        self.emit_clock("dispatch", &dispatch);
        let (now, expires) = dispatch?;
        if (self.mode == "expiry" && !(1_000_000..=2_000_000).contains(&(expires - now)))
            || (self.mode == "cancel" && now >= expires)
        {
            return Err(Failure::at(
                "fault-clock",
                None,
                None,
                "MissedLiveDispatchWindow",
            ));
        }
        if fault_signal(&self.cancel).map_err(|e| Failure::at("fault-gate", None, None, e))? {
            return Err(Failure::at(
                "fault-gate",
                None,
                None,
                "CancelBeforeDispatch",
            ));
        }
        Ok(Some(deadline))
    }
    async fn pending<T>(&self, probe: &Probe, future: impl Future<Output = T>) -> T {
        if self.mode != "cancel" {
            return future.await;
        }
        tokio::pin!(future);
        let mut applied = false;
        loop {
            tokio::select! {
                // Cancellation sets the flag; the actual pinned operation must
                // return. Dropping its future would not prove refused delivery.
                biased;
                result = &mut future => return result,
                () = tokio::time::sleep(Self::POLL), if !applied => {
                    if fault_signal(&self.cancel).expect("invalid cancel signal; custody unknown") {
                        probe.cancelled.store(true, Ordering::Relaxed);
                        applied = true;
                        emit(serde_json::json!({"event":"lineage-history-fault-cancel",
                            "mode":self.mode,"phase":"applied","cancelled":true}));
                    }
                }
            }
        }
    }
    async fn returned(
        &self,
        probe: &Probe,
        owner: &ControlSession,
        deadline: Option<i64>,
    ) -> Result<(), Failure> {
        if let Some(deadline) = deadline {
            let observed = fault_clock(probe, owner).await;
            self.emit_clock("returned", &observed);
            if observed?.1 != deadline {
                return Err(Failure::at("fault-clock", None, None, "LeaseChanged"));
            }
        }
        Ok(())
    }
    fn emit_clock(&self, stage: &str, result: &Result<(i64, i64), Failure>) {
        emit(
            serde_json::json!({"event":"lineage-history-fault-clock","mode":self.mode,
            "stage":stage,"server_us":result.as_ref().ok().map(|v|v.0),
            "control_deadline_us":result.as_ref().ok().map(|v|v.1),
            "remaining_us":result.as_ref().ok().map(|v|v.1-v.0),
            "error":result.as_ref().err().map(|e|&e.error)}),
        );
    }
}

// Match the existing wait protocol: metadata only for an exact empty regular
// nonsymlink signal. Never open a FIFO/device, consume content or remove a file.
fn fault_signal(path: &Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() == 0 => Ok(true),
        Ok(_) => Err(std::io::Error::other(
            "signal must be an empty regular nonsymlink file",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
async fn fault_clock(probe: &Probe, owner: &ControlSession) -> Result<(i64, i64), Failure> {
    let id = owner.identity();
    let row = probe
        .bounded(
            "fault-clock",
            probe.observer.query_one(
                "SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint,
                o.lease_expires_at_us
         FROM control.owner_epochs o JOIN pg_catalog.pg_stat_activity a
           ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start
         WHERE o.resource_id=$1::bytea AND o.epoch=$2::text::numeric
           AND o.process_incarnation=$3::bytea AND o.role='control' AND o.state='active'
           AND a.datname=current_database()",
                &[
                    &&id.resource_id.as_bytes()[..],
                    &id.epoch.get().to_string(),
                    &&id.process_incarnation.as_bytes()[..],
                ],
            ),
        )
        .await
        .map_err(|e| Failure::at("fault-clock", None, None, e))?;
    Ok((row.get(0), row.get(1)))
}

pub async fn run(config: Config, config_file: &Path, metadata_bytes: u64) {
    run_profile(config, config_file, metadata_bytes, false, None).await;
}

pub async fn run_compacted(config: Config, config_file: &Path, metadata_bytes: u64) {
    run_profile(config, config_file, metadata_bytes, true, None).await;
}

pub async fn run_compacted_fault(
    config: Config,
    config_file: &Path,
    metadata_bytes: u64,
    port: u16,
    mode: &str,
    prefix: &Path,
) {
    let fault = HistoryFault::new(port, mode, prefix);
    run_profile(config, config_file, metadata_bytes, true, Some(&fault)).await;
}

async fn run_profile(
    config: Config,
    config_file: &Path,
    metadata_bytes: u64,
    compacted: bool,
    fault: Option<&HistoryFault>,
) {
    assert!(fault.is_none() || compacted);
    assert!(metadata_bytes <= MAX_LINEAGE_METADATA_BYTES);
    let root =
        crate::config::resolve::parse(&std::fs::read_to_string(config_file).unwrap()).unwrap();
    assert_eq!(config.get_dbname(), root.text("postgres.control.database"));
    assert!(root.local("control") && root.local("ingest"));
    let path = PathBuf::from(root.text("storage.ingest_root").unwrap());
    let dataset: DatasetId = root.text("topology.dataset_id").unwrap().parse().unwrap();
    let instance =
        crate::ingest_history::configured_instance(root.text("topology.instance_id").unwrap());
    let n = |field: &str| root.number(&format!("runtime.processes.application.{field}"));
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            n("memory_max_bytes"),
            n("baseline_bytes"),
            ResourceLimits::new(
                n("payload_bytes"),
                8,
                n("native_bytes"),
                n("completion_reserve_bytes"),
            ),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    // These are finite observation/transport grants, separate from the one
    // product lineage grant. A raw snapshot is bounded by the existing fixture
    // census limits and its emitted frame limit, not a physical-RSS guarantee.
    let _observations = grant(&budget, 32 * MIB, 0);
    let _transport = grant(&budget, 4 * MIB, 0);
    let metadata = grant(&budget, metadata_bytes, 0);
    let mut custody = Custody {
        native: Some(
            IngestRecovery::inspect(&path, dataset, limits(), grant(&budget, 0, 128 * MIB))
                .unwrap(),
        ),
        owner: None,
    };
    // The explicit capacity does not claim a pending target exists. SQL/native
    // inspection decides that. Preflight the largest live overlap before claim.
    drop(replay_grants(&budget));
    drop(effect_grant(&budget));
    if compacted {
        drop(compacted_grant(&budget));
        emit(serde_json::json!({"event":"lineage-history-profile",
            "profile":"compacted-initial",
            "compacted_outcome_bytes":compacted_outcome_data_bytes(recovery_limits().event).unwrap()}));
    }
    emit(
        serde_json::json!({"event":"lineage-history-resources","metadata_bytes":metadata_bytes,
        "metadata_scratch_bytes":LINEAGE_SCRATCH_BYTES,"metadata_entry_bytes":LINEAGE_ENTRY_BYTES,
        "input_bytes":RetainedRequestInput::required_data_bytes(recovery_limits().input).unwrap(),
        "outcome_bytes":recovery_limits().required_data_bytes().unwrap(),
        "effect_bytes":recovery_limits().required_data_bytes().unwrap(),
        "native_bytes":128*MIB,"transport_bytes":4*MIB,"observation_bytes":32*MIB,
        "operation_timeout_ms":10000,"frame_bytes":FRAME_BYTES}),
    );
    let (observer, observer_connection) =
        tokio::time::timeout(Duration::from_secs(10), config.connect(NoTls))
            .await
            .expect("observer connection outcome unknown")
            .unwrap();
    let observer_driver = tokio::spawn(observer_connection);
    let probe = Probe {
        root,
        path,
        budget,
        observer,
        database: config.get_dbname().unwrap().into(),
        cancelled: AtomicBool::new(false),
    };
    probe.bounded("observer-settings",probe.observer.batch_execute(
        "SET max_parallel_workers_per_gather=0; SET statement_timeout='8s'; SET lock_timeout='5s'"))
        .await.unwrap();
    let mut role = [0; 16];
    let mut incarnation = [0; 16];
    getrandom::fill(&mut role).unwrap();
    getrandom::fill(&mut incarnation).unwrap();
    assert_ne!(role, [0; 16]);
    assert_ne!(incarnation, [0; 16]);
    emit(
        serde_json::json!({"event":"lineage-history-control","phase":"claim-planned",
        "role":hex(&role),"incarnation":hex(&incarnation),"lease_ttl_us":CONTROL_TTL.as_micros().to_string()}),
    );
    probe.snapshot("before-claim").await;
    let mut control_config = config.clone();
    if let Some(fault) = fault {
        // Only Control traverses the finite laboratory proxy. Observer queries
        // retain their direct connection, including clock and backend closure.
        control_config.port(fault.port);
        emit(
            serde_json::json!({"event":"lineage-history-fault","mode":fault.mode,
            "control_port":fault.port,"observer_port":5432,"prefix":fault.prefix,"proxied":true}),
        );
    }
    let (client, connection) = probe
        .bounded("connect", control_config.connect(NoTls))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let (owner, policy) = probe
        .bounded(
            "claim",
            ControlSession::acquire(
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
                &Cell::new(false),
            ),
        )
        .await
        .unwrap();
    custody.owner = Some(owner);
    let owner = custody.owner.as_ref().unwrap();
    emit(
        serde_json::json!({"event":"lineage-history-control","phase":"claimed",
        "resource":hex(owner.identity().resource_id.as_bytes()),"epoch":owner.identity().epoch.get(),
        "role":hex(&role),"incarnation":hex(&incarnation)}),
    );
    probe.policy(owner, "acquired", None, None, policy);
    let backend = probe.backend(owner).await;
    probe.snapshot("after-claim").await;
    let result = probe
        .read(
            custody.owner.as_mut().unwrap(),
            custody.native.as_ref().unwrap(),
            metadata,
            compacted,
            fault,
        )
        .await;
    emit(
        serde_json::json!({"event":"lineage-history-result","stage":result.as_ref().err().map_or("complete",|e|e.stage),
        "epoch":result.as_ref().err().and_then(|e|e.epoch),"sequence":result.as_ref().err().and_then(|e|e.sequence),
        "error":result.as_ref().err().map(|e|&e.error)}),
    );
    probe.snapshot("after-read").await;
    // Every history witness has ended. Native closes before the actual owner
    // release/drop and joined driver; failed reads never manufacture release.
    drop(custody.native.take());
    emit(serde_json::json!({"event":"lineage-history-native","state":"closed"}));
    probe.snapshot("before-control-end").await;
    let owner = custody.owner.take().unwrap();
    let identity = owner.identity();
    if result.is_ok() {
        probe.bounded("release", owner.release()).await.unwrap();
    } else {
        drop(owner);
    }
    let driver_result = tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("Control driver join outcome unknown")
        .expect("Control driver panicked");
    let failed = driver_result.is_err();
    emit(
        serde_json::json!({"event":"lineage-history-driver","driver":"control","failed":failed,"joined":true}),
    );
    // Record the actual joined result first. A selected proxy does not prove
    // loss; the independent fixture must also bind the exact wire boundary.
    assert!(!failed || (fault.is_some() && result.is_err()));
    probe.absent(&backend).await;
    emit(serde_json::json!({"event":"lineage-history-control",
        "phase":if result.is_ok(){"released-and-joined"}else{"dropped-and-joined"},
        "resource":hex(identity.resource_id.as_bytes()),"epoch":identity.epoch.get()}));
    probe.snapshot("after-control-end").await;
    probe.snapshot("final").await;
    drop(probe);
    crate::ingest_closing::join(observer_driver, false).await;
    emit(
        serde_json::json!({"event":"lineage-history-driver","driver":"observer","failed":false,"joined":true}),
    );
    emit(
        serde_json::json!({"event":"lineage-history-complete","native":"closed",
        "control":if result.is_ok(){"released-and-joined"}else{"dropped-and-joined"},
        "observer":"joined","success":result.is_ok(),"admission":false,"repair":false}),
    );
}

impl Probe {
    async fn bounded<T>(&self, stage: &str, future: impl Future<Output = T>) -> T {
        match tokio::time::timeout(Duration::from_secs(10), future).await {
            Ok(value) => value,
            Err(_) => {
                self.cancelled.store(true, Ordering::Relaxed);
                emit(
                    serde_json::json!({"event":"lineage-history-timeout","stage":stage,"outcome":"unknown"}),
                );
                panic!("finite history timeout; preserve partial evidence and reap owner");
            }
        }
    }
    fn validate(&self, document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
        crate::config::validate_fixture_policy(document, digest, &self.root)
    }
    fn request(&self) -> InitialHistoryRequest<'_> {
        InitialHistoryRequest {
            ingest_root: &self.path,
            policy_ttl: Duration::from_micros(self.root.number("runtime.authorization_ttl")),
            cancelled: &self.cancelled,
        }
    }
    fn policy(
        &self,
        owner: &ControlSession,
        phase: &str,
        epoch: Option<u64>,
        sequence: Option<u64>,
        policy: PolicySnapshot,
    ) {
        emit(
            serde_json::json!({"event":"lineage-history-policy","phase":phase,"epoch":epoch,"sequence":sequence,
            "control_epoch":owner.identity().epoch.get(),"config_revision":policy.config_revision,
            "policy_revision":policy.policy_revision,"config_digest":hex(&policy.config_digest),
            "policy_digest":hex(&policy.policy_digest)}),
        );
    }
    async fn refresh(
        &self,
        owner: &mut ControlSession,
        epoch: Option<u64>,
        sequence: Option<u64>,
    ) -> Result<(), Failure> {
        let policy = self
            .bounded("refresh", owner.refresh(|d, h| self.validate(d, h)))
            .await
            .map_err(|e| Failure::at("refresh", epoch, sequence, e))?;
        self.policy(owner, "refreshed", epoch, sequence, policy);
        self.snapshot(&format!(
            "refreshed-{}-{}",
            epoch.map_or("final".into(), |n| n.to_string()),
            sequence.map_or("epoch".into(), |n| n.to_string())
        ))
        .await;
        Ok(())
    }
    async fn snapshot(&self, stage: &str) {
        let state = self
            .bounded(
                "snapshot",
                crate::ingest_snapshot::state(&self.observer, &self.path),
            )
            .await;
        let (rows, sequence) = raw_state(&self.path);
        assert_eq!(state["sequence"].as_u64(), Some(sequence));
        let value = serde_json::json!({"event":"lineage-history-state","stage":stage,"state":state,"rows":rows});
        assert!(
            serde_json::to_vec(&value).unwrap().len() <= FRAME_BYTES,
            "finite snapshot frame exceeded"
        );
        emit(value);
    }
    async fn backend(&self, owner: &ControlSession) -> Backend {
        let id = owner.identity();
        let row=self.bounded("backend",self.observer.query_one(
            "SELECT json_build_object('pid',o.control_backend_pid,'backend_start',o.control_backend_start,
                'database',a.datname)::text FROM control.owner_epochs o JOIN pg_catalog.pg_stat_activity a
                ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start
                WHERE o.resource_id=$1::bytea AND o.epoch=$2::text::numeric AND o.process_incarnation=$3::bytea
                AND o.role='control' AND o.state='active' AND a.datname=current_database()",
            &[&&id.resource_id.as_bytes()[..],&id.epoch.get().to_string(),&&id.process_incarnation.as_bytes()[..]])).await.unwrap();
        let value: serde_json::Value = serde_json::from_str(row.get::<_, &str>(0)).unwrap();
        let backend = Backend {
            pid: i32::try_from(value["pid"].as_i64().unwrap()).unwrap(),
            start: value["backend_start"].as_str().unwrap().into(),
            database: value["database"].as_str().unwrap().into(),
            control_epoch: id.epoch.get(),
        };
        assert!(backend.pid > 0 && !backend.start.is_empty());
        assert_eq!(backend.database, self.database);
        backend.emit(true);
        backend
    }
    async fn absent(&self, backend: &Backend) {
        self.bounded("backend-absence",async {
            loop {
                let present:bool=self.observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity
                    WHERE pid=$1::integer AND backend_start=$2::text::timestamptz AND datname=$3::text)",
                    &[&backend.pid,&backend.start,&backend.database]).await.unwrap().get(0);
                if !present { break; }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }).await;
        backend.emit(false);
    }
    async fn read(
        &self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
        metadata: OwnedPermit,
        compacted: bool,
        fault: Option<&HistoryFault>,
    ) -> Result<(), Failure> {
        let history = self
            .load_profile(owner, recovery, metadata, compacted)
            .await?;
        if let Some(fault) = fault {
            fault.checkpoint("after-factory", None).await?;
        }
        self.replay_profile(owner, recovery, &history, compacted, fault)
            .await?;
        Ok(())
    }
    async fn load(
        &self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
        metadata: OwnedPermit,
    ) -> Result<SuccessorHistory, Failure> {
        self.load_profile(owner, recovery, metadata, false).await
    }
    async fn load_profile(
        &self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
        metadata: OwnedPermit,
        compacted: bool,
    ) -> Result<SuccessorHistory, Failure> {
        self.snapshot("before-read").await;
        let history = if compacted {
            self.bounded(
                "inspect-compacted",
                owner.inspect_compacted_successor_history(
                    recovery,
                    self.request(),
                    recovery_limits().event,
                    metadata,
                    effect_grant(&self.budget),
                    |d, h| self.validate(d, h),
                ),
            )
            .await
        } else {
            self.bounded(
                "inspect",
                owner.inspect_successor_history(recovery, self.request(), metadata, |d, h| {
                    self.validate(d, h)
                }),
            )
            .await
        }
        .map_err(|e| Failure::at("inspect", None, None, e))?;
        let lineage = history.lineage();
        let profile = lineage.profile();
        emit(
            serde_json::json!({"event":"lineage-history-loaded","state":format!("{:?}",history.state()),
            "native_epoch":history.native_epoch(),"registered_epoch":history.registered_epoch(),
            "replay_through":history.replay_through(),"producer_slot":hex(history.producer_slot_id().as_bytes()),
            "pending_target":lineage.pending_target(),"metadata_bytes":profile.required_metadata_bytes(),
            "memberships":profile.topology_memberships(),"entries":lineage.entries().iter().map(|entry|
                serde_json::json!({"manifest":hex(entry.manifest().encode().as_bytes()),"issued_through":entry.issued_through(),
                    "source_through":entry.source_through(),"closing":entry.is_closing(),
                    "incoming_sha256":hex(&entry.incoming_handoff_sha256()),
                    "terminal_kind":entry.terminal_claim().map(|(kind,_)|kind_name(kind)),
                    "terminal_digest":entry.terminal_claim().map(|(_,digest)|hex(&digest))})).collect::<Vec<_>>()}),
        );
        self.snapshot("loaded").await;
        Ok(history)
    }
    async fn replay<'a>(
        &'a self,
        owner: &mut ControlSession,
        recovery: &'a IngestRecovery,
        history: &'a SuccessorHistory,
    ) -> Result<VerifiedSuccessorHistory<'a>, Failure> {
        self.replay_profile(owner, recovery, history, false, None)
            .await
    }
    async fn replay_profile<'a>(
        &'a self,
        owner: &mut ControlSession,
        recovery: &'a IngestRecovery,
        history: &'a SuccessorHistory,
        compacted: bool,
        fault: Option<&HistoryFault>,
    ) -> Result<VerifiedSuccessorHistory<'a>, Failure> {
        let mut resolution = history
            .resolution(recovery, &self.cancelled)
            .map_err(|e| Failure::at("resolution", None, None, e))?;
        assert_eq!(resolution.needs_compacted_initial(), compacted);
        let compacted_requests = if compacted {
            let before_epoch = resolution.current_epoch();
            assert_eq!(before_epoch, Some(1));
            assert!(resolution.next_request().is_none());
            self.refresh(owner, Some(1), None).await?;
            let deadline = if let Some(fault) = fault {
                fault.cursor("before", &resolution);
                fault.before_compacted(self, owner).await?
            } else {
                None
            };
            let future = owner.inspect_successor_compacted_initial(
                &mut resolution,
                self.request(),
                recovery_limits().event,
                compacted_grant(&self.budget),
                |d, h| self.validate(d, h),
            );
            let result = self
                .bounded("compacted-initial", async {
                    if let Some(fault) = fault {
                        fault.pending(self, future).await
                    } else {
                        future.await
                    }
                })
                .await;
            let observed_clock = if let Some(fault) = fault {
                fault.cursor("returned", &resolution);
                let gate = fault
                    .checkpoint(
                        "compacted-returned",
                        result.as_ref().err().map(|e| format!("{e:?}")),
                    )
                    .await;
                match gate {
                    Ok(()) => fault.returned(self, owner, deadline).await,
                    Err(error) => Err(error),
                }
            } else {
                Ok(())
            };
            let summary = match result {
                Ok(summary) => summary,
                Err(error) => {
                    assert!(resolution.needs_compacted_initial());
                    assert_eq!(resolution.current_epoch(), before_epoch);
                    assert!(resolution.next_request().is_none());
                    emit(
                        serde_json::json!({"event":"lineage-history-compacted-initial-refused",
                        "epoch":before_epoch,"cursor_unchanged":true}),
                    );
                    return Err(Failure::at("compacted-initial", Some(1), None, error));
                }
            };
            // The actual product error above takes precedence over an observer
            // failure; its typed result and cursor were emitted before either.
            observed_clock?;
            assert!(!resolution.needs_compacted_initial());
            assert_eq!(resolution.current_epoch(), Some(2));
            assert!((1..=64).contains(&summary.compacted_requests));
            emit(
                serde_json::json!({"event":"lineage-history-compacted-initial",
                "compacted_requests":summary.compacted_requests,
                "original_requests_replayed":0,
                "last_complete_sequence":summary.last_complete_sequence,
                "digest":hex(&summary.digest)}),
            );
            self.snapshot("compacted-initial").await;
            summary.compacted_requests
        } else {
            0
        };
        while let Some(epoch) = resolution.current_epoch() {
            assert!((1..=255).contains(&epoch));
            while let Some(request) = resolution.next_request() {
                let sequence = request.request_sequence.get();
                assert!((1..=64).contains(&sequence));
                self.refresh(owner, Some(epoch), Some(sequence)).await?;
                let (input, outcome) = replay_grants(&self.budget);
                let observed = self
                    .bounded(
                        "request",
                        owner.inspect_next_successor_request(
                            &mut resolution,
                            SuccessorReplayRequest {
                                ingest_root: &self.path,
                                limits: recovery_limits(),
                                policy_ttl: self.request().policy_ttl,
                                cancelled: &self.cancelled,
                            },
                            input,
                            outcome,
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await
                    .map_err(|e| Failure::at("request", Some(epoch), Some(sequence), e))?;
                assert_eq!(observed, request);
                emit_request(observed);
                self.snapshot(&format!("resolved-{epoch}-{sequence}")).await;
            }
            self.refresh(owner, Some(epoch), None).await?;
            let summary = self
                .bounded(
                    "epoch",
                    owner.finish_successor_history_epoch(
                        &mut resolution,
                        self.request(),
                        |d, h| self.validate(d, h),
                    ),
                )
                .await
                .map_err(|e| Failure::at("epoch", Some(epoch), None, e))?;
            emit(
                serde_json::json!({"event":"lineage-history-epoch","summary":summary_value(summary)}),
            );
            self.snapshot(&format!("epoch-{epoch}")).await;
        }
        self.refresh(owner, None, None).await?;
        if let Some(fault) = fault {
            fault.checkpoint("before-final", None).await?;
        }
        let result = self
            .bounded(
                "finish",
                owner.finish_successor_history(
                    resolution,
                    self.request(),
                    recovery_limits().event,
                    effect_grant(&self.budget),
                    |d, h| self.validate(d, h),
                ),
            )
            .await;
        let returned_gate = if let Some(fault) = fault {
            fault
                .checkpoint(
                    "final-returned",
                    result.as_ref().err().map(|e| format!("{e:?}")),
                )
                .await
        } else {
            Ok(())
        };
        let proof = result.map_err(|e| Failure::at("finish", None, None, e))?;
        returned_gate?;
        assert!(proof.matches_history(history));
        assert_eq!(proof.compacted_initial_requests(), compacted_requests);
        proof
            .validate(&self.cancelled)
            .map_err(|e| Failure::at("final-validation", None, None, e))?;
        if compacted {
            emit(
                serde_json::json!({"event":"lineage-history-compacted-proof",
                "compacted_initial_requests":proof.compacted_initial_requests(),
                "original_initial_requests_replayed":0}),
            );
        }
        emit(
            serde_json::json!({"event":"lineage-history-proof","verified_through":proof.verified_through(),
            "last_epoch":summary_value(proof.last_epoch()),"matches_history":true,"admission":false}),
        );
        self.snapshot("proof").await;
        Ok(proof)
    }
}

fn kind_name(kind: RetainedTerminalKind) -> &'static str {
    match kind {
        RetainedTerminalKind::Initial => "initial",
        RetainedTerminalKind::Activated => "activated",
        RetainedTerminalKind::UnactivatedEmpty => "unactivated-empty",
    }
}
fn summary_value(summary: LineageEpochSummary) -> serde_json::Value {
    serde_json::json!({"epoch":summary.epoch,"terminal_kind":kind_name(summary.terminal_kind),
        "resolved_requests":summary.resolved_requests,"last_complete_sequence":summary.last_complete_sequence,
        "digest":hex(&summary.digest),"event_request_mask":summary.event_request_mask})
}
fn emit_request(request: ProducerRequestId) {
    emit(
        serde_json::json!({"event":"lineage-history-request","producer_slot":hex(request.producer_slot_id.as_bytes()),
        "epoch":request.producer_epoch.get(),"sequence":request.request_sequence.get(),"existing_copy_checked":true}),
    );
}
fn replay_grants(budget: &ResourceBudget) -> (OwnedPermit, OwnedPermit) {
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
fn effect_grant(budget: &ResourceBudget) -> OwnedPermit {
    // The public replay grant covers the final dictionary/effect census too.
    grant(budget, recovery_limits().required_data_bytes().unwrap(), 0)
}
fn compacted_grant(budget: &ResourceBudget) -> OwnedPermit {
    grant(
        budget,
        compacted_outcome_data_bytes(recovery_limits().event).unwrap(),
        0,
    )
}
fn recovery_limits() -> ProducerRecoveryLimits {
    ProducerRecoveryLimits {
        input: RequestDecodeLimits {
            values: FactDecodeLimits {
                max_encoded_bytes: 16 * MIB as usize,
                max_allocation_bytes: 64 * MIB as usize,
                max_tag_nodes: 4 * MIB as usize,
                max_string_bytes: 16 * MIB as usize,
            },
            max_dictionaries: 256,
        },
        bootstrap: BootstrapLimits {
            max_mappings: 256,
            max_batch_bytes: 4 * MIB as usize,
        },
        event: AdmissionLimits {
            max_dependencies: 256,
            ..AdmissionLimits::default()
        },
    }
}
