//! Finite connected reconstruction in a new process, never admission.
//! Only configuration and retained PostgreSQL/native state cross the boundary.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        InitialHistoryRequest, PolicySnapshot, PreparedInitialHistory, ProducerCloseRequest,
        ProducerResolveRequest,
    },
    rocks::store::{
        AdmissionLimits, AdmissionOutcome, IngestRecovery, InitialHandoff, ProducerRecoveryLimits,
        ResolvedSource, RetainedRequestInput, SourceResolution,
    },
};
use bigbrotr_model::{
    DatasetId, ProducerEpoch, ProducerRequestId, RequestSequence,
    codec::{FactDecodeLimits, request::RequestDecodeLimits},
};
use std::{fmt::Debug, future::Future, path::PathBuf, sync::atomic::Ordering};

type Driver = tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>;
const CONTROL_TTL: Duration = Duration::from_secs(20);

#[derive(Clone, Copy)]
enum Mode {
    History,
    Gap { cancel_before_completion: bool },
}
impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::History => "history",
            Self::Gap { .. } => "gap",
        }
    }
    fn event(self, suffix: &str) -> String {
        format!("successor-{}-{suffix}", self.name())
    }
}

#[derive(Clone, Copy)]
enum Loss {
    BeforeNative,
    AfterNative,
}

#[derive(Clone, Copy)]
struct Qualification {
    select_route: bool,
    loss: Option<Loss>,
    proxy_port: Option<u16>,
}

struct Backend {
    pid: i32,
    start: String,
    database: String,
    control_epoch: u64,
}
impl Backend {
    fn emit(&self, mode: Mode, present: bool) {
        emit(
            serde_json::json!({"event":"successor-backend","stage":mode.name(),
            "pid":self.pid,"backend_start":self.start,"database":self.database,
            "control_epoch":self.control_epoch,"present":present}),
        );
    }
}

struct Probe {
    mode: Mode,
    root: config::Resolved,
    path: PathBuf,
    budget: ResourceBudget,
    observer: tokio_postgres::Client,
    cancelled: AtomicBool,
    qualification: Option<Qualification>,
    database: String,
}

struct Refusal {
    stage: &'static str,
    sequence: Option<u64>,
    error: String,
}
impl Refusal {
    fn at(stage: &'static str, sequence: Option<u64>, error: impl Debug) -> Self {
        Self {
            stage,
            sequence,
            error: format!("{error:?}"),
        }
    }
}

// An infrastructure timeout is not an expected product refusal or rollback.
// The enclosing laboratory preserves partial stdout and independently cleans
// up this finite process. Never report a successful release after timeout.
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("history probe deadline; preserve unknown outcomes")
}

/// Deliberate corruption of an isolated prepared fixture before the reader
/// starts. This raw engine path is never reachable from a product operation.
pub fn corrupt(path: &Path, mutation: &str) {
    assert!(matches!(mutation, "missing-handoff" | "target-nonzero"));
    corrupt_fixture(path, mutation, false);
}

/// Five fixed mutations of the qualified installed off fixture. The laboratory
/// must supply a fresh owned clone with no active native owner. This is not an
/// arbitrary-key mutator or a product recovery operation.
pub fn corrupt_activated(path: &Path, mutation: &str) {
    assert!(matches!(
        mutation,
        "missing-old-dependency"
            | "corrupt-old-dependency"
            | "wrong-handoff"
            | "extra-producer"
            | "foreign-stream"
    ));
    corrupt_fixture(path, mutation, true);
}

fn corrupt_fixture(path: &Path, mutation: &str, activated: bool) {
    use bigbrotr::storage::rocks::keys::IngestKey;
    use bigbrotr_model::{ProducerSlotId, Sequence, SourceRecordId};
    use sha2::{Digest, Sha256};

    let (before_rows, before_sequence) = raw_state(path);
    if activated {
        assert_eq!((before_rows.len(), before_sequence), (70, 84));
    }
    let db = rocksdb::DB::open_cf(
        &rocksdb::Options::default(),
        path,
        ["default", "facts", "receipts"],
    )
    .unwrap();
    let cf = db.cf_handle("default").unwrap();
    let manifest = IngestManifest::decode(&db.get_cf(cf, [1, 1]).unwrap().unwrap()).unwrap();
    assert_eq!(manifest.key_schema(), 2);
    let id = manifest.identity();
    assert_eq!(id.owner_epoch.get(), 2);
    let handoff_key = IngestKey::OwnerHandoff {
        dataset_id: id.dataset_id,
        owner_resource_id: id.owner_resource_id,
        predecessor_epoch: OwnerEpoch::new(1).unwrap(),
    };
    let old_fact_key = IngestKey::Fact {
        source_record_id: SourceRecordId {
            dataset_id: id.dataset_id,
            shard_id: id.shard_id,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            sequence: Sequence::new(3).unwrap(),
        },
    };
    let slot = activated.then(|| {
        // Frozen f12 source1/3 is VisibilityScope ordinal3, class private.
        // Both installed off requests depend on it. Validate its complete value
        // before selecting any corruption; the driver checks the entire seed.
        let old = db
            .get_cf(
                db.cf_handle("facts").unwrap(),
                old_fact_key.encode().unwrap().as_bytes(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            hex(&Sha256::digest(&old)),
            "48f14a320986410db233082bad2b65e7815086a210221781516fe09e6ed5ce38"
        );
        let handoff = db
            .get_cf(cf, handoff_key.encode().unwrap().as_bytes())
            .unwrap()
            .unwrap();
        // InitialHandoffRecord layout: tag2, slot16, epoch8, issued8,
        // source cut8, digest32, then two length-prefixed manifests.
        assert_eq!(handoff.len(), 610);
        assert_eq!(&handoff[..2], &[1, 10]);
        assert_eq!(&handoff[18..26], &1_u64.to_be_bytes());
        assert_eq!(&handoff[26..34], &4_u64.to_be_bytes());
        assert_eq!(&handoff[34..42], &12_u64.to_be_bytes());
        assert_eq!(
            hex(&handoff[42..74]),
            "9e1a5b86a108f064a43c9f69313a39c9c3353fb45dc52dae230d190785d21501"
        );
        assert_eq!(&handoff[74..76], &266_u16.to_be_bytes());
        assert_eq!(&handoff[342..344], &266_u16.to_be_bytes());
        let previous = IngestManifest::decode(&handoff[76..342]).unwrap();
        assert_eq!(previous.key_schema(), 1);
        assert_eq!(previous.identity().dataset_id, id.dataset_id);
        assert_eq!(previous.identity().owner_resource_id, id.owner_resource_id);
        assert_eq!(previous.identity().shard_id, id.shard_id);
        assert_eq!(previous.identity().owner_epoch.get(), 1);
        assert_eq!(&handoff[344..], manifest.encode().as_bytes());
        ProducerSlotId::try_from(<[u8; 16]>::try_from(&handoff[2..18]).unwrap()).unwrap()
    });
    let key = match mutation {
        "missing-handoff" | "wrong-handoff" => handoff_key,
        "target-nonzero" => IngestKey::SourceStream {
            dataset_id: id.dataset_id,
            shard_id: id.shard_id,
            owner_epoch: id.owner_epoch,
        },
        "missing-old-dependency" | "corrupt-old-dependency" => old_fact_key,
        "extra-producer" => IngestKey::ProducerState {
            producer_slot_id: slot.unwrap(),
            producer_epoch: ProducerEpoch::new(3).unwrap(),
        },
        "foreign-stream" => IngestKey::SourceStream {
            dataset_id: id.dataset_id,
            shard_id: id.shard_id,
            owner_epoch: OwnerEpoch::new(3).unwrap(),
        },
        _ => unreachable!(),
    }
    .encode()
    .unwrap();
    let family = key.column_family().name();
    let cf = db.cf_handle(family).unwrap();
    let before = db.get_cf(cf, key.as_bytes()).unwrap();
    let after = match mutation {
        "target-nonzero" => {
            assert_eq!(before.as_deref(), Some(&[1, 2, 0, 0, 0, 0, 0, 0, 0, 0][..]));
            Some(vec![1, 2, 0, 0, 0, 0, 0, 0, 0, 1])
        }
        "missing-handoff" => {
            let before = before.as_ref().unwrap();
            assert!((286..=610).contains(&before.len()));
            assert_eq!(before[..2], [1, 10]);
            None
        }
        "missing-old-dependency" => {
            assert!(before.is_some());
            None
        }
        "corrupt-old-dependency" => {
            let mut after = before.clone().unwrap();
            // Keep the embedded payload digest, but change its scientific bytes.
            *after.last_mut().unwrap() ^= 1;
            Some(after)
        }
        "wrong-handoff" => {
            let mut after = before.clone().unwrap();
            // Offset45 is closure_digest[3], not an identity/length field. The
            // record remains decodable but disagrees with immutable PG intent.
            after[42 + 3] ^= 1;
            Some(after)
        }
        "extra-producer" | "foreign-stream" => {
            assert!(before.is_none());
            let (tag, length) = if mutation == "extra-producer" {
                (0x31, 26)
            } else {
                (2, 10)
            };
            let mut after = vec![0; length];
            after[..2].copy_from_slice(&[1, tag]);
            Some(after)
        }
        _ => unreachable!(),
    };
    let event = if activated {
        "activated-recovery-corruption"
    } else {
        "successor-history-corruption"
    };
    emit(serde_json::json!({"event":event,"phase":"before",
        "mutation":mutation,"cf":family,"key":hex(key.as_bytes()),"value":before.as_deref().map(hex),
        "sequence":before_sequence,"rows":before_rows}));
    let mut options = rocksdb::WriteOptions::default();
    options.set_sync(true);
    if let Some(after) = &after {
        db.put_cf_opt(cf, key.as_bytes(), after, &options).unwrap();
    } else {
        db.delete_cf_opt(cf, key.as_bytes(), &options).unwrap();
    }
    drop(db);
    let (rows, sequence) = raw_state(path);
    emit(serde_json::json!({"event":event,"phase":"after",
        "mutation":mutation,"cf":family,"key":hex(key.as_bytes()),
        "value":after.as_deref().map(hex),"sequence":sequence,"rows":rows}));
    let mut expected = before_rows;
    let position = expected
        .iter()
        .position(|r| r["cf"] == family && r["key"] == hex(key.as_bytes()));
    assert_eq!(position.is_some(), before.is_some());
    if let Some(position) = position {
        expected.remove(position);
    }
    if let Some(after) = after {
        expected
            .push(serde_json::json!({"cf":family,"key":hex(key.as_bytes()),"value":hex(&after)}));
    }
    expected.sort_unstable_by(|a, b| {
        (a["cf"].as_str().unwrap(), a["key"].as_str().unwrap())
            .cmp(&(b["cf"].as_str().unwrap(), b["key"].as_str().unwrap()))
    });
    assert_eq!(sequence, before_sequence.checked_add(1).unwrap());
    assert_eq!(
        rows, expected,
        "only the declared native fixture row changes"
    );
}

pub async fn run(config: Config, config_file: &Path) {
    run_mode(config, config_file, Mode::History, None).await;
}

pub async fn run_gap(config: Config, config_file: &Path, cancel_before_completion: bool) {
    run_mode(
        config,
        config_file,
        Mode::Gap {
            cancel_before_completion,
        },
        None,
    )
    .await;
}

pub async fn run_qualification(
    config: Config,
    config_file: &Path,
    command: &str,
    proxy_port: Option<u16>,
) {
    let loss = match command {
        "successor-gap-loss-before" => Some(Loss::BeforeNative),
        "successor-gap-loss-after" => Some(Loss::AfterNative),
        "successor-gap-policy" | "successor-prepared-recover" => None,
        _ => panic!("unknown initial gap qualification"),
    };
    assert_eq!(loss.is_some(), proxy_port.is_some());
    assert!(proxy_port.is_none_or(|port| port != 0));
    run_mode(
        config,
        config_file,
        Mode::Gap {
            cancel_before_completion: false,
        },
        Some(Qualification {
            select_route: command == "successor-prepared-recover",
            loss,
            proxy_port,
        }),
    )
    .await;
}

async fn run_mode(
    config: Config,
    config_file: &Path,
    mut mode: Mode,
    qualification: Option<Qualification>,
) {
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
    let _metadata = grant(&budget, 16 * MIB, 0);
    let mut recovery =
        IngestRecovery::inspect(&path, dataset, limits(), grant(&budget, 0, 128 * MIB)).unwrap();
    if let Some(qualification) = qualification {
        // This dispatch is only a finite fixture. The selected product adapter
        // still proves the complete retained history under a fresh Control.
        let manifest = recovery.manifest();
        let route = match (manifest.identity().owner_epoch.get(), manifest.key_schema()) {
            (1, 1) => Mode::Gap {
                cancel_before_completion: false,
            },
            (2, 2) if qualification.select_route => Mode::History,
            _ => panic!("unsupported actual prepared recovery state"),
        };
        mode = route;
        emit(
            serde_json::json!({"event":"successor-recovery-selection","route":mode.name(),
            "owner_epoch":manifest.identity().owner_epoch.get(),"key_schema":manifest.key_schema(),
            "manifest":hex(manifest.encode().as_bytes()),"admission":false}),
        );
    }
    let (observer, observer_driver) = bounded(config.connect(NoTls)).await.unwrap();
    let observer_driver = tokio::spawn(observer_driver);
    let probe = Probe {
        mode,
        root,
        path,
        budget,
        observer,
        cancelled: AtomicBool::new(false),
        qualification,
        database: config.get_dbname().unwrap().into(),
    };
    drop(probe.replay_grants());
    let mut role = [0; 16];
    let mut incarnation = [0; 16];
    getrandom::fill(&mut role).unwrap();
    getrandom::fill(&mut incarnation).unwrap();
    assert_ne!(role, [0; 16]);
    assert_ne!(incarnation, [0; 16]);
    emit(
        serde_json::json!({"event":"successor-control","stage":mode.name(),"phase":"claim-planned",
        "role":hex(&role),"incarnation":hex(&incarnation),"lease_ttl_us":CONTROL_TTL.as_micros().to_string()}),
    );
    probe.snapshot("before-claim").await;
    // The observer remains direct. Only this dedicated owner connection crosses
    // the bounded laboratory proxy, and no connection or plan crosses processes.
    let mut owner_config = config;
    if let Some(port) = qualification.and_then(|value| value.proxy_port) {
        assert!(owner_config.get_ports().is_empty());
        owner_config.port(port);
    }
    let (client, driver) = bounded(owner_config.connect(NoTls)).await.unwrap();
    let driver = tokio::spawn(driver);
    let (mut owner, policy) = bounded(ControlSession::acquire(
        client,
        ClaimRequest {
            dataset,
            instance: instance.try_into().unwrap(),
            authority_mode: probe.root.text("config.authority_mode").unwrap().into(),
            role_instance_id: role,
            incarnation: incarnation.try_into().unwrap(),
            lease_ttl: CONTROL_TTL,
        },
        |document, digest| probe.validate(document, digest),
        &Cell::new(false),
    ))
    .await
    .unwrap();
    emit(
        serde_json::json!({"event":"successor-control","stage":mode.name(),"phase":"claimed",
        "resource":hex(owner.identity().resource_id.as_bytes()),"epoch":owner.identity().epoch.get(),
        "role":hex(&role),"incarnation":hex(&incarnation)}),
    );
    probe.emit_policy(&owner, "acquired", None, policy);
    let backend = if qualification.is_some() {
        Some(probe.observe_backend(&owner).await)
    } else {
        None
    };
    probe.snapshot("after-claim").await;
    probe.snapshot("before-read").await;
    let result = match mode {
        Mode::History => probe.read(&mut owner, &recovery).await,
        Mode::Gap {
            cancel_before_completion,
        } => {
            probe
                .complete_gap(&mut owner, &mut recovery, cancel_before_completion)
                .await
        }
    };
    match &result {
        Ok(()) => emit(serde_json::json!({"event":mode.event("result"),
            "stage":"complete","sequence":null,"error":null})),
        Err(error) => emit(serde_json::json!({"event":mode.event("result"),
            "stage":error.stage,"sequence":error.sequence,"error":error.error})),
    }
    if qualification.is_some_and(|value| value.loss.is_some()) && result.is_err() {
        probe.check_poisoned(&mut owner).await;
    }
    probe.snapshot("after-read").await;
    // All borrowed historical witnesses have ended. Close/join native custody
    // before releasing the actual Control owner, matching the installed order.
    drop(recovery);
    probe
        .end_control(owner, driver, result.is_ok(), backend.as_ref())
        .await;
    probe.snapshot("final").await;
    emit(
        serde_json::json!({"event":mode.event("complete"),"native":"closed",
        "control":if result.is_ok() {"released-and-joined"} else {"dropped-and-joined"},
        "admission":false,"success":result.is_ok()}),
    );
    drop(probe);
    crate::ingest_closing::join(observer_driver, false).await;
    if let Some(loss) = qualification.and_then(|value| value.loss) {
        let Err(error) = result else {
            panic!("delivery-loss fixture unexpectedly completed");
        };
        assert_eq!(error.stage, "completion");
        assert_eq!(error.sequence, None);
        assert_eq!(
            error.error,
            match loss {
                Loss::BeforeNative => "Control(Backend)",
                Loss::AfterNative => "DeliveryUnknown",
            }
        );
    }
}

impl Probe {
    fn emit_policy(
        &self,
        owner: &ControlSession,
        phase: &str,
        sequence: Option<u64>,
        policy: PolicySnapshot,
    ) {
        if self.qualification.is_some() {
            emit(
                serde_json::json!({"event":"successor-policy","stage":self.mode.name(),
                "phase":phase,"sequence":sequence,"control_epoch":owner.identity().epoch.get(),
                "config_revision":policy.config_revision,"policy_revision":policy.policy_revision,
                "config_digest":hex(&policy.config_digest),"policy_digest":hex(&policy.policy_digest)}),
            );
        }
    }

    async fn observe_backend(&self, owner: &ControlSession) -> Backend {
        let identity = owner.identity();
        let row = bounded(self.observer.query_one(
            "SELECT json_build_object('pid',o.control_backend_pid,
                'backend_start',o.control_backend_start,'database',a.datname)::text
             FROM control.owner_epochs o JOIN pg_catalog.pg_stat_activity a
               ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start
             WHERE o.resource_id=$1::bytea AND o.epoch=$2::text::numeric
               AND o.process_incarnation=$3::bytea AND o.role='control'
               AND o.state='active' AND a.datname=current_database()",
            &[
                &&identity.resource_id.as_bytes()[..],
                &identity.epoch.get().to_string(),
                &&identity.process_incarnation.as_bytes()[..],
            ],
        ))
        .await
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(row.get::<_, &str>(0)).unwrap();
        let backend = Backend {
            pid: i32::try_from(value["pid"].as_i64().unwrap()).unwrap(),
            start: value["backend_start"].as_str().unwrap().into(),
            database: value["database"].as_str().unwrap().into(),
            control_epoch: identity.epoch.get(),
        };
        assert!(backend.pid > 0 && !backend.start.is_empty());
        assert_eq!(backend.database, self.database);
        backend.emit(self.mode, true);
        backend
    }

    async fn require_backend_absent(&self, backend: &Backend) {
        // A closed client does not prove server exclusion. Observe the exact
        // frozen tuple without terminating it or changing its retained row.
        bounded(async {
            loop {
                let present: bool = self
                    .observer
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity
                         WHERE pid=$1::integer AND backend_start=$2::text::timestamptz
                           AND datname=$3::text)",
                        &[&backend.pid, &backend.start, &backend.database],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if !present {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        backend.emit(self.mode, false);
    }

    async fn check_poisoned(&self, owner: &mut ControlSession) {
        assert_eq!(
            bounded(owner.refresh(|document, digest| self.validate(document, digest))).await,
            Err(OwnerError::Unavailable)
        );
        emit(
            serde_json::json!({"event":"successor-session-poisoned","stage":self.mode.name(),
            "control_epoch":owner.identity().epoch.get()}),
        );
    }

    fn validate(&self, document: &str, digest: [u8; 32]) -> Result<(), OwnerError> {
        crate::config::validate_fixture_policy(document, digest, &self.root)
    }
    fn policy_ttl(&self) -> Duration {
        Duration::from_micros(self.root.number("runtime.authorization_ttl"))
    }
    fn replay_grants(&self) -> (OwnedPermit, OwnedPermit) {
        let limits = recovery_limits();
        (
            grant(
                &self.budget,
                RetainedRequestInput::required_data_bytes(limits.input).unwrap(),
                0,
            ),
            grant(&self.budget, limits.required_data_bytes().unwrap(), 0),
        )
    }
    async fn snapshot(&self, stage: &str) {
        let stage = format!("{}-{stage}", self.mode.name());
        let state = bounded(crate::ingest_snapshot::state(&self.observer, &self.path)).await;
        let (rows, sequence) = raw_state(&self.path);
        assert_eq!(state["sequence"].as_u64(), Some(sequence));
        emit(
            serde_json::json!({"event":"successor-state","stage":stage,"state":state,"rows":rows}),
        );
    }
    async fn end_control(
        &self,
        owner: ControlSession,
        driver: Driver,
        release: bool,
        backend: Option<&Backend>,
    ) {
        self.snapshot("before-control-end").await;
        let identity = owner.identity();
        if release {
            bounded(owner.release()).await.unwrap();
        } else {
            // A refused read may have poisoned this session. Do not attempt a
            // release or invent terminal SQL state for that actual claimant.
            drop(owner);
        }
        let failed = self.qualification.is_some_and(|value| value.loss.is_some());
        assert!(!failed || !release);
        crate::ingest_closing::join(driver, failed).await;
        if let Some(backend) = backend {
            emit(
                serde_json::json!({"event":"successor-driver","stage":self.mode.name(),
                "failed":failed,"joined":true}),
            );
            self.require_backend_absent(backend).await;
        }
        emit(
            serde_json::json!({"event":"successor-control","stage":self.mode.name(),
            "phase":if release {"released-and-joined"} else {"dropped-and-joined"},
            "resource":hex(identity.resource_id.as_bytes()),"epoch":identity.epoch.get()}),
        );
        self.snapshot("after-control-end").await;
    }
    async fn read(
        &self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
    ) -> Result<(), Refusal> {
        let history = bounded(owner.inspect_prepared_initial_history(
            recovery,
            InitialHistoryRequest {
                ingest_root: &self.path,
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            },
            grant(&self.budget, 128 * 1024, 0),
            |document, digest| self.validate(document, digest),
        ))
        .await
        .map_err(|error| Refusal::at("history-load", None, error))?;
        let spec = history.registration();
        emit_registration(spec, &self.mode.event("registration"));
        self.snapshot("loaded").await;
        let source = history
            .source_resolution(grant(&self.budget, 4096, 4096))
            .map_err(|error| Refusal::at("source-start", None, error))?;
        let source = self
            .resolve_originals(
                owner,
                recovery,
                spec,
                source,
                history.issued_through(),
                Some(&history),
            )
            .await?;
        let handoff = history
            .reconstruct_handoff(&source)
            .map_err(|error| Refusal::at("reconstruct", None, error))?;
        self.emit_proof(&source, &handoff);
        self.snapshot("proof").await;
        Ok(())
    }
    async fn complete_gap(
        &self,
        owner: &mut ControlSession,
        recovery: &mut IngestRecovery,
        cancel_before_completion: bool,
    ) -> Result<(), Refusal> {
        let gap = bounded(owner.inspect_prepared_initial_gap(
            recovery,
            InitialHistoryRequest {
                ingest_root: &self.path,
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            },
            grant(&self.budget, 128 * 1024, 0),
            |document, digest| self.validate(document, digest),
        ))
        .await
        .map_err(|error| Refusal::at("gap-load", None, error))?;
        let spec = gap.registration();
        emit_registration(spec, &self.mode.event("registration"));
        self.snapshot("loaded").await;
        let closing = bounded(owner.observe_initial_retirement(
            recovery,
            ProducerCloseRequest {
                ingest_root: &self.path,
                registration: spec,
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            },
            grant(&self.budget, 4096, 4096),
            |document, digest| self.validate(document, digest),
        ))
        .await
        .map_err(|error| Refusal::at("source-start", None, error))?;
        let source = closing
            .source_resolution(recovery)
            .map_err(|error| Refusal::at("source-start", None, error))?;
        self.snapshot("source-started").await;
        let source = self
            .resolve_originals(owner, recovery, spec, source, gap.issued_through(), None)
            .await?;
        let plan = gap
            .reconstruct_handoff(&source)
            .map_err(|error| Refusal::at("reconstruct", None, error))?;
        self.emit_proof(&source, plan.handoff());
        // Only the owned, positively reconstructed plan crosses dispatch. Its
        // metadata grant and actual root/sequence guard survive this borrow.
        drop(source);
        self.snapshot("proof").await;
        if cancel_before_completion {
            self.cancelled.store(true, Ordering::Relaxed);
            emit(serde_json::json!({"event":self.mode.event("cancellation"),"cancelled":true}));
        }
        self.snapshot("before-completion").await;
        let completion = bounded(owner.complete_prepared_initial_gap(
            recovery,
            &plan,
            InitialHistoryRequest {
                ingest_root: &self.path,
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            },
            grant(&self.budget, 8192, 8192),
            |document, digest| self.validate(document, digest),
        ))
        .await;
        if let Ok(observed) = &completion {
            emit(serde_json::json!({"event":self.mode.event("completion"),
                "handoff":hex(observed.handoff().encode().as_bytes()),"admission":false}));
        }
        // Capture both returned success and returned refusal. A timeout above
        // remains an unknown process outcome with its partial evidence retained.
        self.snapshot("after-completion").await;
        completion.map_err(|error| Refusal::at("completion", None, error))?;
        Ok(())
    }
    async fn resolve_originals<'a>(
        &self,
        owner: &mut ControlSession,
        recovery: &'a IngestRecovery,
        spec: &InitialRegistration,
        mut source: SourceResolution<'a>,
        issued: u64,
        history: Option<&PreparedInitialHistory<'a>>,
    ) -> Result<ResolvedSource<'a>, Refusal> {
        assert!(issued <= 64);
        for sequence in 1..=issued {
            let policy = bounded(owner.refresh(|document, digest| self.validate(document, digest)))
                .await
                .map_err(|error| Refusal::at("policy-refresh", Some(sequence), error))?;
            self.emit_policy(owner, "refreshed", Some(sequence), policy);
            self.snapshot(&format!("refreshed-{sequence}")).await;
            let (input, outcome) = self.replay_grants();
            let request = ProducerResolveRequest {
                ingest_root: &self.path,
                registration: spec,
                request: ProducerRequestId {
                    producer_slot_id: spec.producer_slot_id,
                    producer_epoch: ProducerEpoch::new(1).unwrap(),
                    request_sequence: RequestSequence::new(sequence).unwrap(),
                },
                limits: recovery_limits(),
                policy_ttl: self.policy_ttl(),
                cancelled: &self.cancelled,
            };
            let resolved = if let Some(history) = history {
                bounded(owner.inspect_prepared_initial_request(
                    history,
                    request,
                    input,
                    outcome,
                    |document, digest| self.validate(document, digest),
                ))
                .await
            } else {
                bounded(owner.inspect_initial_retirement_request(
                    recovery,
                    request,
                    input,
                    outcome,
                    |document, digest| self.validate(document, digest),
                ))
                .await
            }
            .map_err(|error| Refusal::at("request", Some(sequence), error))?;
            let (outcome, receipt, rejection) = match resolved.outcome() {
                AdmissionOutcome::Durable(receipt) => (
                    "durable",
                    Some(hex(&receipt.encode_persistent(64 * 1024).unwrap())),
                    None,
                ),
                AdmissionOutcome::Rejected(reason) => {
                    ("rejected", None, Some(format!("{reason:?}")))
                }
            };
            source
                .record(&resolved)
                .map_err(|error| Refusal::at("source-record", Some(sequence), error))?;
            emit(
                serde_json::json!({"event":self.mode.event("request"),"sequence":sequence,
                "outcome":outcome,"receipt":receipt,"rejection":rejection}),
            );
            drop(resolved);
            self.snapshot(&format!("resolved-{sequence}")).await;
        }
        source
            .finish()
            .map_err(|error| Refusal::at("source-finish", None, error))
    }
    fn emit_proof(&self, source: &ResolvedSource<'_>, handoff: &InitialHandoff) {
        emit(serde_json::json!({"event":self.mode.event("proof"),
            "issued_through":source.resolved_requests(),"last_complete_sequence":source.last_complete_sequence(),
            "closure_digest":hex(&source.digest()),"previous_manifest":hex(handoff.previous_manifest().encode().as_bytes()),
            "target_manifest":hex(handoff.next_manifest().encode().as_bytes()),"handoff":hex(handoff.encode().as_bytes())}));
    }
}

fn emit_registration(s: &InitialRegistration, event: &str) {
    emit(serde_json::json!({"event":event,
        "dataset_id":hex(s.dataset_id.as_bytes()),"instance_id":hex(&s.instance_id),
        "control_resource":hex(s.control.resource_id.as_bytes()),"control_epoch":s.control.epoch.get(),
        "control_role_instance":hex(&s.control.role_instance_id),"control_incarnation":hex(s.control.process_incarnation.as_bytes()),
        "ingest_resource":hex(s.ingest_resource_id.as_bytes()),"ingest_role_instance":hex(&s.ingest_role_instance_id),
        "ingest_incarnation":hex(s.process_incarnation.as_bytes()),"shard_id":s.shard_id,
        "config_revision":s.config_revision,"policy_revision":s.policy_revision,
        "config_digest":hex(s.config_digest.as_bytes()),"policy_digest":hex(s.policy_digest.as_bytes()),
        "previous_topology":s.previous_topology.map(|v|v.get()),
        "allocation_domains":s.allocation_domains.iter().map(|(kind,id)|serde_json::json!([*kind as u16,hex(id.as_bytes())])).collect::<Vec<_>>(),
        "producer_slot":hex(s.producer_slot_id.as_bytes()),"window":s.outstanding_request_limit,
        "created_at_us":s.created_at_us,"lease_expires_at_us":s.lease_expires_at_us}));
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
