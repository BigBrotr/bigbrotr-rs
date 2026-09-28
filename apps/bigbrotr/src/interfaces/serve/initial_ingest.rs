//! Explicit first lifetime. The worker never returns a live native handle.
mod supervision;
use super::{Result, Stop, duration, owner_error};
use crate::{
    config::Resolved,
    interfaces::{lifecycle_output::LifecycleOutput, postgres_command},
};
use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
    },
    storage::{
        postgres::{
            control_owner::{ClaimRequest, ControlSession, InitialBatchRequest, OwnerError},
            ingest_registry::{InitialRegistration, RegistrationAttempt, RegistrationObservation},
            transport,
        },
        rocks::{
            RocksNativeLimits, RocksNativeSettings,
            manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity},
            store::{BootstrapLimits, IngestStore},
        },
    },
};
use bigbrotr_model::{
    DatasetId, DescriptorLabel, DictionaryDescriptor, DictionaryEntry, DictionaryKind, Digest256,
    EvidenceIntent, EvidenceIntentLimits, ObservedTime, Ordinal, OwnerEpoch, PolicyEpoch,
    QualifiedRef, RecoveryGenerationId,
};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    future::Future,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};

const MIB: u64 = 1024 * 1024;

/// Field order also protects unwind: native close completes before Client Drop.
/// This value is constructed and destroyed only on the blocking owner thread.
struct Custody {
    store: Option<IngestStore>,
    owner: ControlSession,
}

#[derive(Clone)]
struct Progress {
    deadline: Instant,
    initialized: Option<Value>,
}
struct Cancellation {
    requested: AtomicBool,
    wake: Notify,
}
impl Cancellation {
    fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            wake: Notify::new(),
        }
    }
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.wake.notify_one();
    }
    async fn wait(&self) {
        if !self.requested.load(Ordering::Acquire) {
            self.wake.notified().await;
        }
    }
}

fn selected(root: &Resolved) -> Result<()> {
    // This explicit first-lifetime slice cannot silently omit an enabled private
    // listener or consume an archive destination without its owning supervisor.
    if !root.local("control")
        || !root.local("ingest")
        || root.flag("administration.enabled")
        || root.flag("archive.reader.enabled")
        || root.text("export.destination.root").is_some()
        || root.text("read.cursor.keyring_ref").is_some()
        || root.text("administration.assets.directory").is_some()
    {
        return Err((3, "initial Ingest needs local Control/Ingest with administration and archive reader disabled and no export destination, cursor keyring or private assets".into()));
    }
    Ok(())
}

fn native_limits() -> RocksNativeLimits {
    // Explicit small initial profile. These are targets and a reservation, not
    // RSS/space limits or a production capacity claim. No identity cache yet.
    RocksNativeLimits::new(RocksNativeSettings {
        block_cache_bytes: 32 * MIB,
        write_buffer_manager_bytes: 16 * MIB,
        per_cf_write_buffer_bytes: 2 * MIB,
        max_total_wal_bytes: 16 * MIB,
        max_open_files: 64,
        max_background_jobs: 2,
        native_allowance_bytes: 128 * MIB,
    })
    .expect("checked initial native profile")
}

fn budget(root: &Resolved) -> Result<ResourceBudget> {
    let n = |field: &str| root.number(&format!("runtime.processes.application.{field}"));
    let envelope = ProcessEnvelope::new(
        n("memory_max_bytes"),
        n("baseline_bytes"),
        ResourceLimits::new(
            n("payload_bytes"),
            4,
            n("native_bytes"),
            n("completion_reserve_bytes"),
        ),
        WaiterLimits::new(0, 0),
    )
    .map_err(|_| (3, "initial Ingest process envelope refused".into()))?;
    Ok(ResourceBudget::new(envelope))
}
fn grant(budget: &ResourceBudget, bytes: u64, native: u64) -> Result<OwnedPermit> {
    budget
        .try_acquire(ResourceRequest::data(bytes, 1, native).expect("nonzero initial work"))
        .map_err(|_| {
            (
                3,
                "initial Ingest work does not fit its process reservation".into(),
            )
        })
}
fn required<'a>(root: &'a Resolved, key: &str) -> Result<&'a str> {
    root.text(key)
        .ok_or_else(|| (3, format!("{key}: required for initial Ingest")))
}

pub(super) fn run(root: Resolved, output: &mut LifecycleOutput) -> Result<()> {
    selected(&root)?;
    let budget = budget(&root)?;
    let native = grant(&budget, 0, native_limits().native_allowance_bytes())?;
    let bootstrap = grant(&budget, 8 * MIB, 4 * MIB)?;
    let inputs = grant(&budget, 24 * MIB, 0)?;
    let (prepared, control_grant) = postgres_command::prepare_control_shared(
        &root,
        required(&root, "postgres.control.user")?,
        required(&root, "postgres.control.password_ref")?,
        &budget,
    )?;
    let timeout = duration(&root, "renewal_timeout");
    let grace = duration(&root, "graceful_shutdown_timeout");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .map_err(|_| (1, "initial Ingest runtime unavailable".into()))?;
    let outcome = runtime.block_on(async {
        let mut stop = Stop::new()?;
        let (client, connection) = tokio::select! {
            biased;
            _ = stop.requested() => return Err((4, "initial Ingest connection cancelled".into())),
            result = transport::connect(prepared) => result.map_err(|e| (1, e.to_string()))?,
        };
        let driver = tokio::spawn(connection);
        let cancellation = Arc::new(Cancellation::new());
        let worker_cancel = cancellation.clone();
        let (progress, mut observed) = watch::channel(Progress {
            deadline: Instant::now() + timeout, initialized: None,
        });
        let handle = tokio::runtime::Handle::current();
        let mut worker = tokio::task::spawn_blocking(move || {
            // The parent keeps driving current-thread I/O/timers while this
            // owner parks on async SQL or blocks in native open/write/close.
            let _inputs = inputs;
            handle.block_on(own(root, client, native, bootstrap, progress, worker_cancel))
        });
        let (terminal, interrupted) = match supervision::collect(&mut worker, &mut observed,
            &cancellation, stop.requested(), grace, |report| output.emit(report, false)).await {
            supervision::Collected::Joined { terminal, interrupted } => (terminal, interrupted),
            supervision::Collected::Unjoined => {
                output.emit("error: initial Ingest worker not joined; native and Control outcomes retained; terminating process", true);
                // Never abort the driver beneath native FFI or wait forever in
                // runtime shutdown. No graceful-release claim on this path.
                std::process::exit(4);
            }
        };
        // A terminal worker has already closed native and dropped/released its
        // sole Client. The memory reservation survives actual driver termination.
        postgres_command::join(driver).await;
        let report = terminal.map_err(|_| (4, "initial Ingest worker panicked; inspect retained state".into()))??;
        if interrupted { return Err((4, "initial Ingest operation exceeded its deadline; inspect retained state".into())); }
        output.emit(report, false);
        Ok(())
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    drop(control_grant);
    outcome
}

/// Bound async coordination; a native call can overrun this timer while blocking
/// the owner thread. The parent independently enforces the same watchdog/join.
async fn bounded<T>(
    timeout: std::time::Duration,
    cancel: &Cancellation,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {
        biased;
        _ = cancel.wait() => Err((4, "initial Ingest cancelled; original attempt may need recovery".into())),
        result = tokio::time::timeout(timeout, future) => result.map_err(|_| (4, "initial Ingest deadline; original attempt may need recovery".into()))?,
    }
}

fn validate(
    root: &Resolved,
    document: &str,
    digest: [u8; 32],
) -> std::result::Result<(), OwnerError> {
    let restored = super::accepted_policy(root, document, digest)?;
    selected(&restored).map_err(|_| OwnerError::InvalidPolicy)
}
fn phase(progress: &watch::Sender<Progress>, timeout: std::time::Duration) -> Result<()> {
    let now = Instant::now();
    if now >= progress.borrow().deadline {
        return Err((
            4,
            "initial Ingest phase completed after its deadline; preserve retained state".into(),
        ));
    }
    // Only a timely phase may publish a new deadline. This also covers native
    // calls that returned after both the worker and parent were unscheduled.
    progress.send_modify(|value| value.deadline = now + timeout);
    Ok(())
}

async fn own(
    root: Resolved,
    client: tokio_postgres::Client,
    native: OwnedPermit,
    bootstrap: OwnedPermit,
    progress: watch::Sender<Progress>,
    cancellation: Arc<Cancellation>,
) -> Result<Value> {
    let path = Path::new(required(&root, "storage.ingest_root")?);
    if !path.is_absolute()
        || !path
            .parent()
            .and_then(|parent| std::fs::symlink_metadata(parent).ok())
            .is_some_and(|metadata| metadata.is_dir())
    {
        return Err((
            3,
            "initial Ingest requires an absolute target under an existing directory".into(),
        ));
    }
    // Check before even claiming Control. Initialize itself repeats the exclusive
    // path check; no directory, symlink or partial state is adopted on retry.
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
        _ => return Err((3, "initial Ingest target exists or cannot be inspected; preserve it for explicit recovery".into())),
    }
    let dataset: DatasetId = required(&root, "topology.dataset_id")?
        .parse()
        .map_err(|_| (3, "invalid dataset".into()))?;
    let instance: DatasetId = required(&root, "topology.instance_id")?
        .parse()
        .map_err(|_| (3, "invalid instance".into()))?;
    let fresh = || super::fresh_process_incarnation().map_err(|e| (1, e.to_string()));
    let request = ClaimRequest {
        dataset,
        instance,
        authority_mode: required(&root, "config.authority_mode")?.into(),
        role_instance_id: *fresh()?.as_bytes(),
        incarnation: fresh()?,
        lease_ttl: duration(&root, "lease_ttl"),
    };
    let timeout = duration(&root, "renewal_timeout");
    let ttl = duration(&root, "authorization_ttl");
    let commit = Cell::new(false);
    let mut outstanding = u64::MAX;
    phase(&progress, timeout)?;
    let (owner, accepted) = bounded(timeout, &cancellation, async {
        ControlSession::acquire(
            client,
            request,
            |doc, digest| {
                let restored = super::accepted_policy(&root, doc, digest)?;
                selected(&restored).map_err(|_| OwnerError::InvalidPolicy)?;
                outstanding =
                    outstanding.min(restored.number("ingest.producer_outstanding_requests"));
                Ok(())
            },
            &commit,
        )
        .await
        .map_err(owner_error)
    })
    .await?;
    let mut custody = Custody { store: None, owner };
    phase(&progress, timeout)?;
    let basis = bounded(timeout, &cancellation, async {
        custody
            .owner
            .initial_ingest_basis(|doc, digest| validate(&root, doc, digest))
            .await
            .map_err(owner_error)
    })
    .await?;
    if basis.policy != accepted {
        return Err((
            4,
            "accepted policy changed during initial Ingest startup".into(),
        ));
    }
    let mut domains = Vec::with_capacity(9);
    for kind in 1..=9 {
        domains.push((
            DictionaryKind::try_from(kind).expect("assigned kind"),
            (*fresh()?.as_bytes()).try_into().expect("nonzero domain"),
        ));
    }
    let spec = InitialRegistration {
        dataset_id: dataset,
        instance_id: *instance.as_bytes(),
        control: custody.owner.identity(),
        ingest_resource_id: (*fresh()?.as_bytes()).try_into().unwrap(),
        ingest_role_instance_id: *fresh()?.as_bytes(),
        process_incarnation: fresh()?,
        shard_id: 0,
        config_revision: accepted.config_revision,
        policy_revision: accepted.policy_revision,
        config_digest: Digest256::from_bytes(accepted.config_digest),
        policy_digest: Digest256::from_bytes(accepted.policy_digest),
        previous_topology: basis.previous_topology,
        allocation_domains: domains,
        producer_slot_id: (*fresh()?.as_bytes()).try_into().unwrap(),
        outstanding_request_limit: u32::try_from(outstanding).map_err(|_| {
            (
                3,
                "initial producer count is outside supported range".into(),
            )
        })?,
        created_at_us: basis.created_at_us,
        lease_expires_at_us: basis.lease_expires_at_us,
    };
    let manifest = IngestManifest::new(
        ManifestIdentity {
            dataset_id: dataset,
            topology_generation: spec.target_topology().map_err(|e| (3, e.to_string()))?,
            owner_resource_id: spec.ingest_resource_id,
            shard_id: spec.shard_id,
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            process_incarnation: spec.process_incarnation,
            recovery_generation: RecoveryGenerationId::from_bytes(*fresh()?.as_bytes()),
            policy_epoch: PolicyEpoch::new(spec.policy_revision),
        },
        AllocationNamespaces::from_entries(&spec.allocation_domains).unwrap(),
    );
    let mut attempt = RegistrationAttempt::new(spec.clone()).map_err(|e| (3, e.to_string()))?;
    phase(&progress, timeout)?;
    let registered = bounded(timeout, &cancellation, async {
        custody
            .owner
            .register_ingest(&mut attempt)
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    if registered != (RegistrationObservation::Matching { current: true }) {
        return Err((
            4,
            "initial registration conflicts with retained state".into(),
        ));
    }
    phase(&progress, timeout)?;
    if cancellation.requested.load(Ordering::Acquire) {
        return Err((
            4,
            "initialization cancelled after registration; preserve original registry".into(),
        ));
    }
    custody.store = Some(
        IngestStore::initialize(path, manifest, native_limits(), native)
            .map_err(|e| (4, e.to_string()))?,
    );
    let public = QualifiedRef {
        dataset_id: dataset,
        dictionary_kind: DictionaryKind::VisibilityScope,
        allocation_domain: manifest
            .allocation_namespaces()
            .domain(DictionaryKind::VisibilityScope)
            .unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
    };
    let dictionaries = ["public", "private"]
        .into_iter()
        .enumerate()
        .map(|(index, label)| {
            DictionaryEntry::new(
                QualifiedRef {
                    ordinal: Ordinal::new(index as u64 + 1).unwrap(),
                    ..public
                },
                DictionaryDescriptor::Visibility {
                    class: DescriptorLabel::new(label.into()).unwrap(),
                },
            )
            .unwrap()
        })
        .collect();
    let intent = EvidenceIntent::new(
        1,
        public,
        dictionaries,
        EvidenceIntentLimits {
            max_entries: 2,
            max_encoded_intent_bytes: 8192,
            max_validation_scratch_bytes: 1024,
        },
    )
    .map_err(|e| (3, format!("initial dictionaries refused: {e:?}")))?;
    phase(&progress, timeout)?;
    let receipt = bounded(timeout, &cancellation, async {
        custody
            .owner
            .bootstrap_initial_dictionaries(
                custody.store.as_mut().unwrap(),
                InitialBatchRequest {
                    ingest_root: path,
                    registration: &spec,
                    intent: &intent,
                    recorded_at: ObservedTime::new(spec.created_at_us),
                    limits: BootstrapLimits {
                        max_mappings: 2,
                        max_batch_bytes: MIB as usize,
                    },
                    policy_ttl: ttl,
                    cancelled: &cancellation.requested,
                },
                bootstrap,
                |doc, digest| validate(&root, doc, digest),
            )
            .await
            .map_err(|e| (4, e.to_string()))
    })
    .await?;
    drop(receipt);
    let report = json!({"format":"bigbrotr-initial-ingest/1","state":"initialized",
        "dataset_id":dataset,"owner_resource_id":spec.ingest_resource_id,"producer_slot_id":spec.producer_slot_id,
        "recovery_generation":manifest.identity().recovery_generation,
        "topology_generation":spec.target_topology().unwrap().get().to_string(), "public_scope":public,
        "native_profile":1,"native_allowance_bytes":128*MIB,"dictionary_count":2,
        "acquisition":"not-started","ready":false});
    // Refresh both scheduling rows before reporting a maintained lifetime.
    loop {
        phase(&progress, timeout)?;
        bounded(timeout, &cancellation, async {
            let current = custody
                .owner
                .refresh(|doc, digest| validate(&root, doc, digest))
                .await
                .map_err(owner_error)?;
            if current != accepted {
                return Err((4, "accepted policy changed; initial Ingest stopped".into()));
            }
            custody
                .owner
                .renew_initial_ingest(
                    &spec,
                    ttl,
                    |doc, digest| validate(&root, doc, digest),
                    &Cell::new(false),
                )
                .await
                .map_err(owner_error)
        })
        .await?;
        // Polling policy and scheduling renewal share one serial owner. The
        // next period is smaller than the accepted authorization window.
        let period = (ttl / 2).min(duration(&root, "heartbeat_interval"));
        phase(&progress, period + timeout)?;
        progress.send_modify(|value| {
            value.initialized = Some(report.clone());
        });
        tokio::select! {
            biased;
            _ = cancellation.wait() => break,
            _ = tokio::time::sleep(period) => {},
        }
    }
    phase(&progress, duration(&root, "graceful_shutdown_timeout"))?;
    drop(custody.store.take());
    tokio::time::timeout(
        duration(&root, "graceful_shutdown_timeout"),
        custody.owner.release(),
    )
    .await
    .map_err(|_| (4, "Control release deadline after native close".into()))?
    .map_err(owner_error)?;
    Ok(
        json!({"format":"bigbrotr-initial-ingest/1","state":"stopped","native":"closed",
        "owner_release":"committed","source_closure":"not-performed","ready":false}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn a_late_native_completion_cannot_reset_the_phase_watchdog() {
        let expired = Instant::now() - std::time::Duration::from_secs(1);
        let (progress, _observed) = watch::channel(Progress {
            deadline: expired,
            initialized: None,
        });
        let _ = phase(&progress, std::time::Duration::from_secs(5));
        assert_eq!(progress.borrow().deadline, expired);
    }
}
