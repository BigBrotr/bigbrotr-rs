//! Explicit initial lifetimes. The worker never returns a live native handle.
mod admission;
mod archive;
pub(super) mod archive_catch_up;
mod capacity;
mod commands;
mod dictionaries;
pub(super) mod index;
mod owner;
mod readiness;
pub(super) mod successor;
pub(super) mod successor_family;
use owner::{NativeOwner, Profile};
pub(super) mod recovery;
mod supervision;
mod sync;
mod sync_diagnostic;
use super::{Result, Stop, duration, owner_error};
use crate::{
    config::Resolved,
    interfaces::{lifecycle_output::LifecycleOutput, management, postgres_command},
};
use bigbrotr::{
    runtime::{
        OwnedPermit, ProcessEnvelope, ReservedBudget, ResourceBudget, ResourceLimits,
        ResourceRequest, WaiterLimits,
    },
    storage::{
        indexer::custody::IndexDriverCustody,
        postgres::{
            connection_settings::PreparedConnection,
            control_owner::{ClaimRequest, ControlSession, OwnerError},
            ingest_registry::{InitialRegistration, RegistrationAttempt, RegistrationObservation},
            transport,
        },
        rocks::{
            RocksNativeLimits, RocksNativeSettings,
            manifest::{AllocationNamespaces, IngestManifest, ManifestIdentity},
            store::IngestStore,
        },
    },
};
use bigbrotr_model::{
    DatasetId, DictionaryKind, Digest256, OwnerEpoch, PolicyEpoch, RecoveryGenerationId,
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

struct InitialGrants {
    index: Option<index::Index>,
    sync: Option<sync::Sync>,
    archive: Option<archive::Archive>,
    native: OwnedPermit,
    dictionaries: dictionaries::SetupGrants,
}

struct PreparedInitial {
    connection: PreparedConnection,
    control_grant: OwnedPermit,
    initial: InitialGrants,
}

/// Field order also protects unwind: native close completes before Client Drop.
/// Construct and destroy this value only on the blocking owner thread.
struct Custody {
    // The physical Index executor cancels and joins before native/SQL owners.
    index: Option<index::Index>,
    store: Option<NativeOwner>,
    sync: Option<sync::Sync>,
    archive: Option<archive::Archive>,
    owner: ControlSession,
}

#[derive(Clone)]
struct Progress {
    deadline: Instant,
    // Static source metadata only; None is the initial, unpublished phase.
    phase: Option<&'static std::panic::Location<'static>>,
    initialized: Option<Value>,
    shutdown: Option<management::Shutdown>,
    sync: Option<sync_diagnostic::Record>,
    capacity: Option<Arc<capacity::Mailbox>>,
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

fn selected_roots(root: &Resolved) -> Result<()> {
    // Initial publication is explicit; readers and export still use the ordinary
    // Control consumer after this fresh native owner has stopped.
    if !root.local("control")
        || !root.local("ingest")
        || root.flag("archive.reader.enabled")
        || root.text("export.destination.root").is_some()
        || root.text("read.cursor.keyring_ref").is_some()
        || root.text("administration.assets.directory").is_some()
    {
        return Err((3, "initial Ingest needs local Control/Ingest with archive reader disabled and no export destination, cursor keyring or private assets".into()));
    }
    Ok(())
}

fn accepted_profile(root: &Resolved) -> std::result::Result<(), OwnerError> {
    accepted_owner_profile(root, Profile::Initial)
}
fn accepted_owner_profile(
    root: &Resolved,
    profile: Profile,
) -> std::result::Result<(), OwnerError> {
    selected_roots(root).map_err(|_| OwnerError::InvalidPolicy)?;
    // Each accepted document must fund this explicitly selected lifetime.
    // A managed file's unused seed must not veto its valid producer window.
    if root.number("ingest.producer_outstanding_requests") < profile.minimum(false) {
        return Err(OwnerError::InvalidPolicy);
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

fn budget(root: &Resolved, sync: bool, archive: bool) -> Result<ResourceBudget> {
    owner_budget(root, sync, archive, 0)
}
fn owner_budget(
    root: &Resolved,
    sync: bool,
    archive: bool,
    retained: u64,
) -> Result<ResourceBudget> {
    let n = |field: &str| root.number(&format!("runtime.processes.application.{field}"));
    let items = (4 + retained + management::SESSION_ITEMS + if sync { sync::ITEMS } else { 0 })
        .checked_add(if archive { archive::ITEMS } else { 0 })
        .and_then(|items| {
            items.checked_add(if archive {
                bigbrotr_protocol::archive_cut::MAX_BATCHES as u64
            } else {
                0
            })
        })
        .ok_or_else(|| (3, "initial Ingest item reservation overflow".into()))?;
    let envelope = ProcessEnvelope::new(
        n("memory_max_bytes"),
        n("baseline_bytes"),
        ResourceLimits::new(
            n("payload_bytes"),
            // Native, retained inputs, setup progress and Control coexist with
            // bounded session connections, queued requests and one hash worker.
            items,
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

pub(super) fn run(
    root: Resolved,
    sync: bool,
    archive: bool,
    index: bool,
    output: &mut LifecycleOutput,
) -> Result<()> {
    selected_roots(&root)?;
    let budget = owner_budget(&root, sync, archive, if index { index::ITEMS } else { 0 })?;
    let initial = InitialGrants {
        index: None,
        sync: None,
        archive: None,
        native: grant(&budget, 0, native_limits().native_allowance_bytes())?,
        dictionaries: dictionaries::SetupGrants::reserve(&budget)?,
    };
    let inputs = grant(&budget, 24 * MIB, 0)?;
    let timeout = duration(&root, "renewal_timeout");
    let grace = duration(&root, "graceful_shutdown_timeout");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        // Native owner, one finite Archive worker and bounded password work.
        .max_blocking_threads(if archive { 3 } else { 2 })
        .build()
        .map_err(|_| (1, "initial Ingest runtime unavailable".into()))?;
    let outcome = runtime.block_on(async {
        let mut stop = Stop::new()?;
        let cancellation = Arc::new(Cancellation::new());
        // Keep the sender across both phases. Closing it after preparation can
        // otherwise look like terminal owner loss before its JoinHandle is ready.
        let (progress, mut observed) = watch::channel(Progress {
            deadline: Instant::now() + timeout, phase: None, initialized: None, shutdown: None, sync: None, capacity: None,
        });
        let prepare_root = root.clone();
        let prepare_budget = budget.clone();
        let prepare_cancel = cancellation.clone();
        let prepare_deadline = progress.borrow().deadline;
        let mut preparation = tokio::task::spawn_blocking(move || {
            prepare(&prepare_root, &prepare_budget, initial, sync, archive, index,
                prepare_deadline, &prepare_cancel.requested)
        });
        let prepared = match supervision::collect(&mut preparation, &mut observed,
            &cancellation, stop.requested(), grace, |_| {}).await {
            supervision::Collected::Joined { terminal, interrupted } => {
                let prepared = terminal.map_err(|_| (4, "initial Ingest preparation panicked".into()))??;
                // A stop can coincide with successful preparation. collect has
                // consumed the signal, so check its retained cancellation state.
                if interrupted || cancellation.requested.load(Ordering::Acquire) {
                    return Err((4, "initial Ingest preparation cancelled or expired before connection".into()));
                }
                prepared
            },
            supervision::Collected::Unjoined => {
                output.emit("error: initial Ingest preparation not joined; terminating process", true);
                std::process::exit(4);
            }
        };
        phase(&progress, timeout)?;
        let PreparedInitial { connection: prepared, control_grant, initial } = prepared;
        let connect_deadline = progress.borrow().deadline;
        let (client, connection) = tokio::select! {
            biased;
            _ = stop.requested() => return Err((4, "initial Ingest connection cancelled".into())),
            _ = tokio::time::sleep_until(connect_deadline) => return Err((4, "initial Ingest connection deadline".into())),
            result = transport::connect(prepared) => result.map_err(|e| (1, e.to_string()))?,
        };
        phase(&progress, timeout)?;
        let connection: transport::Driver = match &initial.archive {
            Some(archive) => Box::pin(archive.coordinator.retain_connection(connection)
                .map_err(|e| (4, format!("initial Archive Control custody refused: {e:?}")))?),
            None => connection,
        };
        // The Index lock is created after accepted policy and native startup.
        // Attach its shared holder to the actual driver before it is polled.
        let index_driver = IndexDriverCustody::new();
        let connection: transport::Driver = if index {
            Box::pin(index_driver.retain_connection(connection))
        } else { connection };
        let driver = tokio::spawn(connection);
        let worker_cancel = cancellation.clone();
        let handle = tokio::runtime::Handle::current();
        let mut worker = tokio::task::spawn_blocking(move || {
            // The parent keeps driving current-thread I/O/timers while this
            // owner parks on async SQL or blocks in native open/write/close.
            let _inputs = inputs;
            handle.block_on(own(root, client, initial, budget, progress, worker_cancel, index_driver))
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
        drop(control_grant);
        let report = terminal.map_err(|_| (4, "initial Ingest worker panicked; inspect retained state".into()))??;
        if interrupted { return Err((4, "initial Ingest operation exceeded its deadline; inspect retained state".into())); }
        output.emit(report, false);
        Ok(())
    });
    runtime.shutdown_timeout(postgres_command::DRIVER_DRAIN);
    outcome
}

/// All startup file reads run on the joined preparer, under the independent
/// parent's signal/deadline watchdog. Archive probes own and destroy their actual
/// catalog transports here; no Ingest handle or Control claim exists yet.
#[allow(clippy::too_many_arguments)]
fn prepare(
    root: &Resolved,
    budget: &ResourceBudget,
    mut initial: InitialGrants,
    sync: bool,
    archive: bool,
    index: bool,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<PreparedInitial> {
    let (connection, control_grant) = postgres_command::prepare_control_shared(
        root,
        required(root, "postgres.control.user")?,
        required(root, "postgres.control.password_ref")?,
        budget,
    )?;
    initial.sync = sync
        .then(|| sync::Sync::prepare(root, budget, Profile::Initial))
        .transpose()?;
    initial.archive = archive
        .then(|| archive::Archive::prepare(root, budget, deadline, cancelled))
        .transpose()?;
    initial.index = index
        .then(|| index::Index::prepare(root, budget, true))
        .transpose()?;
    if sync {
        // Permanent startup holders cannot be treated as temporary pressure.
        // This check issues no operation; each real round acquires and keeps
        // its own complete reservation after read-only nomination, before claim.
        sync::preflight(budget)?;
    }
    let path = Path::new(required(root, "storage.ingest_root")?);
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
    // Native initialize repeats exclusive creation; preparation never adopts a
    // directory, symlink or partial state observed here or created in the gap.
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
        _ => return Err((3, "initial Ingest target exists or cannot be inspected; preserve it for explicit recovery".into())),
    }
    Ok(PreparedInitial {
        connection,
        control_grant,
        initial,
    })
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
    validate_owner(root, Profile::Initial, document, digest)
}
fn validate_owner(
    root: &Resolved,
    profile: Profile,
    document: &str,
    digest: [u8; 32],
) -> std::result::Result<(), OwnerError> {
    let restored = super::accepted_policy(root, document, digest)?;
    accepted_owner_profile(&restored, profile)
}
fn phase_label(location: Option<&'static std::panic::Location<'static>>) -> String {
    let Some(location) = location else {
        return "initialization".into();
    };
    // Do not print an absolute compiler/worktree path. These private callers
    // belong to this repository subtree; unknown mappings disclose no path.
    let file = location.file();
    let relative = file
        .strip_prefix("apps/bigbrotr/src/")
        .or_else(|| file.strip_prefix("src/"))
        .or_else(|| file.split_once("/apps/bigbrotr/src/").map(|(_, tail)| tail))
        .unwrap_or("<unmapped-source>");
    format!("{relative}:{}:{}", location.line(), location.column())
}

#[track_caller]
fn phase(progress: &watch::Sender<Progress>, timeout: std::time::Duration) -> Result<()> {
    phase_capped(progress, timeout, None)
}

#[track_caller]
fn phase_capped(
    progress: &watch::Sender<Progress>,
    timeout: std::time::Duration,
    ceiling: Option<Instant>,
) -> Result<()> {
    let now = Instant::now();
    let requested = std::panic::Location::caller();
    {
        let previous = progress.borrow();
        if now >= previous.deadline {
            return Err((
                4,
                format!(
                    "initial Ingest phase completed after its deadline; preserve retained state; prior_phase={}; requested_phase={}; overdue_us={}",
                    phase_label(previous.phase),
                    phase_label(Some(requested)),
                    now.duration_since(previous.deadline).as_micros(),
                ),
            ));
        }
    }
    // Only a timely phase may publish a new deadline. This also covers native
    // calls that returned after both the worker and parent were unscheduled.
    progress.send_modify(|value| {
        value.deadline = ceiling.map_or(now + timeout, |ceiling| ceiling.min(now + timeout));
        value.phase = Some(requested);
    });
    Ok(())
}

async fn own(
    root: Resolved,
    client: tokio_postgres::Client,
    initial: InitialGrants,
    budget: ResourceBudget,
    progress: watch::Sender<Progress>,
    cancellation: Arc<Cancellation>,
    index_driver: IndexDriverCustody,
) -> Result<Value> {
    let mut index = initial.index;
    let mut sync = initial.sync;
    let mut archive = initial.archive;
    let path = Path::new(required(&root, "storage.ingest_root")?);
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
    let commit = Cell::new(false);
    let mut outstanding = u64::MAX;
    let mut receipt_retention = None;
    phase(&progress, timeout)?;
    let (owner, accepted) = bounded(timeout, &cancellation, async {
        ControlSession::acquire(
            client,
            request,
            |doc, digest| {
                let restored = super::accepted_policy(&root, doc, digest)?;
                accepted_profile(&restored)?;
                if let Some(sync) = &mut sync {
                    sync.accept(&restored)?;
                }
                if let Some(archive) = &mut archive {
                    archive.accept(&restored)?;
                }
                if let Some(index) = &mut index {
                    index.accept(&restored)?;
                }
                outstanding =
                    outstanding.min(restored.number("ingest.producer_outstanding_requests"));
                let captured = u8::try_from(restored.number("ingest.request_receipt_retention"))
                    .map_err(|_| OwnerError::InvalidPolicy)?;
                if receipt_retention.is_some_and(|old| old != captured) {
                    return Err(OwnerError::InvalidPolicy);
                }
                receipt_retention = Some(captured);
                Ok(())
            },
            &commit,
        )
        .await
        .map_err(owner_error)
    })
    .await?;
    let mut custody = Custody {
        index,
        store: None,
        sync,
        archive,
        owner,
    };
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
    if let Some(archive) = &custody.archive {
        phase(&progress, timeout)?;
        bounded(timeout, &cancellation, archive.require(&mut custody.owner)).await?;
        let admission::Admission::Granted(startup) = archive.reserve(&root, &budget)? else {
            return Err((
                3,
                "initial Archive cannot coexist with retained startup owners".into(),
            ));
        };
        drop(startup);
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
        request_receipt_retention: receipt_retention.ok_or_else(|| {
            (
                4,
                "initial retention setting was not captured from accepted startup policy".into(),
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
    custody.store = Some(NativeOwner::Initial(Box::new(
        IngestStore::initialize(path, manifest, native_limits(), initial.native)
            .map_err(|e| (4, e.to_string()))?,
    )));
    let (public, private) = dictionaries::initialize(
        &mut custody,
        &spec,
        &root,
        &progress,
        &cancellation,
        initial.dictionaries,
    )
    .await?;
    if let Some(index) = &mut custody.index {
        phase(&progress, timeout)?;
        index.initialize(
            &root,
            custody
                .store
                .as_ref()
                .expect("initialized native owner")
                .initial()?,
            &spec,
            &index_driver,
        )?;
    }
    let report = json!({"format":"bigbrotr-initial-ingest/1","state":"initialized",
        "dataset_id":dataset,"owner_resource_id":spec.ingest_resource_id,"producer_slot_id":spec.producer_slot_id,
        "recovery_generation":manifest.identity().recovery_generation,
        "topology_generation":spec.target_topology().unwrap().get().to_string(), "public_scope":public, "private_scope":private,
        "native_profile":1,"native_allowance_bytes":128*MIB,"dictionary_count":3,
        "acquisition":if custody.sync.is_some() { "finite-initial" } else { "not-started" },
        "index": index_observation(custody.index.as_ref()),"ready":false});
    commands::maintain(
        &mut custody,
        &spec,
        &root,
        &budget,
        &progress,
        &cancellation,
        report,
    )
    .await?;
    phase(&progress, duration(&root, "graceful_shutdown_timeout"))?;
    let index = index_observation(custody.index.as_ref());
    drop(custody.index.take());
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
        "owner_release":"committed","source_closure":"not-performed","index":index,"ready":false}),
    )
}

fn index_observation(index: Option<&index::Index>) -> Value {
    index.map_or_else(|| json!({"state":"not-selected"}), |index| {
        json!({"state":index.phase(),"published_revision":index.published_revision().to_string(),
            "complete_through":index.frontier().map(|cut| cut.bounds().iter().map(|bound| {
                json!({"shard_id":bound.stream.shard_id,
                    "owner_epoch":bound.stream.owner_epoch.get().to_string(),
                    "sequence":bound.last_complete_sequence.get().to_string()})
            }).collect::<Vec<_>>()),"readiness":"not-qualified"})
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn a_late_native_completion_cannot_reset_the_phase_watchdog() {
        let expired = Instant::now() - std::time::Duration::from_secs(1);
        let (progress, _observed) = watch::channel(Progress {
            deadline: expired,
            phase: None,
            initialized: None,
            shutdown: None,
            sync: None,
            capacity: None,
        });
        let error = phase(&progress, std::time::Duration::from_secs(5)).unwrap_err();
        assert_eq!(error.0, 4);
        assert!(error.1.starts_with("initial Ingest phase completed after its deadline; preserve retained state; prior_phase=initialization; requested_phase=interfaces/serve/initial_ingest.rs:"));
        let overdue = error
            .1
            .rsplit_once("; overdue_us=")
            .unwrap()
            .1
            .parse::<u128>()
            .unwrap();
        assert!(overdue >= 1_000_000);
        assert!(!error.1.contains(env!("CARGO_MANIFEST_DIR")));
        assert_eq!(progress.borrow().deadline, expired);
        assert!(progress.borrow().phase.is_none());
    }

    #[tokio::test]
    async fn a_phase_refusal_retains_the_prior_static_callsite() {
        let (progress, _observed) = watch::channel(Progress {
            deadline: Instant::now() + std::time::Duration::from_secs(60),
            phase: None,
            initialized: None,
            shutdown: None,
            sync: None,
            capacity: None,
        });
        phase(&progress, std::time::Duration::from_secs(5)).unwrap();
        let prior = progress.borrow().phase.unwrap();
        let expired = Instant::now() - std::time::Duration::from_secs(1);
        progress.send_modify(|value| value.deadline = expired);
        let error = phase(&progress, std::time::Duration::from_secs(60)).unwrap_err();
        assert!(
            error
                .1
                .contains(&format!("prior_phase={};", phase_label(Some(prior))))
        );
        let requested = error
            .1
            .split("requested_phase=")
            .nth(1)
            .unwrap()
            .split(';')
            .next()
            .unwrap();
        assert_ne!(requested, phase_label(Some(prior)));
        assert_eq!(progress.borrow().deadline, expired);
        assert!(std::ptr::eq(progress.borrow().phase.unwrap(), prior));
    }
}
