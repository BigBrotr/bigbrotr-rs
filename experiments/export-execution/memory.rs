//! Finite measurement of one real accepted Export job. No scheduler or publisher changes.
//! This caller names whole-call boundaries; the unchanged worker exposes no inner phases.
pub(super) mod accept;
mod descriptor;
mod observe;
use bigbrotr::{
    export::{
        destination::{CheckedDestination, LocalDestination, check_filesystem_growth},
        execution::{ExecutionCustody, ExportLimits, ExportState, WrittenExport},
    },
    runtime::{
        OwnedPermit, ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest,
        WaiterLimits,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
    secrets::SecretFile,
    storage::{
        archive::{
            ArchiveEventError, ArtifactIdentity, CatalogReaderCredentials, CatalogRoute,
            CatalogTls, LocalCoordinator, Preparation, PreparationError, PreparationLimits,
            ProcessError, WorkerInstallation,
        },
        postgres::{
            catalog_guard::{self, CatalogCapture, CatalogIdentity},
            connection_settings::{ConnectionSettings, PlaintextBoundary, TlsSettings},
            control_owner::{ArchivePinRequest, ClaimRequest, ControlSession, OwnerError},
            transport,
        },
    },
};
use bigbrotr_model::{DatasetId, Digest256, JobId, PolicyEpoch};
use descriptor::{Case, Connection, Documents};
use observe::{Observer, emit};
use sha2::{Digest, Sha256};
use std::{
    cell::Cell,
    fmt::Debug,
    fs,
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio_postgres::Client;
type Result<T> = std::result::Result<T, String>;
type Driver = tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>;
const MIB: u64 = 1024 * 1024;
const FIXTURE_BYTES: u64 = 16 * MIB;
fn error(value: impl Debug) -> String {
    format!("{value:?}")
}
fn fresh() -> Result<[u8; 16]> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(error)?;
    if bytes == [0; 16] {
        return Err("zero random identity".into());
    }
    Ok(bytes)
}
fn grant(budget: &ResourceBudget, bytes: u64) -> Result<OwnedPermit> {
    budget
        .try_acquire(ResourceRequest::data(bytes, 1, 0).map_err(error)?)
        .map_err(error)
}
async fn bounded<T, E: Debug>(
    future: impl Future<Output = std::result::Result<T, E>>,
) -> Result<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .map_err(|_| "SQL/transport deadline; possible unknown outcome".to_owned())?
        .map_err(error)
}
async fn backend(client: &Client) -> Result<serde_json::Value> {
    let row = bounded(client.query_one("SELECT pg_backend_pid(), backend_start::text, current_database(), current_user FROM pg_catalog.pg_stat_activity WHERE pid=pg_backend_pid()",&[])).await?;
    Ok(
        serde_json::json!({"pid":row.try_get::<_,i32>(0).map_err(error)?,
        "start":row.try_get::<_,String>(1).map_err(error)?,"database":row.try_get::<_,String>(2).map_err(error)?,
        "user":row.try_get::<_,String>(3).map_err(error)?}),
    )
}

/// Resource and output custody stays outside fallible/timeout futures through joins.
#[derive(Default)]
struct Held {
    owner: Option<ControlSession>,
    manager: Option<Client>,
    coordinator: Option<LocalCoordinator>,
    control_driver: Option<Driver>,
    catalog_driver: Option<Driver>,
    control_grant: Option<OwnedPermit>,
    catalog_grant: Option<OwnedPermit>,
    output_grant: Option<OwnedPermit>,
    destination: Option<Arc<LocalDestination>>,
    checked_destination: Option<CheckedDestination>,
    output: Option<ExecutionCustody>,
    written: Option<WrittenExport>,
}
async fn join(mut driver: Driver) -> (bool, bool) {
    match tokio::time::timeout(Duration::from_secs(5), &mut driver).await {
        Ok(Ok(Ok(()))) => (true, false),
        Ok(_) => (true, true),
        Err(_) => {
            driver.abort();
            let _ = driver.await;
            (false, true)
        }
    }
}
impl Held {
    async fn close(&mut self, success: bool, observation: &Observer) -> Result<()> {
        let mut clean = true;
        self.manager.take();
        if let Some(driver) = self.catalog_driver.take() {
            let (natural, failed) = join(driver).await;
            clean &= natural && !failed;
            clean &= observation
                .mark(
                    "catalog-driver-joined",
                    serde_json::json!({"natural":natural,"failed":failed}),
                )
                .is_ok();
        }
        self.catalog_grant.take();
        if let Some(owner) = self.owner.take() {
            if success {
                let released = bounded(owner.release()).await;
                clean &= released.is_ok();
                clean &= observation
                    .mark(
                        "control-release-returned",
                        serde_json::json!({"committed":released.is_ok(),"error":released.err()}),
                    )
                    .is_ok();
            } else {
                drop(owner);
            }
        }
        if let Some(driver) = self.control_driver.take() {
            let (natural, failed) = join(driver).await;
            clean &= natural && !failed;
            clean &= observation
                .mark(
                    "control-driver-joined",
                    serde_json::json!({"natural":natural,"failed":failed}),
                )
                .is_ok();
        }
        // Destination and written-observation custody outlive unknown SQL futures
        // and are released only after the actual Control driver has joined.
        self.control_grant.take();
        self.written.take();
        self.output.take();
        self.checked_destination.take();
        self.destination.take();
        self.output_grant.take();
        self.coordinator.take();
        if clean {
            Ok(())
        } else {
            Err("driver/release cleanup not a clean completion".into())
        }
    }
}
async fn connect(
    input: &Connection,
    budget: &ResourceBudget,
    extra: u64,
) -> Result<(
    Client,
    transport::Driver,
    transport::PinnedRoute,
    OwnedPermit,
)> {
    let settings = input.settings()?;
    // Installed connection accounting: input copies +16MiB, plus the owner's
    // extra reservation (Control512MiB; catalog64MiB). Logical charges, not RSS.
    let bytes = (settings.preparation_input_reservation_bytes() as u64)
        .checked_mul(8)
        .and_then(|n| n.checked_add(16 * MIB + extra))
        .ok_or("credential arithmetic")?;
    let permit = grant(budget, bytes)?;
    let prepared = settings.prepare().map_err(error)?;
    let (client, driver, route) = bounded(transport::connect_pinned(prepared, None)).await?;
    Ok((client, driver, route, permit))
}

pub(super) async fn run(path: &Path) -> Result<()> {
    let case = Case::read(path)?;
    let budget = case.limits.budget()?;
    // Fixed small fixture/controller/JSON/observer charge, separate from actual drivers.
    let fixture_grant = grant(&budget, FIXTURE_BYTES)?;
    let documents = Documents::read(&case)?;
    let observer = Observer::start(&case.ledger, &budget)?;
    let cancelled = Arc::new(AtomicBool::new(false));
    let signal_flag = cancelled.clone();
    let signal = tokio::spawn(async move {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            term.recv().await;
            signal_flag.store(true, Ordering::Release);
        }
    });
    let mut held = Held::default();
    let outcome = work(&case, &documents, &budget, &observer, &cancelled, &mut held).await;
    // Never turn an unknown commit/worker result into a completed export or retry.
    let cleanup = held.close(outcome.is_ok(), &observer).await;
    signal.abort();
    let _ = signal.await;
    observer.mark(
        "operation-returned",
        serde_json::json!({"error":outcome.as_ref().err(),"cleanup_error":cleanup.as_ref().err()}),
    )?;
    drop(documents);
    drop(fixture_grant);
    let released = budget.snapshot().active_permits() == 0;
    observer.mark(
        "grants-released",
        serde_json::json!({"all_released":released}),
    )?;
    observer.finish()?;
    outcome?;
    cleanup?;
    if !released {
        return Err("retained grants: outcome requires external exclusion".into());
    }
    emit(
        serde_json::json!({"event":"export-measure-complete","name":case.name,
        "status":"COMPLETE","authority":"one-accepted-job","inner_phases":"not-observed"}),
    )
}

async fn work(
    case: &Case,
    documents: &Documents,
    budget: &ResourceBudget,
    observation: &Observer,
    cancelled: &Arc<AtomicBool>,
    held: &mut Held,
) -> Result<()> {
    observation.mark(
        "preflight",
        serde_json::json!({"job":case.job,"event_id":case.event_id,
        "worker_sha256":case.worker_sha256,"worker_bytes":case.worker_bytes}),
    )?;
    let control_bytes = case
        .control
        .settings()?
        .preparation_input_reservation_bytes() as u64
        * 8
        + 528 * MIB;
    let catalog_bytes = case
        .catalog
        .settings()?
        .preparation_input_reservation_bytes() as u64
        * 8
        + 80 * MIB;
    let parent_bytes = (case.limits.row_bytes as u64 + 2048) * 64;
    let native_bytes = case
        .limits
        .engine_memory_bytes
        .checked_add(case.limits.native_reserve_bytes)
        .ok_or("native arithmetic")?;
    {
        // Serial no-wait admission before claim; actual owners reacquire these
        // same lanes below. The fixed fixture allowance is already retained.
        let _data = budget
            .try_acquire(
                ResourceRequest::data(
                    control_bytes + catalog_bytes + 2 * MIB + parent_bytes,
                    6,
                    native_bytes,
                )
                .map_err(error)?,
            )
            .map_err(error)?;
        let _control = budget
            .try_acquire(ResourceRequest::control(128 * 1024).map_err(error)?)
            .map_err(error)?;
    }
    observation.mark("whole-call-admitted",serde_json::json!({"control_service_bytes":control_bytes,
        "catalog_manager_bytes":catalog_bytes,"reader_bytes":MIB,"output_bytes":MIB,
        "query_parent_bytes":parent_bytes,"native_bytes":native_bytes,"native_control_bytes":128*1024,
        "fixture_bytes":FIXTURE_BYTES,"job_cap_qualification":false}))?;
    case.installation()
        .verify(Instant::now() + Duration::from_secs(5), cancelled)
        .map_err(error)?;
    // Directory must be supplied fresh/private; validation does not create or repair it.
    Preparation {
        installation: &case.installation(),
        temp_root: &case.scratch,
        limits: case.limits.preparation(),
        deadline: Instant::now() + Duration::from_secs(5),
        cancelled,
    }
    .validate_temporary_root()
    .map_err(error)?;
    let (client, driver, _, permit) = connect(&case.control, budget, 512 * MIB).await?;
    held.control_grant = Some(permit);
    held.control_driver = Some(tokio::spawn(driver));
    observation.mark("control-connected", backend(&client).await?)?;
    let incarnation = fresh()?.try_into().map_err(error)?;
    let commit = Cell::new(false);
    let acquired = bounded(ControlSession::acquire(
        client,
        ClaimRequest {
            dataset: case.dataset,
            instance: case.instance,
            authority_mode: "managed".into(),
            role_instance_id: fresh()?,
            incarnation,
            lease_ttl: Duration::from_secs(5),
        },
        |text, digest| documents.validate(text, digest),
        &commit,
    ))
    .await?;
    held.owner = Some(acquired.0);
    let owner = held.owner.as_mut().ok_or("missing Control")?;
    observation.mark(
        "control-acquired",
        serde_json::json!({"epoch":owner.identity().epoch.get(),
        "incarnation":incarnation,"commit_started":commit.get()}),
    )?;
    bounded(owner.require_export_execution()).await?;
    let original = bounded(owner.read_export_job(case.job))
        .await?
        .ok_or("accepted job missing")?;
    if original.state() != ExportState::Accepted
        || original.selection().query().event_id != case.event_id
        || original.selection().query().dataset != case.dataset
    {
        return Err("fixture requires its exact accepted job".into());
    }
    original
        .selection()
        .query()
        .validate(case.limits.row_bytes)
        .map_err(error)?;
    let destination = Arc::new(
        LocalDestination::open(
            &case.destination,
            case.dataset,
            original.selection().destination().id,
        )
        .map_err(error)?,
    );
    let growth = case
        .limits
        .export_limits()?
        .temporary()
        .checked_add(case.limits.temp_bytes)
        .and_then(|n| n.checked_add(MIB))
        .ok_or("disk arithmetic")?;
    destination
        .check_growth(growth, case.limits.disk_reserve_bytes)
        .map_err(error)?;
    check_filesystem_growth(
        &fs::File::open(&case.scratch).map_err(error)?,
        growth,
        case.limits.disk_reserve_bytes,
    )
    .map_err(error)?;
    let checked = destination.clone().check_for_acceptance().map_err(error)?;
    held.destination = Some(destination);
    held.checked_destination = Some(checked);
    let identity = CatalogIdentity::local(
        case.dataset,
        original.selection().catalog(),
        &case.data_root,
    )
    .map_err(error)?;
    held.coordinator = Some(LocalCoordinator::open(&identity, &case.state_root).map_err(error)?);
    let coordinator = held.coordinator.as_ref().ok_or("missing coordinator")?;
    bounded(owner.require_archive_coordinator(coordinator)).await?;
    let (manager, driver, route, permit) = connect(&case.catalog, budget, 64 * MIB).await?;
    held.catalog_grant = Some(permit);
    held.manager = Some(manager);
    held.catalog_driver = Some(tokio::spawn(
        coordinator.retain_connection(driver).map_err(error)?,
    ));
    let manager = held.manager.as_mut().ok_or("missing catalog")?;
    observation.mark("catalog-connected", backend(manager).await?)?;
    bounded(catalog_guard::inspect_reader(
        manager,
        &identity,
        &case.reader_user,
    ))
    .await?;
    let reader_grant = grant(budget, MIB)?;
    let password = SecretFile::parse(&case.reader_password_ref)
        .map_err(error)?
        .read(4096)
        .map_err(error)?;
    std::str::from_utf8(password.as_bytes()).map_err(error)?;
    held.output_grant = Some(grant(budget, MIB)?);
    bounded(owner.refresh(|text, digest| documents.validate(text, digest))).await?;
    let queued = bounded(
        owner.reserve_export(
            case.job,
            case.limits.export_limits()?,
            held.checked_destination
                .as_ref()
                .ok_or("missing checked destination")?,
            &Cell::new(false),
        ),
    )
    .await?;
    observation.mark("reserved", serde_json::json!({"artifact":queued.reservation().ok_or("reservation missing")?.artifact(),
        "query":queued.selection().query(),"catalog":queued.selection().catalog(),"frontier_revision":queued.selection().frontier_revision()}))?;
    let capture = bounded(CatalogCapture::at_snapshot(
        manager,
        coordinator,
        queued.selection().query().snapshot_id,
        Duration::from_secs(30),
    ))
    .await?;
    let pin = bounded(owner.register_archive_pin(
        &capture,
        &ArchivePinRequest {
            lease_id: fresh()?,
            frontier_revision: queued.selection().frontier_revision(),
            ttl: Duration::from_secs(30),
        },
        &Cell::new(false),
    ))
    .await?;
    observation.mark(
        "captured-and-pinned",
        serde_json::json!({"snapshot_id":capture.snapshot_id(),"lease_id":pin.lease_id}),
    )?;
    let mut lease = PolicyLeaseTracker::new(
        case.dataset,
        incarnation,
        PolicyEpoch::new(acquired.1.policy_revision),
        PolicyLeaseTtl::new(Duration::from_secs(5)).map_err(error)?,
    )
    .map_err(error)?;
    let ticket = lease.begin_refresh().map_err(error)?;
    let policy = bounded(owner.refresh(|text, digest| documents.validate(text, digest))).await?;
    lease
        .complete_refresh(
            &ticket,
            LeaseResponse {
                dataset: case.dataset,
                process_incarnation: incarnation,
                policy_epoch: PolicyEpoch::new(policy.policy_revision),
                ttl: Duration::from_secs(5),
            },
        )
        .map_err(error)?;
    if cancelled.load(Ordering::Acquire) {
        return Err("cancelled before start".into());
    }
    let action = lease.begin_action(lease.known_epoch()).map_err(error)?;
    let running =
        bounded(owner.start_export(&queued, &capture, pin.lease_id, &Cell::new(false))).await?;
    let mut window = action.complete(running.remaining()).map_err(error)?;
    let output = running
        .prepare(
            held.destination
                .as_ref()
                .ok_or("missing destination")?
                .clone(),
        )
        .map_err(error)?;
    held.output = Some(output.custody());
    let custody = coordinator.reader_custody().map_err(error)?;
    let query = queued.selection().query().clone();
    let installation = case.installation();
    let scratch = case.scratch.clone();
    let limits = case.limits.preparation();
    let scan = case.limits.scan_bytes;
    let database = case.catalog.database.clone();
    let user = case.reader_user.clone();
    let budget = budget.clone();
    let cancel = cancelled.clone();
    let deadline = Instant::now() + Duration::from_millis(case.limits.query_ms);
    observation.mark("native-query-call", serde_json::json!({"row_bytes":limits.max_encoded_bytes,
        "engine_memory_bytes":limits.memory_bytes,"native_reserve_bytes":limits.native_reserve_bytes,"scan_bytes":scan}))?;
    // Join is never selected away or timed out. The actual native supervisor owns
    // its deadline/cancellation; the outer harness treats missing return as unknown.
    let native = tokio::task::spawn_blocking(move || {
        let _reader_grant = reader_grant;
        window.check().map_err(error)?;
        let route = CatalogRoute {
            address: route.address(),
            port: route.port(),
            database: &database,
            tls: CatalogTls::LocalPlaintext(PlaintextBoundary::Loopback),
        };
        let observed = Preparation {
            installation: &installation,
            temp_root: &scratch,
            limits,
            deadline,
            cancelled: &cancel,
        }
        .query_event_bounded(
            query,
            custody,
            &route,
            CatalogReaderCredentials {
                login: &user,
                password: std::str::from_utf8(password.as_bytes()).map_err(error)?,
            },
            &budget,
            scan,
        );
        match observed {
            Err(
                ArchiveEventError::TerminationUnknown
                | ArchiveEventError::Preparation(PreparationError::Process(
                    ProcessError::TerminationUnknown,
                )),
            ) => Err("native termination unknown; external exclusion required".into()),
            Err(other) => Err(error(other)),
            Ok(observed) => {
                window.check().map_err(error)?;
                Ok((observed, output, window))
            }
        }
    })
    .await
    .map_err(|_| "native caller panicked; custody unknown")??;
    observation.mark("native-query-returned",serde_json::json!({"present":native.0.event().is_some(),
        "normalized_bytes":native.0.event().map(|event|event.normalized_bytes()),"worker":"reaped-by-real-reader","includes_parent_decode":true}))?;
    if native.0.event().is_none() {
        return Err("expected signed event absent".into());
    }
    held.written = Some(
        native
            .1
            .write_with_window(native.0, cancelled, native.2)
            .map_err(error)?,
    );
    let written = held.written.as_ref().ok_or("missing written export")?;
    observation.mark(
        "output-written",
        serde_json::json!({"rows":written.files().data.rows,"bytes":written.files().data.bytes,
        "sha256":written.files().data.sha256,"manifest_sha256":written.files().manifest_sha256}),
    )?;
    lease.check_delivery(lease.known_epoch()).map_err(error)?;
    let complete =
        bounded(owner.complete_export(written, &capture, pin.lease_id, &Cell::new(false))).await?;
    if complete.state() != ExportState::Succeeded || complete.files() != Some(written.files()) {
        return Err("completion differs".into());
    }
    observation.mark("sql-completed",serde_json::json!({"job":case.job,"revision":complete.revision(),
        "manifest":complete.result(),"artifact":complete.reservation().ok_or("completion reservation missing")?.artifact()}))?;
    bounded(owner.release_archive_pin(&pin, &Cell::new(false))).await?;
    bounded(capture.finish()).await?;
    observation.mark("pin-released-capture-finished", serde_json::json!({}))?;
    Ok(())
}
