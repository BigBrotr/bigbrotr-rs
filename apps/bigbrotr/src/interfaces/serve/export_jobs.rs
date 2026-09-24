//! One retained exact-ID job at a time, independent of a browser connection.
use super::{
    Result, duration,
    native_grants::{self, OnShutdown, Supervisor, WorkerError, unavailable},
};
use crate::{
    config::Resolved,
    interfaces::{archive_preflight as preflight, postgres_command},
};
use bigbrotr::{
    export::{
        destination::{LocalDestination, check_filesystem_growth},
        execution::{ExecutionCustody, ExportLimits, ExportPauseReason, ExportState},
        files::ExportFileError,
    },
    management::sessions::SessionError,
    runtime::{OwnedPermit, ResourceBudget, ResourceRequest},
    storage::{
        archive::{
            ArchiveEventError, CatalogReaderCredentials, LocalCoordinator, Preparation,
            PreparationError, ProcessError,
        },
        postgres::{
            catalog_guard::{self, CatalogCapture},
            control_owner::ArchivePinRequest,
            transport,
        },
    },
};
use bigbrotr_model::JobId;
use std::{
    cell::{Cell, RefCell},
    fs::File,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

/// The outer runtime retains this slot until its actual Control driver joins.
/// A dropped completion future must not release filesystem writer exclusion.
pub(super) type Custody = Arc<Mutex<Option<RetainedOutput>>>;
pub(super) struct RetainedOutput {
    execution: ExecutionCustody,
    _memory: Arc<OwnedPermit>,
}
const ROW_BYTES: usize = 1024 * 1024;
const OUTPUT_MEMORY: u64 = 1024 * 1024;
const SCRATCH_BYTES: u64 = 64 * 1024 * 1024;

fn sql_error(error: SessionError) -> (u8, String) {
    (
        4,
        format!("export Control operation failed: {error:?}; retain unresolved obligations"),
    )
}
fn known_refusal(error: SessionError) -> bool {
    matches!(
        error,
        SessionError::Forbidden
            | SessionError::Conflict
            | SessionError::Capacity
            | SessionError::ArchiveUnavailable
    )
}
fn pause_reason(error: SessionError) -> ExportPauseReason {
    match error {
        SessionError::Capacity => ExportPauseReason::ResourceLimit,
        _ => ExportPauseReason::ExecutionRefused,
    }
}
fn limits(config: &Resolved) -> std::result::Result<ExportLimits, SessionError> {
    let required = config
        .number("archive.worker_memory_bytes")
        .checked_add(64 * (ROW_BYTES as u64 + 2048) + 128 * 1024 * 1024 + OUTPUT_MEMORY)
        .ok_or(SessionError::Capacity)?;
    if required > config.number("export.memory.max_bytes") {
        return Err(SessionError::Capacity);
    }
    let maximum = config.number("export.artifact.max_bytes");
    let temporary = maximum.checked_mul(2).ok_or(SessionError::Capacity)?;
    if temporary
        .checked_add(SCRATCH_BYTES + OUTPUT_MEMORY)
        .is_none_or(|v| v > config.number("export.spill.max_bytes"))
    {
        return Err(SessionError::Capacity);
    }
    ExportLimits::new(maximum, temporary).map_err(|_| SessionError::Capacity)
}

/// A completed refusal advances the queue cursor. Unknown SQL/worker outcomes
/// fail-stop instead of reusing a poisoned session or starting another writer.
#[allow(clippy::too_many_arguments)]
pub(super) async fn execute(
    id: JobId,
    config: Arc<Resolved>,
    budget: ResourceBudget,
    destination: Arc<LocalDestination>,
    coordinator: &LocalCoordinator,
    retained: &Custody,
    authority: &mut Supervisor<'_>,
) -> Result<()> {
    let limits = match limits(&config) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    // Parent output/manifest/cut overlap remains reserved through known completion.
    let Ok(output_grant) = budget.try_acquire(ResourceRequest::data(OUTPUT_MEMORY, 1, 0).unwrap())
    else {
        return Ok(());
    };
    let output_grant = Arc::new(output_grant);
    let deadline = Instant::now() + Duration::from_secs(5);
    let checked_destination = destination.clone();
    let checked_config = config.clone();
    let checked = match authority
        .blocking(deadline, move |_| {
            let growth = limits
                .temporary()
                .checked_add(SCRATCH_BYTES + OUTPUT_MEMORY)
                .ok_or(WorkerError::Refused(SessionError::Capacity))?;
            let reserve = checked_config.number("runtime.disk_completion_reserve_bytes");
            checked_destination
                .check_growth(growth, reserve)
                .map_err(|_| WorkerError::Refused(SessionError::Capacity))?;
            let scratch = File::open(native_grants::required(
                &checked_config,
                "storage.scratch_root",
            )?)
            .map_err(unavailable)?;
            // Conservatively admit the complete growth on each filesystem. Resident
            // outputs already reduce free space; no resident reservation is subtracted twice.
            check_filesystem_growth(&scratch, growth, reserve)
                .map_err(|_| WorkerError::Refused(SessionError::Capacity))?;
            checked_destination
                .check_for_acceptance()
                .map_err(unavailable)
        })
        .await?
    {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    let maximum = authority.maximum()?;
    let commit = Cell::new(false);
    let reserved = tokio::select! {
        biased;
        _ = authority.stop.requested() => return Err((4,"export reservation cancelled; outcome unresolved".into())),
        result = tokio::time::timeout(maximum, authority.owner.reserve_export(id, limits, &checked, &commit)) =>
            result.map_err(|_| (4,"export reservation deadline; outcome unresolved".into()))?,
    };
    let job = match reserved {
        Ok(job) => job,
        Err(error) if known_refusal(error) => return Ok(()),
        Err(error) => return Err(sql_error(error)),
    };
    if !matches!(job.state(), ExportState::Queued | ExportState::Running)
        || job.selection().catalog() != coordinator.identity().catalog()
    {
        return Ok(());
    }
    let prepare_config = config.clone();
    let prepare_budget = budget.clone();
    let prepared = match authority
        .blocking(deadline, move |_| {
            native_grants::prepare(&prepare_config, &prepare_budget)
        })
        .await?
    {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    let Some(Ok((mut manager, connection, route))) = authority
        .external(
            deadline,
            transport::connect_pinned(prepared.manager, None),
            OnShutdown::Cancel,
        )
        .await?
    else {
        return Ok(());
    };
    let driver = tokio::spawn(
        coordinator
            .retain_connection(connection)
            .map_err(|_| (4, "export catalog driver custody refused".into()))?,
    );
    let outcome = async {
        let reader = config.text("postgres.catalog.read_user").expect("selected reader");
        let inspected = authority.external(
            deadline,
            catalog_guard::inspect_reader(&mut manager, coordinator.identity(), reader),
            OnShutdown::Cancel,
        ).await?;
        if !matches!(inspected, Some(Ok(()))) {
            return Ok(());
        }
        let ttl = deadline.saturating_duration_since(Instant::now());
        let captured = authority.external(
            deadline,
            CatalogCapture::at_snapshot(
                &mut manager, coordinator, job.selection().query().snapshot_id, ttl,
            ),
            OnShutdown::Cancel,
        ).await?;
        let capture = match captured {
            Some(Ok(value)) => value,
            _ => return Ok(()),
        };
        let pin_id = bigbrotr::runtime::ownership::fresh_process_incarnation()
            .map_err(|_| (4, "export reader identity unavailable".into()))?;
        let request = ArchivePinRequest {
            lease_id: *pin_id.as_bytes(),
            frontier_revision: job.selection().frontier_revision(),
            ttl,
        };
        let maximum = authority.maximum()?;
        let commit = Cell::new(false);
        let pin = tokio::select! {
            biased;
            _ = authority.stop.requested() => {
                return Err((4, "export reader admission cancelled; outcome unresolved".into()));
            },
            result = tokio::time::timeout(maximum,
                authority.owner.register_archive_pin(&capture, &request, &commit)) => {
                result.map_err(|_| (4, "export reader admission deadline; outcome unresolved".into()))?
                    .map_err(native_grants::pin_error)?
            },
        };
        // Every known refusal below still joins the child and releases ONLY this
        // fresh reader pin. The original export retention pin is never released here.
        let mut started_job = None;
        let work = async {
            // Both bounds start before the same SQL request. The Control copy
            // observes expiry during native work; the worker copy gates actual
            // writes without depending on Control's polling or a shared lock.
            let observer = authority.lease.begin_action(authority.lease.known_epoch())
                .map_err(|e| (4, e.to_string()))?;
            let pending = authority.lease.begin_action(authority.lease.known_epoch())
                .map_err(|e| (4, e.to_string()))?;
            let maximum = authority.maximum()?;
            let commit = Cell::new(false);
            let running = tokio::select! {
                biased;
                _ = authority.stop.requested() => {
                    return Err((4, "export start cancelled; outcome unresolved".into()));
                },
                result = tokio::time::timeout(maximum,
                    authority.owner.start_export(&job, &capture, pin.lease_id, &commit)) => {
                    result.map_err(|_| (4, "export start deadline; outcome unresolved".into()))?
                },
            };
            let running = match running {
                Ok(value) => value,
                Err(error) if known_refusal(error) => return Ok(Some(pause_reason(error))),
                Err(error) => return Err(sql_error(error)),
            };
            started_job = Some(running.job().clone());
            let observer = match observer.complete(running.remaining()) {
                Ok(value) => RefCell::new(value),
                Err(_) => return Ok(Some(ExportPauseReason::ExecutionRefused)),
            };
            let mut window = match pending.complete(running.remaining()) {
                Ok(value) => value,
                Err(_) => return Ok(Some(ExportPauseReason::ExecutionRefused)),
            };
            let slot = retained.clone();
            let retained_grant = output_grant.clone();
            let prepared_output = authority.blocking_while(
                deadline,
                || observer.borrow_mut().check().is_ok(),
                move |_| {
                    let output = running.prepare(destination).map_err(unavailable)?;
                    let mut slot = slot.lock().map_err(unavailable)?;
                    if slot.is_some() {
                        return Err(unavailable(()));
                    }
                    *slot = Some(RetainedOutput {
                        execution: output.custody(),
                        _memory: retained_grant,
                    });
                    Ok(output)
                },
            ).await?;
            let output = match prepared_output {
                Ok(value) => value,
                Err(error) => return Ok(Some(pause_reason(error))),
            };
            let custody = coordinator.reader_custody()
                .map_err(|_| (4, "export reader custody refused; retain pin".into()))?;
            let query = job.selection().query().clone();
            let worker_grant = output_grant.clone();
            let native = authority.blocking_while(
                deadline,
                || observer.borrow_mut().check().is_ok(),
                move |cancel| {
                    // The async supervisor may fail to join this worker. Its charge
                    // must outlive that future and every buffer still used here.
                    let _output_grant = worker_grant;
                    window.check().map_err(unavailable)?;
                    let route = preflight::NativeRoute::new(&config, &route, prepared.reader.tls)
                        .map_err(unavailable)?;
                    let mut limits = preflight::limits(&config).map_err(unavailable)?;
                    limits.max_records = 2;
                    limits.max_encoded_bytes = ROW_BYTES;
                    // An opaque preparation failure may already have created scratch.
                    // Fail-stop instead of multiplying unknown directories on retry.
                    let (installation, scratch, _) = preflight::worker(
                        &config, custody.identity(), None, "export-read", limits,
                        deadline.into_std(), cancel,
                    ).map_err(|_| WorkerError::OutputFailure)?;
                    if window.check().is_err() {
                        std::fs::remove_dir_all(&scratch).map_err(|_| WorkerError::OutputFailure)?;
                        return Err(unavailable(()));
                    }
                    let result = Preparation {
                        installation: &installation,
                        temp_root: &scratch,
                        limits,
                        deadline: deadline.into_std(),
                        cancelled: cancel,
                    }.query_event_bounded(
                        query, custody, &route.borrowed(),
                        CatalogReaderCredentials {
                            login: native_grants::required(&config, "postgres.catalog.read_user")?,
                            password: std::str::from_utf8(prepared.reader.password.as_bytes())
                                .map_err(unavailable)?,
                        },
                        &budget,
                        config.number("read.execution.max_scan_bytes").min(256 * 1024 * 1024),
                    );
                    if matches!(result,
                        Err(ArchiveEventError::TerminationUnknown |
                            ArchiveEventError::Preparation(PreparationError::Process(
                                ProcessError::TerminationUnknown)))) {
                        return Err(WorkerError::Unjoined);
                    }
                    std::fs::remove_dir_all(&scratch).map_err(|_| WorkerError::OutputFailure)?;
                    let observation = result.map_err(unavailable)?;
                    output.write_with_window(observation, cancel, window).map_err(|error| match error {
                        ExportFileError::Filesystem => WorkerError::OutputFailure,
                        ExportFileError::ResourceLimit => WorkerError::Refused(SessionError::Capacity),
                        _ => unavailable(error),
                    })
                },
            ).await?;
            let written = match native {
                Ok(value) => value,
                Err(error) => return Ok(Some(pause_reason(error))),
            };
            let maximum = authority.maximum()?;
            let commit = Cell::new(false);
            let completed = tokio::select! {
                biased;
                _ = authority.stop.requested() => {
                    return Err((4, "export completion cancelled; outcome unresolved".into()));
                },
                result = tokio::time::timeout(maximum,
                    authority.owner.complete_export(&written, &capture, pin.lease_id, &commit)) => {
                    result.map_err(|_| (4, "export completion deadline; outcome unresolved".into()))?
                },
            };
            match completed {
                Ok(_) => Ok(None),
                Err(error) if known_refusal(error) => Ok(Some(pause_reason(error))),
                Err(error) => Err(sql_error(error)),
            }
        }.await;
        // Unknown Control or native outcomes deliberately retain both pin and
        // output exclusion through actual driver destruction in the outer runtime.
        let refusal = work?;
        authority.lease.check_delivery(authority.lease.known_epoch())
            .map_err(|e| (4, e.to_string()))?;
        tokio::time::timeout(authority.maximum()?, authority.owner.release_archive_pin(&pin, &commit))
            .await.map_err(|_| (4, "export reader release outcome unresolved".into()))?
            .map_err(native_grants::pin_error)?;
        let cleanup = Instant::now() + duration(authority.root, "graceful_shutdown_timeout");
        let finished = authority.external(cleanup, capture.finish(), OnShutdown::Finish).await?;
        if !matches!(finished, Some(Ok(()))) {
            return Err((4, "export catalog barrier completion unresolved".into()));
        }
        if let Some(reason) = refusal {
            if authority.stopping {
                return Err((4, "export stopped before durable pause; inspect retained job".into()));
            }
            // A start or Busy refusal has no output custody and cannot assert
            // cessation of an old writer. Match the successful start exactly;
            // a slot from another attempt is never a parking witness.
            let custody = retained.lock()
                .map_err(|_| (4, "export custody poisoned".into()))?
                .as_ref()
                .filter(|held| Some(held.execution.job()) == started_job.as_ref())
                .map(|held| held.execution.clone())
                .ok_or_else(|| (4, "export refusal without matched output custody; inspect retained job".into()))?;
            let maximum = authority.maximum()?;
            let commit = Cell::new(false);
            tokio::select! {
                biased;
                _ = authority.stop.requested() => {
                    return Err((4, "export pause cancelled; outcome unresolved".into()));
                },
                result = tokio::time::timeout(maximum,
                    authority.owner.pause_export(&custody, reason, &commit)) => {
                    result.map_err(|_| (4, "export pause deadline; outcome unresolved".into()))?
                        .map_err(sql_error)?;
                },
            }
        }
        Ok(())
    }.await;
    if outcome.is_err() {
        authority.stop_admission();
    }
    drop(manager);
    postgres_command::join(driver).await;
    drop(prepared.manager_grant);
    if outcome.is_ok() {
        retained
            .lock()
            .map_err(|_| (4, "export custody poisoned".into()))?
            .take();
    }
    drop(output_grant);
    outcome
}
