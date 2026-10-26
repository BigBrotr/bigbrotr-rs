//! Coalesced ordinary Archive observation, outside request handling. The complete
//! published selection is verified without acquiring publication/source authority.
use super::{
    Result, duration,
    native_grants::{self, OnShutdown, Supervisor, WorkerError, unavailable},
};
use crate::{
    config::Resolved,
    interfaces::{archive_preflight as preflight, postgres_command},
};
use bigbrotr::{
    management::sessions::SessionError,
    runtime::{OwnedPermit, ResourceBudget, ResourceRequest},
    storage::{
        archive::{
            CatalogReaderCredentials, LocalCoordinator, ObservedCut, Preparation, PreparationError,
            ProcessError, ReaderCustody, ReceiptError, RetainedInputReader, RetainedInputs,
            WorkerBudgets,
        },
        postgres::{
            catalog_guard::{self, CatalogCapture},
            control_owner::{
                ArchivePinRequest, ArchiveReadExpectations, ArchiveReadSelection,
                BoundArchiveReadExpectations, RetainedArchiveError,
            },
            transport,
        },
    },
};
use std::{
    cell::{Cell, RefCell},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::time::Instant;

pub(super) const INTERVAL: Duration = Duration::from_secs(15);
const OPERATION: Duration = Duration::from_secs(30);
// The native verifier also scans receipts outside the selected prefix. This is
// a finite observation ceiling, not a claim that larger archives are unreadable.
const MAX_RECEIPTS: usize = 128;

struct Inputs {
    expected: BoundArchiveReadExpectations,
    retained: Vec<RetainedInputs>,
    prepared: native_grants::Prepared,
}

fn inputs(
    config: &Resolved,
    budget: &ResourceBudget,
    reader: RetainedInputReader,
    expected: ArchiveReadExpectations,
    cancel: &AtomicBool,
) -> std::result::Result<Inputs, WorkerError> {
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(expected.requests().len())
        .map_err(|_| WorkerError::Refused(SessionError::Capacity))?;
    for request in expected.requests() {
        let permit = budget
            .try_acquire(
                ResourceRequest::data(RetainedInputs::required_data_bytes(), 1, 0)
                    .map_err(unavailable)?,
            )
            .map_err(|_| WorkerError::Refused(SessionError::Capacity))?;
        retained.push(
            reader
                .read_inputs(request, permit, cancel)
                .map_err(unavailable)?
                .ok_or_else(|| unavailable(()))?,
        );
    }
    let mut references = Vec::new();
    references
        .try_reserve_exact(retained.len())
        .map_err(|_| WorkerError::Refused(SessionError::Capacity))?;
    references.extend(retained.iter());
    let expected = expected
        .bind_inputs(&references)
        .map_err(|error| match error {
            RetainedArchiveError::ResourceLimit => WorkerError::Refused(SessionError::Capacity),
            _ => unavailable(error),
        })?;
    Ok(Inputs {
        expected,
        retained,
        prepared: native_grants::prepare_export(config, budget)?,
    })
}

struct Observation {
    selection: ArchiveReadSelection,
    _observed: ObservedCut,
    _metadata: OwnedPermit,
    _inputs: Vec<RetainedInputs>,
}
enum Probe {
    Current(Box<Observation>),
    SelectionChanged,
    Unavailable,
}

struct Native {
    config: Arc<Resolved>,
    budget: ResourceBudget,
    reader: native_grants::ReaderInput,
    route: transport::PinnedRoute,
    custody: ReaderCustody,
    expected: BoundArchiveReadExpectations,
    retained: Vec<RetainedInputs>,
}
impl Native {
    fn observe(
        self,
        deadline: Instant,
        cancel: &AtomicBool,
    ) -> std::result::Result<Observation, WorkerError> {
        let (selection, receipt, read, metadata) = self.expected.into_parts();
        let facts = read
            .receipts
            .iter()
            .try_fold(0u64, |count, receipt| {
                count.checked_add(receipt.record_count)
            })
            .and_then(|count| usize::try_from(count.max(1)).ok())
            .ok_or(WorkerError::Refused(SessionError::Capacity))?;
        let mut limits = preflight::limits(&self.config).map_err(unavailable)?;
        limits.max_records = MAX_RECEIPTS;
        limits.max_encoded_bytes = 1024 * 1024;
        let route = preflight::NativeRoute::new(&self.config, &self.route, self.reader.tls)
            .map_err(unavailable)?;
        // Preflight the complete request before allocating scratch or a child.
        bigbrotr::storage::archive::validate_cut_limits(limits, &read, facts).map_err(|error| {
            match error {
                ReceiptError::ResourceLimit => WorkerError::Refused(SessionError::Capacity),
                _ => unavailable(error),
            }
        })?;
        let credentials = CatalogReaderCredentials {
            login: native_grants::required(&self.config, "postgres.catalog.read_user")?,
            password: std::str::from_utf8(self.reader.password.as_bytes()).map_err(unavailable)?,
        };
        route
            .borrowed()
            .validate_cut(&read, self.custody.identity(), credentials)
            .map_err(unavailable)?;
        // An opaque preparation failure can leave scratch. Halt rather than
        // multiply unclassified directories on every readiness tick.
        let (installation, scratch, _) = preflight::worker(
            &self.config,
            self.custody.identity(),
            None,
            "ordinary-archive-readiness",
            limits,
            deadline.into_std(),
            cancel,
        )
        .map_err(|_| WorkerError::OutputFailure)?;
        let result = Preparation {
            installation: &installation,
            temp_root: &scratch,
            limits,
            deadline: deadline.into_std(),
            cancelled: cancel,
        }
        .inspect_cut(
            read,
            self.custody,
            &route.borrowed(),
            CatalogReaderCredentials {
                login: native_grants::required(&self.config, "postgres.catalog.read_user")?,
                password: std::str::from_utf8(self.reader.password.as_bytes())
                    .map_err(unavailable)?,
            },
            WorkerBudgets::combined(&self.budget),
            facts,
        );
        if matches!(
            result,
            Err(ReceiptError::TerminationUnknown
                | ReceiptError::Preparation(PreparationError::Process(
                    ProcessError::TerminationUnknown
                )))
        ) {
            return Err(WorkerError::Unjoined);
        }
        std::fs::remove_dir_all(&scratch).map_err(|_| WorkerError::OutputFailure)?;
        let observed = result.map_err(|error| match error {
            ReceiptError::ResourceLimit => WorkerError::Refused(SessionError::Capacity),
            _ => unavailable(error),
        })?;
        if observed.observation().snapshot_id != receipt.snapshot_id
            || observed.observation().request_digest != receipt.request_digest
            || observed.observation().family_counts != receipt.family_counts
        {
            return Err(unavailable(()));
        }
        Ok(Observation {
            selection,
            _observed: observed,
            _metadata: metadata,
            _inputs: self.retained,
        })
    }
}

pub(super) async fn execute(
    config: Arc<Resolved>,
    budget: ResourceBudget,
    coordinator: &LocalCoordinator,
    authority: &mut Supervisor<'_>,
) -> Result<()> {
    if let Some(observations) = authority.observations.as_mut() {
        observations.begin_probe();
    }
    let outcome = execute_inner(config, budget, coordinator, authority).await;
    if let Some(observations) = authority.observations.as_mut() {
        if let Ok(Probe::Current(observed)) = &outcome {
            // execute_inner has already checked currentness, rolled back the
            // barrier and joined the actual catalog transport with grants held.
            observations.observed_probe(&observed.selection);
        }
        if matches!(&outcome, Ok(Probe::SelectionChanged)) {
            observations.discard_probe();
        } else {
            observations.finish_probe(matches!(&outcome, Ok(Probe::Current(_))));
        }
    }
    outcome.map(|_| ())
}

async fn execute_inner(
    config: Arc<Resolved>,
    budget: ResourceBudget,
    coordinator: &LocalCoordinator,
    authority: &mut Supervisor<'_>,
) -> Result<Probe> {
    let deadline = Instant::now() + OPERATION;
    let Ok(metadata) = budget.try_acquire(
        ResourceRequest::data(ArchiveReadExpectations::required_data_bytes(), 1, 0)
            .expect("bounded Archive expectation grant"),
    ) else {
        return Ok(Probe::Unavailable);
    };
    let maximum = authority.maximum()?;
    let expected = tokio::select! {
        biased;
        _ = authority.stop.requested() => return Err((4, "Archive readiness selection cancelled; Control unavailable".into())),
        result = tokio::time::timeout(maximum,
            authority.owner.read_current_archive_expectations(coordinator, metadata)) =>
            result.map_err(|_| (4, "Archive readiness selection deadline; Control unavailable".into()))?,
    };
    let expected = match expected {
        Ok(Some(expected)) => expected,
        Ok(None) | Err(RetainedArchiveError::ResourceLimit) => return Ok(Probe::Unavailable),
        Err(error) => {
            return Err((
                4,
                format!("Archive readiness Control metadata refused: {error}"),
            ));
        }
    };
    if let Some(observations) = authority.observations.as_mut() {
        observations.note_current(expected.selection());
    }
    let input_reader = coordinator
        .inputs_reader()
        .map_err(|_| (4, "Archive readiness input custody refused".into()))?;
    let preparation_config = config.clone();
    let preparation_budget = budget.clone();
    let inputs = match authority
        .blocking(deadline, move |cancel| {
            inputs(
                &preparation_config,
                &preparation_budget,
                input_reader,
                expected,
                cancel,
            )
        })
        .await?
    {
        Ok(inputs) => inputs,
        Err(_) => return Ok(Probe::Unavailable),
    };
    let Inputs {
        expected,
        retained,
        prepared,
    } = inputs;
    let Some(Ok((mut manager, connection, route))) = authority
        .external(
            deadline,
            transport::connect_pinned(prepared.manager, None),
            OnShutdown::Cancel,
        )
        .await?
    else {
        return Ok(Probe::Unavailable);
    };
    let driver = tokio::spawn(
        coordinator
            .retain_connection(connection)
            .map_err(|_| (4, "Archive readiness catalog custody refused".into()))?,
    );
    let mut completion_window = None;
    let outcome = async {
        let reader = config.text("postgres.catalog.read_user").expect("selected reader");
        if !matches!(authority.external(deadline,
            catalog_guard::inspect_reader(&mut manager, coordinator.identity(), reader),
            OnShutdown::Cancel).await?, Some(Ok(()))) { return Ok(Probe::Unavailable); }
        let ttl = deadline.saturating_duration_since(Instant::now());
        let captured = authority.external(deadline, CatalogCapture::at_snapshot(
            &mut manager, coordinator, expected.selection().snapshot_id,
            ttl,
        ), OnShutdown::Cancel).await?;
        let Some(Ok(capture)) = captured else { return Ok(Probe::Unavailable); };
        let id = bigbrotr::runtime::ownership::fresh_process_incarnation()
            .map_err(|_| (4, "Archive readiness pin identity unavailable".into()))?;
        let request = ArchivePinRequest {
            lease_id: *id.as_bytes(), frontier_revision: expected.selection().frontier_revision, ttl,
        };
        let maximum = authority.maximum()?;
        let commit = Cell::new(false);
        let pending = authority.lease.begin_action(authority.lease.known_epoch())
            .map_err(|e| (4, e.to_string()))?;
        let pin = tokio::select! {
            biased;
            _ = authority.stop.requested() => return Err((4, "Archive readiness pin admission cancelled; retain unknown obligation".into())),
            result = tokio::time::timeout(maximum,
                authority.owner.register_archive_pin(&capture, &request, &commit)) =>
                result.map_err(|_| (4, "Archive readiness pin admission deadline; outcome unresolved".into()))?
                    .map_err(native_grants::pin_error)?,
        };
        // The database caps this pin at the original owner lease. Later Control
        // renewal never extends it. Measure from before admission, including SQL
        // latency and suspend, just like the ordinary history delivery path.
        let lifetime = pin.expires_at_us.checked_sub(pin.acquired_at_us)
            .and_then(|n| u64::try_from(n).ok()).filter(|n| *n > 0)
            .map(Duration::from_micros)
            .ok_or_else(|| (4, "invalid Archive readiness pin lifetime; retain obligation".into()))?;
        let window = RefCell::new(pending.complete(lifetime).ok());
        let permitted = || window.borrow_mut().as_mut().is_some_and(|window| window.check().is_ok());
        let native = Native {
            config, budget, reader: prepared.reader, route,
            custody: coordinator.reader_custody()
                .map_err(|_| (4, "Archive readiness native custody refused".into()))?,
            expected, retained,
        };
        let result = authority.blocking_while(deadline, &permitted,
            move |cancel| native.observe(deadline, cancel)).await?;
        let candidate = match result {
            Ok(observed) => {
                // The selected publication may precede the physical catalog head.
                // CatalogCapture::accept is publication-only and must not relabel it.
                let verified = authority.external(deadline, capture.verify(), OnShutdown::Cancel).await?;
                if matches!(verified, Some(Ok(()))) && !authority.stopping && permitted() {
                    let maximum = authority.maximum()?.min(deadline.saturating_duration_since(Instant::now()));
                    let current = tokio::time::timeout(maximum,
                        authority.owner.read_current_archive_selection(coordinator.identity().catalog()))
                        .await.map_err(|_| (4, "Archive readiness final selection deadline; Control unavailable".into()))?
                        .map_err(native_grants::pin_error)?;
                    if let (Some(current), Some(observations)) = (&current, authority.observations.as_mut()) {
                        observations.note_current(current);
                    }
                    match current {
                        Some(current) if current != observed.selection => Probe::SelectionChanged,
                        Some(_) if permitted() => Probe::Current(Box::new(observed)),
                        Some(_) | None => Probe::Unavailable,
                    }
                } else { Probe::Unavailable }
            },
            Err(_) => Probe::Unavailable,
        };
        // The actual worker has joined. An unknown worker/Control outcome exits
        // above and deliberately retains this durable reader obligation.
        authority.lease.check_delivery(authority.lease.known_epoch())
            .map_err(|e| (4, e.to_string()))?;
        tokio::time::timeout(authority.maximum()?,
            authority.owner.release_archive_pin(&pin, &Cell::new(false)))
            .await.map_err(|_| (4, "Archive readiness pin release outcome unresolved".into()))?
            .map_err(native_grants::pin_error)?;
        let cleanup = Instant::now() + duration(authority.root, "graceful_shutdown_timeout");
        if !matches!(authority.external(cleanup, capture.finish(), OnShutdown::Finish).await?, Some(Ok(()))) {
            return Err((4, "Archive readiness catalog rollback unresolved".into()));
        }
        completion_window = window.into_inner();
        Ok(candidate)
    }.await;
    if outcome.is_err() {
        authority.stop_admission();
    }
    drop(manager);
    postgres_command::join(driver).await;
    drop(prepared.manager_grant);
    if matches!(&outcome, Ok(Probe::Current(_)))
        && !completion_window
            .as_mut()
            .is_some_and(|window| window.check().is_ok())
    {
        return Ok(Probe::Unavailable);
    }
    outcome
}
