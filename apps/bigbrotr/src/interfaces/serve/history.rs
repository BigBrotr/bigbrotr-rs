//! Actual private history read, with native and HTTP consumers inside one pin.
use super::{
    Result, duration,
    native_grants::{
        self, OnShutdown, ReaderInput, Supervisor, WorkerError, pin_error, required, unavailable,
    },
};
use crate::{
    config::Resolved,
    interfaces::{archive_preflight as preflight, management, postgres_command},
};
use bigbrotr::{
    management::sessions::SessionError,
    runtime::{ResourceBudget, ResourceRequest},
    storage::{
        archive::{
            ArchiveEventError, CatalogReaderCredentials, LocalCoordinator, Preparation,
            PreparationError, ProcessError, ReaderCustody,
        },
        postgres::{
            catalog_guard::{self, CatalogCapture},
            control_owner::{ArchivePinError, ArchivePinRequest},
            transport,
        },
    },
};
use bigbrotr_model::{
    CompleteThrough, ObservedTime, OwnerEpoch, PolicyEpoch, SourceCut, StreamBound, StreamId,
    read::{QueryPage, ReadGeneration, ReadMode, ReadResource, ReadViewStamp},
};
use bigbrotr_protocol::archive_event::EventQuery;
use std::{
    cell::Cell,
    io::{self, Write},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::time::Instant;

const ROW_BYTES: usize = 1024 * 1024;
const RESPONSE_BYTES: usize = 2 * 1024 * 1024;
struct BodyBuffer(Vec<u8>, usize);
impl Write for BodyBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.1.saturating_sub(self.0.len()) {
            return Err(io::Error::other("history response limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[allow(clippy::too_many_arguments)]
fn read(
    config: Arc<Resolved>,
    budget: ResourceBudget,
    reader: ReaderInput,
    route: transport::PinnedRoute,
    custody: ReaderCustody,
    query: EventQuery,
    revision: u64,
    policy: PolicyEpoch,
    observed: i64,
    page_size: u16,
    deadline: Instant,
    cancel: &AtomicBool,
) -> std::result::Result<management::HistoryBody, WorkerError> {
    let response_limit = config
        .number("read.response.max_bytes")
        .min(RESPONSE_BYTES as u64) as usize;
    let permit = budget
        .try_acquire(ResourceRequest::data(response_limit as u64, 1, 0).map_err(unavailable)?)
        .map_err(|_| WorkerError::Refused(SessionError::Capacity))?;
    let mut bytes = BodyBuffer(Vec::new(), response_limit);
    bytes
        .0
        .try_reserve_exact(response_limit)
        .map_err(|_| WorkerError::Refused(SessionError::Capacity))?;
    let catalog = custody.identity().catalog();
    let route = preflight::NativeRoute::new(&config, &route, reader.tls).map_err(unavailable)?;
    let mut limits = preflight::limits(&config).map_err(unavailable)?;
    limits.max_records = 2;
    limits.max_encoded_bytes = ROW_BYTES;
    let (installation, scratch, _) = preflight::worker(
        &config,
        custody.identity(),
        None,
        "history-read",
        limits,
        deadline.into_std(),
        cancel,
    )
    .map_err(unavailable)?;
    let result = Preparation {
        installation: &installation,
        temp_root: &scratch,
        limits,
        deadline: deadline.into_std(),
        cancelled: cancel,
    }
    .query_event_bounded(
        query,
        custody,
        &route.borrowed(),
        CatalogReaderCredentials {
            login: required(&config, "postgres.catalog.read_user")?,
            password: std::str::from_utf8(reader.password.as_bytes()).map_err(unavailable)?,
        },
        &budget,
        config
            .number("read.execution.max_scan_bytes")
            .min(256 * 1024 * 1024),
    );
    if matches!(
        result,
        Err(ArchiveEventError::TerminationUnknown
            | ArchiveEventError::Preparation(PreparationError::Process(
                ProcessError::TerminationUnknown
            )))
    ) {
        return Err(WorkerError::Unjoined);
    }
    std::fs::remove_dir_all(&scratch).map_err(unavailable)?;
    let event = result.map_err(|e| match e {
        ArchiveEventError::ResourceLimit => WorkerError::Refused(SessionError::Capacity),
        _ => unavailable(e),
    })?;
    let query = event.query();
    let bounds = query
        .bounds
        .iter()
        .map(|b| {
            Ok(StreamBound {
                stream: StreamId {
                    shard_id: b.shard_id,
                    owner_epoch: OwnerEpoch::new(b.owner_epoch).map_err(unavailable)?,
                },
                last_complete_sequence: CompleteThrough::new(b.complete_through),
            })
        })
        .collect::<std::result::Result<Vec<_>, WorkerError>>()?;
    let source_cut =
        SourceCut::new(query.dataset, query.topology_generation, bounds).map_err(unavailable)?;
    let page = QueryPage {
        version: 1,
        resource: ReadResource::EventHistory,
        items: event.event().map(|e| e.event()).into_iter().collect(),
        effective_page_size: page_size,
        next_cursor: None,
        mode: ReadMode::Snapshot,
        candidate_cut: None,
        view_stamp: ReadViewStamp {
            source_cut,
            input_generations: vec![ReadGeneration::Archive {
                catalog_id: catalog,
                frontier_revision: revision,
                snapshot_id: query.snapshot_id,
            }],
            target_generations: vec![],
            visibility_as_of_us: ObservedTime::new(observed),
            temporal_generation: None,
            expiry_deadline_us: None,
            policy_epoch: policy,
        },
        eligibility_lag_us: None,
    };
    serde_json::to_writer(&mut bytes, &page)
        .map_err(|_| WorkerError::Refused(SessionError::Capacity))?;
    Ok(management::HistoryBody {
        bytes: bytes.0,
        _event: event,
        _permit: permit,
    })
}

pub(super) async fn execute(
    work: management::HistoryWork,
    config: Arc<Resolved>,
    budget: ResourceBudget,
    coordinator: Option<&LocalCoordinator>,
    authority: &mut Supervisor<'_>,
) -> Result<()> {
    let outcome = execute_inner(work, config, budget, coordinator, authority).await;
    if let Some(observations) = authority.observations.as_mut() {
        observations.finish(super::role_status::ReadCapability::History, outcome.is_ok());
    }
    outcome
}

async fn execute_inner(
    work: management::HistoryWork,
    config: Arc<Resolved>,
    budget: ResourceBudget,
    coordinator: Option<&LocalCoordinator>,
    authority: &mut Supervisor<'_>,
) -> Result<()> {
    if work.abandoned() {
        return Ok(());
    }
    let deadline = work
        .deadline()
        .min(Instant::now() + Duration::from_micros(config.number("read.execution.timeout")));
    let pending = authority
        .lease
        .begin_action(authority.lease.known_epoch())
        .map_err(|e| (4, e.to_string()))?;
    let maximum = authority.maximum()?;
    let authorization = tokio::select! {
        biased;
        _ = authority.stop.requested() => return Err((4,"history authorization cancelled; Control unavailable".into())),
        result = work.authorize(authority.owner,maximum) => result?,
    };
    let authorization = match authorization {
        Ok(value) => value,
        Err(error) => {
            work.refuse(error).deliver();
            return Ok(());
        }
    };
    let window = match pending.complete(authorization.remaining().min(Duration::from_micros(
        config.number("read.authorization.lease_ttl"),
    ))) {
        Ok(value) => value,
        Err(_) => {
            work.refuse(SessionError::Unavailable).deliver();
            return Ok(());
        }
    };
    let action = match work.bind_action(window) {
        Ok(action) => action,
        Err(_) => {
            work.refuse(SessionError::Unavailable).deliver();
            return Ok(());
        }
    };
    let Some(coordinator) = coordinator.filter(|_| config.flag("archive.reader.enabled")) else {
        work.refuse(SessionError::ArchiveUnavailable).deliver();
        return Ok(());
    };
    if let Some(observations) = authority.observations.as_mut() {
        observations.begin(
            super::role_status::ReadCapability::History,
            Some(authorization.observed_at_us()),
        );
    }
    // The initial native profile includes its entire isolated worker allowance,
    // parent decode overlap and bounded HTTP output. The128MiB preparation
    // allowance covers the manager's80MiB+8x bounded credential input, the16MiB
    // native mTLS identity and1MiB reader-secret reservation, including overlap.
    let required_memory = config
        .number("archive.worker_memory_bytes")
        .checked_add(64 * (ROW_BYTES as u64 + 2048) + RESPONSE_BYTES as u64 + 128 * 1024 * 1024);
    if required_memory.is_none_or(|v| v > config.number("read.execution.max_memory_bytes")) {
        work.refuse(SessionError::Capacity).deliver();
        return Ok(());
    }
    if work.abandoned() || Instant::now() >= deadline {
        work.refuse(SessionError::ArchiveUnavailable).deliver();
        return Ok(());
    }
    let maximum = authority.maximum()?;
    let selected = tokio::select! {
        biased;
        _ = authority.stop.requested() => return Err((4,"history selection cancelled; Control unavailable".into())),
        result = tokio::time::timeout(maximum,authority.owner.read_current_archive_selection(coordinator.identity().catalog())) => result.map_err(|_| (4,"history selection deadline; Control unavailable".into()))?,
    };
    let selected = match selected {
        Err(ArchivePinError::ResourceLimit) => {
            work.refuse(SessionError::Capacity).deliver();
            return Ok(());
        }
        other => other.map_err(pin_error)?,
    };
    let Some(selected) = selected else {
        work.refuse(SessionError::ArchiveUnavailable).deliver();
        return Ok(());
    };
    let prepared_config = config.clone();
    let prepared_budget = budget.clone();
    let prepared = match authority
        .blocking(deadline, move |_| {
            native_grants::prepare(&prepared_config, &prepared_budget)
        })
        .await?
    {
        Ok(value) => value,
        Err(error) => {
            work.refuse(error).deliver();
            return Ok(());
        }
    };
    let connection = authority
        .external(
            deadline,
            transport::connect_pinned(prepared.manager, None),
            OnShutdown::Cancel,
        )
        .await?;
    let Some(Ok((mut manager, connection, route))) = connection else {
        work.refuse(SessionError::ArchiveUnavailable).deliver();
        return Ok(());
    };
    let driver = tokio::spawn(
        coordinator
            .retain_connection(connection)
            .map_err(|_| (4, "history catalog driver custody refused".into()))?,
    );
    let outcome = async {
        let reader = config.text("postgres.catalog.read_user").expect("selected reader");
        if !matches!(authority.external(deadline,catalog_guard::inspect_reader(&mut manager,coordinator.identity(),reader),OnShutdown::Cancel).await?,Some(Ok(()))) { work.refuse(SessionError::ArchiveUnavailable).deliver(); return Ok(()); }
        let ttl = deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(120));
        let capture = match authority.external(deadline,CatalogCapture::at_snapshot(&mut manager,coordinator,selected.snapshot_id,ttl),OnShutdown::Cancel).await? { Some(Ok(value)) => value, _ => { work.refuse(SessionError::ArchiveUnavailable).deliver(); return Ok(()); } };
        let id = bigbrotr::runtime::ownership::fresh_process_incarnation().map_err(|_| (4,"history pin identity unavailable".into()))?;
        let request = ArchivePinRequest { lease_id:*id.as_bytes(), frontier_revision:selected.frontier_revision, ttl };
        let commit = Cell::new(false);
        let maximum = authority.maximum()?;
        let pin_started = authority.lease.begin_action(authority.lease.known_epoch()).map_err(|e| (4,e.to_string()))?;
        let pin = tokio::select! {
            biased;
            _ = authority.stop.requested() => return Err((4,"history pin admission cancelled; retain unknown obligation".into())),
            result = tokio::time::timeout(maximum, authority.owner.register_archive_pin(&capture,&request,&commit)) => result.map_err(|_| (4,"history pin admission deadline; outcome unresolved".into()))?.map_err(pin_error)?,
        };
        let pin_lifetime = pin.expires_at_us.checked_sub(pin.acquired_at_us).and_then(|v| u64::try_from(v).ok()).map(Duration::from_micros).ok_or_else(|| (4,"invalid history pin lifetime; retain obligation".into()))?;
        let pin_window = pin_started.complete(pin_lifetime).ok();
        let _ = action.constrain_pin(pin_window);
        let native = if work.abandoned() || action.check().is_err() { Err(SessionError::ArchiveUnavailable) } else {
            let event_id = bigbrotr_protocol::read::validate(work.query()).map_err(|_| (4,"admitted history query changed".into()))?;
            let query = EventQuery { dataset:selected.dataset_id, event_id, scope:authorization.scope(), topology_generation:selected.topology_generation, bounds:selected.bounds, snapshot_id:selected.snapshot_id };
            let custody = coordinator.reader_custody().map_err(|_| (4,"history reader custody refused; retain pin".into()))?;
            let policy = authority.lease.known_epoch(); let observed = authorization.observed_at_us(); let page_size = work.query().page_size.expect("transport admitted page size");
            authority.blocking_while(deadline,|| !work.abandoned() && action.check().is_ok(),move |cancel| read(config,budget,prepared.reader,route,custody,query,selected.frontier_revision,policy,observed,page_size,deadline,cancel)).await?
        };
        match native {
            Ok(body) => {
                if !matches!(authority.external(deadline,capture.verify(),OnShutdown::Cancel).await?,Some(Ok(()))) {
                    drop(body);
                    work.refuse(SessionError::ArchiveUnavailable).deliver();
                } else {
                if let Some(observations) = authority.observations.as_mut() {
                    observations.observed(super::role_status::ReadCapability::History,
                        body._event.query(), selected.frontier_revision);
                }
                let mut delivery = work.deliver(body);
                let cleanup = deadline + duration(authority.root,"graceful_shutdown_timeout");
                if authority.external(cleanup,delivery.ended(),OnShutdown::Finish).await?.is_none() { return Err((4,"history delivery not joined; retain pin".into())); }
                }
            }
            Err(error) => work.refuse(error).deliver(),
        }
        authority.lease.check_delivery(authority.lease.known_epoch()).map_err(|e| (4,e.to_string()))?;
        tokio::time::timeout(authority.maximum()?,authority.owner.release_archive_pin(&pin,&Cell::new(false))).await.map_err(|_| (4,"history pin release outcome unresolved".into()))?.map_err(pin_error)?;
        let cleanup = Instant::now() + duration(authority.root,"graceful_shutdown_timeout");
        if !matches!(authority.external(cleanup,capture.finish(),OnShutdown::Finish).await?,Some(Ok(()))) { return Err((4,"history catalog rollback unresolved".into())); }
        Ok(())
    }.await;
    if outcome.is_err() {
        authority.stop_admission();
    }
    drop(manager);
    postgres_command::join(driver).await;
    drop(prepared.manager_grant);
    outcome
}
