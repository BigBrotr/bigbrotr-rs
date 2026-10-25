//! Finite catch-up from a fully terminal retained Ingest lineage.
//! The native proof and coordinator remain local through every joined effect.
use super::*;
use bigbrotr::storage::{
    archive::LocalCoordinator,
    postgres::control_owner::{
        InitialHistoryRequest, PolicySnapshot, RetainedArchiveFrontier, SuccessorHistory,
        SuccessorHistoryState, SuccessorReplayRequest, VerifiedSuccessorHistory,
    },
    rocks::{store::IngestRecovery, succession::MAX_LINEAGE_METADATA_BYTES},
};
use std::time::Duration;

mod authority;
mod capture;
mod inventory;
mod plan;
mod publication;
mod retained_driver;
use authority::Authority;

const MAX_ATTEMPTS: usize = 128;
const OPERATION: Duration = Duration::from_secs(30);

// Native closes first even when acquisition/replay/publication returns an error
// or unwinds. The supervisor retains the actual Control driver and its grant.
#[derive(Default)]
struct Custody {
    native: Option<IngestRecovery>,
    history: Option<SuccessorHistory>,
    coordinator: Option<LocalCoordinator>,
    owner: Option<ControlSession>,
    client: Option<tokio_postgres::Client>,
    frontier: Option<RetainedArchiveFrontier>,
    frontier_sender: Option<tokio::sync::oneshot::Sender<Option<RetainedArchiveFrontier>>>,
}
impl Drop for Custody {
    fn drop(&mut self) {
        drop(self.native.take());
        if let Some(sender) = self.frontier_sender.take() {
            // On send failure the named driver has already destroyed transport
            // before its receiver. Native is closed before either drop path.
            drop(sender.send(self.frontier.take()));
        }
    }
}

struct ClaimExit<'a>(Option<&'a mut Option<IngestRecovery>>);
impl Drop for ClaimExit<'_> {
    fn drop(&mut self) {
        if let Some(native) = self.0.take() {
            drop(native.take());
        }
    }
}

fn error(error: impl std::fmt::Debug) -> (u8, String) {
    (
        4,
        format!("Archive catch-up refused; preserve original state: {error:?}"),
    )
}

fn selected(root: &Resolved) -> Result<()> {
    if !root.local("control") || !root.local("ingest") || !root.local("archive") {
        return Err((
            3,
            "Archive catch-up requires local Control, Ingest and Archive".into(),
        ));
    }
    root.catalog_identity().map_err(error)?;
    root.archive_state_path().map_err(error)?;
    Ok(())
}

fn publication_profile(root: &Resolved) -> std::result::Result<u64, OwnerError> {
    selected(root).map_err(|_| OwnerError::InvalidPolicy)?;
    if !root.flag("archive.publisher.enabled")
        || root.number("archive.row_group_rows") != 64000
        || root.text("archive.compression") != Some("zstd-3")
        || root.number("archive.target_file_bytes") != 256 * MIB
        || root.number("archive.worker_memory_bytes") < 512 * MIB
        || root.number("archive.prepare_workers") == 0
    {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(root.number("archive.worker_memory_bytes"))
}

fn validate(
    root: &Resolved,
    document: &str,
    digest: [u8; 32],
) -> std::result::Result<(), OwnerError> {
    let restored = super::super::accepted_policy(root, document, digest)?;
    if publication_profile(&restored)? != root.number("archive.worker_memory_bytes") {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}

pub(in crate::interfaces::serve) fn run(
    root: Resolved,
    output: &mut LifecycleOutput,
) -> Result<()> {
    selected(&root)?;
    let budget = owner_budget(&root, false, true, 4)?;
    supervision::run_owned(root, budget, output, "Archive catch-up", false, 2, own)
}

async fn own(
    root: Resolved,
    budget: ResourceBudget,
    progress: watch::Sender<Progress>,
    cancellation: Arc<Cancellation>,
    driver_custody: supervision::DriverCustody,
) -> Result<Value> {
    let dataset: DatasetId = required(&root, "topology.dataset_id")?
        .parse()
        .map_err(error)?;
    let instance: DatasetId = required(&root, "topology.instance_id")?
        .parse()
        .map_err(error)?;
    let path = Path::new(required(&root, "storage.ingest_root")?);
    let timeout = duration(&root, "renewal_timeout");
    if cancellation.requested.load(Ordering::Acquire) {
        return Err(error("cancelled before native open"));
    }
    // Existing native custody precedes every protected credential read.
    let native = IngestRecovery::inspect(
        path,
        dataset,
        native_limits(),
        grant(&budget, 0, native_limits().native_allowance_bytes())?,
    )
    .map_err(|e| (4, e.to_string()))?;
    if !matches!(
        (
            native.manifest().identity().owner_epoch.get(),
            native.manifest().key_schema()
        ),
        (2, 2) | (3..=255, 3)
    ) {
        return Err(error(
            "requires retained schema2/epoch2 or schema3/epoch3..255",
        ));
    }
    let mut custody = Custody::default();
    custody.native = Some(native);
    let compacted = custody
        .native
        .as_ref()
        .expect("exclusive native")
        .manifest()
        .key_schema()
        == 3
        && custody
            .native
            .as_ref()
            .expect("exclusive native")
            .has_compacted_successor_ancestor(&cancellation.requested)
            .map_err(error)?;
    phase(&progress, timeout)?;
    let identity = root.catalog_identity().map_err(error)?;
    custody.coordinator = Some(
        LocalCoordinator::open(&identity, &root.archive_state_path().map_err(error)?)
            .map_err(error)?,
    );
    if compacted {
        custody.frontier = Some(
            RetainedArchiveFrontier::reserve_compacted(
                custody.coordinator.as_ref().expect("opened coordinator"),
                recovery::limits().event,
                grant(&budget, compacted_bytes()?, 0)?,
                grant(&budget, RetainedArchiveFrontier::required_data_bytes(), 0)?,
            )
            .map_err(error)?,
        );
    }
    let (prepared, control_grant) = postgres_command::prepare_control_shared(
        &root,
        required(&root, "postgres.control.user")?,
        required(&root, "postgres.control.password_ref")?,
        &budget,
    )?;
    let metadata = grant(&budget, MAX_LINEAGE_METADATA_BYTES, 0)?;
    drop(recovery::replay_grants(&budget)?);
    drop(grant(
        &budget,
        recovery::limits().required_data_bytes().map_err(error)?,
        0,
    )?);
    if compacted {
        // The permanent Check remains retained while transient canonical replay
        // is admitted. Ordinary resource minima retain the original path.
        drop(grant(&budget, compacted_bytes()?, 0)?);
    }
    phase(&progress, timeout)?;
    let (client, connection) = bounded(timeout, &cancellation, async {
        transport::connect(prepared).await.map_err(error)
    })
    .await?;
    custody.client = Some(client);
    let connection = Box::pin(
        custody
            .coordinator
            .as_ref()
            .expect("opened coordinator")
            .retain_connection(connection)
            .map_err(error)?,
    );
    let driver = if compacted {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        custody.frontier_sender = Some(sender);
        tokio::spawn(retained_driver::RetainedDriver::new(connection, receiver))
    } else {
        tokio::spawn(connection)
    };
    if let Err((driver, grant)) = driver_custody.send((driver, control_grant)) {
        drop(custody);
        postgres_command::join(driver).await;
        drop(grant);
        return Err(error("Control supervisor unavailable"));
    }
    let fresh = || super::super::fresh_process_incarnation().map_err(error);
    let claim = ClaimRequest {
        dataset,
        instance,
        authority_mode: required(&root, "config.authority_mode")?.into(),
        role_instance_id: *fresh()?.as_bytes(),
        incarnation: fresh()?,
        lease_ttl: duration(&root, "lease_ttl"),
    };
    let mut worker_bytes = None;
    let accepted: PolicySnapshot;
    phase(&progress, timeout)?;
    {
        let commit_started = Cell::new(false);
        let acquisition = ControlSession::acquire(
            custody.client.take().expect("connected client"),
            claim,
            |document, digest| {
                let restored = super::super::accepted_policy(&root, document, digest)?;
                let bytes = publication_profile(&restored)?;
                if worker_bytes.is_some_and(|prior| prior != bytes) {
                    return Err(OwnerError::InvalidPolicy);
                }
                worker_bytes = Some(bytes);
                Ok(())
            },
            &commit_started,
        );
        tokio::pin!(acquisition);
        let mut claim_exit = ClaimExit(Some(&mut custody.native));
        let (owner, policy) = bounded(timeout, &cancellation, async {
            acquisition.as_mut().await.map_err(owner_error)
        })
        .await?;
        custody.owner = Some(owner);
        accepted = policy;
        claim_exit.0 = None;
    }
    let mut runtime_root = root.clone();
    runtime_root.values.insert(
        "archive.worker_memory_bytes".into(),
        json!(worker_bytes.ok_or_else(|| error("missing accepted Archive profile"))?),
    );
    let mut authority = Authority::new(
        &runtime_root,
        &progress,
        &cancellation,
        custody.owner.as_ref().expect("claimed Control"),
        dataset,
        accepted,
    )?;
    authority
        .refresh(custody.owner.as_mut().expect("claimed Control"))
        .await?;
    custody.history = Some(if compacted {
        authority
            .operation(
                custody
                    .owner
                    .as_mut()
                    .expect("claimed Control")
                    .inspect_compacted_successor_history(
                        custody.native.as_ref().expect("exclusive native"),
                        request(&runtime_root, &cancellation)?,
                        recovery::limits().event,
                        metadata,
                        grant(&budget, compacted_bytes()?, 0)?,
                        |d, h| validate(&runtime_root, d, h),
                    ),
            )
            .await?
    } else {
        authority
            .operation(
                custody
                    .owner
                    .as_mut()
                    .expect("claimed Control")
                    .inspect_successor_history(
                        custody.native.as_ref().expect("exclusive native"),
                        request(&runtime_root, &cancellation)?,
                        metadata,
                        |d, h| validate(&runtime_root, d, h),
                    ),
            )
            .await?
    });
    let report = {
        let native = custody.native.as_ref().expect("exclusive native");
        let history = custody.history.as_ref().expect("retained history");
        if history.state() != SuccessorHistoryState::Terminal
            || history.registered_epoch() != history.native_epoch()
        {
            return Err(error("requires a fully terminal matching SQL/native head"));
        }
        let owner = custody.owner.as_mut().expect("claimed Control");
        let proof = replay(
            &runtime_root,
            &budget,
            &cancellation,
            &mut authority,
            owner,
            native,
            history,
        )
        .await?;
        publication::run(
            &runtime_root,
            &budget,
            &mut authority,
            owner,
            native,
            &proof,
            custody.coordinator.as_ref().expect("retained coordinator"),
            &mut custody.frontier,
        )
        .await?
    };
    let grace = duration(&runtime_root, "graceful_shutdown_timeout");
    phase(&progress, grace)?;
    drop(custody.native.take());
    bounded(grace, &cancellation, async {
        custody
            .owner
            .take()
            .expect("claimed Control")
            .release()
            .await
            .map_err(owner_error)
    })
    .await?;
    // The parent joins Control's actual driver before reporting completion.
    Ok(report)
}

fn compacted_bytes() -> Result<u64> {
    bigbrotr::storage::rocks::store::compacted_outcome_data_bytes(recovery::limits().event)
        .map_err(error)
}

fn request<'a>(root: &'a Resolved, cancel: &'a Cancellation) -> Result<InitialHistoryRequest<'a>> {
    Ok(InitialHistoryRequest {
        ingest_root: Path::new(required(root, "storage.ingest_root")?),
        policy_ttl: duration(root, "authorization_ttl"),
        cancelled: &cancel.requested,
    })
}

#[allow(clippy::too_many_arguments)]
async fn replay<'a>(
    root: &Resolved,
    budget: &ResourceBudget,
    cancel: &'a Cancellation,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    native: &'a IngestRecovery,
    history: &'a SuccessorHistory,
) -> Result<VerifiedSuccessorHistory<'a>> {
    let mut resolution = history
        .resolution(native, &cancel.requested)
        .map_err(error)?;
    if resolution.needs_compacted_initial() {
        authority.refresh(owner).await?;
        authority
            .operation(owner.inspect_successor_compacted_initial(
                &mut resolution,
                request(root, cancel)?,
                recovery::limits().event,
                grant(budget, compacted_bytes()?, 0)?,
                |d, h| validate(root, d, h),
            ))
            .await?;
    }
    while resolution.current_epoch().is_some() {
        while resolution.next_request().is_some() {
            authority.refresh(owner).await?;
            let (input, outcome) = recovery::replay_grants(budget)?;
            authority
                .operation(owner.inspect_next_successor_request(
                    &mut resolution,
                    SuccessorReplayRequest {
                        ingest_root: Path::new(required(root, "storage.ingest_root")?),
                        limits: recovery::limits(),
                        policy_ttl: duration(root, "authorization_ttl"),
                        cancelled: &cancel.requested,
                    },
                    input,
                    outcome,
                    |d, h| validate(root, d, h),
                ))
                .await?;
        }
        authority.refresh(owner).await?;
        authority
            .operation(owner.finish_successor_history_epoch(
                &mut resolution,
                request(root, cancel)?,
                |d, h| validate(root, d, h),
            ))
            .await?;
    }
    authority.refresh(owner).await?;
    let proof = authority
        .operation(owner.finish_successor_history(
            resolution,
            request(root, cancel)?,
            recovery::limits().event,
            grant(
                budget,
                recovery::limits().required_data_bytes().map_err(error)?,
                0,
            )?,
            |d, h| validate(root, d, h),
        ))
        .await?;
    proof.validate(&cancel.requested).map_err(error)?;
    if proof.verified_through() != history.registered_epoch() {
        return Err(error("positive history does not reach the terminal head"));
    }
    Ok(proof)
}
