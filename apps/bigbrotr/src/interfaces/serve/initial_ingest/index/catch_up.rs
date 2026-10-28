//! Finite same-store recovery. Published history, retained pending work and new
//! backlog have distinct cursors; physical verification is never publication.
use super::super::{
    Cancellation, Progress, bounded, duration, grant, native_limits, owner_budget, owner_error,
    phase, recovery, supervision,
};
use super::*;
use crate::interfaces::{lifecycle_output::LifecycleOutput, postgres_command};
use bigbrotr::storage::{
    clickhouse::adapter::IndexRecoveryVerifier,
    postgres::{
        IndexPlan, IndexPublicationReceipt, IndexRecoveryState,
        control_owner::{ClaimRequest, PolicySnapshot, ProducerCloseRequest},
        ingest_registry, transport,
    },
    rocks::store::{IngestRecovery, ResolvedSource},
};
use bigbrotr_model::{DatasetId, PolicyEpoch};
use serde_json::{Value, json};
use tokio::sync::watch;

mod authority;
mod publication;
use authority::Authority;

fn error(reason: impl std::fmt::Debug) -> (u8, String) {
    (
        4,
        format!("initial Index catch-up refused; preserve retained state: {reason:?}"),
    )
}

fn validate(
    root: &Resolved,
    expected: Profile,
    document: &str,
    digest: [u8; 32],
) -> std::result::Result<(), OwnerError> {
    let restored = super::super::super::accepted_policy(root, document, digest)?;
    if profile(&restored)? != expected {
        return Err(OwnerError::InvalidPolicy);
    }
    Ok(())
}

pub(in crate::interfaces::serve) fn run(
    root: Resolved,
    output: &mut LifecycleOutput,
) -> Result<()> {
    if !root.local("control") || !root.local("ingest") || !root.local("index") {
        return Err((
            3,
            "Index catch-up requires local Control, Ingest and Index".into(),
        ));
    }
    let budget = owner_budget(&root, false, false, 8)?;
    supervision::run_owned(root, budget, output, "Index catch-up", false, 1, own)
}

// Native closes before the connected client on every error/unwind. Index's
// descriptor is also retained by the actual PG driver, not only this struct.
#[derive(Default)]
struct Custody {
    native: Option<IngestRecovery>,
    state: Option<IndexState>,
    owner: Option<ControlSession>,
    client: Option<tokio_postgres::Client>,
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
    let timeout = duration(&root, "renewal_timeout");
    if cancellation.requested.load(Ordering::Acquire) {
        return Err(error("cancelled before native custody"));
    }
    let mut custody = Custody {
        native: Some(
            IngestRecovery::inspect(
                Path::new(required(&root, "storage.ingest_root")?),
                dataset,
                native_limits(),
                grant(&budget, 0, native_limits().native_allowance_bytes())?,
            )
            .map_err(error)?,
        ),
        ..Custody::default()
    };
    let manifest = *custody.native.as_ref().unwrap().manifest();
    require_initial_manifest(&manifest)?;
    // Both actual stores are held before reading any credential. No fresh
    // journal or table identity is generated on this path.
    custody.state = Some(
        IndexState::recover(Path::new(required(&root, "storage.index_root")?)).map_err(error)?,
    );
    let state = custody.state.as_ref().unwrap();
    if state.binding().dataset() != dataset
        || state.tables().database() != required(&root, "clickhouse.database")?
    {
        return Err(error("retained Index deployment identity differs"));
    }
    for key in root
        .selected_storage_roots()
        .filter(|key| *key != "storage.index_root")
    {
        let other = Path::new(required(&root, key)?)
            .canonicalize()
            .map_err(error)?;
        LocalCoordinator::require_disjoint(
            Path::new(required(&root, "storage.index_root")?),
            &other,
        )
        .map_err(error)?;
    }
    let index_driver = IndexDriverCustody::new();
    index_driver.attach(state.custody()).map_err(error)?;
    let _metadata = grant(&budget, METADATA_BYTES, 0)?;
    let prepared_index = prepare::prepare(&root, &budget, false)?;
    phase(&progress, timeout)?;
    let (prepared, control_grant) = postgres_command::prepare_control_shared(
        &root,
        required(&root, "postgres.control.user")?,
        required(&root, "postgres.control.password_ref")?,
        &budget,
    )?;
    // Replay grants coexist with the retained closing proof, stores and both
    // connection preparations. Index has a separate complete-operation check
    // against these same real holders in its first accepted-policy callback.
    {
        let closing = grant(&budget, 4096, 4096)?;
        drop(recovery::replay_grants(&budget)?);
        drop(closing);
    }
    phase(&progress, timeout)?;
    let (client, connection) = bounded(timeout, &cancellation, async {
        transport::connect(prepared).await.map_err(error)
    })
    .await?;
    custody.client = Some(client);
    let driver = tokio::spawn(index_driver.retain_connection(connection));
    if let Err((driver, permit)) = driver_custody.send((driver, control_grant)) {
        drop(custody);
        postgres_command::join(driver).await;
        drop(permit);
        return Err(error("Control supervisor unavailable"));
    }
    phase(&progress, timeout)?;
    let spec = bounded(timeout, &cancellation, async {
        ingest_registry::load_historical(
            custody.client.as_mut().unwrap(),
            *instance.as_bytes(),
            custody.native.as_ref().unwrap(),
        )
        .await
        .map_err(error)
    })
    .await?;
    validate_source(&manifest, &spec)?;
    let fresh = || super::super::super::fresh_process_incarnation().map_err(error);
    let claim = ClaimRequest {
        dataset,
        instance,
        authority_mode: required(&root, "config.authority_mode")?.into(),
        role_instance_id: *fresh()?.as_bytes(),
        incarnation: fresh()?,
        lease_ttl: duration(&root, "lease_ttl"),
    };
    let mut accepted_profile = None;
    let accepted: PolicySnapshot;
    phase(&progress, timeout)?;
    {
        let commit = Cell::new(false);
        let acquisition = ControlSession::acquire(
            custody.client.take().unwrap(),
            claim,
            |document, digest| {
                let restored = super::super::super::accepted_policy(&root, document, digest)?;
                let candidate = profile(&restored)?;
                if candidate.provenance != state.binding().provenance()
                    || accepted_profile.is_some_and(|prior| prior != candidate)
                {
                    return Err(OwnerError::InvalidPolicy);
                }
                if accepted_profile.is_none() {
                    preflight(candidate, &budget).map_err(|_| OwnerError::InvalidPolicy)?;
                }
                accepted_profile = Some(candidate);
                Ok(())
            },
            &commit,
        );
        tokio::pin!(acquisition);
        let mut claim_exit = supervision::ClaimExit(Some(&mut custody.native));
        let (owner, policy) = bounded(timeout, &cancellation, async {
            acquisition.as_mut().await.map_err(owner_error)
        })
        .await?;
        custody.owner = Some(owner);
        accepted = policy;
        claim_exit.0 = None;
    }
    let profile = accepted_profile.ok_or_else(|| error("accepted Index policy missing"))?;
    let owner = custody.owner.as_mut().unwrap();
    let mut authority = Authority::new(&root, &progress, &cancellation, owner, accepted, profile)?;
    authority.refresh(owner, None, state).await?;
    let closed = authority
        .operation(owner.observe_initial_retirement(
            custody.native.as_ref().unwrap(),
            request(&root, &cancellation, &spec)?,
            grant(&budget, 4096, 4096)?,
            |d, h| validate(&root, profile, d, h),
        ))
        .await?;
    let reserved = closed.reserved_through();
    let mut resolution = closed
        .source_resolution(custody.native.as_ref().unwrap())
        .map_err(error)?;
    recovery::replay::originals(
        &root,
        &budget,
        &progress,
        &cancellation,
        owner,
        custody.native.as_ref().unwrap(),
        &spec,
        reserved,
        Some(recovery::replay::Prefix::Source(&mut resolution)),
        recovery::replay::Mode::InspectRetirement,
        |d, h| validate(&root, profile, d, h),
    )
    .await?;
    authority.refresh(owner, None, state).await?;
    let source = authority.local(|_| resolution.finish().map_err(error))?;
    authority
        .operation(owner.inspect_closed_initial_source(
            &source,
            request(&root, &cancellation, &spec)?,
            grant(&budget, 64 * 1024, 0)?,
            |d, h| validate(&root, profile, d, h),
        ))
        .await?;
    let report = publication::run(
        &budget,
        &prepared_index,
        &mut authority,
        owner,
        state,
        &source,
        manifest,
        profile,
    )
    .await?;
    drop(source);
    drop(prepared_index);
    let grace = duration(&root, "graceful_shutdown_timeout");
    phase(&progress, grace)?;
    drop(custody.native.take());
    bounded(grace, &cancellation, async {
        custody
            .owner
            .take()
            .unwrap()
            .release()
            .await
            .map_err(owner_error)
    })
    .await?;
    // run_owned joins the actual PG driver before emitting this terminal value.
    Ok(report)
}

fn require_initial_manifest(manifest: &IngestManifest) -> Result<()> {
    if manifest.key_schema() != 1
        || manifest.identity().owner_epoch.get() != 1
        || manifest.identity().topology_generation.get() != 0
    {
        return Err(error(
            "requires the original topology0/schema1/epoch1 source",
        ));
    }
    Ok(())
}

fn request<'a>(
    root: &'a Resolved,
    cancel: &'a Cancellation,
    spec: &'a InitialRegistration,
) -> Result<ProducerCloseRequest<'a>> {
    Ok(ProducerCloseRequest {
        ingest_root: Path::new(required(root, "storage.ingest_root")?),
        registration: spec,
        policy_ttl: duration(root, "authorization_ttl"),
        cancelled: &cancel.requested,
    })
}

fn preflight(profile: Profile, budget: &ResourceBudget) -> Result<()> {
    let physical = profile.physical()?;
    let bytes = physical
        .required_local_data_bytes()
        .map_err(error)?
        .checked_add(profile.source().required_data_bytes().map_err(error)?)
        .and_then(|n| n.checked_add(plan::required_data_bytes(profile.source()).ok()?))
        .and_then(|n| n.checked_add(required_recovery_metadata_bytes()))
        .and_then(|n| n.checked_add(64 * 1024 + 4096))
        .ok_or_else(|| error("recovery reservation overflow"))?;
    let request = ResourceRequest::data(
        bytes,
        6,
        physical
            .required_local_native_bytes()
            .checked_add(4096)
            .ok_or_else(|| error("native allowance overflow"))?,
    )
    .map_err(error)?;
    drop(budget.try_acquire(request).map_err(|_| {
        (
            3,
            "Index recovery cannot coexist with retained owners".into(),
        )
    })?);
    Ok(())
}

#[cfg(test)]
mod tests;
