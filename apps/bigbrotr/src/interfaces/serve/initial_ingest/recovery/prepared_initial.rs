//! Complete an existing initial preparation without adopting its attempted writer.
use super::super::supervision::ClaimExit;
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::InitialGapPlan,
    rocks::{
        store::{
            COMPACTED_HANDOFF_DATA_BYTES, COMPACTED_PREFIX_METADATA_BYTES, InitialHandoff,
            RetainedCompactedPrefix, RetainedLineage, compacted_outcome_data_bytes,
        },
        succession::{LINEAGE_ENTRY_BYTES, LINEAGE_SCRATCH_BYTES},
    },
};
use bigbrotr_model::Digest256;
use sha2::{Digest, Sha256};
use std::future::Future;

const METADATA_BYTES: u64 = 128 * 1024;
const STRUCTURAL_BYTES: u64 = LINEAGE_SCRATCH_BYTES + 2 * LINEAGE_ENTRY_BYTES;

// Field order closes the actual native owner before releasing proof grants or
// Control, including errors and unwinding. Borrowed operations cannot move it.
#[derive(Default)]
struct Custody {
    native: Option<IngestRecovery>,
    plan: Option<InitialGapPlan>,
    prefix: Option<RetainedCompactedPrefix>,
    lineage: Option<RetainedLineage>,
    owner: Option<ControlSession>,
    client: Option<tokio_postgres::Client>,
}

pub(super) fn run(root: Resolved, output: &mut LifecycleOutput) -> Result<()> {
    let budget = owner_budget(&root, false, false, 2)?;
    supervision::run_owned(
        root,
        budget,
        output,
        "initial preparation recovery",
        false,
        1,
        own,
    )
}

async fn own(
    root: Resolved,
    budget: ResourceBudget,
    progress: watch::Sender<Progress>,
    cancellation: Arc<Cancellation>,
    driver_custody: supervision::DriverCustody,
) -> Result<Value> {
    let path = Path::new(required(&root, "storage.ingest_root")?);
    let dataset: DatasetId = required(&root, "topology.dataset_id")?
        .parse()
        .map_err(|_| (3, "invalid dataset".into()))?;
    let instance: DatasetId = required(&root, "topology.instance_id")?
        .parse()
        .map_err(|_| (3, "invalid instance".into()))?;
    let timeout = duration(&root, "renewal_timeout");
    if cancellation.requested.load(Ordering::Acquire) {
        return Err((
            4,
            "initial preparation recovery cancelled before native open".into(),
        ));
    }
    let native = IngestRecovery::inspect(
        path,
        dataset,
        native_limits(),
        grant(&budget, 0, native_limits().native_allowance_bytes())?,
    )
    .map_err(error)?;
    let from_epoch = native.manifest().identity().owner_epoch.get();
    if !matches!(
        (from_epoch, native.manifest().key_schema()),
        (1, 1) | (2, 2)
    ) {
        return Err((
            4,
            "initial preparation recovery requires native epoch1 or epoch2".into(),
        ));
    }
    let mut custody = Custody {
        native: Some(native),
        ..Custody::default()
    };
    let work = Work {
        root: &root,
        path,
        budget: &budget,
        progress: &progress,
        cancellation: &cancellation,
        timeout,
    };
    phase(&progress, timeout)?;
    let (prepared, control_grant) = postgres_command::prepare_control_shared(
        &root,
        required(&root, "postgres.control.user")?,
        required(&root, "postgres.control.password_ref")?,
        &budget,
    )?;
    work.preflight()?;
    // A native2 structural selection is independent of SQL. Original readers
    // remain strict; no failed compacted proof falls back to original replay.
    if from_epoch == 2 {
        phase(&progress, timeout)?;
        let lineage = RetainedLineage::inspect_initial_preparation(
            custody.native.as_ref().expect("native custody"),
            grant(&budget, STRUCTURAL_BYTES, 0)?,
            &cancellation.requested,
        )
        .map_err(error)?;
        if lineage.has_compacted_initial() {
            custody.lineage = Some(lineage);
        }
    }
    phase(&progress, timeout)?;
    let (client, connection) = bounded(timeout, &cancellation, async {
        transport::connect(prepared)
            .await
            .map_err(|e| (1, e.to_string()))
    })
    .await?;
    custody.client = Some(client);
    let driver = tokio::spawn(connection);
    if let Err((driver, grant)) = driver_custody.send((driver, control_grant)) {
        drop(custody);
        postgres_command::join(driver).await;
        drop(grant);
        return Err((4, "initial preparation supervisor unavailable".into()));
    }
    if from_epoch == 1 {
        let _metadata = grant(&budget, METADATA_BYTES, 0)?;
        let native = custody.native.as_ref().expect("native custody");
        let spec = work
            .operation(ingest_registry::load_historical(
                custody.client.as_mut().expect("connected client"),
                *instance.as_bytes(),
                native,
            ))
            .await?;
        if native
            .closed_producer_floor(spec.producer_slot_id)
            .map_err(error)?
            .is_some()
        {
            phase(&progress, timeout)?;
            custody.prefix = Some(
                RetainedCompactedPrefix::inspect_initial(
                    native,
                    spec.producer_slot_id,
                    limits().event,
                    grant(&budget, COMPACTED_PREFIX_METADATA_BYTES, 0)?,
                    work.effect_grant()?,
                    &cancellation.requested,
                )
                .map_err(error)?,
            );
        }
    }
    let compacted = custody.prefix.is_some() || custody.lineage.is_some();
    let fresh = || super::super::super::fresh_process_incarnation().map_err(|e| (1, e.to_string()));
    let claim = ClaimRequest {
        dataset,
        instance,
        authority_mode: required(&root, "config.authority_mode")?.into(),
        role_instance_id: *fresh()?.as_bytes(),
        incarnation: fresh()?,
        lease_ttl: duration(&root, "lease_ttl"),
    };
    phase(&progress, timeout)?;
    {
        let commit = Cell::new(false);
        let acquisition = ControlSession::acquire(
            custody.client.take().expect("connected client"),
            claim,
            |d, h| work.validate(d, h),
            &commit,
        );
        tokio::pin!(acquisition);
        let mut guard = ClaimExit(Some(&mut custody.native));
        let (owner, _) = bounded(timeout, &cancellation, async {
            acquisition.as_mut().await.map_err(owner_error)
        })
        .await?;
        custody.owner = Some(owner);
        guard.0 = None;
    }
    let report = work.complete(&mut custody, from_epoch, compacted).await?;
    let grace = duration(&root, "graceful_shutdown_timeout");
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
    // The outer supervisor joins the worker and actual driver before output.
    Ok(report)
}

struct Work<'a> {
    root: &'a Resolved,
    path: &'a Path,
    budget: &'a ResourceBudget,
    progress: &'a watch::Sender<Progress>,
    cancellation: &'a Cancellation,
    timeout: Duration,
}
impl Work<'_> {
    fn request(&self) -> InitialHistoryRequest<'_> {
        InitialHistoryRequest {
            ingest_root: self.path,
            policy_ttl: duration(self.root, "authorization_ttl"),
            cancelled: &self.cancellation.requested,
        }
    }
    fn validate(&self, document: &str, digest: [u8; 32]) -> std::result::Result<(), OwnerError> {
        super::super::super::accepted_policy(self.root, document, digest).map(|_| ())
    }
    fn effect_grant(&self) -> Result<OwnedPermit> {
        grant(
            self.budget,
            compacted_outcome_data_bytes(limits().event).map_err(error)?,
            0,
        )
    }
    fn preflight(&self) -> Result<()> {
        // These phases are serial on this sole worker. Charge each overlap
        // before a claim, without retaining vectors of decoded requests.
        let metadata = grant(self.budget, METADATA_BYTES, 0)?;
        let structural = grant(self.budget, STRUCTURAL_BYTES, 0)?;
        drop(self.effect_grant()?);
        drop(structural);
        let source = grant(self.budget, 4096, 4096)?;
        drop(replay_grants(self.budget)?);
        drop(source);
        let dispatch = grant(self.budget, COMPACTED_HANDOFF_DATA_BYTES, 8192)?;
        drop(self.effect_grant()?);
        drop(dispatch);
        drop(metadata);
        Ok(())
    }
    async fn operation<T, E: std::fmt::Display>(
        &self,
        future: impl Future<Output = std::result::Result<T, E>>,
    ) -> Result<T> {
        phase(self.progress, self.timeout)?;
        bounded(self.timeout, self.cancellation, async {
            future.await.map_err(error)
        })
        .await
    }
    async fn complete(
        &self,
        custody: &mut Custody,
        from_epoch: u64,
        compacted: bool,
    ) -> Result<Value> {
        let owner = custody.owner.as_mut().expect("claimed Control");
        let native = custody.native.as_mut().expect("native custody");
        if compacted {
            let metadata = grant(self.budget, METADATA_BYTES, 0)?;
            let effects = self.effect_grant()?;
            let plan = if let Some(prefix) = &custody.prefix {
                self.operation(owner.inspect_prepared_compacted_initial_gap(
                    native,
                    prefix,
                    self.request(),
                    limits().event,
                    metadata,
                    effects,
                    |d, h| self.validate(d, h),
                ))
                .await?
            } else {
                self.operation(
                    owner.inspect_prepared_compacted_initial_history(
                        native,
                        custody
                            .lineage
                            .as_ref()
                            .expect("compacted prepared lineage"),
                        self.request(),
                        limits().event,
                        metadata,
                        effects,
                        |d, h| self.validate(d, h),
                    ),
                )
                .await?
            };
            custody.plan = Some(plan);
            drop(custody.prefix.take());
            drop(custody.lineage.take());
        } else if from_epoch == 1 {
            let gap = self
                .operation(owner.inspect_prepared_initial_gap(
                    native,
                    self.request(),
                    grant(self.budget, METADATA_BYTES, 0)?,
                    |d, h| self.validate(d, h),
                ))
                .await?;
            let closed = self
                .operation(owner.observe_initial_retirement(
                    native,
                    ProducerCloseRequest {
                        ingest_root: self.path,
                        registration: gap.registration(),
                        policy_ttl: duration(self.root, "authorization_ttl"),
                        cancelled: &self.cancellation.requested,
                    },
                    grant(self.budget, 4096, 4096)?,
                    |d, h| self.validate(d, h),
                ))
                .await?;
            let mut prefix = closed.source_resolution(native).map_err(error)?;
            replay::originals(
                self.root,
                self.budget,
                self.progress,
                self.cancellation,
                owner,
                native,
                gap.registration(),
                gap.issued_through(),
                Some(replay::Prefix::Source(&mut prefix)),
                replay::Mode::InspectRetirement,
                |d, h| self.validate(d, h),
            )
            .await?;
            let source = prefix.finish().map_err(error)?;
            custody.plan = Some(gap.reconstruct_handoff(&source).map_err(error)?);
            drop(source);
        } else {
            let history = self
                .operation(owner.inspect_prepared_initial_history(
                    native,
                    self.request(),
                    grant(self.budget, METADATA_BYTES, 0)?,
                    |d, h| self.validate(d, h),
                ))
                .await?;
            let mut prefix = history
                .source_resolution(grant(self.budget, 4096, 4096)?)
                .map_err(error)?;
            replay::originals(
                self.root,
                self.budget,
                self.progress,
                self.cancellation,
                owner,
                native,
                history.registration(),
                history.issued_through(),
                Some(replay::Prefix::Source(&mut prefix)),
                replay::Mode::InspectPrepared(&history),
                |d, h| self.validate(d, h),
            )
            .await?;
            let source = prefix.finish().map_err(error)?;
            let handoff = history.reconstruct_handoff(&source).map_err(error)?;
            return Ok(report(&handoff, from_epoch, false));
        }
        // The retained plan survives all fallible dispatch/teardown boundaries.
        self.operation(owner.refresh(|d, h| self.validate(d, h)))
            .await?;
        let plan = custody.plan.as_ref().expect("positive preparation plan");
        let observed = if compacted {
            self.operation(owner.complete_prepared_compacted_initial_gap(
                native,
                plan,
                self.request(),
                limits().event,
                grant(self.budget, COMPACTED_HANDOFF_DATA_BYTES, 8192)?,
                self.effect_grant()?,
                |d, h| self.validate(d, h),
            ))
            .await?
        } else {
            self.operation(owner.complete_prepared_initial_gap(
                native,
                plan,
                self.request(),
                grant(self.budget, 8192, 8192)?,
                |d, h| self.validate(d, h),
            ))
            .await?
        };
        Ok(report(observed.handoff(), from_epoch, compacted))
    }
}

fn report(handoff: &InitialHandoff, from_epoch: u64, compacted: bool) -> Value {
    let target = handoff.next_manifest().identity();
    let count = handoff.issued_through();
    json!({"format":"bigbrotr-ingest-recovery/1", "state":"initial-preparation-completed",
        "dataset_id":target.dataset_id, "owner_resource_id":target.owner_resource_id,
        "producer_slot_id":handoff.producer_slot_id(), "producer_epoch":"2",
        "from_owner_epoch":from_epoch.to_string(), "issued_through":"0", "reserved_through":"0",
        "resolved_original_requests":if compacted {0}else{count}.to_string(),
        "compacted_requests":if compacted {count}else{0}.to_string(),
        "recovery_basis":if compacted {"closed-producer-floor-v3"}else{"original-inputs"},
        "original_source_closure":{"producer_epoch":"1", "issued_through":count.to_string(),
            "last_complete_sequence":handoff.last_complete_sequence().to_string(),
            "sha256":Digest256::from_bytes(handoff.closure_digest())},
        "prepared_target":{"owner_epoch":"2", "incarnation":target.process_incarnation,
            "handoff_sha256":Digest256::from_bytes(Sha256::digest(handoff.encode().as_bytes()).into())},
        "native":"closed", "owner_release":"committed", "ready":false})
}

fn error(error: impl std::fmt::Display) -> (u8, String) {
    (4, error.to_string())
}
