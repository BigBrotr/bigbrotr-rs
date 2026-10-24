//! Retire one prepared empty attempt without adopting its writer incarnation.
use super::super::supervision::ClaimExit;
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{CompactedInitialEmptyRetirementPlan, InitialEmptyRetirementPlan},
    rocks::{
        store::{RetainedLineage, compacted_outcome_data_bytes},
        succession::{LINEAGE_ENTRY_BYTES, LINEAGE_SCRATCH_BYTES},
    },
};
use bigbrotr_model::Digest256;
use std::{future::Future, time::Duration};

const METADATA_BYTES: u64 = 128 * 1024;
const STRUCTURAL_BYTES: u64 = LINEAGE_SCRATCH_BYTES + 2 * LINEAGE_ENTRY_BYTES;

// On errors and unwind, close the actual native owner before grants or Control.
#[derive(Default)]
struct Custody {
    native: Option<IngestRecovery>,
    original: Option<InitialEmptyRetirementPlan>,
    compacted: Option<CompactedInitialEmptyRetirementPlan>,
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
        "empty initial retirement",
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
            "empty initial retirement cancelled before native open".into(),
        ));
    }
    let native = IngestRecovery::inspect(
        path,
        dataset,
        native_limits(),
        grant(&budget, 0, native_limits().native_allowance_bytes())?,
    )
    .map_err(error)?;
    // Empty retirement cannot complete an epoch1 preparation gap.
    if native.manifest().identity().owner_epoch.get() != 2 || native.manifest().key_schema() != 2 {
        return Err((
            4,
            "empty-attempt retirement requires native owner epoch2".into(),
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
    phase(&progress, timeout)?;
    let lineage = RetainedLineage::inspect_initial_empty_attempt(
        custody.native.as_ref().expect("native custody"),
        grant(&budget, STRUCTURAL_BYTES, 0)?,
        &cancellation.requested,
    )
    .map_err(error)?;
    if lineage.has_compacted_initial() {
        custody.lineage = Some(lineage);
    } else {
        drop(lineage);
    }
    work.preflight(custody.lineage.is_some())?;
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
        return Err((4, "empty initial retirement supervisor unavailable".into()));
    }
    let fresh = || super::super::super::fresh_process_incarnation().map_err(|e| (1, e.to_string()));
    let request = ClaimRequest {
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
            request,
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
    let report = work.complete(&mut custody).await?;
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
    // The existing supervisor joins the actual worker and driver before output.
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
    fn effects(&self) -> Result<OwnedPermit> {
        grant(
            self.budget,
            compacted_outcome_data_bytes(limits().event).map_err(error)?,
            0,
        )
    }
    fn preflight(&self, compacted: bool) -> Result<()> {
        if compacted {
            // The selector's two-entry charge is retained, then transferred to
            // the owned plan. Canonical effects overlap the final close grant.
            let _effects = self.effects()?;
            drop(grant(self.budget, METADATA_BYTES, 4096)?);
        } else {
            let _metadata = grant(self.budget, METADATA_BYTES, 0)?;
            let source = grant(self.budget, 4096, 4096)?;
            drop(replay_grants(self.budget)?);
            drop(source);
            drop(grant(self.budget, METADATA_BYTES, 4096)?);
        }
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
    async fn complete(&self, custody: &mut Custody) -> Result<Value> {
        let owner = custody.owner.as_mut().expect("claimed Control");
        let recovery = custody.native.as_mut().expect("native custody");
        let (original_closure, domains, compacted) = if let Some(lineage) = custody.lineage.take() {
            let plan = self
                .operation(owner.inspect_compacted_empty_initial_attempt(
                    recovery,
                    lineage,
                    self.request(),
                    limits().event,
                    self.effects()?,
                    |d, h| self.validate(d, h),
                ))
                .await?;
            let proof = plan.retirement();
            let original_closure = json!({
                "producer_epoch":"1", "issued_through":proof.issued_through().to_string(),
                "resolved_requests":"0", "last_complete_sequence":proof.last_complete_sequence().to_string(),
                "sha256":Digest256::from_bytes(proof.original_closure_digest())});
            let count = proof.issued_through();
            let domains = plan.allocation_domains();
            custody.compacted = Some(plan);
            (original_closure, domains, count)
        } else {
            let (plan, original_closure, domains) = self.original(owner, recovery).await?;
            custody.original = Some(plan);
            (original_closure, domains, 0)
        };
        self.operation(owner.refresh(|d, h| self.validate(d, h)))
            .await?;
        let (retired_at, slot, closure_digest) = if let Some(plan) = &custody.compacted {
            let retired_at = self
                .operation(owner.retire_compacted_empty_initial_attempt(
                    recovery,
                    plan,
                    self.request(),
                    grant(self.budget, METADATA_BYTES, 4096)?,
                    |d, h| self.validate(d, h),
                ))
                .await?;
            (
                retired_at,
                plan.retirement().producer_slot_id(),
                plan.retirement().closure_digest(),
            )
        } else {
            let plan = custody.original.as_ref().expect("original retirement plan");
            let retired_at = self
                .operation(owner.retire_empty_initial_attempt(
                    recovery,
                    plan,
                    self.request(),
                    grant(self.budget, METADATA_BYTES, 4096)?,
                    |d, h| self.validate(d, h),
                ))
                .await?;
            (
                retired_at,
                plan.retirement().producer_slot_id(),
                plan.retirement().closure_digest(),
            )
        };
        let target = recovery.manifest().identity();
        let resolved = original_closure["resolved_requests"].clone();
        let mut report = json!({"format":"bigbrotr-ingest-recovery/1",
            "state":"empty-attempt-retired", "dataset_id":target.dataset_id,
            "owner_resource_id":target.owner_resource_id,
            "producer_slot_id":slot, "producer_epoch":"2",
            "issued_through":"0", "reserved_through":"0",
            "resolved_original_requests":resolved, "original_source_closure":original_closure,
            "empty_attempt_retirement":{
                "owner_epoch":"2", "incarnation":target.process_incarnation,
                "retired_at_us":retired_at.to_string(), "sha256":Digest256::from_bytes(closure_digest),
                "allocation_domains":domains.to_string(), "native_acknowledged":"0",
                "native_retired":"0", "owner_release":"committed"},
            "native":"closed", "owner_release":"committed", "ready":false});
        if compacted != 0 {
            report["compacted_requests"] = json!(compacted.to_string());
            report["recovery_basis"] = json!("closed-producer-floor-v3");
        }
        Ok(report)
    }

    async fn original(
        &self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
    ) -> Result<(InitialEmptyRetirementPlan, Value, usize)> {
        let (root, budget, progress, cancellation, timeout) = (
            self.root,
            self.budget,
            self.progress,
            self.cancellation,
            self.timeout,
        );
        let validate = |document: &str, digest| self.validate(document, digest);
        let history = self
            .operation(owner.inspect_empty_initial_attempt(
                recovery,
                self.request(),
                grant(budget, METADATA_BYTES, 0)?,
                validate,
            ))
            .await?;
        let issued = history.issued_through();
        let domains = history.registration().allocation_domains.len();
        phase(progress, timeout)?;
        let mut prefix = history
            .source_resolution(grant(budget, 4096, 4096)?)
            .map_err(|e| (4, e.to_string()))?;
        for sequence in 1..=issued {
            phase(progress, timeout)?;
            bounded(timeout, cancellation, async {
                owner.refresh(validate).await.map_err(owner_error)
            })
            .await?;
            let (input, outcome) = replay_grants(budget)?;
            phase(progress, timeout)?;
            let result = bounded(timeout, cancellation, async {
                let spec = history.registration();
                owner
                    .inspect_empty_initial_request(
                        &history,
                        ProducerResolveRequest {
                            ingest_root: self.path,
                            registration: spec,
                            request: ProducerRequestId {
                                producer_slot_id: spec.producer_slot_id,
                                producer_epoch: ProducerEpoch::new(1).expect("original producer"),
                                request_sequence: RequestSequence::new(sequence)
                                    .expect("nonzero sequence"),
                            },
                            limits: limits(),
                            policy_ttl: duration(root, "authorization_ttl"),
                            cancelled: &cancellation.requested,
                        },
                        input,
                        outcome,
                        validate,
                    )
                    .await
                    .map_err(|e| (4, e.to_string()))
            })
            .await?;
            prefix.record(&result).map_err(|e| (4, e.to_string()))?;
            drop(result);
        }
        phase(progress, timeout)?;
        let source = prefix.finish().map_err(|e| (4, e.to_string()))?;
        let resolved = source.resolved_requests();
        let original_closure = json!({
            "producer_epoch":"1", "issued_through":issued.to_string(),
            "resolved_requests":resolved.to_string(),
            "last_complete_sequence":source.last_complete_sequence().to_string(),
            "sha256":Digest256::from_bytes(source.digest())});
        let plan = history
            .reconstruct_retirement(&source)
            .map_err(|e| (4, e.to_string()))?;
        drop(source);
        Ok((plan, original_closure, domains))
    }
}

fn error(error: impl std::fmt::Display) -> (u8, String) {
    (4, error.to_string())
}
