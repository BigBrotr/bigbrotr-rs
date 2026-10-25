//! Finite recovery of a retained epoch3-or-later lifetime. Every mode retains
//! the exclusive native handle and its opaque proof owner through teardown;
//! none returns a writer, adopts an incarnation or resolves import obligations.
use super::super::supervision::ClaimExit;
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{
        ActivatedSuccessorRecovery, ActivatedSuccessorRetirement, ActivatedSuccessorSourceRecovery,
        InitialHistoryRequest, SuccessorEmptyPlan, SuccessorGapPlan, SuccessorHistory,
        SuccessorReplayRequest,
    },
    rocks::{store::LineageEpochSummary, succession::MAX_LINEAGE_METADATA_BYTES},
};
use bigbrotr_model::ProducerSlotId;
use std::time::Duration;

mod prepared;
mod retirement;
mod source;

const FINAL_JOB_BYTES: u64 = 128 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Resolve,
    CloseSource,
    AbandonAcquisition,
    Retire,
    RetireEmpty,
}

// Field order also protects unwinding: close native before releasing retained
// proof grants or dropping Control. Only the selected consumer's proof owners are used.
// After acquisition, bounded operations borrow Control in this custody. The
// acquisition future owns its client and has a separate native-first guard.
#[derive(Default)]
struct Custody {
    native: Option<IngestRecovery>,
    gap: Option<SuccessorGapPlan>,
    empty: Option<SuccessorEmptyPlan>,
    positive: Option<ActivatedSuccessorRecovery>,
    source: Option<ActivatedSuccessorSourceRecovery>,
    retirement: Option<ActivatedSuccessorRetirement>,
    history: Option<SuccessorHistory>,
    owner: Option<ControlSession>,
    client: Option<tokio_postgres::Client>,
}

pub(super) fn run(root: Resolved, mode: Mode, output: &mut LifecycleOutput) -> Result<()> {
    let budget = owner_budget(&root, false, false, 2)?;
    supervision::run_owned(
        root,
        budget,
        output,
        "successor Ingest recovery",
        false,
        1,
        move |root, budget, progress, cancellation, driver| {
            own(root, budget, progress, cancellation, driver, mode)
        },
    )
}

async fn own(
    root: Resolved,
    budget: ResourceBudget,
    progress: watch::Sender<Progress>,
    cancellation: Arc<Cancellation>,
    driver_custody: supervision::DriverCustody,
    mode: Mode,
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
            "successor Ingest recovery cancelled before native open".into(),
        ));
    }
    let native = IngestRecovery::inspect(
        path,
        dataset,
        native_limits(),
        grant(&budget, 0, native_limits().native_allowance_bytes())?,
    )
    .map_err(error)?;
    let shape = (
        native.manifest().identity().owner_epoch.get(),
        native.manifest().key_schema(),
    );
    if !matches!(shape, (3..=255, 3)) && !(mode == Mode::RetireEmpty && shape == (2, 2)) {
        return Err((
            4,
            if mode == Mode::RetireEmpty {
                "empty successor recovery requires schema2/epoch2 or schema3/epoch3..255"
            } else {
                "successor recovery requires native schema3 and epoch3..255"
            }
            .into(),
        ));
    }
    let mut custody = Custody {
        native: Some(native),
        ..Custody::default()
    };
    let compacted_replay = matches!(
        mode,
        Mode::Resolve | Mode::CloseSource | Mode::AbandonAcquisition | Mode::Retire
    ) && custody
        .native
        .as_ref()
        .expect("exclusive native owner")
        .has_compacted_successor_ancestor(&cancellation.requested)
        .map_err(error)?;
    phase(&progress, timeout)?;
    let (prepared, control_grant) = postgres_command::prepare_control_shared(
        &root,
        required(&root, "postgres.control.user")?,
        required(&root, "postgres.control.password_ref")?,
        &budget,
    )?;
    // One lineage allocation includes its fixed PG/native scratch. Retain the
    // mutation grant alongside it; input/outcome replay and the final effect
    // census are serial. Admit each actual overlap before a claim or barrier.
    let metadata = grant(&budget, MAX_LINEAGE_METADATA_BYTES, 0)?;
    let mutation_bytes = if mode == Mode::RetireEmpty {
        8192
    } else {
        4096
    };
    let closing = grant(&budget, mutation_bytes, mutation_bytes)?;
    // Selected compacted recovery retains its canonical grant across every
    // replay step and through final ACK. Preflight those actual overlaps.
    let retained_compacted = compacted_replay
        .then(|| compacted_grant(&budget))
        .transpose()?;
    drop(replay_grants(&budget)?);
    drop(effect_grant(&budget)?);
    if mode == Mode::RetireEmpty {
        drop(compacted_grant(&budget)?);
    }
    if mode != Mode::RetireEmpty {
        let ack_effects = compacted_replay
            .then(|| compacted_grant(&budget))
            .transpose()?;
        drop(grant(
            &budget,
            if compacted_replay {
                RecoveryAcknowledgement::COMPACTED_REQUIRED_DATA_BYTES
            } else {
                RecoveryAcknowledgement::REQUIRED_DATA_BYTES
            },
            RecoveryAcknowledgement::REQUIRED_NATIVE_BYTES,
        )?);
        drop(ack_effects);
    }
    if matches!(mode, Mode::AbandonAcquisition | Mode::Retire) {
        // Abandonment and retirement retain Check through terminal job writes.
        // The compacted ACK overlap above already dominates this grant; admit
        // the actual job overlap explicitly rather than rely on equal constants.
        drop(grant(&budget, FINAL_JOB_BYTES, 0)?);
    }
    drop(retained_compacted);
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
        return Err((4, "successor recovery supervisor unavailable".into()));
    }
    let fresh = || super::super::super::fresh_process_incarnation().map_err(|e| (1, e.to_string()));
    let claim = ClaimRequest {
        dataset,
        instance,
        authority_mode: required(&root, "config.authority_mode")?.into(),
        role_instance_id: *fresh()?.as_bytes(),
        incarnation: fresh()?,
        lease_ttl: duration(&root, "lease_ttl"),
    };
    let work = Work {
        root: &root,
        path,
        budget: &budget,
        progress: &progress,
        cancellation: &cancellation,
        timeout,
    };
    // A phase refusal leaves the client in Custody. Once transferred, retain
    // the acquisition future outside bounded: dropping its borrowed Pin on
    // timeout must not destroy the client before native has closed.
    phase(&progress, timeout)?;
    {
        let commit_started = Cell::new(false);
        let acquisition = ControlSession::acquire(
            custody.client.take().expect("connected client"),
            claim,
            |d, h| work.validate(d, h),
            &commit_started,
        );
        tokio::pin!(acquisition);
        let mut claim_exit = ClaimExit(Some(&mut custody.native));
        let (owner, _) = bounded(timeout, &cancellation, async {
            acquisition.as_mut().await.map_err(owner_error)
        })
        .await?;
        // Infallibly move the delivered session into native-first custody
        // before disarming the acquisition guard.
        custody.owner = Some(owner);
        claim_exit.0 = None;
    }
    let report = match mode {
        Mode::Resolve => {
            work.resolve(&mut custody, metadata, closing, compacted_replay)
                .await?
        }
        Mode::CloseSource | Mode::AbandonAcquisition => {
            work.source(&mut custody, metadata, closing, mode, compacted_replay)
                .await?
        }
        Mode::Retire => {
            work.retire(&mut custody, metadata, closing, compacted_replay)
                .await?
        }
        Mode::RetireEmpty => work.retire_empty(&mut custody, metadata, closing).await?,
    };
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
    // The parent joins the actual driver before publishing this report.
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
    fn replay_request(&self) -> SuccessorReplayRequest<'_> {
        SuccessorReplayRequest {
            ingest_root: self.path,
            limits: limits(),
            policy_ttl: duration(self.root, "authorization_ttl"),
            cancelled: &self.cancellation.requested,
        }
    }
    fn validate(&self, document: &str, digest: [u8; 32]) -> std::result::Result<(), OwnerError> {
        super::super::super::accepted_policy(self.root, document, digest).map(|_| ())
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
    async fn refresh(&self, owner: &mut ControlSession) -> Result<()> {
        phase(self.progress, self.timeout)?;
        bounded(self.timeout, self.cancellation, async {
            owner
                .refresh(|d, h| self.validate(d, h))
                .await
                .map_err(owner_error)
        })
        .await?;
        Ok(())
    }
    fn input_grant(&self) -> Result<OwnedPermit> {
        grant(
            self.budget,
            RetainedRequestInput::required_data_bytes(limits().input).map_err(|e| {
                (
                    3,
                    format!("Ingest recovery input reservation refused: {e:?}"),
                )
            })?,
            0,
        )
    }

    async fn resolve(
        &self,
        custody: &mut Custody,
        metadata: OwnedPermit,
        closing: OwnedPermit,
        compacted: bool,
    ) -> Result<Value> {
        let native = custody.native.as_mut().expect("exclusive native owner");
        let owner = custody.owner.as_mut().expect("claimed Control");
        custody.positive = Some(if compacted {
            self.operation(owner.inspect_activated_successor_producer_with_compacted(
                native,
                self.replay_request(),
                metadata,
                self.input_grant()?,
                compacted_grant(self.budget)?,
                |d, h| self.validate(d, h),
            ))
            .await?
        } else {
            self.operation(owner.inspect_activated_successor_producer(
                native,
                self.replay_request(),
                metadata,
                self.input_grant()?,
                |d, h| self.validate(d, h),
            ))
            .await?
        });
        let recovered = custody.positive.as_mut().expect("retained positive owner");
        self.operation(owner.close_activated_successor_producer(
            native,
            recovered,
            self.request(),
            closing,
            |d, h| self.validate(d, h),
        ))
        .await?;
        let mut resolution = recovered
            .resolution(native, &self.cancellation.requested)
            .map_err(error)?;
        while resolution.current_epoch().is_some() {
            if resolution.needs_compacted_initial() {
                self.refresh(owner).await?;
                self.operation(owner.inspect_activated_successor_compacted_initial(
                    &mut resolution,
                    self.request(),
                    |d, h| self.validate(d, h),
                ))
                .await?;
                continue;
            }
            while resolution.next_request().is_some() {
                self.refresh(owner).await?;
                let (input, outcome) = replay_grants(self.budget)?;
                self.operation(owner.resolve_next_activated_successor_request(
                    &mut resolution,
                    self.replay_request(),
                    input,
                    outcome,
                    |d, h| self.validate(d, h),
                ))
                .await?;
            }
            self.refresh(owner).await?;
            self.operation(owner.finish_activated_successor_epoch(
                &mut resolution,
                self.request(),
                |d, h| self.validate(d, h),
            ))
            .await?;
        }
        self.refresh(owner).await?;
        let proof = self
            .operation(owner.finish_activated_successor_recovery(
                resolution,
                self.request(),
                limits().event,
                effect_grant(self.budget)?,
                |d, h| self.validate(d, h),
            ))
            .await?;
        proof
            .validate(&self.cancellation.requested)
            .map_err(error)?;
        let completed = Completed {
            slot: recovered.producer_slot_id(),
            issued: recovered.issued_through(),
            reserved: recovered.reserved_through(),
            verified: proof.verified_through(),
            last: proof.last_epoch(),
        };
        let replay = proof
            .into_recovery_acknowledgement(&self.cancellation.requested)
            .map_err(error)?;
        let compacted = replay.compacted_requests();
        acknowledge(
            self.root,
            self.budget,
            self.progress,
            self.cancellation,
            owner,
            native,
            replay,
        )
        .await?;
        let mut report = completed.report(native, Mode::Resolve);
        if compacted != 0 {
            report["recovery_basis"] = json!("closed-producer-floor-v3");
            report["compacted_requests"] = json!(compacted.to_string());
        }
        Ok(report)
    }
}

// Fixed observations copied only after a complete proof, never portable authority.
struct Completed {
    slot: ProducerSlotId,
    issued: u64,
    reserved: u64,
    verified: u64,
    last: LineageEpochSummary,
}
impl Completed {
    fn report(&self, native: &IngestRecovery, mode: Mode) -> Value {
        let id = native.manifest().identity();
        let state = match mode {
            Mode::Resolve => "successor-positive-outcomes-resolved",
            Mode::CloseSource => "successor-source-closed",
            Mode::AbandonAcquisition => "successor-acquisition-abandoned",
            Mode::Retire => "successor-retired",
            Mode::RetireEmpty => unreachable!("separate empty-successor report"),
        };
        json!({"format":"bigbrotr-ingest-recovery/1","state":state,
            "dataset_id":id.dataset_id,"owner_resource_id":id.owner_resource_id,
            "producer_slot_id":self.slot,"producer_epoch":id.owner_epoch.get().to_string(),
            "issued_through":self.issued.to_string(),"reserved_through":self.reserved.to_string(),
            "resolved_requests":self.last.resolved_requests.to_string(),"verified_through":self.verified.to_string(),
            "source_closure":if mode==Mode::Resolve {json!("not-performed")}else{
                json!({"last_complete_sequence":self.last.last_complete_sequence.to_string(),
                    "sha256":Digest256::from_bytes(self.last.digest)})},
            "native":"closed","owner_release":"committed","ready":false})
    }
}
fn effect_grant(budget: &ResourceBudget) -> Result<OwnedPermit> {
    grant(budget, limits().required_data_bytes().map_err(error)?, 0)
}
fn compacted_grant(budget: &ResourceBudget) -> Result<OwnedPermit> {
    grant(
        budget,
        bigbrotr::storage::rocks::store::compacted_outcome_data_bytes(limits().event)
            .map_err(error)?,
        0,
    )
}
fn error(error: impl std::fmt::Display) -> (u8, String) {
    (4, format!("successor recovery unavailable: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::RefCell, rc::Rc};

    struct Dropped(&'static str, Rc<RefCell<Vec<&'static str>>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.1.borrow_mut().push(self.0);
        }
    }

    // Exercise the actual guard and shared bounded helper. The sentinels stand
    // for local ownership only; these tests make no transport-liveness claim.
    #[tokio::test]
    async fn interrupted_claim_closes_native_before_retained_future() {
        for already_cancelled in [true, false] {
            let dropped = Rc::new(RefCell::new(Vec::new()));
            let mut native = Some(Dropped("native", dropped.clone()));
            let client = Dropped("client", dropped.clone());
            let cancel = Cancellation::new();
            if already_cancelled {
                cancel.request();
            }
            let result: Result<()> = async {
                let acquisition = async move {
                    let _client = client;
                    std::future::pending::<Result<()>>().await
                };
                tokio::pin!(acquisition);
                let _claim_exit = ClaimExit(Some(&mut native));
                bounded(Duration::ZERO, &cancel, acquisition.as_mut()).await?;
                Ok(())
            }
            .await;
            let (code, message) = result.unwrap_err();
            assert_eq!(code, 4);
            assert!(message.contains(if already_cancelled {
                "cancelled"
            } else {
                "deadline"
            }));
            assert!(native.is_none());
            assert_eq!(*dropped.borrow(), ["native", "client"]);
        }
    }

    #[test]
    fn claim_guard_covers_unwind_and_disarms_after_delivery() {
        let dropped = Rc::new(RefCell::new(Vec::new()));
        let mut native = Some(Dropped("native", dropped.clone()));
        let client = Dropped("client", dropped.clone());
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let acquisition = async move {
                let _client = client;
                std::future::pending::<()>().await;
            };
            let _acquisition = std::pin::pin!(acquisition);
            let _claim_exit = ClaimExit(Some(&mut native));
            panic!("caller unwinding while acquisition remains pinned");
        }));
        assert!(unwind.is_err());
        assert!(native.is_none());
        assert_eq!(*dropped.borrow(), ["native", "client"]);

        dropped.borrow_mut().clear();
        let mut native = Some(Dropped("native", dropped.clone()));
        {
            let mut claim_exit = ClaimExit(Some(&mut native));
            claim_exit.0 = None;
        }
        assert!(dropped.borrow().is_empty());
        assert!(native.is_some());
        drop(native);
        assert_eq!(*dropped.borrow(), ["native"]);
    }
}
