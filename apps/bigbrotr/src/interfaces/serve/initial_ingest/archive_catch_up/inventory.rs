//! Complete original input replay precedes catalog reconciliation or new work.
use super::super::archive::native;
use super::*;
use bigbrotr::storage::{
    archive::{LocalAttempt, RetainedInputs, WorkerBudgets},
    postgres::{
        archive_attempt::ArchiveAttemptReceipt,
        control_owner::{RetainedArchiveFrontier, RetainedArchiveRequest},
    },
    rocks::store::{PendingBatch, PendingBatchHeader, PendingReadLimits},
};
use bigbrotr_model::{AdmissionBatch, ArchiveBatch, SourceRecordId, codec::FactDecodeLimits};

pub(super) const RECORDS: usize = 64;
pub(super) const ENCODED: usize = 1024 * 1024;

pub(super) fn limits() -> PendingReadLimits {
    PendingReadLimits {
        max_records: RECORDS,
        max_encoded_bytes: ENCODED,
        fact: FactDecodeLimits::default(),
    }
}

pub(super) struct Attempt {
    pub(super) receipt: ArchiveAttemptReceipt,
    pub(super) inputs: RetainedInputs,
    pub(super) present: bool,
}
pub(super) struct Inventory {
    pub(super) attempts: Vec<Attempt>,
    // Fixed receipt pages, vector headers and temporary input references. The
    // scientific inputs each retain their separate required allocation grant.
    _metadata: OwnedPermit,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn load(
    root: &Resolved,
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    recovery: &IngestRecovery,
    proof: &VerifiedSuccessorHistory<'_>,
    coordinator: &LocalCoordinator,
    frontier: &mut Option<RetainedArchiveFrontier>,
) -> Result<Inventory> {
    let metadata = grant(budget, MIB, 0)?;
    authority.refresh(owner).await?;
    authority
        .operation(owner.require_archive_coordinator(coordinator))
        .await?;
    authority.refresh(owner).await?;
    let mut scan = authority
        .operation(owner.begin_archive_attempt_scan(coordinator.identity().catalog()))
        .await?;
    if scan.high_water() > MAX_ATTEMPTS as u64 {
        return Err(error(
            "complete issued attempt history exceeds finite profile",
        ));
    }
    let mut attempts = Vec::new();
    attempts
        .try_reserve_exact(scan.high_water() as usize)
        .map_err(error)?;
    while !scan.is_complete() {
        authority.refresh(owner).await?;
        let page = authority
            .operation(owner.read_archive_attempt_page(&mut scan))
            .await?;
        for receipt in page {
            authority.refresh(owner).await?;
            let inputs = authority.local(|cancel| {
                coordinator
                    .read_inputs(
                        receipt.request(),
                        grant(budget, RetainedInputs::required_data_bytes(), 0)?,
                        cancel,
                    )
                    .map_err(error)?
                    .ok_or_else(|| error("issued attempt has no original input linkage"))
            })?;
            replay_inputs(budget, authority, owner, proof, &inputs).await?;
            attempts.push(Attempt {
                receipt,
                inputs,
                present: false,
            });
        }
    }
    authority.refresh(owner).await?;
    let history_request = request(root, authority.cancellation)?;
    if let Some(keeper) = frontier.as_mut() {
        authority
            .operation(owner.inspect_compacted_retained_archive_frontier(
                keeper,
                proof,
                recovery,
                RetainedArchiveRequest {
                    coordinator,
                    attempts: &scan,
                },
                history_request,
                |d, h| validate(root, d, h),
            ))
            .await?;
    } else {
        *frontier = Some(
            authority
                .operation(owner.inspect_retained_archive_frontier(
                    proof,
                    recovery,
                    RetainedArchiveRequest {
                        coordinator,
                        attempts: &scan,
                    },
                    history_request,
                    grant(budget, RetainedArchiveFrontier::required_data_bytes(), 0)?,
                    |d, h| validate(root, d, h),
                ))
                .await?,
        );
    }
    let references: Vec<_> = attempts.iter().map(|attempt| &attempt.inputs).collect();
    frontier
        .as_mut()
        .expect("loaded frontier")
        .bind_current_inputs_in_place(&references)
        .map_err(error)?;
    Ok(Inventory {
        attempts,
        _metadata: metadata,
    })
}

pub(super) fn pending(
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    proof: &VerifiedSuccessorHistory<'_>,
    first: SourceRecordId,
) -> Result<PendingBatch> {
    authority.local(|cancel| {
        let resolved = proof.resolved_lineage();
        let permit = grant(budget, limits().required_data_bytes().map_err(error)?, 0)?;
        if proof.compacted_initial_requests() != 0 {
            resolved.read_complete_compacted_pending_batch(first, limits(), permit, cancel)
        } else {
            resolved.read_complete_pending_batch(first, limits(), permit, cancel)
        }
        .map_err(error)
    })
}

async fn replay_inputs(
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    proof: &VerifiedSuccessorHistory<'_>,
    inputs: &RetainedInputs,
) -> Result<()> {
    let count = inputs.expectation().record_count;
    if count == 0 || count > RECORDS as u64 || inputs.headers().len() > RECORDS {
        return Err(error("retained attempt exceeds complete-batch profile"));
    }
    // Only fixed FactRef metadata is copied, under the retained input's 1MiB
    // allowance. Each original payload is checked and dropped before the next.
    let mut batches = Vec::new();
    batches
        .try_reserve_exact(inputs.headers().len())
        .map_err(error)?;
    for header in inputs.headers() {
        authority.refresh(owner).await?;
        let pending = pending(budget, authority, proof, header.first())?;
        if PendingBatchHeader::from_batch(pending.batch()) != *header {
            return Err(error("original complete input header differs"));
        }
        let mut records = Vec::new();
        records
            .try_reserve_exact(pending.batch().records().len())
            .map_err(error)?;
        records.extend_from_slice(pending.batch().records());
        batches.push(AdmissionBatch::new(records).map_err(error)?);
    }
    let archive =
        ArchiveBatch::new(1, inputs.request().topology_generation(), &batches).map_err(error)?;
    inputs.verify_replay(&archive).map_err(error)
}

impl Inventory {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn reconcile(
        &mut self,
        root: &Resolved,
        budget: &ResourceBudget,
        native_budget: &ResourceBudget,
        authority: &mut Authority<'_>,
        owner: &mut ControlSession,
        proof: &VerifiedSuccessorHistory<'_>,
        coordinator: &LocalCoordinator,
        reader: &Arc<native::Reader>,
        recovery: &IngestRecovery,
        frontier: &RetainedArchiveFrontier,
    ) -> Result<()> {
        // Move each nonclone custody/input owner into actual joined work. Keep
        // the vector allocation and every other input grant until it returns.
        let mut remaining = std::mem::take(&mut self.attempts).into_iter();
        self.attempts
            .try_reserve_exact(remaining.len())
            .map_err(error)?;
        for mut attempt in &mut remaining {
            authority.refresh(owner).await?;
            authority.history(owner, recovery, proof, frontier).await?;
            let local = authority.local(|cancel| {
                proof.validate(cancel).map_err(error)?;
                LocalAttempt::recover(
                    coordinator.state_root(),
                    &attempt.receipt,
                    coordinator.identity(),
                )
                .map_err(error)
            })?;
            let custody = coordinator.reader_custody().map_err(error)?;
            let binding = coordinator.binding_digest();
            let (root, app, native, reader) = (
                root.clone(),
                budget.clone(),
                native_budget.clone(),
                reader.clone(),
            );
            let deadline = Instant::now() + OPERATION;
            attempt = authority.blocking(owner, deadline, move |cancel| {
                let result = native::catch_up::reconcile_attempt(
                    &root, &attempt.receipt, &local, &attempt.inputs, &reader, &custody, binding,
                    WorkerBudgets { application: &app, native: &native }, deadline, cancel,
                )?;
                attempt.present = match result {
                    native::catch_up::ReconciledAttempt::Absent { resolution } => {
                        // Both known resolutions permit an independently absent
                        // receipt; neither permits reuse of the old credential.
                        match resolution {
                            bigbrotr::storage::postgres::catalog_guard::Resolution::NotImported
                            | bigbrotr::storage::postgres::catalog_guard::Resolution::Sealed { .. } => false,
                        }
                    }
                    native::catch_up::ReconciledAttempt::Present { resolution, membership } => {
                        if !matches!(resolution, bigbrotr::storage::postgres::catalog_guard::Resolution::Sealed { .. }) {
                            return Err(error("present attempt lacks positive exclusion"));
                        }
                        membership.validate(&bigbrotr_protocol::archive_membership::MembershipRead {
                            receipt: attempt.inputs.expectation().clone(), snapshot_id: None,
                        }).map_err(error)?;
                        true
                    }
                };
                Ok(attempt)
            }).await?;
            authority.history(owner, recovery, proof, frontier).await?;
            self.attempts.push(attempt);
        }
        if let Some(current) = frontier
            .bound_current_inputs()
            .map_err(error)?
            .current_cut()
        {
            for expected in &current.receipts {
                if !self
                    .attempts
                    .iter()
                    .any(|a| a.present && a.inputs.expectation() == expected)
                {
                    return Err(error("published frontier receipt is absent or incomplete"));
                }
            }
        }
        Ok(())
    }
}
