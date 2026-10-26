//! Bounded recovery of the actual captured PG lineage. Historical metadata comes
//! directly from the current ControlSession, never from caller-supplied pages.
//! Each step owns at most one complete admission-batch plan; the service can
//! refresh its Control lease between joined operations.
use super::{IndexAdapter, IndexOperation, IndexOperationId, PhysicalError, inspect, readback};
use crate::{
    runtime::OwnedPermit,
    storage::{
        clickhouse::verified::{VerifiedIndexPlan, VerifiedIndexRecovery},
        indexer::{custody::HeldIndexCustody, plan::PlannedBatch},
        postgres::{
            IndexBinding, IndexOwnerIdentity, IndexRecoveryState, MAX_INDEX_STREAMS,
            RetainedIndexPlan, control_owner::ControlSession,
        },
    },
};
use bigbrotr_model::{Digest256, SourceCut, StreamBound};
use sha2::{Digest, Sha256};
use std::{fs::File, mem::size_of, sync::Arc, time::Instant};

/// One retained PG plan, one in-flight plan copy, prior cut, receipt/hash state
/// and bounded PG decoding scratch. It does not reserve the complete history.
pub fn required_recovery_metadata_bytes() -> u64 {
    (6 * MAX_INDEX_STREAMS * size_of::<StreamBound>()
        + 2 * size_of::<RetainedIndexPlan>()
        + size_of::<IndexRecoveryVerifier>()
        + 64 * 1024) as u64
}

pub struct IndexRecoveryVerifier {
    adapter: IndexAdapter,
    binding: IndexBinding,
    previous: IndexOwnerIdentity,
    state: IndexRecoveryState,
    completed_published: u64,
    pending_complete: bool,
    active: Option<RetainedIndexPlan>,
    cut: Option<SourceCut>,
    history: Sha256,
    pending_proof: Option<VerifiedIndexPlan>,
    _custody: File,
    _metadata: Arc<OwnedPermit>,
}

pub struct RecoveryResult {
    proof: VerifiedIndexRecovery,
    pending: Option<VerifiedIndexPlan>,
}
impl RecoveryResult {
    pub fn proof(&self) -> &VerifiedIndexRecovery {
        &self.proof
    }
    pub fn pending(&self) -> Option<&VerifiedIndexPlan> {
        self.pending.as_ref()
    }
    pub fn into_parts(self) -> (VerifiedIndexRecovery, Option<VerifiedIndexPlan>) {
        (self.proof, self.pending)
    }
}

impl IndexRecoveryVerifier {
    pub async fn begin(
        adapter: IndexAdapter,
        control: &mut ControlSession,
        custody: &HeldIndexCustody,
        metadata: OwnedPermit,
    ) -> Result<Self, PhysicalError> {
        if metadata.request().units().data_bytes() < required_recovery_metadata_bytes()
            || metadata.request().units().items() == 0
        {
            return Err(PhysicalError::InsufficientReservation);
        }
        adapter
            .tables
            .check_binding(custody.binding())
            .map_err(|_| PhysicalError::Schema)?;
        let retained = custody.retain_lane().map_err(|_| PhysicalError::Custody)?;
        let binding = *custody.binding();
        let (previous, state) = control
            .read_index_recovery_state(&binding)
            .await
            .map_err(|_| PhysicalError::Control)?;
        if previous.resource != binding.resource()
            || (state.published_revision == 0) != state.published_plan.is_none()
        {
            return Err(PhysicalError::Sequence);
        }
        let history = history_start(binding, previous, state);
        Ok(Self {
            adapter,
            binding,
            previous,
            state,
            completed_published: 0,
            pending_complete: state.pending_plan.is_none(),
            active: None,
            cut: None,
            history,
            pending_proof: None,
            _custody: retained,
            _metadata: Arc::new(metadata),
        })
    }
    pub fn is_complete(&self) -> bool {
        self.active.is_none()
            && self.completed_published == self.state.published_revision
            && self.pending_complete
    }
    pub fn captured_state(&self) -> IndexRecoveryState {
        self.state
    }
    /// Captured metadata, not a current owner or physical capability.
    pub fn captured_owner(&self) -> IndexOwnerIdentity {
        self.previous
    }
    /// Last accepted readback cut. Only the complete finish operation can
    /// establish a full-lineage physical observation.
    pub fn verified_cut(&self) -> Option<&SourceCut> {
        self.cut.as_ref()
    }
    /// Inspectable metadata only, for reading the exact retained native input on
    /// a repair/replay attempt. This reference is not a physical capability.
    pub fn active_plan(&self) -> Option<&crate::storage::postgres::IndexPlan> {
        self.active.as_ref().map(|row| &row.plan)
    }

    /// A failed known-local attempt may call next again for the same active plan.
    /// The engine slot refuses another attempt while remote uncertainty persists.
    /// Optional retained input replays exact immutable values before readback;
    /// missing rows without that input refuse, never certify a partial subset.
    #[allow(clippy::too_many_arguments)]
    pub async fn next(
        &mut self,
        control: &mut ControlSession,
        custody: &HeldIndexCustody,
        replay: Option<PlannedBatch>,
        local: OwnedPermit,
        engine: OwnedPermit,
        operation: IndexOperationId,
        deadline: Instant,
    ) -> Result<IndexOperation<VerifiedIndexPlan>, PhysicalError> {
        if custody.binding() != &self.binding {
            return Err(PhysicalError::Custody);
        }
        custody.check_live().map_err(|_| PhysicalError::Custody)?;
        if self.active.is_none() {
            if self.completed_published < self.state.published_revision {
                let mut page = control
                    .read_index_plans(&self.binding, self.completed_published, 1)
                    .await
                    .map_err(|_| PhysicalError::Control)?;
                if page.len() != 1 {
                    return Err(PhysicalError::Sequence);
                }
                let retained = page.pop().ok_or(PhysicalError::Sequence)?;
                self.active = Some(retained);
            } else if !self.pending_complete {
                let digest = self.state.pending_plan.ok_or(PhysicalError::Sequence)?;
                let retained = control
                    .read_index_plan(&self.binding, digest)
                    .await
                    .map_err(|_| PhysicalError::Control)?
                    .ok_or(PhysicalError::Sequence)?;
                self.active = Some(retained);
            } else {
                return Err(PhysicalError::Sequence);
            }
        }
        let retained = self.active.as_ref().ok_or(PhysicalError::Sequence)?;
        validate_step(
            &self.binding,
            self.previous,
            self.state,
            self.completed_published,
            self.pending_complete,
            self.cut.as_ref(),
            retained,
        )?;
        if let Some(replay) = replay {
            if replay.plan() != &retained.plan {
                return Err(PhysicalError::Source);
            }
            self.adapter.replay_and_verify(
                custody,
                Arc::new(replay),
                local,
                engine,
                operation,
                deadline,
                false,
                Some(Arc::clone(&self._metadata)),
            )
        } else {
            let plan = retained.plan.clone();
            let tables = Arc::clone(&self.adapter.tables);
            let connection = Arc::clone(&self.adapter.connection);
            let limits = self.adapter.limits;
            self.adapter.start(
                custody,
                local,
                engine,
                operation,
                deadline,
                None,
                Some(Arc::clone(&self._metadata)),
                move |mut owner, file| async move {
                    inspect::engine(&mut owner, &tables, connection.user(), true).await?;
                    let digest = readback::plan(&mut owner, &tables, &plan, limits).await?;
                    Ok(VerifiedIndexPlan::observed(
                        *plan.binding(),
                        plan.digest(),
                        digest,
                        file,
                    ))
                },
            )
        }
    }

    pub fn accept(&mut self, proof: VerifiedIndexPlan) -> Result<(), PhysicalError> {
        let retained = self.active.as_ref().ok_or(PhysicalError::Sequence)?;
        if proof.binding() != &self.binding || proof.plan_digest() != retained.plan.digest() {
            return Err(PhysicalError::Sequence);
        }
        if let Some(receipt) = &retained.publication {
            if proof.engine_receipt_digest() != receipt.engine_receipt_digest {
                return Err(PhysicalError::Rows);
            }
            self.completed_published = self
                .completed_published
                .checked_add(1)
                .ok_or(PhysicalError::Limit)?;
            self.history.update([0]);
        } else {
            if self.pending_complete {
                return Err(PhysicalError::Sequence);
            }
            self.pending_complete = true;
            self.history.update([1]);
        }
        self.history
            .update(retained.plan.expected_revision().to_be_bytes());
        self.history.update(retained.plan.digest().as_bytes());
        self.history
            .update(proof.engine_receipt_digest().as_bytes());
        self.cut = Some(retained.plan.after().clone());
        if retained.publication.is_none() {
            self.pending_proof = Some(proof);
        }
        self.active = None;
        Ok(())
    }

    /// Final PG recovery also compares this exact owner/head/pending tuple under
    /// its publication locks. The final physical guard rejects rows outside the
    /// fully verified lineage and rechecks every schema/UUID before proof output.
    pub async fn finish(
        self,
        control: &mut ControlSession,
        custody: &HeldIndexCustody,
        local: OwnedPermit,
        engine: OwnedPermit,
        operation: IndexOperationId,
        deadline: Instant,
    ) -> Result<IndexOperation<RecoveryResult>, PhysicalError> {
        if !self.is_complete() || custody.binding() != &self.binding {
            return Err(PhysicalError::Sequence);
        }
        let observed = control
            .read_index_recovery_state(&self.binding)
            .await
            .map_err(|_| PhysicalError::Control)?;
        if observed != (self.previous, self.state) {
            return Err(PhysicalError::Sequence);
        }
        let tables = Arc::clone(&self.adapter.tables);
        let connection = Arc::clone(&self.adapter.connection);
        let adapter = self.adapter.clone();
        adapter.start(
            custody,
            local,
            engine,
            operation,
            deadline,
            None,
            Some(Arc::clone(&self._metadata)),
            move |mut owner, file| async move {
                inspect::engine(&mut owner, &tables, connection.user(), true).await?;
                inspect::installed(&mut owner, &tables, self.cut.is_none()).await?;
                if let Some(cut) = &self.cut {
                    readback::inside_cut(
                        &mut owner,
                        &tables,
                        self.binding.generation().as_bytes(),
                        cut,
                    )
                    .await?;
                }
                inspect::installed(&mut owner, &tables, false).await?;
                let digest = Digest256::from_bytes(self.history.finalize().into());
                Ok(RecoveryResult {
                    proof: VerifiedIndexRecovery::observed(
                        self.binding,
                        self.previous,
                        self.state,
                        digest,
                        file,
                    ),
                    pending: self.pending_proof,
                })
            },
        )
    }
}

fn history_start(
    binding: IndexBinding,
    previous: IndexOwnerIdentity,
    state: IndexRecoveryState,
) -> Sha256 {
    let mut history = Sha256::new();
    history.update(b"bigbrotr/index-recovery-observation/1\0");
    for id in [
        binding.dataset().as_bytes(),
        binding.resource().as_bytes(),
        binding.generation().as_bytes(),
        binding.storage_incarnation().as_bytes(),
    ] {
        history.update(id);
    }
    history.update(binding.table_identity_digest().as_bytes());
    history.update(binding.custody_binding_digest().as_bytes());
    history.update([match binding.provenance() {
        crate::storage::postgres::IndexProvenance::LocalFirstWitness => 1,
        crate::storage::postgres::IndexProvenance::Off => 0,
    }]);
    history.update(previous.epoch.get().to_be_bytes());
    history.update(previous.incarnation.as_bytes());
    history.update(state.published_revision.to_be_bytes());
    for digest in [state.published_plan, state.pending_plan] {
        history.update([u8::from(digest.is_some())]);
        if let Some(digest) = digest {
            history.update(digest.as_bytes());
        }
    }
    history
}

// Structural admission for the next actual PG row. The capture owner above is
// the only caller in production; this helper does not create a physical proof.
fn validate_step(
    binding: &IndexBinding,
    previous: IndexOwnerIdentity,
    state: IndexRecoveryState,
    completed: u64,
    pending_complete: bool,
    cut: Option<&SourceCut>,
    retained: &RetainedIndexPlan,
) -> Result<(), PhysicalError> {
    if retained.plan.binding() != binding
        || cut.is_some_and(|cut| cut != retained.plan.before())
        || completed > state.published_revision
    {
        return Err(PhysicalError::Sequence);
    }
    if completed < state.published_revision {
        let revision = completed.checked_add(1).ok_or(PhysicalError::Limit)?;
        let receipt = retained
            .publication
            .as_ref()
            .ok_or(PhysicalError::Sequence)?;
        if retained.plan.expected_revision() != completed
            || receipt.revision != revision
            || receipt.binding != *binding
            || receipt.plan_digest != retained.plan.digest()
            || receipt.cut != *retained.plan.after()
            || receipt.family_counts
                != std::array::from_fn(|i| retained.plan.chunks()[i].row_count())
            || receipt.publisher.resource != previous.resource
            || receipt.publisher.epoch > previous.epoch
            || (receipt.publisher.epoch == previous.epoch && receipt.publisher != previous)
            || (revision == state.published_revision
                && Some(retained.plan.digest()) != state.published_plan)
        {
            return Err(PhysicalError::Sequence);
        }
    } else if pending_complete
        || retained.publication.is_some()
        || Some(retained.plan.digest()) != state.pending_plan
        || retained.plan.expected_revision() != state.published_revision
    {
        return Err(PhysicalError::Sequence);
    }
    Ok(())
}

#[cfg(test)]
#[path = "recovery/tests.rs"]
mod tests;
