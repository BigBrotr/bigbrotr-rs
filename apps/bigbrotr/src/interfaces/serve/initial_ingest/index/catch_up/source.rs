//! One retained source proof, continuously held through Index effects. The
//! successor branch is only the complete un-compacted initial epoch1→2 lineage.
use super::*;
use bigbrotr::storage::{
    postgres::{
        IndexBinding, IndexHandoff,
        control_owner::{
            InitialHistoryRequest, SuccessorHistory, SuccessorHistoryState, SuccessorReplayRequest,
            VerifiedSuccessorHistory,
        },
    },
    rocks::store::{PendingBatch, RetainedTerminalKind},
};
use bigbrotr_model::{Digest256, TopologyGeneration};

#[allow(
    clippy::large_enum_variant,
    reason = "one retained proof stays under the owner metadata grant without another heap allocation"
)]
enum Proof<'a> {
    Initial(ResolvedSource<'a>),
    Successor(VerifiedSuccessorHistory<'a>),
}
pub(super) struct Source<'a> {
    manifest: IngestManifest,
    complete: SourceCut,
    requests: u64,
    proof: Proof<'a>,
}
impl<'a> Source<'a> {
    pub(super) fn initial(manifest: IngestManifest, proof: ResolvedSource<'a>) -> Result<Self> {
        if proof.resolved_requests() > 64 {
            return Err(error("global retained request ceiling exceeded"));
        }
        let mut bound = zero_cut(*manifest.identity())?.bounds()[0];
        bound.last_complete_sequence = CompleteThrough::new(proof.last_complete_sequence());
        let complete = SourceCut::new(
            manifest.identity().dataset_id,
            TopologyGeneration::new(0),
            vec![bound],
        )
        .map_err(error)?;
        Ok(Self {
            manifest,
            complete,
            requests: proof.resolved_requests(),
            proof: Proof::Initial(proof),
        })
    }
    pub(super) fn successor(
        manifest: IngestManifest,
        proof: VerifiedSuccessorHistory<'a>,
    ) -> Result<Self> {
        let history = proof.history();
        let lineage = history.lineage();
        if manifest.key_schema() != 2
            || manifest.identity().owner_epoch.get() != 2
            || manifest.identity().topology_generation.get() != 1
            || history.state() != SuccessorHistoryState::Terminal
            || history.native_epoch() != 2
            || history.registered_epoch() != 2
            || proof.verified_through() != 2
            || proof.last_epoch().terminal_kind != RetainedTerminalKind::Activated
            || lineage.has_compacted_initial()
            || lineage.pending_target().is_some()
            || lineage.entries().len() != 2
            || lineage.native_head().manifest() != &manifest
        {
            return Err(error(
                "requires the complete activated initial successor lineage",
            ));
        }
        let mut bounds = Vec::with_capacity(2);
        let mut requests = 0_u64;
        for (n, entry) in lineage.entries().iter().enumerate() {
            let id = entry.manifest().identity();
            if id.owner_epoch.get() != n as u64 + 1
                || id.topology_generation.get() != n as u64
                || id.dataset_id != manifest.identity().dataset_id
                || id.shard_id != manifest.identity().shard_id
                || id.owner_resource_id != manifest.identity().owner_resource_id
                || (n == 0
                    && !matches!(
                        entry.terminal_claim(),
                        Some((RetainedTerminalKind::Initial, _))
                    ))
                || (n == 1 && (!entry.is_closing() || entry.terminal_claim().is_some()))
            {
                return Err(error("initial successor source inventory differs"));
            }
            requests = requests
                .checked_add(entry.issued_through())
                .ok_or_else(|| error("request census overflow"))?;
            bounds.push(StreamBound {
                stream: StreamId {
                    shard_id: id.shard_id,
                    owner_epoch: id.owner_epoch,
                },
                last_complete_sequence: CompleteThrough::new(entry.source_through()),
            });
        }
        if requests > 64 {
            return Err(error("global retained request ceiling exceeded"));
        }
        let complete = SourceCut::new(
            manifest.identity().dataset_id,
            TopologyGeneration::new(1),
            bounds,
        )
        .map_err(error)?;
        Ok(Self {
            manifest,
            complete,
            requests,
            proof: Proof::Successor(proof),
        })
    }
    pub(super) fn complete(&self) -> &SourceCut {
        &self.complete
    }
    pub(super) fn requests(&self) -> u64 {
        self.requests
    }
    pub(super) fn epoch(&self) -> u64 {
        self.manifest.identity().owner_epoch.get()
    }
    pub(super) fn through(&self) -> u64 {
        self.complete
            .bounds()
            .last()
            .unwrap()
            .last_complete_sequence
            .get()
    }
    pub(super) fn successor_proof(&self) -> Option<&VerifiedSuccessorHistory<'a>> {
        match &self.proof {
            Proof::Successor(proof) => Some(proof),
            Proof::Initial(_) => None,
        }
    }
    pub(super) fn validate(&self, cancel: &AtomicBool) -> Result<()> {
        match &self.proof {
            Proof::Initial(proof) => proof.validate_initial(cancel).map_err(error),
            Proof::Successor(proof) => proof.validate(cancel).map_err(error),
        }
    }
    pub(super) fn handoff(
        &self,
        binding: IndexBinding,
        revision: u64,
        before: &SourceCut,
    ) -> Result<IndexHandoff> {
        let proof = self
            .successor_proof()
            .ok_or_else(|| error("no successor proof"))?;
        if before.bounds() != &self.complete.bounds()[..1]
            || before.topology_generation().get() != 0
        {
            return Err(error("Index predecessor is not fully published"));
        }
        IndexHandoff::new(
            binding,
            revision,
            self.manifest.identity().owner_resource_id,
            Digest256::from_bytes(
                proof
                    .history()
                    .lineage()
                    .entry(2)
                    .unwrap()
                    .incoming_handoff_sha256(),
            ),
            before.clone(),
        )
        .map_err(error)
    }
    pub(super) fn next(&self, before: &SourceCut) -> Result<SourceRecordId> {
        if !within(before, &self.complete)
            || before.topology_generation() != self.complete.topology_generation()
            || before.bounds().len() != self.complete.bounds().len()
        {
            return Err(error(
                "source requires its explicit verified topology bridge",
            ));
        }
        let bound = before.bounds().last().unwrap();
        Ok(SourceRecordId {
            dataset_id: self.complete.dataset_id(),
            shard_id: bound.stream.shard_id,
            owner_epoch: bound.stream.owner_epoch,
            sequence: Sequence::new(
                bound
                    .last_complete_sequence
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| error("source sequence exhausted"))?,
            )
            .map_err(error)?,
        })
    }
    pub(super) fn read(
        &self,
        before: &SourceCut,
        limits: PendingReadLimits,
        permit: OwnedPermit,
        cancel: &AtomicBool,
    ) -> Result<PendingBatch> {
        let next = self.next(before)?;
        match &self.proof {
            Proof::Initial(proof) => proof
                .read_complete_pending_batch(next, limits, permit, cancel)
                .map_err(error),
            Proof::Successor(proof) => proof
                .resolved_lineage()
                .read_complete_pending_batch(next, limits, permit, cancel)
                .map_err(error),
        }
    }
}

/// Structural bound check only; all production values come from the retained
/// source proof. A topology1 cursor preserves the closed old stream exactly.
pub(super) fn within(cut: &SourceCut, complete: &SourceCut) -> bool {
    if cut.dataset_id() != complete.dataset_id() {
        return false;
    }
    match (
        cut.topology_generation().get(),
        complete.topology_generation().get(),
        cut.bounds(),
        complete.bounds(),
    ) {
        (0, 0, [a], [b]) | (0, 1, [a], [b, _]) => {
            a.stream == b.stream && a.last_complete_sequence <= b.last_complete_sequence
        }
        (1, 1, [a, b], [c, d]) => {
            a == c && b.stream == d.stream && b.last_complete_sequence <= d.last_complete_sequence
        }
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn replay<'a>(
    root: &Resolved,
    budget: &ResourceBudget,
    cancel: &'a Cancellation,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    state: &IndexState,
    native: &'a IngestRecovery,
    history: &'a SuccessorHistory,
    accepted: Profile,
) -> Result<VerifiedSuccessorHistory<'a>> {
    let request = || {
        Ok::<_, (u8, String)>(InitialHistoryRequest {
            ingest_root: Path::new(required(root, "storage.ingest_root")?),
            policy_ttl: duration(root, "authorization_ttl"),
            cancelled: &cancel.requested,
        })
    };
    let mut resolution = history
        .resolution(native, &cancel.requested)
        .map_err(error)?;
    if resolution.needs_compacted_initial() {
        return Err(error("compacted source is outside finite epoch2 Index"));
    }
    while resolution.current_epoch().is_some() {
        while resolution.next_request().is_some() {
            authority.refresh(owner, None, state).await?;
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
                    |d, h| validate(root, accepted, d, h),
                ))
                .await?;
        }
        authority.refresh(owner, None, state).await?;
        authority
            .operation(
                owner.finish_successor_history_epoch(&mut resolution, request()?, |d, h| {
                    validate(root, accepted, d, h)
                }),
            )
            .await?;
    }
    authority.refresh(owner, None, state).await?;
    let proof = authority
        .operation(owner.finish_successor_history(
            resolution,
            request()?,
            recovery::limits().event,
            grant(
                budget,
                recovery::limits().required_data_bytes().map_err(error)?,
                0,
            )?,
            |d, h| validate(root, accepted, d, h),
        ))
        .await?;
    proof.validate(&cancel.requested).map_err(error)?;
    Ok(proof)
}
