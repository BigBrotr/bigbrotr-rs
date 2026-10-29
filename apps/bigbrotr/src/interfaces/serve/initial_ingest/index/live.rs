//! One explicit retained epoch1→2 activation. Recovery and the zero bridge
//! finish before the command owner can dispatch any successor acquisition.
use super::super::{Cancellation, Progress};
use super::*;
use bigbrotr::storage::{
    clickhouse::adapter::IndexRecoveryVerifier,
    postgres::{
        IndexHandoff, IndexOwnerIdentity, IndexRecoveryState, RetainedIndexRevision,
        control_owner::LiveInitialWriter,
    },
};
use serde_json::{Value, json};
use sha2::Digest;
use tokio::sync::watch;

impl Index {
    /// Both native custodies are acquired before any protected connection input.
    /// No fresh journal, generation, table UUID or initializer is created.
    pub(in crate::interfaces::serve::initial_ingest) fn retain(
        root: &Resolved,
        application: &ResourceBudget,
        driver: &IndexDriverCustody,
    ) -> Result<Self> {
        let state = IndexState::recover(Path::new(required(root, "storage.index_root")?))
            .map_err(|_| failed("retained Index custody refused"))?;
        let dataset = required(root, "topology.dataset_id")?
            .parse()
            .map_err(|_| failed("dataset invalid"))?;
        if state.binding().dataset() != dataset
            || state.tables().database() != required(root, "clickhouse.database")?
        {
            return Err(failed("retained Index deployment identity differs"));
        }
        for key in root
            .selected_storage_roots()
            .filter(|key| *key != "storage.index_root")
        {
            let other = Path::new(required(root, key)?)
                .canonicalize()
                .map_err(|_| failed("store placement unavailable"))?;
            LocalCoordinator::require_disjoint(
                Path::new(required(root, "storage.index_root")?),
                &other,
            )
            .map_err(|_| failed("retained Index overlaps another selected store"))?;
        }
        driver
            .attach(state.custody())
            .map_err(|_| failed("Control driver custody attachment refused"))?;
        let mut index = Self::prepare(root, application, false)?;
        index.state = Some(state);
        Ok(index)
    }

    /// A deterministic metadata refusal before native succession preparation.
    /// The actual old retirement/handoff already came from this retained owner;
    /// this read is neither a physical proof nor future publication authority.
    pub(in crate::interfaces::serve::initial_ingest) async fn preflight_live_predecessor(
        &self,
        owner: &mut ControlSession,
        handoff: &bigbrotr::storage::rocks::store::InitialHandoff,
    ) -> Result<()> {
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| failed("retained Index state missing"))?;
        if self
            .accepted
            .is_none_or(|profile| profile.provenance != state.binding().provenance())
        {
            return Err(failed("retained Index provenance differs"));
        }
        state
            .custody()
            .check_live()
            .map_err(|_| failed("retained Index custody changed"))?;
        let before = predecessor_cut(handoff)?;
        read_predecessor(owner, state, handoff.issued_through(), &before).await?;
        Ok(())
    }

    pub(in crate::interfaces::serve::initial_ingest) async fn recover_live_initial(
        &mut self,
        root: &Resolved,
        progress: &watch::Sender<Progress>,
        cancellation: &Cancellation,
        owner: &mut ControlSession,
        writer: &LiveInitialWriter,
    ) -> Result<Value> {
        let profile = self
            .accepted
            .ok_or_else(|| failed("accepted Index policy missing"))?;
        let source = writer
            .index_source()
            .map_err(|_| failed("original live predecessor required"))?;
        let head = source
            .observe(&cancellation.requested)
            .map_err(|_| failed("live source unavailable"))?;
        if head.through().get() != 0
            || source
                .retained_requests()
                .map_err(|_| failed("request census unavailable"))?
                > source.history_request_limit()
            || self.session.is_some()
            || self.active.is_some()
            || self.pending.is_some()
        {
            return Err(failed(
                "live startup requires untouched epoch2 and no Index work",
            ));
        }
        let native = source.handoff();
        let id = *source.manifest().identity();
        let before = predecessor_cut(native)?;
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| failed("retained Index state missing"))?;
        if state.binding().provenance() != profile.provenance {
            return Err(failed("retained provenance changed"));
        }
        self.control = Some(owner.identity());
        self.source_identity = Some(id);
        let adapter = IndexAdapter::new(
            Arc::new(state.tables().clone()),
            Arc::clone(self.prepared.writer()),
            profile.physical()?,
            Arc::clone(&self.slot),
        );
        let mut authority = catch_up::authority::Authority::new(
            root,
            progress,
            cancellation,
            owner,
            writer.policy_snapshot(),
            profile,
        )?
        .with_live_initial(writer);
        authority.refresh(owner, None, state).await?;
        let metadata = admitted(&self.application, required_recovery_metadata_bytes(), 0)?
            .ok_or_else(|| failed("recovery metadata unavailable"))?;
        let mut verifier = authority
            .operation(IndexRecoveryVerifier::begin(
                adapter.clone(),
                owner,
                state.custody(),
                metadata,
            ))
            .await?;
        let captured = verifier.captured_state();
        let previous = verifier.captured_owner();
        if authority
            .operation(read_predecessor(
                owner,
                state,
                native.issued_through(),
                &before,
            ))
            .await?
            != (previous, captured)
        {
            return Err(failed("old Index head changed during recovery capture"));
        }
        let binding = *state.binding();
        // One retained metadata plan at a time. Published history is verify-only.
        for _ in 0..captured.published_revision {
            let state = self.state.as_ref().unwrap();
            authority.refresh(owner, None, state).await?;
            if authority
                .operation(verifier.advance_handoff(owner, state.custody()))
                .await?
                .is_some()
            {
                return Err(failed("old prefix unexpectedly contains a handoff"));
            }
            let (local, engine, operation, deadline) = self.recovery_dispatch(owner)?;
            let state = self.state.as_ref().unwrap();
            let operation = authority
                .operation(verifier.next(
                    owner,
                    state.custody(),
                    None,
                    local,
                    engine,
                    operation,
                    deadline,
                ))
                .await?;
            let joined = authority
                .join(owner, None, state, &adapter, operation, deadline)
                .await?;
            let (outcome, permit) = joined.into_parts();
            match outcome {
                IndexOperationOutcome::Completed(Ok(proof)) => verifier
                    .accept(proof)
                    .map_err(|_| failed("old physical plan differs"))?,
                _ => return Err(failed("old physical plan unavailable")),
            }
            drop(permit);
        }
        if !verifier.is_complete() || verifier.verified_cut() != Some(&before) {
            return Err(failed("physical recovery did not reach the closed old cut"));
        }
        let state = self.state.as_ref().unwrap();
        authority.refresh(owner, None, state).await?;
        let (local, engine, operation, deadline) = self.recovery_dispatch(owner)?;
        let state = self.state.as_ref().unwrap();
        let operation = authority
            .operation(verifier.finish(owner, state.custody(), local, engine, operation, deadline))
            .await?;
        let joined = authority
            .join(owner, None, state, &adapter, operation, deadline)
            .await?;
        let recovered = match joined.outcome() {
            IndexOperationOutcome::Completed(Ok(result)) if result.pending().is_none() => result,
            _ => return Err(failed("complete physical recovery unavailable")),
        };
        authority.refresh(owner, None, state).await?;
        let session = authority
            .operation(owner.recover_index(
                state.binding(),
                state.custody(),
                recovered.proof(),
                &Cell::new(false),
            ))
            .await?;
        let handoff = IndexHandoff::new(
            binding,
            captured.published_revision,
            id.owner_resource_id,
            bigbrotr_model::Digest256::from_bytes(
                sha2::Sha256::digest(native.encode().as_bytes()).into(),
            ),
            before,
        )
        .map_err(|_| failed("live handoff shape invalid"))?;
        authority.refresh(owner, Some(&session), state).await?;
        let receipt = authority
            .operation(owner.publish_live_index_handoff(
                &session,
                state.custody(),
                &handoff,
                recovered.proof(),
                writer,
                bigbrotr::storage::postgres::control_owner::InitialHistoryRequest {
                    ingest_root: Path::new(required(root, "storage.ingest_root")?),
                    policy_ttl: super::super::duration(root, "authorization_ttl"),
                    cancelled: &cancellation.requested,
                },
                &Cell::new(false),
                |d, h| {
                    let restored = super::super::super::accepted_policy(root, d, h)?;
                    if super::profile(&restored)? != profile {
                        return Err(OwnerError::InvalidPolicy);
                    }
                    Ok(())
                },
            ))
            .await?;
        if receipt.handoff != handoff || receipt.publisher != session.owner() || !receipt.created {
            return Err(failed("live handoff receipt differs"));
        }
        drop(joined);
        self.revision = captured
            .published_revision
            .checked_add(1)
            .ok_or_else(|| failed("revision exhausted"))?;
        self.cut = Some(handoff.after().clone());
        let report = json!({"format":"bigbrotr-live-initial-index/1","state":"bridged",
            "generation_id":binding.generation(),"storage_incarnation":binding.storage_incarnation(),
            "table_identity_digest":binding.table_identity_digest(),
            "previous_owner_epoch":previous.epoch.get(),"owner_epoch":session.owner().epoch.get(),
            "published_revision_before":captured.published_revision,"published_revision_after":self.revision,
            "published_cut":self.cut,"ready":false});
        self.writer = Some(adapter);
        self.session = Some(session);
        // Rebinding and a bridge do not mint a readiness observation.
        Ok(report)
    }

    fn recovery_dispatch(
        &mut self,
        owner: &ControlSession,
    ) -> Result<(OwnedPermit, OwnedPermit, IndexOperationId, Instant)> {
        let limits = self.accepted.unwrap().physical()?;
        let local = admitted(
            &self.application,
            limits
                .required_local_data_bytes()
                .map_err(|_| failed("recovery bounds invalid"))?,
            limits.required_local_native_bytes(),
        )?
        .ok_or_else(|| failed("physical recovery allocation unavailable"))?;
        let engine = admitted(
            self.prepared.engine(),
            limits.required_engine_data_bytes(),
            0,
        )?
        .ok_or_else(|| failed("physical recovery engine credit unavailable"))?;
        if self.control != Some(owner.identity()) {
            return Err(failed("Control changed during recovery"));
        }
        Ok((
            local,
            engine,
            self.operation()?,
            Instant::now() + OPERATION_TIMEOUT,
        ))
    }
}

fn predecessor_cut(handoff: &bigbrotr::storage::rocks::store::InitialHandoff) -> Result<SourceCut> {
    let old = handoff.previous_manifest();
    let next = handoff.next_manifest();
    let id = old.identity();
    if old.key_schema() != 1
        || id.owner_epoch.get() != 1
        || id.topology_generation.get() != 0
        || next.key_schema() != 2
        || next.identity().owner_epoch.get() != 2
        || next.identity().topology_generation.get() != 1
    {
        return Err(failed(
            "Index supports only the initial topology0 to1 handoff",
        ));
    }
    SourceCut::new(
        id.dataset_id,
        id.topology_generation,
        vec![StreamBound {
            stream: StreamId {
                shard_id: id.shard_id,
                owner_epoch: id.owner_epoch,
            },
            last_complete_sequence: CompleteThrough::new(handoff.last_complete_sequence()),
        }],
    )
    .map_err(|_| failed("closed predecessor cut invalid"))
}

async fn read_predecessor(
    owner: &mut ControlSession,
    state: &IndexState,
    requests: u64,
    before: &SourceCut,
) -> Result<(IndexOwnerIdentity, IndexRecoveryState)> {
    let current = owner
        .read_index_recovery_state(state.binding())
        .await
        .map_err(|_| failed("old Index inventory unavailable"))?;
    let captured = current.1;
    if requests > 64
        || !captured.valid_head()
        || captured.published_revision == 0
        || captured.published_revision > requests
        || captured.pending_plan.is_some()
        || captured.published_plan.is_none()
        || captured.published_handoff.is_some()
    {
        return Err(failed(
            "live bridge requires a bounded published old prefix without pending work",
        ));
    }
    let head = owner
        .read_index_revision(state.binding(), captured.published_revision)
        .await
        .map_err(|_| failed("old Index head unavailable"))?;
    let Some(RetainedIndexRevision::Batch(head)) = head else {
        return Err(failed("old published batch head missing"));
    };
    let receipt = head
        .publication
        .as_ref()
        .ok_or_else(|| failed("old head not published"))?;
    if head.plan.binding() != state.binding()
        || Some(head.plan.digest()) != captured.published_plan
        || head.plan.expected_revision().checked_add(1) != Some(captured.published_revision)
        || head.plan.after() != before
        || receipt.binding != *state.binding()
        || receipt.plan_digest != head.plan.digest()
        || receipt.revision != captured.published_revision
        || &receipt.cut != before
    {
        return Err(failed(
            "Index has not published the exact closed predecessor",
        ));
    }
    Ok(current)
}
