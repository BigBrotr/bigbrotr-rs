use super::*;
use bigbrotr::storage::postgres::{IndexBinding, IndexOwnerIdentity, RetainedIndexPlan};

/// SQL publication state only. The verifier's pending-inclusive cut never
/// enters this cursor; advance requires an actual publication receipt.
pub(super) struct Published {
    pub(super) revision: u64,
    pub(super) cut: SourceCut,
}
impl Published {
    pub(super) fn captured(
        binding: &IndexBinding,
        identity: ManifestIdentity,
        state: IndexRecoveryState,
        head: Option<&RetainedIndexPlan>,
        through: u64,
    ) -> Result<Self> {
        let value = match (state.published_revision, state.published_plan, head) {
            (0, None, None) => Self {
                revision: 0,
                cut: zero_cut(identity)?,
            },
            (revision, Some(digest), Some(head)) if revision > 0 => {
                let receipt = head
                    .publication
                    .as_ref()
                    .ok_or_else(|| error("head is not published"))?;
                if head.plan.binding() != binding
                    || head.plan.digest() != digest
                    || head.plan.expected_revision().checked_add(1) != Some(revision)
                    || receipt.binding != *binding
                    || receipt.plan_digest != digest
                    || receipt.revision != revision
                    || receipt.cut != *head.plan.after()
                {
                    return Err(error("published head identity differs"));
                }
                Self {
                    revision,
                    cut: receipt.cut.clone(),
                }
            }
            _ => return Err(error("published head missing or inconsistent")),
        };
        value.require_within(identity, through)?;
        Ok(value)
    }
    fn require_within(&self, identity: ManifestIdentity, through: u64) -> Result<()> {
        let next = next_source(identity, &self.cut)?;
        if next.sequence.get() - 1 > through {
            return Err(error("publication exceeds closed source"));
        }
        Ok(())
    }
    pub(super) fn accept(
        &mut self,
        binding: &IndexBinding,
        publisher: IndexOwnerIdentity,
        plan: &IndexPlan,
        receipt: IndexPublicationReceipt,
    ) -> Result<()> {
        if plan.binding() != binding
            || plan.expected_revision() != self.revision
            || plan.before() != &self.cut
            || receipt.binding != *binding
            || receipt.plan_digest != plan.digest()
            || self.revision.checked_add(1) != Some(receipt.revision)
            || receipt.cut != *plan.after()
            || receipt.family_counts != std::array::from_fn(|i| plan.chunks()[i].row_count())
            || receipt.publisher != publisher
            || !receipt.created
        {
            return Err(error("new publication receipt differs"));
        }
        self.revision = receipt.revision;
        self.cut = receipt.cut;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    budget: &ResourceBudget,
    prepared: &PreparedIndex,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    state: &IndexState,
    source: &ResolvedSource<'_>,
    manifest: IngestManifest,
    profile: Profile,
) -> Result<Value> {
    let identity = *manifest.identity();
    let physical = profile.physical()?;
    let adapter = IndexAdapter::new(
        Arc::new(state.tables().clone()),
        Arc::clone(prepared.writer()),
        physical,
        Arc::new(IndexEngineSlot::new()),
    );
    let mut operation_number = 0_u64;
    let mut verifier = authority
        .operation(IndexRecoveryVerifier::begin(
            adapter.clone(),
            owner,
            state.custody(),
            grant(budget, required_recovery_metadata_bytes(), 0)?,
        ))
        .await?;
    let captured = verifier.captured_state();
    if captured.published_revision > source.resolved_requests()
        || (captured.pending_plan.is_some()
            && captured.published_revision >= source.resolved_requests())
    {
        return Err(error(
            "plan history exceeds the finite original request prefix",
        ));
    }
    let previous = verifier.captured_owner();
    let head = match captured.published_plan {
        Some(digest) => {
            authority
                .operation(owner.read_index_plan(state.binding(), digest))
                .await?
        }
        None => None,
    };
    let mut published = Published::captured(
        state.binding(),
        identity,
        captured,
        head.as_ref(),
        source.last_complete_sequence(),
    )?;
    drop(head);
    // Published history is verify-only. Missing/corrupt accepted rows refuse;
    // this finite path does not silently repair a damaged published generation.
    for _ in 0..captured.published_revision {
        authority.refresh(owner, None, state).await?;
        authority.local(|cancel| source.validate_initial(cancel).map_err(error))?;
        let (local, engine, id, deadline) =
            dispatch(budget, prepared, physical, owner, &mut operation_number)?;
        let operation = authority
            .operation(verifier.next(owner, state.custody(), None, local, engine, id, deadline))
            .await?;
        let joined = authority
            .join(owner, None, state, &adapter, operation, deadline)
            .await?;
        let (outcome, reservation) = joined.into_parts();
        if let IndexOperationOutcome::Completed(Ok(proof)) = outcome {
            verifier.accept(proof).map_err(error)?;
        } else {
            return Err(error("published proof unavailable"));
        }
        drop(reservation);
    }
    // Retain just one immutable pending metadata value, charged by the fixed
    // owner allowance. Native bytes/planner grants are reacquired as necessary.
    let pending = if let Some(digest) = captured.pending_plan {
        authority.refresh(owner, None, state).await?;
        let retained = authority
            .operation(owner.read_index_plan(state.binding(), digest))
            .await?
            .ok_or_else(|| error("retained pending plan missing"))?;
        if retained.publication.is_some()
            || retained.plan.digest() != digest
            || retained.plan.expected_revision() != published.revision
            || retained.plan.before() != &published.cut
        {
            return Err(error("pending plan changed publication boundary"));
        }
        let planned = planned(
            budget,
            authority,
            state,
            source,
            identity,
            profile,
            published.revision,
            &published.cut,
        )?;
        if planned.plan() != &retained.plan {
            return Err(error("pending source/mapping changed"));
        }
        let (local, engine, id, deadline) =
            dispatch(budget, prepared, physical, owner, &mut operation_number)?;
        let operation = authority
            .operation(verifier.next(
                owner,
                state.custody(),
                Some(planned),
                local,
                engine,
                id,
                deadline,
            ))
            .await?;
        let joined = authority
            .join(owner, None, state, &adapter, operation, deadline)
            .await?;
        let (outcome, reservation) = joined.into_parts();
        if let IndexOperationOutcome::Completed(Ok(proof)) = outcome {
            verifier.accept(proof).map_err(error)?;
        } else {
            return Err(error("pending proof unavailable"));
        }
        drop(reservation);
        Some(retained.plan)
    } else {
        None
    };
    authority.refresh(owner, None, state).await?;
    authority.local(|cancel| source.validate_initial(cancel).map_err(error))?;
    let (local, engine, id, deadline) =
        dispatch(budget, prepared, physical, owner, &mut operation_number)?;
    let operation = authority
        .operation(verifier.finish(owner, state.custody(), local, engine, id, deadline))
        .await?;
    let joined = authority
        .join(owner, None, state, &adapter, operation, deadline)
        .await?;
    let recovered = match joined.outcome() {
        IndexOperationOutcome::Completed(Ok(result)) => result,
        _ => return Err(error("complete recovery proof unavailable")),
    };
    authority.refresh(owner, None, state).await?;
    authority.local(|cancel| source.validate_initial(cancel).map_err(error))?;
    let session = authority
        .operation(owner.recover_index(
            state.binding(),
            state.custody(),
            recovered.proof(),
            &Cell::new(false),
        ))
        .await?;
    // Rebinding itself preserves the published cursor. A verified pending cut
    // remains pending until its own positive PostgreSQL publication COMMIT.
    if let Some(pending) = &pending {
        authority.refresh(owner, Some(&session), state).await?;
        let planned = planned(
            budget,
            authority,
            state,
            source,
            identity,
            profile,
            published.revision,
            &published.cut,
        )?;
        if planned.plan() != pending {
            return Err(error("pending replay identity changed"));
        }
        let prepared_plan = authority
            .operation(owner.prepare_index_plan(
                &session,
                state.custody(),
                pending,
                planned.source(),
                &Cell::new(false),
            ))
            .await?;
        let proof = recovered
            .pending()
            .ok_or_else(|| error("pending physical proof missing"))?;
        authority.local(|cancel| source.validate_initial(cancel).map_err(error))?;
        let receipt = authority
            .operation(owner.publish_index_plan(
                &session,
                state.custody(),
                &prepared_plan,
                proof,
                &Cell::new(false),
            ))
            .await?;
        published.accept(session.binding(), session.owner(), pending, receipt)?;
    } else if recovered.pending().is_some() {
        return Err(error("unexpected pending physical proof"));
    }
    drop(joined);
    let mut new_batches = 0_u64;
    while published.cut.bounds()[0].last_complete_sequence.get() < source.last_complete_sequence() {
        if published.revision >= source.resolved_requests() {
            return Err(error("source batch census exceeded"));
        }
        authority.refresh(owner, Some(&session), state).await?;
        let planned = planned(
            budget,
            authority,
            state,
            source,
            identity,
            profile,
            published.revision,
            &published.cut,
        )?;
        let prepared_plan = authority
            .operation(owner.prepare_index_plan(
                &session,
                state.custody(),
                planned.plan(),
                planned.source(),
                &Cell::new(false),
            ))
            .await?;
        authority.local(|cancel| source.validate_initial(cancel).map_err(error))?;
        let (local, engine, id, deadline) =
            dispatch(budget, prepared, physical, owner, &mut operation_number)?;
        let operation = adapter
            .write_and_verify(
                state.custody(),
                &prepared_plan,
                planned,
                local,
                engine,
                id,
                deadline,
            )
            .map_err(error)?;
        let joined = authority
            .join(owner, Some(&session), state, &adapter, operation, deadline)
            .await?;
        let proof = match joined.outcome() {
            IndexOperationOutcome::Completed(Ok(proof)) => proof,
            _ => return Err(error("new batch proof missing")),
        };
        authority.refresh(owner, Some(&session), state).await?;
        authority.local(|cancel| source.validate_initial(cancel).map_err(error))?;
        let receipt = authority
            .operation(owner.publish_index_plan(
                &session,
                state.custody(),
                &prepared_plan,
                proof,
                &Cell::new(false),
            ))
            .await?;
        published.accept(
            session.binding(),
            session.owner(),
            prepared_plan.plan(),
            receipt,
        )?;
        new_batches += 1;
    }
    authority.refresh(owner, Some(&session), state).await?;
    authority.local(|cancel| source.validate_initial(cancel).map_err(error))?;
    let (current_owner, current) = authority
        .operation(owner.read_index_recovery_state(state.binding()))
        .await?;
    if current_owner != session.owner()
        || current.published_revision != published.revision
        || current.pending_plan.is_some()
    {
        return Err(error("final PostgreSQL head changed"));
    }
    let head = match current.published_plan {
        Some(digest) => {
            authority
                .operation(owner.read_index_plan(state.binding(), digest))
                .await?
        }
        None => None,
    };
    let final_prefix = Published::captured(
        state.binding(),
        identity,
        current,
        head.as_ref(),
        source.last_complete_sequence(),
    )?;
    if final_prefix.cut != published.cut
        || final_prefix.cut.bounds()[0].last_complete_sequence.get()
            != source.last_complete_sequence()
    {
        return Err(error("final publication does not cover the closed source"));
    }
    Ok(json!({
        "format":"bigbrotr-initial-index-catch-up/1", "state":"complete",
        "dataset_id":state.binding().dataset().to_string(),
        "generation_id":state.binding().generation().to_string(),
        "storage_incarnation":state.binding().storage_incarnation().to_string(),
        "table_identity_digest":state.binding().table_identity_digest().to_string(),
        "previous_owner_epoch":previous.epoch.get(), "owner_epoch":session.owner().epoch.get(),
        "published_revision_before":captured.published_revision,
        "published_revision_after":published.revision,
        "source_through":source.last_complete_sequence(),
        "published_through":published.cut.bounds()[0].last_complete_sequence.get(),
        "recovered_pending":pending.is_some(), "new_batches":new_batches,
        "ready":false, "acknowledgement":"not-performed", "ingest_retirement":"not-performed",
        "topology_transition":"not-performed"
    }))
}

#[allow(clippy::too_many_arguments)]
fn planned(
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    state: &IndexState,
    source: &ResolvedSource<'_>,
    identity: ManifestIdentity,
    profile: Profile,
    revision: u64,
    before: &SourceCut,
) -> Result<PlannedBatch> {
    authority.local(|cancel| {
        let pending = source
            .read_complete_pending_batch(
                next_source(identity, before)?,
                profile.source(),
                grant(
                    budget,
                    profile.source().required_data_bytes().map_err(error)?,
                    0,
                )?,
                cancel,
            )
            .map_err(error)?;
        let planned = plan::plan_batch(
            *state.binding(),
            revision,
            before.clone(),
            pending,
            profile.source(),
            grant(
                budget,
                plan::required_data_bytes(profile.source()).map_err(error)?,
                0,
            )?,
            cancel,
        )
        .map_err(error)?;
        check_plan_bounds(&planned, profile)?;
        Ok(planned)
    })
}

fn dispatch(
    budget: &ResourceBudget,
    prepared: &PreparedIndex,
    limits: PhysicalLimits,
    owner: &ControlSession,
    number: &mut u64,
) -> Result<(OwnedPermit, OwnedPermit, IndexOperationId, Instant)> {
    *number = number
        .checked_add(1)
        .ok_or_else(|| error("operation counter exhausted"))?;
    let local = grant(
        budget,
        limits.required_local_data_bytes().map_err(error)?,
        limits.required_local_native_bytes(),
    )?;
    let engine = grant(prepared.engine(), limits.required_engine_data_bytes(), 0)?;
    let id = IndexOperationId::new(
        owner.identity().process_incarnation,
        NonZeroU64::new(*number).unwrap(),
    );
    Ok((local, engine, id, Instant::now() + OPERATION_TIMEOUT))
}
