use super::source::Source;
use super::*;
use bigbrotr::storage::postgres::{
    IndexBinding, IndexHandoff, IndexHandoffReceipt, IndexOwnerIdentity, RetainedIndexRevision,
};

/// SQL publication state only. The verifier's pending-inclusive cut never
/// enters this cursor; advance requires an actual publication receipt.
pub(super) struct Published {
    pub(super) revision: u64,
    pub(super) cut: SourceCut,
}
impl Published {
    pub(super) fn captured(
        binding: &IndexBinding,
        state: IndexRecoveryState,
        head: Option<&RetainedIndexRevision>,
        complete: &SourceCut,
    ) -> Result<Self> {
        if !state.valid_head() {
            return Err(error("invalid typed head"));
        }
        let first = complete
            .bounds()
            .first()
            .ok_or_else(|| error("complete source inventory missing"))?;
        let value = match (
            state.published_revision,
            state.published_plan,
            state.published_handoff,
            head,
        ) {
            (0, None, None, None) => Self {
                revision: 0,
                cut: SourceCut::new(
                    binding.dataset(),
                    bigbrotr_model::TopologyGeneration::new(0),
                    vec![StreamBound {
                        stream: first.stream,
                        last_complete_sequence: CompleteThrough::new(0),
                    }],
                )
                .map_err(error)?,
            },
            (revision, Some(digest), None, Some(RetainedIndexRevision::Batch(head)))
                if revision > 0 =>
            {
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
            (revision, None, Some(digest), Some(RetainedIndexRevision::Handoff(receipt)))
                if revision > 0 =>
            {
                let h = &receipt.handoff;
                if h.binding() != binding
                    || h.digest() != digest
                    || h.expected_revision().checked_add(1) != Some(revision)
                {
                    return Err(error("published handoff head identity differs"));
                }
                Self {
                    revision,
                    cut: h.after().clone(),
                }
            }
            _ => return Err(error("published head missing or inconsistent")),
        };
        if !source::within(&value.cut, complete) {
            return Err(error("publication exceeds closed source"));
        }
        Ok(value)
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
    pub(super) fn accept_handoff(
        &mut self,
        binding: &IndexBinding,
        publisher: IndexOwnerIdentity,
        expected: &IndexHandoff,
        receipt: IndexHandoffReceipt,
    ) -> Result<()> {
        if receipt.handoff != *expected
            || expected.binding() != binding
            || expected.expected_revision() != self.revision
            || expected.before() != &self.cut
            || receipt.publisher != publisher
            || !receipt.created
        {
            return Err(error("new handoff receipt differs"));
        }
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| error("revision exhausted"))?;
        self.cut = expected.after().clone();
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    root: &Resolved,
    cancel: &Cancellation,
    native: &IngestRecovery,
    budget: &ResourceBudget,
    prepared: &PreparedIndex,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    state: &IndexState,
    source: &Source<'_>,
    profile: Profile,
) -> Result<Value> {
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
    if !history_bound(
        source.requests(),
        captured.published_revision,
        captured.pending_plan.is_some(),
        source.epoch() == 2,
        source.compacted_requests(),
    ) {
        return Err(error(
            "plan history exceeds the finite original request prefix",
        ));
    }
    let previous = verifier.captured_owner();
    let head = match captured.published_revision {
        revision if revision > 0 => {
            authority
                .operation(owner.read_index_revision(state.binding(), revision))
                .await?
        }
        _ => None,
    };
    let mut published =
        Published::captured(state.binding(), captured, head.as_ref(), source.complete())?;
    drop(head);
    let bridge = if source.epoch() == 2 && published.cut.topology_generation().get() == 0 {
        if captured.pending_plan.is_some() {
            return Err(error(
                "old pending plan must be published before initial handoff",
            ));
        }
        Some(source.handoff(*state.binding(), published.revision, &published.cut)?)
    } else {
        None
    };
    let mut batch_count = 0_u64;
    let mut handoff_revision = None;
    // Published history is verify-only. Missing/corrupt accepted rows refuse;
    // this finite path does not silently repair a damaged published generation.
    for _ in 0..captured.published_revision {
        authority.refresh(owner, None, state).await?;
        authority.local(|cancel| source.validate(cancel))?;
        if let Some(receipt) = authority
            .operation(verifier.advance_handoff(owner, state.custody()))
            .await?
        {
            if handoff_revision.is_some()
                || source.handoff(
                    *state.binding(),
                    receipt.handoff.expected_revision(),
                    receipt.handoff.before(),
                )? != receipt.handoff
            {
                return Err(error(
                    "retained topology bridge differs from native source history",
                ));
            }
            handoff_revision = Some(receipt.handoff.expected_revision() + 1);
            continue;
        }
        batch_count += 1;
        if batch_count > source.requests() {
            return Err(error("batch history exceeds request census"));
        }
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
    if source.epoch() == 2
        && published.cut.topology_generation().get() == 1
        && handoff_revision.is_none()
    {
        return Err(error(
            "topology1 publication history has no explicit bridge",
        ));
    }
    // Retain just one immutable pending metadata value, charged by the fixed
    // owner allowance. Native bytes/planner grants are reacquired as necessary.
    let pending = if let Some(digest) = captured.pending_plan {
        if batch_count >= source.requests() {
            return Err(error("pending batch exceeds request census"));
        }
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
    authority.local(|cancel| source.validate(cancel))?;
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
    authority.local(|cancel| source.validate(cancel))?;
    let session = authority
        .operation(owner.recover_index(
            state.binding(),
            state.custody(),
            recovered.proof(),
            &Cell::new(false),
        ))
        .await?;
    let published_handoff = bridge.is_some();
    if let Some(bridge) = bridge {
        authority.refresh(owner, Some(&session), state).await?;
        authority.local(|cancel| source.validate(cancel))?;
        let compacted = source.compacted_request(root, cancel)?;
        let receipt = authority
            .operation(
                owner.publish_index_handoff(
                    &session,
                    state.custody(),
                    &bridge,
                    recovered.proof(),
                    source
                        .successor_proof()
                        .ok_or_else(|| error("source history missing"))?,
                    native,
                    InitialHistoryRequest {
                        ingest_root: Path::new(required(root, "storage.ingest_root")?),
                        policy_ttl: duration(root, "authorization_ttl"),
                        cancelled: &cancel.requested,
                    },
                    compacted.as_ref(),
                    &Cell::new(false),
                    |d, h| validate(root, profile, d, h),
                ),
            )
            .await?;
        published.accept_handoff(session.binding(), session.owner(), &bridge, receipt)?;
        handoff_revision = Some(published.revision);
    }
    // Rebinding itself preserves the published cursor. A verified pending cut
    // remains pending until its own positive PostgreSQL publication COMMIT.
    if let Some(pending) = &pending {
        authority.refresh(owner, Some(&session), state).await?;
        let planned = planned(
            budget,
            authority,
            state,
            source,
            profile,
            published.revision,
            &published.cut,
        )?;
        if planned.plan() != pending {
            return Err(error("pending replay identity changed"));
        }
        let prepared_plan = authority
            .operation(source.prepare_index_plan(
                root,
                cancel,
                owner,
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
        authority.local(|cancel| source.validate(cancel))?;
        let receipt = authority
            .operation(source.publish_index_plan(
                root,
                cancel,
                owner,
                &session,
                state.custody(),
                &prepared_plan,
                proof,
                &Cell::new(false),
            ))
            .await?;
        published.accept(session.binding(), session.owner(), pending, receipt)?;
        batch_count += 1;
    } else if recovered.pending().is_some() {
        return Err(error("unexpected pending physical proof"));
    }
    drop(joined);
    let mut new_batches = 0_u64;
    while published
        .cut
        .bounds()
        .last()
        .unwrap()
        .last_complete_sequence
        .get()
        < source.through()
    {
        if batch_count >= source.requests() {
            return Err(error("source batch census exceeded"));
        }
        authority.refresh(owner, Some(&session), state).await?;
        let planned = planned(
            budget,
            authority,
            state,
            source,
            profile,
            published.revision,
            &published.cut,
        )?;
        let prepared_plan = authority
            .operation(source.prepare_index_plan(
                root,
                cancel,
                owner,
                &session,
                state.custody(),
                planned.plan(),
                planned.source(),
                &Cell::new(false),
            ))
            .await?;
        authority.local(|cancel| source.validate(cancel))?;
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
        authority.local(|cancel| source.validate(cancel))?;
        let receipt = authority
            .operation(source.publish_index_plan(
                root,
                cancel,
                owner,
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
        batch_count += 1;
    }
    authority.refresh(owner, Some(&session), state).await?;
    authority.local(|cancel| source.validate(cancel))?;
    let (current_owner, current) = authority
        .operation(owner.read_index_recovery_state(state.binding()))
        .await?;
    if current_owner != session.owner()
        || current.published_revision != published.revision
        || current.pending_plan.is_some()
    {
        return Err(error("final PostgreSQL head changed"));
    }
    let head = match current.published_revision {
        revision if revision > 0 => {
            authority
                .operation(owner.read_index_revision(state.binding(), revision))
                .await?
        }
        _ => None,
    };
    let final_prefix =
        Published::captured(state.binding(), current, head.as_ref(), source.complete())?;
    if final_prefix.cut != published.cut || &final_prefix.cut != source.complete() {
        return Err(error("final publication does not cover the closed source"));
    }
    let mut report = json!({
        "format":"bigbrotr-initial-index-catch-up/2", "state":"complete",
        "dataset_id":state.binding().dataset().to_string(),
        "generation_id":state.binding().generation().to_string(),
        "storage_incarnation":state.binding().storage_incarnation().to_string(),
        "table_identity_digest":state.binding().table_identity_digest().to_string(),
        "previous_owner_epoch":previous.epoch.get(), "owner_epoch":session.owner().epoch.get(),
        "published_revision_before":captured.published_revision,
        "published_revision_after":published.revision,
        "source_epoch":source.epoch(), "source_through":source.through(),
        "published_through":published.cut.bounds().last().unwrap().last_complete_sequence.get(),
        "source_cut":cut_json(source.complete()), "published_cut":cut_json(&published.cut),
        "index_handoff_revision":handoff_revision, "published_handoff":published_handoff,
        "recovered_pending":pending.is_some(), "new_batches":new_batches,
        "ready":false, "acknowledgement":"not-performed", "ingest_retirement":"not-performed",
        "topology_transition":if source.epoch() == 1 { "not-performed" } else { "existing-ingest-handoff-verified" }
    });
    if source.compacted_requests() != 0 {
        report["recovery_basis"] = json!("closed-producer-floor-v3");
        report["compacted_requests"] = json!(source.compacted_requests().to_string());
    }
    Ok(report)
}

pub(super) fn history_bound(
    requests: u64,
    published: u64,
    pending: bool,
    successor: bool,
    compacted_initial: u64,
) -> bool {
    compacted_initial <= 64
        && requests <= 64 + compacted_initial
        && published
            .checked_add(u64::from(pending))
            .is_some_and(|revisions| revisions <= requests + u64::from(successor))
}

#[allow(clippy::too_many_arguments)]
fn planned(
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    state: &IndexState,
    source: &Source<'_>,
    profile: Profile,
    revision: u64,
    before: &SourceCut,
) -> Result<PlannedBatch> {
    authority.local(|cancel| {
        let pending = source.read(
            before,
            profile.source(),
            grant(
                budget,
                profile.source().required_data_bytes().map_err(error)?,
                0,
            )?,
            cancel,
        )?;
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

fn cut_json(cut: &SourceCut) -> Value {
    json!({"topology_generation":cut.topology_generation().get(), "bounds":cut.bounds().iter().map(|bound|
        json!({"shard_id":bound.stream.shard_id,"owner_epoch":bound.stream.owner_epoch.get(),
            "last_complete_sequence":bound.last_complete_sequence.get()})).collect::<Vec<_>>()})
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
