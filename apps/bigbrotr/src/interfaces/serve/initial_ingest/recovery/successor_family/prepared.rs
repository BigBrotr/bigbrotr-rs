//! Complete only a retained unactivated attempt; never create or adopt a writer.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{SuccessorHistoryState, VerifiedSuccessorHistory},
    rocks::manifest::ManifestIdentity,
};

impl Work<'_> {
    pub(super) async fn retire_empty(
        &self,
        custody: &mut Custody,
        metadata: OwnedPermit,
        mutation: OwnedPermit,
    ) -> Result<Value> {
        self.inspect_prepared(custody, metadata).await?;
        let history = custody.history.as_ref().expect("retained history");
        let target = history.registered_epoch();
        let closing = if history.state() == SuccessorHistoryState::PendingNative {
            {
                let plan = {
                    let native = custody.native.as_ref().expect("exclusive native owner");
                    let owner = custody.owner.as_mut().expect("claimed Control");
                    let (proof, _) = self.prepared_prefix(owner, native, history).await?;
                    self.refresh(owner).await?;
                    if history.lineage().has_compacted_initial() {
                        self.operation(owner.plan_prepared_compacted_successor_gap(
                            native,
                            &proof,
                            self.request(),
                            limits().event,
                            compacted_grant(self.budget)?,
                            |d, h| self.validate(d, h),
                        ))
                        .await?
                    } else {
                        self.operation(owner.plan_prepared_successor_gap(
                            native,
                            &proof,
                            self.request(),
                            |d, h| self.validate(d, h),
                        ))
                        .await?
                    }
                };
                custody.gap = Some(plan);
                let plan = custody.gap.as_ref().expect("retained gap plan");
                // The borrowed proof has ended. The fixed plan still binds the old
                // positive history and the exact immutable, already committed intent.
                let owner = custody.owner.as_mut().expect("claimed Control");
                let native = custody.native.as_mut().expect("exclusive native owner");
                let observed = if history.lineage().has_compacted_initial() {
                    self.operation(owner.complete_prepared_compacted_successor_gap(
                        native,
                        history,
                        plan,
                        self.request(),
                        mutation,
                        |d, h| self.validate(d, h),
                    ))
                    .await?
                } else {
                    self.operation(owner.complete_prepared_successor_gap(
                        native,
                        history,
                        plan,
                        self.request(),
                        mutation,
                        |d, h| self.validate(d, h),
                    ))
                    .await?
                };
                drop(observed);
            }
            // A confirmed four-put handoff makes the old lineage snapshot stale.
            // Release its one allocation and the observed mutation grant before
            // taking their replacements. Never rebase or reuse the old witness.
            drop(custody.gap.take());
            drop(custody.history.take());
            let metadata = grant(self.budget, MAX_LINEAGE_METADATA_BYTES, 0)?;
            let closing = grant(self.budget, 4096, 4096)?;
            self.inspect_prepared(custody, metadata).await?;
            let fresh = custody.history.as_ref().expect("fresh prepared history");
            if fresh.registered_epoch() != target
                || fresh.state() != SuccessorHistoryState::PreparedOpen
            {
                return Err((4, "completed successor gap has an incompatible head".into()));
            }
            closing
        } else {
            // Only a native gap needs 8KiB. Empty close uses its actual 4KiB
            // grant; terminal observation never invokes a native mutation.
            drop(mutation);
            grant(self.budget, 4096, 4096)?
        };
        self.finish_empty(custody, closing).await
    }

    async fn inspect_prepared(&self, custody: &mut Custody, metadata: OwnedPermit) -> Result<()> {
        custody.history = Some(
            self.operation(
                custody
                    .owner
                    .as_mut()
                    .expect("claimed Control")
                    .inspect_successor_history_with_compacted(
                        custody.native.as_ref().expect("exclusive native owner"),
                        self.request(),
                        limits().event,
                        metadata,
                        compacted_grant(self.budget)?,
                        |d, h| self.validate(d, h),
                    ),
            )
            .await?,
        );
        // Native epoch2 is accepted solely to reach a real pending epoch3. A
        // terminal genesis alone is not an empty successor attempt or retry.
        if custody
            .history
            .as_ref()
            .expect("retained history")
            .registered_epoch()
            < 3
        {
            return Err((
                4,
                "empty successor recovery requires a retained target epoch3 or later".into(),
            ));
        }
        Ok(())
    }

    async fn finish_empty(&self, custody: &mut Custody, closing: OwnedPermit) -> Result<Value> {
        let (plan, completed) = {
            let native = custody.native.as_ref().expect("exclusive native owner");
            let history = custody.history.as_ref().expect("retained history");
            let owner = custody.owner.as_mut().expect("claimed Control");
            let (proof, predecessor) = self.prepared_prefix(owner, native, history).await?;
            let completed = EmptyCompleted {
                target: *native.manifest().identity(),
                slot: history.producer_slot_id(),
                verified: proof.verified_through(),
                predecessor,
                compacted: history
                    .lineage()
                    .has_compacted_initial()
                    .then(|| proof.compacted_initial_requests()),
            };
            self.refresh(owner).await?;
            match history.state() {
                SuccessorHistoryState::Terminal => {
                    // This adapter requires a full-through-head UnactivatedEmpty
                    // proof. An activated terminal head cannot become this retry.
                    let retired_at = if history.lineage().has_compacted_initial() {
                        self.operation(owner.observe_retired_compacted_empty_successor(
                            native,
                            &proof,
                            self.request(),
                            limits().event,
                            compacted_grant(self.budget)?,
                            |d, h| self.validate(d, h),
                        ))
                        .await?
                    } else {
                        self.operation(owner.observe_retired_empty_successor(
                            native,
                            &proof,
                            self.request(),
                            |d, h| self.validate(d, h),
                        ))
                        .await?
                    };
                    return Ok(completed.report(retired_at, proof.last_epoch().digest));
                }
                SuccessorHistoryState::PreparedOpen | SuccessorHistoryState::PreparedClosing => {
                    let plan = if history.lineage().has_compacted_initial() {
                        self.operation(owner.plan_compacted_empty_successor_retirement(
                            native,
                            &proof,
                            self.request(),
                            limits().event,
                            compacted_grant(self.budget)?,
                            |d, h| self.validate(d, h),
                        ))
                        .await?
                    } else {
                        self.operation(owner.plan_empty_successor_retirement(
                            native,
                            &proof,
                            self.request(),
                            |d, h| self.validate(d, h),
                        ))
                        .await?
                    };
                    (plan, completed)
                }
                SuccessorHistoryState::PendingNative => {
                    return Err((4, "empty successor native gap remains incomplete".into()));
                }
            }
        };
        // End the positive borrow before the native barrier. The adapter checks
        // the sealed plan across its exact +1/+0 native delta and SQL COMMIT;
        // the ordinary history snapshot remains stale afterward, held only for
        // custody until native closes. No validation or rearming follows here.
        custody.empty = Some(plan);
        let plan = custody.empty.as_ref().expect("retained empty plan");
        let owner = custody.owner.as_mut().expect("claimed Control");
        let native = custody.native.as_mut().expect("exclusive native owner");
        let history = custody.history.as_ref().expect("retained history");
        let retired_at = if history.lineage().has_compacted_initial() {
            self.operation(owner.retire_compacted_empty_successor(
                native,
                history,
                plan,
                self.request(),
                closing,
                |d, h| self.validate(d, h),
            ))
            .await?
        } else {
            self.operation(owner.retire_empty_successor(
                native,
                history,
                plan,
                self.request(),
                closing,
                |d, h| self.validate(d, h),
            ))
            .await?
        };
        Ok(completed.report(retired_at, plan.closure_digest()))
    }

    async fn prepared_prefix<'a>(
        &'a self,
        owner: &mut ControlSession,
        native: &'a IngestRecovery,
        history: &'a SuccessorHistory,
    ) -> Result<(VerifiedSuccessorHistory<'a>, LineageEpochSummary)> {
        let mut resolution = history
            .resolution(native, &self.cancellation.requested)
            .map_err(error)?;
        let mut predecessor = None;
        while resolution.current_epoch().is_some() {
            if resolution.needs_compacted_initial() {
                self.refresh(owner).await?;
                self.operation(owner.inspect_successor_compacted_initial(
                    &mut resolution,
                    self.request(),
                    limits().event,
                    compacted_grant(self.budget)?,
                    |d, h| self.validate(d, h),
                ))
                .await?;
                continue;
            }
            while resolution.next_request().is_some() {
                self.refresh(owner).await?;
                let (input, outcome) = replay_grants(self.budget)?;
                self.operation(owner.inspect_next_successor_request(
                    &mut resolution,
                    self.replay_request(),
                    input,
                    outcome,
                    |d, h| self.validate(d, h),
                ))
                .await?;
            }
            self.refresh(owner).await?;
            let summary = self
                .operation(owner.finish_successor_history_epoch(
                    &mut resolution,
                    self.request(),
                    |d, h| self.validate(d, h),
                ))
                .await?;
            if summary.epoch + 1 == history.registered_epoch() {
                predecessor = Some(summary);
            }
        }
        self.refresh(owner).await?;
        let proof = self
            .operation(owner.finish_successor_history(
                resolution,
                self.request(),
                limits().event,
                if history.lineage().has_compacted_initial() {
                    compacted_grant(self.budget)?
                } else {
                    effect_grant(self.budget)?
                },
                |d, h| self.validate(d, h),
            ))
            .await?;
        proof
            .validate(&self.cancellation.requested)
            .map_err(error)?;
        let predecessor = predecessor.ok_or_else(|| {
            (
                4,
                "empty successor predecessor was not positively replayed".into(),
            )
        })?;
        Ok((proof, predecessor))
    }
}

// Fixed observed coordinates only. This report is delivered by the parent after
// native close, Control release and the actual driver join, never by a plan.
struct EmptyCompleted {
    target: ManifestIdentity,
    slot: ProducerSlotId,
    verified: u64,
    predecessor: LineageEpochSummary,
    compacted: Option<u64>,
}
impl EmptyCompleted {
    fn report(self, retired_at: i64, digest: [u8; 32]) -> Value {
        let target = self.target;
        let predecessor = self.predecessor;
        let mut report = json!({"format":"bigbrotr-ingest-recovery/1","state":"successor-empty-attempt-retired",
            "dataset_id":target.dataset_id,"owner_resource_id":target.owner_resource_id,
            "producer_slot_id":self.slot,"producer_epoch":target.owner_epoch.get().to_string(),
            "issued_through":"0","reserved_through":"0","resolved_requests":"0",
            "verified_through":self.verified.to_string(),
            "predecessor":{"producer_epoch":predecessor.epoch.to_string(),
                "resolved_requests":predecessor.resolved_requests.to_string(),
                "last_complete_sequence":predecessor.last_complete_sequence.to_string(),
                "sha256":Digest256::from_bytes(predecessor.digest)},
            "empty_attempt_retirement":{"owner_epoch":target.owner_epoch.get().to_string(),
                "incarnation":target.process_incarnation,"retired_at_us":retired_at.to_string(),
                "sha256":Digest256::from_bytes(digest),"allocation_domains":"9",
                "native_acknowledged":"0","native_retired":"0","owner_release":"committed"},
            "native":"closed","owner_release":"committed","ready":false});
        if let Some(count) = self.compacted {
            report["recovery_basis"] = json!("closed-producer-floor-v3");
            report["compacted_requests"] = json!(count.to_string());
        }
        report
    }
}
