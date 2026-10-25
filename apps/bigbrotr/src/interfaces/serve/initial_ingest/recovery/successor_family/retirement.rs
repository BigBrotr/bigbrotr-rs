//! Retirement observes existing closure and copies; it performs no repairs.
use super::*;

impl Work<'_> {
    pub(super) async fn retire(
        &self,
        custody: &mut Custody,
        metadata: OwnedPermit,
        observation: OwnedPermit,
        compacted: bool,
    ) -> Result<Value> {
        let native = custody.native.as_mut().expect("exclusive native owner");
        let owner = custody.owner.as_mut().expect("claimed Control");
        custody.retirement = Some(if compacted {
            self.operation(owner.inspect_activated_successor_retirement_with_compacted(
                native,
                self.replay_request(),
                metadata,
                self.input_grant()?,
                compacted_grant(self.budget)?,
                |d, h| self.validate(d, h),
            ))
            .await?
        } else {
            self.operation(owner.inspect_activated_successor_retirement(
                native,
                self.replay_request(),
                metadata,
                self.input_grant()?,
                |d, h| self.validate(d, h),
            ))
            .await?
        });
        let retirement = custody
            .retirement
            .as_mut()
            .expect("retained retirement owner");
        self.operation(owner.observe_activated_successor_retirement(
            native,
            retirement,
            self.request(),
            observation,
            |d, h| self.validate(d, h),
        ))
        .await?;
        // Resolution exclusively borrows the one-shot owner; retain only fixed
        // observations, never reconstruct it from the terminal SQL shape.
        let slot = retirement.producer_slot_id();
        let issued = retirement.issued_through();
        let reserved = retirement.reserved_through();
        let mut resolution = retirement
            .resolution(native, &self.cancellation.requested)
            .map_err(error)?;
        while resolution.current_epoch().is_some() {
            if resolution.needs_compacted_initial() {
                self.refresh(owner).await?;
                self.operation(
                    owner.inspect_activated_successor_retirement_compacted_initial(
                        &mut resolution,
                        self.request(),
                        |d, h| self.validate(d, h),
                    ),
                )
                .await?;
                continue;
            }
            while resolution.next_request().is_some() {
                self.refresh(owner).await?;
                let (input, outcome) = replay_grants(self.budget)?;
                self.operation(owner.inspect_next_activated_successor_retirement_request(
                    &mut resolution,
                    self.replay_request(),
                    input,
                    outcome,
                    |d, h| self.validate(d, h),
                ))
                .await?;
            }
            self.refresh(owner).await?;
            self.operation(owner.finish_activated_successor_retirement_epoch(
                &mut resolution,
                self.request(),
                |d, h| self.validate(d, h),
            ))
            .await?;
        }
        self.refresh(owner).await?;
        let proof = self
            .operation(owner.finish_activated_successor_retirement(
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
            slot,
            issued,
            reserved,
            verified: proof.verified_through(),
            last: proof.last_epoch(),
        };
        let replay = proof
            .recovery_acknowledgement(&self.cancellation.requested)
            .map_err(error)?;
        self.refresh(owner).await?;
        let compacted_requests = replay.compacted_requests();
        let gaps = if compacted {
            self.operation(owner.retire_activated_successor_producer_with_compacted(
                proof,
                self.request(),
                grant(self.budget, FINAL_JOB_BYTES, 0)?,
                |d, h| self.validate(d, h),
            ))
            .await?
        } else {
            self.operation(owner.retire_activated_successor_producer(
                proof,
                self.request(),
                grant(self.budget, FINAL_JOB_BYTES, 0)?,
                |d, h| self.validate(d, h),
            ))
            .await?
        };
        let receipt = acknowledge(
            self.root,
            self.budget,
            self.progress,
            self.cancellation,
            owner,
            native,
            replay,
        )
        .await?;
        let mut report = completed.report(native, Mode::Retire);
        if compacted_requests != 0 {
            report["recovery_basis"] = json!("closed-producer-floor-v3");
            report["compacted_requests"] = json!(compacted_requests.to_string());
        }
        report["ingest_retirement"] = json!({"allocation_domains":"9","acquisition_gaps":gaps.to_string(),
            "owner_release":"committed","native_acknowledged":receipt.acknowledged_through().to_string(),"native_retired":"0"});
        Ok(report)
    }
}
