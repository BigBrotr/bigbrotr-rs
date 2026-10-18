//! Source publication and acquisition abandonment consume distinct fresh passes.
use super::*;

impl Work<'_> {
    pub(super) async fn source(
        &self,
        custody: &mut Custody,
        metadata: OwnedPermit,
        closing: OwnedPermit,
        mode: Mode,
    ) -> Result<Value> {
        let native = custody.native.as_mut().expect("exclusive native owner");
        let owner = custody.owner.as_mut().expect("claimed Control");
        custody.source = Some(
            self.operation(owner.inspect_activated_successor_source(
                native,
                self.replay_request(),
                metadata,
                self.input_grant()?,
                |d, h| self.validate(d, h),
            ))
            .await?,
        );
        let recovered = custody.source.as_mut().expect("retained source owner");
        if mode == Mode::AbandonAcquisition && !recovered.source_closed() {
            // Refuse before the native barrier. Publication spends its owner;
            // this mode requires a fresh process and an already closed source.
            return Err((
                4,
                "successor acquisition abandonment requires an already closed source".into(),
            ));
        }
        self.operation(owner.close_activated_successor_source_producer(
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
            while resolution.next_request().is_some() {
                self.refresh(owner).await?;
                let (input, outcome) = replay_grants(self.budget)?;
                self.operation(owner.resolve_next_activated_successor_source_request(
                    &mut resolution,
                    self.replay_request(),
                    input,
                    outcome,
                    |d, h| self.validate(d, h),
                ))
                .await?;
            }
            self.refresh(owner).await?;
            self.operation(owner.finish_activated_successor_source_epoch(
                &mut resolution,
                self.request(),
                |d, h| self.validate(d, h),
            ))
            .await?;
        }
        self.refresh(owner).await?;
        let proof = self
            .operation(owner.finish_activated_successor_source(
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
        self.refresh(owner).await?;
        let jobs = if mode == Mode::AbandonAcquisition {
            Some(
                self.operation(owner.abandon_activated_successor_acquisition(
                    proof,
                    self.request(),
                    grant(self.budget, FINAL_JOB_BYTES, 0)?,
                    |d, h| self.validate(d, h),
                ))
                .await?,
            )
        } else {
            self.operation(owner.close_activated_successor_source(
                proof,
                self.request(),
                |d, h| self.validate(d, h),
            ))
            .await?;
            None
        };
        // Only a known final Result constructs the report. The spent owner and
        // its grants remain in Custody through native close and Control release.
        let mut report = completed.report(native, mode);
        if let Some(jobs) = jobs {
            report["acquisition_abandonment"] = json!({"jobs":jobs.to_string(),
                "reason":"ingest_successor_abandoned","coverage":"unknown"});
        }
        Ok(report)
    }
}
