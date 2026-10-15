//! Finite observe-only retirement of an already closed activated successor.
//! All native and SQL custody remains in the host through success or refusal.
use super::*;
pub(super) use bigbrotr::storage::postgres::control_owner::ActivatedSuccessorRetirement;
use recovery::staged_value;

const RETIREMENT_BYTES: u64 = 128 * 1024;

pub(super) fn preflight(budget: &ResourceBudget) {
    drop(grant(budget, RETIREMENT_BYTES, 0));
    emit(serde_json::json!({"event":"lineage-retirement-resources",
        "data_bytes":RETIREMENT_BYTES,"native_bytes":0,"max_jobs":64,
        "source_closure":false,"workflow":false,"admission":false}));
}

impl Probe {
    pub(super) async fn retire_successor(
        &self,
        owner: &mut ControlSession,
        native: &mut Native,
        metadata: OwnedPermit,
        observation: OwnedPermit,
    ) -> Result<serde_json::Value, Failure> {
        self.snapshot("before-retirement").await;
        let result = self
            .bounded(
                "retirement-inspect",
                owner.inspect_activated_successor_retirement(
                    native.recovery(),
                    self.recovery_request(),
                    metadata,
                    grant(
                        &self.budget,
                        RetainedRequestInput::required_data_bytes(recovery_limits().input).unwrap(),
                        0,
                    ),
                    |d, h| self.validate(d, h),
                ),
            )
            .await
            .map(|retirement| native.retirement = Some(retirement));
        self.snapshot("retirement-inspect-returned").await;
        result.map_err(|error| Failure::at("inspect", None, None, error))?;
        let retirement = native.retirement.as_ref().unwrap();
        let epoch = retirement.epoch();
        emit(
            serde_json::json!({"event":"lineage-retirement-loaded","epoch":epoch,
            "manifest":hex(native.recovery().manifest().encode().as_bytes()),
            "producer_slot":hex(retirement.producer_slot_id().as_bytes()),
            "issued_through":retirement.issued_through(),"reserved_through":retirement.reserved_through(),
            "staged_input":staged_value(retirement.staged_input()),
            "barrier_confirmed":retirement.is_closed(),"retired_at_us":retirement.retired_at_us()}),
        );
        self.snapshot("before-retirement-observe").await;
        let result = self
            .bounded(
                "retirement-observe",
                owner.observe_activated_successor_retirement(
                    native.recovery.as_ref().unwrap(),
                    native.retirement.as_mut().unwrap(),
                    self.request(),
                    observation,
                    |d, h| self.validate(d, h),
                ),
            )
            .await;
        self.snapshot("retirement-observe-returned").await;
        let retirement = native.retirement.as_ref().unwrap();
        emit(
            serde_json::json!({"event":"lineage-retirement-observed","epoch":epoch,
            "confirmed":retirement.is_closed(),"error":result.as_ref().err().map(|e|format!("{e:?}"))}),
        );
        result.map_err(|error| Failure::at("observe", Some(epoch), None, error))?;
        assert!(retirement.is_closed());
        self.replay_retirement(
            owner,
            native.recovery.as_ref().unwrap(),
            native.retirement.as_mut().unwrap(),
        )
        .await
    }

    async fn replay_retirement(
        &self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
        retirement: &mut ActivatedSuccessorRetirement,
    ) -> Result<serde_json::Value, Failure> {
        // The cursor exclusively borrows this one-shot owner. Capture only
        // fixed observations before borrowing; never reconstruct its capability.
        let target = retirement.epoch();
        let issued = retirement.issued_through();
        let reserved = retirement.reserved_through();
        let retired = retirement.retired_at_us();
        let mut resolution = retirement
            .resolution(recovery, &self.cancelled)
            .map_err(|error| Failure::at("resolution", None, None, error))?;
        while let Some(epoch) = resolution.current_epoch() {
            assert!((1..=255).contains(&epoch));
            while let Some(request) = resolution.next_request() {
                let sequence = request.request_sequence.get();
                assert!((1..=64).contains(&sequence));
                self.retirement_refresh(owner, Some(epoch), Some(sequence))
                    .await?;
                let (input, outcome) = replay_grants(&self.budget);
                let result = self
                    .bounded(
                        "retirement-request",
                        owner.inspect_next_activated_successor_retirement_request(
                            &mut resolution,
                            self.recovery_request(),
                            input,
                            outcome,
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await;
                self.snapshot(&format!("retirement-request-returned-{epoch}-{sequence}"))
                    .await;
                let observed = result
                    .map_err(|error| Failure::at("request", Some(epoch), Some(sequence), error))?;
                assert_eq!(observed, request);
                let (reservation, outcome) = self.recovery_copy(request).await;
                emit(serde_json::json!({"event":"lineage-retirement-request",
                    "producer_slot":hex(request.producer_slot_id.as_bytes()),"epoch":epoch,"sequence":sequence,
                    "copy_mode":"existing-retirement","reservation":reservation,"outcome":outcome}));
            }
            self.retirement_refresh(owner, Some(epoch), None).await?;
            let result = self
                .bounded(
                    "retirement-epoch",
                    owner.finish_activated_successor_retirement_epoch(
                        &mut resolution,
                        self.request(),
                        |d, h| self.validate(d, h),
                    ),
                )
                .await;
            self.snapshot(&format!("retirement-epoch-returned-{epoch}"))
                .await;
            let summary = result.map_err(|error| Failure::at("epoch", Some(epoch), None, error))?;
            emit(
                serde_json::json!({"event":"lineage-retirement-epoch","summary":summary_value(summary)}),
            );
        }
        self.retirement_refresh(owner, None, None).await?;
        let result = self
            .bounded(
                "retirement-finish",
                owner.finish_activated_successor_retirement(
                    resolution,
                    self.request(),
                    recovery_limits().event,
                    effect_grant(&self.budget),
                    |d, h| self.validate(d, h),
                ),
            )
            .await;
        self.snapshot("retirement-finish-returned").await;
        let proof = result.map_err(|error| Failure::at("finish", None, None, error))?;
        proof
            .validate(&self.cancelled)
            .map_err(|error| Failure::at("final-validation", None, None, error))?;
        let last = summary_value(proof.last_epoch());
        let through = proof.verified_through();
        emit(
            serde_json::json!({"event":"lineage-retirement-proof","verified_through":through,
            "last_epoch":last,"source_closure":false,"workflow":false,"retirement":false,
            "admission":false,"activation":false}),
        );
        self.snapshot("before-retirement-commit").await;
        let result = self
            .bounded(
                "retirement-commit",
                owner.retire_activated_successor_producer(
                    proof,
                    self.request(),
                    grant(&self.budget, RETIREMENT_BYTES, 0),
                    |d, h| self.validate(d, h),
                ),
            )
            .await;
        self.snapshot("retirement-commit-returned").await;
        emit(
            serde_json::json!({"event":"lineage-retirement-committed","epoch":target,
            "gaps":result.as_ref().ok(),"previous_retired_at_us":retired,
            "error":result.as_ref().err().map(|e|format!("{e:?}"))}),
        );
        let gaps = result.map_err(|error| Failure::at("retire", Some(target), None, error))?;
        Ok(
            serde_json::json!({"target_epoch":target,"issued_through":issued,"reserved_through":reserved,
            "verified_through":through,"last_epoch":last,"gaps":gaps,
            "source_closure":false,"workflow":false,"retirement":true,"admission":false,"activation":false}),
        )
    }

    async fn retirement_refresh(
        &self,
        owner: &mut ControlSession,
        epoch: Option<u64>,
        sequence: Option<u64>,
    ) -> Result<(), Failure> {
        let result = self
            .bounded(
                "retirement-refresh",
                owner.refresh(|d, h| self.validate(d, h)),
            )
            .await;
        self.snapshot(&format!(
            "retirement-refresh-returned-{}-{}",
            epoch.map_or("final".into(), |n| n.to_string()),
            sequence.map_or("epoch".into(), |n| n.to_string())
        ))
        .await;
        let policy = result.map_err(|error| Failure::at("refresh", epoch, sequence, error))?;
        self.policy(owner, "refreshed", epoch, sequence, policy);
        Ok(())
    }
}
