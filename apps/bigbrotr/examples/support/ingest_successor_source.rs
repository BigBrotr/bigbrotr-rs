//! Finite activated-successor source publication and fresh exact retry.
//! The complete prefix proof precedes the separate SQL publication; neither
//! outcome confers workflow, retirement, admission or writer authority.
use super::*;
pub(super) use bigbrotr::storage::postgres::control_owner::ActivatedSuccessorSourceRecovery;
use recovery::staged_value;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SourceFinish {
    Publish,
    Abandon,
}

impl Probe {
    pub(super) async fn close_successor_source(
        &self,
        owner: &mut ControlSession,
        native: &mut Native,
        metadata: OwnedPermit,
        closing: OwnedPermit,
    ) -> Result<serde_json::Value, Failure> {
        self.source_pass(owner, native, metadata, closing, SourceFinish::Publish)
            .await
    }

    pub(super) async fn source_pass(
        &self,
        owner: &mut ControlSession,
        native: &mut Native,
        metadata: OwnedPermit,
        closing: OwnedPermit,
        finish: SourceFinish,
    ) -> Result<serde_json::Value, Failure> {
        self.snapshot("before-source").await;
        let result = self
            .bounded(
                "source-inspect",
                owner.inspect_activated_successor_source(
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
            .map(|recovered| native.source_recovery = Some(recovered));
        // Store the actual owner before observation/classification. A snapshot
        // failure cannot drop its retained metadata/closing grant ahead of native.
        self.snapshot("source-inspect-returned").await;
        result.map_err(|error| Failure::at("inspect", None, None, error))?;
        let recovered = native.source_recovery.as_ref().unwrap();
        let epoch = recovered.epoch();
        let states = self.source_states(recovered).await;
        emit(
            serde_json::json!({"event":"lineage-source-loaded","epoch":epoch,
            "manifest":hex(native.recovery().manifest().encode().as_bytes()),
            "producer_slot":hex(recovered.producer_slot_id().as_bytes()),
            "issued_through":recovered.issued_through(),"reserved_through":recovered.reserved_through(),
            "staged_input":staged_value(recovered.staged_input()),
            "barrier_confirmed":recovered.is_closed(),
            "sql_producer_state":states.0,"sql_source_state":states.1}),
        );
        self.snapshot("before-source-producer-close").await;
        let result = self
            .bounded(
                "source-close",
                owner.close_activated_successor_source_producer(
                    native.recovery.as_mut().unwrap(),
                    native.source_recovery.as_mut().unwrap(),
                    self.request(),
                    closing,
                    |d, h| self.validate(d, h),
                ),
            )
            .await;
        self.snapshot("source-producer-close-returned").await;
        let recovered = native.source_recovery.as_ref().unwrap();
        emit(
            serde_json::json!({"event":"lineage-source-producer-closed","epoch":epoch,
            "confirmed":recovered.is_closed(),"issued_through":recovered.issued_through(),
            "reserved_through":recovered.reserved_through(),"staged_input":staged_value(recovered.staged_input()),
            "error":result.as_ref().err().map(|error|format!("{error:?}"))}),
        );
        result.map_err(|error| Failure::at("close", Some(epoch), None, error))?;
        assert!(recovered.is_closed());
        self.replay_source(owner, native.recovery(), recovered, finish)
            .await
    }

    async fn replay_source(
        &self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
        recovered: &ActivatedSuccessorSourceRecovery,
        finish: SourceFinish,
    ) -> Result<serde_json::Value, Failure> {
        let mut resolution = recovered
            .resolution(recovery, &self.cancelled)
            .map_err(|error| Failure::at("resolution", None, None, error))?;
        while let Some(epoch) = resolution.current_epoch() {
            assert!((1..=255).contains(&epoch));
            while let Some(request) = resolution.next_request() {
                let sequence = request.request_sequence.get();
                assert!((1..=64).contains(&sequence));
                self.source_refresh(owner, Some(epoch), Some(sequence))
                    .await?;
                let (input, outcome) = replay_grants(&self.budget);
                let result = self
                    .bounded(
                        "source-request",
                        owner.resolve_next_activated_successor_source_request(
                            &mut resolution,
                            self.recovery_request(),
                            input,
                            outcome,
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await;
                self.snapshot(&format!("source-request-returned-{epoch}-{sequence}"))
                    .await;
                let observed = result
                    .map_err(|error| Failure::at("request", Some(epoch), Some(sequence), error))?;
                assert_eq!(observed, request);
                let (reservation, outcome) = self.recovery_copy(request).await;
                emit(serde_json::json!({"event":"lineage-source-request",
                    "producer_slot":hex(request.producer_slot_id.as_bytes()),"epoch":epoch,"sequence":sequence,
                    "copy_mode":if epoch<recovered.epoch(){"existing-terminal"}else if recovered.source_closed(){"existing-source-closed"}else{"current-positive"},
                    "reservation":reservation,"outcome":outcome}));
            }
            self.source_refresh(owner, Some(epoch), None).await?;
            let result = self
                .bounded(
                    "source-epoch",
                    owner.finish_activated_successor_source_epoch(
                        &mut resolution,
                        self.request(),
                        |d, h| self.validate(d, h),
                    ),
                )
                .await;
            self.snapshot(&format!("source-epoch-returned-{epoch}"))
                .await;
            let summary = result.map_err(|error| Failure::at("epoch", Some(epoch), None, error))?;
            emit(
                serde_json::json!({"event":"lineage-source-epoch","summary":summary_value(summary)}),
            );
        }
        self.source_refresh(owner, None, None).await?;
        let result = self
            .bounded(
                "source-finish",
                owner.finish_activated_successor_source(
                    resolution,
                    self.request(),
                    recovery_limits().event,
                    effect_grant(&self.budget),
                    |d, h| self.validate(d, h),
                ),
            )
            .await;
        self.snapshot("source-finish-returned").await;
        let proof = result.map_err(|error| Failure::at("finish", None, None, error))?;
        assert!(proof.matches_source(recovered));
        proof
            .validate(&self.cancelled)
            .map_err(|error| Failure::at("final-validation", None, None, error))?;
        let last = summary_value(proof.last_epoch());
        let verified_through = proof.verified_through();
        emit(
            serde_json::json!({"event":"lineage-source-proof","verified_through":verified_through,
            "last_epoch":last,"matches_source":true,"source_closure":false,"workflow":false,
            "retirement":false,"admission":false,"activation":false}),
        );
        if finish == SourceFinish::Abandon {
            let jobs = self
                .finish_acquisition(owner, proof, recovered.epoch())
                .await?;
            return Ok(serde_json::json!({"target_epoch":recovered.epoch(),
                "issued_through":recovered.issued_through(),"reserved_through":recovered.reserved_through(),
                "staged_input":staged_value(recovered.staged_input()),"verified_through":verified_through,
                "last_epoch":last,"positive_recovery":true,"source_closure":false,
                "workflow":true,"acquisition_jobs":jobs}));
        }
        self.snapshot("before-source-publish").await;
        let result = self
            .bounded(
                "source-publish",
                owner.close_activated_successor_source(proof, self.request(), |d, h| {
                    self.validate(d, h)
                }),
            )
            .await;
        self.snapshot("source-publish-returned").await;
        let row = self.source_row(recovered).await;
        emit(
            serde_json::json!({"event":"lineage-source-published","epoch":recovered.epoch(),
            "confirmed":result.is_ok(),"source":row,
            "error":result.as_ref().err().map(|error|format!("{error:?}"))}),
        );
        result.map_err(|error| Failure::at("publish", Some(recovered.epoch()), None, error))?;
        Ok(
            serde_json::json!({"target_epoch":recovered.epoch(),"issued_through":recovered.issued_through(),
            "reserved_through":recovered.reserved_through(),"staged_input":staged_value(recovered.staged_input()),
            "verified_through":verified_through,"last_epoch":last,"positive_recovery":true,"source_closure":true}),
        )
    }

    async fn source_refresh(
        &self,
        owner: &mut ControlSession,
        epoch: Option<u64>,
        sequence: Option<u64>,
    ) -> Result<(), Failure> {
        let result = self
            .bounded("source-refresh", owner.refresh(|d, h| self.validate(d, h)))
            .await;
        self.snapshot(&format!(
            "source-refresh-returned-{}-{}",
            epoch.map_or("final".into(), |n| n.to_string()),
            sequence.map_or("epoch".into(), |n| n.to_string())
        ))
        .await;
        let policy = result.map_err(|error| Failure::at("refresh", epoch, sequence, error))?;
        self.policy(owner, "refreshed", epoch, sequence, policy);
        Ok(())
    }

    async fn source_states(
        &self,
        recovered: &ActivatedSuccessorSourceRecovery,
    ) -> (String, String) {
        let row = self
            .bounded(
                "source-states",
                self.observer.query_one(
                    "SELECT left(p.state,8),left(s.state,8) FROM control.producer_epochs p
                JOIN control.source_streams s ON s.dataset_id=p.dataset_id
                    AND s.owner_resource_id=p.owner_resource_id AND s.owner_epoch=p.owner_epoch
                WHERE p.producer_slot_id=$1::bytea AND p.producer_epoch=$2::text::numeric",
                    &[
                        &&recovered.producer_slot_id().as_bytes()[..],
                        &recovered.epoch().to_string(),
                    ],
                ),
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    }

    async fn source_row(&self, recovered: &ActivatedSuccessorSourceRecovery) -> serde_json::Value {
        let row = self
            .bounded(
                "source-row",
                self.observer.query_one(
                    "SELECT (to_jsonb(s)||jsonb_build_object('xmin',s.xmin::text))::text
                FROM control.source_streams s JOIN control.producer_epochs p
                    ON s.dataset_id=p.dataset_id AND s.owner_resource_id=p.owner_resource_id
                    AND s.owner_epoch=p.owner_epoch
                WHERE p.producer_slot_id=$1::bytea AND p.producer_epoch=$2::text::numeric",
                    &[
                        &&recovered.producer_slot_id().as_bytes()[..],
                        &recovered.epoch().to_string(),
                    ],
                ),
            )
            .await
            .unwrap();
        let value: &str = row.get(0);
        assert!(
            value.len() <= 64 * 1024,
            "finite source projection exceeded"
        );
        serde_json::from_str(value).unwrap()
    }
}
