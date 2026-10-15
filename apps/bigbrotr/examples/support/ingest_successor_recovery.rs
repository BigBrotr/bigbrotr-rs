//! Finite activated-family recovery using the public connected owner. This
//! process repairs only positive current outcome copies, never source/workflow
//! closure, terminal retirement, admission or adoption of a previous writer.
use super::*;
pub(super) use bigbrotr::storage::postgres::control_owner::ActivatedSuccessorRecovery;
use bigbrotr::storage::postgres::control_owner::SuccessorRecoveryInput;

impl Probe {
    pub(super) async fn recover_successor(
        &self,
        owner: &mut ControlSession,
        native: &mut Native,
        metadata: OwnedPermit,
        closing: OwnedPermit,
    ) -> Result<serde_json::Value, Failure> {
        self.snapshot("before-recovery").await;
        let result = self
            .bounded(
                "recovery-inspect",
                owner.inspect_activated_successor_producer(
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
            .map(|recovered| native.activated_recovery = Some(recovered));
        // Store the actual owner before observation/classification. A snapshot
        // failure cannot drop its retained metadata/closing grant ahead of native.
        self.snapshot("recovery-inspect-returned").await;
        result.map_err(|error| Failure::at("inspect", None, None, error))?;
        let recovered = native.activated_recovery.as_ref().unwrap();
        let epoch = recovered.epoch();
        let states = self.recovery_states(recovered).await;
        emit(
            serde_json::json!({"event":"lineage-recovery-loaded","epoch":epoch,
            "manifest":hex(native.recovery().manifest().encode().as_bytes()),
            "producer_slot":hex(recovered.producer_slot_id().as_bytes()),
            "issued_through":recovered.issued_through(),"reserved_through":recovered.reserved_through(),
            "staged_input":staged_value(recovered.staged_input()),
            "barrier_confirmed":recovered.is_closed(),
            "sql_producer_state":states.0,"sql_source_state":states.1}),
        );
        self.snapshot("before-recovery-close").await;
        let result = self
            .bounded(
                "recovery-close",
                owner.close_activated_successor_producer(
                    native.recovery.as_mut().unwrap(),
                    native.activated_recovery.as_mut().unwrap(),
                    self.request(),
                    closing,
                    |d, h| self.validate(d, h),
                ),
            )
            .await;
        self.snapshot("recovery-close-returned").await;
        let recovered = native.activated_recovery.as_ref().unwrap();
        emit(
            serde_json::json!({"event":"lineage-recovery-closed","epoch":epoch,
            "confirmed":recovered.is_closed(),"issued_through":recovered.issued_through(),
            "reserved_through":recovered.reserved_through(),"staged_input":staged_value(recovered.staged_input()),
            "error":result.as_ref().err().map(|error|format!("{error:?}"))}),
        );
        result.map_err(|error| Failure::at("close", Some(epoch), None, error))?;
        assert!(recovered.is_closed());
        self.replay_recovery(owner, native.recovery(), recovered)
            .await
    }

    async fn replay_recovery(
        &self,
        owner: &mut ControlSession,
        recovery: &IngestRecovery,
        recovered: &ActivatedSuccessorRecovery,
    ) -> Result<serde_json::Value, Failure> {
        let mut resolution = recovered
            .resolution(recovery, &self.cancelled)
            .map_err(|error| Failure::at("resolution", None, None, error))?;
        while let Some(epoch) = resolution.current_epoch() {
            assert!((1..=255).contains(&epoch));
            while let Some(request) = resolution.next_request() {
                let sequence = request.request_sequence.get();
                assert!((1..=64).contains(&sequence));
                self.recovery_refresh(owner, Some(epoch), Some(sequence))
                    .await?;
                let (input, outcome) = replay_grants(&self.budget);
                let result = self
                    .bounded(
                        "recovery-request",
                        owner.resolve_next_activated_successor_request(
                            &mut resolution,
                            self.recovery_request(),
                            input,
                            outcome,
                            |d, h| self.validate(d, h),
                        ),
                    )
                    .await;
                self.snapshot(&format!("recovery-request-returned-{epoch}-{sequence}"))
                    .await;
                let observed = result
                    .map_err(|error| Failure::at("request", Some(epoch), Some(sequence), error))?;
                assert_eq!(observed, request);
                let (reservation, outcome) = self.recovery_copy(request).await;
                emit(serde_json::json!({"event":"lineage-recovery-request",
                    "producer_slot":hex(request.producer_slot_id.as_bytes()),"epoch":epoch,"sequence":sequence,
                    "copy_mode":if epoch<recovered.epoch(){"existing-terminal"}else{"current-positive"},
                    "reservation":reservation,"outcome":outcome}));
            }
            self.recovery_refresh(owner, Some(epoch), None).await?;
            let result = self
                .bounded(
                    "recovery-epoch",
                    owner.finish_activated_successor_epoch(
                        &mut resolution,
                        self.request(),
                        |d, h| self.validate(d, h),
                    ),
                )
                .await;
            self.snapshot(&format!("recovery-epoch-returned-{epoch}"))
                .await;
            let summary = result.map_err(|error| Failure::at("epoch", Some(epoch), None, error))?;
            emit(
                serde_json::json!({"event":"lineage-recovery-epoch","summary":summary_value(summary)}),
            );
        }
        self.recovery_refresh(owner, None, None).await?;
        let result = self
            .bounded(
                "recovery-finish",
                owner.finish_activated_successor_recovery(
                    resolution,
                    self.request(),
                    recovery_limits().event,
                    effect_grant(&self.budget),
                    |d, h| self.validate(d, h),
                ),
            )
            .await;
        self.snapshot("recovery-finish-returned").await;
        let proof = result.map_err(|error| Failure::at("finish", None, None, error))?;
        assert!(proof.matches_recovery(recovered));
        proof
            .validate(&self.cancelled)
            .map_err(|error| Failure::at("final-validation", None, None, error))?;
        let last = summary_value(proof.last_epoch());
        emit(
            serde_json::json!({"event":"lineage-recovery-proof","verified_through":proof.verified_through(),
            "last_epoch":last,"matches_recovery":true,"source_closure":false,"workflow":false,
            "retirement":false,"admission":false,"activation":false}),
        );
        Ok(
            serde_json::json!({"target_epoch":recovered.epoch(),"issued_through":recovered.issued_through(),
            "reserved_through":recovered.reserved_through(),"staged_input":staged_value(recovered.staged_input()),
            "verified_through":proof.verified_through(),"last_epoch":last,"positive_recovery":true}),
        )
    }

    fn recovery_request(&self) -> SuccessorReplayRequest<'_> {
        SuccessorReplayRequest {
            ingest_root: &self.path,
            limits: recovery_limits(),
            policy_ttl: self.request().policy_ttl,
            cancelled: &self.cancelled,
        }
    }

    // Unlike the original history helper, retain a snapshot of a failed refresh
    // as well. This branch is new; historical command/event behavior is unchanged.
    async fn recovery_refresh(
        &self,
        owner: &mut ControlSession,
        epoch: Option<u64>,
        sequence: Option<u64>,
    ) -> Result<(), Failure> {
        let result = self
            .bounded(
                "recovery-refresh",
                owner.refresh(|d, h| self.validate(d, h)),
            )
            .await;
        self.snapshot(&format!(
            "recovery-refresh-returned-{}-{}",
            epoch.map_or("final".into(), |n| n.to_string()),
            sequence.map_or("epoch".into(), |n| n.to_string())
        ))
        .await;
        let policy = result.map_err(|error| Failure::at("refresh", epoch, sequence, error))?;
        self.policy(owner, "refreshed", epoch, sequence, policy);
        Ok(())
    }

    async fn recovery_states(&self, recovered: &ActivatedSuccessorRecovery) -> (String, String) {
        let row = self
            .bounded(
                "recovery-states",
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

    async fn recovery_copy(
        &self,
        request: ProducerRequestId,
    ) -> (serde_json::Value, serde_json::Value) {
        // These are independent observer bytes, not a public native receipt
        // extraction or a new Control operation. Existing table bounds keep one
        // reservation/outcome finite; raw snapshots independently retain all rows.
        let row = self
            .bounded(
                "recovery-copy",
                self.observer.query_one(
                    "SELECT (to_jsonb(r)||jsonb_build_object('xmin',r.xmin::text))::text,
                    (to_jsonb(o)||jsonb_build_object('xmin',o.xmin::text))::text
                FROM control.producer_requests r JOIN control.producer_outcomes o
                    USING(producer_slot_id,producer_epoch,request_sequence)
                WHERE r.producer_slot_id=$1::bytea AND r.producer_epoch=$2::text::numeric
                    AND r.request_sequence=$3::text::numeric",
                    &[
                        &&request.producer_slot_id.as_bytes()[..],
                        &request.producer_epoch.get().to_string(),
                        &request.request_sequence.get().to_string(),
                    ],
                ),
            )
            .await
            .unwrap();
        let reservation: &str = row.get(0);
        let outcome: &str = row.get(1);
        assert!(
            reservation.len() + outcome.len() <= MIB as usize,
            "finite outcome projection exceeded"
        );
        (
            serde_json::from_str(reservation).unwrap(),
            serde_json::from_str(outcome).unwrap(),
        )
    }
}

fn staged_value(input: Option<SuccessorRecoveryInput>) -> serde_json::Value {
    input.map_or(serde_json::Value::Null,|input|serde_json::json!({
        "request":input.request,"digest":input.digest,"input_bytes":input.input_bytes,"reserved":input.reserved,
    }))
}
