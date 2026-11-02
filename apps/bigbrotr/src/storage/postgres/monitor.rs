//! One-time Monitor dispatch under the actual Control backend. SQL intents and
//! historical bindings are not execution witnesses or native admission receipts.
use super::monitor_types::{MonitorEvidenceBinding, MonitorIntent};
use super::{
    ControlSession, OwnerError, PolicySnapshot, backend, require, snapshot, transaction_error,
};
use crate::{
    acquisition::relay_source::source_visibility_allowed,
    network::{
        DirectAttempt, DirectConnector, DirectPolicy, DirectStream, RouteSelection, Target,
        TransportError, TransportTrace,
    },
    runtime::policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    storage::{
        postgres::{initialization::application_transaction, producer_sources},
        rocks::store::StoredVisibility,
    },
};
use bigbrotr_model::{
    Digest256, MeasurementAttemptId, PolicyEpoch, ProducerEpoch, ProducerRequestId, RequestSequence,
};
use bigbrotr_protocol::endpoint::Endpoint;
use std::{io, num::NonZeroU64, time::Duration};
use tokio::time::{Instant, timeout_at};
use tokio_postgres::{Row, Transaction};

/// Created only after a fresh positively acknowledged intent COMMIT. The
/// original suspend-aware lease and deadline move with this non-Clone value.
/// A historical read/duplicate/unknown COMMIT never reconstructs it.
pub struct MonitorDispatchToken {
    binding: MonitorEvidenceBinding,
    owner: super::ingest_registry::ControlOwner,
    policy: PolicySnapshot,
    lease: PolicyLeaseTracker,
    deadline: Instant,
}
impl MonitorDispatchToken {
    pub fn evidence_binding(&self) -> MonitorEvidenceBinding {
        self.binding
    }
    /// Positive refusal while the unique token has never entered the source.
    pub fn refuse(self) -> MonitorNoIoProof {
        MonitorNoIoProof {
            binding: self.binding,
            reason: MonitorNoIoReason::NotDispatched,
        }
    }
}
#[allow(
    clippy::large_enum_variant,
    reason = "fixed bounded capability; no hidden heap allocation"
)]
pub enum MonitorCreation {
    Created(MonitorDispatchToken),
    Existing(MonitorEvidenceBinding),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorSourceError {
    Control(OwnerError),
    Source(producer_sources::SourceBindingError),
    Visibility,
    Transport(TransportError),
}
impl From<OwnerError> for MonitorSourceError {
    fn from(value: OwnerError) -> Self {
        Self::Control(value)
    }
}
impl std::fmt::Display for MonitorSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Monitor source unavailable: {self:?}")
    }
}
impl std::error::Error for MonitorSourceError {}
/// Closed operational reasons. No strings, remote failure vote or start claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum MonitorNoIoReason {
    AuthorityUnavailable = 1,
    ContextRefused = 2,
    LocalTransportRefused = 3,
    NotDispatched = 4,
}
impl MonitorNoIoReason {
    fn decode(value: i16) -> Result<Self, OwnerError> {
        match value {
            1 => Ok(Self::AuthorityUnavailable),
            2 => Ok(Self::ContextRefused),
            3 => Ok(Self::LocalTransportRefused),
            4 => Ok(Self::NotDispatched),
            _ => Err(OwnerError::InvalidMetadata),
        }
    }
}
/// Only a completed local source invocation can construct this proof. A row,
/// recovered intent, timeout or dropped future cannot recreate it.
pub struct MonitorNoIoProof {
    binding: MonitorEvidenceBinding,
    reason: MonitorNoIoReason,
}
pub struct MonitorSourceOutcome<T> {
    result: Result<T, MonitorSourceError>,
    no_io: Option<MonitorNoIoProof>,
}
impl<T> MonitorSourceOutcome<T> {
    pub fn into_parts(self) -> (Result<T, MonitorSourceError>, Option<MonitorNoIoProof>) {
        (self.result, self.no_io)
    }
    fn completed(
        result: Result<T, MonitorSourceError>,
        binding: MonitorEvidenceBinding,
        trace: &TransportTrace,
    ) -> Self {
        let no_io = if trace.snapshot().first_io.is_none() {
            result.as_ref().err().map(|error| MonitorNoIoProof {
                binding,
                reason: match error {
                    MonitorSourceError::Visibility
                    | MonitorSourceError::Source(_)
                    | MonitorSourceError::Control(
                        OwnerError::InvalidInput
                        | OwnerError::InvalidMetadata
                        | OwnerError::InvalidPolicy,
                    ) => MonitorNoIoReason::ContextRefused,
                    MonitorSourceError::Control(_)
                    | MonitorSourceError::Transport(TransportError::Authority) => {
                        MonitorNoIoReason::AuthorityUnavailable
                    }
                    MonitorSourceError::Transport(_) => MonitorNoIoReason::LocalTransportRefused,
                },
            })
        } else {
            None
        };
        Self { result, no_io }
    }
}
/// Original evidence identity, if reserved. `outcome_recorded` observes a ledger
/// copy only; callers resolve the original native receipt through existing recovery.
#[derive(Debug, Clone, Copy)]
pub struct MonitorEvidenceHistory {
    pub binding: MonitorEvidenceBinding,
    pub request: Option<(ProducerRequestId, Digest256)>,
    pub outcome_recorded: bool,
    pub pre_io_refusal: Option<MonitorNoIoReason>,
    /// Historical custody coordinates, never proof that this process stopped.
    pub original_control: super::ingest_registry::ControlOwner,
    pub created_at_us: i64,
    pub dispatch_deadline_us: i64,
    pub config_revision: u64,
    pub policy_revision: u64,
}

impl ControlSession {
    /// Read-only startup prerequisite. This grants neither dispatch nor evidence
    /// authority and must run before publishing Monitor readiness.
    pub async fn require_monitor_execution(&mut self) -> Result<(), OwnerError> {
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 44)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        let allowed:bool=tx.query_one("SELECT (SELECT bool_and(has_table_privilege(CURRENT_USER,t.name,p.privilege)) FROM(VALUES('control.measurement_dispatches'),('control.measurement_dispatch_refusals'),('control.producer_requests'))t(name) CROSS JOIN(VALUES('SELECT'),('INSERT'),('UPDATE'))p(privilege)) AND has_function_privilege(CURRENT_USER,'control.measurement_context_valid(bytea,bytea)','EXECUTE') AND has_function_privilege(CURRENT_USER,'control.require_measurement_evidence(bytea,bytea,bytea,bytea,numeric,bytea)','EXECUTE') AND has_function_privilege(CURRENT_USER,'control.require_relay_source(bytea,bytea,bytea,numeric,bytea)','EXECUTE') AND has_function_privilege(CURRENT_USER,'control.command_u64_bytes(control.u64)','EXECUTE')", &[]).await.map_err(backend)?.try_get(0).map_err(backend)?;
        if !allowed {
            return Err(OwnerError::Unavailable);
        }
        tx.rollback().await.map_err(backend)?;
        if self.client.is_closed() {
            return Err(OwnerError::Unavailable);
        }
        self.available = true;
        Ok(())
    }
    /// Caller reserves policy/PG buffers and retains actual source/native worker
    /// custody. `authorize` restores BOTH complete accepted documents and checks
    /// Monitor selection, fixed membership, bans and actual static route roots.
    pub async fn create_monitor_dispatch(
        &mut self,
        intent: MonitorIntent,
        visibility: &StoredVisibility,
        deadline: Instant,
        mut authorize: impl FnMut(
            &str,
            [u8; 32],
            &Endpoint,
            RouteSelection,
        ) -> Result<DirectPolicy, OwnerError>,
    ) -> Result<MonitorCreation, MonitorSourceError> {
        if intent.dataset() != self.request.dataset
            || visibility.entry().reference() != intent.visibility()
        {
            return Err(OwnerError::InvalidInput.into());
        }
        let ttl = Duration::from_micros(intent.profile().effective_authority_us().get());
        let deadline = deadline.min(Instant::now() + ttl);
        let mut lease = PolicyLeaseTracker::new(
            self.request.dataset,
            self.request.incarnation,
            PolicyEpoch::new(self.validated_policy.policy_revision),
            PolicyLeaseTtl::new(ttl).map_err(|_| OwnerError::InvalidInput)?,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        timeout_at(deadline, async {
            let tx = application_transaction(&mut self.client, 44)
                .await
                .map_err(transaction_error)?;
            require(&tx, self.request.dataset, self.identity).await?;
            // Dataset lock precedes every dispatch-row read and new insertion.
            tx.query_one(
                "SELECT 1 FROM control.datasets WHERE dataset_id=$1::bytea FOR UPDATE",
                &[&&self.request.dataset.as_bytes()[..]],
            )
            .await
            .map_err(backend)?;
            if let Some(old) = read(&tx, self.request.dataset, intent.attempt_id()).await? {
                if old.intent() != intent {
                    return Err(OwnerError::InvalidInput.into());
                }
                tx.rollback().await.map_err(backend)?;
                if self.client.is_closed() {
                    return Err(OwnerError::Unavailable.into());
                }
                self.available = true;
                return Ok(MonitorCreation::Existing(old));
            }
            producer_sources::require_current_enrollment(
                &tx,
                self.request.dataset,
                intent.source(),
                Some(intent.visibility()),
            )
            .await
            .map_err(MonitorSourceError::Source)?;
            let (endpoint, route) =
                super::relay_source::load_destination(&tx, self.request.dataset, intent.source())
                    .await?;
            if !source_visibility_allowed(&endpoint, route, visibility.entry()) {
                return Err(MonitorSourceError::Visibility);
            }
            let policy = snapshot(&tx, &self.request, &mut |d, h| {
                authorize(d, h, &endpoint, route).map(|_| ())
            })
            .await?;
            let remaining = owner_remaining(&tx, self.identity).await?;
            lease
                .complete_refresh(
                    &ticket,
                    LeaseResponse {
                        dataset: self.request.dataset,
                        process_incarnation: self.request.incarnation,
                        policy_epoch: PolicyEpoch::new(policy.policy_revision),
                        ttl: ttl.min(remaining),
                    },
                )
                .map_err(|_| OwnerError::Unavailable)?;
            let source = intent.source();
            tx.execute(
                INSERT,
                &[
                    &&self.request.dataset.as_bytes()[..],
                    &&intent.attempt_id().as_bytes()[..],
                    &&source.endpoint_id.as_bytes()[..],
                    &&source.access_scope_id.as_bytes()[..],
                    &source.enrollment_generation.get().to_string(),
                    &&source.route_id.as_bytes()[..],
                    &&intent.context_bytes()[..],
                    &(ttl.as_micros() as i64),
                    &policy.config_revision.to_string(),
                    &policy.policy_revision.to_string(),
                    &&self.identity.resource_id.as_bytes()[..],
                    &self.identity.epoch.get().to_string(),
                    &&self.identity.process_incarnation.as_bytes()[..],
                ],
            )
            .await
            .map_err(backend)?;
            lease
                .check_delivery(PolicyEpoch::new(policy.policy_revision))
                .map_err(|_| OwnerError::Unavailable)?;
            tx.commit().await.map_err(|_| OwnerError::CommitUnknown)?;
            // A successful commit followed by lost authority still creates only
            // a recoverable intent, never a renewed or historical dispatch token.
            lease
                .check_delivery(PolicyEpoch::new(policy.policy_revision))
                .map_err(|_| OwnerError::Unavailable)?;
            if self.client.is_closed() || Instant::now() >= deadline {
                return Err(OwnerError::Unavailable.into());
            }
            self.available = true;
            Ok(MonitorCreation::Created(MonitorDispatchToken {
                binding: MonitorEvidenceBinding { intent },
                owner: self.identity,
                policy,
                lease,
                deadline,
            }))
        })
        .await
        .unwrap_or(Err(OwnerError::Unavailable.into()))
    }

    /// Consumes the only local dispatch token. Holds source/policy locks until
    /// connector/socket destruction. No generic Sync job or native authority is
    /// created here. The supervisor retains native/socket custody until joined.
    pub async fn run_monitor_source<T>(
        &mut self,
        mut token: MonitorDispatchToken,
        visibility: &StoredVisibility,
        connector: &DirectConnector,
        trace: &TransportTrace,
        mut authorize: impl FnMut(
            &str,
            [u8; 32],
            &Endpoint,
            RouteSelection,
        ) -> Result<DirectPolicy, OwnerError>,
        operation: impl for<'a, 'owner> AsyncFnOnce(&'a mut DirectStream<'owner>) -> io::Result<T>,
    ) -> MonitorSourceOutcome<T> {
        let binding = token.binding;
        if token.owner != self.identity
            || token.binding.intent().dataset() != self.request.dataset
            || visibility.entry().reference() != token.binding.intent().visibility()
        {
            return MonitorSourceOutcome::completed(
                Err(OwnerError::InvalidInput.into()),
                binding,
                trace,
            );
        }
        let epoch = PolicyEpoch::new(token.policy.policy_revision);
        // Start the action before SQL; retain the creation lease, never refresh it.
        let action = match token.lease.begin_action(epoch) {
            Ok(action) => action,
            Err(_) => {
                return MonitorSourceOutcome::completed(
                    Err(OwnerError::Unavailable.into()),
                    binding,
                    trace,
                );
            }
        };
        if let Err(error) = self.begin_operation() {
            return MonitorSourceOutcome::completed(Err(error.into()), binding, trace);
        }
        let completed=timeout_at(token.deadline,async {
            let tx=application_transaction(&mut self.client,44).await.map_err(transaction_error)?;
            let result=async {
                require(&tx,self.request.dataset,self.identity).await?;
                let intent=token.binding.intent();
                producer_sources::require_current_enrollment(&tx,self.request.dataset,intent.source(),Some(intent.visibility()))
                    .await.map_err(MonitorSourceError::Source)?;
                let found=read(&tx,self.request.dataset,intent.attempt_id()).await?.ok_or(OwnerError::InvalidMetadata)?;
                if found!=token.binding { return Err(OwnerError::InvalidMetadata.into()); }
                let row=tx.query_one("SELECT issuer_resource=$3::bytea AND issuer_epoch=$4::text::numeric AND issuer_incarnation=$5::bytea AND config_revision=$6::text::numeric AND policy_revision=$7::text::numeric AND NOT EXISTS(SELECT 1 FROM control.measurement_dispatch_refusals f WHERE f.dataset_id=$1::bytea AND f.attempt_id=$2::bytea) AND NOT EXISTS(SELECT 1 FROM control.producer_requests r WHERE r.dataset_id=$1::bytea AND r.monitor_attempt_id=$2::bytea), dispatch_deadline_us-(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint FROM control.measurement_dispatches WHERE dataset_id=$1::bytea AND attempt_id=$2::bytea FOR SHARE",&[&&self.request.dataset.as_bytes()[..],&&intent.attempt_id().as_bytes()[..],&&self.identity.resource_id.as_bytes()[..],&self.identity.epoch.get().to_string(),&&self.identity.process_incarnation.as_bytes()[..],&token.policy.config_revision.to_string(),&token.policy.policy_revision.to_string()]).await.map_err(backend)?;
                let remaining:i64=row.try_get(1).map_err(backend)?;
                if !row.try_get::<_,bool>(0).map_err(backend)? || remaining<=0 { return Err(OwnerError::Unavailable.into()); }
                let (endpoint,route)=super::relay_source::load_destination(&tx,self.request.dataset,intent.source()).await?;
                if !source_visibility_allowed(&endpoint,route,visibility.entry()) { return Err(MonitorSourceError::Visibility); }
                let mut destination=None;
                let policy=snapshot(&tx,&self.request,&mut |d,h| { destination=Some(authorize(d,h,&endpoint,route)?);Ok(()) }).await?;
                if policy!=token.policy { return Err(OwnerError::InvalidPolicy.into()); }
                let remaining=Duration::from_micros(remaining as u64).min(owner_remaining(&tx,self.identity).await?);
                let window=action.complete(remaining).map_err(|_|OwnerError::Unavailable)?;
                let (attempt,_cancel)=DirectAttempt::new(Target::new(endpoint).map_err(|_|OwnerError::InvalidMetadata)?,
                    destination.ok_or(OwnerError::InvalidPolicy)?,window,token.deadline);
                let live=||!tx.client().is_closed();
                connector.run_observed(attempt.with_liveness_check(&live),trace,operation).await.map_err(MonitorSourceError::Transport)
            }.await;
            tx.rollback().await.map_err(backend)?;
            if self.client.is_closed() { return Err(OwnerError::Unavailable.into()); }
            self.available=true;
            // Diagnostic values confer no authority after cleanup. The caller
            // admits through a fresh evidence operation, even after interruption.
            result
        }).await;
        match completed {
            Ok(result) => MonitorSourceOutcome::completed(result, binding, trace),
            Err(_) => MonitorSourceOutcome {
                result: Err(MonitorSourceError::Control(OwnerError::Unavailable)),
                no_io: None,
            },
        }
    }

    /// Record a positive local no-I/O proof under current Control. Retain the
    /// private proof through unknown COMMIT; exact retry is read-only after the
    /// closure is found. A historical row alone cannot reconstruct this proof.
    pub async fn record_monitor_pre_io_refusal(
        &mut self,
        proof: &MonitorNoIoProof,
    ) -> Result<(), OwnerError> {
        if proof.binding.intent().dataset() != self.request.dataset {
            return Err(OwnerError::InvalidInput);
        }
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 44)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        tx.query_one(
            "SELECT 1 FROM control.datasets WHERE dataset_id=$1::bytea FOR UPDATE",
            &[&&self.request.dataset.as_bytes()[..]],
        )
        .await
        .map_err(backend)?;
        if read(
            &tx,
            self.request.dataset,
            proof.binding.intent().attempt_id(),
        )
        .await?
            != Some(proof.binding)
        {
            return Err(OwnerError::InvalidMetadata);
        }
        if let Some(row)=tx.query_opt("SELECT reason FROM control.measurement_dispatch_refusals WHERE dataset_id=$1::bytea AND attempt_id=$2::bytea",&[&&self.request.dataset.as_bytes()[..],&&proof.binding.intent().attempt_id().as_bytes()[..]]).await.map_err(backend)? {
            if row.try_get::<_,i16>(0).map_err(backend)?!=proof.reason as i16 { return Err(OwnerError::InvalidMetadata); }
            tx.rollback().await.map_err(backend)?;
            if self.client.is_closed() { return Err(OwnerError::Unavailable); }
            self.available=true;
            return Ok(());
        }
        tx.execute("INSERT INTO control.measurement_dispatch_refusals(dataset_id,attempt_id,reason,issuer_resource,issuer_epoch,issuer_incarnation,recorded_at_us) VALUES($1::bytea,$2::bytea,$3::smallint,$4::bytea,$5::text::numeric,$6::bytea,(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint)",&[&&self.request.dataset.as_bytes()[..],&&proof.binding.intent().attempt_id().as_bytes()[..],&(proof.reason as i16),&&self.identity.resource_id.as_bytes()[..],&self.identity.epoch.get().to_string(),&&self.identity.process_incarnation.as_bytes()[..]]).await.map_err(backend)?;
        tx.commit().await.map_err(|_| OwnerError::CommitUnknown)?;
        if self.client.is_closed() {
            return Err(OwnerError::Unavailable);
        }
        self.available = true;
        Ok(())
    }

    /// Recover the identity lost with the old process before choosing any new
    /// attempt ID. Current enrollment may have changed or been withdrawn. The
    /// result is history only; the SQL creation veto remains authoritative.
    pub async fn pending_monitor_dispatch(
        &mut self,
        endpoint: bigbrotr_model::EndpointId,
        access: bigbrotr_model::AccessScopeId,
    ) -> Result<Option<MeasurementAttemptId>, OwnerError> {
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 44)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        tx.query_one(
            "SELECT 1 FROM control.datasets WHERE dataset_id=$1::bytea FOR UPDATE",
            &[&&self.request.dataset.as_bytes()[..]],
        )
        .await
        .map_err(backend)?;
        let row=tx.query_opt("SELECT substring(d.attempt_id FROM 1 FOR 17) FROM control.measurement_dispatches d WHERE d.dataset_id=$1::bytea AND d.endpoint_id=$2::bytea AND d.access_scope_id=$3::bytea AND NOT EXISTS(SELECT 1 FROM control.measurement_dispatch_refusals f WHERE f.dataset_id=d.dataset_id AND f.attempt_id=d.attempt_id) AND NOT EXISTS(SELECT 1 FROM control.producer_requests r JOIN control.producer_outcomes o ON o.dataset_id=r.dataset_id AND o.producer_slot_id=r.producer_slot_id AND o.producer_epoch=r.producer_epoch AND o.request_sequence=r.request_sequence WHERE r.dataset_id=d.dataset_id AND r.monitor_attempt_id=d.attempt_id AND NOT r.monitor_resolution AND o.outcome_kind IN(1,2))",&[&&self.request.dataset.as_bytes()[..],&&endpoint.as_bytes()[..],&&access.as_bytes()[..]]).await.map_err(backend)?;
        let result = row
            .map(|row| {
                fixed::<16>(&row, 0)?
                    .try_into()
                    .map_err(|_| OwnerError::InvalidMetadata)
            })
            .transpose()?;
        tx.rollback().await.map_err(backend)?;
        if self.client.is_closed() {
            return Err(OwnerError::Unavailable);
        }
        self.available = true;
        Ok(result)
    }

    /// Historical observation under current Control, independent of old source
    /// enablement/deadline. Never returns a runnable token or native receipt.
    pub async fn read_monitor_dispatch(
        &mut self,
        attempt: MeasurementAttemptId,
        resolution: bool,
    ) -> Result<Option<MonitorEvidenceHistory>, OwnerError> {
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 44)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        tx.query_one(
            "SELECT 1 FROM control.datasets WHERE dataset_id=$1::bytea FOR UPDATE",
            &[&&self.request.dataset.as_bytes()[..]],
        )
        .await
        .map_err(backend)?;
        let binding = read(&tx, self.request.dataset, attempt).await?;
        let result = if let Some(binding) = binding {
            let row=tx.query_opt("SELECT substring(r.producer_slot_id FROM 1 FOR 17),r.producer_epoch::text,r.request_sequence::text,substring(r.request_digest FROM 1 FOR 33),EXISTS(SELECT 1 FROM control.producer_outcomes o WHERE o.dataset_id=r.dataset_id AND o.producer_slot_id=r.producer_slot_id AND o.producer_epoch=r.producer_epoch AND o.request_sequence=r.request_sequence) FROM control.producer_requests r WHERE r.dataset_id=$1::bytea AND r.monitor_attempt_id=$2::bytea AND r.monitor_resolution=$3::boolean",&[&&self.request.dataset.as_bytes()[..],&&attempt.as_bytes()[..],&resolution]).await.map_err(backend)?;
            let (request, outcome_recorded) = if let Some(row) = row {
                (
                    Some((
                        ProducerRequestId {
                            producer_slot_id: fixed::<16>(&row, 0)?
                                .try_into()
                                .map_err(|_| OwnerError::InvalidMetadata)?,
                            producer_epoch: ProducerEpoch::new(number(&row, 1)?)
                                .map_err(|_| OwnerError::InvalidMetadata)?,
                            request_sequence: RequestSequence::new(number(&row, 2)?)
                                .map_err(|_| OwnerError::InvalidMetadata)?,
                        },
                        Digest256::from_bytes(fixed(&row, 3)?),
                    )),
                    row.try_get(4).map_err(backend)?,
                )
            } else {
                (None, false)
            };
            let row=tx.query_one("SELECT substring(d.issuer_resource FROM 1 FOR 17),d.issuer_epoch::text,substring(d.issuer_incarnation FROM 1 FOR 17),substring(o.role_instance_id FROM 1 FOR 17),d.created_at_us,d.dispatch_deadline_us,d.config_revision::text,d.policy_revision::text FROM control.measurement_dispatches d JOIN control.owner_epochs o ON o.dataset_id=d.dataset_id AND o.resource_id=d.issuer_resource AND o.epoch=d.issuer_epoch WHERE d.dataset_id=$1::bytea AND d.attempt_id=$2::bytea", &[&&self.request.dataset.as_bytes()[..],&&attempt.as_bytes()[..]]).await.map_err(backend)?;
            let refusal=tx.query_opt("SELECT reason FROM control.measurement_dispatch_refusals WHERE dataset_id=$1::bytea AND attempt_id=$2::bytea",&[&&self.request.dataset.as_bytes()[..],&&attempt.as_bytes()[..]]).await.map_err(backend)?;
            let pre_io_refusal = refusal
                .map(|row| MonitorNoIoReason::decode(row.try_get(0).map_err(backend)?))
                .transpose()?;
            Some(MonitorEvidenceHistory {
                pre_io_refusal,
                binding,
                request,
                outcome_recorded,
                original_control: super::ingest_registry::ControlOwner {
                    resource_id: fixed::<16>(&row, 0)?
                        .try_into()
                        .map_err(|_| OwnerError::InvalidMetadata)?,
                    epoch: bigbrotr_model::OwnerEpoch::new(number(&row, 1)?)
                        .map_err(|_| OwnerError::InvalidMetadata)?,
                    process_incarnation: fixed::<16>(&row, 2)?
                        .try_into()
                        .map_err(|_| OwnerError::InvalidMetadata)?,
                    role_instance_id: fixed(&row, 3)?,
                },
                created_at_us: row.try_get(4).map_err(backend)?,
                dispatch_deadline_us: row.try_get(5).map_err(backend)?,
                config_revision: number(&row, 6)?,
                policy_revision: number(&row, 7)?,
            })
        } else {
            None
        };
        tx.rollback().await.map_err(backend)?;
        if self.client.is_closed() {
            return Err(OwnerError::Unavailable);
        }
        self.available = true;
        Ok(result)
    }
}
const INSERT: &str = "INSERT INTO control.measurement_dispatches(dataset_id,attempt_id,endpoint_id,access_scope_id,enrollment_generation,route_id,context_bytes,authority_ceiling_us,config_revision,policy_revision,issuer_resource,issuer_epoch,issuer_incarnation,created_at_us,dispatch_deadline_us) SELECT $1::bytea,$2::bytea,$3::bytea,$4::bytea,$5::text::numeric,$6::bytea,$7::bytea,$8::bigint,$9::text::numeric,$10::text::numeric,$11::bytea,$12::text::numeric,$13::bytea,t,t+$8::bigint FROM(SELECT(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint AS t)s";
async fn owner_remaining(
    tx: &Transaction<'_>,
    owner: super::ingest_registry::ControlOwner,
) -> Result<Duration, OwnerError> {
    let n:i64=tx.query_one("SELECT lease_expires_at_us-(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric",&[&&owner.resource_id.as_bytes()[..],&owner.epoch.get().to_string()]).await.map_err(backend)?.try_get(0).map_err(backend)?;
    if n <= 0 {
        return Err(OwnerError::Unavailable);
    }
    Ok(Duration::from_micros(n as u64))
}
async fn read(
    tx: &Transaction<'_>,
    dataset: bigbrotr_model::DatasetId,
    attempt: MeasurementAttemptId,
) -> Result<Option<MonitorEvidenceBinding>, OwnerError> {
    let row=tx.query_opt("SELECT substring(endpoint_id FROM 1 FOR 17),substring(access_scope_id FROM 1 FOR 17),enrollment_generation::text,substring(route_id FROM 1 FOR 17),substring(context_bytes FROM 1 FOR 295),authority_ceiling_us,profile_version FROM control.measurement_dispatches WHERE dataset_id=$1::bytea AND attempt_id=$2::bytea FOR SHARE",&[&&dataset.as_bytes()[..],&&attempt.as_bytes()[..]]).await.map_err(backend)?;
    row.map(|row| {
        if row.try_get::<_, i16>(6).map_err(backend)? != 1 {
            return Err(OwnerError::InvalidMetadata);
        }
        let source = crate::acquisition::relay_source::RelaySourceBinding {
            endpoint_id: fixed::<16>(&row, 0)?
                .try_into()
                .map_err(|_| OwnerError::InvalidMetadata)?,
            access_scope_id: fixed::<16>(&row, 1)?
                .try_into()
                .map_err(|_| OwnerError::InvalidMetadata)?,
            enrollment_generation: NonZeroU64::new(number(&row, 2)?)
                .ok_or(OwnerError::InvalidMetadata)?,
            route_id: fixed::<16>(&row, 3)?
                .try_into()
                .map_err(|_| OwnerError::InvalidMetadata)?,
        };
        let authority: u64 = u64::try_from(row.try_get::<_, i64>(5).map_err(backend)?)
            .map_err(|_| OwnerError::InvalidMetadata)?;
        let intent =
            MonitorIntent::decode(attempt, source, row.try_get(4).map_err(backend)?, authority)?;
        if intent.dataset() != dataset {
            return Err(OwnerError::InvalidMetadata);
        }
        Ok(MonitorEvidenceBinding { intent })
    })
    .transpose()
}
fn fixed<const N: usize>(row: &Row, column: usize) -> Result<[u8; N], OwnerError> {
    row.try_get::<_, &[u8]>(column)
        .map_err(backend)?
        .try_into()
        .map_err(|_| OwnerError::InvalidMetadata)
}
fn number(row: &Row, column: usize) -> Result<u64, OwnerError> {
    row.try_get::<_, &str>(column)
        .map_err(backend)?
        .parse()
        .map_err(|_| OwnerError::InvalidMetadata)
}
