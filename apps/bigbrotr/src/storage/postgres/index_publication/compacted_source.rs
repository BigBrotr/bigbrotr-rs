//! Final-transaction checks for the two selected compacted finite Index sources.
//! A retained terminal proof supplies integrity; current Control/Index session
//! and the fresh canonical check remain separate publication obligations.
use super::*;
use crate::{
    runtime::{
        OwnedPermit,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
    storage::postgres::control_owner::{
        CompactedInitialRetirement, InitialHistoryRequest, VerifiedSuccessorHistory,
    },
    storage::rocks::store::{AdmissionLimits, IngestRecovery, compacted_outcome_data_bytes},
};
use bigbrotr_model::PolicyEpoch;

#[derive(Clone, Copy)]
pub enum CompactedIndexSource<'a> {
    Initial {
        proof: &'a CompactedInitialRetirement<'a>,
        native: &'a IngestRecovery,
        registration: &'a ingest_registry::InitialRegistration,
    },
    Successor {
        proof: &'a VerifiedSuccessorHistory<'a>,
        native: &'a IngestRecovery,
    },
}
pub struct CompactedIndexRequest<'a> {
    pub source: CompactedIndexSource<'a>,
    pub limits: AdmissionLimits,
    pub effects: &'a OwnedPermit,
    pub authority: InitialHistoryRequest<'a>,
}
impl CompactedIndexRequest<'_> {
    pub(super) fn matches_successor(
        &self,
        proof: &VerifiedSuccessorHistory<'_>,
        native: &IngestRecovery,
    ) -> bool {
        matches!(self.source,CompactedIndexSource::Successor { proof: own, native: store }
            if std::ptr::eq(own,proof) && std::ptr::eq(store,native))
    }
    pub(super) async fn require(
        &self,
        tx: &Transaction<'_>,
        session: &IndexSession,
    ) -> Result<(), IndexError> {
        use super::super::{producer_compaction, successor_history};
        if self.effects.request().units().items() == 0
            || self.effects.request().units().data_bytes()
                < compacted_outcome_data_bytes(self.limits).map_err(|_| IndexError::Exhausted)?
        {
            return Err(IndexError::Exhausted);
        }
        let native = match self.source {
            CompactedIndexSource::Initial { native, .. }
            | CompactedIndexSource::Successor { native, .. } => native,
        };
        if !native.matches_configured_root(self.authority.ingest_root)
            || native.manifest().identity().dataset_id != session.binding.dataset
        {
            return Err(IndexError::Conflict);
        }
        // This fixed reader's maximum is the unchanged physical freshness
        // window; the caller's original operation/cancellation deadline also
        // bounds it and is never extended by this final SQL observation.
        let ttl = self.authority.policy_ttl.min(Duration::from_secs(30));
        let mut lease = PolicyLeaseTracker::new(
            session.binding.dataset,
            session.issuer.process_incarnation,
            native.manifest().identity().policy_epoch,
            PolicyLeaseTtl::new(ttl).map_err(|_| IndexError::Unavailable)?,
        )
        .map_err(|_| IndexError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| IndexError::Unavailable)?;
        let row=tx.query_one("SELECT left(d.policy_revision::text,21),\n            LEAST(c.lease_expires_at_us,i.lease_expires_at_us)-(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint\n            FROM control.datasets d JOIN control.owner_epochs c ON c.dataset_id=d.dataset_id\n            JOIN control.owner_epochs i ON i.dataset_id=d.dataset_id\n            WHERE d.dataset_id=$1::bytea AND c.resource_id=$2::bytea AND c.epoch=$3::text::numeric\n                AND c.process_incarnation=$4::bytea AND i.resource_id=$5::bytea AND i.epoch=$6::text::numeric",
            &[&&session.binding.dataset.as_bytes()[..],&&session.issuer.resource_id.as_bytes()[..],&session.issuer.epoch.get().to_string(),
                &&session.issuer.process_incarnation.as_bytes()[..],&&session.owner.resource.as_bytes()[..],&session.owner.epoch.get().to_string()])
            .await.map_err(|_|IndexError::Backend)?;
        let policy = PolicyEpoch::new(
            row.try_get::<_, &str>(0)
                .map_err(|_| IndexError::Backend)?
                .parse()
                .map_err(|_| IndexError::Conflict)?,
        );
        let remaining = u64::try_from(row.try_get::<_, i64>(1).map_err(|_| IndexError::Backend)?)
            .map_err(|_| IndexError::Unavailable)?;
        drop(row);
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset: session.binding.dataset,
                    process_incarnation: session.issuer.process_incarnation,
                    policy_epoch: policy,
                    ttl: ttl.min(Duration::from_micros(remaining)),
                },
            )
            .map_err(|_| IndexError::Unavailable)?;
        match self.source {
            CompactedIndexSource::Initial {
                proof,
                native,
                registration,
            } => producer_compaction::require_initial_index_source(
                tx,
                proof,
                native,
                registration,
                session.issuer,
                self.limits,
                self.effects,
                &mut lease,
                policy,
                self.authority.cancelled,
            )
            .await
            .map_err(successor_history::compacted_error),
            CompactedIndexSource::Successor { proof, native } => {
                successor_history::require_compacted_index_source(
                    tx,
                    proof,
                    native,
                    session.issuer,
                    self.limits,
                    self.effects,
                    &mut lease,
                    policy,
                    self.authority.cancelled,
                )
                .await
            }
        }
        .map_err(|_| IndexError::Unavailable)
    }
}
