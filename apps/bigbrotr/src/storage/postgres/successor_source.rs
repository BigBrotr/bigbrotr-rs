//! Positive source closure for an activated repeatable successor. These opaque
//! wrappers share the recovery kernel, never its public source-open capability.
//! Publication is terminal for this owner: retain native/Control custody on all
//! outcomes, then tear down and inspect again for a retry.
use super::*;

#[path = "successor_acquisition.rs"]
mod acquisition;

// Atomic only to preserve the owner's Send/Sync custody across async workers;
// operations are still serialized by the borrowed Control session.
#[derive(Debug, Default)]
struct PublicationState(AtomicBool);
impl PublicationState {
    fn start(&self) -> Result<(), OwnerError> {
        if self.0.swap(true, Ordering::Relaxed) {
            return Err(OwnerError::Unavailable);
        }
        Ok(())
    }
    fn require_available(&self) -> Result<(), OwnerError> {
        if self.0.load(Ordering::Relaxed) {
            return Err(OwnerError::Unavailable);
        }
        Ok(())
    }
}

pub struct ActivatedSuccessorSourceRecovery {
    inner: ActivatedSuccessorRecovery,
    publication: PublicationState,
}
pub struct ActivatedSuccessorSourceResolution<'a> {
    inner: ActivatedSuccessorResolution<'a>,
    owner: &'a ActivatedSuccessorSourceRecovery,
}
/// A complete native/SQL prefix under the distinct source purpose. This is not
/// a retirement, handoff, workflow or writer capability. It borrows the original
/// owner and its single retained lineage allocation.
pub struct ResolvedActivatedSuccessorSource<'a> {
    inner: ResolvedActivatedSuccessor<'a>,
    owner: &'a ActivatedSuccessorSourceRecovery,
}

// Extra source wrappers are fixed headers within the same PG half-scratch. The
// larger common recovery assertion includes all projections and both digest
// arrays; this allowance also covers the post-publication Epoch projection.
const _: () = assert!(
    size_of::<ActivatedSuccessorSourceRecovery>() - size_of::<ActivatedSuccessorRecovery>()
        + size_of::<ActivatedSuccessorSourceResolution<'_>>()
        - size_of::<ActivatedSuccessorResolution<'_>>()
        + size_of::<ResolvedActivatedSuccessorSource<'_>>()
        - size_of::<ResolvedActivatedSuccessor<'_>>()
        + size_of::<Epoch>()
        < 1024
);

impl ActivatedSuccessorSourceRecovery {
    pub fn epoch(&self) -> u64 {
        self.inner.epoch()
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.inner.producer_slot_id()
    }
    pub fn issued_through(&self) -> u64 {
        self.inner.issued_through()
    }
    pub fn reserved_through(&self) -> u64 {
        self.inner.reserved_through()
    }
    pub fn staged_input(&self) -> Option<SuccessorRecoveryInput> {
        self.inner.staged_input()
    }
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
    /// The immutable SQL observation made at inspection, not a publication ACK.
    pub fn source_closed(&self) -> bool {
        self.inner.head.source == SourceState::Closed
    }
    pub fn resolution<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        cancelled: &'a AtomicBool,
    ) -> Result<ActivatedSuccessorSourceResolution<'a>, BootstrapError> {
        self.require_available()
            .map_err(|_| BootstrapError::AuthorityUnavailable)?;
        Ok(ActivatedSuccessorSourceResolution {
            inner: self.inner.resolution(recovery, cancelled)?,
            owner: self,
        })
    }
    fn require_available(&self) -> Result<(), OwnerError> {
        self.publication.require_available()
    }
}
impl ActivatedSuccessorSourceResolution<'_> {
    pub fn current_epoch(&self) -> Option<u64> {
        self.inner.current_epoch()
    }
    pub fn next_request(&self) -> Option<ProducerRequestId> {
        self.inner.next_request()
    }
}
impl ResolvedActivatedSuccessorSource<'_> {
    pub fn recovery_acknowledgement(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<crate::storage::rocks::store::RecoveryAcknowledgement, BootstrapError> {
        self.validate(cancelled)?;
        self.inner.recovery_acknowledgement(cancelled)
    }
    pub fn into_recovery_acknowledgement(
        self,
        cancelled: &AtomicBool,
    ) -> Result<crate::storage::rocks::store::RecoveryAcknowledgement, BootstrapError> {
        self.recovery_acknowledgement(cancelled)
    }
    pub fn verified_through(&self) -> u64 {
        self.inner.verified_through()
    }
    pub fn last_epoch(&self) -> LineageEpochSummary {
        self.inner.last_epoch()
    }
    pub fn matches_source(&self, owner: &ActivatedSuccessorSourceRecovery) -> bool {
        std::ptr::eq(self.owner, owner) && self.inner.matches_recovery(&owner.inner)
    }
    pub fn validate(&self, cancelled: &AtomicBool) -> Result<(), BootstrapError> {
        self.owner
            .require_available()
            .map_err(|_| BootstrapError::AuthorityUnavailable)?;
        self.inner.validate(cancelled)
    }
}

impl ControlSession {
    pub async fn inspect_activated_successor_source(
        &mut self,
        recovery: &IngestRecovery,
        input: SuccessorReplayRequest<'_>,
        metadata_permit: OwnedPermit,
        input_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ActivatedSuccessorSourceRecovery, ProducerResolveError> {
        let inner = self
            .inspect_successor_recovery(
                recovery,
                input,
                metadata_permit,
                input_permit,
                Purpose::Source,
                validate,
            )
            .await?;
        Ok(ActivatedSuccessorSourceRecovery {
            inner,
            publication: PublicationState::default(),
        })
    }

    /// Source-open retains the durable native-first barrier behavior. A closed
    /// source instead observes an already CLOSING native marker without a write.
    pub async fn close_activated_successor_source_producer(
        &mut self,
        recovery: &mut IngestRecovery,
        owner: &mut ActivatedSuccessorSourceRecovery,
        input: InitialHistoryRequest<'_>,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(), ProducerResolveError> {
        owner.require_available()?;
        self.close_activated_successor_producer(recovery, &mut owner.inner, input, permit, validate)
            .await
    }

    pub async fn resolve_next_activated_successor_source_request(
        &mut self,
        resolution: &mut ActivatedSuccessorSourceResolution<'_>,
        input: SuccessorReplayRequest<'_>,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ProducerRequestId, ProducerResolveError> {
        resolution.owner.require_available()?;
        self.resolve_next_activated_successor_request(
            &mut resolution.inner,
            input,
            input_permit,
            outcome_permit,
            validate,
        )
        .await
    }

    pub async fn finish_activated_successor_source_epoch(
        &mut self,
        resolution: &mut ActivatedSuccessorSourceResolution<'_>,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<LineageEpochSummary, ProducerResolveError> {
        resolution.owner.require_available()?;
        self.finish_activated_successor_epoch(&mut resolution.inner, input, validate)
            .await
    }

    pub async fn finish_activated_successor_source<'a>(
        &mut self,
        resolution: ActivatedSuccessorSourceResolution<'a>,
        input: InitialHistoryRequest<'_>,
        limits: AdmissionLimits,
        permit: OwnedPermit,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<ResolvedActivatedSuccessorSource<'a>, ProducerResolveError> {
        resolution.owner.require_available()?;
        let inner = self
            .finish_activated_successor_recovery(resolution.inner, input, limits, permit, validate)
            .await?;
        Ok(ResolvedActivatedSuccessorSource {
            inner,
            owner: resolution.owner,
        })
    }

    /// Consumes the borrowed proof and spends its owner's publication state
    /// before fallible work. On error/unknown/cancellation there is no reusable
    /// source capability: native and its owner remain held by the caller until
    /// teardown. A fresh process must inspect the actual SQL outcome.
    pub async fn close_activated_successor_source(
        &mut self,
        source: ResolvedActivatedSuccessorSource<'_>,
        input: InitialHistoryRequest<'_>,
        validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(), ProducerResolveError> {
        source.owner.publication.start()?;
        let owner = &source.owner.inner;
        let proof = &source.inner;
        let current = self.identity;
        let (tx, mut lease, policy) = self
            .begin_checked_successor_recovery(proof.recovery, owner, &input, validate)
            .await?;
        if !proof.matches_recovery(owner) || !owner.is_closed() {
            return Err(OwnerError::InvalidInput.into());
        }
        proof.validate(input.cancelled).map_err(native_error)?;
        let summary = proof.last_epoch();
        require_head_summary(&summary, owner)?;
        require_complete(
            owner.issued_through(),
            owner.reserved_through(),
            owner.staged_input().is_some(),
        )?;
        require_recorded_recovery(
            &tx,
            proof.recovery,
            owner,
            &proof.native,
            &mut lease,
            policy,
            input.cancelled,
        )
        .await?;
        let id = owner.head.manifest.identity();
        let epoch = owner.epoch().to_string();
        let dataset = &&id.dataset_id.as_bytes()[..];
        let shard = i64::from(id.shard_id);
        if owner.head.source == SourceState::Open {
            let closing = tx
                .execute(
                    "UPDATE control.source_streams SET state='closing'
                WHERE dataset_id=$1::bytea AND shard_id=$2::bigint
                    AND owner_epoch=$3::text::numeric AND state='open'",
                    &[dataset, &shard, &epoch],
                )
                .await
                .map_err(backend)?;
            let closed = tx
                .execute(
                    "UPDATE control.source_streams SET state='closed',
                    closed_at_us=(extract(epoch FROM clock_timestamp())*1000000)::bigint,
                    last_complete_sequence=$4::text::numeric,closure_digest=$5::bytea
                WHERE dataset_id=$1::bytea AND shard_id=$2::bigint
                    AND owner_epoch=$3::text::numeric AND state='closing'",
                    &[
                        dataset,
                        &shard,
                        &epoch,
                        &summary.last_complete_sequence.to_string(),
                        &&summary.digest[..],
                    ],
                )
                .await
                .map_err(backend)?;
            if (closing, closed) != (1, 1) {
                return Err(OwnerError::InvalidMetadata.into());
            }
        }
        // The old observation stays immutable while the proof borrows it. Check
        // the exact new SQL state locally, never refresh or relax owner equality.
        projection::require_context(&tx, &owner.intent, current, Purpose::Source).await?;
        if projection::activation(&tx, &owner.intent).await? != owner.activation {
            return Err(OwnerError::InvalidMetadata.into());
        }
        let after = history_rows::read_epoch(&tx, proof.recovery, owner.epoch()).await?;
        require_published(&owner.head, &after, &summary)?;
        proof.validate(input.cancelled).map_err(native_error)?;
        rows::require_live(&tx, current, None).await?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        tx.commit()
            .await
            .map_err(|_| ProducerResolveError::CommitUnknown)?;
        proof.validate(input.cancelled).map_err(native_error)?;
        initial_history::check_delivery(&mut lease, policy, input.cancelled)?;
        if self.client.is_closed() {
            return Err(ProducerResolveError::DeliveryUnknown);
        }
        self.available = true;
        Ok(())
    }
}

fn require_published(
    before: &Epoch,
    after: &Epoch,
    summary: &LineageEpochSummary,
) -> Result<(), OwnerError> {
    let expected = Epoch {
        source: SourceState::Closed,
        source_cut: Some(summary.last_complete_sequence),
        source_digest: Some(summary.digest),
        ..*before
    };
    if before.producer != ProducerState::Closing || *after != expected {
        return Err(OwnerError::InvalidMetadata);
    }
    Ok(())
}

#[cfg(test)]
#[path = "successor_source_tests.rs"]
mod tests;
