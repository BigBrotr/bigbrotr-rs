//! Streaming validation of retained canonical outcomes against a native seal.
//! The caller still owes PostgreSQL identity/lifecycle/head checks and confirmed
//! transaction completion under its own read or retirement contract.
use super::*;
use crate::storage::rocks::store::MAX_OUTCOME_BYTES;
use crate::storage::rocks::store::admission::{ReadBudget, read_rejection};
use crate::storage::rocks::store::{NativeOutcomeKind, PendingBatchHeader};
use bigbrotr_model::{
    Digest256, FactRef, PolicyEpoch, ProtectionClass, ProtectionContext, Receipt,
    receipt_codec::ReceiptDecodeLimits,
};

/// Caller-owned row bytes remain covered by its SQL grant. The native grant
/// covers a decoded receipt, one Fact, re-encoding and the bounded input scan.
pub fn compacted_outcome_data_bytes(limits: AdmissionLimits) -> Result<u64, AdmissionError> {
    Ok(limits.required_dictionary_replay_data_bytes()? + 2 * MAX_OUTCOME_BYTES as u64)
}

pub struct CompactedOutcomeRow<'a> {
    pub request: ProducerRequestId,
    pub digest: Digest256,
    pub kind: NativeOutcomeKind,
    pub policy_epoch: PolicyEpoch,
    pub bytes: &'a [u8],
    pub input_bytes: u32,
}

pub struct CompactedPrefixResolution<'a> {
    prefix: PrefixOwner<'a>,
    recovery: &'a IngestRecovery,
    requests: u64,
    source_through: u64,
    digest: Sha256,
}

/// Native consistency only. It cannot be converted to original-input replay,
/// ACK credit, a writer or reclamation permission. A bounded native handoff
/// may retain its compacted basis; SQL preparation/activation remain separate.
/// The PG owner wraps it only after authenticating the exact retained history
/// and confirming transaction
/// completion: COMMIT for current initial retirement, ROLLBACK for history reads.
pub struct VerifiedCompactedPrefix<'a> {
    prefix: PrefixOwner<'a>,
    recovery: &'a IngestRecovery,
}

impl RetainedCompactedPrefix {
    pub fn resolution<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<CompactedPrefixResolution<'a>, AdmissionError> {
        CompactedPrefixResolution::new(PrefixOwner::Initial(self), recovery, cancelled)
    }
}

impl<'a> CompactedPrefixResolution<'a> {
    pub(in crate::storage::rocks::store) fn from_handoff(
        view: CompactedInitialPredecessor<'a>,
        recovery: &'a IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<Self, AdmissionError> {
        Self::new(PrefixOwner::Handoff(view), recovery, cancelled)
    }

    pub(super) fn new(
        prefix: PrefixOwner<'a>,
        recovery: &'a IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<Self, AdmissionError> {
        prefix.validate(recovery, cancelled)?;
        let mut digest = Sha256::new();
        digest.update(b"bigbrotr/initial-source-closure/1\0");
        digest.update(prefix.floor().fields().manifest_sha256);
        digest.update(prefix.slot().as_bytes());
        digest.update(1_u64.to_be_bytes());
        digest.update(prefix.floor().fields().issued_through.to_be_bytes());
        Ok(CompactedPrefixResolution {
            prefix,
            recovery,
            requests: 0,
            source_through: 0,
            digest,
        })
    }
}

impl CompactedPrefixResolution<'_> {
    pub(in crate::storage) fn manifest(&self) -> &IngestManifest {
        self.prefix.manifest()
    }
    pub(in crate::storage) fn floor(&self) -> &ClosedProducerFloor {
        self.prefix.floor()
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.prefix.slot()
    }

    pub fn record(
        &mut self,
        row: CompactedOutcomeRow<'_>,
        limits: AdmissionLimits,
        permit: &OwnedPermit,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<(), AdmissionError> {
        if permit.request().units().items() == 0
            || permit.request().units().data_bytes() < compacted_outcome_data_bytes(limits)?
        {
            return Err(AdmissionError::InsufficientReservation);
        }
        authority()?;
        self.prefix.validate(self.recovery, cancelled)?;
        let floor = self.prefix.floor().fields();
        let identity = self.prefix.manifest().identity();
        if row.request.producer_slot_id != self.prefix.slot()
            || row.request.producer_epoch.get() != 1
            || self.requests.checked_add(1) != Some(row.request.request_sequence.get())
            || row.request.request_sequence.get() > floor.issued_through
            || row.bytes.is_empty()
            || row.bytes.len() > MAX_OUTCOME_BYTES
            || row.input_bytes == 0
            || row.input_bytes > 16 * 1024 * 1024
        {
            return Err(AdmissionError::StateMismatch);
        }
        self.prefix.require_input(
            self.recovery,
            (
                row.request,
                *row.digest.as_bytes(),
                u64::from(row.input_bytes),
            ),
            cancelled,
        )?;
        let next_source = if row.kind == NativeOutcomeKind::Rejected {
            let (_, policy) =
                read_rejection(row.bytes, row.request, row.digest, identity.dataset_id)?;
            if policy != row.policy_epoch {
                return Err(AdmissionError::StateMismatch);
            }
            self.source_through
        } else {
            let receipt = Receipt::decode_persistent(
                row.bytes,
                ReceiptDecodeLimits {
                    max_encoded_bytes: MAX_OUTCOME_BYTES,
                    max_allocation_bytes: 2 * limits.max_dependencies * size_of::<FactRef>(),
                    max_batch_records: limits.max_dependencies,
                    max_dependencies: limits.max_dependencies,
                },
            )
            .map_err(BootstrapError::from)?;
            let protection = ProtectionContext {
                dataset_id: identity.dataset_id,
                topology_generation: identity.topology_generation,
                protection_class: ProtectionClass::LocalSynced,
                recovery_generation: identity.recovery_generation,
                // Preserve the original decision, independently of the manifest policy.
                policy_epoch: row.policy_epoch,
            };
            if receipt.request_id() != row.request
                || receipt.request_digest() != row.digest
                || receipt.protection_context() != &protection
                || (receipt.batch().is_some() != (row.kind == NativeOutcomeKind::NewFacts))
                || receipt
                    .encode_persistent(MAX_OUTCOME_BYTES)
                    .map_err(BootstrapError::from)?
                    != row.bytes
            {
                return Err(AdmissionError::StateMismatch);
            }
            let mut reads = ReadBudget::dictionaries(limits);
            let mut next = self.source_through;
            if let Some(batch) = receipt.batch() {
                let first = batch.first();
                if first.dataset_id != identity.dataset_id
                    || first.shard_id != identity.shard_id
                    || first.owner_epoch != identity.owner_epoch
                    || next.checked_add(1) != Some(first.sequence.get())
                    || batch.last().sequence.get() > floor.source_through
                {
                    return Err(AdmissionError::StateMismatch);
                }
                next = batch.last().sequence.get();
                authority()?;
                check_cancelled(cancelled)?;
                let pending = self
                    .recovery
                    .store
                    .admission_value(IngestKey::PendingBatch { first }, 128, &mut reads)?
                    .ok_or(AdmissionError::StateMismatch)?;
                if pending != PendingBatchHeader::from_batch(batch).encode() {
                    return Err(AdmissionError::StateMismatch);
                }
                for reference in batch.records() {
                    authority()?;
                    check_cancelled(cancelled)?;
                    self.recovery
                        .store
                        .admission_fact(*reference, limits, &mut reads)?;
                }
            }
            for reference in receipt.dependencies().records() {
                authority()?;
                check_cancelled(cancelled)?;
                let source = reference.source_record_id;
                if source.dataset_id != identity.dataset_id
                    || source.shard_id != identity.shard_id
                    || source.owner_epoch != identity.owner_epoch
                    || source.sequence.get() > next
                {
                    return Err(AdmissionError::StateMismatch);
                }
                self.recovery
                    .store
                    .admission_fact(*reference, limits, &mut reads)?;
            }
            next
        };
        authority()?;
        self.prefix.validate(self.recovery, cancelled)?;
        // A refused row leaves the ordered accumulator unchanged.
        self.digest
            .update(row.request.request_sequence.get().to_be_bytes());
        self.digest.update(row.digest.as_bytes());
        self.digest.update(Sha256::digest(row.bytes));
        self.requests = row.request.request_sequence.get();
        self.source_through = next_source;
        Ok(())
    }
}

impl<'a> CompactedPrefixResolution<'a> {
    pub fn finish(
        mut self,
        cancelled: &AtomicBool,
    ) -> Result<VerifiedCompactedPrefix<'a>, AdmissionError> {
        self.prefix.validate(self.recovery, cancelled)?;
        let floor = self.prefix.floor().fields();
        self.digest.update(self.source_through.to_be_bytes());
        if self.requests != floor.issued_through
            || self.source_through != floor.source_through
            || <[u8; 32]>::from(self.digest.finalize()) != floor.closure_digest
        {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(VerifiedCompactedPrefix {
            prefix: self.prefix,
            recovery: self.recovery,
        })
    }
}

impl VerifiedCompactedPrefix<'_> {
    pub(in crate::storage::rocks::store) fn recovery(&self) -> &IngestRecovery {
        self.recovery
    }

    // A handoff can retain only current epoch1 or a genuinely empty two-entry
    // Prepared2 observation. Later history and plan-derived rechecks cannot mint
    // a fresh handoff basis or silently renew a stored sequence/input stamp.
    pub(in crate::storage::rocks::store) fn handoff_context(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<(&IngestRecovery, [u8; 32]), AdmissionError> {
        self.prefix.validate(self.recovery, cancelled)?;
        let inputs = match self.prefix {
            PrefixOwner::Initial(prefix) => prefix.inputs,
            PrefixOwner::Ancestor(lineage) => {
                let head = lineage.native_head();
                if lineage.entries.len() != 2
                    || lineage.pending_target().is_some()
                    || head.manifest.key_schema() != 2
                    || head.manifest.identity().owner_epoch.get() != 2
                    || head.issued != 0
                    || head.acknowledged != 0
                    || head.source_through != 0
                    || !head.source_present
                    || head.closing
                    || head.allocations != [0; 9]
                {
                    return Err(AdmissionError::StateMismatch);
                }
                lineage.inputs
            }
            PrefixOwner::Handoff(_) => return Err(AdmissionError::StateMismatch),
        };
        Ok((self.recovery, inputs))
    }

    pub(in crate::storage::rocks::store) fn matches_lineage(
        &self,
        lineage: &RetainedLineage,
    ) -> bool {
        matches!(self.prefix, PrefixOwner::Ancestor(owner) if std::ptr::eq(owner, lineage))
    }
    pub fn floor(&self) -> &ClosedProducerFloor {
        self.prefix.floor()
    }
    pub fn manifest(&self) -> &IngestManifest {
        self.prefix.manifest()
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.prefix.slot()
    }
    pub fn validate(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), AdmissionError> {
        self.prefix.validate(self.recovery, cancelled)?;
        self.prefix.validate(recovery, cancelled)
    }
}
