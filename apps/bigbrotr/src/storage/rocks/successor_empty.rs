//! Positive predecessor replay followed by one exact empty target close. The
//! connected classifier must separately prove absence of activation and detail.
use super::bootstrap::{Prepared, frame, read_options};
use super::lineage::LineageStamp;
use super::successor_handoff::{
    CLOSING_PRODUCER, EMPTY_SOURCE, OPEN_PRODUCER, producer_key, require_current_authority,
    require_permit, require_predecessor, require_value, source_key,
};
use super::{BootstrapError, BootstrapLimits, IngestRecovery, ResolvedLineage, RetainedLineage};
use crate::runtime::OwnedPermit;
use crate::storage::rocks::{
    keys::IngestKey,
    manifest::IngestManifest,
    succession::{MAX_SUCCESSION_BYTES, SuccessionRecord},
};
use bigbrotr_model::{OwnerEpoch, ProducerSlotId};
use sha2::{Digest, Sha256};
use std::sync::atomic::AtomicBool;

/// Fixed owned proof with no writer conversion. The original lineage grant
/// remains retained by the connected owner through native close and SQL commit.
pub struct EmptySuccessorRetirement {
    record: SuccessionRecord,
    stamp: LineageStamp,
    initially_closing: bool,
    digest: [u8; 32],
}
impl EmptySuccessorRetirement {
    pub fn from_lineage(
        proof: &ResolvedLineage<'_>,
        cancelled: &AtomicBool,
    ) -> Result<Self, BootstrapError> {
        if proof.lineage().has_compacted_initial() {
            return Err(BootstrapError::StateMismatch);
        }
        proof.validate(cancelled)?;
        let lineage = proof.lineage();
        let head = lineage.native_head();
        let epoch = head.manifest().identity().owner_epoch.get();
        if epoch < 3
            || head.manifest().key_schema() != 3
            || lineage.pending_target().is_some()
            || proof.verified_through().checked_add(1) != Some(epoch)
            || head.issued_through() != 0
            || head.source_through() != 0
        {
            return Err(BootstrapError::StateMismatch);
        }
        let identity = head.manifest().identity();
        let key = IngestKey::OwnerHandoff {
            dataset_id: identity.dataset_id,
            owner_resource_id: identity.owner_resource_id,
            predecessor_epoch: OwnerEpoch::new(epoch - 1)
                .map_err(|_| BootstrapError::StateMismatch)?,
        };
        let encoded = key.encode().map_err(|_| BootstrapError::InvalidIntent)?;
        let bytes = proof
            .recovery()
            .store
            .db
            .get_pinned_opt(encoded.as_bytes(), &read_options())
            .map_err(|_| BootstrapError::EngineRead)?
            .ok_or(BootstrapError::StateMismatch)?;
        if bytes.len() > MAX_SUCCESSION_BYTES {
            return Err(BootstrapError::Limit);
        }
        let record = SuccessionRecord::decode(&bytes).map_err(|_| BootstrapError::StateMismatch)?;
        if record.key() != key
            || record.fields().next_manifest != *head.manifest()
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != head.incoming_handoff_sha256()
        {
            return Err(BootstrapError::StateMismatch);
        }
        require_predecessor(proof, &record)?;
        let mut digest = Sha256::new();
        digest.update(b"bigbrotr/ingest-successor-empty-closure/1\0");
        digest.update(head.incoming_handoff_sha256());
        digest.update(Sha256::digest(head.manifest().encode().as_bytes()));
        digest.update(lineage.producer_slot_id().as_bytes());
        digest.update(epoch.to_be_bytes());
        digest.update(0_u64.to_be_bytes());
        digest.update(0_u64.to_be_bytes());
        let plan = Self {
            record,
            stamp: lineage.transition_stamp(),
            initially_closing: head.is_closing(),
            digest: digest.finalize().into(),
        };
        drop(bytes);
        proof
            .recovery()
            .validate_empty_successor_retirement(lineage, &plan, cancelled)?;
        Ok(plan)
    }
    pub fn record(&self) -> &SuccessionRecord {
        &self.record
    }
    pub fn target_manifest(&self) -> &IngestManifest {
        &self.record.fields().next_manifest
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.record.fields().producer_slot_id
    }
    pub fn closure_digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn initially_closing(&self) -> bool {
        self.initially_closing
    }
}

pub struct ObservedEmptySuccessorRetirement {
    record: SuccessionRecord,
    digest: [u8; 32],
    _permit: OwnedPermit,
}
impl ObservedEmptySuccessorRetirement {
    pub fn record(&self) -> &SuccessionRecord {
        &self.record
    }
    pub fn closure_digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl IngestRecovery {
    pub(in crate::storage) fn validate_empty_successor_retirement(
        &self,
        lineage: &RetainedLineage,
        plan: &EmptySuccessorRetirement,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        self.empty_successor_state(lineage, plan, cancelled)
            .map(|_| ())
    }

    fn empty_successor_state(
        &self,
        lineage: &RetainedLineage,
        plan: &EmptySuccessorRetirement,
        cancelled: &AtomicBool,
    ) -> Result<bool, BootstrapError> {
        if lineage.native_head().manifest() != plan.target_manifest()
            || lineage.producer_slot_id() != plan.producer_slot_id()
        {
            return Err(BootstrapError::StateMismatch);
        }
        let sequence = self.store.db.latest_sequence_number();
        let closing = if sequence == plan.stamp.sequence {
            plan.initially_closing
        } else if !plan.initially_closing && plan.stamp.sequence.checked_add(1) == Some(sequence) {
            true
        } else {
            return Err(BootstrapError::StateMismatch);
        };
        lineage.validate_transition_snapshot(
            self,
            plan.stamp,
            plan.target_manifest(),
            sequence,
            cancelled,
        )?;
        require_value(
            self,
            IngestKey::Manifest,
            Some(plan.target_manifest().encode().as_bytes()),
        )?;
        require_value(
            self,
            plan.record.key(),
            Some(plan.record.encode().as_bytes()),
        )?;
        require_value(
            self,
            source_key(plan.target_manifest()),
            Some(&EMPTY_SOURCE),
        )?;
        require_value(
            self,
            producer_key(plan.target_manifest(), plan.producer_slot_id()),
            Some(if closing {
                &CLOSING_PRODUCER[..]
            } else {
                &OPEN_PRODUCER[..]
            }),
        )?;
        lineage.validate_transition_snapshot(
            self,
            plan.stamp,
            plan.target_manifest(),
            sequence,
            cancelled,
        )?;
        Ok(closing)
    }

    /// Only the connected empty-attempt adapter may dispatch, under current
    /// Control and exact absence of activation. The native change is one marker;
    /// source closure and all relational terminal transitions remain PG work.
    pub(in crate::storage) fn close_empty_successor(
        &mut self,
        lineage: &RetainedLineage,
        plan: &EmptySuccessorRetirement,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ObservedEmptySuccessorRetirement, BootstrapError> {
        require_permit(&permit, 4096)?;
        authority()?;
        if !self.empty_successor_state(lineage, plan, cancelled)? {
            plan.stamp
                .sequence
                .checked_add(1)
                .ok_or(BootstrapError::Limit)?;
            let mut batch = Prepared::new(BootstrapLimits {
                max_mappings: 1,
                max_batch_bytes: 1024,
            })?;
            batch.put(
                producer_key(plan.target_manifest(), plan.producer_slot_id()),
                frame(&[], &[&CLOSING_PRODUCER])?,
            )?;
            self.store.commit_prepared(batch, || {
                require_current_authority(cancelled, &mut authority)
            })?;
        }
        authority()?;
        if !self.empty_successor_state(lineage, plan, cancelled)? {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(ObservedEmptySuccessorRetirement {
            record: plan.record,
            digest: plan.digest,
            _permit: permit,
        })
    }
}
