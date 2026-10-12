//! Initial metadata succession under continuous native custody; never admission.
use super::bootstrap::{Prepared, frame, read_options};
use super::{BootstrapError, BootstrapLimits, IngestRecovery, IngestStore, ResolvedSource};
use crate::runtime::OwnedPermit;
use crate::storage::rocks::{
    keys::IngestKey,
    manifest::{IngestManifest, MAX_MANIFEST_BYTES},
};
use bigbrotr_model::{
    DictionaryKind, Ordinal, ProducerEpoch, ProducerSlotId, QualifiedRef, Sequence, SourceRecordId,
};

use std::os::unix::fs::MetadataExt;

const MAX_HANDOFF_BYTES: usize = 78 + 2 * MAX_MANIFEST_BYTES;

/// A positive, bounded initial source proof and exact proposed target. This is
/// data, not authority. No external decoder can fabricate its native witness.
#[derive(Clone, Debug)]
pub struct InitialHandoff {
    previous: IngestManifest,
    next: IngestManifest,
    slot: ProducerSlotId,
    epoch: ProducerEpoch,
    issued: u64,
    source_through: u64,
    closure_digest: [u8; 32],
    // Local proof guard, deliberately not part of the portable receipt. A
    // future process-recovery adapter must reconstruct a positive witness.
    native_sequence: u64,
    native_root: (u64, u64),
}
impl InitialHandoff {
    pub fn from_source(
        source: &ResolvedSource<'_>,
        next: IngestManifest,
    ) -> Result<Self, BootstrapError> {
        let previous = *source.producer().manifest();
        let old = previous.identity();
        let new = next.identity();
        if previous.key_schema() != 1
            || next.key_schema() != 2
            || old.owner_epoch.get() != 1
            || source.producer().producer_epoch().get() != 1
            || old.owner_epoch.get().checked_add(1) != Some(new.owner_epoch.get())
            || old.topology_generation.get().checked_add(1) != Some(new.topology_generation.get())
            || old.dataset_id != new.dataset_id
            || old.owner_resource_id != new.owner_resource_id
            || old.shard_id != new.shard_id
            || old.recovery_generation != new.recovery_generation
            || old.process_incarnation == new.process_incarnation
        {
            return Err(BootstrapError::InvalidIntent);
        }
        for number in 1..=9 {
            let kind = DictionaryKind::try_from(number).expect("assigned bounded kind");
            let old_domain = previous.allocation_namespaces().domain(kind);
            let next_domain = next.allocation_namespaces().domain(kind);
            if old_domain.is_some() != next_domain.is_some() {
                return Err(BootstrapError::InvalidIntent);
            }
            if let Some(domain) = next_domain {
                for other in 1..=9 {
                    let other_kind =
                        DictionaryKind::try_from(other).expect("assigned bounded kind");
                    if previous.allocation_namespaces().domain(other_kind) == Some(domain)
                        || (other != number
                            && next.allocation_namespaces().domain(other_kind) == Some(domain))
                    {
                        return Err(BootstrapError::InvalidIntent);
                    }
                }
            }
        }
        Ok(Self {
            previous,
            next,
            slot: source.producer().producer_slot_id(),
            epoch: source.producer().producer_epoch(),
            issued: source.resolved_requests(),
            source_through: source.last_complete_sequence(),
            closure_digest: source.digest(),
            native_sequence: source.recovery().store.db.latest_sequence_number(),
            native_root: source.recovery().store.handoff_root_identity()?,
        })
    }
    pub fn previous_manifest(&self) -> &IngestManifest {
        &self.previous
    }
    pub fn next_manifest(&self) -> &IngestManifest {
        &self.next
    }
    pub fn producer_slot_id(&self) -> ProducerSlotId {
        self.slot
    }
    pub fn producer_epoch(&self) -> ProducerEpoch {
        self.epoch
    }
    pub fn issued_through(&self) -> u64 {
        self.issued
    }
    pub fn last_complete_sequence(&self) -> u64 {
        self.source_through
    }
    pub fn closure_digest(&self) -> [u8; 32] {
        self.closure_digest
    }

    pub fn encode(&self) -> EncodedHandoff {
        let mut output = EncodedHandoff {
            bytes: [0; MAX_HANDOFF_BYTES],
            len: 0,
        };
        output.push(&[1, 10]);
        output.push(self.slot.as_bytes());
        output.push(&self.epoch.get().to_be_bytes());
        output.push(&self.issued.to_be_bytes());
        output.push(&self.source_through.to_be_bytes());
        output.push(&self.closure_digest);
        for manifest in [&self.previous, &self.next] {
            let encoded = manifest.encode();
            output.push(&(encoded.as_bytes().len() as u16).to_be_bytes());
            output.push(encoded.as_bytes());
        }
        output
    }
    fn key(&self) -> IngestKey {
        let id = self.previous.identity();
        IngestKey::OwnerHandoff {
            dataset_id: id.dataset_id,
            owner_resource_id: id.owner_resource_id,
            predecessor_epoch: id.owner_epoch,
        }
    }
    fn source_key(&self) -> IngestKey {
        let id = self.next.identity();
        IngestKey::SourceStream {
            dataset_id: id.dataset_id,
            shard_id: id.shard_id,
            owner_epoch: id.owner_epoch,
        }
    }
    fn producer_key(&self) -> IngestKey {
        IngestKey::ProducerState {
            producer_slot_id: self.slot,
            producer_epoch: ProducerEpoch::new(2).expect("initial successor"),
        }
    }
}

pub struct EncodedHandoff {
    bytes: [u8; MAX_HANDOFF_BYTES],
    len: usize,
}
impl EncodedHandoff {
    fn push(&mut self, bytes: &[u8]) {
        self.bytes[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// An observation only. No conversion to a native writer or producer grant.
pub struct ObservedHandoff {
    handoff: InitialHandoff,
    _permit: OwnedPermit,
}
impl ObservedHandoff {
    pub fn handoff(&self) -> &InitialHandoff {
        &self.handoff
    }
}

impl IngestRecovery {
    pub(in crate::storage) fn apply_initial_handoff(
        &mut self,
        handoff: &InitialHandoff,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ObservedHandoff, BootstrapError> {
        if permit.request().units().data_bytes() < 8192
            || permit.request().units().native_bytes() < 8192
            || permit.request().units().items() == 0
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        authority()?;
        self.validate_initial_handoff(handoff)?;
        let completed = self.manifest() == &handoff.next;
        let encoded = handoff.encode();
        let source_zero = [1, 2, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut producer_zero = [0; 26];
        producer_zero[..2].copy_from_slice(&[1, 49]);
        if !completed {
            let mut batch = Prepared::new(BootstrapLimits {
                max_mappings: 1,
                max_batch_bytes: 2048,
            })?;
            batch.put(handoff.key(), frame(&[], &[encoded.as_bytes()])?)?;
            batch.put(
                IngestKey::Manifest,
                frame(&[], &[handoff.next.encode().as_bytes()])?,
            )?;
            batch.put(handoff.source_key(), frame(&[], &[&source_zero])?)?;
            batch.put(handoff.producer_key(), frame(&[], &[&producer_zero])?)?;
            self.store.commit_prepared(batch, &mut authority)?;
            self.store.manifest = handoff.next;
        }
        authority()?;
        Ok(ObservedHandoff {
            handoff: handoff.clone(),
            _permit: permit,
        })
    }
    /// Read-only preflight before PostgreSQL prepares persistent metadata.
    pub(in crate::storage) fn validate_initial_handoff(
        &self,
        handoff: &InitialHandoff,
    ) -> Result<(), BootstrapError> {
        if self.store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        let completed = self.manifest() == &handoff.next;
        let sequence = if completed {
            handoff
                .native_sequence
                .checked_add(4)
                .ok_or(BootstrapError::StateMismatch)?
        } else {
            handoff.native_sequence
        };
        if self.store.handoff_root_identity()? != handoff.native_root
            || (!completed && self.manifest() != &handoff.previous)
            || self.store.db.latest_sequence_number() != sequence
        {
            return Err(BootstrapError::StateMismatch);
        }
        let manifest = if completed {
            handoff.next
        } else {
            handoff.previous
        };
        self.store
            .handoff_value(IngestKey::Manifest, Some(manifest.encode().as_bytes()))?;
        let encoded = handoff.encode();
        let source_zero = [1, 2, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut producer_zero = [0; 26];
        producer_zero[..2].copy_from_slice(&[1, 49]);
        for (key, bytes) in [
            (handoff.key(), encoded.as_bytes()),
            (handoff.source_key(), &source_zero),
            (handoff.producer_key(), &producer_zero),
        ] {
            self.store
                .handoff_value(key, if completed { Some(bytes) } else { None })?;
        }
        // The sequence guard preserves the entire prior positive proof, not
        // merely its last receipt. Validate the explicit empty target domains
        // separately: they were not part of the initial source closure census.
        self.store.require_empty_handoff_target(handoff)?;
        Ok(())
    }
}
impl IngestStore {
    fn handoff_root_identity(&self) -> Result<(u64, u64), BootstrapError> {
        let metadata =
            std::fs::metadata(&self._reservation.path.0).map_err(|_| BootstrapError::EngineRead)?;
        Ok((metadata.dev(), metadata.ino()))
    }
    fn handoff_value(&self, key: IngestKey, expected: Option<&[u8]>) -> Result<(), BootstrapError> {
        let key = key.encode().map_err(|_| BootstrapError::InvalidIntent)?;
        let cf = self
            .db
            .cf_handle(key.column_family().name())
            .ok_or(BootstrapError::EngineRead)?;
        let value = self
            .db
            .get_pinned_cf_opt(cf, key.as_bytes(), &read_options())
            .map_err(|_| BootstrapError::EngineRead)?;
        if value.as_deref() != expected {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }
    fn handoff_empty_prefix(&self, key: IngestKey, suffix: usize) -> Result<(), BootstrapError> {
        let key = key.encode().map_err(|_| BootstrapError::InvalidIntent)?;
        let prefix = &key.as_bytes()[..key.as_bytes().len() - suffix];
        let cf = self
            .db
            .cf_handle(key.column_family().name())
            .ok_or(BootstrapError::EngineRead)?;
        let mut cursor = self.db.raw_iterator_cf_opt(cf, read_options());
        cursor.seek(prefix);
        cursor.status().map_err(|_| BootstrapError::EngineRead)?;
        if cursor.key().is_some_and(|key| key.starts_with(prefix)) {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }
    fn require_empty_handoff_target(&self, handoff: &InitialHandoff) -> Result<(), BootstrapError> {
        let id = handoff.next.identity();
        let first = SourceRecordId {
            dataset_id: id.dataset_id,
            shard_id: id.shard_id,
            owner_epoch: id.owner_epoch,
            sequence: Sequence::new(1).expect("nonzero"),
        };
        self.handoff_empty_prefix(
            IngestKey::Fact {
                source_record_id: first,
            },
            8,
        )?;
        self.handoff_empty_prefix(IngestKey::PendingBatch { first }, 8)?;
        self.handoff_empty_prefix(
            IngestKey::Request {
                request_id: bigbrotr_model::ProducerRequestId {
                    producer_slot_id: handoff.slot,
                    producer_epoch: ProducerEpoch::new(2).expect("initial successor"),
                    request_sequence: bigbrotr_model::RequestSequence::new(1).expect("nonzero"),
                },
            },
            8,
        )?;
        for number in 1..=9 {
            let kind = DictionaryKind::try_from(number).expect("assigned bounded kind");
            if let Some(domain) = handoff.next.allocation_namespaces().domain(kind) {
                self.handoff_value(
                    IngestKey::AllocationStream {
                        dataset_id: id.dataset_id,
                        dictionary_kind: kind,
                        allocation_domain: domain,
                    },
                    None,
                )?;
                self.handoff_empty_prefix(
                    IngestKey::Dictionary {
                        reference: QualifiedRef {
                            dataset_id: id.dataset_id,
                            dictionary_kind: kind,
                            allocation_domain: domain,
                            ordinal: Ordinal::new(1).expect("nonzero"),
                        },
                    },
                    8,
                )?;
            }
        }
        Ok(())
    }
}
