//! Bounded positive prefix proof under continuous exclusive native custody.
use super::bootstrap::read_options;
use super::{
    AdmissionOutcome, BootstrapError, HistoricalInitial, IngestRecovery, ProducerClosing,
    ResolvedProducerRequest,
};
use crate::storage::rocks::keys::IngestKey;
use bigbrotr_model::{Sequence, SourceRecordId};
use sha2::{Digest, Sha256};

/// This accumulator owns the existing closing grant and borrows native custody.
/// It retains no receipt vector and cannot authorize admission, ACK or retirement.
pub struct SourceResolution<'a> {
    recovery: &'a IngestRecovery,
    closing: ProducerClosing,
    manifest_digest: [u8; 32],
    requests: u64,
    source_through: u64,
    digest: Sha256,
    event_requests: u64,
    historical: Option<HistoricalInitial<'a>>,
}

/// Only complete positive resolution can construct this native witness. Its
/// borrow prevents closing/replacing the native owner before connected use.
pub struct ResolvedSource<'a> {
    recovery: &'a IngestRecovery,
    closing: ProducerClosing,
    source_through: u64,
    digest: [u8; 32],
    event_requests: u64,
    historical: Option<HistoricalInitial<'a>>,
}
impl ResolvedSource<'_> {
    pub fn last_complete_sequence(&self) -> u64 {
        self.source_through
    }
    pub fn resolved_requests(&self) -> u64 {
        self.closing.issued_through()
    }
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub(in crate::storage) fn event_request_mask(&self) -> u64 {
        self.event_requests
    }
    pub(in crate::storage) fn recovery(&self) -> &IngestRecovery {
        self.recovery
    }
    pub(in crate::storage) fn producer(&self) -> &ProducerClosing {
        &self.closing
    }
    pub(super) fn matches_historical(&self, historical: &HistoricalInitial<'_>) -> bool {
        self.historical
            .as_ref()
            .is_some_and(|view| view.same_view(historical))
    }
    pub(in crate::storage) fn matches_recorded(
        &self,
        records: impl IntoIterator<Item = (u64, [u8; 32], [u8; 32])>,
    ) -> bool {
        let mut digest = initial_digest(&self.closing);
        let mut seen = 0_u64;
        for (sequence, input, outcome) in records {
            if seen.checked_add(1) != Some(sequence) || sequence > self.resolved_requests() {
                return false;
            }
            digest.update(sequence.to_be_bytes());
            digest.update(input);
            digest.update(outcome);
            seen = sequence;
        }
        digest.update(self.source_through.to_be_bytes());
        seen == self.resolved_requests() && <[u8; 32]>::from(digest.finalize()) == self.digest
    }
}

fn initial_digest(closing: &ProducerClosing) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(b"bigbrotr/initial-source-closure/1\0");
    digest.update(Sha256::digest(closing.manifest().encode().as_bytes()));
    digest.update(closing.producer_slot_id().as_bytes());
    digest.update(closing.producer_epoch().get().to_be_bytes());
    digest.update(closing.issued_through().to_be_bytes());
    digest
}

impl ProducerClosing {
    pub(in crate::storage) fn source_resolution(
        self,
        recovery: &IngestRecovery,
        reserved_through: u64,
    ) -> Result<SourceResolution<'_>, BootstrapError> {
        self.source_resolution_in(recovery, reserved_through, None)
    }

    pub(super) fn source_resolution_in<'a>(
        self,
        recovery: &'a IngestRecovery,
        reserved_through: u64,
        historical: Option<HistoricalInitial<'a>>,
    ) -> Result<SourceResolution<'a>, BootstrapError> {
        let manifest = historical
            .as_ref()
            .map_or(recovery.manifest(), |view| view.manifest());
        if self.manifest() != manifest || self.issued_through() != reserved_through {
            return Err(BootstrapError::StateMismatch);
        }
        let manifest_digest: [u8; 32] = Sha256::digest(self.manifest().encode().as_bytes()).into();
        let digest = initial_digest(&self);
        Ok(SourceResolution {
            recovery,
            closing: self,
            manifest_digest,
            requests: 0,
            source_through: 0,
            digest,
            event_requests: 0,
            historical,
        })
    }
}

impl<'a> SourceResolution<'a> {
    pub fn record(&mut self, resolved: &ResolvedProducerRequest) -> Result<(), BootstrapError> {
        resolved.require_native_context(self.recovery)?;
        let copy = resolved.native_outcome();
        let request = copy.request();
        if copy.manifest_sha256() != self.manifest_digest
            || request.producer_slot_id != self.closing.producer_slot_id()
            || request.producer_epoch != self.closing.producer_epoch()
            || self.requests.checked_add(1) != Some(request.request_sequence.get())
            || request.request_sequence.get() > self.closing.issued_through()
        {
            return Err(BootstrapError::StateMismatch);
        }
        let mut next_source = self.source_through;
        if let AdmissionOutcome::Durable(receipt) = resolved.outcome() {
            let identity = self.closing.manifest().identity();
            if let Some(batch) = receipt.batch() {
                let first = batch.first();
                if first.dataset_id != identity.dataset_id
                    || first.shard_id != identity.shard_id
                    || first.owner_epoch != identity.owner_epoch
                    || next_source.checked_add(1) != Some(first.sequence.get())
                {
                    return Err(BootstrapError::StateMismatch);
                }
                next_source = batch.last().sequence.get();
            }
            if receipt.dependencies().records().iter().any(|r| {
                let source = r.source_record_id;
                source.dataset_id != identity.dataset_id
                    || source.shard_id != identity.shard_id
                    || source.owner_epoch != identity.owner_epoch
                    || source.sequence.get() > next_source
            }) {
                return Err(BootstrapError::StateMismatch);
            }
        }
        // Advance only after the entire observation passes; a refused out-of-order
        // result must not mutate the accumulator or silently fill its gap.
        let event_bit = if resolved.is_event() {
            u32::try_from(request.request_sequence.get() - 1)
                .ok()
                .and_then(|shift| 1_u64.checked_shl(shift))
                .ok_or(BootstrapError::InsufficientReservation)?
        } else {
            0
        };
        self.digest
            .update(request.request_sequence.get().to_be_bytes());
        self.digest.update(copy.digest().as_bytes());
        self.digest.update(Sha256::digest(copy.bytes()));
        self.requests = request.request_sequence.get();
        self.source_through = next_source;
        self.event_requests |= event_bit;
        Ok(())
    }

    pub fn finish(mut self) -> Result<ResolvedSource<'a>, BootstrapError> {
        if self.requests != self.closing.issued_through() {
            return Err(BootstrapError::StateMismatch);
        }
        let store = &self.recovery.store;
        if store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if let Some(view) = &self.historical {
            view.validate()?;
        }
        // Initial closure supports exactly one producer, with only the checked
        // prepared successor allowed through a historical read view. Refuse any
        // other marker even if it has never emitted a fact.
        let producer = IngestKey::ProducerState {
            producer_slot_id: self.closing.producer_slot_id(),
            producer_epoch: self.closing.producer_epoch(),
        }
        .encode()
        .map_err(|_| BootstrapError::InvalidIntent)?;
        let cf = store
            .db
            .cf_handle("receipts")
            .ok_or(BootstrapError::EngineRead)?;
        let mut iterator = store.db.raw_iterator_cf_opt(cf, read_options());
        iterator.seek([1, 0x31]);
        iterator.status().map_err(|_| BootstrapError::EngineRead)?;
        if iterator.key() != Some(producer.as_bytes()) {
            return Err(BootstrapError::StateMismatch);
        }
        iterator.next();
        iterator.status().map_err(|_| BootstrapError::EngineRead)?;
        if let Some(view) = &self.historical {
            let target = IngestKey::ProducerState {
                producer_slot_id: view.producer_slot_id(),
                producer_epoch: bigbrotr_model::ProducerEpoch::new(2)
                    .expect("checked initial successor"),
            }
            .encode()
            .map_err(|_| BootstrapError::InvalidIntent)?;
            if iterator.key() != Some(target.as_bytes()) {
                return Err(BootstrapError::StateMismatch);
            }
            iterator.next();
            iterator.status().map_err(|_| BootstrapError::EngineRead)?;
        }
        if iterator.key().is_some_and(|k| k.starts_with(&[1, 0x31])) {
            return Err(BootstrapError::StateMismatch);
        }
        drop(iterator);
        let state = store
            .producer_state(
                self.closing.producer_slot_id(),
                self.closing.producer_epoch(),
            )?
            .ok_or(BootstrapError::StateMismatch)?;
        if !state.closing
            || state.issued != self.requests
            || state.acknowledged != 0
            || state.retired != 0
        {
            return Err(BootstrapError::StateMismatch);
        }
        let identity = self.closing.manifest().identity();
        let key = IngestKey::SourceStream {
            dataset_id: identity.dataset_id,
            shard_id: identity.shard_id,
            owner_epoch: identity.owner_epoch,
        }
        .encode()
        .map_err(|_| BootstrapError::InvalidIntent)?;
        let value = store
            .db
            .get_pinned_opt(key.as_bytes(), &read_options())
            .map_err(|_| BootstrapError::EngineRead)?;
        let native_through = match value.as_deref() {
            None => 0,
            Some(bytes) if bytes.len() == 10 && bytes[..2] == [1, 2] => {
                u64::from_be_bytes(bytes[2..].try_into().expect("fixed validated length"))
            }
            Some(_) => return Err(BootstrapError::StateMismatch),
        };
        if native_through != self.source_through {
            return Err(BootstrapError::StateMismatch);
        }
        // A missing/zero counter cannot conceal retained source facts. Check the
        // actual last key without decoding or allocating an unbounded tail.
        let source_key = |sequence| {
            IngestKey::Fact {
                source_record_id: SourceRecordId {
                    dataset_id: identity.dataset_id,
                    shard_id: identity.shard_id,
                    owner_epoch: identity.owner_epoch,
                    sequence: Sequence::new(sequence).expect("nonzero source boundary"),
                },
            }
            .encode()
            .map_err(|_| BootstrapError::InvalidIntent)
        };
        let maximum = source_key(u64::MAX)?;
        let prefix = &maximum.as_bytes()[..maximum.as_bytes().len() - 8];
        let cf = store
            .db
            .cf_handle("facts")
            .ok_or(BootstrapError::EngineRead)?;
        let mut iterator = store.db.raw_iterator_cf_opt(cf, read_options());
        if self.source_through == 0 {
            iterator.seek(prefix);
        } else {
            iterator.seek_for_prev(maximum.as_bytes());
        }
        iterator.status().map_err(|_| BootstrapError::EngineRead)?;
        let last = iterator.key().filter(|key| key.starts_with(prefix));
        if (self.source_through == 0 && last.is_some())
            || (self.source_through > 0
                && last != Some(source_key(self.source_through)?.as_bytes()))
        {
            return Err(BootstrapError::StateMismatch);
        }
        self.digest.update(self.source_through.to_be_bytes());
        Ok(ResolvedSource {
            recovery: self.recovery,
            closing: self.closing,
            source_through: self.source_through,
            digest: self.digest.finalize().into(),
            event_requests: self.event_requests,
            historical: self.historical,
        })
    }
}
