//! Bounded positive prefix proof under continuous exclusive native custody.
use super::bootstrap::read_options;
use super::{
    ActivatedProducerClosing, AdmissionOutcome, BootstrapError, HistoricalInitial, IngestRecovery,
    ProducerClosing, ResolvedProducerRequest,
};
use crate::storage::rocks::{keys::IngestKey, manifest::IngestManifest};
use bigbrotr_model::{ProducerEpoch, ProducerSlotId, Sequence, SourceRecordId};
use sha2::{Digest, Sha256};

/// This accumulator owns the existing closing grant and borrows native custody.
/// It retains no receipt vector and cannot authorize admission, ACK or retirement.
pub struct SourceResolution<'a> {
    recovery: &'a IngestRecovery,
    closing: ProducerClosing,
    prefix: Prefix,
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
        matches_recorded(
            initial_digest(&self.closing),
            self.resolved_requests(),
            self.source_through,
            self.digest,
            records,
        )
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
        let prefix = Prefix::new(
            PrefixIdentity {
                manifest: *self.manifest(),
                slot: self.producer_slot_id(),
                epoch: self.producer_epoch(),
                issued: self.issued_through(),
                predecessor_cut: None,
            },
            initial_digest(&self),
        );
        Ok(SourceResolution {
            recovery,
            closing: self,
            prefix,
            historical,
        })
    }
}

impl<'a> SourceResolution<'a> {
    pub fn record(&mut self, resolved: &ResolvedProducerRequest) -> Result<(), BootstrapError> {
        resolved.require_native_context(self.recovery)?;
        self.prefix.record(resolved)
    }

    pub fn finish(mut self) -> Result<ResolvedSource<'a>, BootstrapError> {
        if self.prefix.requests != self.closing.issued_through() {
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
            || state.issued != self.prefix.requests
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
        if native_through != self.prefix.source_through {
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
        if self.prefix.source_through == 0 {
            iterator.seek(prefix);
        } else {
            iterator.seek_for_prev(maximum.as_bytes());
        }
        iterator.status().map_err(|_| BootstrapError::EngineRead)?;
        let last = iterator.key().filter(|key| key.starts_with(prefix));
        if (self.prefix.source_through == 0 && last.is_some())
            || (self.prefix.source_through > 0
                && last != Some(source_key(self.prefix.source_through)?.as_bytes()))
        {
            return Err(BootstrapError::StateMismatch);
        }
        self.prefix
            .digest
            .update(self.prefix.source_through.to_be_bytes());
        Ok(ResolvedSource {
            recovery: self.recovery,
            closing: self.closing,
            source_through: self.prefix.source_through,
            digest: self.prefix.digest.finalize().into(),
            event_requests: self.prefix.event_requests,
            historical: self.historical,
        })
    }
}

/// Shared fixed-size ordered-prefix reducer. The enclosing initial/activated
/// owners provide different native authority and final-census checks.
#[derive(Clone)]
struct PrefixIdentity {
    manifest: IngestManifest,
    slot: ProducerSlotId,
    epoch: ProducerEpoch,
    issued: u64,
    predecessor_cut: Option<u64>,
}
#[derive(Clone)]
pub(super) struct Prefix {
    identity: PrefixIdentity,
    manifest_digest: [u8; 32],
    requests: u64,
    source_through: u64,
    digest: Sha256,
    event_requests: u64,
}
impl Prefix {
    pub(super) fn lineage(
        manifest: IngestManifest,
        slot: ProducerSlotId,
        issued: u64,
        digest: Sha256,
    ) -> Self {
        Self::new(
            PrefixIdentity {
                epoch: ProducerEpoch::new(manifest.identity().owner_epoch.get())
                    .expect("manifest epoch"),
                manifest,
                slot,
                issued,
                predecessor_cut: None,
            },
            digest,
        )
    }

    pub(super) fn requests(&self) -> u64 {
        self.requests
    }
    pub(super) fn source_through(&self) -> u64 {
        self.source_through
    }
    pub(super) fn event_requests(&self) -> u64 {
        self.event_requests
    }
    pub(super) fn completed_digest(&self) -> [u8; 32] {
        let mut digest = self.digest.clone();
        digest.update(self.source_through.to_be_bytes());
        digest.finalize().into()
    }

    fn new(identity: PrefixIdentity, digest: Sha256) -> Self {
        let manifest_digest = Sha256::digest(identity.manifest.encode().as_bytes()).into();
        Self {
            identity,
            manifest_digest,
            requests: 0,
            source_through: 0,
            digest,
            event_requests: 0,
        }
    }
    fn record(&mut self, resolved: &ResolvedProducerRequest) -> Result<(), BootstrapError> {
        let predecessor_cut = self.identity.predecessor_cut;
        self.record_with_frontier(resolved, |source| {
            predecessor_cut
                .is_some_and(|cut| source.owner_epoch.get() == 1 && source.sequence.get() <= cut)
        })
    }

    pub(super) fn record_with_frontier(
        &mut self,
        resolved: &ResolvedProducerRequest,
        earlier: impl Fn(SourceRecordId) -> bool,
    ) -> Result<(), BootstrapError> {
        let copy = resolved.native_outcome();
        let request = copy.request();
        if copy.manifest_sha256() != self.manifest_digest
            || request.producer_slot_id != self.identity.slot
            || request.producer_epoch != self.identity.epoch
            || self.requests.checked_add(1) != Some(request.request_sequence.get())
            || request.request_sequence.get() > self.identity.issued
        {
            return Err(BootstrapError::StateMismatch);
        }
        let mut next_source = self.source_through;
        if let AdmissionOutcome::Durable(receipt) = resolved.outcome() {
            let identity = self.identity.manifest.identity();
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
                    || if source.owner_epoch == identity.owner_epoch {
                        source.sequence.get() > next_source
                    } else {
                        !earlier(source)
                    }
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
}

fn matches_recorded(
    mut digest: Sha256,
    requests: u64,
    source_through: u64,
    expected: [u8; 32],
    records: impl IntoIterator<Item = (u64, [u8; 32], [u8; 32])>,
) -> bool {
    let mut seen = 0_u64;
    for (sequence, input, outcome) in records {
        if seen.checked_add(1) != Some(sequence) || sequence > requests {
            return false;
        }
        digest.update(sequence.to_be_bytes());
        digest.update(input);
        digest.update(outcome);
        seen = sequence;
    }
    digest.update(source_through.to_be_bytes());
    seen == requests && <[u8; 32]>::from(digest.finalize()) == expected
}

/// Ordered epoch2 resolution borrowing the exact closing proof and native owner.
/// Neither this accumulator nor its completed witness can become an initial
/// source, a handoff, a retirement permit or a writer.
pub struct ActivatedSourceResolution<'a> {
    recovery: &'a IngestRecovery,
    closing: &'a ActivatedProducerClosing,
    prefix: Prefix,
}

/// Complete positive target prefix, tied to the same two-stream native custody.
pub struct ResolvedActivatedSource<'a> {
    recovery: &'a IngestRecovery,
    closing: &'a ActivatedProducerClosing,
    source_through: u64,
    digest: [u8; 32],
    event_requests: u64,
}

impl ActivatedProducerClosing {
    pub(in crate::storage) fn source_resolution<'a>(
        &'a self,
        recovery: &'a IngestRecovery,
        reserved_through: u64,
    ) -> Result<ActivatedSourceResolution<'a>, BootstrapError> {
        self.validate(recovery)?;
        if reserved_through != self.issued_through() {
            return Err(BootstrapError::StateMismatch);
        }
        // The selected profile retains at most64 inputs, without ACK/GC. The
        // connected wrapper additionally checks the immutable activation limit.
        if reserved_through > 64 {
            return Err(BootstrapError::InsufficientReservation);
        }
        Ok(ActivatedSourceResolution {
            recovery,
            closing: self,
            prefix: Prefix::new(
                PrefixIdentity {
                    manifest: *self.manifest(),
                    slot: self.producer_slot_id(),
                    epoch: ProducerEpoch::new(2).expect("checked activated producer"),
                    issued: reserved_through,
                    predecessor_cut: Some(self.predecessor_source_through()),
                },
                activated_digest(self),
            ),
        })
    }
}
impl<'a> ActivatedSourceResolution<'a> {
    pub fn record(&mut self, resolved: &ResolvedProducerRequest) -> Result<(), BootstrapError> {
        self.closing.validate(self.recovery)?;
        resolved.require_native_context(self.recovery)?;
        self.prefix.record(resolved)
    }

    pub fn finish(mut self) -> Result<ResolvedActivatedSource<'a>, BootstrapError> {
        if self.prefix.requests != self.closing.issued_through() {
            return Err(BootstrapError::StateMismatch);
        }
        self.closing
            .require_source_cut(self.recovery, self.prefix.source_through)?;
        self.prefix
            .digest
            .update(self.prefix.source_through.to_be_bytes());
        Ok(ResolvedActivatedSource {
            recovery: self.recovery,
            closing: self.closing,
            source_through: self.prefix.source_through,
            digest: self.prefix.digest.finalize().into(),
            event_requests: self.prefix.event_requests,
        })
    }
}
impl ResolvedActivatedSource<'_> {
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
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.closing.producer_slot_id()
    }
    pub(in crate::storage) fn manifest(&self) -> &IngestManifest {
        self.closing.manifest()
    }
    pub(in crate::storage) fn validate(&self) -> Result<(), BootstrapError> {
        self.closing
            .require_source_cut(self.recovery, self.source_through)
    }
    pub(in crate::storage) fn matches_closed(&self, closing: &ActivatedProducerClosing) -> bool {
        std::ptr::eq(self.closing, closing)
    }
    pub(in crate::storage) fn matches_recorded(
        &self,
        records: impl IntoIterator<Item = (u64, [u8; 32], [u8; 32])>,
    ) -> bool {
        matches_recorded(
            activated_digest(self.closing),
            self.resolved_requests(),
            self.source_through,
            self.digest,
            records,
        )
    }
}
fn activated_digest(closing: &ActivatedProducerClosing) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(b"bigbrotr/activated-initial-source-closure/1\0");
    digest.update(closing.handoff_sha256());
    digest.update(Sha256::digest(closing.manifest().encode().as_bytes()));
    digest.update(closing.producer_slot_id().as_bytes());
    digest.update(2_u64.to_be_bytes());
    digest.update(closing.issued_through().to_be_bytes());
    digest
}
