//! Checked two-stream recovery interpretation, never an activation capability.
use super::super::{
    PendingBatchHeader, producer_closing::ProducerState, producer_recovery::RecoveryContext,
};
use super::*;
use crate::storage::rocks::keys::{ColumnFamily, KeyDomain, decode_key};
use bigbrotr_model::Receipt;

/// Fixed-size checked metadata. The connected owner retains its metadata grant
/// and validates the immutable SQL activation separately. Decoding a handoff
/// alone cannot construct this actual-root/sequence observation.
pub(in crate::storage) struct ActivatedInitial {
    record: InitialHandoffRecord,
    root: (u64, u64),
    sequence: u64,
    producer: ProducerState,
    source_through: u64,
}

/// A synchronized barrier plus bounded replay context. Deliberately exposes no
/// SourceResolution, handoff reconstruction, mutable store or writer conversion.
pub(in crate::storage) struct ActivatedProducerClosing {
    context: ActivatedInitial,
    _permit: OwnedPermit,
}

impl ActivatedInitial {
    pub(in crate::storage) fn inspect(
        recovery: &IngestRecovery,
        record: InitialHandoffRecord,
    ) -> Result<Self, BootstrapError> {
        let store = &recovery.store;
        if store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if recovery.manifest() != record.next_manifest() {
            return Err(BootstrapError::StateMismatch);
        }
        let root = store.handoff_root_identity()?;
        let sequence = store.db.latest_sequence_number();
        store.handoff_value(IngestKey::Manifest, Some(record.next.encode().as_bytes()))?;
        store.handoff_value(record.key(), Some(record.encode().as_bytes()))?;
        let epoch2 = ProducerEpoch::new(2).expect("selected target");
        let previous = store
            .producer_state(record.slot, record.epoch)?
            .ok_or(BootstrapError::StateMismatch)?;
        let producer = store
            .producer_state(record.slot, epoch2)?
            .ok_or(BootstrapError::StateMismatch)?;
        if !previous.closing || previous.issued != record.issued {
            return Err(BootstrapError::StateMismatch);
        }
        store.require_producer_boundary(record.slot, record.epoch, previous)?;
        store.require_producer_boundary(record.slot, epoch2, producer)?;
        store.require_recovery_census(
            KeyDomain::ProducerState,
            &[
                IngestKey::ProducerState {
                    producer_slot_id: record.slot,
                    producer_epoch: record.epoch,
                },
                record.producer_key(),
            ],
        )?;
        store.require_recovery_census(
            KeyDomain::SourceStream,
            &[source_key(&record.previous), record.source_key()],
        )?;
        store.require_recovery_census(KeyDomain::OwnerHandoff, &[record.key()])?;
        let previous_through = store.recovery_source_bound(&record.previous)?;
        let source_through = store.recovery_source_bound(&record.next)?;
        if previous_through != record.source_through
            || (producer.issued == 0 && source_through != 0)
        {
            return Err(BootstrapError::StateMismatch);
        }
        for domain in [KeyDomain::Fact, KeyDomain::PendingBatch] {
            store.require_recovery_record_streams(domain, &record)?;
        }
        store.require_recovery_allocations(&record, source_through)?;
        let context = Self {
            record,
            root,
            sequence,
            producer,
            source_through,
        };
        context.validate(recovery)?;
        Ok(context)
    }

    pub(in crate::storage) fn previous_manifest(&self) -> &IngestManifest {
        &self.record.previous
    }
    pub(in crate::storage) fn manifest(&self) -> &IngestManifest {
        &self.record.next
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.record.slot
    }
    pub(in crate::storage) fn issued_through(&self) -> u64 {
        self.producer.issued
    }
    pub(in crate::storage) fn is_closing(&self) -> bool {
        self.producer.closing
    }

    pub(in crate::storage) fn validate(
        &self,
        recovery: &IngestRecovery,
    ) -> Result<(), BootstrapError> {
        let store = &recovery.store;
        if store.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        if recovery.manifest() != self.manifest()
            || store.handoff_root_identity()? != self.root
            || store.db.latest_sequence_number() != self.sequence
        {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }

    pub(in crate::storage) fn close_producer(
        &self,
        recovery: &mut IngestRecovery,
        permit: OwnedPermit,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<ActivatedProducerClosing, BootstrapError> {
        if permit.request().units().data_bytes() < 4096
            || permit.request().units().native_bytes() < 4096
            || permit.request().units().items() == 0
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        authority()?;
        self.validate(recovery)?;
        recovery.store.close_producer_marker(
            self.record.slot,
            ProducerEpoch::new(2).expect("selected target"),
            self.producer,
            &mut authority,
        )?;
        let context = Self::inspect(recovery, self.record)?;
        if !context.producer.closing
            || context.root != self.root
            || context.sequence
                != self
                    .sequence
                    .checked_add(u64::from(!self.producer.closing))
                    .ok_or(BootstrapError::StateMismatch)?
        {
            return Err(BootstrapError::StateMismatch);
        }
        authority()?;
        Ok(ActivatedProducerClosing {
            context,
            _permit: permit,
        })
    }

    pub(in crate::storage::rocks::store) fn require_receipt_bounds(
        &self,
        receipt: &Receipt,
    ) -> Result<(), AdmissionError> {
        let current = self.manifest().identity();
        let in_stream = |source: SourceRecordId, epoch: u64, through: u64| {
            source.dataset_id == current.dataset_id
                && source.shard_id == current.shard_id
                && source.owner_epoch.get() == epoch
                && source.sequence.get() <= through
        };
        if receipt.dependencies().records().iter().any(|reference| {
            let source = reference.source_record_id;
            !in_stream(source, 1, self.record.source_through)
                && !in_stream(source, 2, self.source_through)
        }) || receipt.batch().is_some_and(|batch| {
            !in_stream(batch.first(), 2, self.source_through)
                || !in_stream(batch.last(), 2, self.source_through)
        }) {
            return Err(AdmissionError::StateMismatch);
        }
        Ok(())
    }
}

impl ActivatedProducerClosing {
    pub(in crate::storage) fn manifest(&self) -> &IngestManifest {
        self.context.manifest()
    }
    pub(in crate::storage) fn producer_slot_id(&self) -> ProducerSlotId {
        self.context.producer_slot_id()
    }
    pub(in crate::storage) fn issued_through(&self) -> u64 {
        self.context.issued_through()
    }
    pub(in crate::storage) fn validate(
        &self,
        recovery: &IngestRecovery,
    ) -> Result<(), BootstrapError> {
        self.context.validate(recovery)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "separate original input and outcome grants"
    )]
    pub(in crate::storage) fn resolve_original_request(
        &self,
        recovery: &IngestRecovery,
        request: ProducerRequestId,
        digest: Digest256,
        limits: ProducerRecoveryLimits,
        input_permit: OwnedPermit,
        outcome_permit: OwnedPermit,
        authority: impl FnMut() -> Result<(), AdmissionError>,
    ) -> Result<ResolvedProducerRequest, ProducerRecoveryError> {
        recovery.resolve_original_request_in(
            request,
            digest,
            limits,
            input_permit,
            outcome_permit,
            authority,
            RecoveryContext::Activated(&self.context),
        )
    }
}

fn source_key(manifest: &IngestManifest) -> IngestKey {
    let id = manifest.identity();
    IngestKey::SourceStream {
        dataset_id: id.dataset_id,
        shard_id: id.shard_id,
        owner_epoch: id.owner_epoch,
    }
}

impl IngestStore {
    /// Skip each known stream by its maximum key, rather than scanning its
    /// retained facts. Orphan third/foreign streams are still refused.
    fn require_recovery_record_streams(
        &self,
        domain: KeyDomain,
        record: &InitialHandoffRecord,
    ) -> Result<(), BootstrapError> {
        let cf = self
            .db
            .cf_handle("facts")
            .ok_or(BootstrapError::EngineRead)?;
        let mut cursor = self.db.raw_iterator_cf_opt(cf, read_options());
        cursor.seek(domain.prefix());
        for manifest in [&record.previous, &record.next] {
            cursor.status().map_err(|_| BootstrapError::EngineRead)?;
            let id = manifest.identity();
            let source = SourceRecordId {
                dataset_id: id.dataset_id,
                shard_id: id.shard_id,
                owner_epoch: id.owner_epoch,
                sequence: Sequence::new(u64::MAX).expect("nonzero maximum"),
            };
            let maximum = if domain == KeyDomain::Fact {
                IngestKey::Fact {
                    source_record_id: source,
                }
            } else {
                IngestKey::PendingBatch { first: source }
            }
            .encode()
            .map_err(|_| BootstrapError::InvalidIntent)?;
            let prefix = &maximum.as_bytes()[..maximum.as_bytes().len() - 8];
            if cursor.key().is_some_and(|key| key.starts_with(prefix)) {
                cursor.seek(maximum.as_bytes());
                cursor.status().map_err(|_| BootstrapError::EngineRead)?;
                if cursor.key() == Some(maximum.as_bytes()) {
                    cursor.next();
                }
            }
        }
        cursor.status().map_err(|_| BootstrapError::EngineRead)?;
        if cursor
            .key()
            .is_some_and(|key| key.starts_with(&domain.prefix()))
        {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }

    /// Exact fixed-size key census, with one additional cursor observation to
    /// refuse an unsupported stream/producer/lifetime even if it has zero work.
    fn require_recovery_census(
        &self,
        domain: KeyDomain,
        expected: &[IngestKey],
    ) -> Result<(), BootstrapError> {
        let cf = self
            .db
            .cf_handle(domain.column_family().name())
            .ok_or(BootstrapError::EngineRead)?;
        let mut cursor = self.db.raw_iterator_cf_opt(cf, read_options());
        cursor.seek(domain.prefix());
        for key in expected {
            cursor.status().map_err(|_| BootstrapError::EngineRead)?;
            if cursor.key()
                != Some(
                    key.encode()
                        .map_err(|_| BootstrapError::InvalidIntent)?
                        .as_bytes(),
                )
            {
                return Err(BootstrapError::StateMismatch);
            }
            cursor.next();
        }
        cursor.status().map_err(|_| BootstrapError::EngineRead)?;
        if cursor
            .key()
            .is_some_and(|key| key.starts_with(&domain.prefix()))
        {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }

    fn recovery_counter(&self, key: IngestKey) -> Result<Option<u64>, BootstrapError> {
        let encoded = key.encode().map_err(|_| BootstrapError::InvalidIntent)?;
        let bytes = self
            .db
            .get_pinned_opt(encoded.as_bytes(), &read_options())
            .map_err(|_| BootstrapError::EngineRead)?;
        bytes
            .map(|bytes| {
                if bytes.len() != 10 || bytes[..2] != key.domain().prefix() {
                    return Err(BootstrapError::StateMismatch);
                }
                Ok(u64::from_be_bytes(
                    bytes[2..].try_into().expect("checked fixed length"),
                ))
            })
            .transpose()
    }

    fn recovery_source_bound(&self, manifest: &IngestManifest) -> Result<u64, BootstrapError> {
        let through = self
            .recovery_counter(source_key(manifest))?
            .ok_or(BootstrapError::StateMismatch)?;
        let id = manifest.identity();
        let source = |sequence| SourceRecordId {
            dataset_id: id.dataset_id,
            shard_id: id.shard_id,
            owner_epoch: id.owner_epoch,
            sequence: Sequence::new(sequence).expect("nonzero checked bound"),
        };
        let cf = self
            .db
            .cf_handle("facts")
            .ok_or(BootstrapError::EngineRead)?;
        for pending in [false, true] {
            let key = |n| {
                if pending {
                    IngestKey::PendingBatch { first: source(n) }
                } else {
                    IngestKey::Fact {
                        source_record_id: source(n),
                    }
                }
            };
            let maximum = key(u64::MAX)
                .encode()
                .map_err(|_| BootstrapError::InvalidIntent)?;
            let prefix = &maximum.as_bytes()[..maximum.as_bytes().len() - 8];
            let mut cursor = self.db.raw_iterator_cf_opt(cf, read_options());
            if through == 0 {
                cursor.seek(prefix);
            } else {
                cursor.seek_for_prev(maximum.as_bytes());
            }
            cursor.status().map_err(|_| BootstrapError::EngineRead)?;
            let last = cursor.key().filter(|key| key.starts_with(prefix));
            if through == 0 {
                if last.is_some() {
                    return Err(BootstrapError::StateMismatch);
                }
            } else if pending {
                let Some(IngestKey::PendingBatch { first }) = last
                    .map(|key| decode_key(ColumnFamily::Facts, key))
                    .transpose()
                    .map_err(|_| BootstrapError::StateMismatch)?
                else {
                    return Err(BootstrapError::StateMismatch);
                };
                let header = PendingBatchHeader::decode(
                    first,
                    cursor.value().ok_or(BootstrapError::StateMismatch)?,
                )
                .map_err(|_| BootstrapError::StateMismatch)?;
                if header.last().sequence.get() != through {
                    return Err(BootstrapError::StateMismatch);
                }
            } else if last
                != Some(
                    key(through)
                        .encode()
                        .map_err(|_| BootstrapError::InvalidIntent)?
                        .as_bytes(),
                )
            {
                return Err(BootstrapError::StateMismatch);
            }
        }
        Ok(through)
    }

    fn require_recovery_allocations(
        &self,
        record: &InitialHandoffRecord,
        target_through: u64,
    ) -> Result<(), BootstrapError> {
        // Allocation sequence zero remains invalid even when source sequence zero
        // is valid. A zero target source cannot conceal dictionary/index state.
        for (manifest, through) in [
            (&record.previous, record.source_through),
            (&record.next, target_through),
        ] {
            for number in 1..=9 {
                let kind = DictionaryKind::try_from(number).expect("assigned kind");
                let Some(domain) = manifest.allocation_namespaces().domain(kind) else {
                    continue;
                };
                let dataset_id = manifest.identity().dataset_id;
                let counter = self.recovery_counter(IngestKey::AllocationStream {
                    dataset_id,
                    dictionary_kind: kind,
                    allocation_domain: domain,
                })?;
                if counter.is_some_and(|value| value == 0 || value > through) {
                    return Err(BootstrapError::StateMismatch);
                }
                if through == 0 {
                    self.handoff_empty_prefix(
                        IngestKey::Dictionary {
                            reference: QualifiedRef {
                                dataset_id,
                                dictionary_kind: kind,
                                allocation_domain: domain,
                                ordinal: Ordinal::new(1).expect("nonzero"),
                            },
                        },
                        8,
                    )?;
                }
            }
        }
        let mut cursor = self.db.raw_iterator_opt(read_options());
        cursor.seek(KeyDomain::AllocationStream.prefix());
        for _ in 0..=18 {
            cursor.status().map_err(|_| BootstrapError::EngineRead)?;
            let Some(key) = cursor
                .key()
                .filter(|key| key.starts_with(&KeyDomain::AllocationStream.prefix()))
            else {
                return Ok(());
            };
            let IngestKey::AllocationStream {
                dataset_id,
                dictionary_kind,
                allocation_domain,
            } = decode_key(ColumnFamily::Default, key)
                .map_err(|_| BootstrapError::StateMismatch)?
            else {
                return Err(BootstrapError::StateMismatch);
            };
            if dataset_id != record.next.identity().dataset_id
                || ![&record.previous, &record.next].iter().any(|manifest| {
                    manifest.allocation_namespaces().domain(dictionary_kind)
                        == Some(allocation_domain)
                })
            {
                return Err(BootstrapError::StateMismatch);
            }
            cursor.next();
        }
        Err(BootstrapError::StateMismatch)
    }
}
