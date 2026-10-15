//! Complete key classification and bounded structural census, not effect replay.
use super::*;
use crate::storage::rocks::store::PendingBatchHeader;
use crate::storage::rocks::succession::MAX_LINEAGE_EPOCHS;
use bigbrotr_model::{QualifiedRef, Sequence, SourceRecordId};

const EPOCHS: usize = MAX_LINEAGE_EPOCHS as usize;
// Counters cover every key without retaining keys, facts or outcome vectors.
// Two namespace counters plus facts/pending/requests fit the scratch reservation.
const _: () = assert!((2 * 9 + 3) * EPOCHS * size_of::<u64>() < 64 * 1024);

impl RetainedLineage {
    pub(super) fn inspect_census(
        &self,
        recovery: &IngestRecovery,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        let mut dictionaries = [[0_u64; 9]; EPOCHS];
        let mut reverse = [[0_u64; 9]; EPOCHS];
        let mut facts = [0_u64; EPOCHS];
        let mut pending = [0_u64; EPOCHS];
        let mut requests = [0_u64; EPOCHS];
        let (mut manifests, mut sources, mut producers, mut handoffs, mut allocations) =
            (0, 0, 0, 0, 0);
        let head = recovery.manifest().identity();
        for cf in [
            ColumnFamily::Default,
            ColumnFamily::Facts,
            ColumnFamily::Receipts,
        ] {
            let handle = recovery
                .store
                .db
                .cf_handle(cf.name())
                .ok_or(BootstrapError::EngineRead)?;
            let mut cursor = recovery
                .store
                .db
                .raw_iterator_cf_opt(handle, read_options());
            cursor.seek_to_first();
            while cursor.valid() {
                check_cancelled(cancelled)?;
                cursor.status().map_err(|_| BootstrapError::EngineRead)?;
                let key = decode_key(cf, cursor.key().ok_or(BootstrapError::StateMismatch)?)
                    .map_err(|_| BootstrapError::StateMismatch)?;
                let bytes = cursor.value().ok_or(BootstrapError::StateMismatch)?;
                match key {
                    IngestKey::Manifest => {
                        manifests += 1;
                        if bytes != recovery.manifest().encode().as_bytes() {
                            return Err(BootstrapError::StateMismatch);
                        }
                    }
                    IngestKey::OwnerHandoff {
                        predecessor_epoch, ..
                    } => {
                        if key != handoff_key(recovery.manifest(), predecessor_epoch.get())?
                            || predecessor_epoch.get() >= self.entries.len() as u64
                        {
                            return Err(BootstrapError::StateMismatch);
                        }
                        handoffs += 1;
                    }
                    IngestKey::SourceStream {
                        dataset_id,
                        shard_id,
                        owner_epoch,
                    } => {
                        let entry = self
                            .at(owner_epoch.get())
                            .ok_or(BootstrapError::StateMismatch)?;
                        if dataset_id != head.dataset_id
                            || shard_id != head.shard_id
                            || !entry.source_present
                        {
                            return Err(BootstrapError::StateMismatch);
                        }
                        sources += 1;
                    }
                    IngestKey::ProducerState {
                        producer_slot_id,
                        producer_epoch,
                    } => {
                        if producer_slot_id != self.slot || self.at(producer_epoch.get()).is_none()
                        {
                            return Err(BootstrapError::StateMismatch);
                        }
                        // The exact marker, count and ACK/GC veto were checked
                        // while constructing this entry under the same sequence.
                        ProducerState::decode(bytes)?;
                        producers += 1;
                    }
                    IngestKey::AllocationStream {
                        dataset_id,
                        dictionary_kind,
                        allocation_domain,
                    } => {
                        let index =
                            self.domain_index(dataset_id, dictionary_kind, allocation_domain)?;
                        if self.entries[index].allocations[dictionary_kind as usize - 1] == 0 {
                            return Err(BootstrapError::StateMismatch);
                        }
                        allocations += 1;
                    }
                    IngestKey::Dictionary { reference } => {
                        let index = self.reference_index(reference)?;
                        let count =
                            &mut dictionaries[index][reference.dictionary_kind as usize - 1];
                        if count.checked_add(1) != Some(reference.ordinal.get()) {
                            return Err(BootstrapError::StateMismatch);
                        }
                        *count = reference.ordinal.get();
                        let source = self.index_pointer(recovery, key, bytes, cancelled)?;
                        if source.owner_epoch != self.entries[index].manifest.identity().owner_epoch
                        {
                            return Err(BootstrapError::StateMismatch);
                        }
                    }
                    IngestKey::DictionaryReverseCandidate {
                        reference,
                        descriptor_digest,
                        ..
                    } => {
                        let index = self.reference_index(reference)?;
                        if bytes.len() < 3
                            || bytes[..2] != key.domain().prefix()
                            || digest_bytes(&bytes[2..], cancelled)?
                                != *descriptor_digest.as_bytes()
                        {
                            return Err(BootstrapError::StateMismatch);
                        }
                        let count = &mut reverse[index][reference.dictionary_kind as usize - 1];
                        *count = count.checked_add(1).ok_or(BootstrapError::Limit)?;
                        if *count
                            > self.entries[index].allocations
                                [reference.dictionary_kind as usize - 1]
                        {
                            return Err(BootstrapError::StateMismatch);
                        }
                    }
                    IngestKey::EventPayload { dataset_id, .. } => {
                        if dataset_id != head.dataset_id {
                            return Err(BootstrapError::StateMismatch);
                        }
                        self.index_pointer(recovery, key, bytes, cancelled)?;
                    }
                    IngestKey::EventEligibility {
                        visibility_scope, ..
                    }
                    | IngestKey::EventReceive {
                        visibility_scope, ..
                    } => {
                        self.reference_index(visibility_scope)?;
                        self.index_pointer(recovery, key, bytes, cancelled)?;
                    }
                    IngestKey::Provenance {
                        endpoint_id,
                        observer_id,
                        access_scope_id,
                        visibility_scope_id,
                        ..
                    } => {
                        for reference in [
                            endpoint_id,
                            observer_id,
                            access_scope_id,
                            visibility_scope_id,
                        ] {
                            self.reference_index(reference)?;
                        }
                        self.index_pointer(recovery, key, bytes, cancelled)?;
                    }
                    IngestKey::Fact { source_record_id } => {
                        let index = self.source_index(source_record_id)?;
                        if facts[index].checked_add(1) != Some(source_record_id.sequence.get()) {
                            return Err(BootstrapError::StateMismatch);
                        }
                        require_fact_bytes(bytes, cancelled)?;
                        facts[index] = source_record_id.sequence.get();
                    }
                    IngestKey::PendingBatch { first } => {
                        let index = self.source_index(first)?;
                        let header = PendingBatchHeader::decode(first, bytes)
                            .map_err(|_| BootstrapError::StateMismatch)?;
                        if pending[index].checked_add(1) != Some(first.sequence.get())
                            || header.last().sequence.get() > self.entries[index].source_through
                        {
                            return Err(BootstrapError::StateMismatch);
                        }
                        pending[index] = header.last().sequence.get();
                    }
                    IngestKey::Request { request_id } => {
                        let index = usize::try_from(request_id.producer_epoch.get() - 1)
                            .map_err(|_| BootstrapError::StateMismatch)?;
                        let entry = self
                            .entries
                            .get(index)
                            .ok_or(BootstrapError::StateMismatch)?;
                        if request_id.producer_slot_id != self.slot
                            || requests[index].checked_add(1)
                                != Some(request_id.request_sequence.get())
                            || request_id.request_sequence.get() > entry.issued
                            || bytes.is_empty()
                            || bytes.len() > super::super::MAX_OUTCOME_BYTES
                        {
                            return Err(BootstrapError::StateMismatch);
                        }
                        requests[index] = request_id.request_sequence.get();
                    }
                }
                cursor.next();
            }
            cursor.status().map_err(|_| BootstrapError::EngineRead)?;
        }
        let expected_allocations = self
            .entries
            .iter()
            .map(|e| e.allocations.iter().filter(|n| **n > 0).count())
            .sum::<usize>();
        if manifests != 1
            || handoffs != self.entries.len() - 1
            || producers != self.entries.len()
            || sources
                != self
                    .entries
                    .iter()
                    .filter(|entry| entry.source_present)
                    .count()
            || allocations != expected_allocations
        {
            return Err(BootstrapError::StateMismatch);
        }
        for (index, entry) in self.entries.iter().enumerate() {
            if requests[index] != entry.issued
                || facts[index] != entry.source_through
                || pending[index] != entry.source_through
                || dictionaries[index] != entry.allocations
                || reverse[index] != entry.allocations
            {
                return Err(BootstrapError::StateMismatch);
            }
        }
        check_cancelled(cancelled)
    }

    fn domain_index(
        &self,
        dataset: bigbrotr_model::DatasetId,
        kind: DictionaryKind,
        domain: bigbrotr_model::AllocationDomainId,
    ) -> Result<usize, BootstrapError> {
        self.entries
            .iter()
            .position(|entry| {
                entry.manifest.identity().dataset_id == dataset
                    && entry.manifest.allocation_namespaces().domain(kind) == Some(domain)
            })
            .ok_or(BootstrapError::StateMismatch)
    }
    fn reference_index(&self, reference: QualifiedRef) -> Result<usize, BootstrapError> {
        let index = self.domain_index(
            reference.dataset_id,
            reference.dictionary_kind,
            reference.allocation_domain,
        )?;
        if reference.ordinal.get()
            > self.entries[index].allocations[reference.dictionary_kind as usize - 1]
        {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(index)
    }
    fn source_index(&self, source: SourceRecordId) -> Result<usize, BootstrapError> {
        let index = usize::try_from(source.owner_epoch.get() - 1)
            .map_err(|_| BootstrapError::StateMismatch)?;
        let entry = self
            .entries
            .get(index)
            .ok_or(BootstrapError::StateMismatch)?;
        let id = entry.manifest.identity();
        if source.dataset_id != id.dataset_id
            || source.shard_id != id.shard_id
            || source.sequence.get() > entry.source_through
        {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(index)
    }
    fn index_pointer(
        &self,
        recovery: &IngestRecovery,
        key: IngestKey,
        bytes: &[u8],
        cancelled: &AtomicBool,
    ) -> Result<SourceRecordId, BootstrapError> {
        if bytes.len() != 70 || bytes[..2] != key.domain().prefix() {
            return Err(BootstrapError::StateMismatch);
        }
        let source = SourceRecordId {
            dataset_id: bigbrotr_model::DatasetId::try_from(
                <[u8; 16]>::try_from(&bytes[2..18]).expect("fixed pointer"),
            )
            .map_err(|_| BootstrapError::StateMismatch)?,
            shard_id: u32::from_be_bytes(bytes[18..22].try_into().expect("fixed pointer")),
            owner_epoch: OwnerEpoch::new(u64::from_be_bytes(
                bytes[22..30].try_into().expect("fixed pointer"),
            ))
            .map_err(|_| BootstrapError::StateMismatch)?,
            sequence: Sequence::new(u64::from_be_bytes(
                bytes[30..38].try_into().expect("fixed pointer"),
            ))
            .map_err(|_| BootstrapError::StateMismatch)?,
        };
        self.source_index(source)?;
        let fact = value(
            recovery,
            IngestKey::Fact {
                source_record_id: source,
            },
        )?;
        require_fact_bytes(&fact, cancelled)?;
        if bytes[38..] != fact[2..34] {
            return Err(BootstrapError::StateMismatch);
        }
        // The positive replay phase still validates the fact's meaning against
        // the original request and the precise semantic index key.
        Ok(source)
    }
}
fn require_fact_bytes(bytes: &[u8], cancelled: &AtomicBool) -> Result<(), BootstrapError> {
    if bytes.len() < 34
        || bytes[..2] != [1, 0x20]
        || digest_bytes(&bytes[34..], cancelled)? != bytes[2..34]
    {
        Err(BootstrapError::StateMismatch)
    } else {
        Ok(())
    }
}

fn digest_bytes(bytes: &[u8], cancelled: &AtomicBool) -> Result<[u8; 32], BootstrapError> {
    let mut digest = Sha256::new();
    for chunk in bytes.chunks(8192) {
        check_cancelled(cancelled)?;
        digest.update(chunk);
    }
    Ok(digest.finalize().into())
}
