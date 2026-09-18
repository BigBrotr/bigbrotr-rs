//! One initial dictionary batch. The connected Control owner is the only caller.
use super::{IngestStore, PendingBatchHeader, StoredReceipt};
use crate::{
    runtime::OwnedPermit,
    storage::rocks::keys::{ColumnFamily, EncodedKey, IngestKey},
};
use bigbrotr_model::{
    AdmissionBatch, DictionaryDescriptor, DictionaryKind, Digest256, EvidenceIntent, Fact,
    FactPayload, FactRef, ObservedTime, ProducerRequestId, ProtectionClass, ProtectionContext,
    Receipt, Sequence, SourceDependencies, SourceRecordId, codec::FactDecodeLimits,
    receipt_codec::ReceiptDecodeLimits,
};
use rocksdb::{ReadOptions, WriteBatch, WriteOptions};
use sha2::{Digest, Sha256};
use std::{fmt, mem::size_of};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapError {
    InvalidLimits,
    InsufficientReservation,
    InvalidIntent,
    Allocation,
    Limit,
    EngineRead,
    StateMismatch,
    RequestConflict,
    AuthorityUnavailable,
    /// No negative outcome is implied. Close/join and resolve the exact request.
    CommitUnknown,
}
impl fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "initial dictionary batch failed: {self:?}")
    }
}
impl std::error::Error for BootstrapError {}

#[derive(Debug, Clone, Copy)]
pub struct BootstrapLimits {
    pub max_mappings: usize,
    pub max_batch_bytes: usize,
}
impl BootstrapLimits {
    /// Local allocation floors, not RSS bounds. Caller retains its input grant.
    /// Includes staged values, transient encoding/decoding and returned receipt.
    pub fn required_data_bytes(self) -> Result<u64, BootstrapError> {
        if self.max_mappings == 0
            || self.max_mappings > 256
            || self.max_batch_bytes == 0
            || self.max_batch_bytes > 16 * 1024 * 1024
        {
            return Err(BootstrapError::InvalidLimits);
        }
        let vectors = self.max_mappings * (4 * size_of::<Entry>() + 2 * size_of::<FactRef>());
        Ok((4 * self.max_batch_bytes
            + vectors
            + 5 * size_of::<Entry>()
            + size_of::<Receipt>()
            + size_of::<Fact>()) as u64)
    }
    pub fn required_native_bytes(self) -> Result<u64, BootstrapError> {
        self.required_data_bytes()?;
        // WriteBatch's initial capacity is a reservation, not an enforced limit.
        // The staged conservative serialized bound below prevents its growth.
        Ok(2 * self.max_batch_bytes as u64 + 1024)
    }
    fn fact_limits(self) -> FactDecodeLimits {
        FactDecodeLimits {
            max_encoded_bytes: self.max_batch_bytes,
            max_allocation_bytes: self.max_batch_bytes,
            max_tag_nodes: 1,
            max_string_bytes: self.max_batch_bytes,
        }
    }
    fn receipt_limits(self) -> ReceiptDecodeLimits {
        ReceiptDecodeLimits {
            max_encoded_bytes: self.max_batch_bytes,
            max_allocation_bytes: 2 * self.max_mappings * size_of::<FactRef>(),
            max_batch_records: self.max_mappings,
            max_dependencies: self.max_mappings,
        }
    }
}

struct Entry {
    key: EncodedKey,
    cf: ColumnFamily,
    value: Vec<u8>,
}
pub(super) struct Prepared {
    entries: Vec<Entry>,
    serialized_bound: usize,
    limit: usize,
}
impl Prepared {
    pub(super) fn entry_allocation_bytes(max_mappings: usize) -> usize {
        (4 * max_mappings + 5) * size_of::<Entry>()
    }
    pub(super) fn new(limits: BootstrapLimits) -> Result<Self, BootstrapError> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(4 * limits.max_mappings + 5)
            .map_err(|_| BootstrapError::Allocation)?;
        Ok(Self {
            entries,
            serialized_bound: 12,
            limit: limits.max_batch_bytes,
        })
    }
    pub(super) fn put(&mut self, key: IngestKey, value: Vec<u8>) -> Result<(), BootstrapError> {
        let cf = key.domain().column_family();
        let key = key.encode().map_err(|_| BootstrapError::InvalidIntent)?;
        // Native tag + CF varint32 + two length varint32s need <= 16 bytes;
        // 21 deliberately leaves room without treating capacity as a maximum.
        self.serialized_bound = self
            .serialized_bound
            .checked_add(21 + key.as_bytes().len())
            .and_then(|n| n.checked_add(value.len()))
            .filter(|n| *n <= self.limit)
            .ok_or(BootstrapError::Limit)?;
        self.entries.push(Entry { key, cf, value });
        Ok(())
    }
    fn sort(&mut self) {
        self.entries.sort_unstable_by(|a, b| {
            (a.cf.name(), a.key.as_bytes()).cmp(&(b.cf.name(), b.key.as_bytes()))
        });
    }
}

impl IngestStore {
    /// Internal primitive, never a public authority-by-callback interface.
    /// &mut serializes decisions; the retained native LOCK excludes other opens.
    /// Only initial epoch/request one is supported. Any later state is refused.
    pub(in crate::storage) fn bootstrap_dictionaries(
        &mut self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        recorded_at: ObservedTime,
        limits: BootstrapLimits,
        permit: OwnedPermit,
        mut require_authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<StoredReceipt, BootstrapError> {
        if permit.request().units().data_bytes() < limits.required_data_bytes()?
            || permit.request().units().native_bytes() < limits.required_native_bytes()?
            || permit.request().units().items() == 0
        {
            return Err(BootstrapError::InsufficientReservation);
        }
        require_authority()?;
        if self.write_uncertain {
            return Err(BootstrapError::CommitUnknown);
        }
        self.validate_bootstrap(request, intent, limits)?;
        let digest = intent.request_digest(request);
        let receipts = self
            .db
            .cf_handle("receipts")
            .ok_or(BootstrapError::EngineRead)?;
        let request_key = IngestKey::Request {
            request_id: request,
        }
        .encode()
        .map_err(|_| BootstrapError::InvalidIntent)?;
        let retry_time = {
            let retained = self
                .db
                .get_pinned_cf_opt(receipts, request_key.as_bytes(), &read_options())
                .map_err(|_| BootstrapError::EngineRead)?;
            if let Some(value) = retained {
                let receipt = Receipt::decode_persistent(&value, limits.receipt_limits())
                    .map_err(|_| BootstrapError::StateMismatch)?;
                if receipt.request_id() != request {
                    return Err(BootstrapError::StateMismatch);
                }
                if receipt.request_digest() != digest {
                    return Err(BootstrapError::RequestConflict);
                }
                drop(value);
                let first = receipt
                    .batch()
                    .ok_or(BootstrapError::StateMismatch)?
                    .first();
                let facts = self
                    .db
                    .cf_handle("facts")
                    .ok_or(BootstrapError::EngineRead)?;
                let key = IngestKey::Fact {
                    source_record_id: first,
                }
                .encode()
                .map_err(|_| BootstrapError::StateMismatch)?;
                let value = self
                    .db
                    .get_pinned_cf_opt(facts, key.as_bytes(), &read_options())
                    .map_err(|_| BootstrapError::EngineRead)?
                    .ok_or(BootstrapError::StateMismatch)?;
                if value.len() < 34 || value[..2] != [1, 0x20] {
                    return Err(BootstrapError::StateMismatch);
                }
                let fact = Fact::decode_preimage(
                    &value[34..],
                    receipt
                        .batch()
                        .ok_or(BootstrapError::StateMismatch)?
                        .records()[0]
                        .payload_digest,
                    limits.fact_limits(),
                )
                .map_err(|_| BootstrapError::StateMismatch)?;
                Some(fact.recorded_at())
            } else {
                None
            }
        };
        let (mut prepared, receipt) =
            self.prepare_bootstrap(request, intent, retry_time.unwrap_or(recorded_at), limits)?;
        prepared.sort();
        if retry_time.is_some() {
            // Complete equality also checks pending closure, both counters, all
            // inventory values, producer state, original context and receipt.
            self.require_exact_state(&prepared.entries)?;
            require_authority()?;
            return Ok(StoredReceipt::new(receipt, permit));
        }
        let manifest = Entry {
            key: IngestKey::Manifest
                .encode()
                .map_err(|_| BootstrapError::StateMismatch)?,
            cf: ColumnFamily::Default,
            value: self.manifest.encode().as_bytes().to_vec(),
        };
        self.require_exact_state(&[manifest])?;
        self.commit_prepared(prepared, &mut require_authority)?;
        Ok(StoredReceipt::new(receipt, permit))
    }

    pub(super) fn commit_prepared(
        &mut self,
        prepared: Prepared,
        mut require_authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<(), BootstrapError> {
        let mut batch = WriteBatch::with_capacity_bytes(prepared.serialized_bound);
        for entry in &prepared.entries {
            let cf = self
                .db
                .cf_handle(entry.cf.name())
                .ok_or(BootstrapError::EngineRead)?;
            batch.put_cf(cf, entry.key.as_bytes(), &entry.value);
        }
        // Every byte was bounded before entering native put_cf. This check is a
        // binding/profile invariant, not the primary allocation defense.
        if batch.size_in_bytes() > prepared.serialized_bound {
            return Err(BootstrapError::Limit);
        }
        let mut options = WriteOptions::default();
        options.set_sync(true);
        options.disable_wal(false);
        options.set_ignore_missing_column_families(false);
        require_authority()?;
        self.write_uncertain = true;
        self.db
            .write_opt(batch, &options)
            .map_err(|_| BootstrapError::CommitUnknown)?;
        self.write_uncertain = false;
        Ok(())
    }

    fn validate_bootstrap(
        &self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        limits: BootstrapLimits,
    ) -> Result<(), BootstrapError> {
        let identity = self.manifest.identity();
        if request.producer_epoch.get() != 1
            || request.request_sequence.get() != 1
            || identity.owner_epoch.get() != 1
            || intent.admission_policy_version() != 1
            || intent.visibility_scope_id().dataset_id != identity.dataset_id
        {
            return Err(BootstrapError::InvalidIntent);
        }
        if intent.dictionaries().len() > limits.max_mappings
            || intent.encoded_intent_bytes() > limits.max_batch_bytes
        {
            return Err(BootstrapError::Limit);
        }
        let mut through = [0u64; 9];
        for (index, entry) in intent.dictionaries().iter().enumerate() {
            if let DictionaryDescriptor::Endpoint(endpoint) = entry.descriptor() {
                let normalized = bigbrotr_protocol::endpoint::Endpoint::parse(endpoint.url())
                    .map_err(|_| BootstrapError::InvalidIntent)?;
                if normalized.as_str() != endpoint.url() {
                    return Err(BootstrapError::InvalidIntent);
                }
            }
            let r = entry.reference();
            let counter = &mut through[r.dictionary_kind as usize - 1];
            if r.dictionary_kind == DictionaryKind::Event
                || self
                    .manifest
                    .allocation_namespaces()
                    .domain(r.dictionary_kind)
                    != Some(r.allocation_domain)
                || r.ordinal.get() != *counter + 1
                || intent.dictionaries()[..index]
                    .iter()
                    .any(|prior| prior.descriptor() == entry.descriptor())
            {
                return Err(BootstrapError::InvalidIntent);
            }
            *counter += 1;
        }
        Ok(())
    }

    fn prepare_bootstrap(
        &self,
        request: ProducerRequestId,
        intent: &EvidenceIntent,
        recorded_at: ObservedTime,
        limits: BootstrapLimits,
    ) -> Result<(Prepared, Receipt), BootstrapError> {
        let mut prepared = Prepared::new(limits)?;
        prepared.put(
            IngestKey::Manifest,
            self.manifest.encode().as_bytes().to_vec(),
        )?;
        let identity = self.manifest.identity();
        let mut refs = Vec::new();
        refs.try_reserve_exact(intent.dictionaries().len())
            .map_err(|_| BootstrapError::Allocation)?;
        let mut through = [0u64; 9];
        for (index, entry) in intent.dictionaries().iter().enumerate() {
            let reference = entry.reference();
            through[reference.dictionary_kind as usize - 1] = reference.ordinal.get();
            let source_record_id = SourceRecordId {
                dataset_id: identity.dataset_id,
                shard_id: identity.shard_id,
                owner_epoch: identity.owner_epoch,
                sequence: Sequence::new(index as u64 + 1)
                    .map_err(|_| BootstrapError::InvalidIntent)?,
            };
            let fact = Fact::new(
                source_record_id,
                intent.visibility_scope_id(),
                recorded_at,
                FactPayload::Dictionary(entry.clone()),
            )
            .map_err(|_| BootstrapError::InvalidIntent)?;
            // The scope can define itself; every other dependency must precede
            // this fact in the same closed initial batch.
            for dependency in fact.dictionary_refs() {
                if dependency != reference
                    && !intent.dictionaries()[..index]
                        .iter()
                        .any(|e| e.reference() == dependency)
                {
                    return Err(BootstrapError::InvalidIntent);
                }
            }
            let fact_ref = fact.fact_ref();
            let bytes = fact
                .encode_preimage(limits.max_batch_bytes)
                .map_err(|_| BootstrapError::Limit)?;
            prepared.put(
                IngestKey::Fact { source_record_id },
                frame(&[1, 0x20], &[fact_ref.payload_digest.as_bytes(), &bytes])?,
            )?;
            drop(bytes);
            drop(fact);
            let source_key = IngestKey::Fact { source_record_id }
                .encode()
                .map_err(|_| BootstrapError::InvalidIntent)?;
            prepared.put(
                IngestKey::Dictionary { reference },
                frame(
                    &[1, 4],
                    &[
                        &source_key.as_bytes()[2..],
                        fact_ref.payload_digest.as_bytes(),
                    ],
                )?,
            )?;
            let descriptor = entry
                .encode_descriptor(limits.max_batch_bytes)
                .map_err(|_| BootstrapError::Limit)?;
            let descriptor_digest = Digest256::from_bytes(Sha256::digest(&descriptor).into());
            prepared.put(
                IngestKey::DictionaryReverseCandidate {
                    dataset_id: identity.dataset_id,
                    dictionary_kind: reference.dictionary_kind,
                    descriptor_digest,
                    reference,
                },
                frame(&[1, 5], &[&descriptor])?,
            )?;
            refs.push(fact_ref);
        }
        for (index, value) in through
            .into_iter()
            .enumerate()
            .filter(|(_, value)| *value > 0)
        {
            let kind = DictionaryKind::try_from(index as u16 + 1)
                .map_err(|_| BootstrapError::InvalidIntent)?;
            let domain = self
                .manifest
                .allocation_namespaces()
                .domain(kind)
                .ok_or(BootstrapError::InvalidIntent)?;
            prepared.put(
                IngestKey::AllocationStream {
                    dataset_id: identity.dataset_id,
                    dictionary_kind: kind,
                    allocation_domain: domain,
                },
                frame(&[1, 3], &[&value.to_be_bytes()])?,
            )?;
        }
        let mut dependencies = Vec::new();
        dependencies
            .try_reserve_exact(refs.len())
            .map_err(|_| BootstrapError::Allocation)?;
        dependencies.extend_from_slice(&refs);
        let batch = AdmissionBatch::new(refs).map_err(|_| BootstrapError::InvalidIntent)?;
        prepared.put(
            IngestKey::SourceStream {
                dataset_id: identity.dataset_id,
                shard_id: identity.shard_id,
                owner_epoch: identity.owner_epoch,
            },
            frame(&[1, 2], &[&batch.last().sequence.get().to_be_bytes()])?,
        )?;
        prepared.put(
            IngestKey::PendingBatch {
                first: batch.first(),
            },
            PendingBatchHeader::from_batch(&batch).encode().to_vec(),
        )?;
        let receipt = Receipt::new_facts(
            request,
            intent.request_digest(request),
            ProtectionContext {
                dataset_id: identity.dataset_id,
                topology_generation: identity.topology_generation,
                protection_class: ProtectionClass::LocalSynced,
                recovery_generation: identity.recovery_generation,
                policy_epoch: identity.policy_epoch,
            },
            batch,
            SourceDependencies::new(identity.dataset_id, dependencies)
                .map_err(|_| BootstrapError::InvalidIntent)?,
        )
        .map_err(|_| BootstrapError::InvalidIntent)?;
        prepared.put(
            IngestKey::Request {
                request_id: request,
            },
            receipt
                .encode_persistent(limits.max_batch_bytes)
                .map_err(|_| BootstrapError::Limit)?,
        )?;
        prepared.put(
            IngestKey::ProducerState {
                producer_slot_id: request.producer_slot_id,
                producer_epoch: request.producer_epoch,
            },
            frame(
                &[1, 0x31],
                &[
                    &1u64.to_be_bytes(),
                    &0u64.to_be_bytes(),
                    &0u64.to_be_bytes(),
                ],
            )?,
        )?;
        Ok((prepared, receipt))
    }

    fn require_exact_state(&self, expected: &[Entry]) -> Result<(), BootstrapError> {
        for name in ["default", "facts", "receipts"] {
            let cf = self.db.cf_handle(name).ok_or(BootstrapError::EngineRead)?;
            let mut iterator = self.db.raw_iterator_cf_opt(cf, read_options());
            iterator.seek_to_first();
            for entry in expected.iter().filter(|e| e.cf.name() == name) {
                if iterator.key() != Some(entry.key.as_bytes())
                    || iterator.value() != Some(entry.value.as_slice())
                {
                    iterator.status().map_err(|_| BootstrapError::EngineRead)?;
                    return Err(BootstrapError::StateMismatch);
                }
                iterator.next();
            }
            if iterator.valid() {
                return Err(BootstrapError::StateMismatch);
            }
            iterator.status().map_err(|_| BootstrapError::EngineRead)?;
        }
        Ok(())
    }
}

pub(super) fn frame(prefix: &[u8], parts: &[&[u8]]) -> Result<Vec<u8>, BootstrapError> {
    let length = parts
        .iter()
        .try_fold(prefix.len(), |n, p| n.checked_add(p.len()))
        .ok_or(BootstrapError::Limit)?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(length)
        .map_err(|_| BootstrapError::Allocation)?;
    result.extend_from_slice(prefix);
    for part in parts {
        result.extend_from_slice(part);
    }
    Ok(result)
}
pub(super) fn read_options() -> ReadOptions {
    let mut options = ReadOptions::default();
    options.set_verify_checksums(true);
    options.fill_cache(false);
    options
}
