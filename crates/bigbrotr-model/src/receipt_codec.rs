//! Closed persistent encoding for operational mutation receipts.
//! Decoding establishes structure and exact bytes only; authority, commit,
//! protection and current usability remain storage-owner responsibilities.
use crate::{
    AdmissionBatch, DatasetId, Digest256, FactRef, OwnerEpoch, PolicyEpoch, ProducerEpoch,
    ProducerRequestId, ProducerSlotId, ProtectionClass, ProtectionContext, Receipt,
    RecoveryGenerationId, RequestSequence, Sequence, SourceDependencies, SourceRecordId,
    TopologyGeneration, effect::EffectError, source::SourceError,
};
use minicbor::{Decoder, Encoder, encode::Write};
use std::{fmt, mem::size_of};

const RECEIPT_PREFIX: &[u8] = b"bigbrotr/receipt/1\0";
const NEW_FACTS: u8 = 1;
const ALREADY_DURABLE: u8 = 2;
const LOCAL_SYNCED: u8 = 1;
// array(2) + array(4) + bytes16 + three shortest integers + bytes32.
const MIN_FACT_REF_BYTES: usize = 56;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptCodecError {
    InputLimit,
    AllocationLimit,
    BatchRecordLimit,
    DependencyLimit,
    AllocationFailed,
    Malformed,
    InvalidValue,
    IncompatibleSchema,
    NonCanonical,
    InvalidBatch(SourceError),
    InvalidReceipt(EffectError),
}
impl fmt::Display for ReceiptCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InputLimit => "receipt exceeds the encoded byte limit",
            Self::AllocationLimit => "receipt exceeds the requested allocation budget",
            Self::BatchRecordLimit => "receipt batch exceeds the record-count limit",
            Self::DependencyLimit => "receipt closure exceeds the dependency-count limit",
            Self::AllocationFailed => "unable to reserve memory for the receipt",
            Self::Malformed => "malformed closed receipt encoding",
            Self::InvalidValue => "invalid receipt scalar or fixed identity",
            Self::IncompatibleSchema => "unsupported receipt version, result or protection class",
            Self::NonCanonical => "receipt is not exact canonical encoding",
            Self::InvalidBatch(_) => "receipt batch violates structural invariants",
            Self::InvalidReceipt(_) => "receipt closure violates structural invariants",
        })
    }
}
impl std::error::Error for ReceiptCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidBatch(error) => Some(error),
            Self::InvalidReceipt(error) => Some(error),
            _ => None,
        }
    }
}
impl From<minicbor::decode::Error> for ReceiptCodecError {
    fn from(_: minicbor::decode::Error) -> Self {
        Self::Malformed
    }
}

/// Per-value decode bounds. The adapter must also reserve the encoded input and
/// this requested Vec storage from its shared runtime grant. Requested bytes do
/// not include allocator overhead and are not a process RSS promise.
#[derive(Debug, Clone, Copy)]
pub struct ReceiptDecodeLimits {
    pub max_encoded_bytes: usize,
    pub max_allocation_bytes: usize,
    pub max_batch_records: usize,
    pub max_dependencies: usize,
}
impl Default for ReceiptDecodeLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 8 * 1024 * 1024,
            max_allocation_bytes: 8 * 1024 * 1024,
            max_batch_records: 65_536,
            max_dependencies: 65_536,
        }
    }
}

struct CountWriter {
    count: usize,
    limit: usize,
}
impl Write for CountWriter {
    type Error = ReceiptCodecError;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.count = self
            .count
            .checked_add(bytes.len())
            .filter(|count| *count <= self.limit)
            .ok_or(ReceiptCodecError::InputLimit)?;
        Ok(())
    }
}
struct CompareWriter<'a>(&'a [u8]);
impl Write for CompareWriter<'_> {
    type Error = ReceiptCodecError;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = self
            .0
            .strip_prefix(bytes)
            .ok_or(ReceiptCodecError::NonCanonical)?;
        Ok(())
    }
}

impl Receipt {
    /// Encode the v1 operational receipt value. The caller supplies its storage
    /// bound; no self-digest or scientific fingerprint is added.
    pub fn encode_persistent(
        &self,
        max_encoded_bytes: usize,
    ) -> Result<Vec<u8>, ReceiptCodecError> {
        let mut count = Encoder::new(CountWriter {
            count: 0,
            limit: max_encoded_bytes,
        });
        encode_receipt(self, &mut count).map_err(|_| ReceiptCodecError::InputLimit)?;
        let size = count.into_writer().count;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| ReceiptCodecError::AllocationFailed)?;
        bytes.resize(size, 0);
        let mut encoder = Encoder::new(bytes.as_mut_slice());
        encode_receipt(self, &mut encoder).map_err(|_| ReceiptCodecError::Malformed)?;
        if !encoder.into_writer().is_empty() {
            return Err(ReceiptCodecError::Malformed);
        }
        Ok(bytes)
    }

    /// Decode a structural receipt and reproduce its exact canonical bytes.
    /// This does not prove that the named write committed, remains protected,
    /// belongs to a live producer, or can currently authorize progress.
    pub fn decode_persistent(
        bytes: &[u8],
        limits: ReceiptDecodeLimits,
    ) -> Result<Self, ReceiptCodecError> {
        if bytes.len() > limits.max_encoded_bytes {
            return Err(ReceiptCodecError::InputLimit);
        }
        let input = bytes
            .strip_prefix(RECEIPT_PREFIX)
            .ok_or(ReceiptCodecError::IncompatibleSchema)?;
        let mut reader = ReceiptReader {
            decoder: Decoder::new(input),
            limits,
            allocated: 0,
        };
        let receipt = reader.receipt()?;
        if reader.decoder.position() != input.len() {
            return Err(ReceiptCodecError::NonCanonical);
        }
        let mut encoder = Encoder::new(CompareWriter(bytes));
        encode_receipt(&receipt, &mut encoder).map_err(|_| ReceiptCodecError::NonCanonical)?;
        if !encoder.into_writer().0.is_empty() {
            return Err(ReceiptCodecError::NonCanonical);
        }
        Ok(receipt)
    }
}

fn encode_source<W: Write>(
    id: SourceRecordId,
    encoder: &mut Encoder<W>,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    encoder
        .array(4)?
        .bytes(id.dataset_id.as_bytes())?
        .u32(id.shard_id)?
        .u64(id.owner_epoch.get())?
        .u64(id.sequence.get())?
        .ok()
}
fn encode_record<W: Write>(
    record: &FactRef,
    encoder: &mut Encoder<W>,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    encoder.array(2)?;
    encode_source(record.source_record_id, encoder)?;
    encoder.bytes(record.payload_digest.as_bytes())?.ok()
}
fn encode_records<W: Write>(
    records: &[FactRef],
    encoder: &mut Encoder<W>,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    encoder.array(records.len() as u64)?;
    for record in records {
        encode_record(record, encoder)?;
    }
    Ok(())
}
fn encode_receipt<W: Write>(
    receipt: &Receipt,
    encoder: &mut Encoder<W>,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(RECEIPT_PREFIX)
        .map_err(minicbor::encode::Error::write)?;
    let result = if receipt.batch().is_some() {
        NEW_FACTS
    } else {
        ALREADY_DURABLE
    };
    let request = receipt.request_id();
    let context = receipt.protection_context();
    let protection = match context.protection_class {
        ProtectionClass::LocalSynced => LOCAL_SYNCED,
    };
    encoder
        .array(6)?
        .u8(result)?
        .array(3)?
        .bytes(request.producer_slot_id.as_bytes())?
        .u64(request.producer_epoch.get())?
        .u64(request.request_sequence.get())?
        .bytes(receipt.request_digest().as_bytes())?
        .array(5)?
        .bytes(context.dataset_id.as_bytes())?
        .u64(context.topology_generation.get())?
        .u8(protection)?
        .bytes(context.recovery_generation.as_bytes())?
        .u64(context.policy_epoch.get())?;
    match receipt.batch() {
        Some(batch) => encode_records(batch.records(), encoder)?,
        None => {
            encoder.null()?;
        }
    }
    encode_records(receipt.dependencies().records(), encoder)
}

struct ReceiptReader<'a> {
    decoder: Decoder<'a>,
    limits: ReceiptDecodeLimits,
    allocated: usize,
}
impl ReceiptReader<'_> {
    fn array(&mut self, expected: u64) -> Result<(), ReceiptCodecError> {
        if self.decoder.array()? != Some(expected) {
            return Err(ReceiptCodecError::Malformed);
        }
        Ok(())
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], ReceiptCodecError> {
        self.decoder
            .bytes()?
            .try_into()
            .map_err(|_| ReceiptCodecError::InvalidValue)
    }
    fn request(&mut self) -> Result<ProducerRequestId, ReceiptCodecError> {
        self.array(3)?;
        Ok(ProducerRequestId {
            producer_slot_id: ProducerSlotId::try_from(self.fixed()?)
                .map_err(|_| ReceiptCodecError::InvalidValue)?,
            producer_epoch: ProducerEpoch::new(self.decoder.u64()?)
                .map_err(|_| ReceiptCodecError::InvalidValue)?,
            request_sequence: RequestSequence::new(self.decoder.u64()?)
                .map_err(|_| ReceiptCodecError::InvalidValue)?,
        })
    }
    fn context(&mut self) -> Result<ProtectionContext, ReceiptCodecError> {
        self.array(5)?;
        let dataset_id =
            DatasetId::try_from(self.fixed()?).map_err(|_| ReceiptCodecError::InvalidValue)?;
        let topology_generation = TopologyGeneration::new(self.decoder.u64()?);
        if self.decoder.u8()? != LOCAL_SYNCED {
            return Err(ReceiptCodecError::IncompatibleSchema);
        }
        Ok(ProtectionContext {
            dataset_id,
            topology_generation,
            protection_class: ProtectionClass::LocalSynced,
            recovery_generation: RecoveryGenerationId::from_bytes(self.fixed()?),
            policy_epoch: PolicyEpoch::new(self.decoder.u64()?),
        })
    }
    fn source(&mut self) -> Result<SourceRecordId, ReceiptCodecError> {
        self.array(4)?;
        Ok(SourceRecordId {
            dataset_id: DatasetId::try_from(self.fixed()?)
                .map_err(|_| ReceiptCodecError::InvalidValue)?,
            shard_id: self.decoder.u32()?,
            owner_epoch: OwnerEpoch::new(self.decoder.u64()?)
                .map_err(|_| ReceiptCodecError::InvalidValue)?,
            sequence: Sequence::new(self.decoder.u64()?)
                .map_err(|_| ReceiptCodecError::InvalidValue)?,
        })
    }
    fn record(&mut self) -> Result<FactRef, ReceiptCodecError> {
        self.array(2)?;
        Ok(FactRef {
            source_record_id: self.source()?,
            payload_digest: Digest256::from_bytes(self.fixed()?),
        })
    }
    fn charge_records(&mut self, count: usize) -> Result<(), ReceiptCodecError> {
        let bytes = count
            .checked_mul(size_of::<FactRef>())
            .ok_or(ReceiptCodecError::AllocationLimit)?;
        self.allocated = self
            .allocated
            .checked_add(bytes)
            .filter(|total| *total <= self.limits.max_allocation_bytes)
            .ok_or(ReceiptCodecError::AllocationLimit)?;
        Ok(())
    }
    fn records(
        &mut self,
        max_count: usize,
        count_error: ReceiptCodecError,
    ) -> Result<Vec<FactRef>, ReceiptCodecError> {
        let count = self.decoder.array()?.ok_or(ReceiptCodecError::Malformed)?;
        let count = usize::try_from(count).map_err(|_| count_error)?;
        if count > max_count {
            return Err(count_error);
        }
        let remaining = self.decoder.input().len() - self.decoder.position();
        if count > remaining / MIN_FACT_REF_BYTES {
            return Err(ReceiptCodecError::Malformed);
        }
        self.charge_records(count)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(count)
            .map_err(|_| ReceiptCodecError::AllocationFailed)?;
        for _ in 0..count {
            records.push(self.record()?);
        }
        Ok(records)
    }
    fn receipt(&mut self) -> Result<Receipt, ReceiptCodecError> {
        self.array(6)?;
        let result = self.decoder.u8()?;
        if !matches!(result, NEW_FACTS | ALREADY_DURABLE) {
            return Err(ReceiptCodecError::IncompatibleSchema);
        }
        let request = self.request()?;
        let request_digest = Digest256::from_bytes(self.fixed()?);
        let context = self.context()?;
        let batch = match result {
            NEW_FACTS => {
                if self.decoder.datatype()? == minicbor::data::Type::Null {
                    return Err(ReceiptCodecError::IncompatibleSchema);
                }
                Some(
                    AdmissionBatch::new(self.records(
                        self.limits.max_batch_records,
                        ReceiptCodecError::BatchRecordLimit,
                    )?)
                    .map_err(ReceiptCodecError::InvalidBatch)?,
                )
            }
            ALREADY_DURABLE => {
                if self.decoder.datatype()? != minicbor::data::Type::Null {
                    return Err(ReceiptCodecError::IncompatibleSchema);
                }
                self.decoder.null()?;
                None
            }
            _ => return Err(ReceiptCodecError::IncompatibleSchema),
        };
        let dependencies = SourceDependencies::new(
            context.dataset_id,
            self.records(
                self.limits.max_dependencies,
                ReceiptCodecError::DependencyLimit,
            )?,
        )
        .map_err(ReceiptCodecError::InvalidReceipt)?;
        match batch {
            Some(batch) => {
                Receipt::new_facts(request, request_digest, context, batch, dependencies)
            }
            None => Receipt::already_durable(request, request_digest, context, dependencies),
        }
        .map_err(ReceiptCodecError::InvalidReceipt)
    }
}
