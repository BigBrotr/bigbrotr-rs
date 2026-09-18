//! Private pending-to-worker framing; neither a portable export nor commit authority.
use bigbrotr_model::{
    AdmissionBatch, DatasetId, Digest256, Fact, FactRef, OwnerEpoch, Sequence, SourceRecordId,
    codec::{CodecError, FactDecodeLimits},
};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    io::{self, Read, Write},
    mem::size_of,
    sync::atomic::{AtomicBool, Ordering},
};

const MAGIC: &[u8; 8] = b"BBAP\0\0\0\x01";

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_records: usize,
    /// Sum of canonical preimages, excluding fixed framing overhead.
    pub max_encoded_bytes: usize,
    pub fact: FactDecodeLimits,
}
impl Limits {
    /// Reader allocation floor, excluding the caller's retained decoded facts,
    /// allocator overhead and native staging. The caller owns the runtime grant.
    pub fn required_data_bytes(self) -> Result<usize, Error> {
        if self.max_records == 0
            || self.max_records > u32::MAX as usize
            || self.max_encoded_bytes == 0
            || self.fact.max_encoded_bytes == 0
            || self.fact.max_encoded_bytes > u32::MAX as usize
            || self.fact.max_allocation_bytes == 0
            || self.fact.max_tag_nodes == 0
            || self.fact.max_string_bytes == 0
        {
            return Err(Error::InvalidLimits);
        }
        self.max_records
            .checked_mul(size_of::<FactRef>())
            .and_then(|n| n.checked_add(self.fact.max_encoded_bytes))
            .and_then(|n| n.checked_add(self.fact.max_allocation_bytes))
            .and_then(|n| n.checked_add(size_of::<Fact>()))
            .ok_or(Error::InvalidLimits)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidLimits,
    Limit,
    Allocation,
    Io,
    Malformed,
    IncompatibleSchema,
    Integrity,
    Incomplete,
    Cancelled,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "archive batch transfer failed: {self:?}")
    }
}
impl std::error::Error for Error {}

/// Checks all supplied lengths/digests before the first output byte. Does not
/// copy the retained preimages or grant authority. Blocking I/O must be owned by
/// a terminable transport; cancellation is checked between bounded records.
pub fn write_batch(
    batch: &AdmissionBatch,
    records: &[Vec<u8>],
    output: &mut impl Write,
    limits: Limits,
    cancelled: &AtomicBool,
) -> Result<(), Error> {
    limits.required_data_bytes()?;
    check_cancel(cancelled)?;
    if records.len() != batch.records().len() {
        return Err(Error::Incomplete);
    }
    if records.len() > limits.max_records {
        return Err(Error::Limit);
    }
    let mut remaining = limits.max_encoded_bytes;
    for (bytes, expected) in records.iter().zip(batch.records()) {
        check_cancel(cancelled)?;
        remaining = remaining.checked_sub(bytes.len()).ok_or(Error::Limit)?;
        if bytes.len() > limits.fact.max_encoded_bytes {
            return Err(Error::Limit);
        }
        if Digest256::from_bytes(Sha256::digest(bytes).into()) != expected.payload_digest {
            return Err(Error::Integrity);
        }
    }
    let first = batch.first();
    let mut header = [0; 80];
    header[..8].copy_from_slice(MAGIC);
    header[8..24].copy_from_slice(first.dataset_id.as_bytes());
    header[24..28].copy_from_slice(&first.shard_id.to_be_bytes());
    header[28..36].copy_from_slice(&first.owner_epoch.get().to_be_bytes());
    header[36..44].copy_from_slice(&first.sequence.get().to_be_bytes());
    header[44..48].copy_from_slice(&(records.len() as u32).to_be_bytes());
    header[48..].copy_from_slice(batch.digest().as_bytes());
    check_cancel(cancelled)?;
    output.write_all(&header).map_err(io_error)?;
    for (bytes, expected) in records.iter().zip(batch.records()) {
        check_cancel(cancelled)?;
        output
            .write_all(expected.payload_digest.as_bytes())
            .map_err(io_error)?;
        output
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(io_error)?;
        output.write_all(bytes).map_err(io_error)?;
    }
    check_cancel(cancelled)?;
    output.flush().map_err(io_error)
}

/// Decodes one fact at a time. Drop each fact after private staging to keep the
/// allocation floor valid. Partial staging must never be published: only finish
/// validates the full batch digest and exact EOF. Any read failure poisons this
/// reader. The enclosing worker/transport owns blocking-read termination.
pub struct BatchReader<R: Read> {
    input: R,
    limits: Limits,
    first: SourceRecordId,
    digest: Digest256,
    count: usize,
    references: Vec<FactRef>,
    buffer: Vec<u8>,
    remaining: usize,
    available: bool,
}
impl<R: Read> BatchReader<R> {
    pub fn new(mut input: R, limits: Limits, cancelled: &AtomicBool) -> Result<Self, Error> {
        limits.required_data_bytes()?;
        check_cancel(cancelled)?;
        let header = read_array::<80>(&mut input)?;
        if &header[..8] != MAGIC {
            return Err(Error::IncompatibleSchema);
        }
        let first = SourceRecordId {
            dataset_id: DatasetId::try_from(part::<16>(&header[8..24])?)
                .map_err(|_| Error::Malformed)?,
            shard_id: u32::from_be_bytes(part(&header[24..28])?),
            owner_epoch: OwnerEpoch::new(u64::from_be_bytes(part(&header[28..36])?))
                .map_err(|_| Error::Malformed)?,
            sequence: Sequence::new(u64::from_be_bytes(part(&header[36..44])?))
                .map_err(|_| Error::Malformed)?,
        };
        let count = u32::from_be_bytes(part(&header[44..48])?) as usize;
        if count == 0 {
            return Err(Error::Malformed);
        }
        if count > limits.max_records {
            return Err(Error::Limit);
        }
        first
            .sequence
            .get()
            .checked_add(count as u64 - 1)
            .ok_or(Error::Malformed)?;
        let mut references = Vec::new();
        references
            .try_reserve_exact(count)
            .map_err(|_| Error::Allocation)?;
        check_cancel(cancelled)?;
        Ok(Self {
            input,
            limits,
            first,
            digest: Digest256::from_bytes(part(&header[48..])?),
            count,
            references,
            buffer: Vec::new(),
            remaining: limits.max_encoded_bytes,
            available: true,
        })
    }
    pub fn next_fact(&mut self, cancelled: &AtomicBool) -> Result<Option<Fact>, Error> {
        if !self.available {
            return Err(Error::Incomplete);
        }
        self.available = false;
        check_cancel(cancelled)?;
        if self.references.len() == self.count {
            self.available = true;
            return Ok(None);
        }
        let digest = Digest256::from_bytes(read_array(&mut self.input)?);
        let length = u32::from_be_bytes(read_array(&mut self.input)?) as usize;
        self.remaining = self.remaining.checked_sub(length).ok_or(Error::Limit)?;
        if length > self.limits.fact.max_encoded_bytes {
            return Err(Error::Limit);
        }
        self.buffer.clear();
        self.buffer
            .try_reserve_exact(length)
            .map_err(|_| Error::Allocation)?;
        self.buffer.resize(length, 0);
        self.input.read_exact(&mut self.buffer).map_err(io_error)?;
        check_cancel(cancelled)?;
        let fact =
            Fact::decode_preimage(&self.buffer, digest, self.limits.fact).map_err(codec_error)?;
        let expected = SourceRecordId {
            sequence: Sequence::new(self.first.sequence.get() + self.references.len() as u64)
                .map_err(|_| Error::Malformed)?,
            ..self.first
        };
        if fact.source_record_id() != expected {
            return Err(Error::Integrity);
        }
        self.references.push(FactRef {
            source_record_id: expected,
            payload_digest: digest,
        });
        self.available = true;
        Ok(Some(fact))
    }
    pub fn finish(mut self, cancelled: &AtomicBool) -> Result<AdmissionBatch, Error> {
        check_cancel(cancelled)?;
        if !self.available || self.references.len() != self.count {
            return Err(Error::Incomplete);
        }
        let batch = AdmissionBatch::new(self.references).map_err(|_| Error::Integrity)?;
        if batch.digest() != self.digest {
            return Err(Error::Integrity);
        }
        let mut byte = [0];
        loop {
            check_cancel(cancelled)?;
            match self.input.read(&mut byte) {
                Ok(0) => return Ok(batch),
                Ok(_) => return Err(Error::Malformed),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(io_error(e)),
            }
        }
    }
}
fn part<const N: usize>(bytes: &[u8]) -> Result<[u8; N], Error> {
    bytes.try_into().map_err(|_| Error::Malformed)
}
fn read_array<const N: usize>(input: &mut impl Read) -> Result<[u8; N], Error> {
    let mut bytes = [0; N];
    input.read_exact(&mut bytes).map_err(io_error)?;
    Ok(bytes)
}
fn io_error(error: io::Error) -> Error {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        Error::Incomplete
    } else {
        Error::Io
    }
}
fn check_cancel(cancelled: &AtomicBool) -> Result<(), Error> {
    if cancelled.load(Ordering::Relaxed) {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}
fn codec_error(error: CodecError) -> Error {
    match error {
        CodecError::InputLimit
        | CodecError::AllocationLimit
        | CodecError::TagLimit
        | CodecError::StringLimit => Error::Limit,
        CodecError::AllocationFailed => Error::Allocation,
        CodecError::IncompatibleSchema => Error::IncompatibleSchema,
        _ => Error::Integrity,
    }
}
