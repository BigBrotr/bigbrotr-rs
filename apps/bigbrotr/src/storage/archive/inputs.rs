//! Immutable complete pending-batch locators, synchronized before Control issuance.
//! These are recovery inputs, never receipt, exclusion or protection evidence.
use super::{LifetimeError, LocalCoordinator, lifetime};
use crate::{
    runtime::OwnedPermit,
    storage::{postgres::archive_attempt::ArchiveAttemptRequest, rocks::store::PendingBatchHeader},
};
use bigbrotr_model::{ArchiveBatch, Digest256, OwnerEpoch, Sequence, SourceRecordId};
use bigbrotr_protocol::archive_receipt::{ReceiptExpectation, ReceiptRange};
use rustix::fs::{Mode, OFlags, openat};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Write},
    sync::atomic::{AtomicBool, Ordering},
};

const MAGIC: &[u8; 8] = b"BBINPUT\x01";
const MAX_BATCHES: usize = 1024;
const PREFIX: usize = 8 + 32 + 128 + 4;
const HEADER: usize = 20 + 42;
const MAX_BYTES: usize = PREFIX + HEADER * MAX_BATCHES + 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputError {
    Custody(LifetimeError),
    Identity,
    ResourceLimit,
    Cancelled,
}
impl From<LifetimeError> for InputError {
    fn from(value: LifetimeError) -> Self {
        Self::Custody(value)
    }
}

/// Keeps the original coordinator lock and allocation grant through async
/// issuance/inspection. The actual Control driver must also retain its own
/// custody clone until joined. No method grants another native launch.
pub struct RetainedInputs {
    request: ArchiveAttemptRequest,
    headers: Vec<PendingBatchHeader>,
    expectation: ReceiptExpectation,
    coordinator_binding: Digest256,
    _custody: File,
    _permit: OwnedPermit,
}
impl RetainedInputs {
    /// Covers bounded encoded/retry buffers, headers, ranges and their temporary
    /// serialization. Not a process RSS, native engine or retained-disk allowance.
    pub const fn required_data_bytes() -> u64 {
        1024 * 1024
    }
    pub fn request(&self) -> &ArchiveAttemptRequest {
        &self.request
    }
    pub fn headers(&self) -> &[PendingBatchHeader] {
        &self.headers
    }
    pub fn expectation(&self) -> &ReceiptExpectation {
        &self.expectation
    }
    pub fn coordinator_binding(&self) -> Digest256 {
        self.coordinator_binding
    }

    /// Call only after the actual Ingest owner has read every exact first batch
    /// and verified its fact bytes. An equal semantic digest alone cannot detect
    /// different admission grouping; compare every complete header too.
    pub fn verify_replay(&self, batch: &ArchiveBatch<'_>) -> Result<(), InputError> {
        matches_batch(&self.request, batch)?;
        if self.headers.len() != batch.batches().len()
            || !self
                .headers
                .iter()
                .copied()
                .eq(batch.batches().iter().map(PendingBatchHeader::from_batch))
        {
            return Err(InputError::Identity);
        }
        Ok(())
    }
}

impl LocalCoordinator {
    /// Synchronous bounded local I/O: run on an owned/joined blocking worker in
    /// a service. Check actual downstream route/wire/resource admissibility before
    /// issuance. Publication currently consumes one complete PendingBatch.
    /// A failed or cancelled write may leave bytes; preserve and resolve them.
    pub fn retain_inputs(
        &self,
        request: &ArchiveAttemptRequest,
        batch: &ArchiveBatch<'_>,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<RetainedInputs, InputError> {
        self.check_inputs(request, &permit, cancelled)?;
        if batch.batches().len() > MAX_BATCHES {
            return Err(InputError::ResourceLimit);
        }
        matches_batch(request, batch)?;
        let mut headers = Vec::new();
        headers
            .try_reserve_exact(batch.batches().len())
            .map_err(|_| InputError::ResourceLimit)?;
        headers.extend(batch.batches().iter().map(PendingBatchHeader::from_batch));
        let expectation = expectation(request, &headers)?;
        let bytes = encode(request, self.initialization_sha256, &headers)?;
        let custody = self.custody.retain_custody()?;
        let name = name(request);
        check_cancel(cancelled)?;
        if let Some((file, prior)) = read(&self.directory, &name)? {
            if prior != bytes {
                return Err(InputError::Identity);
            }
            file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        } else {
            let mut file = lifetime::create_file(&self.directory, &name)?;
            file.write_all(&bytes)
                .map_err(|_| LifetimeError::Filesystem)?;
            file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        }
        self.directory
            .sync_all()
            .map_err(|_| LifetimeError::Filesystem)?;
        check_cancel(cancelled)?;
        Ok(RetainedInputs {
            request: request.clone(),
            headers,
            expectation,
            coordinator_binding: self.binding_digest(),
            _custody: custody,
            _permit: permit,
        })
    }

    /// Missing linkage means unknown input, never rollback. This does not create,
    /// repair, adopt or register anything, and requires no fact bodies or secrets.
    pub fn read_inputs(
        &self,
        request: &ArchiveAttemptRequest,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Option<RetainedInputs>, InputError> {
        self.check_inputs(request, &permit, cancelled)?;
        let Some((file, bytes)) = read(&self.directory, &name(request))? else {
            return Ok(None);
        };
        let headers = decode(request, self.initialization_sha256, &bytes)?;
        let expectation = expectation(request, &headers)?;
        file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        self.directory
            .sync_all()
            .map_err(|_| LifetimeError::Filesystem)?;
        check_cancel(cancelled)?;
        Ok(Some(RetainedInputs {
            request: request.clone(),
            headers,
            expectation,
            coordinator_binding: self.binding_digest(),
            _custody: self.custody.retain_custody()?,
            _permit: permit,
        }))
    }

    fn check_inputs(
        &self,
        request: &ArchiveAttemptRequest,
        permit: &OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<(), InputError> {
        if permit.request().units().data_bytes() < RetainedInputs::required_data_bytes()
            || permit.request().units().items() == 0
        {
            return Err(InputError::ResourceLimit);
        }
        check_cancel(cancelled)?;
        let identity = self.custody.identity();
        if request.dataset() != identity.dataset() || request.catalog() != identity.catalog() {
            return Err(InputError::Identity);
        }
        Ok(())
    }
}

fn matches_batch(
    request: &ArchiveAttemptRequest,
    batch: &ArchiveBatch<'_>,
) -> Result<(), InputError> {
    if batch.schema_version() != 1
        || batch.dataset_id() != request.dataset()
        || batch.topology_generation() != request.topology_generation()
        || batch.digest() != request.batch_digest()
    {
        return Err(InputError::Identity);
    }
    Ok(())
}
fn check_cancel(cancelled: &AtomicBool) -> Result<(), InputError> {
    if cancelled.load(Ordering::Relaxed) {
        Err(InputError::Cancelled)
    } else {
        Ok(())
    }
}
fn name(request: &ArchiveAttemptRequest) -> String {
    use std::fmt::Write;
    let mut name = String::with_capacity(39);
    for byte in request.attempt_id() {
        write!(&mut name, "{byte:02x}").expect("String write");
    }
    name.push_str(".inputs");
    name
}
fn prefix(
    request: &ArchiveAttemptRequest,
    initialization: [u8; 32],
    count: usize,
    bytes: &mut Vec<u8>,
) {
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&initialization);
    bytes.extend_from_slice(request.attempt_id());
    bytes.extend_from_slice(request.dataset().as_bytes());
    bytes.extend_from_slice(request.catalog().as_bytes());
    bytes.extend_from_slice(request.coordinator().as_bytes());
    bytes.extend_from_slice(request.worker().as_bytes());
    bytes.extend_from_slice(&request.ordinal().to_be_bytes()); // predecessor = ordinal - 1
    bytes.extend_from_slice(&request.topology_generation().get().to_be_bytes());
    bytes.extend_from_slice(request.batch_digest().as_bytes());
    bytes.extend_from_slice(&(count as u32).to_be_bytes());
}
fn encode(
    request: &ArchiveAttemptRequest,
    initialization: [u8; 32],
    headers: &[PendingBatchHeader],
) -> Result<Vec<u8>, InputError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(PREFIX + headers.len() * HEADER + 32)
        .map_err(|_| InputError::ResourceLimit)?;
    prefix(request, initialization, headers.len(), &mut bytes);
    for header in headers {
        bytes.extend_from_slice(&header.first().shard_id.to_be_bytes());
        bytes.extend_from_slice(&header.first().owner_epoch.get().to_be_bytes());
        bytes.extend_from_slice(&header.first().sequence.get().to_be_bytes());
        bytes.extend_from_slice(&header.encode());
    }
    let checksum = Sha256::digest(&bytes);
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}
fn decode(
    request: &ArchiveAttemptRequest,
    initialization: [u8; 32],
    bytes: &[u8],
) -> Result<Vec<PendingBatchHeader>, InputError> {
    if bytes.len() < PREFIX + HEADER + 32 {
        return Err(InputError::Identity);
    }
    let count =
        u32::from_be_bytes(bytes[PREFIX - 4..PREFIX].try_into().expect("fixed prefix")) as usize;
    if count > MAX_BATCHES {
        return Err(InputError::ResourceLimit);
    }
    if count == 0 || bytes.len() != PREFIX + count * HEADER + 32 {
        return Err(InputError::Identity);
    }
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(PREFIX)
        .map_err(|_| InputError::ResourceLimit)?;
    prefix(request, initialization, count, &mut expected);
    let end = bytes.len() - 32;
    if bytes[..PREFIX] != expected || Sha256::digest(&bytes[..end])[..] != bytes[end..] {
        return Err(InputError::Identity);
    }
    let mut headers = Vec::new();
    headers
        .try_reserve_exact(count)
        .map_err(|_| InputError::ResourceLimit)?;
    for entry in bytes[PREFIX..end].as_chunks::<HEADER>().0 {
        let first = SourceRecordId {
            dataset_id: request.dataset(),
            shard_id: u32::from_be_bytes(entry[..4].try_into().expect("fixed header")),
            owner_epoch: OwnerEpoch::new(u64::from_be_bytes(
                entry[4..12].try_into().expect("fixed header"),
            ))
            .map_err(|_| InputError::Identity)?,
            sequence: Sequence::new(u64::from_be_bytes(
                entry[12..20].try_into().expect("fixed header"),
            ))
            .map_err(|_| InputError::Identity)?,
        };
        headers.push(
            PendingBatchHeader::decode(first, &entry[20..]).map_err(|_| InputError::Identity)?,
        );
    }
    Ok(headers)
}
fn expectation(
    request: &ArchiveAttemptRequest,
    headers: &[PendingBatchHeader],
) -> Result<ReceiptExpectation, InputError> {
    let mut ranges: Vec<ReceiptRange> = Vec::new();
    ranges
        .try_reserve_exact(headers.len())
        .map_err(|_| InputError::ResourceLimit)?;
    let mut count = 0u64;
    let mut previous = None;
    for header in headers {
        let first = header.first();
        let last = header.last();
        if first.dataset_id != request.dataset() || previous.is_some_and(|prior| prior >= first) {
            return Err(InputError::Identity);
        }
        count = count
            .checked_add(last.sequence.get() - first.sequence.get() + 1)
            .ok_or(InputError::Identity)?;
        previous = Some(last);
        if let Some(range) = ranges.last_mut().filter(|range| {
            range.shard_id == first.shard_id
                && range.owner_epoch == first.owner_epoch.get()
                && range.last_sequence.checked_add(1) == Some(first.sequence.get())
        }) {
            range.last_sequence = last.sequence.get();
        } else {
            ranges.push(ReceiptRange {
                shard_id: first.shard_id,
                owner_epoch: first.owner_epoch.get(),
                first_sequence: first.sequence.get(),
                last_sequence: last.sequence.get(),
            });
        }
    }
    let result = ReceiptExpectation {
        dataset_id: request.dataset(),
        schema_version: 1,
        topology_generation: request.topology_generation(),
        batch_digest: request.batch_digest(),
        record_count: count,
        input_ranges: ranges,
        attempt_id: *request.attempt_id(),
        publication_ordinal: request.ordinal(),
    };
    result.validate().map_err(|_| InputError::Identity)?;
    Ok(result)
}
fn read(parent: &File, name: &str) -> Result<Option<(File, Vec<u8>)>, InputError> {
    let file = match openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => File::from(fd),
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(_) => return Err(LifetimeError::Incomplete.into()),
    };
    let m = file.metadata().map_err(|_| LifetimeError::Filesystem)?;
    if !lifetime::private_file(&m) || m.len() == 0 {
        return Err(LifetimeError::Incomplete.into());
    }
    if m.len() > MAX_BYTES as u64 {
        return Err(InputError::ResourceLimit);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(MAX_BYTES + 1)
        .map_err(|_| InputError::ResourceLimit)?;
    (&file)
        .take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| LifetimeError::Filesystem)?;
    if bytes.len() > MAX_BYTES {
        return Err(InputError::ResourceLimit);
    }
    Ok(Some((file, bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr_model::{AdmissionBatch, Digest256, FactRef, TopologyGeneration};
    #[test]
    fn frozen_bytes_and_all_single_byte_corruptions() {
        let oracle: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/storage/archive-inputs-v1.json"
        ))
        .unwrap();
        let input = [AdmissionBatch::new(
            (1..=2)
                .map(|sequence| FactRef {
                    source_record_id: SourceRecordId {
                        dataset_id: [1; 16].try_into().unwrap(),
                        shard_id: 0,
                        owner_epoch: OwnerEpoch::new(1).unwrap(),
                        sequence: Sequence::new(sequence).unwrap(),
                    },
                    payload_digest: Digest256::from_bytes([9; 32]),
                })
                .collect(),
        )
        .unwrap()];
        let batch = ArchiveBatch::new(1, TopologyGeneration::new(0), &input).unwrap();
        let request = ArchiveAttemptRequest::new(
            [6; 16],
            [2; 16].try_into().unwrap(),
            [4; 16].try_into().unwrap(),
            [5; 16].try_into().unwrap(),
            7,
            &batch,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(batch.digest()).unwrap(),
            oracle["archive_digest"]
        );
        assert_eq!(
            serde_json::to_value(input[0].digest()).unwrap(),
            oracle["admission_digest"]
        );
        let headers = [PendingBatchHeader::from_batch(&input[0])];
        let literal = oracle["file_hex"].as_str().unwrap();
        let golden: Vec<u8> = (0..literal.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&literal[i..i + 2], 16).unwrap())
            .collect();
        assert_eq!(encode(&request, [7; 32], &headers).unwrap(), golden);
        assert_eq!(decode(&request, [7; 32], &golden).unwrap(), headers);
        for i in 0..golden.len() {
            let mut bad = golden.clone();
            bad[i] ^= 1;
            assert!(
                decode(&request, [7; 32], &bad).is_err(),
                "corruption at {i}"
            );
            assert!(
                decode(&request, [7; 32], &golden[..i]).is_err(),
                "truncation at {i}"
            );
        }
        let mut appended = golden.clone();
        appended.push(0);
        assert!(decode(&request, [7; 32], &appended).is_err());
        assert!(decode(&request, [8; 32], &golden).is_err());
    }
}
