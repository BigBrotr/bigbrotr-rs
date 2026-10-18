//! Immutable local operation identities for resolving lost frontier replies.
//! This journal supplies no catalog observation, current Control, or frontier
//! authority. Every original operation must be looked up by the fenced caller.
use super::{LifetimeError, LocalCoordinator, ObservedCut, lifetime};
use crate::{runtime::OwnedPermit, storage::postgres::archive_frontier::ArchiveFrontierReceipt};
use bigbrotr_model::{DatasetId, Digest256, OwnerResourceId};
use bigbrotr_protocol::archive_cut::{CutObservation, CutRead, FORMAT};
use rustix::fs::{Dir, Mode};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Write},
    sync::atomic::{AtomicBool, Ordering},
};

const MAGIC: &[u8; 8] = b"BBFRONT\x01";
const DOMAIN: &[u8] = b"bigbrotr/archive-frontier-intent/1\0";
const PREFIX: usize = 180;
const MAX_BYTES: usize = 64 * 1024;
const MAX_INTENTS: usize = 256;
const MAX_ENTRIES: usize = 1024;
const NAME_PREFIX: &str = "frontier-";
const NAME_SUFFIX: &str = ".intent";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierIntentError {
    Custody(LifetimeError),
    Identity,
    Incomplete,
    ResourceLimit,
    Cancelled,
    Unavailable,
}
impl From<LifetimeError> for FrontierIntentError {
    fn from(value: LifetimeError) -> Self {
        Self::Custody(value)
    }
}
type Result<T> = std::result::Result<T, FrontierIntentError>;

/// A fixed complete name census, not proof that any operation committed. The
/// scan owns actual directory/coordinator descriptors and a separate allocation.
pub struct FrontierIntentScan {
    ids: Vec<[u8; 16]>,
    entries: usize,
    next: usize,
    available: bool,
    directory: File,
    binding: Digest256,
    dataset: DatasetId,
    catalog: OwnerResourceId,
    _custody: File,
    _permit: OwnedPermit,
}
/// One synced immutable record. Its read/counts are original declarations and
/// observations only: they cannot manufacture ObservedCut or a live capture.
pub struct RetainedFrontierIntent {
    data: Data,
    _custody: File,
    _permit: OwnedPermit,
}
struct Data {
    binding: Digest256,
    dataset: DatasetId,
    catalog: OwnerResourceId,
    operation: [u8; 16],
    expected_revision: u64,
    observation: CutObservation,
    read: CutRead,
}
impl RetainedFrontierIntent {
    /// Two <=64KiB serialized/read buffers, parser scratch, bounded typed vectors
    /// and validation overlap fit this conservative allowance. No native budget.
    pub const fn required_data_bytes() -> u64 {
        1024 * 1024
    }
    pub fn operation_id(&self) -> [u8; 16] {
        self.data.operation
    }
    pub fn expected_revision(&self) -> u64 {
        self.data.expected_revision
    }
    pub fn coordinator_binding(&self) -> Digest256 {
        self.data.binding
    }
    pub fn catalog(&self) -> OwnerResourceId {
        self.data.catalog
    }
    pub fn read(&self) -> &CutRead {
        &self.data.read
    }
    pub fn observation(&self) -> &CutObservation {
        &self.data.observation
    }
    /// Compare a separately returned current-Control lookup. This predicate
    /// neither performs lookup nor converts absence into fresh-work authority.
    pub fn matches_receipt(&self, receipt: &ArchiveFrontierReceipt) -> bool {
        receipt.operation_id == self.data.operation
            && receipt.consumer_id == self.data.catalog
            && self.data.expected_revision.checked_add(1) == Some(receipt.revision)
            && receipt.snapshot_id == self.data.observation.snapshot_id
            && receipt.request_digest == self.data.observation.request_digest
            && receipt.family_counts == self.data.observation.family_counts
    }
}
impl FrontierIntentScan {
    pub const fn required_data_bytes() -> u64 {
        64 * 1024
    }
    pub fn len(&self) -> usize {
        self.ids.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
    pub fn remaining_slots(&self) -> usize {
        (MAX_INTENTS - self.ids.len()).min(self.remaining_directory_entries())
    }
    /// Capacity observed by this census, not a reservation. Include new input
    /// and attempt entries as well as intents in whole-job admission; append
    /// repeats the complete census immediately before creating its own record.
    pub fn remaining_directory_entries(&self) -> usize {
        MAX_ENTRIES - self.entries
    }
    pub fn observed_through(&self) -> usize {
        self.next
    }
    /// Load and validate serially. Any error poisons this cursor; a fresh census
    /// may retry, but may never skip the failed original operation.
    pub fn read_next(
        &mut self,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Option<RetainedFrontierIntent>> {
        if !self.available {
            return Err(FrontierIntentError::Unavailable);
        }
        self.available = false;
        check_grant(
            &permit,
            RetainedFrontierIntent::required_data_bytes(),
            cancelled,
        )?;
        let result = if let Some(id) = self.ids.get(self.next) {
            let (file, bytes) = read_record(&self.directory, *id)?;
            let data = decode(&bytes, self.binding, self.dataset, self.catalog, *id)?;
            let custody = self
                ._custody
                .try_clone()
                .map_err(|_| LifetimeError::Filesystem)?;
            file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
            self.directory
                .sync_all()
                .map_err(|_| LifetimeError::Filesystem)?;
            check_cancel(cancelled)?;
            self.next += 1;
            Some(RetainedFrontierIntent {
                data,
                _custody: custody,
                _permit: permit,
            })
        } else {
            check_cancel(cancelled)?;
            None
        };
        self.available = true;
        Ok(result)
    }
}
impl LocalCoordinator {
    /// Census every reserved journal name, including completed/absent/failed
    /// operations. Invalid metadata/version/checksum and partial bytes refuse.
    /// All other non-dot directory entries also count toward the finite1024 cap.
    pub fn frontier_intents(
        &self,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<FrontierIntentScan> {
        check_grant(
            &permit,
            FrontierIntentScan::required_data_bytes(),
            cancelled,
        )?;
        let (ids, entries) = inventory(
            &self.directory,
            self.binding_digest(),
            self.identity().dataset(),
            self.identity().catalog(),
            cancelled,
        )?;
        let directory = self
            .directory
            .try_clone()
            .map_err(|_| LifetimeError::Filesystem)?;
        let custody = self.custody.retain_custody()?;
        Ok(FrontierIntentScan {
            ids,
            entries,
            next: 0,
            available: true,
            directory,
            binding: self.binding_digest(),
            dataset: self.identity().dataset(),
            catalog: self.identity().catalog(),
            _custody: custody,
            _permit: permit,
        })
    }
    /// Sync the exact identity BEFORE sending a frontier transaction. The caller
    /// owns current authority, the live capture, original-input validation and
    /// the actual driver. Complete identical retries only; never repair bytes.
    pub fn retain_frontier_intent(
        &self,
        operation: [u8; 16],
        expected_revision: u64,
        observed: &ObservedCut,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<RetainedFrontierIntent> {
        check_grant(
            &permit,
            RetainedFrontierIntent::required_data_bytes(),
            cancelled,
        )?;
        check_layout()?;
        if observed.catalog() != self.identity() {
            return Err(FrontierIntentError::Identity);
        }
        // Validate the already-owned input before cloning it: the protocol's
        // 1024 total ranges/bounds and 128 receipts bound the owned clone.
        observed
            .observation()
            .validate(observed.read(), usize::MAX)
            .map_err(|_| FrontierIntentError::Identity)?;
        let data = Data {
            binding: self.binding_digest(),
            dataset: self.identity().dataset(),
            catalog: self.identity().catalog(),
            operation,
            expected_revision,
            observation: observed.observation().clone(),
            read: observed.read().clone(),
        };
        self.store_frontier_intent(data, permit, cancelled)
    }
    fn store_frontier_intent(
        &self,
        data: Data,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<RetainedFrontierIntent> {
        check_grant(
            &permit,
            RetainedFrontierIntent::required_data_bytes(),
            cancelled,
        )?;
        if data.binding != self.binding_digest()
            || data.dataset != self.identity().dataset()
            || data.catalog != self.identity().catalog()
        {
            return Err(FrontierIntentError::Identity);
        }
        validate(&data)?;
        let bytes = encode(&data)?;
        // Re-census immediately before any create, not a cached free-slot count.
        // Decode all originals serially so malformed retained JSON cannot be
        // bypassed by asking for a different random operation identity.
        let (ids, entries) = inventory(
            &self.directory,
            data.binding,
            data.dataset,
            data.catalog,
            cancelled,
        )?;
        let mut existing = false;
        for id in &ids {
            check_cancel(cancelled)?;
            let (file, prior) = read_record(&self.directory, *id)?;
            let _checked = decode(&prior, data.binding, data.dataset, data.catalog, *id)?;
            if id == &data.operation {
                if prior != bytes {
                    return Err(FrontierIntentError::Identity);
                }
                file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
                existing = true;
            }
        }
        if !existing {
            if ids.len() >= MAX_INTENTS || entries >= MAX_ENTRIES {
                return Err(FrontierIntentError::ResourceLimit);
            }
            check_cancel(cancelled)?;
            let mut file = lifetime::create_file(&self.directory, &name(data.operation))?;
            rustix::fs::fchmod(&file, Mode::RUSR | Mode::WUSR)
                .map_err(|_| LifetimeError::Filesystem)?;
            if !lifetime::private_file(&file.metadata().map_err(|_| LifetimeError::Filesystem)?) {
                return Err(FrontierIntentError::Identity);
            }
            // Write/sync errors leave the original file untouched for inspection.
            // An error after complete sync is still not permission to change ID.
            file.write_all(&bytes)
                .map_err(|_| LifetimeError::Filesystem)?;
            file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        }
        self.directory
            .sync_all()
            .map_err(|_| LifetimeError::Filesystem)?;
        check_cancel(cancelled)?;
        Ok(RetainedFrontierIntent {
            data,
            _custody: self.custody.retain_custody()?,
            _permit: permit,
        })
    }
}
fn check_cancel(cancelled: &AtomicBool) -> Result<()> {
    if cancelled.load(Ordering::Relaxed) {
        Err(FrontierIntentError::Cancelled)
    } else {
        Ok(())
    }
}
fn check_grant(permit: &OwnedPermit, bytes: u64, cancelled: &AtomicBool) -> Result<()> {
    if permit.request().units().data_bytes() < bytes || permit.request().units().items() == 0 {
        return Err(FrontierIntentError::ResourceLimit);
    }
    check_cancel(cancelled)
}
fn check_layout() -> Result<()> {
    // Explicit scalar-size ceilings keep the documented allocation bound honest
    // if these protocol structures grow in a later format change. In <=64KiB
    // typed JSON, each completed bound/range has >=40 bytes and each receipt
    // >=160 bytes excluding ranges; Vec growth is at most 2x. Thus one decoded
    // cut stays below 256KiB including vectors/headers and a partial element.
    // Two cuts, three raw 64KiB buffers, parser scratch 64KiB and the <=8KiB
    // validation index fit in 1MiB. Field/identifier scratch is also bounded by
    // the input length; no untrusted count is used to reserve a collection.
    use bigbrotr_protocol::{
        archive_cut::CutBound,
        archive_receipt::{ReceiptExpectation, ReceiptRange},
    };
    if std::mem::size_of::<CutBound>() > 32
        || std::mem::size_of::<ReceiptRange>() > 40
        || std::mem::size_of::<ReceiptExpectation>() > 192
        || std::mem::size_of::<CutRead>() > 128
    {
        return Err(FrontierIntentError::ResourceLimit);
    }
    Ok(())
}
fn validate(data: &Data) -> Result<()> {
    check_layout()?;
    if data.operation == [0; 16]
        || data.expected_revision == u64::MAX
        || data.read.dataset_id != data.dataset
        || data.read.snapshot_id != Some(data.observation.snapshot_id)
        || data.observation.snapshot_id == 0
    {
        return Err(FrontierIntentError::Identity);
    }
    data.observation
        .validate(&data.read, usize::MAX)
        .map_err(|_| FrontierIntentError::Identity)
}
fn name(id: [u8; 16]) -> String {
    use std::fmt::Write;
    let mut s = String::from(NAME_PREFIX);
    for b in id {
        write!(&mut s, "{b:02x}").expect("String write");
    }
    s.push_str(NAME_SUFFIX);
    s
}
fn parse_name(bytes: &[u8]) -> Result<Option<[u8; 16]>> {
    if !bytes.starts_with(NAME_PREFIX.as_bytes()) {
        return Ok(None);
    }
    let s = std::str::from_utf8(bytes).map_err(|_| FrontierIntentError::Identity)?;
    let hex = s
        .strip_prefix(NAME_PREFIX)
        .and_then(|s| s.strip_suffix(NAME_SUFFIX))
        .filter(|s| s.len() == 32)
        .ok_or(FrontierIntentError::Identity)?;
    if !hex
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(FrontierIntentError::Identity);
    }
    let mut id = [0; 16];
    for (i, b) in id.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| FrontierIntentError::Identity)?;
    }
    if id == [0; 16] {
        return Err(FrontierIntentError::Identity);
    }
    Ok(Some(id))
}
fn inventory(
    directory: &File,
    binding: Digest256,
    dataset: DatasetId,
    catalog: OwnerResourceId,
    cancelled: &AtomicBool,
) -> Result<(Vec<[u8; 16]>, usize)> {
    let mut ids = Vec::new();
    ids.try_reserve_exact(MAX_INTENTS)
        .map_err(|_| FrontierIntentError::ResourceLimit)?;
    let mut count = 0;
    for entry in Dir::read_from(directory).map_err(|_| LifetimeError::Filesystem)? {
        check_cancel(cancelled)?;
        let entry = entry.map_err(|_| LifetimeError::Filesystem)?;
        let bytes = entry.file_name().to_bytes();
        if bytes == b"." || bytes == b".." {
            continue;
        }
        count += 1;
        if count > MAX_ENTRIES {
            return Err(FrontierIntentError::ResourceLimit);
        }
        if let Some(id) = parse_name(bytes)? {
            if ids.len() == MAX_INTENTS {
                return Err(FrontierIntentError::ResourceLimit);
            }
            let mut file = open_record(directory, id)?;
            let mut header = [0; PREFIX];
            file.read_exact(&mut header)
                .map_err(|_| FrontierIntentError::Incomplete)?;
            let length = check_header(&header, binding, dataset, catalog, id)?;
            if file
                .metadata()
                .map_err(|_| LifetimeError::Filesystem)?
                .len()
                != (PREFIX + length + 32) as u64
            {
                return Err(FrontierIntentError::Incomplete);
            }
            let mut hash = Sha256::new();
            hash.update(DOMAIN);
            hash.update(header);
            let mut left = length;
            let mut buffer = [0; 4096];
            while left > 0 {
                check_cancel(cancelled)?;
                let n = left.min(buffer.len());
                file.read_exact(&mut buffer[..n])
                    .map_err(|_| FrontierIntentError::Incomplete)?;
                hash.update(&buffer[..n]);
                left -= n;
            }
            let mut checksum = [0; 32];
            file.read_exact(&mut checksum)
                .map_err(|_| FrontierIntentError::Incomplete)?;
            if hash.finalize()[..] != checksum {
                return Err(FrontierIntentError::Identity);
            }
            if file
                .read(&mut buffer[..1])
                .map_err(|_| LifetimeError::Filesystem)?
                != 0
            {
                return Err(FrontierIntentError::Incomplete);
            }
            ids.push(id);
        }
    }
    ids.sort_unstable();
    if ids.windows(2).any(|w| w[0] == w[1]) {
        return Err(FrontierIntentError::Identity);
    }
    check_cancel(cancelled)?;
    Ok((ids, count))
}
fn open_record(directory: &File, id: [u8; 16]) -> Result<File> {
    let file = lifetime::open_file(directory, &name(id))?;
    let m = file.metadata().map_err(|_| LifetimeError::Filesystem)?;
    if !lifetime::private_file(&m) || m.len() < (PREFIX + 34) as u64 {
        return Err(FrontierIntentError::Incomplete);
    }
    if m.len() > MAX_BYTES as u64 {
        return Err(FrontierIntentError::ResourceLimit);
    }
    Ok(file)
}
fn read_record(directory: &File, id: [u8; 16]) -> Result<(File, Vec<u8>)> {
    let mut file = open_record(directory, id)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(MAX_BYTES + 1)
        .map_err(|_| FrontierIntentError::ResourceLimit)?;
    (&mut file)
        .take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| LifetimeError::Filesystem)?;
    if bytes.len() > MAX_BYTES {
        return Err(FrontierIntentError::ResourceLimit);
    }
    Ok((file, bytes))
}
fn encode(data: &Data) -> Result<Vec<u8>> {
    validate(data)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(MAX_BYTES)
        .map_err(|_| FrontierIntentError::ResourceLimit)?;
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(data.binding.as_bytes());
    bytes.extend_from_slice(data.dataset.as_bytes());
    bytes.extend_from_slice(data.catalog.as_bytes());
    bytes.extend_from_slice(&data.operation);
    bytes.extend_from_slice(&data.expected_revision.to_be_bytes());
    bytes.extend_from_slice(&data.observation.snapshot_id.to_be_bytes());
    bytes.extend_from_slice(data.observation.request_digest.as_bytes());
    for n in data.observation.family_counts {
        bytes.extend_from_slice(&n.to_be_bytes());
    }
    bytes.extend_from_slice(&[0; 4]);
    struct Bounded<'a>(&'a mut Vec<u8>);
    impl Write for Bounded<'_> {
        fn write(&mut self, part: &[u8]) -> std::io::Result<usize> {
            if self
                .0
                .len()
                .checked_add(part.len())
                .is_none_or(|n| n > MAX_BYTES - 32)
            {
                return Err(std::io::Error::other("frontier intent limit"));
            }
            self.0.extend_from_slice(part);
            Ok(part.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Bounded(&mut bytes), &data.read)
        .map_err(|_| FrontierIntentError::ResourceLimit)?;
    let n = (bytes.len() - PREFIX) as u32;
    bytes[PREFIX - 4..PREFIX].copy_from_slice(&n.to_be_bytes());
    let checksum = Sha256::new()
        .chain_update(DOMAIN)
        .chain_update(&bytes)
        .finalize();
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}
fn check_header(
    bytes: &[u8],
    binding: Digest256,
    dataset: DatasetId,
    catalog: OwnerResourceId,
    id: [u8; 16],
) -> Result<usize> {
    if bytes.len() < PREFIX
        || &bytes[..8] != MAGIC
        || bytes[8..40] != *binding.as_bytes()
        || bytes[40..56] != *dataset.as_bytes()
        || bytes[56..72] != *catalog.as_bytes()
        || bytes[72..88] != id
        || id == [0; 16]
    {
        return Err(FrontierIntentError::Identity);
    }
    let n = u32::from_be_bytes(bytes[176..180].try_into().expect("header")) as usize;
    if n < 2 || n > MAX_BYTES - PREFIX - 32 {
        return Err(FrontierIntentError::ResourceLimit);
    }
    Ok(n)
}
fn decode(
    bytes: &[u8],
    binding: Digest256,
    dataset: DatasetId,
    catalog: OwnerResourceId,
    id: [u8; 16],
) -> Result<Data> {
    let n = check_header(bytes, binding, dataset, catalog, id)?;
    if bytes.len() != PREFIX + n + 32 {
        return Err(FrontierIntentError::Incomplete);
    }
    let checksum = Sha256::new()
        .chain_update(DOMAIN)
        .chain_update(&bytes[..PREFIX + n])
        .finalize();
    if checksum[..] != bytes[PREFIX + n..] {
        return Err(FrontierIntentError::Identity);
    }
    let number = |offset| u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("header"));
    check_layout()?;
    let data = Data {
        binding,
        dataset,
        catalog,
        operation: id,
        expected_revision: number(88),
        observation: CutObservation {
            format: FORMAT.into(),
            snapshot_id: number(96),
            request_digest: Digest256::from_bytes(bytes[104..136].try_into().expect("header")),
            family_counts: std::array::from_fn(|i| number(136 + 8 * i)),
        },
        read: serde_json::from_slice(&bytes[PREFIX..PREFIX + n])
            .map_err(|_| FrontierIntentError::Identity)?,
    };
    validate(&data)?;
    if encode(&data)? != bytes {
        return Err(FrontierIntentError::Identity);
    }
    Ok(data)
}

#[cfg(test)]
pub(super) mod tests;
