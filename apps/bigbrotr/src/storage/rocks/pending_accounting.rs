//! Mandatory current-store accounting. This value is mutable metadata, never
//! an original request effect, physical disk grant or scientific publication.
use super::{IngestStore, PendingBatchHeader};
use crate::storage::rocks::{
    keys::{ColumnFamily, IngestKey, KeyDomain, decode_key},
    manifest::IngestManifest,
};
use bigbrotr_model::{OwnerEpoch, Sequence, SourceRecordId};
use minicbor::{Encoder, encode::Write};
use rocksdb::{DB, ReadOptions, Snapshot};
use sha2::{Digest, Sha256};
use std::{convert::Infallible, fmt};

pub const PENDING_ACCOUNTING_BYTES: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingAccountingError {
    Missing,
    Malformed,
    ManifestMismatch,
    SourceMismatch,
    ScienceMismatch,
    ChargeMismatch,
    Overflow,
    EngineRead,
    WriteUncertain,
}
impl fmt::Display for PendingAccountingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pending accounting refused: {self:?}")
    }
}
impl std::error::Error for PendingAccountingError {}
impl From<PendingAccountingError> for super::BootstrapError {
    fn from(error: PendingAccountingError) -> Self {
        match error {
            PendingAccountingError::WriteUncertain => Self::CommitUnknown,
            PendingAccountingError::EngineRead => Self::EngineRead,
            PendingAccountingError::Overflow => Self::Limit,
            _ => Self::StateMismatch,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingAccounting {
    manifest_sha256: [u8; 32],
    pending_bytes: u64,
    source_through: u64,
}
impl PendingAccounting {
    pub(super) fn new(manifest: &IngestManifest, pending_bytes: u64, source_through: u64) -> Self {
        Self {
            manifest_sha256: Sha256::digest(manifest.encode().as_bytes()).into(),
            pending_bytes,
            source_through,
        }
    }
    pub const fn pending_bytes(self) -> u64 {
        self.pending_bytes
    }
    pub const fn source_through(self) -> u64 {
        self.source_through
    }
    pub fn encode(self) -> [u8; PENDING_ACCOUNTING_BYTES] {
        let mut bytes = [0; PENDING_ACCOUNTING_BYTES];
        bytes[..2].copy_from_slice(&[1, 0x0b]);
        bytes[2..34].copy_from_slice(&self.manifest_sha256);
        bytes[34..42].copy_from_slice(&self.pending_bytes.to_be_bytes());
        bytes[42..50].copy_from_slice(&self.source_through.to_be_bytes());
        bytes
    }
    pub fn decode(bytes: &[u8], manifest: &IngestManifest) -> Result<Self, PendingAccountingError> {
        if bytes.len() != PENDING_ACCOUNTING_BYTES || bytes[..2] != [1, 0x0b] {
            return Err(PendingAccountingError::Malformed);
        }
        let value = Self {
            manifest_sha256: bytes[2..34].try_into().expect("fixed counter"),
            pending_bytes: u64::from_be_bytes(bytes[34..42].try_into().expect("fixed counter")),
            source_through: u64::from_be_bytes(bytes[42..50].try_into().expect("fixed counter")),
        };
        if value.manifest_sha256 != Self::new(manifest, 0, 0).manifest_sha256 {
            return Err(PendingAccountingError::ManifestMismatch);
        }
        Ok(value)
    }
    pub(super) fn advance(self, delta: u64, source: u64) -> Result<Self, PendingAccountingError> {
        if source < self.source_through || (delta == 0) != (source == self.source_through) {
            return Err(PendingAccountingError::SourceMismatch);
        }
        Ok(Self {
            pending_bytes: self
                .pending_bytes
                .checked_add(delta)
                .ok_or(PendingAccountingError::Overflow)?,
            source_through: source,
            ..self
        })
    }
}

pub(super) fn charge(
    current: u64,
    key: usize,
    value: usize,
) -> Result<u64, PendingAccountingError> {
    current
        .checked_add(u64::try_from(key).map_err(|_| PendingAccountingError::Overflow)?)
        .and_then(|n| n.checked_add(u64::try_from(value).ok()?))
        .ok_or(PendingAccountingError::Overflow)
}

impl IngestStore {
    /// Reopen has already independently censused all retained streams. The
    /// exclusive handle and its current counter remain mandatory for dispatch.
    pub fn pending_accounting(&self) -> Result<PendingAccounting, PendingAccountingError> {
        self.require_pending_accounting()?;
        Ok(self.accounting)
    }

    pub(super) fn require_pending_accounting(&self) -> Result<(), PendingAccountingError> {
        let snapshot = self.db.snapshot();
        self.require_pending_accounting_at(&snapshot)
    }

    pub(super) fn require_pending_accounting_at(
        &self,
        snapshot: &Snapshot<'_>,
    ) -> Result<(), PendingAccountingError> {
        if self.write_uncertain {
            return Err(PendingAccountingError::WriteUncertain);
        }
        let manifest = snapshot
            .get_pinned_opt(KeyDomain::Manifest.prefix(), read_options())
            .map_err(|_| PendingAccountingError::EngineRead)?
            .ok_or(PendingAccountingError::ManifestMismatch)?;
        if manifest.as_ref() != self.manifest.encode().as_bytes() {
            return Err(PendingAccountingError::ManifestMismatch);
        }
        drop(manifest);
        let current = read_counter(snapshot, &self.manifest)?;
        if current != self.accounting {
            return Err(PendingAccountingError::ChargeMismatch);
        }
        require_source(snapshot, &self.manifest, current.source_through)?;
        Ok(())
    }
}

fn read_options() -> ReadOptions {
    let mut options = ReadOptions::default();
    options.set_verify_checksums(true);
    options.fill_cache(false);
    options
}
fn read_counter(
    snapshot: &Snapshot<'_>,
    manifest: &IngestManifest,
) -> Result<PendingAccounting, PendingAccountingError> {
    let bytes = snapshot
        .get_pinned_opt(KeyDomain::PendingAccounting.prefix(), read_options())
        .map_err(|_| PendingAccountingError::EngineRead)?
        .ok_or(PendingAccountingError::Missing)?;
    PendingAccounting::decode(&bytes, manifest)
}
fn source_key(manifest: &IngestManifest) -> IngestKey {
    let id = manifest.identity();
    IngestKey::SourceStream {
        dataset_id: id.dataset_id,
        shard_id: id.shard_id,
        owner_epoch: id.owner_epoch,
    }
}
fn require_source(
    snapshot: &Snapshot<'_>,
    manifest: &IngestManifest,
    wanted: u64,
) -> Result<(), PendingAccountingError> {
    let key = source_key(manifest)
        .encode()
        .map_err(|_| PendingAccountingError::SourceMismatch)?;
    let value = snapshot
        .get_pinned_opt(key.as_bytes(), read_options())
        .map_err(|_| PendingAccountingError::EngineRead)?;
    match value.as_deref() {
        Some(bytes)
            if bytes.len() == 10
                && bytes[..2] == [1, 2]
                && u64::from_be_bytes(bytes[2..].try_into().expect("fixed source")) == wanted =>
        {
            Ok(())
        }
        _ => Err(PendingAccountingError::SourceMismatch),
    }
}

/// No variable application buffers or decoded Facts are allocated. The snapshot
/// pins/iterators remain inside the already retained native reservation. Hashes
/// and canonical source coordinates are checked separately from later semantic
/// original-input replay. Every stream is scanned, including ACKed/reclaimed
/// request history; receipt floors never change the charged scientific range.
pub(super) fn census(
    db: &DB,
    manifest: &IngestManifest,
) -> Result<PendingAccounting, PendingAccountingError> {
    let snapshot = db.snapshot();
    let counter = read_counter(&snapshot, manifest)?;
    require_source(&snapshot, manifest, counter.source_through)?;
    let mut direct_charge = 0;
    let mut stream_charge = 0u64;
    let mut current_source = false;
    for cf in [
        ColumnFamily::Default,
        ColumnFamily::Facts,
        ColumnFamily::Receipts,
    ] {
        let handle = db
            .cf_handle(cf.name())
            .ok_or(PendingAccountingError::EngineRead)?;
        let mut cursor = snapshot.raw_iterator_cf_opt(handle, read_options());
        cursor.seek_to_first();
        while cursor.valid() {
            let key = cursor
                .key()
                .ok_or(PendingAccountingError::ScienceMismatch)?;
            let value = cursor
                .value()
                .ok_or(PendingAccountingError::ScienceMismatch)?;
            let decoded =
                decode_key(cf, key).map_err(|_| PendingAccountingError::ScienceMismatch)?;
            match decoded {
                IngestKey::Manifest if value != manifest.encode().as_bytes() => {
                    return Err(PendingAccountingError::ManifestMismatch);
                }
                IngestKey::PendingAccounting => {
                    PendingAccounting::decode(value, manifest)?;
                }
                IngestKey::SourceStream {
                    dataset_id,
                    shard_id,
                    owner_epoch,
                } => {
                    let id = manifest.identity();
                    if dataset_id != id.dataset_id
                        || shard_id != id.shard_id
                        || owner_epoch > id.owner_epoch
                        || value.len() != 10
                        || value[..2] != [1, 2]
                    {
                        return Err(PendingAccountingError::SourceMismatch);
                    }
                    let through = u64::from_be_bytes(value[2..].try_into().expect("fixed source"));
                    current_source |= decoded == source_key(manifest);
                    stream_charge = stream_charge
                        .checked_add(scan_stream(db, &snapshot, manifest, owner_epoch, through)?)
                        .ok_or(PendingAccountingError::Overflow)?;
                }
                IngestKey::Fact { .. } | IngestKey::PendingBatch { .. } => {
                    direct_charge = charge(direct_charge, key.len(), value.len())?;
                }
                _ => {}
            }
            cursor.next();
        }
        cursor
            .status()
            .map_err(|_| PendingAccountingError::EngineRead)?;
    }
    if !current_source {
        return Err(PendingAccountingError::SourceMismatch);
    }
    if direct_charge != stream_charge || direct_charge != counter.pending_bytes {
        return Err(PendingAccountingError::ChargeMismatch);
    }
    Ok(counter)
}

fn scan_stream(
    db: &DB,
    snapshot: &Snapshot<'_>,
    manifest: &IngestManifest,
    epoch: OwnerEpoch,
    through: u64,
) -> Result<u64, PendingAccountingError> {
    let cf = db
        .cf_handle("facts")
        .ok_or(PendingAccountingError::EngineRead)?;
    let first = SourceRecordId {
        dataset_id: manifest.identity().dataset_id,
        shard_id: manifest.identity().shard_id,
        owner_epoch: epoch,
        sequence: Sequence::new(1).expect("nonzero lower bound"),
    };
    let mut total = 0;
    let mut complete = 0_u64;
    for domain in [KeyDomain::Fact, KeyDomain::PendingBatch] {
        let key = match domain {
            KeyDomain::Fact => IngestKey::Fact {
                source_record_id: first,
            },
            _ => IngestKey::PendingBatch { first },
        }
        .encode()
        .map_err(|_| PendingAccountingError::ScienceMismatch)?;
        let prefix = &key.as_bytes()[..key.as_bytes().len() - 8];
        let mut cursor = snapshot.raw_iterator_cf_opt(cf, read_options());
        cursor.seek(prefix);
        while cursor.key().is_some_and(|bytes| bytes.starts_with(prefix)) {
            let key = cursor
                .key()
                .ok_or(PendingAccountingError::ScienceMismatch)?;
            let value = cursor
                .value()
                .ok_or(PendingAccountingError::ScienceMismatch)?;
            let decoded = decode_key(ColumnFamily::Facts, key)
                .map_err(|_| PendingAccountingError::ScienceMismatch)?;
            let source = match decoded {
                IngestKey::Fact { source_record_id } => source_record_id,
                IngestKey::PendingBatch { first } => first,
                _ => return Err(PendingAccountingError::ScienceMismatch),
            };
            if complete.checked_add(1) != Some(source.sequence.get()) {
                return Err(PendingAccountingError::ScienceMismatch);
            }
            complete = if domain == KeyDomain::Fact {
                require_fact(source, value)?;
                source.sequence.get()
            } else {
                let header = PendingBatchHeader::decode(source, value)
                    .map_err(|_| PendingAccountingError::ScienceMismatch)?;
                if header.last().sequence.get() > through {
                    return Err(PendingAccountingError::ScienceMismatch);
                }
                require_membership(snapshot, cf, header)?;
                header.last().sequence.get()
            };
            if complete > through {
                return Err(PendingAccountingError::SourceMismatch);
            }
            total = charge(total, key.len(), value.len())?;
            cursor.next();
        }
        cursor
            .status()
            .map_err(|_| PendingAccountingError::EngineRead)?;
        if complete != through {
            return Err(PendingAccountingError::ScienceMismatch);
        }
        complete = 0;
    }
    Ok(total)
}

fn require_fact(source: SourceRecordId, bytes: &[u8]) -> Result<(), PendingAccountingError> {
    let prefix = b"bigbrotr/fact/1\0";
    let hash: [u8; 32] = Sha256::digest(bytes.get(34..).unwrap_or_default()).into();
    // Every current producer's complete serialized Prepared is at most16MiB;
    // one stored framed value cannot exceed that existing hard batch bound.
    if bytes.len() > 16 * 1024 * 1024
        || bytes.len() < 34 + prefix.len()
        || bytes[..2] != [1, 0x20]
        || hash != bytes[2..34]
        || !bytes[34..].starts_with(prefix)
    {
        return Err(PendingAccountingError::ScienceMismatch);
    }
    let mut decoder = minicbor::Decoder::new(&bytes[34 + prefix.len()..]);
    let matches = (|| -> Result<bool, minicbor::decode::Error> {
        Ok(decoder.array()? == Some(6)
            && decoder.array()? == Some(4)
            && decoder.bytes()? == source.dataset_id.as_bytes()
            && decoder.u32()? == source.shard_id
            && decoder.u64()? == source.owner_epoch.get()
            && decoder.u64()? == source.sequence.get())
    })()
    .map_err(|_| PendingAccountingError::ScienceMismatch)?;
    if matches {
        Ok(())
    } else {
        Err(PendingAccountingError::ScienceMismatch)
    }
}

struct HashWriter(Sha256);
impl Write for HashWriter {
    type Error = Infallible;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Infallible> {
        self.0.update(bytes);
        Ok(())
    }
}
fn require_membership(
    snapshot: &Snapshot<'_>,
    cf: &rocksdb::ColumnFamily,
    header: PendingBatchHeader,
) -> Result<(), PendingAccountingError> {
    let first = header.first();
    let last = header.last().sequence.get();
    let count = last
        .checked_sub(first.sequence.get())
        .and_then(|n| n.checked_add(1))
        .ok_or(PendingAccountingError::Overflow)?;
    let mut hash = Sha256::new();
    hash.update(b"bigbrotr/admission-batch/1\0");
    let mut encoder = Encoder::new(HashWriter(hash));
    encoder
        .array(6)
        .and_then(|e| e.bytes(first.dataset_id.as_bytes()))
        .and_then(|e| e.u32(first.shard_id))
        .and_then(|e| e.u64(first.owner_epoch.get()))
        .and_then(|e| e.u64(first.sequence.get()))
        .and_then(|e| e.u64(last))
        .and_then(|e| e.array(count))
        .expect("infallible digest writer");
    for sequence in first.sequence.get()..=last {
        let source = SourceRecordId {
            sequence: Sequence::new(sequence).expect("positive member"),
            ..first
        };
        let key = IngestKey::Fact {
            source_record_id: source,
        }
        .encode()
        .map_err(|_| PendingAccountingError::ScienceMismatch)?;
        let bytes = snapshot
            .get_pinned_cf_opt(cf, key.as_bytes(), read_options())
            .map_err(|_| PendingAccountingError::EngineRead)?
            .ok_or(PendingAccountingError::ScienceMismatch)?;
        require_fact(source, &bytes)?;
        encoder
            .array(2)
            .and_then(|e| e.array(4))
            .and_then(|e| e.bytes(source.dataset_id.as_bytes()))
            .and_then(|e| e.u32(source.shard_id))
            .and_then(|e| e.u64(source.owner_epoch.get()))
            .and_then(|e| e.u64(sequence))
            .and_then(|e| e.bytes(&bytes[2..34]))
            .expect("infallible digest writer");
    }
    let digest: [u8; 32] = encoder.into_writer().0.finalize().into();
    if &digest != header.digest().as_bytes() {
        return Err(PendingAccountingError::ScienceMismatch);
    }
    Ok(())
}
