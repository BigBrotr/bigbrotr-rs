//! Fixed-size current-stream metadata observation, never a publication proof.
use super::IngestStore;
use crate::storage::rocks::{
    keys::{IngestKey, KeyDomain},
    manifest::{IngestManifest, ManifestIdentity},
};
use bigbrotr_model::{CompleteThrough, Sequence, SourceRecordId, StreamBound, StreamId};
use rocksdb::{DB, ReadOptions, Snapshot};
use std::{
    fmt,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceHeadError {
    Cancelled,
    WriteUncertain,
    ManifestMismatch,
    MissingManifest,
    MalformedManifest,
    UnsupportedIdentity,
    MissingSource,
    MalformedSource,
    NonemptyZeroSource,
    EngineRead,
}
impl fmt::Display for SourceHeadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "native source-head observation refused: {self:?}")
    }
}
impl std::error::Error for SourceHeadError {}

/// Inert metadata at one native snapshot, including the complete stored identity.
/// This is one stream, not a topology-wide SourceCut or a verified retained prefix.
/// It grants no ownership, publication, acknowledgement or reclamation authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceHead {
    manifest: IngestManifest,
    through: CompleteThrough,
}
impl SourceHead {
    pub const fn manifest(&self) -> &IngestManifest {
        &self.manifest
    }
    pub const fn identity(&self) -> &ManifestIdentity {
        self.manifest.identity()
    }
    pub const fn through(&self) -> CompleteThrough {
        self.through
    }
    pub fn bound(&self) -> StreamBound {
        StreamBound {
            stream: StreamId {
                shard_id: self.identity().shard_id,
                owner_epoch: self.identity().owner_epoch,
            },
            last_complete_sequence: self.through,
        }
    }
}

impl IngestStore {
    /// Observe only the current manifest's source counter. All native reads use
    /// one snapshot, checksums and no cache filling. Pins and the at-most-one
    /// iterator remain local under this store's live native reservation; no
    /// variable application buffer or native handle escapes in the fixed result.
    /// Like other native reads, call on the supervised, joined owner: cancellation
    /// is checked around FFI but cannot interrupt a blocked native operation.
    ///
    /// A missing counter means zero only in the exact fresh schema1 layout.
    /// Historical zero-source recovery needs its separate positive replay proof.
    /// Established nonzero counters can outlive reclaimed facts/pending markers;
    /// this observation deliberately does not certify their retained contents.
    pub fn observe_source_head(
        &self,
        expected: &IngestManifest,
        cancelled: &AtomicBool,
    ) -> Result<SourceHead, SourceHeadError> {
        check_cancel(cancelled)?;
        if self.write_uncertain {
            return Err(SourceHeadError::WriteUncertain);
        }
        if &self.manifest != expected {
            return Err(SourceHeadError::ManifestMismatch);
        }
        let identity = expected.identity();
        if !matches!(
            (expected.key_schema(), identity.owner_epoch.get()),
            (1, 1) | (2, 2) | (3, 3..)
        ) {
            return Err(SourceHeadError::UnsupportedIdentity);
        }
        let snapshot = self.db.snapshot();
        {
            let value = snapshot
                .get_pinned_opt(KeyDomain::Manifest.prefix(), &read_options())
                .map_err(|_| SourceHeadError::EngineRead)?
                .ok_or(SourceHeadError::MissingManifest)?;
            check_cancel(cancelled)?;
            let persisted =
                IngestManifest::decode(&value).map_err(|_| SourceHeadError::MalformedManifest)?;
            if &persisted != expected {
                return Err(SourceHeadError::ManifestMismatch);
            }
        }
        let key = IngestKey::SourceStream {
            dataset_id: identity.dataset_id,
            shard_id: identity.shard_id,
            owner_epoch: identity.owner_epoch,
        }
        .encode()
        .map_err(|_| SourceHeadError::UnsupportedIdentity)?;
        let through = {
            let value = snapshot
                .get_pinned_opt(key.as_bytes(), &read_options())
                .map_err(|_| SourceHeadError::EngineRead)?;
            check_cancel(cancelled)?;
            match value.as_deref() {
                None => {
                    if expected.key_schema() != 1 {
                        return Err(SourceHeadError::MissingSource);
                    }
                    require_fresh(&self.db, &snapshot, cancelled)?;
                    0
                }
                Some(bytes) if bytes.len() == 10 && bytes[..2] == [1, 2] => {
                    // Preserve the full unsigned domain. No next-sequence
                    // addition or narrowing can turn an exhausted head into zero.
                    let head = u64::from_be_bytes(bytes[2..].try_into().expect("checked length"));
                    if head == 0 {
                        if expected.key_schema() == 1 {
                            return Err(SourceHeadError::MalformedSource);
                        }
                        require_empty_stream(&self.db, &snapshot, identity, cancelled)?;
                    }
                    head
                }
                Some(_) => return Err(SourceHeadError::MalformedSource),
            }
        };
        drop(snapshot);
        check_cancel(cancelled)?;
        Ok(SourceHead {
            manifest: *expected,
            through: CompleteThrough::new(through),
        })
    }
}

fn require_fresh(
    db: &DB,
    snapshot: &Snapshot<'_>,
    cancelled: &AtomicBool,
) -> Result<(), SourceHeadError> {
    for name in ["default", "facts", "receipts"] {
        check_cancel(cancelled)?;
        let cf = db.cf_handle(name).ok_or(SourceHeadError::EngineRead)?;
        let mut rows = snapshot.raw_iterator_cf_opt(cf, read_options());
        rows.seek_to_first();
        rows.status().map_err(|_| SourceHeadError::EngineRead)?;
        check_cancel(cancelled)?;
        if name == "default" {
            if rows.key() != Some(KeyDomain::Manifest.prefix().as_slice()) {
                return Err(SourceHeadError::MissingSource);
            }
            rows.next();
            rows.status().map_err(|_| SourceHeadError::EngineRead)?;
            check_cancel(cancelled)?;
        }
        if rows.valid() {
            return Err(SourceHeadError::MissingSource);
        }
    }
    Ok(())
}

fn require_empty_stream(
    db: &DB,
    snapshot: &Snapshot<'_>,
    identity: &ManifestIdentity,
    cancelled: &AtomicBool,
) -> Result<(), SourceHeadError> {
    let id = SourceRecordId {
        dataset_id: identity.dataset_id,
        shard_id: identity.shard_id,
        owner_epoch: identity.owner_epoch,
        sequence: Sequence::new(1).expect("nonzero lower bound"),
    };
    let cf = db.cf_handle("facts").ok_or(SourceHeadError::EngineRead)?;
    for key in [
        IngestKey::Fact {
            source_record_id: id,
        },
        IngestKey::PendingBatch { first: id },
    ] {
        check_cancel(cancelled)?;
        let key = key
            .encode()
            .map_err(|_| SourceHeadError::UnsupportedIdentity)?;
        let prefix = &key.as_bytes()[..key.as_bytes().len() - 8];
        let mut rows = snapshot.raw_iterator_cf_opt(cf, read_options());
        rows.seek(prefix);
        rows.status().map_err(|_| SourceHeadError::EngineRead)?;
        check_cancel(cancelled)?;
        if rows.key().is_some_and(|key| key.starts_with(prefix)) {
            return Err(SourceHeadError::NonemptyZeroSource);
        }
    }
    Ok(())
}

fn check_cancel(cancelled: &AtomicBool) -> Result<(), SourceHeadError> {
    if cancelled.load(Ordering::Acquire) {
        Err(SourceHeadError::Cancelled)
    } else {
        Ok(())
    }
}
fn read_options() -> ReadOptions {
    let mut read = ReadOptions::default();
    read.set_verify_checksums(true);
    read.fill_cache(false);
    read
}
