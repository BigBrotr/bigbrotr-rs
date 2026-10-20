//! The private native ingest directory owner; opening is not an authority grant.
use super::{
    keys::KeyDomain,
    manifest::{IngestManifest, ManifestError},
    options::{
        INGEST_COLUMN_FAMILIES, RocksNativeLimits, RocksNativeProfile, RocksNativeResources,
        RocksNativeUsage,
    },
};
use crate::runtime::OwnedPermit;
use bigbrotr_model::DatasetId;
use rocksdb::{DB, ReadOptions, WriteBatch, WriteOptions};
use rustix::fs::{Mode, OFlags};
use std::{
    collections::BTreeSet,
    fmt, fs,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Mutex,
};

#[path = "pending.rs"]
mod pending;
pub use pending::{PendingBatch, PendingBatchHeader, PendingError, PendingReadLimits};

#[path = "receipts.rs"]
mod receipts;
pub use receipts::{ReceiptReadError, StoredReceipt, required_receipt_data_bytes};

#[path = "outcome_copy.rs"]
mod outcome_copy;
pub use outcome_copy::NativeOutcomeKind;
pub(in crate::storage) use outcome_copy::{MAX_OUTCOME_BYTES, NativeOutcomeCopy};

#[path = "request_inputs.rs"]
mod request_inputs;
pub use request_inputs::{RequestInputError, RetainedRequestInput};
pub(in crate::storage) use request_inputs::{StagedProducerRequest, SuccessorStagedInput};

#[path = "bootstrap.rs"]
mod bootstrap;
pub use bootstrap::{BootstrapError, BootstrapLimits};

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod bootstrap_tests;

#[cfg(test)]
#[path = "request_input_tests.rs"]
mod request_input_tests;

#[cfg(test)]
#[path = "wal_recovery_tests.rs"]
mod wal_recovery_tests;

#[path = "admission.rs"]
mod admission;
#[path = "dictionary_append.rs"]
mod dictionary_append;
#[path = "dictionary_preparation.rs"]
mod dictionary_preparation;
pub use dictionary_preparation::{DictionaryPreparation, PreparedDictionaries};
#[cfg(test)]
#[path = "dictionary_append_tests.rs"]
mod dictionary_append_tests;
#[path = "receipt_effects.rs"]
mod receipt_effects;
pub use admission::{
    AdmissionError, AdmissionLimits, AdmissionOutcome, PreparedEvent, RejectionReason,
    StoredOutcome,
};

#[path = "certification.rs"]
mod certification;
pub use certification::{
    CertificationLimits, CertificationOutcome, PreparedCertification, StoredCertification,
};

#[path = "producer_checkpoint.rs"]
mod producer_checkpoint;
pub(in crate::storage) use producer_checkpoint::{CheckpointKind, NativeCheckpointBasis};

#[path = "visibility.rs"]
mod visibility;
pub use visibility::StoredVisibility;

#[path = "observation.rs"]
mod observation;

#[cfg(test)]
#[path = "visibility_tests.rs"]
mod visibility_tests;

#[path = "producer_closing.rs"]
mod producer_closing;
pub use producer_closing::ProducerClosing;

#[path = "producer_recovery.rs"]
mod producer_recovery;
pub use producer_recovery::{
    ProducerRecoveryError, ProducerRecoveryLimits, ResolvedProducerRequest,
};

#[path = "source_closure.rs"]
mod source_closure;
pub use source_closure::{
    ActivatedSourceResolution, ResolvedActivatedSource, ResolvedSource, SourceResolution,
};

#[path = "initial_handoff.rs"]
mod initial_handoff;
pub(in crate::storage) use initial_handoff::{
    ActivatedInitial, ActivatedProducerClosing, EmptyInitialAttempt, HistoricalInitial,
    InitialHandoffRecord,
};
pub use initial_handoff::{
    EmptyInitialRetirement, EncodedHandoff, InitialHandoff, ObservedEmptyInitialRetirement,
    ObservedHandoff,
};

#[path = "initial_writer.rs"]
mod initial_writer;
pub use initial_writer::{ActiveInitialWriter, InitialWriterCandidate};

#[path = "lineage.rs"]
mod lineage;
pub(in crate::storage) use lineage::{
    NativeSuccessorRecovery, NativeSuccessorRetirement, ResolvedSuccessorRecovery,
    ResolvedSuccessorRetirement, SuccessorRecoveryResolution, SuccessorRetirementResolution,
};
#[cfg(test)]
use lineage::{PendingSuccessorRequest, PendingSuccessorRetirementRequest};
pub use lineage::{RetainedLineage, RetainedLineageEntry, RetainedTerminalKind};

#[path = "lineage_replay.rs"]
mod lineage_replay;
pub use lineage_replay::{
    LineageEpochSummary, LineagePendingError, LineageResolution, ResolvedLineage,
};

#[path = "lineage_effects.rs"]
mod lineage_effects;

#[path = "successor_handoff.rs"]
mod successor_handoff;
pub use successor_handoff::{ObservedSuccessorHandoff, SuccessorCandidate, SuccessorHandoff};

#[path = "successor_writer.rs"]
mod successor_writer;
pub use successor_writer::ActiveSuccessorWriter;
pub(in crate::storage) use successor_writer::NativeSuccessorActivation;

#[path = "successor_empty.rs"]
mod successor_empty;
pub use successor_empty::{EmptySuccessorRetirement, ObservedEmptySuccessorRetirement};

#[cfg(test)]
#[path = "admission_tests.rs"]
mod admission_tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    InsufficientNativeReservation,
    InvalidPath,
    PathExists,
    MissingDatabase,
    InvalidCurrent,
    Filesystem,
    ParentDirectorySync,
    AlreadyOpen,
    ReservationUnavailable,
    EngineOpen,
    ColumnFamiliesMismatch,
    MissingManifest,
    InvalidManifest(ManifestError),
    ManifestMismatch,
    ManifestRead,
    /// The synchronized write may have committed. Preserve and resolve the directory.
    ManifestCommitUnknown,
}
impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InsufficientNativeReservation => {
                "store requires an adequate owned native reservation"
            }
            Self::InvalidPath => {
                "store requires an absolute directory path with an existing parent"
            }
            Self::PathExists => "initialization target already exists; inspect the existing state",
            Self::MissingDatabase => {
                "initialized store or CURRENT is absent; reopen cannot create it"
            }
            Self::InvalidCurrent => "CURRENT must be a bounded regular manifest pointer",
            Self::Filesystem => "store filesystem operation failed",
            Self::ParentDirectorySync => {
                "initialization parent could not be synchronized; preserve the partial directory"
            }
            Self::AlreadyOpen => "store path is already reserved by this process",
            Self::ReservationUnavailable => "store reservation state is unavailable",
            Self::EngineOpen => "native store inspection or exclusive open failed",
            Self::ColumnFamiliesMismatch => "store column families do not match the ingest layout",
            Self::MissingManifest => "ingest manifest is absent; existing state must be preserved",
            Self::InvalidManifest(_) => "ingest manifest is malformed or incompatible",
            Self::ManifestMismatch => {
                "ingest manifest does not match the complete expected identity"
            }
            Self::ManifestRead => "ingest manifest could not be read",
            Self::ManifestCommitUnknown => {
                "manifest commit outcome is unknown; preserve and resolve the store"
            }
        })
    }
}
impl std::error::Error for StoreError {}

/// A native directory owner. Only the connected Control backend may dispatch
/// its internal initial batch; opening alone supplies no authority grant.
///
/// Open/close are synchronous. The caller must retain and join its bounded worker;
/// an async timeout cannot cancel FFI or authorize a replacement writer.
pub struct IngestStore {
    // Field order is the lifetime invariant, including every error after open:
    // CFs/DB and native jobs close before resources, permit and path reservation.
    db: DB,
    resources: RocksNativeResources,
    _reservation: StoreReservation,
    manifest: IngestManifest,
    write_uncertain: bool,
    // Only consuming a locally minted candidate after connected activation can
    // populate this field. Opening a schema2 manifest never reconstructs it.
    live_initial: Option<initial_writer::LiveInitialBinding>,
    live_successor: Option<lineage::AdmittedFrontier>,
}

/// Exclusive historical-state recovery. There is no direct conversion or mutable
/// exposure to IngestStore, and opening never grants fresh admission authority.
/// A live candidate must retain custody through connected preparation/activation.
pub struct IngestRecovery {
    store: IngestStore,
}
impl IngestRecovery {
    /// Inspect retained historical identity under the actual exclusive native
    /// lock. The connected recovery consumer must still bind this manifest to
    /// the complete registry before any monotone recovery operation. This handle
    /// cannot issue fresh admissions or directly become an IngestStore.
    pub fn inspect(
        path: &Path,
        dataset: DatasetId,
        limits: RocksNativeLimits,
        permit: OwnedPermit,
    ) -> Result<Self, StoreError> {
        let store = IngestStore::open_existing(path, limits, permit)?;
        if store.manifest.identity().dataset_id != dataset {
            return Err(StoreError::ManifestMismatch);
        }
        Ok(Self { store })
    }

    pub fn open(
        path: &Path,
        expected: IngestManifest,
        limits: RocksNativeLimits,
        permit: OwnedPermit,
    ) -> Result<Self, StoreError> {
        IngestStore::reopen(path, expected, limits, permit).map(|store| Self { store })
    }
    pub const fn manifest(&self) -> &IngestManifest {
        self.store.manifest()
    }
    pub(in crate::storage) fn matches_configured_root(&self, path: &Path) -> bool {
        self.store.matches_configured_root(path)
    }
}
impl IngestStore {
    /// Creates only an absent target under an existing parent, then synchronizes
    /// the initial manifest. Failure never removes or silently reinitializes data.
    pub fn initialize(
        path: &Path,
        manifest: IngestManifest,
        limits: RocksNativeLimits,
        permit: OwnedPermit,
    ) -> Result<Self, StoreError> {
        if manifest.key_schema() != 1 {
            return Err(StoreError::InvalidManifest(
                ManifestError::UnsupportedFormat,
            ));
        }
        require_permit(&permit, limits)?;
        let path = fresh_path(path)?;
        let reservation = StoreReservation {
            _permit: permit,
            path: PathReservation::acquire(path)?,
        };
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&reservation.path.0).map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                StoreError::PathExists
            } else {
                StoreError::Filesystem
            }
        })?;
        // RocksDB synchronizes entries inside its DB/WAL directories, but did
        // not create this directory and cannot persist its entry in the parent.
        // Keep both reservations until this syscall returns, even on failure.
        let parent = rustix::fs::open(
            reservation.path.0.parent().ok_or(StoreError::InvalidPath)?,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(|_| StoreError::ParentDirectorySync)?;
        rustix::fs::fsync(&parent).map_err(|_| StoreError::ParentDirectorySync)?;
        drop(parent);
        let store = Self::open_native(reservation, Some(manifest), limits)?;
        let mut batch = WriteBatch::default();
        batch.put(KeyDomain::Manifest.prefix(), manifest.encode().as_bytes());
        let mut write = WriteOptions::default();
        write.set_sync(true);
        write.disable_wal(false);
        write.set_ignore_missing_column_families(false);
        store
            .db
            .write_opt(batch, &write)
            .map_err(|_| StoreError::ManifestCommitUnknown)?;
        Ok(store)
    }

    /// Opens exactly the existing ingest layout and manifest. Compatibility
    /// checks do not establish current control authority or authorize takeover.
    pub fn reopen(
        path: &Path,
        expected: IngestManifest,
        limits: RocksNativeLimits,
        permit: OwnedPermit,
    ) -> Result<Self, StoreError> {
        let store = Self::open_existing(path, limits, permit)?;
        if store.manifest != expected {
            return Err(StoreError::ManifestMismatch);
        }
        Ok(store)
    }

    fn open_existing(
        path: &Path,
        limits: RocksNativeLimits,
        permit: OwnedPermit,
    ) -> Result<Self, StoreError> {
        require_permit(&permit, limits)?;
        if !path.is_absolute() {
            return Err(StoreError::InvalidPath);
        }
        let path = fs::canonicalize(path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                StoreError::MissingDatabase
            } else {
                StoreError::Filesystem
            }
        })?;
        if !path.is_dir() {
            return Err(StoreError::InvalidPath);
        }
        let reservation = StoreReservation {
            _permit: permit,
            path: PathReservation::acquire(path)?,
        };
        check_current(&reservation.path.0)?;
        Self::open_native(reservation, None, limits)
    }

    fn open_native(
        reservation: StoreReservation,
        initialize: Option<IngestManifest>,
        limits: RocksNativeLimits,
    ) -> Result<Self, StoreError> {
        // On a failed native open, this later local drops its native allocations
        // before the reservation argument can release the budget/path.
        let mut profile = RocksNativeProfile::build(limits);
        if initialize.is_some() {
            profile.db_options.create_if_missing(true);
            profile.db_options.create_missing_column_families(true);
            profile.db_options.set_error_if_exists(true);
        } else {
            let mut names = DB::list_cf(&profile.db_options, &reservation.path.0)
                .map_err(|_| StoreError::EngineOpen)?;
            if names.len() != INGEST_COLUMN_FAMILIES.len() {
                return Err(StoreError::ColumnFamiliesMismatch);
            }
            names.sort_unstable();
            if !names.iter().map(String::as_str).eq(INGEST_COLUMN_FAMILIES) {
                return Err(StoreError::ColumnFamiliesMismatch);
            }
        }
        let db = DB::open_cf_descriptors(
            &profile.db_options,
            &reservation.path.0,
            profile.column_families,
        )
        .map_err(|_| StoreError::EngineOpen)?;
        let manifest = match initialize {
            Some(manifest) => manifest,
            None => {
                // Read only after exclusive open; never invent a historical
                // identity to obtain a handle. The fixed codec copies no
                // oversized native value, and the read pin stays local.
                let mut read = ReadOptions::default();
                read.set_verify_checksums(true);
                read.fill_cache(false);
                let pinned = db
                    .get_pinned_opt(KeyDomain::Manifest.prefix(), &read)
                    .map_err(|_| StoreError::ManifestRead)?
                    .ok_or(StoreError::MissingManifest)?;
                IngestManifest::decode(&pinned).map_err(StoreError::InvalidManifest)?
            }
        };
        Ok(Self {
            db,
            resources: profile.resources,
            _reservation: reservation,
            manifest,
            write_uncertain: false,
            live_initial: None,
            live_successor: None,
        })
    }

    pub const fn manifest(&self) -> &IngestManifest {
        &self.manifest
    }
    /// Static-root comparison for the trusted configured factory. Paths are not
    /// capabilities; operator-controlled roots must remain stable while open.
    pub(in crate::storage) fn matches_configured_root(&self, expected: &Path) -> bool {
        expected.is_absolute()
            && fs::canonicalize(expected).is_ok_and(|path| path == self._reservation.path.0)
    }
    pub const fn native_limits(&self) -> RocksNativeLimits {
        self.resources.limits()
    }
    pub fn native_usage(&self) -> RocksNativeUsage {
        self.resources.usage()
    }
}

fn require_permit(permit: &OwnedPermit, limits: RocksNativeLimits) -> Result<(), StoreError> {
    if permit.request().units().native_bytes() < limits.native_allowance_bytes() {
        return Err(StoreError::InsufficientNativeReservation);
    }
    Ok(())
}

fn fresh_path(path: &Path) -> Result<PathBuf, StoreError> {
    if !path.is_absolute() {
        return Err(StoreError::InvalidPath);
    }
    let leaf = path.file_name().ok_or(StoreError::InvalidPath)?;
    let parent = fs::canonicalize(path.parent().ok_or(StoreError::InvalidPath)?)
        .map_err(|_| StoreError::InvalidPath)?;
    if !parent.is_dir() {
        return Err(StoreError::InvalidPath);
    }
    let path = parent.join(leaf);
    match fs::symlink_metadata(&path) {
        Ok(_) => Err(StoreError::PathExists),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(path),
        Err(_) => Err(StoreError::Filesystem),
    }
}

fn check_current(path: &Path) -> Result<(), StoreError> {
    let path = path.join("CURRENT");
    // Do not follow a replaced symlink or block opening a FIFO. Validate the
    // opened descriptor itself; stable operator-controlled roots are required.
    let descriptor = rustix::fs::open(
        &path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|error| {
        if error == rustix::io::Errno::NOENT {
            StoreError::MissingDatabase
        } else {
            StoreError::InvalidCurrent
        }
    })?;
    let mut file = fs::File::from(descriptor);
    let metadata = file.metadata().map_err(|_| StoreError::Filesystem)?;
    if !metadata.is_file() || !(1..=128).contains(&metadata.len()) {
        return Err(StoreError::InvalidCurrent);
    }
    let mut bytes = [0; 129];
    let len = metadata.len() as usize; // Proven <= 128 above, on every target.
    file.read_exact(&mut bytes[..len])
        .map_err(|_| StoreError::InvalidCurrent)?;
    if file
        .read(&mut bytes[len..len + 1])
        .map_err(|_| StoreError::InvalidCurrent)?
        != 0
    {
        return Err(StoreError::InvalidCurrent);
    }
    let digits = bytes[..len]
        .strip_prefix(b"MANIFEST-")
        .and_then(|s| s.strip_suffix(b"\n"))
        .ok_or(StoreError::InvalidCurrent)?;
    if digits.is_empty() || digits.len() > 20 || !digits.iter().all(u8::is_ascii_digit) {
        return Err(StoreError::InvalidCurrent);
    }
    digits
        .iter()
        .try_fold(0u64, |number, digit| {
            number.checked_mul(10)?.checked_add(u64::from(digit - b'0'))
        })
        .ok_or(StoreError::InvalidCurrent)?;
    Ok(())
}

struct StoreReservation {
    _permit: OwnedPermit,
    path: PathReservation,
}
static OPEN_PATHS: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());
struct PathReservation(PathBuf);
impl PathReservation {
    fn acquire(path: PathBuf) -> Result<Self, StoreError> {
        let mut paths = OPEN_PATHS
            .lock()
            .map_err(|_| StoreError::ReservationUnavailable)?;
        if !paths.insert(path.clone()) {
            return Err(StoreError::AlreadyOpen);
        }
        Ok(Self(path))
    }
}
impl Drop for PathReservation {
    fn drop(&mut self) {
        // Remove our own path even during unwinding, but never clear poison:
        // future acquisition remains closed after a reservation invariant panic.
        OPEN_PATHS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.0);
    }
}
