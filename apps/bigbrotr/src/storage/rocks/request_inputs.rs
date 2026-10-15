//! Co-located producer staging under the already exclusively open native owner.
//! Input presence is never an admission outcome, producer checkpoint or ACK.
use super::IngestStore;
use crate::runtime::OwnedPermit;
use bigbrotr_model::{
    Digest256, ProducerEpoch, ProducerRequestId, ProducerSlotId, RequestSequence,
    codec::{
        CodecError,
        request::{MutationIntent, RequestDecodeLimits, decode_request},
    },
};
use rustix::fs::{Dir, Mode, OFlags, mkdirat, openat};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
};

const DIRECTORY: &str = "producer-inputs-v1";
const MAX_INPUT: usize = 16 * 1024 * 1024;
const MAX_FILES: usize = 64;
const MAX_TOTAL: u64 = 32 * 1024 * 1024;
const SCRATCH: usize = 8192;
#[path = "lineage_inputs.rs"]
mod lineage_inputs;
// Covers file comparison plus bounded directory/name/descriptor bookkeeping.
// The qualified native profile still supplies the wider engine/libc allowance.
pub(super) const STAGING_OVERHEAD_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestInputError {
    Limit,
    Allocation,
    Missing,
    Incomplete,
    Identity,
    Conflict,
    Filesystem,
    InsufficientReservation,
    Codec(CodecError),
}

/// Constructed only by this exclusive native owner after synchronizing exact
/// input bytes. This witnesses staging, never native admission or authority.
pub(in crate::storage) struct StagedProducerRequest {
    pub request: ProducerRequestId,
    pub dataset: bigbrotr_model::DatasetId,
    pub digest: Digest256,
    pub input_bytes: u32,
    pub manifest_sha256: [u8; 32],
    _synchronized: (),
}

/// Exact retained bytes beyond issued, not a reservation, outcome or authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage) struct SuccessorStagedInput {
    pub request: ProducerRequestId,
    pub digest: Digest256,
    pub input_bytes: u32,
}

/// Decoded original input plus its retained grant. Recovery still needs the
/// producer ledger, original outcome resolution and current dispatch authority.
pub struct RetainedRequestInput {
    request: ProducerRequestId,
    intent: MutationIntent,
    encoded_bytes: u32,
    permit: OwnedPermit,
}
impl RetainedRequestInput {
    pub(super) fn encoded_bytes(&self) -> u32 {
        self.encoded_bytes
    }
    pub fn required_data_bytes(limits: RequestDecodeLimits) -> Result<u64, RequestInputError> {
        limits
            .values
            .max_encoded_bytes
            .checked_add(limits.values.max_allocation_bytes)
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>() + STAGING_OVERHEAD_BYTES))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(RequestInputError::Limit)
    }
    pub fn into_parts(self) -> (ProducerRequestId, MutationIntent, OwnedPermit) {
        (self.request, self.intent, self.permit)
    }
}
impl IngestStore {
    /// A prepared successor has staged no request, including a request whose PG
    /// reservation never committed. Inspect the same finite shared spool; do not
    /// infer emptiness from SQL or create the directory while checking it.
    pub(super) fn require_no_epoch_inputs(
        &self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
    ) -> Result<(), RequestInputError> {
        let directory = match self.input_directory(false) {
            Ok(directory) => directory,
            Err(RequestInputError::Missing) => return Ok(()),
            Err(error) => return Err(error),
        };
        let mut prefix = name(ProducerRequestId {
            producer_slot_id: slot,
            producer_epoch: epoch,
            request_sequence: RequestSequence::new(1).expect("nonzero fixed boundary"),
        });
        prefix.truncate(50);
        let mut count = 0usize;
        let mut total = 0u64;
        for entry in Dir::read_from(&directory).map_err(|_| RequestInputError::Filesystem)? {
            let entry = entry.map_err(|_| RequestInputError::Filesystem)?;
            let raw = entry.file_name().to_bytes();
            if raw == b"." || raw == b".." {
                continue;
            }
            count += 1;
            if count > MAX_FILES {
                return Err(RequestInputError::Limit);
            }
            if !valid_name(raw) {
                return Err(RequestInputError::Identity);
            }
            let filename = std::str::from_utf8(raw).map_err(|_| RequestInputError::Identity)?;
            let file = open_input(&directory, filename)?;
            total = total
                .checked_add(
                    file.metadata()
                        .map_err(|_| RequestInputError::Filesystem)?
                        .len(),
                )
                .filter(|n| *n <= MAX_TOTAL)
                .ok_or(RequestInputError::Limit)?;
            if raw.starts_with(prefix.as_bytes()) {
                return Err(RequestInputError::Conflict);
            }
        }
        Ok(())
    }

    pub(super) fn stage_producer_request(
        &mut self,
        request: ProducerRequestId,
        digest: Digest256,
        bytes: &[u8],
    ) -> Result<StagedProducerRequest, RequestInputError> {
        self.retain_request_input(request, bytes)?;
        Ok(StagedProducerRequest {
            request,
            dataset: self.manifest.identity().dataset_id,
            digest,
            input_bytes: bytes.len() as u32, // retain_request_input bounded to 16 MiB
            manifest_sha256: Sha256::digest(self.manifest.encode().as_bytes()).into(),
            _synchronized: (),
        })
    }
    pub(super) fn stage_admission_input(
        &mut self,
        request: ProducerRequestId,
        digest: Digest256,
        bytes: &[u8],
        cancelled: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<StagedProducerRequest, super::AdmissionError> {
        self.retain_admission_input(request, bytes, cancelled)?;
        Ok(StagedProducerRequest {
            request,
            dataset: self.manifest.identity().dataset_id,
            digest,
            input_bytes: bytes.len() as u32,
            manifest_sha256: Sha256::digest(self.manifest.encode().as_bytes()).into(),
            _synchronized: (),
        })
    }

    /// Bounded synchronous read through the existing exclusive owner. Missing,
    /// malformed or mismatched bytes remain unresolved, never an absent effect.
    /// This does not recreate a producer incarnation or authorize a new commit.
    pub fn read_request_input(
        &self,
        request: ProducerRequestId,
        digest: Digest256,
        limits: RequestDecodeLimits,
        permit: OwnedPermit,
    ) -> Result<RetainedRequestInput, RequestInputError> {
        if permit.request().units().items() == 0
            || permit.request().units().data_bytes()
                < RetainedRequestInput::required_data_bytes(limits)?
        {
            return Err(RequestInputError::InsufficientReservation);
        }
        let directory = self.input_directory(false)?;
        let mut file = open_input(&directory, &name(request))?;
        let size = file
            .metadata()
            .map_err(|_| RequestInputError::Filesystem)?
            .len();
        if size > limits.values.max_encoded_bytes as u64 {
            return Err(RequestInputError::Codec(CodecError::InputLimit));
        }
        let size = usize::try_from(size).map_err(|_| RequestInputError::Limit)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| RequestInputError::Allocation)?;
        bytes.resize(size, 0);
        file.read_exact(&mut bytes)
            .map_err(|_| RequestInputError::Incomplete)?;
        if file
            .read(&mut [0u8; 1])
            .map_err(|_| RequestInputError::Filesystem)?
            != 0
        {
            return Err(RequestInputError::Incomplete);
        }
        let (found, intent) =
            decode_request(&bytes, digest, limits).map_err(RequestInputError::Codec)?;
        let dataset = match &intent {
            MutationIntent::Event(v) => v.visibility_scope_id().dataset_id,
            MutationIntent::Evidence(v) => v.visibility_scope_id().dataset_id,
        };
        if found != request || dataset != self.manifest.identity().dataset_id {
            return Err(RequestInputError::Identity);
        }
        Ok(RetainedRequestInput {
            request,
            intent,
            encoded_bytes: size as u32, // open_input enforces the 16 MiB bound
            permit,
        })
    }

    /// Called only after native request/window validation and before its synced
    /// effects. One &mut owner plus the native LOCK serializes this sidecar too.
    /// No separate descriptor ever opens RocksDB's LOCK. Retain all input detail;
    /// reaching the finite disk envelope refuses work, never erases old requests.
    pub(super) fn retain_request_input(
        &mut self,
        request: ProducerRequestId,
        bytes: &[u8],
    ) -> Result<(), RequestInputError> {
        if bytes.is_empty() || bytes.len() > MAX_INPUT {
            return Err(RequestInputError::Limit);
        }
        let directory = self.input_directory(true)?;
        let filename = name(request);
        let mut count = 0usize;
        let mut total = 0u64;
        // Validate every entry within the finite starting spool; an unexpected
        // name/type or empty file cannot disappear behind a filtered scan.
        for entry in Dir::read_from(&directory).map_err(|_| RequestInputError::Filesystem)? {
            let entry = entry.map_err(|_| RequestInputError::Filesystem)?;
            let raw = entry.file_name().to_bytes();
            if raw == b"." || raw == b".." {
                continue;
            }
            count += 1;
            if count > MAX_FILES {
                return Err(RequestInputError::Limit);
            }
            let entry_name = std::str::from_utf8(raw).map_err(|_| RequestInputError::Identity)?;
            if !valid_name(raw) {
                return Err(RequestInputError::Identity);
            }
            let file = open_input(&directory, entry_name)?;
            total = total
                .checked_add(
                    file.metadata()
                        .map_err(|_| RequestInputError::Filesystem)?
                        .len(),
                )
                .filter(|n| *n <= MAX_TOTAL)
                .ok_or(RequestInputError::Limit)?;
        }
        match open_input(&directory, &filename) {
            Ok(mut file) => {
                if file
                    .metadata()
                    .map_err(|_| RequestInputError::Filesystem)?
                    .len()
                    != bytes.len() as u64
                {
                    return Err(RequestInputError::Conflict);
                }
                let mut buffer = [0u8; SCRATCH];
                for part in bytes.chunks(SCRATCH) {
                    file.read_exact(&mut buffer[..part.len()])
                        .map_err(|_| RequestInputError::Incomplete)?;
                    if buffer[..part.len()] != *part {
                        return Err(RequestInputError::Conflict);
                    }
                }
                if file
                    .read(&mut buffer[..1])
                    .map_err(|_| RequestInputError::Filesystem)?
                    != 0
                {
                    return Err(RequestInputError::Conflict);
                }
                file.sync_all().map_err(|_| RequestInputError::Filesystem)?;
            }
            Err(RequestInputError::Missing) => {
                if count >= MAX_FILES
                    || total
                        .checked_add(bytes.len() as u64)
                        .is_none_or(|n| n > MAX_TOTAL)
                {
                    return Err(RequestInputError::Limit);
                }
                let mut file = File::from(
                    openat(
                        &directory,
                        filename.as_str(),
                        OFlags::WRONLY
                            | OFlags::CLOEXEC
                            | OFlags::NOFOLLOW
                            | OFlags::CREATE
                            | OFlags::EXCL,
                        Mode::RUSR | Mode::WUSR,
                    )
                    .map_err(|_| RequestInputError::Filesystem)?,
                );
                // A restrictive umask can remove owner read/write bits while
                // the already opened writer still works. Publish only a file
                // that the recovery reader will accept.
                rustix::fs::fchmod(&file, Mode::RUSR | Mode::WUSR)
                    .map_err(|_| RequestInputError::Filesystem)?;
                let created = file.metadata().map_err(|_| RequestInputError::Filesystem)?;
                if !private_file(&created) || created.len() != 0 {
                    return Err(RequestInputError::Identity);
                }
                // Partial/error state is retained for explicit recovery. No
                // caller can dispatch before this file and directory both sync.
                file.write_all(bytes)
                    .map_err(|_| RequestInputError::Filesystem)?;
                file.sync_all().map_err(|_| RequestInputError::Filesystem)?;
            }
            Err(error) => return Err(error),
        }
        directory
            .sync_all()
            .map_err(|_| RequestInputError::Filesystem)?;
        Ok(())
    }
    fn input_directory(&self, create: bool) -> Result<File, RequestInputError> {
        let root = File::from(
            rustix::fs::open(
                &self._reservation.path.0,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .map_err(|_| RequestInputError::Filesystem)?,
        );
        check_directory(&root)?;
        if create {
            match mkdirat(&root, DIRECTORY, Mode::RWXU) {
                Ok(()) => {}
                Err(rustix::io::Errno::EXIST) => {}
                Err(_) => return Err(RequestInputError::Filesystem),
            }
        }
        let directory = File::from(
            openat(
                &root,
                DIRECTORY,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .map_err(missing)?,
        );
        check_directory(&directory)?;
        if create {
            root.sync_all().map_err(|_| RequestInputError::Filesystem)?;
        }
        Ok(directory)
    }
}
fn missing(error: rustix::io::Errno) -> RequestInputError {
    if error == rustix::io::Errno::NOENT {
        RequestInputError::Missing
    } else {
        RequestInputError::Filesystem
    }
}
fn check_directory(file: &File) -> Result<(), RequestInputError> {
    let m = file.metadata().map_err(|_| RequestInputError::Filesystem)?;
    if !m.is_dir() || m.mode() & 0o777 != 0o700 || m.uid() != rustix::process::geteuid().as_raw() {
        return Err(RequestInputError::Identity);
    }
    Ok(())
}
fn open_input(directory: &File, name: &str) -> Result<File, RequestInputError> {
    let file = File::from(
        openat(
            directory,
            name,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(missing)?,
    );
    let m = file.metadata().map_err(|_| RequestInputError::Filesystem)?;
    if !private_file(&m) {
        return Err(RequestInputError::Identity);
    }
    if m.len() == 0 {
        return Err(RequestInputError::Incomplete);
    }
    if m.len() > MAX_INPUT as u64 {
        return Err(RequestInputError::Limit);
    }
    Ok(file)
}
fn name(request: ProducerRequestId) -> String {
    format!(
        "{}-{:016x}-{:016x}.cbor",
        request.producer_slot_id,
        request.producer_epoch.get(),
        request.request_sequence.get()
    )
}
fn valid_name(bytes: &[u8]) -> bool {
    bytes.len() == 71
        && bytes[32] == b'-'
        && bytes[49] == b'-'
        && &bytes[66..] == b".cbor"
        && bytes[..66]
            .iter()
            .enumerate()
            .all(|(i, b)| [32, 49].contains(&i) || b.is_ascii_digit() || (b'a'..=b'f').contains(b))
}

fn private_file(m: &std::fs::Metadata) -> bool {
    m.is_file()
        && m.mode() & 0o777 == 0o600
        && m.nlink() == 1
        && m.uid() == rustix::process::geteuid().as_raw()
}
