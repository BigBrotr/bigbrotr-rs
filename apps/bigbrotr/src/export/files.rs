//! Descriptor-owned output for one durably reserved, nonterminal export unit.
//! This module supplies filesystem exclusion, never SQL/job/read authority.
use super::{
    destination::{LocalDestination, private},
    jsonl::{JsonlError, JsonlFile, write_exact_event},
    manifest::{MAX_MANIFEST_BYTES, ManifestError, SignedEventsManifest},
};
use bigbrotr_model::{ArtifactId, Digest256};
use bigbrotr_protocol::VerifiedEvent;
use rustix::fs::{
    AtFlags, Dir, FlockOperation, Mode, OFlags, RenameFlags, chmodat, flock, mkdirat, openat,
    renameat_with, unlinkat,
};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, Permissions},
    io::{self, BufReader, BufWriter, Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

const DATA: &str = "events.jsonl";
const MANIFEST: &str = "manifest.json";
const BUFFER: usize = 64 * 1024;
const OPEN: OFlags = OFlags::RDONLY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK)
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOCTTY);
const CREATE: OFlags = OFlags::RDWR
    .union(OFlags::CREATE)
    .union(OFlags::EXCL)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const MODE: Mode = Mode::RUSR.union(Mode::WUSR);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFileError {
    Filesystem,
    Conflict,
    Busy,
    ResourceLimit,
    Cancelled,
}
use ExportFileError as Error;
impl From<io::Error> for Error {
    fn from(_: io::Error) -> Self {
        Self::Filesystem
    }
}
impl From<rustix::io::Errno> for Error {
    fn from(_: rustix::io::Errno) -> Self {
        Self::Filesystem
    }
}
impl From<JsonlError> for Error {
    fn from(error: JsonlError) -> Self {
        match error {
            JsonlError::ResourceLimit => Self::ResourceLimit,
            JsonlError::Cancelled => Self::Cancelled,
            JsonlError::WrongEvent => Self::Conflict,
            JsonlError::Output => Self::Filesystem,
        }
    }
}

/// A filesystem result only. SQL must atomically bind these bytes to the original
/// unit/checkpoint after rechecking authority; existence does not complete a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredExport {
    pub data: JsonlFile,
    pub manifest_bytes: u64,
    pub manifest_sha256: Digest256,
}

/// Retain outside cancellable futures through actual worker and SQL driver joins.
/// Cloning custody cannot construct another writer. The lock inode is never
/// removed or replaced: waiters must always contend on the same inode.
pub struct OutputCustody {
    destination: Arc<LocalDestination>,
    _lock: File,
}
pub struct ExportUnit {
    custody: Arc<OutputCustody>,
    artifact: ArtifactId,
    final_name: String,
    staging: String,
}
impl ExportUnit {
    /// Caller must first resolve unknown commits and durably reserve this exact
    /// destination/artifact namespace for a nonterminal execution unit. An empty
    /// lock is a mutex, not evidence that any job may adopt the existing files.
    pub fn acquire(
        destination: Arc<LocalDestination>,
        artifact: ArtifactId,
    ) -> Result<Self, Error> {
        destination.verify().map_err(|_| Error::Conflict)?;
        let root = &destination.directory;
        let name = format!(".artifact-{artifact}.lock");
        let lock = match openat(root, name.as_str(), CREATE, MODE) {
            Ok(fd) => {
                let file = File::from(fd);
                file.set_permissions(Permissions::from_mode(0o600))?;
                file
            }
            Err(rustix::io::Errno::EXIST) => {
                File::from(openat(root, name.as_str(), OPEN, Mode::empty())?)
            }
            Err(error) => return Err(error.into()),
        };
        check_file(&lock, root)?;
        if lock.metadata()?.len() != 0 {
            return Err(Error::Conflict);
        }
        flock(&lock, FlockOperation::NonBlockingLockExclusive).map_err(|error| {
            if error == rustix::io::Errno::WOULDBLOCK {
                Error::Busy
            } else {
                Error::Filesystem
            }
        })?;
        Ok(Self {
            custody: Arc::new(OutputCustody {
                destination,
                _lock: lock,
            }),
            artifact,
            final_name: artifact.to_string(),
            staging: format!(".artifact-{artifact}.partial"),
        })
    }
    pub fn custody(&self) -> Arc<OutputCustody> {
        self.custody.clone()
    }

    /// Rebuild on fresh inodes even after an apparently complete earlier rename:
    /// repeating fsync on old inodes does not repair an earlier writeback error.
    /// `maximum` covers data + manifest; `temporary_maximum` covers two complete
    /// copies. These logical byte limits are not a physical filesystem quota.
    pub fn write(
        &mut self,
        manifest: &SignedEventsManifest<'_>,
        event: Option<&VerifiedEvent>,
        maximum: u64,
        temporary_maximum: u64,
        cancelled: &AtomicBool,
    ) -> Result<StoredExport, Error> {
        check_cancel(cancelled)?;
        let destination = &self.custody.destination;
        destination.verify().map_err(|_| Error::Conflict)?;
        if manifest.artifact_id != self.artifact
            || manifest.source_cut.dataset_id() != destination.dataset()
        {
            return Err(Error::Conflict);
        }
        let data = write_exact_event(io::sink(), manifest.event_id, event, maximum, cancelled)?;
        let bytes = manifest
            .encode(data, MAX_MANIFEST_BYTES)
            .map_err(|error| match error {
                ManifestError::InvalidInput => Error::Conflict,
                ManifestError::ResourceLimit => Error::ResourceLimit,
            })?;
        let total = data
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or(Error::ResourceLimit)?;
        if total > maximum || total.checked_mul(2).is_none_or(|v| v > temporary_maximum) {
            return Err(Error::ResourceLimit);
        }
        let expected = Expected {
            manifest,
            event,
            data,
            bytes: &bytes,
            cancelled,
        };
        let root = &destination.directory;
        // Validate ALL preexisting contents before any removal. A partial file is
        // disposable only as a prefix of this unit's independently reconstructed
        // expected result, under durable namespace ownership and exclusive custody.
        let final_dir = inspect(root, &self.final_name, &expected, false)?;
        let staging = inspect(root, &self.staging, &expected, true)?;
        if let (Some(a), Some(b)) = (&final_dir, &staging)
            && identity(&a.directory)? == identity(&b.directory)?
        {
            return Err(Error::Conflict);
        }
        if let Some(staging) = staging {
            check_cancel(cancelled)?;
            for (name, present) in [(DATA, staging.data), (MANIFEST, staging.manifest)] {
                check_cancel(cancelled)?;
                if present {
                    unlinkat(&staging.directory, name, AtFlags::empty())?;
                }
            }
            unlinkat(root, self.staging.as_str(), AtFlags::REMOVEDIR)?;
            root.sync_all()?;
        }
        check_cancel(cancelled)?;
        mkdirat(root, self.staging.as_str(), MODE | Mode::XUSR)?;
        // Only the exclusively created name is normalized; no preexisting path
        // is repaired. This handles a restrictive umask without changing global
        // process state. Same-UID hostile namespace mutation is outside custody.
        chmodat(
            root,
            self.staging.as_str(),
            MODE | Mode::XUSR,
            AtFlags::empty(),
        )?;
        let directory = File::from(openat(
            root,
            self.staging.as_str(),
            OPEN | OFlags::DIRECTORY,
            Mode::empty(),
        )?);
        check_directory(&directory, root)?;
        let mut output = BufWriter::with_capacity(
            BUFFER,
            CancelOutput {
                writer: create(&directory, DATA)?,
                cancelled,
            },
        );
        let written =
            write_exact_event(&mut output, manifest.event_id, event, data.bytes, cancelled);
        let flushed = if written.is_ok() {
            output.flush()
        } else {
            Ok(())
        };
        // BufWriter::drop may flush. Dismantle on EVERY outcome so cancellation
        // or failure cannot cause an unobserved write while unwinding the buffer.
        let (file, _) = output.into_parts();
        check_cancel(cancelled)?;
        if written? != data {
            return Err(Error::Conflict);
        }
        flushed?;
        check_cancel(cancelled)?;
        file.writer.sync_all()?;
        check_cancel(cancelled)?;
        let mut file = CancelOutput {
            writer: create(&directory, MANIFEST)?,
            cancelled,
        };
        for chunk in bytes.chunks(BUFFER) {
            check_cancel(cancelled)?;
            let result = file.write_all(chunk);
            check_cancel(cancelled)?;
            result?;
        }
        check_cancel(cancelled)?;
        file.writer.sync_all()?;
        check_cancel(cancelled)?;
        directory.sync_all()?;
        check_cancel(cancelled)?;
        destination.verify().map_err(|_| Error::Conflict)?;
        renameat_with(
            root,
            self.staging.as_str(),
            root,
            self.final_name.as_str(),
            if final_dir.is_some() {
                RenameFlags::EXCHANGE
            } else {
                RenameFlags::NOREPLACE
            },
        )?;
        root.sync_all()?;
        check_cancel(cancelled)?;
        Ok(StoredExport {
            data,
            manifest_bytes: bytes.len() as u64,
            manifest_sha256: Digest256::from_bytes(Sha256::digest(&bytes).into()),
        })
    }
}

fn check_cancel(cancelled: &AtomicBool) -> Result<(), Error> {
    if cancelled.load(Ordering::Acquire) {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}
fn create(directory: &File, name: &str) -> Result<File, Error> {
    let file = File::from(openat(directory, name, CREATE, MODE)?);
    file.set_permissions(Permissions::from_mode(0o600))?;
    Ok(file)
}
fn identity(file: &File) -> Result<(u64, u64), Error> {
    let m = file.metadata()?;
    Ok((m.dev(), m.ino()))
}
fn check_file(file: &File, parent: &File) -> Result<(), Error> {
    private(&file.metadata()?, false).map_err(|_| Error::Conflict)?;
    same_mount(file, parent)
}
fn check_directory(file: &File, parent: &File) -> Result<(), Error> {
    private(&file.metadata()?, true).map_err(|_| Error::Conflict)?;
    if identity(file)? == identity(parent)? {
        return Err(Error::Conflict);
    }
    same_mount(file, parent)
}
fn same_mount(file: &File, parent: &File) -> Result<(), Error> {
    if file.metadata()?.dev() != parent.metadata()?.dev() {
        return Err(Error::Conflict);
    }
    // dev/ino alone cannot detect a same-filesystem bind mount. Fail closed if
    // Linux cannot report mount IDs, rather than deleting through an alias.
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{StatxFlags, statx};
        let mask = StatxFlags::MNT_ID;
        let a = statx(file, "", AtFlags::EMPTY_PATH, mask)?;
        let b = statx(parent, "", AtFlags::EMPTY_PATH, mask)?;
        if a.stx_mask & mask.bits() == 0
            || b.stx_mask & mask.bits() == 0
            || a.stx_mnt_id != b.stx_mnt_id
        {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}
struct Expected<'a, 'b> {
    manifest: &'a SignedEventsManifest<'b>,
    event: Option<&'a VerifiedEvent>,
    data: JsonlFile,
    bytes: &'a [u8],
    cancelled: &'a AtomicBool,
}
struct Existing {
    directory: File,
    data: bool,
    manifest: bool,
}
fn inspect(
    root: &File,
    name: &str,
    expected: &Expected<'_, '_>,
    prefix: bool,
) -> Result<Option<Existing>, Error> {
    let directory = match openat(root, name, OPEN | OFlags::DIRECTORY, Mode::empty()) {
        Ok(fd) => File::from(fd),
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    check_directory(&directory, root)?;
    let mut existing = Existing {
        directory,
        data: false,
        manifest: false,
    };
    for entry in Dir::read_from(&existing.directory)? {
        check_cancel(expected.cancelled)?;
        let entry = entry?;
        let name = match entry.file_name().to_bytes() {
            b"." | b".." => continue,
            b"events.jsonl" => {
                existing.data = true;
                DATA
            }
            b"manifest.json" => {
                existing.manifest = true;
                MANIFEST
            }
            _ => return Err(Error::Conflict),
        };
        let file = File::from(openat(&existing.directory, name, OPEN, Mode::empty())?);
        check_file(&file, root)?;
        let length = file.metadata()?.len();
        let maximum = if name == DATA {
            expected.data.bytes
        } else {
            expected.bytes.len() as u64
        };
        if length > maximum || (!prefix && length != maximum) {
            return Err(Error::Conflict);
        }
        let mut compare = Compare {
            file: BufReader::with_capacity(BUFFER, file),
            remaining: length,
            mismatch: false,
        };
        if name == DATA {
            let result = write_exact_event(
                &mut compare,
                expected.manifest.event_id,
                expected.event,
                maximum,
                expected.cancelled,
            );
            if compare.mismatch {
                return Err(Error::Conflict);
            }
            result?;
        } else {
            let result = compare.write_all(expected.bytes);
            if compare.mismatch {
                return Err(Error::Conflict);
            }
            result?;
        }
        if compare.remaining != 0 {
            return Err(Error::Conflict);
        }
    }
    if !prefix && (!existing.data || !existing.manifest) {
        return Err(Error::Conflict);
    }
    Ok(Some(existing))
}
/// Compare a possibly incomplete file against streamed expected bytes without
/// collecting another event-sized buffer. A short expected prefix is not a receipt.
struct Compare {
    file: BufReader<File>,
    remaining: u64,
    mismatch: bool,
}

// The gate belongs BELOW buffering: flush/write_all may retry short writes.
struct CancelOutput<'a, W> {
    writer: W,
    cancelled: &'a AtomicBool,
}
impl<W: Write> Write for CancelOutput<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::other("export output cancelled"));
        }
        self.writer.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl Write for Compare {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self.remaining.min(bytes.len() as u64) as usize;
        let mut buffer = [0u8; 8192];
        for chunk in bytes[..length].chunks(buffer.len()) {
            self.file.read_exact(&mut buffer[..chunk.len()])?;
            if &buffer[..chunk.len()] != chunk {
                self.mismatch = true;
                return Err(io::Error::other("conflicting artifact bytes"));
            }
        }
        self.remaining -= length as u64;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    struct Short<'a> {
        accepted: usize,
        cancel: &'a AtomicBool,
    }
    impl Write for Short<'_> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            assert!(!bytes.is_empty());
            self.accepted += 1;
            self.cancel.store(true, Ordering::Release);
            Ok(1)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn cancellation_stops_new_writes_inside_a_partial_buffer_flush() {
        let cancel = AtomicBool::new(false);
        let sink = Short {
            accepted: 0,
            cancel: &cancel,
        };
        let mut buffered = BufWriter::with_capacity(
            16,
            CancelOutput {
                writer: sink,
                cancelled: &cancel,
            },
        );
        buffered.write_all(b"complete data").unwrap();
        let result = buffered.flush();
        let (output, pending) = buffered.into_parts();
        assert!(result.is_err());
        assert_eq!(output.writer.accepted, 1);
        assert_eq!(pending.unwrap(), b"omplete data");
    }
}
