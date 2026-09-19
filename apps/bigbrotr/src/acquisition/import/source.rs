use rustix::fs::{Mode, OFlags, open};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    fs::File,
    io::{self, Read, Seek, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// A complete byte identity, not a path, job ID, policy grant or durable checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceIdentity {
    pub bytes: u64,
    pub sha256: [u8; 32],
}

#[derive(Debug)]
pub enum ImportSourceError {
    Io(io::ErrorKind),
    NotRegular,
    InvalidScratch,
    CleanupUncertain,
    ScratchUnavailable,
    ScratchLimit {
        required_bytes: u64,
        available_bytes: u64,
        required_files: u64,
        available_files: u64,
    },
    SourceChanged,
    Cancelled,
    InvalidLimits,
    MemoryLimit,
    ReaderStopped,
}
impl fmt::Display for ImportSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "import source refused: {self:?}")
    }
}
impl std::error::Error for ImportSourceError {}
impl From<io::Error> for ImportSourceError {
    fn from(error: io::Error) -> Self {
        // Paths and input content are not diagnostics.
        Self::Io(error.kind())
    }
}

pub(super) fn check_cancelled(cancelled: &AtomicBool) -> Result<(), ImportSourceError> {
    if cancelled.load(Ordering::Acquire) {
        Err(ImportSourceError::Cancelled)
    } else {
        Ok(())
    }
}

/// One shared finite staging allowance. Counts logical bytes and live files;
/// actual filesystem exhaustion remains an I/O error. No disk space is promised.
#[derive(Clone)]
pub struct ScratchSpace(Arc<ScratchInner>);
struct ScratchInner {
    directory: PathBuf,
    max_bytes: u64,
    max_files: u64,
    used: Mutex<ScratchState>,
}
#[derive(Default)]
struct ScratchState {
    bytes: u64,
    files: u64,
    unavailable: bool,
}
impl ScratchSpace {
    /// The caller must authorize this operator-owned root separately. Ancestor
    /// aliases and a malicious process with the same OS identity are not isolated.
    pub fn new(
        directory: &Path,
        max_bytes: u64,
        max_files: u64,
    ) -> Result<Self, ImportSourceError> {
        let metadata = std::fs::symlink_metadata(directory)?;
        if !metadata.is_dir()
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.mode() & 0o022 != 0
            || max_files == 0
        {
            return Err(ImportSourceError::InvalidScratch);
        }
        Ok(Self(Arc::new(ScratchInner {
            directory: directory.to_owned(),
            max_bytes,
            max_files,
            used: Mutex::new(ScratchState::default()),
        })))
    }

    pub fn used_bytes(&self) -> u64 {
        self.0.used.lock().expect("scratch accounting lock").bytes
    }

    fn reserve(&self, bytes: u64) -> Result<ScratchPermit, ImportSourceError> {
        let mut used = self.0.used.lock().expect("scratch accounting lock");
        if used.unavailable {
            return Err(ImportSourceError::ScratchUnavailable);
        }
        let available_bytes = self.0.max_bytes - used.bytes;
        if bytes > available_bytes || used.files == self.0.max_files {
            return Err(ImportSourceError::ScratchLimit {
                required_bytes: bytes,
                available_bytes,
                required_files: 1,
                available_files: self.0.max_files - used.files,
            });
        }
        used.bytes += bytes;
        used.files += 1;
        Ok(ScratchPermit {
            space: self.clone(),
            bytes,
            retained: false,
        })
    }
}
pub(super) struct ScratchPermit {
    space: ScratchSpace,
    bytes: u64,
    retained: bool,
}
impl ScratchPermit {
    fn retain_unknown_cleanup(&mut self) {
        self.space
            .0
            .used
            .lock()
            .expect("scratch accounting lock")
            .unavailable = true;
        self.retained = true;
    }
}
impl Drop for ScratchPermit {
    fn drop(&mut self) {
        if self.retained {
            return;
        }
        let mut used = self.space.0.used.lock().expect("scratch accounting lock");
        used.bytes -= self.bytes;
        used.files -= 1;
    }
}

/// Private unlinked captured bytes. Exposes no writable handle or pathname.
/// This is ephemeral staging: process death discards it, so resume must recapture
/// the source and compare the whole identity from the durable job manifest.
pub struct SourceSnapshot {
    // Close the file before returning its scratch allowance.
    pub(super) file: File,
    pub(super) scratch: ScratchPermit,
    pub(super) identity: SourceIdentity,
}
impl SourceSnapshot {
    pub fn identity(&self) -> SourceIdentity {
        self.identity
    }

    /// Capture all bytes before any record is exposed. With no expected manifest,
    /// the digest identifies the captured byte stream, not a filesystem snapshot
    /// of the original path at one instant. An expected digest also rejects
    /// same-size in-place changes, regardless of restored timestamps.
    pub fn capture(
        source: &Path,
        scratch: &ScratchSpace,
        expected: Option<SourceIdentity>,
        cancelled: &AtomicBool,
    ) -> Result<Self, ImportSourceError> {
        check_cancelled(cancelled)?;
        let fd = open(
            source,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?;
        let mut input = File::from(fd);
        let metadata = input.metadata()?;
        if !metadata.is_file() {
            return Err(ImportSourceError::NotRegular);
        }
        if expected.is_some_and(|identity| identity.bytes != metadata.len()) {
            return Err(ImportSourceError::SourceChanged);
        }
        let mut permit = scratch.reserve(metadata.len())?;
        // tempfile's unnamed Unix fallback ignores unlink errors. Explicitly
        // close the TempPath and check link count before copying any source bytes.
        let mut file = unlink_staging(
            tempfile::NamedTempFile::new_in(&scratch.0.directory)?,
            &mut permit,
        )?;
        let mut hash = Sha256::new();
        let mut copied = 0u64;
        let mut buffer = [0; 16 * 1024];
        loop {
            check_cancelled(cancelled)?;
            let count = match input.read(&mut buffer) {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if count == 0 {
                break;
            }
            copied = copied
                .checked_add(count as u64)
                .filter(|n| *n <= metadata.len())
                .ok_or(ImportSourceError::SourceChanged)?;
            file.write_all(&buffer[..count])?;
            hash.update(&buffer[..count]);
        }
        let after = input.metadata()?;
        if copied != metadata.len()
            || after.len() != metadata.len()
            || after.modified()? != metadata.modified()?
        {
            return Err(ImportSourceError::SourceChanged);
        }
        let identity = SourceIdentity {
            bytes: copied,
            sha256: hash.finalize().into(),
        };
        if expected.is_some_and(|wanted| wanted != identity) {
            return Err(ImportSourceError::SourceChanged);
        }
        check_cancelled(cancelled)?;
        file.rewind()?;
        Ok(Self {
            file,
            scratch: permit,
            identity,
        })
    }
}

fn unlink_staging(
    staging: tempfile::NamedTempFile,
    permit: &mut ScratchPermit,
) -> Result<File, ImportSourceError> {
    let (file, path) = staging.into_parts();
    if path.close().is_err() || !file.metadata().is_ok_and(|metadata| metadata.nlink() == 0) {
        // The scratch owner cannot forget an unconfirmed named object and retry
        // indefinitely. Preserve its allowance and stop this owner for inventory
        // and cleanup. No source payload has been copied into the object yet.
        permit.retain_unknown_cleanup();
        return Err(ImportSourceError::CleanupUncertain);
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unconfirmed_unlink_retains_capacity_and_stops_the_scratch_owner() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = ScratchSpace::new(dir.path(), 100, 1).unwrap();
        let mut permit = scratch.reserve(50).unwrap();
        let staging = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        let orphan = dir.path().join("retained-owned-empty-file");
        // Real failing unlink: its originally created name has disappeared,
        // but the descriptor still has a link and cleanup is not established.
        std::fs::rename(staging.path(), &orphan).unwrap();
        assert!(unlink_staging(staging, &mut permit).is_err());
        drop(permit);
        assert!(
            scratch.reserve(1).is_err(),
            "unknown cleanup must not admit another file"
        );
        assert_eq!(scratch.used_bytes(), 50);
        assert_eq!(std::fs::metadata(&orphan).unwrap().len(), 0);
        std::fs::remove_file(orphan).unwrap();
        assert!(
            scratch.reserve(1).is_err(),
            "manual removal cannot silently reset owner state"
        );
    }
}
