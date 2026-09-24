//! An explicitly initialized private filesystem destination, retained by descriptor.
//! Never create directories or repair conflicting/incomplete markers implicitly.
use bigbrotr_model::{DatasetId, DestinationId, Digest256};
use rustix::fs::{Dir, FlockOperation, Mode, OFlags, flock, open, openat, renameat};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    fmt,
    fs::File,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

const MARKER: &str = "destination.json";
const MAX_MARKER: usize = 16 * 1024;
const DIRECTORY: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationError {
    InvalidRoot,
    Filesystem,
    Permissions,
    Busy,
    NotInitialized,
    NotEmpty,
    Mismatch,
    ResourceLimit,
}
impl fmt::Display for DestinationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "export destination refused: {self:?}")
    }
}
impl std::error::Error for DestinationError {}
use DestinationError as Error;

/// Held through the destination's actual consumers. The marker is a storage
/// binding only, never a dataset/scope grant, publication or write authority.
pub struct LocalDestination {
    pub(super) directory: File,
    root: PathBuf,
    dataset: DatasetId,
    id: DestinationId,
    marker: Vec<u8>,
    digest: Digest256,
}

/// An observed binding with retained custody, not current execution authority.
/// Construct on a supervised blocking worker, before acquiring SQL locks. The
/// supervisor retains this value (and the original destination) through Control
/// driver join on cancellation/unknown COMMIT; the SQL future only borrows it.
pub struct CheckedDestination {
    destination: Arc<LocalDestination>,
}
impl CheckedDestination {
    pub fn dataset(&self) -> DatasetId {
        self.destination.dataset
    }
    pub fn id(&self) -> DestinationId {
        self.destination.id
    }
    pub fn binding_digest(&self) -> Digest256 {
        self.destination.digest
    }
}
impl LocalDestination {
    pub fn check_for_acceptance(self: Arc<Self>) -> Result<CheckedDestination, Error> {
        self.verify()?;
        Ok(CheckedDestination { destination: self })
    }
    /// Observe available filesystem capacity through the retained descriptor.
    /// This is a preflight, not a quota or a grant against concurrent writers.
    pub fn check_growth(&self, growth: u64, reserve: u64) -> Result<(), Error> {
        self.verify()?;
        check_filesystem_growth(&self.directory, growth, reserve)
    }
    pub fn initialize(root: &Path, dataset: DatasetId, id: DestinationId) -> Result<Self, Error> {
        Self::load(root, dataset, id, true)
    }
    pub fn open(root: &Path, dataset: DatasetId, id: DestinationId) -> Result<Self, Error> {
        Self::load(root, dataset, id, false)
    }
    pub fn dataset(&self) -> DatasetId {
        self.dataset
    }
    pub fn id(&self) -> DestinationId {
        self.id
    }
    pub fn binding_digest(&self) -> Digest256 {
        self.digest
    }

    fn load(
        root: &Path,
        dataset: DatasetId,
        id: DestinationId,
        initialize: bool,
    ) -> Result<Self, Error> {
        validate_root(root)?;
        let directory = directory(root)?;
        let metadata = directory.metadata().map_err(|_| Error::Filesystem)?;
        private(&metadata, true)?;
        flock(
            &directory,
            if initialize {
                FlockOperation::NonBlockingLockExclusive
            } else {
                FlockOperation::NonBlockingLockShared
            },
        )
        .map_err(|e| {
            if e == rustix::io::Errno::WOULDBLOCK {
                Error::Busy
            } else {
                Error::Filesystem
            }
        })?;
        #[derive(Serialize)]
        struct Binding<'a> {
            format: &'static str,
            dataset_id: DatasetId,
            destination_id: DestinationId,
            root: &'a str,
            device: u64,
            inode: u64,
        }
        let marker = serde_json::to_vec(&Binding {
            format: "bigbrotr-export-destination/1",
            dataset_id: dataset,
            destination_id: id,
            root: root.to_str().ok_or(Error::InvalidRoot)?,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
        .map_err(|_| Error::ResourceLimit)?;
        if marker.len() > MAX_MARKER {
            return Err(Error::ResourceLimit);
        }
        if !matches_marker(&directory, &marker)? {
            if !initialize {
                return Err(Error::NotInitialized);
            }
            // Enumerate through the retained descriptor; a second path-based
            // directory read could inspect a substituted root.
            for entry in Dir::read_from(&directory).map_err(|_| Error::Filesystem)? {
                let entry = entry.map_err(|_| Error::Filesystem)?;
                if !matches!(entry.file_name().to_bytes(), b"." | b"..") {
                    return Err(Error::NotEmpty);
                }
            }
        }
        if initialize {
            // Recreate even an identical marker through a fresh synced inode.
            // Merely retrying fsync cannot repair an earlier writeback error.
            // Exclusive root custody makes replacement of these already verified
            // exact bytes safe; conflicting/partial markers never reach this path.
            let mut nonce = [0u8; 16];
            getrandom::fill(&mut nonce).map_err(|_| Error::Filesystem)?;
            let temporary = format!(".destination-{:032x}.partial", u128::from_be_bytes(nonce));
            let mut file = File::from(
                openat(
                    &directory,
                    temporary.as_str(),
                    OFlags::WRONLY
                        | OFlags::CREATE
                        | OFlags::EXCL
                        | OFlags::NOFOLLOW
                        | OFlags::CLOEXEC,
                    Mode::RUSR | Mode::WUSR,
                )
                .map_err(|_| Error::Filesystem)?,
            );
            // Correct only the newly created descriptor, never existing modes.
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(|_| Error::Filesystem)?;
            file.write_all(&marker).map_err(|_| Error::Filesystem)?;
            file.sync_all().map_err(|_| Error::Filesystem)?;
            // Failed staging is deliberately retained. A crash before first
            // publication leaves an uninitialized nonempty root, not a grant.
            renameat(&directory, temporary.as_str(), &directory, MARKER)
                .map_err(|_| Error::Filesystem)?;
            directory.sync_all().map_err(|_| Error::Filesystem)?;
            self::directory(root.parent().ok_or(Error::InvalidRoot)?)?
                .sync_all()
                .map_err(|_| Error::Filesystem)?;
        }
        let digest = Digest256::from_bytes(Sha256::digest(&marker).into());
        let value = Self {
            directory,
            root: root.to_owned(),
            dataset,
            id,
            marker,
            digest,
        };
        value.verify()?;
        Ok(value)
    }
    /// Recheck the configured name and exact marker before accepting/resuming
    /// work. Mount/root replacement requires explicit reinitialization elsewhere;
    /// it never adopts old jobs under another path or inode.
    pub fn verify(&self) -> Result<(), Error> {
        let current = directory(&self.root)?;
        let a = self.directory.metadata().map_err(|_| Error::Filesystem)?;
        let b = current.metadata().map_err(|_| Error::Filesystem)?;
        private(&a, true)?;
        private(&b, true)?;
        if (a.dev(), a.ino()) != (b.dev(), b.ino())
            || !matches_marker(&self.directory, &self.marker)?
        {
            return Err(Error::Mismatch);
        }
        Ok(())
    }
}

/// A bounded local preflight; resident/retained bytes already reduce f_bavail.
/// Callers serialize their outstanding work and include its entire growth plus
/// allocation/metadata headroom. This does not replace a shared reservation ledger.
pub fn check_filesystem_growth(directory: &File, growth: u64, reserve: u64) -> Result<(), Error> {
    let stats = rustix::fs::fstatvfs(directory).map_err(|_| Error::Filesystem)?;
    if stats.f_frsize == 0 || stats.f_bavail > stats.f_blocks {
        return Err(Error::ResourceLimit);
    }
    let free = stats
        .f_bavail
        .checked_mul(stats.f_frsize)
        .ok_or(Error::ResourceLimit)?;
    if free.checked_sub(growth).is_none_or(|left| left < reserve) {
        return Err(Error::ResourceLimit);
    }
    Ok(())
}

pub fn validate_root(root: &Path) -> Result<(), Error> {
    let text = root.to_str().ok_or(Error::InvalidRoot)?;
    if !root.is_absolute()
        || text.len() > 8192
        || text == "/"
        || text.ends_with('/')
        || text.contains("//")
        || text.chars().any(char::is_control)
        || text.split('/').any(|p| matches!(p, "." | ".."))
    {
        return Err(Error::InvalidRoot);
    }
    Ok(())
}
fn directory(path: &Path) -> Result<File, Error> {
    let mut descriptor = open("/", DIRECTORY, Mode::empty()).map_err(|_| Error::Filesystem)?;
    for part in path.components().skip(1) {
        let Component::Normal(name) = part else {
            return Err(Error::InvalidRoot);
        };
        descriptor =
            openat(&descriptor, name, DIRECTORY, Mode::empty()).map_err(|_| Error::Filesystem)?;
    }
    Ok(File::from(descriptor))
}
pub(super) fn private(meta: &std::fs::Metadata, directory: bool) -> Result<(), Error> {
    if meta.uid() != rustix::process::geteuid().as_raw()
        || if directory {
            !meta.is_dir() || meta.mode() & 0o7777 != 0o700
        } else {
            !meta.is_file() || meta.mode() & 0o7777 != 0o600 || meta.nlink() != 1
        }
    {
        return Err(Error::Permissions);
    }
    Ok(())
}
fn matches_marker(parent: &File, expected: &[u8]) -> Result<bool, Error> {
    let fd = match openat(
        parent,
        MARKER,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC | OFlags::NOCTTY,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(rustix::io::Errno::NOENT) => return Ok(false),
        Err(_) => return Err(Error::Filesystem),
    };
    let file = File::from(fd);
    let meta = file.metadata().map_err(|_| Error::Filesystem)?;
    private(&meta, false)?;
    if meta.len() != expected.len() as u64 {
        return Err(Error::Mismatch);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(expected.len() + 1)
        .map_err(|_| Error::ResourceLimit)?;
    (&file)
        .take(expected.len() as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Filesystem)?;
    if bytes != expected {
        return Err(Error::Mismatch);
    }
    Ok(true)
}

#[cfg(test)]
mod capacity_tests {
    use super::*;
    #[test]
    fn filesystem_growth_never_consumes_the_completion_reserve() {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().canonicalize().unwrap();
        let destination = LocalDestination::initialize(
            &path,
            [1; 16].try_into().unwrap(),
            [2; 16].try_into().unwrap(),
        )
        .unwrap();
        assert_eq!(destination.check_growth(0, 0), Ok(()));
        assert_eq!(
            destination.check_growth(1, u64::MAX),
            Err(DestinationError::ResourceLimit)
        );
        assert_eq!(
            destination.check_growth(u64::MAX, 1),
            Err(DestinationError::ResourceLimit)
        );
    }
}
