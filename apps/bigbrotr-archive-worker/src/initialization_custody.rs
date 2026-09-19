//! Private descriptor/journal handoff for reserved initialization. The trusted
//! parent created and locked this reservation before exposing bootstrap authority.
use crate::catalog::{BootstrapRequest, CatalogError};
use rustix::fs::{Mode, OFlags, openat};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, Metadata},
    io::Read,
    os::unix::fs::MetadataExt,
    path::Path,
};

const DIRECTORY: &str = ".bigbrotr-initialization";
const MAX_INTENT: u64 = 32768;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservedRequest {
    dsn: String,
    data_root: String,
    custody: NativeCustody,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeCustody {
    custody_fd: i32,
    lock_device: u64,
    lock_inode: u64,
    root_device: u64,
    root_inode: u64,
    journal_sha256: [u8; 32],
}
impl ReservedRequest {
    pub fn read(input: impl Read) -> Result<Self, CatalogError> {
        let mut bytes = Vec::new();
        input
            .take(32769)
            .read_to_end(&mut bytes)
            .map_err(|_| CatalogError::Input)?;
        if bytes.len() > 32768 {
            return Err(CatalogError::InvalidInput);
        }
        serde_json::from_slice(&bytes).map_err(|_| CatalogError::InvalidInput)
    }
    pub(crate) fn validate(self) -> Result<BootstrapRequest, CatalogError> {
        let request = BootstrapRequest::from_reserved(self.dsn, self.data_root)?;
        self.custody.validate(Path::new(request.data_root()))?;
        Ok(request)
    }
}
impl NativeCustody {
    fn validate(&self, root: &Path) -> Result<(), CatalogError> {
        let invalid = || CatalogError::InvalidDataRoot;
        if self.custody_fd < 3 || root.canonicalize().ok().as_deref() != Some(root) {
            return Err(invalid());
        }
        let root_file = File::from(
            rustix::fs::open(
                root,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| invalid())?,
        );
        let metadata = root_file.metadata().map_err(|_| invalid())?;
        if !private_directory(&metadata)
            || (metadata.dev(), metadata.ino()) != (self.root_device, self.root_inode)
        {
            return Err(invalid());
        }
        entries(root, &[DIRECTORY])?;
        let directory = File::from(
            openat(
                &root_file,
                DIRECTORY,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| invalid())?,
        );
        if !private_directory(&directory.metadata().map_err(|_| invalid())?) {
            return Err(invalid());
        }
        entries(&root.join(DIRECTORY), &["custody.lock", "intent.json"])?;
        let lock = open_file(&directory, "custody.lock")?;
        let lock_metadata = lock.metadata().map_err(|_| invalid())?;
        // /proc observes the original inherited descriptor without introducing
        // unsafe raw-fd ownership or reopening it as a new custody owner.
        let inherited =
            fs::metadata(format!("/proc/self/fd/{}", self.custody_fd)).map_err(|_| invalid())?;
        for m in [&lock_metadata, &inherited] {
            if !private_file(m)
                || m.len() != 0
                || (m.dev(), m.ino()) != (self.lock_device, self.lock_inode)
            {
                return Err(invalid());
            }
        }
        let journal = open_file(&directory, "intent.json")?;
        let m = journal.metadata().map_err(|_| invalid())?;
        if !private_file(&m) || m.len() == 0 || m.len() > MAX_INTENT {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        journal
            .take(MAX_INTENT + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid())?;
        if bytes.len() as u64 > MAX_INTENT
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != self.journal_sha256
        {
            return Err(invalid());
        }
        Ok(())
    }
}
fn private_directory(m: &Metadata) -> bool {
    m.is_dir() && m.mode() & 0o777 == 0o700 && m.uid() == rustix::process::geteuid().as_raw()
}
fn private_file(m: &Metadata) -> bool {
    m.is_file()
        && m.mode() & 0o777 == 0o600
        && m.nlink() == 1
        && m.uid() == rustix::process::geteuid().as_raw()
}
fn open_file(parent: &File, name: &str) -> Result<File, CatalogError> {
    openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| CatalogError::InvalidDataRoot)
}
fn entries(path: &Path, expected: &[&str]) -> Result<(), CatalogError> {
    let found = fs::read_dir(path)
        .map_err(|_| CatalogError::InvalidDataRoot)?
        .take(expected.len() + 1)
        .map(|e| e.map(|e| e.file_name()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CatalogError::InvalidDataRoot)?;
    if found.len() != expected.len()
        || expected
            .iter()
            .any(|name| !found.iter().any(|actual| actual == name))
    {
        return Err(CatalogError::InvalidDataRoot);
    }
    Ok(())
}
