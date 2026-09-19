//! Permanent local reservation for explicit catalog initialization. Database
//! reservation, generated credentials and server transaction resolution are separate.
use super::{LifetimeError, lifetime};
use crate::storage::postgres::catalog_guard::CatalogIdentity;
use command_fds::CommandFdExt;
use rustix::fs::{Mode, mkdirat};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    cell::Cell,
    fs::{self, File},
    future::Future,
    io::{Read, Write},
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    path::Path,
    process::Command,
};

const DIRECTORY: &str = ".bigbrotr-initialization";
const LOCK: &str = "custody.lock";
const INTENT: &str = "intent.json";
const MAX_INTENT: usize = 32 * 1024;

/// Close-only custody inside the actual archive root. Never remove/replace its
/// marker or explicitly unlock it. A recovered holder cannot launch a child.
pub struct LocalInitialization {
    lock: File,
    identity: CatalogIdentity,
    initialization_id: [u8; 16],
    journal_sha256: [u8; 32],
    root_device: u64,
    root_inode: u64,
    launch_available: Cell<bool>,
}

#[derive(Clone, Copy, Serialize)]
pub(super) struct NativeCustody {
    pub(super) custody_fd: i32,
    pub(super) lock_device: u64,
    pub(super) lock_inode: u64,
    pub(super) root_device: u64,
    pub(super) root_inode: u64,
    pub(super) journal_sha256: [u8; 32],
}

impl LocalInitialization {
    pub fn create(
        identity: &CatalogIdentity,
        initialization_id: [u8; 16],
    ) -> Result<Self, LifetimeError> {
        let boot = lifetime::host_boot(Path::new(identity.root()))?;
        Self::create_for_boot(identity, initialization_id, &boot)
    }
    /// Reacquire the same-boot recorded custody. This is not database rollback,
    /// bootstrap exclusion or permission to provision another credential/child.
    pub fn recover(identity: &CatalogIdentity) -> Result<Self, LifetimeError> {
        let boot = lifetime::host_boot(Path::new(identity.root()))?;
        Self::recover_for_boot(identity, &boot)
    }
    pub fn initialization_id(&self) -> &[u8; 16] {
        &self.initialization_id
    }
    pub fn identity(&self) -> &CatalogIdentity {
        &self.identity
    }
    pub fn retain_catalog_connection<F: Future + Unpin>(
        &self,
        future: F,
    ) -> Result<impl Future<Output = F::Output> + use<F>, LifetimeError> {
        lifetime::retain_connection(&self.lock, future)
    }
    pub(super) fn inherit(&self, command: &mut Command) -> Result<NativeCustody, LifetimeError> {
        if !self.launch_available.replace(false) {
            return Err(LifetimeError::Identity);
        }
        let fd = self
            .lock
            .try_clone()
            .map_err(|_| LifetimeError::Filesystem)?;
        let custody_fd = fd.as_raw_fd();
        if custody_fd < 3 {
            return Err(LifetimeError::Filesystem);
        }
        let proof = self.native_custody(custody_fd)?;
        command.preserved_fds(vec![fd.into()]);
        Ok(proof)
    }
    pub(super) fn native_custody(&self, custody_fd: i32) -> Result<NativeCustody, LifetimeError> {
        let metadata = self
            .lock
            .metadata()
            .map_err(|_| LifetimeError::Filesystem)?;
        Ok(NativeCustody {
            custody_fd,
            lock_device: metadata.dev(),
            lock_inode: metadata.ino(),
            root_device: self.root_device,
            root_inode: self.root_inode,
            journal_sha256: self.journal_sha256,
        })
    }
    fn create_for_boot(
        identity: &CatalogIdentity,
        id: [u8; 16],
        boot: &str,
    ) -> Result<Self, LifetimeError> {
        validate(identity, id, boot)?;
        let root = Path::new(identity.root());
        let parent = lifetime::directory(root)?;
        if fs::read_dir(root)
            .map_err(|_| LifetimeError::Filesystem)?
            .next()
            .is_some()
        {
            return Err(LifetimeError::Exists);
        }
        mkdirat(&parent, DIRECTORY, Mode::RWXU).map_err(|e| {
            if e == rustix::io::Errno::EXIST {
                LifetimeError::Exists
            } else {
                LifetimeError::Filesystem
            }
        })?;
        let directory = lifetime::open_directory(&parent, DIRECTORY)?;
        let file = lifetime::create_file(&directory, LOCK)?;
        file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        drop(file);
        let lock = lifetime::open_file(&directory, LOCK)?;
        lifetime::validate_lock(&lock)?;
        lifetime::acquire(&lock)?;
        // The complete journal is visible only after the permanent lock is held.
        // Every partial failure preserves the reservation and exposes no authority.
        let bytes = manifest(identity, id, boot, &parent, &lock)?;
        let mut file = lifetime::create_file(&directory, INTENT)?;
        file.write_all(&bytes)
            .map_err(|_| LifetimeError::Filesystem)?;
        file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        directory
            .sync_all()
            .map_err(|_| LifetimeError::Filesystem)?;
        parent.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        Self::held(identity, id, &bytes, parent, lock, true)
    }
    fn recover_for_boot(identity: &CatalogIdentity, boot: &str) -> Result<Self, LifetimeError> {
        let parent = lifetime::directory(Path::new(identity.root()))?;
        let directory = lifetime::open_directory(&parent, DIRECTORY)?;
        let lock = lifetime::open_file(&directory, LOCK)?;
        lifetime::validate_lock(&lock)?;
        lifetime::acquire(&lock)?;
        let file = lifetime::open_file(&directory, INTENT)?;
        let metadata = file.metadata().map_err(|_| LifetimeError::Filesystem)?;
        if !lifetime::private_file(&metadata) || metadata.len() == 0 {
            return Err(LifetimeError::Incomplete);
        }
        if metadata.len() > MAX_INTENT as u64 {
            return Err(LifetimeError::ResourceLimit);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(MAX_INTENT + 1)
            .map_err(|_| LifetimeError::ResourceLimit)?;
        file.take(MAX_INTENT as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| LifetimeError::Filesystem)?;
        if bytes.len() > MAX_INTENT {
            return Err(LifetimeError::ResourceLimit);
        }
        // Discover only the retained nonce. Every other field, ordering, duplicate
        // and extra byte is checked against a freshly reconstructed full journal.
        #[derive(Deserialize)]
        struct StoredIdentity {
            initialization_id: [u8; 16],
        }
        let stored: StoredIdentity =
            serde_json::from_slice(&bytes).map_err(|_| LifetimeError::Identity)?;
        validate(identity, stored.initialization_id, boot)?;
        if bytes != manifest(identity, stored.initialization_id, boot, &parent, &lock)? {
            return Err(LifetimeError::Identity);
        }
        Self::held(
            identity,
            stored.initialization_id,
            &bytes,
            parent,
            lock,
            false,
        )
    }
    fn held(
        identity: &CatalogIdentity,
        id: [u8; 16],
        bytes: &[u8],
        parent: File,
        lock: File,
        launch: bool,
    ) -> Result<Self, LifetimeError> {
        let metadata = parent.metadata().map_err(|_| LifetimeError::Filesystem)?;
        Ok(Self {
            lock,
            identity: identity.clone(),
            initialization_id: id,
            journal_sha256: Sha256::digest(bytes).into(),
            root_device: metadata.dev(),
            root_inode: metadata.ino(),
            launch_available: Cell::new(launch),
        })
    }
}
fn validate(identity: &CatalogIdentity, id: [u8; 16], boot: &str) -> Result<(), LifetimeError> {
    if id == [0; 16]
        || CatalogIdentity::local(identity.dataset(), identity.catalog(), identity.root()).as_ref()
            != Ok(identity)
        || boot.len() != 36
        || !boot.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
        || !boot.bytes().any(|b| b != b'0' && b != b'-')
    {
        return Err(LifetimeError::Identity);
    }
    Ok(())
}
fn manifest(
    identity: &CatalogIdentity,
    id: [u8; 16],
    boot: &str,
    root: &File,
    lock: &File,
) -> Result<Vec<u8>, LifetimeError> {
    #[derive(Serialize)]
    struct Manifest<'a> {
        format: &'static str,
        initialization_id: [u8; 16],
        dataset_id: bigbrotr_model::DatasetId,
        catalog_id: bigbrotr_model::OwnerResourceId,
        catalog_root: &'a str,
        boot_id: &'a str,
        root_device: u64,
        root_inode: u64,
        lock_device: u64,
        lock_inode: u64,
    }
    let root = root.metadata().map_err(|_| LifetimeError::Filesystem)?;
    let lock = lock.metadata().map_err(|_| LifetimeError::Filesystem)?;
    let value = Manifest {
        format: "bigbrotr-local-initialization/1",
        initialization_id: id,
        dataset_id: identity.dataset(),
        catalog_id: identity.catalog(),
        catalog_root: identity.root(),
        boot_id: boot,
        root_device: root.dev(),
        root_inode: root.ino(),
        lock_device: lock.dev(),
        lock_inode: lock.ino(),
    };
    let bytes = serde_json::to_vec(&value).map_err(|_| LifetimeError::ResourceLimit)?;
    if bytes.len() > MAX_INTENT {
        return Err(LifetimeError::ResourceLimit);
    }
    Ok(bytes)
}
#[cfg(test)]
mod tests;
