//! Local custody of one retained archive attempt. Catalog exclusion and receipt
//! resolution remain separate from descriptor custody.
use crate::storage::postgres::{
    archive_attempt::{ArchiveAttemptReceipt, ArchiveAttemptRequest},
    catalog_guard::CatalogIdentity,
};
use command_fds::CommandFdExt;
use rustix::fs::{Mode, mkdirat};
use serde::Serialize;
use std::{
    cell::Cell,
    fs::File,
    future::Future,
    io::{Read, Write},
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    path::Path,
    process::Command,
};

const MAX_MANIFEST: usize = 32 * 1024;
const LOCK: &str = "custody.lock";
const MANIFEST: &str = "intent.json";

pub use crate::storage::custody::CustodyError as LifetimeError;

#[cfg(target_os = "linux")]
pub(super) use crate::storage::custody::bounded_proc;
pub(super) use crate::storage::custody::{
    acquire, create_file, directory, host_boot, open_directory, open_file, private_file,
    retain_connection, validate_lock,
};

/// Close-only lock custody. Never unlink/replace the journal or explicitly unlock
/// this open file description: the worker and catalog driver may still own it.
/// A recovered value can hold recovery exclusion, but cannot launch a child.
/// Trusted callers must bind every authority-bearing catalog driver before use.
/// This does not prove catalog rollback, file protection or whole-process death.
pub struct LocalAttempt {
    lock: File,
    request: ArchiveAttemptRequest,
    identity: CatalogIdentity,
    launch_available: Cell<bool>,
    recovered: bool,
}

impl LocalAttempt {
    /// Create and synchronize a new immutable local intent before provisioning
    /// any writer credential. Existing/partial attempts are preserved and refused.
    /// Initial host profile is Linux, same boot, local ext4 only. No automatic
    /// adoption of old journals, restored copies or another host's paths.
    pub fn create(
        root: &Path,
        receipt: &ArchiveAttemptReceipt,
        identity: &CatalogIdentity,
    ) -> Result<Self, LifetimeError> {
        let boot = host_boot(root)?;
        Self::create_for_boot(root, receipt.request(), identity, &boot)
    }

    /// Nonblocking custody reacquisition. Keep this value through fresh catalog
    /// resolution. Success excludes registered descriptor holders only, not an
    /// unregistered client or server-side SQL; never infer receipt absence here.
    pub fn recover(
        root: &Path,
        receipt: &ArchiveAttemptReceipt,
        identity: &CatalogIdentity,
    ) -> Result<Self, LifetimeError> {
        let boot = host_boot(root)?;
        Self::recover_for_boot(root, receipt.request(), identity, &boot)
    }

    /// Wrap the actual dedicated PostgreSQL Connection before spawning/polling
    /// it. Dropping a query future does not end that connection's queued COMMIT.
    /// The clone lives even in an unpolled connection future and closes only
    /// after that future completes or is destroyed. A JoinHandle alone is not
    /// custody: dropping it can detach a running transport.
    pub fn retain_catalog_connection<F: Future + Unpin>(
        &self,
        future: F,
    ) -> Result<impl Future<Output = F::Output> + use<F>, LifetimeError> {
        retain_connection(&self.lock, future)
    }

    pub(super) fn matches(
        &self,
        request: &ArchiveAttemptRequest,
        identity: &CatalogIdentity,
    ) -> bool {
        self.request == *request && self.identity == *identity && self.launch_available.get()
    }

    pub(crate) fn recovered_for(
        &self,
        request: &ArchiveAttemptRequest,
        identity: &CatalogIdentity,
    ) -> bool {
        self.recovered && self.request == *request && self.identity == *identity
    }

    /// Preserve a duplicate of the SAME open file description. An open of the
    /// same path would create a different lock owner and is not an alternative.
    /// The Command owns its clone until dropped; only one launch may be formed.
    pub(super) fn inherit(&self, command: &mut Command) -> Result<(), LifetimeError> {
        if !self.launch_available.replace(false) {
            return Err(LifetimeError::Identity);
        }
        let fd = self
            .lock
            .try_clone()
            .map_err(|_| LifetimeError::Filesystem)?;
        if fd.as_raw_fd() < 3 {
            return Err(LifetimeError::Filesystem);
        }
        command.preserved_fds(vec![fd.into()]);
        Ok(())
    }

    fn create_for_boot(
        root: &Path,
        request: &ArchiveAttemptRequest,
        identity: &CatalogIdentity,
        boot: &str,
    ) -> Result<Self, LifetimeError> {
        validate_identity(request, identity, boot)?;
        if root.starts_with(identity.root()) || Path::new(identity.root()).starts_with(root) {
            return Err(LifetimeError::InvalidRoot);
        }
        let parent = directory(root)?;
        let name = attempt_name(request);
        mkdirat(&parent, &name, Mode::RWXU).map_err(|e| {
            if e == rustix::io::Errno::EXIST {
                LifetimeError::Exists
            } else {
                LifetimeError::Filesystem
            }
        })?;
        let attempt = open_directory(&parent, &name)?;
        // Partial creation is deliberately never repaired or removed here. No
        // authority may be exposed before every sync below has succeeded.
        let file = create_file(&attempt, LOCK)?;
        file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        drop(file);
        let lock = open_file(&attempt, LOCK)?;
        validate_lock(&lock)?;
        acquire(&lock)?;
        // Publish the complete manifest only after acquiring the permanent lock.
        // Otherwise a recovery reader could overtake this still-live creator.
        let bytes = manifest(request, identity, boot, &lock)?;
        let mut file = create_file(&attempt, MANIFEST)?;
        file.write_all(&bytes)
            .map_err(|_| LifetimeError::Filesystem)?;
        file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        attempt.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        parent.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        Ok(Self {
            lock,
            request: request.clone(),
            identity: identity.clone(),
            launch_available: Cell::new(true),
            recovered: false,
        })
    }

    fn recover_for_boot(
        root: &Path,
        request: &ArchiveAttemptRequest,
        identity: &CatalogIdentity,
        boot: &str,
    ) -> Result<Self, LifetimeError> {
        validate_identity(request, identity, boot)?;
        if root.starts_with(identity.root()) || Path::new(identity.root()).starts_with(root) {
            return Err(LifetimeError::InvalidRoot);
        }
        let parent = directory(root)?;
        let attempt = open_directory(&parent, &attempt_name(request))?;
        let lock = open_file(&attempt, LOCK)?;
        validate_lock(&lock)?;
        acquire(&lock)?;
        let file = open_file(&attempt, MANIFEST)?;
        let metadata = file.metadata().map_err(|_| LifetimeError::Filesystem)?;
        if !private_file(&metadata) || metadata.len() == 0 {
            return Err(LifetimeError::Incomplete);
        }
        if metadata.len() > MAX_MANIFEST as u64 {
            return Err(LifetimeError::ResourceLimit);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(MAX_MANIFEST + 1)
            .map_err(|_| LifetimeError::ResourceLimit)?;
        file.take(MAX_MANIFEST as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| LifetimeError::Filesystem)?;
        if bytes.len() > MAX_MANIFEST {
            return Err(LifetimeError::ResourceLimit);
        }
        if bytes != manifest(request, identity, boot, &lock)? {
            return Err(LifetimeError::Identity);
        }
        Ok(Self {
            lock,
            request: request.clone(),
            identity: identity.clone(),
            launch_available: Cell::new(false),
            recovered: true,
        })
    }
}

fn attempt_name(request: &ArchiveAttemptRequest) -> String {
    use std::fmt::Write;
    let mut name = String::with_capacity(32);
    for byte in request.attempt_id() {
        write!(&mut name, "{byte:02x}").expect("String write");
    }
    name
}
fn validate_identity(
    request: &ArchiveAttemptRequest,
    identity: &CatalogIdentity,
    boot: &str,
) -> Result<(), LifetimeError> {
    let boot_valid = boot.len() == 36
        && boot.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
        && boot.bytes().any(|b| b != b'0' && b != b'-');
    if !boot_valid
        || request.dataset() != identity.dataset()
        || request.catalog() != identity.catalog()
        || CatalogIdentity::local(identity.dataset(), identity.catalog(), identity.root()).as_ref()
            != Ok(identity)
    {
        return Err(LifetimeError::Identity);
    }
    Ok(())
}
fn manifest(
    request: &ArchiveAttemptRequest,
    identity: &CatalogIdentity,
    boot: &str,
    lock: &File,
) -> Result<Vec<u8>, LifetimeError> {
    #[derive(Serialize)]
    struct Manifest<'a> {
        format: &'static str,
        boot_id: &'a str,
        lock_device: u64,
        lock_inode: u64,
        dataset_id: bigbrotr_model::DatasetId,
        catalog_id: bigbrotr_model::OwnerResourceId,
        catalog_root: &'a str,
        attempt_id: &'a [u8; 16],
        coordinator_incarnation: bigbrotr_model::ProcessIncarnation,
        worker_incarnation: bigbrotr_model::ProcessIncarnation,
        publication_ordinal: u64,
        topology_generation: bigbrotr_model::TopologyGeneration,
        batch_digest: bigbrotr_model::Digest256,
    }
    let metadata = lock.metadata().map_err(|_| LifetimeError::Filesystem)?;
    let value = Manifest {
        format: "bigbrotr-local-archive-custody/1",
        boot_id: boot,
        lock_device: metadata.dev(),
        lock_inode: metadata.ino(),
        dataset_id: request.dataset(),
        catalog_id: request.catalog(),
        catalog_root: identity.root(),
        attempt_id: request.attempt_id(),
        coordinator_incarnation: request.coordinator(),
        worker_incarnation: request.worker(),
        publication_ordinal: request.ordinal(),
        topology_generation: request.topology_generation(),
        batch_digest: request.batch_digest(),
    };
    let bytes = serde_json::to_vec(&value).map_err(|_| LifetimeError::ResourceLimit)?;
    if bytes.len() > MAX_MANIFEST {
        return Err(LifetimeError::ResourceLimit);
    }
    Ok(bytes)
}
#[cfg(test)]
mod tests;
