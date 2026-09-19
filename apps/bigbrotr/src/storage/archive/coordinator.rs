//! One explicitly bound state directory per initialized archive. The permanent
//! initialization lock also excludes coordinators before per-attempt custody exists.
use super::{LifetimeError, LocalInitialization, lifetime};
use crate::storage::postgres::catalog_guard::CatalogIdentity;
use bigbrotr_model::Digest256;
use rustix::fs::{Mode, OFlags, openat};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    future::Future,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

const BINDING: &str = "coordinator.json";
const MAX_BINDING: usize = 32 * 1024;

/// Keep this owner before Control issuance and until coordinator work and drivers
/// have joined. Per-attempt native/driver custody remains a separate obligation.
/// This excludes cooperating same-boot coordinators only: it neither adopts old
/// pre-protocol attempts nor resolves catalog transactions or missing journals.
pub struct LocalCoordinator {
    pub(super) custody: LocalInitialization,
    state_root: PathBuf,
    pub(super) directory: File,
    pub(super) initialization_sha256: [u8; 32],
    binding_digest: Digest256,
}

impl LocalCoordinator {
    /// Explicit finite binding. The state root must already be private and empty.
    /// Identical complete retries are allowed; partial bytes are never repaired.
    /// No engine connection or publication authority is established here.
    pub fn initialize(identity: &CatalogIdentity, root: &Path) -> Result<Self, LifetimeError> {
        lifetime::host_boot(root)?;
        Self::require_disjoint(root, Path::new(identity.root()))?;
        Self::from_custody(LocalInitialization::recover(identity)?, root, true)
    }

    /// Ordinary startup only reopens an existing two-ended binding. Never create
    /// missing state, even if the catalog or Control history appears empty.
    pub fn open(identity: &CatalogIdentity, root: &Path) -> Result<Self, LifetimeError> {
        lifetime::host_boot(root)?;
        Self::require_disjoint(root, Path::new(identity.root()))?;
        Self::from_custody(LocalInitialization::recover(identity)?, root, false)
    }

    pub fn state_root(&self) -> &Path {
        &self.state_root
    }

    /// Identity of the exact two-ended local binding, not a SQL capability.
    pub fn binding_digest(&self) -> Digest256 {
        self.binding_digest
    }

    pub fn identity(&self) -> &CatalogIdentity {
        self.custody.identity()
    }

    /// Compare actual mount-relative locations as well as canonical names.
    /// Deployment mount changes require quiescence and repeated preflight.
    pub fn require_disjoint(root: &Path, other: &Path) -> Result<(), LifetimeError> {
        super::placement::disjoint(root, other)
    }

    pub fn retain_connection<F: Future + Unpin>(
        &self,
        future: F,
    ) -> Result<impl Future<Output = F::Output> + use<F>, LifetimeError> {
        self.custody.retain_catalog_connection(future)
    }

    pub(super) fn from_custody(
        custody: LocalInitialization,
        root: &Path,
        initialize: bool,
    ) -> Result<Self, LifetimeError> {
        let identity = custody.identity();
        if root.starts_with(identity.root()) || Path::new(identity.root()).starts_with(root) {
            return Err(LifetimeError::InvalidRoot);
        }
        let directory = lifetime::directory(root)?;
        let (anchor, initialization_sha256) = custody.coordinator_binding()?;
        let metadata = directory
            .metadata()
            .map_err(|_| LifetimeError::Filesystem)?;
        #[derive(Serialize)]
        struct Binding<'a> {
            format: &'static str,
            initialization_sha256: [u8; 32],
            state_root: &'a str,
            state_device: u64,
            state_inode: u64,
        }
        // The initialization hash transitively binds dataset/catalog/data root,
        // boot, nonce and the exact permanent lock/root device and inode pairs.
        let path = root
            .to_str()
            .filter(|p| p.len() <= 8192)
            .ok_or(LifetimeError::InvalidRoot)?;
        let expected = serde_json::to_vec(&Binding {
            format: "bigbrotr-local-coordinator/1",
            initialization_sha256,
            state_root: path,
            state_device: metadata.dev(),
            state_inode: metadata.ino(),
        })
        .map_err(|_| LifetimeError::ResourceLimit)?;
        if expected.len() > MAX_BINDING {
            return Err(LifetimeError::ResourceLimit);
        }
        // Read the reverse binding first: a different configured root must not
        // acquire independent authority or even receive a new marker.
        let anchored = verify(&anchor, &expected)?;
        if !anchored && !initialize {
            return Err(LifetimeError::Incomplete);
        }
        let stored = verify(&directory, &expected)?;
        if !anchored {
            // Retry a fully written first half only. No unrelated directory or
            // pre-existing attempt history may be adopted as fresh local state.
            let mut entries = fs::read_dir(root).map_err(|_| LifetimeError::Filesystem)?;
            if stored {
                let entry = entries
                    .next()
                    .ok_or(LifetimeError::Incomplete)?
                    .map_err(|_| LifetimeError::Filesystem)?;
                if entry.file_name() != BINDING || entries.next().is_some() {
                    return Err(LifetimeError::Exists);
                }
            } else if entries.next().is_some() {
                return Err(LifetimeError::Exists);
            }
            if !stored {
                write(&directory, &expected)?;
            }
            directory
                .sync_all()
                .map_err(|_| LifetimeError::Filesystem)?;
            write(&anchor, &expected)?;
        } else if !stored {
            return Err(LifetimeError::Incomplete);
        }
        // A retry may have observed complete bytes just before a prior crash.
        // Verify syncs both files; close the directory durability boundary too.
        directory
            .sync_all()
            .map_err(|_| LifetimeError::Filesystem)?;
        anchor.sync_all().map_err(|_| LifetimeError::Filesystem)?;
        Ok(Self {
            custody,
            state_root: root.to_owned(),
            directory,
            initialization_sha256,
            binding_digest: Digest256::from_bytes(Sha256::digest(&expected).into()),
        })
    }
}

fn verify(parent: &File, expected: &[u8]) -> Result<bool, LifetimeError> {
    let fd = match openat(
        parent,
        BINDING,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(rustix::io::Errno::NOENT) => return Ok(false),
        Err(_) => return Err(LifetimeError::Incomplete),
    };
    let file = File::from(fd);
    let metadata = file.metadata().map_err(|_| LifetimeError::Filesystem)?;
    if !lifetime::private_file(&metadata) || metadata.len() == 0 {
        return Err(LifetimeError::Incomplete);
    }
    if metadata.len() > MAX_BINDING as u64 {
        return Err(LifetimeError::ResourceLimit);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(MAX_BINDING + 1)
        .map_err(|_| LifetimeError::ResourceLimit)?;
    (&file)
        .take(MAX_BINDING as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| LifetimeError::Filesystem)?;
    if bytes.len() > MAX_BINDING {
        return Err(LifetimeError::ResourceLimit);
    }
    if bytes != expected {
        return Err(LifetimeError::Identity);
    }
    file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
    Ok(true)
}

fn write(parent: &File, bytes: &[u8]) -> Result<(), LifetimeError> {
    let mut file = lifetime::create_file(parent, BINDING)?;
    file.write_all(bytes)
        .map_err(|_| LifetimeError::Filesystem)?;
    file.sync_all().map_err(|_| LifetimeError::Filesystem)?;
    parent.sync_all().map_err(|_| LifetimeError::Filesystem)
}
