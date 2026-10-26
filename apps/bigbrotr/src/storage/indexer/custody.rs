//! Same-host custody for the initial immutable ClickHouse projection.
//! This excludes registered local senders, not server-side queries. Recovery
//! separately verifies/replays their retained immutable plans before publishing.
pub use crate::storage::custody::CustodyError;
use crate::storage::{
    custody,
    postgres::{IndexBinding, IndexProvenance},
};
use bigbrotr_model::{
    DatasetId, Digest256, OwnerResourceId, ProcessIncarnation, ProjectionGenerationId,
};
use rustix::fs::{Mode, mkdirat};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    borrow::Cow,
    fs::File,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

const DIRECTORY: &str = ".bigbrotr-index-custody";
const LOCK: &str = "custody.lock";
const MANIFEST: &str = "identity.json";
const MAX_MANIFEST: usize = 32 * 1024;
const TABLES: &str = "tables.json";
pub(super) const MAX_TABLE_MANIFEST_BYTES: usize = 4096;

/// A close-only lock, bound to the retained projection and physical local root.
/// Never remove its journal or explicitly unlock its open file description.
/// It grants no PostgreSQL authority and certifies no ClickHouse contents.
pub struct HeldIndexCustody {
    root: PathBuf,
    root_directory: File,
    journal_directory: File,
    lock: File,
    binding: IndexBinding,
    manifest: Vec<u8>,
}

impl HeldIndexCustody {
    /// Reserve a fresh journal on the supported Linux/local-ext4 host profile.
    /// Existing and partial journals are retained and refused, never repaired.
    pub fn create(
        root: &Path,
        dataset: DatasetId,
        resource: OwnerResourceId,
        generation: ProjectionGenerationId,
        storage_incarnation: ProcessIncarnation,
        table_identity_digest: Digest256,
        provenance: IndexProvenance,
    ) -> Result<Self, CustodyError> {
        let boot = custody::host_boot(root)?;
        Self::create_for_boot(
            root,
            dataset,
            resource,
            generation,
            storage_incarnation,
            table_identity_digest,
            provenance,
            &boot,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_for_boot(
        root: &Path,
        dataset: DatasetId,
        resource: OwnerResourceId,
        generation: ProjectionGenerationId,
        storage_incarnation: ProcessIncarnation,
        table_identity_digest: Digest256,
        provenance: IndexProvenance,
        boot: &str,
    ) -> Result<Self, CustodyError> {
        let parent = custody::directory(root)?;
        mkdirat(&parent, DIRECTORY, Mode::RWXU).map_err(|e| {
            if e == rustix::io::Errno::EXIST {
                CustodyError::Exists
            } else {
                CustodyError::Filesystem
            }
        })?;
        let journal = custody::open_directory(&parent, DIRECTORY)?;
        let file = custody::create_file(&journal, LOCK)?;
        file.sync_all().map_err(|_| CustodyError::Filesystem)?;
        drop(file);
        let lock = custody::open_file(&journal, LOCK)?;
        custody::validate_lock(&lock)?;
        custody::acquire(&lock)?;
        let identity = Identity {
            format: "bigbrotr-local-index-custody/1".into(),
            root: root.to_str().ok_or(CustodyError::InvalidRoot)?.into(),
            boot_id: boot.into(),
            root_identity: FileIdentity::of(&parent)?,
            journal_identity: FileIdentity::of(&journal)?,
            lock_identity: FileIdentity::of(&lock)?,
            dataset,
            resource,
            generation,
            storage_incarnation,
            table_identity_digest,
            provenance: provenance.as_str().into(),
        };
        let manifest = identity.encode()?;
        let binding = IndexBinding::new(
            dataset,
            resource,
            generation,
            storage_incarnation,
            table_identity_digest,
            Digest256::from_bytes(Sha256::digest(&manifest).into()),
            provenance,
        )
        .map_err(|_| CustodyError::Identity)?;
        let mut file = custody::create_file(&journal, MANIFEST)?;
        file.write_all(&manifest)
            .map_err(|_| CustodyError::Filesystem)?;
        file.sync_all().map_err(|_| CustodyError::Filesystem)?;
        journal.sync_all().map_err(|_| CustodyError::Filesystem)?;
        parent.sync_all().map_err(|_| CustodyError::Filesystem)?;
        Ok(Self {
            root: root.into(),
            root_directory: parent,
            journal_directory: journal,
            lock,
            binding,
            manifest,
        })
    }

    /// Reacquire exactly the recorded journal. A different host boot, directory,
    /// lock inode or projection binding needs explicit recovery/reconstruction;
    /// absence and expiry never create a replacement identity here.
    pub fn recover(root: &Path, binding: &IndexBinding) -> Result<Self, CustodyError> {
        let boot = custody::host_boot(root)?;
        Self::recover_for_boot(root, binding, &boot)
    }

    /// Reopen the retained binding under its actual lock. This is local custody,
    /// not PG authority or proof of the remote tables. The caller still verifies
    /// its closed table manifest and the complete authoritative PG/CH lineage.
    pub fn recover_stored(root: &Path) -> Result<Self, CustodyError> {
        let boot = custody::host_boot(root)?;
        Self::recover_stored_for_boot(root, &boot)
    }

    fn recover_for_boot(
        root: &Path,
        binding: &IndexBinding,
        boot: &str,
    ) -> Result<Self, CustodyError> {
        let retained = Self::recover_stored_for_boot(root, boot)?;
        if retained.binding() != binding {
            return Err(CustodyError::Identity);
        }
        Ok(retained)
    }

    fn recover_stored_for_boot(root: &Path, boot: &str) -> Result<Self, CustodyError> {
        let parent = custody::directory(root)?;
        let journal = custody::open_directory(&parent, DIRECTORY)?;
        let lock = custody::open_file(&journal, LOCK)?;
        custody::validate_lock(&lock)?;
        custody::acquire(&lock)?;
        let manifest = read_manifest(&journal)?;
        let recorded: Identity<'_> =
            serde_json::from_slice(&manifest).map_err(|_| CustodyError::Identity)?;
        let provenance = match recorded.provenance.as_ref() {
            "local_first_witness" => IndexProvenance::LocalFirstWitness,
            "off" => IndexProvenance::Off,
            _ => return Err(CustodyError::Identity),
        };
        let binding = IndexBinding::new(
            recorded.dataset,
            recorded.resource,
            recorded.generation,
            recorded.storage_incarnation,
            recorded.table_identity_digest,
            Digest256::from_bytes(Sha256::digest(&manifest).into()),
            provenance,
        )
        .map_err(|_| CustodyError::Identity)?;
        let expected = Identity {
            format: "bigbrotr-local-index-custody/1".into(),
            root: root.to_str().ok_or(CustodyError::InvalidRoot)?.into(),
            boot_id: boot.into(),
            root_identity: FileIdentity::of(&parent)?,
            journal_identity: FileIdentity::of(&journal)?,
            lock_identity: FileIdentity::of(&lock)?,
            dataset: binding.dataset(),
            resource: binding.resource(),
            generation: binding.generation(),
            storage_incarnation: binding.storage_incarnation(),
            table_identity_digest: binding.table_identity_digest(),
            provenance: binding.provenance().as_str().into(),
        }
        .encode()?;
        if manifest != expected {
            return Err(CustodyError::Identity);
        }
        Ok(Self {
            root: root.into(),
            root_directory: parent,
            journal_directory: journal,
            lock,
            binding,
            manifest: expected,
        })
    }

    /// Save only the closed, caller-validated IndexTables manifest. Exclusive
    /// creation preserves partial attempts; initialization must not dispatch any
    /// remote effect until this file and its parent are durably synchronized.
    pub(super) fn create_table_manifest(&self, bytes: &[u8]) -> Result<(), CustodyError> {
        if bytes.is_empty() || bytes.len() > MAX_TABLE_MANIFEST_BYTES {
            return Err(CustodyError::ResourceLimit);
        }
        self.check_live()?;
        let mut file = custody::create_file(&self.journal_directory, TABLES)?;
        file.write_all(bytes)
            .map_err(|_| CustodyError::Filesystem)?;
        file.sync_all().map_err(|_| CustodyError::Filesystem)?;
        self.journal_directory
            .sync_all()
            .map_err(|_| CustodyError::Filesystem)?;
        self.check_live()
    }

    pub(super) fn read_table_manifest(&self) -> Result<Vec<u8>, CustodyError> {
        self.check_live()?;
        let (file, bytes) =
            read_private(&self.journal_directory, TABLES, MAX_TABLE_MANIFEST_BYTES)?;
        // A complete readable file may have survived process death before the
        // original fsync. Synchronize this exact read descriptor and parent
        // before any caller can resume remote effects using these bytes.
        file.sync_all().map_err(|_| CustodyError::Filesystem)?;
        self.journal_directory
            .sync_all()
            .map_err(|_| CustodyError::Filesystem)?;
        self.check_live()?;
        Ok(bytes)
    }

    pub fn binding(&self) -> &IndexBinding {
        &self.binding
    }

    /// Check the actual held descriptors and reachable immutable journal before
    /// durable authority transitions. A renamed/replaced path is not this store.
    pub fn check_live(&self) -> Result<(), CustodyError> {
        custody::validate_lock(&self.lock)?;
        let root = custody::directory(&self.root)?;
        let journal = custody::open_directory(&root, DIRECTORY)?;
        let lock = custody::open_file(&journal, LOCK)?;
        custody::validate_lock(&lock)?;
        if FileIdentity::of(&root)? != FileIdentity::of(&self.root_directory)?
            || FileIdentity::of(&journal)? != FileIdentity::of(&self.journal_directory)?
            || FileIdentity::of(&lock)? != FileIdentity::of(&self.lock)?
            || read_manifest(&journal)? != self.manifest
        {
            return Err(CustodyError::Identity);
        }
        Ok(())
    }

    /// Only the owning Index transport lane may retain this descriptor. It must
    /// outlive all request futures and their actual executor, including hidden
    /// client tasks; dropping a JoinHandle is not transport cessation.
    pub(in crate::storage) fn retain_lane(&self) -> Result<File, CustodyError> {
        self.check_live()?;
        self.lock.try_clone().map_err(|_| CustodyError::Filesystem)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileIdentity {
    device: u64,
    inode: u64,
}
impl FileIdentity {
    fn of(file: &File) -> Result<Self, CustodyError> {
        let metadata = file.metadata().map_err(|_| CustodyError::Filesystem)?;
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity<'a> {
    #[serde(borrow)]
    format: Cow<'a, str>,
    #[serde(borrow)]
    root: Cow<'a, str>,
    #[serde(borrow)]
    boot_id: Cow<'a, str>,
    root_identity: FileIdentity,
    journal_identity: FileIdentity,
    lock_identity: FileIdentity,
    dataset: DatasetId,
    resource: OwnerResourceId,
    generation: ProjectionGenerationId,
    storage_incarnation: ProcessIncarnation,
    table_identity_digest: Digest256,
    #[serde(borrow)]
    provenance: Cow<'a, str>,
}
impl Identity<'_> {
    fn encode(&self) -> Result<Vec<u8>, CustodyError> {
        let bytes = serde_json::to_vec(self).map_err(|_| CustodyError::ResourceLimit)?;
        if bytes.len() > MAX_MANIFEST {
            return Err(CustodyError::ResourceLimit);
        }
        Ok(bytes)
    }
}

fn read_manifest(journal: &File) -> Result<Vec<u8>, CustodyError> {
    read_private(journal, MANIFEST, MAX_MANIFEST).map(|(_, bytes)| bytes)
}

fn read_private(
    journal: &File,
    name: &str,
    maximum: usize,
) -> Result<(File, Vec<u8>), CustodyError> {
    let file = custody::open_file(journal, name)?;
    let metadata = file.metadata().map_err(|_| CustodyError::Filesystem)?;
    if !custody::private_file(&metadata) || metadata.len() == 0 {
        return Err(CustodyError::Incomplete);
    }
    if metadata.len() > maximum as u64 {
        return Err(CustodyError::ResourceLimit);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(maximum + 1)
        .map_err(|_| CustodyError::ResourceLimit)?;
    (&file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| CustodyError::Filesystem)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(CustodyError::ResourceLimit);
    }
    Ok((file, bytes))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        time::{Duration, Instant},
    };

    pub(in crate::storage::indexer) const BOOT: &str = "01234567-89ab-cdef-0123-456789abcdef";

    fn create(root: &Path) -> Result<HeldIndexCustody, CustodyError> {
        create_bound(root, Digest256::from_bytes([5; 32]))
    }
    pub(in crate::storage::indexer) fn create_bound(
        root: &Path,
        table_identity: Digest256,
    ) -> Result<HeldIndexCustody, CustodyError> {
        HeldIndexCustody::create_for_boot(
            root,
            [1; 16].try_into().unwrap(),
            [2; 16].try_into().unwrap(),
            [3; 16].try_into().unwrap(),
            [4; 16].try_into().unwrap(),
            table_identity,
            IndexProvenance::LocalFirstWitness,
            BOOT,
        )
    }
    pub(in crate::storage::indexer) fn root() -> (tempfile::TempDir, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().canonicalize().unwrap();
        (root, path)
    }
    pub(in crate::storage::indexer) fn recover_released(
        root: &Path,
        binding: &IndexBinding,
        boot: &str,
    ) -> Result<HeldIndexCustody, CustodyError> {
        // Concurrent test forks can retain CLOEXEC descriptors until exec. Wait
        // only for actual reacquisition; never substitute time for exclusion.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match HeldIndexCustody::recover_for_boot(root, binding, boot) {
                Err(CustodyError::Occupied) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(2))
                }
                result => return result,
            }
        }
    }

    #[test]
    fn retained_lane_excludes_successor_and_recovery_preserves_binding() {
        let (_root, path) = root();
        let custody = create(&path).unwrap();
        let binding = *custody.binding();
        custody.check_live().unwrap();
        assert_eq!(create(&path).err(), Some(CustodyError::Exists));
        let lane = custody.retain_lane().unwrap();
        drop(custody);
        assert_eq!(
            HeldIndexCustody::recover_for_boot(&path, &binding, BOOT).err(),
            Some(CustodyError::Occupied)
        );
        drop(lane);
        let recovered = recover_released(&path, &binding, BOOT).unwrap();
        assert_eq!(recovered.binding(), &binding);
        recovered.check_live().unwrap();
        drop(recovered);
        assert_eq!(
            recover_released(&path, &binding, "fedcba98-7654-3210-fedc-ba9876543210").err(),
            Some(CustodyError::Identity)
        );
    }

    #[test]
    fn changed_manifest_and_replaced_lock_never_authorize_current_or_recovered_owner() {
        let (_root, path) = root();
        let custody = create(&path).unwrap();
        let binding = *custody.binding();
        let manifest = path.join(DIRECTORY).join(MANIFEST);
        let original = fs::read(&manifest).unwrap();
        fs::write(&manifest, b"{}").unwrap();
        assert_eq!(custody.check_live(), Err(CustodyError::Identity));
        drop(custody);
        assert_eq!(
            recover_released(&path, &binding, BOOT).err(),
            Some(CustodyError::Identity)
        );
        fs::write(&manifest, original).unwrap();
        let custody = recover_released(&path, &binding, BOOT).unwrap();
        let lock = path.join(DIRECTORY).join(LOCK);
        fs::rename(&lock, lock.with_extension("original")).unwrap();
        let replacement = File::create(&lock).unwrap();
        replacement
            .set_permissions(fs::Permissions::from_mode(0o600))
            .unwrap();
        assert_eq!(custody.check_live(), Err(CustodyError::Identity));
        assert_eq!(
            HeldIndexCustody::recover_for_boot(&path, &binding, BOOT).err(),
            Some(CustodyError::Identity)
        );
    }

    #[test]
    fn incomplete_journal_is_preserved_and_never_recreated() {
        let (_root, path) = root();
        let custody = create(&path).unwrap();
        let binding = *custody.binding();
        drop(custody);
        let manifest = path.join(DIRECTORY).join(MANIFEST);
        fs::write(&manifest, []).unwrap();
        assert_eq!(
            recover_released(&path, &binding, BOOT).err(),
            Some(CustodyError::Incomplete)
        );
        assert_eq!(create(&path).err(), Some(CustodyError::Exists));
        assert!(manifest.exists());
    }

    #[test]
    fn stored_identity_and_exclusive_table_manifest_survive_reopen() {
        let (_parent, parent) = root();
        // JSON escapes are legitimate path bytes, not a reason to lose a valid
        // retained store when recovering without in-memory binding state.
        let path = parent.join("quoted-\"-and-backslash-\\");
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        let custody = create(&path).unwrap();
        let binding = *custody.binding();
        assert_eq!(
            HeldIndexCustody::recover_stored_for_boot(&path, BOOT).err(),
            Some(CustodyError::Occupied)
        );
        assert_eq!(
            custody.create_table_manifest(b""),
            Err(CustodyError::ResourceLimit)
        );
        // This tests file custody only; the public Index state owner separately
        // validates the closed physical table manifest and its binding digest.
        let bytes = br#"{"fixture":"retained table bytes"}"#;
        custody.create_table_manifest(bytes).unwrap();
        assert_eq!(custody.read_table_manifest().unwrap(), bytes);
        assert_eq!(
            custody.create_table_manifest(b"replacement"),
            Err(CustodyError::Filesystem)
        );
        drop(custody);
        let retained = recover_released(&path, &binding, BOOT).unwrap();
        assert_eq!(retained.binding(), &binding);
        assert_eq!(retained.read_table_manifest().unwrap(), bytes);
        fs::write(
            path.join(DIRECTORY).join(TABLES),
            vec![b'x'; MAX_TABLE_MANIFEST_BYTES + 1],
        )
        .unwrap();
        assert_eq!(
            retained.read_table_manifest(),
            Err(CustodyError::ResourceLimit)
        );
    }

    #[test]
    fn discovered_identity_requires_the_exact_closed_canonical_record() {
        let (_root, path) = root();
        let custody = create(&path).unwrap();
        let binding = *custody.binding();
        let original = custody.manifest.clone();
        drop(custody);
        let manifest = path.join(DIRECTORY).join(MANIFEST);
        let mut decorated = original.clone();
        decorated.extend_from_slice(b" \n");
        fs::write(&manifest, decorated).unwrap();
        assert_eq!(
            recover_released(&path, &binding, BOOT).err(),
            Some(CustodyError::Identity)
        );
        let mut extended: serde_json::Value = serde_json::from_slice(&original).unwrap();
        extended["unknown"] = serde_json::json!(true);
        fs::write(&manifest, serde_json::to_vec(&extended).unwrap()).unwrap();
        assert_eq!(
            recover_released(&path, &binding, BOOT).err(),
            Some(CustodyError::Identity)
        );
        fs::write(&manifest, original).unwrap();
        let retained = recover_released(&path, &binding, BOOT).unwrap();
        assert_eq!(retained.binding(), &binding);
        assert_eq!(
            retained.read_table_manifest(),
            Err(CustodyError::Incomplete)
        );
    }
}
