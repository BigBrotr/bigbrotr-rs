//! Explicit preparation of protected cursor keys, separate from Control activation.
use crate::{
    runtime::OwnedPermit,
    secrets::{SecretError, SecretFile, read_file},
};
use bigbrotr_model::{DatasetId, Digest256};
use rustix::fs::{Dir, FlockOperation, Mode, OFlags, RenameFlags, flock, openat, renameat_with};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    fs::File,
    io::Write,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, PermissionsExt},
    },
};
use zeroize::Zeroizing;

pub const KEYRING_RESERVATION: u64 = 32 * 1024;
const MAGIC: &[u8; 8] = b"BBCKEY\0\x01";
const HEADER: usize = 42;
const RECORD: usize = 48;
const MAX_KEYS: usize = 16;
const CHECKSUM: usize = 32;
const MAX_FILE: usize = HEADER + MAX_KEYS * RECORD + CHECKSUM;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyringError {
    InvalidReference,
    Filesystem,
    Permissions,
    Busy,
    NotInitialized,
    Incomplete,
    Mismatch,
    Capacity,
    Entropy,
}
impl fmt::Display for KeyringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cursor keyring refused: {self:?}")
    }
}
impl std::error::Error for KeyringError {}

/// Local protected bytes only. Control separately owns registration, issuance,
/// decryption lifecycle and current authorization; loading keys grants none.
/// The reservation outlives all key bytes and is released last.
pub struct Keyring {
    bytes: Zeroizing<Vec<u8>>,
    _permit: OwnedPermit,
}
impl fmt::Debug for Keyring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Keyring([redacted])")
    }
}
/// Successfully synchronized existing keys. Only this local preparation can
/// construct registration input; a plain read cannot attest prior publication.
/// Holds bytes and their reservation; does not retain filesystem path custody.
pub struct PreparedKeyring(Keyring);
impl PreparedKeyring {
    pub fn existing(
        reference: &str,
        dataset: DatasetId,
        permit: OwnedPermit,
    ) -> Result<Self, KeyringError> {
        Keyring::load(reference, dataset, permit, Load::PrepareExisting).map(Self)
    }
    pub fn keys(&self) -> &Keyring {
        &self.0
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Load {
    Inspect,
    Prepare,
    PrepareExisting,
    PrepareKey([u8; 16]),
}
impl Keyring {
    /// Add one key to an existing local ring, or re-sync the same key on retry.
    /// Prepare in one location, then distribute identical bytes to all workers.
    /// The public ID is caller-stable; this grants no Control lifecycle authority.
    pub fn prepare_key(
        reference: &str,
        dataset: DatasetId,
        key_id: [u8; 16],
        permit: OwnedPermit,
    ) -> Result<Self, KeyringError> {
        Self::load(reference, dataset, permit, Load::PrepareKey(key_id))
    }
    pub fn dataset(&self) -> DatasetId {
        DatasetId::try_from(<[u8; 16]>::try_from(&self.bytes[8..24]).expect("bounded dataset"))
            .expect("validated dataset")
    }
    /// Non-secret registration bindings, preserving file order. These hashes
    /// cannot authorize a key or substitute for its protected bytes.
    pub fn bindings(&self) -> impl Iterator<Item = ([u8; 16], Digest256)> + '_ {
        self.key_ids().map(|id| {
            let digest = Sha256::new()
                .chain_update(b"bigbrotr/cursor-key-material/1\0")
                .chain_update(self.dataset_bytes())
                .chain_update(self.id())
                .chain_update(id)
                .chain_update(self.key(&id).expect("validated key"))
                .finalize()
                .into();
            (id, Digest256::from_bytes(digest))
        })
    }
    pub fn prepare(
        reference: &str,
        dataset: DatasetId,
        permit: OwnedPermit,
    ) -> Result<Self, KeyringError> {
        Self::load(reference, dataset, permit, Load::Prepare)
    }
    pub fn open(
        reference: &str,
        dataset: DatasetId,
        permit: OwnedPermit,
    ) -> Result<Self, KeyringError> {
        Self::load(reference, dataset, permit, Load::Inspect)
    }
    pub fn id(&self) -> [u8; 16] {
        self.bytes[24..40].try_into().expect("validated ring ID")
    }
    pub fn key_ids(&self) -> impl Iterator<Item = [u8; 16]> + '_ {
        self.bytes[HEADER..self.bytes.len() - CHECKSUM]
            .as_chunks::<RECORD>()
            .0
            .iter()
            .map(|record| record[..16].try_into().expect("fixed key ID"))
    }
    pub fn fingerprint(&self) -> Digest256 {
        Digest256::from_bytes(
            self.bytes[self.bytes.len() - CHECKSUM..]
                .try_into()
                .expect("validated checksum"),
        )
    }

    pub(super) fn dataset_bytes(&self) -> &[u8] {
        &self.bytes[8..24]
    }

    pub(super) fn key(&self, id: &[u8; 16]) -> Option<&[u8; 32]> {
        self.bytes[HEADER..self.bytes.len() - CHECKSUM]
            .as_chunks::<RECORD>()
            .0
            .iter()
            .find(|record| &record[..16] == id)
            .map(|record| record[16..].try_into().expect("validated key record"))
    }

    fn load(
        reference: &str,
        dataset: DatasetId,
        permit: OwnedPermit,
        operation: Load,
    ) -> Result<Self, KeyringError> {
        use KeyringError as Error;
        let prepare = operation != Load::Inspect;
        if permit.request().units().data_bytes() < KEYRING_RESERVATION
            || permit.request().units().items() < 1
        {
            return Err(Error::Capacity);
        }
        if operation == Load::PrepareKey([0; 16]) {
            return Err(Error::Mismatch);
        }
        let reference = SecretFile::parse(reference).map_err(|_| Error::InvalidReference)?;
        let (directory, name) = reference.open_parent().map_err(secret_error)?;
        let metadata = directory.metadata().map_err(|_| Error::Filesystem)?;
        if !metadata.is_dir()
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.mode() & 0o7777 != 0o700
        {
            return Err(Error::Permissions);
        }
        flock(
            &directory,
            if prepare {
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
        let existing = match openat(
            &directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC | OFlags::NOCTTY,
            Mode::empty(),
        ) {
            Ok(fd) => {
                let file = File::from(fd);
                if file.metadata().map_err(|_| Error::Filesystem)?.nlink() != 1 {
                    return Err(Error::Permissions);
                }
                let bytes = read_file(file, MAX_FILE)
                    .map_err(secret_error)?
                    .into_zeroizing();
                validate(&bytes, dataset)?;
                Some(bytes)
            }
            Err(rustix::io::Errno::NOENT) => None,
            Err(_) => return Err(Error::Filesystem),
        };
        if prepare {
            // A dedicated directory bounds incomplete staging to one explicit
            // attempt. Never regenerate after finding unresolved partial bytes,
            // adopt another credential directory or remove an unknown file.
            for entry in Dir::read_from(&directory).map_err(|_| Error::Filesystem)? {
                let entry = entry.map_err(|_| Error::Filesystem)?;
                let found = entry.file_name().to_bytes();
                if !matches!(found, b"." | b"..")
                    && !(existing.is_some() && found == name.as_bytes())
                {
                    return Err(Error::Incomplete);
                }
            }
        }
        let replace = existing.is_some();
        let bytes = match existing {
            Some(bytes) => bytes,
            None if operation == Load::Prepare => fresh(dataset)?,
            None => return Err(Error::NotInitialized),
        };
        let bytes = if let Load::PrepareKey(id) = operation {
            append_key(bytes, id)?
        } else {
            bytes
        };
        if prepare {
            let mut nonce = [0u8; 16];
            getrandom::fill(&mut nonce).map_err(|_| Error::Entropy)?;
            let temporary = format!(
                ".cursor-keyring-{:032x}.partial",
                u128::from_be_bytes(nonce)
            );
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
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(|_| Error::Filesystem)?;
            file.write_all(&bytes).map_err(|_| Error::Filesystem)?;
            file.sync_all().map_err(|_| Error::Filesystem)?;
            // Retry rewrites verified identical bytes through a fresh inode:
            // fsync alone cannot repair a previous writeback failure. Staging
            // failures are retained; no cleanup or fresh key is guessed safe.
            publish(&directory, &temporary, name, replace)?;
            directory.sync_all().map_err(|_| Error::Filesystem)?;
            let parent = File::from(
                openat(
                    &directory,
                    "..",
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|_| Error::Filesystem)?,
            );
            parent.sync_all().map_err(|_| Error::Filesystem)?;
        }
        Ok(Self {
            bytes,
            _permit: permit,
        })
    }
}

fn append_key(bytes: Zeroizing<Vec<u8>>, id: [u8; 16]) -> Result<Zeroizing<Vec<u8>>, KeyringError> {
    let body = &bytes[..bytes.len() - CHECKSUM];
    let records = body[HEADER..].as_chunks::<RECORD>().0;
    if records.iter().any(|record| record[..16] == id) {
        return Ok(bytes);
    }
    if records.len() == MAX_KEYS {
        return Err(KeyringError::Capacity);
    }
    // Never reallocate a buffer containing old secrets: the allocator would
    // free an unzeroized copy. Both bounded buffers remain owned/charged here.
    let mut next = Zeroizing::new(Vec::new());
    next.try_reserve_exact(bytes.len() + RECORD)
        .map_err(|_| KeyringError::Capacity)?;
    next.extend_from_slice(body);
    next[40..42].copy_from_slice(&((records.len() + 1) as u16).to_be_bytes());
    next.extend_from_slice(&id);
    let material = next.len();
    next.resize(material + 32, 0);
    getrandom::fill(&mut next[material..]).map_err(|_| KeyringError::Entropy)?;
    if next[material..] == [0; 32] {
        return Err(KeyringError::Entropy);
    }
    let digest = Sha256::digest(&next);
    next.extend_from_slice(&digest);
    Ok(next)
}

fn publish(
    directory: &File,
    temporary: &str,
    name: &std::ffi::OsStr,
    replace: bool,
) -> Result<(), KeyringError> {
    // First publication must not overwrite an external provisioner's late file.
    // Complete retry replaces known bytes under the cooperative directory lock;
    // operators must exclude other writers throughout that operation.
    let flags = if replace {
        RenameFlags::empty()
    } else {
        RenameFlags::NOREPLACE
    };
    renameat_with(directory, temporary, directory, name, flags)
        .map_err(|_| KeyringError::Filesystem)
}

fn fresh(dataset: DatasetId) -> Result<Zeroizing<Vec<u8>>, KeyringError> {
    let mut bytes = Zeroizing::new(Vec::new());
    bytes
        .try_reserve_exact(HEADER + RECORD + CHECKSUM)
        .map_err(|_| KeyringError::Capacity)?;
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(dataset.as_bytes());
    bytes.resize(HEADER + RECORD, 0);
    getrandom::fill(&mut bytes[24..40]).map_err(|_| KeyringError::Entropy)?;
    bytes[40..42].copy_from_slice(&1u16.to_be_bytes());
    getrandom::fill(&mut bytes[HEADER..]).map_err(|_| KeyringError::Entropy)?;
    let digest = Sha256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    validate(&bytes, dataset).map_err(|_| KeyringError::Entropy)?;
    Ok(bytes)
}

fn validate(bytes: &[u8], dataset: DatasetId) -> Result<(), KeyringError> {
    use KeyringError as Error;
    if bytes.len() > MAX_FILE {
        return Err(Error::Capacity);
    }
    if bytes.len() < HEADER + RECORD + CHECKSUM
        || &bytes[..8] != MAGIC
        || &bytes[8..24] != dataset.as_bytes()
        || bytes[24..40] == [0; 16]
    {
        return Err(Error::Mismatch);
    }
    let count = u16::from_be_bytes(bytes[40..42].try_into().expect("bounded header")) as usize;
    if count > MAX_KEYS {
        return Err(Error::Capacity);
    }
    if count == 0 || bytes.len() != HEADER + count * RECORD + CHECKSUM {
        return Err(Error::Mismatch);
    }
    let body = &bytes[..bytes.len() - CHECKSUM];
    if Sha256::digest(body).as_slice() != &bytes[body.len()..] {
        return Err(Error::Mismatch);
    }
    let records = bytes[HEADER..body.len()].as_chunks::<RECORD>().0;
    for (index, record) in records.iter().enumerate() {
        if record[..16] == [0; 16]
            || record[16..] == [0; 32]
            || records[..index]
                .iter()
                .any(|earlier| earlier[..16] == record[..16])
        {
            return Err(Error::Mismatch);
        }
    }
    Ok(())
}

fn secret_error(error: SecretError) -> KeyringError {
    match error {
        SecretError::Reference => KeyringError::InvalidReference,
        SecretError::TooLarge | SecretError::Allocation | SecretError::InvalidLimit => {
            KeyringError::Capacity
        }
        SecretError::NotRegular | SecretError::Owner | SecretError::Permissions => {
            KeyringError::Permissions
        }
        SecretError::Open | SecretError::Read => KeyringError::Filesystem,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn initial_publication_never_replaces_a_file_created_after_the_absence_check() {
        let root = tempfile::tempdir().unwrap();
        let directory = File::open(root.path()).unwrap();
        flock(&directory, FlockOperation::NonBlockingLockExclusive).unwrap();
        assert!(!root.path().join("keys").exists());
        std::fs::write(root.path().join("staging"), b"prepared public fixture").unwrap();
        // An external provisioning tool can write despite this advisory lock.
        std::fs::write(root.path().join("keys"), b"preserve external fixture").unwrap();
        assert_eq!(
            publish(&directory, "staging", std::ffi::OsStr::new("keys"), false),
            Err(KeyringError::Filesystem)
        );
        assert_eq!(
            std::fs::read(root.path().join("keys")).unwrap(),
            b"preserve external fixture"
        );
        assert_eq!(
            std::fs::read(root.path().join("staging")).unwrap(),
            b"prepared public fixture"
        );
    }
}
