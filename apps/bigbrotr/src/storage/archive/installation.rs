use bigbrotr_model::Digest256;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Command,
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

#[derive(Debug, Clone, Copy)]
pub struct ArtifactIdentity {
    pub bytes: u64,
    pub sha256: Digest256,
}

/// Expected executable identity must come from the authenticated release/build,
/// not from an untrusted sidecar next to the executable being checked.
pub struct WorkerInstallation {
    pub root: std::path::PathBuf,
    pub executable: ArtifactIdentity,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallationError {
    InvalidAsset,
    HashMismatch,
    Io,
    Cancelled,
    Deadline,
}
impl WorkerInstallation {
    /// The installation must remain immutable to the worker's runtime identity.
    /// Hashes detect wrong inputs; they do not revoke a concurrent host writer.
    pub(super) fn verified_command(
        &self,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Command, InstallationError> {
        check(deadline, cancelled)?;
        if !self.root.is_absolute()
            || self.root.canonicalize().ok().as_deref() != Some(self.root.as_path())
            || !self.root.is_dir()
        {
            return Err(InstallationError::InvalidAsset);
        }
        let executable = self.root.join("bigbrotr-archive-worker");
        verify_file(&executable, self.executable, deadline, cancelled)?;
        let mode = fs::metadata(&executable)
            .map_err(|_| InstallationError::Io)?
            .permissions()
            .mode();
        if mode & 0o111 == 0 || mode & 0o022 != 0 {
            return Err(InstallationError::InvalidAsset);
        }
        let spec: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("OUT_DIR"),
            "/archive-bundle.json"
        )))
        .map_err(|_| InstallationError::InvalidAsset)?;
        let library = &spec["library"];
        verify_file(
            &self.root.join("lib").join(asset_name(library)?),
            asset_identity(library)?,
            deadline,
            cancelled,
        )?;
        for asset in spec["extensions"]
            .as_array()
            .ok_or(InstallationError::InvalidAsset)?
        {
            verify_file(
                &self
                    .root
                    .join("extensions")
                    .join(format!("{}.duckdb_extension", asset_name(asset)?)),
                asset_identity(asset)?,
                deadline,
                cancelled,
            )?;
        }
        let mut command = Command::new(executable);
        command.env_clear().env("LC_ALL", "C").env(
            "BIGBROTR_ARCHIVE_PARENT_PID",
            std::process::id().to_string(),
        );
        // Explicitly select the verified local library; inherit no injection,
        // fallback search path, credentials, HOME or engine configuration.
        command.env(
            if cfg!(target_os = "macos") {
                "DYLD_LIBRARY_PATH"
            } else {
                "LD_LIBRARY_PATH"
            },
            self.root.join("lib"),
        );
        Ok(command)
    }
}
fn asset_name(value: &serde_json::Value) -> Result<&str, InstallationError> {
    value["name"]
        .as_str()
        .filter(|s| !s.is_empty() && !s.contains(['/', '\\']))
        .ok_or(InstallationError::InvalidAsset)
}
fn asset_identity(value: &serde_json::Value) -> Result<ArtifactIdentity, InstallationError> {
    Ok(ArtifactIdentity {
        bytes: value["bytes"]
            .as_u64()
            .ok_or(InstallationError::InvalidAsset)?,
        sha256: value["sha256"]
            .as_str()
            .ok_or(InstallationError::InvalidAsset)?
            .parse()
            .map_err(|_| InstallationError::InvalidAsset)?,
    })
}
fn verify_file(
    path: &Path,
    identity: ArtifactIdentity,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<(), InstallationError> {
    check(deadline, cancelled)?;
    let metadata = fs::symlink_metadata(path).map_err(|_| InstallationError::Io)?;
    if !path.is_absolute()
        || path.canonicalize().ok().as_deref() != Some(path)
        || !metadata.is_file()
        || metadata.len() != identity.bytes
        || identity.bytes == 0
    {
        return Err(InstallationError::InvalidAsset);
    }
    let mut file = fs::File::open(path).map_err(|_| InstallationError::Io)?;
    let mut hash = Sha256::new();
    let mut bytes = [0; 65536];
    let mut total = 0u64;
    loop {
        check(deadline, cancelled)?;
        let count = file.read(&mut bytes).map_err(|_| InstallationError::Io)?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .filter(|n| *n <= identity.bytes)
            .ok_or(InstallationError::InvalidAsset)?;
        hash.update(&bytes[..count]);
    }
    if total != identity.bytes || Digest256::from_bytes(hash.finalize().into()) != identity.sha256 {
        return Err(InstallationError::HashMismatch);
    }
    check(deadline, cancelled)
}
fn check(deadline: Instant, cancelled: &AtomicBool) -> Result<(), InstallationError> {
    if cancelled.load(Ordering::Relaxed) {
        Err(InstallationError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(InstallationError::Deadline)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, time::Duration};
    fn identity() -> ArtifactIdentity {
        ArtifactIdentity {
            bytes: 3,
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                .parse()
                .unwrap(),
        }
    }
    #[test]
    fn actual_file_identity_refuses_changed_bytes_size_and_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path().canonicalize().unwrap();
        let path = dir.join("asset");
        let cancel = AtomicBool::new(false);
        let deadline = Instant::now() + Duration::from_secs(2);
        fs::write(&path, b"abc").unwrap();
        assert_eq!(verify_file(&path, identity(), deadline, &cancel), Ok(()));
        fs::write(&path, b"abd").unwrap();
        assert_eq!(
            verify_file(&path, identity(), deadline, &cancel),
            Err(InstallationError::HashMismatch)
        );
        fs::write(&path, b"abcd").unwrap();
        assert_eq!(
            verify_file(&path, identity(), deadline, &cancel),
            Err(InstallationError::InvalidAsset)
        );
        fs::write(&path, b"abc").unwrap();
        std::os::unix::fs::symlink(&path, dir.join("link")).unwrap();
        assert_eq!(
            verify_file(&dir.join("link"), identity(), deadline, &cancel),
            Err(InstallationError::InvalidAsset)
        );
    }
    #[test]
    fn expired_or_cancelled_verification_cannot_admit_an_executable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().canonicalize().unwrap().join("asset");
        fs::write(&path, b"abc").unwrap();
        assert_eq!(
            verify_file(&path, identity(), Instant::now(), &AtomicBool::new(false)),
            Err(InstallationError::Deadline)
        );
        assert_eq!(
            verify_file(
                &path,
                identity(),
                Instant::now() + Duration::from_secs(1),
                &AtomicBool::new(true)
            ),
            Err(InstallationError::Cancelled)
        );
    }
}
