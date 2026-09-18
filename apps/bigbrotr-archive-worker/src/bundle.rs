//! Only compiled, checksum-pinned assets can be loaded. The launcher additionally
//! verifies the library before exec and supplies a private immutable bundle.
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{fmt, fs, io::Read, path::Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleError {
    InvalidRoot,
    InvalidManifest,
    InvalidAsset,
    Read,
    HashMismatch,
    NativeLoad,
}
impl fmt::Display for BundleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "archive bundle failed: {self:?}")
    }
}
impl std::error::Error for BundleError {}

#[derive(Deserialize)]
pub(crate) struct Asset {
    pub name: String,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Deserialize)]
pub(crate) struct BundleSpec {
    pub target: String,
    pub engine_version: String,
    pub engine_source_id: String,
    pub library: Asset,
    pub extensions: Vec<Asset>,
}
pub(crate) fn specification() -> Result<BundleSpec, BundleError> {
    serde_json::from_str(include_str!(concat!(
        env!("OUT_DIR"),
        "/archive-bundle.json"
    )))
    .map_err(|_| BundleError::InvalidManifest)
}

fn verify(path: &Path, asset: &Asset) -> Result<(), BundleError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| BundleError::Read)?;
    if path.canonicalize().ok().as_deref() != Some(path) {
        return Err(BundleError::InvalidAsset);
    }
    if !metadata.is_file() || metadata.len() != asset.bytes {
        return Err(BundleError::InvalidAsset);
    }
    let mut file = fs::File::open(path).map_err(|_| BundleError::Read)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    let mut total = 0u64;
    loop {
        let count = file.read(&mut buffer).map_err(|_| BundleError::Read)?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .filter(|n| *n <= asset.bytes)
            .ok_or(BundleError::InvalidAsset)?;
        hash.update(&buffer[..count]);
    }
    let actual: String = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if total != asset.bytes || actual != asset.sha256 {
        return Err(BundleError::HashMismatch);
    }
    Ok(())
}

impl crate::engine::Engine {
    /// Verify every asset before loading any extension. Signed extension loading
    /// is still performed by DuckDB; a matching hash is not a replacement for it.
    pub fn load_bundle(&self, root: &Path) -> Result<(), BundleError> {
        if !root.is_absolute() || root.canonicalize().ok().as_deref() != Some(root) {
            return Err(BundleError::InvalidRoot);
        }
        let spec = specification()?;
        verify(&root.join("lib").join(&spec.library.name), &spec.library)?;
        for asset in &spec.extensions {
            verify(
                &root
                    .join("extensions")
                    .join(format!("{}.duckdb_extension", asset.name)),
                asset,
            )?;
        }
        for asset in &spec.extensions {
            let path = root
                .join("extensions")
                .join(format!("{}.duckdb_extension", asset.name));
            let text = path
                .to_str()
                .filter(|s| !s.contains('\0'))
                .ok_or(BundleError::InvalidRoot)?;
            // LOAD does not accept bind parameters. This value is a verified
            // absolute local asset path, quoted as a SQL string, never SQL syntax.
            self.connection
                .execute_batch(&format!("LOAD '{}'", text.replace('\'', "''")))
                .map_err(|_| BundleError::NativeLoad)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, EngineLimits};
    use std::io::{Seek, SeekFrom, Write};
    use std::os::unix::fs::PermissionsExt;

    fn engine(temp: &Path) -> Engine {
        Engine::open(
            &temp.canonicalize().unwrap(),
            EngineLimits {
                memory_bytes: 128 * 1024 * 1024,
                temp_bytes: 64 * 1024 * 1024,
                threads: 1,
            },
        )
        .unwrap()
    }

    #[test]
    fn exact_bytes_required_and_links_refused() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let path = root.join("asset");
        let asset = Asset {
            name: "fixture".into(),
            bytes: 3,
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
        };
        fs::write(&path, b"abc").unwrap();
        assert_eq!(verify(&path, &asset), Ok(()));
        fs::write(&path, b"abd").unwrap();
        assert_eq!(verify(&path, &asset), Err(BundleError::HashMismatch));
        fs::write(&path, b"abcd").unwrap();
        assert_eq!(verify(&path, &asset), Err(BundleError::InvalidAsset));
        std::os::unix::fs::symlink(&path, root.join("link")).unwrap();
        assert_eq!(
            verify(&root.join("link"), &asset),
            Err(BundleError::InvalidAsset)
        );
    }

    #[test]
    fn exact_offline_bundle_loads_with_signed_extensions() {
        // This required real-asset case must fail if the prerequisite is absent.
        let root = std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE")
            .expect("prepare the pinned native bundle and pass BIGBROTR_ARCHIVE_TEST_BUNDLE");
        let temp = tempfile::tempdir().unwrap();
        let engine = engine(temp.path());
        engine.load_bundle(Path::new(&root)).unwrap();
        let report = engine.inspection().unwrap();
        assert_eq!(report["extensions"].as_array().unwrap().len(), 3);
        for extension in report["extensions"].as_array().unwrap() {
            assert_eq!(extension["loaded"], true);
            assert!(!extension["version"].as_str().unwrap().is_empty());
        }
        assert_eq!(report["settings"]["allow_unsigned_extensions"], "false");
    }

    #[test]
    fn native_loader_refuses_a_modified_signature() {
        let root = std::env::var_os("BIGBROTR_ARCHIVE_TEST_BUNDLE")
            .expect("required native bundle missing");
        let temp = tempfile::tempdir().unwrap();
        let engine = engine(temp.path());
        let artifact = tempfile::tempdir().unwrap();
        let path = artifact.path().join("ducklake.duckdb_extension");
        fs::copy(
            Path::new(&root).join("extensions/ducklake.duckdb_extension"),
            &path,
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        let mut last = [0];
        file.read_exact(&mut last).unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(&[last[0] ^ 1]).unwrap();
        drop(file);
        let sql = format!("LOAD '{}'", path.to_str().unwrap().replace('\'', "''"));
        assert!(engine.connection.execute_batch(&sql).is_err());
        let loaded: bool = engine
            .connection
            .query_row(
                "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'ducklake'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!loaded);
    }
}
