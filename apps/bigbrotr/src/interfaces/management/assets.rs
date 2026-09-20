//! Immutable, bounded private bundle. All filesystem IO precedes Control startup;
//! HTTP only clones verified bytes and never resolves a request against disk.
use axum::body::Bytes;
use bigbrotr::runtime::{OwnedPermit, ResourceBudget, ResourceRequest};
use rustix::{
    fd::OwnedFd,
    fs::{Mode, OFlags, open, openat},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::{Component, Path},
    sync::Arc,
};

const MANIFEST_BYTES: usize = 64 * 1024;
const FILE_BYTES: usize = 2 * 1024 * 1024;
const TOTAL_BYTES: usize = 16 * 1024 * 1024;
// Payload plus bounded manifest, decoded metadata, maps and allocation overhead.
// Each cloned response retains this same reservation through its Bytes owner.
const RESERVATION_BYTES: u64 = 20 * 1024 * 1024;
const DIRECTORY: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

#[derive(Debug, PartialEq, Eq)]
pub(super) enum AssetError {
    Capacity,
    Manifest,
    Integrity,
    Io,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    base_path: String,
    files: Vec<Entry>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    path: String,
    bytes: usize,
    sha256: String,
}

pub(super) struct Asset {
    pub body: Bytes,
    pub mime: &'static str,
}
pub(crate) struct Assets {
    base: String,
    files: BTreeMap<String, Asset>,
    bytes: usize,
    manifest_sha256: String,
}
struct AssetBytes {
    bytes: Vec<u8>,
    _grant: Arc<OwnedPermit>,
}
impl AsRef<[u8]> for AssetBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Assets {
    pub(super) fn load(
        root: &Path,
        base: &str,
        budget: &ResourceBudget,
    ) -> Result<Self, AssetError> {
        if !matches!(base, "/" | "/control/") {
            return Err(AssetError::Manifest);
        }
        let grant = Arc::new(
            budget
                .try_acquire(
                    ResourceRequest::data(RESERVATION_BYTES, 1, 0)
                        .expect("fixed private asset reservation"),
                )
                .map_err(|_| AssetError::Capacity)?,
        );
        let root = directory(root)?;
        let raw = read_file(&root, "manifest.json", MANIFEST_BYTES)?;
        let manifest_sha256 = digest(&raw);
        let manifest: Manifest = serde_json::from_slice(&raw).map_err(|_| AssetError::Manifest)?;
        if manifest.format != "bigbrotr-private-assets/1"
            || manifest.base_path != base
            || manifest.files.is_empty()
            || manifest.files.len() > 128
        {
            return Err(AssetError::Manifest);
        }
        let mut expected = BTreeMap::new();
        let mut bytes = 0usize;
        for entry in manifest.files {
            if !valid_path(&entry.path)
                || entry.path == "manifest.json"
                || entry.bytes > FILE_BYTES
                || entry.sha256.len() != 64
                || !entry
                    .sha256
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            {
                return Err(AssetError::Manifest);
            }
            bytes = bytes
                .checked_add(entry.bytes)
                .filter(|n| *n <= TOTAL_BYTES)
                .ok_or(AssetError::Manifest)?;
            if expected.insert(entry.path.clone(), entry).is_some() {
                return Err(AssetError::Manifest);
            }
        }
        if !expected.contains_key("index.html") {
            return Err(AssetError::Manifest);
        }
        let mut files = BTreeMap::new();
        for (path, entry) in expected {
            let body = read_file(&root, &path, entry.bytes)?;
            if body.len() != entry.bytes || digest(&body) != entry.sha256 {
                return Err(AssetError::Integrity);
            }
            let mime = mime(&path).ok_or(AssetError::Manifest)?;
            let body = Bytes::from_owner(AssetBytes {
                bytes: body,
                _grant: Arc::clone(&grant),
            });
            files.insert(path, Asset { body, mime });
        }
        Ok(Self {
            base: base.into(),
            files,
            bytes,
            manifest_sha256,
        })
    }

    pub(super) fn lookup(&self, path: &str) -> Option<&Asset> {
        let relative = path.strip_prefix(&self.base)?;
        self.files.get(if relative.is_empty() {
            "index.html"
        } else {
            relative
        })
    }

    pub(crate) fn observation(&self) -> serde_json::Value {
        serde_json::json!({"base_path": self.base, "files": self.files.len(),
            "bytes": self.bytes, "manifest_sha256": self.manifest_sha256})
    }
}

fn digest(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(output, "{byte:02x}").expect("string write");
    }
    output
}

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 256
        && mime(path).is_some()
        && path.split('/').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
                && part
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
        })
}
fn mime(path: &str) -> Option<&'static str> {
    Some(match path.rsplit_once('.')?.1 {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        _ => return None,
    })
}

fn directory(path: &Path) -> Result<OwnedFd, AssetError> {
    let text = path.to_str().ok_or(AssetError::Manifest)?;
    if !path.is_absolute() || text.len() > 8192 || text.split('/').any(|p| p == "." || p == "..") {
        return Err(AssetError::Manifest);
    }
    let mut parent = open("/", DIRECTORY, Mode::empty()).map_err(|_| AssetError::Io)?;
    for component in path.components().skip(1) {
        let Component::Normal(name) = component else {
            return Err(AssetError::Manifest);
        };
        parent = openat(&parent, name, DIRECTORY, Mode::empty()).map_err(|_| AssetError::Io)?;
    }
    Ok(parent)
}

fn read_file(root: &OwnedFd, path: &str, limit: usize) -> Result<Vec<u8>, AssetError> {
    let mut names = path.split('/').peekable();
    let mut parent = None;
    let descriptor = loop {
        let name = names.next().ok_or(AssetError::Manifest)?;
        let directory = parent.as_ref().unwrap_or(root);
        if names.peek().is_none() {
            break openat(
                directory,
                name,
                OFlags::RDONLY
                    | OFlags::NOFOLLOW
                    | OFlags::CLOEXEC
                    | OFlags::NONBLOCK
                    | OFlags::NOCTTY,
                Mode::empty(),
            )
            .map_err(|_| AssetError::Io)?;
        }
        parent =
            Some(openat(directory, name, DIRECTORY, Mode::empty()).map_err(|_| AssetError::Io)?);
    };
    let mut file = File::from(descriptor);
    let metadata = file.metadata().map_err(|_| AssetError::Io)?;
    if !metadata.is_file() {
        return Err(AssetError::Io);
    }
    let len = usize::try_from(metadata.len()).map_err(|_| AssetError::Integrity)?;
    if len > limit {
        return Err(AssetError::Integrity);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| AssetError::Capacity)?;
    bytes.resize(len, 0);
    file.read_exact(&mut bytes).map_err(|_| AssetError::Io)?;
    // Detect growth as well as truncation without an unbounded read-to-end.
    let mut extra = [0];
    loop {
        match file.read(&mut extra) {
            Ok(0) => return Ok(bytes),
            Ok(_) => return Err(AssetError::Integrity),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(AssetError::Io),
        }
    }
}

#[cfg(test)]
mod tests;
