//! A single protected local trigger, not a command queue or dispatch authority.
use super::*;
use bigbrotr_model::{AccessScopeId, EndpointId, RouteId};
use rustix::fs::{Mode, OFlags, openat};
use serde::Deserialize;
use std::{fs::File, io::Read, num::NonZeroU64, os::unix::fs::MetadataExt, path::PathBuf};

const MAX_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Visibility {
    Public,
    Private,
}

pub(super) struct Selection {
    pub(super) source: RelaySourceBinding,
    pub(super) visibility: Visibility,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fields {
    format: String,
    endpoint_id: String,
    access_scope_id: String,
    enrollment_generation: String,
    route_id: String,
    visibility: String,
}

fn parse(bytes: &[u8]) -> Result<Selection> {
    let f: Fields =
        serde_json::from_slice(bytes).map_err(|_| failure("invalid selection document"))?;
    if f.format != "bigbrotr-monitor-once-v1" {
        return Err(failure("unsupported selection format"));
    }
    let endpoint_id: EndpointId = f
        .endpoint_id
        .parse()
        .map_err(|_| failure("invalid endpoint ID"))?;
    let access_scope_id: AccessScopeId = f
        .access_scope_id
        .parse()
        .map_err(|_| failure("invalid access ID"))?;
    let route_id: RouteId = f
        .route_id
        .parse()
        .map_err(|_| failure("invalid route ID"))?;
    let generation: NonZeroU64 = f
        .enrollment_generation
        .parse()
        .map_err(|_| failure("invalid enrollment generation"))?;
    if endpoint_id.to_string() != f.endpoint_id
        || access_scope_id.to_string() != f.access_scope_id
        || route_id.to_string() != f.route_id
        || generation.to_string() != f.enrollment_generation
    {
        return Err(failure(
            "selection identities must use their canonical management spelling",
        ));
    }
    let visibility = match f.visibility.as_str() {
        "public" => Visibility::Public,
        "private" => Visibility::Private,
        _ => return Err(failure("visibility must be public or private")),
    };
    Ok(Selection {
        source: RelaySourceBinding {
            endpoint_id,
            access_scope_id,
            enrollment_generation: generation,
            route_id,
        },
        visibility,
    })
}

/// The canonical owned parent is pinned before process resources or Native are
/// allocated. Absence arms a wait; the first existing object consumes the path,
/// even if validation fails. Nothing unlinks or rearms it.
pub(in super::super) struct SelectionPath {
    parent: File,
    path: PathBuf,
    name: std::ffi::OsString,
    identity: (u64, u64),
    consumed: bool,
}

impl SelectionPath {
    pub(in super::super) fn prepare(path: &Path) -> Result<Self> {
        let parent_path = path
            .parent()
            .ok_or_else(|| failure("selection needs an absolute protected parent"))?;
        let name = path
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| failure("selection needs a file name"))?;
        if !path.is_absolute() || parent_path.canonicalize().ok().as_deref() != Some(parent_path) {
            return Err(failure("selection parent must be absolute and canonical"));
        }
        let parent = File::from(
            rustix::fs::open(
                parent_path,
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
                Mode::empty(),
            )
            .map_err(|_| failure("selection parent unavailable"))?,
        );
        let metadata = parent
            .metadata()
            .map_err(|_| failure("selection parent unavailable"))?;
        private_directory(&metadata)?;
        Ok(Self {
            parent,
            path: path.into(),
            name: name.into(),
            identity: (metadata.dev(), metadata.ino()),
            consumed: false,
        })
    }

    pub(super) fn poll(&mut self) -> Result<Option<Selection>> {
        if self.consumed {
            return Ok(None);
        }
        self.require_parent()?;
        let opened = openat(
            &self.parent,
            &self.name,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::NOCTTY,
            Mode::empty(),
        );
        let fd = match opened {
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            other => {
                self.consumed = true;
                other.map_err(|_| failure("selection file refused"))?
            }
        };
        let mut file = File::from(fd);
        let before = file
            .metadata()
            .map_err(|_| failure("selection file unavailable"))?;
        private_file(&before)?;
        let mut bytes = [0; MAX_BYTES + 1];
        let count = (&mut file)
            .take(bytes.len() as u64)
            .read(&mut bytes)
            .map_err(|_| failure("selection read failed"))?;
        // read() may be short even for a regular file. Read the bounded tail
        // before accepting EOF, without allocating from an untrusted length.
        let mut count = count;
        while count < bytes.len() {
            let n = file
                .read(&mut bytes[count..])
                .map_err(|_| failure("selection read failed"))?;
            if n == 0 {
                break;
            }
            count += n;
        }
        let after = file
            .metadata()
            .map_err(|_| failure("selection file unavailable"))?;
        let named =
            std::fs::symlink_metadata(&self.path).map_err(|_| failure("selection file changed"))?;
        self.require_parent()?;
        if count == 0
            || count > MAX_BYTES
            || count as u64 != before.len()
            || fingerprint(&before) != fingerprint(&after)
            || fingerprint(&after) != fingerprint(&named)
        {
            return Err(failure("selection file changed or exceeded its bound"));
        }
        parse(&bytes[..count]).map(Some)
    }

    fn require_parent(&self) -> Result<()> {
        let path = self.path.parent().expect("validated parent");
        if path.canonicalize().ok().as_deref() != Some(path) {
            return Err(failure("selection parent changed"));
        }
        let named =
            std::fs::symlink_metadata(path).map_err(|_| failure("selection parent changed"))?;
        let held = self
            .parent
            .metadata()
            .map_err(|_| failure("selection parent unavailable"))?;
        private_directory(&named)?;
        private_directory(&held)?;
        if (named.dev(), named.ino()) != self.identity || (held.dev(), held.ino()) != self.identity
        {
            return Err(failure("selection parent changed"));
        }
        Ok(())
    }
}

fn private_directory(m: &std::fs::Metadata) -> Result<()> {
    if !m.is_dir() || m.mode() & 0o7777 != 0o700 || m.uid() != rustix::process::geteuid().as_raw() {
        Err(failure("selection parent must be owned mode 0700"))
    } else {
        Ok(())
    }
}
fn private_file(m: &std::fs::Metadata) -> Result<()> {
    if !m.is_file()
        || m.mode() & 0o7777 != 0o600
        || m.uid() != rustix::process::geteuid().as_raw()
        || m.nlink() != 1
        || m.len() == 0
        || m.len() > MAX_BYTES as u64
    {
        Err(failure(
            "selection must be owned mode 0600, regular, single-link and at most 4096 bytes",
        ))
    } else {
        Ok(())
    }
}
fn fingerprint(m: &std::fs::Metadata) -> (u64, u64, u32, u32, u32, u64, u64, i64, i64, i64, i64) {
    (
        m.dev(),
        m.ino(),
        m.mode(),
        m.uid(),
        m.gid(),
        m.nlink(),
        m.len(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    fn document() -> Value {
        json!({"format":"bigbrotr-monitor-once-v1", "endpoint_id":"11".repeat(16),
        "access_scope_id":"22".repeat(16),"enrollment_generation":"1","route_id":"33".repeat(16),"visibility":"private"})
    }
    fn write(path: &Path, bytes: &[u8]) {
        std::fs::write(path, bytes).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    #[test]
    fn selection_uses_exact_management_identities_and_rejects_ambiguous_fields() {
        let valid = document();
        let selection = parse(&serde_json::to_vec(&valid).unwrap()).unwrap();
        assert_eq!(selection.source.endpoint_id.to_string(), "11".repeat(16));
        assert_eq!(selection.source.enrollment_generation.get(), 1);
        assert_eq!(selection.visibility, Visibility::Private);
        for (key, value) in [
            ("endpoint_id", json!(1)),
            ("endpoint_id", json!("AA".repeat(16))),
            ("enrollment_generation", json!("01")),
            ("enrollment_generation", json!("0")),
            ("enrollment_generation", json!(1)),
            ("visibility", json!("private3")),
            ("unknown", json!(true)),
        ] {
            let mut bad = valid.clone();
            bad[key] = value;
            assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err(), "{key}");
        }
        let mut duplicate = serde_json::to_string(&valid).unwrap();
        duplicate.pop();
        duplicate.push_str(",\"visibility\":\"private\"}");
        assert!(parse(duplicate.as_bytes()).is_err());
    }
    #[test]
    fn absent_path_arms_then_atomic_file_is_consumed_once() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = temp.path().join("source.json");
        let mut selected = SelectionPath::prepare(&path).unwrap();
        assert!(selected.poll().unwrap().is_none());
        let staging = temp.path().join("staging");
        write(&staging, &serde_json::to_vec(&document()).unwrap());
        std::fs::rename(staging, &path).unwrap();
        assert!(selected.poll().unwrap().is_some());
        write(&path, b"broken");
        assert!(selected.poll().unwrap().is_none());
    }
    #[test]
    fn unsafe_or_malformed_first_selection_never_rearms() {
        for bad in ["symlink", "mode", "hardlink", "oversize", "malformed"] {
            let temp = tempfile::tempdir().unwrap();
            std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let path = temp.path().join("source.json");
            let mut selected = SelectionPath::prepare(&path).unwrap();
            match bad {
                "symlink" => {
                    write(&temp.path().join("other"), b"{}");
                    symlink("other", &path).unwrap();
                }
                "mode" => {
                    write(&path, b"{}");
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
                        .unwrap();
                }
                "hardlink" => {
                    write(&path, b"{}");
                    std::fs::hard_link(&path, temp.path().join("other")).unwrap();
                }
                "oversize" => write(&path, &vec![b' '; 4097]),
                _ => write(&path, b"{}"),
            }
            assert!(selected.poll().is_err(), "{bad}");
            std::fs::remove_file(&path).unwrap();
            write(&path, &serde_json::to_vec(&document()).unwrap());
            assert!(selected.poll().unwrap().is_none(), "{bad}");
        }
    }
    #[test]
    fn replaced_or_noncanonical_parent_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut selected = SelectionPath::prepare(&parent.join("source.json")).unwrap();
        std::fs::rename(&parent, temp.path().join("old")).unwrap();
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(selected.poll().is_err());
        symlink(&parent, temp.path().join("alias")).unwrap();
        assert!(SelectionPath::prepare(&temp.path().join("alias/source.json")).is_err());
        assert!(SelectionPath::prepare(Path::new("relative/source.json")).is_err());
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o750)).unwrap();
        assert!(SelectionPath::prepare(&parent.join("source.json")).is_err());
    }
}
