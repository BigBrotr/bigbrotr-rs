//! Map bind mounts back to their filesystem-relative root before comparing them.
//! Canonical names and leaf inodes alone cannot detect a nested bind alias.
use super::LifetimeError;
use std::path::Path;
#[cfg(any(target_os = "linux", test))]
use std::path::PathBuf;

#[cfg(target_os = "linux")]
pub(super) fn disjoint(root: &Path, other: &Path) -> Result<(), LifetimeError> {
    use super::lifetime;
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
    if root.starts_with(other) || other.starts_with(root) {
        return Err(LifetimeError::InvalidRoot);
    }
    let first = lifetime::directory(root)?;
    let second = lifetime::directory(other)?;
    let a = first.metadata().map_err(|_| LifetimeError::Filesystem)?;
    let b = second.metadata().map_err(|_| LifetimeError::Filesystem)?;
    if a.dev() != b.dev() {
        return Ok(());
    }
    if a.ino() == b.ino() {
        return Err(LifetimeError::InvalidRoot);
    }
    let mounts = lifetime::bounded_proc("/proc/self/mountinfo", 128 * 1024)?;
    let location = |file: &std::fs::File, path: &Path| {
        let info =
            lifetime::bounded_proc(&format!("/proc/self/fdinfo/{}", file.as_raw_fd()), 4096)?;
        filesystem_path(path, &info, &mounts)
    };
    let a = location(&first, root)?;
    let b = location(&second, other)?;
    if a.starts_with(&b) || b.starts_with(&a) {
        Err(LifetimeError::InvalidRoot)
    } else {
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn disjoint(_root: &Path, _other: &Path) -> Result<(), LifetimeError> {
    Err(LifetimeError::UnsupportedHost)
}

#[cfg(any(target_os = "linux", test))]
fn filesystem_path(path: &Path, fdinfo: &str, mounts: &str) -> Result<PathBuf, LifetimeError> {
    let mut ids = fdinfo
        .lines()
        .filter_map(|line| line.strip_prefix("mnt_id:\t"));
    let id = ids
        .next()
        .filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
        .ok_or(LifetimeError::UnsupportedHost)?;
    if ids.next().is_some() {
        return Err(LifetimeError::UnsupportedHost);
    }
    let mut matching = mounts
        .lines()
        .filter(|line| line.split_ascii_whitespace().next() == Some(id));
    let line = matching.next().ok_or(LifetimeError::UnsupportedHost)?;
    if matching.next().is_some() {
        return Err(LifetimeError::UnsupportedHost);
    }
    let (fields, tail) = line
        .split_once(" - ")
        .ok_or(LifetimeError::UnsupportedHost)?;
    // This mapping is selected for the same local ext4 filesystem as the state
    // root; subvolume and network identity rules are not inferred from st_dev.
    if tail.split_ascii_whitespace().next() != Some("ext4") {
        return Err(LifetimeError::UnsupportedHost);
    }
    let mut fields = fields.split_ascii_whitespace().skip(3);
    let root = unescape(fields.next().ok_or(LifetimeError::UnsupportedHost)?)?;
    let mount = unescape(fields.next().ok_or(LifetimeError::UnsupportedHost)?)?;
    let suffix = path
        .strip_prefix(&mount)
        .map_err(|_| LifetimeError::UnsupportedHost)?;
    Ok(root.join(suffix))
}

#[cfg(any(target_os = "linux", test))]
fn unescape(value: &str) -> Result<PathBuf, LifetimeError> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut bytes = value.as_bytes();
    while let Some((&byte, rest)) = bytes.split_first() {
        if byte == b'\\' {
            let (escape, rest) = rest
                .split_at_checked(3)
                .ok_or(LifetimeError::UnsupportedHost)?;
            decoded.push(match escape {
                b"040" => b' ',
                b"011" => b'\t',
                b"012" => b'\n',
                b"134" => b'\\',
                _ => return Err(LifetimeError::UnsupportedHost),
            });
            bytes = rest;
        } else {
            decoded.push(byte);
            bytes = rest;
        }
    }
    let value = String::from_utf8(decoded).map_err(|_| LifetimeError::UnsupportedHost)?;
    let path = PathBuf::from(&value);
    if !path.is_absolute() || value.split('/').any(|part| part == "." || part == "..") {
        return Err(LifetimeError::UnsupportedHost);
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mount_roots_reveal_nested_bind_aliases_and_decode_escapes() {
        let mounts = "41 1 8:1 /lab /out rw - ext4 /dev/root rw\n42 1 8:1 /lab/state/child /scratch rw shared:9 - ext4 /dev/root rw\n43 1 8:1 /lab/space\\040and\\134slash /escaped rw - ext4 /dev/root rw\n";
        let state = filesystem_path(Path::new("/out/state"), "mnt_id:\t41\n", mounts).unwrap();
        let scratch = filesystem_path(Path::new("/scratch/work"), "mnt_id:\t42\n", mounts).unwrap();
        assert_eq!(state, Path::new("/lab/state"));
        assert_eq!(scratch, Path::new("/lab/state/child/work"));
        assert!(scratch.starts_with(state));
        assert_eq!(
            filesystem_path(Path::new("/escaped"), "mnt_id:\t43\n", mounts).unwrap(),
            Path::new("/lab/space and\\slash")
        );
        for fd in ["mnt_id:\t44\n", "mnt_id:\t41\nmnt_id:\t41\n"] {
            assert!(filesystem_path(Path::new("/out/state"), fd, mounts).is_err());
        }
        assert!(
            filesystem_path(
                Path::new("/out/state"),
                "mnt_id:\t41\n",
                &mounts.replace("ext4", "ext3")
            )
            .is_err()
        );
    }
}
