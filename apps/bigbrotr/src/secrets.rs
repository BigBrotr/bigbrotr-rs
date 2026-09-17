//! Protected local-file input for credential owners, never offline configuration.
//!
//! This boundary checks descriptors and POSIX ownership/mode, not provider
//! authorization or filesystem ACLs. The logical read buffer is `limit + 1`;
//! allocator overhead and any driver copies require separate resource accounting.
//! Callers retain the allocation's charge for its whole lifetime.
use rustix::fs::{Mode, OFlags, open, openat};
use std::{
    fmt,
    fs::File,
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Component, Path},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretError {
    Reference,
    InvalidLimit,
    Open,
    NotRegular,
    Owner,
    Permissions,
    TooLarge,
    Allocation,
    Read,
}
impl fmt::Display for SecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Reference => "invalid bounded absolute local-file secret reference",
            Self::InvalidLimit => "secret input requires a finite positive byte limit",
            Self::Open => "cannot open protected secret descriptor",
            Self::NotRegular => "secret descriptor must be a regular file",
            Self::Owner => "secret file must be owned by the effective process user",
            Self::Permissions => "secret file mode must be 0400 or 0600",
            Self::TooLarge => "secret input exceeds the declared byte limit",
            Self::Allocation => "cannot reserve secret input buffer",
            Self::Read => "cannot read protected secret descriptor",
        })
    }
}
impl std::error::Error for SecretError {}

/// A validated reference is a name, not permission to load it.
pub struct SecretFile<'a>(&'a Path);
impl fmt::Debug for SecretFile<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretFile([redacted])")
    }
}
impl<'a> SecretFile<'a> {
    pub fn parse(reference: &'a str) -> Result<Self, SecretError> {
        let text = reference
            .strip_prefix("file:")
            .ok_or(SecretError::Reference)?;
        if reference.len() > 8192
            || !text.starts_with('/')
            || text.ends_with('/')
            || text.chars().any(char::is_control)
            || text.split('/').any(|part| part == "." || part == "..")
        {
            return Err(SecretError::Reference);
        }
        Ok(Self(Path::new(text)))
    }

    /// Load exact bytes without trimming, text conversion or a second path open.
    /// Owner adapters choose their own nonempty/encoding/password/key semantics.
    pub fn read(&self, limit: usize) -> Result<SecretBytes, SecretError> {
        let capacity = limit
            .checked_add(1)
            .filter(|_| limit > 0)
            .ok_or(SecretError::InvalidLimit)?;
        // Walk relative to retained directory descriptors. NOFOLLOW on only the
        // final path would still follow a replaced parent symlink.
        let directory_flags =
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let mut parent =
            open("/", directory_flags, Mode::empty()).map_err(|_| SecretError::Open)?;
        let mut components = self.0.components().peekable();
        components.next(); // Absolute root, established by parse.
        let descriptor = loop {
            let Some(Component::Normal(name)) = components.next() else {
                return Err(SecretError::Reference);
            };
            if components.peek().is_none() {
                break openat(
                    &parent,
                    name,
                    OFlags::RDONLY
                        | OFlags::CLOEXEC
                        | OFlags::NOFOLLOW
                        | OFlags::NONBLOCK
                        | OFlags::NOCTTY,
                    Mode::empty(),
                )
                .map_err(|_| SecretError::Open)?;
            }
            parent = openat(&parent, name, directory_flags, Mode::empty())
                .map_err(|_| SecretError::Open)?;
        };
        let mut file = File::from(descriptor);
        let metadata = file.metadata().map_err(|_| SecretError::Open)?;
        if !metadata.is_file() {
            return Err(SecretError::NotRegular);
        }
        if metadata.uid() != rustix::process::geteuid().as_raw() {
            return Err(SecretError::Owner);
        }
        if !matches!(metadata.mode() & 0o7777, 0o400 | 0o600) {
            return Err(SecretError::Permissions);
        }
        if metadata.len() > u64::try_from(limit).map_err(|_| SecretError::InvalidLimit)? {
            return Err(SecretError::TooLarge);
        }
        read_contents(&mut file, limit, capacity)
    }
}

fn read_contents(
    file: &mut impl Read,
    limit: usize,
    capacity: usize,
) -> Result<SecretBytes, SecretError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| SecretError::Allocation)?;
    bytes.resize(capacity, 0);
    let mut used = 0;
    loop {
        match file.read(&mut bytes[used..]) {
            Ok(0) => break,
            Ok(read) => {
                used += read;
                if used > limit {
                    return Err(SecretError::TooLarge);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(SecretError::Read),
        }
    }
    bytes.truncate(used);
    Ok(SecretBytes(bytes))
}

/// Move-only bytes with intentionally redacted diagnostics. No serialization or
/// Clone implementation; a driver's explicit copy needs its own resource charge.
/// This does not promise zeroization of process memory or downstream driver copies.
pub struct SecretBytes(Vec<u8>);
impl SecretBytes {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn allocated_bytes(&self) -> usize {
        self.0.capacity()
    }
}
impl fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretBytes([redacted])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn growth_after_metadata_is_bounded_by_the_actual_read() {
        let mut file = tempfile::tempfile().unwrap();
        let checked_size = file.metadata().unwrap().len();
        assert_eq!(checked_size, 0);
        file.write_all(b"12345").unwrap();
        std::io::Seek::rewind(&mut file).unwrap();
        assert_eq!(
            read_contents(&mut file, 4, 5).unwrap_err(),
            SecretError::TooLarge
        );
        assert_eq!(std::io::Seek::stream_position(&mut file).unwrap(), 5);
    }
}
