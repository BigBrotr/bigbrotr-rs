//! Private local descriptor custody shared by durable storage owners.
//! The helpers never replace a lock inode or explicitly unlock a duplicated
//! open file description. Store-specific identity and authority remain with the
//! owning adapter; a successful flock is not proof of database rollback.
use rustix::fs::{FlockOperation, Mode, OFlags, flock, openat};
use std::{
    fs::{self, File},
    future::Future,
    os::unix::fs::MetadataExt,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};
#[cfg(target_os = "linux")]
use std::{io::Read, os::fd::AsRawFd};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustodyError {
    InvalidRoot,
    UnsupportedHost,
    Identity,
    Exists,
    Occupied,
    Incomplete,
    Filesystem,
    ResourceLimit,
}

pub(crate) fn retain_connection<F: Future + Unpin>(
    lock: &File,
    future: F,
) -> Result<impl Future<Output = F::Output> + use<F>, CustodyError> {
    let custody = lock.try_clone().map_err(|_| CustodyError::Filesystem)?;
    Ok(CustodiedConnection {
        future,
        _custody: custody,
    })
}

// Field order is intentional for cancellation: destroy the actual transport
// before closing its lock clone, whether or not it was ever polled.
struct CustodiedConnection<F> {
    future: F,
    _custody: File,
}
impl<F: Future + Unpin> Future for CustodiedConnection<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().future).poll(cx)
    }
}

pub(crate) fn directory(path: &Path) -> Result<File, CustodyError> {
    if !path.is_absolute() || path.canonicalize().ok().as_deref() != Some(path) {
        return Err(CustodyError::InvalidRoot);
    }
    let file = File::from(
        rustix::fs::open(
            path,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .map_err(|_| CustodyError::InvalidRoot)?,
    );
    validate_directory(&file)?;
    Ok(file)
}
fn validate_directory(file: &File) -> Result<(), CustodyError> {
    let m = file.metadata().map_err(|_| CustodyError::Filesystem)?;
    if !m.is_dir() || m.mode() & 0o777 != 0o700 || m.uid() != rustix::process::geteuid().as_raw() {
        Err(CustodyError::InvalidRoot)
    } else {
        Ok(())
    }
}
pub(crate) fn open_directory(parent: &File, name: &str) -> Result<File, CustodyError> {
    let file = File::from(
        openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .map_err(|_| CustodyError::Incomplete)?,
    );
    validate_directory(&file)?;
    Ok(file)
}
pub(crate) fn create_file(parent: &File, name: &str) -> Result<File, CustodyError> {
    openat(
        parent,
        name,
        OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::CREATE | OFlags::EXCL,
        Mode::RUSR | Mode::WUSR,
    )
    .map(File::from)
    .map_err(|_| CustodyError::Filesystem)
}
pub(crate) fn open_file(parent: &File, name: &str) -> Result<File, CustodyError> {
    openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| CustodyError::Incomplete)
}
pub(crate) fn private_file(m: &fs::Metadata) -> bool {
    m.is_file()
        && m.mode() & 0o777 == 0o600
        && m.uid() == rustix::process::geteuid().as_raw()
        && m.nlink() == 1
}
pub(crate) fn validate_lock(file: &File) -> Result<(), CustodyError> {
    let m = file.metadata().map_err(|_| CustodyError::Filesystem)?;
    if private_file(&m) && m.len() == 0 {
        Ok(())
    } else {
        Err(CustodyError::Identity)
    }
}
pub(crate) fn acquire(file: &File) -> Result<(), CustodyError> {
    flock(file, FlockOperation::NonBlockingLockExclusive).map_err(|e| {
        if e == rustix::io::Errno::WOULDBLOCK {
            CustodyError::Occupied
        } else {
            CustodyError::Filesystem
        }
    })
}
#[cfg(target_os = "linux")]
pub(crate) fn host_boot(root: &Path) -> Result<String, CustodyError> {
    // This starting implementation deliberately excludes network/overlay storage.
    // Identical paths or copied inode metadata on another host are not exclusion.
    let directory = directory(root)?;
    if rustix::fs::fstatfs(&directory)
        .map_err(|_| CustodyError::Filesystem)?
        .f_type
        != 0xef53
    {
        return Err(CustodyError::UnsupportedHost);
    }
    // ext2/ext3 share ext4's statfs magic. Inspect this open descriptor's
    // actual mount ID in this mount namespace before selecting ext4 semantics.
    let fdinfo = bounded_proc(
        &format!("/proc/self/fdinfo/{}", directory.as_raw_fd()),
        4096,
    )?;
    let mountinfo = bounded_proc("/proc/self/mountinfo", 128 * 1024)?;
    if !ext4_mount(&fdinfo, &mountinfo)? {
        return Err(CustodyError::UnsupportedHost);
    }
    let mut bytes = Vec::with_capacity(38);
    File::open("/proc/sys/kernel/random/boot_id")
        .map_err(|_| CustodyError::UnsupportedHost)?
        .take(38)
        .read_to_end(&mut bytes)
        .map_err(|_| CustodyError::UnsupportedHost)?;
    if bytes.len() != 37 || bytes.pop() != Some(b'\n') {
        return Err(CustodyError::UnsupportedHost);
    }
    String::from_utf8(bytes).map_err(|_| CustodyError::UnsupportedHost)
}

#[cfg(any(target_os = "linux", test))]
fn ext4_mount(fdinfo: &str, mountinfo: &str) -> Result<bool, CustodyError> {
    let mut ids = fdinfo
        .lines()
        .filter_map(|line| line.strip_prefix("mnt_id:\t"));
    let id = ids
        .next()
        .filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
        .ok_or(CustodyError::UnsupportedHost)?;
    if ids.next().is_some() {
        return Err(CustodyError::UnsupportedHost);
    }
    let mut mounts = mountinfo
        .lines()
        .filter(|line| line.split_ascii_whitespace().next() == Some(id));
    let mounted = mounts
        .next()
        .and_then(|line| line.split_once(" - "))
        .and_then(|(_, tail)| tail.split_ascii_whitespace().next())
        .ok_or(CustodyError::UnsupportedHost)?;
    if mounts.next().is_some() {
        return Err(CustodyError::UnsupportedHost);
    }
    Ok(mounted == "ext4")
}
#[cfg(target_os = "linux")]
pub(crate) fn bounded_proc(path: &str, maximum: usize) -> Result<String, CustodyError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(maximum + 1)
        .map_err(|_| CustodyError::ResourceLimit)?;
    File::open(path)
        .map_err(|_| CustodyError::UnsupportedHost)?
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| CustodyError::UnsupportedHost)?;
    if bytes.len() > maximum {
        return Err(CustodyError::ResourceLimit);
    }
    String::from_utf8(bytes).map_err(|_| CustodyError::UnsupportedHost)
}
#[cfg(not(target_os = "linux"))]
pub(crate) fn host_boot(_root: &Path) -> Result<String, CustodyError> {
    Err(CustodyError::UnsupportedHost)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mount_profile_uses_the_descriptors_actual_mount_not_shared_ext_magic() {
        let fdinfo = "pos:\t0\nflags:\t02100000\nmnt_id:\t45\nino:\t55\n";
        for (fstype, accepted) in [
            ("ext4", true),
            ("ext3", false),
            ("ext2", false),
            ("nfs", false),
            ("overlay", false),
        ] {
            let mounts = format!(
                "44 1 8:1 / /other rw - ext4 /dev/other rw\n45 1 8:2 / /journal rw - {fstype} /dev/journal rw\n"
            );
            assert_eq!(ext4_mount(fdinfo, &mounts), Ok(accepted), "{fstype}");
        }
        assert!(ext4_mount("mnt_id:\t45\nmnt_id:\t46\n", "").is_err());
        assert!(ext4_mount(fdinfo, "44 1 8:1 / / rw - ext4 /dev/a rw\n").is_err());
    }
}
