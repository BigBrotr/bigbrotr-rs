//! Bounded regular public-file reads without following any symlink component.
use rustix::fs::{Mode, OFlags, open, openat};
use std::{
    fs::File,
    io::Read,
    path::{Component, Path},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileError {
    Open,
    NotRegular,
    TooLarge,
    Read,
}

pub(crate) fn read(path: &Path, limit: usize) -> Result<Vec<u8>, FileError> {
    if !path.is_absolute() {
        return Err(FileError::Open);
    }
    let directory_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut parent = open("/", directory_flags, Mode::empty()).map_err(|_| FileError::Open)?;
    let mut components = path.components().peekable();
    components.next();
    let descriptor = loop {
        let Some(Component::Normal(name)) = components.next() else {
            return Err(FileError::Open);
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
            .map_err(|_| FileError::Open)?;
        }
        parent =
            openat(&parent, name, directory_flags, Mode::empty()).map_err(|_| FileError::Open)?;
    };
    let mut file = File::from(descriptor);
    let metadata = file.metadata().map_err(|_| FileError::Open)?;
    if !metadata.is_file() {
        return Err(FileError::NotRegular);
    }
    if metadata.len() > u64::try_from(limit).map_err(|_| FileError::TooLarge)? {
        return Err(FileError::TooLarge);
    }
    let capacity = limit.checked_add(1).ok_or(FileError::TooLarge)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| FileError::Read)?;
    bytes.resize(capacity, 0);
    let mut used = 0;
    loop {
        match file.read(&mut bytes[used..]) {
            Ok(0) => break,
            Ok(read) => {
                used += read;
                if used > limit {
                    return Err(FileError::TooLarge);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(FileError::Read),
        }
    }
    bytes.truncate(used);
    Ok(bytes)
}
