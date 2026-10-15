//! Complete finite input census and exact staged-file deltas; this scanner never writes.
use super::*;
use crate::storage::rocks::store::BootstrapError;
use std::sync::atomic::{AtomicBool, Ordering};

// Names are fixed canonical bytes. Hash every complete file through the existing
// 8KiB buffer; sorting only these fixed stamps makes the aggregate order stable.
type Stamp = ([u8; 71], [u8; 32]);
const _: () = assert!(std::mem::size_of::<[Stamp; MAX_FILES]>() + SCRATCH < 16 * 1024);

impl IngestStore {
    pub(in crate::storage::rocks::store) fn lineage_input_census(
        &self,
        expected_count: u64,
        accepts: impl FnMut(ProducerRequestId) -> bool,
        cancelled: &AtomicBool,
    ) -> Result<[u8; 32], BootstrapError> {
        self.lineage_input_delta(expected_count, accepts, None, cancelled)
            .map(|(all, _)| all)
    }

    /// Same complete bounded scan, optionally proving one exact new staged file.
    /// The second digest excludes only that file and must match the prior census.
    /// Unknown files are still refused, never filtered out of the directory.
    pub(in crate::storage::rocks::store) fn lineage_input_delta(
        &self,
        expected_count: u64,
        mut accepts: impl FnMut(ProducerRequestId) -> bool,
        addition: Option<(ProducerRequestId, [u8; 32], u64)>,
        cancelled: &AtomicBool,
    ) -> Result<([u8; 32], [u8; 32]), BootstrapError> {
        let check = || {
            if cancelled.load(Ordering::Relaxed) {
                Err(BootstrapError::AuthorityUnavailable)
            } else {
                Ok(())
            }
        };
        check()?;
        if expected_count > MAX_FILES as u64 {
            return Err(BootstrapError::Limit);
        }
        let mut stamps: [Stamp; MAX_FILES] = [([0; 71], [0; 32]); MAX_FILES];
        let mut count = 0;
        let mut total = 0_u64;
        let mut buffer = [0; SCRATCH];
        let directory = match self.input_directory(false) {
            Ok(directory) => Some(directory),
            Err(RequestInputError::Missing) if expected_count == 0 => None,
            Err(error) => return Err(BootstrapError::RequestInput(error)),
        };
        if let Some(directory) = directory {
            for entry in Dir::read_from(&directory)
                .map_err(|_| BootstrapError::RequestInput(RequestInputError::Filesystem))?
            {
                check()?;
                let entry = entry
                    .map_err(|_| BootstrapError::RequestInput(RequestInputError::Filesystem))?;
                let name = entry.file_name().to_bytes();
                if name == b"." || name == b".." {
                    continue;
                }
                if count == MAX_FILES {
                    return Err(BootstrapError::Limit);
                }
                let id = parse_name(name).map_err(BootstrapError::RequestInput)?;
                if !accepts(id) {
                    return Err(BootstrapError::RequestInput(RequestInputError::Identity));
                }
                let mut file = open_input(
                    &directory,
                    std::str::from_utf8(name)
                        .map_err(|_| BootstrapError::RequestInput(RequestInputError::Identity))?,
                )
                .map_err(BootstrapError::RequestInput)?;
                let before = file
                    .metadata()
                    .map_err(|_| BootstrapError::RequestInput(RequestInputError::Filesystem))?;
                if before.len() == 0 || before.len() > MAX_INPUT as u64 || !private_file(&before) {
                    return Err(BootstrapError::RequestInput(RequestInputError::Conflict));
                }
                total = total
                    .checked_add(before.len())
                    .filter(|total| *total <= MAX_TOTAL)
                    .ok_or(BootstrapError::Limit)?;
                let mut content = Sha256::new();
                let mut read = 0_u64;
                loop {
                    check()?;
                    let n = file
                        .read(&mut buffer)
                        .map_err(|_| BootstrapError::RequestInput(RequestInputError::Filesystem))?;
                    if n == 0 {
                        break;
                    }
                    read = read
                        .checked_add(n as u64)
                        .filter(|read| *read <= before.len())
                        .ok_or(BootstrapError::RequestInput(RequestInputError::Conflict))?;
                    content.update(&buffer[..n]);
                }
                let after = file
                    .metadata()
                    .map_err(|_| BootstrapError::RequestInput(RequestInputError::Filesystem))?;
                if read != before.len()
                    || identity(&before) != identity(&after)
                    || !private_file(&after)
                {
                    return Err(BootstrapError::RequestInput(RequestInputError::Conflict));
                }
                let content: [u8; 32] = content.finalize().into();
                if let Some((added, expected, length)) = addition
                    && id == added
                    && (content != expected || before.len() != length)
                {
                    return Err(BootstrapError::RequestInput(RequestInputError::Conflict));
                }
                let mut stamp = Sha256::new();
                stamp.update(content);
                for word in identity(&before) {
                    stamp.update(word.to_be_bytes());
                }
                stamps[count] = (
                    name.try_into().expect("validated fixed name"),
                    stamp.finalize().into(),
                );
                count += 1;
            }
        }
        if count as u64 != expected_count {
            return Err(BootstrapError::RequestInput(RequestInputError::Missing));
        }
        stamps[..count].sort_unstable_by_key(|(name, _)| *name);
        if stamps[..count]
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0)
        {
            return Err(BootstrapError::RequestInput(RequestInputError::Conflict));
        }
        let excluded = addition.map(|(id, _, _)| name(id));
        if let Some(excluded) = &excluded
            && !stamps[..count]
                .iter()
                .any(|(name, _)| name.as_slice() == excluded.as_bytes())
        {
            return Err(BootstrapError::RequestInput(RequestInputError::Missing));
        }
        let digest = |omit: bool| {
            let mut hash = Sha256::new();
            hash.update(b"bigbrotr/local-lineage-input-census/1\0");
            hash.update((count as u64 - u64::from(omit)).to_be_bytes());
            hash.update(
                (total
                    - if omit {
                        addition.expect("one checked addition").2
                    } else {
                        0
                    })
                .to_be_bytes(),
            );
            for (name, stamp) in &stamps[..count] {
                if !(omit
                    && excluded
                        .as_ref()
                        .is_some_and(|excluded| name.as_slice() == excluded.as_bytes()))
                {
                    hash.update(name);
                    hash.update(stamp);
                }
            }
            hash.finalize().into()
        };
        let all = digest(false);
        let previous = if addition.is_some() {
            digest(true)
        } else {
            all
        };
        check()?;
        Ok((all, previous))
    }
}

fn identity(m: &std::fs::Metadata) -> [u64; 7] {
    [
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime() as u64,
        m.mtime_nsec() as u64,
        m.ctime() as u64,
        m.ctime_nsec() as u64,
    ]
}
fn parse_name(name: &[u8]) -> Result<ProducerRequestId, RequestInputError> {
    if !valid_name(name) {
        return Err(RequestInputError::Identity);
    }
    let mut slot = [0; 16];
    for (index, byte) in slot.iter_mut().enumerate() {
        *byte = u8::from_str_radix(
            std::str::from_utf8(&name[2 * index..2 * index + 2])
                .map_err(|_| RequestInputError::Identity)?,
            16,
        )
        .map_err(|_| RequestInputError::Identity)?;
    }
    let number = |bytes: &[u8]| {
        u64::from_str_radix(
            std::str::from_utf8(bytes).map_err(|_| RequestInputError::Identity)?,
            16,
        )
        .map_err(|_| RequestInputError::Identity)
    };
    Ok(ProducerRequestId {
        producer_slot_id: ProducerSlotId::try_from(slot)
            .map_err(|_| RequestInputError::Identity)?,
        producer_epoch: ProducerEpoch::new(number(&name[33..49])?)
            .map_err(|_| RequestInputError::Identity)?,
        request_sequence: RequestSequence::new(number(&name[50..66])?)
            .map_err(|_| RequestInputError::Identity)?,
    })
}
