//! Complete finite input census and exact staged-file deltas; this scanner never writes.
use super::*;
use crate::storage::rocks::store::BootstrapError;
use std::sync::atomic::{AtomicBool, Ordering};

// Names are fixed canonical bytes. Hash every complete file through the existing
// 8KiB buffer; sorting only these fixed stamps makes the aggregate order stable.
type Stamp = ([u8; 71], [u8; 32]);
const _: () = assert!(std::mem::size_of::<[Stamp; MAX_FILES]>() + SCRATCH < 16 * 1024);

struct InputScan {
    all: [u8; 32],
    previous: [u8; 32],
    tail: Option<SuccessorStagedInput>,
}

/// Required originals and one optional initial residual range share a physical
/// spool. The predicate supplied to a scan identifies required originals only
/// after subtracting this range; residual presence never supplies required credit.
#[derive(Clone, Copy)]
pub(in crate::storage::rocks::store) struct LineageInputSet {
    required_count: u64,
    compacted_initial: Option<(ProducerSlotId, u64)>,
    optional: Option<(ProducerSlotId, ProducerEpoch, u64, u64)>,
}

impl From<u64> for LineageInputSet {
    fn from(required_count: u64) -> Self {
        Self {
            required_count,
            compacted_initial: None,
            optional: None,
        }
    }
}

impl LineageInputSet {
    pub(in crate::storage::rocks::store) fn with_compacted_initial(
        required_count: u64,
        slot: ProducerSlotId,
        floor: u64,
    ) -> Result<Self, BootstrapError> {
        if floor == 0 {
            return Err(BootstrapError::InvalidIntent);
        }
        if required_count > MAX_FILES as u64 || floor > MAX_FILES as u64 {
            return Err(BootstrapError::Limit);
        }
        // Only actual residual files consume the shared 64-file allowance.
        Ok(Self {
            required_count,
            compacted_initial: Some((slot, floor)),
            optional: None,
        })
    }

    /// Positive recovery may preserve staged bytes beyond native issuance.
    /// These consume physical capacity but never satisfy the required count.
    pub(in crate::storage::rocks::store) fn with_optional_inputs(
        mut self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        issued: u64,
        through: u64,
    ) -> Result<Self, BootstrapError> {
        if epoch.get() != 2 || issued > through || through > MAX_FILES as u64 {
            return Err(BootstrapError::InvalidIntent);
        }
        self.optional = (issued < through).then_some((slot, epoch, issued, through));
        Ok(self)
    }

    fn optional(self, id: ProducerRequestId) -> bool {
        self.optional.is_some_and(|(slot, epoch, issued, through)| {
            id.producer_slot_id == slot
                && id.producer_epoch == epoch
                && id.request_sequence.get() > issued
                && id.request_sequence.get() <= through
        })
    }

    fn residual(self, id: ProducerRequestId) -> bool {
        self.compacted_initial.is_some_and(|(slot, floor)| {
            id.producer_slot_id == slot
                && id.producer_epoch.get() == 1
                && id.request_sequence.get() <= floor
        })
    }
}

impl IngestStore {
    pub(in crate::storage::rocks::store) fn lineage_input_census(
        &self,
        inputs: impl Into<LineageInputSet>,
        accepts: impl FnMut(ProducerRequestId) -> bool,
        cancelled: &AtomicBool,
    ) -> Result<[u8; 32], BootstrapError> {
        self.lineage_input_delta(inputs, accepts, None, cancelled)
            .map(|(all, _)| all)
    }

    /// Same complete bounded scan, optionally proving one exact new staged file.
    /// The second digest excludes only that file and must match the prior census.
    /// Unknown files are still refused, never filtered out of the directory.
    pub(in crate::storage::rocks::store) fn lineage_input_delta(
        &self,
        inputs: impl Into<LineageInputSet>,
        accepts: impl FnMut(ProducerRequestId) -> bool,
        addition: Option<(ProducerRequestId, [u8; 32], u64)>,
        cancelled: &AtomicBool,
    ) -> Result<([u8; 32], [u8; 32]), BootstrapError> {
        self.lineage_input_scan(inputs.into(), accepts, addition, None, None, cancelled)
            .map(|scan| (scan.all, scan.previous))
    }

    /// Recovery alone may retain one current issued+1 input. All issued files
    /// remain required; the optional file is stamped, never treated as issued.
    pub(in crate::storage::rocks::store) fn successor_recovery_inputs(
        &self,
        inputs: impl Into<LineageInputSet>,
        accepts: impl FnMut(ProducerRequestId) -> bool,
        tail: ProducerRequestId,
        cancelled: &AtomicBool,
    ) -> Result<([u8; 32], Option<SuccessorStagedInput>), BootstrapError> {
        self.lineage_input_scan(inputs.into(), accepts, None, Some(tail), None, cancelled)
            .map(|scan| (scan.all, scan.tail))
    }

    /// Optional residual originals beneath one already observed native floor.
    /// Their bytes are stamped, not authenticated against PostgreSQL here.
    pub(in crate::storage::rocks::store) fn compacted_input_census(
        &self,
        slot: ProducerSlotId,
        floor: u64,
        cancelled: &AtomicBool,
    ) -> Result<[u8; 32], BootstrapError> {
        self.lineage_input_scan(
            super::LineageInputSet::with_compacted_initial(0, slot, floor)?,
            |_| false,
            None,
            None,
            None,
            cancelled,
        )
        .map(|scan| scan.all)
    }

    pub(in crate::storage::rocks::store) fn require_compacted_input(
        &self,
        floor: u64,
        request: ProducerRequestId,
        digest: [u8; 32],
        length: u64,
        cancelled: &AtomicBool,
    ) -> Result<[u8; 32], BootstrapError> {
        self.require_compacted_lineage_input(
            super::LineageInputSet::with_compacted_initial(0, request.producer_slot_id, floor)?,
            |_| false,
            (request, digest, length),
            cancelled,
        )
    }

    /// Authenticate a residual only if present, while retaining the complete
    /// required/residual census. An absent original never excuses a missing
    /// required file or changes the caller's recorded global stamp.
    pub(in crate::storage::rocks::store) fn require_compacted_lineage_input(
        &self,
        inputs: LineageInputSet,
        accepts: impl FnMut(ProducerRequestId) -> bool,
        checked: (ProducerRequestId, [u8; 32], u64),
        cancelled: &AtomicBool,
    ) -> Result<[u8; 32], BootstrapError> {
        self.lineage_input_scan(inputs, accepts, None, None, Some(checked), cancelled)
            .map(|scan| scan.all)
    }

    fn lineage_input_scan(
        &self,
        inputs: LineageInputSet,
        mut accepts: impl FnMut(ProducerRequestId) -> bool,
        addition: Option<(ProducerRequestId, [u8; 32], u64)>,
        tail: Option<ProducerRequestId>,
        checked: Option<(ProducerRequestId, [u8; 32], u64)>,
        cancelled: &AtomicBool,
    ) -> Result<InputScan, BootstrapError> {
        let check = || {
            if cancelled.load(Ordering::Relaxed) {
                Err(BootstrapError::AuthorityUnavailable)
            } else {
                Ok(())
            }
        };
        check()?;
        if inputs.required_count > MAX_FILES as u64 {
            return Err(BootstrapError::Limit);
        }
        // A staged tail is not issued; a delta removes one required original
        // from its prior stamp. Neither can silently consume a residual slot.
        if tail.is_some_and(|id| inputs.residual(id) || inputs.optional(id) || accepts(id))
            || addition.is_some_and(|(id, _, _)| {
                inputs.residual(id) || inputs.optional(id) || Some(id) == tail || !accepts(id)
            })
            || checked.is_some_and(|(id, _, _)| !inputs.residual(id))
        {
            return Err(BootstrapError::InvalidIntent);
        }
        let mut stamps: [Stamp; MAX_FILES] = [([0; 71], [0; 32]); MAX_FILES];
        let mut count = 0;
        let mut required_count = 0;
        let mut total = 0_u64;
        let mut observed_tail = None;
        let mut buffer = [0; SCRATCH];
        let directory = match self.input_directory(false) {
            Ok(directory) => Some(directory),
            // An actually empty legacy lifetime may lack a spool. A nonempty
            // compacted lifetime only unlinks files; its directory must remain.
            Err(RequestInputError::Missing)
                if inputs.required_count == 0 && inputs.compacted_initial.is_none() =>
            {
                None
            }
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
                if inputs.residual(id) || inputs.optional(id) || Some(id) == tail {
                    // Every optional class is still read, charged and stamped.
                } else if accepts(id) {
                    required_count += 1;
                } else {
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
                if Some(id) == tail {
                    observed_tail = Some(SuccessorStagedInput {
                        request: id,
                        digest: Digest256::from_bytes(content),
                        input_bytes: before.len() as u32, // bounded by MAX_INPUT
                    });
                }
                if let Some((added, expected, length)) = addition.or(checked)
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
        if required_count != inputs.required_count {
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
        Ok(InputScan {
            all,
            previous,
            tail: observed_tail,
        })
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
