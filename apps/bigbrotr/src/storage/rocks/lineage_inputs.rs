//! Complete finite input census and exact staged-file deltas; this scanner never writes.
use super::*;
use crate::storage::rocks::store::BootstrapError;
use std::sync::atomic::{AtomicBool, Ordering};

// Names are fixed canonical bytes. Hash every complete file through the existing
// 8KiB buffer; sorting only these fixed stamps makes the aggregate order stable.
type Stamp = ([u8; 71], [u8; 32], u64);
const _: () = assert!(std::mem::size_of::<[Stamp; MAX_FILES]>() + SCRATCH < 16 * 1024);

struct InputScan {
    all: [u8; 32],
    previous: [u8; 32],
    tail: Option<SuccessorStagedInput>,
    inventory: ClosedInputInventory,
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

    /// Recovery authenticates a retained initial residual without hiding its
    /// separately observed issued+1 tail. Neither optional class grants credit.
    pub(in crate::storage::rocks::store) fn require_compacted_recovery_input(
        &self,
        inputs: LineageInputSet,
        accepts: impl FnMut(ProducerRequestId) -> bool,
        tail: ProducerRequestId,
        checked: (ProducerRequestId, [u8; 32], u64),
        cancelled: &AtomicBool,
    ) -> Result<([u8; 32], Option<SuccessorStagedInput>), BootstrapError> {
        self.lineage_input_scan(inputs, accepts, None, Some(tail), Some(checked), cancelled)
            .map(|scan| (scan.all, scan.tail))
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
        let mut stamps: [Stamp; MAX_FILES] = [([0; 71], [0; 32], 0); MAX_FILES];
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
        let directory_identity = directory
            .as_ref()
            .map(|file| file.metadata().map(|m| (m.dev(), m.ino())))
            .transpose()
            .map_err(|_| BootstrapError::RequestInput(RequestInputError::Filesystem))?;
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
                    before.len(),
                );
                count += 1;
            }
        }
        if required_count != inputs.required_count {
            return Err(BootstrapError::RequestInput(RequestInputError::Missing));
        }
        stamps[..count].sort_unstable_by_key(|(name, _, _)| *name);
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
                .any(|(name, _, _)| name.as_slice() == excluded.as_bytes())
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
            for (name, stamp, _) in &stamps[..count] {
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
            inventory: ClosedInputInventory {
                stamps,
                directory: directory_identity,
                count,
                total,
            },
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

/// Complete fixed-size local inventory retained by a confirmed reclamation.
/// Canonical outcomes authenticate the bytes before this snapshot is minted.
/// Each successful removal changes exactly one entry; a missing or replaced
/// file outside that owned delta is never accepted as a retry of this seal.
pub(in crate::storage::rocks::store) struct ClosedInputInventory {
    stamps: [Stamp; MAX_FILES],
    directory: Option<(u64, u64)>,
    count: usize,
    total: u64,
}
impl ClosedInputInventory {
    pub(in crate::storage::rocks::store) fn range_count(
        &self,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        through: u64,
    ) -> u64 {
        self.stamps[..self.count]
            .iter()
            .filter(|stamp| {
                parse_name(&stamp.0).is_ok_and(|id| {
                    id.producer_slot_id == slot
                        && id.producer_epoch == epoch
                        && id.request_sequence.get() <= through
                })
            })
            .count() as u64
    }
    fn require(&self, store: &IngestStore, cancelled: &AtomicBool) -> Result<(), BootstrapError> {
        // The reclamation protocol keeps this directory even for an empty cut.
        let directory = store
            .input_directory(false)
            .map_err(BootstrapError::RequestInput)?;
        let metadata = directory
            .metadata()
            .map_err(|_| BootstrapError::RequestInput(RequestInputError::Filesystem))?;
        if self.directory != Some((metadata.dev(), metadata.ino())) {
            return Err(BootstrapError::StateMismatch);
        }
        drop(directory);
        let actual = store
            .lineage_input_scan(
                (self.count as u64).into(),
                |id| {
                    self.stamps[..self.count]
                        .iter()
                        .any(|stamp| stamp.0.as_slice() == name(id).as_bytes())
                },
                None,
                None,
                None,
                cancelled,
            )?
            .inventory;
        if actual.directory != self.directory
            || actual.count != self.count
            || actual.total != self.total
            || actual.stamps[..actual.count] != self.stamps[..self.count]
        {
            return Err(BootstrapError::StateMismatch);
        }
        Ok(())
    }
    /// This kernel is independent of initial-lifetime authority. Its caller
    /// supplies the exact authenticated producer range and current native seal.
    pub(in crate::storage::rocks::store) fn remove_range(
        &mut self,
        store: &IngestStore,
        slot: ProducerSlotId,
        epoch: ProducerEpoch,
        through: u64,
        cancelled: &AtomicBool,
        mut authority: impl FnMut() -> Result<(), BootstrapError>,
    ) -> Result<u64, BootstrapError> {
        let mut removed = 0;
        loop {
            authority()?;
            self.require(store, cancelled)?;
            let target = self.stamps[..self.count].iter().position(|stamp| {
                parse_name(&stamp.0).is_ok_and(|id| {
                    id.producer_slot_id == slot
                        && id.producer_epoch == epoch
                        && id.request_sequence.get() <= through
                })
            });
            let Some(index) = target else { break };
            let directory = store
                .input_directory(false)
                .map_err(BootstrapError::RequestInput)?;
            let filename = std::str::from_utf8(&self.stamps[index].0)
                .map_err(|_| BootstrapError::StateMismatch)?;
            authority()?;
            if cancelled.load(Ordering::Acquire) {
                return Err(BootstrapError::AuthorityUnavailable);
            }
            rustix::fs::unlinkat(&directory, filename, rustix::fs::AtFlags::empty())
                .map_err(|_| BootstrapError::CommitUnknown)?;
            self.total -= self.stamps[index].2;
            self.stamps.copy_within(index + 1..self.count, index);
            self.count -= 1;
            removed += 1;
            // Only our exact successful unlink authorizes this new census.
            // Cancellation or external loss returns no completion claim.
            self.require(store, cancelled)?;
        }
        let directory = store
            .input_directory(false)
            .map_err(BootstrapError::RequestInput)?;
        authority()?;
        directory
            .sync_all()
            .map_err(|_| BootstrapError::CommitUnknown)?;
        self.require(store, cancelled)?;
        authority()?;
        Ok(removed)
    }
}
impl IngestStore {
    pub(in crate::storage::rocks::store) fn closed_input_inventory(
        &self,
        slot: ProducerSlotId,
        through: u64,
        original: bool,
        cancelled: &AtomicBool,
    ) -> Result<ClosedInputInventory, BootstrapError> {
        let inputs = if original {
            through.into()
        } else {
            LineageInputSet::with_compacted_initial(0, slot, through)?
        };
        self.lineage_input_scan(
            inputs,
            |id| {
                id.producer_slot_id == slot
                    && id.producer_epoch.get() == 1
                    && id.request_sequence.get() <= through
            },
            None,
            None,
            None,
            cancelled,
        )
        .map(|scan| scan.inventory)
    }
    pub(in crate::storage::rocks::store) fn require_closed_input_inventory(
        &self,
        inventory: &ClosedInputInventory,
        cancelled: &AtomicBool,
    ) -> Result<(), BootstrapError> {
        inventory.require(self, cancelled)
    }
}
