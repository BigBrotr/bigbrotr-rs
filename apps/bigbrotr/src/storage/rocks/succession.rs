//! Portable repeatable handoff data and finite profile arithmetic.
//!
//! Neither a decoded record nor an admitted arithmetic profile establishes a
//! lineage, native custody, PostgreSQL authority or a usable writer.
use super::{
    keys::IngestKey,
    manifest::{IngestManifest, MAX_MANIFEST_BYTES, ManifestError},
};
use bigbrotr_model::{DictionaryKind, ProducerEpoch, ProducerSlotId};
use std::fmt;

const HEADER: [u8; 2] = [2, 10];
const FIXED_BYTES: usize = 111;
const MIN_MANIFEST_BYTES: usize = 104;
pub const MIN_SUCCESSION_BYTES: usize = FIXED_BYTES + 2 * MIN_MANIFEST_BYTES;
pub const MAX_SUCCESSION_BYTES: usize = FIXED_BYTES + 2 * MAX_MANIFEST_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PredecessorTerminalKind {
    /// The predecessor activated and subsequently reached terminal retirement.
    Activated = 1,
    /// The prepared predecessor retired empty without ever activating.
    UnactivatedEmpty = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuccessionError {
    WrongLength,
    UnsupportedFormat,
    UnknownTerminalKind,
    InvalidIdentity,
    InvalidEpoch,
    InvalidCounts,
    InvalidTransition,
    InvalidNamespaces,
    Manifest(ManifestError),
}
impl fmt::Display for SuccessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WrongLength => "succession record length does not match its closed fields",
            Self::UnsupportedFormat => "unsupported succession value version or type",
            Self::UnknownTerminalKind => "unknown predecessor terminal kind",
            Self::InvalidIdentity => "succession producer slot must be nonzero",
            Self::InvalidEpoch => "succession predecessor epoch must be at least two",
            Self::InvalidCounts => "succession counts do not match the terminal kind",
            Self::InvalidTransition => "succession manifests do not describe the next lifetime",
            Self::InvalidNamespaces => {
                "succession allocation namespaces are not fresh and distinct"
            }
            Self::Manifest(_) => "succession contains an invalid manifest",
        })
    }
}
impl std::error::Error for SuccessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Manifest(error) => Some(error),
            _ => None,
        }
    }
}

/// Untrusted portable fields. Construction checks only the two-manifest shape;
/// hashes, terminal states, older freshness and native effects need real owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuccessionFields {
    pub terminal_kind: PredecessorTerminalKind,
    pub producer_slot_id: ProducerSlotId,
    pub predecessor_epoch: ProducerEpoch,
    pub issued_through: u64,
    pub last_complete_sequence: u64,
    pub closure_digest: [u8; 32],
    pub incoming_handoff_sha256: [u8; 32],
    pub previous_manifest: IngestManifest,
    pub next_manifest: IngestManifest,
}

/// Closed version2 bytes under the unchanged owner-handoff key. This is data,
/// intentionally copyable, with no method converting it to an authority proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuccessionRecord {
    fields: SuccessionFields,
}
impl SuccessionRecord {
    pub fn new(fields: SuccessionFields) -> Result<Self, SuccessionError> {
        validate(&fields)?;
        Ok(Self { fields })
    }

    pub const fn fields(&self) -> &SuccessionFields {
        &self.fields
    }

    /// Expected portable key; a reader must also compare the actual retained key.
    pub fn key(&self) -> IngestKey {
        let identity = self.fields.previous_manifest.identity();
        IngestKey::OwnerHandoff {
            dataset_id: identity.dataset_id,
            owner_resource_id: identity.owner_resource_id,
            predecessor_epoch: identity.owner_epoch,
        }
    }

    pub fn encode(&self) -> EncodedSuccession {
        let fields = &self.fields;
        let previous = fields.previous_manifest.encode();
        let next = fields.next_manifest.encode();
        let previous = previous.as_bytes();
        let next = next.as_bytes();
        let mut encoded = EncodedSuccession {
            bytes: [0; MAX_SUCCESSION_BYTES],
            len: FIXED_BYTES + previous.len() + next.len(),
        };
        let bytes = &mut encoded.bytes;
        bytes[..2].copy_from_slice(&HEADER);
        bytes[2] = fields.terminal_kind as u8;
        bytes[3..19].copy_from_slice(fields.producer_slot_id.as_bytes());
        bytes[19..27].copy_from_slice(&fields.predecessor_epoch.get().to_be_bytes());
        bytes[27..35].copy_from_slice(&fields.issued_through.to_be_bytes());
        bytes[35..43].copy_from_slice(&fields.last_complete_sequence.to_be_bytes());
        bytes[43..75].copy_from_slice(&fields.closure_digest);
        bytes[75..107].copy_from_slice(&fields.incoming_handoff_sha256);
        // The manifest codec's fixed maximum is 266, below u16::MAX.
        bytes[107..109].copy_from_slice(&(previous.len() as u16).to_be_bytes());
        bytes[109..109 + previous.len()].copy_from_slice(previous);
        let offset = 109 + previous.len();
        bytes[offset..offset + 2].copy_from_slice(&(next.len() as u16).to_be_bytes());
        bytes[offset + 2..encoded.len].copy_from_slice(next);
        encoded
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SuccessionError> {
        if !(MIN_SUCCESSION_BYTES..=MAX_SUCCESSION_BYTES).contains(&bytes.len()) {
            return Err(SuccessionError::WrongLength);
        }
        let mut reader = Reader(bytes);
        if reader.take::<2>()? != HEADER {
            return Err(SuccessionError::UnsupportedFormat);
        }
        let terminal_kind = match reader.take::<1>()?[0] {
            1 => PredecessorTerminalKind::Activated,
            2 => PredecessorTerminalKind::UnactivatedEmpty,
            _ => return Err(SuccessionError::UnknownTerminalKind),
        };
        let producer_slot_id = ProducerSlotId::try_from(reader.take::<16>()?)
            .map_err(|_| SuccessionError::InvalidIdentity)?;
        let predecessor_epoch = ProducerEpoch::new(u64::from_be_bytes(reader.take()?))
            .map_err(|_| SuccessionError::InvalidEpoch)?;
        let issued_through = u64::from_be_bytes(reader.take()?);
        let last_complete_sequence = u64::from_be_bytes(reader.take()?);
        let closure_digest = reader.take()?;
        let incoming_handoff_sha256 = reader.take()?;
        let previous_manifest = reader.manifest()?;
        let next_manifest = reader.manifest()?;
        if !reader.0.is_empty() {
            return Err(SuccessionError::WrongLength);
        }
        Self::new(SuccessionFields {
            terminal_kind,
            producer_slot_id,
            predecessor_epoch,
            issued_through,
            last_complete_sequence,
            closure_digest,
            incoming_handoff_sha256,
            previous_manifest,
            next_manifest,
        })
    }
}

fn validate(fields: &SuccessionFields) -> Result<(), SuccessionError> {
    let epoch = fields.predecessor_epoch.get();
    if epoch < 2 {
        return Err(SuccessionError::InvalidEpoch);
    }
    if fields.issued_through > 64
        || (fields.issued_through == 0 && fields.last_complete_sequence != 0)
        || (fields.terminal_kind == PredecessorTerminalKind::UnactivatedEmpty
            && fields.issued_through != 0)
    {
        return Err(SuccessionError::InvalidCounts);
    }
    let previous = &fields.previous_manifest;
    let next = &fields.next_manifest;
    let old = previous.identity();
    let new = next.identity();
    let previous_schema = if epoch == 2 { 2 } else { 3 };
    if previous.key_schema() != previous_schema
        || next.key_schema() != 3
        || old.owner_epoch.get() != epoch
        || epoch.checked_add(1) != Some(new.owner_epoch.get())
        || old.topology_generation.get().checked_add(1) != Some(new.topology_generation.get())
        || old.dataset_id != new.dataset_id
        || old.owner_resource_id != new.owner_resource_id
        || old.shard_id != new.shard_id
        || old.recovery_generation != new.recovery_generation
        || old.process_incarnation == new.process_incarnation
    {
        return Err(SuccessionError::InvalidTransition);
    }
    for number in 1..=9 {
        let kind = DictionaryKind::try_from(number).expect("assigned bounded kind");
        let old_domain = previous.allocation_namespaces().domain(kind);
        let next_domain = next.allocation_namespaces().domain(kind);
        if old_domain.is_some() != next_domain.is_some() {
            return Err(SuccessionError::InvalidNamespaces);
        }
        if let Some(domain) = next_domain {
            for other in 1..=9 {
                let other_kind = DictionaryKind::try_from(other).expect("assigned bounded kind");
                if previous.allocation_namespaces().domain(other_kind) == Some(domain)
                    || (other != number
                        && next.allocation_namespaces().domain(other_kind) == Some(domain))
                {
                    return Err(SuccessionError::InvalidNamespaces);
                }
            }
        }
    }
    Ok(())
}

struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], SuccessionError> {
        let bytes = self.0.get(..N).ok_or(SuccessionError::WrongLength)?;
        let value = bytes.try_into().map_err(|_| SuccessionError::WrongLength)?;
        self.0 = &self.0[N..];
        Ok(value)
    }

    fn manifest(&mut self) -> Result<IngestManifest, SuccessionError> {
        let len = usize::from(u16::from_be_bytes(self.take()?));
        if !(MIN_MANIFEST_BYTES..=MAX_MANIFEST_BYTES).contains(&len) {
            return Err(SuccessionError::WrongLength);
        }
        let bytes = self.0.get(..len).ok_or(SuccessionError::WrongLength)?;
        let manifest = IngestManifest::decode(bytes).map_err(SuccessionError::Manifest)?;
        self.0 = &self.0[len..];
        Ok(manifest)
    }
}

/// Stack-backed encoding, with no allocation or native-engine interaction.
pub struct EncodedSuccession {
    bytes: [u8; MAX_SUCCESSION_BYTES],
    len: usize,
}
impl EncodedSuccession {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

pub const MAX_TOPOLOGY_MEMBERSHIPS: u64 = 32_768;
/// Derived from E(E+1)/2 <=32,768; includes genesis and prepared empty attempts.
pub const MAX_LINEAGE_EPOCHS: u64 = 255;
pub const MAX_LINEAGE_METADATA_BYTES: u64 = 256 * 1024;
pub const LINEAGE_SCRATCH_BYTES: u64 = 128 * 1024;
/// A reservation allowance, not a measured or asserted size of a future entry.
pub const LINEAGE_ENTRY_BYTES: u64 = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuccessionProfileError {
    InvalidEpoch,
    InvalidPendingTarget,
    ArithmeticOverflow,
    TopologyLimit,
    MetadataLimit,
}
impl fmt::Display for SuccessionProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidEpoch => "repeatable succession starts from owner epoch two",
            Self::InvalidPendingTarget => "prepared target must immediately follow the native head",
            Self::ArithmeticOverflow => "succession profile arithmetic overflow",
            Self::TopologyLimit => "succession exceeds retained topology membership capacity",
            Self::MetadataLimit => "succession exceeds lineage metadata reservation capacity",
        })
    }
}
impl std::error::Error for SuccessionProfileError {}

/// Arithmetic only. Actual complete topology/census, allocation capacity and all
/// overlapping grants must be checked by the future connected owner. In
/// particular, this value is no proof that a compact entry fits its allowance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuccessionProfile {
    registered_epochs: u64,
    topology_memberships: u64,
    metadata_bytes: u64,
}
impl SuccessionProfile {
    /// Include the newly prepared attempt before any durable preparation. Its
    /// later recovery uses this same budget without reserving another epoch.
    pub fn for_preparation(predecessor_epoch: u64) -> Result<Self, SuccessionProfileError> {
        let next = predecessor_epoch
            .checked_add(1)
            .ok_or(SuccessionProfileError::ArithmeticOverflow)?;
        Self::for_recovery(predecessor_epoch, Some(next))
    }

    /// `prepared_target` describes only the optional PG-prepared/native-old gap.
    /// An existing native target uses None, including an empty unactivated head.
    /// These scalars do not prove that the indicated durable shape exists.
    pub fn for_recovery(
        native_epoch: u64,
        prepared_target: Option<u64>,
    ) -> Result<Self, SuccessionProfileError> {
        if native_epoch < 2 {
            return Err(SuccessionProfileError::InvalidEpoch);
        }
        let registered_epochs = match prepared_target {
            None => native_epoch,
            Some(target) => {
                if native_epoch.checked_add(1) != Some(target) {
                    return Err(SuccessionProfileError::InvalidPendingTarget);
                }
                target
            }
        };
        let next = registered_epochs
            .checked_add(1)
            .ok_or(SuccessionProfileError::ArithmeticOverflow)?;
        // Divide the even factor first, avoiding an unnecessarily overflowing
        // intermediate product. Persistent row counts are separate from bytes.
        let (left, right) = if registered_epochs.is_multiple_of(2) {
            (registered_epochs / 2, next)
        } else {
            (registered_epochs, next / 2)
        };
        let topology_memberships = left
            .checked_mul(right)
            .ok_or(SuccessionProfileError::ArithmeticOverflow)?;
        if topology_memberships > MAX_TOPOLOGY_MEMBERSHIPS {
            return Err(SuccessionProfileError::TopologyLimit);
        }
        let metadata_bytes = registered_epochs
            .checked_mul(LINEAGE_ENTRY_BYTES)
            .and_then(|entries| LINEAGE_SCRATCH_BYTES.checked_add(entries))
            .ok_or(SuccessionProfileError::ArithmeticOverflow)?;
        if metadata_bytes > MAX_LINEAGE_METADATA_BYTES {
            return Err(SuccessionProfileError::MetadataLimit);
        }
        Ok(Self {
            registered_epochs,
            topology_memberships,
            metadata_bytes,
        })
    }

    pub const fn registered_epochs(&self) -> u64 {
        self.registered_epochs
    }
    pub const fn topology_memberships(&self) -> u64 {
        self.topology_memberships
    }
    pub const fn required_metadata_bytes(&self) -> u64 {
        self.metadata_bytes
    }
    /// At most one sealed empty generation may precede genesis. It contributes
    /// no memberships but still requires a bounded inspection slot.
    pub const fn maximum_topology_generations(&self) -> u64 {
        // for_recovery checked this exact addition before construction.
        self.registered_epochs + 1
    }
}
