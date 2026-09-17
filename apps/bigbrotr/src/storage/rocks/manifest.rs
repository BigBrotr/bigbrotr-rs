//! Closed ingest manifest bytes. Compatibility metadata never grants write authority.
use bigbrotr_model::{
    AllocationDomainId, DatasetId, DictionaryKind, OwnerEpoch, OwnerResourceId, PolicyEpoch,
    ProcessIncarnation, RecoveryGenerationId, TopologyGeneration,
};
use std::fmt;

// Value format, ingest-manifest tag, key/value schemas, fingerprint and native
// option profiles. Reinterpreting any of these requires an explicit migration.
const HEADER: [u8; 10] = [1, 1, 0, 1, 0, 1, 0, 1, 0, 1];
const FIXED_BYTES: usize = 104;
const NAMESPACE_BYTES: usize = 18;
const DICTIONARY_KINDS: usize = 9;
pub const MAX_MANIFEST_BYTES: usize = FIXED_BYTES + DICTIONARY_KINDS * NAMESPACE_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestError {
    WrongLength,
    UnsupportedFormat,
    InvalidIdentity,
    InvalidCounter,
    UnsupportedProtection,
    UnorderedNamespaces,
    UnknownDictionaryKind,
}

impl fmt::Display for ManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WrongLength => "manifest length does not match its bounded namespace count",
            Self::UnsupportedFormat => "unsupported ingest manifest schema or profile",
            Self::InvalidIdentity => "manifest contains a zero allocated identity",
            Self::InvalidCounter => "manifest owner epoch must be nonzero",
            Self::UnsupportedProtection => "unsupported ingest manifest protection class",
            Self::UnorderedNamespaces => "manifest namespaces must be strictly ordered by kind",
            Self::UnknownDictionaryKind => "manifest contains an unassigned dictionary kind",
        })
    }
}
impl std::error::Error for ManifestError {}

/// The exact tuple compared with current control authority on open. Topology,
/// recovery and policy coordinates can represent zero; freshness is not a codec
/// decision. The store role is always Ingest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestIdentity {
    pub dataset_id: DatasetId,
    pub topology_generation: TopologyGeneration,
    pub owner_resource_id: OwnerResourceId,
    pub shard_id: u32,
    pub owner_epoch: OwnerEpoch,
    pub process_incarnation: ProcessIncarnation,
    pub recovery_generation: RecoveryGenerationId,
    pub policy_epoch: PolicyEpoch,
}

/// One current allocation domain per kind in this store, not an inventory of all
/// retained historical domains. Missing is distinct from an allocated zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocationNamespaces([Option<AllocationDomainId>; DICTIONARY_KINDS]);

impl AllocationNamespaces {
    pub fn from_entries(
        entries: &[(DictionaryKind, AllocationDomainId)],
    ) -> Result<Self, ManifestError> {
        if entries.len() > DICTIONARY_KINDS {
            return Err(ManifestError::WrongLength);
        }
        let mut domains = [None; DICTIONARY_KINDS];
        let mut previous = 0;
        for &(kind, domain) in entries {
            let value = kind as usize;
            if value <= previous {
                return Err(ManifestError::UnorderedNamespaces);
            }
            domains[value - 1] = Some(domain);
            previous = value;
        }
        Ok(Self(domains))
    }

    pub fn domain(&self, kind: DictionaryKind) -> Option<AllocationDomainId> {
        self.0[kind as usize - 1]
    }
}

/// Version 1 records LocalSynced only. This spelling alone does not qualify a
/// machine, grant authority, or certify any effects. Counters have separate keys
/// committed with their mappings/facts; they are not duplicated in this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestManifest {
    identity: ManifestIdentity,
    namespaces: AllocationNamespaces,
}

impl IngestManifest {
    pub const fn new(identity: ManifestIdentity, namespaces: AllocationNamespaces) -> Self {
        Self {
            identity,
            namespaces,
        }
    }

    pub const fn identity(&self) -> &ManifestIdentity {
        &self.identity
    }

    pub const fn allocation_namespaces(&self) -> &AllocationNamespaces {
        &self.namespaces
    }

    pub fn encode(&self) -> EncodedManifest {
        let mut output = EncodedManifest {
            bytes: [0; MAX_MANIFEST_BYTES],
            len: FIXED_BYTES,
        };
        let bytes = &mut output.bytes;
        let identity = &self.identity;
        bytes[..10].copy_from_slice(&HEADER);
        bytes[10..26].copy_from_slice(identity.dataset_id.as_bytes());
        bytes[26..34].copy_from_slice(&identity.topology_generation.get().to_be_bytes());
        bytes[34..50].copy_from_slice(identity.owner_resource_id.as_bytes());
        bytes[50..54].copy_from_slice(&identity.shard_id.to_be_bytes());
        bytes[54..62].copy_from_slice(&identity.owner_epoch.get().to_be_bytes());
        bytes[62..78].copy_from_slice(identity.process_incarnation.as_bytes());
        bytes[78..94].copy_from_slice(identity.recovery_generation.as_bytes());
        bytes[94..102].copy_from_slice(&identity.policy_epoch.get().to_be_bytes());
        bytes[102] = 1;
        for (index, domain) in self.namespaces.0.iter().enumerate() {
            if let Some(domain) = domain {
                let start = output.len;
                bytes[start..start + 2].copy_from_slice(&((index + 1) as u16).to_be_bytes());
                bytes[start + 2..start + NAMESPACE_BYTES].copy_from_slice(domain.as_bytes());
                bytes[103] += 1;
                output.len += NAMESPACE_BYTES;
            }
        }
        output
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ManifestError> {
        if !(FIXED_BYTES..=MAX_MANIFEST_BYTES).contains(&bytes.len()) {
            return Err(ManifestError::WrongLength);
        }
        let count = usize::from(bytes[103]);
        if count > DICTIONARY_KINDS || bytes.len() != FIXED_BYTES + count * NAMESPACE_BYTES {
            return Err(ManifestError::WrongLength);
        }
        if bytes[..10] != HEADER {
            return Err(ManifestError::UnsupportedFormat);
        }
        if bytes[102] != 1 {
            return Err(ManifestError::UnsupportedProtection);
        }
        let identity = ManifestIdentity {
            dataset_id: DatasetId::try_from(fixed(bytes, 10)?)
                .map_err(|_| ManifestError::InvalidIdentity)?,
            topology_generation: TopologyGeneration::new(u64::from_be_bytes(fixed(bytes, 26)?)),
            owner_resource_id: OwnerResourceId::try_from(fixed(bytes, 34)?)
                .map_err(|_| ManifestError::InvalidIdentity)?,
            shard_id: u32::from_be_bytes(fixed(bytes, 50)?),
            owner_epoch: OwnerEpoch::new(u64::from_be_bytes(fixed(bytes, 54)?))
                .map_err(|_| ManifestError::InvalidCounter)?,
            process_incarnation: ProcessIncarnation::try_from(fixed(bytes, 62)?)
                .map_err(|_| ManifestError::InvalidIdentity)?,
            recovery_generation: RecoveryGenerationId::from_bytes(fixed(bytes, 78)?),
            policy_epoch: PolicyEpoch::new(u64::from_be_bytes(fixed(bytes, 94)?)),
        };
        let mut namespaces = AllocationNamespaces([None; DICTIONARY_KINDS]);
        let mut previous = 0;
        for index in 0..count {
            let start = FIXED_BYTES + index * NAMESPACE_BYTES;
            let kind = u16::from_be_bytes(fixed(bytes, start)?);
            DictionaryKind::try_from(kind).map_err(|_| ManifestError::UnknownDictionaryKind)?;
            if kind <= previous {
                return Err(ManifestError::UnorderedNamespaces);
            }
            namespaces.0[usize::from(kind - 1)] = Some(
                AllocationDomainId::try_from(fixed(bytes, start + 2)?)
                    .map_err(|_| ManifestError::InvalidIdentity)?,
            );
            previous = kind;
        }
        Ok(Self::new(identity, namespaces))
    }
}

fn fixed<const N: usize>(bytes: &[u8], start: usize) -> Result<[u8; N], ManifestError> {
    bytes
        .get(start..start + N)
        .ok_or(ManifestError::WrongLength)?
        .try_into()
        .map_err(|_| ManifestError::WrongLength)
}

/// Fixed maximum storage; encoding/decoding never allocates a variable buffer.
pub struct EncodedManifest {
    bytes: [u8; MAX_MANIFEST_BYTES],
    len: usize,
}
impl EncodedManifest {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}
