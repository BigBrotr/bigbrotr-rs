//! Closed immutable descriptor values. Identity, integrity and attribution are
//! not allocation, visibility, verification or current control authority.
use crate::{
    DatasetId, DictionaryKind, Digest256, EventId, PublicKey, QualifiedRef, source::SourceError,
};
use std::fmt;

pub const MAX_DESCRIPTOR_LABEL_BYTES: usize = 64;
pub const MAX_DESCRIPTOR_ENDPOINT_BYTES: usize = 8192;
pub const EVENT_VERIFICATION_NIPS_REVISION: &str = "a2494f4f81d46684e5814a9bf35e2b1df978f955";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictionaryError {
    InvalidLabel,
    InvalidEndpointShape,
    InvalidAccessLabel,
    InvalidFormatVersion,
    Reference(SourceError),
    DifferentReference,
    MappingConflict,
}
impl fmt::Display for DictionaryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLabel => "descriptor label must be a bounded stable ASCII identifier",
            Self::InvalidEndpointShape => {
                "descriptor endpoint must have bounded ws/wss URL shape without userinfo or fragment"
            }
            Self::InvalidAccessLabel => {
                "public access requires null label; protected access requires a stable label"
            }
            Self::InvalidFormatVersion => "source format version must be nonzero",
            Self::Reference(_) => "descriptor reference has an incompatible namespace or kind",
            Self::DifferentReference => "dictionary mappings name different qualified references",
            Self::MappingConflict => {
                "the same qualified reference has conflicting complete descriptor values"
            }
        })
    }
}
impl std::error::Error for DictionaryError {}
impl From<SourceError> for DictionaryError {
    fn from(error: SourceError) -> Self {
        Self::Reference(error)
    }
}

/// A stable attribution label, never a URL, credential, process or authorization
/// token. Syntax checks cannot determine whether an operator supplied a secret.
#[derive(Clone, PartialEq, Eq)]
pub struct DescriptorLabel(String);
impl DescriptorLabel {
    pub fn new(value: String) -> Result<Self, DictionaryError> {
        if value.is_empty()
            || value.len() > MAX_DESCRIPTOR_LABEL_BYTES
            || !value.as_bytes()[0].is_ascii_alphanumeric()
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-+".contains(&b))
        {
            return Err(DictionaryError::InvalidLabel);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for DescriptorLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DescriptorLabel([redacted])")
    }
}

/// Bounded structural bytes only. The admission consumer must require exact
/// equality with the single protocol Endpoint normalizer and apply source and
/// credential/export policy; this model does not duplicate URL normalization.
#[derive(Clone, PartialEq, Eq)]
pub struct EndpointDescriptor(String);
impl EndpointDescriptor {
    pub fn new(value: String) -> Result<Self, DictionaryError> {
        let rest = value
            .strip_prefix("ws://")
            .or_else(|| value.strip_prefix("wss://"));
        let authority = rest.and_then(|s| s.split(['/', '?']).next());
        if value.len() > MAX_DESCRIPTOR_ENDPOINT_BYTES
            || authority.is_none_or(|a| a.is_empty() || a.contains(['@', '%']))
            || value.contains(['\\', '#'])
            || value.chars().any(|c| c.is_control() || c.is_whitespace())
        {
            return Err(DictionaryError::InvalidEndpointShape);
        }
        Ok(Self(value))
    }
    pub fn url(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for EndpointDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EndpointDescriptor([redacted])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteClass {
    Public,
    Private,
    Tor,
    I2p,
    Lokinet,
}
impl RouteClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Private => "private",
            Self::Tor => "tor",
            Self::I2p => "i2p",
            Self::Lokinet => "lokinet",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockSource {
    SystemUtc,
    ExternalUtc,
}
impl ClockSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SystemUtc => "system-utc",
            Self::ExternalUtc => "external-utc",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessClass {
    Public,
    Protected,
}
impl AccessClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Protected => "protected",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverDescriptor {
    pub clock: Option<ClockSource>,
    pub vantage: Option<DescriptorLabel>,
    pub software: Option<DescriptorLabel>,
    pub method_id: Option<QualifiedRef>,
    pub public_key: Option<PublicKey>,
    pub route_class: Option<RouteClass>,
    pub software_version: Option<DescriptorLabel>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessScopeDescriptor {
    pub class: AccessClass,
    pub label: Option<DescriptorLabel>,
    pub visibility_scope_id: QualifiedRef,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetLineageDescriptor {
    pub format: DescriptorLabel,
    pub format_version: u16,
    pub source_dataset_id: Option<DatasetId>,
    pub source_artifact_digest: Digest256,
}

/// Only actual admission dependencies are implemented. Route and HTTP-resource
/// references remain valid portable names, but have no accepted descriptor here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DictionaryDescriptor {
    Event(EventId),
    Visibility { class: DescriptorLabel },
    Endpoint(EndpointDescriptor),
    Observer(ObserverDescriptor),
    AccessScope(AccessScopeDescriptor),
    EventVerification,
    DatasetLineage(DatasetLineageDescriptor),
}
impl DictionaryDescriptor {
    pub const fn kind(&self) -> DictionaryKind {
        match self {
            Self::Event(_) => DictionaryKind::Event,
            Self::Visibility { .. } => DictionaryKind::VisibilityScope,
            Self::Endpoint(_) => DictionaryKind::Endpoint,
            Self::Observer(_) => DictionaryKind::Observer,
            Self::AccessScope(_) => DictionaryKind::AccessScope,
            Self::EventVerification => DictionaryKind::Method,
            Self::DatasetLineage(_) => DictionaryKind::DatasetLineage,
        }
    }
    pub const fn schema(&self) -> &'static str {
        match self {
            Self::Event(_) => "event",
            Self::Visibility { .. } => "visibility",
            Self::Endpoint(_) => "endpoint",
            Self::Observer(_) => "observer",
            Self::AccessScope(_) => "access-scope",
            Self::EventVerification => "event-verification",
            Self::DatasetLineage(_) => "dataset-lineage",
        }
    }
    pub(crate) fn reference(&self) -> Option<(QualifiedRef, DictionaryKind)> {
        match self {
            Self::Observer(value) => value.method_id.map(|r| (r, DictionaryKind::Method)),
            Self::AccessScope(value) => {
                Some((value.visibility_scope_id, DictionaryKind::VisibilityScope))
            }
            _ => None,
        }
    }
    fn validate(&self, dataset: DatasetId) -> Result<(), DictionaryError> {
        if let Some((reference, kind)) = self.reference() {
            reference.require(dataset, kind)?;
        }
        if let Self::AccessScope(value) = self
            && (value.class == AccessClass::Public) != value.label.is_none()
        {
            return Err(DictionaryError::InvalidAccessLabel);
        }
        if let Self::DatasetLineage(value) = self
            && value.format_version == 0
        {
            return Err(DictionaryError::InvalidFormatVersion);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryEntry {
    reference: QualifiedRef,
    descriptor: DictionaryDescriptor,
}
impl DictionaryEntry {
    pub fn new(
        reference: QualifiedRef,
        descriptor: DictionaryDescriptor,
    ) -> Result<Self, DictionaryError> {
        reference.require(reference.dataset_id, descriptor.kind())?;
        descriptor.validate(reference.dataset_id)?;
        Ok(Self {
            reference,
            descriptor,
        })
    }
    pub const fn reference(&self) -> QualifiedRef {
        self.reference
    }
    pub const fn descriptor(&self) -> &DictionaryDescriptor {
        &self.descriptor
    }
    pub const fn normalization_version(&self) -> u16 {
        1
    }
    pub const fn descriptor_version(&self) -> u16 {
        1
    }
    pub const fn descriptor_schema(&self) -> &'static str {
        self.descriptor.schema()
    }
    /// Compare full typed values, not a digest candidate. The owner must perform
    /// this check against its committed mapping inside the serialized transaction.
    pub fn require_same_mapping(&self, other: &Self) -> Result<(), DictionaryError> {
        if self.reference != other.reference {
            return Err(DictionaryError::DifferentReference);
        }
        if self != other {
            return Err(DictionaryError::MappingConflict);
        }
        Ok(())
    }
}
