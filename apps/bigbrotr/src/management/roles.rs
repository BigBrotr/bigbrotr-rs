//! Closed supervisor observations, never leases, read pins or publication proof.
use super::{grants::Revision, sessions::SessionError};
use crate::{runtime::OwnedPermit, storage::postgres::MAX_INDEX_STREAMS};
use bigbrotr_model::{DatasetId, ProcessIncarnation, ProjectionGenerationId, SourceCut};
use serde::{
    Serialize, Serializer,
    ser::{SerializeSeq, SerializeStruct},
};
use std::time::Duration;

pub const MAX_SNAPSHOT_STREAM_BOUNDS: usize = MAX_INDEX_STREAMS;
pub const RESPONSE_RESERVATION: u64 = 128 * 1024;
const RESPONSE_BYTES: usize = RESPONSE_RESERVATION as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SnapshotIdentity {
    pub dataset_id: DatasetId,
    pub process_incarnation: ProcessIncarnation,
    pub config_revision: Revision,
    pub policy_revision: Revision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleKind {
    Control,
    Ingest,
    Sync,
    Archive,
    Index,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleState {
    Starting,
    Ready,
    Idle,
    Degraded,
    Draining,
    Stopped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKind {
    SignedEventAdmission,
    FiniteAcquisition,
    ArchivePublication,
    IndexPublication,
    PrivateHistory,
    SignedEventExport,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityState {
    Disabled,
    Building,
    Ready,
    Stale,
    Unavailable,
    Retiring,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Freshness {
    Fresh,
    Stale,
    Unknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusReason {
    Current,
    NotSelected,
    NotObserved,
    Starting,
    AuthorityUnavailable,
    SourceUnavailable,
    DependencyUnavailable,
    PublicationInFlight,
    BehindSource,
    VerificationInFlight,
    VerificationStale,
    RemoteOutcomeUnknown,
    ResourceUnavailable,
    ReadPathUnavailable,
    Stopping,
    Stopped,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ObservationTime {
    #[serde(serialize_with = "decimal")]
    observed_at_us: i64,
    #[serde(serialize_with = "decimal")]
    age_us: u64,
}
impl ObservationTime {
    /// `observed_at_us` is the current authenticated PostgreSQL report timestamp.
    /// Age comes from the oldest required physical/native observation using the
    /// owning suspend-aware clock; never subtract UTC timestamps for freshness.
    pub fn new(observed_at_us: i64, age: Duration) -> Result<Self, SessionError> {
        Ok(Self {
            observed_at_us,
            age_us: age
                .as_micros()
                .try_into()
                .map_err(|_| SessionError::InvalidInput)?,
        })
    }
}
#[derive(Debug, Serialize)]
pub struct RoleObservation {
    pub role: RoleKind,
    pub state: RoleState,
    pub reason: StatusReason,
}
#[derive(Debug, Serialize)]
pub struct SourceObservation {
    #[serde(serialize_with = "optional_cut")]
    pub cut: Option<SourceCut>,
    pub freshness: Freshness,
    pub time: Option<ObservationTime>,
    pub reason: StatusReason,
}
#[derive(Debug, Serialize)]
pub struct PublishedCut {
    pub revision: Revision,
    #[serde(serialize_with = "cut")]
    pub cut: SourceCut,
}
#[derive(Debug, Serialize)]
pub struct CapabilityObservation {
    pub capability: CapabilityKind,
    pub state: CapabilityState,
    pub reason: StatusReason,
    pub freshness: Freshness,
    pub time: Option<ObservationTime>,
    pub published: Option<PublishedCut>,
    pub generation: Option<ProjectionGenerationId>,
    pub storage_incarnation: Option<ProcessIncarnation>,
}

/// Construction checks boundedness and structural consistency only. The owning
/// supervisor supplies actual dependency evidence and decides capability state.
#[derive(Debug, Serialize)]
pub struct RoleSnapshot {
    format: &'static str,
    identity: SnapshotIdentity,
    #[serde(serialize_with = "decimal")]
    observed_at_us: i64,
    source: SourceObservation,
    roles: Vec<RoleObservation>,
    capabilities: Vec<CapabilityObservation>,
    authority: &'static str,
}
impl RoleSnapshot {
    /// Bound aggregate stream lengths before cloning cuts into this value; the
    /// caller retains its request reservation throughout snapshot construction.
    pub fn new(
        identity: SnapshotIdentity,
        observed_at_us: i64,
        source: SourceObservation,
        roles: Vec<RoleObservation>,
        capabilities: Vec<CapabilityObservation>,
    ) -> Result<Self, SessionError> {
        let valid_time = |freshness, time: Option<ObservationTime>| match (freshness, time) {
            (Freshness::Unknown, None) => true,
            (Freshness::Fresh | Freshness::Stale, Some(time)) => {
                time.observed_at_us == observed_at_us
            }
            _ => false,
        };
        if roles.first().map(|v| v.role) != Some(RoleKind::Control)
            || roles.len() > 5
            || capabilities.len() > 6
            || roles.windows(2).any(|v| v[0].role >= v[1].role)
            || capabilities
                .windows(2)
                .any(|v| v[0].capability >= v[1].capability)
            || !valid_time(source.freshness, source.time)
            || source.cut.is_some() == (source.freshness == Freshness::Unknown)
        {
            return Err(SessionError::InvalidInput);
        }
        let mut streams = 0usize;
        let mut check_cut = |cut: &SourceCut| -> Result<(), SessionError> {
            streams = streams
                .checked_add(cut.bounds().len())
                .ok_or(SessionError::Capacity)?;
            if cut.dataset_id() != identity.dataset_id {
                return Err(SessionError::InvalidInput);
            }
            if streams > MAX_SNAPSHOT_STREAM_BOUNDS {
                return Err(SessionError::Capacity);
            }
            Ok(())
        };
        if let Some(cut) = &source.cut {
            check_cut(cut)?;
        }
        for cap in &capabilities {
            if !valid_time(cap.freshness, cap.time)
                || (cap.state == CapabilityState::Ready && cap.freshness != Freshness::Fresh)
                || ((cap.generation.is_some() || cap.storage_incarnation.is_some())
                    && cap.capability != CapabilityKind::IndexPublication)
            {
                return Err(SessionError::InvalidInput);
            }
            if let Some(published) = &cap.published {
                check_cut(&published.cut)?;
            }
        }
        Ok(Self {
            format: "bigbrotr-role-status/1",
            identity,
            observed_at_us,
            source,
            roles,
            capabilities,
            authority: "observation-only",
        })
    }
    pub fn identity(&self) -> SnapshotIdentity {
        self.identity
    }
    pub fn observed_at_us(&self) -> i64 {
        self.observed_at_us
    }
    pub fn encode(self, permit: OwnedPermit) -> Result<RoleBody, SessionError> {
        if permit.request().units().data_bytes() < RESPONSE_RESERVATION
            || permit.request().units().items() == 0
        {
            return Err(SessionError::Capacity);
        }
        struct Buffer(Vec<u8>);
        impl std::io::Write for Buffer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > RESPONSE_BYTES.saturating_sub(self.0.len()) {
                    return Err(std::io::Error::other("role observation byte limit"));
                }
                self.0.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut bytes = Buffer(Vec::new());
        bytes
            .0
            .try_reserve_exact(RESPONSE_BYTES)
            .map_err(|_| SessionError::Capacity)?;
        serde_json::to_writer(&mut bytes, &self).map_err(|_| SessionError::Capacity)?;
        Ok(RoleBody {
            bytes: bytes.0,
            _permit: permit,
        })
    }
}
/// The actual Hyper buffer keeps its bounded allocation charged through clones.
pub struct RoleBody {
    bytes: Vec<u8>,
    _permit: OwnedPermit,
}
impl AsRef<[u8]> for RoleBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn decimal<T: std::fmt::Display, S: Serializer>(
    value: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}
struct Cut<'a>(&'a SourceCut);
impl Serialize for Cut<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        struct Bounds<'a>(&'a [bigbrotr_model::StreamBound]);
        impl Serialize for Bounds<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                #[derive(Serialize)]
                struct Bound {
                    #[serde(serialize_with = "decimal")]
                    shard_id: u32,
                    owner_epoch: bigbrotr_model::OwnerEpoch,
                    last_complete_sequence: bigbrotr_model::CompleteThrough,
                }
                let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                for v in self.0 {
                    seq.serialize_element(&Bound {
                        shard_id: v.stream.shard_id,
                        owner_epoch: v.stream.owner_epoch,
                        last_complete_sequence: v.last_complete_sequence,
                    })?;
                }
                seq.end()
            }
        }
        let mut out = serializer.serialize_struct("SourceCut", 3)?;
        out.serialize_field("dataset_id", &self.0.dataset_id())?;
        out.serialize_field("topology_generation", &self.0.topology_generation())?;
        out.serialize_field("bounds", &Bounds(self.0.bounds()))?;
        out.end()
    }
}
fn cut<S: Serializer>(value: &SourceCut, serializer: S) -> Result<S::Ok, S::Error> {
    Cut(value).serialize(serializer)
}
fn optional_cut<S: Serializer>(
    value: &Option<SourceCut>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    value.as_ref().map(Cut).serialize(serializer)
}

#[cfg(test)]
#[path = "roles_tests.rs"]
mod tests;
