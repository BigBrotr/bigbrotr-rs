//! Shared read values. Neither request fields nor response metadata grant access.
use crate::{
    EventId, ObservedTime, OwnerResourceId, PolicyEpoch, ProjectionGenerationId, QualifiedRef,
    SourceCut,
};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadResource {
    EventHistory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    Snapshot,
    Traversal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventIdFilter {
    pub ids: Vec<EventId>,
}

/// The initial history consumer supports one exact ID. Broader filters,
/// projections and cursors remain explicitly unavailable, never silently ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub version: u8,
    pub resource: ReadResource,
    #[serde(default = "snapshot")]
    pub mode: ReadMode,
    pub scope: QualifiedRef,
    pub filters: Vec<EventIdFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u16>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default, deserialize_with = "no_projection")]
    pub projection: Option<Vec<String>>,
}
fn no_projection<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error> {
    // The initial wire profile allows only null. Do not allocate a vector of
    // unsupported fields just to reject it later; many empty strings amplify
    // a small request beyond the caller's reserved decode memory.
    <()>::deserialize(deserializer)?;
    Ok(None)
}
fn snapshot() -> ReadMode {
    ReadMode::Snapshot
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReadGeneration {
    Archive {
        catalog_id: OwnerResourceId,
        #[serde(serialize_with = "decimal")]
        frontier_revision: u64,
        #[serde(serialize_with = "decimal")]
        snapshot_id: u64,
    },
}
fn decimal<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

#[derive(Debug, Serialize)]
pub struct ReadViewStamp {
    pub source_cut: SourceCut,
    pub input_generations: Vec<ReadGeneration>,
    pub target_generations: Vec<ReadGeneration>,
    pub visibility_as_of_us: ObservedTime,
    /// Immutable history has no ordinary-current temporal reduction generation.
    pub temporal_generation: Option<ProjectionGenerationId>,
    pub expiry_deadline_us: Option<ObservedTime>,
    pub policy_epoch: PolicyEpoch,
}

#[derive(Debug, Serialize)]
pub struct QueryPage<T> {
    pub version: u8,
    pub resource: ReadResource,
    pub items: Vec<T>,
    pub effective_page_size: u16,
    pub next_cursor: Option<String>,
    pub mode: ReadMode,
    pub candidate_cut: Option<SourceCut>,
    pub view_stamp: ReadViewStamp,
    pub eligibility_lag_us: Option<crate::DurationUs>,
}
