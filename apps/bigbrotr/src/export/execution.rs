//! One retained SQL selection joined to its actual native observation and output.
use super::{
    destination::LocalDestination,
    files::{ExportFileError, ExportUnit, OutputCustody, StoredExport},
    manifest::SignedEventsManifest,
};
use crate::storage::postgres::catalog_guard::CatalogIdentity;
use crate::{
    management::jobs::ExportDestination,
    storage::{archive::ArchivedEventQuery, postgres::ingest_registry::ControlOwner},
};
use bigbrotr_model::{
    ArtifactId, CompleteThrough, Digest256, JobId, OwnerEpoch, OwnerResourceId, PrincipalId,
    SourceCut, StreamBound, StreamId,
};
use bigbrotr_protocol::archive_event::EventQuery;
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

fn same_custody(expected: &CatalogIdentity, actual: &CatalogIdentity) -> bool {
    expected == actual
}

/// Historical immutable intent, not current authorization. Only the Control
/// adapter constructs it from the original sealed job and frontier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportSelection {
    pub(crate) job: JobId,
    pub(crate) principal: PrincipalId,
    pub(crate) query: EventQuery,
    pub(crate) catalog: OwnerResourceId,
    pub(crate) frontier_revision: u64,
    pub(crate) destination: ExportDestination,
    pub(crate) config_revision: u64,
    pub(crate) policy_revision: u64,
}
impl ExportSelection {
    pub fn job(&self) -> JobId {
        self.job
    }
    pub fn principal(&self) -> PrincipalId {
        self.principal
    }
    pub fn query(&self) -> &EventQuery {
        &self.query
    }
    pub fn catalog(&self) -> OwnerResourceId {
        self.catalog
    }
    pub fn frontier_revision(&self) -> u64 {
        self.frontier_revision
    }
    pub fn destination(&self) -> ExportDestination {
        self.destination
    }
    fn matches_observation(&self, catalog: OwnerResourceId, query: &EventQuery) -> bool {
        self.catalog == catalog && self.query == *query
    }
    fn source_cut(&self) -> Result<SourceCut, ExportFileError> {
        let mut bounds = Vec::new();
        bounds
            .try_reserve_exact(self.query.bounds.len())
            .map_err(|_| ExportFileError::ResourceLimit)?;
        for bound in &self.query.bounds {
            bounds.push(StreamBound {
                stream: StreamId {
                    shard_id: bound.shard_id,
                    owner_epoch: OwnerEpoch::new(bound.owner_epoch)
                        .map_err(|_| ExportFileError::Conflict)?,
                },
                last_complete_sequence: CompleteThrough::new(bound.complete_through),
            });
        }
        SourceCut::new(self.query.dataset, self.query.topology_generation, bounds)
            .map_err(|_| ExportFileError::Conflict)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportState {
    Accepted,
    Queued,
    Running,
    Paused,
    CancelRequested,
    Succeeded,
    Partial,
    Failed,
    Cancelled,
    Skipped,
}

/// Retained logical byte caps. Physical allocation overhead is reserved by the
/// supervisor separately. A lower new configuration cannot shrink an old unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportLimits {
    maximum: u64,
    temporary: u64,
}
impl ExportLimits {
    pub fn new(maximum: u64, temporary: u64) -> Result<Self, ExportFileError> {
        if maximum == 0 || maximum.checked_mul(2).is_none_or(|v| v > temporary) {
            return Err(ExportFileError::ResourceLimit);
        }
        Ok(Self { maximum, temporary })
    }
    pub fn maximum(self) -> u64 {
        self.maximum
    }
    pub fn temporary(self) -> u64 {
        self.temporary
    }
    pub(crate) fn fits(self, available: Self) -> bool {
        self.maximum <= available.maximum && self.temporary <= available.temporary
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportReservation {
    pub(crate) artifact: ArtifactId,
    pub(crate) limits: ExportLimits,
}
impl ExportReservation {
    pub fn artifact(self) -> ArtifactId {
        self.artifact
    }
    pub fn limits(self) -> ExportLimits {
        self.limits
    }
}
/// Historical lookup/COMMIT resolution. Even Running is an observed state,
/// not permission to dispatch a worker. Completed jobs never enter prepare().
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportJob {
    pub(crate) selection: ExportSelection,
    pub(crate) state: ExportState,
    pub(crate) revision: u64,
    pub(crate) reservation: Option<ExportReservation>,
    pub(crate) result: Option<Digest256>,
    pub(crate) files: Option<StoredExport>,
}
impl ExportJob {
    pub fn selection(&self) -> &ExportSelection {
        &self.selection
    }
    pub fn state(&self) -> ExportState {
        self.state
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn reservation(&self) -> Option<ExportReservation> {
        self.reservation
    }
    pub fn result(&self) -> Option<Digest256> {
        self.result
    }
    pub fn files(&self) -> Option<StoredExport> {
        self.files
    }
}

/// Issued by a committed current-owner transition. The caller still pairs it
/// with an action window started BEFORE that SQL request and a fresh reader pin.
pub struct RunningExport {
    pub(crate) job: ExportJob,
    pub(crate) owner: ControlOwner,
    pub(crate) remaining: Duration,
    pub(crate) catalog_identity: CatalogIdentity,
}
impl RunningExport {
    pub fn job(&self) -> &ExportJob {
        &self.job
    }
    pub fn remaining(&self) -> Duration {
        self.remaining
    }
    /// Blocking filesystem acquisition, supervised before any SQL transaction.
    /// Retain the returned custody outside cancellable futures through all joins.
    pub fn prepare(
        self,
        destination: Arc<LocalDestination>,
    ) -> Result<PreparedExport, ExportFileError> {
        let selection = &self.job.selection;
        if destination.dataset() != selection.query.dataset
            || destination.id() != selection.destination.id
            || destination.binding_digest() != selection.destination.binding_sha256
        {
            return Err(ExportFileError::Conflict);
        }
        let reservation = self.job.reservation.ok_or(ExportFileError::Conflict)?;
        let unit = ExportUnit::acquire(destination, reservation.artifact)?;
        Ok(PreparedExport {
            running: self,
            unit,
        })
    }
}
pub struct PreparedExport {
    running: RunningExport,
    unit: ExportUnit,
}
impl PreparedExport {
    pub fn custody(&self) -> Arc<OutputCustody> {
        self.unit.custody()
    }
    /// Consumes an actual native lookup, including authorized absence. Neither a
    /// caller-supplied manifest nor StoredExport can create a completion input.
    /// The supervisor gates current authority/cancellation until actual join.
    pub fn write(
        mut self,
        observation: ArchivedEventQuery,
        cancelled: &AtomicBool,
    ) -> Result<WrittenExport, ExportFileError> {
        let selection = &self.running.job.selection;
        if !same_custody(
            &self.running.catalog_identity,
            observation.catalog_identity(),
        ) || !selection.matches_observation(
            observation.catalog_identity().catalog(),
            observation.query(),
        ) {
            return Err(ExportFileError::Conflict);
        }
        let reservation = self
            .running
            .job
            .reservation
            .ok_or(ExportFileError::Conflict)?;
        let cut = selection.source_cut()?;
        let manifest = SignedEventsManifest {
            artifact_id: reservation.artifact,
            job_id: selection.job,
            source_cut: &cut,
            catalog_id: selection.catalog,
            frontier_revision: selection.frontier_revision,
            snapshot_id: selection.query.snapshot_id,
            event_id: selection.query.event_id,
            scope: selection.query.scope,
            accepted_config_revision: selection.config_revision,
            accepted_policy_revision: selection.policy_revision,
        };
        let stored = self.unit.write(
            &manifest,
            observation.event(),
            reservation.limits.maximum,
            reservation.limits.temporary,
            cancelled,
        )?;
        Ok(WrittenExport {
            running: self.running,
            stored,
            observation,
            _unit: self.unit,
        })
    }
}
/// Not a completed job. Keep this value AND independently retained output
/// custody until the actual Control driver has joined, including unknown COMMIT.
pub struct WrittenExport {
    pub(crate) running: RunningExport,
    pub(crate) stored: StoredExport,
    pub(crate) observation: ArchivedEventQuery,
    _unit: ExportUnit,
}
impl WrittenExport {
    pub fn job(&self) -> &ExportJob {
        &self.running.job
    }
    pub fn files(&self) -> StoredExport {
        self.stored
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr_model::{
        DictionaryKind, Digest256, EventId, Ordinal, QualifiedRef, TopologyGeneration,
    };
    use bigbrotr_protocol::archive_cut::CutBound;

    fn selection() -> ExportSelection {
        let dataset = [1; 16].try_into().unwrap();
        ExportSelection {
            job: [2; 16].try_into().unwrap(),
            principal: [3; 16].try_into().unwrap(),
            query: EventQuery {
                dataset,
                event_id: EventId::from_bytes([4; 32]),
                scope: QualifiedRef {
                    dataset_id: dataset,
                    dictionary_kind: DictionaryKind::VisibilityScope,
                    allocation_domain: [5; 16].try_into().unwrap(),
                    ordinal: Ordinal::new(1).unwrap(),
                },
                topology_generation: TopologyGeneration::new(0),
                bounds: vec![
                    CutBound {
                        shard_id: 0,
                        owner_epoch: 1,
                        complete_through: 0,
                    },
                    CutBound {
                        shard_id: u32::MAX,
                        owner_epoch: u64::MAX,
                        complete_through: u64::MAX,
                    },
                ],
                snapshot_id: 7,
            },
            catalog: [6; 16].try_into().unwrap(),
            frontier_revision: 2,
            destination: ExportDestination {
                id: [7; 16].try_into().unwrap(),
                binding_sha256: Digest256::from_bytes([8; 32]),
            },
            config_revision: 9,
            policy_revision: 10,
        }
    }

    #[test]
    fn same_catalog_coordinates_do_not_adopt_another_native_root() {
        let selection = selection();
        let expected = CatalogIdentity::local(
            selection.query.dataset,
            selection.catalog,
            "/archive/original/",
        )
        .unwrap();
        let other =
            CatalogIdentity::local(selection.query.dataset, selection.catalog, "/archive/copy/")
                .unwrap();
        assert!(same_custody(&expected, &expected));
        assert!(!same_custody(&expected, &other));
        let native_only = CatalogIdentity::native_only_local(
            selection.query.dataset,
            selection.catalog,
            "/archive/original/",
        )
        .unwrap();
        assert!(!same_custody(&expected, &native_only));
    }

    #[test]
    fn export_observation_requires_original_catalog_and_every_cut_field() {
        let selection = selection();
        assert!(selection.matches_observation(selection.catalog, &selection.query));
        assert!(!selection.matches_observation([9; 16].try_into().unwrap(), &selection.query));
        for field in [
            "dataset",
            "event",
            "scope-dataset",
            "kind",
            "domain",
            "ordinal",
            "topology",
            "snapshot",
            "shard",
            "epoch",
            "prefix",
            "zero-member",
            "order",
        ] {
            let mut query = selection.query.clone();
            match field {
                "dataset" => query.dataset = [9; 16].try_into().unwrap(),
                "event" => query.event_id = EventId::from_bytes([9; 32]),
                "scope-dataset" => query.scope.dataset_id = [9; 16].try_into().unwrap(),
                "kind" => query.scope.dictionary_kind = DictionaryKind::Event,
                "domain" => query.scope.allocation_domain = [9; 16].try_into().unwrap(),
                "ordinal" => query.scope.ordinal = Ordinal::new(2).unwrap(),
                "topology" => query.topology_generation = TopologyGeneration::new(1),
                "snapshot" => query.snapshot_id += 1,
                "shard" => query.bounds[0].shard_id += 1,
                "epoch" => query.bounds[0].owner_epoch += 1,
                "prefix" => query.bounds[0].complete_through += 1,
                "zero-member" => {
                    query.bounds.remove(0);
                }
                _ => query.bounds.reverse(),
            }
            assert!(
                !selection.matches_observation(selection.catalog, &query),
                "{field}"
            );
        }
    }
}
