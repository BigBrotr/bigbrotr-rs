//! Read-only expectations for one published Archive selection. No Ingest source
//! observation, writer recovery, native presence, retention pin or delivery grant.
use super::{
    ArchiveFrontierReceipt, BOUND, COUNTS, CURRENT, Current, Error, Input, METADATA_BYTES,
    SelectedAttempt, backend, bind_current, decode_current, number, positive,
};
use crate::{
    runtime::OwnedPermit,
    storage::{
        archive::{LocalCoordinator, RetainedInputs},
        postgres::{
            archive_attempt::{self, ArchiveAttemptRequest},
            control_owner::{
                ArchiveReadSelection, ControlSession, application_transaction, require,
            },
        },
    },
};
use bigbrotr_model::{DatasetId, Digest256, OwnerResourceId};
use bigbrotr_protocol::{
    archive_cut::{CutBound, CutRead, MAX_BATCHES},
    archive_receipt::MAX_RANGES,
};
use std::mem::size_of;
use tokio_postgres::Transaction;

const READ_METADATA_BYTES: u64 = 512 * 1024;

/// Authoritative immutable metadata for the head observed by this query. It may
/// become an older published selection while local inputs/native work run. The
/// caller must recheck current selection under its actual catalog barrier before
/// reporting current readiness. No original publisher needs to remain live.
pub struct ArchiveReadExpectations {
    selection: ArchiveReadSelection,
    current: Current,
    requests: Vec<ArchiveAttemptRequest>,
    binding: Digest256,
    permit: OwnedPermit,
}

/// Complete inputs bound to the exact selected metadata. Still not physical
/// evidence: retain this grant and real custody through the joined native read
/// and the caller's final Control/policy/current-selection checks.
pub struct BoundArchiveReadExpectations {
    selection: ArchiveReadSelection,
    receipt: ArchiveFrontierReceipt,
    cut: CutRead,
    permit: OwnedPermit,
}

impl ArchiveReadExpectations {
    /// Fixed metadata, bounded SQL decode, selected requests, complete cut/ranges
    /// and borrowed validation scratch. Each RetainedInputs, actual Control
    /// transport and native operation requires its own separate resource grant.
    pub const fn required_data_bytes() -> u64 {
        READ_METADATA_BYTES
    }
    pub fn selection(&self) -> &ArchiveReadSelection {
        &self.selection
    }
    pub fn requests(&self) -> &[ArchiveAttemptRequest] {
        &self.requests
    }

    pub fn bind_inputs(
        self,
        inputs: &[&RetainedInputs],
    ) -> Result<BoundArchiveReadExpectations, Error> {
        let cut = self.bind(inputs.iter().map(|input| Input {
            request: input.request(),
            headers: input.headers(),
            expectation: input.expectation(),
            binding: input.coordinator_binding(),
        }))?;
        Ok(BoundArchiveReadExpectations {
            selection: self.selection,
            receipt: self.current.receipt,
            cut,
            permit: self.permit,
        })
    }

    fn bind<'a>(&self, inputs: impl Iterator<Item = Input<'a>> + Clone) -> Result<CutRead, Error> {
        // Unlike recovery inventory, ordinary observation accepts exactly the
        // selected inputs. Failed/unselected attempts neither fill gaps nor
        // become an extra native expectation.
        if inputs.clone().take(MAX_BATCHES + 1).count() != self.requests.len() {
            return Err(Error::InvalidMetadata);
        }
        bind_current(
            self.selection.dataset_id,
            self.selection.catalog_id,
            self.binding,
            self.requests
                .last()
                .map_or(0, ArchiveAttemptRequest::ordinal),
            Some(&self.current),
            inputs,
        )?
        .ok_or(Error::InvalidMetadata)
    }
}
impl BoundArchiveReadExpectations {
    pub fn selection(&self) -> &ArchiveReadSelection {
        &self.selection
    }
    pub fn receipt(&self) -> ArchiveFrontierReceipt {
        self.receipt
    }
    pub fn cut(&self) -> &CutRead {
        &self.cut
    }
    /// Move the cut without a second uncharged copy. The consumer keeps the
    /// returned permit through native completion and final SQL, alongside the
    /// separately retained real custody and database-driver guards.
    pub fn into_parts(
        self,
    ) -> (
        ArchiveReadSelection,
        ArchiveFrontierReceipt,
        CutRead,
        OwnedPermit,
    ) {
        (self.selection, self.receipt, self.cut, self.permit)
    }
}

impl ControlSession {
    /// No local filesystem I/O and no writes. A supplied coordinator contributes
    /// only its already checked identity/binding; actual input reads belong on an
    /// owned joined blocking worker. Cancellation/metadata/backend errors poison
    /// this session as usual; only successful rollback restores availability.
    pub async fn read_current_archive_expectations(
        &mut self,
        coordinator: &LocalCoordinator,
        metadata: OwnedPermit,
    ) -> Result<Option<ArchiveReadExpectations>, Error> {
        let identity = coordinator.identity();
        if identity.dataset() != self.request.dataset
            || identity.catalog() == self.identity.resource_id
            || !coordinator.permits_reader_pins()
        {
            return Err(Error::InvalidInput);
        }
        capacity(0, 0, &metadata)?;
        self.begin_operation().map_err(|_| Error::Unavailable)?;
        let tx = application_transaction(&mut self.client, 17)
            .await
            .map_err(|_| Error::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| Error::Unavailable)?;
        let outcome = load(
            &tx,
            identity.dataset(),
            identity.catalog(),
            coordinator.binding_digest(),
            metadata,
        )
        .await;
        match outcome {
            Ok(_) | Err(Error::ResourceLimit) => {
                // An earlier fence check is insufficient after any bounded wait.
                require(&tx, self.request.dataset, self.identity)
                    .await
                    .map_err(|_| Error::Unavailable)?;
                tx.rollback().await.map_err(backend)?;
                if self.client.is_closed() {
                    return Err(Error::Unavailable);
                }
                self.available = true;
                outcome
            }
            error => error,
        }
    }
}

fn capacity(bounds: usize, attempts: usize, permit: &OwnedPermit) -> Result<(), Error> {
    if bounds > MAX_RANGES
        || attempts > MAX_BATCHES
        || permit.request().units().data_bytes() < READ_METADATA_BYTES
        || permit.request().units().items() == 0
    {
        Err(Error::ResourceLimit)
    } else {
        Ok(())
    }
}

async fn load(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    catalog: OwnerResourceId,
    binding: Digest256,
    permit: OwnedPermit,
) -> Result<Option<ArchiveReadExpectations>, Error> {
    let d = &dataset.as_bytes()[..];
    let c = &catalog.as_bytes()[..];
    let b = &binding.as_bytes()[..];
    if !tx
        .query_one(PREREQUISITES, &[&d, &c, &b])
        .await
        .map_err(backend)?
        .try_get::<_, bool>(0)
        .map_err(backend)?
    {
        return Err(Error::Prerequisite);
    }
    let row = tx.query_opt(CURRENT, &[&d, &c]).await.map_err(backend)?;
    if row
        .as_ref()
        .map(|r| r.try_get::<_, Option<&str>>(1))
        .transpose()
        .map_err(|_| Error::InvalidMetadata)?
        .flatten()
        .is_none()
    {
        // Initialization/issued attempts may precede any published consumer.
        // Missing joins beneath an existing head never enter this absence path.
        if !tx
            .query_one(UNPUBLISHED, &[&d, &c])
            .await
            .map_err(backend)?
            .try_get::<_, bool>(0)
            .map_err(backend)?
        {
            return Err(Error::InvalidMetadata);
        }
        return Ok(None);
    }
    let (receipt, topology) = decode_current(row.as_ref().ok_or(Error::InvalidMetadata)?, catalog)?;
    drop(row);
    let revision = receipt.revision.to_string();
    let generation = topology.get().to_string();
    let counts = tx
        .query_one(COUNTS, &[&d, &c, &revision, &generation])
        .await
        .map_err(backend)?;
    let bounds_count = count(&counts, 0)?;
    let attempts_count = count(&counts, 1)?;
    capacity(bounds_count, attempts_count, &permit)?;
    if count(&counts, 3)? > MAX_RANGES {
        return Err(Error::ResourceLimit);
    }
    if count(&counts, 3)? != bounds_count || !counts.try_get::<_, bool>(2).map_err(backend)? {
        return Err(Error::InvalidMetadata);
    }
    drop(counts);
    if !tx
        .query_one(MEMBERSHIP, &[&d, &c, &revision, &generation])
        .await
        .map_err(backend)?
        .try_get::<_, bool>(0)
        .map_err(backend)?
    {
        return Err(Error::InvalidMetadata);
    }
    let mut bounds = Vec::new();
    let mut selected_bounds = Vec::new();
    bounds
        .try_reserve_exact(bounds_count)
        .map_err(|_| Error::ResourceLimit)?;
    selected_bounds
        .try_reserve_exact(bounds_count)
        .map_err(|_| Error::ResourceLimit)?;
    for index in 0..bounds_count {
        let row = tx
            .query_one(BOUND, &[&d, &c, &revision, &generation, &(index as i64)])
            .await
            .map_err(backend)?;
        let value = CutBound {
            shard_id: u32::try_from(row.try_get::<_, i64>(0).map_err(backend)?)
                .map_err(|_| Error::InvalidMetadata)?,
            owner_epoch: positive(&row, 1)?,
            complete_through: number(&row, 2)?,
        };
        let attestation = row
            .try_get::<_, Option<&[u8]>>(3)
            .map_err(backend)?
            .map(|bytes| {
                bytes
                    .try_into()
                    .map(Digest256::from_bytes)
                    .map_err(|_| Error::InvalidMetadata)
            })
            .transpose()?;
        if !row.try_get::<_, bool>(4).map_err(backend)?
            || (value.complete_through == 0) != attestation.is_none()
            || selected_bounds
                .last()
                .is_some_and(|prior: &CutBound| prior.stream() >= value.stream())
        {
            return Err(Error::InvalidMetadata);
        }
        selected_bounds.push(value);
        bounds.push(super::Bound { value, attestation });
    }
    let mut attempts = Vec::new();
    let mut requests = Vec::new();
    attempts
        .try_reserve_exact(attempts_count)
        .map_err(|_| Error::ResourceLimit)?;
    requests
        .try_reserve_exact(attempts_count)
        .map_err(|_| Error::ResourceLimit)?;
    // Reuse the actual immutable-attempt parser, including predecessor, nonzero
    // identities, distinct native lifetimes and bounded scalar projections.
    let query = format!(
        "SELECT {},a.dataset_id=$1::bytea AND a.persistence_protocol=1 AND a.coordinator_binding_sha256=$5::bytea AND a.topology_generation<=$4::text::numeric FROM control.archive_frontier_batches b LEFT JOIN control.archive_attempts a USING(attempt_id) WHERE b.consumer_id=$2::bytea AND b.frontier_revision=$3::text::numeric ORDER BY a.ordinal NULLS FIRST,b.attempt_id LIMIT 1 OFFSET $6::bigint",
        archive_attempt::OBSERVATION
    );
    for index in 0..attempts_count {
        let row = tx
            .query_one(
                &query,
                &[&d, &c, &revision, &generation, &b, &(index as i64)],
            )
            .await
            .map_err(backend)?;
        if row.try_get::<_, Option<bool>>(11).map_err(backend)? != Some(true) {
            return Err(Error::InvalidMetadata);
        }
        let request = archive_attempt::decode(&row, dataset, catalog)
            .map_err(|_| Error::InvalidMetadata)?
            .request()
            .clone();
        if requests
            .last()
            .is_some_and(|prior: &ArchiveAttemptRequest| prior.ordinal() >= request.ordinal())
        {
            return Err(Error::InvalidMetadata);
        }
        attempts.push(SelectedAttempt {
            id: *request.attempt_id(),
            ordinal: request.ordinal(),
            topology: request.topology_generation(),
            digest: request.batch_digest(),
            coordinator: request.coordinator(),
            worker: request.worker(),
        });
        requests.push(request);
    }
    Ok(Some(ArchiveReadExpectations {
        selection: ArchiveReadSelection {
            dataset_id: dataset,
            catalog_id: catalog,
            frontier_revision: receipt.revision,
            snapshot_id: receipt.snapshot_id,
            topology_generation: topology,
            bounds: selected_bounds,
        },
        current: Current {
            receipt,
            topology,
            bounds,
            attempts,
        },
        requests,
        binding,
        permit,
    }))
}

fn count(row: &tokio_postgres::Row, index: usize) -> Result<usize, Error> {
    usize::try_from(row.try_get::<_, i64>(index).map_err(backend)?)
        .map_err(|_| Error::InvalidMetadata)
}

const PREREQUISITES: &str = "SELECT
    EXISTS(SELECT 1 FROM control.archive_coordinators WHERE dataset_id=$1::bytea AND catalog_id=$2::bytea AND binding_sha256=$3::bytea AND protocol_version=1)
    AND EXISTS(SELECT 1 FROM control.archive_persistence WHERE dataset_id=$1::bytea AND catalog_id=$2::bytea AND coordinator_binding_sha256=$3::bytea AND protocol_version=1 AND protection_class='local-synced' AND baseline_snapshot=1)
    AND NOT EXISTS(SELECT 1 FROM control.consumers WHERE dataset_id=$1::bytea AND consumer_role='archive' AND consumer_id<>$2::bytea)";

const UNPUBLISHED: &str = "SELECT
    NOT EXISTS(SELECT 1 FROM control.consumers WHERE consumer_id=$2::bytea AND NOT coalesce(dataset_id=$1::bytea AND consumer_role='archive' AND current_revision IS NULL AND current_topology_generation IS NULL,false))
    AND NOT EXISTS(SELECT 1 FROM control.consumer_frontier_revisions WHERE consumer_id=$2::bytea OR (dataset_id=$1::bytea AND consumer_role='archive'))
    AND NOT EXISTS(SELECT 1 FROM control.consumer_frontiers WHERE consumer_id=$2::bytea)
    AND NOT EXISTS(SELECT 1 FROM control.archive_frontier_snapshots WHERE consumer_id=$2::bytea OR catalog_id=$2::bytea OR dataset_id=$1::bytea)
    AND NOT EXISTS(SELECT 1 FROM control.archive_frontier_batches WHERE consumer_id=$2::bytea)";

// Counts have already admitted at most 1024 bounds/streams. Exact membership plus
// equal counts includes zero streams and refuses missing/foreign source rows.
// Open source rows give no current native head; only their namespace is checked.
const MEMBERSHIP: &str = "SELECT NOT EXISTS(
    SELECT 1 FROM control.consumer_frontiers f
    LEFT JOIN control.topology_streams t ON t.dataset_id=$1::bytea AND t.topology_generation=$4::text::numeric AND t.shard_id=f.shard_id AND t.owner_epoch=f.owner_epoch
    LEFT JOIN control.source_streams s ON s.dataset_id=t.dataset_id AND s.shard_id=t.shard_id AND s.owner_epoch=t.owner_epoch
    WHERE f.consumer_id=$2::bytea AND f.frontier_revision=$3::text::numeric
        AND NOT coalesce(t.dataset_id=f.dataset_id AND t.topology_generation=f.topology_generation
            AND s.dataset_id=f.dataset_id AND s.state IN ('open','closing','closed')
            AND (s.state<>'closed' OR s.last_complete_sequence>=f.last_complete_sequence),false))";

// The existing binder's full metadata/range bound plus the ordinary selection,
// original request vector, both owner headers and fixed SQL scratch fit together.
const _: () = assert!(
    METADATA_BYTES as usize
        + MAX_RANGES * size_of::<CutBound>()
        + MAX_BATCHES * size_of::<ArchiveAttemptRequest>()
        + size_of::<ArchiveReadExpectations>()
        + size_of::<BoundArchiveReadExpectations>()
        + 64 * 1024
        < READ_METADATA_BYTES as usize
);

#[cfg(test)]
#[path = "archive_read_expectations_tests.rs"]
mod tests;
