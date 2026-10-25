//! Validate a retained typed closure under the caller's existing custody.
//! No ACK, admission capability, decoded Fact or collection of rows is returned.
use super::*;
use crate::storage::rocks::{
    manifest::IngestManifest,
    producer_floor::ClosedProducerFloor,
    store::{
        CompactedAcknowledgementPredecessor, CompactedEmptyInitialPredecessor,
        CompactedInitialAncestor, CompactedInitialPredecessor, CompactedLiveSuccessor,
        CompactedSuccessorPredecessor, IngestRecovery, ProgressRead, RetainedCompactedPrefix,
    },
};
use bigbrotr_model::{ProducerEpoch, SourceRecordId};
use futures_util::TryStreamExt;
use tokio_postgres::{Transaction, types::ToSql};

pub(in crate::storage::postgres::control_owner) fn authority(
    lease: &mut crate::runtime::policy_authority::PolicyLeaseTracker,
    policy: bigbrotr_model::PolicyEpoch,
    cancelled: &std::sync::atomic::AtomicBool,
) -> Result<(), crate::storage::rocks::store::AdmissionError> {
    use crate::storage::rocks::store::AdmissionError;
    if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(AdmissionError::AuthorityUnavailable);
    }
    lease
        .check_delivery(policy)
        .map_err(|_| AdmissionError::AuthorityUnavailable)
}

// Incremental local scratch, used before the factory moves its permit into a
// native result. Fixed row bodies and queued response chunks remain in the
// separately retained Control/driver allocation; native pins in the store's.
// Neither allowance claims to bound process RSS or PostgreSQL server memory.
const SCRATCH_BYTES: u64 = 4096;
const _: () = assert!(
    2 * std::mem::size_of::<JobHead>()
        + 2 * std::mem::size_of::<FactRef>()
        + 2 * std::mem::size_of::<Sha256>()
        + 2 * std::mem::size_of::<Context>()
        + JOB_BYTES
        + DEPENDENCY_BYTES
        + 2048 // fixed query parameters, row/stream headers and native reader/key
        < SCRATCH_BYTES as usize
);

fn backend(_: tokio_postgres::Error) -> ProducerProgressError {
    ProducerProgressError::Backend
}

/// Called only inside an authenticated historical factory. Its native context
/// must be positively checked by that factory before returning a result. All
/// progress writers and this reader take the same producer UPDATE lock before
/// any head/child read, including across READ COMMITTED statements.
#[allow(
    clippy::too_many_arguments,
    reason = "locked SQL, native context, retained grant and original deadline are distinct"
)]
pub(in crate::storage::postgres::control_owner) async fn require(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    manifest: &IngestManifest,
    slot: ProducerSlotId,
    scratch: &OwnedPermit,
    authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<u64, ProducerProgressError> {
    require_inner(tx, recovery, manifest, slot, scratch, None, authority).await
}

/// Only an inspected native v3 prefix selects this path. Ordinary recovery
/// keeps its R=0 boundary and complete-original requirement unchanged.
pub(in crate::storage::postgres::control_owner) async fn require_compacted(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    prefix: &RetainedCompactedPrefix,
    scratch: &OwnedPermit,
    cancelled: &std::sync::atomic::AtomicBool,
    authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<u64, ProducerProgressError> {
    prefix
        .validate(recovery, cancelled)
        .map_err(ProducerProgressError::Native)?;
    let complete = require_inner(
        tx,
        recovery,
        prefix.manifest(),
        prefix.producer_slot_id(),
        scratch,
        Some(prefix.floor()),
        authority,
    )
    .await?;
    prefix
        .validate(recovery, cancelled)
        .map_err(ProducerProgressError::Native)?;
    Ok(complete)
}

/// The original manifest and floor come only from a complete checked native
/// lineage view. Historical SQL classification and completion remain the
/// connected caller's obligations; no current-initial terminal rule is relaxed.
pub(in crate::storage::postgres::control_owner) async fn require_compacted_ancestor(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    ancestor: &CompactedInitialAncestor<'_>,
    scratch: &OwnedPermit,
    cancelled: &std::sync::atomic::AtomicBool,
    authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<u64, ProducerProgressError> {
    ancestor
        .validate(recovery, cancelled)
        .map_err(ProducerProgressError::Native)?;
    let complete = require_inner(
        tx,
        recovery,
        ancestor.manifest(),
        ancestor.producer_slot_id(),
        scratch,
        Some(ancestor.floor()),
        authority,
    )
    .await?;
    ancestor
        .validate(recovery, cancelled)
        .map_err(ProducerProgressError::Native)?;
    Ok(complete)
}

/// A sealed later handoff or empty-close plan retains the same ancestor on
/// either side of its exact native delta. No portable row selects this view.
pub(in crate::storage::postgres::control_owner) async fn require_compacted_successor(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    predecessor: &CompactedSuccessorPredecessor<'_>,
    scratch: &OwnedPermit,
    cancelled: &std::sync::atomic::AtomicBool,
    authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<u64, ProducerProgressError> {
    predecessor
        .validate(recovery, cancelled)
        .map_err(ProducerProgressError::Native)?;
    let complete = require_inner(
        tx,
        recovery,
        predecessor.manifest(),
        predecessor.producer_slot_id(),
        scratch,
        Some(predecessor.floor()),
        authority,
    )
    .await?;
    predecessor
        .validate(recovery, cancelled)
        .map_err(ProducerProgressError::Native)?;
    Ok(complete)
}

/// The fixed handoff seal was minted from canonical native completion. It
/// binds the exact initial floor and complete input stamp on either side of
/// the single four-write transition; portable handoff bytes cannot select it.
pub(in crate::storage::postgres::control_owner) async fn require_compacted_handoff(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    predecessor: &CompactedInitialPredecessor<'_>,
    scratch: &OwnedPermit,
    cancelled: &std::sync::atomic::AtomicBool,
    authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<u64, ProducerProgressError> {
    predecessor
        .validate(recovery, cancelled)
        .map_err(|error| ProducerProgressError::Native(error.into()))?;
    let complete = require_inner(
        tx,
        recovery,
        predecessor.manifest(),
        predecessor.producer_slot_id(),
        scratch,
        Some(predecessor.floor()),
        authority,
    )
    .await?;
    predecessor
        .validate(recovery, cancelled)
        .map_err(|error| ProducerProgressError::Native(error.into()))?;
    Ok(complete)
}

/// The empty retirement seal permits its one closing write, while preserving
/// the exact ancestor floor and complete input census through final dispatch.
pub(in crate::storage::postgres::control_owner) async fn require_compacted_empty_retirement(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    predecessor: &CompactedEmptyInitialPredecessor<'_>,
    scratch: &OwnedPermit,
    cancelled: &std::sync::atomic::AtomicBool,
    authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<u64, ProducerProgressError> {
    predecessor
        .validate(recovery, cancelled)
        .map_err(|error| ProducerProgressError::Native(error.into()))?;
    let complete = require_inner(
        tx,
        recovery,
        predecessor.manifest(),
        predecessor.producer_slot_id(),
        scratch,
        Some(predecessor.floor()),
        authority,
    )
    .await?;
    predecessor
        .validate(recovery, cancelled)
        .map_err(|error| ProducerProgressError::Native(error.into()))?;
    Ok(complete)
}

/// The owned recovery ACK seal preserves the confirmed compacted ancestor after
/// releasing the borrowed closing context. Recheck its exact H before native ACK.
pub(in crate::storage::postgres::control_owner) async fn require_compacted_acknowledgement(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    predecessor: &CompactedAcknowledgementPredecessor<'_>,
    scratch: &OwnedPermit,
    cancelled: &std::sync::atomic::AtomicBool,
    authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<u64, ProducerProgressError> {
    predecessor
        .validate(recovery, cancelled)
        .map_err(ProducerProgressError::Native)?;
    let complete = require_inner(
        tx,
        recovery,
        predecessor.manifest(),
        predecessor.producer_slot_id(),
        scratch,
        Some(predecessor.floor()),
        authority,
    )
    .await?;
    predecessor
        .validate(recovery, cancelled)
        .map_err(ProducerProgressError::Native)?;
    Ok(complete)
}

pub(in crate::storage::postgres::control_owner) async fn require_compacted_live(
    tx: &Transaction<'_>,
    view: &CompactedLiveSuccessor<'_>,
    cancelled: &std::sync::atomic::AtomicBool,
    authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<u64, ProducerProgressError> {
    view.validate(cancelled)
        .map_err(ProducerProgressError::Native)?;
    let complete = require_read(
        tx,
        view.progress_read()
            .map_err(ProducerProgressError::Native)?,
        view.manifest(),
        view.producer_slot_id(),
        view.progress_scratch(),
        Some(view.floor()),
        authority,
    )
    .await?;
    view.validate(cancelled)
        .map_err(ProducerProgressError::Native)?;
    Ok(complete)
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit floor selects a distinct recovery format under the existing locked reader"
)]
async fn require_inner(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    manifest: &IngestManifest,
    slot: ProducerSlotId,
    scratch: &OwnedPermit,
    floor: Option<&ClosedProducerFloor>,
    mut authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<u64, ProducerProgressError> {
    authority().map_err(ProducerProgressError::Native)?;
    let native = recovery
        .progress_read(scratch)
        .map_err(ProducerProgressError::Native)?;
    require_read(tx, native, manifest, slot, scratch, floor, authority).await
}
#[allow(
    clippy::too_many_arguments,
    reason = "locked typed head, borrowed native read and existing scratch/authority"
)]
async fn require_read(
    tx: &Transaction<'_>,
    native: ProgressRead<'_>,
    manifest: &IngestManifest,
    slot: ProducerSlotId,
    scratch: &OwnedPermit,
    floor: Option<&ClosedProducerFloor>,
    mut authority: impl FnMut() -> Result<(), crate::storage::rocks::store::AdmissionError>,
) -> Result<u64, ProducerProgressError> {
    let native_error = ProducerProgressError::Native;
    authority().map_err(native_error)?;
    let id = manifest.identity();
    let actual = native.manifest().identity();
    if id.dataset_id != actual.dataset_id
        || id.owner_resource_id != actual.owner_resource_id
        || id.shard_id != actual.shard_id
        || id.owner_epoch > actual.owner_epoch
    {
        return Err(ProducerProgressError::InvalidContext);
    }
    if scratch.request().units().data_bytes() < SCRATCH_BYTES
        || scratch.request().units().items() == 0
    {
        return Err(ProducerProgressError::InsufficientReservation);
    }
    let context = Context {
        dataset: id.dataset_id,
        slot,
        epoch: id.owner_epoch.get(),
        manifest: Sha256::digest(manifest.encode().as_bytes()).into(),
    };
    let epoch = context.epoch.to_string();
    let params: &[&(dyn ToSql + Sync)] = &[&&slot.as_bytes()[..], &epoch];
    let producer = tx
        .query_one(
            "SELECT substring(dataset_id FROM 1 FOR 17),
                left(reserved_through_request::text,21),
                EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=39),
                EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=40)
             FROM control.producer_epochs
             WHERE producer_slot_id=$1::bytea AND producer_epoch=$2::text::numeric
             FOR UPDATE",
            params,
        )
        .await
        .map_err(backend)?;
    authority().map_err(native_error)?;
    if producer.try_get::<_, &[u8]>(0).map_err(backend)? != context.dataset.as_bytes() {
        return Err(ProducerProgressError::InvalidContext);
    }
    let reserved = rows::number(&producer, 1)?;
    let typed: bool = producer.try_get(2).map_err(backend)?;
    let acknowledgements: bool = producer.try_get(3).map_err(backend)?;
    drop(producer);
    if floor.is_some() && (!typed || !acknowledgements) {
        return Err(ProducerProgressError::InvalidContext);
    }
    if !acknowledgements {
        native
            .require_frontier(
                slot,
                ProducerEpoch::new(context.epoch)
                    .map_err(|_| ProducerProgressError::InvalidContext)?,
                0,
                reserved,
            )
            .map_err(native_error)?;
    }
    if !typed {
        // application_transaction already authenticated the complete migration
        // ledger under the migration lock. Old profiles can only have no head;
        // an opaque legacy head never becomes accepted typed progress.
        let present: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM control.producer_checkpoints
                 WHERE producer_slot_id=$1::bytea AND producer_epoch=$2::text::numeric)",
                params,
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        authority().map_err(native_error)?;
        if present {
            return Err(ProducerProgressError::InvalidContext);
        }
        return native
            .require_frontier(
                slot,
                ProducerEpoch::new(context.epoch)
                    .map_err(|_| ProducerProgressError::InvalidContext)?,
                0,
                reserved,
            )
            .map_err(native_error)
            .map(|()| 0);
    }
    // SQL checks original reservation/outcome closure, historical attribution,
    // capacity and orphan absence. Client hashing and native reads below are
    // independent of this server-side digest comparison.
    tx.query_one(
        "SELECT control.require_retained_producer_progress($1::bytea,$2::text::numeric)",
        params,
    )
    .await
    .map_err(backend)?;
    authority().map_err(native_error)?;
    let row = tx
        .query_opt(
            "SELECT progress_format,substring(dataset_id FROM 1 FOR 17),
                substring(manifest_sha256 FROM 1 FOR 33),
                left(checkpoint_revision::text,21),left(complete_through_request::text,21),
                progress_jobs,dependency_count,substring(checkpoint_digest FROM 1 FOR 33)
             FROM control.producer_checkpoints
             WHERE producer_slot_id=$1::bytea AND producer_epoch=$2::text::numeric",
            params,
        )
        .await
        .map_err(backend)?;
    authority().map_err(native_error)?;
    let (revision, complete, jobs, dependencies, expected) = if let Some(row) = row {
        if row.try_get::<_, Option<i16>>(0).map_err(backend)? != Some(1)
            || row.try_get::<_, &[u8]>(1).map_err(backend)? != context.dataset.as_bytes()
            || row.try_get::<_, &[u8]>(2).map_err(backend)? != context.manifest
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        let revision = rows::number(&row, 3)?;
        let complete = rows::number(&row, 4)?;
        let jobs: i32 = row.try_get(5).map_err(backend)?;
        let dependencies: i32 = row.try_get(6).map_err(backend)?;
        let expected: [u8; 32] = row
            .try_get::<_, &[u8]>(7)
            .map_err(backend)?
            .try_into()
            .map_err(|_| ProducerProgressError::InvalidContext)?;
        if revision == 0
            || complete > reserved
            || !(0..=MAX_PROGRESS_JOBS as i32).contains(&jobs)
            || !(0..=MAX_DEPENDENCIES as i32).contains(&dependencies)
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        (
            revision,
            complete,
            jobs as usize,
            dependencies as usize,
            Some(expected),
        )
    } else {
        (0, 0, 0, 0, None)
    };
    if let Some(floor) = floor {
        require_compacted_head(context, floor, reserved, revision, complete, expected)?;
    } else {
        native
            .require_frontier(
                slot,
                ProducerEpoch::new(context.epoch)
                    .map_err(|_| ProducerProgressError::InvalidContext)?,
                complete,
                reserved,
            )
            .map_err(native_error)?;
    }
    let mut hash = Sha256::new();
    hash.update(DIGEST_DOMAIN);
    hash.update(context.dataset.as_bytes());
    hash.update(context.slot.as_bytes());
    hash.update(context.epoch.to_be_bytes());
    hash.update(context.manifest);
    hash.update(revision.to_be_bytes());
    hash.update(complete.to_be_bytes());
    hash.update((jobs as u64).to_be_bytes());
    hash.update((dependencies as u64).to_be_bytes());
    {
        let stream = tx
            .query_raw(
                "SELECT substring(control.acquisition_progress_bytes(p) FROM 1 FOR 217)
                 FROM control.acquisition_progress p
                 WHERE producer_slot_id=$1::bytea AND producer_epoch=$2::text::numeric
                 ORDER BY job_id LIMIT 65",
                params.iter().copied(),
            )
            .await
            .map_err(backend)?;
        futures_util::pin_mut!(stream);
        let mut previous = None;
        let mut count = 0;
        while let Some(row) = stream.try_next().await.map_err(backend)? {
            authority().map_err(native_error)?;
            if count >= jobs {
                return Err(ProducerProgressError::InvalidContext);
            }
            let bytes: &[u8] = row.try_get(0).map_err(backend)?;
            let head = rows::decode_job(bytes)?;
            if previous.is_some_and(|previous| previous >= head.job) || head.mutation > complete {
                return Err(ProducerProgressError::InvalidContext);
            }
            previous = Some(head.job);
            hash.update(bytes);
            count += 1;
        }
        if count != jobs {
            return Err(ProducerProgressError::InvalidContext);
        }
    }
    {
        let stream = tx
            .query_raw(
                "SELECT substring(dataset_id || control.command_u64_bytes(source_shard)
                    || control.command_u64_bytes(source_epoch)
                    || control.command_u64_bytes(source_sequence) || payload_digest FROM 1 FOR 73)
                 FROM control.producer_checkpoint_dependencies
                 WHERE producer_slot_id=$1::bytea AND producer_epoch=$2::text::numeric
                 ORDER BY source_shard,source_epoch,source_sequence LIMIT 16385",
                params.iter().copied(),
            )
            .await
            .map_err(backend)?;
        futures_util::pin_mut!(stream);
        let mut previous: Option<SourceRecordId> = None;
        let mut count = 0;
        while let Some(row) = stream.try_next().await.map_err(backend)? {
            authority().map_err(native_error)?;
            if count >= dependencies {
                return Err(ProducerProgressError::InvalidContext);
            }
            let bytes: &[u8] = row.try_get(0).map_err(backend)?;
            let reference = rows::decode_dependency(bytes)?;
            if reference.source_record_id.dataset_id != context.dataset
                || previous.is_some_and(|previous| previous >= reference.source_record_id)
            {
                return Err(ProducerProgressError::InvalidContext);
            }
            previous = Some(reference.source_record_id);
            hash.update(bytes);
            // This owns at most one native pin; it is dropped before the next
            // SQL await. Hash checks retain the original caller deadline.
            native
                .require_fact(reference, &mut authority)
                .map_err(native_error)?;
            count += 1;
        }
        if count != dependencies {
            return Err(ProducerProgressError::InvalidContext);
        }
    }
    let actual: [u8; 32] = hash.finalize().into();
    if expected.is_some_and(|expected| actual != expected) {
        return Err(ProducerProgressError::InvalidContext);
    }
    authority().map_err(native_error)?;
    native.validate().map_err(native_error)?;
    Ok(complete)
}

fn require_compacted_head(
    context: Context,
    floor: &ClosedProducerFloor,
    reserved: u64,
    revision: u64,
    complete: u64,
    expected: Option<[u8; 32]>,
) -> Result<(), ProducerProgressError> {
    let fields = floor.fields();
    if context.epoch != 1
        || context.manifest != fields.manifest_sha256
        || reserved != fields.issued_through
        || complete != fields.issued_through
        || revision != fields.checkpoint_revision
        || expected != Some(fields.checkpoint_digest)
    {
        return Err(ProducerProgressError::InvalidContext);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::rocks::producer_floor::ClosedProducerFloorFields;
    use bigbrotr_model::DatasetId;

    #[test]
    fn compacted_floor_requires_exact_retained_checkpoint() {
        let context = Context {
            dataset: DatasetId::try_from([1; 16]).unwrap(),
            slot: ProducerSlotId::try_from([2; 16]).unwrap(),
            epoch: 1,
            manifest: [3; 32],
        };
        let floor = ClosedProducerFloor::new(ClosedProducerFloorFields {
            issued_through: 7,
            manifest_sha256: context.manifest,
            source_through: 27,
            closure_digest: [4; 32],
            checkpoint_revision: 8,
            checkpoint_digest: [5; 32],
        })
        .unwrap();
        assert_eq!(
            require_compacted_head(context, &floor, 7, 8, 7, Some([5; 32])),
            Ok(())
        );
        // The complete canonical checksum cannot excuse an older/missing head,
        // a changed context or a reserved tail outside this entire closed cut.
        for (context, reserved, revision, complete, digest) in [
            (
                Context {
                    epoch: 2,
                    ..context
                },
                7,
                8,
                7,
                Some([5; 32]),
            ),
            (
                Context {
                    manifest: [9; 32],
                    ..context
                },
                7,
                8,
                7,
                Some([5; 32]),
            ),
            (context, 8, 8, 7, Some([5; 32])),
            (context, 7, 7, 7, Some([5; 32])),
            (context, 7, 8, 6, Some([5; 32])),
            (context, 7, 8, 7, Some([9; 32])),
            (context, 7, 0, 0, None),
        ] {
            assert_eq!(
                require_compacted_head(context, &floor, reserved, revision, complete, digest),
                Err(ProducerProgressError::InvalidContext)
            );
        }
    }
}
