//! Shared fenced transaction reducer. Native witnesses are opaque and borrowed;
//! only the connected operation owner may commit the resulting head.
use super::*;
use crate::storage::rocks::store::{CheckpointKind, NativeCheckpointBasis};
use tokio_postgres::Transaction;

fn backend(_: tokio_postgres::Error) -> ProducerProgressError {
    ProducerProgressError::Backend
}

pub(in crate::storage::postgres::control_owner) async fn prepare(
    tx: &Transaction<'_>,
    context: Context,
    event: Option<BoundEvent<'_>>,
    request: Option<(u64, u64)>,
    maximum_new_dependencies: usize,
) -> Result<Snapshot, ProducerProgressError> {
    let mut snapshot = load(tx, context).await?;
    if snapshot.revision == 0 && snapshot.reserved != 0 {
        // No retroactive adoption of completed legacy setup from PG copies.
        return Err(ProducerProgressError::InvalidContext);
    }
    let mut jobs = snapshot.jobs.len();
    if let Some(event) = event {
        let job = event.job();
        match snapshot.jobs.binary_search_by_key(&job.id, |head| head.job) {
            Ok(index) => {
                let head = snapshot.jobs[index];
                if head.scope != job.scope {
                    return Err(ProducerProgressError::InvalidContext);
                }
                if head.position != event.position() {
                    event.position().require_next(Some(head.position))?;
                }
            }
            Err(_) => {
                event.position().require_next(None)?;
                jobs += 1;
            }
        }
    }
    if let Some((sequence, bytes)) = request {
        if sequence == 0
            || sequence > 64
            || sequence
                > snapshot
                    .complete
                    .checked_add(1)
                    .ok_or(ProducerProgressError::Limit)?
            || snapshot.reserved > snapshot.complete + 1
        {
            return Err(ProducerProgressError::Conflict);
        }
        if sequence > snapshot.reserved {
            snapshot.input_completion_bytes = snapshot
                .input_completion_bytes
                .checked_add(bytes)
                .and_then(|n| n.checked_add(65536))
                .ok_or(ProducerProgressError::Limit)?;
            snapshot.reserved = sequence;
        }
    } else if snapshot.complete != snapshot.reserved {
        return Err(ProducerProgressError::Conflict);
    }
    // An exact earlier mutation replay must only recover its original closure.
    // record() requires that closure to be a subset, so no new capacity is needed.
    let maximum_new_dependencies =
        if request.is_some_and(|(sequence, _)| sequence <= snapshot.complete) {
            0
        } else {
            maximum_new_dependencies
        };
    let dependencies = snapshot
        .dependencies
        .len()
        .checked_add(maximum_new_dependencies)
        .ok_or(ProducerProgressError::Limit)?;
    retained_charge(snapshot.input_completion_bytes, jobs, dependencies)?;
    Ok(snapshot)
}

/// Between reservation COMMIT and native dispatch, reacquire existing authority
/// and check the exact head observed before staging. Children are immutable or
/// require an atomic head update; no second full closure allocation is needed.
pub(in crate::storage::postgres::control_owner) async fn recheck(
    tx: &Transaction<'_>,
    context: Context,
    snapshot: &Snapshot,
) -> Result<(), ProducerProgressError> {
    let row = tx
        .query_one(
            "SELECT p.reserved_through_request=$4::text::numeric,
            c.checkpoint_revision::text, substring(c.checkpoint_digest FROM 1 FOR 33)
         FROM control.producer_epochs p LEFT JOIN control.producer_checkpoints c
            USING(producer_slot_id,producer_epoch,dataset_id)
         WHERE p.dataset_id=$1::bytea AND p.producer_slot_id=$2::bytea
            AND p.producer_epoch=$3::text::numeric FOR UPDATE OF p",
            &[
                &&context.dataset.as_bytes()[..],
                &&context.slot.as_bytes()[..],
                &context.epoch.to_string(),
                &snapshot.reserved.to_string(),
            ],
        )
        .await
        .map_err(backend)?;
    let revision: Option<&str> = row.try_get(1).map_err(backend)?;
    let matches = if snapshot.revision == 0 {
        revision.is_none()
            && row
                .try_get::<_, Option<&[u8]>>(2)
                .map_err(backend)?
                .is_none()
    } else {
        revision == Some(snapshot.revision.to_string().as_str())
            && row.try_get::<_, Option<&[u8]>>(2).map_err(backend)?
                == Some(digest(context, snapshot).as_slice())
    };
    if !row.try_get::<_, bool>(0).map_err(backend)? || !matches {
        return Err(ProducerProgressError::Conflict);
    }
    Ok(())
}

pub(in crate::storage::postgres::control_owner) async fn record(
    tx: &Transaction<'_>,
    context: Context,
    snapshot: &mut Snapshot,
    event: Option<BoundEvent<'_>>,
    basis: &NativeCheckpointBasis<'_>,
    control: ControlOwner,
) -> Result<(), ProducerProgressError> {
    if basis.manifest_sha256() != context.manifest {
        return Err(ProducerProgressError::InvalidContext);
    }
    let mutation = basis
        .request()
        .map(|request| {
            if request.producer_slot_id != context.slot
                || request.producer_epoch.get() != context.epoch
            {
                return Err(ProducerProgressError::InvalidContext);
            }
            Ok(request.request_sequence.get())
        })
        .transpose()?;
    let kind = basis.kind();
    let replacement = if let Some(event) = event {
        if basis.captured() != Some(event.captured) || kind == CheckpointKind::InternalEvidence {
            return Err(ProducerProgressError::InvalidContext);
        }
        if matches!(
            kind,
            CheckpointKind::ExistingProof | CheckpointKind::CertificationRejected
        ) && basis.intent_digest() != event.captured.certification_digest
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        let job = event.job();
        let old = snapshot
            .jobs
            .binary_search_by_key(&job.id, |head| head.job)
            .ok()
            .map(|index| snapshot.jobs[index]);
        let head = JobHead {
            job: job.id,
            scope: job.scope,
            revision: old.map_or(Ok(1), |h| {
                h.revision
                    .checked_add(1)
                    .ok_or(ProducerProgressError::Limit)
            })?,
            position: event.position(),
            intent: *basis.intent_digest().as_bytes(),
            kind: kind as u16,
            mutation: mutation.unwrap_or(0),
            classified: old.map_or(Ok(1), |h| {
                h.classified
                    .checked_add(1)
                    .ok_or(ProducerProgressError::Limit)
            })?,
            successful: old.map_or(0, |h| h.successful),
            rejected: old.map_or(0, |h| h.rejected),
            observed_at_us: event.captured.observed_at_us,
            clock_resolution_us: event.captured.clock_resolution_us,
            clock_anomaly_flags: event.captured.clock_anomaly_flags,
        };
        if let Some(old) = old.filter(|h| h.position == head.position) {
            if old.scope != head.scope
                || old.intent != head.intent
                || old.kind != head.kind
                || old.mutation != head.mutation
                || old.observed_at_us != head.observed_at_us
                || old.clock_resolution_us != head.clock_resolution_us
                || old.clock_anomaly_flags != head.clock_anomaly_flags
            {
                return Err(ProducerProgressError::Conflict);
            }
            require_subset(snapshot, basis)?;
            return Ok(());
        }
        head.position.require_next(old.map(|h| h.position))?;
        let mut head = head;
        if matches!(
            kind,
            CheckpointKind::EventNewFacts
                | CheckpointKind::EventAlreadyDurable
                | CheckpointKind::ExistingProof
        ) {
            head.successful = head
                .successful
                .checked_add(1)
                .ok_or(ProducerProgressError::Limit)?;
        } else {
            head.rejected = head
                .rejected
                .checked_add(1)
                .ok_or(ProducerProgressError::Limit)?;
        }
        Some(head)
    } else {
        if kind != CheckpointKind::InternalEvidence || basis.captured().is_some() {
            return Err(ProducerProgressError::InvalidContext);
        }
        None
    };
    if let Some(sequence) = mutation {
        if sequence <= snapshot.complete {
            if replacement.is_some() {
                return Err(ProducerProgressError::Conflict);
            }
            require_subset(snapshot, basis)?;
            return Ok(());
        }
        if snapshot.complete.checked_add(1) != Some(sequence) || sequence > snapshot.reserved {
            return Err(ProducerProgressError::Conflict);
        }
        snapshot.complete = sequence;
    } else if replacement.is_none() {
        return Err(ProducerProgressError::InvalidContext);
    }
    // Preview the exact deduplicated growth before any progress/dependency SQL.
    let mut additional = 0;
    for reference in basis.dependencies() {
        if reference.source_record_id.dataset_id != context.dataset {
            return Err(ProducerProgressError::InvalidContext);
        }
        match snapshot
            .dependencies
            .binary_search_by_key(&reference.source_record_id, |r| r.source_record_id)
        {
            Ok(index) if snapshot.dependencies[index] != *reference => {
                return Err(ProducerProgressError::Conflict);
            }
            Ok(_) => {}
            Err(_) => additional += 1,
        }
    }
    let new_job = replacement.is_some_and(|head| {
        snapshot
            .jobs
            .binary_search_by_key(&head.job, |h| h.job)
            .is_err()
    });
    retained_charge(
        snapshot.input_completion_bytes,
        snapshot.jobs.len() + usize::from(new_job),
        snapshot.dependencies.len() + additional,
    )?;
    let epoch = context.epoch.to_string();
    for reference in basis.dependencies() {
        if let Err(index) = snapshot
            .dependencies
            .binary_search_by_key(&reference.source_record_id, |r| r.source_record_id)
        {
            let id = reference.source_record_id;
            tx.execute("INSERT INTO control.producer_checkpoint_dependencies
                (dataset_id,producer_slot_id,producer_epoch,source_shard,source_epoch,source_sequence,payload_digest)
                VALUES($1::bytea,$2::bytea,$3::text::numeric,$4::bigint,$5::text::numeric,$6::text::numeric,$7::bytea)",
                &[&&context.dataset.as_bytes()[..],&&context.slot.as_bytes()[..],&epoch,
                    &i64::from(id.shard_id),&id.owner_epoch.get().to_string(),
                    &id.sequence.get().to_string(),&&reference.payload_digest.as_bytes()[..]]
            ).await.map_err(backend)?;
            snapshot.dependencies.insert(index, *reference);
        }
    }
    if let Some(head) = replacement {
        write_job(tx, context, head).await?;
        match snapshot.jobs.binary_search_by_key(&head.job, |h| h.job) {
            Ok(index) => snapshot.jobs[index] = head,
            Err(index) => snapshot.jobs.insert(index, head),
        }
    }
    snapshot.revision = snapshot
        .revision
        .checked_add(1)
        .ok_or(ProducerProgressError::Limit)?;
    let hash = digest(context, snapshot);
    let sql = if snapshot.revision == 1 {
        "INSERT INTO control.producer_checkpoints
        (dataset_id,producer_slot_id,producer_epoch,checkpoint_revision,complete_through_request,
            checkpoint_digest,committed_at_us,progress_format,manifest_sha256,progress_jobs,
            dependency_count,issuer_resource,issuer_epoch,issuer_incarnation)
        VALUES($1::bytea,$2::bytea,$3::text::numeric,$4::text::numeric,$5::text::numeric,$6::bytea,
            (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,1,$7::bytea,$8::integer,
            $9::integer,$10::bytea,$11::text::numeric,$12::bytea)
        "
    } else {
        "UPDATE control.producer_checkpoints SET
            checkpoint_revision=$4::text::numeric,complete_through_request=$5::text::numeric,
            checkpoint_digest=$6::bytea,committed_at_us=(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
            manifest_sha256=$7::bytea,progress_jobs=$8::integer,dependency_count=$9::integer,
            issuer_resource=$10::bytea,issuer_epoch=$11::text::numeric,issuer_incarnation=$12::bytea
            WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea AND producer_epoch=$3::text::numeric
                AND checkpoint_revision=$4::text::numeric-1"
    };
    let written = tx
        .execute(
            sql,
            &[
                &&context.dataset.as_bytes()[..],
                &&context.slot.as_bytes()[..],
                &epoch,
                &snapshot.revision.to_string(),
                &snapshot.complete.to_string(),
                &&hash[..],
                &&context.manifest[..],
                &(snapshot.jobs.len() as i32),
                &(snapshot.dependencies.len() as i32),
                &&control.resource_id.as_bytes()[..],
                &control.epoch.get().to_string(),
                &&control.process_incarnation.as_bytes()[..],
            ],
        )
        .await
        .map_err(backend)?;
    if written != 1 {
        return Err(ProducerProgressError::Conflict);
    }
    Ok(())
}

fn require_subset(
    snapshot: &Snapshot,
    basis: &NativeCheckpointBasis<'_>,
) -> Result<(), ProducerProgressError> {
    for reference in basis.dependencies() {
        let index = snapshot
            .dependencies
            .binary_search_by_key(&reference.source_record_id, |r| r.source_record_id)
            .map_err(|_| ProducerProgressError::Conflict)?;
        if snapshot.dependencies[index] != *reference {
            return Err(ProducerProgressError::Conflict);
        }
    }
    Ok(())
}

async fn write_job(
    tx: &Transaction<'_>,
    context: Context,
    head: JobHead,
) -> Result<(), ProducerProgressError> {
    let mutation = (head.mutation != 0).then(|| head.mutation.to_string());
    let sql = if head.revision == 1 {
        "INSERT INTO control.acquisition_progress
        (dataset_id,producer_slot_id,producer_epoch,job_id,scope_id,progress_revision,request_id,
            event_ordinal,wire_sequence,input_sha256,intent_digest,classification,mutation_request,
            classified_events,successful_events,rejected_events,
            observed_at_us,clock_resolution_us,clock_anomaly_flags)
        VALUES($1::bytea,$2::bytea,$3::text::numeric,$4::bytea,$5::bytea,$6::text::numeric,
            $7::bytea,$8::smallint,$9::text::numeric,$10::bytea,$11::bytea,$12::smallint,
            $13::text::numeric,$14::text::numeric,$15::text::numeric,$16::text::numeric,
            $17::bigint,$18::text::numeric,$19::bigint)
        "
    } else {
        "UPDATE control.acquisition_progress SET scope_id=$5::bytea,
            progress_revision=$6::text::numeric,request_id=$7::bytea,event_ordinal=$8::smallint,
            wire_sequence=$9::text::numeric,input_sha256=$10::bytea,intent_digest=$11::bytea,
            classification=$12::smallint,mutation_request=$13::text::numeric,
            classified_events=$14::text::numeric,successful_events=$15::text::numeric,
            rejected_events=$16::text::numeric,observed_at_us=$17::bigint,
            clock_resolution_us=$18::text::numeric,clock_anomaly_flags=$19::bigint
            WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea AND producer_epoch=$3::text::numeric
                AND job_id=$4::bytea AND progress_revision=$6::text::numeric-1"
    };
    let written = tx
        .execute(
            sql,
            &[
                &&context.dataset.as_bytes()[..],
                &&context.slot.as_bytes()[..],
                &context.epoch.to_string(),
                &&head.job.as_bytes()[..],
                &&head.scope.as_bytes()[..],
                &head.revision.to_string(),
                &&head.position.request[..],
                &(head.position.ordinal as i16),
                &head.position.wire_sequence.to_string(),
                &&head.position.input_digest[..],
                &&head.intent[..],
                &(head.kind as i16),
                &mutation,
                &head.classified.to_string(),
                &head.successful.to_string(),
                &head.rejected.to_string(),
                &head.observed_at_us,
                &head.clock_resolution_us.to_string(),
                &i64::from(head.clock_anomaly_flags),
            ],
        )
        .await
        .map_err(backend)?;
    if written != 1 {
        return Err(ProducerProgressError::Conflict);
    }
    Ok(())
}
