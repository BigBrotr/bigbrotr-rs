//! Bounded current-head decoding. Stream SQL rows; never collect a Row vector.
use super::*;
use bigbrotr_model::{Digest256, OwnerEpoch, Sequence, SourceRecordId};
use futures_util::TryStreamExt;
use tokio_postgres::{Row, Transaction};

fn backend(_: tokio_postgres::Error) -> ProducerProgressError {
    ProducerProgressError::Backend
}

pub(super) async fn load(
    tx: &Transaction<'_>,
    context: Context,
) -> Result<Snapshot, ProducerProgressError> {
    let epoch = context.epoch.to_string();
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
        &&context.dataset.as_bytes()[..],
        &&context.slot.as_bytes()[..],
        &epoch,
    ];
    let producer = tx
        .query_one(
            "SELECT left(reserved_through_request::text,21) FROM control.producer_epochs
         WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea
            AND producer_epoch=$3::text::numeric FOR UPDATE",
            params,
        )
        .await
        .map_err(backend)?;
    let reserved = number(&producer, 0)?;
    let prefix_schema: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=42)",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    let retained = tx.query_one(
        "SELECT coalesce(sum(input_bytes::bigint+65536),0)::bigint FROM control.producer_requests
         WHERE dataset_id=$1::bytea AND producer_slot_id=$2::bytea
            AND producer_epoch=$3::text::numeric", params
    ).await.map_err(backend)?.try_get::<_, i64>(0).map_err(backend)?;
    let quantum_schema: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=43)",
            &[],
        )
        .await
        .map_err(backend)?
        .try_get(0)
        .map_err(backend)?;
    let quantum_charge = if quantum_schema {
        tx.query_one(
            "SELECT control.quantum_retained_charge($1,$2::text::numeric)",
            &[&&context.slot.as_bytes()[..], &epoch],
        )
        .await
        .map_err(backend)?
        .try_get::<_, i64>(0)
        .map_err(backend)?
    } else {
        0
    };
    let input_completion_bytes = u64::try_from(
        retained
            .checked_add(quantum_charge)
            .ok_or(ProducerProgressError::Limit)?,
    )
    .map_err(|_| ProducerProgressError::Limit)?;
    let head_sql = format!(
        "SELECT progress_format, substring(manifest_sha256 FROM 1 FOR 33),
            left(checkpoint_revision::text,21), left(complete_through_request::text,21),
            progress_jobs, dependency_count, substring(checkpoint_digest FROM 1 FOR 33), {}
         FROM control.producer_checkpoints WHERE dataset_id=$1::bytea
            AND producer_slot_id=$2::bytea AND producer_epoch=$3::text::numeric",
        if prefix_schema {
            "prefix_profile, substring(prefix_active_bytes FROM 1 FOR 16385),
            substring(prefix_active_sha256 FROM 1 FOR 33), prefix_active_generation::text,
            prefix_active_retired::text, prefix_active_source::text,
            prefix_prepared_bytes IS NOT NULL"
        } else {
            "false, NULL::bytea, NULL::bytea, NULL::text, NULL::text, NULL::text, false"
        }
    );
    let head = tx.query_opt(&head_sql, params).await.map_err(backend)?;
    let mut snapshot = Snapshot {
        revision: 0,
        complete: 0,
        reserved,
        input_completion_bytes,
        jobs: Vec::new(),
        dependencies: Vec::new(),
        prefix_enabled: prefix_schema && context.epoch == 1,
        base: None,
    };
    snapshot
        .jobs
        .try_reserve_exact(MAX_PROGRESS_JOBS)
        .map_err(|_| ProducerProgressError::Limit)?;
    snapshot
        .dependencies
        .try_reserve_exact(MAX_DEPENDENCIES)
        .map_err(|_| ProducerProgressError::Limit)?;
    let retained_allocation = snapshot
        .jobs
        .capacity()
        .checked_mul(std::mem::size_of::<JobHead>())
        .and_then(|n| {
            snapshot
                .dependencies
                .capacity()
                .checked_mul(std::mem::size_of::<FactRef>())
                .and_then(|refs| n.checked_add(refs))
        })
        .and_then(|n| {
            n.checked_add(2 * 64 * 1024 + std::mem::size_of::<Snapshot>() + PREFIX_RUNTIME_BYTES)
        })
        .ok_or(ProducerProgressError::Limit)?;
    if retained_allocation as u64 > ProgressGrant::REQUIRED_DATA_BYTES {
        return Err(ProducerProgressError::Limit);
    }
    let expected = if let Some(row) = head {
        snapshot.prefix_enabled = row.try_get::<_, bool>(7).map_err(backend)?;
        if snapshot.prefix_enabled && context.epoch != 1 {
            return Err(ProducerProgressError::InvalidContext);
        }
        if row.try_get::<_, bool>(13).map_err(backend)? {
            // A prepared operation must finish through its recovery/finalizer;
            // it cannot be hidden by an otherwise matching current head.
            return Err(ProducerProgressError::Conflict);
        }
        let active: Option<&[u8]> = row.try_get(8).map_err(backend)?;
        if let Some(bytes) = active {
            if !snapshot.prefix_enabled || context.epoch != 1 {
                return Err(ProducerProgressError::InvalidContext);
            }
            let base =
                crate::storage::producer_prefix::Base::decode(bytes, context.prefix_context())
                    .map_err(|_| ProducerProgressError::InvalidContext)?;
            let reference = base
                .reference()
                .map_err(|_| ProducerProgressError::InvalidContext)?;
            if row.try_get::<_, Option<&[u8]>>(9).map_err(backend)?
                != Some(reference.sha256.as_slice())
                || row.try_get::<_, Option<&str>>(10).map_err(backend)?
                    != Some(reference.generation.to_string().as_str())
                || row.try_get::<_, Option<&str>>(11).map_err(backend)?
                    != Some(base.retired.to_string().as_str())
                || row.try_get::<_, Option<&str>>(12).map_err(backend)?
                    != Some(base.source.to_string().as_str())
            {
                return Err(ProducerProgressError::InvalidContext);
            }
            snapshot.base = Some(ActiveBase {
                value: base,
                reference,
            });
        } else if row
            .try_get::<_, Option<&[u8]>>(9)
            .map_err(backend)?
            .is_some()
            || (10..=12).any(|i| row.try_get::<_, Option<&str>>(i).ok().flatten().is_some())
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        if row.try_get::<_, Option<i16>>(0).map_err(backend)? != Some(snapshot.format() as i16)
            || row.try_get::<_, &[u8]>(1).map_err(backend)? != context.manifest
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        snapshot.revision = number(&row, 2)?;
        snapshot.complete = number(&row, 3)?;
        let jobs: i32 = row.try_get(4).map_err(backend)?;
        let dependencies: i32 = row.try_get(5).map_err(backend)?;
        if snapshot.revision == 0
            || snapshot.complete > reserved
            || snapshot
                .base
                .as_ref()
                .is_some_and(|base| snapshot.complete < base.value.retired)
            || !(0..=MAX_PROGRESS_JOBS as i32).contains(&jobs)
            || !(0..=MAX_DEPENDENCIES as i32).contains(&dependencies)
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        Some((
            jobs as usize,
            dependencies as usize,
            <[u8; 32]>::try_from(row.try_get::<_, &[u8]>(6).map_err(backend)?)
                .map_err(|_| ProducerProgressError::InvalidContext)?,
        ))
    } else {
        None
    };
    let retired = snapshot.base.as_ref().map_or(0, |base| base.value.retired);
    if reserved.checked_sub(retired).is_none_or(|tail| tail > 64)
        || (!snapshot.prefix_enabled && reserved > 64)
    {
        return Err(ProducerProgressError::Limit);
    }
    let rows = tx
        .query_raw(
            "SELECT substring(control.acquisition_progress_bytes(p) FROM 1 FOR 217)
         FROM control.acquisition_progress p WHERE dataset_id=$1::bytea
            AND producer_slot_id=$2::bytea AND producer_epoch=$3::text::numeric
         ORDER BY job_id LIMIT 65",
            params.iter().copied(),
        )
        .await
        .map_err(backend)?;
    futures_util::pin_mut!(rows);
    while let Some(row) = rows.try_next().await.map_err(backend)? {
        if snapshot.jobs.len() == MAX_PROGRESS_JOBS {
            return Err(ProducerProgressError::Limit);
        }
        let head = decode_job_with_limit(
            row.try_get::<_, &[u8]>(0).map_err(backend)?,
            if snapshot.prefix_enabled {
                u64::MAX
            } else {
                64
            },
        )?;
        if snapshot.jobs.last().is_some_and(|old| old.job >= head.job)
            || head.mutation > snapshot.complete
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        snapshot.jobs.push(head);
    }
    if let Some(base) = &snapshot.base {
        // The base's complete heads remain represented after detail retirement.
        for old in &base.value.jobs {
            let job = <[u8; 16]>::try_from(&old[..16]).expect("fixed base job");
            let current = snapshot
                .jobs
                .iter()
                .find(|head| head.job.as_bytes() == &job)
                .ok_or(ProducerProgressError::InvalidContext)?
                .encode();
            if current[16..32] != old[16..32]
                || [32, 172, 180, 188]
                    .iter()
                    .any(|at| u64_at(&current, *at) < u64_at(old, *at))
                || (u64_at(&current, 32) == u64_at(old, 32) && current != *old)
            {
                return Err(ProducerProgressError::InvalidContext);
            }
        }
    }
    let rows = tx
        .query_raw(
            "SELECT substring(dataset_id || control.command_u64_bytes(source_shard)
            || control.command_u64_bytes(source_epoch)
            || control.command_u64_bytes(source_sequence) || payload_digest FROM 1 FOR 73)
         FROM control.producer_checkpoint_dependencies WHERE dataset_id=$1::bytea
            AND producer_slot_id=$2::bytea AND producer_epoch=$3::text::numeric
         ORDER BY source_shard, source_epoch, source_sequence LIMIT 16385",
            params.iter().copied(),
        )
        .await
        .map_err(backend)?;
    futures_util::pin_mut!(rows);
    while let Some(row) = rows.try_next().await.map_err(backend)? {
        if snapshot.dependencies.len() == MAX_DEPENDENCIES {
            return Err(ProducerProgressError::Limit);
        }
        let reference = decode_dependency(row.try_get::<_, &[u8]>(0).map_err(backend)?)?;
        if snapshot.covered(context, reference)?
            || reference.source_record_id.dataset_id != context.dataset
            || snapshot
                .dependencies
                .last()
                .is_some_and(|old| old.source_record_id >= reference.source_record_id)
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        snapshot.dependencies.push(reference);
    }
    match expected {
        Some((jobs, dependencies, expected))
            if jobs == snapshot.jobs.len()
                && dependencies == snapshot.dependencies.len()
                && digest(context, &snapshot) == expected => {}
        None if snapshot.jobs.is_empty() && snapshot.dependencies.is_empty() => {}
        _ => return Err(ProducerProgressError::InvalidContext),
    }
    if snapshot.revision != 0 {
        snapshot.require_charge(snapshot.jobs.len(), snapshot.dependencies.len())?;
    } else if input_completion_bytes > RETAINED_BYTES {
        return Err(ProducerProgressError::Limit);
    }
    Ok(snapshot)
}

pub(super) fn number(row: &Row, index: usize) -> Result<u64, ProducerProgressError> {
    let text: &str = row.try_get(index).map_err(backend)?;
    if text.is_empty() || text.len() > 20 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ProducerProgressError::InvalidContext);
    }
    text.parse()
        .map_err(|_| ProducerProgressError::InvalidContext)
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(bytes[at..at + 8].try_into().expect("fixed profile checked"))
}

pub(super) fn decode_job(bytes: &[u8]) -> Result<JobHead, ProducerProgressError> {
    decode_job_with_limit(bytes, 64)
}
pub(super) fn decode_job_with_limit(
    bytes: &[u8],
    mutation_limit: u64,
) -> Result<JobHead, ProducerProgressError> {
    let invalid = || ProducerProgressError::InvalidContext;
    if bytes.len() != JOB_BYTES {
        return Err(invalid());
    }
    let head = JobHead {
        job: <[u8; 16]>::try_from(&bytes[..16])
            .unwrap()
            .try_into()
            .map_err(|_| invalid())?,
        scope: <[u8; 16]>::try_from(&bytes[16..32])
            .unwrap()
            .try_into()
            .map_err(|_| invalid())?,
        revision: u64_at(bytes, 32),
        position: Position {
            request: bytes[40..88].try_into().unwrap(),
            ordinal: u16::from_be_bytes(bytes[88..90].try_into().unwrap()),
            wire_sequence: u64_at(bytes, 90),
            input_digest: bytes[98..130].try_into().unwrap(),
        },
        intent: bytes[130..162].try_into().unwrap(),
        kind: u16::from_be_bytes(bytes[162..164].try_into().unwrap()),
        mutation: u64_at(bytes, 164),
        classified: u64_at(bytes, 172),
        successful: u64_at(bytes, 180),
        rejected: u64_at(bytes, 188),
        observed_at_us: i64::from_be_bytes(bytes[196..204].try_into().unwrap()),
        clock_resolution_us: u64_at(bytes, 204),
        clock_anomaly_flags: u32::from_be_bytes(bytes[212..216].try_into().unwrap()),
    };
    let mut first = head.position;
    first.ordinal = 1;
    first.require_next(None)?;
    if !(1..=MAX_QUANTUM_EVENTS).contains(&head.position.ordinal)
        || head.clock_resolution_us == 0
        || head.clock_anomaly_flags & !5 != 0
        || ((head.clock_anomaly_flags & 4 != 0) != (head.clock_resolution_us > 1))
        || head.revision == 0
        || head.revision != head.classified
        || head.successful.checked_add(head.rejected) != Some(head.classified)
        || head.classified < u64::from(head.position.ordinal)
        || !(2..=6).contains(&head.kind)
        || ((head.kind <= 4) != (head.mutation > 0))
        || head.mutation > mutation_limit
        || (matches!(head.kind, 2 | 3 | 5) && head.successful == 0)
        || (matches!(head.kind, 4 | 6) && head.rejected == 0)
    {
        return Err(invalid());
    }
    Ok(head)
}

pub(super) fn decode_dependency(bytes: &[u8]) -> Result<FactRef, ProducerProgressError> {
    let invalid = || ProducerProgressError::InvalidContext;
    if bytes.len() != DEPENDENCY_BYTES {
        return Err(invalid());
    }
    Ok(FactRef {
        source_record_id: SourceRecordId {
            dataset_id: <[u8; 16]>::try_from(&bytes[..16])
                .unwrap()
                .try_into()
                .map_err(|_| invalid())?,
            shard_id: u32::try_from(u64_at(bytes, 16)).map_err(|_| invalid())?,
            owner_epoch: OwnerEpoch::new(u64_at(bytes, 24)).map_err(|_| invalid())?,
            sequence: Sequence::new(u64_at(bytes, 32)).map_err(|_| invalid())?,
        },
        payload_digest: Digest256::from_bytes(bytes[40..72].try_into().unwrap()),
    })
}
