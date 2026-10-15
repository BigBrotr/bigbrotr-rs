//! Retirement-only fixed projections. The existing positive/source classifiers
//! stay strict; SQL chooses exactly ready or terminal and native proves inputs.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Receipt {
    pub(super) transaction: u64,
    pub(super) epoch: u64,
    pub(super) incarnation: [u8; 16],
    pub(super) xmin: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Terminal {
    pub(super) source_closed_at: i64,
    pub(super) source_xmin: u32,
    pub(super) retired_at: Option<i64>,
    pub(super) receipt: Option<Receipt>,
}
pub(super) struct Context {
    pub(super) intent: Intent,
    pub(super) activation: projection::Activation,
    pub(super) head: Epoch,
    pub(super) terminal: Terminal,
    pub(super) control: ingest_registry::ControlOwner,
}

pub(super) async fn require_context(
    tx: &Transaction<'_>,
    intent: &Intent,
    current: ingest_registry::ControlOwner,
) -> Result<Option<i64>, ProducerResolveError> {
    if current.resource_id != intent.preparer.resource_id
        || current.epoch <= intent.preparer.epoch
        || !(1..=64).contains(&intent.limit)
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    // This distinct classifier also takes ordered job custody, rejects genuine
    // zero-request claims/import/checkpoint/binding obligations, and checks UTC.
    let terminal = rows::require_target(tx, intent, "retirement").await?;
    rows::require_intent(tx, intent).await?;
    Ok(terminal)
}

pub(super) fn require_head(
    head: &Epoch,
    issued: u64,
    limit: u32,
    terminal: &Terminal,
) -> Result<(), OwnerError> {
    if !(3..=255).contains(&head.epoch())
        || !(1..=64).contains(&limit)
        || !head.activated
        || head.source != SourceState::Closed
        || head.source_cut.is_none()
        || head.source_digest.is_none()
        || issued != head.reserved
        || issued > u64::from(limit)
        || head.producer
            != if terminal.retired_at.is_some() {
                ProducerState::Retired
            } else {
                ProducerState::Closing
            }
        || terminal.retired_at.is_some() != terminal.receipt.is_some()
        || terminal
            .retired_at
            .is_some_and(|at| at < terminal.source_closed_at)
    {
        return Err(OwnerError::InvalidMetadata);
    }
    Ok(())
}

pub(super) async fn require_native(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    context: &Context,
    native: &NativeSuccessorRetirement,
    cancelled: &AtomicBool,
) -> Result<(), ProducerResolveError> {
    let head = &context.head;
    if history_rows::registered_epoch(tx, recovery).await? != head.epoch()
        || recovery.manifest().identity().owner_epoch.get() != head.epoch()
        || head.manifest != context.intent.record.fields().next_manifest
        || head.slot != context.intent.record.fields().producer_slot_id
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    require_head(
        head,
        native.issued_through(),
        context.intent.limit,
        &context.terminal,
    )?;
    for epoch in 1..=head.epoch() {
        if cancelled.load(Ordering::Relaxed) {
            return Err(OwnerError::Unavailable.into());
        }
        let entry = native.entry(epoch).ok_or(OwnerError::InvalidMetadata)?;
        let row = history_rows::read_epoch(tx, recovery, epoch).await?;
        if row.manifest != *entry.manifest()
            || row.incoming != entry.incoming_handoff_sha256()
            || row.slot != native.producer_slot_id()
            || row.reserved != entry.issued_through()
            || !entry.is_closing()
            || row.source != SourceState::Closed
            || row.source_cut != Some(entry.source_through())
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        if epoch < head.epoch() {
            if row.producer != ProducerState::Retired
                || entry.terminal_claim() != row.source_digest.map(|digest| (row.kind(), digest))
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
        } else if row != *head || entry.terminal_claim().is_some() {
            return Err(OwnerError::InvalidMetadata.into());
        }
    }
    native.validate(recovery, cancelled).map_err(native_error)
}

pub(super) fn require_summary(
    summary: &LineageEpochSummary,
    context: &Context,
    native: &NativeSuccessorRetirement,
) -> Result<(), OwnerError> {
    projection::require_summary(
        summary,
        &context.head,
        native.issued_through(),
        native.head().source_through(),
    )
}

pub(super) async fn terminal(
    tx: &Transaction<'_>,
    intent: &Intent,
    current: ingest_registry::ControlOwner,
    expected: Option<i64>,
) -> Result<Terminal, ProducerResolveError> {
    let id = intent.record.fields().next_manifest.identity();
    let row = tx.query_one("SELECT s.closed_at_us,left(s.xmin::text,11),p.retired_at_us,
        left(t.terminal_transaction::text,21),left(t.control_epoch::text,21),
        substring(t.control_incarnation FROM 1 FOR 17),left(t.xmin::text,11),
        coalesce(t.control_resource=$3::bytea AND t.control_epoch>$4::text::numeric
            AND t.control_epoch<=$5::text::numeric AND o.dataset_id=s.dataset_id AND o.role='control'
            AND o.control_fence_version=1 AND o.process_incarnation=t.control_incarnation
            AND r.dataset_id=s.dataset_id AND r.role='control'
            AND ((t.control_epoch=$5::text::numeric AND t.control_incarnation=$6::bytea
                    AND o.state='active' AND r.retired_at_us IS NULL)
                OR (t.control_epoch<$5::text::numeric AND o.state='released'
                    AND o.exclusion_method='storage_fenced' AND o.exclusion_digest IS NOT NULL
                    AND r.retired_at_us IS NOT NULL)),false)
        FROM control.source_streams s JOIN control.producer_epochs p
            ON p.owner_resource_id=s.owner_resource_id AND p.owner_epoch=s.owner_epoch AND p.dataset_id=s.dataset_id
        LEFT JOIN control.ingest_terminal_transactions t ON t.owner_resource_id=p.owner_resource_id AND t.target_epoch=p.owner_epoch
        LEFT JOIN control.owner_epochs o ON o.resource_id=t.control_resource AND o.epoch=t.control_epoch
        LEFT JOIN control.role_instances r ON r.role_instance_id=o.role_instance_id
        WHERE s.owner_resource_id=$1::bytea AND s.owner_epoch=$2::text::numeric", &[
        &&id.owner_resource_id.as_bytes()[..], &intent.epoch().to_string(),
        &&current.resource_id.as_bytes()[..], &intent.preparer.epoch.get().to_string(),
        &current.epoch.get().to_string(), &&current.process_incarnation.as_bytes()[..],
    ]).await.map_err(backend)?;
    let retired_at: Option<i64> = row.try_get(2).map_err(backend)?;
    let receipt = match row.try_get::<_, Option<&str>>(3).map_err(backend)? {
        None => None,
        Some(transaction) => {
            if !row.try_get::<_, bool>(7).map_err(backend)? {
                return Err(OwnerError::InvalidMetadata.into());
            }
            let transaction = decimal(transaction)?;
            if transaction == 0 {
                return Err(OwnerError::InvalidMetadata.into());
            }
            Some(Receipt {
                transaction,
                epoch: decimal(row.try_get(4).map_err(backend)?)?,
                incarnation: row
                    .try_get::<_, &[u8]>(5)
                    .map_err(backend)?
                    .try_into()
                    .map_err(|_| OwnerError::InvalidMetadata)?,
                xmin: xid(row.try_get(6).map_err(backend)?)?,
            })
        }
    };
    if retired_at != expected || receipt.is_some() != retired_at.is_some() {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(Terminal {
        source_closed_at: row.try_get(0).map_err(backend)?,
        source_xmin: xid(row.try_get(1).map_err(backend)?)?,
        retired_at,
        receipt,
    })
}

pub(super) fn require_terminal_head(before: &Epoch, after: &Epoch) -> Result<(), OwnerError> {
    if *after
        != (Epoch {
            producer: ProducerState::Retired,
            ..*before
        })
    {
        return Err(OwnerError::InvalidMetadata);
    }
    Ok(())
}
pub(super) async fn require_terminal_effect(
    tx: &Transaction<'_>,
    before: &Terminal,
    after: &Terminal,
    current: ingest_registry::ControlOwner,
) -> Result<(), ProducerResolveError> {
    let transaction = if before.retired_at.is_none() {
        let row = tx
            .query_one("SELECT left(pg_current_xact_id()::text,21)", &[])
            .await
            .map_err(backend)?;
        Some(decimal(row.try_get(0).map_err(backend)?)?)
    } else {
        None
    };
    require_terminal_change(before, after, current, transaction)?;
    Ok(())
}
pub(super) fn require_terminal_change(
    before: &Terminal,
    after: &Terminal,
    current: ingest_registry::ControlOwner,
    transaction: Option<u64>,
) -> Result<(), OwnerError> {
    if before.source_closed_at != after.source_closed_at
        || before.source_xmin != after.source_xmin
        || after
            .retired_at
            .is_none_or(|at| at < after.source_closed_at)
    {
        return Err(OwnerError::InvalidMetadata);
    }
    if before.retired_at.is_some() {
        if before != after || transaction.is_some() {
            return Err(OwnerError::InvalidMetadata);
        }
    } else {
        let receipt = after.receipt.ok_or(OwnerError::InvalidMetadata)?;
        if before.receipt.is_some()
            || Some(receipt.transaction) != transaction
            || receipt.transaction == 0
            || receipt.epoch != current.epoch.get()
            || receipt.incarnation != *current.process_incarnation.as_bytes()
        {
            return Err(OwnerError::InvalidMetadata);
        }
    }
    Ok(())
}

pub(super) async fn require_event_jobs(
    tx: &Transaction<'_>,
    context: &Context,
    summary: &LineageEpochSummary,
) -> Result<u64, ProducerResolveError> {
    let slot = context.head.slot;
    let epoch = context.head.epoch().to_string();
    let requests = tx.query("SELECT left(r.request_sequence::text,21),r.acquisition_job_id IS NOT NULL
        AND EXISTS(SELECT 1 FROM control.acquisition_gaps g WHERE g.job_id=r.acquisition_job_id
            AND g.dataset_id=r.dataset_id AND g.producer_slot_id=r.producer_slot_id AND g.producer_epoch=r.producer_epoch)
        FROM control.producer_requests r WHERE r.producer_slot_id=$1::bytea AND r.producer_epoch=$2::text::numeric
        ORDER BY r.request_sequence LIMIT 65", &[&&slot.as_bytes()[..], &epoch]).await.map_err(backend)?;
    require_request_count(summary, requests.len())?;
    for (offset, row) in requests.iter().enumerate() {
        require_event_binding(
            summary,
            offset,
            decimal(row.try_get(0).map_err(backend)?)?,
            row.try_get(1).map_err(backend)?,
        )?;
    }
    drop(requests);
    let row = tx
        .query_one(
            "SELECT count(*) FROM(SELECT 1 FROM control.acquisition_gaps
        WHERE producer_slot_id=$1::bytea AND producer_epoch=$2::text::numeric LIMIT 65)b",
            &[&&slot.as_bytes()[..], &epoch],
        )
        .await
        .map_err(backend)?;
    u64::try_from(row.try_get::<_, i64>(0).map_err(backend)?)
        .ok()
        .filter(|n| *n <= 64)
        .ok_or_else(|| OwnerError::InvalidMetadata.into())
}
pub(super) fn require_request_count(
    summary: &LineageEpochSummary,
    count: usize,
) -> Result<(), OwnerError> {
    if count > 64
        || summary.resolved_requests != count as u64
        || (count < 64 && summary.event_request_mask >> count != 0)
    {
        return Err(OwnerError::InvalidMetadata);
    }
    Ok(())
}
pub(super) fn require_event_binding(
    summary: &LineageEpochSummary,
    offset: usize,
    sequence: u64,
    has_gap: bool,
) -> Result<(), OwnerError> {
    if offset >= 64
        || sequence != offset as u64 + 1
        || (summary.event_request_mask & (1_u64 << offset) != 0 && !has_gap)
    {
        return Err(OwnerError::InvalidMetadata);
    }
    Ok(())
}

pub(super) async fn retire(
    tx: &Transaction<'_>,
    context: &Context,
    at: i64,
) -> Result<(), ProducerResolveError> {
    let id = context.head.manifest.identity();
    let epoch = context.head.epoch().to_string();
    let digest = context
        .head
        .source_digest
        .ok_or(OwnerError::InvalidMetadata)?;
    let producer = tx.execute("UPDATE control.producer_epochs SET state='retired',retired_at_us=$3::bigint,closure_digest=$4::bytea
        WHERE producer_slot_id=$1::bytea AND producer_epoch=$2::text::numeric AND state='closing'",
        &[&&context.head.slot.as_bytes()[..], &epoch, &at, &&digest[..]]).await.map_err(backend)?;
    let domains = tx.execute("UPDATE control.allocation_domains SET state='closed',closed_at_us=$4::bigint,closure_digest=$5::bytea
        WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea AND owner_epoch=$3::text::numeric AND state='active'",
        &[&&id.dataset_id.as_bytes()[..], &&id.owner_resource_id.as_bytes()[..], &epoch, &at, &&digest[..]]).await.map_err(backend)?;
    let owner = tx.execute("UPDATE control.owner_epochs SET state='released',released_at_us=$3::bigint,
        exclusion_method='storage_fenced',exclusion_digest=$4::bytea WHERE resource_id=$1::bytea AND epoch=$2::text::numeric",
        &[&&id.owner_resource_id.as_bytes()[..], &epoch, &at, &&digest[..]]).await.map_err(backend)?;
    let role = tx.execute("UPDATE control.role_instances SET retired_at_us=$2::bigint WHERE role_instance_id=$1::bytea",
        &[&&context.intent.role[..], &at]).await.map_err(backend)?;
    if (producer, domains, owner, role) != (1, 9, 1, 1) {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(())
}
fn decimal(value: &str) -> Result<u64, OwnerError> {
    ingest_registry::decimal(value).map_err(|_| OwnerError::InvalidMetadata)
}
fn xid(value: &str) -> Result<u32, OwnerError> {
    u32::try_from(decimal(value)?).map_err(|_| OwnerError::InvalidMetadata)
}
