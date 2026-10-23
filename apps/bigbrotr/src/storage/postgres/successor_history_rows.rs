//! Serial bounded projections under the caller's actual Control/dataset locks.
//! The SQL flat checker owns registry/workflow shape; native replay supplies the
//! independent positive prefix and never trusts SQL's closure digest alone.
use super::*;
use crate::storage::rocks::manifest::IngestManifest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::postgres::control_owner) enum ProducerState {
    Open,
    Closing,
    Retired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::postgres::control_owner) enum SourceState {
    Open,
    Closing,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::postgres::control_owner) struct Epoch {
    pub(in crate::storage::postgres::control_owner) manifest: IngestManifest,
    pub(in crate::storage::postgres::control_owner) incoming: [u8; 32],
    pub(in crate::storage::postgres::control_owner) slot: ProducerSlotId,
    pub(in crate::storage::postgres::control_owner) reserved: u64,
    pub(in crate::storage::postgres::control_owner) activated: bool,
    pub(in crate::storage::postgres::control_owner) producer: ProducerState,
    pub(in crate::storage::postgres::control_owner) source: SourceState,
    pub(in crate::storage::postgres::control_owner) source_cut: Option<u64>,
    pub(in crate::storage::postgres::control_owner) source_digest: Option<[u8; 32]>,
}
impl Epoch {
    pub(super) fn require_compacted_summary(
        &self,
        summary: &CompactedInitialSummary,
    ) -> Result<(), OwnerError> {
        if self.epoch() != 1
            || self.producer != ProducerState::Retired
            || self.source != SourceState::Closed
            || !(1..=64).contains(&summary.compacted_requests)
            || summary.compacted_requests != self.reserved
            || Some(summary.last_complete_sequence) != self.source_cut
            || Some(summary.digest) != self.source_digest
        {
            return Err(OwnerError::InvalidMetadata);
        }
        Ok(())
    }

    pub(in crate::storage::postgres::control_owner) fn epoch(&self) -> u64 {
        self.manifest.identity().owner_epoch.get()
    }
    pub(in crate::storage::postgres::control_owner) fn prepared(&self) -> bool {
        !self.activated
            && self.producer == ProducerState::Open
            && self.source == SourceState::Open
            && self.reserved == 0
            && self.source_cut.is_none()
            && self.source_digest.is_none()
    }
    pub(in crate::storage::postgres::control_owner) fn kind(&self) -> RetainedTerminalKind {
        if self.epoch() == 1 {
            RetainedTerminalKind::Initial
        } else if self.activated {
            RetainedTerminalKind::Activated
        } else {
            RetainedTerminalKind::UnactivatedEmpty
        }
    }
    pub(in crate::storage::postgres::control_owner) fn require_summary(
        &self,
        summary: &LineageEpochSummary,
    ) -> Result<(), OwnerError> {
        if self.producer != ProducerState::Retired
            || self.source != SourceState::Closed
            || summary.epoch != self.epoch()
            || summary.terminal_kind != self.kind()
            || summary.resolved_requests != self.reserved
            || Some(summary.last_complete_sequence) != self.source_cut
            || Some(summary.digest) != self.source_digest
        {
            return Err(OwnerError::InvalidMetadata);
        }
        Ok(())
    }
}

pub(in crate::storage::postgres::control_owner) async fn registered_epoch(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
) -> Result<u64, ProducerResolveError> {
    let id = recovery.manifest().identity();
    let row = tx
        .query_one(
            "SELECT left(owner_epoch::text,21) FROM control.producer_epochs
        WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea
        ORDER BY owner_epoch DESC LIMIT 1",
            &[
                &&id.dataset_id.as_bytes()[..],
                &&id.owner_resource_id.as_bytes()[..],
            ],
        )
        .await
        .map_err(backend)?;
    let epoch = decimal(row.try_get(0).map_err(backend)?)?;
    if !(2..=255).contains(&epoch) {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(epoch)
}

pub(in crate::storage::postgres::control_owner) async fn read_epoch(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    epoch: u64,
) -> Result<Epoch, ProducerResolveError> {
    if !(1..=255).contains(&epoch) {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let id = recovery.manifest().identity();
    // One row, fixed-size byte fields, at most 21 decimal characters. The full
    // values are independently checked by the flat classifier before use.
    let row = tx.query_one("SELECT
        substring(control.ingest_lineage_manifest_at($2::bytea,$3::text::numeric) FROM 1 FOR 267),
        CASE WHEN $3::text::numeric=1 THEN decode(repeat('00',32),'hex')
            ELSE sha256(control.ingest_lineage_handoff_at($2::bytea,$3::text::numeric)) END,
        substring(p.producer_slot_id FROM 1 FOR 17), left(p.reserved_through_request::text,21),
        EXISTS(SELECT 1 FROM control.ingest_lineage_activation($2::bytea,$3::text::numeric)),
        left(p.state,8),left(s.state,8),left(s.last_complete_sequence::text,21),
        substring(s.closure_digest FROM 1 FOR 33)
        FROM control.producer_epochs p JOIN control.source_streams s
          ON s.dataset_id=p.dataset_id AND s.owner_resource_id=p.owner_resource_id AND s.owner_epoch=p.owner_epoch
        WHERE p.dataset_id=$1::bytea AND p.owner_resource_id=$2::bytea
          AND p.owner_epoch=$3::text::numeric", &[
        &&id.dataset_id.as_bytes()[..], &&id.owner_resource_id.as_bytes()[..], &epoch.to_string(),
    ]).await.map_err(backend)?;
    let manifest = IngestManifest::decode(row.try_get(0).map_err(backend)?)
        .map_err(|_| OwnerError::InvalidMetadata)?;
    let m = manifest.identity();
    if m.dataset_id != id.dataset_id
        || m.owner_resource_id != id.owner_resource_id
        || m.shard_id != id.shard_id
        || m.owner_epoch.get() != epoch
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let slot: [u8; 16] = row
        .try_get::<_, &[u8]>(2)
        .map_err(backend)?
        .try_into()
        .map_err(|_| OwnerError::InvalidMetadata)?;
    Ok(Epoch {
        manifest,
        incoming: fixed(&row, 1)?,
        slot: ProducerSlotId::try_from(slot).map_err(|_| OwnerError::InvalidMetadata)?,
        reserved: decimal(row.try_get(3).map_err(backend)?)?,
        activated: row.try_get(4).map_err(backend)?,
        producer: match row.try_get::<_, &str>(5).map_err(backend)? {
            "open" => ProducerState::Open,
            "closing" => ProducerState::Closing,
            "retired" => ProducerState::Retired,
            _ => return Err(OwnerError::InvalidMetadata.into()),
        },
        source: match row.try_get::<_, &str>(6).map_err(backend)? {
            "open" => SourceState::Open,
            "closing" => SourceState::Closing,
            "closed" => SourceState::Closed,
            _ => return Err(OwnerError::InvalidMetadata.into()),
        },
        source_cut: row
            .try_get::<_, Option<&str>>(7)
            .map_err(backend)?
            .map(decimal)
            .transpose()?,
        source_digest: row
            .try_get::<_, Option<&[u8]>>(8)
            .map_err(backend)?
            .map(|value| value.try_into().map_err(|_| OwnerError::InvalidMetadata))
            .transpose()?,
    })
}

pub(super) async fn require_head(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    head: &Epoch,
) -> Result<(), ProducerResolveError> {
    let resource = &recovery.manifest().identity().owner_resource_id;
    let terminal = head.producer == ProducerState::Retired && head.source == SourceState::Closed;
    if head.epoch() == 2 {
        if !terminal {
            return Err(OwnerError::InvalidMetadata.into());
        }
        // A terminal genesis needs no fabricated new-family receipt or legacy
        // InitialRegistration. The same flat checker validates its exact anchor.
        tx.query_one(
            "SELECT control.lock_ingest_lineage($1::bytea,2)",
            &[&&resource.as_bytes()[..]],
        )
        .await
        .map_err(backend)?;
    } else {
        let purpose = if head.prepared() {
            "prepared"
        } else if terminal && head.activated {
            "retirement"
        } else if terminal {
            "empty"
        } else {
            return Err(OwnerError::InvalidMetadata.into());
        };
        let row = tx
            .query_one(
                "SELECT control.require_ingest_successor($1::bytea,$2::text::numeric,$3::text)",
                &[
                    &&resource.as_bytes()[..],
                    &head.epoch().to_string(),
                    &purpose,
                ],
            )
            .await
            .map_err(backend)?;
        let retired: Option<i64> = row.try_get(0).map_err(backend)?;
        if retired.is_some() != terminal {
            return Err(OwnerError::InvalidMetadata.into());
        }
    }
    Ok(())
}

pub(super) async fn require_native(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    history: &SuccessorHistory,
    cancelled: &AtomicBool,
) -> Result<(), ProducerResolveError> {
    for entry in history.lineage.entries() {
        if cancelled.load(Ordering::Relaxed) {
            return Err(OwnerError::Unavailable.into());
        }
        let epoch = entry.manifest().identity().owner_epoch.get();
        let row = read_epoch(tx, recovery, epoch).await?;
        if row.manifest != *entry.manifest()
            || row.incoming != entry.incoming_handoff_sha256()
            || row.slot != history.lineage.producer_slot_id()
            || row.reserved != entry.issued_through()
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        if epoch <= history.selection.through {
            if row.producer != ProducerState::Retired
                || row.source != SourceState::Closed
                || row.source_cut != Some(entry.source_through())
                || !entry.is_closing()
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
            if let Some((kind, digest)) = entry.terminal_claim() {
                if kind != row.kind() || row.source_digest != Some(digest) {
                    return Err(OwnerError::InvalidMetadata.into());
                }
            } else {
                let kind = if row.activated {
                    PredecessorTerminalKind::Activated
                } else {
                    PredecessorTerminalKind::UnactivatedEmpty
                };
                if history.selection.head_kind != Some(kind) || row.source_digest.is_none() {
                    return Err(OwnerError::InvalidMetadata.into());
                }
            }
        } else if row != history.head
            || !row.prepared()
            || entry.issued_through() != 0
            || entry.source_through() != 0
            || entry.terminal_claim().is_some()
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
    }
    Ok(())
}

pub(super) async fn require_recorded(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    native: &ResolvedLineage<'_>,
    epoch: u64,
) -> Result<(), ProducerResolveError> {
    let entry = native
        .lineage()
        .entry(epoch)
        .ok_or(OwnerError::InvalidMetadata)?;
    let count = usize::try_from(entry.issued_through()).map_err(|_| OwnerError::InvalidMetadata)?;
    if count > 64 {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let records = recorded_prefix(
        tx,
        recovery,
        native.lineage().producer_slot_id(),
        epoch,
        count,
    )
    .await?;
    if !native.matches_recorded(epoch, records[..count].iter().copied()) {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(())
}

/// One bounded row at a time, reused by distinct proof owners. Reading these
/// hashes is not itself positive evidence; each caller compares its sealed proof.
pub(in crate::storage::postgres::control_owner) async fn recorded_prefix(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    slot: ProducerSlotId,
    epoch: u64,
    count: usize,
) -> Result<[(u64, [u8; 32], [u8; 32]); 64], ProducerResolveError> {
    if !(1..=255).contains(&epoch) || count > 64 {
        return Err(OwnerError::InvalidMetadata.into());
    }
    // Fixed stack scratch, not a second lineage/history allocation. Each query
    // returns one bounded projection; the flat checker already proved exact
    // prefix cardinality and locked all immutable producer/source parents.
    let mut records = [(0_u64, [0_u8; 32], [0_u8; 32]); 64];
    let id = recovery.manifest().identity();
    for (offset, record) in records[..count].iter_mut().enumerate() {
        let sequence = (offset + 1) as u64;
        let row = tx
            .query_one(
                "SELECT left(r.request_sequence::text,21),
            substring(r.request_digest FROM 1 FOR 33),substring(o.outcome_sha256 FROM 1 FOR 33)
            FROM control.producer_requests r JOIN control.producer_outcomes o
                USING(producer_slot_id,producer_epoch,request_sequence)
            WHERE r.dataset_id=$1::bytea AND o.dataset_id=$1::bytea AND r.producer_slot_id=$2::bytea
                AND r.producer_epoch=$3::text::numeric AND r.request_sequence=$4::text::numeric",
                &[
                    &&id.dataset_id.as_bytes()[..],
                    &&slot.as_bytes()[..],
                    &epoch.to_string(),
                    &sequence.to_string(),
                ],
            )
            .await
            .map_err(backend)?;
        *record = (
            decimal(row.try_get(0).map_err(backend)?)?,
            fixed(&row, 1)?,
            fixed(&row, 2)?,
        );
    }
    Ok(records)
}

const _: () = assert!(size_of::<[(u64, [u8; 32], [u8; 32]); 64]>() < 8 * 1024);
fn fixed(row: &Row, index: usize) -> Result<[u8; 32], ProducerResolveError> {
    row.try_get::<_, &[u8]>(index)
        .map_err(backend)?
        .try_into()
        .map_err(|_| OwnerError::InvalidMetadata.into())
}
fn decimal(value: &str) -> Result<u64, OwnerError> {
    ingest_registry::decimal(value).map_err(|_| OwnerError::InvalidMetadata)
}
