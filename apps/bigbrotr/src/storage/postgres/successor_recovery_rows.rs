//! Fixed projections shared with the exact history reader; this purpose alone
//! accepts an activated, nonterminal head and an unresolved reservation suffix.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Activation {
    at: i64,
    transaction: u64,
}

pub(super) async fn require_positive(
    tx: &Transaction<'_>,
    intent: &Intent,
    current: ingest_registry::ControlOwner,
) -> Result<(), ProducerResolveError> {
    if current.resource_id != intent.preparer.resource_id
        || current.epoch <= intent.preparer.epoch
        || !(1..=64).contains(&intent.limit)
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    // This exact classifier excludes the old preparer, checks every ancestor,
    // and samples current Control UTC after locks. It does not require the old
    // Ingest deadline or old current-policy selection to remain live/current.
    if rows::require_target(tx, intent, "positive")
        .await?
        .is_some()
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    rows::require_intent(tx, intent).await
}

pub(super) async fn activation(
    tx: &Transaction<'_>,
    intent: &Intent,
) -> Result<Activation, ProducerResolveError> {
    let f = intent.record.fields();
    let id = f.next_manifest.identity();
    let manifest = f.next_manifest.encode();
    let handoff = intent.record.encode();
    let row = tx.query_one("SELECT activated_at_us,left(activation_transaction::text,21),
        ROW(dataset_id,target_incarnation,target_role_instance_id,producer_slot_id,producer_epoch,
            target_manifest_sha256,handoff_sha256,activated_by_resource,activated_by_epoch,activated_by_incarnation)
        IS NOT DISTINCT FROM ROW($3::bytea,$4::bytea,$5::bytea,$6::bytea,$2::text::numeric,
            sha256($7::bytea),sha256($8::bytea),$9::bytea,$10::text::numeric,$11::bytea)
        AND activation_transaction<>pg_current_xact_id()
        FROM control.ingest_activations WHERE owner_resource_id=$1::bytea AND target_epoch=$2::text::numeric", &[
        &&id.owner_resource_id.as_bytes()[..], &intent.epoch().to_string(), &&id.dataset_id.as_bytes()[..],
        &&id.process_incarnation.as_bytes()[..], &&intent.role[..], &&f.producer_slot_id.as_bytes()[..],
        &manifest.as_bytes(), &handoff.as_bytes(), &&intent.preparer.resource_id.as_bytes()[..],
        &intent.preparer.epoch.get().to_string(), &&intent.preparer.process_incarnation.as_bytes()[..],
    ]).await.map_err(backend)?;
    let at = row.try_get(0).map_err(backend)?;
    let transaction = ingest_registry::decimal(row.try_get(1).map_err(backend)?)
        .map_err(|_| OwnerError::InvalidMetadata)?;
    if !row.try_get::<_, bool>(2).map_err(backend)? || at < intent.created || transaction == 0 {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(Activation { at, transaction })
}

pub(super) fn require_head(
    head: &Epoch,
    issued: u64,
    native_closing: bool,
    limit: u32,
) -> Result<(), OwnerError> {
    if !(3..=255).contains(&head.epoch())
        || !(1..=64).contains(&limit)
        || !head.activated
        || head.source != SourceState::Open
        || head.source_cut.is_some()
        || head.source_digest.is_some()
        || !matches!(head.producer, ProducerState::Open | ProducerState::Closing)
        || issued > head.reserved
        || head.reserved > u64::from(limit)
        || (head.producer == ProducerState::Closing && !native_closing)
    {
        return Err(OwnerError::InvalidMetadata);
    }
    Ok(())
}

pub(super) async fn require_native(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    owner: &ActivatedSuccessorRecovery,
    cancelled: &AtomicBool,
) -> Result<(), ProducerResolveError> {
    let id = recovery.manifest().identity();
    if history_rows::registered_epoch(tx, recovery).await? != owner.epoch()
        || id.owner_epoch.get() != owner.epoch()
        || owner.head.manifest != owner.intent.record.fields().next_manifest
        || owner.head.slot != owner.intent.record.fields().producer_slot_id
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    require_head(
        &owner.head,
        owner.issued_through(),
        owner.native.is_closing(),
        owner.intent.limit,
    )?;
    for epoch in 1..=owner.epoch() {
        if cancelled.load(Ordering::Relaxed) {
            return Err(OwnerError::Unavailable.into());
        }
        let entry = owner
            .native
            .entry(epoch)
            .ok_or(OwnerError::InvalidMetadata)?;
        let row = history_rows::read_epoch(tx, recovery, epoch).await?;
        if row.manifest != *entry.manifest()
            || row.incoming != entry.incoming_handoff_sha256()
            || row.slot != owner.producer_slot_id()
        {
            return Err(OwnerError::InvalidMetadata.into());
        }
        if epoch < owner.epoch() {
            if row.reserved != entry.issued_through()
                || row.producer != ProducerState::Retired
                || row.source != SourceState::Closed
                || !entry.is_closing()
                || row.source_cut != Some(entry.source_through())
                || entry.terminal_claim() != row.source_digest.map(|digest| (row.kind(), digest))
            {
                return Err(OwnerError::InvalidMetadata.into());
            }
        } else if row != owner.head || entry.terminal_claim().is_some() {
            return Err(OwnerError::InvalidMetadata.into());
        }
    }
    owner
        .native
        .validate(recovery, cancelled)
        .map_err(native_error)
}

pub(super) async fn require_tail(
    tx: &Transaction<'_>,
    owner: &ActivatedSuccessorRecovery,
) -> Result<(), ProducerResolveError> {
    if let Some(tail) = owner.staged_input() {
        let original =
            producer_requests::read(tx, owner.head.manifest.identity().dataset_id, tail.request)
                .await
                .map_err(ProducerResolveError::Reservation)?;
        require_tail_binding(
            tail,
            original.as_ref(),
            Sha256::digest(owner.head.manifest.encode().as_bytes()).into(),
        )?;
    }
    Ok(())
}

pub(super) fn require_tail_binding(
    tail: SuccessorRecoveryInput,
    original: Option<&producer_requests::ProducerReservation>,
    manifest: [u8; 32],
) -> Result<(), OwnerError> {
    match (tail.reserved, original) {
        (false, None) => Ok(()), // no manifest/protection inference from preimage
        (true, Some(row))
            if row.request == tail.request
                && row.digest == tail.digest
                && row.input_bytes == tail.input_bytes
                && row.manifest_sha256 == manifest =>
        {
            Ok(())
        }
        _ => Err(OwnerError::InvalidMetadata),
    }
}
