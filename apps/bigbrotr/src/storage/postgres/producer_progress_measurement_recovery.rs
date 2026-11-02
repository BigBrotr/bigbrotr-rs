//! Exact historical Monitor attribution, separate from live source admission.
use super::*;
use crate::storage::rocks::store::HistoricalMeasurementCheckpoint;
use tokio_postgres::Transaction;

/// snapshot() already holds dataset UPDATE before registry/producer locks. The
/// immutable dispatch and reservation may therefore be checked without a new
/// execution token, old deadline or current enrollment. No PG receipt is decoded
/// into native evidence. The separate grant funds the overlapping snapshot.
pub(in crate::storage::postgres::control_owner) async fn prepare_measurement_recovery(
    tx: &Transaction<'_>,
    context: Context,
    witness: &HistoricalMeasurementCheckpoint<'_>,
    _grant: &ProgressGrant,
) -> Result<Snapshot, ProducerProgressError> {
    witness.require().map_err(ProducerProgressError::Native)?;
    let original = witness.original();
    let measurement = original
        .measurement()
        .ok_or(ProducerProgressError::InvalidContext)?;
    let copy = original.native_outcome();
    let request = copy.request();
    if context.epoch != 1
        || measurement.resolution
        || copy.dataset() != context.dataset
        || copy.manifest_sha256() != context.manifest
        || request.producer_slot_id != context.slot
        || request.producer_epoch.get() != context.epoch
    {
        return Err(ProducerProgressError::InvalidContext);
    }
    let installed: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM control.schema_versions WHERE migration=44)",
            &[],
        )
        .await
        .map_err(|_| ProducerProgressError::Backend)?
        .try_get(0)
        .map_err(|_| ProducerProgressError::Backend)?;
    if !installed {
        return Err(ProducerProgressError::InvalidContext);
    }
    let m = measurement;
    let bytes = super::super::monitor_types::context_bytes([
        m.context.endpoint_id(),
        m.context.observer_id,
        m.context.route_id,
        m.context.access_scope_id,
        m.context.method_id,
        m.clock,
        m.visibility,
    ]);
    let row = tx.query_opt("SELECT r.request_digest=$5::bytea AND r.input_bytes=$6::integer
        AND r.input_profile=1 AND r.manifest_sha256=$7::bytea
        AND r.monitor_attempt_id=$8::bytea AND NOT r.monitor_resolution
        AND r.acquisition_job_id IS NULL AND r.acquisition_scope_id IS NULL
        AND num_nonnulls(r.progress_request_id,r.progress_event_ordinal,r.progress_wire_sequence,r.progress_input_sha256)=0
        AND (r.relay_endpoint_id,r.relay_access_scope_id,r.relay_enrollment_generation,r.relay_route_id)
            =(d.endpoint_id,d.access_scope_id,d.enrollment_generation,d.route_id)
        AND (r.issuer_resource,r.issuer_epoch,r.issuer_incarnation)=(d.issuer_resource,d.issuer_epoch,d.issuer_incarnation)
        AND d.context_bytes=$9::bytea AND d.profile_version=1 AND d.authority_ceiling_us=$10::bigint
        AND NOT EXISTS(SELECT 1 FROM control.measurement_dispatch_refusals f
            WHERE f.dataset_id=d.dataset_id AND f.attempt_id=d.attempt_id)
        FROM control.producer_requests r JOIN control.measurement_dispatches d
            ON d.dataset_id=r.dataset_id AND d.attempt_id=r.monitor_attempt_id
        WHERE r.dataset_id=$1::bytea AND r.producer_slot_id=$2::bytea
            AND r.producer_epoch=$3::text::numeric AND r.request_sequence=$4::text::numeric",
        &[&&context.dataset.as_bytes()[..],&&context.slot.as_bytes()[..],&context.epoch.to_string(),
          &request.request_sequence.get().to_string(),&&copy.digest().as_bytes()[..],&(original.input_bytes() as i32),
          &&context.manifest[..],&&m.attempt.as_bytes()[..],&&bytes[..],&(m.profile.effective_authority_us().get() as i64)]
    ).await.map_err(|_| ProducerProgressError::Backend)?;
    if row
        .and_then(|row| row.try_get::<_, Option<bool>>(0).ok())
        .flatten()
        != Some(true)
    {
        return Err(ProducerProgressError::InvalidContext);
    }
    let sequence = request.request_sequence.get();
    let snapshot = prepare(
        tx,
        context,
        None,
        Some((sequence, u64::from(original.input_bytes()))),
        witness.basis().dependencies().len(),
    )
    .await?;
    // Only the next reserved Monitor tail may grow a closing producer. An exact
    // already-complete replay still uses record()'s subset/read-only branch.
    if snapshot.revision == 0
        || !snapshot.prefix_enabled
        || snapshot.base.is_some()
        || (snapshot.complete < sequence
            && (snapshot.complete.checked_add(1) != Some(sequence)
                || snapshot.reserved != sequence))
    {
        return Err(ProducerProgressError::Conflict);
    }
    witness.require().map_err(ProducerProgressError::Native)?;
    Ok(snapshot)
}
