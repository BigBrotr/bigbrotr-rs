//! Two-slot coordinator beneath the connected owner's existing transaction and
//! deadline. Portable bases and SQL observations supply no native capability.
use super::*;
use crate::{
    runtime::{ResourceBudget, ResourceRequest},
    storage::{
        producer_prefix::Base,
        rocks::store::{
            AdmissionError, PrefixOriginalRequest, PrefixReadRequest, PrefixRetirement,
            ProducerRecoveryLimits, RetainedRequestInput, UnlinkedPrefix,
        },
    },
};
use bigbrotr_model::{Digest256, PolicyEpoch, ProducerEpoch, ProducerRequestId, RequestSequence};
use futures_util::TryStreamExt;
use std::sync::atomic::AtomicBool;
use tokio_postgres::Transaction;

pub(in crate::storage::postgres::control_owner) struct PendingPrefixPrepare(PrefixRetirement);
pub(in crate::storage) struct ConfirmedPrefixPrepare(PrefixRetirement);
impl ConfirmedPrefixPrepare {
    pub(in crate::storage::postgres::control_owner) fn plan(&self) -> &PrefixRetirement {
        &self.0
    }
    pub(in crate::storage) fn into_plan(self) -> PrefixRetirement {
        self.0
    }
}
impl PendingPrefixPrepare {
    pub(in crate::storage::postgres::control_owner) async fn commit(
        self,
        tx: Transaction<'_>,
    ) -> Result<ConfirmedPrefixPrepare, ProducerProgressError> {
        tx.commit()
            .await
            .map_err(|_| ProducerProgressError::CommitUnknown)?;
        Ok(ConfirmedPrefixPrepare(self.0))
    }
}

fn backend(_: tokio_postgres::Error) -> ProducerProgressError {
    ProducerProgressError::Backend
}

/// The caller already holds current Control/dataset and initial parent custody.
/// Keep the grant, both native permits and the connection driver through COMMIT.
/// This can retire existing originals; it never supplies durable-growth credit.
#[allow(
    clippy::too_many_arguments,
    reason = "distinct native/SQL/effect grants and existing authority"
)]
pub(in crate::storage::postgres::control_owner) async fn prepare(
    tx: &Transaction<'_>,
    context: Context,
    store: &crate::storage::rocks::store::IngestStore,
    control: ControlOwner,
    policy: PolicyEpoch,
    progress: &ProgressGrant,
    retention: u8,
    limits: ProducerRecoveryLimits,
    budget: &ResourceBudget,
    effects: &OwnedPermit,
    metadata: OwnedPermit,
    cancelled: &AtomicBool,
    mut authority: impl FnMut() -> Result<(), AdmissionError>,
) -> Result<Option<PendingPrefixPrepare>, ProducerProgressError> {
    authority().map_err(ProducerProgressError::Native)?;
    if context.epoch != 1
        || context.prefix_context() != store.prefix_context(context.slot)
        || !(1..=64).contains(&retention)
    {
        return Err(ProducerProgressError::InvalidContext);
    }
    let snapshot = load(tx, context).await?;
    if !snapshot.prefix_enabled || snapshot.revision == 0 {
        return Err(ProducerProgressError::InvalidContext);
    }
    let prior = snapshot.base.as_ref().map(|base| &base.value);
    snapshot.require_native_base(store, context)?;
    let (issued, acknowledged, retired, _) = store
        .prefix_observation(context.slot)
        .map_err(ProducerProgressError::Native)?;
    let retained = issued
        .checked_sub(retired)
        .filter(|n| *n <= 64)
        .ok_or(ProducerProgressError::InvalidContext)?;
    if retained < u64::from(retention) {
        return Ok(None);
    }
    let mut native = store
        .prefix_read(
            PrefixReadRequest {
                slot: context.slot,
                prior,
            },
            progress.permit(),
            effects,
            budget,
            cancelled,
            &mut authority,
        )
        .map_err(ProducerProgressError::Native)?;
    if !snapshot.prefix_enabled
        || snapshot.revision == 0
        || snapshot.complete != snapshot.reserved
        || snapshot.complete != native.issued()
        || native.acknowledged() != native.issued()
        || native.retired() >= native.issued()
        || native.closing()
        || issued != native.issued()
        || acknowledged != native.acknowledged()
        || native
            .prior_reference()
            .map_err(ProducerProgressError::Native)?
            != snapshot.base.as_ref().map(|base| base.reference)
    {
        return Err(ProducerProgressError::Conflict);
    }
    tx.query_one(
        "SELECT control.require_retained_producer_progress($1::bytea,1)",
        &[&&context.slot.as_bytes()[..]],
    )
    .await
    .map_err(backend)?;
    // Stream all retained rows, including incomplete rows and extras. An inner
    // join or filtered range would conceal holes/retired aliases from this seal.
    let stream = tx
        .query_raw(
            "SELECT left(r.request_sequence::text,21), substring(r.request_digest FROM 1 FOR 33),
            r.input_bytes, substring(r.manifest_sha256 FROM 1 FOR 33),
            substring(o.outcome_bytes FROM 1 FOR 65537), o.outcome_kind,
            left(o.policy_epoch::text,21), substring(o.outcome_sha256 FROM 1 FOR 33),
            r.dataset_id=$2::bytea AND r.input_profile=1 AND o.dataset_id=r.dataset_id
                AND o.request_digest=r.request_digest AND o.manifest_sha256=r.manifest_sha256,
            EXISTS(SELECT 1 FROM control.producer_input_bindings b
                WHERE b.producer_slot_id=r.producer_slot_id AND b.producer_epoch=r.producer_epoch
                    AND b.request_sequence=r.request_sequence)
         FROM control.producer_requests r LEFT JOIN control.producer_outcomes o
            USING(producer_slot_id,producer_epoch,request_sequence)
         WHERE r.producer_slot_id=$1::bytea AND r.producer_epoch=1
         ORDER BY r.request_sequence LIMIT 65",
            [
                &context.slot.as_bytes().as_slice() as &(dyn tokio_postgres::types::ToSql + Sync),
                &context.dataset.as_bytes().as_slice(),
            ],
        )
        .await
        .map_err(backend)?;
    futures_util::pin_mut!(stream);
    let mut next = native.retired();
    let mut count = 0;
    while let Some(row) = stream.try_next().await.map_err(backend)? {
        authority().map_err(ProducerProgressError::Native)?;
        next = next.checked_add(1).ok_or(ProducerProgressError::Limit)?;
        count += 1;
        if count > 64
            || rows::number(&row, 0)? != next
            || next > native.issued()
            || row.try_get::<_, Option<bool>>(8).map_err(backend)? != Some(true)
            || row.try_get::<_, bool>(9).map_err(backend)?
            || row.try_get::<_, &[u8]>(3).map_err(backend)? != context.manifest
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        // The selected connected Sync profile has no import obligations. An
        // import binding is refused intact, never silently deleted or adopted.
        let request = ProducerRequestId {
            producer_slot_id: context.slot,
            producer_epoch: ProducerEpoch::new(1).expect("initial prefix"),
            request_sequence: RequestSequence::new(next)
                .map_err(|_| ProducerProgressError::Limit)?,
        };
        let input_digest = row
            .try_get::<_, &[u8]>(1)
            .map_err(backend)?
            .try_into()
            .map_err(|_| ProducerProgressError::InvalidContext)?;
        let input_permit = budget
            .try_acquire(
                ResourceRequest::data(
                    RetainedRequestInput::required_data_bytes(limits.input)
                        .map_err(|_| ProducerProgressError::Limit)?,
                    1,
                    0,
                )
                .map_err(|_| ProducerProgressError::Limit)?,
            )
            .map_err(|_| ProducerProgressError::InsufficientReservation)?;
        let original = native
            .resolve(
                PrefixOriginalRequest {
                    request,
                    digest: Digest256::from_bytes(input_digest),
                },
                limits,
                input_permit,
                effects,
                cancelled,
                &mut authority,
            )
            .map_err(ProducerProgressError::Native)?;
        let bytes: &[u8] = row.try_get(4).map_err(backend)?;
        if row.try_get::<_, i32>(2).map_err(backend)? != original.input_bytes as i32
            || bytes != original.copy.bytes()
            || row.try_get::<_, i16>(5).map_err(backend)? != original.copy.kind() as i16
            || rows::number(&row, 6)? != original.copy.policy_epoch().get()
            || row.try_get::<_, &[u8]>(7).map_err(backend)? != Sha256::digest(bytes).as_slice()
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        native
            .record(&original)
            .map_err(ProducerProgressError::Native)?;
    }
    if next != native.issued() {
        return Err(ProducerProgressError::InvalidContext);
    }
    authority().map_err(ProducerProgressError::Native)?;
    native
        .validate(cancelled)
        .map_err(ProducerProgressError::Native)?;
    let mut jobs = Vec::new();
    jobs.try_reserve_exact(snapshot.jobs.len())
        .map_err(|_| ProducerProgressError::Limit)?;
    jobs.extend(snapshot.jobs.iter().map(|job| job.encode()));
    let base = Base {
        context: context.prefix_context(),
        generation: prior.map_or(Ok(1), |old| {
            old.generation
                .checked_add(1)
                .ok_or(ProducerProgressError::Limit)
        })?,
        previous: snapshot
            .base
            .as_ref()
            .map_or([0; 32], |old| old.reference.sha256),
        retired: native.issued(),
        source: native.source(),
        issuer_resource: *control.resource_id.as_bytes(),
        issuer_epoch: control.epoch.get(),
        issuer_incarnation: *control.process_incarnation.as_bytes(),
        policy_epoch: policy.get(),
        batches: native.batches(),
        science: native.science(),
        requests: native
            .request_chain()
            .map_err(ProducerProgressError::Native)?,
        progress_format: snapshot.format(),
        progress_revision: snapshot.revision,
        progress_digest: digest(context, &snapshot),
        jobs,
    };
    base.require_successor(prior)
        .map_err(|_| ProducerProgressError::InvalidContext)?;
    let plan = native
        .retirement(base, metadata, cancelled)
        .map_err(ProducerProgressError::Native)?;
    let bytes = plan
        .base()
        .encode()
        .map_err(|_| ProducerProgressError::InvalidContext)?;
    tx.query_one(
        "SELECT control.prepare_producer_prefix($1::bytea,$2::bytea,$3::bytea)",
        &[
            &&context.slot.as_bytes()[..],
            &bytes.as_slice(),
            &plan.reference().sha256.as_slice(),
        ],
    )
    .await
    .map_err(backend)?;
    authority().map_err(ProducerProgressError::Native)?;
    Ok(Some(PendingPrefixPrepare(plan)))
}

/// This operation must use a fresh authenticated transaction after unlink. It
/// consumes no proof until exact SQL promotion and deletion positively commit.
pub(in crate::storage::postgres::control_owner) async fn finalize(
    tx: Transaction<'_>,
    store: &crate::storage::rocks::store::IngestStore,
    proof: UnlinkedPrefix,
    control: ControlOwner,
    cancelled: &AtomicBool,
    mut authority: impl FnMut() -> Result<(), AdmissionError>,
) -> Result<(), ProducerProgressError> {
    authority().map_err(ProducerProgressError::Native)?;
    proof
        .validate(store, cancelled)
        .map_err(ProducerProgressError::Native)?;
    let base = proof.base();
    let count = base
        .retired
        .checked_sub(proof.old_retired())
        .filter(|n| *n <= 64)
        .ok_or(ProducerProgressError::Limit)?
        .to_string();
    tx.query_one(
        "SELECT control.finalize_producer_prefix($1::bytea,$2::bytea,$3::text::numeric,
            $4::bytea,$5::text::numeric,$6::bytea)",
        &[
            &base.context.slot.as_slice(),
            &proof.reference().sha256.as_slice(),
            &count,
            &control.resource_id.as_bytes().as_slice(),
            &control.epoch.get().to_string(),
            &control.process_incarnation.as_bytes().as_slice(),
        ],
    )
    .await
    .map_err(backend)?;
    proof
        .validate(store, cancelled)
        .map_err(ProducerProgressError::Native)?;
    authority().map_err(ProducerProgressError::Native)?;
    tx.commit()
        .await
        .map_err(|_| ProducerProgressError::CommitUnknown)?;
    // Delivery/cancellation after COMMIT supplies no permission to retry SQL or
    // admit new work. The caller retains its ordinary unavailable-on-error gate.
    proof
        .validate(store, cancelled)
        .map_err(ProducerProgressError::Native)?;
    authority().map_err(ProducerProgressError::Native)
}
