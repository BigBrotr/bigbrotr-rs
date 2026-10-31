//! Two-slot coordinator beneath the connected owner's existing transaction and
//! deadline. Portable bases and SQL observations supply no native capability.
use super::*;
use crate::{
    runtime::{ResourceBudget, ResourceRequest},
    storage::{
        producer_prefix::{Base, NativeFloor},
        rocks::store::{
            AdmissionError, NativePrefixRead, PrefixRetirement, ProducerRecoveryLimits,
            RetainedRequestInput, UnlinkedPrefix,
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

pub(super) struct ObservedSlots {
    pub state: CommitState,
    pub snapshot: Snapshot,
    pub prepared: Option<Base>,
}

/// Resolve an uncertain prepare/finalize only by a fresh complete locked read.
/// No write, unlink or ACK can be minted from this reporting value.
pub(super) async fn observe(
    tx: &Transaction<'_>,
    context: Context,
    store: &crate::storage::rocks::store::IngestStore,
    _progress: &ProgressGrant,
    cancelled: &AtomicBool,
    mut authority: impl FnMut() -> Result<(), AdmissionError>,
) -> Result<ObservedSlots, ProducerProgressError> {
    authority().map_err(ProducerProgressError::Native)?;
    if context.epoch != 1 || store.prefix_context(context.slot) != context.prefix_context() {
        return Err(ProducerProgressError::InvalidContext);
    }
    let snapshot = rows::load_resolving(tx, context).await?;
    if !snapshot.prefix_enabled || snapshot.revision == 0 {
        return Err(ProducerProgressError::InvalidContext);
    }
    let row = tx.query_one(
        "SELECT substring(prefix_prepared_bytes FROM 1 FOR 16385),
            substring(prefix_prepared_sha256 FROM 1 FOR 33),
            prefix_prepared_generation::text,prefix_prepared_retired::text,prefix_prepared_source::text
         FROM control.producer_checkpoints WHERE producer_slot_id=$1::bytea AND producer_epoch=1",
        &[&&context.slot.as_bytes()[..]],
    ).await.map_err(backend)?;
    let prepared = if let Some(bytes) = row.try_get::<_, Option<&[u8]>>(0).map_err(backend)? {
        let base = Base::decode(bytes, context.prefix_context())
            .map_err(|_| ProducerProgressError::InvalidContext)?;
        let reference = base
            .reference()
            .map_err(|_| ProducerProgressError::InvalidContext)?;
        if row.try_get::<_, Option<&[u8]>>(1).map_err(backend)? != Some(reference.sha256.as_slice())
            || rows::number(&row, 2)? != reference.generation
            || rows::number(&row, 3)? != base.retired
            || rows::number(&row, 4)? != base.source
            || base.progress_format != snapshot.format()
            || base.progress_revision != snapshot.revision
            || base.progress_digest != digest(context, &snapshot)
            || base.retired != snapshot.complete
            || snapshot.complete != snapshot.reserved
            || base.jobs.len() != snapshot.jobs.len()
            || base
                .jobs
                .iter()
                .zip(&snapshot.jobs)
                .any(|(old, head)| *old != head.encode())
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        Some(base)
    } else {
        if row
            .try_get::<_, Option<&[u8]>>(1)
            .map_err(backend)?
            .is_some()
            || (2..=4).any(|i| row.try_get::<_, Option<&str>>(i).ok().flatten().is_some())
        {
            return Err(ProducerProgressError::InvalidContext);
        }
        None
    };
    authority().map_err(ProducerProgressError::Native)?;
    if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(ProducerProgressError::Native(
            AdmissionError::AuthorityUnavailable,
        ));
    }
    let (issued, acknowledged, retired, floor) = store
        .prefix_observation(context.slot)
        .map_err(ProducerProgressError::Native)?;
    let state = classify(
        context.prefix_context(),
        snapshot.base.as_ref().map(|base| &base.value),
        prepared.as_ref(),
        floor,
        issued,
        acknowledged,
        retired,
    )?;
    Ok(ObservedSlots {
        state,
        snapshot,
        prepared,
    })
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
    mut native: NativePrefixRead<'_>,
    control: ControlOwner,
    policy: PolicyEpoch,
    _progress: &ProgressGrant,
    limits: ProducerRecoveryLimits,
    budget: &ResourceBudget,
    effects: &OwnedPermit,
    metadata: OwnedPermit,
    cancelled: &AtomicBool,
    mut authority: impl FnMut() -> Result<(), AdmissionError>,
) -> Result<PendingPrefixPrepare, ProducerProgressError> {
    authority().map_err(ProducerProgressError::Native)?;
    if context.epoch != 1 || context.prefix_context() != native.context() {
        return Err(ProducerProgressError::InvalidContext);
    }
    let snapshot = load(tx, context).await?;
    let prior = snapshot.base.as_ref().map(|base| &base.value);
    if !snapshot.prefix_enabled
        || snapshot.revision == 0
        || snapshot.complete != snapshot.reserved
        || snapshot.complete != native.issued()
        || native.acknowledged() != native.issued()
        || native.retired() >= native.issued()
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
                request,
                Digest256::from_bytes(input_digest),
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
    Ok(PendingPrefixPrepare(plan))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CommitState {
    Original,
    PreparedOldFloor,
    PreparedNewFloor,
    Active,
}

/// Exact positive classification for fresh recovery after an unknown commit.
/// It authorizes no effects. Old-floor replay and new-floor residual proofs are
/// distinct factories; a caller cannot turn this observation into an ACK.
pub(super) fn classify(
    context: crate::storage::producer_prefix::Context,
    active: Option<&Base>,
    prepared: Option<&Base>,
    floor: Option<NativeFloor>,
    issued: u64,
    acknowledged: u64,
    retired: u64,
) -> Result<CommitState, ProducerProgressError> {
    if acknowledged > issued
        || retired > acknowledged
        || issued.checked_sub(retired).is_none_or(|n| n > 64)
    {
        return Err(ProducerProgressError::InvalidContext);
    }
    let active_ref = active
        .map(|base| {
            if base.context != context {
                return Err(ProducerProgressError::InvalidContext);
            }
            base.reference()
                .map_err(|_| ProducerProgressError::InvalidContext)
        })
        .transpose()?;
    if let Some(base) = prepared {
        if base.context != context || base.retired != issued || acknowledged != issued {
            return Err(ProducerProgressError::InvalidContext);
        }
        base.require_successor(active)
            .map_err(|_| ProducerProgressError::InvalidContext)?;
        let reference = base
            .reference()
            .map_err(|_| ProducerProgressError::InvalidContext)?;
        if let Some(floor) = floor
            && floor.base == reference
            && floor.retired == base.retired
            && retired == base.retired
            && floor.issued == issued
            && floor.acknowledged == acknowledged
        {
            return Ok(CommitState::PreparedNewFloor);
        }
    }
    let matches = match (active, active_ref, floor) {
        (None, None, None) => retired == 0,
        (Some(base), Some(reference), Some(floor)) => {
            floor.base == reference
                && floor.retired == base.retired
                && retired == base.retired
                && floor.issued == issued
                && floor.acknowledged == acknowledged
        }
        _ => false,
    };
    if !matches {
        return Err(ProducerProgressError::InvalidContext);
    }
    Ok(if prepared.is_some() {
        CommitState::PreparedOldFloor
    } else if active.is_some() {
        CommitState::Active
    } else {
        CommitState::Original
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::producer_prefix::Context;

    fn context() -> Context {
        Context {
            dataset: [1; 16],
            slot: [2; 16],
            epoch: 1,
            manifest: [3; 32],
        }
    }

    fn first() -> Base {
        Base {
            context: context(),
            generation: 1,
            previous: [0; 32],
            retired: 62,
            source: 7,
            issuer_resource: [4; 16],
            issuer_epoch: 1,
            issuer_incarnation: [5; 16],
            policy_epoch: 1,
            batches: 1,
            science: [6; 32],
            requests: [7; 32],
            progress_format: 1,
            progress_revision: 63,
            progress_digest: [8; 32],
            jobs: Vec::new(),
        }
    }

    fn floor(base: &Base, issued: u64) -> NativeFloor {
        NativeFloor {
            issued,
            acknowledged: issued,
            retired: base.retired,
            closing: false,
            base: base.reference().unwrap(),
        }
    }

    #[test]
    fn commit_observation_distinguishes_prepared_old_new_and_finalized() {
        let base = first();
        assert_eq!(
            classify(context(), None, None, None, 62, 62, 0),
            Ok(CommitState::Original)
        );
        assert_eq!(
            classify(context(), None, Some(&base), None, 62, 62, 0),
            Ok(CommitState::PreparedOldFloor)
        );
        assert_eq!(
            classify(
                context(),
                None,
                Some(&base),
                Some(floor(&base, 62)),
                62,
                62,
                62
            ),
            Ok(CommitState::PreparedNewFloor)
        );
        assert_eq!(
            classify(
                context(),
                Some(&base),
                None,
                Some(floor(&base, 66)),
                66,
                66,
                62
            ),
            Ok(CommitState::Active)
        );
    }

    #[test]
    fn later_prepare_requires_the_exact_old_base_and_whole_resolved_tail() {
        let old = first();
        let new = Base {
            generation: 2,
            previous: old.reference().unwrap().sha256,
            retired: 124,
            progress_format: 2,
            progress_revision: 126,
            ..old.clone()
        };
        assert_eq!(
            classify(
                context(),
                Some(&old),
                Some(&new),
                Some(floor(&old, 124)),
                124,
                124,
                62
            ),
            Ok(CommitState::PreparedOldFloor)
        );
        assert_eq!(
            classify(
                context(),
                Some(&old),
                Some(&new),
                Some(floor(&new, 124)),
                124,
                124,
                124
            ),
            Ok(CommitState::PreparedNewFloor)
        );
        for (issued, ack, retired) in [(125, 124, 62), (124, 123, 62), (124, 124, 63)] {
            assert!(
                classify(
                    context(),
                    Some(&old),
                    Some(&new),
                    Some(floor(&old, issued)),
                    issued,
                    ack,
                    retired
                )
                .is_err()
            );
        }
        let changed = Base {
            previous: [9; 32],
            ..new
        };
        assert!(
            classify(
                context(),
                Some(&old),
                Some(&changed),
                Some(floor(&old, 124)),
                124,
                124,
                62
            )
            .is_err()
        );
    }

    #[test]
    fn commit_observation_refuses_crossed_or_missing_durable_halves() {
        let base = first();
        // Native publication cannot survive without its exact active/prepared
        // SQL base; SQL finalization cannot precede the atomic native write.
        assert!(classify(context(), None, None, Some(floor(&base, 62)), 62, 62, 62).is_err());
        assert!(classify(context(), Some(&base), None, None, 62, 62, 0).is_err());
        let mut changed = floor(&base, 62);
        changed.base.sha256[0] ^= 1;
        assert!(classify(context(), None, Some(&base), Some(changed), 62, 62, 62).is_err());
        assert!(
            classify(
                context(),
                Some(&base),
                None,
                Some(floor(&base, 127)),
                127,
                127,
                62
            )
            .is_err()
        );
        assert!(
            classify(
                Context {
                    manifest: [9; 32],
                    ..context()
                },
                Some(&base),
                None,
                Some(floor(&base, 62)),
                62,
                62,
                62
            )
            .is_err()
        );
    }
}
