//! Complete original input replay precedes catalog reconciliation or new work.
use super::super::archive::native;
use super::*;
use bigbrotr::storage::{
    archive::{LocalAttempt, RetainedInputs, WorkerBudgets},
    postgres::{
        archive_attempt::ArchiveAttemptReceipt,
        control_owner::{RetainedArchiveFrontier, RetainedArchiveRequest},
    },
    rocks::store::{PendingBatch, PendingBatchHeader, PendingReadLimits},
};
use bigbrotr_model::{AdmissionBatch, ArchiveBatch, SourceRecordId, codec::FactDecodeLimits};
use bigbrotr_protocol::archive_receipt::ReceiptExpectation;

pub(super) const RECORDS: usize = 64;
pub(super) const ENCODED: usize = 1024 * 1024;

pub(super) fn limits() -> PendingReadLimits {
    PendingReadLimits {
        max_records: RECORDS,
        max_encoded_bytes: ENCODED,
        fact: FactDecodeLimits::default(),
    }
}

pub(super) struct Attempt {
    pub(super) receipt: ArchiveAttemptReceipt,
    pub(super) inputs: RetainedInputs,
    pub(super) present: bool,
}
pub(super) struct Inventory {
    pub(super) attempts: Vec<Attempt>,
    // Fixed receipt pages, vector headers and temporary input references. The
    // scientific inputs each retain their separate required allocation grant.
    _metadata: OwnedPermit,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn load(
    root: &Resolved,
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    recovery: &IngestRecovery,
    proof: &VerifiedSuccessorHistory<'_>,
    coordinator: &LocalCoordinator,
    frontier: &mut Option<RetainedArchiveFrontier>,
) -> Result<Inventory> {
    let metadata = grant(budget, MIB, 0)?;
    authority.refresh(owner).await?;
    authority
        .operation(owner.require_archive_coordinator(coordinator))
        .await?;
    authority.refresh(owner).await?;
    let mut scan = authority
        .operation(owner.begin_archive_attempt_scan(coordinator.identity().catalog()))
        .await?;
    if scan.high_water() > MAX_ATTEMPTS as u64 {
        return Err(error(
            "complete issued attempt history exceeds finite profile",
        ));
    }
    let mut attempts = Vec::new();
    attempts
        .try_reserve_exact(scan.high_water() as usize)
        .map_err(error)?;
    while !scan.is_complete() {
        authority.refresh(owner).await?;
        let page = authority
            .operation(owner.read_archive_attempt_page(&mut scan))
            .await?;
        for receipt in page {
            authority.refresh(owner).await?;
            let inputs = authority.local(|cancel| {
                coordinator
                    .read_inputs(
                        receipt.request(),
                        grant(budget, RetainedInputs::required_data_bytes(), 0)?,
                        cancel,
                    )
                    .map_err(error)?
                    .ok_or_else(|| error("issued attempt has no original input linkage"))
            })?;
            replay_inputs(budget, authority, owner, proof, &inputs).await?;
            attempts.push(Attempt {
                receipt,
                inputs,
                present: false,
            });
        }
    }
    authority.refresh(owner).await?;
    let history_request = request(root, authority.cancellation)?;
    if let Some(keeper) = frontier.as_mut() {
        authority
            .operation(owner.inspect_compacted_retained_archive_frontier(
                keeper,
                proof,
                recovery,
                RetainedArchiveRequest {
                    coordinator,
                    attempts: &scan,
                },
                history_request,
                |d, h| validate(root, d, h),
            ))
            .await?;
    } else {
        *frontier = Some(
            authority
                .operation(owner.inspect_retained_archive_frontier(
                    proof,
                    recovery,
                    RetainedArchiveRequest {
                        coordinator,
                        attempts: &scan,
                    },
                    history_request,
                    grant(budget, RetainedArchiveFrontier::required_data_bytes(), 0)?,
                    |d, h| validate(root, d, h),
                ))
                .await?,
        );
    }
    let references: Vec<_> = attempts.iter().map(|attempt| &attempt.inputs).collect();
    frontier
        .as_mut()
        .expect("loaded frontier")
        .bind_current_inputs_in_place(&references)
        .map_err(error)?;
    Ok(Inventory {
        attempts,
        _metadata: metadata,
    })
}

pub(super) fn pending(
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    proof: &VerifiedSuccessorHistory<'_>,
    first: SourceRecordId,
) -> Result<PendingBatch> {
    authority.local(|cancel| {
        let resolved = proof.resolved_lineage();
        let permit = grant(budget, limits().required_data_bytes().map_err(error)?, 0)?;
        if proof.compacted_initial_requests() != 0 {
            resolved.read_complete_compacted_pending_batch(first, limits(), permit, cancel)
        } else {
            resolved.read_complete_pending_batch(first, limits(), permit, cancel)
        }
        .map_err(error)
    })
}

async fn replay_inputs(
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    proof: &VerifiedSuccessorHistory<'_>,
    inputs: &RetainedInputs,
) -> Result<()> {
    let count = inputs.expectation().record_count;
    if count == 0 || count > RECORDS as u64 || inputs.headers().len() > RECORDS {
        return Err(error("retained attempt exceeds complete-batch profile"));
    }
    // Only fixed FactRef metadata is copied, under the retained input's 1MiB
    // allowance. Each original payload is checked and dropped before the next.
    let mut batches = Vec::new();
    batches
        .try_reserve_exact(inputs.headers().len())
        .map_err(error)?;
    for header in inputs.headers() {
        authority.refresh(owner).await?;
        let pending = pending(budget, authority, proof, header.first())?;
        if PendingBatchHeader::from_batch(pending.batch()) != *header {
            return Err(error("original complete input header differs"));
        }
        let mut records = Vec::new();
        records
            .try_reserve_exact(pending.batch().records().len())
            .map_err(error)?;
        records.extend_from_slice(pending.batch().records());
        batches.push(AdmissionBatch::new(records).map_err(error)?);
    }
    let archive =
        ArchiveBatch::new(1, inputs.request().topology_generation(), &batches).map_err(error)?;
    inputs.verify_replay(&archive).map_err(error)
}

impl Inventory {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn reconcile(
        &mut self,
        root: &Resolved,
        budget: &ResourceBudget,
        native_budget: &ResourceBudget,
        authority: &mut Authority<'_>,
        owner: &mut ControlSession,
        proof: &VerifiedSuccessorHistory<'_>,
        coordinator: &LocalCoordinator,
        reader: &Arc<native::Reader>,
        recovery: &IngestRecovery,
        frontier: &RetainedArchiveFrontier,
    ) -> Result<()> {
        // Validate every real issued request and retained coordinator binding
        // before any original lifetime is resolved or candidate is selected.
        for attempt in &self.attempts {
            native::catch_up::require_retained_identity(
                &attempt.receipt,
                &attempt.inputs,
                coordinator.identity(),
                coordinator.binding_digest(),
            )?;
        }
        // Move each nonclone custody/input owner into actual joined work. Keep
        // the vector allocation and every other input grant until it returns.
        let mut remaining = std::mem::take(&mut self.attempts).into_iter();
        self.attempts
            .try_reserve_exact(remaining.len())
            .map_err(error)?;
        let current = frontier.bound_current_inputs().map_err(error)?;
        while let Some(mut attempt) = remaining.next() {
            authority.refresh(owner).await?;
            authority.history(owner, recovery, proof, frontier).await?;
            // Selection borrows the still-retained inputs. Only one bounded
            // expected receipt is copied into the joined job, never custody.
            let replacement = retained_replacement(
                attempt.inputs.expectation(),
                attempt.inputs.headers(),
                current.current_cut().map(|cut| cut.receipts.as_slice()),
                remaining
                    .as_slice()
                    .iter()
                    .map(|candidate| (candidate.inputs.expectation(), candidate.inputs.headers())),
            )?
            .map(|(mode, expected)| (mode, expected.clone()));
            let local = authority.local(|cancel| {
                proof.validate(cancel).map_err(error)?;
                LocalAttempt::recover(
                    coordinator.state_root(),
                    &attempt.receipt,
                    coordinator.identity(),
                )
                .map_err(error)
            })?;
            let custody = coordinator.reader_custody().map_err(error)?;
            let binding = coordinator.binding_digest();
            let (root, app, native, reader) = (
                root.clone(),
                budget.clone(),
                native_budget.clone(),
                reader.clone(),
            );
            let deadline = Instant::now() + OPERATION;
            attempt = authority.blocking(owner, deadline, move |cancel| {
                let result = native::catch_up::reconcile_attempt(
                    &root, &attempt.receipt, &local, &attempt.inputs,
                    replacement.as_ref().map(|(mode, expected)| (*mode, expected)), &reader, &custody, binding,
                    WorkerBudgets { application: &app, native: &native }, deadline, cancel,
                )?;
                attempt.present = match result {
                    native::catch_up::ReconciledAttempt::Absent { resolution } => {
                        // Both known resolutions permit an independently absent
                        // receipt; neither permits reuse of the old credential.
                        match resolution {
                            bigbrotr::storage::postgres::catalog_guard::Resolution::NotImported
                            | bigbrotr::storage::postgres::catalog_guard::Resolution::Sealed { .. } => false,
                        }
                    }
                    native::catch_up::ReconciledAttempt::Present { resolution, membership } => {
                        if !matches!(resolution, bigbrotr::storage::postgres::catalog_guard::Resolution::Sealed { .. }) {
                            return Err(error("present attempt lacks positive exclusion"));
                        }
                        membership.validate(&bigbrotr_protocol::archive_membership::MembershipRead {
                            receipt: attempt.inputs.expectation().clone(), snapshot_id: None,
                        }).map_err(error)?;
                        true
                    }
                };
                Ok(attempt)
            }).await?;
            authority.history(owner, recovery, proof, frontier).await?;
            self.attempts.push(attempt);
        }
        if let Some(current) = frontier
            .bound_current_inputs()
            .map_err(error)?
            .current_cut()
        {
            for expected in &current.receipts {
                if !self
                    .attempts
                    .iter()
                    .any(|a| a.present && a.inputs.expectation() == expected)
                {
                    return Err(error("published frontier receipt is absent or incomplete"));
                }
            }
        }
        Ok(())
    }
}

fn retained_replacement<'a>(
    original: &ReceiptExpectation,
    headers: &[PendingBatchHeader],
    current: Option<&[ReceiptExpectation]>,
    candidates: impl Iterator<Item = (&'a ReceiptExpectation, &'a [PendingBatchHeader])> + Clone,
) -> Result<Option<(native::catch_up::ReplacementMode, &'a ReceiptExpectation)>> {
    use native::catch_up::ReplacementMode;
    original.validate().map_err(error)?;
    if let Some(current) = current {
        for (index, selected) in current.iter().enumerate() {
            selected.validate().map_err(error)?;
            if selected.dataset_id != original.dataset_id
                || current[..index].iter().any(|prior| {
                    prior.attempt_id == selected.attempt_id
                        || prior.publication_ordinal == selected.publication_ordinal
                })
            {
                return Err(error("conflicting current retained receipt identity"));
            }
        }
    }
    let bound = current_replacement(original, headers, current, candidates.clone())?;
    let mut unbound = None;
    let mut multiple = false;
    for (index, (candidate, candidate_headers)) in candidates.clone().enumerate() {
        candidate.validate().map_err(error)?;
        if candidate.attempt_id == original.attempt_id
            || candidate.publication_ordinal <= original.publication_ordinal
            || candidates.clone().take(index).any(|(prior, _)| {
                prior.attempt_id == candidate.attempt_id
                    || prior.publication_ordinal == candidate.publication_ordinal
            })
        {
            return Err(error("conflicting retained candidate identity or order"));
        }
        if candidate.batch_digest == original.batch_digest || candidate_headers == headers {
            // Do not discard incompatible metadata to manufacture uniqueness,
            // including a malformed third candidate after a valid match.
            native::catch_up::require_replacement(original, candidate)?;
            if candidate_headers != headers {
                return Err(error("retained replacement original headers differ"));
            }
            multiple |= unbound.is_some();
            unbound = Some(candidate);
        }
    }
    if let Some(bound) = bound {
        return Ok(Some((ReplacementMode::CurrentBound, bound)));
    }
    if current.is_some_and(|current| {
        current.iter().any(|selected| {
            selected.attempt_id == original.attempt_id
                || selected.publication_ordinal == original.publication_ordinal
        })
    }) {
        // Explicit current selection of A keeps its ordinary strict path.
        return Ok(None);
    }
    // Several valid later candidates select none. Ordinary strict A observation
    // still permits all-absent history and refuses any committed digest collision.
    Ok(unbound
        .filter(|_| !multiple)
        .map(|expected| (ReplacementMode::ProvisionalUnbound, expected)))
}

fn current_replacement<'a>(
    original: &ReceiptExpectation,
    headers: &[PendingBatchHeader],
    current: Option<&[ReceiptExpectation]>,
    candidates: impl Iterator<Item = (&'a ReceiptExpectation, &'a [PendingBatchHeader])>,
) -> Result<Option<&'a ReceiptExpectation>> {
    let Some(current) = current else {
        return Ok(None);
    };
    if let Some(selected) = current.iter().find(|selected| {
        selected.attempt_id == original.attempt_id
            || selected.publication_ordinal == original.publication_ordinal
    }) {
        if selected != original {
            return Err(error("current original receipt identity differs"));
        }
        // Current members retain their ordinary strict scan and membership.
        return Ok(None);
    }
    let mut selected = current
        .iter()
        .filter(|selected| selected.batch_digest == original.batch_digest);
    let Some(expected) = selected.next() else {
        // No current replacement was selected. Inventory may separately choose
        // one provisional unbound target, without treating metadata as adoption.
        return Ok(None);
    };
    if selected.next().is_some() {
        return Err(error("ambiguous current retained replacement"));
    }
    native::catch_up::require_replacement(original, expected)?;
    let mut found = None;
    for (candidate, candidate_headers) in candidates {
        if candidate.attempt_id == expected.attempt_id
            || candidate.publication_ordinal == expected.publication_ordinal
        {
            if found.is_some() || candidate != expected || candidate_headers != headers {
                return Err(error(
                    "retained replacement input identity or headers differ",
                ));
            }
            found = Some(candidate);
        }
    }
    found
        .map(Some)
        .ok_or_else(|| error("current replacement original input missing"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr_model::{OwnerEpoch, Sequence};

    fn receipts() -> (ReceiptExpectation, ReceiptExpectation) {
        let original: ReceiptExpectation = serde_json::from_value(serde_json::json!({
            "dataset_id":"01".repeat(16),"schema_version":1,"topology_generation":"0",
            "batch_digest":"05".repeat(32),"record_count":3,
            "input_ranges":[{"shard_id":0,"owner_epoch":1,"first_sequence":1,"last_sequence":3}],
            "attempt_id":([8;16]),"publication_ordinal":3
        }))
        .unwrap();
        let mut replacement = original.clone();
        replacement.attempt_id = [9; 16];
        replacement.publication_ordinal = 4;
        (original, replacement)
    }

    fn header(first: u64, last: u64, digest: u8) -> PendingBatchHeader {
        let mut encoded = [digest; 42];
        encoded[..2].copy_from_slice(&[1, 0x21]);
        encoded[2..10].copy_from_slice(&last.to_be_bytes());
        PendingBatchHeader::decode(
            SourceRecordId {
                dataset_id: [1; 16].try_into().unwrap(),
                shard_id: 0,
                owner_epoch: OwnerEpoch::new(1).unwrap(),
                sequence: Sequence::new(first).unwrap(),
            },
            &encoded,
        )
        .unwrap()
    }

    #[test]
    fn current_replacement_requires_one_exact_later_retained_current_member() {
        let (original, replacement) = receipts();
        let headers = [header(1, 3, 7)];
        let current = [replacement.clone()];
        let candidates = [(&replacement, headers.as_slice())];
        assert_eq!(
            current_replacement(&original, &headers, Some(&current), candidates.into_iter())
                .unwrap(),
            Some(&replacement)
        );
        // A current original remains on its normal strict inspection path.
        assert!(
            current_replacement(
                &original,
                &headers,
                Some(std::slice::from_ref(&original)),
                candidates.into_iter()
            )
            .unwrap()
            .is_none()
        );
        // Native commit before frontier publication is deliberately not adopted.
        for current in [None, Some([].as_slice())] {
            assert!(
                current_replacement(&original, &headers, current, candidates.into_iter())
                    .unwrap()
                    .is_none()
            );
        }
        assert!(
            current_replacement(&original, &headers, Some(&current), std::iter::empty()).is_err()
        );
        assert!(
            current_replacement(
                &original,
                &headers,
                Some(&current),
                candidates.into_iter().chain(candidates)
            )
            .is_err()
        );
        let mut other = replacement.clone();
        other.attempt_id = [10; 16];
        other.publication_ordinal = 5;
        assert!(
            current_replacement(
                &original,
                &headers,
                Some(&[replacement.clone(), other]),
                candidates.into_iter()
            )
            .is_err()
        );
    }

    #[test]
    fn current_replacement_refuses_changed_headers_and_conflicting_input_identity() {
        let (original, replacement) = receipts();
        let headers = [header(1, 3, 7)];
        let current = [replacement.clone()];
        for changed in [
            vec![header(1, 3, 8)],
            vec![header(1, 1, 9), header(2, 3, 10)],
        ] {
            assert!(
                current_replacement(
                    &original,
                    &headers,
                    Some(&current),
                    std::iter::once((&replacement, changed.as_slice()))
                )
                .is_err()
            );
        }
        for field in 0..3 {
            let mut changed = replacement.clone();
            match field {
                0 => changed.record_count = 4,
                1 => changed.attempt_id = [10; 16],
                2 => changed.publication_ordinal = 5,
                _ => unreachable!(),
            }
            assert!(
                current_replacement(
                    &original,
                    &headers,
                    Some(&current),
                    std::iter::once((&changed, headers.as_slice()))
                )
                .is_err()
            );
        }
        let mut conflicting = original.clone();
        conflicting.batch_digest = bigbrotr_model::Digest256::from_bytes([6; 32]);
        assert!(
            current_replacement(
                &original,
                &headers,
                Some(&[conflicting]),
                std::iter::once((&replacement, headers.as_slice()))
            )
            .is_err()
        );
    }

    #[test]
    fn unbound_replacement_is_unique_provisional_and_never_overrides_current() {
        use native::catch_up::ReplacementMode::{CurrentBound, ProvisionalUnbound};
        let (original, replacement) = receipts();
        let headers = [header(1, 3, 7)];
        let candidates = [(&replacement, headers.as_slice())];
        for current in [None, Some([].as_slice())] {
            assert_eq!(
                retained_replacement(&original, &headers, current, candidates.into_iter()).unwrap(),
                Some((ProvisionalUnbound, &replacement))
            );
            assert!(
                retained_replacement(&original, &headers, current, std::iter::empty())
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(
            retained_replacement(
                &original,
                &headers,
                Some(std::slice::from_ref(&replacement)),
                candidates.into_iter(),
            )
            .unwrap(),
            Some((CurrentBound, &replacement))
        );
        assert!(
            retained_replacement(
                &original,
                &headers,
                Some(std::slice::from_ref(&original)),
                candidates.into_iter(),
            )
            .unwrap()
            .is_none()
        );
        let mut third = replacement.clone();
        third.attempt_id = [10; 16];
        third.publication_ordinal = 5;
        let mut unrelated = third.clone();
        unrelated.batch_digest = bigbrotr_model::Digest256::from_bytes([6; 32]);
        unrelated.input_ranges[0].first_sequence = 4;
        unrelated.input_ranges[0].last_sequence = 6;
        let unrelated_headers = [header(4, 6, 8)];
        assert_eq!(
            retained_replacement(
                &original,
                &headers,
                None,
                [
                    (&replacement, headers.as_slice()),
                    (&unrelated, unrelated_headers.as_slice())
                ]
                .into_iter(),
            )
            .unwrap(),
            Some((ProvisionalUnbound, &replacement))
        );
        // Multiple exact later candidates keep ordinary strict A observation;
        // no newest/first/native-hit selection is inferred from this metadata.
        for candidates in [
            [
                (&replacement, headers.as_slice()),
                (&third, headers.as_slice()),
            ],
            [
                (&third, headers.as_slice()),
                (&replacement, headers.as_slice()),
            ],
        ] {
            assert!(
                retained_replacement(&original, &headers, None, candidates.into_iter())
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn unbound_replacement_checks_all_semantics_headers_and_identity_conflicts() {
        let (original, replacement) = receipts();
        let headers = [header(1, 3, 7)];
        for field in 0..10 {
            let mut changed = replacement.clone();
            match field {
                0 => changed.dataset_id = [2; 16].try_into().unwrap(),
                1 => changed.schema_version = 2,
                2 => changed.topology_generation = bigbrotr_model::TopologyGeneration::new(1),
                3 => changed.batch_digest = bigbrotr_model::Digest256::from_bytes([6; 32]),
                4 => changed.record_count = 4,
                5 => {
                    changed.input_ranges[0].first_sequence = 2;
                    changed.input_ranges[0].last_sequence = 4;
                }
                6 => changed.attempt_id = original.attempt_id,
                7 => changed.publication_ordinal = original.publication_ordinal,
                8 => changed.publication_ordinal = 2,
                9 => changed.attempt_id = [0; 16],
                _ => unreachable!(),
            }
            assert!(
                retained_replacement(
                    &original,
                    &headers,
                    None,
                    std::iter::once((&changed, headers.as_slice())),
                )
                .is_err(),
                "field {field}"
            );
        }
        for changed in [
            vec![header(1, 3, 8)],
            vec![header(1, 1, 9), header(2, 3, 10)],
        ] {
            assert!(
                retained_replacement(
                    &original,
                    &headers,
                    None,
                    std::iter::once((&replacement, changed.as_slice())),
                )
                .is_err()
            );
        }
        let mut third = replacement.clone();
        third.attempt_id = [10; 16];
        third.publication_ordinal = 5;
        for field in 0..4 {
            let mut changed = third.clone();
            match field {
                0 => changed.record_count = 4,
                1 => changed.attempt_id = replacement.attempt_id,
                2 => changed.publication_ordinal = replacement.publication_ordinal,
                3 => changed.batch_digest = bigbrotr_model::Digest256::from_bytes([6; 32]),
                _ => unreachable!(),
            }
            // A bad later row cannot be ignored after finding a valid B,
            // including when B is already selected by the current frontier.
            for current in [None, Some(std::slice::from_ref(&replacement))] {
                assert!(
                    retained_replacement(
                        &original,
                        &headers,
                        current,
                        [
                            (&replacement, headers.as_slice()),
                            (&changed, headers.as_slice())
                        ]
                        .into_iter(),
                    )
                    .is_err(),
                    "field {field}"
                );
            }
        }
        assert!(
            retained_replacement(
                &original,
                &headers,
                None,
                [(&replacement, headers.as_slice()); 2].into_iter(),
            )
            .is_err()
        );
        let mut changed_current = original.clone();
        changed_current.batch_digest = bigbrotr_model::Digest256::from_bytes([6; 32]);
        assert!(
            retained_replacement(
                &original,
                &headers,
                Some(&[changed_current]),
                std::iter::once((&replacement, headers.as_slice())),
            )
            .is_err()
        );
    }
}
