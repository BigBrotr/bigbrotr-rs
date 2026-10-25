//! Admit the entire missing-batch plan before issuing the first new attempt.
use super::super::archive::native;
use super::inventory::Inventory;
use super::*;
use crate::interfaces::archive_preflight as preflight;
use bigbrotr::storage::{
    archive::RetainedInputs, postgres::archive_attempt::ArchiveAttemptRequest,
    rocks::store::PendingBatchHeader,
};
use bigbrotr_model::{ArchiveBatch, Sequence, SourceRecordId, TopologyGeneration};
use bigbrotr_protocol::{
    archive_cut::{CutBound, CutRead},
    archive_receipt::ReceiptExpectation,
};

pub(super) struct Missing {
    pub(super) header: PendingBatchHeader,
    pub(super) request: ArchiveAttemptRequest,
    pub(super) expectation: ReceiptExpectation,
    pub(super) input_permit: OwnedPermit,
}
pub(super) struct Plan {
    pub(super) missing: Vec<Missing>,
    pub(super) complete: CutRead,
    // Prospective receipts, plans and at most one temporary intermediate cut.
    _permit: OwnedPermit,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build(
    root: &Resolved,
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    proof: &VerifiedSuccessorHistory<'_>,
    coordinator: &LocalCoordinator,
    inventory: &Inventory,
    frontier: &RetainedArchiveFrontier,
    reader: &native::Reader,
) -> Result<Plan> {
    let current = frontier.bound_current_inputs().map_err(error)?;
    let permit = grant(budget, MIB, 0)?;
    let lineage = proof.history().lineage();
    let target = lineage
        .native_head()
        .manifest()
        .identity()
        .topology_generation;
    let mut complete = CutRead {
        dataset_id: coordinator.identity().dataset(),
        topology_generation: target,
        bounds: lineage
            .entries()
            .iter()
            .map(|entry| {
                let id = entry.manifest().identity();
                CutBound {
                    shard_id: id.shard_id,
                    owner_epoch: id.owner_epoch.get(),
                    complete_through: entry.source_through(),
                }
            })
            .collect(),
        receipts: inventory
            .attempts
            .iter()
            .filter(|a| a.present)
            .map(|a| a.inputs.expectation().clone())
            .collect(),
        // Conservative wire-size preflight only, never a requested native snapshot.
        snapshot_id: Some(i64::MAX as u64),
    };
    complete.bounds.sort_unstable_by_key(|bound| bound.stream());
    let facts = complete
        .bounds
        .iter()
        .try_fold(0_u64, |n, bound| n.checked_add(bound.complete_through))
        .ok_or_else(|| error("terminal source count overflow"))?;
    if facts > (MAX_ATTEMPTS * inventory::RECORDS) as u64 {
        return Err(error(
            "terminal cut exceeds total finite receipt membership",
        ));
    }
    let mut missing = Vec::new();
    missing
        .try_reserve_exact(MAX_ATTEMPTS - inventory.attempts.len())
        .map_err(error)?;
    // Source epoch precedes source sequence. Cumulative topology/source proof
    // supplies dependencies; publication ordinals are not scientific cursors.
    for entry in lineage.entries() {
        let id = entry.manifest().identity();
        let mut next = 1_u64;
        while next <= entry.source_through() {
            authority.refresh(owner).await?;
            let first = SourceRecordId {
                dataset_id: id.dataset_id,
                shard_id: id.shard_id,
                owner_epoch: id.owner_epoch,
                sequence: Sequence::new(next).map_err(error)?,
            };
            let pending = inventory::pending(budget, authority, proof, first)?;
            let header = PendingBatchHeader::from_batch(pending.batch());
            next = header
                .last()
                .sequence
                .get()
                .checked_add(1)
                .ok_or_else(|| error("source sequence exhausted"))?;
            let Some(topology) = missing_topology(
                inventory.attempts.iter().map(|a| {
                    (
                        a.present,
                        a.inputs.headers(),
                        a.inputs.request().topology_generation(),
                    )
                }),
                header,
                target,
            )?
            else {
                continue;
            };
            if !current.admits_new_attempts(missing.len() as u64 + 1) {
                return Err(error("issued plus planned attempts exceed finite profile"));
            }
            let archive = ArchiveBatch::new(1, topology, std::slice::from_ref(pending.batch()))
                .map_err(error)?;
            // Original IDs are chosen once during whole-job planning, then used
            // unchanged by persistence, issuance, native work and final cut.
            let fresh = || super::super::super::fresh_process_incarnation().map_err(error);
            let request = ArchiveAttemptRequest::new(
                *fresh()?.as_bytes(),
                coordinator.identity().catalog(),
                owner.identity().process_incarnation,
                fresh()?,
                current.issued_attempts() + missing.len() as u64,
                &archive,
            )
            .map_err(error)?;
            let expectation = ReceiptExpectation::from_archive(
                &archive,
                *request.attempt_id(),
                request.ordinal(),
            )
            .map_err(error)?;
            let input_permit = grant(budget, RetainedInputs::required_data_bytes(), 0)?;
            complete.receipts.push(expectation.clone());
            missing.push(Missing {
                header,
                request,
                expectation,
                input_permit,
            });
        }
    }
    complete
        .receipts
        .sort_unstable_by_key(|receipt| receipt.publication_ordinal);
    complete
        .validate(MAX_ATTEMPTS * inventory::RECORDS)
        .map_err(error)?;
    // Every future retained-input allocation now coexists. Admit the peak
    // pending + cut + one journal record, with the already retained metadata.
    let _pending = grant(
        budget,
        inventory::limits().required_data_bytes().map_err(error)?,
        0,
    )?;
    let _cut = grant(budget, MIB, 0)?;
    let _intent = grant(budget, MIB, 0)?;
    let mut limits = preflight::limits(root)?;
    limits.max_records = MAX_ATTEMPTS;
    limits.max_encoded_bytes = inventory::ENCODED;
    native::validate_cut(reader, coordinator.identity(), &complete, limits)?;
    // Validate every required zero-extension shape before any writer is issued.
    // Only one intermediate serialization/vector exists at a time.
    if let Some(current) = current.current_cut() {
        for entry in lineage
            .entries()
            .iter()
            .filter(|e| e.manifest().identity().topology_generation > current.topology_generation)
        {
            let step = bridge(
                proof,
                current,
                entry.manifest().identity().topology_generation,
                i64::MAX as u64,
            )?;
            native::validate_cut(reader, coordinator.identity(), &step, limits)?;
        }
    }
    Ok(Plan {
        missing,
        complete,
        _permit: permit,
    })
}

fn missing_topology<'a>(
    attempts: impl Iterator<Item = (bool, &'a [PendingBatchHeader], TopologyGeneration)>,
    header: PendingBatchHeader,
    target: TopologyGeneration,
) -> Result<Option<TopologyGeneration>> {
    let mut present = 0;
    let mut topology = None;
    let mut incompatible_absent = false;
    for (exists, headers, original) in attempts {
        if !headers.contains(&header) {
            continue;
        }
        if exists {
            present += 1;
        } else {
            incompatible_absent |=
                headers.len() != 1 || topology.is_some_and(|prior| prior != original);
            topology = Some(original);
        }
    }
    if present > 1 {
        return Err(error(
            "multiple present receipts own the same original batch",
        ));
    }
    if present == 1 {
        return Ok(None);
    }
    if incompatible_absent {
        return Err(error(
            "absent attempt grouping or topology exceeds single-batch replacement profile",
        ));
    }
    Ok(Some(topology.unwrap_or(target)))
}

/// The input current cut was explicitly loaded and bound. All added streams
/// come from the positive cumulative lineage; every previous bound is unchanged.
pub(super) fn bridge(
    proof: &VerifiedSuccessorHistory<'_>,
    current: &CutRead,
    topology: TopologyGeneration,
    snapshot: u64,
) -> Result<CutRead> {
    if topology <= current.topology_generation {
        return Err(error("topology bridge must advance"));
    }
    let mut read = current.clone();
    read.topology_generation = topology;
    read.snapshot_id = Some(snapshot);
    for entry in proof.history().lineage().entries().iter().filter(|e| {
        let generation = e.manifest().identity().topology_generation;
        generation > current.topology_generation && generation <= topology
    }) {
        let id = entry.manifest().identity();
        read.bounds.push(CutBound {
            shard_id: id.shard_id,
            owner_epoch: id.owner_epoch.get(),
            complete_through: 0,
        });
    }
    read.bounds.sort_unstable_by_key(|bound| bound.stream());
    read.validate(MAX_ATTEMPTS * inventory::RECORDS)
        .map_err(error)?;
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(sequence: u64) -> PendingBatchHeader {
        let mut encoded = [0; 42];
        encoded[..2].copy_from_slice(&[1, 0x21]);
        encoded[2..10].copy_from_slice(&sequence.to_be_bytes());
        encoded[10..].fill(7);
        PendingBatchHeader::decode(
            SourceRecordId {
                dataset_id: [1; 16].try_into().unwrap(),
                shard_id: 0,
                owner_epoch: OwnerEpoch::new(2).unwrap(),
                sequence: Sequence::new(sequence).unwrap(),
            },
            &encoded,
        )
        .unwrap()
    }

    #[test]
    fn replacement_preserves_original_topology_and_never_splits_absent_headers() {
        let first = header(1);
        let second = header(2);
        let one = [first];
        let two = [first, second];
        let old = TopologyGeneration::new(2);
        let target = TopologyGeneration::new(5);
        assert_eq!(
            missing_topology(std::iter::empty(), first, target).unwrap(),
            Some(target)
        );
        assert_eq!(
            missing_topology([(false, one.as_slice(), old)].into_iter(), first, target).unwrap(),
            Some(old)
        );
        assert!(
            missing_topology([(false, two.as_slice(), old)].into_iter(), first, target).is_err()
        );
        assert_eq!(
            missing_topology([(true, two.as_slice(), old)].into_iter(), first, target).unwrap(),
            None
        );
        assert!(
            missing_topology(
                [
                    (false, one.as_slice(), old),
                    (false, one.as_slice(), target)
                ]
                .into_iter(),
                first,
                target
            )
            .is_err()
        );
        assert!(
            missing_topology(
                [(true, one.as_slice(), old), (true, two.as_slice(), target)].into_iter(),
                first,
                target
            )
            .is_err()
        );
        // A later genuine complete replacement can resolve an older absent
        // grouping. No new dispatch or regrouping happens on this path.
        assert_eq!(
            missing_topology(
                [(false, two.as_slice(), old), (true, one.as_slice(), target)].into_iter(),
                first,
                target
            )
            .unwrap(),
            None
        );
    }
}
