//! Finite caller composition: original outcomes, whole-job plan, joined effects.
use super::super::archive::native;
use super::*;
use bigbrotr::storage::{
    archive::{FrontierIntentScan, LocalAttempt, RetainedFrontierIntent},
    rocks::store::PendingBatchHeader,
};
use bigbrotr_model::ArchiveBatch;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    root: &Resolved,
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    recovery: &IngestRecovery,
    proof: &VerifiedSuccessorHistory<'_>,
    coordinator: &LocalCoordinator,
) -> Result<Value> {
    let mut inventory =
        inventory::load(root, budget, authority, owner, recovery, proof, coordinator).await?;
    let journal = resolve_intents(budget, authority, owner, proof, coordinator, &inventory).await?;
    let native_budget = native_budget(root)?;
    authority.refresh(owner).await?;
    let prepared = authority.local(|_| native::reader(root, budget))?;
    let deadline = Instant::now() + OPERATION;
    let cancel = authority.cancellation;
    let reader = authority
        .external(
            owner,
            deadline,
            native::catch_up::checked_reader(
                root,
                coordinator,
                prepared,
                deadline,
                &cancel.requested,
            ),
        )
        .await?;
    inventory
        .reconcile(
            root,
            budget,
            &native_budget,
            authority,
            owner,
            proof,
            coordinator,
            &reader,
        )
        .await?;
    let context = capture::Context {
        root,
        budget,
        native: &native_budget,
        reader: &reader,
        proof,
        coordinator,
    };
    let plan = plan::build(
        root,
        budget,
        authority,
        owner,
        proof,
        coordinator,
        &inventory,
        &reader,
    )
    .await?;
    let mut current_cut = inventory.current.current_cut().cloned();
    let mut current_receipt = inventory.current.current_receipt();
    let issued = inventory.current.issued_attempts();
    let planned = plan.missing.len();
    // Authenticate the actual native reader even with no old attempts. This
    // observation never invents an empty/virgin initialization or a snapshot ID.
    let initial = current_cut.clone().unwrap_or_else(|| {
        let mut empty = plan.complete.clone();
        empty.receipts.clear();
        for bound in &mut empty.bounds {
            bound.complete_through = 0;
        }
        empty
    });
    let observed = context
        .run(authority, owner, initial, capture::Action::Observe)
        .await?;
    if let Some(receipt) = current_receipt {
        require_current_counts(receipt.family_counts, observed.family_counts)?;
    }

    let bridge_count = current_cut.as_ref().map_or(0, |current| {
        proof
            .history()
            .lineage()
            .entries()
            .iter()
            .filter(|entry| {
                entry.manifest().identity().topology_generation > current.topology_generation
            })
            .count()
    });
    let same_shape = current_cut.as_ref().is_some_and(|current| {
        current.dataset_id == plan.complete.dataset_id
            && current.topology_generation == plan.complete.topology_generation
            && current.bounds == plan.complete.bounds
            && current.receipts == plan.complete.receipts
    });
    // An identical shape may still need a fresh snapshot revision. With no
    // journal capacity left, run only the exact-retry capture below; appending a
    // non-identical intent will refuse. New publication always reserves all slots.
    if !same_shape || planned != 0 {
        let frontiers = bridge_count
            .checked_add(1)
            .ok_or_else(|| error("frontier count overflow"))?;
        let entries = planned
            .checked_mul(2)
            .and_then(|n| n.checked_add(frontiers))
            .ok_or_else(|| error("state entry count overflow"))?;
        if frontiers > 256 - journal.len() || entries > journal.remaining_directory_entries() {
            return Err(error(
                "complete planned frontier/input/attempt journal capacity unavailable",
            ));
        }
    }
    let mut prepared = if planned > 0 {
        Some(publisher(&context, authority, owner).await?)
    } else {
        None
    };
    // With no new native writer, no Publisher retains the manager reservation.
    // Still admit manager + cut + original intent together before any bridge.
    let manager_preflight = if prepared.is_none() {
        Some(
            context
                .prepare_manager(authority, owner, Instant::now() + OPERATION)
                .await?,
        )
    } else {
        None
    };
    peak(root, budget, &native_budget)?;
    drop(manager_preflight);

    let (selected, absent): (Vec<_>, Vec<_>) = std::mem::take(&mut inventory.attempts)
        .into_iter()
        .partition(|a| a.present);
    inventory.attempts = absent; // Original failed input custody remains retained.
    let mut prefix = Vec::new();
    let mut additional = Vec::new();
    prefix.try_reserve_exact(MAX_ATTEMPTS).map_err(error)?;
    additional.try_reserve_exact(MAX_ATTEMPTS).map_err(error)?;
    for attempt in selected {
        if current_cut.as_ref().is_some_and(|cut| {
            cut.receipts
                .iter()
                .any(|r| r.attempt_id == *attempt.inputs.request().attempt_id())
        }) {
            prefix.push(attempt.inputs);
        } else {
            additional.push(attempt.inputs);
        }
    }
    for missing in plan.missing {
        authority.refresh(owner).await?;
        let pending = inventory::pending(budget, authority, proof, missing.header.first())?;
        if PendingBatchHeader::from_batch(pending.batch()) != missing.header {
            return Err(error("planned original pending batch changed"));
        }
        let archive = ArchiveBatch::new(
            1,
            missing.request.topology_generation(),
            std::slice::from_ref(pending.batch()),
        )
        .map_err(error)?;
        let retained = authority.local(|cancel| {
            proof.validate(cancel).map_err(error)?;
            coordinator
                .retain_inputs(&missing.request, &archive, missing.input_permit, cancel)
                .map_err(error)
        })?;
        if retained.expectation() != &missing.expectation {
            return Err(error("persisted input differs from whole-job plan"));
        }
        let publication = match prepared.take() {
            Some(value) => value,
            None => publisher(&context, authority, owner).await?,
        };
        authority.refresh(owner).await?;
        authority.local(|cancel| proof.validate(cancel).map_err(error))?;
        let receipt = authority
            .operation(owner.register_retained_archive_attempt(&retained, &Cell::new(false)))
            .await?;
        let local = authority.local(|_| {
            LocalAttempt::create(coordinator.state_root(), &receipt, coordinator.identity())
                .map_err(error)
        })?;
        let (app, native, reader, identity) = (
            budget.clone(),
            native_budget.clone(),
            reader.clone(),
            coordinator.identity().clone(),
        );
        let deadline = Instant::now() + OPERATION;
        authority
            .blocking(owner, deadline, move |cancel| {
                native::publish(
                    app,
                    native,
                    identity,
                    receipt,
                    local,
                    pending,
                    publication,
                    reader,
                    deadline,
                    cancel,
                )
            })
            .await?;
        authority.local(|cancel| proof.validate(cancel).map_err(error))?;
        additional.push(retained);
    }

    let mut created_frontiers = 0_u64;
    if let Some(mut current) = current_cut.take() {
        for entry in proof.history().lineage().entries() {
            let topology = entry.manifest().identity().topology_generation;
            if topology <= current.topology_generation {
                continue;
            }
            let mut next = plan::bridge(proof, &current, topology, i64::MAX as u64)?;
            let result = context
                .run(
                    authority,
                    owner,
                    next.clone(),
                    capture::Action::Publish {
                        inputs: &prefix,
                        current: current_receipt.map(|receipt| capture::Current {
                            receipt,
                            cut: &current,
                        }),
                    },
                )
                .await?;
            next.snapshot_id = Some(result.snapshot);
            current_receipt = Some(
                result
                    .receipt
                    .ok_or_else(|| error("missing bridge delivery"))?,
            );
            created_frontiers += u64::from(result.created);
            current = next;
        }
        current_cut = Some(current);
    }
    prefix.append(&mut additional);
    prefix.sort_unstable_by_key(|input| input.request().ordinal());
    let result = context
        .run(
            authority,
            owner,
            plan.complete,
            capture::Action::Publish {
                inputs: &prefix,
                current: current_receipt
                    .zip(current_cut.as_ref())
                    .map(|(receipt, cut)| capture::Current { receipt, cut }),
            },
        )
        .await?;
    let receipt = result
        .receipt
        .ok_or_else(|| error("missing final frontier delivery"))?;
    created_frontiers += u64::from(result.created);
    Ok(
        json!({"format":"bigbrotr-archive-catch-up/1", "dataset_id":coordinator.identity().dataset(),
        "catalog_id":coordinator.identity().catalog(), "terminal_epoch":proof.verified_through(),
        "issued_attempts_before":issued, "new_attempts":planned,
        "frontier_revision":receipt.revision,"snapshot_id":receipt.snapshot_id,
        "operation_id":receipt.operation_id,"request_digest":receipt.request_digest,
        "family_counts":receipt.family_counts,"new_frontier_revisions":created_frontiers}),
    )
}

fn native_budget(root: &Resolved) -> Result<ResourceBudget> {
    let n = |field: &str| root.number(&format!("runtime.processes.native.{field}"));
    Ok(ResourceBudget::new(
        ProcessEnvelope::new(
            n("memory_max_bytes"),
            n("baseline_bytes"),
            ResourceLimits::new(
                n("payload_bytes"),
                1,
                n("native_bytes"),
                n("completion_reserve_bytes"),
            ),
            WaiterLimits::new(0, 0),
        )
        .map_err(error)?,
    ))
}
fn peak(root: &Resolved, budget: &ResourceBudget, native: &ResourceBudget) -> Result<()> {
    let _pending = grant(
        budget,
        inventory::limits().required_data_bytes().map_err(error)?,
        0,
    )?;
    let _cut = grant(budget, MIB, 0)?;
    let _intent = grant(budget, RetainedFrontierIntent::required_data_bytes(), 0)?;
    let _native = native
        .try_acquire(
            ResourceRequest::data(0, 1, root.number("archive.worker_memory_bytes"))
                .map_err(error)?,
        )
        .map_err(error)?;
    let _control = budget
        .try_acquire(ResourceRequest::control(128 * 1024).map_err(error)?)
        .map_err(error)?;
    Ok(())
}
async fn publisher(
    context: &capture::Context<'_, '_>,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
) -> Result<native::Publisher> {
    let (root, app, identity) = (
        context.root.clone(),
        context.budget.clone(),
        context.coordinator.identity().clone(),
    );
    let deadline = Instant::now() + OPERATION;
    authority
        .blocking(owner, deadline, move |cancel| {
            native::publisher(&root, &app, &identity, deadline, cancel)
        })
        .await
}

async fn resolve_intents(
    budget: &ResourceBudget,
    authority: &mut Authority<'_>,
    owner: &mut ControlSession,
    proof: &VerifiedSuccessorHistory<'_>,
    coordinator: &LocalCoordinator,
    inventory: &inventory::Inventory,
) -> Result<FrontierIntentScan> {
    authority.refresh(owner).await?;
    let mut scan = authority.local(|cancel| {
        coordinator
            .frontier_intents(
                grant(budget, FrontierIntentScan::required_data_bytes(), 0)?,
                cancel,
            )
            .map_err(error)
    })?;
    while scan.observed_through() < scan.len() {
        authority.refresh(owner).await?;
        let intent = authority
            .local(|cancel| {
                scan.read_next(
                    grant(budget, RetainedFrontierIntent::required_data_bytes(), 0)?,
                    cancel,
                )
                .map_err(error)
            })?
            .ok_or_else(|| error("incomplete frontier intent scan"))?;
        let read = intent.read();
        let entries = proof.history().lineage().entries();
        if intent.expected_revision()
            > inventory
                .current
                .current_receipt()
                .map_or(0, |r| r.revision)
            || read.dataset_id != coordinator.identity().dataset()
            || !entries.iter().any(|entry| {
                entry.manifest().identity().topology_generation == read.topology_generation
            })
            || read.bounds.len()
                != entries
                    .iter()
                    .filter(|entry| {
                        entry.manifest().identity().topology_generation <= read.topology_generation
                    })
                    .count()
            || read.bounds.iter().any(|bound| {
                !entries.iter().any(|entry| {
                    let id = entry.manifest().identity();
                    id.topology_generation <= read.topology_generation
                        && id.shard_id == bound.shard_id
                        && id.owner_epoch.get() == bound.owner_epoch
                        && entry.source_through() >= bound.complete_through
                })
            })
            || read.receipts.iter().any(|receipt| {
                !inventory
                    .attempts
                    .iter()
                    .any(|a| a.inputs.expectation() == receipt)
            })
        {
            return Err(error(
                "frontier intent lies outside retained terminal history",
            ));
        }
        let receipt = authority
            .operation(
                owner
                    .read_archive_frontier(coordinator.identity().catalog(), intent.operation_id()),
            )
            .await?;
        if let Some(receipt) = receipt {
            if !intent.matches_receipt(&receipt)
                || inventory.current.current_receipt().is_none_or(|current| {
                    current.revision < receipt.revision
                        || current.revision == receipt.revision && current != receipt
                })
            {
                return Err(error(
                    "original frontier receipt differs from retained intent/current head",
                ));
            }
        }
    }
    Ok(scan)
}

// The snapshot can advance while this exact prefix remains unchanged. A changed
// family distribution is corrupt retained metadata, never a new-revision reason.
fn require_current_counts(retained: [u64; 5], observed: [u64; 5]) -> Result<()> {
    if retained != observed {
        return Err(error(
            "retained frontier family counts differ from actual original prefix",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changed_current_family_distribution_refuses_even_with_an_equal_total() {
        let original = [3, 2, 2, 4, 1];
        assert!(require_current_counts(original, original).is_ok());
        for changed in [[2, 3, 2, 4, 1], [3, 2, 1, 4, 2], [3, 2, 2, 5, 0]] {
            assert_eq!(original.iter().sum::<u64>(), changed.iter().sum::<u64>());
            assert!(require_current_counts(original, changed).is_err());
        }
        assert!(require_current_counts([0; 5], [0; 5]).is_ok());
        assert!(require_current_counts(original, [0; 5]).is_err());
    }
}
