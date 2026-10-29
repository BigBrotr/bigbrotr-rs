//! Joined reconciliation of every lifetime for one complete alternative group.
use super::*;
use bigbrotr::runtime::{ReservedBudget, ResourceLimits};
use bigbrotr_protocol::archive_receipt::{
    MAX_INPUT_BYTES, ReceiptRange,
    group::{GroupRead, Member},
};
use std::{fs::File, mem::size_of, path::Path};

/// Charged by Inventory's existing metadata permit before any lifetime effect.
/// LocalAttempt clones the catalog root; its bounded read/manifest buffers are
/// transient. Each erased driver wrapper stores one Driver and one lock File.
/// Fixed metadata also covers the optional current identity across the selector,
/// joined job, validation and observation stages.
pub(in crate::interfaces::serve::initial_ingest) fn metadata_bytes(
    count: usize,
    root_bytes: usize,
) -> Result<usize> {
    if count == 0 || count > SCAN_LIMIT {
        return Err(error("group metadata count"));
    }
    let per = size_of::<LocalAttempt>()
        .checked_add(root_bytes)
        .and_then(|n| n.checked_add(size_of::<Member>()))
        .and_then(|n| n.checked_add(size_of::<catalog_guard::Resolution>()))
        .and_then(|n| n.checked_add(size_of::<(&ArchiveAttemptReceipt, &RetainedInputs)>()))
        .and_then(|n| n.checked_add(size_of::<(transport::Driver, File)>()))
        .and_then(|n| n.checked_add(size_of::<bool>()))
        .ok_or_else(|| error("group metadata overflow"))?;
    per.checked_mul(count)
        .and_then(|n| n.checked_add(4 * MAX_INPUT_BYTES))
        .and_then(|n| n.checked_add(RECORDS * size_of::<ReceiptRange>()))
        .and_then(|n| n.checked_add(4 * size_of::<Option<Member>>()))
        .ok_or_else(|| error("group metadata overflow"))
}

fn active(deadline: Instant, cancel: &AtomicBool) -> Result<()> {
    if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
        Err(error("retained group cancelled or expired"))
    } else {
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::interfaces::serve::initial_ingest) fn reconcile(
    root: &Resolved,
    state_root: &Path,
    attempts: &[(&ArchiveAttemptReceipt, &RetainedInputs)],
    required_current: Option<Member>,
    reader: &Reader,
    custody: &ReaderCustody,
    binding: Digest256,
    budgets: WorkerBudgets<'_>,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Result<Vec<bool>> {
    active(deadline, cancel)?;
    let identity = custody.identity();
    let (_, first) = attempts
        .first()
        .ok_or_else(|| error("empty retained group"))?;
    let mut members = Vec::new();
    let mut locals = Vec::new();
    let mut resolutions = Vec::new();
    let mut present = Vec::new();
    if attempts.len() > SCAN_LIMIT {
        return Err(error("retained group limit"));
    }
    members.try_reserve_exact(attempts.len()).map_err(error)?;
    locals.try_reserve_exact(attempts.len()).map_err(error)?;
    resolutions
        .try_reserve_exact(attempts.len())
        .map_err(error)?;
    present.try_reserve_exact(attempts.len()).map_err(error)?;
    present.resize(attempts.len(), false);
    let mut limits = preflight::limits(root)?;
    limits.max_records = SCAN_LIMIT;
    limits.max_encoded_bytes = ENCODED;
    let credential = || -> Result<CatalogReaderCredentials<'_>> {
        Ok(CatalogReaderCredentials {
            login: &reader.login,
            password: std::str::from_utf8(reader.password.as_bytes()).map_err(error)?,
        })
    };
    // Allocate one reusable membership request before any mutation. Its ranges
    // are common to every validated member; only the compact identity changes.
    let mut membership = MembershipRead {
        receipt: first.expectation().clone(),
        snapshot_id: None,
    };
    for (index, (receipt, inputs)) in attempts.iter().enumerate() {
        active(deadline, cancel)?;
        require_retained_identity(receipt, inputs, identity, binding)?;
        if index != 0 {
            require_replacement(first.expectation(), inputs.expectation())?;
        }
        if inputs.headers() != first.headers() {
            return Err(error("group original headers differ"));
        }
        members.push(Member::from_receipt(inputs.expectation()));
        reader
            .route
            .borrowed()
            .validate_receipt(inputs.expectation(), identity, credential()?)
            .map_err(error)?;
        membership.receipt.attempt_id = inputs.expectation().attempt_id;
        membership.receipt.publication_ordinal = inputs.expectation().publication_ordinal;
        validate_membership_limits(
            limits,
            &membership,
            inputs.expectation().record_count as usize,
        )
        .map_err(error)?;
        reader
            .route
            .borrowed()
            .validate_membership(&membership, identity, credential()?)
            .map_err(error)?;
    }
    let read = GroupRead {
        common: first.expectation(),
        members: &members,
    };
    read.validate().map_err(error)?;
    let required_current = required_current
        .map(|member| {
            read.member_index(member)
                .ok_or_else(|| error("required current retained member missing"))
        })
        .transpose()?;
    reader
        .route
        .borrowed()
        .validate_receipt_group(read, identity, credential()?)
        .map_err(error)?;
    bigbrotr::storage::archive::validate_receipt_limits(limits).map_err(error)?;
    let (group_worker, group_scratch, _) = preflight::worker(
        root,
        identity,
        None,
        "catch-up-group-receipt",
        limits,
        deadline.into_std(),
        cancel,
    )?;
    let (membership_worker, membership_scratch, _) = preflight::worker(
        root,
        identity,
        None,
        "catch-up-group-membership",
        limits,
        deadline.into_std(),
        cancel,
    )?;
    let (prepared, grant) = postgres_command::prepare_catalog_as(
        root,
        required(root, "postgres.catalog.user")?,
        required(root, "postgres.catalog.password_ref")?,
        budgets.application,
    )?;
    // Fund the same serial child lane through both actual reap boundaries. The
    // existing reservation retains escaped process grants on unknown termination.
    let (reader_app, reader_native) = reserve_readers(budgets, limits)?;
    let reader_budgets = WorkerBudgets {
        application: reader_app.budget(),
        native: reader_native.budget(),
    };
    // Every actual lock is acquired before the first catalog resolution. Missing
    // or occupied custody cannot silently remove a member from this request.
    for (receipt, _) in attempts {
        active(deadline, cancel)?;
        locals.push(LocalAttempt::recover(state_root, receipt, identity).map_err(error)?);
    }
    tokio::runtime::Handle::current().block_on(async {
        let (mut manager, mut connection) =
            finite(deadline, cancel, connect_manager(prepared, reader)).await?;
        // Each wrapper retains the SAME lock open-file-description. Erasure
        // permits the existing wrappers to nest without a new custody carrier.
        // No query or polling of the actual driver precedes all these wrappers.
        for local in &locals {
            connection = Box::pin(local.retain_catalog_connection(connection).map_err(error)?);
        }
        let result = drive(connection, async {
            let result = async {
                resolve_members(
                    &mut resolutions,
                    attempts.len(),
                    deadline,
                    cancel,
                    async |index, deadline, cancel| {
                        finite(deadline, cancel, async {
                            catalog_guard::resolve_after_local_custody(
                                &mut manager,
                                identity,
                                attempts[index].0,
                                &locals[index],
                                &Cell::new(false),
                            )
                            .await
                            .map_err(error)
                        })
                        .await
                    },
                )
                .await?;
                observe_resolved(
                    &resolutions,
                    &mut present,
                    required_current,
                    deadline,
                    cancel,
                    |deadline, cancel| {
                        Preparation {
                            installation: &group_worker,
                            temp_root: &group_scratch,
                            limits,
                            deadline: deadline.into_std(),
                            cancelled: cancel,
                        }
                        .inspect_receipt_group_retained(
                            read,
                            custody,
                            &reader.route.borrowed(),
                            credential()?,
                            reader_budgets,
                        )
                        .map_err(error)
                    },
                    |index, deadline, cancel| {
                        membership.receipt.attempt_id = members[index].attempt_id;
                        membership.receipt.publication_ordinal = members[index].publication_ordinal;
                        let observed = Preparation {
                            installation: &membership_worker,
                            temp_root: &membership_scratch,
                            limits,
                            deadline: deadline.into_std(),
                            cancelled: cancel,
                        }
                        .inspect_membership_retained(
                            &membership,
                            custody,
                            &reader.route.borrowed(),
                            credential()?,
                            reader_budgets,
                            membership.receipt.record_count as usize,
                        )
                        .map_err(error)?;
                        observed.validate(&membership).map_err(error)
                    },
                )?;
                Ok(())
            }
            .await;
            drop(manager);
            result
        })
        .await;
        drop(grant);
        result
    })?;
    active(deadline, cancel)?;
    // No partial status vector escapes before membership and manager shutdown.
    Ok(present)
}

/// The vector is allocated before custody recovery. Every known canonical
/// outcome must finish under the same operation before observation is allowed.
async fn resolve_members(
    resolutions: &mut Vec<catalog_guard::Resolution>,
    count: usize,
    deadline: Instant,
    cancel: &AtomicBool,
    mut resolve: impl std::ops::AsyncFnMut(
        usize,
        Instant,
        &AtomicBool,
    ) -> Result<catalog_guard::Resolution>,
) -> Result<()> {
    active(deadline, cancel)?;
    if count == 0 || count > SCAN_LIMIT || !resolutions.is_empty() || resolutions.capacity() < count
    {
        return Err(error("retained group resolution allocation"));
    }
    for index in 0..count {
        active(deadline, cancel)?;
        let known = resolve(index, deadline, cancel).await?;
        active(deadline, cancel)?;
        resolutions.push(known);
    }
    Ok(())
}

/// No native observation can start with an incomplete resolution vector. Stage
/// callbacks return only after actual process/output join; statuses change once,
/// after fresh membership and both delivery checks under the original deadline.
pub(in crate::interfaces::serve::initial_ingest) fn observe_resolved(
    resolutions: &[catalog_guard::Resolution],
    statuses: &mut [bool],
    required_current: Option<usize>,
    deadline: Instant,
    cancel: &AtomicBool,
    observe: impl FnOnce(Instant, &AtomicBool) -> Result<Option<usize>>,
    membership: impl FnOnce(usize, Instant, &AtomicBool) -> Result<()>,
) -> Result<()> {
    active(deadline, cancel)?;
    if statuses.is_empty()
        || statuses.len() > SCAN_LIMIT
        || resolutions.len() != statuses.len()
        || statuses.iter().any(|present| *present)
        || required_current.is_some_and(|index| index >= statuses.len())
    {
        return Err(error("incomplete retained group resolutions"));
    }
    let selected = observe(deadline, cancel)?;
    active(deadline, cancel)?;
    if required_current.is_some() && selected != required_current {
        return Err(error(
            "fresh retained group differs from required current member",
        ));
    }
    if let Some(index) = require_selected(resolutions, selected)? {
        membership(index, deadline, cancel)?;
        active(deadline, cancel)?;
    }
    active(deadline, cancel)?;
    if let Some(index) = selected {
        statuses[index] = true;
    }
    Ok(())
}

fn reserve_readers(
    budgets: WorkerBudgets<'_>,
    limits: PreparationLimits,
) -> Result<(ReservedBudget, ReservedBudget)> {
    let bytes = limits
        .memory_bytes
        .checked_add(limits.native_reserve_bytes)
        .ok_or_else(|| error("retained group native allowance overflow"))?;
    let native = budgets
        .native
        .try_reserve(ResourceLimits::new(0, 1, bytes, 0))
        .map_err(error)?;
    let application = budgets
        .application
        .try_reserve(ResourceLimits::new(0, 1, 0, 128 * 1024))
        .map_err(error)?;
    Ok((application, native))
}

fn require_selected(
    resolutions: &[catalog_guard::Resolution],
    selected: Option<usize>,
) -> Result<Option<usize>> {
    if let Some(index) = selected
        && !matches!(
            resolutions.get(index),
            Some(catalog_guard::Resolution::Sealed { .. })
        )
    {
        return Err(error("group present member lacks its own canonical seal"));
    }
    Ok(selected)
}

#[cfg(test)]
mod tests;
