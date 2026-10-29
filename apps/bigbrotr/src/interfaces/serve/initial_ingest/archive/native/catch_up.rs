//! Finite retained-attempt inspection. No old credential is relaunched and no
//! observation grants source protection. The parent owns whole-job admission,
//! exact PG persistence binding and every blocking job through actual join.
use super::*;
use bigbrotr::storage::archive::{ReceiptObservation, validate_membership_limits};
use bigbrotr_model::Digest256;
use bigbrotr_protocol::archive_membership::{MembershipObservation, MembershipRead};
use bigbrotr_protocol::archive_receipt::ReceiptExpectation;

pub(in crate::interfaces::serve::initial_ingest) mod group;

/// The native worker scans the entire receipt table to EOF, independently of
/// the number selected by a particular prefix or individual expected receipt.
pub(in crate::interfaces::serve::initial_ingest) const SCAN_LIMIT: usize = 128;

/// Guard and effective-reader privileges, pinned to the manager's actual route.
/// This does not authenticate the reader password or assert an empty catalog.
/// Before fresh publication, the caller must complete a fresh retained receipt/cut
/// inspection with this same credential; the zero-attempt path still needs a cut
/// at CatalogCapture's actual current snapshot (not necessarily snapshot one).
pub(in crate::interfaces::serve::initial_ingest) async fn checked_reader(
    root: &Resolved,
    coordinator: &LocalCoordinator,
    prepared: PreparedReader,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Result<Arc<Reader>> {
    let (mut manager, connection, pinned) = finite(deadline, cancel, async {
        transport::connect_pinned(prepared.connection, None)
            .await
            .map_err(error)
    })
    .await?;
    let connection = Box::pin(coordinator.retain_connection(connection).map_err(error)?);
    let result = drive(connection, async move {
        let result = async {
            finite(deadline, cancel, async {
                catalog_guard::inspect_reader(
                    &mut manager,
                    coordinator.identity(),
                    required(root, "postgres.catalog.read_user")?,
                )
                .await
                .map_err(error)
            })
            .await?;
            Reader::connect_input(
                root,
                pinned,
                prepared.password,
                prepared.tls,
                prepared.permit,
            )
            .map(Arc::new)
        }
        .await;
        drop(manager);
        result
    })
    .await;
    drop(prepared.grant);
    result
}

/// Evidence from a known catalog resolution followed by a new native process's
/// full receipt EOF scan. Present additionally requires actual file membership.
/// Neither case is an ACK or an authority to reuse the old attempt/credential.
#[derive(Debug)]
pub(in crate::interfaces::serve::initial_ingest) enum ReconciledAttempt {
    Absent {
        resolution: catalog_guard::Resolution,
    },
    Present {
        resolution: catalog_guard::Resolution,
        membership: MembershipObservation,
    },
}

/// A current member must exist; an unbound candidate is only a provisional read
/// target until its full strict scan succeeds with Present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::interfaces::serve::initial_ingest) enum ReplacementMode {
    CurrentBound,
    ProvisionalUnbound,
}

/// Called only inside a retained, joined blocking job. Its closure owns the
/// borrowed attempt, inputs and reader custody, even when the parent cancels.
/// The helper does not detach work, release recovered custody early, reconstruct
/// missing journals, infer rollback from an error, or adopt the old writer.
#[allow(clippy::too_many_arguments)]
pub(in crate::interfaces::serve::initial_ingest) fn reconcile_attempt(
    root: &Resolved,
    receipt: &ArchiveAttemptReceipt,
    attempt: &LocalAttempt,
    inputs: &RetainedInputs,
    replacement: Option<(ReplacementMode, &ReceiptExpectation)>,
    reader: &Reader,
    reader_custody: &ReaderCustody,
    coordinator_binding: Digest256,
    budgets: WorkerBudgets<'_>,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Result<ReconciledAttempt> {
    let identity = reader_custody.identity();
    let expected = inputs.expectation();
    require_retained_identity(receipt, inputs, identity, coordinator_binding)?;
    let read = if let Some((_, replacement)) = replacement {
        require_replacement(expected, replacement)?;
        replacement
    } else {
        expected
    };
    let mut limits = preflight::limits(root)?;
    limits.max_records = SCAN_LIMIT;
    limits.max_encoded_bytes = ENCODED;
    let max_facts = usize::try_from(expected.record_count).map_err(error)?;
    let membership = MembershipRead {
        receipt: expected.clone(),
        snapshot_id: None,
    };
    validate_membership_limits(limits, &membership, max_facts).map_err(error)?;
    let credential = || -> Result<CatalogReaderCredentials<'_>> {
        Ok(CatalogReaderCredentials {
            login: &reader.login,
            password: std::str::from_utf8(reader.password.as_bytes()).map_err(error)?,
        })
    };
    // Validate the original receipt too: a provisional candidate's complete
    // absence can require that strict read after the resolver disables a writer.
    // All requests and worker identities precede that lifetime resolution.
    reader
        .route
        .borrowed()
        .validate_receipt(expected, identity, credential()?)
        .map_err(error)?;
    if replacement.is_some() {
        reader
            .route
            .borrowed()
            .validate_receipt(read, identity, credential()?)
            .map_err(error)?;
    }
    reader
        .route
        .borrowed()
        .validate_membership(&membership, identity, credential()?)
        .map_err(error)?;
    let (receipt_worker, receipt_scratch, _) = preflight::worker(
        root,
        identity,
        None,
        "catch-up-receipt",
        limits,
        deadline.into_std(),
        cancel,
    )?;
    let (membership_worker, membership_scratch, _) = preflight::worker(
        root,
        identity,
        None,
        "catch-up-membership",
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
    tokio::runtime::Handle::current().block_on(async {
        let (mut manager, connection) =
            finite(deadline, cancel, connect_manager(prepared, reader)).await?;
        // The actual connection future retains this recovered attempt's lock;
        // a JoinHandle or socket observation alone cannot stand in for it.
        let connection = Box::pin(
            attempt
                .retain_catalog_connection(connection)
                .map_err(error)?,
        );
        let result = drive(connection, async {
            let result = async {
                let resolution = finite(deadline, cancel, async {
                    catalog_guard::resolve_after_local_custody(
                        &mut manager,
                        identity,
                        receipt,
                        attempt,
                        &Cell::new(false),
                    )
                    .await
                    .map_err(error)
                })
                .await?;
                let observed = original_observation(
                    expected,
                    replacement,
                    deadline,
                    cancel,
                    |read, deadline, cancel| {
                        Preparation {
                            installation: &receipt_worker,
                            temp_root: &receipt_scratch,
                            limits,
                            deadline: deadline.into_std(),
                            cancelled: cancel,
                        }
                        .inspect_receipt_retained(
                            read,
                            reader_custody,
                            &reader.route.borrowed(),
                            credential()?,
                            budgets,
                        )
                        .map_err(error)
                    },
                )?;
                let membership = if require_membership(resolution, observed)? {
                    Some(
                        Preparation {
                            installation: &membership_worker,
                            temp_root: &membership_scratch,
                            limits,
                            deadline: deadline.into_std(),
                            cancelled: cancel,
                        }
                        .inspect_membership_retained(
                            &membership,
                            reader_custody,
                            &reader.route.borrowed(),
                            credential()?,
                            budgets,
                            max_facts,
                        )
                        .map_err(error)?,
                    )
                } else {
                    None
                };
                finish_observation(resolution, observed, membership)
            }
            .await;
            drop(manager);
            result
        })
        .await;
        drop(grant);
        result
    })
}

/// Check every actual issued request/input binding before selecting a later
/// expectation. Historical process incarnations belong to their own immutable
/// requests; only the persistent coordinator binding is common across attempts.
pub(in crate::interfaces::serve::initial_ingest) fn require_retained_identity(
    receipt: &ArchiveAttemptReceipt,
    inputs: &RetainedInputs,
    identity: &CatalogIdentity,
    coordinator_binding: Digest256,
) -> Result<()> {
    let request = receipt.request();
    let expected = inputs.expectation();
    expected.validate().map_err(error)?;
    if inputs.request() != request
        || inputs.coordinator_binding() != coordinator_binding
        || receipt.ordinal != request.ordinal()
        || request.dataset() != identity.dataset()
        || request.catalog() != identity.catalog()
        || expected.dataset_id != request.dataset()
        || expected.record_count > RECORDS as u64
        || inputs.headers().len() > RECORDS
        || expected.topology_generation != request.topology_generation()
        || expected.batch_digest != request.batch_digest()
        || expected.attempt_id != *request.attempt_id()
        || expected.publication_ordinal != receipt.ordinal
    {
        return Err(error("retained attempt/input identity"));
    }
    Ok(())
}

/// Same complete batch under a distinct, strictly later attempt identity.
/// Selection mode and original headers are checked by Inventory.
pub(in crate::interfaces::serve::initial_ingest) fn require_replacement(
    original: &ReceiptExpectation,
    replacement: &ReceiptExpectation,
) -> Result<()> {
    original.validate().map_err(error)?;
    replacement.validate().map_err(error)?;
    if original.dataset_id != replacement.dataset_id
        || original.schema_version != replacement.schema_version
        || original.topology_generation != replacement.topology_generation
        || original.batch_digest != replacement.batch_digest
        || original.record_count != replacement.record_count
        || original.input_ranges != replacement.input_ranges
        || original.attempt_id == replacement.attempt_id
        || original.publication_ordinal >= replacement.publication_ordinal
    {
        return Err(error("invalid retained replacement expectation"));
    }
    Ok(())
}

fn original_observation(
    original: &ReceiptExpectation,
    replacement: Option<(ReplacementMode, &ReceiptExpectation)>,
    deadline: Instant,
    cancel: &AtomicBool,
    mut inspect: impl FnMut(&ReceiptExpectation, Instant, &AtomicBool) -> Result<ReceiptObservation>,
) -> Result<ReceiptObservation> {
    let mut observe = |expected: &ReceiptExpectation| {
        let check = || {
            if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
                Err(error("retained receipt observation cancelled or expired"))
            } else {
                Ok(())
            }
        };
        check()?;
        let observed = inspect(expected, deadline, cancel)?;
        check()?;
        Ok(observed)
    };
    if let Some((mode, expected)) = replacement {
        // Each call returns only after the actual worker is joined. A successful
        // full-EOF scan also excludes original/third same-digest collisions.
        match observe(expected)? {
            ReceiptObservation::Present => return Ok(ReceiptObservation::Absent),
            ReceiptObservation::Absent if mode == ReplacementMode::CurrentBound => {
                return Err(error("current retained replacement receipt is absent"));
            }
            ReceiptObservation::Absent => {}
        }
    }
    // Only known complete absence of a provisional unbound candidate reaches
    // this original strict read, under the same admitted job, custody, grants,
    // absolute deadline and cancellation. Errors never select this fallback.
    observe(original)
}

fn require_membership(
    resolution: catalog_guard::Resolution,
    observed: ReceiptObservation,
) -> Result<bool> {
    match (resolution, observed) {
        (catalog_guard::Resolution::NotImported, ReceiptObservation::Present) => {
            Err(error("receipt for non-imported attempt"))
        }
        (_, ReceiptObservation::Absent) => Ok(false),
        (catalog_guard::Resolution::Sealed { .. }, ReceiptObservation::Present) => Ok(true),
    }
}
fn finish_observation(
    resolution: catalog_guard::Resolution,
    observed: ReceiptObservation,
    membership: Option<MembershipObservation>,
) -> Result<ReconciledAttempt> {
    match (require_membership(resolution, observed)?, membership) {
        (false, None) => Ok(ReconciledAttempt::Absent { resolution }),
        (true, Some(membership)) => Ok(ReconciledAttempt::Present {
            resolution,
            membership,
        }),
        _ => Err(error("receipt/membership observation incomplete")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr::storage::archive::ReceiptError;

    fn expected() -> ReceiptExpectation {
        serde_json::from_value(serde_json::json!({
            "dataset_id":"01".repeat(16),"schema_version":1,"topology_generation":"0",
            "batch_digest":"05".repeat(32),"record_count":3,
            "input_ranges":[{"shard_id":0,"owner_epoch":1,"first_sequence":1,"last_sequence":3}],
            "attempt_id":([8;16]),"publication_ordinal":3
        }))
        .unwrap()
    }

    #[test]
    fn retained_replacement_preserves_every_semantic_field_and_later_identity() {
        let original = expected();
        let mut replacement = original.clone();
        replacement.attempt_id = [9; 16];
        replacement.publication_ordinal = 4;
        require_replacement(&original, &replacement).unwrap();
        for field in 0..10 {
            let mut invalid = replacement.clone();
            match field {
                0 => invalid.dataset_id = [2; 16].try_into().unwrap(),
                1 => invalid.schema_version = 2,
                2 => invalid.topology_generation = bigbrotr_model::TopologyGeneration::new(1),
                3 => invalid.batch_digest = Digest256::from_bytes([6; 32]),
                4 => invalid.record_count = 4,
                5 => {
                    invalid.input_ranges[0].first_sequence = 2;
                    invalid.input_ranges[0].last_sequence = 4;
                }
                6 => invalid.attempt_id = original.attempt_id,
                7 => invalid.publication_ordinal = original.publication_ordinal,
                8 => invalid.publication_ordinal = 2,
                9 => invalid.attempt_id = [0; 16],
                _ => unreachable!(),
            }
            assert!(require_replacement(&original, &invalid).is_err(), "{field}");
        }
    }

    #[test]
    fn retained_replacement_requires_present_without_hiding_unknown_or_absent() {
        let original = expected();
        let mut replacement = original.clone();
        replacement.attempt_id = [9; 16];
        replacement.publication_ordinal = 4;
        let deadline = Instant::now() + Duration::from_secs(30);
        let cancel = AtomicBool::new(false);
        for observed in [ReceiptObservation::Present, ReceiptObservation::Absent] {
            assert_eq!(
                original_observation(&original, None, deadline, &cancel, |read, _, _| {
                    assert_eq!(read, &original);
                    Ok(observed)
                })
                .unwrap(),
                observed
            );
        }
        for mode in [
            ReplacementMode::CurrentBound,
            ReplacementMode::ProvisionalUnbound,
        ] {
            let mut calls = 0;
            assert_eq!(
                original_observation(
                    &original,
                    Some((mode, &replacement)),
                    deadline,
                    &cancel,
                    |read, _, _| {
                        calls += 1;
                        assert_eq!(read, &replacement);
                        Ok(ReceiptObservation::Present)
                    },
                )
                .unwrap(),
                ReceiptObservation::Absent
            );
            assert_eq!(calls, 1);
        }
        let mut calls = 0;
        assert!(
            original_observation(
                &original,
                Some((ReplacementMode::CurrentBound, &replacement)),
                deadline,
                &cancel,
                |read, _, _| {
                    calls += 1;
                    assert_eq!(read, &replacement);
                    Ok(ReceiptObservation::Absent)
                },
            )
            .is_err()
        );
        assert_eq!(calls, 1);
        for error in [
            ReceiptError::ObservationUnknown,
            ReceiptError::TerminationUnknown,
            ReceiptError::ResourceLimit,
            ReceiptError::InvalidInput,
        ] {
            for mode in [
                None,
                Some(ReplacementMode::CurrentBound),
                Some(ReplacementMode::ProvisionalUnbound),
            ] {
                let mut calls = 0;
                assert!(
                    original_observation(
                        &original,
                        mode.map(|mode| (mode, &replacement)),
                        deadline,
                        &cancel,
                        |_, _, _| {
                            calls += 1;
                            Err(super::error(error))
                        },
                    )
                    .is_err()
                );
                assert_eq!(calls, 1);
            }
        }
        for resolution in [
            catalog_guard::Resolution::NotImported,
            catalog_guard::Resolution::Sealed {
                disabled_current: false,
            },
        ] {
            let observed = original_observation(
                &original,
                Some((ReplacementMode::ProvisionalUnbound, &replacement)),
                deadline,
                &cancel,
                |_, _, _| Ok(ReceiptObservation::Present),
            )
            .unwrap();
            assert!(matches!(
                finish_observation(resolution, observed, None),
                Ok(ReconciledAttempt::Absent { .. })
            ));
            // The later replacement still takes its ordinary Present/membership path.
            let observed = original_observation(&original, None, deadline, &cancel, |_, _, _| {
                Ok(ReceiptObservation::Present)
            })
            .unwrap();
            assert!(finish_observation(resolution, observed, None).is_err());
        }
    }

    #[test]
    fn provisional_absence_observes_original_under_the_same_deadline_and_cancel() {
        let original = expected();
        let mut replacement = original.clone();
        replacement.attempt_id = [9; 16];
        replacement.publication_ordinal = 4;
        let deadline = Instant::now() + Duration::from_secs(30);
        for observed in [ReceiptObservation::Absent, ReceiptObservation::Present] {
            let cancel = AtomicBool::new(false);
            let mut calls = Vec::new();
            assert_eq!(
                original_observation(
                    &original,
                    Some((ReplacementMode::ProvisionalUnbound, &replacement)),
                    deadline,
                    &cancel,
                    |read, actual_deadline, actual_cancel| {
                        assert_eq!(actual_deadline, deadline);
                        assert!(std::ptr::eq(actual_cancel, &cancel));
                        calls.push(read.attempt_id);
                        Ok(if read == &replacement {
                            ReceiptObservation::Absent
                        } else {
                            observed
                        })
                    },
                )
                .unwrap(),
                observed
            );
            assert_eq!(calls, [replacement.attempt_id, original.attempt_id]);
        }
        // The original strict scan's unknown/collision cannot become absence.
        for failure in [
            ReceiptError::ObservationUnknown,
            ReceiptError::TerminationUnknown,
            ReceiptError::ResourceLimit,
        ] {
            let cancel = AtomicBool::new(false);
            let mut calls = 0;
            assert!(
                original_observation(
                    &original,
                    Some((ReplacementMode::ProvisionalUnbound, &replacement)),
                    deadline,
                    &cancel,
                    |read, _, _| {
                        calls += 1;
                        if read == &replacement {
                            Ok(ReceiptObservation::Absent)
                        } else {
                            Err(error(failure))
                        }
                    },
                )
                .is_err()
            );
            assert_eq!(calls, 2);
        }
        let cancel = AtomicBool::new(false);
        let mut calls = 0;
        assert!(
            original_observation(
                &original,
                Some((ReplacementMode::ProvisionalUnbound, &replacement)),
                deadline,
                &cancel,
                |_, _, cancel| {
                    calls += 1;
                    cancel.store(true, Ordering::Release);
                    Ok(ReceiptObservation::Absent)
                },
            )
            .is_err()
        );
        assert_eq!(calls, 1);
        for (cancelled, expired) in [(true, false), (false, true)] {
            let cancel = AtomicBool::new(cancelled);
            let mut called = false;
            assert!(
                original_observation(
                    &original,
                    Some((ReplacementMode::ProvisionalUnbound, &replacement)),
                    if expired { Instant::now() } else { deadline },
                    &cancel,
                    |_, _, _| {
                        called = true;
                        Ok(ReceiptObservation::Absent)
                    },
                )
                .is_err()
            );
            assert!(!called);
        }
    }

    #[test]
    fn receipt_presence_never_substitutes_for_import_or_membership() {
        use catalog_guard::Resolution::{NotImported, Sealed};
        for resolution in [
            NotImported,
            Sealed {
                disabled_current: false,
            },
            Sealed {
                disabled_current: true,
            },
        ] {
            assert!(
                matches!(finish_observation(resolution, ReceiptObservation::Absent, None), Ok(ReconciledAttempt::Absent { resolution: actual }) if actual == resolution)
            );
            assert!(finish_observation(resolution, ReceiptObservation::Present, None).is_err());
        }
        let membership = MembershipObservation {
            format: bigbrotr_protocol::archive_membership::FORMAT.into(),
            snapshot_id: 2,
            batch_digest: Digest256::from_bytes([5; 32]),
            attempt_id: [9; 16],
            publication_ordinal: 1,
            family_counts: [1, 0, 0, 0, 0],
            files: 2,
        };
        assert!(
            finish_observation(
                NotImported,
                ReceiptObservation::Present,
                Some(membership.clone())
            )
            .is_err()
        );
        assert!(
            finish_observation(
                Sealed {
                    disabled_current: true
                },
                ReceiptObservation::Absent,
                Some(membership.clone())
            )
            .is_err()
        );
        assert!(matches!(
            finish_observation(
                Sealed {
                    disabled_current: false
                },
                ReceiptObservation::Present,
                Some(membership)
            ),
            Ok(ReconciledAttempt::Present {
                resolution: Sealed {
                    disabled_current: false
                },
                ..
            })
        ));
        assert!(require_membership(NotImported, ReceiptObservation::Present).is_err());
        for disabled_current in [false, true] {
            assert!(
                require_membership(Sealed { disabled_current }, ReceiptObservation::Present)
                    .unwrap()
            );
        }
    }
}
