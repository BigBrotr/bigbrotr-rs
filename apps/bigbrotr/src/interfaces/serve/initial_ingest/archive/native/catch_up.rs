//! Finite retained-attempt inspection. No old credential is relaunched and no
//! observation grants source protection. The parent owns whole-job admission,
//! exact PG persistence binding and every blocking job through actual join.
use super::*;
use bigbrotr::storage::archive::{ReceiptObservation, validate_membership_limits};
use bigbrotr_model::Digest256;
use bigbrotr_protocol::archive_membership::{MembershipObservation, MembershipRead};

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
    reader: &Reader,
    reader_custody: &ReaderCustody,
    coordinator_binding: Digest256,
    budgets: WorkerBudgets<'_>,
    deadline: Instant,
    cancel: &AtomicBool,
) -> Result<ReconciledAttempt> {
    let identity = reader_custody.identity();
    let request = receipt.request();
    let expected = inputs.expectation();
    if inputs.request() != request
        || inputs.coordinator_binding() != coordinator_binding
        || receipt.ordinal != request.ordinal()
        || request.dataset() != identity.dataset()
        || request.catalog() != identity.catalog()
        || expected.dataset_id != request.dataset()
        || expected.schema_version != 1
        || expected.record_count == 0
        || expected.record_count > RECORDS as u64
        || expected.topology_generation != request.topology_generation()
        || expected.batch_digest != request.batch_digest()
        || expected.attempt_id != *request.attempt_id()
        || expected.publication_ordinal != receipt.ordinal
    {
        return Err(error("retained attempt/input identity"));
    }
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
    // Validate both complete credential-bearing requests, and prepare both
    // worker/scratch identities, before the resolver can disable a writer.
    reader
        .route
        .borrowed()
        .validate_receipt(expected, identity, credential()?)
        .map_err(error)?;
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
                let observed = Preparation {
                    installation: &receipt_worker,
                    temp_root: &receipt_scratch,
                    limits,
                    deadline: deadline.into_std(),
                    cancelled: cancel,
                }
                .inspect_receipt_retained(
                    expected,
                    reader_custody,
                    &reader.route.borrowed(),
                    credential()?,
                    budgets,
                )
                .map_err(error)?;
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
