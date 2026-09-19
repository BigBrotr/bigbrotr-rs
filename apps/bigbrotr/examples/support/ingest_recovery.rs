//! Finite consumer of a retained native outcome after a failed original PG copy.
use super::*;
use bigbrotr::{
    storage::postgres::{
        control_owner::{ProducerResolveError, ProducerResolveRequest},
        producer_outcomes::OutcomeError,
        producer_requests::ReservationError,
    },
    storage::rocks::store::{
        AdmissionError, AdmissionLimits, IngestRecovery, ProducerRecoveryError,
        ProducerRecoveryLimits, RequestInputError, ResolvedProducerRequest,
    },
};
use bigbrotr_model::{
    ProducerEpoch, ProducerRequestId, RequestSequence,
    codec::{CodecError, FactDecodeLimits, request::RequestDecodeLimits},
};

#[allow(
    clippy::too_many_arguments,
    reason = "finite fixture owns actual lab connections and native exclusion"
)]
pub async fn run(
    direct_config: Config,
    mut current: ControlSession,
    mut driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    recovery: IngestRecovery,
    observer: &tokio_postgres::Client,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
) {
    let before = crate::ingest_snapshot::state(observer, path).await;
    emit(serde_json::json!({"event":"recovery-before", "state":before}));
    let expected = match mode {
        "event-recovery-pg-only" => Some(ProducerResolveError::Native(
            ProducerRecoveryError::ProducerOpen,
        )),
        "event-recovery-input-size" => Some(ProducerResolveError::Outcome(OutcomeError::Conflict)),
        "event-recovery-input-digest" => Some(ProducerResolveError::Native(
            ProducerRecoveryError::Input(RequestInputError::Codec(CodecError::DigestMismatch)),
        )),
        "event-recovery-input-manifest" => Some(ProducerResolveError::Reservation(
            ReservationError::Conflict,
        )),
        "event-recovery-resource" => Some(ProducerResolveError::Native(
            ProducerRecoveryError::Input(RequestInputError::Codec(CodecError::InputLimit)),
        )),
        "event-recovery-cancelled" => Some(ProducerResolveError::Native(
            ProducerRecoveryError::Native(AdmissionError::AuthorityUnavailable),
        )),
        "event-recovery-policy" => Some(ProducerResolveError::Control(OwnerError::InvalidPolicy)),
        "event-recovery-expired" => Some(ProducerResolveError::Control(OwnerError::Unavailable)),
        "event-recovery-rollback" => Some(ProducerResolveError::Outcome(OutcomeError::Backend)),
        "event-recovery-late-sql" => Some(ProducerResolveError::DeliveryUnknown),
        "event-recovery-commit-lost" => Some(ProducerResolveError::CommitUnknown),
        "event-recovery-normal" => None,
        _ => panic!("unknown recovery fixture"),
    };
    if let Some(expected) = expected {
        let error = resolve(&mut current, &recovery, spec, path, budget, 2, mode)
            .await
            .err()
            .expect("required refusal");
        assert_eq!(error, expected);
        // Pre-begin cancellation leaves the session usable. Every other selected
        // refusal happens after begin_operation and must require a fresh claim.
        if mode != "event-recovery-cancelled" {
            let retry = resolve(
                &mut current,
                &recovery,
                spec,
                path,
                budget,
                2,
                "event-recovery-normal",
            )
            .await;
            assert!(matches!(
                retry,
                Err(ProducerResolveError::Control(OwnerError::Unavailable))
            ));
        }
        emit(
            serde_json::json!({"event":"recovery-refused", "error":format!("{error:?}"),
            "session_poison_checked":mode != "event-recovery-cancelled"}),
        );
        drop(current);
        crate::ingest_closing::join(driver, mode == "event-recovery-commit-lost").await;
        let interrupted = crate::ingest_snapshot::state(observer, path).await;
        emit(serde_json::json!({"event":"recovery-interrupted", "state":interrupted}));
        if mode != "event-recovery-commit-lost" {
            assert_eq!(interrupted, before);
        }
        if matches!(
            mode,
            "event-recovery-pg-only"
                | "event-recovery-input-size"
                | "event-recovery-input-digest"
                | "event-recovery-input-manifest"
        ) {
            return;
        }
        (current, driver) = crate::ingest_closing::acquire(&direct_config, spec, 34).await;
        assert_eq!(current.identity().epoch.get(), 3);
    }
    let mut previous = None;
    // The event has no PG outcome after the deliberate original transaction
    // failure. Bootstrap already does; both exact retries must preserve xmin.
    for (attempt, sequence) in [2, 2, 1].into_iter().enumerate() {
        let resolved = resolve(
            &mut current,
            &recovery,
            spec,
            path,
            budget,
            sequence,
            "event-recovery-normal",
        )
        .await
        .unwrap();
        let bigbrotr::storage::rocks::store::AdmissionOutcome::Durable(receipt) =
            resolved.outcome()
        else {
            panic!()
        };
        assert_eq!(receipt.request_id().request_sequence.get(), sequence);
        emit(
            serde_json::json!({"event":"producer-resolved", "request":sequence,
            "bytes":hex(&receipt.encode_persistent(65536).unwrap()),
            "current_control_epoch":current.identity().epoch.get(),
            "historical_control_epoch":spec.control.epoch.get()}),
        );
        let observed = crate::ingest_snapshot::state(observer, path).await;
        assert_eq!(observed["native_sha256"], before["native_sha256"]);
        assert_eq!(observed["sequence"], before["sequence"]);
        assert_eq!(observed["originals"], before["originals"]);
        if let Some(previous) = &previous {
            assert_eq!(&observed, previous);
        }
        emit(
            serde_json::json!({"event":"recovery-observed", "attempt":attempt,
            "request":sequence, "state":observed}),
        );
        previous = Some(observed);
    }
    emit(serde_json::json!({"event":"recovery-after", "state":previous.unwrap()}));
    current.release().await.unwrap();
    crate::ingest_closing::join(driver, false).await;
}

#[allow(
    clippy::too_many_arguments,
    reason = "finite fixture keeps independently selected fault separate from original request"
)]
async fn resolve(
    current: &mut ControlSession,
    recovery: &IngestRecovery,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    sequence: u64,
    mode: &str,
) -> Result<ResolvedProducerRequest, ProducerResolveError> {
    let cancelled = AtomicBool::new(mode == "event-recovery-cancelled");
    let mut validations = 0;
    current
        .resolve_initial_producer_request(
            recovery,
            ProducerResolveRequest {
                ingest_root: path,
                registration: spec,
                request: ProducerRequestId {
                    producer_slot_id: spec.producer_slot_id,
                    producer_epoch: ProducerEpoch::new(1).unwrap(),
                    request_sequence: RequestSequence::new(sequence).unwrap(),
                },
                limits: ProducerRecoveryLimits {
                    input: RequestDecodeLimits {
                        values: FactDecodeLimits {
                            max_encoded_bytes: if mode == "event-recovery-resource" {
                                1
                            } else {
                                MIB as usize
                            },
                            max_allocation_bytes: MIB as usize,
                            max_tag_nodes: 1024,
                            max_string_bytes: MIB as usize,
                        },
                        max_dictionaries: 8,
                    },
                    bootstrap: BootstrapLimits {
                        max_mappings: 8,
                        max_batch_bytes: MIB as usize,
                    },
                    event: AdmissionLimits {
                        max_outstanding_requests: 2,
                        ..AdmissionLimits::default()
                    },
                },
                policy_ttl: Duration::from_secs(2),
                cancelled: &cancelled,
            },
            grant(budget, 3 * MIB, 0),
            grant(budget, 40 * MIB, 0),
            |document, digest| {
                validate(document, digest)?;
                validations += 1;
                if mode == "event-recovery-policy" {
                    return Err(OwnerError::InvalidPolicy);
                }
                if mode == "event-recovery-expired" && validations == 1 {
                    std::thread::sleep(Duration::from_secs(3));
                }
                Ok(())
            },
        )
        .await
}
