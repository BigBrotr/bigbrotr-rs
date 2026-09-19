//! Finite consumer of a retained native outcome after a failed original PG copy.
use super::*;
use bigbrotr::{
    storage::postgres::control_owner::ProducerResolveRequest,
    storage::rocks::store::{AdmissionLimits, IngestRecovery, ProducerRecoveryLimits},
};
use bigbrotr_model::{
    ProducerEpoch, ProducerRequestId, RequestSequence,
    codec::{FactDecodeLimits, request::RequestDecodeLimits},
};

pub async fn run(
    current: &mut ControlSession,
    recovery: &IngestRecovery,
    observer: &tokio_postgres::Client,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
) {
    let cancelled = AtomicBool::new(false);
    let before = crate::ingest_snapshot::state(observer, path).await;
    emit(serde_json::json!({"event":"recovery-before", "state":before}));
    let mut previous = None;
    // The event has no PG outcome after the deliberate original transaction
    // failure. Bootstrap already does; both exact retries must preserve xmin.
    for sequence in [2, 2, 1] {
        let resolved = current
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
                                max_encoded_bytes: MIB as usize,
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
                validate,
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
        previous = Some(observed);
    }
    emit(serde_json::json!({"event":"recovery-after", "state":previous.unwrap()}));
}
