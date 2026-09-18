//! Finite signed import consumer for connected native admission qualification.
use super::*;
use bigbrotr::storage::postgres::control_owner::{InitialEventError, InitialEventRequest};
use bigbrotr::storage::rocks::store::{
    AdmissionError, AdmissionLimits, AdmissionOutcome, PreparedEvent,
};
use bigbrotr_model::{EventIntent, ProducerEpoch, ProducerRequestId, RequestSequence, SignedEvent};

#[allow(
    clippy::too_many_arguments,
    reason = "isolated acceptance client carries the existing owned laboratory handles"
)]
pub async fn run(
    mut owner: ControlSession,
    observer: &tokio_postgres::Client,
    store: &mut IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
    scope: QualifiedRef,
) {
    let lineage = QualifiedRef {
        dictionary_kind: DictionaryKind::DatasetLineage,
        allocation_domain: [69; 16].try_into().unwrap(),
        ordinal: Ordinal::new(1).unwrap(),
        ..scope
    };
    let event: SignedEvent = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/ingest/import-event.jsonl"
    ))
    .unwrap();
    let original_id = event.id;
    let request_id = ProducerRequestId {
        producer_slot_id: spec.producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(2).unwrap(),
    };
    let prepare = || {
        PreparedEvent::new(
            request_id,
            EventIntent::import(1, scope, event.clone(), lineage).unwrap(),
            256 * 1024,
            grant(budget, MIB, 0),
        )
        .unwrap()
    };
    if mode == "event-retired" {
        observer.execute("UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=$1::bytea AND producer_epoch=1",&[&&spec.producer_slot_id.as_bytes()[..]]).await.unwrap();
    }
    let cancelled = AtomicBool::new(mode == "event-cancelled");
    let input = |recorded_at| InitialEventRequest {
        ingest_root: if mode == "event-wrong-root" {
            path.parent().unwrap()
        } else {
            path
        },
        registration: spec,
        event: prepare(),
        provenance_enabled: true,
        recorded_at: ObservedTime::new(recorded_at),
        limits: AdmissionLimits {
            max_outstanding_requests: 2,
            ..AdmissionLimits::default()
        },
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    let mut validations = 0;
    let authorize = |document: &str, digest: [u8; 32], candidate: &PreparedEvent| {
        validate(document, digest)?;
        // This exact fixture source/scope/lineage is the laboratory grant. It is
        // not an implementation of installed operator source policy.
        if candidate.verification_limit_bytes() != 256 * 1024
            || candidate.visibility_scope_id() != scope
            || candidate.origin() != bigbrotr_model::EventOrigin::Import(lineage)
            || candidate.observation().is_some()
            || candidate
                .verified_event()
                .is_none_or(|e| e.event().id != original_id)
        {
            return Err(OwnerError::InvalidPolicy);
        }
        Ok(())
    };
    let result = owner
        .admit_initial_event(
            store,
            input(456),
            grant(budget, 40 * MIB, 10 * MIB),
            |document, digest, candidate| {
                authorize(document, digest, candidate)?;
                validations += 1;
                if validations == 2 {
                    if mode == "event-before" {
                        emit(serde_json::json!({"event":"before-native"}));
                        std::thread::sleep(Duration::from_secs(120));
                    }
                    if mode == "event-expired" {
                        std::thread::sleep(Duration::from_secs(3));
                    }
                }
                Ok(())
            },
        )
        .await;
    if matches!(mode, "event-write" | "event-after") {
        let outcome = result.unwrap();
        let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
            panic!("verified import refused")
        };
        assert_eq!(receipt.batch().unwrap().records().len(), 3);
        let first = receipt.batch().unwrap().first();
        let bytes = receipt.encode_persistent(8192).unwrap();
        if mode == "event-after" {
            emit(serde_json::json!({"event":"after-native-before-receipt"}));
            tokio::time::sleep(Duration::from_secs(120)).await;
        }
        drop(outcome);
        let retry = owner
            .admit_initial_event(
                store,
                input(789),
                grant(budget, 40 * MIB, 10 * MIB),
                authorize,
            )
            .await
            .unwrap();
        let AdmissionOutcome::Durable(receipt) = retry.outcome() else {
            panic!()
        };
        assert_eq!(receipt.encode_persistent(8192).unwrap(), bytes);
        drop(retry);
        let pending = store
            .read_pending_batch(
                first,
                PendingReadLimits {
                    max_records: 8,
                    max_encoded_bytes: 8192,
                    fact: bigbrotr_model::codec::FactDecodeLimits {
                        max_encoded_bytes: 8192,
                        max_allocation_bytes: 65536,
                        max_tag_nodes: 128,
                        max_string_bytes: 8192,
                    },
                },
                grant(budget, MIB, 0),
                &cancelled,
            )
            .unwrap()
            .unwrap();
        emit(
            serde_json::json!({"event":"receipt","bytes":hex(&bytes),"facts":pending.records().iter().map(|r|hex(r)).collect::<Vec<_>>(),"replay_exact":true}),
        );
        drop(pending);
        owner.release().await.unwrap();
    } else {
        let error = match result {
            Err(e) => e,
            Ok(_) => panic!("negative event control accepted"),
        };
        match mode {
            "event-cancelled" => assert_eq!(
                error,
                InitialEventError::Native(AdmissionError::AuthorityUnavailable)
            ),
            "event-expired" => {
                assert_eq!(error, InitialEventError::Control(OwnerError::Unavailable))
            }
            "event-retired" => assert_eq!(
                error,
                InitialEventError::Registry(
                    bigbrotr::storage::postgres::ingest_registry::RegistryError::InvalidMetadata
                )
            ),
            "event-wrong-root" => {
                assert_eq!(error, InitialEventError::Control(OwnerError::InvalidInput))
            }
            _ => panic!("unknown mode"),
        }
        emit(
            serde_json::json!({"event":"refused","error":format!("{error:?}"),"validated":validations}),
        );
        drop(owner);
    }
}
