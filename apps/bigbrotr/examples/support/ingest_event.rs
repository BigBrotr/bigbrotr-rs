//! Finite signed import consumer for connected native admission qualification.
use super::*;
use bigbrotr::acquisition::import::{JsonlReader, ScratchSpace, SourceIdentity, SourceSnapshot};
use bigbrotr::storage::postgres::control_owner::{InitialEventError, InitialEventRequest};
use bigbrotr::storage::rocks::store::{
    AdmissionError, AdmissionLimits, AdmissionOutcome, PreparedEvent,
};
use bigbrotr_model::{EventIntent, ProducerEpoch, ProducerRequestId, RequestSequence};

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
    let source_bytes = include_bytes!("../../../../tests/fixtures/ingest/import-event.jsonl");
    let mut source = tempfile::NamedTempFile::new_in(path.parent().unwrap()).unwrap();
    source.write_all(source_bytes).unwrap();
    let scratch = ScratchSpace::new(path.parent().unwrap(), 4096, 1).unwrap();
    let captured = SourceSnapshot::capture(
        source.path(),
        &scratch,
        Some(SourceIdentity {
            bytes: source_bytes.len() as u64,
            sha256: *"c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae"
                .parse::<Digest256>()
                .unwrap()
                .as_bytes(),
        }),
        &AtomicBool::new(false),
    )
    .unwrap();
    let mut reader = JsonlReader::new(
        captured,
        bigbrotr_protocol::EventLimits::default(),
        grant(budget, 11 * MIB, 0),
    )
    .unwrap();
    let record = reader
        .next_record(&AtomicBool::new(false))
        .unwrap()
        .unwrap();
    let position = record.position();
    assert_eq!(
        (position.line, position.byte_start, position.byte_end),
        (1, 0, source_bytes.len() as u64)
    );
    // The outer fixture holds its input allocation grant before this copy.
    let event = record.result().as_ref().unwrap().event().clone();
    drop(record);
    assert!(
        reader
            .next_record(&AtomicBool::new(false))
            .unwrap()
            .is_none()
    );
    drop(reader);
    assert_eq!(scratch.used_bytes(), 0);
    emit(
        serde_json::json!({"event":"import-position", "line":position.line,
        "byte_start":position.byte_start,"byte_end":position.byte_end,
        "source_bytes":position.source.bytes,"source_sha256":hex(&position.source.sha256),
        "record_sha256":hex(&position.sha256),"completed_import_prefix_before_admission":0}),
    );
    let original_id = event.id;
    let request_id = ProducerRequestId {
        producer_slot_id: spec.producer_slot_id,
        producer_epoch: ProducerEpoch::new(1).unwrap(),
        request_sequence: RequestSequence::new(2).unwrap(),
    };
    let prepare = |sequence| {
        PreparedEvent::new(
            ProducerRequestId {
                request_sequence: RequestSequence::new(sequence).unwrap(),
                ..request_id
            },
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
    let input = |recorded_at, sequence| InitialEventRequest {
        ingest_root: if mode == "event-wrong-root" {
            path.parent().unwrap()
        } else {
            path
        },
        registration: spec,
        event: prepare(sequence),
        provenance_enabled: true,
        recorded_at: ObservedTime::new(recorded_at),
        limits: AdmissionLimits {
            max_outstanding_requests: if mode == "event-outcomes" { 3 } else { 2 },
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
            input(456, 2),
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
    if matches!(mode, "event-write" | "event-after" | "event-outcomes") {
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
                input(789, 2),
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
        if mode == "event-outcomes" {
            let mut exact = None;
            let mut retained = None;
            for recorded_at in [1000, 2000] {
                let outcome = owner
                    .admit_initial_event(
                        store,
                        input(recorded_at, 3),
                        grant(budget, 40 * MIB, 10 * MIB),
                        authorize,
                    )
                    .await
                    .unwrap();
                let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
                    panic!("existing valid event refused")
                };
                assert!(receipt.batch().is_none());
                let bytes = receipt.encode_persistent(8192).unwrap();
                let copied = owner
                    .read_producer_outcome(receipt.request_id())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    copied.kind,
                    bigbrotr::storage::rocks::store::NativeOutcomeKind::AlreadyDurable
                );
                let row = observer.query_one(
                    "SELECT row_to_json(o)::text, xmin::text FROM control.producer_outcomes o WHERE request_sequence=3", &[],
                ).await.unwrap();
                let row = (row.get::<_, String>(0), row.get::<_, String>(1));
                if let Some(previous) = &exact {
                    assert_eq!(previous, &bytes);
                    assert_eq!(retained.as_ref(), Some(&row));
                } else {
                    exact = Some(bytes);
                    retained = Some(row);
                }
            }
            emit(
                serde_json::json!({"event":"existing-receipt","bytes":hex(&exact.unwrap()),"replay_exact":true,"row_unchanged":true}),
            );
        }
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
