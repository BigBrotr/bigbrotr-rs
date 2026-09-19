//! Finite signed import consumer for connected native admission qualification.
use super::*;
use bigbrotr::acquisition::import::{JsonlReader, ScratchSpace, SourceIdentity, SourceSnapshot};
use bigbrotr::storage::postgres::control_owner::{
    InitialEventError, InitialEventRequest, InitialImportBatchRequest,
};
use bigbrotr::storage::postgres::input_bindings::{ImportBatch, ImportJobBinding};
use bigbrotr::storage::rocks::store::{
    AdmissionError, AdmissionLimits, AdmissionOutcome, PreparedEvent,
};
use bigbrotr_model::{EventIntent, ProducerEpoch, ProducerRequestId, RequestSequence};

pub fn source_digest(mode: &str) -> Digest256 {
    if mode.starts_with("event-bindings") {
        "ce439b495fd2f99031706e9625529e264cd3043ffc16af4fd6002243503e90fc"
    } else {
        "c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae"
    }
    .parse()
    .unwrap()
}

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
    let binding_mode = mode.starts_with("event-bindings");
    if binding_mode {
        source.write_all(source_bytes).unwrap();
    }
    let scratch = ScratchSpace::new(path.parent().unwrap(), 4096, 1).unwrap();
    let captured = SourceSnapshot::capture(
        source.path(),
        &scratch,
        Some(SourceIdentity {
            bytes: source_bytes.len() as u64 * if binding_mode { 2 } else { 1 },
            sha256: *source_digest(mode).as_bytes(),
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
    let mut positions = vec![position];
    if binding_mode {
        let second = reader
            .next_record(&AtomicBool::new(false))
            .unwrap()
            .unwrap();
        assert_eq!(second.result().as_ref().unwrap().event(), &event);
        positions.push(second.position());
        drop(second);
    }
    assert!(
        reader
            .next_record(&AtomicBool::new(false))
            .unwrap()
            .is_none()
    );
    drop(reader);
    assert_eq!(scratch.used_bytes(), 0);
    for position in &positions {
        emit(
            serde_json::json!({"event":"import-position", "line":position.line,
        "byte_start":position.byte_start,"byte_end":position.byte_end,
        "source_bytes":position.source.bytes,"source_sha256":hex(&position.source.sha256),
        "record_sha256":hex(&position.sha256),"completed_import_prefix_before_admission":0}),
        );
    }
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
    let cancelled = AtomicBool::new(matches!(
        mode,
        "event-cancelled" | "event-bindings-cancelled"
    ));
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
            max_outstanding_requests: if mode == "event-outcomes" || binding_mode {
                3
            } else {
                2
            },
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
    if binding_mode {
        // Trusted lab setup of an existing running job, not operator acceptance.
        observer.execute("INSERT INTO control.jobs(job_id,dataset_id,principal_id,request_key,request_digest,kind,config_revision,policy_revision,created_at_us,input_complete) SELECT $1::bytea,dataset_id,principal_id,'input-bindings'::bytea,$2::bytea,'import',1,1,0,true FROM control.principals WHERE kind='installation'", &[&&[90u8;16][..], &&[91u8;32][..]]).await.unwrap();
        for state in ["queued", "running"] {
            observer.execute("UPDATE control.jobs SET state=$1::text,job_revision=job_revision+1,owner_resource_id=$2::bytea,owner_epoch=$3::text::numeric,owner_incarnation=$4::bytea WHERE job_id=$5::bytea", &[&state, &&spec.control.resource_id.as_bytes()[..], &spec.control.epoch.get().to_string(), &&spec.control.process_incarnation.as_bytes()[..], &&[90u8;16][..]]).await.unwrap();
        }
        if mode.starts_with("event-bindings-checkpoints") {
            // Native request IDs remain contiguous while logical source order is
            // deliberately reversed. The duplicate event needs no new facts.
            for (index, sequence) in [(1, 2), (0, 3)] {
                let batch = ImportBatch::new(
                    ImportJobBinding {
                        id: [90; 16],
                        request_digest: [91; 32],
                        expected_revision: 3,
                        config_revision: 1,
                        policy_revision: 1,
                    },
                    [92; 32],
                    0,
                    &positions[index..=index],
                )
                .unwrap();
                owner
                    .reserve_initial_import_batch(
                        store,
                        InitialImportBatchRequest {
                            ingest_root: path,
                            registration: spec,
                            batch: &batch,
                            events: &[prepare(sequence)],
                            limits: AdmissionLimits {
                                max_outstanding_requests: 3,
                                ..AdmissionLimits::default()
                            },
                            policy_ttl: Duration::from_secs(2),
                            cancelled: &cancelled,
                        },
                        grant(budget, 40 * MIB, 10 * MIB),
                        authorize,
                    )
                    .await
                    .unwrap();
                let mut previous = None;
                for recorded_at in [456, 789] {
                    let result = owner
                        .admit_initial_event(
                            store,
                            input(recorded_at, sequence),
                            grant(budget, 40 * MIB, 10 * MIB),
                            authorize,
                        )
                        .await;
                    if sequence == 3 && mode != "event-bindings-checkpoints" {
                        let expected = match mode {
                            "event-bindings-checkpoints-expired" => InitialEventError::DeliveryUnknown,
                            "event-bindings-checkpoints-rollback" => InitialEventError::ImportCheckpoint(
                                bigbrotr::storage::postgres::import_checkpoints::ImportCheckpointError::Backend,
                            ),
                            "event-bindings-checkpoints-commit-lost" => InitialEventError::Outcome(
                                bigbrotr::storage::postgres::producer_outcomes::OutcomeError::CommitUnknown,
                            ),
                            _ => panic!("unknown checkpoint fault"),
                        };
                        assert!(matches!(&result, Err(actual) if actual == &expected));
                        emit(
                            serde_json::json!({"event":"logical-checkpoint-refused","error":format!("{expected:?}")}),
                        );
                        return;
                    }
                    let outcome = result.unwrap();
                    let AdmissionOutcome::Durable(receipt) = outcome.outcome() else {
                        panic!("verified bound import refused")
                    };
                    assert_eq!(receipt.batch().is_some(), sequence == 2);
                    let observation: String = observer.query_one("SELECT jsonb_build_object('closures',(SELECT coalesce(jsonb_agg(to_jsonb(c)||jsonb_build_object('xmin',xmin::text) ORDER BY logical_record),'[]'::jsonb) FROM control.import_record_closures c),'heads',(SELECT coalesce(jsonb_agg(to_jsonb(h)||jsonb_build_object('xmin',xmin::text)),'[]'::jsonb) FROM control.import_checkpoints h),'job',(SELECT to_jsonb(j)||jsonb_build_object('xmin',xmin::text) FROM control.jobs j),'producer_checkpoints',(SELECT count(*) FROM control.producer_checkpoints))::text", &[]).await.unwrap().get(0);
                    let state: serde_json::Value = serde_json::from_str(&observation).unwrap();
                    assert_eq!(
                        state["closures"].as_array().unwrap().len(),
                        (sequence - 1) as usize
                    );
                    assert_eq!(state["producer_checkpoints"], 0);
                    if sequence == 2 {
                        assert!(state["heads"].as_array().unwrap().is_empty());
                        assert_eq!(state["closures"][0]["logical_record"], 2);
                        assert_eq!(state["job"]["checkpoint_revision"], 0);
                        assert_eq!(state["job"]["job_revision"], 3);
                    } else {
                        assert_eq!(state["heads"].as_array().unwrap().len(), 1);
                        assert_eq!(state["heads"][0]["complete_through_record"], 2);
                        assert_eq!(state["heads"][0]["byte_offset"], 706);
                        assert_eq!(state["heads"][0]["revision"], 1);
                        assert_eq!(state["job"]["checkpoint_revision"], 1);
                        assert_eq!(state["job"]["job_revision"], 4);
                    }
                    let current = (receipt.encode_persistent(8192).unwrap(), observation);
                    if let Some(prior) = &previous {
                        assert_eq!(
                            prior, &current,
                            "exact replay must not rewrite retained progress"
                        );
                    }
                    emit(
                        serde_json::json!({"event":"logical-checkpoint", "request_sequence":sequence, "receipt":hex(&current.0), "state":state, "retry":previous.is_some()}),
                    );
                    previous = Some(current);
                }
            }
            owner.release().await.unwrap();
            return;
        }
        let batch = ImportBatch::new(
            ImportJobBinding {
                id: [90; 16],
                request_digest: [91; 32],
                expected_revision: 3,
                config_revision: 1,
                policy_revision: 1,
            },
            [92; 32],
            0,
            &positions,
        )
        .unwrap();
        let events = [prepare(2), prepare(3)];
        let mut original_rows = None;
        for _ in 0..2 {
            let result = owner
                .reserve_initial_import_batch(
                    store,
                    InitialImportBatchRequest {
                        ingest_root: path,
                        registration: spec,
                        batch: &batch,
                        events: &events,
                        limits: AdmissionLimits {
                            max_outstanding_requests: 3,
                            ..AdmissionLimits::default()
                        },
                        policy_ttl: Duration::from_secs(2),
                        cancelled: &cancelled,
                    },
                    grant(budget, 40 * MIB, 10 * MIB),
                    authorize,
                )
                .await;
            let expected_error = match mode {
                "event-bindings-commit-lost" => Some(InitialEventError::InputBinding(
                    bigbrotr::storage::postgres::input_bindings::InputBindingError::CommitUnknown,
                )),
                "event-bindings-rollback" => Some(InitialEventError::Reservation(
                    bigbrotr::storage::postgres::producer_requests::ReservationError::Backend,
                )),
                "event-bindings-cancelled" => Some(InitialEventError::Native(
                    AdmissionError::AuthorityUnavailable,
                )),
                _ => None,
            };
            if let Some(expected) = expected_error {
                assert_eq!(result, Err(expected));
                emit(
                    serde_json::json!({"event":"binding-refused", "error":format!("{expected:?}")}),
                );
                return;
            }
            result.unwrap();
            let observed: String = observer.query_one("SELECT jsonb_agg(to_jsonb(b)||jsonb_build_object('xmin',xmin::text) ORDER BY logical_record)::text FROM control.producer_input_bindings b", &[]).await.unwrap().get(0);
            if let Some(prior) = &original_rows {
                assert_eq!(prior, &observed);
            }
            original_rows = Some(observed);
            let row = observer.query_one("SELECT (SELECT count(*) FROM control.producer_requests),(SELECT count(*) FROM control.producer_outcomes),(SELECT count(*) FROM control.producer_checkpoints)", &[]).await.unwrap();
            assert_eq!(
                (
                    row.get::<_, i64>(0),
                    row.get::<_, i64>(1),
                    row.get::<_, i64>(2)
                ),
                (3, 1, 0)
            );
        }
        emit(
            serde_json::json!({"event":"input-batch-bound","records":2,"requests":3,"outcomes":1,"checkpoints":0,"retry_unchanged":true}),
        );
        if mode == "event-bindings-conflict" {
            let mut changed = positions.clone();
            changed[0].sha256[0] ^= 1;
            let wrong = ImportBatch::new(
                ImportJobBinding {
                    id: [90; 16],
                    request_digest: [91; 32],
                    expected_revision: 3,
                    config_revision: 1,
                    policy_revision: 1,
                },
                [92; 32],
                0,
                &changed,
            )
            .unwrap();
            let error = owner
                .reserve_initial_import_batch(
                    store,
                    InitialImportBatchRequest {
                        ingest_root: path,
                        registration: spec,
                        batch: &wrong,
                        events: &events,
                        limits: AdmissionLimits {
                            max_outstanding_requests: 3,
                            ..AdmissionLimits::default()
                        },
                        policy_ttl: Duration::from_secs(2),
                        cancelled: &cancelled,
                    },
                    grant(budget, 40 * MIB, 10 * MIB),
                    authorize,
                )
                .await
                .unwrap_err();
            assert_eq!(
                error,
                InitialEventError::InputBinding(
                    bigbrotr::storage::postgres::input_bindings::InputBindingError::Conflict
                )
            );
            emit(serde_json::json!({"event":"binding-refused", "error":format!("{error:?}")}));
            return;
        }
        if mode == "event-bindings-before" {
            tokio::time::sleep(Duration::from_secs(120)).await;
        }
        if mode == "event-bindings-paused" {
            observer.execute("UPDATE control.jobs SET state='paused',reason_code='operator_requested',job_revision=job_revision+1 WHERE job_id=$1::bytea", &[&&[90u8;16][..]]).await.unwrap();
        }
    }
    let result = owner
        .admit_initial_event(
            store,
            input(456, 2),
            grant(budget, 40 * MIB, 10 * MIB),
            |document, digest, candidate| {
                authorize(document, digest, candidate)?;
                validations += 1;
                if validations == 3 && mode == "event-bindings-pause-between" {
                    emit(serde_json::json!({"event":"before-native-job-recheck"}));
                    let mut reply = [0; 9];
                    std::io::Read::read_exact(&mut std::io::stdin(), &mut reply).unwrap();
                    assert_eq!(&reply, b"continue\n");
                }
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
    if matches!(
        mode,
        "event-write" | "event-after" | "event-outcomes" | "event-bindings"
    ) {
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
        if mode == "event-outcomes" || mode == "event-bindings" {
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
            "event-bindings-paused" | "event-bindings-pause-between" => assert_eq!(
                error,
                InitialEventError::InputBinding(
                    bigbrotr::storage::postgres::input_bindings::InputBindingError::Conflict
                )
            ),
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
