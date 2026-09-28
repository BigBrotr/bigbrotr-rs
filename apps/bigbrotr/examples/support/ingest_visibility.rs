//! Real Control/native reads with independent complete state observations.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::{InitialEventError, InitialVisibilityRequest},
    rocks::store::{AdmissionError, CertificationLimits},
};
use std::sync::atomic::Ordering;

#[allow(
    clippy::too_many_arguments,
    reason = "finite fixture carries retained owner and native handles"
)]
pub async fn run(
    mut owner: ControlSession,
    observer: &tokio_postgres::Client,
    mut store: IngestStore,
    spec: &InitialRegistration,
    path: &Path,
    budget: &ResourceBudget,
    mode: &str,
    scope: QualifiedRef,
) -> IngestStore {
    let limits = CertificationLimits {
        max_fact_bytes: 1024,
        max_dependencies: 8,
    };
    let bytes = limits.required_data_bytes().unwrap();
    let cancelled = AtomicBool::new(false);
    let request = |reference, limits| InitialVisibilityRequest {
        ingest_root: path,
        registration: spec,
        reference,
        limits,
        policy_ttl: Duration::from_secs(2),
        cancelled: &cancelled,
    };
    if mode == "visibility-closing" {
        observer.execute("UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=$1::bytea AND producer_epoch=1", &[&&spec.producer_slot_id.as_bytes()[..]]).await.unwrap();
    }
    let before = ingest_snapshot::state(observer, path).await;
    emit(serde_json::json!({"event":"visibility-before", "state":before}));
    if mode == "visibility" {
        let baseline = budget.snapshot().used().data_bytes();
        for (ordinal, expected) in [(1, Some("public")), (2, Some("private")), (3, None)] {
            let reference = QualifiedRef {
                ordinal: Ordinal::new(ordinal).unwrap(),
                ..scope
            };
            let read = owner
                .read_initial_visibility(
                    &mut store,
                    request(reference, limits),
                    grant(budget, bytes, 0),
                    validate,
                )
                .await
                .unwrap();
            match (read.as_ref(), expected) {
                (Some(value), Some(expected)) => {
                    assert_eq!(value.entry().reference(), reference);
                    assert!(
                        matches!(value.entry().descriptor(), DictionaryDescriptor::Visibility { class } if class.as_str() == expected)
                    );
                    assert_eq!(budget.snapshot().used().data_bytes(), baseline + bytes);
                }
                (None, None) => {}
                _ => panic!("wrong committed visibility"),
            }
            emit(
                serde_json::json!({"event":"visibility-read", "ordinal":ordinal, "class":expected}),
            );
            drop(read);
            assert_eq!(budget.snapshot().used().data_bytes(), baseline);
        }
        let small = CertificationLimits {
            max_fact_bytes: 32,
            ..limits
        };
        assert_eq!(
            owner
                .read_initial_visibility(
                    &mut store,
                    request(scope, small),
                    grant(budget, bytes, 0),
                    validate
                )
                .await
                .err(),
            Some(InitialEventError::Native(AdmissionError::Limit))
        );
        let wrong_kind = QualifiedRef {
            dictionary_kind: DictionaryKind::Endpoint,
            ..scope
        };
        assert_eq!(
            owner
                .read_initial_visibility(
                    &mut store,
                    request(wrong_kind, limits),
                    grant(budget, bytes, 0),
                    validate
                )
                .await
                .err(),
            Some(InitialEventError::Native(AdmissionError::InvalidInput))
        );
        cancelled.store(true, Ordering::Relaxed);
        assert_eq!(
            owner
                .read_initial_visibility(
                    &mut store,
                    request(scope, limits),
                    grant(budget, bytes, 0),
                    validate
                )
                .await
                .err(),
            Some(InitialEventError::Native(
                AdmissionError::AuthorityUnavailable
            ))
        );
        cancelled.store(false, Ordering::Relaxed);
        let manifest = *store.manifest();
        drop(store);
        store = IngestStore::reopen(path, manifest, super::limits(), grant(budget, 0, 128 * MIB))
            .unwrap();
        let read = owner
            .read_initial_visibility(
                &mut store,
                request(scope, limits),
                grant(budget, bytes, 0),
                validate,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.entry().reference(), scope);
        assert!(
            matches!(read.entry().descriptor(), DictionaryDescriptor::Visibility { class } if class.as_str() == "public")
        );
        drop(read);
        assert_eq!(budget.snapshot().used().data_bytes(), baseline);
    } else {
        let mut validations = 0;
        let error = owner
            .read_initial_visibility(
                &mut store,
                request(scope, limits),
                grant(budget, bytes, 0),
                |document, digest| {
                    validate(document, digest)?;
                    validations += 1;
                    if validations == 2 {
                        match mode {
                            "visibility-expired" => std::thread::sleep(Duration::from_secs(3)),
                            "visibility-cancelled" => cancelled.store(true, Ordering::Relaxed),
                            "visibility-policy" => return Err(OwnerError::InvalidPolicy),
                            "visibility-closing" => {}
                            _ => panic!("unknown mode"),
                        }
                    }
                    Ok(())
                },
            )
            .await
            .err()
            .unwrap();
        let expected = match mode {
            "visibility-expired" => InitialEventError::Control(OwnerError::Unavailable),
            "visibility-cancelled" => InitialEventError::DeliveryUnknown,
            "visibility-policy" => InitialEventError::Control(OwnerError::InvalidPolicy),
            "visibility-closing" => InitialEventError::Registry(
                bigbrotr::storage::postgres::ingest_registry::RegistryError::InvalidMetadata,
            ),
            _ => unreachable!(),
        };
        assert_eq!(error, expected);
        assert_eq!(validations, 2);
        emit(serde_json::json!({"event":"visibility-refused", "error":format!("{error:?}")}));
    }
    let after = ingest_snapshot::state(observer, path).await;
    assert_eq!(after, before);
    emit(
        serde_json::json!({"event":"visibility-after", "state":after, "mode":mode, "status":"PASS"}),
    );
    // A poisoned read has no mutation to resolve. Drop/join the actual owner;
    // never turn failed delivery into an admission receipt or network grant.
    drop(owner);
    store
}
