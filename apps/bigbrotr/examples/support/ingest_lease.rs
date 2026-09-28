//! Actual scheduling renewal followed by a native read after the original expiry.
use super::*;
use bigbrotr::storage::{
    postgres::control_owner::InitialVisibilityRequest, rocks::store::CertificationLimits,
};

#[allow(
    clippy::too_many_arguments,
    reason = "finite fixture retains actual owner and engines"
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
    let validate = |document: &str, digest| validate_version(document, digest, true);
    let cases = [
        "lease-pass",
        "lease-expired",
        "lease-control-expired",
        "lease-closing",
        "lease-draining",
        "lease-policy-first",
        "lease-policy-second",
        "lease-delayed",
        "lease-wrong-registration",
        "lease-wrong-control",
        "lease-cancelled",
    ];
    assert!(cases.contains(&mode));
    if mode == "lease-closing" {
        observer
            .batch_execute("UPDATE control.producer_epochs SET state='closing'")
            .await
            .unwrap();
    }
    if mode == "lease-draining" {
        observer
            .batch_execute("UPDATE control.owner_epochs SET state='draining' WHERE role='ingest'")
            .await
            .unwrap();
    }
    let old_expiry: i64 = observer.query_one(
        "UPDATE control.owner_epochs SET lease_expires_at_us = (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint + $1::bigint WHERE role='ingest' RETURNING lease_expires_at_us",
        &[&if mode == "lease-expired" { -1_i64 } else if mode == "lease-pass" { 2_000_000 } else { 60_000_000 }]
    ).await.unwrap().get(0);
    if mode == "lease-control-expired" {
        tokio::time::sleep(Duration::from_millis(2100)).await;
    }
    let before = ingest_snapshot::state(observer, path).await;
    emit(serde_json::json!({"event":"lease-before", "state":before,"original_expiry":old_expiry}));
    let commit = Cell::new(false);
    let mut attempt = spec.clone();
    if mode == "lease-wrong-registration" {
        attempt.ingest_role_instance_id = [99; 16];
    }
    if mode == "lease-wrong-control" {
        attempt.control.role_instance_id = [99; 16];
    }
    let mut validated = 0;
    if mode == "lease-cancelled" {
        observer
            .batch_execute("BEGIN; SELECT dataset_id FROM control.datasets FOR UPDATE")
            .await
            .unwrap();
        {
            let operation =
                owner.renew_initial_ingest(&attempt, Duration::from_secs(2), validate, &commit);
            tokio::pin!(operation);
            let blocked = async {
                loop {
                    observer
                        .query_one("SELECT pg_catalog.pg_stat_clear_snapshot()", &[])
                        .await
                        .unwrap();
                    let blocked: bool = observer.query_one(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity a JOIN control.owner_epochs o ON a.pid=o.control_backend_pid AND a.backend_start=o.control_backend_start WHERE o.role='control' AND o.state='active' AND a.wait_event_type='Lock' AND a.query LIKE '%FROM control.datasets%')", &[]
                    ).await.unwrap().get(0);
                    if blocked {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            tokio::select! {
                result = &mut operation => panic!("renewal escaped actual lock: {result:?}"),
                result = tokio::time::timeout(Duration::from_secs(2), blocked) => result.unwrap(),
            }
        }
        assert!(!commit.get());
        assert_eq!(
            owner.poll_policy(validate).await,
            Err(OwnerError::Unavailable)
        );
        // Cancellation poisons reuse, while the actual backend still holds its fence.
        let retained: bool = observer.query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN control.owner_epochs o ON o.control_backend_pid=l.pid WHERE o.role='control' AND o.state='active' AND l.locktype='advisory' AND l.classid=1111651442::oid AND l.objid=1129599564::oid AND l.granted)", &[]
        ).await.unwrap().get(0);
        assert!(retained);
        observer.batch_execute("ROLLBACK").await.unwrap();
        emit(
            serde_json::json!({"event":"lease-cancelled", "actual_backend_lock":true,"retained_control_fence":retained}),
        );
    } else {
        let result = owner
            .renew_initial_ingest(
                &attempt,
                Duration::from_secs(2),
                |document, digest| {
                    validate(document, digest)?;
                    validated += 1;
                    if (mode == "lease-policy-first" && validated == 1)
                        || (mode == "lease-policy-second" && validated == 2)
                    {
                        return Err(OwnerError::InvalidPolicy);
                    }
                    if mode == "lease-delayed" && validated == 2 {
                        std::thread::sleep(Duration::from_millis(2100));
                    }
                    Ok(())
                },
                &commit,
            )
            .await;
        let expected = match mode {
            "lease-pass" => Ok(()),
            "lease-expired" | "lease-control-expired" | "lease-delayed" => {
                Err(OwnerError::Unavailable)
            }
            "lease-closing" | "lease-draining" | "lease-wrong-registration" => {
                Err(OwnerError::InvalidMetadata)
            }
            "lease-wrong-control" => Err(OwnerError::InvalidInput),
            "lease-policy-first" | "lease-policy-second" => Err(OwnerError::InvalidPolicy),
            _ => unreachable!(),
        };
        assert_eq!(result, expected);
        assert_eq!(commit.get(), mode == "lease-pass");
        emit(
            serde_json::json!({"event":"lease-result","result":format!("{result:?}"),"validated":validated,"commit_started":commit.get()}),
        );
        if mode != "lease-pass" && mode != "lease-wrong-control" {
            assert_eq!(
                owner.poll_policy(validate).await,
                Err(OwnerError::Unavailable)
            );
        }
    }
    if mode != "lease-pass" {
        drop(owner);
        // A following observer read waits for the actual queued transaction to end.
        // It is not an inference of rollback from dropping the Rust future.
        observer.batch_execute("BEGIN; SELECT resource_id FROM control.owner_epochs WHERE role='ingest' FOR UPDATE; ROLLBACK").await.unwrap();
        let after = ingest_snapshot::state(observer, path).await;
        assert_eq!(after, before);
        emit(
            serde_json::json!({"event":"lease-finished", "mode":mode,"state":after,"status":"PASS", "zero_writes":true}),
        );
        return store;
    }
    assert!(commit.get());
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let row = observer.query_one("SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint, lease_expires_at_us FROM control.owner_epochs WHERE role='ingest'", &[]).await.unwrap();
    let now: i64 = row.get(0);
    let renewed_expiry: i64 = row.get(1);
    assert!(now > old_expiry && renewed_expiry > now);
    let limits = CertificationLimits {
        max_fact_bytes: 1024,
        max_dependencies: 8,
    };
    let cancel = AtomicBool::new(false);
    let read = owner
        .read_initial_visibility(
            &mut store,
            InitialVisibilityRequest {
                ingest_root: path,
                registration: spec,
                reference: scope,
                limits,
                policy_ttl: Duration::from_secs(2),
                cancelled: &cancel,
            },
            grant(budget, limits.required_data_bytes().unwrap(), 0),
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
    let after = ingest_snapshot::state(observer, path).await;
    for field in ["native_sha256", "native_rows", "sequence", "originals"] {
        assert_eq!(before[field], after[field], "{field}");
    }
    emit(
        serde_json::json!({"event":"lease-finished", "state":after,"mode":mode,"status":"PASS", "original_expiry":old_expiry,"observed_at":now,"renewed_expiry":renewed_expiry}),
    );
    owner.release().await.unwrap();
    store
}
