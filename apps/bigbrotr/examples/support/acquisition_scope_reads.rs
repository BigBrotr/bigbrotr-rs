//! Actual-engine scope reader checks; expected identities/bytes come from the lab.
use bigbrotr::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::postgres::control_owner::{ControlSession, ScopeReadError},
};
use bigbrotr_model::{AcquisitionScopeId, Digest256};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};
use tokio::{task::JoinHandle, time::timeout};

pub async fn run(
    mut owner: ControlSession,
    driver: JoinHandle<Result<(), tokio_postgres::Error>>,
    mode: &str,
    expected_path: &str,
    policy: u64,
    backend: (i32, String),
) {
    let expected: Value = serde_json::from_slice(&std::fs::read(expected_path).unwrap()).unwrap();
    let identity: AcquisitionScopeId = expected["scope_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(policy, expected["current_policy"].as_u64().unwrap());
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            64 * 1024 * 1024,
            16 * 1024 * 1024,
            ResourceLimits::new(32 * 1024 * 1024, 4, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    // The actual driver is already retained above, before the bounded query.
    // Keep this separate reservation through its final join, including errors.
    let driver_permit = budget
        .try_acquire(ResourceRequest::data(8 * 1024 * 1024, 1, 0).unwrap())
        .unwrap();
    let permit = |bytes, items| {
        budget
            .try_acquire(ResourceRequest::data(bytes, items, 0).unwrap())
            .unwrap()
    };
    if mode.starts_with("read-source-") {
        source_read(owner, driver, mode, &expected, &budget).await;
        drop(driver_permit);
        assert_eq!(budget.snapshot().active_permits(), 0);
        println!(
            "{}",
            json!({"status":"PASS","mode":mode,"scope_id":identity,
                "backend_pid":backend.0,"backend_start":backend.1})
        );
        return;
    }
    if mode == "read-valid" {
        for (bytes, items) in [(8 * 1024 * 1024 - 1, 1), (8 * 1024 * 1024, 0)] {
            assert_eq!(
                owner
                    .read_acquisition_scope(identity, permit(bytes, items))
                    .await
                    .err(),
                Some(ScopeReadError::Capacity)
            );
            assert_eq!(budget.snapshot().active_permits(), 1);
        }
    }
    if mode == "read-cancel" {
        println!("{}", json!({"event":"read-start","scope_id":identity}));
        assert!(
            timeout(
                Duration::from_secs(2),
                owner.read_acquisition_scope(identity, permit(8 * 1024 * 1024, 1))
            )
            .await
            .is_err()
        );
        assert_eq!(budget.snapshot().active_permits(), 1);
        println!("{}", json!({"event":"read-cancelled","scope_id":identity}));
        assert_eq!(
            owner
                .read_acquisition_scope(identity, permit(8 * 1024 * 1024, 1))
                .await
                .err(),
            Some(ScopeReadError::Unavailable)
        );
        drop(owner);
        driver.await.unwrap().unwrap();
    } else {
        let result = owner
            .read_acquisition_scope(identity, permit(8 * 1024 * 1024, 1))
            .await;
        if mode == "read-invalid" || mode == "read-old-schema" {
            let error = match expected["error"].as_str().unwrap() {
                "InvalidMetadata" => ScopeReadError::InvalidMetadata,
                "Capacity" => ScopeReadError::Capacity,
                "Prerequisite" => ScopeReadError::Prerequisite,
                other => panic!("unexpected fixture error {other}"),
            };
            assert_eq!(result.err(), Some(error));
            assert_eq!(budget.snapshot().active_permits(), 1);
            assert_eq!(
                owner
                    .read_acquisition_scope(identity, permit(8 * 1024 * 1024, 1))
                    .await
                    .err(),
                Some(ScopeReadError::Unavailable)
            );
            drop(owner);
            driver.await.unwrap().unwrap();
        } else if mode == "read-missing" {
            assert!(result.unwrap().is_none());
            assert_eq!(budget.snapshot().active_permits(), 1);
            owner.release().await.unwrap();
            driver.await.unwrap().unwrap();
        } else {
            assert!(matches!(
                mode,
                "read-valid" | "read-large" | "read-historical"
            ));
            let loaded = result.unwrap().unwrap();
            let scope = loaded.scope();
            assert_eq!(loaded.identity(), identity);
            assert_eq!(
                json!({
                    "dataset":scope.dataset(), "endpoint":scope.endpoint(),
                    "selector":scope.selector(), "access":scope.access(),
                    "visibility_domain":scope.visibility().allocation_domain,
                    "visibility_ordinal":scope.visibility().ordinal.get(),
                    "policy":scope.policy_epoch().get(),
                }),
                expected["scope"]
            );
            assert_eq!(budget.snapshot().active_permits(), 2);
            assert_eq!(loaded.reserved_bytes(), 8 * 1024 * 1024);
            let bytes = serde_json::to_vec(loaded.selector()).unwrap();
            assert_eq!(
                bytes.len(),
                expected["document_bytes"].as_u64().unwrap() as usize
            );
            assert_eq!(
                Digest256::from_bytes(Sha256::digest(&bytes).into()).to_string(),
                expected["document_sha256"].as_str().unwrap()
            );
            drop(bytes);
            // The data survives Control release. It confers no live grant.
            owner.release().await.unwrap();
            driver.await.unwrap().unwrap();
            assert_eq!(loaded.scope(), scope);
            drop(loaded);
            assert_eq!(budget.snapshot().active_permits(), 1);
        }
    }
    drop(driver_permit);
    assert_eq!(budget.snapshot().active_permits(), 0);
    budget.close();
    budget
        .drain_until(Instant::now() + Duration::from_secs(1))
        .unwrap();
    println!(
        "{}",
        json!({"status":"PASS","mode":mode,"scope_id":identity})
    );
}

/// Independent literal source identities are supplied by the original SQL fixture.
async fn source_read(
    mut owner: ControlSession,
    driver: JoinHandle<Result<(), tokio_postgres::Error>>,
    mode: &str,
    expected: &Value,
    budget: &ResourceBudget,
) {
    use bigbrotr::acquisition::relay_source::StoredRelaySource;
    let permit = |bytes, items| {
        budget
            .try_acquire(ResourceRequest::data(bytes, items, 0).unwrap())
            .unwrap()
    };
    let scope = owner
        .read_acquisition_scope(
            expected["scope_id"].as_str().unwrap().parse().unwrap(),
            permit(8 * 1024 * 1024, 1),
        )
        .await
        .unwrap()
        .unwrap();
    let bytes = 128 * 1024;
    assert_eq!(StoredRelaySource::REQUIRED_BYTES, bytes);
    if mode == "read-source-valid" {
        for (size, items) in [(bytes - 1, 1), (bytes, 0)] {
            assert_eq!(
                owner
                    .read_acquisition_source(&scope, permit(size, items))
                    .await
                    .err(),
                Some(ScopeReadError::Capacity)
            );
            assert_eq!(budget.snapshot().active_permits(), 2);
        }
    }
    if mode == "read-source-cancel" {
        println!(
            "{}",
            json!({"event":"read-start","scope_id":scope.identity()})
        );
        assert!(
            timeout(
                Duration::from_secs(2),
                owner.read_acquisition_source(&scope, permit(bytes, 1))
            )
            .await
            .is_err()
        );
        assert_eq!(budget.snapshot().active_permits(), 2);
        println!(
            "{}",
            json!({"event":"read-cancelled","scope_id":scope.identity()})
        );
        assert_eq!(
            owner
                .read_acquisition_source(&scope, permit(bytes, 1))
                .await
                .err(),
            Some(ScopeReadError::Unavailable)
        );
        drop(owner);
        driver.await.unwrap().unwrap();
    } else {
        let loaded = owner
            .read_acquisition_source(&scope, permit(bytes, 1))
            .await;
        match expected["source_error"].as_str() {
            Some("InvalidMetadata") => {
                assert_eq!(loaded.err(), Some(ScopeReadError::InvalidMetadata));
                assert_eq!(
                    owner
                        .read_acquisition_source(&scope, permit(bytes, 1))
                        .await
                        .err(),
                    Some(ScopeReadError::Unavailable)
                );
                drop(owner);
                driver.await.unwrap().unwrap();
            }
            None => {
                let loaded = loaded.unwrap();
                if expected["source"].is_null() {
                    assert!(loaded.is_none());
                } else {
                    let source = loaded.as_ref().unwrap();
                    let binding = source.binding();
                    assert_eq!(
                        json!({"endpoint_id":binding.endpoint_id,"access_scope_id":binding.access_scope_id,
                        "enrollment_generation":binding.enrollment_generation.get(),"route_id":binding.route_id,
                        "url":source.endpoint().as_str(),"route_bytes":source.route().canonical_bytes()}),
                        expected["source"]
                    );
                    assert_eq!(source.reserved_bytes(), bytes);
                    assert_eq!(budget.snapshot().active_permits(), 3);
                }
                owner.release().await.unwrap();
                driver.await.unwrap().unwrap();
                drop(loaded);
            }
            other => panic!("unknown source expectation {other:?}"),
        }
    }
    drop(scope);
    assert_eq!(budget.snapshot().active_permits(), 1);
}
