//! Actual-adapter fault schedules; file gates exist only in the isolated lab.
use bigbrotr::storage::postgres::control_owner::{
    ControlSession, ScopeIntent, ScopeRegistrationError,
};
use serde_json::{Value, json};
use std::{cell::Cell, path::Path, time::Duration};

pub async fn gate(prefix: &str, suffix: &str) {
    while !Path::new(&format!("{prefix}.{suffix}")).exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn run(
    mut owner: ControlSession,
    driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    mode: &str,
    prefix: &str,
    input: ScopeIntent<'_>,
) {
    if matches!(mode, "fault-policy" | "fault-existing-expired") {
        gate(prefix, "go").await;
    }
    let started = Cell::new(false);
    let retry = || ScopeIntent { ..input };
    let outcome: Value = if mode == "fault-cancel" {
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            owner.register_acquisition_scope(retry(), &started),
        )
        .await;
        assert!(result.is_err() && started.get());
        assert_eq!(
            owner
                .register_acquisition_scope(retry(), &Cell::new(false))
                .await,
            Err(ScopeRegistrationError::Unavailable)
        );
        println!("{}", json!({"event":"cancelled","commit_started":true}));
        // The controller observes the real backend fence and chooses whether
        // to forward COMMIT before this probe drops either custody owner.
        gate(prefix, "finish").await;
        json!({"result":"cancelled","commit_started":true})
    } else {
        let result = owner.register_acquisition_scope(retry(), &started).await;
        let expected = match mode {
            "fault-unknown" => Some((ScopeRegistrationError::CommitUnknown, true)),
            "fault-held-expired" => Some((ScopeRegistrationError::AuthorityExpired, true)),
            "fault-policy" | "fault-stale" => Some((ScopeRegistrationError::PolicyChanged, false)),
            "fault-overfull" | "fault-overfull-new" => {
                Some((ScopeRegistrationError::InvalidMetadata, false))
            }
            "fault-insert" | "fault-current" | "fault-existing-expired" => None,
            _ => panic!("unknown fault mode"),
        };
        if let Some((error, commit)) = expected {
            assert_eq!(result, Err(error));
            assert_eq!(started.get(), commit);
            assert_eq!(
                owner
                    .register_acquisition_scope(retry(), &Cell::new(false))
                    .await,
                Err(ScopeRegistrationError::Unavailable)
            );
            json!({"result":error.to_string(),"commit_started":started.get()})
        } else {
            let value = result.unwrap();
            assert_eq!(started.get(), value.created);
            json!({"scope":value.identity,"selector":value.scope.selector(),
                "policy":value.scope.policy_epoch().get(),"created":value.created,"commit_started":started.get()})
        }
    };
    drop(owner);
    let driver_ok = driver.await.unwrap().is_ok();
    println!(
        "{}",
        json!({"event":"done","outcome":outcome,"driver_joined":true,"driver_ok":driver_ok})
    );
}
