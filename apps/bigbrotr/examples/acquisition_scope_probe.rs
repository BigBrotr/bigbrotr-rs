//! Isolated actual PostgreSQL registration probe. No installed Sync claim.
use bigbrotr::storage::postgres::control_owner::{
    ClaimRequest, ControlSession, OwnerError, ScopeIntent, ScopeRegistrationError,
};
use bigbrotr_model::{
    AccessScopeId, DatasetId, DictionaryKind, EndpointId, Ordinal, PolicyEpoch, ProcessIncarnation,
    QualifiedRef,
    acquisition::{CollectionSelector, EphemeralPolicy, Filter, FilterFields},
};
use bigbrotr_protocol::selector::decode_selector;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{cell::Cell, collections::BTreeSet, time::Duration};
use tokio_postgres::{Config, NoTls};

#[path = "support/acquisition_scope_faults.rs"]
mod faults;

fn selector(kinds: Vec<u16>) -> CollectionSelector {
    CollectionSelector::new(
        vec![
            Filter::new(FilterFields {
                kinds: Some(kinds),
                ..FilterFields::default()
            })
            .unwrap(),
        ],
        bigbrotr_model::NostrTime(0),
        EphemeralPolicy::Retain,
    )
    .unwrap()
}

fn intent(
    selection: &CollectionSelector,
    endpoint: EndpointId,
    context: (AccessScopeId, QualifiedRef),
    epoch: u64,
) -> ScopeIntent<'_> {
    ScopeIntent {
        selector: selection,
        endpoint,
        access: context.0,
        visibility: context.1,
        policy_epoch: PolicyEpoch::new(epoch),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::time::timeout(Duration::from_secs(90), run())
        .await
        .expect("finite probe");
}

async fn run() {
    let args: Vec<_> = std::env::args().collect();
    assert!(matches!(args.len(), 5 | 8));
    let mode = args[4].as_str();
    assert!(
        mode.starts_with("fault-")
            || matches!(
                mode,
                "basic" | "collision" | "rollback" | "expired" | "historical" | "old-schema"
            )
    );
    assert_eq!(args.len() == 8, mode.starts_with("fault-"));
    let expected: Value = serde_json::from_slice(&std::fs::read(&args[3]).unwrap()).unwrap();
    assert_eq!(expected["format"], "bigbrotr-product-policy/2");
    let digest: [u8; 32] = Sha256::new()
        .chain_update(b"bigbrotr/product-policy/2\0")
        .chain_update(serde_json::to_vec(&expected).unwrap())
        .finalize()
        .into();
    let validate = |document: &str, actual: [u8; 32]| {
        if serde_json::from_str::<Value>(document).ok().as_ref() == Some(&expected)
            && actual == digest
        {
            Ok(())
        } else {
            Err(OwnerError::InvalidPolicy)
        }
    };
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(args.get(5).map_or(5432, |port| port.parse().unwrap()))
        .user("bigbrotr_control")
        .dbname(&args[1])
        .password(std::fs::read(&args[2]).unwrap());
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    let row = client
        .query_one(
            "SELECT dataset_id::bytea,instance_id::bytea,pg_backend_pid() FROM control.datasets",
            &[],
        )
        .await
        .unwrap();
    let dataset =
        DatasetId::try_from(<[u8; 16]>::try_from(row.get::<_, &[u8]>(0)).unwrap()).unwrap();
    let instance =
        DatasetId::try_from(<[u8; 16]>::try_from(row.get::<_, &[u8]>(1)).unwrap()).unwrap();
    let rows=client.query("
        SELECT e.endpoint_id::bytea,a.access_scope_id::bytea,a.allocation_domain::bytea,a.ordinal::text
        FROM control.relay_enrollments e JOIN control.relay_access_contexts a
            ON a.dataset_id=e.dataset_id AND a.access_scope_id=e.access_scope_id
        ORDER BY a.ordinal LIMIT 3",&[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let endpoint =
        EndpointId::try_from(<[u8; 16]>::try_from(rows[0].get::<_, &[u8]>(0)).unwrap()).unwrap();
    let contexts: Vec<_> = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            assert_eq!(row.get::<_, &[u8]>(0), endpoint.as_bytes());
            assert_eq!(row.get::<_, &[u8]>(2), &[0x40; 16]);
            assert_eq!(row.get::<_, &str>(3), (index + 1).to_string());
            (
                AccessScopeId::try_from(<[u8; 16]>::try_from(row.get::<_, &[u8]>(1)).unwrap())
                    .unwrap(),
                QualifiedRef {
                    dataset_id: dataset,
                    dictionary_kind: DictionaryKind::VisibilityScope,
                    allocation_domain: [0x40; 16].try_into().unwrap(),
                    ordinal: Ordinal::new(index as u64 + 1).unwrap(),
                },
            )
        })
        .collect();
    let first_nonce = args
        .get(6)
        .map_or(if mode == "historical" { 231 } else { 211 }, |value| {
            value.parse::<u8>().unwrap()
        });
    assert!(first_nonce > 0);
    let claim = |nonce| ClaimRequest {
        dataset,
        instance,
        authority_mode: "managed".into(),
        role_instance_id: [nonce; 16],
        incarnation: ProcessIncarnation::try_from([nonce; 16]).unwrap(),
        lease_ttl: if mode == "expired" {
            Duration::from_millis(300)
        } else if mode == "fault-held-expired" {
            Duration::from_secs(3)
        } else if mode == "fault-existing-expired" {
            Duration::from_secs(1)
        } else {
            Duration::from_secs(60)
        },
    };
    let claim_started = Cell::new(false);
    let acquired =
        ControlSession::acquire(client, claim(first_nonce), validate, &claim_started).await;
    if mode == "fault-busy" {
        assert!(matches!(acquired, Err(OwnerError::Busy)) && !claim_started.get());
        driver.await.unwrap().unwrap();
        println!("{}", json!({"event":"busy","commit_started":false}));
        return;
    }
    let (mut owner, policy) = acquired.unwrap();
    if mode.starts_with("fault-") {
        println!(
            "{}",
            json!({"event":"ready","pid":row.get::<_,i32>(2),
            "epoch":owner.identity().epoch.get(),"policy":policy.policy_revision})
        );
        let selection = selector(match mode {
            "fault-overfull" => vec![0],
            "fault-overfull-new" | "fault-stale" => vec![777],
            "fault-existing-expired" => vec![1, 2],
            _ => vec![10],
        });
        let epoch = if mode == "fault-current" {
            policy.policy_revision
        } else {
            1
        };
        faults::run(
            owner,
            driver,
            mode,
            &args[7],
            intent(&selection, endpoint, contexts[0], epoch),
        )
        .await;
        return;
    }

    let base = selector(vec![2, 1, 1]);
    if matches!(mode, "rollback" | "expired" | "old-schema") {
        let mut input = intent(&base, endpoint, contexts[0], 1);
        if mode == "rollback" {
            input.visibility = contexts[1].1;
        }
        if mode == "expired" {
            tokio::time::sleep(Duration::from_millis(450)).await;
            let (observer, connection) = config.connect(NoTls).await.unwrap();
            let observer_driver = tokio::spawn(connection);
            let expired:bool=observer.query_one("
                SELECT lease_expires_at_us <= (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
                FROM control.owner_epochs WHERE role='control' AND process_incarnation=$1::bytea",
                &[&&[211u8;16][..]]).await.unwrap().get(0);
            assert!(expired);
            drop(observer);
            observer_driver.await.unwrap().unwrap();
        }
        let commit = Cell::new(false);
        let expected = match mode {
            "expired" => ScopeRegistrationError::AuthorityExpired,
            "old-schema" => ScopeRegistrationError::Prerequisite,
            _ => ScopeRegistrationError::Backend,
        };
        assert_eq!(
            owner.register_acquisition_scope(input, &commit).await,
            Err(expected)
        );
        assert_eq!(commit.get(), mode == "expired");
        assert_eq!(
            owner
                .register_acquisition_scope(
                    intent(&base, endpoint, contexts[0], 1),
                    &Cell::new(false)
                )
                .await,
            Err(ScopeRegistrationError::Unavailable)
        );
        drop(owner);
        driver.await.unwrap().unwrap();
        println!(
            "{}",
            json!({"status":"PASS","mode":mode,"commit_started":commit.get()})
        );
        return;
    }
    if mode == "collision" {
        let mut selectors = BTreeSet::new();
        let mut scopes = BTreeSet::new();
        let mut first = None;
        for kind in 0..64 {
            let selection = selector(vec![kind]);
            let result = owner
                .register_acquisition_scope(
                    intent(&selection, endpoint, contexts[0], 1),
                    &Cell::new(false),
                )
                .await
                .unwrap();
            assert!(
                result.created
                    && selectors.insert(result.scope.selector())
                    && scopes.insert(result.identity)
            );
            if kind == 0 {
                first = Some(result);
            }
        }
        let original = selector(vec![0]);
        let mut expected = first.unwrap();
        expected.created = false;
        assert_eq!(
            owner
                .register_acquisition_scope(
                    intent(&original, endpoint, contexts[0], 1),
                    &Cell::new(false)
                )
                .await
                .unwrap(),
            expected
        );
        let extra = selector(vec![64]);
        let commit = Cell::new(false);
        assert_eq!(
            owner
                .register_acquisition_scope(intent(&extra, endpoint, contexts[0], 1), &commit)
                .await,
            Err(ScopeRegistrationError::Capacity)
        );
        assert!(!commit.get());
        drop(owner);
        driver.await.unwrap().unwrap();
        println!(
            "{}",
            json!({"status":"PASS","mode":mode,"distinct_selectors":selectors.len(),"distinct_scopes":scopes.len()})
        );
        return;
    }
    let first = owner
        .register_acquisition_scope(intent(&base, endpoint, contexts[0], 1), &Cell::new(false))
        .await
        .unwrap();
    assert_eq!(first.created, mode == "basic");
    if mode == "historical" {
        let new = owner
            .register_acquisition_scope(intent(&base, endpoint, contexts[0], 2), &Cell::new(false))
            .await
            .unwrap();
        assert!(
            new.created
                && new.identity != first.identity
                && new.scope.selector() == first.scope.selector()
        );
        let missing = selector(vec![777]);
        let commit = Cell::new(false);
        assert_eq!(
            owner
                .register_acquisition_scope(intent(&missing, endpoint, contexts[0], 1), &commit)
                .await,
            Err(ScopeRegistrationError::PolicyChanged)
        );
        assert!(!commit.get());
        drop(owner);
        driver.await.unwrap().unwrap();
        println!(
            "{}",
            json!({"status":"PASS","mode":mode,"original":first.identity,"fresh":new.identity})
        );
        return;
    }
    let reordered = decode_selector(
        br#"{"ephemeral":"retain","history_since":0,"filters":[{"kinds":[1,2]}],"version":1}"#,
    )
    .unwrap();
    let repeat = owner
        .register_acquisition_scope(
            intent(&reordered, endpoint, contexts[0], 1),
            &Cell::new(false),
        )
        .await
        .unwrap();
    assert!(!repeat.created && repeat.identity == first.identity && repeat.scope == first.scope);
    let other = owner
        .register_acquisition_scope(intent(&base, endpoint, contexts[1], 1), &Cell::new(false))
        .await
        .unwrap();
    assert!(
        other.created
            && other.identity != first.identity
            && other.scope.selector() == first.scope.selector()
    );
    let wider = selector(vec![1, 2, 3]);
    let changed = owner
        .register_acquisition_scope(intent(&wider, endpoint, contexts[0], 1), &Cell::new(false))
        .await
        .unwrap();
    assert!(
        changed.created
            && changed.identity != first.identity
            && changed.scope.selector() != first.scope.selector()
    );
    owner.release().await.unwrap();
    driver.await.unwrap().unwrap();
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    let (mut successor, _) =
        ControlSession::acquire(client, claim(212), validate, &Cell::new(false))
            .await
            .unwrap();
    let recovered = successor
        .register_acquisition_scope(intent(&base, endpoint, contexts[0], 1), &Cell::new(false))
        .await
        .unwrap();
    assert_eq!(recovered, repeat);
    successor.release().await.unwrap();
    driver.await.unwrap().unwrap();
    println!(
        "{}",
        json!({"status":"PASS","mode":mode,"original":first.identity,"other":other.identity,
        "wider":changed.identity,"selector":first.scope.selector(),"wider_selector":changed.scope.selector()})
    );
}
