//! Actual TCP/queue controls; PG authorization and live dependency proof are separate gates.
use super::*;
use bigbrotr::management::roles::*;

fn auth() -> String {
    format!(
        "Cookie: BigBrotrLocalSession={}\r\nX-CSRF-Token: {}\r\n",
        "11".repeat(32),
        "22".repeat(32)
    )
}
fn snapshot() -> RoleSnapshot {
    RoleSnapshot::new(
        SnapshotIdentity {
            dataset_id: [1; 16].try_into().unwrap(),
            process_incarnation: [2; 16].try_into().unwrap(),
            config_revision: "18446744073709551615".to_owned().try_into().unwrap(),
            policy_revision: "9007199254740993".to_owned().try_into().unwrap(),
        },
        123,
        SourceObservation {
            cut: None,
            freshness: Freshness::Unknown,
            time: None,
            reason: StatusReason::NotObserved,
        },
        vec![RoleObservation {
            role: RoleKind::Control,
            state: RoleState::Ready,
            reason: StatusReason::Current,
        }],
        vec![],
    )
    .unwrap()
}
#[tokio::test]
async fn role_status_requires_session_csrf_and_closed_get_before_queueing() {
    let (mut server, address, budget) = server().await;
    for (method, suffix, headers, body, status) in [
        ("GET", "", String::new(), "", 401),
        (
            "GET",
            "",
            format!("Cookie: BigBrotrLocalSession={}\r\n", "11".repeat(32)),
            "",
            403,
        ),
        ("GET", "?extra=1", auth(), "", 400),
        ("GET", "", auth(), "{}", 400),
        (
            "GET",
            "",
            format!("{}X-CSRF-Token: {}\r\n", auth(), "22".repeat(32)),
            "",
            403,
        ),
        ("HEAD", "", auth(), "", 405),
        ("POST", "", auth(), "", 405),
    ] {
        let answer = exchange(address,format!("{method} /management/v1/roles{suffix} HTTP/1.1\r\nHost: {address}\r\nContent-Length: {}\r\n{headers}\r\n{body}",body.len())).await;
        assert!(
            answer.starts_with(&format!("HTTP/1.1 {status} ")),
            "{answer}"
        );
        assert!(server.commands.try_recv().is_err());
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}
#[tokio::test]
async fn role_status_delivers_only_redacted_observation_and_releases_body_grant() {
    let (mut server, address, budget) = server().await;
    let client = tokio::spawn(exchange(
        address,
        format!(
            "GET /management/v1/roles HTTP/1.1\r\nHost: {address}\r\n{}\r\n",
            auth()
        ),
    ));
    let received = tokio::time::timeout(Duration::from_secs(1), server.commands.recv())
        .await
        .unwrap()
        .unwrap();
    let transport::Operation::RoleStatus { session, csrf, .. } = received.operation else {
        panic!("wrong operation");
    };
    assert_eq!(session.expose_hex().as_str(), "11".repeat(32));
    assert_eq!(csrf.expose_hex().as_str(), "22".repeat(32));
    let permit = budget
        .try_acquire(ResourceRequest::data(RESPONSE_RESERVATION, 1, 0).unwrap())
        .unwrap();
    received.completion.finish(Ok(transport::Reply::RoleStatus(
        snapshot().encode(permit).unwrap(),
    )));
    let answer = client.await.unwrap();
    let (headers, body) = answer.split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("HTTP/1.1 200 "));
    assert!(headers.contains("cache-control: no-store\r\n"));
    assert!(!headers.contains("set-cookie:"));
    let value: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(value["identity"]["config_revision"], "18446744073709551615");
    assert_eq!(value["authority"], "observation-only");
    assert!(value["source"]["cut"].is_null());
    assert!(value.get("pending_bytes").is_none());
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}
#[tokio::test]
async fn role_status_bound_action_expiry_refuses_bytes_after_queue_completion() {
    use bigbrotr::runtime::policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl};
    let (mut server, address, budget) = server().await;
    let client = tokio::spawn(exchange(
        address,
        format!(
            "GET /management/v1/roles HTTP/1.1\r\nHost: {address}\r\n{}\r\n",
            auth()
        ),
    ));
    let received = tokio::time::timeout(Duration::from_secs(1), server.commands.recv())
        .await
        .unwrap()
        .unwrap();
    let transport::Operation::RoleStatus { delivery, .. } = received.operation else {
        panic!("wrong operation");
    };
    let identity = snapshot().identity();
    let epoch = bigbrotr_model::PolicyEpoch::new(1);
    let ttl = Duration::from_secs(5);
    let mut lease = PolicyLeaseTracker::new(
        identity.dataset_id,
        identity.process_incarnation,
        epoch,
        PolicyLeaseTtl::new(ttl).unwrap(),
    )
    .unwrap();
    let ticket = lease.begin_refresh().unwrap();
    lease
        .complete_refresh(
            &ticket,
            LeaseResponse {
                dataset: identity.dataset_id,
                process_incarnation: identity.process_incarnation,
                policy_epoch: epoch,
                ttl,
            },
        )
        .unwrap();
    delivery
        .action
        .bind(
            lease
                .begin_action(epoch)
                .unwrap()
                .complete(Duration::from_millis(100))
                .unwrap(),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let permit = budget
        .try_acquire(ResourceRequest::data(RESPONSE_RESERVATION, 1, 0).unwrap())
        .unwrap();
    received.completion.finish(Ok(transport::Reply::RoleStatus(
        snapshot().encode(permit).unwrap(),
    )));
    assert!(client.await.unwrap().is_empty());
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}
