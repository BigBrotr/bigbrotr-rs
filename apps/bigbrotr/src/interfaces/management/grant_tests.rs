//! Actual TCP decoding and delivery; database effects are tested on PostgreSQL.
use super::*;

fn input() -> serde_json::Value {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/management/scope-grant.json"
    ))
    .unwrap();
    serde_json::json!({"proof_id":"09".repeat(16),"command":fixture["vectors"][1]["command"]})
}
fn request(address: SocketAddr, principal: &str, headers: &str, body: &str) -> String {
    format!(
        "PUT /management/v1/principals/{principal}/grants HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}\r\n{body}",
        body.len()
    )
}
fn authenticated() -> String {
    format!(
        "Cookie: BigBrotrLocalSession={}\r\nX-CSRF-Token: {}\r\nIdempotency-Key: {}\r\n",
        "11".repeat(32),
        "22".repeat(32),
        "04".repeat(16)
    )
}

#[tokio::test]
async fn grant_route_rejects_missing_session_csrf_and_ambiguous_inputs_before_queueing() {
    let (mut server, address, budget) = server().await;
    let body = input();
    let principal = body["command"]["principal_id"].as_str().unwrap();
    let encoded = body.to_string();
    let auth = authenticated();
    let cookie = format!("Cookie: BigBrotrLocalSession={}\r\n", "11".repeat(32));
    let mut unknown = body.clone();
    unknown["extra"] = serde_json::json!(true);
    let mut no_proof = body.clone();
    no_proof.as_object_mut().unwrap().remove("proof_id");
    for (candidate, status) in [
        (request(address, principal, "", &encoded), 401),
        (request(address, principal, &cookie, &encoded), 403),
        (
            request(
                address,
                principal,
                &auth.replace(&format!("Idempotency-Key: {}\r\n", "04".repeat(16)), ""),
                &encoded,
            ),
            400,
        ),
        (
            request(
                address,
                principal,
                &format!("{auth}Idempotency-Key: {}\r\n", "04".repeat(16)),
                &encoded,
            ),
            400,
        ),
        (
            request(
                address,
                principal,
                &format!("{auth}X-CSRF-Token: {}\r\n", "22".repeat(32)),
                &encoded,
            ),
            403,
        ),
        (request(address, "not-an-id", &auth, &encoded), 400),
        (request(address, &"ff".repeat(16), &auth, &encoded), 400),
        (
            request(address, principal, &auth, &unknown.to_string()),
            400,
        ),
        (
            request(address, principal, &auth, &no_proof.to_string()),
            400,
        ),
        (request(address, principal, &auth, &"x".repeat(8193)), 413),
    ] {
        let response = exchange(address, candidate).await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status} ")),
            "{response}"
        );
        assert!(server.commands.try_recv().is_err());
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn grant_route_preserves_exact_command_and_only_reports_acceptance() {
    let (mut server, address, budget) = server().await;
    let body = input();
    let principal = body["command"]["principal_id"].as_str().unwrap();
    for created in [true, false] {
        let client = tokio::spawn(exchange(
            address,
            request(address, principal, &authenticated(), &body.to_string()),
        ));
        let received = server.commands.recv().await.unwrap();
        let transport::Operation::Grant {
            session,
            csrf,
            key,
            command,
            proof,
        } = received.operation
        else {
            panic!("wrong queued operation");
        };
        assert_eq!(session.expose_hex().as_str(), "11".repeat(32));
        assert_eq!(csrf.expose_hex().as_str(), "22".repeat(32));
        assert_eq!(key.to_string(), "04".repeat(16));
        assert_eq!(proof.to_string(), "09".repeat(16));
        assert_eq!(serde_json::to_value(&command).unwrap(), body["command"]);
        received.completion.finish(Ok(transport::Reply::Grant(
            bigbrotr::management::grants::GrantReceipt {
                command_key: key,
                principal_id: command.principal_id,
                scope: command.scope,
                enabled: command.enabled,
                scope_generation: "2".to_string().try_into().unwrap(),
                account_grant_revision: "18446744073709551615".to_string().try_into().unwrap(),
                accepted_at_us: 9_223_372_036_854_775_807,
                created,
            },
        )));
        let response = client.await.unwrap();
        let (headers, returned) = response.split_once("\r\n\r\n").unwrap();
        assert!(headers.starts_with("HTTP/1.1 200 "));
        assert!(headers.contains("cache-control: no-store\r\n"));
        assert!(!headers.contains("set-cookie:"));
        let returned: serde_json::Value = serde_json::from_str(returned).unwrap();
        assert_eq!(
            returned,
            serde_json::json!({"format":"bigbrotr-scope-grant/1",
            "command_key":"04".repeat(16),"principal_id":principal,"scope":body["command"]["scope"],
            "enabled":body["command"]["enabled"],"scope_generation":"2",
            "account_grant_revision":"18446744073709551615","accepted_at_us":"9223372036854775807",
            "created":created,"application":"unobserved","authority":"acceptance-only"})
        );
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn archive_refusal_preserves_service_but_control_loss_remains_fatal() {
    let completed = sql::<()>(Duration::from_secs(1), async {
        Err(SessionError::ArchiveUnavailable)
    })
    .await
    .unwrap();
    assert_eq!(completed, Err(SessionError::ArchiveUnavailable));
    for error in [
        SessionError::Unavailable,
        SessionError::Backend,
        SessionError::CommitUnknown,
        SessionError::Prerequisite,
        SessionError::InvalidMetadata,
        SessionError::PolicyChanged,
    ] {
        assert!(
            sql::<()>(Duration::from_secs(1), async { Err(error) })
                .await
                .is_err()
        );
    }
    let (mut server, address, budget) = server().await;
    let body = input();
    let principal = body["command"]["principal_id"].as_str().unwrap();
    for (error, status) in [
        (SessionError::ArchiveUnavailable, 503),
        (SessionError::Denied, 401),
        (SessionError::Forbidden, 403),
        (SessionError::Conflict, 409),
        (SessionError::Capacity, 429),
    ] {
        let client = tokio::spawn(exchange(
            address,
            request(address, principal, &authenticated(), &body.to_string()),
        ));
        server
            .commands
            .recv()
            .await
            .unwrap()
            .completion
            .finish(Err(error));
        let response = client.await.unwrap();
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status} ")),
            "{response}"
        );
        assert!(!response.contains("set-cookie:") && !response.contains("acceptance-only"));
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}
