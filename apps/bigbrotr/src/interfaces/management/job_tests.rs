//! Actual private TCP boundary. Durable effects require the native SQL fixture.
use super::*;

fn input() -> serde_json::Value {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/management/export-command.json"
    ))
    .unwrap();
    fixture["vectors"][0]["command"].clone()
}
fn request(address: SocketAddr, headers: &str, body: &str) -> String {
    format!(
        "POST /management/v1/jobs HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}\r\n{body}",
        body.len()
    )
}
fn authenticated() -> String {
    format!(
        "Cookie: BigBrotrLocalSession={}\r\nX-CSRF-Token: {}\r\nIdempotency-Key: {}\r\n",
        "11".repeat(32),
        "22".repeat(32),
        "08".repeat(16)
    )
}

#[tokio::test]
async fn job_status_requires_session_csrf_and_a_closed_request_before_queueing() {
    let (mut server, address, budget) = server().await;
    let id = "09".repeat(16);
    let cookie = format!("Cookie: BigBrotrLocalSession={}\r\n", "11".repeat(32));
    let auth = format!("{cookie}X-CSRF-Token: {}\r\n", "22".repeat(32));
    for (path, headers, body, status) in [
        (id.clone(), String::new(), "", 401),
        (id.clone(), cookie, "", 403),
        ("invalid".into(), auth.clone(), "", 400),
        ("00".repeat(16), auth.clone(), "", 400),
        ("AB".repeat(16), auth.clone(), "", 400),
        (format!("%30{}", &id[1..]), auth.clone(), "", 400),
        (format!("{id}?extra=1"), auth.clone(), "", 400),
        (id.clone(), auth.clone(), "{}", 400),
        (
            id,
            format!("{auth}X-CSRF-Token: {}\r\n", "22".repeat(32)),
            "",
            403,
        ),
    ] {
        let request = format!(
            "GET /management/v1/jobs/{path} HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Length: {}\r\n{headers}\r\n{body}",
            body.len()
        );
        let response = exchange(address, request).await;
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
async fn job_status_preserves_closed_metadata_and_never_sets_session_cookies() {
    use bigbrotr::{
        export::execution::ExportState,
        management::jobs::{JobObservation, JobReason},
    };
    let (mut server, address, budget) = server().await;
    for missing in [false, true] {
        let client = tokio::spawn(exchange(
            address,
            format!(
                "GET /management/v1/jobs/{} HTTP/1.1\r\nHost: {address}\r\n{}\r\n",
                "09".repeat(16),
                authenticated()
            ),
        ));
        let received = tokio::time::timeout(Duration::from_secs(1), server.commands.recv())
            .await
            .unwrap()
            .unwrap();
        let transport::Operation::JobStatus {
            session, csrf, job, ..
        } = received.operation
        else {
            panic!("wrong operation");
        };
        assert_eq!(session.expose_hex().as_str(), "11".repeat(32));
        assert_eq!(csrf.expose_hex().as_str(), "22".repeat(32));
        assert_eq!(job.to_string(), "09".repeat(16));
        received
            .completion
            .finish(Ok(transport::Reply::JobStatus((!missing).then_some(
                JobObservation {
                    job_id: job,
                    state: ExportState::Paused,
                    job_revision: u64::MAX,
                    checkpoint_revision: 0,
                    created_at_us: i64::MIN,
                    completed_at_us: None,
                    observed_at_us: i64::MAX,
                    reason: Some(JobReason::ResourceLimit),
                },
            ))));
        let response = client.await.unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(headers.starts_with(if missing {
            "HTTP/1.1 404 "
        } else {
            "HTTP/1.1 200 "
        }));
        assert!(headers.contains("cache-control: no-store\r\n"));
        assert!(!headers.contains("set-cookie:"));
        let body: serde_json::Value = serde_json::from_str(body).unwrap();
        if missing {
            assert_eq!(body["error"]["code"], "job_unavailable");
            assert_eq!(body["error"]["message"], "Job unavailable to this account.");
        } else {
            assert_eq!(
                body,
                serde_json::json!({
                    "format":"bigbrotr-job-status/1", "job_id":"09".repeat(16), "kind":"export",
                    "state":"paused", "job_revision":"18446744073709551615", "checkpoint_revision":"0",
                    "created_at_us":"-9223372036854775808", "completed_at_us":null,
                    "observed_at_us":"9223372036854775807", "reason_code":"resource_limit",
                    "authority":"observation-only"
                })
            );
        }
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn job_route_refuses_unauthenticated_ambiguous_and_unsupported_requests_before_queueing() {
    let (mut server, address, budget) = server().await;
    let body = input();
    let encoded = body.to_string();
    let auth = authenticated();
    let cookie = format!("Cookie: BigBrotrLocalSession={}\r\n", "11".repeat(32));
    let mut unknown = body.clone();
    unknown["path"] = "/tmp/browser-selected".into();
    let mut kind = body.clone();
    kind["kind"] = "shell".into();
    let mut format = body.clone();
    format["format"] = "parquet".into();
    let mut query = body.clone();
    query["query"]["mode"] = "live".into();
    for (candidate, status) in [
        (request(address, "", &encoded), 401),
        (request(address, &cookie, &encoded), 403),
        (
            request(
                address,
                &auth.replace(&format!("Idempotency-Key: {}\r\n", "08".repeat(16)), ""),
                &encoded,
            ),
            400,
        ),
        (
            request(
                address,
                &format!("{auth}Idempotency-Key: {}\r\n", "08".repeat(16)),
                &encoded,
            ),
            400,
        ),
        (
            request(
                address,
                &format!("{auth}X-CSRF-Token: {}\r\n", "22".repeat(32)),
                &encoded,
            ),
            403,
        ),
        (request(address, &auth, &unknown.to_string()), 400),
        (request(address, &auth, &kind.to_string()), 400),
        (request(address, &auth, &format.to_string()), 400),
        (request(address, &auth, &query.to_string()), 400),
        (
            request(
                address,
                &auth,
                &encoded.replacen(
                    "\"kind\":\"export\"",
                    "\"kind\":\"export\",\"kind\":\"export\"",
                    1,
                ),
            ),
            400,
        ),
        (request(address, &auth, &"x".repeat(8193)), 413),
        (
            request(address, &auth, &encoded).replace("/jobs HTTP", "/jobs?extra=1 HTTP"),
            400,
        ),
        (
            request(address, &auth, &encoded).replace(
                &format!("Origin: http://{address}"),
                "Origin: https://foreign.invalid",
            ),
            403,
        ),
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
async fn job_route_preserves_typed_input_and_historical_receipt_without_claiming_execution() {
    let (mut server, address, budget) = server().await;
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/management/export-command.json"
    ))
    .unwrap();
    for vector in &fixture["vectors"].as_array().unwrap()[..2] {
        let body = &vector["command"];
        for created in [true, false] {
            let client = tokio::spawn(exchange(
                address,
                request(address, &authenticated(), &body.to_string()),
            ));
            let received = tokio::time::timeout(Duration::from_secs(1), server.commands.recv())
                .await
                .unwrap()
                .unwrap();
            let transport::Operation::Job {
                session,
                csrf,
                key,
                command,
            } = received.operation
            else {
                panic!("wrong queued operation");
            };
            assert_eq!(session.expose_hex().as_str(), "11".repeat(32));
            assert_eq!(csrf.expose_hex().as_str(), "22".repeat(32));
            assert_eq!(key.to_string(), "08".repeat(16));
            assert_eq!(serde_json::to_value(&command).unwrap(), *body);
            received.completion.finish(Ok(transport::Reply::Job(
                bigbrotr::management::jobs::JobReceipt {
                    command_key: key,
                    job_id: "09".repeat(16).parse().unwrap(),
                    accepted_at_us: i64::MAX,
                    created,
                },
            )));
            let response = client.await.unwrap();
            let (headers, returned) = response.split_once("\r\n\r\n").unwrap();
            let status = if created { 202 } else { 200 };
            assert!(headers.starts_with(&format!("HTTP/1.1 {status} ")));
            assert!(headers.contains("cache-control: no-store\r\n"));
            assert!(!headers.contains("set-cookie:"));
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(returned).unwrap(),
                serde_json::json!({"format":"bigbrotr-job-acceptance/1", "command_key":"08".repeat(16),
                    "job_id":"09".repeat(16), "accepted_at_us":"9223372036854775807", "created":created,
                    "execution":"unobserved", "authority":"acceptance-only"})
            );
        }
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn job_route_reports_refusal_without_a_receipt_or_cookie() {
    let (mut server, address, budget) = server().await;
    for (failure, status) in [
        (SessionError::ArchiveUnavailable, 503),
        (SessionError::Denied, 401),
        (SessionError::Forbidden, 403),
        (SessionError::Conflict, 409),
        (SessionError::Capacity, 429),
    ] {
        let client = tokio::spawn(exchange(
            address,
            request(address, &authenticated(), &input().to_string()),
        ));
        tokio::time::timeout(Duration::from_secs(1), server.commands.recv())
            .await
            .unwrap()
            .unwrap()
            .completion
            .finish(Err(failure));
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
