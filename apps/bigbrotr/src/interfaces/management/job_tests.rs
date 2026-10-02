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
async fn job_listing_requires_session_and_csrf_before_queueing_a_cursor() {
    let (mut server, address, budget) = server().await;
    for (headers, expected) in [
        (String::new(), 401),
        (
            format!("Cookie: BigBrotrLocalSession={}\r\n", "11".repeat(32)),
            403,
        ),
    ] {
        let response = exchange(address, format!(
            "GET /management/v1/jobs?cursor=not-a-valid-token HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\n{headers}\r\n"
        )).await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {expected} ")),
            "{response}"
        );
        assert!(server.commands.try_recv().is_err());
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
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
                    kind: bigbrotr::management::jobs::JobKind::Export,
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

#[tokio::test]
async fn resume_route_refuses_missing_authority_and_noncanonical_input_before_queueing() {
    let (mut server, address, budget) = server().await;
    let id = "09".repeat(16);
    let body = r#"{"expected_job_revision":"7","expected_config_revision":"1","expected_policy_revision":"1"}"#;
    let auth = authenticated();
    let cookie = format!("Cookie: BigBrotrLocalSession={}\r\n", "11".repeat(32));
    for (path, headers, input, status) in [
        (id.clone(), String::new(), body.to_owned(), 401),
        (id.clone(), cookie, body.to_owned(), 403),
        (
            id.clone(),
            auth.replace(&format!("Idempotency-Key: {}\r\n", "08".repeat(16)), ""),
            body.to_owned(),
            400,
        ),
        (
            id.clone(),
            format!("{auth}Idempotency-Key: {}\r\n", "08".repeat(16)),
            body.to_owned(),
            400,
        ),
        (
            id.clone(),
            format!("{auth}X-CSRF-Token: {}\r\n", "22".repeat(32)),
            body.to_owned(),
            403,
        ),
        ("AB".repeat(16), auth.clone(), body.to_owned(), 400),
        ("00".repeat(16), auth.clone(), body.to_owned(), 400),
        (
            format!("%30{}", &id[1..]),
            auth.clone(),
            body.to_owned(),
            400,
        ),
        (id.clone(), auth.clone(), "{}".into(), 400),
        (
            id.clone(),
            auth.clone(),
            body.replace("\"7\"", "\"07\""),
            400,
        ),
        (id.clone(), auth.clone(), body.replace("\"7\"", "7"), 400),
        (
            id.clone(),
            auth.clone(),
            body.replace("\"7\"", "\"7\",\"expected_job_revision\":\"7\""),
            400,
        ),
        (
            id.clone(),
            auth.clone(),
            body.replace('}', ",\"destination\":\"/tmp/other\"}"),
            400,
        ),
        (id, auth, "x".repeat(8193), 413),
    ] {
        let request = format!(
            "POST /management/v1/jobs/{path}/resume HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}\r\n{input}",
            input.len()
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
async fn resume_route_preserves_exact_revision_receipt_and_known_refusals() {
    let (mut server, address, budget) = server().await;
    let body = r#"{"expected_job_revision":"18446744073709551614","expected_config_revision":"9","expected_policy_revision":"10"}"#;
    for outcome in [
        Ok(true),
        Ok(false),
        Err(SessionError::Conflict),
        Err(SessionError::Denied),
        Err(SessionError::Forbidden),
        Err(SessionError::ArchiveUnavailable),
        Err(SessionError::Capacity),
    ] {
        let client = tokio::spawn(exchange(
            address,
            request(address, &authenticated(), body).replace(
                "/jobs HTTP",
                &format!("/jobs/{}/resume HTTP", "09".repeat(16)),
            ),
        ));
        let received = tokio::time::timeout(Duration::from_secs(1), server.commands.recv())
            .await
            .unwrap()
            .unwrap();
        let transport::Operation::ResumeExport {
            session,
            csrf,
            key,
            job,
            command,
        } = received.operation
        else {
            panic!("wrong operation");
        };
        assert_eq!(session.expose_hex().as_str(), "11".repeat(32));
        assert_eq!(csrf.expose_hex().as_str(), "22".repeat(32));
        assert_eq!(key.to_string(), "08".repeat(16));
        assert_eq!(job.to_string(), "09".repeat(16));
        assert_eq!(command.expected_job_revision.get(), u64::MAX - 1);
        assert_eq!(command.expected_config_revision.get(), 9);
        assert_eq!(command.expected_policy_revision.get(), 10);
        received.completion.finish(outcome.map(|created| {
            transport::Reply::ResumeExport(bigbrotr::management::jobs::ResumeExportReceipt {
                command_key: key,
                job_id: job,
                job_revision: u64::MAX.to_string().try_into().unwrap(),
                accepted_at_us: i64::MAX,
                created,
            })
        }));
        let response = client.await.unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        let status = match outcome {
            Ok(true) => 202,
            Ok(false) => 200,
            Err(SessionError::Conflict) => 409,
            Err(SessionError::Denied) => 401,
            Err(SessionError::Forbidden) => 403,
            Err(SessionError::Capacity) => 429,
            _ => 503,
        };
        assert!(
            headers.starts_with(&format!("HTTP/1.1 {status} ")),
            "{response}"
        );
        assert!(!headers.contains("set-cookie:"));
        assert!(headers.contains("cache-control: no-store\r\n"));
        if let Ok(created) = outcome {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(body).unwrap(),
                serde_json::json!({
                    "format":"bigbrotr-export-resume/1", "command_key":"08".repeat(16), "job_id":"09".repeat(16),
                    "job_revision":"18446744073709551615", "accepted_at_us":"9223372036854775807", "created":created,
                    "execution":"unobserved", "authority":"acceptance-only"
                })
            );
        } else {
            assert!(!body.contains("acceptance-only"));
        }
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn listing_queue_preserves_normalized_bindings_and_closed_cursor_errors() {
    let (mut server, address, budget) = server().await;
    for (failure, status, code) in [
        (SessionError::CursorInvalid, 400, "invalid_cursor"),
        (SessionError::CursorExpired, 410, "cursor_expired"),
        (SessionError::CursorIncompatible, 409, "cursor_incompatible"),
        (
            SessionError::CursorUnavailable,
            503,
            "authority_unavailable",
        ),
    ] {
        let client = tokio::spawn(exchange(
            address,
            format!(
                "GET /management/v1/jobs?kind=export&state=paused&created_from_us=-1&page_size=300&cursor=abc-_12 HTTP/1.1\r\nHost: {address}\r\n{}\r\n",
                authenticated()
            ),
        ));
        let received = tokio::time::timeout(Duration::from_secs(1), server.commands.recv())
            .await
            .unwrap()
            .unwrap();
        let transport::Operation::JobList {
            session,
            csrf,
            request,
            ..
        } = received.operation
        else {
            panic!("wrong operation");
        };
        assert_eq!(session.expose_hex().as_str(), "11".repeat(32));
        assert_eq!(csrf.expose_hex().as_str(), "22".repeat(32));
        assert_eq!(request.query.page_size(), 200);
        assert_eq!(
            request.query.state(),
            Some(bigbrotr::export::execution::ExportState::Paused)
        );
        assert_eq!(request.query.created_from_us(), Some(-1));
        assert_eq!(
            request.cursor.as_deref().map(|v| v.as_str()),
            Some("abc-_12")
        );
        received.completion.finish(Err(failure));
        let response = client.await.unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(
            headers.starts_with(&format!("HTTP/1.1 {status} ")),
            "{response}"
        );
        assert!(headers.contains("cache-control: no-store\r\n"));
        assert!(!headers.contains("set-cookie:"));
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(value["error"]["code"], code);
        assert!(!body.contains("abc-_12"));
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn listing_refuses_ambiguous_filters_bodies_and_head_without_queueing() {
    let (mut server, address, budget) = server().await;
    for (method, query, body, status) in [
        ("HEAD", "", "", 405),
        ("GET", "?page_size=2&page_size=2", "", 400),
        ("GET", "?cursor=%61bc", "", 400),
        ("GET", "?kind=import", "", 400),
        ("GET", "?state=paused&extra=1", "", 400),
        ("GET", "", "{}", 400),
    ] {
        let response = exchange(address,format!(
            "{method} /management/v1/jobs{query} HTTP/1.1\r\nHost: {address}\r\nContent-Length: {}\r\n{}\r\n{body}",body.len(),authenticated()
        )).await;
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
async fn operational_profile_refuses_valid_acquisition_before_queueing() {
    let (mut server, address, budget) = server().await;
    let body = r#"{"kind":"acquisition","scope_id":"04040404040404040404040404040404","deadline_us":"120000000","expected_config_revision":"5","expected_policy_revision":"6"}"#;
    let response = exchange(address, request(address, &authenticated(), body)).await;
    assert!(response.starts_with("HTTP/1.1 400 "), "{response}");
    assert!(server.commands.try_recv().is_err());
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}
