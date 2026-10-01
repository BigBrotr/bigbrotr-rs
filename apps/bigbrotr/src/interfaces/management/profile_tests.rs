use super::*;

async fn sessions() -> (transport::Server, SocketAddr, ResourceBudget, DeliveryGate) {
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            16 << 20,
            1 << 20,
            ResourceLimits::new(8 << 20, 34, 0, 1 << 20),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let gate = delivery_gate(Duration::from_secs(5));
    let server = transport::Server::from_listener_profile(
        listener,
        budget.clone(),
        gate.clone(),
        None,
        Profile::Initial {
            public_routes: true,
        },
    )
    .unwrap();
    (server, address, budget, gate)
}

#[tokio::test]
async fn initial_profile_preserves_sessions_and_refuses_other_unwired_operations() {
    let (mut server, address, budget, _) = sessions().await;
    for (method, path, status) in [
        ("GET", "/v1/resources/event_history", 404),
        ("POST", "/v1/resources/event_history", 405),
        ("GET", "/management/v1/jobs", 404),
        ("POST", "/management/v1/jobs", 405),
        (
            "GET",
            "/management/v1/jobs/01010101010101010101010101010101",
            404,
        ),
        (
            "POST",
            "/management/v1/jobs/01010101010101010101010101010101/resume",
            405,
        ),
        (
            "PUT",
            "/management/v1/principals/01010101010101010101010101010101/grants",
            405,
        ),
        (
            "POST",
            "/management/v1/sessions/current/reauthenticate",
            405,
        ),
    ] {
        let answer = exchange(address, format!(
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Length: 0\r\n\r\n"
        )).await;
        assert!(
            answer.starts_with(&format!("HTTP/1.1 {status} ")),
            "{answer}"
        );
        assert!(server.commands.try_recv().is_err());
    }
    let client = tokio::spawn(exchange(
        address,
        login_request(
            address,
            "",
            r#"{"username":"alice","password":"Fixture password"}"#,
        ),
    ));
    let command = server.commands.recv().await.unwrap();
    assert!(matches!(
        command.operation,
        transport::Operation::Login { .. }
    ));
    command.completion.finish(Err(SessionError::Denied));
    assert!(client.await.unwrap().starts_with("HTTP/1.1 401 "));

    for (method, path) in [
        ("GET", "/management/v1/me"),
        ("DELETE", "/management/v1/sessions/current"),
    ] {
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nCookie: BigBrotrLocalSession={}\r\nX-CSRF-Token: {}\r\nContent-Length: 0\r\n\r\n",
            "11".repeat(32),
            "22".repeat(32)
        );
        let client = tokio::spawn(exchange(address, request));
        let command = server.commands.recv().await.unwrap();
        assert!(matches!(
            (method, command.operation),
            ("GET", transport::Operation::Me(_)) | ("DELETE", transport::Operation::Logout { .. })
        ));
        command.completion.finish(Err(SessionError::Denied));
        assert!(client.await.unwrap().starts_with("HTTP/1.1 401 "));
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn parent_stop_closes_http_while_command_credit_stays_with_the_owner() {
    let (mut server, address, budget, gate) = sessions().await;
    let stop = Shutdown {
        delivery: gate,
        server: server.stop_signal(),
    };
    let client = tokio::spawn(exchange(
        address,
        login_request(
            address,
            "",
            r#"{"username":"alice","password":"Fixture password"}"#,
        ),
    ));
    let command = server.commands.recv().await.unwrap();
    assert!(!command.completion.abandoned());
    stop.stop_admission();
    let answer = tokio::time::timeout(Duration::from_secs(1), client)
        .await
        .unwrap()
        .unwrap();
    assert!(answer.is_empty(), "{answer}");
    assert!(command.completion.abandoned());
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(command);
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn session_start_requires_shared_bytes_and_items_before_binding() {
    let mut root = crate::config::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config/recipes/archive-only.toml"),
    )
    .unwrap();
    // Keep the address occupied: a bind attempt would produce an IO refusal,
    // distinct from the required capacity refusal before opening the listener.
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    root.values.insert(
        "administration.listen.port".into(),
        serde_json::json!(reserved.local_addr().unwrap().port()),
    );
    for (bytes, items) in [(80 << 20, 2), (64 << 20, 3)] {
        let budget = ResourceBudget::new(
            ProcessEnvelope::new(
                128 << 20,
                8 << 20,
                ResourceLimits::new(bytes, items, 0, 8 << 20),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let (_, window) = delivery_gate_with_observer(Duration::from_secs(5));
        let result = Management::start(
            &root,
            budget.clone(),
            window,
            None,
            Profile::Initial {
                public_routes: true,
            },
        )
        .await;
        assert!(
            matches!(result, Err((3, ref message)) if message == "private management cannot reserve one complete login")
        );
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}

#[tokio::test]
async fn initial_relay_enrollment_requires_session_before_accepting_input() {
    let (mut server, address, budget, _) = sessions().await;
    let response = exchange(address, format!(
        "POST /management/v1/relay-enrollments HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{{}}"
    )).await;
    assert!(response.starts_with("HTTP/1.1 401 "), "{response}");
    assert!(server.commands.try_recv().is_err());
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

fn enrollment_input() -> serde_json::Value {
    serde_json::json!({
        "endpoint_id":"44".repeat(16),
        "visibility":{"dataset_id":"11".repeat(16),"dictionary_kind":4,
            "allocation_domain":"55".repeat(16),"ordinal":"7"},
        "route":{"class":"private","cidr":"10.2.0.0/16"},"enabled":true,
        "expected_generation":null,"expected_config_revision":"2","expected_policy_revision":"3"
    })
}

fn enrollment_request(address: SocketAddr, headers: &str, body: &str) -> String {
    format!(
        "POST /management/v1/relay-enrollments HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}\r\n{body}",
        body.len()
    )
}

fn enrollment_auth() -> String {
    format!(
        "Cookie: BigBrotrLocalSession={}\r\nX-CSRF-Token: {}\r\nIdempotency-Key: {}\r\n",
        "11".repeat(32),
        "22".repeat(32),
        "08".repeat(16)
    )
}

#[tokio::test]
async fn enrollment_transport_preserves_exact_intent_and_acceptance_only_receipts() {
    use bigbrotr::management::relay_enrollment::EnrollmentReceipt;
    let (mut server, address, budget, _) = sessions().await;
    let body = enrollment_input();
    for created in [true, false] {
        let client = tokio::spawn(exchange(
            address,
            enrollment_request(address, &enrollment_auth(), &body.to_string()),
        ));
        let received = tokio::time::timeout(Duration::from_secs(1), server.commands.recv())
            .await
            .unwrap()
            .unwrap();
        let transport::Operation::RelayEnrollment {
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
        assert_eq!(serde_json::to_value(&command).unwrap(), body);
        received
            .completion
            .finish(Ok(transport::Reply::RelayEnrollment(EnrollmentReceipt {
                command_key: key,
                endpoint_id: command.endpoint_id,
                access_scope_id: "66".repeat(16).parse().unwrap(),
                route_id: "77".repeat(16).parse().unwrap(),
                enabled: true,
                generation: u64::MAX.to_string().try_into().unwrap(),
                accepted_at_us: i64::MAX,
                created,
            })));
        let response = client.await.unwrap();
        let (headers, returned) = response.split_once("\r\n\r\n").unwrap();
        assert!(headers.starts_with(if created {
            "HTTP/1.1 202 "
        } else {
            "HTTP/1.1 200 "
        }));
        assert!(
            !headers.contains("set-cookie:") && headers.contains("cache-control: no-store\r\n")
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(returned).unwrap(),
            serde_json::json!({
                "format":"bigbrotr-relay-enrollment/1", "command_key":"08".repeat(16),
                "endpoint_id":"44".repeat(16),"access_scope_id":"66".repeat(16),"route_id":"77".repeat(16),
                "enabled":true,"generation":"18446744073709551615","accepted_at_us":"9223372036854775807",
                "created":created,"application":"unobserved","authority":"acceptance-only"
            })
        );
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn enrollment_transport_refuses_ambiguous_authority_and_invalid_shapes_before_queueing() {
    let (mut server, address, budget, _) = sessions().await;
    let body = enrollment_input().to_string();
    let auth = enrollment_auth();
    let key = format!("Idempotency-Key: {}\r\n", "08".repeat(16));
    let csrf = format!("X-CSRF-Token: {}\r\n", "22".repeat(32));
    let mut wrong_kind = enrollment_input();
    wrong_kind["visibility"]["dictionary_kind"] = 1.into();
    let mut absent_generation = enrollment_input();
    absent_generation
        .as_object_mut()
        .unwrap()
        .remove("expected_generation");
    for (headers, input, status) in [
        (auth.replace(&csrf, ""), body.clone(), 403),
        (format!("{auth}{csrf}"), body.clone(), 403),
        (auth.replace(&key, ""), body.clone(), 400),
        (format!("{auth}{key}"), body.clone(), 400),
        (auth.clone(), wrong_kind.to_string(), 400),
        (auth.clone(), absent_generation.to_string(), 400),
        (
            auth.clone(),
            body.replace("\"enabled\":true", "\"enabled\":true,\"enabled\":true"),
            400,
        ),
        (
            auth.clone(),
            body.replace("\"enabled\":true", "\"enabled\":false"),
            400,
        ),
        (
            auth.clone(),
            body.replace("10.2.0.0/16", "10.2.0.1/16"),
            400,
        ),
        (auth.clone(), body.replace("\"private\"", "\"tor\""), 400),
        (
            auth.clone(),
            body.replace(
                "\"enabled\":true",
                "\"enabled\":true,\"root\":\"/tmp/other\"",
            ),
            400,
        ),
        (auth.clone(), "x".repeat(8193), 413),
    ] {
        let response = exchange(address, enrollment_request(address, &headers, &input)).await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status} ")),
            "{response}"
        );
        assert!(server.commands.try_recv().is_err());
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}
