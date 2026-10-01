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
        Profile::Sessions,
    )
    .unwrap();
    (server, address, budget, gate)
}

#[tokio::test]
async fn initial_profile_queues_only_the_three_session_operations() {
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
        let result =
            Management::start(&root, budget.clone(), window, None, Profile::Sessions).await;
        assert!(
            matches!(result, Err((3, ref message)) if message == "private management cannot reserve one complete login")
        );
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}
