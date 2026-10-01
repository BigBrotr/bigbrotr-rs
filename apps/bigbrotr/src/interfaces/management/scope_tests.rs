use super::*;

async fn scope_server(bytes: u64) -> (transport::Server, SocketAddr, ResourceBudget) {
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            32 << 20,
            1 << 20,
            ResourceLimits::new(bytes, 34, 0, 1 << 20),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = transport::Server::from_listener_profile(
        listener,
        budget.clone(),
        delivery_gate(Duration::from_secs(5)),
        None,
        Profile::Initial {
            public_routes: true,
        },
    )
    .unwrap();
    (server, address, budget)
}
fn request(address: SocketAddr, headers: &str, body: &str) -> String {
    format!(
        "POST /management/v1/acquisition-scopes HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}\r\n{body}",
        body.len()
    )
}
#[tokio::test]
async fn scope_registration_requires_session_before_accepting_input() {
    let (mut server, address, budget) = scope_server(24 << 20).await;
    let response = exchange(address, request(address, "", "{}")).await;
    assert!(response.starts_with("HTTP/1.1 401 "), "{response}");
    assert!(server.commands.try_recv().is_err());
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

fn auth() -> String {
    format!(
        "Cookie: BigBrotrLocalSession={}\r\nX-CSRF-Token: {}\r\nIdempotency-Key: {}\r\n",
        "11".repeat(32),
        "22".repeat(32),
        "08".repeat(16)
    )
}
fn input() -> String {
    serde_json::json!({
        "endpoint_id":"44".repeat(16), "access_scope_id":"55".repeat(16),
        "visibility":{"dataset_id":"11".repeat(16),"dictionary_kind":4,
            "allocation_domain":"66".repeat(16),"ordinal":"7"},
        "selector":{"version":1,"filters":[{"kinds":[2,1,1]}],
            "history_since":0,"ephemeral":"retain"},
        "expected_config_revision":"2", "expected_policy_revision":"3"
    })
    .to_string()
}

#[tokio::test]
async fn scope_transport_preserves_normalized_intent_and_receipt() {
    use bigbrotr::management::acquisition_scope::{BODY_BYTES, ScopeReceipt};
    let (mut server, address, budget) = scope_server(24 << 20).await;
    for created in [true, false] {
        let mut body = input();
        if created {
            body.extend(std::iter::repeat_n(' ', BODY_BYTES - body.len()));
        }
        let mut client = tokio::spawn(exchange(address, request(address, &auth(), &body)));
        let received = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                command=server.commands.recv()=>command.unwrap(),
                response=&mut client=>panic!("valid scope refused: {}",response.unwrap()),
            }
        })
        .await
        .unwrap();
        let transport::Operation::AcquisitionScope {
            session,
            csrf,
            key,
            command,
        } = received.operation
        else {
            panic!("wrong operation");
        };
        assert_eq!(session.expose_hex().as_str(), "11".repeat(32));
        assert_eq!(csrf.expose_hex().as_str(), "22".repeat(32));
        assert_eq!(key.to_string(), "08".repeat(16));
        assert_eq!(command.endpoint_id.to_string(), "44".repeat(16));
        assert_eq!(command.access_scope_id.to_string(), "55".repeat(16));
        assert_eq!(
            command.visibility.allocation_domain.to_string(),
            "66".repeat(16)
        );
        assert_eq!(command.visibility.ordinal.get(), 7);
        assert_eq!(command.expected_config_revision.get(), 2);
        assert_eq!(command.expected_policy_revision.get(), 3);
        assert_eq!(
            command.document(),
            br#"{"version":1,"filters":[{"kinds":[1,2]}],"history_since":0,"ephemeral":"retain"}"#
        );
        drop(command);
        received
            .completion
            .finish(Ok(transport::Reply::AcquisitionScope(ScopeReceipt {
                command_key: key,
                scope_id: "77".repeat(16).parse().unwrap(),
                accepted_at_us: i64::MAX,
                created,
            })));
        let response = client.await.unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(headers.starts_with(if created {
            "HTTP/1.1 202 "
        } else {
            "HTTP/1.1 200 "
        }));
        assert!(
            !headers.contains("set-cookie:") && headers.contains("cache-control: no-store\r\n")
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).unwrap(),
            serde_json::json!({
                "format":"bigbrotr-acquisition-scope-registration/1","command_key":"08".repeat(16),
                "scope_id":"77".repeat(16),"accepted_at_us":"9223372036854775807",
                "created":created,"authority":"identity-only"
            })
        );
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn scope_transport_retains_large_reservation_after_http_cancellation() {
    use bigbrotr::management::acquisition_scope::REQUEST_BYTES;
    let (mut server, address, budget) = scope_server(24 << 20).await;
    let client = tokio::spawn(exchange(address, request(address, &auth(), &input())));
    let command = tokio::time::timeout(Duration::from_secs(1), server.commands.recv())
        .await
        .unwrap()
        .unwrap();
    let second = exchange(address, request(address, &auth(), &input())).await;
    assert!(second.starts_with("HTTP/1.1 429 "), "{second}");
    assert!(server.commands.try_recv().is_err());
    // HTTP task owns its separate connection grant. Stop and join it while the
    // queued operation retains the entire selector reservation, not just64KiB.
    server.stop().await.unwrap();
    assert!(client.await.unwrap().is_empty());
    assert!(command.completion.abandoned());
    assert_eq!(
        budget.snapshot().used(),
        ResourceLimits::new(REQUEST_BYTES, 1, 0, 0)
    );
    drop(command);
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn scope_reservation_refuses_before_body_and_does_not_overcharge_small_commands() {
    use bigbrotr::management::acquisition_scope::REQUEST_BYTES;
    let (mut server, address, budget) = scope_server(REQUEST_BYTES - 1).await;
    // An unreceived body must not make the capacity refusal wait for its bytes.
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        exchange(
            address,
            request(address, &auth(), "").replace("Content-Length: 0", "Content-Length: 99"),
        ),
    )
    .await
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 429 "), "{response}");
    assert!(server.commands.try_recv().is_err());
    let client = tokio::spawn(exchange(
        address,
        login_request(address, "", r#"{"username":"alice","password":"fixture"}"#),
    ));
    let command = tokio::time::timeout(Duration::from_secs(1), server.commands.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        command.operation,
        transport::Operation::Login { .. }
    ));
    command.completion.finish(Err(SessionError::Denied));
    assert!(client.await.unwrap().starts_with("HTTP/1.1 401 "));
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn scope_transport_refuses_ambiguous_auth_and_closed_shape_before_queueing() {
    use bigbrotr::management::acquisition_scope::BODY_BYTES;
    let (mut server, address, budget) = scope_server(24 << 20).await;
    let body = input();
    let headers = auth();
    let key = format!("Idempotency-Key: {}\r\n", "08".repeat(16));
    let csrf = format!("X-CSRF-Token: {}\r\n", "22".repeat(32));
    for (headers, body, status) in [
        (headers.replace(&csrf, ""), body.clone(), 403),
        (format!("{headers}{csrf}"), body.clone(), 403),
        (headers.replace(&key, ""), body.clone(), 400),
        (format!("{headers}{key}"), body.clone(), 400),
        (
            headers.clone(),
            body.replace("\"selector\":", "\"actor_id\":\"forged\",\"selector\":"),
            400,
        ),
        (headers.clone(), body.replace("[2,1,1]", "[]"), 400),
        (headers.clone(), body.replace("\"3\"", "\"03\""), 400),
        (headers.clone(), "x".repeat(BODY_BYTES + 1), 413),
    ] {
        let response = exchange(address, request(address, &headers, &body)).await;
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
async fn scope_transport_accepts_one_byte_http_chunks_with_a_bounded_body() {
    let (mut server, address, budget) = scope_server(24 << 20).await;
    let mut body = input();
    body.extend(std::iter::repeat_n(' ', 65536 - body.len()));
    let mut chunks = String::with_capacity(body.len() * 6 + 5);
    for byte in body.bytes() {
        chunks.push_str("1\r\n");
        chunks.push(char::from(byte));
        chunks.push_str("\r\n");
    }
    chunks.push_str("0\r\n\r\n");
    let wire = format!(
        "POST /management/v1/acquisition-scopes HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n{}\r\n{chunks}",
        auth()
    );
    let mut client = tokio::spawn(exchange(address, wire));
    let command = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            command=server.commands.recv()=>command.unwrap(),
            response=&mut client=>panic!("chunked scope refused: {}",response.unwrap()),
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        command.operation,
        transport::Operation::AcquisitionScope { .. }
    ));
    drop(command.operation);
    command.completion.finish(Err(SessionError::Denied));
    assert!(client.await.unwrap().starts_with("HTTP/1.1 401 "));
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}
