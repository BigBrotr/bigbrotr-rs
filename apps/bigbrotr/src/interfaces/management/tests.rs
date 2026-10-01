use super::*;

#[path = "grant_tests.rs"]
mod grants;
#[path = "job_tests.rs"]
mod jobs;
#[path = "profile_tests.rs"]
mod profiles;

#[tokio::test]
async fn private_history_requires_session_before_admitting_read_work() {
    let (mut server, address, budget) = server().await;
    for method in ["GET", "POST"] {
        let request = format!(
            "{method} /v1/resources/event_history HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Length: 0\r\n\r\n"
        );
        let response = exchange(address, request).await;
        assert!(response.starts_with("HTTP/1.1 401 "), "{response}");
        assert!(server.commands.try_recv().is_err());
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn reauthentication_requires_session_and_explicit_csrf_before_queueing() {
    let (mut server, address, budget) = server().await;
    let path = "/management/v1/sessions/current/reauthenticate";
    for (extra, expected) in [
        (String::new(), 401),
        (
            format!("Cookie: BigBrotrLocalSession={}\r\n", "11".repeat(32)),
            403,
        ),
    ] {
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Type: application/json\r\nContent-Length: 2\r\n{extra}\r\n{{}}"
        );
        let response = exchange(address, request).await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {expected} ")),
            "{response}"
        );
        assert!(server.commands.try_recv().is_err());
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}
use axum::http::{HeaderMap, HeaderValue, Method, Uri};

#[tokio::test]
async fn reauthentication_transport_binds_key_command_and_returns_no_cookies() {
    let (mut server, address, budget) = server().await;
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/management/scope-grant.json"
    ))
    .unwrap();
    let command = &vector["vectors"][1]["command"];
    let body =
        serde_json::json!({"password":"One exact private password","command":command}).to_string();
    let header = format!(
        "Cookie: BigBrotrLocalSession={}\r\nX-CSRF-Token: {}\r\n",
        "11".repeat(32),
        "22".repeat(32)
    );
    let request = |extra: &str, body: &str| {
        format!(
            "POST /management/v1/sessions/current/reauthenticate HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{header}{extra}\r\n{body}",
            body.len()
        )
    };
    for extra in [
        String::new(),
        "Idempotency-Key: bad\r\n".into(),
        format!(
            "Idempotency-Key: {}\r\nIdempotency-Key: {}\r\n",
            "04".repeat(16),
            "04".repeat(16)
        ),
    ] {
        assert!(
            exchange(address, request(&extra, &body))
                .await
                .starts_with("HTTP/1.1 400 ")
        );
        assert!(server.commands.try_recv().is_err());
    }
    let extra = format!("Idempotency-Key: {}\r\n", "04".repeat(16));
    let client = tokio::spawn(exchange(address, request(&extra, &body)));
    let received = server.commands.recv().await.unwrap();
    let transport::Operation::Reauthenticate {
        session,
        csrf,
        key,
        command: actual,
        password,
        peer,
    } = received.operation
    else {
        panic!("wrong queued operation");
    };
    assert_eq!(session.expose_hex().as_str(), "11".repeat(32));
    assert_eq!(csrf.expose_hex().as_str(), "22".repeat(32));
    assert_eq!(key.to_string(), "04".repeat(16));
    assert_eq!(serde_json::to_value(actual).unwrap(), *command);
    assert!(password.as_str() == "One exact private password");
    assert_eq!(peer, address.ip());
    received
        .completion
        .finish(Ok(transport::Reply::Reauthenticated(
            bigbrotr::management::reauthentication::ReauthenticationProof {
                id: "09".repeat(16).parse().unwrap(),
                command_key: key,
                command_digest: "03".repeat(32).parse().unwrap(),
                expires_at_us: 123,
            },
        )));
    let response = client.await.unwrap();
    assert!(response.starts_with("HTTP/1.1 201 "));
    assert!(!response.contains("set-cookie:") && !response.contains("One exact private password"));
    let reply: serde_json::Value =
        serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(
        reply,
        serde_json::json!({"format":"bigbrotr-reauthentication/1","proof_id":"09".repeat(16),"command_key":"04".repeat(16),"command_digest":"03".repeat(32),"expires_at_us":"123","authority":"same-session-command-only"})
    );
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

fn headers(host: &str, origin: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("host", HeaderValue::from_str(host).unwrap());
    if let Some(origin) = origin {
        headers.insert("origin", HeaderValue::from_str(origin).unwrap());
    }
    headers
}

#[test]
fn browser_boundary_rejects_ambiguous_and_foreign_authorities_before_authentication() {
    let boundary = BrowserBoundary::loopback("127.0.0.1:8081".parse().unwrap()).unwrap();
    let uri: Uri = "/management/v1/sessions".parse().unwrap();
    let good = headers("127.0.0.1:8081", Some("http://127.0.0.1:8081"));
    assert!(boundary.check(&Method::POST, &uri, &good).is_ok());
    for origin in [
        None,
        Some("null"),
        Some("http://127.0.0.1:8080"),
        Some("http://localhost:8081"),
        Some("http://127.0.0.1:8081/"),
    ] {
        assert!(
            boundary
                .check(&Method::POST, &uri, &headers("127.0.0.1:8081", origin))
                .is_err()
        );
    }
    for host in [
        "localhost:8081",
        "127.0.0.1:8080",
        "127.0.0.1:8081.evil.example",
    ] {
        assert!(
            boundary
                .check(
                    &Method::POST,
                    &uri,
                    &headers(host, Some("http://127.0.0.1:8081"))
                )
                .is_err()
        );
    }
    for field in ["host", "origin"] {
        let mut duplicate = good.clone();
        duplicate.append(field, good[field].clone());
        assert!(boundary.check(&Method::POST, &uri, &duplicate).is_err());
    }
    assert!(
        boundary
            .check(
                &Method::POST,
                &"http://127.0.0.1:8081/management/v1/sessions"
                    .parse()
                    .unwrap(),
                &good
            )
            .is_err()
    );
    assert!(
        boundary
            .check(&Method::GET, &uri, &headers("127.0.0.1:8081", None))
            .is_ok()
    );
    assert!(
        boundary
            .check(
                &Method::GET,
                &uri,
                &headers("127.0.0.1:8081", Some("http://127.0.0.1:8080"))
            )
            .is_err()
    );
}

#[test]
fn browser_origin_uses_ipv6_brackets_and_omits_default_http_port() {
    let uri: Uri = "/management/v1/me".parse().unwrap();
    for (address, host, origin) in [
        ("127.0.0.1:80", "127.0.0.1", "http://127.0.0.1"),
        ("[::1]:8081", "[::1]:8081", "http://[::1]:8081"),
        ("[::1]:80", "[::1]", "http://[::1]"),
    ] {
        let boundary = BrowserBoundary::loopback(address.parse().unwrap()).unwrap();
        assert!(
            boundary
                .check(&Method::POST, &uri, &headers(host, Some(origin)))
                .is_ok()
        );
    }
    for address in [
        "0.0.0.0:8081",
        "192.168.1.2:8081",
        "[::]:8081",
        "127.0.0.1:0",
    ] {
        assert!(BrowserBoundary::loopback(address.parse().unwrap()).is_err());
    }
}

use bigbrotr::management::sessions::{BrowserSecret, IssuedSession, SessionObservation};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn server() -> (transport::Server, SocketAddr, ResourceBudget) {
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
    (
        transport::Server::from_listener(
            listener,
            budget.clone(),
            delivery_gate(Duration::from_secs(5)),
            None,
        )
        .unwrap(),
        address,
        budget,
    )
}

#[tokio::test]
async fn actual_static_http_preserves_mount_headers_authority_and_held_bytes() {
    for base in ["/", "/control/"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::write(root.join("index.html"), b"hello").unwrap();
        std::fs::write(root.join("manifest.json"), format!(r#"{{"format":"bigbrotr-private-assets/1","base_path":"{base}","files":[{{"path":"index.html","bytes":5,"sha256":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}]}}"#)).unwrap();
        let budget = ResourceBudget::new(
            ProcessEnvelope::new(
                64 << 20,
                1 << 20,
                ResourceLimits::new(32 << 20, 35, 0, 1 << 20),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let assets = Assets::load(&root, base, &budget).unwrap();
        // Startup verified and retained the bytes. Later disk changes cannot
        // replace an admitted executable asset or silently require new IO.
        std::fs::write(root.join("index.html"), b"tampered").unwrap();
        std::fs::remove_file(root.join("manifest.json")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let gate = delivery_gate(Duration::from_secs(5));
        let mut server =
            transport::Server::from_listener(listener, budget.clone(), gate.clone(), Some(assets))
                .unwrap();
        let request = |method: &str, path: &str, extra: &str| {
            format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\n{extra}\r\n")
        };
        for method in ["GET", "HEAD"] {
            let response = exchange(address, request(method, base, "")).await;
            let (headers, body) = response.split_once("\r\n\r\n").unwrap();
            assert!(headers.starts_with("HTTP/1.1 200 "));
            assert!(headers.contains("content-type: text/html; charset=utf-8\r\n"));
            assert!(headers.contains("content-length: 5\r\n"));
            assert!(headers.contains("cache-control: no-store\r\n"));
            assert!(headers.contains("content-security-policy: frame-ancestors 'none'; base-uri 'none'; object-src 'none'\r\n"));
            assert!(headers.contains("x-content-type-options: nosniff\r\n"));
            assert!(headers.contains("referrer-policy: same-origin\r\n"));
            assert!(!headers.contains("set-cookie:"));
            assert_eq!(body, if method == "GET" { "hello" } else { "" });
        }
        for (method, path, extra, status) in [
            ("GET", format!("{base}manifest.json"), String::new(), 404),
            ("GET", format!("{base}missing.js"), String::new(), 404),
            ("GET", format!("{base}../index.html"), String::new(), 404),
            ("GET", format!("{base}%69ndex.html"), String::new(), 404),
            ("GET", format!("{base}?token=private"), String::new(), 400),
            (
                "GET",
                base.into(),
                "Origin: http://127.0.0.1:1\r\n".into(),
                403,
            ),
            (
                "POST",
                base.into(),
                format!("Origin: http://{address}\r\n"),
                405,
            ),
            ("GET", "/management/v1/me".into(), String::new(), 401),
        ] {
            let response = exchange(address, request(method, &path, &extra)).await;
            assert!(
                response.starts_with(&format!("HTTP/1.1 {status} ")),
                "{path}: {response}"
            );
            assert!(!response.contains("hello"));
            if status == 405 {
                assert!(response.contains("allow: GET, HEAD\r\n"));
            }
        }
        if base != "/" {
            for path in ["/", "/control", "/index.html"] {
                assert!(
                    exchange(address, request("GET", path, ""))
                        .await
                        .starts_with("HTTP/1.1 404 ")
                );
            }
        }
        assert!(server.commands.try_recv().is_err());
        gate.revoke();
        let mut socket = TcpStream::connect(address).await.unwrap();
        let sent = socket.write_all(request("GET", base, "").as_bytes()).await;
        assert!(
            sent.is_ok()
                || sent.is_err_and(|e| matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ))
        );
        let mut denied = Vec::new();
        let closed = tokio::time::timeout(Duration::from_secs(1), socket.read_to_end(&mut denied))
            .await
            .unwrap();
        // A gate denied before the request is read may close with RST rather
        // than FIN. Neither outcome may expose headers or admitted asset bytes.
        assert!(
            closed.is_ok()
                || closed.is_err_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset)
        );
        assert!(denied.is_empty());
        server.stop().await.unwrap();
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}

fn delivery_gate(ttl: Duration) -> DeliveryGate {
    delivery_gate_with_observer(ttl).0
}

fn delivery_gate_with_observer(
    ttl: Duration,
) -> (
    DeliveryGate,
    bigbrotr::runtime::policy_authority::DeliveryWindow,
) {
    use bigbrotr::runtime::policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl};
    let dataset = "01010101010101010101010101010101".parse().unwrap();
    let process = "02020202020202020202020202020202".parse().unwrap();
    let epoch = bigbrotr_model::PolicyEpoch::new(1);
    let mut lease =
        PolicyLeaseTracker::new(dataset, process, epoch, PolicyLeaseTtl::new(ttl).unwrap())
            .unwrap();
    let ticket = lease.begin_refresh().unwrap();
    lease
        .complete_refresh(
            &ticket,
            LeaseResponse {
                dataset,
                process_incarnation: process,
                policy_epoch: epoch,
                ttl,
            },
        )
        .unwrap();
    (
        DeliveryGate::new(lease.delivery_window(epoch).unwrap()),
        lease.delivery_window(epoch).unwrap(),
    )
}

#[tokio::test]
async fn large_static_response_retains_custody_and_stops_after_revocation() {
    use tokio::net::TcpSocket;
    const LENGTH: usize = 2 * 1024 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    std::fs::write(root.join("index.html"), vec![b'A'; LENGTH]).unwrap();
    // Independently frozen SHA256 of exactly 2 MiB of ASCII A.
    std::fs::write(root.join("manifest.json"), r#"{"format":"bigbrotr-private-assets/1","base_path":"/","files":[{"path":"index.html","bytes":2097152,"sha256":"5b766f6d76a999636fd93b4e039d5a32187f84a19c0950449f0c721da0223914"}]}"#).unwrap();
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            64 << 20,
            1 << 20,
            ResourceLimits::new(32 << 20, 35, 0, 1 << 20),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let assets = Assets::load(&root, "/", &budget).unwrap();
    // Bound both kernel queues below the body size. This is real TCP; no
    // in-memory Pending implementation stands in for the slow consumer.
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_send_buffer_size(4096).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(8).unwrap();
    let address = listener.local_addr().unwrap();
    let (gate, mut observer) = delivery_gate_with_observer(Duration::from_secs(5));
    let mut server =
        transport::Server::from_listener(listener, budget.clone(), gate.clone(), Some(assets))
            .unwrap();
    let peer = TcpSocket::new_v4().unwrap();
    peer.set_recv_buffer_size(16 * 1024).unwrap();
    let mut peer = peer.connect(address).await.unwrap();
    peer.write_all(format!("GET / HTTP/1.1\r\nHost: {address}\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let headers = tokio::time::timeout(Duration::from_secs(1), async {
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(peer.read_u8().await.unwrap());
            assert!(headers.len() < 8192);
        }
        headers
    })
    .await
    .unwrap();
    let headers = String::from_utf8(headers).unwrap();
    assert!(headers.starts_with("HTTP/1.1 200 "));
    assert!(headers.contains("content-length: 2097152\r\n"));
    tokio::time::sleep(Duration::from_millis(100)).await;
    // A fully queued response releases its connection grant. Require the
    // unfinished response and retained immutable bundle before revoking.
    assert_eq!(budget.snapshot().active_permits(), 2);
    observer.check().unwrap();
    gate.revoke();
    let mut body = Vec::new();
    let closed = tokio::time::timeout(
        Duration::from_secs(2),
        peer.take((LENGTH + 1) as u64).read_to_end(&mut body),
    )
    .await
    .expect("revoked response did not close before the 5s connection deadline");
    // This window has the gate's exact deadline, established before accept.
    // Neither lease expiry (including suspend) nor the later connection timeout
    // may substitute for revocation in this experiment.
    observer.check().unwrap();
    assert!(
        closed.is_ok() || closed.is_err_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset)
    );
    assert!(!body.is_empty(), "expected a previously queued body prefix");
    assert!(
        body.len() < LENGTH,
        "revoked response delivered {} bytes of {LENGTH}",
        body.len()
    );
    assert!(body.iter().all(|byte| *byte == b'A'));
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn actually_blocked_socket_cannot_resume_writing_after_gate_revocation() {
    use std::{pin::Pin, task::Poll};
    use tokio::io::AsyncWrite;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut reader, _) = listener.accept().await.unwrap();
    client.writable().await.unwrap();
    let block = [42_u8; 16 * 1024];
    let mut accepted = 0;
    loop {
        match client.try_write(&block) {
            Ok(n) => {
                assert!(n > 0);
                accepted += n;
                assert!(
                    accepted < 64 * 1024 * 1024,
                    "OS did not block within explicit fixture bound"
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("unexpected socket error {e}"),
        }
    }
    assert!(accepted > 0);
    let gate = delivery_gate(Duration::from_secs(5));
    let mut writer =
        delivery::GuardedSocket::new(client, gate.clone(), delivery::ActionGate::new());
    // Already full at the OS, and the same pending writer remains alive.
    std::future::poll_fn(|cx| {
        loop {
            match Pin::new(&mut writer).poll_write(cx, &block) {
                Poll::Pending => return Poll::Ready(()),
                Poll::Ready(Ok(n)) => {
                    assert!(n > 0);
                    accepted += n;
                    assert!(
                        accepted < 64 * 1024 * 1024,
                        "fixture socket did not remain bounded"
                    );
                }
                Poll::Ready(Err(e)) => panic!("unexpected write error {e}"),
            }
        }
    })
    .await;
    gate.revoke();
    let drained = tokio::spawn(async move {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        bytes
    });
    let error = writer
        .write_all(b"must-not-be-delivered")
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    drop(writer);
    let bytes = tokio::time::timeout(Duration::from_secs(2), drained)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes.len(), accepted);
    assert!(bytes.iter().all(|b| *b == 42));
}

#[tokio::test]
async fn blocked_socket_action_expires_while_global_delivery_is_renewed() {
    blocked_action(false).await;
}
#[tokio::test]
async fn blocked_socket_pin_expires_while_action_and_global_delivery_remain_live() {
    blocked_action(true).await;
}
async fn blocked_action(pin: bool) {
    use std::{pin::Pin, task::Poll};
    use tokio::io::AsyncWrite;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut reader, _) = listener.accept().await.unwrap();
    client.writable().await.unwrap();
    let block = [42_u8; 16 * 1024];
    let mut accepted = 0;
    loop {
        match client.try_write(&block) {
            Ok(n) => {
                assert!(n > 0);
                accepted += n;
                assert!(
                    accepted < 64 * 1024 * 1024,
                    "OS did not block within explicit fixture bound"
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("unexpected socket error {e}"),
        }
    }
    assert!(accepted > 0);
    use bigbrotr::runtime::policy_authority::*;
    let dataset = "01010101010101010101010101010101".parse().unwrap();
    let process = "02020202020202020202020202020202".parse().unwrap();
    let epoch = bigbrotr_model::PolicyEpoch::new(1);
    let mut lease = PolicyLeaseTracker::new(
        dataset,
        process,
        epoch,
        PolicyLeaseTtl::new(Duration::from_secs(5)).unwrap(),
    )
    .unwrap();
    let ticket = lease.begin_refresh().unwrap();
    lease
        .complete_refresh(
            &ticket,
            LeaseResponse {
                dataset,
                process_incarnation: process,
                policy_epoch: epoch,
                ttl: Duration::from_secs(5),
            },
        )
        .unwrap();
    let action = delivery::ActionGate::new();
    action
        .bind(
            lease
                .begin_action(epoch)
                .unwrap()
                .complete(if pin {
                    Duration::from_secs(5)
                } else {
                    Duration::from_millis(100)
                })
                .unwrap(),
        )
        .unwrap();
    if pin {
        action
            .constrain_pin(Some(
                lease
                    .begin_action(epoch)
                    .unwrap()
                    .complete(Duration::from_millis(100))
                    .unwrap(),
            ))
            .unwrap();
    }
    let gate = DeliveryGate::new(lease.delivery_window(epoch).unwrap());
    let mut writer = delivery::GuardedSocket::new(client, gate.clone(), action);
    // Already full at the OS, and the same pending writer remains alive.
    std::future::poll_fn(|cx| {
        loop {
            match Pin::new(&mut writer).poll_write(cx, &block) {
                Poll::Pending => return Poll::Ready(()),
                Poll::Ready(Ok(n)) => {
                    assert!(n > 0);
                    accepted += n;
                    assert!(
                        accepted < 64 * 1024 * 1024,
                        "fixture socket did not remain bounded"
                    );
                }
                Poll::Ready(Err(e)) => panic!("unexpected write error {e}"),
            }
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    let ticket = lease.begin_refresh().unwrap();
    lease
        .complete_refresh(
            &ticket,
            LeaseResponse {
                dataset,
                process_incarnation: process,
                policy_epoch: epoch,
                ttl: Duration::from_secs(5),
            },
        )
        .unwrap();
    gate.renew(lease.delivery_window(epoch).unwrap()).unwrap();
    lease.check_delivery(epoch).unwrap();
    let drained = tokio::spawn(async move {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        bytes
    });
    let error = writer
        .write_all(b"must-not-be-delivered")
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    drop(writer);
    let bytes = tokio::time::timeout(Duration::from_secs(2), drained)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes.len(), accepted);
    assert!(bytes.iter().all(|b| *b == 42));
}

#[tokio::test]
async fn socket_delivery_checks_expiry_even_without_a_supervisor_poll() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut reader, _) = listener.accept().await.unwrap();
    let gate = delivery_gate(Duration::from_millis(50));
    let mut writer =
        delivery::GuardedSocket::new(client, gate.clone(), delivery::ActionGate::new());
    writer.write_all(b"before").await.unwrap();
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(
        writer.write_all(b"after").await.unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert!(
        gate.renew({
            use bigbrotr::runtime::policy_authority::*;
            let epoch = bigbrotr_model::PolicyEpoch::new(1);
            let dataset = "01010101010101010101010101010101".parse().unwrap();
            let process = "02020202020202020202020202020202".parse().unwrap();
            let mut lease = PolicyLeaseTracker::new(
                dataset,
                process,
                epoch,
                PolicyLeaseTtl::new(Duration::from_secs(5)).unwrap(),
            )
            .unwrap();
            let ticket = lease.begin_refresh().unwrap();
            lease
                .complete_refresh(
                    &ticket,
                    LeaseResponse {
                        dataset,
                        process_incarnation: process,
                        policy_epoch: epoch,
                        ttl: Duration::from_secs(5),
                    },
                )
                .unwrap();
            lease.delivery_window(epoch).unwrap()
        })
        .is_err()
    );
    drop(writer);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"before");
}
async fn exchange(address: SocketAddr, request: String) -> String {
    tokio::time::timeout(Duration::from_secs(7), async {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.unwrap();
        String::from_utf8(bytes).unwrap()
    })
    .await
    .unwrap()
}
fn login_request(address: SocketAddr, extra: &str, body: &str) -> String {
    format!(
        "POST /management/v1/sessions HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}\r\n{body}",
        body.len()
    )
}

#[tokio::test]
async fn actual_http_refusals_never_enqueue_authentication() {
    let (mut server, address, budget) = server().await;
    let body = r#"{"username":"owner","password":"correct horse battery"}"#;
    let valid = login_request(address, "", body);
    let oversized = "x".repeat(8193);
    for (request, status) in [
        (
            valid.replace(&format!("Host: {address}"), "Host: localhost"),
            403,
        ),
        (
            valid.replace(&format!("Origin: http://{address}"), "Origin: null"),
            403,
        ),
        (
            valid.replace(&format!("Origin: http://{address}\r\n"), ""),
            403,
        ),
        (
            login_request(address, &format!("Origin: http://{address}\r\n"), body),
            403,
        ),
        (login_request(address, "", &oversized), 413),
        (
            login_request(
                address,
                "",
                r#"{"username":"owner","password":"never-log-this","extra":1}"#,
            ),
            400,
        ),
    ] {
        let response = exchange(address, request).await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status} ")),
            "{response}"
        );
        assert!(response.contains("cache-control: no-store\r\n"));
        assert!(!response.contains("never-log-this"));
        assert!(server.commands.try_recv().is_err());
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn actual_http_keeps_separate_cookies_and_explicit_csrf_proof() {
    let (mut server, address, budget) = server().await;
    let body = r#"{"username":"owner","password":"correct horse battery"}"#;
    let client = tokio::spawn(exchange(address, login_request(address, "", body)));
    let command = server.commands.recv().await.unwrap();
    let transport::Operation::Login {
        username,
        password,
        peer,
    } = command.operation
    else {
        panic!("wrong command");
    };
    assert_eq!(username, "owner");
    assert_eq!(password.as_str(), "correct horse battery");
    assert_eq!(peer, address.ip());
    let session = "11".repeat(32);
    let csrf = "22".repeat(32);
    command
        .completion
        .finish(Ok(transport::Reply::Login(IssuedSession {
            session: BrowserSecret::parse_hex(session.as_bytes()).unwrap(),
            csrf: BrowserSecret::parse_hex(csrf.as_bytes()).unwrap(),
            observation: SessionObservation {
                principal: "01010101010101010101010101010101".parse().unwrap(),
                credential_revision: 1,
                grant_revision: 1,
                config_revision: u64::MAX,
                policy_revision: 1,
                expires_at_us: 50,
                idle_expires_at_us: 40,
                stored_actions: vec![],
            },
        })));
    let response = client.await.unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("HTTP/1.1 201 "));
    let cookies: Vec<_> = headers
        .split("\r\n")
        .filter(|s| s.starts_with("set-cookie:"))
        .collect();
    assert_eq!(cookies.len(), 2);
    assert_eq!(
        cookies[0],
        format!(
            "set-cookie: BigBrotrLocalSession={session}; Path=/; Max-Age=28800; SameSite=Strict; HttpOnly"
        )
    );
    assert_eq!(
        cookies[1],
        format!("set-cookie: BigBrotrLocalCsrf={csrf}; Path=/; Max-Age=28800; SameSite=Strict")
    );
    assert!(!body.contains(&session) && !body.contains(&csrf));
    assert!(body.contains("observation-only"));
    let observed: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(observed["config_revision"], "18446744073709551615");
    assert_eq!(observed["policy_revision"], "1");
    let logout = format!(
        "DELETE /management/v1/sessions/current HTTP/1.1\r\nHost: {address}\r\nOrigin: http://{address}\r\nCookie: BigBrotrLocalSession={session}; BigBrotrLocalCsrf={csrf}\r\n"
    );
    // Automatic cookies alone cannot supply the explicit command proof.
    assert!(
        exchange(address, format!("{logout}\r\n"))
            .await
            .starts_with("HTTP/1.1 403 ")
    );
    assert!(server.commands.try_recv().is_err());
    for known in [false, true] {
        let client = tokio::spawn(exchange(
            address,
            format!("{logout}X-CSRF-Token: {csrf}\r\n\r\n"),
        ));
        let command = server.commands.recv().await.unwrap();
        let transport::Operation::Logout {
            session: token,
            csrf: proof,
        } = command.operation
        else {
            panic!("wrong command");
        };
        assert_eq!(token.expose_hex().as_str(), session);
        assert_eq!(proof.expose_hex().as_str(), csrf);
        command.completion.finish(if known {
            Ok(transport::Reply::Logout)
        } else {
            Err(SessionError::CommitUnknown)
        });
        let response = client.await.unwrap();
        if known {
            assert!(response.starts_with("HTTP/1.1 204 "));
            assert_eq!(response.matches("set-cookie:").count(), 2);
            assert_eq!(response.matches("Max-Age=0;").count(), 2);
        } else {
            assert!(response.starts_with("HTTP/1.1 503 "));
            assert!(!response.contains("set-cookie:"));
        }
    }
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn actual_incomplete_sockets_are_bounded_and_joined_at_stop() {
    let (mut server, address, budget) = server().await;
    let mut clients = Vec::new();
    for _ in 0..transport::CONNECTIONS {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(b"POST /management/v1/sessions HTTP/1.1\r\n")
            .await
            .unwrap();
        clients.push(socket);
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while budget.snapshot().active_permits() != 16 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut excess = TcpStream::connect(address).await.unwrap();
    let mut buffer = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), excess.read(&mut buffer))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(budget.snapshot().active_permits(), 16);
    tokio::time::timeout(Duration::from_secs(1), server.stop())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
    assert!(TcpStream::connect(address).await.is_err());
    for mut socket in clients {
        assert_eq!(socket.read(&mut buffer).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn expired_connection_leaves_queued_command_and_its_grant_until_collected() {
    let (mut server, address, budget) = server().await;
    let request = login_request(
        address,
        "",
        r#"{"username":"owner","password":"correct horse battery"}"#,
    );
    let client = tokio::spawn(exchange(address, request));
    let command = server.commands.recv().await.unwrap();
    assert!(!command.completion.abandoned());
    assert_eq!(budget.snapshot().active_permits(), 2);
    assert_eq!(client.await.unwrap(), "");
    assert!(command.completion.abandoned());
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(command);
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn management_stop_cannot_finish_or_release_grants_before_blocking_worker_joins() {
    use std::sync::{Arc, Condvar, Mutex};
    // The controlled closure is a lifecycle discriminator, not substitute
    // cryptographic evidence. Native fixtures separately execute actual Argon2.
    struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            *self.0.0.lock().unwrap() = true;
            self.0.1.notify_all();
        }
    }
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            256 << 20,
            1 << 20,
            ResourceLimits::new(128 << 20, 34, 0, 1 << 20),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let delivery = delivery_gate(Duration::from_secs(5));
    let server =
        transport::Server::from_listener(listener, budget.clone(), delivery.clone(), None).unwrap();
    let mut management = Management {
        server,
        hashes: JoinSet::new(),
        budget: budget.clone(),
        delivery,
        export_jobs: false,
        export_resume: false,
        job_pages: false,
        cursor_lifetime: CursorLifetime {
            ttl_us: 900_000_000,
            skew_us: 30_000_000,
        },
        enrollment: None,
    };
    let client = tokio::spawn(exchange(
        address,
        login_request(
            address,
            "",
            r#"{"username":"owner","password":"retained until worker completion"}"#,
        ),
    ));
    let transport::Command {
        operation,
        completion,
    } = management.server.commands.recv().await.unwrap();
    let grant = budget
        .try_acquire(ResourceRequest::data(HASH_RESERVATION, 1, 0).unwrap())
        .unwrap();
    let barrier = Arc::new((Mutex::new(false), Condvar::new()));
    let release = ReleaseOnDrop(Arc::clone(&barrier));
    let (started, entered) = tokio::sync::oneshot::channel();
    management.hashes.spawn_blocking(move || {
        let _ = started.send(());
        let (lock, wake) = &*barrier;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = wake.wait(released).unwrap();
        }
        drop(operation);
        drop(grant);
        (completion, Err(SessionError::Denied))
    });
    entered.await.unwrap();
    let stopped = tokio::spawn(async move { management.stop(Duration::from_secs(2)).await });
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(500), client)
            .await
            .unwrap()
            .unwrap(),
        ""
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !stopped.is_finished(),
        "stop returned while its worker was blocked"
    );
    assert_eq!(
        budget.snapshot().active_permits(),
        2,
        "request/hash grants must survive closed HTTP"
    );
    drop(release);
    tokio::time::timeout(Duration::from_secs(1), stopped)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[tokio::test]
async fn admission_stops_before_the_supervisor_joins_retained_work() {
    let (mut server, address, budget) = server().await;
    let client = tokio::spawn(exchange(
        address,
        format!(
            "GET /management/v1/me HTTP/1.1\r\nHost: {address}\r\nCookie: BigBrotrLocalSession={}\r\n\r\n",
            "11".repeat(32)
        ),
    ));
    let pending = server.commands.recv().await.unwrap();
    server.request_stop();
    let response = tokio::time::timeout(Duration::from_secs(1), client)
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.is_empty(),
        "shutdown must not deliver pending work"
    );
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    assert!(pending.completion.abandoned());
    assert_eq!(
        budget.snapshot().active_permits(),
        1,
        "work custody survives socket shutdown"
    );
    drop(pending);
    server.stop().await.unwrap();
    assert_eq!(budget.snapshot().active_permits(), 0);
}
