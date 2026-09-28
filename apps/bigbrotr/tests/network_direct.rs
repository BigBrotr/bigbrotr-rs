use bigbrotr::{
    network::{
        DirectAttempt, DirectConnector, DirectPolicy, DnsResolver, Target, TlsPolicy,
        TransportError,
    },
    runtime::{
        ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits,
        policy_authority::{ActionWindow, LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
};
use bigbrotr_model::{DatasetId, PolicyEpoch, ProcessIncarnation};
use bigbrotr_protocol::endpoint::Endpoint;
use std::{io, net::IpAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout},
};

const CA: &[u8] = include_bytes!("../../../tests/fixtures/relay-lifecycle/transport/ca.der");
const WRONG_CA: &[u8] =
    include_bytes!("../../../tests/fixtures/relay-lifecycle/transport/wrong-ca.der");
const CERT: &[u8] = include_bytes!("../../../tests/fixtures/relay-lifecycle/transport/server.der");
const KEY: &[u8] =
    include_bytes!("../../../tests/fixtures/relay-lifecycle/transport/server-test-key.der");

fn tls_server() -> tokio_rustls::TlsAcceptor {
    let mut config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![CERT.to_vec().into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(KEY.to_vec()).into(),
    )
    .unwrap();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    std::sync::Arc::new(config).into()
}

// Independent RFC1035 wire fixture with literal A/AAAA results. Never use the
// production resolver or serializer as oracle.
async fn dns_query(socket: &mut tokio::net::TcpStream, name: &str, kind: u16) -> Vec<u8> {
    let len = socket.read_u16().await.unwrap();
    assert!(len < 512);
    let mut query = vec![0; usize::from(len)];
    socket.read_exact(&mut query).await.unwrap();
    let mut question = Vec::new();
    for label in name.split('.') {
        question.push(label.len() as u8);
        question.extend_from_slice(label.as_bytes());
    }
    question.push(0);
    question.extend_from_slice(&kind.to_be_bytes());
    question.extend_from_slice(&[0, 1]);
    assert_eq!(&query[12..], &question);
    query
}

fn addresses(values: &[&str]) -> Vec<IpAddr> {
    values.iter().map(|value| value.parse().unwrap()).collect()
}

async fn dns_answers(listener: TcpListener, name: &str, sets: &[Vec<IpAddr>]) {
    for addresses in sets {
        for kind in [1u16, 28] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let query = dns_query(&mut socket, name, kind).await;
            let family: Vec<_> = addresses
                .iter()
                .filter(|address| address.is_ipv4() == (kind == 1))
                .collect();
            let count = family.len();
            let mut reply = vec![
                query[0],
                query[1],
                0x81,
                0x80,
                0,
                1,
                0,
                count as u8,
                0,
                0,
                0,
                0,
            ];
            reply.extend_from_slice(&query[12..]);
            for address in family {
                reply.extend_from_slice(&[
                    0xc0,
                    0x0c,
                    0,
                    kind as u8,
                    0,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    if kind == 1 { 4 } else { 16 },
                ]);
                match address {
                    IpAddr::V4(ip) => reply.extend_from_slice(&ip.octets()),
                    IpAddr::V6(ip) => reply.extend_from_slice(&ip.octets()),
                }
            }
            socket.write_u16(reply.len() as u16).await.unwrap();
            socket.write_all(&reply).await.unwrap();
            closed(&mut socket).await;
        }
    }
    assert!(
        timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err(),
        "unexpected second resolution/fallback"
    );
}

fn budget(bytes: u64, items: u64) -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            bytes,
            0,
            ResourceLimits::new(bytes, items, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}

// Temporal fixture only: no enrollment or authenticated Control claim.
fn window(ttl: Duration) -> ActionWindow {
    let dataset = DatasetId::try_from([1; 16]).unwrap();
    let process = ProcessIncarnation::try_from([2; 16]).unwrap();
    let epoch = PolicyEpoch::new(1);
    let mut tracker = PolicyLeaseTracker::new(
        dataset,
        process,
        epoch,
        PolicyLeaseTtl::new(Duration::from_secs(5)).unwrap(),
    )
    .unwrap();
    let ticket = tracker.begin_refresh().unwrap();
    tracker
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
    tracker.begin_action(epoch).unwrap().complete(ttl).unwrap()
}

fn target(port: u16, tls: bool) -> Target {
    Target::new(
        Endpoint::parse(&format!(
            "{}://127.0.0.1:{port}/a%2Fb?secret=one&x=2",
            if tls { "wss" } else { "ws" }
        ))
        .unwrap(),
    )
    .unwrap()
}
fn private(target: &Target) -> DirectPolicy {
    DirectPolicy::private(vec![
        bigbrotr::network::PrivateRule::endpoint(
            target.endpoint().clone(),
            "127.0.0.1/32",
            target.port(),
        )
        .unwrap(),
    ])
    .unwrap()
}
fn connector(dns: &TcpListener, resources: ResourceBudget) -> DirectConnector {
    DirectConnector::new(
        DnsResolver::new(dns.local_addr().unwrap(), resources.clone()).unwrap(),
        TlsPolicy::new(&[]).unwrap(),
        resources,
    )
}

async fn closed(socket: &mut tokio::net::TcpStream) {
    let mut byte = [0];
    assert_eq!(
        timeout(Duration::from_secs(1), socket.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn pinned_plaintext_owns_the_socket_and_permit_through_the_operation() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = target(listener.local_addr().unwrap().port(), false);
    let resources = budget(16 * 1024 * 1024, 2);
    let connector = connector(&dns, resources.clone());
    let (attempt, _cancel) = DirectAttempt::new(
        target.clone(),
        private(&target),
        window(Duration::from_secs(2)),
        Instant::now() + Duration::from_secs(2),
    );
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4];
        socket.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"ping");
        socket.write_all(b"pong").await.unwrap();
        closed(&mut socket).await;
    });
    let observed = connector
        .run(attempt, async |stream| {
            assert_eq!(resources.snapshot().active_permits(), 1);
            assert_eq!(stream.endpoint(), target.endpoint());
            assert_eq!(stream.peer_addr().ip().to_string(), "127.0.0.1");
            stream.write_all(b"ping").await?;
            stream.flush().await?;
            let mut bytes = [0; 4];
            stream.read_exact(&mut bytes).await?;
            Ok::<_, io::Error>(bytes)
        })
        .await
        .unwrap();
    assert_eq!(&observed, b"pong");
    peer.await.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
    assert!(
        timeout(Duration::from_millis(40), dns.accept())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn refused_policy_budget_and_cancelled_work_contact_no_peer() {
    for case in 0..4 {
        let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = target(listener.local_addr().unwrap().port(), false);
        let resources = budget(if case == 1 { 1 } else { 16 * 1024 * 1024 }, 2);
        let connector = connector(&dns, resources.clone());
        let policy = if case == 0 {
            DirectPolicy::public()
        } else {
            private(&target)
        };
        let (attempt, cancel) = DirectAttempt::new(
            target,
            policy,
            window(Duration::from_secs(2)),
            if case == 3 {
                Instant::now()
            } else {
                Instant::now() + Duration::from_secs(2)
            },
        );
        let mut cancel = Some(cancel);
        if case == 2 {
            cancel.take();
        }
        let result = connector
            .run(attempt, async |_| -> io::Result<()> {
                panic!("refused operation dispatched")
            })
            .await;
        assert!(matches!(
            (case, result),
            (0, Err(TransportError::Policy(_)))
                | (1, Err(TransportError::Budget))
                | (2, Err(TransportError::Cancelled))
                | (3, Err(TransportError::Deadline))
        ));
        assert_eq!(resources.snapshot().active_permits(), 0);
        assert!(
            timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
        assert!(
            timeout(Duration::from_millis(30), dns.accept())
                .await
                .is_err()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn idle_reads_end_on_authority_expiry_deadline_or_revocation() {
    for case in 0..3 {
        let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = target(listener.local_addr().unwrap().port(), false);
        let resources = budget(16 * 1024 * 1024, 2);
        let connector = connector(&dns, resources.clone());
        let (attempt, cancel) = DirectAttempt::new(
            target.clone(),
            private(&target),
            window(Duration::from_millis(if case == 0 { 100 } else { 2000 })),
            Instant::now() + Duration::from_millis(if case == 1 { 100 } else { 2000 }),
        );
        let (accepted, seen) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            accepted.send(()).unwrap();
            closed(&mut socket).await;
        });
        let cancellation = async {
            seen.await.unwrap();
            if case == 2 {
                drop(cancel)
            } else {
                tokio::time::sleep(Duration::from_millis(250)).await;
                drop(cancel)
            }
        };
        let operation = connector.run(attempt, async |stream| {
            let mut byte = [0];
            stream.read_exact(&mut byte).await?;
            Ok::<_, io::Error>(())
        });
        let (result, ()) = timeout(Duration::from_secs(1), async {
            tokio::join!(operation, cancellation)
        })
        .await
        .unwrap();
        assert!(matches!(
            (case, result),
            (0, Err(TransportError::Authority))
                | (1, Err(TransportError::Deadline))
                | (2, Err(TransportError::Cancelled))
        ));
        peer.await.unwrap();
        assert_eq!(resources.snapshot().active_permits(), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_tls_preserves_name_path_query_and_filters_mixed_dns_answers() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let target = Target::new(
        Endpoint::parse(&format!(
            "wss://fixture.example:{port}/a%2Fb?secret=one&x=2"
        ))
        .unwrap(),
    )
    .unwrap();
    let resources = budget(16 * 1024 * 1024, 2);
    let connector = DirectConnector::new(
        DnsResolver::new(dns.local_addr().unwrap(), resources.clone()).unwrap(),
        TlsPolicy::new(&[CA.into()]).unwrap(),
        resources.clone(),
    );
    let dns_peer = tokio::spawn(async move {
        dns_answers(dns, "fixture.example", &[addresses(&["127.0.0.1", "::1"])]).await
    });
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut tls = tls_server().accept(socket).await.unwrap();
        assert_eq!(tls.get_ref().1.server_name(), Some("fixture.example"));
        assert_eq!(
            tls.get_ref().1.alpn_protocol(),
            Some(b"http/1.1".as_slice())
        );
        let expected =
            format!("GET /a%2Fb?secret=one&x=2 HTTP/1.1\r\nHost: fixture.example:{port}\r\n\r\n");
        let mut request = vec![0; expected.len()];
        tls.read_exact(&mut request).await.unwrap();
        assert_eq!(request, expected.as_bytes());
        tls.write_all(b"OK").await.unwrap();
        tls.flush().await.unwrap();
        // The operation owner drops without waiting for a TLS close handshake.
        let mut byte = [0];
        assert!(matches!(
            timeout(Duration::from_secs(1), tls.read(&mut byte))
                .await
                .unwrap(),
            Ok(0) | Err(_)
        ));
    });
    let (attempt, _cancel) = DirectAttempt::new(
        target.clone(),
        private(&target),
        window(Duration::from_secs(2)),
        Instant::now() + Duration::from_secs(2),
    );
    connector
        .run(attempt, async |stream| {
            let url = url::Url::parse(stream.endpoint().as_str()).unwrap();
            assert_eq!(stream.peer_addr().ip().to_string(), "127.0.0.1");
            stream
                .write_all(
                    format!(
                        "GET {}?{} HTTP/1.1\r\nHost: {}:{}\r\n\r\n",
                        url.path(),
                        url.query().unwrap(),
                        url.host_str().unwrap(),
                        url.port().unwrap()
                    )
                    .as_bytes(),
                )
                .await?;
            stream.flush().await?;
            let mut response = [0; 2];
            stream.read_exact(&mut response).await?;
            assert_eq!(&response, b"OK");
            Ok::<_, io::Error>(())
        })
        .await
        .unwrap();
    peer.await.unwrap();
    dns_peer.await.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn wrong_ca_or_original_name_never_dispatches_protocol_data() {
    for wrong_name in [false, true] {
        let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let name = if wrong_name {
            "other.example"
        } else {
            "fixture.example"
        };
        let target = Target::new(
            Endpoint::parse(&format!(
                "wss://{name}:{}/secret?token=canary",
                listener.local_addr().unwrap().port()
            ))
            .unwrap(),
        )
        .unwrap();
        let resources = budget(16 * 1024 * 1024, 2);
        let connector = DirectConnector::new(
            DnsResolver::new(dns.local_addr().unwrap(), resources.clone()).unwrap(),
            TlsPolicy::new(&[if wrong_name { CA } else { WRONG_CA }.into()]).unwrap(),
            resources.clone(),
        );
        let dns_peer =
            tokio::spawn(async move { dns_answers(dns, name, &[addresses(&["127.0.0.1"])]).await });
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            assert!(tls_server().accept(socket).await.is_err());
            assert!(
                timeout(Duration::from_millis(40), listener.accept())
                    .await
                    .is_err()
            );
        });
        let (attempt, _cancel) = DirectAttempt::new(
            target.clone(),
            private(&target),
            window(Duration::from_secs(2)),
            Instant::now() + Duration::from_secs(2),
        );
        let result = connector
            .run(attempt, async |_| -> io::Result<()> {
                panic!("TLS refusal dispatched protocol")
            })
            .await;
        assert_eq!(result, Err(TransportError::Tls));
        assert!(!format!("{}", result.unwrap_err()).contains("canary"));
        peer.await.unwrap();
        dns_peer.await.unwrap();
        assert_eq!(resources.snapshot().active_permits(), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_fresh_attempt_rechecks_rebound_dns_and_never_dials_the_denied_answer() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let denied = TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, port))
        .await
        .unwrap();
    let target =
        Target::new(Endpoint::parse(&format!("ws://fixture.example:{port}/")).unwrap()).unwrap();
    let resources = budget(16 * 1024 * 1024, 2);
    let connector = connector(&dns, resources.clone());
    let dns_peer = tokio::spawn(async move {
        dns_answers(
            dns,
            "fixture.example",
            &[addresses(&["127.0.0.1"]), addresses(&["::1"])],
        )
        .await
    });
    for ordinal in 0..2 {
        let (attempt, _cancel) = DirectAttempt::new(
            target.clone(),
            private(&target),
            window(Duration::from_secs(2)),
            Instant::now() + Duration::from_secs(2),
        );
        let result = connector
            .run(attempt, async |_| Ok::<_, io::Error>(()))
            .await;
        if ordinal == 0 {
            result.unwrap();
            let (mut socket, _) = listener.accept().await.unwrap();
            closed(&mut socket).await;
        } else {
            assert!(matches!(result, Err(TransportError::Policy(_))));
        }
    }
    dns_peer.await.unwrap();
    assert!(
        timeout(Duration::from_millis(40), denied.accept())
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err()
    );
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_during_dns_or_tls_joins_socket_before_reusing_capacity() {
    for during_tls in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = if during_tls {
            target(listener.local_addr().unwrap().port(), true)
        } else {
            Target::new(Endpoint::parse("ws://fixture.example:12345/").unwrap()).unwrap()
        };
        let resources = budget(16 * 1024 * 1024, 2);
        let connector = connector(&dns, resources.clone());
        let (attempt, cancel) = DirectAttempt::new(
            target.clone(),
            private(&target),
            window(Duration::from_secs(2)),
            Instant::now() + Duration::from_secs(2),
        );
        let peer = async {
            let chosen = if during_tls { &listener } else { &dns };
            let (mut socket, _) = chosen.accept().await.unwrap();
            if during_tls {
                let mut header = [0; 5];
                socket.read_exact(&mut header).await.unwrap();
                assert_eq!(header[0], 22);
                let mut hello = vec![0; usize::from(u16::from_be_bytes([header[3], header[4]]))];
                socket.read_exact(&mut hello).await.unwrap();
            } else {
                dns_query(&mut socket, "fixture.example", 1).await;
            }
            assert_eq!(
                resources.snapshot().active_permits(),
                if during_tls { 1 } else { 2 }
            );
            drop(cancel);
            closed(&mut socket).await;
        };
        let operation = connector.run(attempt, async |_| -> io::Result<()> {
            panic!("held handshake dispatched")
        });
        let (result, ()) = timeout(Duration::from_secs(1), async {
            tokio::join!(operation, peer)
        })
        .await
        .unwrap();
        assert_eq!(result, Err(TransportError::Cancelled));
        assert_eq!(resources.snapshot().active_permits(), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn expired_attempt_does_not_resolve_and_invalid_extra_roots_are_refused() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = Target::new(Endpoint::parse("wss://fixture.example/").unwrap()).unwrap();
    let connector = connector(&dns, budget(16 * 1024 * 1024, 2));
    let (attempt, _cancel) = DirectAttempt::new(
        target,
        DirectPolicy::public(),
        window(Duration::from_millis(20)),
        Instant::now() + Duration::from_secs(2),
    );
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(
        connector
            .run(attempt, async |_| Ok::<_, io::Error>(()))
            .await,
        Err(TransportError::Authority)
    );
    assert!(
        timeout(Duration::from_millis(40), dns.accept())
            .await
            .is_err()
    );
    assert!(matches!(
        TlsPolicy::new(&[b"bad".as_slice().into()]),
        Err(TransportError::TlsConfiguration)
    ));
    assert!(matches!(
        TlsPolicy::new(&vec![CA.into(); 129]),
        Err(TransportError::TlsConfiguration)
    ));
    assert!(matches!(
        TlsPolicy::new(&[vec![0; 1024 * 1024 + 1].into()]),
        Err(TransportError::TlsConfiguration)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn a_dns_reply_after_action_expiry_cannot_start_the_next_family() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resources = budget(16 * 1024 * 1024, 2);
    let connector = connector(&dns, resources.clone());
    let target = Target::new(Endpoint::parse("ws://fixture.example:12345/").unwrap()).unwrap();
    let (attempt, _cancel) = DirectAttempt::new(
        target.clone(),
        private(&target),
        window(Duration::from_millis(10)),
        Instant::now() + Duration::from_secs(2),
    );
    let started = Instant::now();
    let peer = async {
        let (mut socket, _) = dns.accept().await.unwrap();
        let query = dns_query(&mut socket, "fixture.example", 1).await;
        tokio::time::sleep_until(started + Duration::from_millis(25)).await;
        assert!(
            started.elapsed() < Duration::from_millis(45),
            "fixture missed the interval before the next watchdog sample"
        );
        let mut reply = vec![query[0], query[1], 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0];
        reply.extend_from_slice(&query[12..]);
        // One small response write: the correct client may close immediately on
        // readiness after expiry. Splitting length/body introduced a Linux
        // BrokenPipe fixture race before the forbidden-next-connection oracle.
        let mut frame = (reply.len() as u16).to_be_bytes().to_vec();
        frame.extend_from_slice(&reply);
        socket.write_all(&frame).await.unwrap();
        assert!(
            timeout(Duration::from_millis(100), dns.accept())
                .await
                .is_err(),
            "expired action started another DNS exchange on resumed work"
        );
    };
    let operation = connector.run(attempt, async |_| Ok::<_, io::Error>(()));
    let (result, ()) = timeout(Duration::from_secs(1), async {
        tokio::join!(operation, peer)
    })
    .await
    .unwrap();
    assert_eq!(result, Err(TransportError::Authority));
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn revoked_tls_refuses_already_decrypted_bytes_and_new_writes() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = target(listener.local_addr().unwrap().port(), true);
    let resources = budget(16 * 1024 * 1024, 2);
    let connector = DirectConnector::new(
        DnsResolver::new(dns.local_addr().unwrap(), resources.clone()).unwrap(),
        TlsPolicy::new(&[CA.into()]).unwrap(),
        resources.clone(),
    );
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut tls = tls_server().accept(socket).await.unwrap();
        tls.write_all(b"AB").await.unwrap();
        tls.flush().await.unwrap();
        let mut bytes = [0; 2];
        assert!(
            matches!(
                timeout(Duration::from_secs(1), tls.read(&mut bytes))
                    .await
                    .unwrap(),
                Ok(0) | Err(_)
            ),
            "revoked application output reached the server"
        );
    });
    let (attempt, cancel) = DirectAttempt::new(
        target.clone(),
        private(&target),
        window(Duration::from_secs(2)),
        Instant::now() + Duration::from_secs(2),
    );
    let mut refused = false;
    let result = connector
        .run(attempt, async |stream| {
            let mut first = [0];
            stream.read_exact(&mut first).await?;
            assert_eq!(&first, b"A");
            drop(cancel);
            // These operations run immediately in the same callback poll. The
            // outer supervisor cannot substitute for guards around the TLS buffer.
            assert_eq!(
                stream.read(&mut first).await.unwrap_err().kind(),
                io::ErrorKind::PermissionDenied
            );
            assert_eq!(
                stream.write(b"forbidden").await.unwrap_err().kind(),
                io::ErrorKind::PermissionDenied
            );
            assert_eq!(
                stream.flush().await.unwrap_err().kind(),
                io::ErrorKind::PermissionDenied
            );
            assert_eq!(
                stream.shutdown().await.unwrap_err().kind(),
                io::ErrorKind::PermissionDenied
            );
            refused = true;
            Ok::<_, io::Error>(())
        })
        .await;
    assert_eq!(result, Err(TransportError::Cancelled));
    assert!(refused);
    peer.await.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_a_pending_run_closes_its_socket_without_a_background_driver() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = target(listener.local_addr().unwrap().port(), false);
    let resources = budget(16 * 1024 * 1024, 2);
    let connector = connector(&dns, resources.clone());
    let (attempt, _cancel) = DirectAttempt::new(
        target.clone(),
        private(&target),
        window(Duration::from_secs(2)),
        Instant::now() + Duration::from_secs(2),
    );
    let mut work = Box::pin(connector.run(attempt, async |stream| {
        let mut byte = [0];
        stream.read_exact(&mut byte).await?;
        Ok::<_, io::Error>(())
    }));
    let mut socket = tokio::select! {
        value=listener.accept() => value.unwrap().0,
        result=&mut work => panic!("work ended before cancellation: {result:?}"),
        _=tokio::time::sleep(Duration::from_secs(1)) => panic!("fixture failed to connect"),
    };
    assert_eq!(resources.snapshot().active_permits(), 1);
    drop(work);
    assert_eq!(resources.snapshot().active_permits(), 0);
    closed(&mut socket).await;
}

#[tokio::test(flavor = "current_thread")]
async fn lowering_the_shared_budget_refuses_new_connections_and_drains_the_owner() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = target(listener.local_addr().unwrap().port(), false);
    let resources = budget(16 * 1024 * 1024, 2);
    let connector = connector(&dns, resources.clone());
    let (attempt, _cancel) = DirectAttempt::new(
        target.clone(),
        private(&target),
        window(Duration::from_secs(2)),
        Instant::now() + Duration::from_secs(2),
    );
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        closed(&mut socket).await;
        assert!(
            timeout(Duration::from_millis(40), listener.accept())
                .await
                .is_err()
        );
    });
    connector
        .run(attempt, async |_| {
            resources
                .set_limits(ResourceLimits::new(4 * 1024 * 1024, 1, 0, 0))
                .unwrap();
            assert_eq!(resources.snapshot().active_permits(), 1);
            let (next, _next_cancel) = DirectAttempt::new(
                target.clone(),
                private(&target),
                window(Duration::from_secs(2)),
                Instant::now() + Duration::from_secs(2),
            );
            assert_eq!(
                connector.run(next, async |_| Ok::<_, io::Error>(())).await,
                Err(TransportError::Budget)
            );
            assert_eq!(resources.snapshot().active_permits(), 1);
            Ok::<_, io::Error>(())
        })
        .await
        .unwrap();
    peer.await.unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}
