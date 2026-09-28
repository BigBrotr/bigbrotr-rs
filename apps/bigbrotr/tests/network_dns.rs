use bigbrotr::network::{DnsError, DnsFamilyOutcome, DnsResolver, Target};
use bigbrotr::runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits};
use bigbrotr_protocol::endpoint::Endpoint;
use std::{net::IpAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout},
};

fn budget(bytes: u64) -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            bytes,
            0,
            ResourceLimits::new(bytes, 1, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}

fn target() -> Target {
    Target::new(Endpoint::parse("wss://fixture.example/a?secret=one").unwrap()).unwrap()
}

async fn question(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let length = socket.read_u16().await.unwrap();
    let mut bytes = vec![0; usize::from(length)];
    socket.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes[4..6], &[0, 1]);
    assert_eq!(&bytes[12..29], b"\x07fixture\x07example\0");
    bytes
}

// The peer writes literal RFC1035 bytes and never calls Hickory or product codecs.
fn answer(query: &[u8], address: &[u8]) -> Vec<u8> {
    let mut bytes = vec![query[0], query[1], 0x81, 0x80, 0, 1, 0, 1, 0, 0, 0, 0];
    bytes.extend_from_slice(&query[12..33]);
    bytes.extend_from_slice(&[0xc0, 0x0c]);
    bytes.extend_from_slice(&query[29..33]);
    bytes.extend_from_slice(&[0, 0, 0, 60, 0, address.len() as u8]);
    bytes.extend_from_slice(address);
    bytes
}

async fn reply(socket: &mut tokio::net::TcpStream, bytes: &[u8]) {
    socket.write_u16(bytes.len() as u16).await.unwrap();
    socket.write_all(bytes).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn real_dns_resolves_both_families_and_releases_owned_resources() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resources = budget(4 * 1024 * 1024);
    let resolver = DnsResolver::new(server.local_addr().unwrap(), resources.clone()).unwrap();
    let peer = tokio::spawn(async move {
        let mut types = Vec::new();
        for _ in 0..2 {
            let (mut socket, _) = server.accept().await.unwrap();
            let query = question(&mut socket).await;
            let kind = u16::from_be_bytes([query[29], query[30]]);
            types.push(kind);
            let address = if kind == 1 {
                vec![1, 1, 1, 1]
            } else {
                vec![
                    0x26, 0x06, 0x47, 0, 0x47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x11, 0x11,
                ]
            };
            reply(&mut socket, &answer(&query, &address)).await;
            let mut byte = [0];
            assert_eq!(
                socket.read(&mut byte).await.unwrap(),
                0,
                "DNS socket must close after its one response"
            );
        }
        assert_eq!(types, vec![1, 28]);
    });
    let result = resolver
        .resolve(&target(), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(
        result.addresses(),
        &[
            "1.1.1.1".parse::<IpAddr>().unwrap(),
            "2606:4700:4700::1111".parse().unwrap()
        ]
    );
    timeout(Duration::from_secs(1), peer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resources.snapshot().active_permits(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn unavailable_budget_opens_no_dns_socket() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resolver = DnsResolver::new(server.local_addr().unwrap(), budget(1)).unwrap();
    assert_eq!(
        resolver
            .resolve(&target(), Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err(),
        DnsError::Budget
    );
    assert!(
        timeout(Duration::from_millis(40), server.accept())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn timeout_closes_the_silent_socket_before_releasing_capacity() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resources = budget(4 * 1024 * 1024);
    let resolver = DnsResolver::new(server.local_addr().unwrap(), resources.clone()).unwrap();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = server.accept().await.unwrap();
        let _query = question(&mut socket).await;
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    assert_eq!(
        resolver
            .resolve(&target(), Instant::now() + Duration::from_millis(100))
            .await
            .unwrap_err(),
        DnsError::Deadline
    );
    assert_eq!(resources.snapshot().active_permits(), 0);
    peer.await.unwrap();
    // Keep the resolver and runtime alive: EOF must not rely on process exit.
}

#[tokio::test(flavor = "current_thread")]
async fn response_limits_precede_decoder_allocation_and_other_family_success() {
    for body in [
        vec![0; 4097],
        vec![0, 0, 0x81, 0x80, 255, 255, 0, 0, 0, 0, 0, 0],
    ] {
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let resolver =
            DnsResolver::new(server.local_addr().unwrap(), budget(4 * 1024 * 1024)).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = server.accept().await.unwrap();
            let query = question(&mut socket).await;
            let mut body = body;
            body[..2].copy_from_slice(&query[..2]);
            reply(&mut socket, &body).await;
            assert!(
                timeout(Duration::from_millis(100), server.accept())
                    .await
                    .is_err(),
                "fatal limits must stop resolution, not try another family"
            );
        });
        assert_eq!(
            resolver
                .resolve(&target(), Instant::now() + Duration::from_secs(1))
                .await
                .unwrap_err(),
            DnsError::ResponseLimit
        );
        peer.await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cname_cannot_leak_overlay_questions_or_supply_overlay_addresses_in_one_packet() {
    for suffix in ["onion", "i2p", "loki", "snode"] {
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let resolver =
            DnsResolver::new(server.local_addr().unwrap(), budget(4 * 1024 * 1024)).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = server.accept().await.unwrap();
            let query = question(&mut socket).await;
            let mut cname = b"\x05relay".to_vec();
            cname.push(suffix.len() as u8);
            cname.extend_from_slice(suffix.as_bytes());
            cname.push(0);
            let mut bytes = answer(&query, &cname);
            bytes[35..37].copy_from_slice(&[0, 5]); // CNAME RR type
            // Even an address supplied for that overlay in this same message
            // cannot turn it into a direct-route destination without another query.
            bytes[7] = 2;
            bytes.extend_from_slice(&cname);
            bytes.extend_from_slice(&[0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 1, 1, 1, 1]);
            reply(&mut socket, &bytes).await;
            assert!(
                timeout(Duration::from_millis(100), server.accept())
                    .await
                    .is_err()
            );
        });
        assert_eq!(
            resolver
                .resolve(&target(), Instant::now() + Duration::from_secs(1))
                .await
                .unwrap_err(),
            DnsError::NamePolicy
        );
        peer.await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn direct_cancellation_and_concurrent_refusal_retain_exact_socket_ownership() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resources = budget(4 * 1024 * 1024);
    let resolver = std::sync::Arc::new(
        DnsResolver::new(server.local_addr().unwrap(), resources.clone()).unwrap(),
    );
    let worker_resolver = std::sync::Arc::clone(&resolver);
    let worker = tokio::spawn(async move {
        worker_resolver
            .resolve(&target(), Instant::now() + Duration::from_secs(10))
            .await
    });
    let (mut socket, _) = timeout(Duration::from_secs(1), server.accept())
        .await
        .unwrap()
        .unwrap();
    let _query = question(&mut socket).await;
    assert_eq!(resources.snapshot().active_permits(), 1);
    assert_eq!(
        resolver
            .resolve(&target(), Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err(),
        DnsError::Budget
    );
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    let mut byte = [0];
    assert_eq!(
        timeout(Duration::from_secs(1), socket.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(resources.snapshot().active_permits(), 0);
    assert!(
        timeout(Duration::from_millis(40), server.accept())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_response_binding_fails_the_whole_lookup() {
    for case in 0..7 {
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let resolver =
            DnsResolver::new(server.local_addr().unwrap(), budget(4 * 1024 * 1024)).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = server.accept().await.unwrap();
            let query = question(&mut socket).await;
            let mut bytes = answer(&query, &[1, 1, 1, 1]);
            match case {
                0 => bytes[0] ^= 1,    // ID
                1 => bytes[2] &= 0x7f, // response flag
                2 => bytes[2] |= 0x08, // opcode
                3 => bytes[2] |= 0x02, // TCP truncation
                4 => bytes[13] = b'g', // question name
                5 => bytes[32] = 3,    // question class
                6 => bytes.push(0),    // trailing frame data
                _ => unreachable!(),
            }
            reply(&mut socket, &bytes).await;
            assert!(
                timeout(Duration::from_millis(40), server.accept())
                    .await
                    .is_err()
            );
        });
        assert_eq!(
            resolver
                .resolve(&target(), Instant::now() + Duration::from_secs(1))
                .await
                .unwrap_err(),
            DnsError::InvalidResponse,
            "case {case}"
        );
        peer.await.unwrap();
    }
}

fn empty_answer(query: &[u8]) -> Vec<u8> {
    let mut bytes = vec![query[0], query[1], 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0];
    bytes.extend_from_slice(&query[12..33]);
    bytes
}

#[tokio::test(flavor = "current_thread")]
async fn a_same_packet_cname_uses_only_its_bound_address_and_no_cache_hides_a_change() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resolver = DnsResolver::new(server.local_addr().unwrap(), budget(4 * 1024 * 1024)).unwrap();
    let peer = tokio::spawn(async move {
        for ordinal in 0..4 {
            let (mut socket, _) = server.accept().await.unwrap();
            let query = question(&mut socket).await;
            if ordinal % 2 == 1 {
                assert_eq!(&query[29..31], &[0, 28]);
                reply(&mut socket, &empty_answer(&query)).await;
            } else {
                let cname = b"\x05alias\x07example\0";
                let mut bytes = answer(&query, cname);
                bytes[35..37].copy_from_slice(&[0, 5]);
                bytes[7] = 3;
                bytes.extend_from_slice(cname);
                bytes.extend_from_slice(&[
                    0,
                    1,
                    0,
                    1,
                    0,
                    0,
                    0,
                    60,
                    0,
                    4,
                    1,
                    1,
                    1,
                    if ordinal == 0 { 1 } else { 2 },
                ]);
                bytes.extend_from_slice(b"\x05other\x07example\0");
                bytes.extend_from_slice(&[0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 10, 9, 8, 7]);
                reply(&mut socket, &bytes).await;
            }
        }
    });
    for value in ["1.1.1.1", "1.1.1.2"] {
        let result = resolver
            .resolve(&target(), Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(result.addresses(), &[value.parse::<IpAddr>().unwrap()]);
        assert_eq!(result.ipv4, DnsFamilyOutcome::Answered);
        assert_eq!(result.ipv6, DnsFamilyOutcome::NoRecords);
    }
    timeout(Duration::from_secs(1), peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn an_unavailable_family_does_not_fabricate_failure_of_the_other_family() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resolver = DnsResolver::new(server.local_addr().unwrap(), budget(4 * 1024 * 1024)).unwrap();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = server.accept().await.unwrap();
        let _query = question(&mut socket).await;
        drop(socket); // real EOF before response, not an empty DNS answer
        let (mut socket, _) = server.accept().await.unwrap();
        let query = question(&mut socket).await;
        assert_eq!(&query[29..31], &[0, 28]);
        reply(
            &mut socket,
            &answer(
                &query,
                &[
                    0x26, 0x06, 0x47, 0, 0x47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x11, 0x11,
                ],
            ),
        )
        .await;
    });
    let result = resolver
        .resolve(&target(), Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(result.ipv4, DnsFamilyOutcome::Unavailable);
    assert_eq!(result.ipv6, DnsFamilyOutcome::Answered);
    assert_eq!(
        result.addresses(),
        &["2606:4700:4700::1111".parse::<IpAddr>().unwrap()]
    );
    timeout(Duration::from_secs(1), peer)
        .await
        .unwrap()
        .unwrap();
}
