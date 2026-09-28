use base64::{Engine, engine::general_purpose::STANDARD};
use bigbrotr::{
    network::{
        CancelAttempt, DirectAttempt, DirectConnector, DirectPolicy, DnsResolver, Target, TlsPolicy,
    },
    runtime::{
        ProcessEnvelope, ResourceBudget, ResourceLimits, WaiterLimits,
        policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    },
};
use bigbrotr_model::{DatasetId, PolicyEpoch, ProcessIncarnation};
use bigbrotr_protocol::endpoint::Endpoint;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::{TcpListener, TcpSocket},
    time::Instant,
};

pub struct Connection {
    pub listener: TcpListener,
    pub resources: ResourceBudget,
    pub connector: DirectConnector,
    pub attempt: DirectAttempt<'static>,
    pub cancel: CancelAttempt,
}

pub async fn connection() -> Connection {
    connection_with_receive_buffer(None).await
}

pub async fn connection_with_receive_buffer(receive_bytes: Option<u32>) -> Connection {
    connection_options(receive_bytes, false).await
}

pub async fn tls_connection() -> Connection {
    connection_options(None, true).await
}

async fn connection_options(receive_bytes: Option<u32>, tls: bool) -> Connection {
    let socket = TcpSocket::new_v4().unwrap();
    if let Some(bytes) = receive_bytes {
        socket.set_recv_buffer_size(bytes).unwrap();
    }
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(8).unwrap();
    let address = listener.local_addr().unwrap();
    let scheme = if tls { "wss" } else { "ws" };
    let endpoint =
        Endpoint::parse(&format!("{scheme}://{address}/events?order=1&order=2")).unwrap();
    let resources = ResourceBudget::new(
        ProcessEnvelope::new(
            64 * 1024 * 1024,
            0,
            ResourceLimits::new(64 * 1024 * 1024, 8, 0, 0),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let resolver = DnsResolver::new("127.0.0.1:9".parse().unwrap(), resources.clone()).unwrap();
    let roots = if tls {
        vec![
            include_bytes!("../../../../tests/fixtures/relay-lifecycle/transport/ca.der")
                .to_vec()
                .into(),
        ]
    } else {
        vec![]
    };
    let connector =
        DirectConnector::new(resolver, TlsPolicy::new(&roots).unwrap(), resources.clone());
    let target = Target::new(endpoint.clone()).unwrap();
    let policy = DirectPolicy::private(vec![
        bigbrotr::network::PrivateRule::endpoint(endpoint, "127.0.0.1/32", address.port()).unwrap(),
    ])
    .unwrap();
    let dataset = DatasetId::try_from([1; 16]).unwrap();
    let incarnation = ProcessIncarnation::try_from([2; 16]).unwrap();
    let epoch = PolicyEpoch::new(1);
    let mut tracker = PolicyLeaseTracker::new(
        dataset,
        incarnation,
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
                process_incarnation: incarnation,
                policy_epoch: epoch,
                ttl: Duration::from_secs(5),
            },
        )
        .unwrap();
    let window = tracker
        .begin_action(epoch)
        .unwrap()
        .complete(Duration::from_secs(5))
        .unwrap();
    let (attempt, cancel) = DirectAttempt::new(
        target,
        policy,
        window,
        Instant::now() + Duration::from_secs(5),
    );
    Connection {
        listener,
        resources,
        connector,
        attempt,
        cancel,
    }
}

pub async fn handshake(stream: &mut (impl AsyncRead + Unpin), address: SocketAddr) -> String {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        assert!(request.len() < 4096);
        request.push(stream.read_u8().await.unwrap());
    }
    let request = std::str::from_utf8(&request).unwrap();
    assert!(request.starts_with("GET /events?order=1&order=2 HTTP/1.1\r\n"));
    assert!(request.contains(&format!("Host: {address}\r\n")));
    let key = request
        .lines()
        .find_map(|line| line.strip_prefix("Sec-WebSocket-Key: "))
        .unwrap();
    let challenge = format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let digest = ring::digest::digest(
        &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
        challenge.as_bytes(),
    );
    format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        STANDARD.encode(digest.as_ref())
    )
}
